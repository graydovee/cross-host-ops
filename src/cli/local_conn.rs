//! Local control-channel connection (transport-agnostic).
//!
//! The CLI talks to the local daemon over either a Unix-domain socket or a
//! TCP loopback listener (chosen by `local.transport` in the config). This
//! module hides the difference behind [`LocalEndpoint`] so callers don't branch
//! on transport.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
#[cfg(unix)]
use hyper_util::rt::TokioIo;
use tokio::time::{Duration, sleep};
use tonic::transport::{Channel, Endpoint};
#[cfg(unix)]
use tower::service_fn;

use crate::config::{ClientConfig, LocalTransport};
use crate::protocol::rpc;

/// A resolved local control endpoint.
#[derive(Clone, Debug)]
pub(crate) enum LocalEndpoint {
    /// Unix-domain socket at this filesystem path.
    #[cfg(unix)]
    Unix(PathBuf),
    /// TCP loopback; the daemon publishes its actual address in this lock file.
    Tcp(PathBuf),
}

impl LocalEndpoint {
    /// Resolve the endpoint from the client config.
    pub(crate) fn from_config(client_config: &ClientConfig) -> Result<Self> {
        match client_config.local.transport {
            #[cfg(unix)]
            LocalTransport::Unix => Ok(Self::Unix(PathBuf::from(
                &client_config.local.socket_path,
            ))),
            #[cfg(not(unix))]
            LocalTransport::Unix => Err(anyhow!(
                "local transport \"unix\" is not supported on Windows; \
                 set transport = \"tcp\" in client.toml"
            )),
            LocalTransport::Tcp => Ok(Self::Tcp(PathBuf::from(
                &client_config.local.tcp_lock_file,
            ))),
        }
    }

    /// Human-readable description for diagnostics.
    pub(crate) fn describe_internal(&self) -> String {
        match self {
            #[cfg(unix)]
            Self::Unix(p) => format!("unix socket {}", p.display()),
            Self::Tcp(p) => format!("TCP lock file {}", p.display()),
        }
    }
}

/// Connect to the local daemon control channel, returning a tonic gRPC client.
pub(crate) async fn connect(endpoint: &LocalEndpoint) -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>> {
    match endpoint {
        #[cfg(unix)]
        LocalEndpoint::Unix(path) => connect_unix(path).await,
        LocalEndpoint::Tcp(lock_file) => connect_tcp(lock_file).await,
    }
}

#[cfg(unix)]
async fn connect_unix(path: &Path) -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>> {
    let path = path.to_path_buf();
    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: tonic::Uri| {
            let path = path.clone();
            async move { tokio::net::UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?;
    Ok(rpc::xho_rpc_client::XhoRpcClient::new(channel))
}

async fn connect_tcp(lock_file: &Path) -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>> {
    let addr = read_lock_file_addr(lock_file).await?;
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
    let channel = endpoint.connect().await?;
    Ok(rpc::xho_rpc_client::XhoRpcClient::new(channel))
}

/// Read the daemon's actual TCP address from its lock file.
///
/// Format: line 1 = `host:port`, line 2 = PID (used for staleness checks).
/// Returns a stale-error if the lock file is missing or the daemon PID is no
/// longer alive (so callers can trigger auto-start).
pub(crate) async fn read_lock_file_addr(lock_file: &Path) -> Result<String> {
    let content = tokio::fs::read_to_string(lock_file)
        .await
        .with_context(|| format!("failed to read TCP lock file {}", lock_file.display()))?;
    let mut lines = content.lines();
    let addr = lines
        .next()
        .ok_or_else(|| anyhow!("TCP lock file {} is empty", lock_file.display()))?
        .trim()
        .to_string();
    if addr.is_empty() {
        return Err(anyhow!(
            "TCP lock file {} has no address",
            lock_file.display()
        ));
    }
    // Optional staleness check: if a PID is present and that PID is not alive,
    // the lock file is stale.
    if let Some(pid_line) = lines.next() {
        if let Ok(pid) = pid_line.trim().parse::<u32>() {
            if !is_process_alive(pid) {
                return Err(anyhow!(
                    "TCP lock file {} is stale (daemon PID {} not running)",
                    lock_file.display(),
                    pid
                ));
            }
        }
    }
    Ok(addr)
}

/// Poll until the local endpoint is connectable, up to ~5 seconds.
pub(crate) async fn wait_for_ready(endpoint: &LocalEndpoint) -> Result<()> {
    for _ in 0..50 {
        if connect(endpoint).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!(
        "timed out waiting for local daemon at {}",
        endpoint.describe_internal()
    ))
}

#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    // kill(pid, 0) returns 0 if the process exists.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
fn is_process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    unsafe {
        let handle: HANDLE = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        CloseHandle(handle);
        true
    }
}
