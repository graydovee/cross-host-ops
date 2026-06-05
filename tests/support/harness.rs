//! Integration test harness for property tests P1–P4.
//!
//! Provides a `TestHarness` that spawns local-daemon and remote-daemon
//! `XhoRpcService` instances over `tokio::io::duplex`, backed by tempdirs
//! for filesystem operations. The harness exercises the same control flow as
//! production while remaining fully in-process.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;

use xho::config::{AppConfig, DirectAuth, ServerEntry};
use xho::daemon::gateway::GatewayKind;
use xho::daemon::test_support::make_test_rpc_service;
use xho::protocol::rpc;
use xho::protocol::rpc::xho_rpc_client::XhoRpcClient;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tower::service_fn;

/// The buffer size for the in-process duplex channel (1 MB).
const DUPLEX_BUFFER_SIZE: usize = 1024 * 1024;

/// Counter for generating unique tempdir names.
static HARNESS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Represents the direction of a copy operation for the harness helpers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum HarnessCopyDirection {
    Upload,
    Download,
}

/// An integration test harness that spawns local and remote daemon services
/// over `tokio::io::duplex`, with tempdir-backed filesystem operations.
///
/// This harness exercises the same gRPC control flow as production:
/// - CLI → local daemon (over duplex)
/// - local daemon → remote daemon (over duplex, for xhod routes)
///
/// The end target is a tempdir that the stub `Gateway` performs filesystem
/// ops against directly.
#[allow(dead_code)]
pub struct TestHarness {
    /// gRPC client connected to the "local daemon" service.
    pub local_client: XhoRpcClient<Channel>,
    /// gRPC client connected to the "remote daemon" service (for direct testing).
    pub remote_client: XhoRpcClient<Channel>,
    /// Tempdir for local-side filesystem operations (simulates the user's machine).
    pub local_tempdir: PathBuf,
    /// Tempdir for remote-side filesystem operations (simulates the end target).
    pub remote_tempdir: PathBuf,
    /// Tempdir for the local daemon's config files.
    local_config_dir: PathBuf,
    /// Tempdir for the remote daemon's config files.
    remote_config_dir: PathBuf,
    /// The config used by the local daemon.
    pub local_config: AppConfig,
    /// The config used by the remote daemon.
    pub remote_config: AppConfig,
}

#[allow(dead_code)]
impl TestHarness {
    /// Create a new test harness with default stub server entries.
    ///
    /// The remote daemon has a single stub target `stub-target` at 127.0.0.1:22.
    pub async fn new() -> Self {
        Self::with_servers(vec![ServerEntry {
            alias: "stub-target".to_string(),
            host: "127.0.0.1".to_string(),
            port: 22,
            user: "testuser".to_string(),
            auth: DirectAuth::Key {
                identity_file: "/dev/null".to_string(),
            },
        }])
        .await
    }

    /// Create a new test harness with the given server entries on the remote daemon.
    pub async fn with_servers(remote_servers: Vec<ServerEntry>) -> Self {
        let id = HARNESS_COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!("xho-harness-{}-{}", std::process::id(), id));

        // Create tempdirs for filesystem operations
        let local_tempdir = base.join("local-fs");
        let remote_tempdir = base.join("remote-fs");
        let local_config_dir = base.join("local-config");
        let remote_config_dir = base.join("remote-config");

        fs::create_dir_all(&local_tempdir).expect("failed to create local tempdir");
        fs::create_dir_all(&remote_tempdir).expect("failed to create remote tempdir");
        fs::create_dir_all(&local_config_dir).expect("failed to create local config dir");
        fs::create_dir_all(&remote_config_dir).expect("failed to create remote config dir");

        // --- Remote daemon setup ---
        let remote_server_config_path = remote_config_dir.join("server.toml");
        let remote_server_toml = build_server_toml(&remote_servers);
        fs::write(&remote_server_config_path, &remote_server_toml)
            .expect("failed to write remote server.toml");

        let remote_config_path = remote_config_dir.join("config.toml");
        let mut remote_config = AppConfig::default();
        remote_config.ssh.server_config_path = remote_server_config_path.display().to_string();
        remote_config.server.local.enable = false;
        remote_config.server.remote.enable = false;
        remote_config.review.enable = false;

        let remote_service = make_test_rpc_service(remote_config.clone(), remote_config_path);

        // --- Local daemon setup ---
        let local_server_config_path = local_config_dir.join("server.toml");
        fs::write(
            &local_server_config_path,
            "[defaults]\nuser = \"testuser\"\nport = 22\n",
        )
        .expect("failed to write local server.toml");

        let local_config_path = local_config_dir.join("config.toml");
        let mut local_config = AppConfig::default();
        local_config.ssh.server_config_path = local_server_config_path.display().to_string();
        local_config.server.local.enable = false;
        local_config.server.remote.enable = false;
        local_config.review.enable = false;

        let local_service = make_test_rpc_service(local_config.clone(), local_config_path);

        // --- Wire up remote daemon over duplex ---
        let (remote_client_io, remote_server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);
        tokio::spawn(async move {
            Server::builder()
                .add_service(remote_service)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(
                    remote_server_io,
                )))
                .await
                .expect("remote gRPC server failed");
        });
        let remote_client = connect_client_via_duplex(remote_client_io).await;

        // --- Wire up local daemon over duplex ---
        let (local_client_io, local_server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);
        tokio::spawn(async move {
            Server::builder()
                .add_service(local_service)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(local_server_io)))
                .await
                .expect("local gRPC server failed");
        });
        let local_client = connect_client_via_duplex(local_client_io).await;

        Self {
            local_client,
            remote_client,
            local_tempdir,
            remote_tempdir,
            local_config_dir,
            remote_config_dir,
            local_config,
            remote_config,
        }
    }

    /// Execute a command via the local daemon and return (exit_code, stdout, stderr).
    ///
    /// Sends a `StartRequest` to the local daemon's `Execute` RPC and collects
    /// all response events, extracting stdout/stderr bytes and the exit code.
    pub async fn cli_exec(&mut self, target: &str, argv: &[&str]) -> (i32, Vec<u8>, Vec<u8>) {
        let start_request = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: target.to_string(),
                argv: argv.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            })),
        };

        let response = self
            .local_client
            .execute(tokio_stream::once(start_request))
            .await
            .expect("Execute RPC failed");

        let mut stream = response.into_inner();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code = -1i32;

        while let Some(msg) = stream
            .message()
            .await
            .expect("failed to read Execute response stream")
        {
            if let Some(event) = msg.event {
                match event {
                    rpc::execute_response::Event::Stdout(chunk) => {
                        stdout.extend_from_slice(&chunk.data);
                    }
                    rpc::execute_response::Event::Stderr(chunk) => {
                        stderr.extend_from_slice(&chunk.data);
                    }
                    rpc::execute_response::Event::ExitStatus(status) => {
                        exit_code = status.code;
                    }
                    _ => {}
                }
            }
        }

        (exit_code, stdout, stderr)
    }

    /// Perform a copy operation via the local daemon.
    ///
    /// Sends a `CopyStartRequest` to the local daemon's `Copy` RPC and collects
    /// all response events. Returns `Ok(())` on success or the error message.
    pub async fn cli_cp(
        &mut self,
        local_path: &str,
        remote_path: &str,
        direction: HarnessCopyDirection,
    ) -> Result<(), String> {
        let rpc_direction = match direction {
            HarnessCopyDirection::Upload => rpc::CopyDirection::Upload,
            HarnessCopyDirection::Download => rpc::CopyDirection::Download,
        };

        let start_request = rpc::CopyRequest {
            request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
                target: "stub-target".to_string(),
                local_path: local_path.to_string(),
                remote_path: remote_path.to_string(),
                recursive: false,
                direction: rpc_direction as i32,
                ..Default::default()
            })),
        };

        let response = self
            .local_client
            .copy(tokio_stream::once(start_request))
            .await
            .expect("Copy RPC failed");

        let mut stream = response.into_inner();
        while let Some(msg) = stream
            .message()
            .await
            .expect("failed to read Copy response stream")
        {
            if let Some(event) = msg.event {
                match event {
                    rpc::copy_response::Event::Error(err) => {
                        return Err(err.message);
                    }
                    rpc::copy_response::Event::Complete(_) => {
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Build a target string for the given route kind, end alias, and remote path.
    ///
    /// - `Direct` → `"<end_alias>"` (bare alias, resolved as direct SSH)
    /// - `Jumpserver` → `"<end_alias>"` (bare alias, resolved via jumpserver config)
    /// - `Xhod` → `"<xhod_alias>:<end_alias>"` (explicit jump-host qualification)
    pub fn target_for(
        &self,
        route_kind: GatewayKind,
        end_alias: &str,
        _remote_path: &str,
    ) -> String {
        match route_kind {
            GatewayKind::Direct => end_alias.to_string(),
            GatewayKind::Jumpserver => end_alias.to_string(),
            GatewayKind::Xhod => format!("xhod:{}", end_alias),
        }
    }

    /// Read a file from the local tempdir.
    pub fn read_local(&self, relative_path: &str) -> Vec<u8> {
        let path = self.local_tempdir.join(relative_path);
        fs::read(&path)
            .unwrap_or_else(|e| panic!("failed to read local file {}: {}", path.display(), e))
    }

    /// Write data to a file in the local tempdir.
    pub fn write_local(&self, relative_path: &str, data: &[u8]) {
        let path = self.local_tempdir.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create parent dirs");
        }
        fs::write(&path, data)
            .unwrap_or_else(|e| panic!("failed to write local file {}: {}", path.display(), e));
    }

    /// Generate a fresh file path in the local tempdir.
    /// The file does not exist yet; the caller is responsible for creating it.
    pub fn fresh_local_path(&self) -> PathBuf {
        let id = HARNESS_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.local_tempdir
            .join(format!("file-{}-{}", std::process::id(), id))
    }

    /// Read a file from the remote tempdir (end-target filesystem).
    pub fn read_remote(&self, relative_path: &str) -> Vec<u8> {
        let path = self.remote_tempdir.join(relative_path);
        fs::read(&path)
            .unwrap_or_else(|e| panic!("failed to read remote file {}: {}", path.display(), e))
    }

    /// Write data to a file in the remote tempdir (end-target filesystem).
    pub fn write_remote(&self, relative_path: &str, data: &[u8]) {
        let path = self.remote_tempdir.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create parent dirs");
        }
        fs::write(&path, data)
            .unwrap_or_else(|e| panic!("failed to write remote file {}: {}", path.display(), e));
    }

    /// Returns the path to the local tempdir.
    pub fn local_tempdir_path(&self) -> &Path {
        &self.local_tempdir
    }

    /// Returns the path to the remote tempdir (end-target filesystem).
    pub fn remote_tempdir_path(&self) -> &Path {
        &self.remote_tempdir
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        // Best-effort cleanup of all tempdirs
        let base = self.local_tempdir.parent().unwrap_or(Path::new("/tmp"));
        let _ = fs::remove_dir_all(base);
    }
}

/// Proptest strategy that generates arbitrary `GatewayKind` values.
/// Used by property tests P1–P4 to parameterize over route kinds.
#[allow(dead_code)]
pub fn route_kind_strategy() -> impl Strategy<Value = GatewayKind> {
    prop_oneof![
        Just(GatewayKind::Direct),
        Just(GatewayKind::Jumpserver),
        Just(GatewayKind::Xhod),
    ]
}

// --- Private helpers ---

/// Connect a gRPC client through a pre-connected duplex stream.
async fn connect_client_via_duplex(io: tokio::io::DuplexStream) -> XhoRpcClient<Channel> {
    let io = std::sync::Mutex::new(Some(io));

    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(service_fn(move |_: Uri| {
            let stream = io
                .lock()
                .unwrap()
                .take()
                .expect("duplex stream already consumed");
            async move { Ok::<_, std::io::Error>(TokioIo::new(stream)) }
        }))
        .await
        .expect("failed to connect gRPC client via duplex");

    XhoRpcClient::new(channel)
}

/// Build a minimal `server.toml` content from a list of server entries.
fn build_server_toml(servers: &[ServerEntry]) -> String {
    let mut toml = String::from("[defaults]\nuser = \"testuser\"\nport = 22\n\n");

    for entry in servers {
        toml.push_str(&format!("[servers.{}]\n", entry.alias));
        toml.push_str(&format!("host = \"{}\"\n", entry.host));
        toml.push_str(&format!("port = {}\n", entry.port));
        toml.push_str(&format!("user = \"{}\"\n", entry.user));
        match &entry.auth {
            DirectAuth::Key { identity_file } => {
                toml.push_str(&format!("identity_file = \"{}\"\n", identity_file));
            }
            DirectAuth::Password { .. } => {
                toml.push_str("auth = \"password\"\n");
            }
        }
        toml.push('\n');
    }

    toml
}
