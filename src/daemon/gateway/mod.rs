// Gateway trait and supporting types.
//
// `Gateway` is THE high-level abstraction for every connection backend (direct
// SSH, localhost, a remote xhod, a reverse-proxy node, a jumpserver bastion).
// A gateway knows how to:
//   - open a low-level [`TargetSession`] to one of its end targets
//     (`open_session`) — used by the transparent proxy, the `OpenSession`
//     tunnel, and `xho cp`;
//   - open a session for the CLI exec path together with the kind-aware command
//     string to run (`open_exec_session`);
//   - enumerate its reachable servers (`list_servers`).
//
// Backends declare which operations they support via [`Capabilities`]. Callers
// gate generically on the advertised capabilities — there is no per-kind
// special-casing outside a gateway's own implementation. A backend that cannot
// realise an operation simply does not advertise the capability (and its
// default trait method returns an `Unsupported` error).

pub mod auth;
pub mod direct;
pub mod jumpserver;
pub mod localhost;
pub mod xhod;

use std::fmt;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::config::GatewayConfig;
use crate::daemon::connection_manager::ConnectionStatusSnapshot;
use crate::daemon::session::TargetSession;
use crate::protocol::ServerListRow;

use self::auth::AuthPrompter;
use self::direct::DirectGateway;
use self::jumpserver::JumpserverGateway;
use self::xhod::XhodGateway;

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

bitflags! {
    /// The set of high-level operations a gateway supports. Callers check the
    /// relevant flag before invoking an operation, so a backend can implement a
    /// subset of functionality and the rest reports a clear `Unsupported` error.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct Capabilities: u32 {
        /// `open_exec_session` — run a command (CLI `xho exec`, including `-it`).
        const EXEC = 1 << 0;
        /// `open_session` + sftp subsystem for `xho cp`.
        const COPY = 1 << 1;
        /// `open_session` as a transparent SSH proxy backend (`ssh node@xhod`).
        const PROXY = 1 << 2;
        /// `list_servers` — enumerate reachable servers.
        const LIST = 1 << 3;
    }
}

// ---------------------------------------------------------------------------
// Gateway trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Gateway: Send + Sync {
    /// The configured name of this gateway (e.g., "local", "remote-xhod").
    fn name(&self) -> &str;

    /// The concrete kind of this gateway.
    fn kind(&self) -> GatewayKind;

    /// The set of operations this gateway supports.
    fn capabilities(&self) -> Capabilities;

    /// Open a [`TargetSession`] to `target` for the CLI exec path, returning the
    /// session plus the command string to run on it. Command construction is
    /// kind-aware (each backend builds with the shell it resolves), which is why
    /// this is distinct from [`Gateway::open_session`].
    ///
    /// Requires [`Capabilities::EXEC`].
    async fn open_exec_session(
        &self,
        target: &str,
        argv: &[String],
        shell: &str,
        no_shell: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError>;

    /// Open a bare [`TargetSession`] to `target`. Used by the transparent proxy,
    /// the multi-hop tunnel, and `xho cp` (which then drives the sftp subsystem).
    ///
    /// Requires [`Capabilities::PROXY`] or [`Capabilities::COPY`] depending on
    /// the caller.
    async fn open_session(&self, target: &str) -> Result<Box<dyn TargetSession>, GatewayError>;

    /// Run a file copy operation. Default: open a session, start its sftp
    /// subsystem, and upload/download via SFTP. Jumpserver overrides this
    /// with shell-based copy (base64/tar over PTY) to avoid the sftp-server
    /// dependency and preserve the navigated shell for session cache reuse.
    ///
    /// Requires [`Capabilities::COPY`].
    async fn copy(
        &self,
        target: &str,
        spec: crate::types::CopySpec,
    ) -> Result<(), GatewayError> {
        let sess = self.open_session(target).await?;
        let sftp = crate::daemon::session::sftp_copy::open_sftp(sess)
            .await
            .map_err(|e| GatewayError::execution(anyhow!("{}", e)))?;
        crate::daemon::session::sftp_copy::run(&sftp, spec)
            .await
            .map_err(|e| GatewayError::execution(anyhow!("{}", e)))
    }

    /// List servers reachable through this gateway. Default: `Unsupported`.
    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        Err(GatewayError::unsupported(anyhow!(
            "gateway '{}' does not support list_servers",
            self.name()
        )))
    }

    /// Snapshot of this gateway's connection pool, for `xho status`.
    /// Default: no pools.
    async fn pool_status(&self) -> Vec<ConnectionStatusSnapshot> {
        Vec::new()
    }

    /// Prune idle connections. Called by the daemon's reaper timer. Default: no-op.
    async fn prune_idle(&self) {}
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Identifies the concrete type of a Gateway.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayKind {
    Xhod,
    Jumpserver,
    Direct,
    ReverseProxy,
    Localhost,
}

impl std::fmt::Display for GatewayKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayKind::Direct => write!(f, "direct"),
            GatewayKind::Jumpserver => write!(f, "jumpserver"),
            GatewayKind::Xhod => write!(f, "xhod"),
            GatewayKind::ReverseProxy => write!(f, "reverse_proxy"),
            GatewayKind::Localhost => write!(f, "localhost"),
        }
    }
}

/// A resolved route from the Resolver: which gateway to use and what
/// end target to pass to it.
#[derive(Clone, Debug)]
pub struct Route {
    pub gateway_name: String,
    pub end_target: String,
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Structured error type for Gateway operations.
/// The `kind` field drives retry and fallback logic.
#[derive(Debug)]
pub struct GatewayError {
    pub kind: ErrorKind,
    pub source: anyhow::Error,
}

/// Classification of Gateway errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Target cannot be found or resolved by this Gateway.
    Resolution,
    /// Network connection failed (SSH disconnect, gRPC stream broken, timeout).
    Transport,
    /// Remote command execution failed.
    Execution,
    /// Gateway does not support the requested operation.
    Unsupported,
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.kind, self.source)
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Resolution => write!(f, "resolution"),
            ErrorKind::Transport => write!(f, "transport"),
            ErrorKind::Execution => write!(f, "execution"),
            ErrorKind::Unsupported => write!(f, "unsupported"),
        }
    }
}

impl std::error::Error for GatewayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl GatewayError {
    /// Create a Resolution error from any error source.
    pub fn resolution(source: impl Into<anyhow::Error>) -> Self {
        Self {
            kind: ErrorKind::Resolution,
            source: source.into(),
        }
    }

    /// Create a Transport error from any error source.
    pub fn transport(source: impl Into<anyhow::Error>) -> Self {
        Self {
            kind: ErrorKind::Transport,
            source: source.into(),
        }
    }

    /// Create an Execution error from any error source.
    pub fn execution(source: impl Into<anyhow::Error>) -> Self {
        Self {
            kind: ErrorKind::Execution,
            source: source.into(),
        }
    }

    /// Create an Unsupported error from any error source.
    pub fn unsupported(source: impl Into<anyhow::Error>) -> Self {
        Self {
            kind: ErrorKind::Unsupported,
            source: source.into(),
        }
    }

    /// Format the error for CLI users.
    pub fn user_message(&self) -> String {
        let message = self.to_string();
        if self.kind == ErrorKind::Transport {
            format!("{message}; please retry the operation to open a fresh connection")
        } else {
            message
        }
    }
}

/// Classify an anyhow::Error as transport or not, by inspecting the error chain.
pub fn is_transport_error(error: &anyhow::Error) -> bool {
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::Internal
        )
    } else if error.downcast_ref::<russh::Error>().is_some() {
        true
    } else {
        let msg = error.to_string().to_ascii_lowercase();
        msg.contains("channel closed")
            || msg.contains("closed unexpectedly")
            || msg.contains("broken pipe")
            || msg.contains("connection reset")
            || msg.contains("send error")
    }
}

/// Classify an anyhow::Error as a resolution error.
pub fn is_resolution_error(error: &anyhow::Error) -> bool {
    let msg = error.to_string().to_ascii_lowercase();
    msg.contains("not found") || msg.contains("no match") || msg.contains("unknown target")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_user_message_includes_retry_hint() {
        let error = GatewayError::transport(anyhow::anyhow!("channel closed"));
        assert_eq!(
            error.user_message(),
            "[transport] channel closed; please retry the operation to open a fresh connection"
        );
    }

    #[test]
    fn non_transport_user_message_is_unchanged() {
        let error = GatewayError::execution(anyhow::anyhow!("command failed"));
        assert_eq!(error.user_message(), "[execution] command failed");
    }
}

// ---------------------------------------------------------------------------
// Gateway factory
// ---------------------------------------------------------------------------

/// Construct all Gateways from the loaded configuration.
/// Always creates one DirectGateway named "local".
/// Creates one XhodGateway, JumpserverGateway, or DirectGateway per
/// `[[gateways]]` entry. No network connections are established here.
pub fn build_gateways(
    config: Arc<RwLock<crate::config::AppConfig>>,
    server_config_path: &str,
    gateways_config: &[GatewayConfig],
    auth_prompter: Arc<AuthPrompter>,
) -> Vec<(String, Arc<dyn Gateway>)> {
    let mut gateways: Vec<(String, Arc<dyn Gateway>)> = Vec::new();

    let (max_connections_per_address, max_idle_time) = match config.try_read() {
        Ok(cfg) => (cfg.ssh.max_connections_per_ip, cfg.ssh.max_idle_time),
        Err(_) => {
            let defaults = crate::config::SshConfig::default();
            (defaults.max_connections_per_ip, defaults.max_idle_time)
        }
    };

    gateways.push((
        "local".to_string(),
        Arc::new(DirectGateway::new(
            "local".to_string(),
            config.clone(),
            server_config_path.to_string(),
            auth_prompter.clone(),
            max_connections_per_address,
            max_idle_time,
        )),
    ));

    for gc in gateways_config {
        let gateway: Arc<dyn Gateway> = match gc {
            GatewayConfig::Xhod(c) => Arc::new(XhodGateway::new(
                c.name.clone(),
                c.address.clone(),
                c.identity_file.clone(),
                c.known_hosts_path.clone(),
                auth_prompter.clone(),
                max_idle_time,
            )),
            GatewayConfig::Jumpserver(c) => Arc::new(JumpserverGateway::new(
                c.name.clone(),
                config.clone(),
                c.clone(),
                auth_prompter.clone(),
                max_idle_time,
            )),
            GatewayConfig::Direct(c) => Arc::new(DirectGateway::new(
                c.name.clone(),
                config.clone(),
                server_config_path.to_string(),
                auth_prompter.clone(),
                max_connections_per_address,
                max_idle_time,
            )),
        };
        gateways.push((gc.name().to_string(), gateway));
    }

    gateways
}
