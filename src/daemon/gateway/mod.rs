// Gateway trait and supporting types.
// Will contain the Gateway trait, GatewayKind, Route, GatewayError, and build_gateways factory.

pub mod auth;
pub mod direct;
pub mod jumpserver;
pub mod localhost;
pub mod xhod;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::config::GatewayConfig;
use crate::protocol::{ServerEvent, ServerListRow};
use crate::types::FlagIntent;

use self::auth::AuthPrompter;
use self::direct::DirectGateway;
use self::jumpserver::JumpserverGateway;
use self::xhod::XhodGateway;

// ---------------------------------------------------------------------------
// Gateway trait
// ---------------------------------------------------------------------------

/// The external interface for all jump host / connection abstractions.
/// Callers only see exec/copy/exec_interactive/list_servers.
/// Connection management (pooling, reconnection, auth) is fully internal.
#[async_trait]
pub trait Gateway: Send + Sync {
    /// Execute a command on the specified end target.
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError>;

    /// Open an interactive PTY session to the specified end target.
    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError>;

    /// List servers reachable through this gateway.
    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError>;

    /// A cloneable handle to this gateway's control-plane gRPC client, when it
    /// has one (Xhod / ReverseProxy). Used to open `OpenSession` tunnels so the
    /// transparent proxy / multi-hop can reach machines behind another xhod.
    /// Returns `None` for gateways without an RPC client (Direct/Localhost/
    /// Jumpserver).
    async fn rpc_client(
        &self,
    ) -> Option<crate::protocol::rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>> {
        None
    }

    /// The concrete kind of this gateway.
    fn kind(&self) -> GatewayKind;

    /// The configured name of this gateway (e.g., "local", "remote-xhod").
    fn name(&self) -> &str;

    /// Prune idle connections. Called by the daemon's reaper timer.
    async fn prune_idle(&self);
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

/// Request payload for exec operations.
#[derive(Debug)]
pub struct ExecRequest {
    pub argv: Vec<String>,
    pub sender: mpsc::UnboundedSender<ServerEvent>,
    pub tty: bool,
    pub tty_intent: FlagIntent,
    pub cols: u32,
    pub rows: u32,
    pub shell: String,
    pub no_shell: bool,
    pub timeout_ms: u64,
    pub stdin: bool,
    pub stdin_intent: FlagIntent,
    /// Optional stdin receiver for forwarding stdin data to the remote process.
    /// Created by process_execute when the client requests stdin forwarding.
    /// Wrapped in Mutex<Option<...>> so the gateway can take ownership from `&self`.
    pub stdin_rx: std::sync::Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
}

/// Request payload for interactive PTY sessions.
#[derive(Clone, Debug)]
pub struct InteractiveRequest {
    pub argv: Vec<String>,
    pub cols: u32,
    pub rows: u32,
    pub sender: mpsc::UnboundedSender<ServerEvent>,
    pub shell: String,
    pub no_shell: bool,
}

/// Handle for driving an interactive session.
pub struct InteractiveHandle {
    pub stdin_tx: mpsc::Sender<Vec<u8>>,
    pub resize_tx: mpsc::Sender<(u32, u32)>,
    pub stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub exit_rx: oneshot::Receiver<i32>,
    pub abort_handles: Vec<AbortHandle>,
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
    /// Daemon should try the next route candidate.
    Resolution,
    /// Network connection failed (SSH disconnect, gRPC stream broken, timeout).
    /// Gateway handles retry internally; if propagated, daemon treats as fatal for this route.
    Transport,
    /// Remote command execution failed (permission denied at remote shell, command not found).
    /// Non-zero exit code is NOT an Execution error — that's a successful exec with non-zero status.
    /// Daemon should return immediately without trying further candidates.
    Execution,
    /// Gateway does not support the requested operation (e.g., list_servers on Jumpserver).
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
    // Check for tonic::Status with transport-indicative codes
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::Internal
        )
    }
    // Check for russh::Error (any variant is transport-level)
    else if error.downcast_ref::<russh::Error>().is_some() {
        true
    }
    // String heuristic fallback
    else {
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
/// Creates one XhodGateway or JumpserverGateway per `[[gateways]]` entry.
/// No network connections are established during construction.
pub fn build_gateways(
    config: Arc<RwLock<crate::config::AppConfig>>,
    server_config_path: &str,
    gateways_config: &[GatewayConfig],
    auth_prompter: Arc<AuthPrompter>,
) -> Vec<(String, Arc<dyn Gateway>)> {
    let mut gateways: Vec<(String, Arc<dyn Gateway>)> = Vec::new();

    // Read max_connections_per_ip and max_idle_time from a blocking snapshot.
    // These are read at construction time and won't change until daemon restart.
    let (max_connections_per_address, max_idle_time) = {
        // Use try_read to avoid async context; fall back to defaults if locked.
        match config.try_read() {
            Ok(cfg) => (cfg.ssh.max_connections_per_ip, cfg.ssh.max_idle_time),
            Err(_) => {
                // Fallback to defaults if config is locked (should not happen at startup)
                let defaults = crate::config::SshConfig::default();
                (defaults.max_connections_per_ip, defaults.max_idle_time)
            }
        }
    };

    // Always create the "local" gateway first.
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

    // Create one gateway per gateways_config entry, preserving declaration order.
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
            GatewayConfig::Direct(c) => {
                // Direct gateways are treated as a DirectGateway with their own name.
                // They use the same server.toml resolution as "local" but route
                // through the named gateway for resolver distinction.
                Arc::new(DirectGateway::new(
                    c.name.clone(),
                    config.clone(),
                    server_config_path.to_string(),
                    auth_prompter.clone(),
                    max_connections_per_address,
                    max_idle_time,
                ))
            }
        };
        gateways.push((gc.name().to_string(), gateway));
    }

    gateways
}
