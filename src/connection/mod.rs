pub mod direct;
pub mod jump;
pub mod resolver;
mod shared;
pub mod types;

use anyhow::{Result, bail};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::UnboundedSender;

use crate::config::AppConfig;
use crate::protocol::ServerEvent;

pub use direct::DirectSshConnection;
pub use jump::JumpSshConnection;
pub use resolver::{Resolver, derive_target_ip};
pub use crate::config::JumpHostConfig;
pub use shared::{build_final_command, build_remote_command, resolve_shell, shell_quote, wrap_in_shell};
pub use types::{CopyDirection, CopySpec, DirectTarget};

pub type AuthFuture = Pin<Box<dyn Future<Output = Result<String>> + Send>>;
pub type AuthPrompter = dyn Fn(AuthPromptRequest) -> AuthFuture + Send + Sync;

#[derive(Clone, Debug)]
pub struct AuthPromptRequest {
    pub target_label: String,
    pub kind: String,
    pub message: String,
    pub secret: bool,
}

/// Handles for an interactive PTY session.
/// The caller is responsible for driving stdin/stdout forwarding.
pub struct InteractiveSession {
    /// Write stdin bytes to the remote process.
    pub stdin_tx: mpsc::Sender<Vec<u8>>,
    /// Receive stdout bytes from the remote process.
    pub stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Send window resize events.
    pub resize_tx: mpsc::Sender<(u32, u32)>,
    /// Await the exit code.
    pub exit_rx: oneshot::Receiver<i32>,
}

#[tonic::async_trait]
pub trait Connection: Send {
    async fn execute(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
        pty: bool,
        cols: u32,
        rows: u32,
        shell: &str,
    ) -> Result<i32>;

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()>;

    /// Open an interactive PTY session without sentinel wrapping.
    /// Returns handles for the caller to drive I/O.
    ///
    /// Default implementation returns an error for connection types
    /// that do not support interactive execution.
    async fn execute_interactive(
        &mut self,
        _argv: &[String],
        _cols: u32,
        _rows: u32,
        _config: &AppConfig,
        _shell: &str,
    ) -> Result<InteractiveSession> {
        bail!("interactive execution is not supported for this connection type")
    }
}
