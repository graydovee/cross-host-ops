// Connection trait module (daemon-internal).
// Defines the Connection trait used internally by Gateway implementations.

pub mod direct;
pub mod jumpserver;
pub mod rhopd;
pub mod shared;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::connection::CopySpec;
use crate::protocol::ServerEvent;

// ---------------------------------------------------------------------------
// Placeholder types for the Connection trait.
// These will be replaced by proper types defined in daemon::gateway (task 4.2)
// once that module is implemented. For now they allow the trait to compile.
// ---------------------------------------------------------------------------

/// Request payload for exec operations (Connection-level).
#[derive(Clone, Debug)]
pub(super) struct ExecRequest {
    pub argv: Vec<String>,
    pub sender: mpsc::UnboundedSender<ServerEvent>,
    pub pty: bool,
    pub cols: u32,
    pub rows: u32,
    pub shell: String,
}

/// Request payload for interactive PTY sessions (Connection-level).
#[derive(Clone, Debug)]
pub(super) struct InteractiveRequest {
    pub argv: Vec<String>,
    pub cols: u32,
    pub rows: u32,
    pub sender: mpsc::UnboundedSender<ServerEvent>,
    pub shell: String,
}

/// Handle for driving an interactive session (Connection-level).
pub(super) struct InteractiveHandle {
    pub stdin_tx: mpsc::Sender<Vec<u8>>,
    pub resize_tx: mpsc::Sender<(u32, u32)>,
    pub stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub exit_rx: oneshot::Receiver<i32>,
}

// ---------------------------------------------------------------------------
// Connection trait — internal to the daemon module.
// ---------------------------------------------------------------------------

/// Internal trait for a connection to an end target.
/// NOT publicly exported — used only by Gateway implementations.
#[async_trait]
pub(super) trait Connection: Send {
    /// Execute a command on the connected end target.
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32>;

    /// Copy files to/from the connected end target.
    async fn copy(&mut self, spec: &CopySpec) -> Result<()>;

    /// Open an interactive PTY session.
    async fn exec_interactive(&mut self, request: &InteractiveRequest) -> Result<InteractiveHandle>;

    /// Check whether the connection is still alive.
    fn is_alive(&self) -> bool;
}
