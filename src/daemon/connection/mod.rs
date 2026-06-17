// Connection trait module (daemon-internal).
// Defines the Connection trait used internally by Gateway implementations.

pub mod direct;
pub mod jumpserver;
pub mod shared;
pub mod xhod;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::protocol::ServerEvent;
use crate::types::{CopySpec, FlagIntent};

// ---------------------------------------------------------------------------
// Placeholder types for the Connection trait.
// These will be replaced by proper types defined in daemon::gateway (task 4.2)
// once that module is implemented. For now they allow the trait to compile.
// ---------------------------------------------------------------------------

/// Request payload for exec operations (Connection-level).
///
/// NOTE: `stdin_rx` is not Clone because `mpsc::Receiver` is not Clone.
/// The `Connection` trait takes `&mut self`, so the connection can take
/// ownership of `stdin_rx` by calling `Option::take()`.
#[derive(Debug)]
pub(super) struct ExecRequest {
    pub argv: Vec<String>,
    pub sender: mpsc::UnboundedSender<ServerEvent>,
    pub tty: bool,
    pub cols: u32,
    pub rows: u32,
    pub shell: String,
    pub no_shell: bool,
    pub timeout_ms: u64,
    pub stdin: bool,
    pub stdin_intent: FlagIntent,
    /// Optional stdin receiver. When Some, the connection implementation SHALL
    /// read from this channel and forward bytes to the remote process.
    /// When None, behavior is identical to the pre-fix implementation.
    pub stdin_rx: Option<mpsc::Receiver<Vec<u8>>>,
}

/// Request payload for interactive PTY sessions (Connection-level).
#[derive(Clone, Debug)]
pub(super) struct InteractiveRequest {
    pub argv: Vec<String>,
    pub cols: u32,
    pub rows: u32,
    pub sender: mpsc::UnboundedSender<ServerEvent>,
    pub shell: String,
    pub no_shell: bool,
}

/// Handle for driving an interactive session (Connection-level).
pub(super) struct InteractiveHandle {
    pub stdin_tx: mpsc::Sender<Vec<u8>>,
    pub resize_tx: mpsc::Sender<(u32, u32)>,
    pub stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub exit_rx: oneshot::Receiver<i32>,
    pub abort_handles: Vec<AbortHandle>,
}

// ---------------------------------------------------------------------------
// Connection trait — internal to the daemon module.
// ---------------------------------------------------------------------------

/// Internal trait for a connection to an end target.
/// NOT publicly exported — used only by Gateway implementations.
#[async_trait]
pub(super) trait Connection: Send {
    /// Execute a command on the connected end target.
    ///
    /// Takes `&mut ExecRequest` so that implementations can call
    /// `request.stdin_rx.take()` to move the receiver out for forwarding.
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32>;

    /// Copy files to/from the connected end target.
    ///
    /// Takes owned `CopySpec` so implementations can consume upload/download
    /// frame channels without borrowing gymnastics.
    async fn copy(&mut self, spec: CopySpec) -> Result<()>;

    /// Open an interactive PTY session.
    async fn exec_interactive(&mut self, request: &InteractiveRequest)
    -> Result<InteractiveHandle>;

    /// Check whether the connection is still alive.
    fn is_alive(&self) -> bool;
}
