// Unified session abstraction.
//
// `TargetSession` is THE single low-level abstraction every operation goes
// through — CLI `xho exec`/`xho cp`, the transparent SSH proxy
// (`ssh node@xhod`), and the multi-hop `OpenSession` tunnel all drive a
// `TargetSession`. It models SSH "session channel" semantics so exec, shell,
// pty, the sftp subsystem, and raw data streaming are all expressible through
// one contract.
//
// There is one implementation per *transport* (not per *feature*):
//   - `DirectSshSession`  — raw russh client channel to a direct SSH target.
//   - `LocalSession`      — local process on a PTY (+ in-process sftp server).
//   - `TunneledSession`   — drives an `OpenSession` RPC over the control plane.
//
// Third-party gateways (e.g. jumpserver) implement the trait but return
// `Unsupported` errors for methods they cannot realize.

pub mod direct;
pub mod jumpserver;
pub mod local;
pub mod sftp_copy;
pub mod shell_copy;
pub mod tunnel;

use anyhow::Result;
use async_trait::async_trait;
use russh::Pty;

/// An event produced by a backend session, polled via [`TargetSession::next_event`].
#[derive(Debug)]
pub enum SessionEvent {
    /// Bytes written to stdout by the remote program.
    Stdout(Vec<u8>),
    /// Bytes written to stderr by the remote program.
    Stderr(Vec<u8>),
    /// The remote program exited with this status code.
    ExitStatus(i32),
    /// The remote program was terminated by a signal (named).
    ExitSignal(String),
    /// The peer signaled end-of-file on the channel.
    Eof,
}

/// The unified session-channel contract.
///
/// All methods are fallible; transport implementations that cannot realize a
/// method (e.g. jumpserver `exec`) return an error classified via
/// [`crate::daemon::session::unsupported`].
#[async_trait]
pub trait TargetSession: Send {
    /// Request a pseudo-terminal before exec/shell. `modes` are SSH terminal
    /// modes (opcode, value). Implementations that do not use PTY modes may
    /// ignore them.
    async fn request_pty(
        &mut self,
        term: &str,
        cols: u32,
        rows: u32,
        modes: &[(Pty, u32)],
    ) -> Result<()>;

    /// Set an environment variable on the upcoming process.
    async fn set_env(&mut self, key: &str, value: &str) -> Result<()>;

    /// Execute a command (passed to a remote shell).
    async fn exec(&mut self, command: &str) -> Result<()>;

    /// Request an interactive login shell.
    async fn shell(&mut self) -> Result<()>;

    /// Request a subsystem by name (e.g. `"sftp"`).
    async fn subsystem(&mut self, name: &str) -> Result<()>;

    /// Notify the peer of a terminal window-size change.
    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()>;

    /// Send a signal (by name, e.g. `"INT"`) to the remote process.
    async fn signal(&mut self, signal: &str) -> Result<()>;

    /// Forward stdin bytes to the remote process.
    async fn write_stdin(&mut self, data: &[u8]) -> Result<()>;

    /// Signal end-of-file on the stdin side.
    async fn eof(&mut self) -> Result<()>;

    /// Poll the next event from the session, or `None` when the session has
    /// ended (after the terminal `ExitStatus`/`ExitSignal`/`Eof` is returned,
    /// subsequent calls return `None`).
    async fn next_event(&mut self) -> Option<SessionEvent>;
}

/// Build an "unsupported" error for a transport that cannot realize an
/// operation. Callers classify transport-level failures themselves; this is
/// the canonical "this transport does not support X" error.
pub fn unsupported(what: &str) -> anyhow::Error {
    anyhow::anyhow!("unsupported operation for this transport: {what}")
}

use anyhow::anyhow;

use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::protocol::ServerEvent;
use crate::types::CopySpec;

use super::DaemonState;
use super::gateway::{Capabilities, Route};

/// Handle for driving an interactive session: stdin/resize in, stdout out, plus
/// an exit-code oneshot and abort handles for the bridging tasks.
pub struct InteractiveHandle {
    pub stdin_tx: mpsc::Sender<Vec<u8>>,
    pub resize_tx: mpsc::Sender<(u32, u32)>,
    pub stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub exit_rx: oneshot::Receiver<i32>,
    pub abort_handles: Vec<AbortHandle>,
}

/// Resolve a route's gateway and confirm it advertises `needed`. Every consumer
/// gates generically on the capability flag — there is no per-kind branching.
async fn gateway_with_capability(
    state: &DaemonState,
    route: &Route,
    needed: Capabilities,
) -> Result<std::sync::Arc<dyn super::gateway::Gateway>> {
    let gateway = state
        .find_gateway_any(&route.gateway_name)
        .await
        .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;
    if !gateway.capabilities().contains(needed) {
        return Err(unsupported(&format!(
            "gateway '{}' ({}) does not support this operation",
            gateway.name(),
            gateway.kind()
        )));
    }
    Ok(gateway)
}

/// Run a copy (`xho cp`) over the gateway. Each gateway decides its own copy
/// strategy: the default uses the sftp subsystem; jumpserver overrides with
/// shell-based copy (base64/tar over the navigated PTY). Gateways that do not
/// advertise [`Capabilities::COPY`] return a clear `unsupported` error.
pub async fn copy_via_session(state: &DaemonState, route: &Route, spec: CopySpec) -> Result<()> {
    let gateway = gateway_with_capability(state, route, Capabilities::COPY).await?;
    gateway
        .copy(&route.end_target, spec)
        .await
        .map_err(|e| anyhow!("{}", e.user_message()))
}

/// Open a bare [`TargetSession`] to a target. This is the single entry point the
/// transparent proxy and the `OpenSession` tunnel use; dispatch lives entirely
/// inside the gateway's `open_session`. Requires [`Capabilities::PROXY`].
pub async fn open_target_session(
    state: &DaemonState,
    route: &Route,
) -> Result<Box<dyn TargetSession>> {
    let gateway = gateway_with_capability(state, route, Capabilities::PROXY).await?;
    gateway
        .open_session(&route.end_target)
        .await
        .map_err(|e| anyhow!("{}", e.user_message()))
}

/// Open a `TargetSession` for the CLI exec path plus the command string to run.
/// Command construction is kind-aware inside each gateway's `open_exec_session`.
/// Requires [`Capabilities::EXEC`].
pub async fn open_exec_session(
    state: &DaemonState,
    route: &Route,
    argv: &[String],
    cli_shell: &str,
    no_shell: bool,
) -> Result<(Box<dyn TargetSession>, String)> {
    let gateway = gateway_with_capability(state, route, Capabilities::EXEC).await?;
    gateway
        .open_exec_session(&route.end_target, argv, cli_shell, no_shell)
        .await
        .map_err(|e| anyhow!("{}", e.user_message()))
}

/// Drive a `TargetSession` for a non-interactive exec: optional PTY, exec,
/// then pump events to `sender` and forward stdin until exit. Returns the exit
/// code. Reused by the Execute RPC handler (replacing the old gateway.exec).
pub async fn drive_exec(
    mut sess: Box<dyn TargetSession>,
    command: String,
    tty: bool,
    cols: u32,
    rows: u32,
    sender: tokio::sync::mpsc::UnboundedSender<ServerEvent>,
    mut stdin_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
) -> Result<i32> {
    if tty && cols > 0 && rows > 0 {
        let _ = sess.request_pty("xterm-256color", cols, rows, &[]).await;
    }
    sess.exec(&command).await?;
    let mut stdin_done = stdin_rx.is_none();
    loop {
        tokio::select! {
            ev = sess.next_event() => match ev {
                Some(SessionEvent::Stdout(d)) => {
                    let _ = sender.send(ServerEvent::Stdout { data: d });
                }
                Some(SessionEvent::Stderr(d)) => {
                    let _ = sender.send(ServerEvent::Stderr { data: d });
                }
                Some(SessionEvent::ExitStatus(c)) => return Ok(c),
                Some(SessionEvent::ExitSignal(s)) => {
                    let _ = sender.send(ServerEvent::Stderr {
                        data: format!("killed by signal {s}\n").into_bytes(),
                    });
                    return Ok(255);
                }
                Some(SessionEvent::Eof) | None => return Ok(0),
            },
            stdin = async {
                match &mut stdin_rx {
                    Some(r) if !stdin_done => r.recv().await,
                    _ => std::future::pending::<Option<Vec<u8>>>().await,
                }
            } => match stdin {
                Some(d) => {
                    let _ = sess.write_stdin(&d).await;
                }
                None => {
                    let _ = sess.eof().await;
                    stdin_done = true;
                }
            },
        }
    }
}

/// Drive a `TargetSession` for an interactive (`xho exec -it`) session: request
/// a PTY, start the command (or a login shell when `exec_command` is `None`),
/// then bridge stdin/stdout/resize/exit into a [`InteractiveHandle`] that the
/// Execute RPC handler drives exactly as it did for the legacy gateway path.
pub async fn drive_interactive(
    mut sess: Box<dyn TargetSession>,
    exec_command: Option<String>,
    cols: u32,
    rows: u32,
) -> Result<InteractiveHandle> {
    use tokio::sync::{mpsc, oneshot};

    sess.request_pty("xterm-256color", cols, rows, &[]).await?;
    match exec_command {
        Some(cmd) => sess.exec(&cmd).await?,
        None => sess.shell().await?,
    }

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(32);
    let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(8);
    let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (exit_tx, exit_rx) = oneshot::channel::<i32>();

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                ev = sess.next_event() => match ev {
                    Some(SessionEvent::Stdout(d)) => {
                        if stdout_tx.send(d).is_err() { break; }
                    }
                    Some(SessionEvent::Stderr(d)) => {
                        let _ = stdout_tx.send(d);
                    }
                    Some(SessionEvent::ExitStatus(c)) => { let _ = exit_tx.send(c); return; }
                    Some(SessionEvent::ExitSignal(_)) => { let _ = exit_tx.send(255); return; }
                    Some(SessionEvent::Eof) | None => { let _ = exit_tx.send(0); return; }
                },
                stdin = stdin_rx.recv() => match stdin {
                    Some(d) => { let _ = sess.write_stdin(&d).await; }
                    None => { let _ = sess.eof().await; }
                },
                resize = resize_rx.recv() => {
                    if let Some((c, r)) = resize { let _ = sess.window_change(c, r).await; }
                }
            }
        }
    });

    Ok(InteractiveHandle {
        stdin_tx,
        resize_tx,
        stdout_rx,
        exit_rx,
        abort_handles: vec![task.abort_handle()],
    })
}
