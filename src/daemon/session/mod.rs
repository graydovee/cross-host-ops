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

pub mod b64;
pub mod direct;
pub mod jumpserver;
pub mod local;
pub mod sftp_copy;
pub mod shell_copy;
pub mod tunnel;

use anyhow::Result;
use async_trait::async_trait;
use russh::Pty;
use tokio::sync::{mpsc, oneshot};

/// An event produced by a backend session, polled via [`SessionStream::next`].
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

/// A request sent to a session's write half. "Start" operations (pty, env,
/// exec, shell, subsystem) carry a oneshot reply so callers can observe
/// transport failures; data/resize/signal/eof are fire-and-forget.
pub(crate) enum SessionCommand {
    Pty {
        term: String,
        cols: u32,
        rows: u32,
        modes: Vec<(Pty, u32)>,
        reply: oneshot::Sender<Result<()>>,
    },
    Env {
        key: String,
        value: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Exec {
        command: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Shell {
        reply: oneshot::Sender<Result<()>>,
    },
    Subsystem {
        name: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Resize {
        cols: u32,
        rows: u32,
    },
    Signal {
        signal: String,
    },
    Data {
        bytes: Vec<u8>,
    },
    Eof,
}

/// The write half of a session: sends requests (start, stdin, resize, signal)
/// without owning the event stream, so the two halves can live in separate
/// tasks. Backpressure applies only to this half.
pub struct SessionWriter {
    pub(crate) tx: mpsc::Sender<SessionCommand>,
}

/// Cloning is safe and preserves FIFO ordering: all clones share one
/// command channel, so concurrent holders (e.g. separate stdin/resize tasks)
/// cannot reorder relative to a previously established sequence.
impl Clone for SessionWriter {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl SessionWriter {
    fn check(send: Result<(), mpsc::error::SendError<SessionCommand>>) -> Result<()> {
        send.map_err(|_| anyhow!("session closed"))
    }

    async fn request(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<()>>) -> SessionCommand,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        Self::check(self.tx.send(build(reply_tx)).await)?;
        reply_rx
            .await
            .unwrap_or_else(|_| Err(anyhow!("session closed")))
    }

    pub(crate) async fn request_pty(
        &self,
        term: &str,
        cols: u32,
        rows: u32,
        modes: &[(Pty, u32)],
    ) -> Result<()> {
        self.request(|reply| SessionCommand::Pty {
            term: term.to_string(),
            cols,
            rows,
            modes: modes.to_vec(),
            reply,
        })
        .await
    }

    pub(crate) async fn set_env(&self, key: &str, value: &str) -> Result<()> {
        self.request(|reply| SessionCommand::Env {
            key: key.to_string(),
            value: value.to_string(),
            reply,
        })
        .await
    }

    pub async fn exec(&self, command: &str) -> Result<()> {
        self.request(|reply| SessionCommand::Exec {
            command: command.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn shell(&self) -> Result<()> {
        self.request(|reply| SessionCommand::Shell { reply }).await
    }

    pub(crate) async fn subsystem(&self, name: &str) -> Result<()> {
        self.request(|reply| SessionCommand::Subsystem {
            name: name.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn window_change(&self, cols: u32, rows: u32) -> Result<()> {
        Self::check(self.tx.send(SessionCommand::Resize { cols, rows }).await)
    }

    pub(crate) async fn signal(&self, signal: &str) -> Result<()> {
        Self::check(
            self.tx
                .send(SessionCommand::Signal {
                    signal: signal.to_string(),
                })
                .await,
        )
    }

    pub async fn write_stdin(&self, data: &[u8]) -> Result<()> {
        Self::check(
            self.tx
                .send(SessionCommand::Data {
                    bytes: data.to_vec(),
                })
                .await,
        )
    }

    pub async fn eof(&self) -> Result<()> {
        Self::check(self.tx.send(SessionCommand::Eof).await)
    }
}

/// The read half of a session: yields events until the session ends.
pub struct SessionStream {
    pub(crate) rx: mpsc::UnboundedReceiver<SessionEvent>,
}

impl SessionStream {
    pub async fn next(&mut self) -> Option<SessionEvent> {
        self.rx.recv().await
    }
}

/// The unified session-channel contract.
///
/// The primary interface is [`TargetSession::split`], which separates the
/// request (write) half from the event (read) half so streaming callers can
/// run each direction in its own task — a send that parks on flow control in
/// one direction never freezes the other. The method family below is the
/// legacy single-object interface kept for stateful implementations (see
/// [`AdapterSession`]); every method defaults to `unsupported`.
#[async_trait]
pub trait TargetSession: Send {
    /// Consume the session and split it into independent halves.
    fn split(self: Box<Self>) -> (SessionWriter, SessionStream);

    /// Request a pseudo-terminal before exec/shell. `modes` are SSH terminal
    /// modes (opcode, value). Implementations that do not use PTY modes may
    /// ignore them.
    async fn request_pty(
        &mut self,
        _term: &str,
        _cols: u32,
        _rows: u32,
        _modes: &[(Pty, u32)],
    ) -> Result<()> {
        Err(unsupported("request_pty"))
    }

    /// Set an environment variable on the upcoming process.
    async fn set_env(&mut self, _key: &str, _value: &str) -> Result<()> {
        Err(unsupported("set_env"))
    }

    /// Execute a command (passed to a remote shell).
    async fn exec(&mut self, _command: &str) -> Result<()> {
        Err(unsupported("exec"))
    }

    /// Request an interactive login shell.
    async fn shell(&mut self) -> Result<()> {
        Err(unsupported("shell"))
    }

    /// Request a subsystem by name (e.g. `"sftp"`).
    async fn subsystem(&mut self, _name: &str) -> Result<()> {
        Err(unsupported("subsystem"))
    }

    /// Notify the peer of a terminal window-size change.
    async fn window_change(&mut self, _cols: u32, _rows: u32) -> Result<()> {
        Err(unsupported("window_change"))
    }

    /// Send a signal (by name, e.g. `"INT"`) to the remote process.
    async fn signal(&mut self, _signal: &str) -> Result<()> {
        Err(unsupported("signal"))
    }

    /// Forward stdin bytes to the remote process.
    async fn write_stdin(&mut self, _data: &[u8]) -> Result<()> {
        Err(unsupported("write_stdin"))
    }

    /// Signal end-of-file on the stdin side.
    async fn eof(&mut self) -> Result<()> {
        Err(unsupported("eof"))
    }

    /// Poll the next event from the session, or `None` when the session has
    /// ended.
    async fn next_event(&mut self) -> Option<SessionEvent> {
        None
    }
}

/// Adapt a stateful method-family session (e.g. the jumpserver state machine,
/// which is not channel-driven) to the split interface. A mediation task owns
/// the inner session and translates commands into method calls.
pub(crate) fn adapt_split(inner: Box<dyn TargetSession>) -> (SessionWriter, SessionStream) {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SessionCommand>(64);
    let (ev_tx, ev_rx) = mpsc::unbounded_channel::<SessionEvent>();
    tokio::spawn(async move {
        let mut sess = inner;
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => match cmd {
                    Some(c) => apply_command(&mut *sess, c).await,
                    None => break,
                },
                ev = sess.next_event() => match ev {
                    Some(e) => {
                        if ev_tx.send(e).is_err() { break; }
                    }
                    None => break,
                },
            }
        }
    });
    (SessionWriter { tx: cmd_tx }, SessionStream { rx: ev_rx })
}

/// Translate a unified command into method calls on a method-family session.
async fn apply_command(sess: &mut dyn TargetSession, cmd: SessionCommand) {
    match cmd {
        SessionCommand::Pty {
            term,
            cols,
            rows,
            modes,
            reply,
        } => {
            let _ = reply.send(sess.request_pty(&term, cols, rows, &modes).await);
        }
        SessionCommand::Env { key, value, reply } => {
            let _ = reply.send(sess.set_env(&key, &value).await);
        }
        SessionCommand::Exec { command, reply } => {
            let _ = reply.send(sess.exec(&command).await);
        }
        SessionCommand::Shell { reply } => {
            let _ = reply.send(sess.shell().await);
        }
        SessionCommand::Subsystem { name, reply } => {
            let _ = reply.send(sess.subsystem(&name).await);
        }
        SessionCommand::Resize { cols, rows } => {
            let _ = sess.window_change(cols, rows).await;
        }
        SessionCommand::Signal { signal } => {
            let _ = sess.signal(&signal).await;
        }
        SessionCommand::Data { bytes } => {
            let _ = sess.write_stdin(&bytes).await;
        }
        SessionCommand::Eof => {
            let _ = sess.eof().await;
        }
    }
}

/// Build an "unsupported" error for a transport that cannot realize an
/// operation. Callers classify transport-level failures themselves; this is
/// the canonical "this transport does not support X" error.
pub fn unsupported(what: &str) -> anyhow::Error {
    anyhow::anyhow!("unsupported operation for this transport: {what}")
}

use anyhow::anyhow;

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
/// shell-based copy (base64 over the navigated PTY). Gateways that do not
/// advertise [`Capabilities::COPY`] return a clear `unsupported` error.
pub async fn copy_via_session(state: &DaemonState, route: &Route, spec: CopySpec) -> Result<()> {
    let gateway = gateway_with_capability(state, route, Capabilities::COPY).await?;
    gateway
        .copy(&route.end_target, spec)
        .await
        .map_err(|e| anyhow!("{}", e.user_message()))
}

/// Validate CLI resume hints against remote partial-upload state; returns
/// entries with effective offsets (0 = fresh). Used by the daemon's copy
/// handler to build the `resume_ack` the CLI waits for before streaming
/// upload frames. Requires [`Capabilities::COPY`].
pub async fn probe_upload_resume(
    state: &DaemonState,
    route: &Route,
    spec: &mut CopySpec,
) -> Result<Vec<crate::types::ResumeEntry>> {
    let gateway = gateway_with_capability(state, route, Capabilities::COPY).await?;
    gateway
        .probe_upload_resume(&route.end_target, spec)
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
    sess: Box<dyn TargetSession>,
    command: String,
    tty: bool,
    cols: u32,
    rows: u32,
    sender: tokio::sync::mpsc::UnboundedSender<ServerEvent>,
    mut stdin_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
) -> Result<i32> {
    let (writer, mut stream) = sess.split();
    if tty && cols > 0 && rows > 0 {
        let _ = writer.request_pty("xterm-256color", cols, rows, &[]).await;
    }
    writer.exec(&command).await?;
    // Stdin forwarding runs as its own task: a parked `write_stdin` (remote
    // stopped reading stdin) must not stop event consumption below, or the
    // remote can never drain its output and resume.
    if let Some(mut stdin_rx) = stdin_rx.take() {
        tokio::spawn(async move {
            while let Some(d) = stdin_rx.recv().await {
                if writer.write_stdin(&d).await.is_err() {
                    return;
                }
            }
            let _ = writer.eof().await;
        });
    } else {
        // No stdin channel — signal EOF immediately so the session's spawned
        // exec task knows not to wait for stdin data.
        let _ = writer.eof().await;
    }
    while let Some(ev) = stream.next().await {
        match ev {
            SessionEvent::Stdout(d) => {
                let _ = sender.send(ServerEvent::Stdout { data: d });
            }
            SessionEvent::Stderr(d) => {
                let _ = sender.send(ServerEvent::Stderr { data: d });
            }
            SessionEvent::ExitStatus(c) => return Ok(c),
            SessionEvent::ExitSignal(s) => {
                let _ = sender.send(ServerEvent::Stderr {
                    data: format!("killed by signal {s}\n").into_bytes(),
                });
                return Ok(255);
            }
            SessionEvent::Eof => return Ok(0),
        }
    }
    Ok(0)
}

/// Drive a `TargetSession` for an interactive (`xho exec -it`) session: request
/// a PTY, start the command (or a login shell when `exec_command` is `None`),
/// then bridge stdin/stdout/resize/exit into a [`InteractiveHandle`] that the
/// Execute RPC handler drives exactly as it did for the legacy gateway path.
pub async fn drive_interactive(
    sess: Box<dyn TargetSession>,
    exec_command: Option<String>,
    cols: u32,
    rows: u32,
) -> Result<InteractiveHandle> {
    use tokio::sync::{mpsc, oneshot};

    let (writer, mut stream) = sess.split();
    writer
        .request_pty("xterm-256color", cols, rows, &[])
        .await?;
    match exec_command {
        Some(cmd) => writer.exec(&cmd).await?,
        None => writer.shell().await?,
    }

    let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(32);
    let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(8);
    let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (exit_tx, exit_rx) = oneshot::channel::<i32>();

    // Input forwarding (stdin + resize) runs as its own task so a parked
    // `write_stdin` cannot stall the output pump — the same directional
    // independence the transport drivers guarantee.
    let writer2 = writer.clone();
    let input_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                stdin = stdin_rx.recv() => match stdin {
                    Some(d) => { if writer.write_stdin(&d).await.is_err() { break; } }
                    None => { let _ = writer.eof().await; break; }
                },
                resize = resize_rx.recv() => {
                    if let Some((c, r)) = resize { let _ = writer.window_change(c, r).await; }
                }
            }
        }
    });
    let task = tokio::spawn(async move {
        let _ = writer2;
        while let Some(ev) = stream.next().await {
            match ev {
                SessionEvent::Stdout(d) => {
                    if stdout_tx.send(d).is_err() {
                        break;
                    }
                }
                SessionEvent::Stderr(d) => {
                    let _ = stdout_tx.send(d);
                }
                SessionEvent::ExitStatus(c) => {
                    let _ = exit_tx.send(c);
                    return;
                }
                SessionEvent::ExitSignal(_) => {
                    let _ = exit_tx.send(255);
                    return;
                }
                SessionEvent::Eof => {
                    let _ = exit_tx.send(0);
                    return;
                }
            }
        }
        let _ = exit_tx.send(0);
    });
    let _ = input_task;

    Ok(InteractiveHandle {
        stdin_tx,
        resize_tx,
        stdout_rx,
        exit_rx,
        abort_handles: vec![task.abort_handle()],
    })
}
