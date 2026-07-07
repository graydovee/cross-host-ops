// LocalSession — a `TargetSession` backed by a local process.
//
// shell/exec run on a pseudo-terminal (or pipes when no PTY was requested),
// reusing openpty/dup/resize/spawn primitives. The sftp subsystem is served by
// spawning the OS `sftp-server` binary and bridging its stdio — matching
// OpenSSH semantics and giving the transparent proxy and `xho cp` a uniform
// SFTP path over the same session contract.
//
// A dedicated waiter task owns each spawned `Child` (so the driver's `select!`
// never holds a borrow across `child.wait()`) and reports `ExitStatus`/`Eof`.

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use russh::Pty;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot};

use super::{SessionEvent, TargetSession};

// -----------------------------------------------------------------------
// PTY helpers (platform-specific)
// -----------------------------------------------------------------------

/// Opaque handle to a live PTY controller, used for resize/signaling.
///
/// On Unix this is the master fd; on Windows it will hold the ConPTY
/// `HPCON` + process handle once implemented (see `windows` backend).
#[cfg(unix)]
type PtyHandle = libc::c_int;

/// Windows placeholder. ConPTY support is added in a follow-up; until then
/// PTY-backed local sessions return an error and only pipe mode is available.
#[cfg(not(unix))]
type PtyHandle = ();

#[cfg(unix)]
fn openpty_pair() -> Result<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if rc != 0 {
        return Err(anyhow::anyhow!(
            "openpty: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) })
}

#[cfg(unix)]
fn dup_fd(fd: &OwnedFd) -> Result<OwnedFd> {
    let new = unsafe { libc::dup(fd.as_raw_fd()) };
    if new < 0 {
        return Err(anyhow::anyhow!("dup: {}", std::io::Error::last_os_error()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(new) })
}

#[cfg(unix)]
fn pty_resize(fd: libc::c_int, cols: u32, rows: u32) {
    let ws = libc::winsize {
        ws_row: rows as u16,
        ws_col: cols as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &ws) };
}

/// No-op window resize for the (not-yet-implemented) Windows PTY backend.
#[cfg(not(unix))]
fn pty_resize(_handle: PtyHandle, _cols: u32, _rows: u32) {}

/// Resolve the sftp-server binary: explicit config, common locations, PATH.
fn resolve_sftp_server(configured: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = configured {
        let expanded = crate::config::expand_tilde(p).unwrap_or_else(|_| p.to_string());
        return Some(PathBuf::from(expanded));
    }
    for candidate in sftp_server_candidates() {
        if candidate.exists() {
            return Some(candidate);
        }
    }
    let exe_name = sftp_server_exe_name();
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path).find_map(|dir| {
            let full = dir.join(exe_name);
            full.is_file().then_some(full)
        })
    })
}

/// Common `sftp-server` install locations, per platform.
fn sftp_server_candidates() -> Vec<PathBuf> {
    #[cfg(unix)]
    {
        vec![
            PathBuf::from("/usr/lib/openssh/sftp-server"),
            PathBuf::from("/usr/libexec/openssh/sftp-server"),
            PathBuf::from("/usr/libexec/sftp-server"),
            PathBuf::from("/usr/lib/ssh/sftp-server"),
        ]
    }
    #[cfg(not(unix))]
    {
        // Win32 OpenSSH (Microsoft's port) installs sftp-server here.
        vec![
            PathBuf::from(r"C:\Program Files\OpenSSH\sftp-server.exe"),
            PathBuf::from(r"C:\Windows\System32\OpenSSH\sftp-server.exe"),
        ]
    }
}

/// Executable name to search for on PATH.
#[cfg(unix)]
fn sftp_server_exe_name() -> &'static str {
    "sftp-server"
}

#[cfg(not(unix))]
fn sftp_server_exe_name() -> &'static str {
    "sftp-server.exe"
}

// -----------------------------------------------------------------------
// Control protocol
// -----------------------------------------------------------------------

enum Control {
    Pty {
        term: String,
        cols: u32,
        rows: u32,
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
    WindowChange {
        cols: u32,
        rows: u32,
    },
    Signal {
        signal: String,
    },
    Eof,
}

/// What the driver needs to drive a running backend.
struct Backend {
    /// Write side for stdin (PTY master or pipe stdin).
    write: WriteSide,
    /// PTY controller handle for window-resize (None for pipes).
    pty_fd: Option<PtyHandle>,
    /// Process id for signal delivery.
    pid: u32,
}

enum WriteSide {
    Pty(tokio::fs::File),
    Pipe(Option<ChildStdin>),
}

impl WriteSide {
    async fn write(&mut self, data: &[u8]) {
        match self {
            WriteSide::Pty(f) => {
                let _ = f.write_all(data).await;
                let _ = f.flush().await;
            }
            WriteSide::Pipe(Some(s)) => {
                if s.write_all(data).await.is_err() {
                    *self = WriteSide::Pipe(None);
                }
            }
            WriteSide::Pipe(None) => {}
        }
    }

    async fn eof(&mut self) {
        match self {
            WriteSide::Pty(f) => {
                let _ = f.write_all(b"\x04").await;
            }
            WriteSide::Pipe(s) => {
                s.take();
            }
        }
    }
}

pub(crate) struct LocalSession {
    control_tx: mpsc::Sender<Control>,
    stdin_tx: mpsc::Sender<Vec<u8>>,
    events_rx: mpsc::UnboundedReceiver<SessionEvent>,
}

impl LocalSession {
    pub(crate) fn new(shell: String, sftp_server_path: Option<String>) -> Self {
        let (control_tx, control_rx) = mpsc::channel::<Control>(32);
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        tokio::spawn(driver(
            shell,
            sftp_server_path,
            control_rx,
            stdin_rx,
            events_tx,
        ));
        Self {
            control_tx,
            stdin_tx,
            events_rx,
        }
    }
}

async fn driver(
    shell: String,
    sftp_server_path: Option<String>,
    mut control_rx: mpsc::Receiver<Control>,
    mut stdin_rx: mpsc::Receiver<Vec<u8>>,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
) {
    let mut pty: Option<(String, u32, u32)> = None;
    let mut env: Vec<(String, String)> = Vec::new();
    let mut backend: Option<Backend> = None;

    loop {
        tokio::select! {
            // Only poll stdin when a backend exists.
            stdin = async {
                match &backend {
                    Some(_) => stdin_rx.recv().await,
                    None => std::future::pending::<Option<Vec<u8>>>().await,
                }
            } => match stdin {
                Some(bytes) => {
                    if let Some(b) = backend.as_mut() {
                        b.write.write(&bytes).await;
                    }
                }
                None => {
                    if let Some(b) = backend.as_mut() { b.write.eof().await; }
                }
            },
            ctrl = control_rx.recv() => match ctrl {
                Some(Control::Pty { term, cols, rows, reply }) => {
                    pty = Some((term, cols, rows));
                    let _ = reply.send(Ok(()));
                }
                Some(Control::Env { key, value, reply }) => {
                    env.push((key, value));
                    let _ = reply.send(Ok(()));
                }
                Some(Control::Exec { command, reply }) => {
                    if backend.is_some() {
                        let _ = reply.send(Err(anyhow::anyhow!("session already running")));
                        continue;
                    }
                    let argv = vec![shell.clone(), "-c".to_string(), command];
                    match spawn(&pty, &env, &argv, &events_tx).await {
                        Ok(b) => { backend = Some(b); let _ = reply.send(Ok(())); }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                }
                Some(Control::Shell { reply }) => {
                    if backend.is_some() {
                        let _ = reply.send(Err(anyhow::anyhow!("session already running")));
                        continue;
                    }
                    let argv = vec![shell.clone()];
                    match spawn(&pty, &env, &argv, &events_tx).await {
                        Ok(b) => { backend = Some(b); let _ = reply.send(Ok(())); }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                }
                Some(Control::Subsystem { name, reply }) => {
                    if name != "sftp" {
                        let _ = reply.send(Err(super::unsupported(&format!("subsystem {name}"))));
                        continue;
                    }
                    let Some(sftp) = resolve_sftp_server(sftp_server_path.as_deref()) else {
                        let _ = reply.send(Err(anyhow::anyhow!("sftp-server binary not found")));
                        continue;
                    };
                    match spawn_sftp(&sftp, &events_tx).await {
                        Ok(b) => { backend = Some(b); let _ = reply.send(Ok(())); }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                }
                Some(Control::WindowChange { cols, rows }) => {
                    if let Some(fd) = backend.as_ref().and_then(|b| b.pty_fd) {
                        pty_resize(fd, cols, rows);
                    }
                }
                Some(Control::Signal { signal }) => {
                    if let Some(b) = backend.as_ref() {
                        signal_pid(b.pid, &signal);
                    }
                }
                Some(Control::Eof) => {
                    if let Some(b) = backend.as_mut() { b.write.eof().await; }
                }
                None => break,
            },
        }
    }
}

#[cfg(unix)]
fn signal_pid(pid: u32, signal: &str) {
    let sig = match signal.to_ascii_uppercase().as_str() {
        "HUP" => libc::SIGHUP,
        "INT" => libc::SIGINT,
        "QUIT" => libc::SIGQUIT,
        "TERM" => libc::SIGTERM,
        "KILL" => libc::SIGKILL,
        "USR1" => libc::SIGUSR1,
        "USR2" => libc::SIGUSR2,
        _ => libc::SIGTERM,
    };
    unsafe {
        libc::kill(pid as libc::pid_t, sig);
    }
}

/// Windows has no POSIX signals; the ConPTY backend will translate INT/TERM to
/// `GenerateConsoleCtrlEvent` and KILL to `TerminateProcess` once implemented.
/// For now this is a no-op (local Windows sessions are pipe-only).
#[cfg(not(unix))]
fn signal_pid(_pid: u32, _signal: &str) {}

/// Spawn `argv` on a pseudo-terminal.
///
/// Unix uses openpty + setsid + TIOCSCTTY; Windows ConPTY support is tracked
/// separately and currently returns an error (local Windows sessions fall back
/// to pipe mode).
#[cfg(unix)]
async fn spawn_pty(
    term: &str,
    cols: u32,
    rows: u32,
    program: String,
    args: &[String],
    env: &[(String, String)],
    events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    let (master, slave) = openpty_pair()?;
    if cols > 0 && rows > 0 {
        pty_resize(slave.as_raw_fd(), cols, rows);
    }
    let pty_fd = master.as_raw_fd();
    let read_fd = dup_fd(&master)?;
    let master_read = tokio::fs::File::from_std(std::fs::File::from(read_fd));
    let master_write = tokio::fs::File::from_std(std::fs::File::from(master));

    let slave_file = std::fs::File::from(slave);
    let stdin = std::process::Stdio::from(slave_file.try_clone()?);
    let stdout = std::process::Stdio::from(slave_file.try_clone()?);
    let stderr = std::process::Stdio::from(slave_file);
    let mut cmd = Command::new(&program);
    cmd.args(args).stdin(stdin).stdout(stdout).stderr(stderr);
    cmd.env(
        "TERM",
        if term.is_empty() {
            "xterm-256color"
        } else {
            term
        },
    );
    for (k, v) in env {
        cmd.env(k, v);
    }
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            libc::ioctl(0, libc::TIOCSCTTY as _, 0i32);
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    let pid = child.id().unwrap_or(0);
    spawn_pty_reader(master_read, events_tx.clone());
    spawn_waiter(child, events_tx.clone());
    Ok(Backend {
        write: WriteSide::Pty(master_write),
        pty_fd: Some(pty_fd),
        pid,
    })
}

/// Windows ConPTY backend is not yet implemented; local Windows sessions use
/// pipe mode only. This keeps the daemon compiling and the non-PTY paths
/// (`xho exec` without `--tty`, sftp subsystem) functional on Windows.
#[cfg(not(unix))]
async fn spawn_pty(
    _term: &str,
    _cols: u32,
    _rows: u32,
    _program: String,
    _args: &[String],
    _env: &[(String, String)],
    _events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    Err(anyhow::anyhow!(
        "PTY-backed local sessions are not yet supported on Windows; \
         re-run without --tty or connect to a Unix xhod for interactive mode"
    ))
}

/// Spawn `argv` (program + args) on a PTY (when requested) or pipes. A waiter
/// task owns the `Child` and reports exit status.
async fn spawn(
    pty: &Option<(String, u32, u32)>,
    env: &[(String, String)],
    argv: &[String],
    events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    let program = argv
        .first()
        .ok_or_else(|| anyhow::anyhow!("empty argv"))?
        .clone();
    let args = &argv[1..];

    if let Some((term, cols, rows)) = pty {
        return spawn_pty(term, *cols, *rows, program, args, env, events_tx).await;
    } else {
        let mut cmd = Command::new(&program);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn()?;
        if let Some(stdout) = child.stdout.take() {
            spawn_pipe_reader(stdout, events_tx.clone(), false);
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_pipe_reader(stderr, events_tx.clone(), true);
        }
        let pid = child.id().unwrap_or(0);
        let stdin = child.stdin.take();
        spawn_waiter(child, events_tx.clone());
        Ok(Backend {
            write: WriteSide::Pipe(stdin),
            pty_fd: None,
            pid,
        })
    }
}

async fn spawn_sftp(
    path: &std::path::Path,
    events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    let mut cmd = Command::new(path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    if let Some(stdout) = cmd.stdout.take() {
        spawn_pipe_reader(stdout, events_tx.clone(), false);
    }
    let pid = cmd.id().unwrap_or(0);
    let stdin = cmd.stdin.take();
    spawn_waiter(cmd, events_tx.clone());
    Ok(Backend {
        write: WriteSide::Pipe(stdin),
        pty_fd: None,
        pid,
    })
}

#[cfg(unix)]
fn spawn_pty_reader(mut read: tokio::fs::File, events_tx: mpsc::UnboundedSender<SessionEvent>) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if events_tx
                        .send(SessionEvent::Stdout(buf[..n].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

fn spawn_pipe_reader<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    mut stream: R,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
    is_stderr: bool,
) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let evt = if is_stderr {
                        SessionEvent::Stderr(buf[..n].to_vec())
                    } else {
                        SessionEvent::Stdout(buf[..n].to_vec())
                    };
                    if events_tx.send(evt).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

fn spawn_waiter(mut child: Child, events_tx: mpsc::UnboundedSender<SessionEvent>) {
    tokio::spawn(async move {
        let code = child.wait().await.ok().and_then(|s| s.code()).unwrap_or(1);
        let _ = events_tx.send(SessionEvent::ExitStatus(code));
        let _ = events_tx.send(SessionEvent::Eof);
    });
}

// -----------------------------------------------------------------------
// TargetSession impl
// -----------------------------------------------------------------------

async fn request(
    control_tx: &mpsc::Sender<Control>,
    build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control,
) -> Result<()> {
    let (rtx, rrx) = oneshot::channel();
    control_tx
        .send(build(rtx))
        .await
        .map_err(|_| anyhow::anyhow!("session closed"))?;
    rrx.await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("session closed")))
}

#[async_trait]
impl TargetSession for LocalSession {
    async fn request_pty(
        &mut self,
        term: &str,
        cols: u32,
        rows: u32,
        _modes: &[(Pty, u32)],
    ) -> Result<()> {
        request(&self.control_tx, |reply| Control::Pty {
            term: term.to_string(),
            cols,
            rows,
            reply,
        })
        .await
    }

    async fn set_env(&mut self, key: &str, value: &str) -> Result<()> {
        request(&self.control_tx, |reply| Control::Env {
            key: key.to_string(),
            value: value.to_string(),
            reply,
        })
        .await
    }

    async fn exec(&mut self, command: &str) -> Result<()> {
        request(&self.control_tx, |reply| Control::Exec {
            command: command.to_string(),
            reply,
        })
        .await
    }

    async fn shell(&mut self) -> Result<()> {
        request(&self.control_tx, |reply| Control::Shell { reply }).await
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        request(&self.control_tx, |reply| Control::Subsystem {
            name: name.to_string(),
            reply,
        })
        .await
    }

    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()> {
        let _ = self
            .control_tx
            .send(Control::WindowChange { cols, rows })
            .await;
        Ok(())
    }

    async fn signal(&mut self, signal: &str) -> Result<()> {
        let _ = self
            .control_tx
            .send(Control::Signal {
                signal: signal.to_string(),
            })
            .await;
        Ok(())
    }

    async fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
        self.stdin_tx
            .send(data.to_vec())
            .await
            .map_err(|_| anyhow::anyhow!("session closed"))?;
        Ok(())
    }

    async fn eof(&mut self) -> Result<()> {
        let _ = self.control_tx.send(Control::Eof).await;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<SessionEvent> {
        self.events_rx.recv().await
    }
}
