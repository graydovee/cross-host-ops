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

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;

use anyhow::Result;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use super::{SessionCommand, SessionEvent, SessionStream, SessionWriter, TargetSession};

// -----------------------------------------------------------------------
// PTY helpers
// -----------------------------------------------------------------------

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

fn dup_fd(fd: &OwnedFd) -> Result<OwnedFd> {
    let new = unsafe { libc::dup(fd.as_raw_fd()) };
    if new < 0 {
        return Err(anyhow::anyhow!("dup: {}", std::io::Error::last_os_error()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(new) })
}

fn pty_resize(fd: libc::c_int, cols: u32, rows: u32) {
    let ws = libc::winsize {
        ws_row: rows as u16,
        ws_col: cols as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &ws) };
}

/// Resolve the sftp-server binary: explicit config, common locations, PATH.
fn resolve_sftp_server(configured: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = configured {
        let expanded = crate::config::expand_tilde(p).unwrap_or_else(|_| p.to_string());
        return Some(PathBuf::from(expanded));
    }
    for candidate in [
        "/usr/lib/openssh/sftp-server",
        "/usr/libexec/openssh/sftp-server",
        "/usr/libexec/sftp-server",
        "/usr/lib/ssh/sftp-server",
    ] {
        if std::path::Path::new(candidate).exists() {
            return Some(PathBuf::from(candidate));
        }
    }
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path).find_map(|dir| {
            let full = dir.join("sftp-server");
            full.is_file().then_some(full)
        })
    })
}

// -----------------------------------------------------------------------
// Session command handling
// -----------------------------------------------------------------------

/// What the driver needs to drive a running backend.
struct Backend {
    /// Write side for stdin (PTY master or pipe stdin).
    write: WriteSide,
    /// PTY master raw fd for window-resize ioctl (None for pipes).
    pty_fd: Option<libc::c_int>,
    /// Process id for signal delivery.
    pid: u32,
}

enum WriteSide {
    Pty(std::sync::Arc<tokio::io::unix::AsyncFd<std::fs::File>>),
    Pipe(Option<ChildStdin>),
}

impl WriteSide {
    async fn write(&mut self, data: &[u8]) {
        match self {
            WriteSide::Pty(fd) => {
                // True readiness-driven async write. A PTY master MUST NOT use
                // tokio::fs::File: every write there runs as a blocking-pool
                // task, and when the terminal buffer fills (vim redraws while
                // the consumer is momentarily slow) those tasks park forever,
                // eating blocking-pool threads one per keystroke until the
                // daemon wedges.
                loop {
                    let mut guard = match fd.writable().await {
                        Ok(g) => g,
                        Err(_) => return,
                    };
                    match guard.try_io(|inner| {
                        <&std::fs::File as std::io::Write>::write(&mut { &inner.get_ref() }, data)
                    }) {
                        Ok(Ok(n)) if n == data.len() => break,
                        Ok(_) => break, // partial write on pty is not expected; drop remainder like old behavior
                        Err(_would_block) => continue,
                        Ok(Err(_)) => return,
                    }
                }
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
            WriteSide::Pty(fd) => {
                if let Ok(mut guard) = fd.writable().await {
                    let _ = guard.try_io(|inner| {
                        <&std::fs::File as std::io::Write>::write(
                            &mut { &inner.get_ref() },
                            b"\x04",
                        )
                    });
                }
            }
            WriteSide::Pipe(s) => {
                s.take();
            }
        }
    }
}

pub struct LocalSession {
    writer: Option<SessionWriter>,
    stream: Option<SessionStream>,
}

impl LocalSession {
    pub fn new(shell: String, sftp_server_path: Option<String>, workdir: Option<PathBuf>) -> Self {
        // Control and stdin share ONE ordered channel so eof/data cannot
        // overtake the exec/subsystem start (see DirectSshSession for the
        // rationale); pre-start stdin is buffered by the driver.
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        tokio::spawn(driver(shell, sftp_server_path, workdir, cmd_rx, events_tx));
        Self {
            writer: Some(SessionWriter { tx: cmd_tx }),
            stream: Some(SessionStream { rx: events_rx }),
        }
    }
}

impl TargetSession for LocalSession {
    fn split(mut self: Box<Self>) -> (SessionWriter, SessionStream) {
        (
            self.writer.take().expect("local session split twice"),
            self.stream.take().expect("local session split twice"),
        )
    }
}

async fn driver(
    shell: String,
    sftp_server_path: Option<String>,
    workdir: Option<PathBuf>,
    mut cmd_rx: mpsc::Receiver<SessionCommand>,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
) {
    let mut pty: Option<(String, u32, u32)> = None;
    let mut env: Vec<(String, String)> = Vec::new();
    let mut backend: Option<Backend> = None;
    // Stdin that arrived before a backend was started (kept for parity with
    // the old backend-gated stdin channel; callers normally start first).
    let mut pending_stdin: std::collections::VecDeque<Vec<u8>> = Default::default();
    let mut pending_eof = false;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                Some(SessionCommand::Pty { term, cols, rows, reply, .. }) => {
                    pty = Some((term, cols, rows));
                    let _ = reply.send(Ok(()));
                }
                Some(SessionCommand::Env { key, value, reply }) => {
                    env.push((key, value));
                    let _ = reply.send(Ok(()));
                }
                Some(SessionCommand::Exec { command, reply }) => {
                    if backend.is_some() {
                        let _ = reply.send(Err(anyhow::anyhow!("session already running")));
                        continue;
                    }
                    let argv = vec![shell.clone(), "-c".to_string(), command];
                    match spawn(&pty, &env, &argv, &workdir, &events_tx).await {
                        Ok(b) => {
                            backend = Some(b);
                            flush_pending_stdin(&mut backend, &mut pending_stdin, &mut pending_eof).await;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                }
                Some(SessionCommand::Shell { reply }) => {
                    if backend.is_some() {
                        let _ = reply.send(Err(anyhow::anyhow!("session already running")));
                        continue;
                    }
                    let argv = vec![shell.clone()];
                    match spawn(&pty, &env, &argv, &workdir, &events_tx).await {
                        Ok(b) => {
                            backend = Some(b);
                            flush_pending_stdin(&mut backend, &mut pending_stdin, &mut pending_eof).await;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                }
                Some(SessionCommand::Subsystem { name, reply }) => {
                    if name != "sftp" {
                        let _ = reply.send(Err(super::unsupported(&format!("subsystem {name}"))));
                        continue;
                    }
                    let Some(sftp) = resolve_sftp_server(sftp_server_path.as_deref()) else {
                        let _ = reply.send(Err(anyhow::anyhow!("sftp-server binary not found")));
                        continue;
                    };
                    match spawn_sftp(&sftp, &events_tx).await {
                        Ok(b) => {
                            backend = Some(b);
                            flush_pending_stdin(&mut backend, &mut pending_stdin, &mut pending_eof).await;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => { let _ = reply.send(Err(e)); }
                    }
                }
                Some(SessionCommand::Resize { cols, rows }) => {
                    if let Some(fd) = backend.as_ref().and_then(|b| b.pty_fd) {
                        pty_resize(fd, cols, rows);
                    }
                }
                Some(SessionCommand::Signal { signal }) => {
                    if let Some(b) = backend.as_ref() {
                        signal_pid(b.pid, &signal);
                    }
                }
                Some(SessionCommand::Eof) => {
                    if let Some(b) = backend.as_mut() {
                        b.write.eof().await;
                    } else {
                        pending_eof = true;
                    }
                }
                Some(SessionCommand::Data { bytes }) => {
                    if let Some(b) = backend.as_mut() {
                        b.write.write(&bytes).await;
                    } else {
                        pending_stdin.push_back(bytes);
                    }
                }
                None => break,
            },
        }
    }
}

/// Deliver stdin that was buffered while no backend was running.
async fn flush_pending_stdin(
    backend: &mut Option<Backend>,
    pending: &mut std::collections::VecDeque<Vec<u8>>,
    pending_eof: &mut bool,
) {
    let Some(b) = backend.as_mut() else { return };
    while let Some(bytes) = pending.pop_front() {
        b.write.write(&bytes).await;
    }
    if *pending_eof {
        b.write.eof().await;
        *pending_eof = false;
    }
}

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

/// Spawn `argv` (program + args) on a PTY (when requested) or pipes. A waiter
/// task owns the `Child` and reports exit status.
async fn spawn(
    pty: &Option<(String, u32, u32)>,
    env: &[(String, String)],
    argv: &[String],
    workdir: &Option<PathBuf>,
    events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    let program = argv
        .first()
        .ok_or_else(|| anyhow::anyhow!("empty argv"))?
        .clone();
    let args = &argv[1..];

    if let Some((term, cols, rows)) = pty {
        let (master, slave) = openpty_pair()?;
        if *cols > 0 && *rows > 0 {
            pty_resize(slave.as_raw_fd(), *cols, *rows);
        }
        let pty_fd = master.as_raw_fd();
        let read_fd = dup_fd(&master)?;
        let master_read = tokio::fs::File::from_std(std::fs::File::from(read_fd));
        // AsyncFd gives real readiness-driven writes (see WriteSide::write).
        let master_write =
            std::sync::Arc::new(tokio::io::unix::AsyncFd::new(std::fs::File::from(master))?);

        let slave_file = std::fs::File::from(slave);
        let stdin = std::process::Stdio::from(slave_file.try_clone()?);
        let stdout = std::process::Stdio::from(slave_file.try_clone()?);
        let stderr = std::process::Stdio::from(slave_file);
        let mut cmd = Command::new(&program);
        cmd.args(args).stdin(stdin).stdout(stdout).stderr(stderr);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        cmd.env(
            "TERM",
            if term.is_empty() {
                "xterm-256color"
            } else {
                term.as_str()
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
    } else {
        let mut cmd = Command::new(&program);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn split_exec_streams_output_and_exit_code() {
        let sess: Box<dyn TargetSession> =
            Box::new(LocalSession::new("/bin/sh".to_string(), None, None));
        let (writer, mut stream) = sess.split();
        writer
            .exec("printf hello; echo err 1>&2; exit 7")
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let mut code = None;
        while let Some(ev) = stream.next().await {
            match ev {
                SessionEvent::Stdout(d) => stdout.extend_from_slice(&d),
                SessionEvent::Stderr(_) => {}
                SessionEvent::ExitStatus(c) => {
                    code = Some(c);
                    break;
                }
                SessionEvent::ExitSignal(_) | SessionEvent::Eof => break,
            }
        }
        assert_eq!(stdout, b"hello");
        assert_eq!(code, Some(7));
    }

    #[tokio::test]
    async fn split_carries_stdin_through_writer() {
        let sess: Box<dyn TargetSession> =
            Box::new(LocalSession::new("/bin/sh".to_string(), None, None));
        let (writer, mut stream) = sess.split();
        writer.exec("cat").await.unwrap();
        writer.write_stdin(b"ping").await.unwrap();
        writer.eof().await.unwrap();

        let mut stdout = Vec::new();
        while let Some(ev) = stream.next().await {
            match ev {
                SessionEvent::Stdout(d) => stdout.extend_from_slice(&d),
                SessionEvent::ExitStatus(_) | SessionEvent::Eof => break,
                _ => {}
            }
        }
        assert_eq!(stdout, b"ping");
    }

    #[tokio::test]
    async fn exec_runs_in_configured_workdir() {
        let dir = std::env::temp_dir().join(format!("xho-local-workdir-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sess: Box<dyn TargetSession> = Box::new(LocalSession::new(
            "/bin/sh".to_string(),
            None,
            Some(dir.clone()),
        ));
        let (writer, mut stream) = sess.split();
        writer.exec("pwd -P").await.unwrap();

        let mut stdout = Vec::new();
        while let Some(ev) = stream.next().await {
            match ev {
                SessionEvent::Stdout(d) => stdout.extend_from_slice(&d),
                SessionEvent::ExitStatus(_) | SessionEvent::Eof => break,
                _ => {}
            }
        }
        // Compare against the canonicalized path: temp dirs are symlinked
        // (/var → /private/var on macOS) and `pwd -P` prints the physical one.
        let expected = std::fs::canonicalize(&dir).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            String::from_utf8_lossy(&stdout).trim(),
            expected.to_str().unwrap()
        );
    }
}

#[cfg(test)]
mod deadlock_tests {
    use super::*;
    use std::time::Duration;

    // Regression: a remote process that never reads stdin while its stdout is
    // consumed must not freeze the session. The stdin flood parks the write
    // direction (pipe + channel buffers fill); the event stream must still
    // deliver output and the exit status.
    #[tokio::test]
    async fn stdin_backpressure_does_not_stall_output() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);

        // Flood stdin far beyond every buffer in the path. Run in the
        // background so the assertion path below always executes.
        let flood = tokio::spawn(async move {
            let stdin_tx = stdin_tx;
            for _ in 0..64 {
                if stdin_tx.send(vec![b'x'; 65536]).await.is_err() {
                    break;
                }
            }
        });

        let sess: Box<dyn TargetSession> =
            Box::new(LocalSession::new("/bin/sh".to_string(), None, None));
        let handle = tokio::spawn(super::super::drive_exec(
            sess,
            "echo alive-during-flood; exit 9".to_string(),
            false,
            0,
            0,
            event_tx,
            Some(stdin_rx),
        ));

        let mut saw_alive = false;
        let deadline = tokio::time::timeout(Duration::from_secs(20), async {
            while let Some(ev) = event_rx.recv().await {
                match ev {
                    crate::protocol::ServerEvent::Stdout { data } => {
                        if String::from_utf8_lossy(&data).contains("alive-during-flood") {
                            saw_alive = true;
                        }
                    }
                    crate::protocol::ServerEvent::ExitStatus { .. } => break,
                    _ => {}
                }
            }
        })
        .await;
        assert!(deadline.is_ok(), "session froze under stdin backpressure");
        assert!(saw_alive, "output never arrived while stdin was saturated");
        let code = tokio::time::timeout(Duration::from_secs(20), handle)
            .await
            .expect("drive_exec never finished")
            .unwrap()
            .unwrap();
        assert_eq!(code, 9);
        let _ = flood.await;
    }
}
