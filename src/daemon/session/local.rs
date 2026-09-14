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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use super::{SessionCommand, SessionEvent, SessionStream, SessionWriter, TargetSession};

// -----------------------------------------------------------------------
// PTY helpers (platform-specific)
// -----------------------------------------------------------------------

/// Opaque handle to a live PTY controller, used for resize/signaling.
///
/// Each platform's `PtyBackend` impl carries whatever state it needs:
/// Unix holds the master fd (+ pid for signals); Windows holds the ConPTY
/// `HPCON` + process handle (+ pid).
trait PtyBackend: Send + Sync {
    /// Resize the pseudo-terminal to the given dimensions.
    fn resize(&self, cols: u32, rows: u32);
    /// Deliver a named signal (e.g. "INT", "KILL") to the PTY's process.
    fn signal(&self, name: &str);
}

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

/// Unix PTY backend: holds the master fd (for ioctl resize) + child pid (for signals).
#[cfg(unix)]
struct UnixPtyBackend {
    fd: OwnedFd,
    pid: u32,
}

#[cfg(unix)]
impl PtyBackend for UnixPtyBackend {
    fn resize(&self, cols: u32, rows: u32) {
        pty_resize(self.fd.as_raw_fd(), cols, rows);
    }
    fn signal(&self, name: &str) {
        let sig = match name.to_ascii_uppercase().as_str() {
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
            libc::kill(self.pid as libc::pid_t, sig);
        }
    }
}

/// Windows ConPTY backend: holds the pseudoconsole handle (for resize) and the
/// child process handle (for signals/termination).
#[cfg(windows)]
struct ConPtyBackend {
    /// The pseudoconsole handle returned by `CreatePseudoConsole`.
    hpc: windows_sys::Win32::System::Console::HPCON,
    /// The child process handle (owned; closed on Drop).
    hprocess: windows_sys::Win32::Foundation::HANDLE,
    /// The child process id (for `GenerateConsoleCtrlEvent`).
    pid: u32,
}

// SAFETY: the raw handles are not shared across threads concurrently —
// `ConPtyBackend` lives behind a `Box<dyn PtyBackend>` inside a single driver
// task. The handles are used only for resize/signal/drop on that task. Marking
// it `Send` + `Sync` lets the driver task (owned by one tokio worker) hold it
// across `.await` points.
#[cfg(windows)]
unsafe impl Send for ConPtyBackend {}
#[cfg(windows)]
unsafe impl Sync for ConPtyBackend {}

#[cfg(windows)]
impl PtyBackend for ConPtyBackend {
    fn resize(&self, cols: u32, rows: u32) {
        // Guard against zero-size requests; ConPTY rejects degenerate sizes.
        let c = if cols == 0 { 80 } else { cols.min(32767) } as i16;
        let r = if rows == 0 { 24 } else { rows.min(32767) } as i16;
        let size = windows_sys::Win32::System::Console::COORD { X: c, Y: r };
        // ResizePseudoConsole returns 0 on success; ignore failure (best-effort).
        unsafe {
            windows_sys::Win32::System::Console::ResizePseudoConsole(self.hpc, size);
        }
    }

    fn signal(&self, name: &str) {
        use windows_sys::Win32::System::Console::{
            CTRL_BREAK_EVENT, CTRL_C_EVENT, GenerateConsoleCtrlEvent,
        };
        use windows_sys::Win32::System::Threading::TerminateProcess;
        match name.to_ascii_uppercase().as_str() {
            "INT" => unsafe {
                // CTRL_C_EVENT only affects processes sharing the console of the
                // calling thread; because the child is in our ConPTY and was
                // created with CREATE_NEW_PROCESS_GROUP, CTRL_C_EVENT may not
                // reach it. We still try (commonly works for cmd.exe).
                let _ = GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0);
            },
            "BREAK" | "QUIT" => unsafe {
                let _ = GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, self.pid);
            },
            "KILL" | "TERM" | "HUP" | "USR1" | "USR2" | _ => unsafe {
                // Windows has no analog for these; terminate the process.
                let _ = TerminateProcess(self.hprocess, 1);
            },
        }
    }
}

#[cfg(windows)]
impl Drop for ConPtyBackend {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        unsafe {
            // Closing the pseudoconsole drains pending I/O then signals the
            // attached process group to exit. Close the process handle after.
            windows_sys::Win32::System::Console::ClosePseudoConsole(self.hpc);
            if !self.hprocess.is_null() {
                CloseHandle(self.hprocess);
            }
        }
    }
}

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

/// The "run a single command" flag for a given shell program path.
///
/// `cmd.exe`/`powershell` use `/c`; POSIX shells (`sh`/`bash`/`zsh`) use `-c`.
/// The flag must match the *actual* shell, not the platform — on Windows a
/// Git-Bash environment may set `SHELL` to bash.exe, in which case `-c` still
/// applies. `SessionCommand::Exec` wraps a command as `[shell, flag, command]`.
fn shell_exec_flag(shell: &str) -> &'static str {
    // Lowercase the basename for matching (via the unified filepath helper).
    let lower = shell.to_ascii_lowercase();
    let basename = crate::filepath::quote::shell_basename(&lower);
    if basename == "cmd.exe" || basename == "cmd" || basename.starts_with("powershell") {
        "/c"
    } else {
        // sh, bash, zsh, fish, etc. all take -c.
        "-c"
    }
}

// -----------------------------------------------------------------------
// Session command handling
// -----------------------------------------------------------------------

/// What the driver needs to drive a running backend.
struct Backend {
    /// Write side for stdin (PTY master or pipe stdin).
    write: WriteSide,
    /// PTY controller for window-resize + signal delivery (None for pipes).
    pty: Option<Box<dyn PtyBackend>>,
    /// Process id (for the waiter task to reap exit status).
    pid: u32,
}

enum WriteSide {
    /// Unix PTY master: readiness-driven AsyncFd writes (see write()).
    #[cfg(unix)]
    Pty(std::sync::Arc<tokio::io::unix::AsyncFd<std::fs::File>>),
    /// Plain file/handle-backed PTY write (Windows ConPTY pipe).
    #[cfg(windows)]
    Pty(tokio::fs::File),
    Pipe(Option<ChildStdin>),
}

impl WriteSide {
    async fn write(&mut self, data: &[u8]) {
        match self {
            #[cfg(unix)]
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
            #[cfg(windows)]
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
            #[cfg(unix)]
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
            #[cfg(windows)]
            WriteSide::Pty(f) => {
                let _ = f.write_all(b"\x04").await;
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
                    let argv = vec![shell.clone(), shell_exec_flag(&shell).to_string(), command];
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
                    if let Some(b) = backend.as_ref() {
                        if let Some(pty) = b.pty.as_ref() {
                            pty.resize(cols, rows);
                        }
                    }
                }
                Some(SessionCommand::Signal { signal }) => {
                    if let Some(b) = backend.as_ref() {
                        if let Some(pty) = b.pty.as_ref() {
                            pty.signal(&signal);
                        }
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

/// Spawn `argv` on a pseudo-terminal.
///
/// Unix uses openpty + setsid + TIOCSCTTY. The write side is an `AsyncFd`
/// (readiness-driven; a plain tokio::fs::File parks blocking-pool threads once
/// the pty buffer fills). A duplicated master fd feeds the `PtyBackend` used
/// for resize/signal. Windows uses the ConPTY `spawn_pty` below.
#[cfg(unix)]
async fn spawn_pty(
    term: &str,
    cols: u32,
    rows: u32,
    program: String,
    args: &[String],
    env: &[(String, String)],
    workdir: Option<&std::path::PathBuf>,
    events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    let (master, slave) = openpty_pair()?;
    if cols > 0 && rows > 0 {
        pty_resize(slave.as_raw_fd(), cols, rows);
    }
    // Keep a duplicate of the master fd for the backend (resize/signal); the
    // originals move into the read/write tokio Files.
    let resize_fd = dup_fd(&master)?;
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
        pty: Some(Box::new(UnixPtyBackend { fd: resize_fd, pid })),
        pid,
    })
}
/// Spawn `argv` on a Windows ConPTY pseudoconsole.
///
/// Pipeline: CreatePipe ×2 → CreatePseudoConsole →
/// InitializeProcThreadAttributeList + UpdateProcThreadAttribute(PSEUDOCONSOLE)
/// → CreateProcessW(STARTUPINFOEXW) → spawn reader + waiter tasks.
#[cfg(windows)]
async fn spawn_pty(
    term: &str,
    cols: u32,
    rows: u32,
    program: String,
    args: &[String],
    env: &[(String, String)],
    workdir: Option<&std::path::PathBuf>,
    events_tx: &mpsc::UnboundedSender<SessionEvent>,
) -> Result<Backend> {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Console::{
        COORD, CreatePseudoConsole, HPCON, PSEUDOCONSOLE_INHERIT_CURSOR,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::Threading::{
        CREATE_NEW_PROCESS_GROUP, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
        DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT,
        InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
        PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_INFORMATION, STARTUPINFOEXW,
        UpdateProcThreadAttribute,
    };

    // ---- 1. Build the command line (UTF-16, program + args joined by spaces).
    // CreateProcessW's lpCommandLine must start with argv[0]. We quote any
    // token containing whitespace. (cmd.exe-style complex quoting is out of
    // scope; callers should pre-quote if needed.)
    let mut cmdline = String::new();
    for (i, tok) in std::iter::once(&program).chain(args.iter()).enumerate() {
        if i > 0 {
            cmdline.push(' ');
        }
        if tok.contains(' ') || tok.contains('\t') {
            cmdline.push('"');
            cmdline.push_str(tok);
            cmdline.push('"');
        } else {
            cmdline.push_str(tok);
        }
    }
    let mut cmdline_w: Vec<u16> = cmdline.encode_utf16().collect();
    cmdline_w.push(0); // null-terminate

    // ---- 2. Build the UTF-16 environment block (KEY=VALUE\0...\0).
    let mut envblock: Vec<u16> = Vec::new();
    for (k, v) in env {
        envblock.extend(format!("{k}={v}").encode_utf16());
        envblock.push(0);
    }
    envblock.push(0); // terminating double-null
    // Also set TERM so terminal-aware programs (the common case for _self PTY
    // use) behave as if attached to a Unix-style terminal.
    let term_value = if term.is_empty() {
        "xterm-256color"
    } else {
        term
    };
    {
        let mut tmp = format!("TERM={term_value}")
            .encode_utf16()
            .collect::<Vec<_>>();
        tmp.push(0);
        envblock.splice(0..0, tmp); // prepend (order is irrelevant to Windows)
    }

    // ---- 3. Create the input/output pipes for the pseudoconsole.
    let mut input_read: windows_sys::Win32::Foundation::HANDLE = std::ptr::null_mut();
    let mut input_write: windows_sys::Win32::Foundation::HANDLE = std::ptr::null_mut();
    let mut output_read: windows_sys::Win32::Foundation::HANDLE = std::ptr::null_mut();
    let mut output_write: windows_sys::Win32::Foundation::HANDLE = std::ptr::null_mut();
    // SAFETY: anonymous pipe creation; handles are valid on success (non-null).
    let ok = unsafe {
        CreatePipe(&mut input_read, &mut input_write, std::ptr::null(), 0) != 0
            && CreatePipe(&mut output_read, &mut output_write, std::ptr::null(), 0) != 0
    };
    if !ok {
        let err = std::io::Error::last_os_error();
        unsafe {
            if !input_read.is_null() {
                CloseHandle(input_read);
            }
            if !input_write.is_null() {
                CloseHandle(input_write);
            }
            if !output_read.is_null() {
                CloseHandle(output_read);
            }
            if !output_write.is_null() {
                CloseHandle(output_write);
            }
        }
        return Err(anyhow::anyhow!("ConPTY CreatePipe failed: {err}"));
    }

    // ---- 4. Create the pseudoconsole.
    let c = if cols == 0 { 80 } else { cols.min(32767) } as i16;
    let r = if rows == 0 { 24 } else { rows.min(32767) } as i16;
    let size = COORD { X: c, Y: r };
    let mut hpc: HPCON = 0;
    let pc_result = unsafe {
        CreatePseudoConsole(
            size,
            input_read,
            output_write,
            PSEUDOCONSOLE_INHERIT_CURSOR,
            &mut hpc,
        )
    };
    // input_read and output_write now belong to the pseudoconsole; close our
    // copies to avoid handle leaks (the PTY owns them internally).
    unsafe {
        CloseHandle(input_read);
        CloseHandle(output_write);
    }
    if pc_result != 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            CloseHandle(input_write);
            CloseHandle(output_read);
        }
        return Err(anyhow::anyhow!("CreatePseudoConsole failed: {err}"));
    }

    // ---- 5. Initialize the proc-thread attribute list (sized via first call).
    let attr_count = 1u32;
    let mut dummy: [u8; 0] = [];
    let mut needed: usize = 0;
    // First call with null buffer to discover size; it returns FALSE with
    // ERROR_INSUFFICIENT_BUFFER and sets `needed`.
    unsafe {
        InitializeProcThreadAttributeList(
            dummy.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST,
            attr_count,
            0,
            &mut needed,
        );
    }
    if needed == 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            windows_sys::Win32::System::Console::ClosePseudoConsole(hpc);
            CloseHandle(input_write);
            CloseHandle(output_read);
        }
        return Err(anyhow::anyhow!(
            "InitializeProcThreadAttributeList (size) failed: {err}"
        ));
    }
    let mut attr_buf: Vec<u8> = vec![0u8; needed];
    let attr_list = attr_buf.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
    let init_ok =
        unsafe { InitializeProcThreadAttributeList(attr_list, attr_count, 0, &mut needed) != 0 };
    if !init_ok {
        let err = std::io::Error::last_os_error();
        unsafe {
            windows_sys::Win32::System::Console::ClosePseudoConsole(hpc);
            CloseHandle(input_write);
            CloseHandle(output_read);
        }
        return Err(anyhow::anyhow!(
            "InitializeProcThreadAttributeList failed: {err}"
        ));
    }

    // ---- 6. Bind the pseudoconsole handle to the attribute list.
    let upd_ok = unsafe {
        UpdateProcThreadAttribute(
            attr_list,
            0,
            PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
            hpc as *const std::ffi::c_void,
            std::mem::size_of::<HPCON>(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        ) != 0
    };
    if !upd_ok {
        let err = std::io::Error::last_os_error();
        unsafe {
            DeleteProcThreadAttributeList(attr_list);
            windows_sys::Win32::System::Console::ClosePseudoConsole(hpc);
            CloseHandle(input_write);
            CloseHandle(output_read);
        }
        return Err(anyhow::anyhow!("UpdateProcThreadAttribute failed: {err}"));
    }

    // ---- 7. CreateProcessW with STARTUPINFOEXW carrying the attribute list.
    let mut si: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
    si.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
    si.lpAttributeList = attr_list;
    let mut pi: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let creation_flags =
        EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_NEW_PROCESS_GROUP;
    let program_w: Vec<u16> = program.encode_utf16().chain(std::iter::once(0)).collect();
    let cwd_w: Option<Vec<u16>> = workdir.map(|dir| {
        dir.as_os_str()
            .to_string_lossy()
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect()
    });
    let cp_ok = unsafe {
        CreateProcessW(
            program_w.as_ptr(),
            cmdline_w.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0, // bInheritHandles = FALSE: ConPTY owns the pipes
            creation_flags as u32,
            envblock.as_mut_ptr() as *const std::ffi::c_void,
            cwd_w
                .as_ref()
                .map(|w| w.as_ptr())
                .unwrap_or(std::ptr::null()),
            &si.StartupInfo as *const _ as *const _,
            &mut pi,
        ) != 0
    };
    // Attribute list + buf are no longer needed after CreateProcessW.
    unsafe {
        DeleteProcThreadAttributeList(attr_list);
    }
    if !cp_ok {
        let err = std::io::Error::last_os_error();
        unsafe {
            windows_sys::Win32::System::Console::ClosePseudoConsole(hpc);
            CloseHandle(input_write);
            CloseHandle(output_read);
        }
        return Err(anyhow::anyhow!(
            "CreateProcessW failed for `{program}`: {err}"
        ));
    }
    // Close the thread handle immediately (we only track the process).
    unsafe {
        CloseHandle(pi.hThread);
    }
    let hprocess = pi.hProcess;
    let pid = pi.dwProcessId;
    if hprocess.is_null() || hprocess == INVALID_HANDLE_VALUE {
        unsafe {
            windows_sys::Win32::System::Console::ClosePseudoConsole(hpc);
            CloseHandle(input_write);
            CloseHandle(output_read);
        }
        return Err(anyhow::anyhow!(
            "CreateProcessW returned invalid process handle"
        ));
    }

    // ---- 8. Wrap the I/O pipe ends for async + spawn reader/waiter tasks.
    // input_write  → we write stdin here (PTY input)
    // output_read  → we read PTY output here
    let master_write = tokio::fs::File::from_std(unsafe {
        // SAFETY: input_write is a valid HANDLE we own; transfer ownership.
        // FromRawHandle gives us a OwnedHandle; std::fs::File takes it.
        std::os::windows::io::FromRawHandle::from_raw_handle(input_write)
    });
    let master_read = tokio::fs::File::from_std(unsafe {
        std::os::windows::io::FromRawHandle::from_raw_handle(output_read)
    });

    spawn_pty_reader(master_read, events_tx.clone());
    spawn_waiter_conpty(hprocess, pid, events_tx.clone());

    Ok(Backend {
        write: WriteSide::Pty(master_write),
        pty: Some(Box::new(ConPtyBackend { hpc, hprocess, pid })),
        pid,
    })
}

/// Windows ConPTY waiter: blocks on `WaitForSingleObject` + `GetExitCodeProcess`
/// (in a blocking thread) and reports ExitStatus/Eof. Unlike the Unix path,
/// ConPTY processes are not managed by `tokio::process::Child`.
#[cfg(windows)]
fn spawn_waiter_conpty(
    hprocess: windows_sys::Win32::Foundation::HANDLE,
    pid: u32,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
) {
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, INFINITE, WaitForSingleObject,
    };
    // Carry the handle as a usize across the spawn_blocking boundary (raw
    // pointers are not `Send`).
    let handle_usize = hprocess as usize;
    tokio::task::spawn_blocking(move || {
        let hprocess = handle_usize as HANDLE;
        // SAFETY: WaitForSingleObject on a process handle we own is safe.
        unsafe {
            WaitForSingleObject(hprocess, INFINITE);
            let mut code: u32 = 0;
            GetExitCodeProcess(hprocess, &mut code);
            let _ = events_tx.send(SessionEvent::ExitStatus(code as i32));
        }
        let _ = pid; // available for diagnostics if needed later
        let _ = events_tx.send(SessionEvent::Eof);
    });
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
        return spawn_pty(
            term,
            *cols,
            *rows,
            program,
            args,
            env,
            workdir.as_ref(),
            events_tx,
        )
        .await;
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
            pty: None,
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
        pty: None,
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

    // Spawns /bin/sh via the unix PTY path; unix-only.
    #[cfg(unix)]
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

    // Spawns /bin/sh via the unix PTY path; unix-only.
    #[cfg(unix)]
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

    // Spawns /bin/sh via the unix PTY path; unix-only.
    #[cfg(unix)]
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
    // Spawns /bin/sh via the unix PTY path; unix-only.
    #[cfg(unix)]
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

    #[test]
    fn shell_exec_flag_matches_shell_name() {
        // The flag must match the *actual* shell, not the platform: a Windows
        // machine with Git Bash may use bash.exe (needs -c), while cmd.exe
        // needs /c.
        assert_eq!(shell_exec_flag("/bin/sh"), "-c");
        assert_eq!(shell_exec_flag("/usr/bin/bash"), "-c");
        assert_eq!(shell_exec_flag("bash"), "-c");
        assert_eq!(shell_exec_flag("zsh"), "-c");
        assert_eq!(shell_exec_flag(r"C:\Windows\System32\cmd.exe"), "/c");
        assert_eq!(shell_exec_flag("cmd.exe"), "/c");
        assert_eq!(shell_exec_flag("powershell"), "/c");
        assert_eq!(shell_exec_flag("powershell.exe"), "/c");
        // Case-insensitive on the basename.
        assert_eq!(shell_exec_flag("CMD.EXE"), "/c");
    }

    /// The Windows cmd.exe command-join must not double-escape. This checks the
    /// join logic shape (mirrors `open_exec_session`'s Windows branch) without
    /// needing a live daemon — it exercises the pure transformation.
    #[cfg(not(unix))]
    #[test]
    fn windows_argv_join_quotes_spaces() {
        let argv = vec!["echo".to_string(), "hello world".to_string()];
        let command = argv
            .iter()
            .map(|a| {
                if a.contains(' ') || a.contains('\t') {
                    format!("\"{a}\"")
                } else {
                    a.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        // `cmd.exe /c echo "hello world"` preserves the single argument.
        assert_eq!(command, "echo \"hello world\"");
    }
}
