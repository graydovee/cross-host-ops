// LocalhostGateway — executes operations on the local machine directly.
//
// Uses openpty() for TTY mode. The master fd is dup()'d into separate
// read/write handles so concurrent stdin/stdout never contend on a mutex.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::config::DirectAuth;
use crate::daemon::connection::shared::build_final_command;
use crate::protocol::{ServerEvent, ServerListRow};
use crate::types::ServerListSource;

use super::{
    ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle, InteractiveRequest,
};

/// The reserved gateway name for local host access.
pub const SELF_GATEWAY_NAME: &str = "_self";

// ---------------------------------------------------------------------------
// PTY helpers
// ---------------------------------------------------------------------------

/// Call `openpty()` → `(master_fd, slave_fd)`.
fn openpty_pair() -> Result<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if rc != 0 {
        return Err(anyhow!("openpty: {}", std::io::Error::last_os_error()));
    }
    let m = unsafe { OwnedFd::from_raw_fd(master) };
    let s = unsafe { OwnedFd::from_raw_fd(slave) };
    Ok((m, s))
}

/// Duplicate an fd.
fn dup_fd(fd: &OwnedFd) -> Result<OwnedFd> {
    let new = unsafe { libc::dup(fd.as_raw_fd()) };
    if new < 0 {
        return Err(anyhow!("dup: {}", std::io::Error::last_os_error()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(new) })
}

/// `TIOCSWINSZ` ioctl on a raw fd.
fn pty_resize(fd: libc::c_int, cols: u32, rows: u32) {
    let ws = libc::winsize {
        ws_row: rows as u16,
        ws_col: cols as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws) };
}

/// Spawn a child on a PTY slave fd. Sets up controlling terminal + TERM.
fn spawn_on_pty(
    program: &str,
    args: &[String],
    slave_fd: OwnedFd,
) -> std::io::Result<tokio::process::Child> {
    let slave = std::fs::File::from(slave_fd);
    let stdin = std::process::Stdio::from(slave.try_clone()?);
    let stdout = std::process::Stdio::from(slave.try_clone()?);
    let stderr = std::process::Stdio::from(slave);

    let mut cmd = Command::new(program);
    cmd.args(args).stdin(stdin).stdout(stdout).stderr(stderr);
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            libc::ioctl(0, libc::TIOCSCTTY, 0i32);
            libc::setenv(
                b"TERM\0".as_ptr() as *const _,
                b"xterm-256color\0".as_ptr() as *const _,
                1,
            );
            Ok(())
        });
    }
    cmd.spawn()
}

// ---------------------------------------------------------------------------
// LocalhostGateway
// ---------------------------------------------------------------------------

pub struct LocalhostGateway {
    /// Default shell for TTY mode (from config or $SHELL).
    shell: String,
    /// Execution user (from config or $USER).
    user: String,
    /// This machine's hostname.
    hostname: String,
}

impl LocalhostGateway {
    pub fn new(shell: Option<String>, user: Option<String>) -> Self {
        Self {
            shell: shell
                .or_else(|| std::env::var("SHELL").ok())
                .unwrap_or_else(|| "/bin/sh".to_string()),
            user: user
                .or_else(|| std::env::var("USER").ok())
                .or_else(|| std::env::var("LOGNAME").ok())
                .unwrap_or_else(|| "unknown".to_string()),
            hostname: get_hostname(),
        }
    }
}

fn get_hostname() -> String {
    let mut buf = [0u8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret == 0 {
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..len]).to_string()
    } else {
        "localhost".to_string()
    }
}

fn build_program_args(argv: &[String], shell: &str, no_shell: bool) -> (String, Vec<String>) {
    if no_shell {
        (argv[0].clone(), argv[1..].to_vec())
    } else {
        let sh = if shell.is_empty() { "/bin/sh" } else { shell };
        let cmd_str = build_final_command(argv, sh);
        (sh.to_string(), vec!["-c".to_string(), cmd_str])
    }
}

#[async_trait]
impl Gateway for LocalhostGateway {
    async fn exec(&self, _target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let argv = &request.argv;
        if argv.is_empty() {
            return Err(GatewayError::execution(anyhow!("empty argv")));
        }
        // Use request.shell if set (CLI --shell), otherwise fall back to
        // the configured default shell.
        let effective_shell = if request.shell.is_empty() {
            self.shell.as_str()
        } else {
            request.shell.as_str()
        };
        let (program, args) = build_program_args(argv, effective_shell, request.no_shell);
        debug!(cmd = ?argv, tty = request.tty, "host exec");

        if request.tty {
            exec_pty(request, &program, &args).await
        } else {
            exec_pipes(request, &program, &args).await
        }
    }


    async fn exec_interactive(
        &self,
        _target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let argv = &request.argv;
        if argv.is_empty() {
            return Err(GatewayError::execution(anyhow!("empty argv")));
        }
        let effective_shell = if request.shell.is_empty() {
            self.shell.as_str()
        } else {
            request.shell.as_str()
        };
        let (program, args) = build_program_args(argv, effective_shell, request.no_shell);
        debug!(cmd = ?argv, "host interactive (PTY)");

        // Create PTY pair.
        let (master_fd, slave_fd) = openpty_pair().map_err(GatewayError::execution)?;
        if request.cols > 0 && request.rows > 0 {
            pty_resize(slave_fd.as_raw_fd(), request.cols, request.rows);
        }

        // dup master for independent read/write tokio locks.
        let master_raw = master_fd.as_raw_fd();
        let read_fd = dup_fd(&master_fd).map_err(GatewayError::execution)?;
        let master_read = tokio::fs::File::from_std(std::fs::File::from(read_fd));
        let master_write = tokio::fs::File::from_std(std::fs::File::from(master_fd));

        // Spawn child with slave as controlling terminal.
        let mut child = spawn_on_pty(&program, &args, slave_fd)
            .map_err(|e| GatewayError::execution(anyhow!("spawn: {}", e)))?;

        // Channels for InteractiveHandle.
        let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(8);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();

        // Task 1: stdin_rx → PTY master write (independent lock from read).
        let mut w = master_write;
        let stdin_task = tokio::spawn(async move {
            while let Some(data) = stdin_rx.recv().await {
                if w.write_all(&data).await.is_err() {
                    break;
                }
                let _ = w.flush().await;
            }
        });

        // Task 2: PTY master read → stdout (independent lock from write).
        let mut r = master_read;
        let read_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match r.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if stdout_tx.send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        // Task 3: window resize via raw fd ioctl (no lock needed).
        let resize_task = tokio::spawn(async move {
            while let Some((cols, rows)) = resize_rx.recv().await {
                pty_resize(master_raw, cols, rows);
            }
        });

        // Task 4: wait for process exit.
        let wait_task = tokio::spawn(async move {
            let code = child
                .wait()
                .await
                .map(|s| s.code().unwrap_or(1))
                .unwrap_or(1);
            let _ = exit_tx.send(code);
        });

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
            abort_handles: vec![
                stdin_task.abort_handle(),
                read_task.abort_handle(),
                resize_task.abort_handle(),
                wait_task.abort_handle(),
            ],
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        Ok(vec![ServerListRow {
            source: ServerListSource::Local,
            server: crate::config::ServerEntry {
                alias: SELF_GATEWAY_NAME.to_string(),
                host: self.hostname.clone(),
                port: 0,
                user: self.user.clone(),
                auth: DirectAuth::None,
            },
        }])
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Localhost
    }

    fn name(&self) -> &str {
        SELF_GATEWAY_NAME
    }

    async fn prune_idle(&self) {}
}

// ---------------------------------------------------------------------------
// exec (non-interactive) — pipe mode
// ---------------------------------------------------------------------------

async fn exec_pipes(
    request: &ExecRequest,
    program: &str,
    args: &[String],
) -> Result<i32, GatewayError> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| GatewayError::execution(anyhow!("spawn: {}", e)))?;

    // stdout
    let stdout_sender = request.sender.clone();
    let mut stdout = child.stdout.take();
    let stdout_task = tokio::spawn(async move {
        if let Some(ref mut s) = stdout {
            let mut buf = vec![0u8; 8192];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if stdout_sender
                            .send(ServerEvent::Stdout {
                                data: buf[..n].to_vec(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });

    // stderr
    let stderr_sender = request.sender.clone();
    let mut stderr = child.stderr.take();
    let stderr_task = tokio::spawn(async move {
        if let Some(ref mut s) = stderr {
            let mut buf = vec![0u8; 8192];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if stderr_sender
                            .send(ServerEvent::Stderr {
                                data: buf[..n].to_vec(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });

    // stdin
    let stdin_task: Option<tokio::task::JoinHandle<()>> = if request.stdin {
        let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());
        if let Some(mut stdin_rx) = stdin_rx {
            let mut child_stdin = child.stdin.take();
            Some(tokio::spawn(async move {
                if let Some(ref mut sin) = child_stdin {
                    while let Some(data) = stdin_rx.recv().await {
                        if data.is_empty() {
                            break;
                        }
                        if sin.write_all(&data).await.is_err() {
                            break;
                        }
                        let _ = sin.flush().await;
                    }
                    let _ = sin.shutdown().await;
                }
            }))
        } else {
            drop(child.stdin.take());
            None
        }
    } else {
        drop(child.stdin.take());
        None
    };

    let exit_code = wait_child(&mut child, request).await?;
    let _ = stdout_task.await;
    let _ = stderr_task.await;
    // Abort stdin task — awaiting it would deadlock because the sender
    // is held by process_execute until this function returns.
    if let Some(t) = stdin_task {
        t.abort();
    }
    Ok(exit_code)
}

// ---------------------------------------------------------------------------
// exec (non-interactive) — PTY mode
// ---------------------------------------------------------------------------

async fn exec_pty(
    request: &ExecRequest,
    program: &str,
    args: &[String],
) -> Result<i32, GatewayError> {
    let (master_fd, slave_fd) = openpty_pair().map_err(GatewayError::execution)?;
    if request.cols > 0 && request.rows > 0 {
        pty_resize(slave_fd.as_raw_fd(), request.cols, request.rows);
    }

    let master_raw = master_fd.as_raw_fd();
    let read_fd = dup_fd(&master_fd).map_err(GatewayError::execution)?;
    let mut master_read = tokio::fs::File::from_std(std::fs::File::from(read_fd));
    let mut master_write = tokio::fs::File::from_std(std::fs::File::from(master_fd));

    let mut child = spawn_on_pty(program, args, slave_fd)
        .map_err(|e| GatewayError::execution(anyhow!("spawn: {}", e)))?;

    // PTY read → ServerEvent::Stdout.
    let stdout_sender = request.sender.clone();
    let read_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        loop {
            match master_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if stdout_sender
                        .send(ServerEvent::Stdout {
                            data: buf[..n].to_vec(),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // stdin → PTY write (if requested).
    let stdin_task = if request.stdin {
        let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());
        if let Some(mut stdin_rx) = stdin_rx {
            Some(tokio::spawn(async move {
                while let Some(data) = stdin_rx.recv().await {
                    if data.is_empty() {
                        break;
                    }
                    if master_write.write_all(&data).await.is_err() {
                        break;
                    }
                    let _ = master_write.flush().await;
                }
                // Send Ctrl-D to signal EOF to the shell.
                let _ = master_write.write_all(b"\x04").await;
            }))
        } else {
            None
        }
    } else {
        None
    };

    let exit_code = wait_child(&mut child, request).await?;
    let _ = read_task.await;
    if let Some(t) = stdin_task {
        t.abort();
    }
    let _ = master_raw; // kept for potential future resize support
    Ok(exit_code)
}

/// Wait for child exit with optional timeout.
async fn wait_child(
    child: &mut tokio::process::Child,
    request: &ExecRequest,
) -> Result<i32, GatewayError> {
    if request.timeout_ms > 0 {
        match tokio::time::timeout(
            std::time::Duration::from_millis(request.timeout_ms),
            child.wait(),
        )
        .await
        {
            Ok(Ok(status)) => Ok(status.code().unwrap_or(1)),
            Ok(Err(e)) => {
                let _ = child.kill().await;
                Err(GatewayError::execution(anyhow!("wait: {}", e)))
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = request.sender.send(ServerEvent::Stderr {
                    data: b"timed out\r\n".to_vec(),
                });
                Ok(124)
            }
        }
    } else {
        child
            .wait()
            .await
            .map_err(|e| GatewayError::execution(anyhow!("wait: {}", e)))?
            .code()
            .map(Ok)
            .unwrap_or(Ok(1))
    }
}
