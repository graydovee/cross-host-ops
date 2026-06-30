// JumpserverSession — a `TargetSession` backed by a navigated bastion PTY.
//
// The gateway hands this session a `PtyShell` already navigated to the asset
// shell prompt (plus a transport keepalive guard). From there:
//   - `exec(command)` streams stdout until the asset prompt reappears and
//     reports exit 0 (a menu bastion PTY exposes no native exit status);
//   - `shell()` switches the PTY to raw bidirectional passthrough;
//   - `subsystem("sftp")` launches `sftp-server` on the asset in raw mode and
//     bridges SFTP bytes — giving `xho cp` the same sftp path as every backend.
//
// All driving runs in a spawned task; trait methods are thin senders and
// `next_event` pulls from the active backend.

use anyhow::Result;
use async_trait::async_trait;
use russh::Pty;
use tokio::sync::{mpsc, oneshot};

use crate::daemon::jumpserver_engine::PtyShell;

use super::{SessionEvent, TargetSession, unsupported};

/// Shell snippet that locates `sftp-server`, switches the PTY to raw mode, and
/// execs the server so SFTP framing passes through untranslated.
const SFTP_LAUNCH: &str = "P=$(command -v sftp-server 2>/dev/null || \
for c in /usr/lib/openssh/sftp-server /usr/libexec/openssh/sftp-server \
/usr/libexec/sftp-server /usr/lib/ssh/sftp-server; do [ -x \"$c\" ] && echo \"$c\" && break; done); \
stty raw -echo 2>/dev/null; exec \"$P\"";

pub(crate) struct JumpserverSession {
    /// The navigated shell, taken when a backend starts.
    shell: Option<PtyShell>,
    cols: u32,
    rows: u32,
    /// True when `request_pty` was explicitly called (interactive mode).
    /// Drives the exec() branch: interactive → raw passthrough; non-interactive
    /// → prompt-based exec with optional stdin forwarding.
    pty_requested: bool,
    backend: Backend,
    exited: bool,
    /// Keeps the bastion transport lease alive for the session's lifetime.
    _transport_guard: Box<dyn Send>,
    /// Called with the surviving `PtyShell` when `exec` succeeds (the shell is
    /// still at the asset prompt). The gateway uses this to return the shell to
    /// the session cache for reuse. `None` after `start_raw` — raw passthrough
    /// (interactive shell / sftp subsystem) closes the channel, so the shell is
    /// not reusable.
    return_shell: Option<Box<dyn FnOnce(PtyShell) + Send>>,
}

enum Backend {
    /// No backend started yet.
    None,
    /// `exec` (non-interactive): streaming stdout with optional stdin
    /// forwarding, then a synthetic exit code.
    Exec {
        stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        exit_rx: oneshot::Receiver<i32>,
        exit_seen: bool,
        stdin_tx: mpsc::Sender<Vec<u8>>,
    },
    /// `shell`/`subsystem`/interactive-`exec`: raw bidirectional passthrough
    /// with dynamic terminal resize support.
    Raw {
        stdin_tx: mpsc::Sender<Vec<u8>>,
        stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        exit_rx: oneshot::Receiver<i32>,
        resize_tx: mpsc::Sender<(u32, u32)>,
    },
}

impl JumpserverSession {
    /// Wrap a navigated `shell` (at the asset prompt). `transport_guard` keeps
    /// the bastion connection alive while this session is in use. `return_shell`
    /// is invoked with the surviving shell when `exec` completes successfully,
    /// allowing the gateway to cache it for reuse.
    pub(crate) fn new(
        shell: PtyShell,
        transport_guard: Box<dyn Send>,
        return_shell: Option<Box<dyn FnOnce(PtyShell) + Send>>,
    ) -> Self {
        Self {
            shell: Some(shell),
            cols: 80,
            rows: 24,
            pty_requested: false,
            backend: Backend::None,
            exited: false,
            _transport_guard: transport_guard,
            return_shell,
        }
    }

    fn start_raw(&mut self, scan_sentinel: bool) -> Result<()> {
        let shell = self
            .shell
            .take()
            .ok_or_else(|| anyhow::anyhow!("jumpserver session already started"))?;
        // Raw passthrough (interactive shell / sftp subsystem) consumes the
        // channel — it is closed when passthrough ends. The shell cannot be
        // returned to the cache.
        self.return_shell = None;
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();
        let (resize_tx, resize_rx) = mpsc::channel::<(u32, u32)>(8);
        tokio::spawn(async move {
            let code = shell
                .run_raw_passthrough(stdin_rx, stdout_tx, resize_rx, scan_sentinel)
                .await;
            let _ = exit_tx.send(code);
        });
        self.backend = Backend::Raw {
            stdin_tx,
            stdout_rx,
            exit_rx,
            resize_tx,
        };
        Ok(())
    }
}

#[async_trait]
impl TargetSession for JumpserverSession {
    async fn request_pty(
        &mut self,
        _term: &str,
        cols: u32,
        rows: u32,
        _modes: &[(Pty, u32)],
    ) -> Result<()> {
        if cols > 0 {
            self.cols = cols;
        }
        if rows > 0 {
            self.rows = rows;
        }
        self.pty_requested = true;
        Ok(())
    }

    async fn set_env(&mut self, _key: &str, _value: &str) -> Result<()> {
        Ok(())
    }

    async fn exec(&mut self, command: &str) -> Result<()> {
        let mut shell = self
            .shell
            .take()
            .ok_or_else(|| anyhow::anyhow!("jumpserver session already started"))?;

        if self.pty_requested {
            // Interactive exec (-it): resize PTY to the user's terminal size,
            // reset terminal settings (navigation left stty -echo), then write
            // the command with an exit-code sentinel and switch to raw
            // bidirectional passthrough with sentinel scanning.
            //
            // stty sane restores echo/icanon so bash/vim display typed input.
            // The sentinel lets the passthrough detect command exit, capture
            // the exit code, and close the PTY cleanly.
            shell.window_change(self.cols, self.rows).await;
            shell.write_line("stty sane").await?;
            shell.wait_for_prompt().await?;
            shell.clear_pending();
            shell
                .write_line(&format!(
                    "{command}{}",
                    crate::daemon::jumpserver_engine::SENTINEL_SUFFIX
                ))
                .await?;
            shell.clear_pending();
            self.shell = Some(shell);
            return self.start_raw(true);
        }

        // Non-interactive exec: prompt-based execution with optional stdin
        // forwarding (for `xho exec -i`). run_command_plain wraps the command
        // with the exit-code sentinel internally and returns the real exit
        // code. On success the shell is still at the asset prompt — return it
        // to the cache for reuse.
        let return_fn = self.return_shell.take();
        let command = command.to_string();
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();
        let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        tokio::spawn(async move {
            // Buffer all stdin data until EOF.
            let mut stdin_data = Vec::new();
            while let Some(chunk) = stdin_rx.recv().await {
                stdin_data.extend_from_slice(&chunk);
            }

            let code = if stdin_data.is_empty() {
                // No stdin — run the command directly.
                match shell.run_command_plain(&command, &stdout_tx).await {
                    Ok(code) => {
                        if let Some(f) = return_fn {
                            f(shell);
                        }
                        code
                    }
                    Err(_) => 255,
                }
            } else {
                // Pipe stdin data via base64 to avoid Ctrl+D issues on the
                // bastion PTY. printf avoids heredoc's secondary prompt (PS2).
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD.encode(&stdin_data);
                let piped = format!("printf '%s' '{}' | base64 -d | {}", encoded, command);
                match shell.run_command_plain(&piped, &stdout_tx).await {
                    Ok(code) => {
                        if let Some(f) = return_fn {
                            f(shell);
                        }
                        code
                    }
                    Err(_) => 255,
                }
            };
            let _ = exit_tx.send(code);
        });
        self.backend = Backend::Exec {
            stdout_rx,
            exit_rx,
            exit_seen: false,
            stdin_tx,
        };
        Ok(())
    }

    async fn shell(&mut self) -> Result<()> {
        self.start_raw(false)
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        if name != "sftp" {
            return Err(unsupported(&format!("jumpserver subsystem {name}")));
        }
        // Launch sftp-server on the asset in raw mode, then passthrough SFTP.
        if let Some(shell) = self.shell.as_mut() {
            // Clear any leftover data (e.g. shell prompt from a prior exec on
            // a cached shell) so it doesn't corrupt the SFTP byte stream.
            shell.clear_pending();
            shell
                .write_raw(format!("{SFTP_LAUNCH}\r").as_bytes())
                .await?;
        }
        self.start_raw(false)
    }

    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()> {
        match &self.backend {
            Backend::Raw { resize_tx, .. } => {
                let _ = resize_tx.send((cols, rows)).await;
            }
            _ => {
                // Pre-exec: apply directly to the shell.
                if let Some(shell) = self.shell.as_mut() {
                    shell.window_change(cols, rows).await;
                }
            }
        }
        Ok(())
    }

    async fn signal(&mut self, _signal: &str) -> Result<()> {
        Ok(())
    }

    async fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
        match &self.backend {
            Backend::Raw { stdin_tx, .. } | Backend::Exec { stdin_tx, .. } => {
                stdin_tx
                    .send(data.to_vec())
                    .await
                    .map_err(|_| anyhow::anyhow!("jumpserver session closed"))?;
            }
            Backend::None => {}
        }
        Ok(())
    }

    async fn eof(&mut self) -> Result<()> {
        // Dropping the stdin sender signals EOF to the spawned task.
        // For Exec mode, the task receives None on stdin_rx and sends Ctrl+D.
        // For Raw mode, the task receives None and sends channel eof.
        match &mut self.backend {
            Backend::Raw { stdin_tx, .. } | Backend::Exec { stdin_tx, .. } => {
                let (closed_tx, _) = mpsc::channel::<Vec<u8>>(1);
                *stdin_tx = closed_tx;
            }
            Backend::None => {}
        }
        Ok(())
    }

    async fn next_event(&mut self) -> Option<SessionEvent> {
        if self.exited {
            return None;
        }
        match &mut self.backend {
            Backend::None => None,
            Backend::Exec {
                stdout_rx,
                exit_rx,
                exit_seen,
                ..
            } => {
                if let Some(data) = stdout_rx.recv().await {
                    return Some(SessionEvent::Stdout(data));
                }
                // Stdout drained: emit the exit code once, then end.
                if *exit_seen {
                    self.exited = true;
                    return None;
                }
                *exit_seen = true;
                let code = (&mut *exit_rx).await.unwrap_or(255);
                Some(SessionEvent::ExitStatus(code))
            }
            Backend::Raw {
                stdout_rx, exit_rx, ..
            } => {
                tokio::select! {
                    data = stdout_rx.recv() => match data {
                        Some(data) => Some(SessionEvent::Stdout(data)),
                        None => {
                            self.exited = true;
                            Some(SessionEvent::Eof)
                        }
                    },
                    code = &mut *exit_rx => {
                        self.exited = true;
                        Some(SessionEvent::ExitStatus(code.unwrap_or(0)))
                    }
                }
            }
        }
    }
}
