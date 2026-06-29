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
    backend: Backend,
    exited: bool,
    /// Keeps the bastion transport lease alive for the session's lifetime.
    _transport_guard: Box<dyn Send>,
}

enum Backend {
    /// No backend started yet.
    None,
    /// `exec`: streaming stdout, then a synthetic exit code.
    Exec {
        stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        exit_rx: oneshot::Receiver<i32>,
        exit_seen: bool,
    },
    /// `shell`/`subsystem`: raw bidirectional passthrough.
    Raw {
        stdin_tx: mpsc::Sender<Vec<u8>>,
        stdout_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        exit_rx: oneshot::Receiver<i32>,
    },
}

impl JumpserverSession {
    /// Wrap a navigated `shell` (at the asset prompt). `transport_guard` keeps
    /// the bastion connection alive while this session is in use.
    pub(crate) fn new(shell: PtyShell, transport_guard: Box<dyn Send>) -> Self {
        Self {
            shell: Some(shell),
            cols: 80,
            rows: 24,
            backend: Backend::None,
            exited: false,
            _transport_guard: transport_guard,
        }
    }

    fn start_raw(&mut self) -> Result<()> {
        let shell = self
            .shell
            .take()
            .ok_or_else(|| anyhow::anyhow!("jumpserver session already started"))?;
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();
        tokio::spawn(async move {
            let code = shell.run_raw_passthrough(stdin_rx, stdout_tx).await;
            let _ = exit_tx.send(code);
        });
        self.backend = Backend::Raw {
            stdin_tx,
            stdout_rx,
            exit_rx,
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
        let command = command.to_string();
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();
        tokio::spawn(async move {
            // run_command_plain has no native exit code; report 0 on success,
            // 255 if the bastion PTY errored mid-command.
            let code = match shell.run_command_plain(&command, &stdout_tx).await {
                Ok(()) => 0,
                Err(_) => 255,
            };
            let _ = exit_tx.send(code);
        });
        self.backend = Backend::Exec {
            stdout_rx,
            exit_rx,
            exit_seen: false,
        };
        Ok(())
    }

    async fn shell(&mut self) -> Result<()> {
        self.start_raw()
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        if name != "sftp" {
            return Err(unsupported(&format!("jumpserver subsystem {name}")));
        }
        // Launch sftp-server on the asset in raw mode, then passthrough SFTP.
        if let Some(shell) = self.shell.as_mut() {
            shell
                .write_raw(format!("{SFTP_LAUNCH}\r").as_bytes())
                .await?;
        }
        self.start_raw()
    }

    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()> {
        if let Some(shell) = self.shell.as_mut() {
            shell.window_change(cols, rows).await;
        }
        Ok(())
    }

    async fn signal(&mut self, _signal: &str) -> Result<()> {
        Ok(())
    }

    async fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
        if let Backend::Raw { stdin_tx, .. } = &self.backend {
            stdin_tx
                .send(data.to_vec())
                .await
                .map_err(|_| anyhow::anyhow!("jumpserver session closed"))?;
        }
        // exec mode has no live stdin (run_command_plain); ignore.
        Ok(())
    }

    async fn eof(&mut self) -> Result<()> {
        // Dropping the stdin sender signals EOF to the raw passthrough.
        if let Backend::Raw { stdin_tx, .. } = &mut self.backend {
            let (closed_tx, _) = mpsc::channel::<Vec<u8>>(1);
            *stdin_tx = closed_tx;
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
