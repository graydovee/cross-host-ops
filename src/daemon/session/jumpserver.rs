// JumpserverSession — a `TargetSession` backed by a menu-driven bastion.
//
// Jumpserver is a third-party gateway whose model is "connect → navigate an
// interactive asset menu → get a PTY shell on the chosen asset". It is not
// session-channel-shaped, so this implementation reuses the existing, proven
// menu/TOTP/asset-selection engine on `JumpserverGateway`:
//   - `exec(argv)` → `gateway.exec` (now sentinel-free `run_command_plain`:
//     streams stdout until the asset prompt, returns 0 — no exit code, by
//     design, since a menu bastion's PTY has no native exec/exit-status).
//   - `shell()`   → `gateway.exec_interactive` (interactive asset shell).
//   - pty params / data / resize forward to the active backend.
//   - `subsystem` returns `Unsupported` (bastions expose no sftp directly).
//
// The raw argv is stored (not a pre-built command string) so `gateway.exec`
// constructs the command exactly as the legacy path did (no double-quoting).

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use russh::Pty;
use tokio::sync::mpsc;
use tracing::warn;

use crate::protocol::ServerEvent;

use crate::daemon::gateway::{ExecRequest, Gateway, GatewayError, InteractiveHandle, InteractiveRequest};
use super::{SessionEvent, TargetSession, unsupported};

pub(crate) struct JumpserverSession {
    gateway: Arc<dyn Gateway>,
    target: String,
    argv: Vec<String>,
    cols: u32,
    rows: u32,
    // Non-interactive exec backend (sentinel-free gateway.exec).
    exec_rx: Option<mpsc::UnboundedReceiver<ServerEvent>>,
    exec_join: Option<tokio::task::JoinHandle<Result<i32, GatewayError>>>,
    // Interactive shell backend.
    shell: Option<InteractiveHandle>,
    exited: bool,
}

impl JumpserverSession {
    pub(crate) fn new(gateway: Arc<dyn Gateway>, target: String, argv: Vec<String>) -> Self {
        Self {
            gateway,
            target,
            argv,
            cols: 80,
            rows: 24,
            exec_rx: None,
            exec_join: None,
            shell: None,
            exited: false,
        }
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
        // CLI path stores the raw argv tokens (preserves multi-arg quoting); the
        // proxy/tunnel path has only a command string, so use [command]. gateway.exec
        // is sentinel-free (run_command_plain): streams stdout, returns 0.
        let argv = if !self.argv.is_empty() {
            self.argv.clone()
        } else {
            vec![command.to_string()]
        };
        let (sender, stdout_rx) = mpsc::unbounded_channel::<ServerEvent>();
        let request = ExecRequest {
            argv,
            sender,
            tty: false,
            tty_intent: crate::types::FlagIntent::Default,
            cols: self.cols,
            rows: self.rows,
            shell: String::new(),
            no_shell: false,
            timeout_ms: 0,
            // No live stdin on this path (run_command_plain doesn't forward it);
            // stdin=false + None avoids collect_stdin_payload blocking forever.
            stdin: false,
            stdin_intent: crate::types::FlagIntent::Default,
            stdin_rx: std::sync::Mutex::new(None),
        };
        let gateway = self.gateway.clone();
        let target = self.target.clone();
        let join = tokio::spawn(async move { gateway.exec(&target, &request).await });
        self.exec_rx = Some(stdout_rx);
        self.exec_join = Some(join);
        Ok(())
    }

    async fn shell(&mut self) -> Result<()> {
        let (sender, _rx) = mpsc::unbounded_channel::<ServerEvent>();
        let request = InteractiveRequest {
            argv: Vec::new(),
            cols: self.cols,
            rows: self.rows,
            sender,
            shell: String::new(),
            no_shell: false,
        };
        let handle = self
            .gateway
            .exec_interactive(&self.target, &request)
            .await
            .map_err(|e| anyhow!("jumpserver: {}", e.user_message()))?;
        self.shell = Some(handle);
        Ok(())
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        Err(unsupported(&format!("jumpserver subsystem {name}")))
    }

    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()> {
        if let Some(handle) = self.shell.as_ref() {
            let _ = handle.resize_tx.send((cols, rows)).await;
        }
        Ok(())
    }

    async fn signal(&mut self, _signal: &str) -> Result<()> {
        Ok(())
    }

    async fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
        if let Some(handle) = self.shell.as_ref() {
            handle
                .stdin_tx
                .send(data.to_vec())
                .await
                .map_err(|_| anyhow!("jumpserver session closed"))?;
            return Ok(());
        }
        // exec mode has no live stdin forwarding (run_command_plain); ignore.
        Ok(())
    }

    async fn eof(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<SessionEvent> {
        if self.exited {
            return None;
        }

        // Non-interactive exec backend: stream stdout/stderr, then exit code.
        if let Some(rx) = self.exec_rx.as_mut() {
            loop {
                match rx.recv().await {
                    Some(ServerEvent::Stdout { data }) => return Some(SessionEvent::Stdout(data)),
                    Some(ServerEvent::Stderr { data }) => return Some(SessionEvent::Stderr(data)),
                    Some(_) => continue,
                    None => {
                        self.exec_rx = None;
                        self.exited = true;
                        let join = self.exec_join.take()?;
                        let code = match join.await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                warn!(error = %e, "jumpserver exec failed");
                                255
                            }
                            Err(_) => 255,
                        };
                        return Some(SessionEvent::ExitStatus(code));
                    }
                }
            }
        }

        // Interactive shell backend.
        if let Some(handle) = self.shell.as_mut() {
            tokio::select! {
                data = handle.stdout_rx.recv() => match data {
                    Some(data) => return Some(SessionEvent::Stdout(data)),
                    None => {
                        self.exited = true;
                        return Some(SessionEvent::Eof);
                    }
                },
                code = &mut handle.exit_rx => {
                    self.exited = true;
                    return Some(SessionEvent::ExitStatus(code.unwrap_or(0)));
                }
            }
        }

        None
    }
}
