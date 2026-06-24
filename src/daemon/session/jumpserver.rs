// JumpserverSession — a `TargetSession` backed by a menu-driven bastion.
//
// Jumpserver is a third-party gateway whose model is "connect → navigate an
// interactive asset menu → get a PTY shell on the chosen asset". It is not
// session-channel-shaped (no clean one-shot exec or sftp subsystem), so this
// implementation reuses the existing, proven menu/TOTP/asset-selection engine
// (`JumpserverGateway::exec_interactive`) and adapts its `InteractiveHandle`
// to the `TargetSession` contract:
//   - `shell()` / `exec(cmd)` → drive the menu to the asset shell (exec runs the
//     command inside it).
//   - pty params / data / resize are forwarded to the asset shell.
//   - `subsystem` returns `Unsupported` (bastions don't expose sftp directly).
//
// This unifies jumpserver behaviour behind the same `TargetSession` abstraction
// the proxy, tunnel, and CLI exec/copy use, without rewriting the menu engine.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use russh::Pty;
use tokio::sync::mpsc;

use crate::protocol::ServerEvent;

use crate::daemon::gateway::{Gateway, InteractiveHandle, InteractiveRequest};
use super::{SessionEvent, TargetSession, unsupported};

pub(crate) struct JumpserverSession {
    gateway: Arc<dyn Gateway>,
    target: String,
    cols: u32,
    rows: u32,
    handle: Option<InteractiveHandle>,
    exited: bool,
}

impl JumpserverSession {
    pub(crate) fn new(gateway: Arc<dyn Gateway>, target: String) -> Self {
        Self {
            gateway,
            target,
            cols: 80,
            rows: 24,
            handle: None,
            exited: false,
        }
    }

    /// Drive the bastion menu to the asset shell. `argv` empty → interactive
    /// shell; non-empty → run the command inside the asset shell.
    async fn start(&mut self, argv: Vec<String>) -> Result<()> {
        let (sender, _rx) = mpsc::unbounded_channel::<ServerEvent>();
        let request = InteractiveRequest {
            argv,
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
        self.handle = Some(handle);
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
        self.start(vec![command.to_string()]).await
    }

    async fn shell(&mut self) -> Result<()> {
        self.start(Vec::new()).await
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        Err(unsupported(&format!("jumpserver subsystem {name}")))
    }

    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()> {
        if let Some(handle) = self.handle.as_ref() {
            let _ = handle.resize_tx.send((cols, rows)).await;
        }
        Ok(())
    }

    async fn signal(&mut self, _signal: &str) -> Result<()> {
        // The bastion engine has no signal channel; best-effort no-op.
        Ok(())
    }

    async fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| anyhow!("jumpserver session not started"))?;
        handle
            .stdin_tx
            .send(data.to_vec())
            .await
            .map_err(|_| anyhow!("jumpserver session closed"))?;
        Ok(())
    }

    async fn eof(&mut self) -> Result<()> {
        // stdin EOF is signalled when the underlying handle is dropped; no-op here.
        Ok(())
    }

    async fn next_event(&mut self) -> Option<SessionEvent> {
        if self.exited {
            return None;
        }
        let handle = self.handle.as_mut()?;
        loop {
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
    }
}
