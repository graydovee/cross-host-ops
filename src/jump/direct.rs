use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::AppConfig;
use crate::connection::{Connection, CopySpec, DirectSshConnection};
use crate::protocol::ServerEvent;

use super::{InteractiveHandle, JumpHost, JumpHostKind};

/// A [`JumpHost`] wrapper around the existing [`DirectSshConnection`].
///
/// `exec` and `copy` delegate directly to the inner connection.
/// `tui_shell` and `list_servers` fall through to the default
/// [`UnsupportedCapability`](super::UnsupportedCapability) error.
pub struct DirectJumpHost {
    alias: String,
    inner: DirectSshConnection,
}

impl DirectJumpHost {
    pub fn new(alias: String, inner: DirectSshConnection) -> Self {
        Self { alias, inner }
    }
}

#[async_trait]
impl JumpHost for DirectJumpHost {
    async fn exec(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
        pty: bool,
        cols: u32,
        rows: u32,
        shell: &str,
    ) -> Result<i32> {
        self.inner.execute(argv, sender, config, pty, cols, rows, shell).await
    }

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        self.inner.copy(spec, config).await
    }

    async fn exec_interactive(
        &mut self,
        argv: &[String],
        cols: u32,
        rows: u32,
        _sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
        shell: &str,
    ) -> Result<InteractiveHandle> {
        let session = self.inner.execute_interactive(argv, cols, rows, config, shell).await?;
        Ok(InteractiveHandle {
            stdin_tx: session.stdin_tx,
            resize_tx: session.resize_tx,
            stdout_rx: session.stdout_rx,
            exit_rx: session.exit_rx,
        })
    }

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Direct
    }

    fn name(&self) -> &str {
        &self.alias
    }
}
