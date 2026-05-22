use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::AppConfig;
use crate::connection::{Connection, CopySpec, DirectSshConnection};
use crate::protocol::ServerEvent;

use super::{JumpHost, JumpHostKind};

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
    ) -> Result<i32> {
        self.inner.execute(argv, sender, config).await
    }

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        self.inner.copy(spec, config).await
    }

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Direct
    }

    fn alias(&self) -> &str {
        &self.alias
    }
}
