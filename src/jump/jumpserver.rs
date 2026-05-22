use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::AppConfig;
use crate::connection::types::CopySpec;
#[allow(deprecated)]
use crate::connection::{AuthPrompter, Connection, JumpSshConnection, ResolvedTarget};
use crate::protocol::ServerEvent;

use super::{JumpHost, JumpHostKind};

/// Wraps the existing `JumpSshConnection` (interactive jumpserver shell with
/// MFA/menu navigation) behind the unified `JumpHost` trait.
///
/// `tui_shell` and `list_servers` fall through to the trait defaults, which
/// return `UnsupportedCapability`.
pub struct JumpserverJumpHost {
    alias: String,
    inner: JumpSshConnection,
}

#[allow(deprecated)]
impl JumpserverJumpHost {
    /// Connect through the jumpserver to the given target, performing MFA and
    /// menu selection as needed.
    pub async fn connect(
        alias: String,
        target: &ResolvedTarget,
        config: &AppConfig,
        auth_prompter: &AuthPrompter,
    ) -> Result<Self> {
        let inner = JumpSshConnection::connect(target, config, auth_prompter).await?;
        Ok(Self { alias, inner })
    }
}

#[async_trait]
impl JumpHost for JumpserverJumpHost {
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
        JumpHostKind::Jumpserver
    }

    fn alias(&self) -> &str {
        &self.alias
    }
}
