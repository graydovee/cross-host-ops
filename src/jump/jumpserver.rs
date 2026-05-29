use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::{AppConfig, JumpserverJumpHostFields};
use crate::connection::types::CopySpec;
use crate::connection::{AuthPrompter, Connection, JumpSshConnection};
use crate::protocol::ServerEvent;

use super::{JumpHost, JumpHostKind};

/// Wraps the existing `JumpSshConnection` (interactive jumpserver shell with
/// MFA/menu navigation) behind the unified `JumpHost` trait.
///
/// `tui_shell` and `list_servers` fall through to the trait defaults, which
/// return `UnsupportedCapability`.
pub struct JumpserverJumpHost {
    name: String,
    inner: JumpSshConnection,
}

impl JumpserverJumpHost {
    /// Connect through the jumpserver to the given target, performing MFA and
    /// menu selection as needed.
    pub async fn connect(
        name: String,
        target_label: &str,
        fields: &JumpserverJumpHostFields,
        config: &AppConfig,
        auth_prompter: &AuthPrompter,
    ) -> Result<Self> {
        let inner =
            JumpSshConnection::connect(target_label, fields, config, auth_prompter).await?;
        Ok(Self {
            name,
            inner,
        })
    }
}

#[async_trait]
impl JumpHost for JumpserverJumpHost {
    async fn exec(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
        pty: bool,
    ) -> Result<i32> {
        self.inner.execute(argv, sender, config, pty).await
    }

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        self.inner.copy(spec, config).await
    }

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Jumpserver
    }

    fn name(&self) -> &str {
        &self.name
    }
}
