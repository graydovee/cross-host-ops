pub mod direct;
pub mod jump;
pub mod resolver;
mod shared;
pub mod types;

use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::AppConfig;
use crate::protocol::ServerEvent;

pub use direct::DirectSshConnection;
pub use jump::JumpSshConnection;
pub use resolver::{Resolver, derive_target_ip};
pub use crate::config::JumpHostConfig;
pub use shared::{build_remote_command, shell_quote};
pub use types::{CopyDirection, CopySpec, DirectTarget};

pub type AuthFuture = Pin<Box<dyn Future<Output = Result<String>> + Send>>;
pub type AuthPrompter = dyn Fn(AuthPromptRequest) -> AuthFuture + Send + Sync;

#[derive(Clone, Debug)]
pub struct AuthPromptRequest {
    pub target_label: String,
    pub kind: String,
    pub message: String,
    pub secret: bool,
}

#[tonic::async_trait]
pub trait Connection: Send {
    async fn execute(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
        pty: bool,
    ) -> Result<i32>;

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()>;
}
