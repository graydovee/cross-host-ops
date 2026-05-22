use std::sync::Arc;

use anyhow::Result;

use crate::config::{AppConfig, JumpHostConfig, JumpHostFields};
use crate::connection::{AuthPrompter, DirectSshConnection, DirectTarget};
#[allow(deprecated)]
use crate::connection::ResolvedTarget;

use super::direct::DirectJumpHost;
use super::jumpserver::JumpserverJumpHost;
use super::rhopd::RhopdJumpHost;
use super::JumpHost;

/// Build a concrete [`JumpHost`] from a [`JumpHostConfig`] entry.
///
/// This is the **single point of extension** for new jump-host kinds. Adding a
/// future kind means: add the `JumpHostKind` variant, add the factory arm here,
/// and write the impl.
pub async fn build_jump_host(
    spec: &JumpHostConfig,
    auth_prompter: &Arc<AuthPrompter>,
    config: &AppConfig,
) -> Result<Box<dyn JumpHost>> {
    match &spec.fields {
        JumpHostFields::Direct(fields) => {
            let target = DirectTarget {
                host: fields.host.clone(),
                host_name: fields.host.clone(),
                port: fields.port,
                user: fields.user.clone(),
                auth: fields.auth.clone(),
                proxy_command: None,
                pubkey_accepted_algorithms: None,
            };
            let conn = DirectSshConnection::connect(&target, config, auth_prompter.as_ref()).await?;
            Ok(Box::new(DirectJumpHost::new(spec.alias.clone(), conn)))
        }

        JumpHostFields::Jumpserver(fields) => {
            #[allow(deprecated)]
            let target = ResolvedTarget {
                input: spec.alias.clone(),
                ip: fields.host.clone(),
                key: format!("{}:{}", fields.host, fields.port),
                transport: crate::connection::TargetTransport::Jump,
                direct: None,
                target_label: spec.alias.clone(),
            };
            let host = JumpserverJumpHost::connect(
                spec.alias.clone(),
                &target,
                config,
                auth_prompter.as_ref(),
            )
            .await?;
            Ok(Box::new(host))
        }

        JumpHostFields::Rhopd(fields) => {
            let host = RhopdJumpHost::connect(
                spec.alias.clone(),
                fields.address.clone(),
            )
            .await?;
            Ok(Box::new(host))
        }
    }
}
