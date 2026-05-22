use std::sync::Arc;

use anyhow::Result;

use crate::config::{AppConfig, JumpHostConfig, JumpHostFields};
use crate::connection::{AuthPrompter, DirectSshConnection, DirectTarget};

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
    target_label: &str,
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
            Ok(Box::new(DirectJumpHost::new(spec.name.clone(), conn)))
        }

        JumpHostFields::Jumpserver(fields) => {
            let host = JumpserverJumpHost::connect(
                spec.name.clone(),
                target_label,
                fields,
                config,
                auth_prompter.as_ref(),
            )
            .await?;
            Ok(Box::new(host))
        }

        JumpHostFields::Rhopd(fields) => {
            let host = RhopdJumpHost::connect(
                spec.name.clone(),
                fields.address.clone(),
            )
            .await?;
            Ok(Box::new(host))
        }
    }
}
