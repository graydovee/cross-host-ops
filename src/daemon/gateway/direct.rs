// DirectGateway implementation.
//
// Manages direct SSH connections with per-endpoint connection pooling. The
// pooled resource is an authenticated russh `client::Handle` (plus its shared
// exit-code cell): the expensive TCP + SSH + auth handshake is performed once
// and reused, while each operation opens a fresh session channel on the handle.
// This is what makes a second `xho exec` to the same host fast.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use russh::client::Handle;
use tokio::sync::RwLock;
use tracing::debug;

use crate::config::{
    AppConfig, DirectAuth, list_server_entries, load_server_config, resolve_server_entry,
};
use crate::daemon::connection_manager::{ConnectionStatusSnapshot, ManagedPool, PoolLease};
use crate::daemon::session::TargetSession;
use crate::daemon::session::direct::{ClientHandler, DirectSshSession, connect_authenticated};
use crate::daemon::shell::{build_final_command, resolve_shell};
use crate::protocol::ServerListRow;
use crate::types::ServerListSource;

use super::auth::AuthPrompter;
use super::{Capabilities, Gateway, GatewayError, GatewayKind};

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A resolved target from server.toml.
struct ResolvedTarget {
    host: String,
    port: u16,
    user: String,
    auth: DirectAuth,
    shell: Option<String>,
    defaults_shell: String,
}

/// An authenticated, pooled SSH handle. New session channels are opened on it
/// for each operation; the handshake is paid once and reused.
pub(crate) struct PooledHandle {
    handle: Handle<ClientHandler>,
    exit_code: Arc<AtomicU32>,
}

impl PooledHandle {
    fn is_alive(&self) -> bool {
        !self.handle.is_closed()
    }
}

/// Pool identity for a direct SSH transport.
///
/// The auth fingerprint is part of equality/hash so different users or
/// credentials never share a transport by accident, but it is never surfaced
/// in labels/logs.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DirectPoolKey {
    host: String,
    port: u16,
    user: String,
    auth_kind: &'static str,
    auth_fingerprint: String,
}

impl DirectPoolKey {
    fn from_resolved(resolved: &ResolvedTarget) -> Self {
        let (auth_kind, auth_fingerprint) = match &resolved.auth {
            DirectAuth::Key { identity_file } => ("key", format!("key:{identity_file}")),
            DirectAuth::Password { password } => ("password", format!("password:{password}")),
            DirectAuth::None => ("none", "none".to_string()),
            DirectAuth::ReverseProxy => ("reverse_proxy", "none".to_string()),
        };
        Self {
            host: resolved.host.clone(),
            port: resolved.port,
            user: resolved.user.clone(),
            auth_kind,
            auth_fingerprint,
        }
    }

    fn label(&self) -> String {
        format!(
            "{}@{}:{} ({})",
            self.user, self.host, self.port, self.auth_kind
        )
    }
}

// ---------------------------------------------------------------------------
// DirectGateway
// ---------------------------------------------------------------------------

pub struct DirectGateway {
    gateway_name: String,
    config: Arc<RwLock<AppConfig>>,
    server_config_path: String,
    #[allow(dead_code)]
    auth_prompter: Arc<AuthPrompter>,
    /// Pool of authenticated SSH handles, keyed by endpoint + user + auth.
    pool: Arc<ManagedPool<DirectPoolKey, PooledHandle>>,
    max_connections_per_address: usize,
}

impl DirectGateway {
    /// Construct a new DirectGateway. No connections are established.
    pub fn new(
        gateway_name: String,
        config: Arc<RwLock<AppConfig>>,
        server_config_path: String,
        auth_prompter: Arc<AuthPrompter>,
        max_connections_per_address: usize,
        max_idle_time: Duration,
    ) -> Self {
        Self {
            gateway_name,
            config,
            server_config_path,
            auth_prompter,
            pool: Arc::new(ManagedPool::new(max_connections_per_address, max_idle_time)),
            max_connections_per_address,
        }
    }

    /// Resolve a target string to host, port, user, and auth credentials by
    /// looking it up in the server.toml configuration.
    async fn resolve_target(&self, target: &str) -> Result<ResolvedTarget, GatewayError> {
        let path = Path::new(&self.server_config_path);
        let server_config = load_server_config(path).map_err(|e| {
            GatewayError::resolution(anyhow!("failed to load server config: {}", e))
        })?;

        let server_host_config = server_config
            .servers
            .get(target)
            .ok_or_else(|| GatewayError::resolution(anyhow!("target '{}' not found", target)))?;

        let resolver = self
            .config
            .read()
            .await
            .secret_resolver(server_config.defaults.identity_file.as_deref());

        let entry = resolve_server_entry(
            target,
            server_host_config,
            &server_config.defaults,
            Some(&resolver),
        )
        .map_err(|e| {
            GatewayError::resolution(anyhow!("failed to resolve target '{}': {}", target, e))
        })?;

        Ok(ResolvedTarget {
            host: entry.host,
            port: entry.port,
            user: entry.user,
            auth: entry.auth,
            shell: server_host_config.shell.clone(),
            defaults_shell: server_config.defaults.shell.clone(),
        })
    }

    fn effective_shell(cli_shell: &str, no_shell: bool, resolved: &ResolvedTarget) -> String {
        let cli_shell = (!cli_shell.is_empty()).then_some(cli_shell);
        resolve_shell(
            cli_shell,
            no_shell,
            resolved.shell.as_deref(),
            &resolved.defaults_shell,
        )
        .unwrap_or_default()
    }

    /// Acquire (or create) a pooled handle lease for the resolved target,
    /// discarding a dead handle and retrying once.
    async fn checkout_handle(
        &self,
        resolved: &ResolvedTarget,
    ) -> Result<PoolLease<DirectPoolKey, PooledHandle>, GatewayError> {
        let key = DirectPoolKey::from_resolved(resolved);
        for attempt in 0..=1 {
            let key_for_error = key.clone();
            let lease = self
                .pool
                .checkout_or_create_with(key.clone(), || async {
                    let (handle, exit_code) = connect_authenticated(
                        &resolved.host,
                        resolved.port,
                        &resolved.user,
                        &resolved.auth,
                        &self.config.read().await.clone(),
                    )
                    .await
                    .map_err(|e| {
                        GatewayError::transport(anyhow!(
                            "failed to connect to {}: {}",
                            key_for_error.label(),
                            e
                        ))
                    })?;
                    Ok::<PooledHandle, GatewayError>(PooledHandle { handle, exit_code })
                })
                .await;

            let lease = match lease {
                Ok(lease) => lease,
                Err(e) if attempt == 0 && e.kind == super::ErrorKind::Transport => {
                    debug!(gateway = %self.gateway_name, target = %key.label(),
                        "transport error creating direct SSH handle, retrying: {}", e);
                    continue;
                }
                Err(e) => return Err(e),
            };

            if lease.resource().is_alive() {
                return Ok(lease);
            }
            debug!(gateway = %self.gateway_name, target = %key.label(),
                generation = %lease.generation(), "discarding stale direct SSH handle");
            self.pool.discard(lease);
            if attempt == 1 {
                return Err(GatewayError::transport(anyhow!(
                    "direct SSH handle for {} is not alive after refresh",
                    key.label()
                )));
            }
        }
        unreachable!("direct handle checkout loop is bounded")
    }

    /// Open a session channel on a pooled handle and wrap it as a
    /// `DirectSshSession`. The handle lease is returned to the pool (or
    /// discarded if dead) when the session's driver task terminates.
    async fn open_pooled_session(
        &self,
        resolved: &ResolvedTarget,
    ) -> Result<Box<dyn TargetSession>, GatewayError> {
        let lease = self.checkout_handle(resolved).await?;
        let channel = match lease.resource().handle.channel_open_session().await {
            Ok(channel) => channel,
            Err(e) => {
                self.pool.discard(lease);
                return Err(GatewayError::transport(anyhow!(
                    "failed to open direct SSH channel: {}",
                    e
                )));
            }
        };
        let exit_code = lease.resource().exit_code.clone();
        let pool = self.pool.clone();
        let on_done = Box::new(move || {
            if lease.resource().is_alive() {
                pool.return_healthy(lease);
            } else {
                pool.discard(lease);
            }
        });
        Ok(Box::new(DirectSshSession::new(channel, exit_code, on_done)) as Box<dyn TargetSession>)
    }
}

// ---------------------------------------------------------------------------
// Gateway trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Gateway for DirectGateway {
    fn name(&self) -> &str {
        &self.gateway_name
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Direct
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EXEC | Capabilities::COPY | Capabilities::PROXY | Capabilities::LIST
    }

    async fn open_exec_session(
        &self,
        target: &str,
        argv: &[String],
        shell: &str,
        no_shell: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError> {
        let resolved = self.resolve_target(target).await?;
        let eff_shell = Self::effective_shell(shell, no_shell, &resolved);
        let command = build_final_command(argv, &eff_shell);
        let session = self.open_pooled_session(&resolved).await?;
        Ok((session, command))
    }

    async fn open_session(&self, target: &str) -> Result<Box<dyn TargetSession>, GatewayError> {
        let resolved = self.resolve_target(target).await?;
        self.open_pooled_session(&resolved).await
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let path = Path::new(&self.server_config_path);
        let entries = list_server_entries(path)
            .map_err(|e| GatewayError::resolution(anyhow!("failed to list servers: {}", e)))?;
        let rows = entries
            .into_iter()
            .map(|server| ServerListRow {
                source: ServerListSource::Local,
                server,
            })
            .collect();
        Ok(rows)
    }

    async fn pool_status(&self) -> Vec<ConnectionStatusSnapshot> {
        self.pool.status_snapshot_with(|key| key.label())
    }

    async fn prune_idle(&self) {
        self.pool.prune_idle_with(|conn| conn.is_alive());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved_with_auth(auth: DirectAuth) -> ResolvedTarget {
        ResolvedTarget {
            host: "127.0.0.1".to_string(),
            port: 22,
            user: "admin".to_string(),
            auth,
            shell: None,
            defaults_shell: String::new(),
        }
    }

    #[test]
    fn effective_shell_uses_cli_server_defaults_precedence() {
        let mut resolved = resolved_with_auth(DirectAuth::Password {
            password: "one".to_string(),
        });
        resolved.shell = Some("zsh".to_string());
        resolved.defaults_shell = "bash".to_string();

        assert_eq!(DirectGateway::effective_shell("", false, &resolved), "zsh");
        assert_eq!(
            DirectGateway::effective_shell("fish", false, &resolved),
            "fish"
        );
        assert_eq!(DirectGateway::effective_shell("", true, &resolved), "");

        resolved.shell = None;
        assert_eq!(DirectGateway::effective_shell("", false, &resolved), "bash");
    }

    #[test]
    fn direct_pool_key_includes_user_and_auth_identity() {
        let password_key =
            DirectPoolKey::from_resolved(&resolved_with_auth(DirectAuth::Password {
                password: "one".to_string(),
            }));
        let different_password_key =
            DirectPoolKey::from_resolved(&resolved_with_auth(DirectAuth::Password {
                password: "two".to_string(),
            }));
        let key_auth = DirectPoolKey::from_resolved(&resolved_with_auth(DirectAuth::Key {
            identity_file: "~/.ssh/id_rsa".to_string(),
        }));
        let different_user_key = DirectPoolKey::from_resolved(&ResolvedTarget {
            user: "root".to_string(),
            ..resolved_with_auth(DirectAuth::Password {
                password: "one".to_string(),
            })
        });

        assert_ne!(password_key, different_password_key);
        assert_ne!(password_key, key_auth);
        assert_ne!(password_key, different_user_key);
    }

    #[test]
    fn direct_pool_key_label_does_not_expose_auth_secret() {
        let key = DirectPoolKey::from_resolved(&resolved_with_auth(DirectAuth::Password {
            password: "very-secret".to_string(),
        }));
        let label = key.label();

        assert!(label.contains("admin@127.0.0.1:22"));
        assert!(label.contains("password"));
        assert!(!label.contains("very-secret"));
    }

    #[tokio::test]
    async fn real_gateway_pool_starts_empty_with_configured_capacity() {
        let config = Arc::new(RwLock::new(AppConfig::default()));
        let auth_prompter: Arc<AuthPrompter> = Arc::new(|_| Box::pin(async { Ok(String::new()) }));
        let gateway = DirectGateway::new(
            "test".to_string(),
            config,
            "/nonexistent/server.toml".to_string(),
            auth_prompter,
            3,
            Duration::from_secs(300),
        );

        assert_eq!(gateway.max_connections_per_address, 3);
        assert!(
            gateway
                .pool
                .status_snapshot_with(|key| key.label())
                .is_empty()
        );
    }
}
