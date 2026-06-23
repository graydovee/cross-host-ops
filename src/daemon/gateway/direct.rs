// DirectGateway implementation.
// Manages direct SSH connections with per-address connection pooling.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::debug;

use crate::config::{
    AppConfig, DirectAuth, list_server_entries, load_server_config, resolve_server_entry,
};
use crate::protocol::ServerListRow;
use crate::types::{CopySpec, ServerListSource};

use super::auth::AuthPrompter;
use super::{
    ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle, InteractiveRequest,
    is_transport_error,
};
use crate::daemon::connection::direct::DirectConnection;
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest, InteractiveRequest as ConnInteractiveRequest,
};
use crate::daemon::connection_manager::{ManagedPool, PoolLease};

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

/// Pool identity for a direct SSH transport.
///
/// The auth fingerprint is intentionally not exposed in labels/logs, but it is
/// part of equality/hash so different users or credentials never share a
/// transport by accident.
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
    /// Direct SSH pool keyed by endpoint, user, and auth identity.
    pool: Arc<ManagedPool<DirectPoolKey, DirectConnection>>,
    #[allow(dead_code)]
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

    /// Resolve a target string to host, port, user, and auth credentials
    /// by looking it up in the server.toml configuration.
    async fn resolve_target(&self, target: &str) -> Result<ResolvedTarget, GatewayError> {
        let path = Path::new(&self.server_config_path);
        let server_config = load_server_config(path).map_err(|e| {
            GatewayError::resolution(anyhow!("failed to load server config: {}", e))
        })?;

        let server_host_config = server_config
            .servers
            .get(target)
            .ok_or_else(|| GatewayError::resolution(anyhow!("target '{}' not found", target)))?;

        // Build a secret resolver so a `vault:`/`env:`/`file:` password can be
        // resolved at connect time. The vault key source falls back to
        // server.toml's [defaults].identity_file when [secret].key_source is
        // unset.
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

    fn effective_shell(request_shell: &str, no_shell: bool, resolved: &ResolvedTarget) -> String {
        let cli_shell = (!request_shell.is_empty()).then_some(request_shell);
        crate::daemon::connection::shared::resolve_shell(
            cli_shell,
            no_shell,
            resolved.shell.as_deref(),
            &resolved.defaults_shell,
        )
        .unwrap_or_default()
    }

    /// Create a new DirectConnection to the resolved target.
    async fn create_connection(&self, resolved: &ResolvedTarget) -> Result<DirectConnection> {
        let config = self.config.read().await;
        DirectConnection::connect(
            &resolved.host,
            resolved.port,
            &resolved.user,
            &resolved.auth,
            &config,
            None,
        )
        .await
    }

    /// Acquire or create a connection for the given target.
    async fn get_connection(
        &self,
        resolved: &ResolvedTarget,
    ) -> Result<PoolLease<DirectPoolKey, DirectConnection>, GatewayError> {
        let key = DirectPoolKey::from_resolved(resolved);
        for attempt in 0..=1 {
            let key_for_error = key.clone();
            let checkout_result = self
                .pool
                .checkout_or_create_with(key.clone(), || async {
                    self.create_connection(resolved).await.map_err(|e| {
                        GatewayError::transport(anyhow!(
                            "failed to connect to {}: {}",
                            key_for_error.label(),
                            e
                        ))
                    })
                })
                .await;

            let mut lease = match checkout_result {
                Ok(lease) => lease,
                Err(e) if attempt == 0 && e.kind == super::ErrorKind::Transport => {
                    debug!(
                        gateway = %self.gateway_name,
                        target = %key.label(),
                        "transport error creating direct SSH connection, retrying: {}",
                        e
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            if lease.resource_mut().is_alive() {
                return Ok(lease);
            }

            debug!(
                gateway = %self.gateway_name,
                target = %key.label(),
                generation = %lease.generation(),
                "discarding stale direct SSH connection"
            );
            self.pool.discard(lease);

            if attempt == 1 {
                return Err(GatewayError::transport(anyhow!(
                    "direct SSH connection for {} is not alive after refresh",
                    key.label()
                )));
            }
        }
        unreachable!("direct pool checkout loop is bounded")
    }
}

// ---------------------------------------------------------------------------
// Gateway trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Gateway for DirectGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let resolved = self.resolve_target(target).await?;
        let mut lease = self.get_connection(&resolved).await?;

        // Take stdin_rx from the gateway request (consuming it so the channel
        // is owned by the connection layer for forwarding).
        let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());

        // Build the connection-level request
        let mut conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            tty: request.tty,
            cols: request.cols,
            rows: request.rows,
            shell: Self::effective_shell(&request.shell, request.no_shell, &resolved),
            no_shell: request.no_shell,
            timeout_ms: request.timeout_ms,
            stdin: request.stdin,
            stdin_intent: request.stdin_intent,
            stdin_rx,
        };

        let result = lease.resource_mut().exec(&mut conn_request).await;

        match result {
            Ok(exit_code) => {
                self.pool.return_healthy(lease);
                Ok(exit_code)
            }
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    generation = %lease.generation(),
                    "transport error on direct exec after checkout; discarding connection: {}",
                    e
                );
                self.pool.discard(lease);
                Err(GatewayError::transport(e))
            }
            Err(e) => {
                self.pool.return_healthy(lease);
                Err(GatewayError::execution(e))
            }
        }
    }

    async fn copy(&self, target: &str, spec: CopySpec) -> Result<(), GatewayError> {
        let resolved = self.resolve_target(target).await?;
        let mut lease = self.get_connection(&resolved).await?;

        let result = lease.resource_mut().copy(spec).await;

        match result {
            Ok(()) => {
                self.pool.return_healthy(lease);
                Ok(())
            }
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    generation = %lease.generation(),
                    "transport error on direct copy after checkout; discarding connection: {}",
                    e
                );
                self.pool.discard(lease);
                Err(GatewayError::transport(e))
            }
            Err(e) => {
                self.pool.return_healthy(lease);
                Err(GatewayError::execution(e))
            }
        }
    }

    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let resolved = self.resolve_target(target).await?;
        let mut lease = self.get_connection(&resolved).await?;

        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: Self::effective_shell(&request.shell, request.no_shell, &resolved),
            no_shell: request.no_shell,
        };

        let handle = match lease.resource_mut().exec_interactive(&conn_request).await {
            Ok(handle) => handle,
            Err(e) if is_transport_error(&e) => {
                self.pool.discard(lease);
                return Err(GatewayError::transport(e));
            }
            Err(e) => {
                self.pool.return_healthy(lease);
                return Err(GatewayError::execution(e));
            }
        };

        let crate::daemon::connection::InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
            mut abort_handles,
        } = handle;
        let (gateway_exit_tx, gateway_exit_rx) = tokio::sync::oneshot::channel();
        let pool = self.pool.clone();
        let wrapper_task = tokio::spawn(async move {
            let exit_code = exit_rx.await.unwrap_or(255);
            if lease.resource_mut().is_alive() {
                pool.return_healthy(lease);
            } else {
                pool.discard(lease);
            }
            let _ = gateway_exit_tx.send(exit_code);
        });
        abort_handles.push(wrapper_task.abort_handle());

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx: gateway_exit_rx,
            abort_handles,
        })
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

    fn kind(&self) -> GatewayKind {
        GatewayKind::Direct
    }

    fn name(&self) -> &str {
        &self.gateway_name
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
