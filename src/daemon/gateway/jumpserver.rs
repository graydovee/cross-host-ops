// JumpserverGateway implementation.
// Reuses one authenticated SSH transport and caches target-level PTY shells
// for non-interactive exec/copy operations.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::{debug, info};

use crate::config::{AppConfig, JumpserverGatewayConfig, MfaConfig};
use crate::daemon::connection::jumpserver::JumpserverConnection;
use crate::daemon::connection::shared::{PtyShell, request_default_pty};
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest, InteractiveRequest as ConnInteractiveRequest,
};
use crate::daemon::connection_manager::{ManagedPool, ManagedSingleton, PoolLease, SingletonLease};
use crate::daemon::resolver::derive_target_ip;
use crate::protocol::ServerListRow;
use crate::types::CopySpec;

use super::auth::{AuthPrompt, AuthPrompter, ClientHandler, authenticate_with_key, connect_handle};
use super::{
    ErrorKind, ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle,
    InteractiveRequest, is_transport_error,
};

const MENU_PROMPT_CONTAINS: &str = "Opt";
const MFA_PROMPT_CONTAINS: &str = "MFA";
const SHELL_PROMPT_SUFFIXES: &[&str] = &["$ ", "# "];

type JumpserverTransport = AsyncMutex<JumpserverTransportState>;

pub struct JumpserverGateway {
    gateway_name: String,
    config: Arc<RwLock<AppConfig>>,
    fields: JumpserverGatewayConfig,
    auth_prompter: Arc<AuthPrompter>,
    transport: ManagedSingleton<JumpserverTransport>,
    target_shells: ManagedPool<JumpserverTargetKey, JumpserverTargetShell>,
    max_idle_time: Duration,
}

struct JumpserverTransportState {
    handle: russh::client::Handle<ClientHandler>,
    connect_timeout: Duration,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct JumpserverTargetKey(String);

struct JumpserverTargetShell {
    shell: PtyShell,
    transport_generation: u64,
}

impl JumpserverGateway {
    pub fn new(
        gateway_name: String,
        config: Arc<RwLock<AppConfig>>,
        fields: JumpserverGatewayConfig,
        auth_prompter: Arc<AuthPrompter>,
        max_idle_time: Duration,
    ) -> Self {
        Self {
            gateway_name,
            config,
            fields,
            auth_prompter,
            transport: ManagedSingleton::new(),
            target_shells: ManagedPool::new(1, max_idle_time),
            max_idle_time,
        }
    }

    async fn ensure_transport(&self) -> Result<SingletonLease<JumpserverTransport>, GatewayError> {
        for attempt in 0..=1 {
            let result = self
                .transport
                .checkout_or_insert_with(|| async {
                    self.establish_transport()
                        .await
                        .map(AsyncMutex::new)
                        .map_err(|e| {
                            GatewayError::transport(anyhow!(
                                "failed to establish jumpserver transport for '{}': {}",
                                self.gateway_name,
                                e
                            ))
                        })
                })
                .await;
            match result {
                Ok(lease) => return Ok(lease),
                Err(e) if attempt == 0 && matches!(e.kind, ErrorKind::Transport) => {
                    debug!(
                        gateway = %self.gateway_name,
                        "transport error creating jumpserver connection, retrying: {}",
                        e
                    );
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("jumpserver transport checkout loop is bounded")
    }

    async fn establish_transport(&self) -> Result<JumpserverTransportState> {
        let app_config = self.config.read().await.clone();
        let mut handle = connect_handle(&self.fields.host, self.fields.port, &app_config).await?;

        let mfa_config = MfaConfig {
            totp_secret_base32: self.fields.totp_secret_base32.clone(),
            digits: self.fields.totp_digits,
            period: self.fields.totp_period,
            ..MfaConfig::default()
        };
        let mfa = if mfa_config.totp_secret_base32.is_empty() {
            None
        } else {
            Some(&mfa_config)
        };
        let auth_prompter: Option<&AuthPrompter> = if mfa_config.totp_secret_base32.is_empty() {
            Some(self.auth_prompter.as_ref())
        } else {
            None
        };

        authenticate_with_key(
            &mut handle,
            &self.fields.user,
            &self.fields.identity_file,
            &self.gateway_name,
            mfa,
            self.fields.pubkey_accepted_algorithms.as_deref(),
            auth_prompter,
        )
        .await?;

        info!(gateway = %self.gateway_name, "jumpserver SSH transport established");

        Ok(JumpserverTransportState {
            handle,
            connect_timeout: app_config.ssh.connect_timeout,
        })
    }

    async fn invalidate_transport(&self, generation: u64) {
        if self.transport.invalidate_generation(generation).await {
            let discarded_shells = self.discard_idle_target_shells_for_generation(generation);
            debug!(
                gateway = %self.gateway_name,
                generation = %generation,
                discarded_shells = %discarded_shells,
                "discarded jumpserver SSH transport, will reconnect on next use"
            );
        }
    }

    async fn open_target_shell(
        &self,
        lease: &SingletonLease<JumpserverTransport>,
        target: &str,
    ) -> Result<PtyShell, GatewayError> {
        let transport = lease.resource();
        let (channel, connect_timeout) = {
            let guard = transport.lock().await;
            if guard.handle.is_closed() {
                return Err(GatewayError::transport(anyhow!(
                    "jumpserver SSH transport is closed"
                )));
            }
            let channel = guard.handle.channel_open_session().await.map_err(|e| {
                GatewayError::transport(anyhow!("failed to open jumpserver PTY channel: {}", e))
            })?;
            (channel, guard.connect_timeout)
        };

        request_default_pty(&channel).await.map_err(|e| {
            GatewayError::transport(anyhow!("failed to request jumpserver PTY: {}", e))
        })?;
        let mut shell = PtyShell::new(
            channel,
            SHELL_PROMPT_SUFFIXES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            connect_timeout,
        );
        shell.request_shell().await.map_err(|e| {
            GatewayError::transport(anyhow!("failed to start jumpserver shell: {}", e))
        })?;

        self.establish_target_shell(&mut shell, target)
            .await
            .map_err(|e| {
                if is_transport_error(&e) {
                    GatewayError::transport(e)
                } else {
                    GatewayError::execution(e)
                }
            })?;
        Ok(shell)
    }

    async fn open_target_shell_with_retry(
        &self,
        target: &str,
    ) -> Result<(SingletonLease<JumpserverTransport>, PtyShell), GatewayError> {
        for attempt in 0..=1 {
            let lease = self.ensure_transport().await?;
            match self.open_target_shell(&lease, target).await {
                Ok(shell) => return Ok((lease, shell)),
                Err(e) if attempt == 0 && matches!(e.kind, ErrorKind::Transport) => {
                    debug!(
                        gateway = %self.gateway_name,
                        target = %target,
                        generation = %lease.generation(),
                        "transport error preparing jumpserver shell, retrying: {}",
                        e
                    );
                    self.invalidate_transport(lease.generation()).await;
                }
                Err(e) => {
                    if matches!(e.kind, ErrorKind::Transport) {
                        self.invalidate_transport(lease.generation()).await;
                    }
                    return Err(e);
                }
            }
        }
        unreachable!("jumpserver shell preparation loop is bounded")
    }

    fn target_key(&self, target: &str) -> JumpserverTargetKey {
        JumpserverTargetKey(derive_target_ip(target))
    }

    async fn checkout_target_shell(
        &self,
        target: &str,
    ) -> Result<PoolLease<JumpserverTargetKey, JumpserverTargetShell>, GatewayError> {
        let key = self.target_key(target);
        let target = target.to_string();
        self.target_shells
            .checkout_or_create_with(key.clone(), || async move {
                let (transport_lease, shell) = self.open_target_shell_with_retry(&target).await?;
                Ok(JumpserverTargetShell {
                    shell,
                    transport_generation: transport_lease.generation(),
                })
            })
            .await
    }

    async fn return_target_shell_if_current(
        &self,
        lease: PoolLease<JumpserverTargetKey, JumpserverTargetShell>,
    ) {
        let shell_generation = lease.resource().transport_generation;
        if self.transport.current_generation().await == Some(shell_generation) {
            self.target_shells.return_healthy(lease);
        } else {
            debug!(
                gateway = %self.gateway_name,
                shell_generation = %shell_generation,
                "discarding jumpserver target shell from stale transport generation"
            );
            self.target_shells.discard(lease);
        }
    }

    fn discard_idle_target_shells_for_generation(&self, generation: u64) -> usize {
        self.target_shells
            .discard_idle_where(|shell| shell.transport_generation == generation)
    }

    async fn transport_generation_is_closed(&self, generation: u64) -> bool {
        let Some(lease) = self.transport.checkout_generation(generation).await else {
            return true;
        };
        let transport = lease.resource();
        let guard = transport.lock().await;
        guard.handle.is_closed()
    }

    async fn classify_cached_shell_error(
        &self,
        error: anyhow::Error,
        lease: PoolLease<JumpserverTargetKey, JumpserverTargetShell>,
    ) -> GatewayError {
        let shell_generation = lease.resource().transport_generation;
        debug!(
            gateway = %self.gateway_name,
            shell_generation = %shell_generation,
            "discarding jumpserver target shell after operation error: {}",
            error
        );
        self.target_shells.discard(lease);
        if is_transport_error(&error) {
            if self.transport_generation_is_closed(shell_generation).await {
                self.invalidate_transport(shell_generation).await;
            }
            GatewayError::transport(error)
        } else {
            GatewayError::execution(error)
        }
    }

    async fn establish_target_shell(&self, shell: &mut PtyShell, target: &str) -> Result<()> {
        let ip = derive_target_ip(target);
        debug!(gateway = %self.gateway_name, target = %target, ip = %ip, "waiting for jumpserver menu");

        let mut selected = false;
        let mut mfa_sent = false;
        loop {
            let chunk = shell.read_chunk().await?;
            shell.extend_pending(&chunk);
            let text = shell.pending_text();

            if !mfa_sent && text.contains(MFA_PROMPT_CONTAINS) {
                let code = if !self.fields.totp_secret_base32.is_empty() {
                    let mfa_config = MfaConfig {
                        totp_secret_base32: self.fields.totp_secret_base32.clone(),
                        digits: self.fields.totp_digits,
                        period: self.fields.totp_period,
                        ..MfaConfig::default()
                    };
                    super::auth::generate_totp(&mfa_config)?
                } else {
                    (self.auth_prompter)(AuthPrompt {
                        gateway_name: self.gateway_name.clone(),
                        message: format!("jumpserver '{}' requested MFA", self.gateway_name),
                        secret: true,
                    })
                    .await?
                };
                shell.write_line(&code).await?;
                shell.clear_pending();
                mfa_sent = true;
                info!(gateway = %self.gateway_name, target = %target, "jumpserver MFA completed");
                continue;
            }

            if !selected && text.contains(MENU_PROMPT_CONTAINS) {
                debug!(gateway = %self.gateway_name, target = %target, ip = %ip, "jumpserver menu detected, selecting target");
                shell.write_line(&ip).await?;
                shell.clear_pending();
                selected = true;
                continue;
            }

            if selected && shell.pending_has_prompt() {
                debug!(gateway = %self.gateway_name, target = %target, "remote shell prompt detected");
                break;
            }
        }
        shell.clear_pending();

        shell.write_line("stty -echo").await?;
        shell.wait_for_prompt().await?;
        shell.clear_pending();

        Ok(())
    }
}

#[async_trait]
impl Gateway for JumpserverGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let mut lease = self.checkout_target_shell(target).await?;
        let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());

        let mut conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            pty: request.pty,
            cols: request.cols,
            rows: request.rows,
            shell: request.shell.clone(),
            no_shell: request.no_shell,
            timeout_ms: request.timeout_ms,
            stdin: request.stdin,
            stdin_rx,
        };

        let result = {
            let mut conn = JumpserverConnection::new_borrowed(&mut lease.resource_mut().shell);
            conn.exec(&mut conn_request).await
        };

        match result {
            Ok(exit_code) => {
                self.return_target_shell_if_current(lease).await;
                Ok(exit_code)
            }
            Err(e) => Err(self.classify_cached_shell_error(e, lease).await),
        }
    }

    async fn copy(&self, target: &str, spec: CopySpec) -> Result<(), GatewayError> {
        let mut lease = self.checkout_target_shell(target).await?;
        let result = {
            let mut conn = JumpserverConnection::new_borrowed(&mut lease.resource_mut().shell);
            conn.copy(spec).await
        };

        match result {
            Ok(()) => {
                self.return_target_shell_if_current(lease).await;
                Ok(())
            }
            Err(e) => Err(self.classify_cached_shell_error(e, lease).await),
        }
    }

    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let (lease, shell) = self.open_target_shell_with_retry(target).await?;
        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: request.shell.clone(),
        };

        let mut conn = JumpserverConnection::new(shell);
        let handle = match conn.exec_interactive(&conn_request).await {
            Ok(handle) => handle,
            Err(e) if is_transport_error(&e) => {
                self.invalidate_transport(lease.generation()).await;
                return Err(GatewayError::transport(e));
            }
            Err(e) => return Err(GatewayError::execution(e)),
        };

        let crate::daemon::connection::InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
        } = handle;
        let (gateway_exit_tx, gateway_exit_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let exit_code = exit_rx.await.unwrap_or(255);
            drop(lease);
            let _ = gateway_exit_tx.send(exit_code);
        });

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx: gateway_exit_rx,
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        Err(GatewayError::unsupported(anyhow!(
            "jumpserver gateway '{}' does not support list_servers",
            self.gateway_name
        )))
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Jumpserver
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {
        self.target_shells
            .prune_idle_with(|target_shell| target_shell.shell.is_channel_open());
        if self.target_shells.total_entries() == 0 {
            let _ = self.transport.prune_idle(self.max_idle_time).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_gateway() -> JumpserverGateway {
        JumpserverGateway::new(
            "jump".to_string(),
            Arc::new(RwLock::new(AppConfig::default())),
            JumpserverGatewayConfig {
                name: "jump".to_string(),
                host: "jump.example.test".to_string(),
                port: 22,
                user: "admin".to_string(),
                identity_file: "~/.ssh/id_rsa".to_string(),
                pubkey_accepted_algorithms: None,
                totp_secret_base32: String::new(),
                totp_digits: 6,
                totp_period: 30,
            },
            Arc::new(|_| Box::pin(async { Ok(String::new()) })),
            Duration::from_secs(60),
        )
    }

    #[test]
    fn target_key_uses_derived_target_ip() {
        let gateway = test_gateway();
        assert_eq!(
            gateway.target_key("asset-198-51-100-22"),
            JumpserverTargetKey("198.51.100.22".to_string())
        );
        assert_eq!(
            gateway.target_key("plain-target"),
            JumpserverTargetKey("plain-target".to_string())
        );
    }

    #[tokio::test]
    async fn list_servers_does_not_create_transport_or_target_shell_cache() {
        let gateway = test_gateway();
        let result = gateway.list_servers().await;
        assert!(result.is_err());
        assert_eq!(gateway.transport.current_generation().await, None);
        assert_eq!(gateway.target_shells.total_entries(), 0);
    }
}
