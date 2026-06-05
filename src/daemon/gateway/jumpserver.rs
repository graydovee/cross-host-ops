// JumpserverGateway implementation.
// Reuses one authenticated SSH transport and opens a fresh PTY shell channel
// for each routed operation.

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
use crate::daemon::connection_manager::{ManagedSingleton, SingletonLease};
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
    max_idle_time: Duration,
}

struct JumpserverTransportState {
    handle: russh::client::Handle<ClientHandler>,
    connect_timeout: Duration,
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
            debug!(
                gateway = %self.gateway_name,
                generation = %generation,
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
        let (lease, shell) = self.open_target_shell_with_retry(target).await?;
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

        let mut conn = JumpserverConnection::new(shell);
        match conn.exec(&mut conn_request).await {
            Ok(exit_code) => Ok(exit_code),
            Err(e) if is_transport_error(&e) => {
                self.invalidate_transport(lease.generation()).await;
                Err(GatewayError::transport(e))
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn copy(&self, target: &str, spec: CopySpec) -> Result<(), GatewayError> {
        let (lease, shell) = self.open_target_shell_with_retry(target).await?;
        let mut conn = JumpserverConnection::new(shell);
        match conn.copy(spec).await {
            Ok(()) => Ok(()),
            Err(e) if is_transport_error(&e) => {
                self.invalidate_transport(lease.generation()).await;
                Err(GatewayError::transport(e))
            }
            Err(e) => Err(GatewayError::execution(e)),
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

        Ok(InteractiveHandle {
            stdin_tx: handle.stdin_tx,
            resize_tx: handle.resize_tx,
            stdout_rx: handle.stdout_rx,
            exit_rx: handle.exit_rx,
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
        let _ = self.transport.prune_idle(self.max_idle_time).await;
    }
}
