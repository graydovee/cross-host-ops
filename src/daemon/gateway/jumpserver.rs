// JumpserverGateway implementation.
// Manages a single PTY shell session to a jumpserver, with lazy connection,
// MFA handling, serial command execution via AsyncMutex, and idle pruning.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::config::{AppConfig, JumpserverJumpHostFields, ServerEntry};
use crate::connection::resolver::derive_target_ip;
use crate::connection::types::CopySpec;

use super::auth::{
    AuthPrompt, AuthPrompter, ClientHandler, authenticate_with_key, connect_handle,
};
use super::{
    ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle, InteractiveRequest,
    is_transport_error,
};
use crate::daemon::connection::jumpserver::JumpserverConnection;
use crate::daemon::connection::shared::{request_default_pty, PtyShell};
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest,
    InteractiveRequest as ConnInteractiveRequest,
};

// ---------------------------------------------------------------------------
// JumpserverGateway
// ---------------------------------------------------------------------------

pub struct JumpserverGateway {
    gateway_name: String,
    config: Arc<RwLock<AppConfig>>,
    fields: JumpserverJumpHostFields,
    auth_prompter: Arc<AuthPrompter>,
    /// Single PTY shell session (lazily connected, serial access).
    shell: AsyncMutex<Option<ShellState>>,
    /// Tracks the last time the shell was used (for idle pruning).
    last_used: SyncMutex<Instant>,
}

/// Internal state wrapping the PtyShell and the SSH handle that owns it.
struct ShellState {
    /// The SSH client handle — kept alive to maintain the channel.
    _handle: russh::client::Handle<ClientHandler>,
    /// The PTY shell for command execution.
    shell: PtyShell,
}

impl JumpserverGateway {
    /// Construct a new JumpserverGateway. No connections are established.
    pub fn new(
        gateway_name: String,
        config: Arc<RwLock<AppConfig>>,
        fields: JumpserverJumpHostFields,
        auth_prompter: Arc<AuthPrompter>,
    ) -> Self {
        Self {
            gateway_name,
            config,
            fields,
            auth_prompter,
            shell: AsyncMutex::new(None),
            last_used: SyncMutex::new(Instant::now()),
        }
    }

    /// Ensure a shell is connected and ready. If not, establish a new one.
    /// Must be called while holding the AsyncMutex lock on `self.shell`.
    async fn ensure_shell(
        &self,
        shell_slot: &mut Option<ShellState>,
    ) -> Result<(), GatewayError> {
        if shell_slot.is_some() {
            return Ok(());
        }
        let state = self.establish_shell().await.map_err(|e| {
            GatewayError::transport(anyhow!(
                "failed to establish jumpserver shell for '{}': {}",
                self.gateway_name,
                e
            ))
        })?;
        *shell_slot = Some(state);
        Ok(())
    }

    /// Establish a new PTY shell session: SSH connect → authenticate → open channel
    /// → request PTY → request shell → wait for prompt → (MFA if needed).
    async fn establish_shell(&self) -> Result<ShellState> {
        let app_config = self.config.read().await.clone();
        let mut handle = connect_handle(&self.fields.host, self.fields.port, &app_config).await?;

        // Authenticate with key, handling MFA via TOTP or AuthPrompter.
        let mfa = if self.fields.mfa.totp_secret_base32.is_empty() {
            None
        } else {
            Some(&self.fields.mfa)
        };
        let auth_prompter: Option<&AuthPrompter> =
            if self.fields.mfa.totp_secret_base32.is_empty() {
                Some(self.auth_prompter.as_ref())
            } else {
                // Auto-TOTP: no need for auth prompter during keyboard-interactive
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

        let channel = handle.channel_open_session().await?;
        request_default_pty(&channel).await?;

        let mut shell = PtyShell::new(
            channel,
            self.fields.shell_prompt_suffixes.clone(),
            app_config.ssh.connect_timeout,
        );
        shell.request_shell().await?;

        // Wait for initial prompt or MFA prompt
        self.navigate_initial_shell(&mut shell).await?;

        info!(gateway = %self.gateway_name, "jumpserver shell established");

        Ok(ShellState {
            _handle: handle,
            shell,
        })
    }

    /// Handle initial shell output: respond to MFA prompt if present,
    /// then wait for a command prompt.
    async fn navigate_initial_shell(&self, shell: &mut PtyShell) -> Result<()> {
        let mut mfa_sent = false;
        loop {
            let chunk = shell.read_chunk().await?;
            shell.extend_pending(&chunk);
            let text = shell.pending_text();

            // Check for MFA prompt
            if !mfa_sent && text.contains(&self.fields.mfa_prompt_contains) {
                let code = if !self.fields.mfa.totp_secret_base32.is_empty() {
                    super::auth::generate_totp(&self.fields.mfa)?
                } else {
                    (self.auth_prompter)(AuthPrompt {
                        gateway_name: self.gateway_name.clone(),
                        message: format!(
                            "jumpserver '{}' requested MFA",
                            self.gateway_name
                        ),
                        secret: true,
                    })
                    .await?
                };
                shell.write_line(&code).await?;
                shell.clear_pending();
                mfa_sent = true;
                continue;
            }

            // Check for shell prompt (we're ready)
            if shell.pending_has_prompt() {
                break;
            }
        }
        shell.clear_pending();

        // Disable echo for cleaner command output parsing
        shell.write_line("stty -echo").await?;
        shell.wait_for_prompt().await?;
        shell.clear_pending();

        Ok(())
    }

    /// Navigate the jumpserver menu to reach a specific target.
    /// This writes the target IP/identifier to the menu prompt and waits
    /// for a shell prompt on the target host.
    async fn navigate_to_target(
        &self,
        shell: &mut PtyShell,
        target: &str,
    ) -> Result<()> {
        let ip = derive_target_ip(target);
        debug!(gateway = %self.gateway_name, target = %target, ip = %ip, "navigating to target");

        // For jumpserver: the shell is already at the menu or target prompt.
        // We need to select the target via the menu.
        // First, check if we are at a menu prompt or already at a shell prompt.
        // Write the target identifier to navigate.
        shell.write_line(&ip).await?;
        shell.wait_for_prompt().await?;
        shell.clear_pending();

        // Disable echo on the target shell
        shell.write_line("stty -echo").await?;
        shell.wait_for_prompt().await?;
        shell.clear_pending();

        Ok(())
    }

    /// Execute a command through the PTY shell with retry on transport error.
    async fn exec_with_retry(
        &self,
        target: &str,
        request: &ExecRequest,
    ) -> Result<i32, GatewayError> {
        let conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            pty: request.pty,
            cols: request.cols,
            rows: request.rows,
            shell: request.shell.clone(),
        };

        // First attempt
        let first_result = {
            let mut shell_guard = self.shell.lock().await;
            self.ensure_shell(&mut shell_guard).await?;

            let result = self
                .navigate_and_exec(&mut shell_guard, target, &conn_request)
                .await;

            match &result {
                Err(e) if is_transport_error(e) => {
                    debug!(
                        gateway = %self.gateway_name,
                        target = %target,
                        "transport error on first attempt, discarding shell: {}",
                        e
                    );
                    *shell_guard = None;
                }
                _ => {}
            }
            result
        };

        match first_result {
            Ok(exit_code) => {
                self.touch();
                return Ok(exit_code);
            }
            Err(e) if is_transport_error(&e) => {
                // Fall through to retry
            }
            Err(e) => return Err(GatewayError::execution(e)),
        }

        // Retry with fresh connection
        let mut shell_guard = self.shell.lock().await;
        self.ensure_shell(&mut shell_guard).await?;

        let result = self
            .navigate_and_exec(&mut shell_guard, target, &conn_request)
            .await;
        self.touch();

        match result {
            Ok(exit_code) => Ok(exit_code),
            Err(e) => {
                *shell_guard = None;
                if is_transport_error(&e) {
                    Err(GatewayError::transport(e))
                } else {
                    Err(GatewayError::execution(e))
                }
            }
        }
    }

    /// Navigate to target and execute a command. Helper for exec_with_retry.
    async fn navigate_and_exec(
        &self,
        shell_guard: &mut Option<ShellState>,
        target: &str,
        conn_request: &ConnExecRequest,
    ) -> Result<i32> {
        let state = shell_guard.as_mut().unwrap();
        self.navigate_to_target(&mut state.shell, target).await?;
        let mut conn = JumpserverConnection::new_borrowed(&mut state.shell);
        conn.exec(conn_request).await
    }

    /// Update the last-used timestamp.
    fn touch(&self) {
        *self.last_used.lock() = Instant::now();
    }
}

// ---------------------------------------------------------------------------
// Gateway trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Gateway for JumpserverGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        self.exec_with_retry(target, request).await
    }

    async fn copy(&self, target: &str, spec: &CopySpec) -> Result<(), GatewayError> {
        let mut shell_guard = self.shell.lock().await;
        self.ensure_shell(&mut shell_guard).await?;
        let state = shell_guard.as_mut().unwrap();

        self.navigate_to_target(&mut state.shell, target)
            .await
            .map_err(|e| {
                if is_transport_error(&e) {
                    GatewayError::transport(e)
                } else {
                    GatewayError::execution(e)
                }
            })?;

        let mut conn = JumpserverConnection::new_borrowed(&mut state.shell);
        let result = conn.copy(spec).await;
        self.touch();

        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                if is_transport_error(&e) {
                    *shell_guard = None;
                    Err(GatewayError::transport(e))
                } else {
                    Err(GatewayError::execution(e))
                }
            }
        }
    }

    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let mut shell_guard = self.shell.lock().await;
        self.ensure_shell(&mut shell_guard).await?;
        let state = shell_guard.as_mut().unwrap();

        self.navigate_to_target(&mut state.shell, target)
            .await
            .map_err(|e| {
                if is_transport_error(&e) {
                    GatewayError::transport(e)
                } else {
                    GatewayError::execution(e)
                }
            })?;

        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: request.shell.clone(),
        };

        let mut conn = JumpserverConnection::new_borrowed(&mut state.shell);
        let handle = conn.exec_interactive(&conn_request).await.map_err(|e| {
            if is_transport_error(&e) {
                *shell_guard = None;
                GatewayError::transport(e)
            } else {
                GatewayError::execution(e)
            }
        })?;
        self.touch();

        Ok(InteractiveHandle {
            stdin_tx: handle.stdin_tx,
            resize_tx: handle.resize_tx,
            stdout_rx: handle.stdout_rx,
            exit_rx: handle.exit_rx,
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerEntry>, GatewayError> {
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
        let idle_duration = self.last_used.lock().elapsed();
        let max_idle = {
            let config = self.config.read().await;
            config.ssh.max_idle_time
        };
        if idle_duration > max_idle {
            let mut shell_guard = self.shell.lock().await;
            if shell_guard.is_some() {
                debug!(
                    gateway = %self.gateway_name,
                    idle_secs = %idle_duration.as_secs(),
                    "pruning idle jumpserver shell"
                );
                *shell_guard = None;
            }
        }
    }
}
