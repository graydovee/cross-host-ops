// JumpserverGateway implementation.
// Reuses one authenticated SSH transport and caches target-level PTY shells
// for non-interactive exec/copy operations.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::{debug, info};

use crate::config::{AppConfig, JumpserverGatewayConfig, MfaConfig, load_server_config};
use crate::daemon::connection::jumpserver::JumpserverConnection;
use crate::daemon::connection::shared::{PtyShell, request_default_pty};
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest, InteractiveRequest as ConnInteractiveRequest,
};
use crate::daemon::connection_manager::{ManagedPool, ManagedSingleton, PoolLease, SingletonLease};
use crate::daemon::resolver::derive_target_ip;
use crate::protocol::ServerListRow;
use crate::types::{CopySpec, FlagIntent};

use super::auth::{AuthPrompt, AuthPrompter, ClientHandler, authenticate_with_key, connect_handle};
use super::{
    ErrorKind, ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle,
    InteractiveRequest, is_transport_error,
};

const MENU_PROMPT_CONTAINS: &str = "Opt";
const MFA_PROMPT_CONTAINS: &str = "MFA";
const SHELL_PROMPT_SUFFIXES: &[&str] = &["$ ", "# "];
const PAGE_PROMPT_CONTAINS: &str = "上一页";

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct JumpserverAssetRow {
    id: String,
    ip: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageStatus {
    current: u32,
    total: u32,
}

fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == 0x1b {
            index += 1;
            if index >= bytes.len() {
                break;
            }
            match bytes[index] {
                b'[' => {
                    index += 1;
                    while index < bytes.len() {
                        let byte = bytes[index];
                        index += 1;
                        if (0x40..=0x7e).contains(&byte) {
                            break;
                        }
                    }
                }
                b']' => {
                    index += 1;
                    while index < bytes.len() {
                        let byte = bytes[index];
                        index += 1;
                        if byte == 0x07 {
                            break;
                        }
                        if byte == 0x1b && bytes.get(index) == Some(&b'\\') {
                            index += 1;
                            break;
                        }
                    }
                }
                _ => {
                    index += 1;
                }
            }
            continue;
        }

        if let Some(ch) = input[index..].chars().next() {
            output.push(ch);
            index += ch.len_utf8();
        } else {
            break;
        }
    }

    output
}

fn parse_asset_rows(text: &str) -> Vec<JumpserverAssetRow> {
    let clean = strip_ansi(text);
    clean
        .lines()
        .filter_map(|line| {
            let columns = line.split('|').map(str::trim).collect::<Vec<_>>();
            if columns.len() < 3 {
                return None;
            }
            let id = columns[0];
            let ip = columns[2];
            if id.chars().all(|ch| ch.is_ascii_digit()) && looks_like_ipv4(ip) {
                Some(JumpserverAssetRow {
                    id: id.to_string(),
                    ip: ip.to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn select_exact_asset_id(text: &str, ip: &str) -> Result<Option<String>> {
    let matches = parse_asset_rows(text)
        .into_iter()
        .filter(|row| row.ip == ip)
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches[0].id.clone())),
        count => bail!(
            "jumpserver asset search for {} returned {} exact matches",
            ip,
            count
        ),
    }
}

fn looks_like_ipv4(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 4
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

fn parse_page_status(text: &str) -> Option<PageStatus> {
    let clean = strip_ansi(text);
    let (_, rest) = clean.split_once("页码：")?;
    let current = rest
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())?
        .parse()
        .ok()?;
    let (_, rest) = clean.split_once("总页数：")?;
    let total = rest
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())?
        .parse()
        .ok()?;
    Some(PageStatus { current, total })
}

fn contains_menu_prompt(text: &str) -> bool {
    strip_ansi(text).contains(MENU_PROMPT_CONTAINS)
}

fn contains_page_prompt(text: &str) -> bool {
    let clean = strip_ansi(text);
    clean.contains(PAGE_PROMPT_CONTAINS) && clean.trim_end().ends_with(':')
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

    fn validate_exec_request(&self, request: &ExecRequest) -> Result<(), GatewayError> {
        if request.tty_intent == FlagIntent::Disable {
            return Err(GatewayError::unsupported(anyhow!(
                "jumpserver gateway requires an internal tty; --no-tty is not supported"
            )));
        }
        Ok(())
    }

    async fn effective_shell(&self, request_shell: &str, no_shell: bool) -> String {
        let cli_shell = (!request_shell.is_empty()).then_some(request_shell);
        let defaults_shell = {
            let config = self.config.read().await;
            load_server_config(Path::new(&config.ssh.server_config_path))
                .map(|server_config| server_config.defaults.shell)
                .unwrap_or_default()
        };
        crate::daemon::connection::shared::resolve_shell(cli_shell, no_shell, None, &defaults_shell)
            .unwrap_or_default()
    }

    async fn establish_target_shell(&self, shell: &mut PtyShell, target: &str) -> Result<()> {
        let ip = derive_target_ip(target);
        debug!(gateway = %self.gateway_name, target = %target, ip = %ip, "waiting for jumpserver menu");

        let mut search_sent = false;
        let mut asset_id_sent = false;
        let mut mfa_sent = false;
        loop {
            let chunk = shell.read_chunk().await?;
            shell.extend_pending(&chunk);
            let text = shell.pending_text();

            if !mfa_sent && strip_ansi(&text).contains(MFA_PROMPT_CONTAINS) {
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

            if !search_sent && contains_menu_prompt(&text) {
                debug!(gateway = %self.gateway_name, target = %target, ip = %ip, "jumpserver menu detected, selecting target");
                shell.write_line(&ip).await?;
                shell.clear_pending();
                search_sent = true;
                continue;
            }

            if search_sent && !asset_id_sent {
                if let Some(asset_id) = select_exact_asset_id(&text, &ip)? {
                    debug!(
                        gateway = %self.gateway_name,
                        target = %target,
                        ip = %ip,
                        asset_id = %asset_id,
                        "jumpserver asset table matched exact IP"
                    );
                    shell.write_line(&asset_id).await?;
                    shell.clear_pending();
                    asset_id_sent = true;
                    continue;
                }

                if contains_page_prompt(&text) {
                    match parse_page_status(&text) {
                        Some(status) if status.current < status.total => {
                            debug!(
                                gateway = %self.gateway_name,
                                target = %target,
                                ip = %ip,
                                page = %status.current,
                                total_pages = %status.total,
                                "jumpserver asset table did not contain exact IP, advancing page"
                            );
                            shell.write_line("").await?;
                            shell.clear_pending();
                            continue;
                        }
                        Some(status) => {
                            bail!(
                                "jumpserver asset search for {} did not find an exact IP match after {} page(s)",
                                ip,
                                status.total
                            );
                        }
                        None => {
                            bail!(
                                "jumpserver asset search for {} showed a paginated table but page status could not be parsed",
                                ip
                            );
                        }
                    }
                }

                if contains_menu_prompt(&text) && !parse_asset_rows(&text).is_empty() {
                    bail!(
                        "jumpserver asset search for {} returned candidates but no exact IP match",
                        ip
                    );
                }
            }

            if search_sent && shell.pending_has_prompt() {
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
        self.validate_exec_request(request)?;
        let mut lease = self.checkout_target_shell(target).await?;
        let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());
        let shell = self.effective_shell(&request.shell, request.no_shell).await;

        let mut conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            tty: request.tty,
            cols: request.cols,
            rows: request.rows,
            shell,
            no_shell: request.no_shell,
            timeout_ms: request.timeout_ms,
            stdin: request.stdin,
            stdin_intent: request.stdin_intent,
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
        let effective_shell = self.effective_shell(&request.shell, request.no_shell).await;
        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: effective_shell,
            no_shell: request.no_shell,
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
            mut abort_handles,
        } = handle;
        let (gateway_exit_tx, gateway_exit_rx) = tokio::sync::oneshot::channel();
        let wrapper_task = tokio::spawn(async move {
            let exit_code = exit_rx.await.unwrap_or(255);
            drop(lease);
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
    use tokio::sync::mpsc;

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
            gateway.target_key("asset-198-51-100-3"),
            JumpserverTargetKey("198.51.100.3".to_string())
        );
        assert_eq!(
            gateway.target_key("asset-198-51-100-22"),
            JumpserverTargetKey("198.51.100.22".to_string())
        );
        assert_eq!(
            gateway.target_key("plain-target"),
            JumpserverTargetKey("plain-target".to_string())
        );
    }

    #[test]
    fn parses_asset_table_and_selects_exact_ip() {
        let text = r#"
  ID    | 主机名                                                                             | IP                                       | 备注
+-------+------------------------------------------------------------------------------------+------------------------------------------+------------------------------------------------+
  1     | asset-198.51.100.3                                           | 198.51.100.3                             |
  2     | asset-198.51.100.30                                          | 198.51.100.30                            |
  3     | asset-198.51.100.31                                          | 198.51.100.31                            |
页码：1，每页行数：9，总页数：1，总数量：3
Opt>
"#;

        assert_eq!(
            select_exact_asset_id(text, "198.51.100.3").unwrap(),
            Some("1".to_string())
        );
        assert_eq!(select_exact_asset_id(text, "198.51.100.4").unwrap(), None);
    }

    #[test]
    fn parses_ansi_paginated_asset_table() {
        let text = "\u{1b}[H\u{1b}[2J  \u{1b}[1;32mID\u{1b}[0m | \u{1b}[1;32m主机名\u{1b}[0m | \u{1b}[1;32mIP\u{1b}[0m | 备注\n\
  1  | bei....30 | 198.51.100.30 | \n\
\u{1b}[32m页码：1，每页行数：1，总页数：3，总数量：3\u{1b}[0m\n\
上一页：P/p  下一页：Enter|N/n  返回：B/b\n:";

        assert_eq!(
            select_exact_asset_id(text, "198.51.100.30").unwrap(),
            Some("1".to_string())
        );
        assert_eq!(
            parse_page_status(text),
            Some(PageStatus {
                current: 1,
                total: 3
            })
        );
        assert!(contains_page_prompt(text));
    }

    #[test]
    fn duplicate_exact_asset_ids_are_rejected() {
        let text = "\
  1 | host-a | 10.0.0.1 | \n\
  2 | host-b | 10.0.0.1 | \n\
页码：1，每页行数：9，总页数：1，总数量：2\n\
Opt>";

        let error = select_exact_asset_id(text, "10.0.0.1").unwrap_err();
        assert!(error.to_string().contains("returned 2 exact matches"));
    }

    #[test]
    fn explicit_no_tty_is_unsupported_without_connecting() {
        let gateway = test_gateway();
        let (sender, _rx) = mpsc::unbounded_channel();
        let request = ExecRequest {
            argv: vec!["true".to_string()],
            sender,
            tty: false,
            tty_intent: FlagIntent::Disable,
            cols: 0,
            rows: 0,
            shell: String::new(),
            no_shell: false,
            timeout_ms: 0,
            stdin: false,
            stdin_intent: FlagIntent::Default,
            stdin_rx: std::sync::Mutex::new(None),
        };

        let error = gateway.validate_exec_request(&request).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert!(error.to_string().contains("--no-tty is not supported"));
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
