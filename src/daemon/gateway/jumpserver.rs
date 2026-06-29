// JumpserverGateway implementation.
//
// A jumpserver is a partial backend: it supports EXEC, COPY (sftp-over-PTY), and
// PROXY, but not LIST. It reuses one authenticated SSH transport (the expensive
// MFA handshake is paid once) and maintains a session cache of navigated PTY
// shells keyed by target IP. On a cache hit, the ~3-5s menu navigation is
// skipped entirely — the cached shell (still at the asset prompt from a prior
// exec) is handed directly to a new JumpserverSession. Shells are returned to
// the cache after a successful exec; interactive shell and sftp subsystem
// consume the shell (raw passthrough closes the channel) and are not cached.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::{debug, info};

use crate::config::{AppConfig, JumpserverGatewayConfig, MfaConfig, load_server_config};
use crate::daemon::connection_manager::{
    ConnectionStatusSnapshot, ManagedSingleton, SingletonLease,
};
use crate::daemon::jumpserver_engine::{
    MFA_PROMPT_CONTAINS, PtyShell, SHELL_PROMPT_SUFFIXES, contains_menu_prompt,
    contains_page_prompt, parse_asset_rows, parse_page_status, request_default_pty,
    select_exact_asset_id, strip_ansi,
};
use crate::daemon::resolver::derive_target_ip;
use crate::daemon::session::TargetSession;
use crate::daemon::session::jumpserver::JumpserverSession;
use crate::daemon::shell::{build_final_command, resolve_shell};

use super::auth::{AuthPrompt, AuthPrompter, ClientHandler, authenticate_with_key, connect_handle};
use super::{Capabilities, ErrorKind, Gateway, GatewayError, GatewayKind, is_transport_error};

type JumpserverTransport = AsyncMutex<JumpserverTransportState>;

// ---------------------------------------------------------------------------
// Session cache — reuse navigated PTY shells across operations
// ---------------------------------------------------------------------------

/// BTreeMap key: `(last_used, unique_id)`. Ordered ascending by time, so the
/// globally oldest shell is at the front for O(log n) LRU eviction. The
/// `unique_id` tiebreaker guarantees uniqueness when two shells share an
/// `Instant`.
type ShellKey = (Instant, u64);

struct CachedShellEntry {
    ip: String,
    shell: PtyShell,
    /// Clone of the transport lease — keeps the SSH transport alive while the
    /// shell is cached, preventing the singleton from being pruned.
    _transport_lease: SingletonLease<JumpserverTransport>,
}

/// LRU cache of navigated PTY shells, keyed by `(Instant, u64)` for global LRU
/// ordering, with a reverse index (`HashMap<ip, Vec<ShellKey>>`) for O(log n)
/// checkout by target IP.
///
/// All operations are O(log n) or better:
/// - **checkout**: reverse-index lookup + BTreeMap remove → O(log n)
/// - **return**: BTreeMap insert (+ evict oldest if at capacity) → O(log n)
/// - **prune**: sequential `pop_first` of expired entries → O(expired × log n)
///
/// Stale keys in the reverse index (from evicted entries) are cleaned up
/// lazily during checkout and rebuilt periodically during prune.
struct SessionCache {
    /// All cached shells, globally ordered by `last_used` (oldest first).
    shells: BTreeMap<ShellKey, CachedShellEntry>,
    /// Reverse index: target IP → keys of cached shells for that IP.
    by_ip: HashMap<String, Vec<ShellKey>>,
    next_id: u64,
    max_cached_sessions: Option<usize>,
}

impl SessionCache {
    fn new(max_cached_sessions: Option<usize>) -> Self {
        Self {
            shells: BTreeMap::new(),
            by_ip: HashMap::new(),
            next_id: 0,
            max_cached_sessions,
        }
    }

    /// Total number of cached shells across all IPs.
    fn len(&self) -> usize {
        self.shells.len()
    }

    /// Take any cached shell for `ip`. Stale keys (from evicted entries) are
    /// skipped lazily. Returns `None` if no live shell is available.
    fn checkout(&mut self, ip: &str) -> Option<PtyShell> {
        let keys = self.by_ip.get_mut(ip)?;
        while let Some(key) = keys.pop() {
            if let Some(entry) = self.shells.remove(&key) {
                if keys.is_empty() {
                    self.by_ip.remove(ip);
                }
                return Some(entry.shell);
            }
        }
        self.by_ip.remove(ip);
        None
    }

    /// Insert a shell into the cache. If at capacity, evict the globally oldest
    /// shell first. The evicted entry's key stays stale in the reverse index and
    /// is cleaned up lazily on checkout or during prune.
    fn insert(
        &mut self,
        ip: String,
        shell: PtyShell,
        lease: SingletonLease<JumpserverTransport>,
    ) {
        if let Some(max) = self.max_cached_sessions {
            while self.shells.len() >= max {
                self.shells.pop_first();
            }
        }
        let now = Instant::now();
        let id = self.next_id;
        self.next_id += 1;
        let key = (now, id);
        self.shells.insert(
            key,
            CachedShellEntry {
                ip: ip.clone(),
                shell,
                _transport_lease: lease,
            },
        );
        self.by_ip.entry(ip).or_default().push(key);
    }

    /// Remove all cached shells whose `last_used` exceeds `idle_timeout`.
    /// BTreeMap is ordered by time, so we pop from the front until we hit a
    /// non-expired entry. Rebuilds the reverse index afterwards.
    fn prune(&mut self, idle_timeout: Duration) {
        let mut changed = false;
        while let Some((&key, _)) = self.shells.first_key_value() {
            if key.0.elapsed() > idle_timeout {
                self.shells.pop_first();
                changed = true;
            } else {
                break;
            }
        }
        if changed {
            self.by_ip.clear();
            for (&key, entry) in &self.shells {
                self.by_ip.entry(entry.ip.clone()).or_default().push(key);
            }
        }
    }

    /// Clear the entire cache (used on transport invalidation).
    fn clear(&mut self) {
        self.shells.clear();
        self.by_ip.clear();
    }
}

pub struct JumpserverGateway {
    gateway_name: String,
    config: Arc<RwLock<AppConfig>>,
    fields: JumpserverGatewayConfig,
    auth_prompter: Arc<AuthPrompter>,
    transport: ManagedSingleton<JumpserverTransport>,
    max_idle_time: Duration,
    session_cache: Arc<Mutex<SessionCache>>,
    session_idle_timeout: Duration,
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
        let session_idle_timeout = fields.session_idle_timeout;
        let max_cached_sessions = fields.max_cached_sessions;
        Self {
            gateway_name,
            config,
            fields,
            auth_prompter,
            transport: ManagedSingleton::new(),
            max_idle_time,
            session_cache: Arc::new(Mutex::new(SessionCache::new(max_cached_sessions))),
            session_idle_timeout,
        }
    }

    /// Resolve the configured TOTP secret to its plaintext base32 value. An empty
    /// configured value means "no static MFA secret" and short-circuits.
    async fn resolve_totp_secret(&self) -> Result<String> {
        if self.fields.totp_secret_base32.is_empty() {
            return Ok(String::new());
        }
        let resolver = self
            .config
            .read()
            .await
            .secret_resolver(Some(&self.fields.identity_file));
        let value = crate::config::Secret::from_reference(&self.fields.totp_secret_base32)
            .resolve(&resolver)
            .context("failed to resolve jumpserver TOTP secret")?;
        Ok(value.to_string())
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
                    debug!(gateway = %self.gateway_name,
                        "transport error creating jumpserver connection, retrying: {}", e);
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("jumpserver transport checkout loop is bounded")
    }

    async fn establish_transport(&self) -> Result<JumpserverTransportState> {
        let app_config = self.config.read().await.clone();
        let mut handle = connect_handle(&self.fields.host, self.fields.port, &app_config).await?;

        let totp_secret = self.resolve_totp_secret().await?;
        let mfa_config = MfaConfig {
            totp_secret_base32: totp_secret,
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
            debug!(gateway = %self.gateway_name, generation = %generation,
                "discarded jumpserver SSH transport, will reconnect on next use");
        }
        // All cached shells are on the invalidated transport — discard them.
        self.session_cache.lock().clear();
    }

    /// Open a fresh PTY channel on the shared transport and navigate the asset
    /// menu to `target`'s shell prompt. Returns the navigated shell.
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

        self.navigate_to_asset(&mut shell, target)
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

    /// Acquire a navigated PtyShell for `target`, using cache if available.
    /// Returns the shell and its transport lease. On transport error, retries once.
    async fn acquire_shell(
        &self,
        target: &str,
    ) -> Result<(PtyShell, SingletonLease<JumpserverTransport>), GatewayError> {
        let ip = derive_target_ip(target);
        for attempt in 0..=1 {
            let lease = self.ensure_transport().await?;

            // Try cache first (sync lock, no await while held).
            if let Some(shell) = self.session_cache.lock().checkout(&ip) {
                debug!(gateway = %self.gateway_name, ip = %ip,
                    "session cache HIT — reusing navigated shell");
                return Ok((shell, lease));
            }

            // Cache miss — open a fresh PTY channel and navigate the menu.
            match self.open_target_shell(&lease, target).await {
                Ok(shell) => return Ok((shell, lease)),
                Err(e) if attempt == 0 && matches!(e.kind, ErrorKind::Transport) => {
                    debug!(gateway = %self.gateway_name, target = %target,
                        generation = %lease.generation(),
                        "transport error preparing jumpserver shell, retrying: {}", e);
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

    /// Acquire the transport and navigate to the target, retrying once on a
    /// transport-level failure (stale handle). Checks the session cache first —
    /// a cache hit skips the ~3-5s menu navigation entirely.
    async fn open_session_inner(
        &self,
        target: &str,
    ) -> Result<Box<dyn TargetSession>, GatewayError> {
        let ip = derive_target_ip(target);
        let (shell, lease) = self.acquire_shell(target).await?;
        let return_fn = self.make_return_fn(ip, lease.clone());
        let guard: Box<dyn Send> = Box::new(lease);
        Ok(Box::new(JumpserverSession::new(shell, guard, Some(return_fn))) as Box<dyn TargetSession>)
    }

    /// Build the closure invoked by `JumpserverSession::exec` when the command
    /// completes successfully. The surviving shell (still at the asset prompt)
    /// is returned to the session cache for future reuse. The cloned lease keeps
    /// the transport alive while the shell is cached.
    fn make_return_fn(
        &self,
        ip: String,
        lease: SingletonLease<JumpserverTransport>,
    ) -> Box<dyn FnOnce(PtyShell) + Send> {
        let cache = self.session_cache.clone();
        Box::new(move |shell| {
            let mut cache = cache.lock();
            cache.insert(ip, shell, lease);
        })
    }

    async fn effective_shell(&self, cli_shell: &str, no_shell: bool) -> String {
        let cli_shell = (!cli_shell.is_empty()).then_some(cli_shell);
        let defaults_shell = {
            let config = self.config.read().await;
            load_server_config(Path::new(&config.ssh.server_config_path))
                .map(|server_config| server_config.defaults.shell)
                .unwrap_or_default()
        };
        resolve_shell(cli_shell, no_shell, None, &defaults_shell).unwrap_or_default()
    }

    /// Drive the bastion menu state machine to the asset shell prompt:
    /// MFA → search by IP → exact asset selection (with pagination) → prompt.
    /// Finishes by disabling echo so command output is clean.
    async fn navigate_to_asset(&self, shell: &mut PtyShell, target: &str) -> Result<()> {
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
                let totp_secret = self.resolve_totp_secret().await?;
                let code = if !totp_secret.is_empty() {
                    let mfa_config = MfaConfig {
                        totp_secret_base32: totp_secret,
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
                debug!(gateway = %self.gateway_name, target = %target, ip = %ip,
                    "jumpserver menu detected, selecting target");
                shell.write_line(&ip).await?;
                shell.clear_pending();
                search_sent = true;
                continue;
            }

            if search_sent && !asset_id_sent {
                if let Some(asset_id) = select_exact_asset_id(&text, &ip)? {
                    debug!(gateway = %self.gateway_name, target = %target, ip = %ip,
                        asset_id = %asset_id, "jumpserver asset table matched exact IP");
                    shell.write_line(&asset_id).await?;
                    shell.clear_pending();
                    asset_id_sent = true;
                    continue;
                }

                if contains_page_prompt(&text) {
                    match parse_page_status(&text) {
                        Some(status) if status.current < status.total => {
                            debug!(gateway = %self.gateway_name, target = %target, ip = %ip,
                                page = %status.current, total_pages = %status.total,
                                "jumpserver asset table did not contain exact IP, advancing page");
                            shell.write_line("").await?;
                            shell.clear_pending();
                            continue;
                        }
                        Some(status) => bail!(
                            "jumpserver asset search for {} did not find an exact IP match after {} page(s)",
                            ip,
                            status.total
                        ),
                        None => bail!(
                            "jumpserver asset search for {} showed a paginated table but page status could not be parsed",
                            ip
                        ),
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
    fn name(&self) -> &str {
        &self.gateway_name
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Jumpserver
    }

    fn capabilities(&self) -> Capabilities {
        // No LIST: the bastion exposes no machine-readable inventory.
        Capabilities::EXEC | Capabilities::COPY | Capabilities::PROXY
    }

    async fn open_exec_session(
        &self,
        target: &str,
        argv: &[String],
        shell: &str,
        no_shell: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError> {
        let eff_shell = self.effective_shell(shell, no_shell).await;
        let command = build_final_command(argv, &eff_shell);
        let session = self.open_session_inner(target).await?;
        Ok((session, command))
    }

    async fn open_session(&self, target: &str) -> Result<Box<dyn TargetSession>, GatewayError> {
        self.open_session_inner(target).await
    }

    async fn copy(
        &self,
        target: &str,
        mut spec: crate::types::CopySpec,
    ) -> Result<(), GatewayError> {
        let ip = derive_target_ip(target);
        let (mut shell, lease) = self.acquire_shell(target).await?;

        let result = crate::daemon::session::shell_copy::run(&mut shell, &mut spec).await;

        if result.is_ok() {
            // Shell is still at the prompt after shell-based copy — return it
            // to the cache for reuse by future operations.
            let return_fn = self.make_return_fn(ip, lease.clone());
            return_fn(shell);
        }
        // On error the shell is dropped (not cached).

        result.map_err(|e| GatewayError::execution(e))
    }

    async fn prune_idle(&self) {
        let _ = self.transport.prune_idle(self.max_idle_time).await;
        self.session_cache
            .lock()
            .prune(self.session_idle_timeout);
    }

    async fn pool_status(&self) -> Vec<ConnectionStatusSnapshot> {
        let generation = self.transport.current_generation().await.unwrap_or(0);
        let idle = self.session_cache.lock().len();
        let capacity = self.fields.max_cached_sessions.unwrap_or(0);
        vec![ConnectionStatusSnapshot {
            key: format!("{}:sessions", self.gateway_name),
            generation,
            active: 0,
            idle,
            capacity,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::jumpserver_engine::PageStatus;

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
                max_cached_sessions: None,
                session_idle_timeout: Duration::from_secs(300),
            },
            Arc::new(|_| Box::pin(async { Ok(String::new()) })),
            Duration::from_secs(60),
        )
    }

    #[test]
    fn jumpserver_capabilities_exclude_list() {
        let gateway = test_gateway();
        let caps = gateway.capabilities();
        assert!(caps.contains(Capabilities::EXEC));
        assert!(caps.contains(Capabilities::COPY));
        assert!(caps.contains(Capabilities::PROXY));
        assert!(!caps.contains(Capabilities::LIST));
    }

    #[test]
    fn engine_parses_asset_table() {
        let text = "\
  1 | host-a | 10.0.0.1 | \n\
  2 | host-b | 10.0.0.2 | \n\
页码：1，每页行数：9，总页数：1，总数量：2\n\
Opt>";
        assert_eq!(
            select_exact_asset_id(text, "10.0.0.2").unwrap(),
            Some("2".to_string())
        );
        assert_eq!(
            parse_page_status(text),
            Some(PageStatus {
                current: 1,
                total: 1
            })
        );
    }

    #[tokio::test]
    async fn list_servers_is_unsupported_without_connecting() {
        let gateway = test_gateway();
        let result = gateway.list_servers().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, ErrorKind::Unsupported);
        assert_eq!(gateway.transport.current_generation().await, None);
    }
}
