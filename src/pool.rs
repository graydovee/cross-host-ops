use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, anyhow};
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, mpsc::UnboundedSender};
use tracing::info;

use crate::config::AppConfig;
use crate::connection::{AuthPrompter, CopySpec};
use crate::jump::{JumpHost, JumpHostKind};
use crate::jump::direct::DirectJumpHost;
use crate::jump::factory::build_jump_host;
use crate::jump::types::{EndTargetId, TargetRoute};
use crate::protocol::PoolStatus;
use crate::protocol::ServerEvent;

/// Key used to look up a sub-pool in the connection pool.
///
/// Direct routes are keyed by end-target so two direct SSH connections to the
/// same end target share a slot, but different end targets do not.
///
/// All other kinds are keyed solely by jump-host name. The same name always
/// shares a slot; the pool transparently multiplexes requests for different end
/// targets on it.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum PoolKey {
    /// Direct routes have no jump-host name; key by end-target so two direct
    /// SSH connections to the same end target share a slot, but different end
    /// targets do not.
    Direct { end_target: EndTargetId },
    /// Every other kind is keyed solely by jump-host name. The same name
    /// always shares a slot; the pool transparently multiplexes requests for
    /// different end targets on it.
    Aliased { name: String, kind: JumpHostKind },
}

impl std::fmt::Display for PoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolKey::Direct { end_target } => write!(f, "direct:{}", end_target.0),
            PoolKey::Aliased { name, kind } => write!(f, "{}:{}", kind, name),
        }
    }
}

/// Derive a `PoolKey` from a `TargetRoute`.
fn pool_key_for_target(route: &TargetRoute) -> PoolKey {
    if route.hops.is_empty() {
        PoolKey::Direct {
            end_target: route.end_target.id.clone(),
        }
    } else {
        let hop = &route.hops[0];
        PoolKey::Aliased {
            name: hop.name.clone(),
            kind: hop.kind,
        }
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    config: Arc<RwLock<AppConfig>>,
    pools: Arc<Mutex<HashMap<PoolKey, Arc<TargetPool>>>>,
}

impl ConnectionPool {
    pub fn new(config: Arc<RwLock<AppConfig>>) -> Self {
        Self {
            config,
            pools: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn execute(
        &self,
        targets: Vec<TargetRoute>,
        argv: Vec<String>,
        sender: UnboundedSender<ServerEvent>,
        auth_prompter: Arc<AuthPrompter>,
    ) -> Result<i32> {
        let target = targets
            .first()
            .ok_or_else(|| anyhow!("no resolved targets available"))?;
        let pool = self.get_or_create_pool(target);
        let key_display = pool_key_for_target(target).to_string();
        let slot = pool.acquire(self.config.clone()).await?;
        let result = async {
            let mut guard = slot.hop.hop.lock().await;
            if guard.is_none() {
                *guard = Some(self.open_any_jump_host(&targets, auth_prompter.clone()).await?);
            }
            let config = self.config.read().await.clone();
            let first_result = guard
                .as_mut()
                .expect("hop initialized")
                .exec(&argv, &sender, &config)
                .await;
            match first_result {
                Ok(code) => Ok(code),
                Err(error) if classify_transport_error(&error) == ErrorClass::Transport => {
                    info!(target = %key_display, error = %error, "reopening stale pooled connection");
                    *guard = None;
                    *guard = Some(self.open_any_jump_host(&targets, auth_prompter.clone()).await?);
                    let config = self.config.read().await.clone();
                    guard
                        .as_mut()
                        .expect("hop reinitialized")
                        .exec(&argv, &sender, &config)
                        .await
                }
                Err(error) => Err(error),
            }
        }
        .await;
        pool.release(slot.id);
        result
    }

    pub async fn copy(
        &self,
        targets: Vec<TargetRoute>,
        spec: CopySpec,
        auth_prompter: Arc<AuthPrompter>,
    ) -> Result<()> {
        let target = targets
            .first()
            .ok_or_else(|| anyhow!("no resolved targets available"))?;
        let pool = self.get_or_create_pool(target);
        let key_display = pool_key_for_target(target).to_string();
        let slot = pool.acquire(self.config.clone()).await?;
        let result = async {
            let mut guard = slot.hop.hop.lock().await;
            if guard.is_none() {
                *guard = Some(self.open_any_jump_host(&targets, auth_prompter.clone()).await?);
            }
            let config = self.config.read().await.clone();
            let first_result = guard
                .as_mut()
                .expect("hop initialized")
                .copy(&spec, &config)
                .await;
            match first_result {
                Ok(()) => Ok(()),
                Err(error) if classify_transport_error(&error) == ErrorClass::Transport => {
                    info!(target = %key_display, error = %error, "reopening stale pooled connection");
                    *guard = None;
                    *guard = Some(self.open_any_jump_host(&targets, auth_prompter.clone()).await?);
                    let config = self.config.read().await.clone();
                    guard
                        .as_mut()
                        .expect("hop reinitialized")
                        .copy(&spec, &config)
                        .await
                }
                Err(error) => Err(error),
            }
        }
        .await;
        pool.release(slot.id);
        result
    }

    /// Open a JumpHost connection by trying each target route candidate in order.
    /// Uses the factory to build JumpHost instances from TargetRoute.
    async fn open_any_jump_host(
        &self,
        targets: &[TargetRoute],
        auth_prompter: Arc<AuthPrompter>,
    ) -> Result<Box<dyn JumpHost>> {
        let config = self.config.read().await.clone();
        let mut last_error = None;
        for route in targets {
            let key_display = pool_key_for_target(route).to_string();
            info!(target = %key_display, "opening candidate connection");
            match self.build_jump_host_for_route(route, &auth_prompter, &config).await {
                Ok(hop) => return Ok(hop),
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("failed to open any candidate connection")))
    }

    /// Build a JumpHost from a TargetRoute by looking up the JumpHostConfig
    /// for the first hop, or building a DirectJumpHost for direct routes.
    async fn build_jump_host_for_route(
        &self,
        route: &TargetRoute,
        auth_prompter: &Arc<AuthPrompter>,
        config: &AppConfig,
    ) -> Result<Box<dyn JumpHost>> {
        if route.hops.is_empty() {
            // Direct route: build a DirectJumpHost from SSH config
            let target = crate::connection::resolver::resolve_direct_target_for_route(
                &route.end_target.alias,
                config,
            )?;
            let connection = crate::connection::DirectSshConnection::connect(
                &target,
                config,
                auth_prompter.as_ref(),
            )
            .await?;
            Ok(Box::new(DirectJumpHost::new(
                route.end_target.alias.clone(),
                connection,
            )))
        } else {
            // Aliased route: look up the JumpHostConfig by name and use the factory
            let hop = &route.hops[0];
            let jh_config = config
                .jump_hosts
                .iter()
                .find(|jh| jh.name == hop.name)
                .ok_or_else(|| anyhow!("jump host '{}' not found in config", hop.name))?;
            build_jump_host(jh_config, route.end_target.alias.as_str(), auth_prompter, config).await
        }
    }

    pub async fn prune_idle(&self) {
        let config = self.config.read().await.clone();
        let mut remove_keys = Vec::new();
        let pools = self.pools.lock();
        for (key, pool) in pools.iter() {
            pool.prune_idle(config.ssh.max_idle_time);
            if pool.is_empty() {
                remove_keys.push(key.clone());
            }
        }
        drop(pools);
        if !remove_keys.is_empty() {
            let mut pools = self.pools.lock();
            for key in remove_keys {
                pools.remove(&key);
            }
        }
    }

    pub fn status(&self) -> Vec<PoolStatus> {
        let pools = self.pools.lock();
        pools.values().map(|pool| pool.status()).collect()
    }

    fn get_or_create_pool(&self, route: &TargetRoute) -> Arc<TargetPool> {
        let key = pool_key_for_target(route);
        let mut pools = self.pools.lock();
        pools
            .entry(key.clone())
            .or_insert_with(|| Arc::new(TargetPool::new(key)))
            .clone()
    }
}

struct TargetPool {
    key: PoolKey,
    state: Mutex<TargetPoolState>,
    notify: Notify,
}

impl TargetPool {
    fn new(key: PoolKey) -> Self {
        Self {
            key,
            state: Mutex::new(TargetPoolState {
                slots: Vec::new(),
                waiters: 0,
                next_id: 1,
            }),
            notify: Notify::new(),
        }
    }

    async fn acquire(&self, config: Arc<RwLock<AppConfig>>) -> Result<Lease> {
        loop {
            if let Some(lease) = self.try_acquire(&config).await? {
                return Ok(lease);
            }
            let waiter = {
                let mut state = self.state.lock();
                state.waiters += 1;
                self.notify.notified()
            };
            waiter.await;
            let mut state = self.state.lock();
            state.waiters = state.waiters.saturating_sub(1);
        }
    }

    async fn try_acquire(&self, config: &Arc<RwLock<AppConfig>>) -> Result<Option<Lease>> {
        let cfg = config.read().await.clone();
        self.prune_idle(cfg.ssh.max_idle_time);
        let mut create = None;
        {
            let mut state = self.state.lock();
            for slot in &mut state.slots {
                if !slot.busy {
                    slot.busy = true;
                    return Ok(Some(Lease {
                        id: slot.id,
                        hop: slot.hop.clone(),
                    }));
                }
            }
            if state.slots.len() < cfg.ssh.max_connections_per_ip {
                let id = state.next_id;
                state.next_id += 1;
                let hop = Arc::new(PooledJumpHost {
                    hop: AsyncMutex::new(None),
                });
                state.slots.push(SlotState {
                    id,
                    busy: true,
                    last_idle: Instant::now(),
                    hop: hop.clone(),
                });
                create = Some(Lease { id, hop });
            }
        }
        Ok(create)
    }

    fn release(&self, id: usize) {
        let mut state = self.state.lock();
        if let Some(slot) = state.slots.iter_mut().find(|slot| slot.id == id) {
            slot.busy = false;
            slot.last_idle = Instant::now();
        }
        self.notify.notify_one();
    }

    fn prune_idle(&self, max_idle_time: std::time::Duration) {
        let now = Instant::now();
        let mut state = self.state.lock();
        let key_display = self.key.to_string();
        state.slots.retain(|slot| {
            let expired = !slot.busy && now.duration_since(slot.last_idle) >= max_idle_time;
            if expired {
                info!(target = %key_display, slot_id = slot.id, "closing idle pooled connection");
            }
            !expired
        });
    }

    fn status(&self) -> PoolStatus {
        let state = self.state.lock();
        let total = state.slots.len();
        let busy = state.slots.iter().filter(|slot| slot.busy).count();
        let idle = total.saturating_sub(busy);
        PoolStatus {
            key: self.key.to_string(),
            total,
            busy,
            idle,
            queued: state.waiters,
        }
    }

    fn is_empty(&self) -> bool {
        self.state.lock().slots.is_empty()
    }
}

struct TargetPoolState {
    slots: Vec<SlotState>,
    waiters: usize,
    next_id: usize,
}

struct SlotState {
    id: usize,
    busy: bool,
    last_idle: Instant,
    hop: Arc<PooledJumpHost>,
}

struct PooledJumpHost {
    hop: AsyncMutex<Option<Box<dyn JumpHost>>>,
}

struct Lease {
    id: usize,
    hop: Arc<PooledJumpHost>,
}

/// Classification of an error for retry decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    /// Transport-level failure (connection lost, gRPC channel broken, SSH error).
    /// The pool should retry exactly once on a fresh connection.
    Transport,
    /// Application-level failure (permission denied, command not found, etc.).
    /// The pool should return the error immediately without retrying.
    Application,
}

/// Classify an `anyhow::Error` as either a transport-level or application-level error.
///
/// Transport errors trigger a single reconnect attempt; application errors are
/// returned immediately to the caller.
///
/// The classification checks, in order:
/// 1. `tonic::Status` with codes `Unavailable`, `Cancelled`, `Unknown`, or `Internal`.
/// 2. Any `russh::Error` variant (SSH-level failures are always transport).
/// 3. Legacy string heuristic for common connection-closed messages.
pub fn classify_transport_error(error: &anyhow::Error) -> ErrorClass {
    // Check for tonic::Status with transport-indicative codes
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        match status.code() {
            tonic::Code::Unavailable
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::Internal => return ErrorClass::Transport,
            _ => {}
        }
    }

    // Check for russh::Error (any variant is a transport-level SSH failure)
    if error.downcast_ref::<russh::Error>().is_some() {
        return ErrorClass::Transport;
    }

    // Fall back to the string heuristic for errors that don't carry typed context
    if should_reconnect_by_message(&error.to_string()) {
        return ErrorClass::Transport;
    }

    ErrorClass::Application
}

/// Legacy string-based heuristic for detecting transport failures from error messages.
fn should_reconnect_by_message(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("channel closed")
        || lowered.contains("channel send error")
        || lowered.contains("closed unexpectedly")
        || lowered.contains("broken pipe")
        || lowered.contains("connection reset")
        || lowered.contains("connection aborted")
        || lowered.contains("send error")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Feature: rhopd-jumpserver-architecture, Property 6: Pool reuse invariant

    /// Operations that can be performed against a TargetPool.
    #[derive(Clone, Debug)]
    enum PoolOp {
        Acquire,
        /// Release the slot at the given index in our held-leases vec.
        Release(usize),
        /// Prune idle slots (simulates the reaper tick).
        Prune,
    }

    /// Strategy to generate a sequence of pool operations.
    fn arb_pool_ops(max_ops: usize) -> impl Strategy<Value = Vec<PoolOp>> {
        proptest::collection::vec(
            prop_oneof![
                3 => Just(PoolOp::Acquire),
                2 => (0..10usize).prop_map(PoolOp::Release),
                1 => Just(PoolOp::Prune),
            ],
            1..=max_ops,
        )
    }

    /// Strategy to generate a max_connections_per_ip value (1..=8 to keep tests fast).
    fn arb_max_connections() -> impl Strategy<Value = usize> {
        1..=8usize
    }

    /// Strategy to generate arbitrary TargetRoute values for pool-key testing.
    fn arb_target_route() -> impl Strategy<Value = TargetRoute> {
        let arb_key = "[a-zA-Z0-9_]{1,20}";
        let arb_kind = prop_oneof![
            Just(JumpHostKind::Direct),
            Just(JumpHostKind::Jumpserver),
            Just(JumpHostKind::Rhopd),
        ];
        prop_oneof![
            // Direct route (empty hops)
            arb_key.prop_map(|alias| TargetRoute {
                hops: vec![],
                end_target: crate::jump::types::EndTarget {
                    id: EndTargetId(format!("target:{}", alias)),
                    alias,
                },
            }),
            // Aliased route (one hop)
            (arb_key.clone(), arb_kind, arb_key).prop_map(|(hop_name, kind, alias)| TargetRoute {
                hops: vec![crate::jump::types::JumpHopRef {
                    name: hop_name,
                    kind,
                }],
                end_target: crate::jump::types::EndTarget {
                    id: EndTargetId(format!("target:{}", alias)),
                    alias,
                },
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.7, 4.3, 5.1, 5.2, 5.3, 5.4, 7.5**
        ///
        /// For any sequence of acquire/release/prune operations against a single
        /// PoolKey, the pool satisfies:
        /// 1. Idle slots are reused before creating new ones.
        /// 2. Live slot count never exceeds max_connections_per_ip.
        /// 3. PoolKey is a pure function of route's first hop alias + kind for
        ///    non-direct routes, or end_target_id for direct routes.
        #[test]
        fn prop_pool_reuse_invariant(
            ops in arb_pool_ops(30),
            max_conns in arb_max_connections(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Build a config with the generated max_connections_per_ip and
                // a very large max_idle_time so prune only fires when we want.
                let mut config = AppConfig::default();
                config.ssh.max_connections_per_ip = max_conns;
                // Use a zero idle time for prune operations so they always prune idle slots.
                config.ssh.max_idle_time = std::time::Duration::from_secs(0);
                let config = Arc::new(RwLock::new(config));

                let key = PoolKey::Aliased {
                    name: "test-host".to_string(),
                    kind: JumpHostKind::Jumpserver,
                };
                let pool = TargetPool::new(key);

                // Track held leases (slot ids that are currently acquired/busy).
                let mut held_leases: Vec<usize> = Vec::new();
                // Track the total number of unique slot ids ever created.
                let mut created_ids: Vec<usize> = Vec::new();

                for op in &ops {
                    match op {
                        PoolOp::Acquire => {
                            let result = pool.try_acquire(&config).await.unwrap();
                            if let Some(lease) = result {
                                // Check invariant 1: if we got a brand new slot
                                // (not in created_ids), there should have been no
                                // idle slots available — they would have been
                                // returned instead of creating a new one.
                                if !created_ids.contains(&lease.id) {
                                    // This is a newly created slot. After acquire,
                                    // the slot is busy, so verify all other slots
                                    // are also busy (no idle slots were skipped).
                                    let state = pool.state.lock();
                                    let other_idle = state.slots.iter()
                                        .filter(|s| s.id != lease.id && !s.busy)
                                        .count();
                                    prop_assert_eq!(
                                        other_idle, 0,
                                        "New slot created but idle slots exist"
                                    );
                                    drop(state);
                                    created_ids.push(lease.id);
                                }

                                held_leases.push(lease.id);
                            }
                            // If result is None, pool is at capacity — that's fine.
                        }
                        PoolOp::Release(idx) => {
                            if !held_leases.is_empty() {
                                let actual_idx = idx % held_leases.len();
                                let id = held_leases.remove(actual_idx);
                                pool.release(id);
                            }
                        }
                        PoolOp::Prune => {
                            // Small sleep to ensure idle time has elapsed (we set
                            // max_idle_time to 0, so any idle slot is prunable).
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            let cfg = config.read().await.clone();
                            pool.prune_idle(cfg.ssh.max_idle_time);
                            // Remove pruned slot ids from created_ids.
                            let state = pool.state.lock();
                            let live_ids: Vec<usize> = state.slots.iter().map(|s| s.id).collect();
                            drop(state);
                            created_ids.retain(|id| live_ids.contains(id));
                            // Also remove from held_leases any that were pruned
                            // (shouldn't happen since prune only removes !busy slots,
                            // but be safe).
                            held_leases.retain(|id| live_ids.contains(id));
                        }
                    }

                    // Invariant 2: live slot count never exceeds max_connections_per_ip.
                    let state = pool.state.lock();
                    prop_assert!(
                        state.slots.len() <= max_conns,
                        "Slot count {} exceeds max_connections_per_ip {}",
                        state.slots.len(),
                        max_conns
                    );
                    drop(state);
                }

                Ok(())
            })?;
        }

        // Feature: remove-deprecated-jumpserver-config, Property 6: PoolKey derivation from TargetRoute
        /// **Validates: Requirements 7.3**
        ///
        /// PoolKey derivation is deterministic: for non-direct routes it's
        /// Aliased { name, kind } from the first hop; for direct routes it's
        /// Direct { end_target_id }. Same input always produces the same key.
        #[test]
        fn prop_pool_key_deterministic(
            route in arb_target_route(),
        ) {
            let key1 = pool_key_for_target(&route);
            let key2 = pool_key_for_target(&route);

            // Same input always produces the same key.
            prop_assert_eq!(&key1, &key2, "pool_key_for_target is not deterministic");

            // Verify the key structure matches the route type.
            if route.hops.is_empty() {
                match &key1 {
                    PoolKey::Direct { end_target } => {
                        prop_assert_eq!(
                            &end_target.0, &route.end_target.id.0,
                            "Direct pool key should use route end_target.id"
                        );
                    }
                    PoolKey::Aliased { .. } => {
                        prop_assert!(false, "Empty hops should produce PoolKey::Direct");
                    }
                }
            } else {
                match &key1 {
                    PoolKey::Aliased { name, kind } => {
                        prop_assert_eq!(
                            name, &route.hops[0].name,
                            "Aliased pool key name should match first hop name"
                        );
                        prop_assert_eq!(
                            *kind, route.hops[0].kind,
                            "Aliased pool key kind should match first hop kind"
                        );
                    }
                    PoolKey::Direct { .. } => {
                        prop_assert!(false, "Non-empty hops should produce PoolKey::Aliased");
                    }
                }
            }
        }
    }

    #[test]
    fn classifies_tonic_unavailable_as_transport() {
        let err: anyhow::Error = tonic::Status::unavailable("service unavailable").into();
        assert_eq!(classify_transport_error(&err), ErrorClass::Transport);
    }

    #[test]
    fn classifies_tonic_cancelled_as_transport() {
        let err: anyhow::Error = tonic::Status::cancelled("request cancelled").into();
        assert_eq!(classify_transport_error(&err), ErrorClass::Transport);
    }

    #[test]
    fn classifies_tonic_unknown_as_transport() {
        let err: anyhow::Error = tonic::Status::unknown("unknown error").into();
        assert_eq!(classify_transport_error(&err), ErrorClass::Transport);
    }

    #[test]
    fn classifies_tonic_internal_as_transport() {
        let err: anyhow::Error = tonic::Status::internal("internal error").into();
        assert_eq!(classify_transport_error(&err), ErrorClass::Transport);
    }

    #[test]
    fn classifies_tonic_permission_denied_as_application() {
        let err: anyhow::Error = tonic::Status::permission_denied("denied").into();
        assert_eq!(classify_transport_error(&err), ErrorClass::Application);
    }

    #[test]
    fn classifies_tonic_not_found_as_application() {
        let err: anyhow::Error = tonic::Status::not_found("not found").into();
        assert_eq!(classify_transport_error(&err), ErrorClass::Application);
    }

    #[test]
    fn classifies_channel_send_error_string_as_transport() {
        let err = anyhow::anyhow!("Channel send error");
        assert_eq!(classify_transport_error(&err), ErrorClass::Transport);
    }

    #[test]
    fn classifies_closed_transport_errors_as_transport() {
        let err1 = anyhow::anyhow!("shell closed unexpectedly");
        assert_eq!(classify_transport_error(&err1), ErrorClass::Transport);

        let err2 = anyhow::anyhow!("broken pipe");
        assert_eq!(classify_transport_error(&err2), ErrorClass::Transport);

        let err3 = anyhow::anyhow!("connection reset by peer");
        assert_eq!(classify_transport_error(&err3), ErrorClass::Transport);
    }

    #[test]
    fn classifies_permission_denied_string_as_application() {
        let err = anyhow::anyhow!("permission denied");
        assert_eq!(classify_transport_error(&err), ErrorClass::Application);
    }

    #[test]
    fn classifies_generic_error_as_application() {
        let err = anyhow::anyhow!("file not found: /tmp/foo");
        assert_eq!(classify_transport_error(&err), ErrorClass::Application);
    }
}
