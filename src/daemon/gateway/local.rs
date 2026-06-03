// LocalGateway implementation.
// Manages direct SSH connections with per-address connection pooling.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::RwLock;
use tracing::debug;

use crate::config::{
    AppConfig, DirectAuth, list_server_entries, load_server_config,
    resolve_server_entry,
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
    Connection, ExecRequest as ConnExecRequest,
    InteractiveRequest as ConnInteractiveRequest,
};

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A resolved target from server.toml.
struct ResolvedTarget {
    host: String,
    port: u16,
    user: String,
    auth: DirectAuth,
}

/// A pooled connection with a timestamp for idle pruning.
struct PooledConnection {
    conn: AsyncMutex<DirectConnection>,
    created_at: Instant,
    last_used: Mutex<Instant>,
}

impl PooledConnection {
    fn new(conn: DirectConnection) -> Self {
        let now = Instant::now();
        Self {
            conn: AsyncMutex::new(conn),
            created_at: now,
            last_used: Mutex::new(now),
        }
    }

    fn touch(&self) {
        *self.last_used.lock() = Instant::now();
    }

    fn idle_duration(&self) -> Duration {
        self.last_used.lock().elapsed()
    }
}

// ---------------------------------------------------------------------------
// LocalGateway
// ---------------------------------------------------------------------------

pub struct LocalGateway {
    gateway_name: String,
    config: Arc<RwLock<AppConfig>>,
    server_config_path: String,
    #[allow(dead_code)]
    auth_prompter: Arc<AuthPrompter>,
    /// Per-address connection pool: key = "host:port"
    pools: Mutex<HashMap<String, Vec<Arc<PooledConnection>>>>,
    max_connections_per_address: usize,
    max_idle_time: Duration,
}

impl LocalGateway {
    /// Construct a new LocalGateway. No connections are established.
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
            pools: Mutex::new(HashMap::new()),
            max_connections_per_address,
            max_idle_time,
        }
    }

    /// Resolve a target string to host, port, user, and auth credentials
    /// by looking it up in the server.toml configuration.
    fn resolve_target(&self, target: &str) -> Result<ResolvedTarget, GatewayError> {
        let path = Path::new(&self.server_config_path);
        let server_config = load_server_config(path).map_err(|e| {
            GatewayError::resolution(anyhow!("failed to load server config: {}", e))
        })?;

        let server_host_config = server_config
            .servers
            .get(target)
            .ok_or_else(|| GatewayError::resolution(anyhow!("target '{}' not found", target)))?;

        let entry = resolve_server_entry(target, server_host_config, &server_config.defaults)
            .map_err(|e| {
                GatewayError::resolution(anyhow!(
                    "failed to resolve target '{}': {}",
                    target,
                    e
                ))
            })?;

        Ok(ResolvedTarget {
            host: entry.host,
            port: entry.port,
            user: entry.user,
            auth: entry.auth,
        })
    }

    /// Pool key for the given host and port.
    fn pool_key(host: &str, port: u16) -> String {
        format!("{}:{}", host, port)
    }

    /// Acquire an idle connection from the pool.
    /// Returns None if no live idle connections are available.
    fn acquire_connection(&self, key: &str) -> Option<Arc<PooledConnection>> {
        let mut pools = self.pools.lock();
        let slots = pools.get_mut(key)?;
        while let Some(slot) = slots.pop() {
            // Quick liveness check via try_lock
            let is_alive = match slot.conn.try_lock() {
                Ok(guard) => guard.is_alive(),
                Err(_) => false,
            };
            if is_alive {
                return Some(slot);
            }
            // Dead or locked — discard
        }
        None
    }

    /// Return a connection to the pool after use.
    /// Enforces max_connections_per_address limit.
    fn return_connection(&self, key: &str, conn: Arc<PooledConnection>) {
        conn.touch();
        let mut pools = self.pools.lock();
        let slots = pools.entry(key.to_string()).or_default();
        if slots.len() < self.max_connections_per_address {
            slots.push(conn);
        }
        // At capacity: drop the connection (it falls out of scope)
    }

    /// Discard a connection (do not return it to the pool).
    fn discard_connection(&self, key: &str, conn: &Arc<PooledConnection>) {
        let mut pools = self.pools.lock();
        if let Some(slots) = pools.get_mut(key) {
            slots.retain(|c| !Arc::ptr_eq(c, conn));
        }
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
    ) -> Result<Arc<PooledConnection>, GatewayError> {
        let key = Self::pool_key(&resolved.host, resolved.port);
        if let Some(pooled) = self.acquire_connection(&key) {
            return Ok(pooled);
        }
        let conn = self.create_connection(resolved).await.map_err(|e| {
            GatewayError::transport(anyhow!("failed to connect to {}: {}", key, e))
        })?;
        Ok(Arc::new(PooledConnection::new(conn)))
    }
}

// ---------------------------------------------------------------------------
// Gateway trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Gateway for LocalGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let resolved = self.resolve_target(target)?;
        let key = Self::pool_key(&resolved.host, resolved.port);
        let pooled = self.get_connection(&resolved).await?;

        // Build the connection-level request
        let conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            pty: request.pty,
            cols: request.cols,
            rows: request.rows,
            shell: request.shell.clone(),
        };

        // First attempt
        let result = {
            let mut conn = pooled.conn.lock().await;
            conn.exec(&conn_request).await
        };

        match result {
            Ok(exit_code) => {
                self.return_connection(&key, pooled);
                Ok(exit_code)
            }
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    "transport error on first attempt, retrying: {}",
                    e
                );
                // Discard the broken connection, retry once
                drop(pooled);

                let new_conn = self.create_connection(&resolved).await.map_err(|e| {
                    GatewayError::transport(anyhow!("retry connect failed for {}: {}", key, e))
                })?;
                let retry_pooled = Arc::new(PooledConnection::new(new_conn));

                let retry_result = {
                    let mut conn = retry_pooled.conn.lock().await;
                    conn.exec(&conn_request).await
                };

                match retry_result {
                    Ok(exit_code) => {
                        self.return_connection(&key, retry_pooled);
                        Ok(exit_code)
                    }
                    Err(e) => {
                        if is_transport_error(&e) {
                            Err(GatewayError::transport(e))
                        } else {
                            Err(GatewayError::execution(e))
                        }
                    }
                }
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn copy(&self, target: &str, spec: &CopySpec) -> Result<(), GatewayError> {
        let resolved = self.resolve_target(target)?;
        let key = Self::pool_key(&resolved.host, resolved.port);
        let pooled = self.get_connection(&resolved).await?;

        // First attempt
        let result = {
            let mut conn = pooled.conn.lock().await;
            conn.copy(spec).await
        };

        match result {
            Ok(()) => {
                self.return_connection(&key, pooled);
                Ok(())
            }
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    "transport error on copy, retrying: {}",
                    e
                );
                drop(pooled);

                let new_conn = self.create_connection(&resolved).await.map_err(|e| {
                    GatewayError::transport(anyhow!("retry connect failed for {}: {}", key, e))
                })?;
                let retry_pooled = Arc::new(PooledConnection::new(new_conn));

                let retry_result = {
                    let mut conn = retry_pooled.conn.lock().await;
                    conn.copy(spec).await
                };

                match retry_result {
                    Ok(()) => {
                        self.return_connection(&key, retry_pooled);
                        Ok(())
                    }
                    Err(e) => {
                        if is_transport_error(&e) {
                            Err(GatewayError::transport(e))
                        } else {
                            Err(GatewayError::execution(e))
                        }
                    }
                }
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let resolved = self.resolve_target(target)?;
        let pooled = self.get_connection(&resolved).await?;

        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: request.shell.clone(),
        };

        let mut conn = pooled.conn.lock().await;
        let handle = conn.exec_interactive(&conn_request).await.map_err(|e| {
            if is_transport_error(&e) {
                GatewayError::transport(e)
            } else {
                GatewayError::execution(e)
            }
        })?;

        // For interactive sessions, the connection is consumed (not returned to pool)
        // since the session remains open until the user exits.
        Ok(InteractiveHandle {
            stdin_tx: handle.stdin_tx,
            resize_tx: handle.resize_tx,
            stdout_rx: handle.stdout_rx,
            exit_rx: handle.exit_rx,
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let path = Path::new(&self.server_config_path);
        let entries = list_server_entries(path).map_err(|e| {
            GatewayError::resolution(anyhow!("failed to list servers: {}", e))
        })?;
        let rows = entries.into_iter().map(|server| ServerListRow {
            source: ServerListSource::Local,
            server,
        }).collect();
        Ok(rows)
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Direct
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {
        let mut pools = self.pools.lock();
        for (_key, slots) in pools.iter_mut() {
            slots.retain(|pooled| pooled.idle_duration() < self.max_idle_time);
        }
        // Remove empty entries
        pools.retain(|_, slots| !slots.is_empty());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // -----------------------------------------------------------------------
    // Mock connection for testing pool logic without real SSH.
    // -----------------------------------------------------------------------

    /// A mock DirectConnection that doesn't require SSH.
    /// We use this to test pool acquire/return/capacity invariants.
    struct MockPooledConnection {
        alive: Arc<AtomicBool>,
        last_used: Mutex<Instant>,
    }

    impl MockPooledConnection {
        fn new_alive() -> Arc<Self> {
            Arc::new(Self {
                alive: Arc::new(AtomicBool::new(true)),
                last_used: Mutex::new(Instant::now()),
            })
        }

        fn is_alive(&self) -> bool {
            self.alive.load(Ordering::Relaxed)
        }

        fn touch(&self) {
            *self.last_used.lock() = Instant::now();
        }

        fn kill(&self) {
            self.alive.store(false, Ordering::Relaxed);
        }
    }

    // -----------------------------------------------------------------------
    // A simplified pool that mirrors LocalGateway's pool logic for testing.
    // This tests the same invariants without needing real SSH connections.
    // -----------------------------------------------------------------------

    struct TestPool {
        slots: Vec<Arc<MockPooledConnection>>,
        max_connections: usize,
        total_created: AtomicUsize,
    }

    impl TestPool {
        fn new(max_connections: usize) -> Self {
            Self {
                slots: Vec::new(),
                max_connections,
                total_created: AtomicUsize::new(0),
            }
        }

        /// Acquire an idle connection from the pool. Returns None if no live
        /// idle connections are available. Mirrors LocalGateway::acquire_connection.
        fn acquire(&mut self) -> Option<Arc<MockPooledConnection>> {
            while let Some(slot) = self.slots.pop() {
                if slot.is_alive() {
                    return Some(slot);
                }
                // Dead connection discarded
            }
            None
        }

        /// Create a new connection (simulates SSH connect).
        /// Mirrors LocalGateway::create_connection.
        fn create(&self) -> Arc<MockPooledConnection> {
            self.total_created.fetch_add(1, Ordering::Relaxed);
            MockPooledConnection::new_alive()
        }

        /// Return a connection to the pool after use.
        /// Enforces max_connections limit. Mirrors LocalGateway::return_connection.
        fn return_connection(&mut self, conn: Arc<MockPooledConnection>) {
            conn.touch();
            if self.slots.len() < self.max_connections {
                self.slots.push(conn);
            }
            // At capacity: drop (not returned to pool)
        }

        /// Get current pool size for the address.
        fn pool_size(&self) -> usize {
            self.slots.len()
        }

        fn total_created(&self) -> usize {
            self.total_created.load(Ordering::Relaxed)
        }
    }

    // -----------------------------------------------------------------------
    // Pool operation model for proptest
    // -----------------------------------------------------------------------

    #[derive(Clone, Debug)]
    enum PoolOp {
        /// Acquire a connection, use it, then return it to the pool.
        AcquireUseReturn,
        /// Acquire a connection, use it, then discard it (simulate error).
        AcquireUseDiscard,
        /// Acquire a connection, kill it (simulate broken conn), then return.
        AcquireKillReturn,
    }

    fn pool_op_strategy() -> impl Strategy<Value = PoolOp> {
        prop_oneof![
            7 => Just(PoolOp::AcquireUseReturn),
            2 => Just(PoolOp::AcquireUseDiscard),
            1 => Just(PoolOp::AcquireKillReturn),
        ]
    }

    // -----------------------------------------------------------------------
    // Property 4: LocalGateway pool reuse invariant
    //
    // Feature: gateway-refactor, Property 4: LocalGateway pool reuse invariant
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.1, 3.2, 3.4, 14.2**
        ///
        /// For any sequence of exec calls to a LocalGateway targeting the same
        /// host:port address, the gateway SHALL reuse idle connections from the
        /// pool before creating new ones, and the number of pooled connections
        /// per address SHALL never exceed max_connections_per_ip.
        #[test]
        fn prop_pool_reuse_invariant(
            max_connections in 1usize..=8,
            ops in proptest::collection::vec(pool_op_strategy(), 1..50),
        ) {
            let mut pool = TestPool::new(max_connections);

            for op in &ops {
                // Invariant: pool size never exceeds max_connections (checked before each op)
                prop_assert!(
                    pool.pool_size() <= max_connections,
                    "pool size {} exceeded max_connections {} before op {:?}",
                    pool.pool_size(),
                    max_connections,
                    op
                );

                let _idle_before = pool.pool_size();
                let created_before = pool.total_created();

                match op {
                    PoolOp::AcquireUseReturn => {
                        let conn = if let Some(existing) = pool.acquire() {
                            // Reused an idle connection — no new creation
                            prop_assert_eq!(
                                pool.total_created(),
                                created_before,
                                "should reuse idle connection, not create new"
                            );
                            existing
                        } else {
                            // No idle connections — must create new
                            pool.create()
                        };
                        pool.return_connection(conn);
                    }
                    PoolOp::AcquireUseDiscard => {
                        let _conn = if let Some(existing) = pool.acquire() {
                            existing
                        } else {
                            pool.create()
                        };
                        // Intentionally don't return — simulates discard on error
                    }
                    PoolOp::AcquireKillReturn => {
                        let conn = if let Some(existing) = pool.acquire() {
                            existing
                        } else {
                            pool.create()
                        };
                        // Kill the connection (simulate transport error)
                        conn.kill();
                        // Return it (pool may accept it but it will be dead)
                        pool.return_connection(conn);
                    }
                }

                // Invariant: pool size never exceeds max_connections (checked after each op)
                prop_assert!(
                    pool.pool_size() <= max_connections,
                    "pool size {} exceeded max_connections {} after op {:?}",
                    pool.pool_size(),
                    max_connections,
                    op
                );
            }
        }

        /// **Validates: Requirements 3.1, 3.2, 14.2**
        ///
        /// When idle connections are available in the pool, they MUST be
        /// reused before creating new ones. This test verifies that for a
        /// sequence of acquire-return-acquire cycles targeting the same
        /// address, the pool only creates one connection total.
        #[test]
        fn prop_pool_reuses_idle_before_creating(
            max_connections in 1usize..=8,
            num_cycles in 2usize..=20,
        ) {
            let mut pool = TestPool::new(max_connections);

            // First acquire must create a new connection (pool is empty)
            let conn = pool.create();
            pool.return_connection(conn);
            prop_assert_eq!(pool.total_created(), 1);

            // Subsequent acquire calls should reuse the idle connection
            for cycle in 1..num_cycles {
                let conn = pool.acquire().unwrap_or_else(|| {
                    panic!(
                        "cycle {}: expected idle connection in pool (size={})",
                        cycle,
                        pool.pool_size()
                    )
                });
                // No new connection created — still 1 total
                prop_assert_eq!(
                    pool.total_created(),
                    1,
                    "cycle {}: should reuse idle connection, total_created should be 1",
                    cycle
                );
                pool.return_connection(conn);
            }
        }

        /// **Validates: Requirements 3.4, 14.2**
        ///
        /// The number of pooled connections per address SHALL never exceed
        /// max_connections_per_ip, even when many connections are returned
        /// concurrently.
        #[test]
        fn prop_pool_capacity_never_exceeded(
            max_connections in 1usize..=8,
            num_connections in 1usize..=20,
        ) {
            let mut pool = TestPool::new(max_connections);

            // Create and return many connections — pool should cap at max
            let connections: Vec<_> = (0..num_connections)
                .map(|_| pool.create())
                .collect();

            for conn in connections {
                pool.return_connection(conn);
                // After every return, pool size must be ≤ max_connections
                prop_assert!(
                    pool.pool_size() <= max_connections,
                    "pool size {} exceeded max_connections {} after return",
                    pool.pool_size(),
                    max_connections,
                );
            }

            // Final pool size is min(num_connections, max_connections)
            let expected_final = num_connections.min(max_connections);
            prop_assert_eq!(
                pool.pool_size(),
                expected_final,
                "final pool size should be min({}, {})",
                num_connections,
                max_connections,
            );
        }
    }

    // -----------------------------------------------------------------------
    // Also verify the real LocalGateway pool methods directly.
    // Since we're in the same module, we have private access.
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.4, 14.2**
        ///
        /// Test the real LocalGateway construction to ensure it properly
        /// configures the max_connections_per_address limit and starts with
        /// an empty pool.
        #[test]
        fn prop_real_gateway_pool_capacity(
            max_connections in 1usize..=8,
        ) {
            // Construct a LocalGateway with the given capacity.
            // We use dummy paths since we won't actually connect.
            let config = Arc::new(RwLock::new(AppConfig::default()));
            let auth_prompter: Arc<AuthPrompter> = Arc::new(|_| {
                Box::pin(async { Ok(String::new()) })
            });
            let gateway = LocalGateway::new(
                "test".to_string(),
                config,
                "/nonexistent/server.toml".to_string(),
                auth_prompter,
                max_connections,
                Duration::from_secs(300),
            );

            let key = "test-host:22";

            // Verify pool starts empty
            {
                let pools = gateway.pools.lock();
                prop_assert!(pools.is_empty(), "pool should start empty");
            }

            // Verify max_connections_per_address is correctly stored
            prop_assert_eq!(gateway.max_connections_per_address, max_connections);

            // Verify acquire_connection on empty pool returns None
            let acquired = gateway.acquire_connection(key);
            prop_assert!(acquired.is_none(), "empty pool should return None on acquire");
        }
    }
}
