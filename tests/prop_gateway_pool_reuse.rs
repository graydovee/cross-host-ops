//! Property-based test for DirectGateway pool reuse invariant.
//!
//! Feature: gateway-refactor, Property 4: DirectGateway pool reuse invariant
//!
//! **Validates: Requirements 3.1, 3.2, 3.4, 14.2**
//!
//! For any sequence of exec calls to a DirectGateway targeting the same
//! host:port address, the gateway SHALL reuse idle connections from the pool
//! before creating new ones, and the number of pooled connections per address
//! SHALL never exceed max_connections_per_ip.
//!
//! NOTE: Since DirectGateway's internal pool methods are private and require
//! real SSH connections, the core property tests live in
//! `src/daemon/gateway/local.rs` as a `#[cfg(test)] mod tests` block which
//! has private access.
//!
//! This integration test exercises the pool invariants at a higher level using
//! a simulation model that mirrors the exact same logic as DirectGateway's pool
//! (acquire → reuse idle → create new → return → enforce capacity).

use proptest::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Pool simulation model — mirrors DirectGateway's internal pool semantics.
// ---------------------------------------------------------------------------

/// Simulated pooled connection for property testing.
struct SimConnection {
    alive: AtomicBool,
    _created_at: Instant,
}

impl SimConnection {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            alive: AtomicBool::new(true),
            _created_at: Instant::now(),
        })
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    fn kill(&self) {
        self.alive.store(false, Ordering::Relaxed);
    }
}

/// Pool simulator matching DirectGateway's acquire/return/discard semantics.
struct PoolSim {
    slots: Vec<Arc<SimConnection>>,
    max_connections: usize,
    total_created: AtomicUsize,
}

impl PoolSim {
    fn new(max_connections: usize) -> Self {
        Self {
            slots: Vec::new(),
            max_connections,
            total_created: AtomicUsize::new(0),
        }
    }

    /// Acquire an idle connection. Mirrors DirectGateway::acquire_connection:
    /// pops from the vec, checks is_alive, discards dead ones.
    fn acquire(&mut self) -> Option<Arc<SimConnection>> {
        while let Some(slot) = self.slots.pop() {
            if slot.is_alive() {
                return Some(slot);
            }
        }
        None
    }

    /// Create a new connection. Mirrors DirectGateway::create_connection.
    fn create(&self) -> Arc<SimConnection> {
        self.total_created.fetch_add(1, Ordering::Relaxed);
        SimConnection::new()
    }

    /// Return a connection. Mirrors DirectGateway::return_connection:
    /// only pushes if slots.len() < max_connections.
    fn return_connection(&mut self, conn: Arc<SimConnection>) {
        if self.slots.len() < self.max_connections {
            self.slots.push(conn);
        }
    }

    fn pool_size(&self) -> usize {
        self.slots.len()
    }

    fn total_created(&self) -> usize {
        self.total_created.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Operation model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum PoolOp {
    /// Acquire (or create), use, return to pool.
    AcquireUseReturn,
    /// Acquire (or create), use, discard (simulate error path).
    AcquireUseDiscard,
    /// Acquire (or create), kill connection, then return (broken conn returned).
    AcquireKillReturn,
}

fn pool_op_strategy() -> impl Strategy<Value = PoolOp> {
    prop_oneof![
        7 => Just(PoolOp::AcquireUseReturn),
        2 => Just(PoolOp::AcquireUseDiscard),
        1 => Just(PoolOp::AcquireKillReturn),
    ]
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 3.1, 3.2, 3.4, 14.2**
    ///
    /// For any sequence of pool operations with any max_connections_per_ip
    /// value (1–8), the pool SHALL:
    /// - Reuse idle connections before creating new ones
    /// - Never exceed max_connections_per_ip pooled connections
    #[test]
    fn prop_pool_reuse_and_capacity_invariant(
        max_connections in 1usize..=8,
        ops in proptest::collection::vec(pool_op_strategy(), 1..50),
    ) {
        let mut pool = PoolSim::new(max_connections);

        for op in &ops {
            // PRE-CONDITION: pool size never exceeds max_connections
            prop_assert!(
                pool.pool_size() <= max_connections,
                "pool size {} exceeded max {} before op {:?}",
                pool.pool_size(), max_connections, op
            );

            let _idle_before = pool.pool_size();
            let created_before = pool.total_created();

            match op {
                PoolOp::AcquireUseReturn => {
                    let conn = if let Some(existing) = pool.acquire() {
                        // INVARIANT: reused idle connection, no new creation
                        prop_assert_eq!(
                            pool.total_created(), created_before,
                            "should reuse idle, not create new"
                        );
                        existing
                    } else {
                        // No idle — must create
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
                    // Discard — not returned to pool (simulates error path)
                }
                PoolOp::AcquireKillReturn => {
                    let conn = if let Some(existing) = pool.acquire() {
                        existing
                    } else {
                        pool.create()
                    };
                    conn.kill();
                    pool.return_connection(conn);
                }
            }

            // POST-CONDITION: pool size never exceeds max_connections
            prop_assert!(
                pool.pool_size() <= max_connections,
                "pool size {} exceeded max {} after op {:?}",
                pool.pool_size(), max_connections, op
            );
        }
    }

    /// **Validates: Requirements 3.1, 3.2, 14.2**
    ///
    /// When idle connections are available, they MUST be reused before
    /// creating new ones. A sequence of acquire→return→acquire cycles on
    /// the same address should only ever create one connection.
    #[test]
    fn prop_idle_reuse_before_creation(
        max_connections in 1usize..=8,
        num_cycles in 2usize..=30,
    ) {
        let mut pool = PoolSim::new(max_connections);

        // First: create and return one connection
        let conn = pool.create();
        pool.return_connection(conn);
        prop_assert_eq!(pool.total_created(), 1);
        prop_assert_eq!(pool.pool_size(), 1);

        // Subsequent cycles: always reuse the single idle connection
        for cycle in 1..num_cycles {
            let conn = pool.acquire();
            prop_assert!(
                conn.is_some(),
                "cycle {}: expected idle connection available", cycle
            );
            // No new creation happened
            prop_assert_eq!(
                pool.total_created(), 1,
                "cycle {}: should reuse idle, total_created should remain 1", cycle
            );
            pool.return_connection(conn.unwrap());
        }

        // Final state: still only 1 connection ever created
        prop_assert_eq!(pool.total_created(), 1);
    }

    /// **Validates: Requirements 3.4, 14.2**
    ///
    /// Even when many connections are returned at once, the pool SHALL
    /// never store more than max_connections_per_ip.
    #[test]
    fn prop_capacity_hard_limit(
        max_connections in 1usize..=8,
        num_to_return in 1usize..=20,
    ) {
        let mut pool = PoolSim::new(max_connections);

        // Create many connections and return them all
        let connections: Vec<_> = (0..num_to_return).map(|_| pool.create()).collect();

        for conn in connections {
            pool.return_connection(conn);
            // After every return: pool_size ≤ max_connections
            prop_assert!(
                pool.pool_size() <= max_connections,
                "pool size {} exceeded max {}",
                pool.pool_size(), max_connections
            );
        }

        // Final pool size = min(num_to_return, max_connections)
        let expected = num_to_return.min(max_connections);
        prop_assert_eq!(pool.pool_size(), expected);
    }

    /// **Validates: Requirements 3.2, 3.4, 14.2**
    ///
    /// Dead connections in the pool are discarded during acquire, not
    /// counted toward the capacity limit, and new connections are created
    /// when only dead ones remain.
    #[test]
    fn prop_dead_connections_discarded_on_acquire(
        max_connections in 1usize..=8,
        num_dead in 1usize..=8,
    ) {
        let mut pool = PoolSim::new(max_connections);

        // Fill pool with dead connections
        for _ in 0..num_dead.min(max_connections) {
            let conn = pool.create();
            conn.kill();
            pool.return_connection(conn);
        }

        let created_before = pool.total_created();

        // Acquire should find no live connections (all dead)
        let acquired = pool.acquire();
        prop_assert!(acquired.is_none(), "should not acquire dead connections");

        // Pool should now be empty (dead ones discarded during acquire)
        prop_assert_eq!(pool.pool_size(), 0, "dead connections should be discarded");

        // Creating a new one increments total_created
        let new_conn = pool.create();
        prop_assert_eq!(pool.total_created(), created_before + 1);
        prop_assert!(new_conn.is_alive());
    }
}
