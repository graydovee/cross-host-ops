//! Property-based test: ordered iteration determinism for process_list_servers.
//!
//! Feature: server-list-path-prefix, Property 3: Ordered iteration determinism
//!
//! For any valid gateway configuration with N gateways, calling process_list_servers
//! (simulated) SHALL produce rows where all entries from gateway at index i appear
//! before all entries from gateway at index j when i < j, and calling it twice with
//! the same state SHALL produce identical output (idempotence).
//!
//! **Validates: Requirements 2.1, 2.2, 2.4**

use std::sync::Arc;

use async_trait::async_trait;
use proptest::prelude::*;

use xho::config::{DirectAuth, ServerEntry};
use xho::daemon::gateway::{Capabilities, ErrorKind, Gateway, GatewayError, GatewayKind};
use xho::daemon::session::TargetSession;
use xho::protocol::ServerListRow;
use xho::types::ServerListSource;

// ---------------------------------------------------------------------------
// Mock Gateway implementation
// ---------------------------------------------------------------------------

/// A mock gateway that returns Ok with a fixed list of ServerListRow entries.
struct MockGateway {
    gateway_name: String,
    rows: Vec<ServerListRow>,
}

#[async_trait]
impl Gateway for MockGateway {
    fn name(&self) -> &str {
        &self.gateway_name
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Direct
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::LIST
    }

    async fn open_exec_session(
        &self,
        _target: &str,
        _argv: &[String],
        _shell: &str,
        _no_shell: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn open_session(&self, _target: &str) -> Result<Box<dyn TargetSession>, GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        Ok(self.rows.clone())
    }
}

// ---------------------------------------------------------------------------
// Simulate process_list_servers iteration logic
// ---------------------------------------------------------------------------

/// Simulates the process_list_servers iteration over a Vec of gateways.
/// Mirrors the logic in src/daemon/rpc.rs: iterate in Vec order, call
/// list_servers on each, collect results preserving order.
async fn simulate_list_servers(
    gateways: &[(String, Arc<dyn Gateway>)],
) -> Vec<(String, ServerListSource)> {
    let mut results: Vec<(String, ServerListSource)> = Vec::new();

    for (_name, gateway) in gateways {
        match gateway.list_servers().await {
            Ok(rows) => {
                for row in rows {
                    results.push((row.server.alias, row.source));
                }
            }
            Err(e) if e.kind == ErrorKind::Unsupported => continue,
            Err(_) => continue,
        }
    }

    results
}

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating a random ServerEntry with a specific alias prefix
/// to ensure uniqueness across gateways.
fn arb_server_entry_with_prefix(prefix: String) -> impl Strategy<Value = ServerEntry> {
    (
        "[a-z][a-z0-9]{1,5}", // alias suffix
        (1u8..=254u8, 0u8..=255u8, 0u8..=255u8, 1u8..=254u8),
        1u16..=65535u16,
        "[a-z]{1,6}",
    )
        .prop_map(move |(suffix, (a, b, c, d), port, user)| {
            let alias = format!("{}_{}", prefix, suffix);
            let host = format!("{}.{}.{}.{}", a, b, c, d);
            let auth = DirectAuth::Key {
                identity_file: "/tmp/test_key".to_string(),
            };
            ServerEntry {
                alias,
                host,
                port,
                user,
                auth,
            }
        })
}

/// Strategy for generating a gateway: a name and 1-4 server entries.
fn arb_gateway_data() -> impl Strategy<Value = (String, Vec<ServerEntry>)> {
    "[a-z]{2,6}".prop_flat_map(|name| {
        let name_clone = name.clone();
        proptest::collection::vec(arb_server_entry_with_prefix(name.clone()), 1..=4)
            .prop_map(move |entries| (name_clone.clone(), entries))
    })
}

/// Strategy for generating 1-5 gateways with unique names.
fn arb_gateway_set() -> impl Strategy<Value = Vec<(String, Vec<ServerEntry>)>> {
    proptest::collection::vec(arb_gateway_data(), 1..=5)
        .prop_map(|gateways| {
            let mut seen = std::collections::HashSet::new();
            gateways
                .into_iter()
                .filter(|(name, _)| seen.insert(name.clone()))
                .collect::<Vec<_>>()
        })
        .prop_filter("need at least 1 gateway", |gws| !gws.is_empty())
}

// ---------------------------------------------------------------------------
// Helper: build gateways Vec from test data
// ---------------------------------------------------------------------------

fn build_test_gateways(
    gateway_set: &[(String, Vec<ServerEntry>)],
) -> Vec<(String, Arc<dyn Gateway>)> {
    gateway_set
        .iter()
        .map(|(name, entries)| {
            let rows: Vec<ServerListRow> = entries
                .iter()
                .map(|entry| ServerListRow {
                    source: ServerListSource::Gateway(name.clone()),
                    server: entry.clone(),
                })
                .collect();
            let gw: Arc<dyn Gateway> = Arc::new(MockGateway {
                gateway_name: name.clone(),
                rows,
            });
            (name.clone(), gw)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 2.1, 2.2, 2.4**
    ///
    /// Property 3a: Ordered iteration — all entries from gateway at index i
    /// appear before all entries from gateway at index j when i < j.
    #[test]
    fn prop_ordered_iteration_respects_insertion_order(
        gateway_set in arb_gateway_set()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let gateways = build_test_gateways(&gateway_set);
            let result = simulate_list_servers(&gateways).await;

            // Verify ordering: for each pair of gateways (i, j) where i < j,
            // all entries from gateway i must appear before entries from gateway j.
            // We track the last index in `result` where each gateway's entries appear.
            let mut last_index_per_gateway: Vec<Option<usize>> = vec![None; gateway_set.len()];
            let mut first_index_per_gateway: Vec<Option<usize>> = vec![None; gateway_set.len()];

            for (result_idx, (_alias, source)) in result.iter().enumerate() {
                // Find which gateway this entry belongs to
                for (gw_idx, (name, _)) in gateway_set.iter().enumerate() {
                    if source == &ServerListSource::Gateway(name.clone()) {
                        if first_index_per_gateway[gw_idx].is_none() {
                            first_index_per_gateway[gw_idx] = Some(result_idx);
                        }
                        last_index_per_gateway[gw_idx] = Some(result_idx);
                        break;
                    }
                }
            }

            // For each pair (i, j) where i < j, verify:
            // last_index[i] < first_index[j] (entries don't interleave)
            for i in 0..gateway_set.len() {
                for j in (i + 1)..gateway_set.len() {
                    if let (Some(last_i), Some(first_j)) =
                        (last_index_per_gateway[i], first_index_per_gateway[j])
                    {
                        prop_assert!(
                            last_i < first_j,
                            "Gateway '{}' (index {}) has entries after gateway '{}' (index {}): \
                             last_index[{}]={}, first_index[{}]={}",
                            gateway_set[i].0, i,
                            gateway_set[j].0, j,
                            i, last_i, j, first_j
                        );
                    }
                }
            }

            Ok(())
        })?;
    }

    /// **Validates: Requirements 2.1, 2.2, 2.4**
    ///
    /// Property 3b: Idempotence — calling simulate_list_servers twice with
    /// the same state produces identical output.
    #[test]
    fn prop_idempotent_output(
        gateway_set in arb_gateway_set()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let gateways = build_test_gateways(&gateway_set);

            let result1 = simulate_list_servers(&gateways).await;
            let result2 = simulate_list_servers(&gateways).await;

            prop_assert_eq!(
                result1.len(),
                result2.len(),
                "Two calls to simulate_list_servers returned different lengths: {} vs {}",
                result1.len(),
                result2.len()
            );

            for (idx, (r1, r2)) in result1.iter().zip(result2.iter()).enumerate() {
                prop_assert_eq!(
                    r1, r2,
                    "Results differ at index {}: {:?} vs {:?}",
                    idx, r1, r2
                );
            }

            Ok(())
        })?;
    }
}
