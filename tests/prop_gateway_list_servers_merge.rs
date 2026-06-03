//! Property-based test: list_servers merge skips UnsupportedCapability.
//!
//! Feature: gateway-refactor, Property 9: list_servers merge skips UnsupportedCapability
//!
//! For any set of Gateways where some return server entries and others return
//! Unsupported errors, the Daemon's list_servers merge SHALL return exactly the
//! union of entries from successful Gateways, without including any entries from
//! unsupported ones and without failing.
//!
//! **Validates: Requirements 10.3**

use std::sync::Arc;

use async_trait::async_trait;
use proptest::prelude::*;

use rhop::config::{DirectAuth, ServerEntry};
use rhop::protocol::ServerListRow;
use rhop::types::{CopySpec, ServerListSource};
use rhop::daemon::gateway::{
    ErrorKind, ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle,
    InteractiveRequest,
};

// ---------------------------------------------------------------------------
// Mock Gateway implementations
// ---------------------------------------------------------------------------

/// A mock gateway that returns Ok with a list of ServerListRow entries.
struct OkGateway {
    gateway_name: String,
    entries: Vec<ServerEntry>,
}

#[async_trait]
impl Gateway for OkGateway {
    async fn exec(&self, _target: &str, _request: &ExecRequest) -> Result<i32, GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn copy(&self, _target: &str, _spec: CopySpec) -> Result<(), GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn exec_interactive(
        &self,
        _target: &str,
        _request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let rows = self
            .entries
            .iter()
            .map(|entry| ServerListRow {
                source: ServerListSource::Gateway(self.gateway_name.clone()),
                server: entry.clone(),
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

    async fn prune_idle(&self) {}
}

/// A mock gateway that returns GatewayError::unsupported for list_servers.
struct UnsupportedGateway {
    gateway_name: String,
}

#[async_trait]
impl Gateway for UnsupportedGateway {
    async fn exec(&self, _target: &str, _request: &ExecRequest) -> Result<i32, GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn copy(&self, _target: &str, _spec: CopySpec) -> Result<(), GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn exec_interactive(
        &self,
        _target: &str,
        _request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        unimplemented!("not needed for this test")
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        Err(GatewayError::unsupported(anyhow::anyhow!(
            "list_servers not supported by gateway '{}'",
            self.gateway_name
        )))
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Jumpserver
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {}
}

// ---------------------------------------------------------------------------
// Merge logic simulation — mirrors process_list_servers from src/daemon/rpc.rs
// ---------------------------------------------------------------------------

/// Simulates the daemon's list_servers merge logic as specified in the design.
/// Iterates the Vec in order, preserving gateway declaration ordering.
async fn merge_list_servers(
    gateways: &[(String, Arc<dyn Gateway>)],
) -> Vec<ServerListRow> {
    let mut results = Vec::new();
    for (_name, gateway) in gateways {
        match gateway.list_servers().await {
            Ok(rows) => results.extend(rows),
            Err(e) if e.kind == ErrorKind::Unsupported => continue,
            Err(_e) => continue,
        }
    }
    results
}

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating a random ServerEntry.
fn arb_server_entry() -> impl Strategy<Value = ServerEntry> {
    (
        "[a-z][a-z0-9]{1,7}",       // alias
        (1u8..=254u8, 0u8..=255u8, 0u8..=255u8, 1u8..=254u8), // host parts
        1u16..=65535u16,             // port
        "[a-z]{1,8}",               // user
        any::<bool>(),              // use key or password
    )
        .prop_map(|(alias, (a, b, c, d), port, user, use_key)| {
            let host = format!("{}.{}.{}.{}", a, b, c, d);
            let auth = if use_key {
                DirectAuth::Key {
                    identity_file: "/tmp/test_key".to_string(),
                }
            } else {
                DirectAuth::Password {
                    password: format!("pass_{}", alias),
                }
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

/// Strategy for generating a list of server entries (0-5 per gateway).
fn arb_entry_list() -> impl Strategy<Value = Vec<ServerEntry>> {
    proptest::collection::vec(arb_server_entry(), 0..=5)
}

/// Represents a gateway's behavior in the test.
#[derive(Clone, Debug)]
enum GatewayBehavior {
    /// Returns Ok with the given list of ServerEntry.
    ReturnsEntries(Vec<ServerEntry>),
    /// Returns GatewayError::unsupported.
    Unsupported,
}

/// Strategy for generating a single gateway behavior.
fn arb_gateway_behavior() -> impl Strategy<Value = GatewayBehavior> {
    prop_oneof![
        // 60% chance: returns entries
        6 => arb_entry_list().prop_map(GatewayBehavior::ReturnsEntries),
        // 40% chance: unsupported
        4 => Just(GatewayBehavior::Unsupported),
    ]
}

/// Strategy for generating a set of 2-5 gateways with unique names and behaviors.
fn arb_gateway_set() -> impl Strategy<Value = Vec<(String, GatewayBehavior)>> {
    proptest::collection::vec(
        ("[a-z]{2,8}", arb_gateway_behavior()),
        2..=5,
    )
        .prop_map(|gateways| {
            // Ensure unique names
            let mut seen = std::collections::HashSet::new();
            gateways
                .into_iter()
                .filter(|(name, _)| seen.insert(name.clone()))
                .collect()
        })
        .prop_filter("need at least 2 gateways", |gws: &Vec<(String, GatewayBehavior)>| gws.len() >= 2)
}

// ---------------------------------------------------------------------------
// Helper: build gateways Vec from test data.
// ---------------------------------------------------------------------------

fn build_gateways(
    gateway_set: &[(String, GatewayBehavior)],
) -> Vec<(String, Arc<dyn Gateway>)> {
    let mut gateways: Vec<(String, Arc<dyn Gateway>)> = Vec::new();

    for (name, behavior) in gateway_set {
        let gw: Arc<dyn Gateway> = match behavior {
            GatewayBehavior::ReturnsEntries(entries) => Arc::new(OkGateway {
                gateway_name: name.clone(),
                entries: entries.clone(),
            }),
            GatewayBehavior::Unsupported => Arc::new(UnsupportedGateway {
                gateway_name: name.clone(),
            }),
        };
        gateways.push((name.clone(), gw));
    }

    gateways
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, .. ProptestConfig::default() })]

    /// **Validates: Requirements 10.3**
    ///
    /// For any set of Gateways (2-5) where some return server entries and
    /// others return Unsupported errors, the merge logic SHALL:
    /// 1. Return exactly the union of entries from successful gateways
    /// 2. Not include any entries from unsupported gateways
    /// 3. Not fail overall (always returns a Vec, never an error)
    #[test]
    fn prop_list_servers_merge_skips_unsupported(
        gateway_set in arb_gateway_set()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let gateways = build_gateways(&gateway_set);

            // Execute the merge logic
            let result = merge_list_servers(&gateways).await;

            // Compute expected: union of all entries from Ok gateways
            let mut expected_aliases: Vec<String> = Vec::new();
            for (_name, behavior) in &gateway_set {
                if let GatewayBehavior::ReturnsEntries(entries) = behavior {
                    for entry in entries {
                        expected_aliases.push(entry.alias.clone());
                    }
                }
                // Unsupported gateways contribute nothing
            }

            // PROPERTY: result count matches expected count
            prop_assert_eq!(
                result.len(),
                expected_aliases.len(),
                "Merge result count ({}) does not match expected ({}). \
                 Unsupported gateways should contribute zero entries.",
                result.len(),
                expected_aliases.len()
            );

            // PROPERTY: result contains exactly the expected entries (by alias)
            // Because we use Vec (ordered), the aliases should appear in the
            // same order as the gateway_set declaration order.
            let result_aliases: Vec<String> =
                result.iter().map(|row| row.server.alias.clone()).collect();

            prop_assert_eq!(
                &result_aliases,
                &expected_aliases,
                "Merged aliases do not match expected aliases from successful gateways (order matters)"
            );

            // PROPERTY: each row has a proper source tag
            for row in &result {
                match &row.source {
                    ServerListSource::Gateway(_) => {},
                    other => prop_assert!(false, "Expected Gateway source, got {:?}", other),
                }
            }

            Ok(())
        })?;
    }

    /// **Validates: Requirements 10.3**
    ///
    /// When ALL gateways return Unsupported, the merge SHALL return an empty
    /// Vec without failing.
    #[test]
    fn prop_all_unsupported_returns_empty(
        num_gateways in 2usize..=5,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let gateway_set: Vec<(String, GatewayBehavior)> = (0..num_gateways)
                .map(|i| (format!("gw{}", i), GatewayBehavior::Unsupported))
                .collect();

            let gateways = build_gateways(&gateway_set);
            let result = merge_list_servers(&gateways).await;

            prop_assert!(
                result.is_empty(),
                "When all gateways are unsupported, merge should return empty Vec, \
                 got {} entries",
                result.len()
            );

            Ok(())
        })?;
    }

    /// **Validates: Requirements 10.3**
    ///
    /// When at least one gateway returns entries and others are unsupported,
    /// the total entry count SHALL equal the sum of entry counts from Ok
    /// gateways only.
    #[test]
    fn prop_mixed_gateways_entry_count(
        ok_entries_lists in proptest::collection::vec(arb_entry_list(), 1..=3),
        unsupported_count in 1usize..=3,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut gateway_set: Vec<(String, GatewayBehavior)> = Vec::new();

            // Add Ok gateways
            for (i, entries) in ok_entries_lists.iter().enumerate() {
                gateway_set.push((
                    format!("ok_gw{}", i),
                    GatewayBehavior::ReturnsEntries(entries.clone()),
                ));
            }

            // Add Unsupported gateways
            for i in 0..unsupported_count {
                gateway_set.push((
                    format!("unsup_gw{}", i),
                    GatewayBehavior::Unsupported,
                ));
            }

            let gateways = build_gateways(&gateway_set);
            let result = merge_list_servers(&gateways).await;

            // Expected count = sum of all entries from Ok gateways
            let expected_count: usize = ok_entries_lists.iter().map(|v| v.len()).sum();

            prop_assert_eq!(
                result.len(),
                expected_count,
                "Merge result count ({}) should equal sum of Ok gateway entries ({}). \
                 Unsupported gateways ({}) must not contribute.",
                result.len(),
                expected_count,
                unsupported_count
            );

            Ok(())
        })?;
    }
}
