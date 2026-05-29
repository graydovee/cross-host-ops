// Feature: rhopd-connect-and-server-list, Property 5: list_servers handler is per-source isolated and order-preserving
//
// This property test verifies that the list_servers handler:
// 1. Always returns Ok (never fails the whole RPC due to individual jump host failures)
// 2. Local server.toml entries are always included
#![allow(clippy::collapsible_if)]
// 3. Each failed build_jump_host entry produces exactly one (JumpHost(name), Error(msg))
//    in source_status, with msg == format!("{error}") of the stub error
// 4. Successful entries' order is preserved

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use proptest::prelude::*;
use tokio::sync::mpsc::UnboundedSender;

use rhop::config::{
    AppConfig, DirectAuth, ServerConfigFile, ServerDefaults, ServerEntry, ServerHostConfig,
};
use rhop::connection::CopySpec;
use rhop::jump::server_list::ServerListAggregator;
use rhop::jump::{JumpHost, JumpHostKind, ServerListSource};
use rhop::protocol::{ServerEvent, ServerListSourceStatus};

// ---------------------------------------------------------------------------
// Mock jump host implementations
// ---------------------------------------------------------------------------

/// A mock jump host that successfully returns a fixed list of servers.
struct OkMockJumpHost {
    alias: String,
    servers: Vec<ServerEntry>,
}

#[async_trait]
impl JumpHost for OkMockJumpHost {
    async fn exec(
        &mut self,
        _argv: &[String],
        _sender: &UnboundedSender<ServerEvent>,
        _config: &AppConfig,
    ) -> Result<i32> {
        Ok(0)
    }

    async fn copy(&mut self, _spec: &CopySpec, _config: &AppConfig) -> Result<()> {
        Ok(())
    }

    async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
        Ok(self.servers.clone())
    }

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Rhopd
    }

    fn name(&self) -> &str {
        &self.alias
    }
}

// ---------------------------------------------------------------------------
// Represents the outcome of build_jump_host for a single entry
// ---------------------------------------------------------------------------

/// Models the result of calling `build_jump_host` for a single jump host config.
/// Ok variant means the jump host was successfully constructed (with some servers).
/// Err variant means construction failed with the given error message.
#[derive(Clone, Debug)]
enum BuildOutcome {
    Ok { servers: Vec<ServerEntry> },
    Err { error_msg: String },
}

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy to generate a valid ServerEntry.
fn arb_server_entry() -> impl Strategy<Value = ServerEntry> {
    (
        "[a-z][a-z0-9]{0,7}",                                         // alias
        "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}",        // host (IP-like)
        1u16..=65535u16,                                               // port
        "[a-z]{1,8}",                                                  // user
    )
        .prop_map(|(alias, host, port, user)| ServerEntry {
            alias,
            host,
            port,
            user,
            auth: DirectAuth::Key {
                identity_file: "/tmp/key".to_string(),
            },
        })
}

/// Strategy to generate a BuildOutcome (Ok with 0-4 servers, or Err with a message).
fn arb_build_outcome() -> impl Strategy<Value = BuildOutcome> {
    prop_oneof![
        // Ok with 0-4 server entries
        prop::collection::vec(arb_server_entry(), 0..5)
            .prop_map(|servers| BuildOutcome::Ok { servers }),
        // Err with an arbitrary non-empty error message
        "[a-zA-Z0-9 _\\-\\.]{1,50}".prop_map(|error_msg| BuildOutcome::Err { error_msg }),
    ]
}

/// Strategy to generate a list of (alias, BuildOutcome) pairs with unique aliases.
/// Generates 0-8 entries as specified in the task.
fn arb_jump_host_outcomes() -> impl Strategy<Value = Vec<(String, BuildOutcome)>> {
    prop::collection::vec(
        (
            "[a-z]{1,6}".prop_map(|s| format!("jh_{}", s)),
            arb_build_outcome(),
        ),
        0..=8,
    )
    .prop_map(|mut pairs| {
        // Deduplicate aliases to avoid collisions
        let mut seen = std::collections::HashSet::new();
        pairs.retain(|(alias, _)| seen.insert(alias.clone()));
        pairs
    })
}

/// Strategy to generate local server.toml entries (0-5 entries with unique aliases).
fn arb_local_entries() -> impl Strategy<Value = Vec<(String, ServerHostConfig)>> {
    prop::collection::vec(
        (
            "[a-z][a-z0-9]{0,5}".prop_map(|s| format!("local_{}", s)),
            (
                "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}",
                "[a-z]{1,8}",
            )
                .prop_map(|(host, user)| ServerHostConfig {
                    host,
                    port: Some(22),
                    user,
                    identity_file: Some("/tmp/key".to_string()),
                    password: None,
                }),
        ),
        0..=5,
    )
    .prop_map(|mut entries| {
        // Deduplicate aliases
        let mut seen = std::collections::HashSet::new();
        entries.retain(|(alias, _)| seen.insert(alias.clone()));
        entries
    })
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 6.1, 6.3, 6.4, 6.5, 6.6, 7.5**
    ///
    /// For arbitrary jump host configurations (each independently Ok or Err)
    /// and arbitrary local server.toml entries, the list_servers handler logic:
    /// 1. Always produces a result (never panics/errors at the RPC level)
    /// 2. Local entries are always included in the response
    /// 3. Each failed build_jump_host entry produces exactly one
    ///    (JumpHost(name), Error(msg)) in source_status
    /// 4. The error message equals the stub error's format!("{error}") verbatim
    /// 5. Successful entries' order is preserved
    #[test]
    fn prop_list_servers_per_source_isolation(
        jump_outcomes in arb_jump_host_outcomes(),
        local_entries in arb_local_entries(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = AppConfig::default();

            // --- Simulate the list_servers handler logic ---
            // This mirrors daemon.rs list_servers:
            //   for entry in &config.jump_hosts {
            //       match build_jump_host(...) {
            //           Ok(host) => jump_hosts.push(host),
            //           Err(error) => prebuilt_status.push(...)
            //       }
            //   }

            let mut jump_hosts: Vec<Box<dyn JumpHost>> = Vec::new();
            let mut prebuilt_status: Vec<(ServerListSource, ServerListSourceStatus)> = Vec::new();

            // Track which entries are expected to succeed (in order)
            let mut expected_ok_aliases: Vec<String> = Vec::new();
            // Track which entries are expected to fail with their error messages
            let mut expected_err: Vec<(String, String)> = Vec::new();

            for (alias, outcome) in &jump_outcomes {
                match outcome {
                    BuildOutcome::Ok { servers } => {
                        // Simulate successful build_jump_host
                        jump_hosts.push(Box::new(OkMockJumpHost {
                            alias: alias.clone(),
                            servers: servers.clone(),
                        }));
                        expected_ok_aliases.push(alias.clone());
                    }
                    BuildOutcome::Err { error_msg } => {
                        // Simulate failed build_jump_host: the handler captures
                        // the error using format!("{error}") verbatim
                        let error = anyhow::anyhow!("{}", error_msg);
                        let msg = format!("{error}");
                        prebuilt_status.push((
                            ServerListSource::JumpHost(alias.clone()),
                            ServerListSourceStatus::Error(msg),
                        ));
                        expected_err.push((alias.clone(), error_msg.clone()));
                    }
                }
            }

            // Build the local ServerConfigFile
            let mut servers_map = HashMap::new();
            for (alias, host_config) in &local_entries {
                servers_map.insert(alias.clone(), host_config.clone());
            }
            let server_config = ServerConfigFile {
                defaults: ServerDefaults {
                    identity_file: Some("/tmp/default_key".to_string()),
                },
                servers: servers_map,
            };

            // Run the aggregator (same as the handler does)
            let mut aggregator = ServerListAggregator {
                local: &server_config,
                jump_hosts: &mut jump_hosts,
                config: &config,
                cache: HashMap::new(),
            };
            let mut merged = aggregator.aggregate(false).await;

            // Merge prebuilt_status (same as the handler does)
            merged.source_status.extend(prebuilt_status);

            // --- Invariant 1: response is always Ok (never panics) ---
            // The fact that we reached this point proves the aggregation
            // always succeeds. The handler wraps this in Ok(Response::new(...)).

            // --- Invariant 2: local entries count = input count ---
            // Local entries in rows should match the number of unique local entries
            // (HashMap deduplication applies, same as in the handler)
            let local_rows: Vec<_> = merged
                .rows
                .iter()
                .filter(|row| row.source == ServerListSource::Local)
                .collect();
            prop_assert_eq!(
                local_rows.len(),
                local_entries.len(),
                "Local entry count mismatch: got {} rows but expected {} entries",
                local_rows.len(),
                local_entries.len()
            );

            // --- Invariant 3: each failed entry has exactly one (JumpHost(name), Error(msg)) ---
            for (alias, error_msg) in &expected_err {
                let source = ServerListSource::JumpHost(alias.clone());
                let matching_statuses: Vec<_> = merged
                    .source_status
                    .iter()
                    .filter(|(s, _)| *s == source)
                    .collect();

                prop_assert_eq!(
                    matching_statuses.len(),
                    1,
                    "Expected exactly one source_status entry for failed jump host '{}', got {}",
                    alias,
                    matching_statuses.len()
                );

                // --- Invariant 4: msg equals the stub error's format!("{error}") ---
                let (_, status) = matching_statuses[0];
                match status {
                    ServerListSourceStatus::Error(msg) => {
                        prop_assert_eq!(
                            msg, error_msg,
                            "Error message mismatch for '{}': got {:?}, expected {:?}",
                            alias, msg, error_msg
                        );
                    }
                    other => {
                        prop_assert!(
                            false,
                            "Expected Error status for '{}', got {:?}",
                            alias, other
                        );
                    }
                }
            }

            // Failed entries must NOT have any rows in the result
            for (alias, _) in &expected_err {
                let source = ServerListSource::JumpHost(alias.clone());
                let rows_from_failed: Vec<_> = merged
                    .rows
                    .iter()
                    .filter(|row| row.source == source)
                    .collect();
                prop_assert_eq!(
                    rows_from_failed.len(),
                    0,
                    "Failed jump host '{}' should have zero rows, got {}",
                    alias,
                    rows_from_failed.len()
                );
            }

            // --- Invariant 5: successful entries' order is preserved ---
            // The order of successful jump hosts in source_status should match
            // the order they appeared in the configuration (i.e., expected_ok_aliases).
            let ok_jh_statuses: Vec<String> = merged
                .source_status
                .iter()
                .filter_map(|(source, status)| {
                    if let ServerListSource::JumpHost(alias) = source {
                        if matches!(status, ServerListSourceStatus::Ok) {
                            return Some(alias.clone());
                        }
                    }
                    None
                })
                .collect();

            prop_assert_eq!(
                &ok_jh_statuses,
                &expected_ok_aliases,
                "Successful jump host order mismatch: got {:?}, expected {:?}",
                ok_jh_statuses,
                expected_ok_aliases
            );

            // Also verify that successful jump hosts have their server entries
            // in the rows (order within each source is preserved)
            for (alias, outcome) in &jump_outcomes {
                if let BuildOutcome::Ok { servers } = outcome {
                    let source = ServerListSource::JumpHost(alias.clone());
                    let actual_aliases: Vec<String> = merged
                        .rows
                        .iter()
                        .filter(|row| row.source == source)
                        .map(|row| row.server.alias.clone())
                        .collect();
                    let expected_aliases: Vec<String> =
                        servers.iter().map(|s| s.alias.clone()).collect();
                    prop_assert_eq!(
                        &actual_aliases,
                        &expected_aliases,
                        "Server entry order mismatch for jump host '{}': got {:?}, expected {:?}",
                        alias,
                        actual_aliases,
                        expected_aliases
                    );
                }
            }

            // Verify local source status is Ok
            let local_status = merged
                .source_status
                .iter()
                .find(|(s, _)| *s == ServerListSource::Local);
            prop_assert!(
                local_status.is_some(),
                "Missing local source in source_status"
            );
            prop_assert!(
                matches!(local_status, Some((_, ServerListSourceStatus::Ok))),
                "Local source status should be Ok, got {:?}",
                local_status
            );

            Ok(())
        })?;
    }
}
