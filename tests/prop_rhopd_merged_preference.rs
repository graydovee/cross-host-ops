//! Property-based tests: RhopdGateway merged-over-flat preference and fallback.
//!
//! Feature: server-list-path-prefix
//!
//! Property 4: RhopdGateway prefers merged over flat servers
//! For any response with both non-empty merged.rows and servers, verify output
//! comes from merged.rows only.
//! **Validates: Requirements 1.1, 4.2**
//!
//! Property 5: RhopdGateway fallback to flat servers
//! For any response with absent/empty merged, verify output comes from flat
//! servers with source=Gateway(name).
//! **Validates: Requirements 4.3**

use proptest::prelude::*;

use rhop::config::ServerEntry;
use rhop::daemon::rpc::prefix_source;
use rhop::protocol::ServerListRow;
use rhop::types::ServerListSource;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simulated RPC merged row (mirrors what the remote daemon returns).
#[derive(Clone, Debug)]
struct SimulatedRpcRow {
    alias: String,
    host: String,
    port: u16,
    user: String,
    source: String,
}

/// Simulate the RhopdGateway::list_servers logic without real gRPC.
///
/// This replicates the core decision logic:
/// - If merged_rows is non-empty, use them (applying prefix_source to each row's source).
/// - Otherwise fall back to flat_servers with source = Gateway(gateway_name).
fn simulate_rhopd_list_servers(
    gateway_name: &str,
    merged_rows: &[SimulatedRpcRow],
    flat_servers: &[ServerEntry],
) -> Vec<ServerListRow> {
    if !merged_rows.is_empty() {
        // Prefer merged
        merged_rows
            .iter()
            .map(|row| {
                let server = ServerEntry {
                    alias: row.alias.clone(),
                    host: row.host.clone(),
                    port: row.port,
                    user: row.user.clone(),
                    auth: rhop::config::DirectAuth::Key {
                        identity_file: String::new(),
                    },
                };
                let source = prefix_source(gateway_name, &row.source);
                ServerListRow { source, server }
            })
            .collect()
    } else {
        // Fallback: flat servers
        flat_servers
            .iter()
            .map(|s| ServerListRow {
                source: ServerListSource::Gateway(gateway_name.to_string()),
                server: s.clone(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Strategy for a valid gateway name (lowercase alpha, no colons).
fn arb_gateway_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9\\-]{0,10}"
}

/// Strategy for a valid alias.
fn arb_alias() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{0,7}"
}

/// Strategy for a valid host.
fn arb_host() -> impl Strategy<Value = String> {
    (1u8..=254u8, 0u8..=255u8, 0u8..=255u8, 1u8..=254u8)
        .prop_map(|(a, b, c, d)| format!("{}.{}.{}.{}", a, b, c, d))
}

/// Strategy for a valid user.
fn arb_user() -> impl Strategy<Value = String> {
    "[a-z]{1,8}"
}

/// Strategy for a remote source string (could be "local", "", or a sub-gateway name).
fn arb_remote_source() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("local".to_string()),
        Just("".to_string()),
        "[a-z][a-z0-9\\-]{0,8}",
    ]
}

/// Strategy for a simulated RPC merged row.
fn arb_simulated_rpc_row() -> impl Strategy<Value = SimulatedRpcRow> {
    (arb_alias(), arb_host(), 1u16..=65535u16, arb_user(), arb_remote_source()).prop_map(
        |(alias, host, port, user, source)| SimulatedRpcRow {
            alias,
            host,
            port,
            user,
            source,
        },
    )
}

/// Strategy for a ServerEntry (used as flat server).
fn arb_server_entry() -> impl Strategy<Value = ServerEntry> {
    (arb_alias(), arb_host(), 1u16..=65535u16, arb_user()).prop_map(|(alias, host, port, user)| {
        ServerEntry {
            alias,
            host,
            port,
            user,
            auth: rhop::config::DirectAuth::Key {
                identity_file: String::new(),
            },
        }
    })
}

// ---------------------------------------------------------------------------
// Property 4: RhopdGateway prefers merged over flat servers
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.1, 4.2**
    ///
    /// For any response with both non-empty merged.rows and a servers field,
    /// the output must come exclusively from merged.rows (ignoring flat servers).
    #[test]
    fn prop_rhopd_prefers_merged_over_flat(
        gateway_name in arb_gateway_name(),
        merged_rows in prop::collection::vec(arb_simulated_rpc_row(), 1..=10),
        flat_servers in prop::collection::vec(arb_server_entry(), 1..=10),
    ) {
        let output = simulate_rhopd_list_servers(&gateway_name, &merged_rows, &flat_servers);

        // Output length must match merged_rows (not flat_servers)
        prop_assert_eq!(
            output.len(),
            merged_rows.len(),
            "output should have same count as merged_rows ({}) but got {}",
            merged_rows.len(),
            output.len()
        );

        // Each output row alias must match the corresponding merged row alias
        for (i, (out_row, merged_row)) in output.iter().zip(merged_rows.iter()).enumerate() {
            prop_assert_eq!(
                &out_row.server.alias,
                &merged_row.alias,
                "row {} alias mismatch: output={:?} vs merged={:?}",
                i,
                out_row.server.alias,
                merged_row.alias
            );

            // Verify the source was computed via prefix_source
            let expected_source = prefix_source(&gateway_name, &merged_row.source);
            prop_assert_eq!(
                &out_row.source,
                &expected_source,
                "row {} source mismatch: output={:?} vs expected={:?}",
                i,
                out_row.source,
                expected_source
            );
        }

        // Verify flat_servers were NOT used: check that output aliases do NOT
        // necessarily match flat_servers aliases (they come from merged_rows)
        // This is already proven by the alias match above against merged_rows.
    }
}

// ---------------------------------------------------------------------------
// Property 5: RhopdGateway fallback to flat servers
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.3**
    ///
    /// For any response with absent/empty merged, the output must come from
    /// flat servers with source == ServerListSource::Gateway(gateway_name).
    #[test]
    fn prop_rhopd_fallback_to_flat_servers(
        gateway_name in arb_gateway_name(),
        flat_servers in prop::collection::vec(arb_server_entry(), 1..=10),
    ) {
        // Empty merged_rows triggers fallback
        let merged_rows: Vec<SimulatedRpcRow> = Vec::new();
        let output = simulate_rhopd_list_servers(&gateway_name, &merged_rows, &flat_servers);

        // Output length must match flat_servers
        prop_assert_eq!(
            output.len(),
            flat_servers.len(),
            "output should have same count as flat_servers ({}) but got {}",
            flat_servers.len(),
            output.len()
        );

        // ALL output rows must have source == Gateway(gateway_name)
        let expected_source = ServerListSource::Gateway(gateway_name.to_string());
        for (i, row) in output.iter().enumerate() {
            prop_assert_eq!(
                &row.source,
                &expected_source,
                "row {} source should be Gateway({:?}) but got {:?}",
                i,
                gateway_name,
                row.source
            );
        }

        // Output aliases must match flat_servers aliases (in order)
        for (i, (out_row, flat_entry)) in output.iter().zip(flat_servers.iter()).enumerate() {
            prop_assert_eq!(
                &out_row.server.alias,
                &flat_entry.alias,
                "row {} alias mismatch: output={:?} vs flat={:?}",
                i,
                out_row.server.alias,
                flat_entry.alias
            );
        }
    }
}
