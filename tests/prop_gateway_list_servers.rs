//! Property-based test: LocalGateway list_servers reads config only.
//!
//! Feature: gateway-refactor, Property 3: LocalGateway list_servers reads config only
//!
//! For any LocalGateway instance, calling `list_servers()` SHALL return server
//! entries derived solely from the server.toml configuration file, without
//! establishing any SSH connection or performing any network I/O.
//!
//! **Validates: Requirements 3.5, 6.3**

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use tempfile::NamedTempFile;

use xho::config::AppConfig;
use xho::daemon::gateway::auth::{AuthPrompt, AuthPrompter};
use xho::daemon::gateway::local::LocalGateway;
use xho::daemon::gateway::Gateway;
use xho::types::ServerListSource;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a dummy AuthPrompter that panics if ever called.
/// This verifies that list_servers() never triggers authentication.
fn panic_auth_prompter() -> Arc<AuthPrompter> {
    Arc::new(|_prompt: AuthPrompt| -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send>> {
        panic!("AuthPrompter should never be called during list_servers()")
    })
}

/// Represents a generated server entry for test input.
#[derive(Clone, Debug)]
struct GenServerEntry {
    alias: String,
    host: String,
    port: u16,
    user: String,
    /// true = use identity_file auth, false = use password auth
    use_key: bool,
}

/// Generate a server.toml file content from a list of generated entries.
/// Uses a defaults.identity_file so that key-based entries don't need one per-entry.
fn build_server_toml(entries: &[GenServerEntry]) -> String {
    let mut content = String::new();
    content.push_str("[defaults]\n");
    content.push_str("identity_file = \"/tmp/test_default_key\"\n");
    content.push_str("\n");

    for entry in entries {
        content.push_str(&format!("[servers.{}]\n", entry.alias));
        content.push_str(&format!("host = \"{}\"\n", entry.host));
        content.push_str(&format!("port = {}\n", entry.port));
        content.push_str(&format!("user = \"{}\"\n", entry.user));
        if !entry.use_key {
            content.push_str(&format!("password = \"pass_{}\"\n", entry.alias));
        }
        content.push_str("\n");
    }

    content
}

/// Strategy for generating a valid alias (lowercase alpha, 1-8 chars).
fn arb_alias() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{0,7}"
}

/// Strategy for generating a valid host (IP-like pattern).
fn arb_host() -> impl Strategy<Value = String> {
    (1u8..=254u8, 0u8..=255u8, 0u8..=255u8, 1u8..=254u8).prop_map(|(a, b, c, d)| {
        format!("{}.{}.{}.{}", a, b, c, d)
    })
}

/// Strategy for generating a valid user (lowercase alpha, 1-8 chars).
fn arb_user() -> impl Strategy<Value = String> {
    "[a-z]{1,8}"
}

/// Strategy for generating a single GenServerEntry.
fn arb_gen_server_entry() -> impl Strategy<Value = GenServerEntry> {
    (arb_alias(), arb_host(), 1u16..=65535u16, arb_user(), any::<bool>()).prop_map(
        |(alias, host, port, user, use_key)| GenServerEntry {
            alias,
            host,
            port,
            user,
            use_key,
        },
    )
}

/// Strategy for generating a list of GenServerEntry with unique aliases.
/// Generates 0-20 entries, deduplicating by alias.
fn arb_server_entries() -> impl Strategy<Value = Vec<GenServerEntry>> {
    prop::collection::vec(arb_gen_server_entry(), 0..=20).prop_map(|entries| {
        let mut seen = HashSet::new();
        entries
            .into_iter()
            .filter(|e| seen.insert(e.alias.clone()))
            .collect()
    })
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, .. ProptestConfig::default() })]

    /// **Validates: Requirements 3.5, 6.3**
    ///
    /// For any random server.toml configuration (0–20 entries), constructing
    /// a LocalGateway and calling list_servers() SHALL:
    /// 1. Return entries derived solely from the config file
    /// 2. Not establish any SSH connection (verified by panic-on-call auth prompter
    ///    and absence of real network targets)
    /// 3. Return the exact same set of aliases, hosts, ports, and users as written
    #[test]
    fn prop_list_servers_reads_config_only(entries in arb_server_entries()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Write the generated config to a temp file
            let tmp_file = NamedTempFile::new().unwrap();
            let toml_content = build_server_toml(&entries);
            std::fs::write(tmp_file.path(), &toml_content).unwrap();

            // Build a minimal AppConfig (needed for the RwLock)
            let config = Arc::new(tokio::sync::RwLock::new(AppConfig::default()));

            // Construct LocalGateway with a panic auth prompter
            // If any network I/O were attempted, it would fail since
            // the hosts are random IPs and the auth prompter panics.
            let gateway = LocalGateway::new(
                "local".to_string(),
                config,
                tmp_file.path().to_string_lossy().to_string(),
                panic_auth_prompter(),
                10, // max_connections_per_address
                Duration::from_secs(600), // max_idle_time
            );

            // Call list_servers - this should only read the file, no network I/O
            let result = gateway.list_servers().await;

            // Verify the result is Ok
            let server_rows = result.expect("list_servers should succeed for valid config");

            // Verify the count matches
            prop_assert_eq!(
                server_rows.len(),
                entries.len(),
                "entry count mismatch: got {} but expected {}",
                server_rows.len(),
                entries.len()
            );

            // Property 6: All rows from LocalGateway must have source == Local
            for row in &server_rows {
                prop_assert_eq!(
                    &row.source,
                    &ServerListSource::Local,
                    "LocalGateway row should have source == Local, got {:?}",
                    row.source
                );
            }

            // Build expected set of (alias, host, port, user) tuples
            let mut expected: Vec<(String, String, u16, String)> = entries
                .iter()
                .map(|e| (e.alias.clone(), e.host.clone(), e.port, e.user.clone()))
                .collect();
            expected.sort();

            // Build actual set from the returned rows (unwrap row.server)
            let mut actual: Vec<(String, String, u16, String)> = server_rows
                .iter()
                .map(|row| (row.server.alias.clone(), row.server.host.clone(), row.server.port, row.server.user.clone()))
                .collect();
            actual.sort();

            prop_assert_eq!(
                &actual,
                &expected,
                "server entries do not match the generated config"
            );

            Ok(())
        })?;
    }
}
