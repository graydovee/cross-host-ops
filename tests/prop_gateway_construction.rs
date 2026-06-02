//! Property-based test: Gateway construction is I/O-free.
//!
//! Feature: gateway-refactor, Property 1: Gateway construction is I/O-free
//!
//! For any valid configuration (AppConfig with any combination of jump_hosts
//! entries), constructing all Gateways via `build_gateways` SHALL complete
//! without establishing any network connection (no TCP connect, no SSH
//! handshake, no gRPC dial).
//!
//! **Validates: Requirements 6.1, 10.5**

use std::sync::Arc;

use proptest::prelude::*;

use rhop::config::{
    AppConfig, DirectAuth, DirectJumpHostFields, JumpHostConfig, JumpHostFields,
    JumpserverJumpHostFields, MfaConfig, RhopdJumpHostFields,
};
use rhop::daemon::gateway::auth::{AuthPrompt, AuthPrompter};
use rhop::daemon::gateway::build_gateways;
use rhop::jump::JumpHostKind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an AuthPrompter that panics if ever called.
/// This verifies that construction never triggers authentication or any
/// network I/O path.
fn panic_auth_prompter() -> Arc<AuthPrompter> {
    Arc::new(|_prompt: AuthPrompt| -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send>> {
        panic!("AuthPrompter should never be called during Gateway construction")
    })
}

// ---------------------------------------------------------------------------
// Strategies for generating random JumpHostConfig entries
// ---------------------------------------------------------------------------

/// Strategy for generating a unique gateway name (lowercase alpha + digits, 1-12 chars).
fn arb_gateway_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,11}"
}

/// Strategy for generating a random host string.
fn arb_host() -> impl Strategy<Value = String> {
    prop_oneof![
        // IP-like hosts
        (1u8..=254u8, 0u8..=255u8, 0u8..=255u8, 1u8..=254u8)
            .prop_map(|(a, b, c, d)| format!("{}.{}.{}.{}", a, b, c, d)),
        // Hostname-like strings
        "[a-z]{1,8}\\.[a-z]{2,4}",
    ]
}

/// Strategy for generating a random address (host:port).
fn arb_address() -> impl Strategy<Value = String> {
    (arb_host(), 1u16..=65535u16).prop_map(|(host, port)| format!("{}:{}", host, port))
}

/// Strategy for generating a random user (lowercase alpha, 1-8 chars).
fn arb_user() -> impl Strategy<Value = String> {
    "[a-z]{1,8}"
}

/// Strategy for generating a random file path.
fn arb_file_path() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("/tmp/test_key".to_string()),
        Just("/home/user/.ssh/id_ed25519".to_string()),
        "[a-z]{3,10}".prop_map(|s| format!("/tmp/{}", s)),
    ]
}

/// Strategy for generating a random MfaConfig.
fn arb_mfa_config() -> impl Strategy<Value = MfaConfig> {
    prop_oneof![
        // No MFA (empty secret)
        Just(MfaConfig {
            totp_secret_base32: String::new(),
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        }),
        // With MFA secret
        "[A-Z2-7]{16,32}".prop_map(|secret| MfaConfig {
            totp_secret_base32: secret,
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        }),
    ]
}

/// Strategy for generating a Rhopd JumpHostConfig.
fn arb_rhopd_jump_host(name: String) -> impl Strategy<Value = JumpHostConfig> {
    (arb_address(), arb_file_path(), arb_file_path()).prop_map(
        move |(address, identity_file, known_hosts_path)| JumpHostConfig {
            name: name.clone(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address,
                identity_file,
                known_hosts_path,
            }),
        },
    )
}

/// Strategy for generating a Jumpserver JumpHostConfig.
fn arb_jumpserver_jump_host(name: String) -> impl Strategy<Value = JumpHostConfig> {
    (
        arb_host(),
        1u16..=65535u16,
        arb_user(),
        arb_file_path(),
        arb_mfa_config(),
    )
        .prop_map(move |(host, port, user, identity_file, mfa)| JumpHostConfig {
            name: name.clone(),
            kind: JumpHostKind::Jumpserver,
            fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                host,
                port,
                user,
                identity_file,
                pubkey_accepted_algorithms: None,
                menu_prompt_contains: "Opt".to_string(),
                mfa_prompt_contains: "MFA".to_string(),
                shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                mfa,
            }),
        })
}

/// Strategy for generating a Direct JumpHostConfig.
fn arb_direct_jump_host(name: String) -> impl Strategy<Value = JumpHostConfig> {
    (arb_host(), 1u16..=65535u16, arb_user(), arb_file_path()).prop_map(
        move |(host, port, user, identity_file)| JumpHostConfig {
            name: name.clone(),
            kind: JumpHostKind::Direct,
            fields: JumpHostFields::Direct(DirectJumpHostFields {
                host,
                port,
                user,
                auth: DirectAuth::Key { identity_file },
            }),
        },
    )
}

/// Strategy for generating a single JumpHostConfig of any variant.
fn arb_jump_host_config(name: String) -> impl Strategy<Value = JumpHostConfig> {
    prop_oneof![
        arb_rhopd_jump_host(name.clone()),
        arb_jumpserver_jump_host(name.clone()),
        arb_direct_jump_host(name),
    ]
}

/// Strategy for generating a Vec of 0-5 JumpHostConfig entries with unique names.
/// Names are guaranteed unique and not "local" (reserved).
fn arb_jump_hosts_vec() -> impl Strategy<Value = Vec<JumpHostConfig>> {
    proptest::collection::hash_set(arb_gateway_name(), 0..=5)
        .prop_filter("names must not be 'local'", |names| {
            names.iter().all(|n| n != "local")
        })
        .prop_flat_map(|names| {
            let strategies: Vec<_> = names
                .into_iter()
                .map(|name| arb_jump_host_config(name).boxed())
                .collect();
            strategies
        })
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, .. ProptestConfig::default() })]

    /// **Validates: Requirements 6.1, 10.5**
    ///
    /// For any valid configuration (0-5 jump_hosts of any kind), constructing
    /// all Gateways via `build_gateways` SHALL complete without establishing
    /// any network connection.
    ///
    /// Verification:
    /// - The AuthPrompter panics if called (no auth attempt during construction)
    /// - All jump host addresses are random/unreachable, so any real connection
    ///   would fail or timeout — but the test completes instantly because
    ///   construction is I/O-free
    /// - The returned HashMap has the correct number of entries: 1 (local) + N (jump_hosts)
    #[test]
    fn prop_gateway_construction_is_io_free(
        jump_hosts in arb_jump_hosts_vec()
    ) {
        let config = Arc::new(tokio::sync::RwLock::new(AppConfig::default()));
        let auth_prompter = panic_auth_prompter();

        let expected_count = 1 + jump_hosts.len(); // 1 for "local" + 1 per jump_host

        // This must complete instantly without panic (no I/O, no auth).
        let gateways = build_gateways(
            config,
            "/tmp/nonexistent_server.toml",
            &jump_hosts,
            auth_prompter,
        );

        // Verify correct number of gateways constructed.
        prop_assert_eq!(
            gateways.len(),
            expected_count,
            "expected {} gateways (1 local + {} jump_hosts), got {}",
            expected_count,
            jump_hosts.len(),
            gateways.len()
        );

        // Verify "local" gateway is always present.
        prop_assert!(
            gateways.contains_key("local"),
            "gateways map must contain the 'local' key"
        );

        // Verify each jump_host name is present as a gateway.
        for jh in &jump_hosts {
            prop_assert!(
                gateways.contains_key(&jh.name),
                "gateways map must contain key '{}'",
                jh.name
            );
        }
    }
}
