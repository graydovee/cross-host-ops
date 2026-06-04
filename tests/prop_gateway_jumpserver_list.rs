//! Property-based test: JumpserverGateway list_servers never opens a connection.
//!
//! Feature: gateway-refactor, Property 2: JumpserverGateway list_servers never opens a connection
//!
//! For any JumpserverGateway instance (regardless of configuration), calling
//! `list_servers()` SHALL return an `Unsupported` error immediately without
//! triggering any network I/O, PTY shell creation, or SSH connection attempt.
//!
//! **Validates: Requirements 5.4, 6.4**

use std::sync::Arc;

use proptest::prelude::*;

use xho::config::{AppConfig, JumpserverGatewayConfig, MfaConfig};
use xho::daemon::gateway::auth::{AuthPrompt, AuthPrompter};
use xho::daemon::gateway::jumpserver::JumpserverGateway;
use xho::daemon::gateway::{ErrorKind, Gateway};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a dummy AuthPrompter that panics if ever called.
/// This verifies that list_servers() never triggers authentication or
/// any network I/O path.
fn panic_auth_prompter() -> Arc<AuthPrompter> {
    Arc::new(|_prompt: AuthPrompt| -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send>> {
        panic!("AuthPrompter should never be called during list_servers()")
    })
}

/// Strategy for generating a random gateway name (lowercase alpha, 1-12 chars).
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

/// Strategy for generating a random user (lowercase alpha, 1-8 chars).
fn arb_user() -> impl Strategy<Value = String> {
    "[a-z]{1,8}"
}

/// Strategy for generating a random identity file path.
fn arb_identity_file() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("/tmp/test_key".to_string()),
        Just("/home/user/.ssh/id_ed25519".to_string()),
        "[a-z/]{4,20}".prop_map(|s| format!("/tmp/{}", s)),
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

/// Strategy for generating random shell prompt suffixes (kept for reference).
#[allow(dead_code)]
fn arb_shell_prompt_suffixes() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[#$>]{1,3}", 1..=4)
}

/// Strategy for generating a random JumpserverGatewayConfig.
fn arb_jumpserver_fields() -> impl Strategy<Value = JumpserverGatewayConfig> {
    (
        arb_gateway_name(),
        arb_host(),
        1u16..=65535u16,
        arb_user(),
        arb_identity_file(),
        prop::option::of("[a-z+,-]{5,30}"),
        arb_mfa_config(),
    )
        .prop_map(
            |(
                name,
                host,
                port,
                user,
                identity_file,
                pubkey_accepted_algorithms,
                mfa,
            )| {
                JumpserverGatewayConfig {
                    name,
                    host,
                    port,
                    user,
                    identity_file,
                    pubkey_accepted_algorithms,
                    totp_secret_base32: mfa.totp_secret_base32,
                    totp_digits: mfa.digits,
                    totp_period: mfa.period,
                }
            },
        )
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, .. ProptestConfig::default() })]

    /// **Validates: Requirements 5.4, 6.4**
    ///
    /// For any random JumpserverGateway configuration, calling list_servers()
    /// SHALL return an Unsupported error immediately without triggering any
    /// network I/O, PTY shell creation, or SSH connection attempt.
    ///
    /// The panic-on-call auth prompter ensures no authentication path is invoked.
    /// The random hosts are unreachable, so any actual connection attempt would
    /// fail or timeout — but the test completes instantly because list_servers()
    /// returns immediately without I/O.
    #[test]
    fn prop_jumpserver_list_servers_returns_unsupported(
        fields in arb_jumpserver_fields()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Build a minimal AppConfig
            let config = Arc::new(tokio::sync::RwLock::new(AppConfig::default()));

            let gateway_name = fields.name.clone();

            // Construct JumpserverGateway with a panic auth prompter.
            // If any network I/O or auth were attempted, this would panic.
            let gateway = JumpserverGateway::new(
                gateway_name,
                config,
                fields,
                panic_auth_prompter(),
            );

            // Call list_servers — must return immediately with Unsupported error
            let result = gateway.list_servers().await;

            // Verify it is an error
            prop_assert!(result.is_err(), "list_servers() should return an error");

            // Verify the error kind is Unsupported
            let err = result.unwrap_err();
            prop_assert_eq!(
                err.kind,
                ErrorKind::Unsupported,
                "expected ErrorKind::Unsupported, got {:?}",
                err.kind
            );

            Ok(())
        })?;
    }
}
