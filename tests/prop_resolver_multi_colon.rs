//! Feature: server-list-path-prefix, Property 7: Resolver first-colon split preserves multi-level targets
//!
//! Property-based test validating that the Resolver correctly splits multi-colon
//! target strings on the first colon only, preserving the full remainder as end_target.

use proptest::prelude::*;

use rhop::config::{AppConfig, DirectGatewayConfig, GatewayConfig, ServerConfigFile};
use rhop::daemon::resolver::Resolver;

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating gateway names: starts with [a-z], followed by [a-z0-9_]{0,11}.
/// No colons, not "local", non-empty.
fn arb_gateway_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,11}".prop_filter("must not be 'local'", |s| s != "local")
}

/// Strategy for generating the rest portion of a target: non-empty, not purely numeric,
/// may contain colons. Starts with [a-z], followed by [a-z0-9:]{0,15}.
fn arb_rest() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9:]{0,15}".prop_filter("must not be purely numeric", |s| {
        !s.chars().all(|c| c.is_ascii_digit())
    })
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 3.1, 3.2**
    ///
    /// For any gateway_name (no colons, non-empty, not "local") and rest (non-empty,
    /// not purely numeric, may contain colons), resolving "<gateway_name>:<rest>"
    /// through the Resolver SHALL produce a Route with gateway_name as the gateway
    /// and rest as the end_target.
    #[test]
    fn prop_resolver_first_colon_split_preserves_multi_level_targets(
        gateway_name in arb_gateway_name(),
        rest in arb_rest(),
    ) {
        let config = AppConfig::default();
        let server_config = ServerConfigFile::default();

        // Build a gateway config matching the generated name (Direct is simplest).
        let gateways = vec![GatewayConfig::Direct(DirectGatewayConfig {
            name: gateway_name.clone(),
            host: "10.0.0.1".to_string(),
            port: 22,
            user: "testuser".to_string(),
            identity_file: "/tmp/test_key".to_string(),
            password: None,
        })];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let target = format!("{}:{}", gateway_name, rest);
        let routes = resolver.resolve(&target).unwrap();

        prop_assert_eq!(routes.len(), 1, "Expected exactly 1 route, got {}", routes.len());
        prop_assert_eq!(
            &routes[0].gateway_name, &gateway_name,
            "Expected gateway_name='{}', got '{}'",
            gateway_name, routes[0].gateway_name
        );
        prop_assert_eq!(
            &routes[0].end_target, &rest,
            "Expected end_target='{}', got '{}'",
            rest, routes[0].end_target
        );
    }
}
