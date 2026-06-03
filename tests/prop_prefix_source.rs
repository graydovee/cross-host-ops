//! Property-based tests for `prefix_source` function.
//!
//! Feature: server-list-path-prefix
//!
//! Property 1: Path prefix concatenation correctness
//! Property 2: Path prefix round-trip (first-colon split recovers gateway name)

use proptest::prelude::*;
use rhop::daemon::rpc::prefix_source;
use rhop::types::ServerListSource;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Generate a gateway name that contains NO colons (regex: [a-z][a-z0-9_]{0,11}).
fn arb_gateway_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,11}"
}

/// Generate an arbitrary remote source string (may contain colons, "local", empty, etc).
fn arb_remote_source() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("local".to_string()),
        Just("".to_string()),
        "[a-z][a-z0-9_:-]{0,20}",
    ]
}

/// Generate a non-"local", non-empty remote source (for testing the concatenation branch).
fn arb_non_local_remote_source() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_:-]{0,20}".prop_filter(
        "must not be \"local\" or empty",
        |s| s != "local" && !s.is_empty(),
    )
}

// ---------------------------------------------------------------------------
// Property 1: Path prefix concatenation correctness
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.2, 1.3, 1.4**
    ///
    /// For any gateway_name and remote_source == "local", prefix_source SHALL
    /// produce ServerListSource::Gateway(gateway_name).
    #[test]
    fn prop_prefix_source_local_omits_suffix(gateway_name in arb_gateway_name()) {
        let result = prefix_source(&gateway_name, "local");
        prop_assert_eq!(result, ServerListSource::Gateway(gateway_name));
    }

    /// **Validates: Requirements 1.2, 1.3, 1.4**
    ///
    /// For any gateway_name and empty remote_source, prefix_source SHALL
    /// produce ServerListSource::Gateway(gateway_name).
    #[test]
    fn prop_prefix_source_empty_omits_suffix(gateway_name in arb_gateway_name()) {
        let result = prefix_source(&gateway_name, "");
        prop_assert_eq!(result, ServerListSource::Gateway(gateway_name));
    }

    /// **Validates: Requirements 1.2, 1.3, 1.4**
    ///
    /// For any gateway_name and non-"local"/non-empty remote_source, prefix_source
    /// SHALL produce ServerListSource::Gateway(format!("{}:{}", gateway_name, remote_source)).
    #[test]
    fn prop_prefix_source_concatenates_with_colon(
        gateway_name in arb_gateway_name(),
        remote_source in arb_non_local_remote_source()
    ) {
        let result = prefix_source(&gateway_name, &remote_source);
        let expected = ServerListSource::Gateway(format!("{}:{}", gateway_name, remote_source));
        prop_assert_eq!(result, expected);
    }
}

// ---------------------------------------------------------------------------
// Property 2: Path prefix round-trip (first-colon split recovers gateway name)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.5**
    ///
    /// For any gateway_name (no colons) and any remote_source, applying
    /// prefix_source then splitting the Gateway(value) on the first colon
    /// SHALL recover the original gateway_name as the first segment.
    #[test]
    fn prop_prefix_source_round_trip_first_colon_split(
        gateway_name in arb_gateway_name(),
        remote_source in arb_remote_source()
    ) {
        let result = prefix_source(&gateway_name, &remote_source);

        // Extract the inner value from Gateway variant
        let value = match result {
            ServerListSource::Gateway(ref v) => v.as_str(),
            _ => panic!("prefix_source should always return Gateway variant"),
        };

        // Split on first colon to recover gateway name
        let first_segment = if let Some(colon_pos) = value.find(':') {
            &value[..colon_pos]
        } else {
            // No colon means value IS the gateway name (local/empty case)
            value
        };

        prop_assert_eq!(
            first_segment,
            gateway_name.as_str(),
            "First colon-split segment '{}' should equal gateway_name '{}'",
            first_segment,
            gateway_name
        );
    }
}
