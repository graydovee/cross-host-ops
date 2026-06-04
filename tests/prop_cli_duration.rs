//! Property-based test for duration string parsing round-trip.
//!
//! Verifies that `parse_duration` correctly converts duration strings
//! in the format `<number><unit>` (where unit is s, m, or h) into the
//! expected Duration value.
#![allow(clippy::collapsible_if)]

use std::time::Duration;

use proptest::prelude::*;

use xho::config::parse_duration;

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating a duration unit and corresponding numeric range.
/// - "s" (seconds): N in 1..=86400
/// - "m" (minutes): N in 1..=1440
/// - "h" (hours):   N in 1..=24
fn arb_duration_input() -> impl Strategy<Value = (u64, &'static str, u64)> {
    prop_oneof![
        // (N, unit, expected_seconds)
        (1u64..=86400u64).prop_map(|n| (n, "s", n)),
        (1u64..=1440u64).prop_map(|n| (n, "m", n * 60)),
        (1u64..=24u64).prop_map(|n| (n, "h", n * 3600)),
    ]
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

// Feature: cli-command-tree-refactor, Property 4: Duration string parsing round-trip
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.7**
    ///
    /// For any numeric value N in the valid range for a given unit U in {s, m, h},
    /// parsing the string `format!("{}{}", N, U)` SHALL produce a Duration equal
    /// to the expected number of seconds:
    /// - "Ns" → N seconds
    /// - "Nm" → N * 60 seconds
    /// - "Nh" → N * 3600 seconds
    #[test]
    fn prop_duration_string_parsing_round_trip(
        (n, unit, expected_secs) in arb_duration_input(),
    ) {
        let input = format!("{}{}", n, unit);
        let result = parse_duration(&input);

        prop_assert!(
            result.is_ok(),
            "parse_duration({:?}) should succeed, got: {:?}",
            input,
            result.err()
        );

        let duration = result.unwrap();
        let expected = Duration::from_secs(expected_secs);

        prop_assert_eq!(
            duration, expected,
            "parse_duration({:?}) = {:?}, expected {:?} ({} seconds)",
            input, duration, expected, expected_secs
        );
    }
}
