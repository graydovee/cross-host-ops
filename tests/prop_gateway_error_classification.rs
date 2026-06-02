//! Property-based test: Error classification correctness.
//!
//! Feature: gateway-refactor, Property 8: Error classification correctness
//!
//! For any `anyhow::Error` produced during Gateway operations:
//! - `tonic::Status` with codes Unavailable/Cancelled/Unknown/Internal SHALL be
//!   classified as Transport
//! - Any `russh::Error` SHALL be classified as Transport
//! - Errors containing "not found" or "unknown target" SHALL be classified as Resolution
//! - Other errors SHALL NOT be classified as Transport
//!
//! **Validates: Requirements 8.4**

use proptest::prelude::*;

use rhop::daemon::gateway::{is_resolution_error, is_transport_error};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Tonic status codes that SHOULD be classified as transport errors.
fn arb_transport_tonic_code() -> impl Strategy<Value = tonic::Code> {
    prop_oneof![
        Just(tonic::Code::Unavailable),
        Just(tonic::Code::Cancelled),
        Just(tonic::Code::Unknown),
        Just(tonic::Code::Internal),
    ]
}

/// Tonic status codes that should NOT be classified as transport errors.
fn arb_non_transport_tonic_code() -> impl Strategy<Value = tonic::Code> {
    prop_oneof![
        Just(tonic::Code::Ok),
        Just(tonic::Code::InvalidArgument),
        Just(tonic::Code::NotFound),
        Just(tonic::Code::AlreadyExists),
        Just(tonic::Code::PermissionDenied),
        Just(tonic::Code::ResourceExhausted),
        Just(tonic::Code::FailedPrecondition),
        Just(tonic::Code::Aborted),
        Just(tonic::Code::OutOfRange),
        Just(tonic::Code::Unimplemented),
        Just(tonic::Code::DataLoss),
        Just(tonic::Code::Unauthenticated),
        Just(tonic::Code::DeadlineExceeded),
    ]
}

/// Strings that contain resolution-indicative patterns.
fn arb_resolution_message() -> impl Strategy<Value = String> {
    prop_oneof![
        // Contains "not found"
        "[a-z]{1,8}".prop_map(|prefix| format!("{} not found", prefix)),
        "[a-z]{1,8}".prop_map(|prefix| format!("target {} not found in config", prefix)),
        // Contains "unknown target"
        "[a-z]{1,8}".prop_map(|suffix| format!("unknown target: {}", suffix)),
        // Contains "no match"
        "[a-z]{1,8}".prop_map(|suffix| format!("no match for {}", suffix)),
    ]
}

/// Strings that contain transport-indicative patterns (heuristic fallback).
fn arb_transport_message() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("channel closed".to_string()),
        Just("connection closed unexpectedly".to_string()),
        Just("broken pipe".to_string()),
        Just("connection reset by peer".to_string()),
        Just("send error: channel full".to_string()),
        "[a-z]{1,6}".prop_map(|prefix| format!("{}: channel closed", prefix)),
        "[a-z]{1,6}".prop_map(|prefix| format!("{}: broken pipe", prefix)),
        "[a-z]{1,6}".prop_map(|prefix| format!("{}: connection reset", prefix)),
        "[a-z]{1,6}".prop_map(|prefix| format!("{}: send error", prefix)),
    ]
}

/// Strings that should NOT be classified as transport or resolution errors.
/// Avoids any substring that would match the heuristic patterns.
fn arb_neutral_message() -> impl Strategy<Value = String> {
    // Generate strings from a safe alphabet that cannot accidentally contain
    // "channel closed", "broken pipe", "connection reset", "send error",
    // "not found", "no match", "unknown target", "closed unexpectedly"
    "[0-9]{4,12}".prop_map(|digits| format!("generic error code {}", digits))
}

/// Strategy for a random tonic status message (doesn't affect classification).
fn arb_status_message() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("service unavailable".to_string()),
        Just("request cancelled".to_string()),
        Just("internal error".to_string()),
        "[a-z ]{1,20}",
    ]
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, .. ProptestConfig::default() })]

    /// **Validates: Requirements 8.4**
    ///
    /// tonic::Status with codes Unavailable/Cancelled/Unknown/Internal
    /// SHALL be classified as transport errors.
    #[test]
    fn prop_tonic_transport_codes_classified_as_transport(
        code in arb_transport_tonic_code(),
        msg in arb_status_message(),
    ) {
        let status = tonic::Status::new(code, &msg);
        let error: anyhow::Error = anyhow::Error::from(status);

        prop_assert!(
            is_transport_error(&error),
            "tonic::Status with code {:?} should be classified as transport, but was not",
            code
        );
    }

    /// **Validates: Requirements 8.4**
    ///
    /// tonic::Status with non-transport codes (Ok, NotFound, PermissionDenied,
    /// etc.) SHALL NOT be classified as transport errors.
    #[test]
    fn prop_tonic_non_transport_codes_not_classified_as_transport(
        code in arb_non_transport_tonic_code(),
        msg in arb_neutral_message(),
    ) {
        let status = tonic::Status::new(code, &msg);
        let error: anyhow::Error = anyhow::Error::from(status);

        prop_assert!(
            !is_transport_error(&error),
            "tonic::Status with code {:?} should NOT be classified as transport, but was",
            code
        );
    }

    /// **Validates: Requirements 8.4**
    ///
    /// Errors containing "not found", "no match", or "unknown target"
    /// SHALL be classified as resolution errors.
    #[test]
    fn prop_resolution_messages_classified_as_resolution(
        msg in arb_resolution_message(),
    ) {
        let error = anyhow::anyhow!("{}", msg);

        prop_assert!(
            is_resolution_error(&error),
            "Error with message '{}' should be classified as resolution, but was not",
            msg
        );
    }

    /// **Validates: Requirements 8.4**
    ///
    /// Errors containing transport-indicative strings ("channel closed",
    /// "broken pipe", "connection reset", "send error") SHALL be classified
    /// as transport errors via the heuristic fallback.
    #[test]
    fn prop_transport_messages_classified_as_transport(
        msg in arb_transport_message(),
    ) {
        let error = anyhow::anyhow!("{}", msg);

        prop_assert!(
            is_transport_error(&error),
            "Error with message '{}' should be classified as transport, but was not",
            msg
        );
    }

    /// **Validates: Requirements 8.4**
    ///
    /// Random errors that don't match any pattern SHALL NOT be classified
    /// as transport or resolution errors.
    #[test]
    fn prop_neutral_messages_not_classified(
        msg in arb_neutral_message(),
    ) {
        let error = anyhow::anyhow!("{}", msg);

        prop_assert!(
            !is_transport_error(&error),
            "Neutral error '{}' should NOT be classified as transport",
            msg
        );
        prop_assert!(
            !is_resolution_error(&error),
            "Neutral error '{}' should NOT be classified as resolution",
            msg
        );
    }
}
