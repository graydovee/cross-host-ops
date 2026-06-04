//! Property test for single-quote escaping correctness.
//!
//! Feature: shell-wrapped-exec
//! Property 4: Single-quote escaping correctness
//!
//! For any string containing single quotes and any shell name,
//! `wrap_in_shell(s, shell)` shall not contain any unescaped single quotes
//! within the wrapped payload. Every `'` in the original is replaced with
//! `'\''`, so the payload between the outer quotes contains no bare single
//! quotes.
//!
//! **Validates: Requirements 4.6**

use proptest::prelude::*;

use xho::daemon::connection::shared::wrap_in_shell;

/// Strategy that generates strings guaranteed to contain at least one single quote.
fn string_with_quotes() -> impl Strategy<Value = String> {
    // Generate 1-3 segments separated by single quotes, ensuring at least one quote.
    let segment = "[^\\']{0,30}";
    prop::collection::vec(segment, 2..=5).prop_map(|parts| parts.join("'"))
}

/// Strategy that selects a shell name from the supported set.
fn shell_name() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        Just("bash"),
        Just("zsh"),
        Just("sh"),
        Just("fish"),
        Just("ksh"),
    ]
}

// ---------------------------------------------------------------------------
// Property 4: Single-quote escaping correctness
// ---------------------------------------------------------------------------

// **Validates: Requirements 4.6**
//
// For any input string containing single quotes and any shell name, the
// wrapped output's payload (the content between the outermost single quotes
// of the shell invocation) must not contain any bare/unescaped single quotes.
// Every single quote in the payload must be part of the `'\''` escape sequence.
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    #[test]
    fn prop_no_unescaped_single_quotes_in_payload(
        input in string_with_quotes(),
        shell in shell_name(),
    ) {
        // Confirm input actually contains a single quote
        prop_assume!(input.contains('\''));

        let wrapped = wrap_in_shell(&input, shell);

        // The wrapped format is: <shell> <flags> '<payload>'
        // Find the first opening quote after the flags
        let prefix_end = wrapped.find(" '").expect("wrapped should contain \" '\"");
        let payload_start = prefix_end + 2; // skip the space and opening quote
        let payload_end = wrapped.len() - 1; // the final closing quote

        // Extract the payload between the outer quotes
        let payload = &wrapped[payload_start..payload_end];

        // Verify: within the payload, every single quote must be part of the
        // '\'' escape pattern. After we remove all occurrences of '\'' from
        // the payload, there should be no remaining single quotes.
        let without_escapes = payload.replace("'\\''", "");
        prop_assert!(
            !without_escapes.contains('\''),
            "Found unescaped single quote in payload.\n\
             Input: {:?}\n\
             Shell: {}\n\
             Wrapped: {:?}\n\
             Payload: {:?}\n\
             After removing escape sequences: {:?}",
            input, shell, wrapped, payload, without_escapes
        );
    }
}

// ---------------------------------------------------------------------------
// Property 4 (alternative verification): round-trip unescape recovers original
// ---------------------------------------------------------------------------

// **Validates: Requirements 4.6**
//
// For any input string containing single quotes and any shell name, extracting
// the payload from the wrapped output and unescaping `'\''` back to `'` must
// recover the original input string. This is a stronger verification that the
// escaping is both correct and complete.
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    #[test]
    fn prop_unescape_payload_recovers_original(
        input in string_with_quotes(),
        shell in shell_name(),
    ) {
        prop_assume!(input.contains('\''));

        let wrapped = wrap_in_shell(&input, shell);

        // Extract payload between outer quotes
        let prefix_end = wrapped.find(" '").expect("wrapped should contain \" '\"");
        let payload_start = prefix_end + 2;
        let payload_end = wrapped.len() - 1;
        let payload = &wrapped[payload_start..payload_end];

        // Unescape: replace '\'' with '
        let unescaped = payload.replace("'\\''", "'");

        prop_assert_eq!(
            &unescaped, &input,
            "Round-trip unescape should recover original.\n\
             Input: {:?}\n\
             Shell: {}\n\
             Wrapped: {:?}\n\
             Payload: {:?}\n\
             Unescaped: {:?}",
            input, shell, wrapped, payload, unescaped
        );
    }
}
