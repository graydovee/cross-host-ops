//! Property test: Shell wrapping round-trip
//!
//! Feature: shell-wrapped-exec
//! Property 5: Shell wrapping round-trip
//!
//! For any valid inner command string and any shell name, wrapping with
//! `wrap_in_shell` and then extracting the payload by reversing the `'\''`
//! escape (simulating shell single-quote interpretation) recovers the
//! original string.
//!
//! **Validates: Requirements 4.9**

use proptest::prelude::*;

use xho::daemon::connection::shared::wrap_in_shell;

/// Strategy to generate a random command string of 0–200 chars including
/// single quotes, backslashes, spaces, and other special characters.
fn arb_command() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            // ASCII printable range
            (0x20u8..=0x7Eu8).prop_map(|b| b as char),
            // Specific special chars that stress quoting
            Just('\''),
            Just('\\'),
            Just('"'),
            Just('\n'),
            Just('\t'),
            Just(' '),
            Just('$'),
            Just('`'),
            Just('!'),
            Just('|'),
            Just('&'),
            Just(';'),
        ],
        0..=200,
    )
    .prop_map(|chars| chars.into_iter().collect::<String>())
}

/// Strategy to select a shell name from the known set.
fn arb_shell() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        Just("bash"),
        Just("zsh"),
        Just("sh"),
        Just("fish"),
        Just("ksh"),
    ]
}

/// Simulate shell single-quote unwrapping on the wrapped output.
///
/// The wrapped output has the form: `<shell> <flags> '<escaped_payload>'`
/// We extract the payload between the first `'` after the flags and the
/// last `'`, then reverse the `'\''` escape (replace `'\''` with `'`).
fn simulate_shell_unwrap(wrapped: &str) -> String {
    // Find the first single quote — marks the start of the payload
    let first_quote = wrapped.find('\'').expect("wrapped output must contain a quote");
    // Find the last single quote — marks the end of the payload
    let last_quote = wrapped.rfind('\'').expect("wrapped output must contain a quote");

    // Extract the content between the outer quotes
    let payload = &wrapped[first_quote + 1..last_quote];

    // Reverse the '\'' escape: in shell, '\'' means end-quote, literal-quote, start-quote.
    // The wrapped payload uses '\'' to represent each original single quote.
    payload.replace("'\\''", "'")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.9**
    ///
    /// For any command string and any shell from [bash, zsh, sh, fish, ksh],
    /// wrapping with wrap_in_shell and then simulating shell single-quote
    /// unwrapping recovers the original input.
    #[test]
    fn prop_shell_wrap_roundtrip(
        input in arb_command(),
        shell in arb_shell(),
    ) {
        let wrapped = wrap_in_shell(&input, shell);
        let recovered = simulate_shell_unwrap(&wrapped);

        prop_assert_eq!(
            &recovered,
            &input,
            "Round-trip failed: input={:?}, wrapped={:?}, recovered={:?}",
            input,
            wrapped,
            recovered,
        );
    }
}
