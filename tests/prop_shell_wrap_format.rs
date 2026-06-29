//! Property test: Shell wrapping format invariant
//!
//! Feature: shell-wrapped-exec
//! Property 1: Shell wrapping format invariant
//!
//! For any non-empty argv vector and any non-empty shell name,
//! `build_final_command(argv, shell)` produces a string that starts with
//! `<shell> -ic '` (for bash/zsh) or `<shell> -c '` (for others) and ends
//! with `'`.
//!
//! **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**

use proptest::prelude::*;

use xho::daemon::shell::build_final_command;

/// Strategy to generate a single argument string of 0–100 chars including
/// special characters (quotes, backslashes, spaces, newlines, etc.)
fn arb_arg() -> impl Strategy<Value = String> {
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
        ],
        0..=100,
    )
    .prop_map(|chars| chars.into_iter().collect::<String>())
}

/// Strategy to generate an argv vector of 1–10 arguments.
fn arb_argv() -> impl Strategy<Value = Vec<String>> {
    proptest::collection::vec(arb_arg(), 1..=10)
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

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
    ///
    /// For any non-empty argv and any shell from [bash, zsh, sh, fish, ksh],
    /// the output of build_final_command starts with the correct shell prefix
    /// and ends with a closing single quote.
    #[test]
    fn prop_shell_wrap_format_invariant(
        argv in arb_argv(),
        shell in arb_shell(),
    ) {
        let result = build_final_command(&argv, shell);

        // Determine expected prefix based on shell name
        let expected_prefix = match shell {
            "bash" | "zsh" => format!("{} -ic '", shell),
            _ => format!("{} -c '", shell),
        };

        prop_assert!(
            result.starts_with(&expected_prefix),
            "Expected output to start with {:?}, but got {:?}",
            expected_prefix,
            &result[..result.len().min(50)]
        );

        prop_assert!(
            result.ends_with('\''),
            "Expected output to end with single quote, but got {:?}",
            &result[result.len().saturating_sub(10)..]
        );
    }
}
