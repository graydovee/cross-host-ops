//! Property test: argv[0] word-boundary safety in shell-wrapped commands
//!
//! Feature: shell-wrapped-exec
//! Property: argv[0] metacharacter safety
//!
//! For any argv and any shell name, `build_final_command` must keep argv[0]
//! a single shell word: command names made only of safe word characters stay
//! unquoted (so shell aliases can expand), while any argv[0] containing
//! shell metacharacters (spaces, quotes, `;`, `$`, ...) must be single-quoted
//! so the wrapping shell cannot split or re-interpret it as code.

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
            Just(';'),
            Just('|'),
            Just('&'),
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

/// Mirror of the daemon's safe-command-word rule: a word that a shell may
/// safely leave unquoted for alias expansion.
fn is_safe_command_word(arg: &str) -> bool {
    !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | '+'))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// For any argv and shell: an unsafe argv[0] must open with an escaped
    /// single quote (`'\''` — the wrapped form of shell_quote's opening
    /// quote) so it stays one literal word, while a safe argv[0] must appear
    /// verbatim so aliases can expand.
    #[test]
    fn prop_argv0_word_boundary_safety(
        argv in arb_argv(),
        shell in arb_shell(),
    ) {
        let result = build_final_command(&argv, shell);
        let expected_prefix = match shell {
            "bash" | "zsh" => format!("{} -ic '", shell),
            _ => format!("{} -c '", shell),
        };
        let after_prefix = result
            .strip_prefix(expected_prefix.as_str())
            .expect("wrapped output must start with the shell prefix");

        if is_safe_command_word(&argv[0]) {
            prop_assert!(
                after_prefix.starts_with(argv[0].as_str()),
                "safe argv[0] {:?} must appear verbatim, got {:?}",
                argv[0],
                &result[..result.len().min(60)]
            );
        } else {
            prop_assert!(
                after_prefix.starts_with("'\\''"),
                "unsafe argv[0] {:?} must be single-quoted, got {:?}",
                argv[0],
                &result[..result.len().min(60)]
            );
        }
    }
}
