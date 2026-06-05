//! Property-based test: Shell flag selection correctness.
//!
//! For any shell name, `shell_flags` returns `-ic` for "bash" and "zsh",
//! and `-c` for all other values. Since `shell_flags` is private, we test
//! indirectly through `wrap_in_shell`.

use proptest::prelude::*;

use xho::daemon::connection::shared::wrap_in_shell;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Strategy for generating random shell name strings: 1–20 alphanumeric chars.
fn arb_shell_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9]{1,20}").unwrap()
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

// Feature: shell-wrapped-exec, Property 6: Shell flag selection correctness
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
    ///
    /// For any shell name string, `wrap_in_shell` uses `-ic` only for "bash"
    /// and "zsh", and `-c` for all other shell names.
    #[test]
    fn prop_shell_flag_selection(shell_name in arb_shell_name()) {
        let output = wrap_in_shell("test", &shell_name);

        if shell_name == "bash" || shell_name == "zsh" {
            // bash and zsh must use -ic flag
            prop_assert!(
                output.contains(&format!("{} -ic '", shell_name)),
                "Expected '{} -ic ' in output for shell={:?}, got: {:?}",
                shell_name, shell_name, output
            );
        } else {
            // All other shells must use -c flag (not -ic)
            prop_assert!(
                output.contains(&format!("{} -c '", shell_name)),
                "Expected '{} -c ' in output for shell={:?}, got: {:?}",
                shell_name, shell_name, output
            );
            prop_assert!(
                !output.contains(&format!("{} -ic '", shell_name)),
                "Shell {:?} must NOT use -ic flag, got: {:?}",
                shell_name, output
            );
        }
    }
}
