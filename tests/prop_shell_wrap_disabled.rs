//! Property-based test: Disabled wrapping identity.
//!
//! When shell wrapping is disabled (empty shell string), `build_final_command`
//! must produce output identical to `build_remote_command`.

use proptest::prelude::*;

use rhop::connection::{build_final_command, build_remote_command};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Strategy for generating a single argv element: 0–100 chars of printable
/// ASCII plus common special characters (quotes, backslashes, spaces, etc.).
fn arb_argv_element() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::char::range('\x20', '\x7e'), 0..=100)
        .prop_map(|chars| chars.into_iter().collect::<String>())
}

/// Strategy for generating argv vectors of 1–10 elements.
fn arb_argv() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(arb_argv_element(), 1..=10)
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

// Feature: shell-wrapped-exec, Property 3: Disabled wrapping identity
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.7, 4.7, 6.1, 6.2**
    ///
    /// For any non-empty argv vector, `build_final_command(argv, "")` SHALL
    /// produce output identical to `build_remote_command(argv)`.
    #[test]
    fn prop_disabled_wrapping_identity(argv in arb_argv()) {
        let with_empty_shell = build_final_command(&argv, "");
        let direct = build_remote_command(&argv);

        prop_assert_eq!(
            with_empty_shell,
            direct,
            "build_final_command(argv, \"\") must equal build_remote_command(argv) \
             for argv = {:?}",
            argv
        );
    }
}
