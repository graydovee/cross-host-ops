//! Property-based tests for shell resolution precedence.
//!
//! Feature: shell-wrapped-exec, Property 2: Shell resolution precedence
//!
//! Verifies that `resolve_shell` follows the four-layer priority:
//! CLI --no-shell / --shell=false > CLI --shell <name> > per-server shell > defaults.shell

use proptest::prelude::*;

use rhop::connection::resolve_shell;

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating a non-empty alphanumeric shell name (1–20 chars).
fn arb_shell_name() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_\\-]{0,19}"
}

/// Strategy for generating a defaults_shell value: either empty or a valid name.
fn arb_defaults_shell() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        arb_shell_name(),
    ]
}

/// Strategy for generating an optional per-server shell:
/// None (field absent), Some("") (explicitly disabled), or Some(name).
fn arb_server_shell() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        Just(None),
        Just(Some(String::new())),
        arb_shell_name().prop_map(Some),
    ]
}

/// Strategy for generating an optional CLI shell value:
/// None (no flag), Some("false") (disable), or Some(name).
fn arb_cli_shell() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        Just(None),
        Just(Some("false".to_string())),
        arb_shell_name().prop_map(Some),
    ]
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

// Feature: shell-wrapped-exec, Property 2: Shell resolution precedence
proptest! {
    #![proptest_config(ProptestConfig { cases: 300, .. ProptestConfig::default() })]

    /// **Validates: Requirements 2.3, 2.4, 2.5, 3.1, 3.2**
    ///
    /// For any combination of cli_shell (Option<String>), no_shell (bool),
    /// server_shell (Option<String>), and defaults_shell (String),
    /// resolve_shell SHALL return:
    /// - None when no_shell is true (regardless of all other inputs)
    /// - None when cli_shell is Some("false")
    /// - Some(name) when cli_shell is Some(name) and name != "false"
    /// - None when server_shell is Some("")
    /// - Some(name) when server_shell is Some(non-empty name)
    /// - Some(defaults_shell) when server_shell is None and defaults_shell is non-empty
    /// - None when server_shell is None and defaults_shell is empty
    #[test]
    fn prop_shell_resolution_precedence(
        cli_shell in arb_cli_shell(),
        no_shell in any::<bool>(),
        server_shell in arb_server_shell(),
        defaults_shell in arb_defaults_shell(),
    ) {
        let result = resolve_shell(
            cli_shell.as_deref(),
            no_shell,
            server_shell.as_deref(),
            &defaults_shell,
        );

        // Verify the resolution follows the documented priority order.
        let expected = if no_shell {
            // Priority 1: --no-shell always disables wrapping
            None
        } else if let Some(ref name) = cli_shell {
            if name == "false" {
                // Priority 1: --shell=false disables wrapping
                None
            } else {
                // Priority 2: CLI --shell <name> overrides everything
                Some(name.clone())
            }
        } else if let Some(ref shell) = server_shell {
            if shell.is_empty() {
                // Priority 3: per-server explicitly disabled (empty string)
                None
            } else {
                // Priority 3: per-server shell name
                Some(shell.clone())
            }
        } else {
            // Priority 4: defaults.shell
            if defaults_shell.is_empty() {
                None
            } else {
                Some(defaults_shell.clone())
            }
        };

        prop_assert_eq!(
            result, expected,
            "resolve_shell({:?}, {}, {:?}, {:?}) returned unexpected value",
            cli_shell, no_shell, server_shell, defaults_shell,
        );
    }
}
