//! Property test: Per-server shell inheritance
//!
//! Feature: shell-wrapped-exec
//! Property 7: Per-server shell inheritance
//!
//! For any server entry without a `shell` field (server_shell = None) and any
//! `defaults.shell` value, `resolve_shell` returns `Some(defaults_shell)` when
//! defaults_shell is non-empty, and `None` when defaults_shell is empty.
//!
//! **Validates: Requirements 1.6, 3.2, 3.3**

use proptest::prelude::*;

use xho::daemon::connection::shared::resolve_shell;

/// Strategy to generate a defaults_shell string of 0–20 alphanumeric chars
/// (may be empty).
fn arb_defaults_shell() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            (b'a'..=b'z').prop_map(|b| b as char),
            (b'A'..=b'Z').prop_map(|b| b as char),
            (b'0'..=b'9').prop_map(|b| b as char),
        ],
        0..=20,
    )
    .prop_map(|chars| chars.into_iter().collect::<String>())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.6, 3.2, 3.3**
    ///
    /// When server_shell is None (field absent from server entry) and no CLI
    /// override is provided, resolve_shell inherits the defaults_shell value:
    /// - non-empty defaults_shell → Some(defaults_shell)
    /// - empty defaults_shell → None
    #[test]
    fn prop_per_server_shell_inheritance(
        defaults_shell in arb_defaults_shell(),
    ) {
        // Simulate: no CLI shell, no --no-shell, server entry has no shell field
        let result = resolve_shell(None, false, None, &defaults_shell);

        if defaults_shell.is_empty() {
            prop_assert!(
                result.is_none(),
                "Expected None when defaults_shell is empty, got {:?}",
                result
            );
        } else {
            prop_assert_eq!(
                result.as_deref(),
                Some(defaults_shell.as_str()),
                "Expected Some({:?}) when defaults_shell is non-empty, got {:?}",
                defaults_shell,
                result
            );
        }
    }
}
