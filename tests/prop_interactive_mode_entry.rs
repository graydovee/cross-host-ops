// Feature: exec-stdin-tty-refactor, Property 7: Interactive mode entry judgment
// Validates: Requirements 4.3, 7.3, 7.4

use proptest::prelude::*;

use xho::types::should_use_interactive_mode;

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// Property 7: should_use_interactive_mode returns true iff all four
    /// boolean inputs are true (4-boolean conjunction).
    ///
    /// **Validates: Requirements 4.3, 7.3, 7.4**
    #[test]
    fn prop_interactive_mode_entry_is_four_way_conjunction(
        resolved_tty in any::<bool>(),
        resolved_stdin in any::<bool>(),
        stdin_is_tty in any::<bool>(),
        stdout_is_tty in any::<bool>(),
    ) {
        let result = should_use_interactive_mode(
            resolved_tty,
            resolved_stdin,
            stdin_is_tty,
            stdout_is_tty,
        );

        let expected = resolved_tty && resolved_stdin && stdin_is_tty && stdout_is_tty;

        prop_assert_eq!(
            result,
            expected,
            "should_use_interactive_mode({}, {}, {}, {}) returned {} but expected {}",
            resolved_tty, resolved_stdin, stdin_is_tty, stdout_is_tty,
            result, expected
        );
    }
}
