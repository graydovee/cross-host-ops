//! Property-based test for interactive mode detection.
//!
//! Feature: interactive-pty-passthrough
//! Property 1: Interactive Mode Detection is a Pure Function
//!
//! For any combination of (pty, stdin_is_tty, stdout_is_tty) boolean values,
//! `should_use_interactive_mode` returns `true` if and only if all three are
//! `true`. Additionally, if `no_pty` (i.e. pty == false) is the case, the
//! result is always `false` regardless of other inputs.
//!
//! **Validates: Requirements 3.1, 3.2, 3.3**

use proptest::prelude::*;

use rhop::cli::should_use_interactive_mode;

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// Property: should_use_interactive_mode returns true if and only if
    /// all three inputs (pty, stdin_is_tty, stdout_is_tty) are true.
    ///
    /// **Validates: Requirements 3.1, 3.2, 3.3**
    #[test]
    fn prop_interactive_mode_iff_all_true(
        pty in any::<bool>(),
        stdin_is_tty in any::<bool>(),
        stdout_is_tty in any::<bool>(),
    ) {
        let result = should_use_interactive_mode(pty, stdin_is_tty, stdout_is_tty);
        let expected = pty && stdin_is_tty && stdout_is_tty;
        prop_assert_eq!(
            result, expected,
            "should_use_interactive_mode({}, {}, {}) = {}, expected {}",
            pty, stdin_is_tty, stdout_is_tty, result, expected
        );
    }

    /// Property: when pty is false (equivalent to --no-pty), the result is
    /// always false regardless of TTY status.
    ///
    /// **Validates: Requirement 3.3**
    #[test]
    fn prop_no_pty_always_false(
        stdin_is_tty in any::<bool>(),
        stdout_is_tty in any::<bool>(),
    ) {
        let result = should_use_interactive_mode(false, stdin_is_tty, stdout_is_tty);
        prop_assert_eq!(
            result, false,
            "should_use_interactive_mode(false, {}, {}) should always be false, got {}",
            stdin_is_tty, stdout_is_tty, result
        );
    }

    /// Property: when stdin is not a TTY, the result is always false.
    ///
    /// **Validates: Requirement 3.2**
    #[test]
    fn prop_non_tty_stdin_always_false(
        pty in any::<bool>(),
        stdout_is_tty in any::<bool>(),
    ) {
        let result = should_use_interactive_mode(pty, false, stdout_is_tty);
        prop_assert_eq!(
            result, false,
            "should_use_interactive_mode({}, false, {}) should always be false, got {}",
            pty, stdout_is_tty, result
        );
    }

    /// Property: when stdout is not a TTY, the result is always false.
    ///
    /// **Validates: Requirement 3.2**
    #[test]
    fn prop_non_tty_stdout_always_false(
        pty in any::<bool>(),
        stdin_is_tty in any::<bool>(),
    ) {
        let result = should_use_interactive_mode(pty, stdin_is_tty, false);
        prop_assert_eq!(
            result, false,
            "should_use_interactive_mode({}, {}, false) should always be false, got {}",
            pty, stdin_is_tty, result
        );
    }
}
