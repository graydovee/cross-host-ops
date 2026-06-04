//! Property-based test for effective_tty_decision priority chain.
//!
//! Feature: exec-stdin-tty-refactor, Property 5: effective_tty_decision priority chain
//!
//! For any combination of (ExecTtyFlags, SshConfig.tty, auto_tty_detect, stdout_is_tty):
//! - If force_no_tty → result is false
//! - Else if force_tty → result is true
//! - Else if auto_tty_detect && !stdout_is_tty → result is false
//! - Otherwise → result equals ssh_config.tty
//!
//! Validates: Requirements 5.4, 6.2, 7.1

use proptest::prelude::*;

use xho::config::SshConfig;
use xho::types::{ExecTtyFlags, effective_tty_decision};

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// Property 5: effective_tty_decision priority chain.
    ///
    /// The decision function follows a strict priority order:
    /// 1. force_no_tty wins over everything → false
    /// 2. force_tty wins next → true
    /// 3. auto_tty_detect && !stdout_is_tty → false
    /// 4. fallback to config.tty
    ///
    /// **Validates: Requirements 5.4, 6.2, 7.1**
    #[test]
    fn prop_effective_tty_decision_priority_chain(
        force_tty in any::<bool>(),
        force_no_tty in any::<bool>(),
        config_tty in any::<bool>(),
        auto_tty_detect in any::<bool>(),
        stdout_is_tty in any::<bool>(),
    ) {
        let flags = ExecTtyFlags { force_tty, force_no_tty };
        let ssh_config = SshConfig {
            tty: config_tty,
            auto_tty_detect,
            ..Default::default()
        };

        let result = effective_tty_decision(&flags, &ssh_config, stdout_is_tty);

        // Verify the priority chain
        let expected = if force_no_tty {
            false
        } else if force_tty {
            true
        } else if auto_tty_detect && !stdout_is_tty {
            false
        } else {
            config_tty
        };

        prop_assert_eq!(
            result, expected,
            "effective_tty_decision mismatch: flags=({}, {}), config_tty={}, auto_tty_detect={}, stdout_is_tty={} → got {}, expected {}",
            force_tty, force_no_tty, config_tty, auto_tty_detect, stdout_is_tty, result, expected
        );
    }
}
