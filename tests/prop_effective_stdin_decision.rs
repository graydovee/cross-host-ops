//! Property-based test for effective_stdin_decision priority chain.
//!
//! Feature: exec-stdin-tty-refactor, Property 6: effective_stdin_decision priority chain
//!
//! For any combination of ExecStdinFlags (force_stdin, force_no_stdin) and
//! SshConfig.stdin boolean:
//! - If force_no_stdin → result must be false
//! - Else if force_stdin → result must be true
//! - Otherwise → result must equal config.stdin
//!
//! Validates: Requirements 5.3, 6.1, 7.2

use proptest::prelude::*;

use xho::config::SshConfig;
use xho::types::{ExecStdinFlags, effective_stdin_decision};

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// Property 6: effective_stdin_decision follows the priority chain:
    /// force_no_stdin → false, force_stdin → true, else config.stdin
    ///
    /// **Validates: Requirements 5.3, 6.1, 7.2**
    #[test]
    fn prop_stdin_decision_priority_chain(
        force_stdin in any::<bool>(),
        force_no_stdin in any::<bool>(),
        config_stdin in any::<bool>(),
    ) {
        let flags = ExecStdinFlags {
            force_stdin,
            force_no_stdin,
        };
        let ssh_config = SshConfig {
            stdin: config_stdin,
            ..Default::default()
        };

        let result = effective_stdin_decision(&flags, &ssh_config);

        // Priority chain verification
        if force_no_stdin {
            prop_assert_eq!(
                result, false,
                "force_no_stdin=true must always yield false, got true \
                 (force_stdin={}, config_stdin={})",
                force_stdin, config_stdin
            );
        } else if force_stdin {
            prop_assert_eq!(
                result, true,
                "force_no_stdin=false, force_stdin=true must yield true, got false \
                 (config_stdin={})",
                config_stdin
            );
        } else {
            prop_assert_eq!(
                result, config_stdin,
                "no flags set: result must equal config.stdin={}, got {}",
                config_stdin, result
            );
        }
    }
}
