//! Property test: Backward Compatibility — Jumpserver Shell Navigation Preserved
//!
//! Feature: interactive-pty-passthrough
//! Property 6: Backward Compatibility — Jumpserver Shell Navigation Preserved
//!
//! NOTE: This test originally tested the `Gateway` trait architecture which
//! was removed as part of the config-and-legacy-cleanup spec. The jumpserver
//! shell navigation is now an internal implementation detail of
//! `daemon::gateway::jumpserver::JumpserverGateway` and
//! `daemon::jumpserver_engine`.
//!
//! The shell_quote and build_remote_command functions are still available in
//! `daemon::shell` and are tested in prop_shell_*.rs tests.

use proptest::prelude::*;

use xho::daemon::shell::{build_remote_command, shell_quote};

// ---------------------------------------------------------------------------
// Strategy: generate arbitrary command argument vectors
// ---------------------------------------------------------------------------

fn arb_argv() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-zA-Z0-9_./ -]{1,50}", 1..=5)
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.5**
    ///
    /// For any arbitrary command arguments, build_remote_command and shell_quote
    /// produce valid command strings (proving the non-sentinel exec path works).
    #[test]
    fn prop_remote_command_building_works(argv in arb_argv()) {
        // build_remote_command is used by the PTY exec path (request_pty + exec)
        let cmd = build_remote_command(&argv);
        prop_assert!(!cmd.is_empty(), "remote command should not be empty");

        // Each argument should be shell-quoted in the output
        for arg in &argv {
            let quoted = shell_quote(arg);
            prop_assert!(
                cmd.contains(&quoted),
                "command '{}' should contain quoted arg '{}'",
                cmd,
                quoted
            );
        }
    }
}
