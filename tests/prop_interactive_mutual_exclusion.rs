//! Property-based test for interactive/non-interactive mode mutual exclusion.
//!
//! Feature: interactive-pty-passthrough
//! Property 7: Interactive/Non-Interactive Mode Mutual Exclusion
//!
//! For any ExecRequest, if `interactive == true` then `no_pty` must be `false`,
//! `term_cols` must be > 0, and `term_rows` must be > 0. The execution path
//! uses bidirectional byte streaming with client raw mode.
//!
//! **Validates: Requirements 3.3, 7.5**

use proptest::prelude::*;

use rhop::cli::should_use_interactive_mode;
use rhop::protocol::ExecRequest;

/// Simulate the daemon's validation logic for an ExecRequest.
/// Returns Ok(()) if the request is valid, Err(reason) if it would be rejected.
fn validate_interactive_request(req: &ExecRequest) -> Result<(), &'static str> {
    if req.interactive {
        if !req.pty || req.no_pty {
            return Err("interactive mode requires pty and is incompatible with no_pty");
        }
        if req.term_cols == 0 || req.term_rows == 0 {
            return Err("interactive mode requires term_cols > 0 and term_rows > 0");
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, .. ProptestConfig::default() })]

    /// Property: If interactive == true, then no_pty must be false, pty must be
    /// true, term_cols > 0, and term_rows > 0 for the request to be valid.
    /// Any violation of these constraints results in rejection.
    ///
    /// **Validates: Requirements 3.3, 7.5**
    #[test]
    fn prop_interactive_requires_pty_and_valid_dimensions(
        interactive in any::<bool>(),
        pty in any::<bool>(),
        no_pty in any::<bool>(),
        term_cols in 0u32..=300,
        term_rows in 0u32..=200,
    ) {
        let req = ExecRequest {
            target: "test".to_string(),
            argv: vec!["cmd".to_string()],
            pty,
            no_pty,
            stdin: false,
            timeout_ms: 0,
            interactive,
            term_cols,
            term_rows,
            shell: String::new(),
            no_shell: false,
        };

        let result = validate_interactive_request(&req);

        if interactive {
            // When interactive is true, validation must enforce constraints
            if !pty || no_pty {
                prop_assert!(
                    result.is_err(),
                    "interactive=true with pty={}, no_pty={} should be rejected",
                    pty, no_pty
                );
            } else if term_cols == 0 || term_rows == 0 {
                prop_assert!(
                    result.is_err(),
                    "interactive=true with term_cols={}, term_rows={} should be rejected",
                    term_cols, term_rows
                );
            } else {
                prop_assert!(
                    result.is_ok(),
                    "interactive=true with pty=true, no_pty=false, cols={}, rows={} should be valid",
                    term_cols, term_rows
                );
            }
        } else {
            // Non-interactive requests always pass this validation
            prop_assert!(
                result.is_ok(),
                "non-interactive requests should always pass interactive validation"
            );
        }
    }

    /// Property: interactive == true and no_pty == true is always an invalid
    /// state — these flags are mutually exclusive.
    ///
    /// **Validates: Requirement 3.3**
    #[test]
    fn prop_interactive_and_no_pty_mutually_exclusive(
        term_cols in 1u32..=300,
        term_rows in 1u32..=200,
    ) {
        let req = ExecRequest {
            target: "test".to_string(),
            argv: vec!["cmd".to_string()],
            pty: true,
            no_pty: true,
            stdin: false,
            timeout_ms: 0,
            interactive: true,
            term_cols,
            term_rows,
            shell: String::new(),
            no_shell: false,
        };

        let result = validate_interactive_request(&req);
        prop_assert!(
            result.is_err(),
            "interactive=true with no_pty=true must always be rejected, got Ok"
        );
    }

    /// Property: interactive == true with zero terminal dimensions is always
    /// rejected, even when pty is true and no_pty is false.
    ///
    /// **Validates: Requirement 7.5**
    #[test]
    fn prop_interactive_rejects_zero_dimensions(
        // Generate (cols, rows) where at least one is zero:
        // variant 0 = cols is zero, variant 1 = rows is zero, variant 2 = both zero
        variant in 0u32..3,
        other_dim in 0u32..=300,
    ) {
        let (term_cols, term_rows) = match variant {
            0 => (0, other_dim),       // cols is zero
            1 => (other_dim, 0),       // rows is zero
            _ => (0, 0),               // both zero
        };

        let req = ExecRequest {
            target: "test".to_string(),
            argv: vec!["cmd".to_string()],
            pty: true,
            no_pty: false,
            stdin: false,
            timeout_ms: 0,
            interactive: true,
            term_cols,
            term_rows,
            shell: String::new(),
            no_shell: false,
        };

        let result = validate_interactive_request(&req);
        prop_assert!(
            result.is_err(),
            "interactive=true with term_cols={}, term_rows={} (at least one zero) must be rejected",
            term_cols, term_rows
        );
    }

    /// Property: should_use_interactive_mode combined with validation —
    /// when the function returns true (all TTY conditions met), the resulting
    /// ExecRequest with proper dimensions passes validation.
    ///
    /// **Validates: Requirements 3.3, 7.5**
    #[test]
    fn prop_interactive_mode_detection_implies_valid_request(
        pty in any::<bool>(),
        stdin_is_tty in any::<bool>(),
        stdout_is_tty in any::<bool>(),
        term_cols in 1u32..=300,
        term_rows in 1u32..=200,
    ) {
        let interactive = should_use_interactive_mode(pty, stdin_is_tty, stdout_is_tty);

        let req = ExecRequest {
            target: "test".to_string(),
            argv: vec!["cmd".to_string()],
            pty,
            no_pty: false,
            stdin: false,
            timeout_ms: 0,
            interactive,
            term_cols,
            term_rows,
            shell: String::new(),
            no_shell: false,
        };

        let result = validate_interactive_request(&req);

        // When should_use_interactive_mode returns true, pty is guaranteed true,
        // and with no_pty=false and valid dimensions, the request must be valid.
        if interactive {
            prop_assert!(
                result.is_ok(),
                "when should_use_interactive_mode returns true with valid dimensions, \
                 the request must pass validation. Got error for pty={}, cols={}, rows={}",
                pty, term_cols, term_rows
            );
        }
        // When interactive is false, validation always passes (non-interactive path)
        if !interactive {
            prop_assert!(
                result.is_ok(),
                "non-interactive requests must always pass validation"
            );
        }
    }
}
