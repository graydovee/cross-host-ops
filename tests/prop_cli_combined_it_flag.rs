// Feature: exec-stdin-tty-refactor, Property 4: -it combined flag parsing
// Validates: Requirements 4.1, 4.2
//!
//! Property-based test verifying that the combined `-it` flag is parsed
//! equivalently to `-i -t` separately, producing both tty=true and stdin=true.

use clap::Parser;
use proptest::prelude::*;

use rhop::cli::{ArunCli, ArunCommand};

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating valid TARGET strings (simple alphanumeric to avoid
/// parsing issues with special characters).
fn arb_target() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9]{0,30}"
}

/// Strategy for generating a single command element: alphanumeric strings that
/// won't be confused with CLI flags.
fn arb_cmd_element() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,49}"
}

/// Strategy for generating command argument vectors (1–5 elements).
fn arb_cmd() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(arb_cmd_element(), 1..=5)
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.1, 4.2**
    ///
    /// For any valid exec command (target + command args), parsing with `-it`
    /// SHALL produce both tty=true AND stdin=true, identical to providing
    /// `-i -t` separately.
    #[test]
    fn prop_combined_it_flag_sets_both_tty_and_stdin(
        target in arb_target(),
        cmd in arb_cmd(),
    ) {
        // Build command line with combined `-it` flag
        let mut args_combined: Vec<String> = vec![
            "rhop".to_string(),
            "exec".to_string(),
            "-it".to_string(),
            target.clone(),
            "--".to_string(),
        ];
        args_combined.extend(cmd.clone());

        // Parse with `-it`
        let cli_combined = ArunCli::try_parse_from(&args_combined);
        prop_assert!(
            cli_combined.is_ok(),
            "ArunCli::try_parse_from failed for -it args {:?}: {:?}",
            args_combined,
            cli_combined.err()
        );
        let cli_combined = cli_combined.unwrap();

        // Verify -it produces both tty=true and stdin=true
        match &cli_combined.command {
            ArunCommand::Exec { tty, stdin, no_tty, no_stdin, .. } => {
                prop_assert!(
                    *tty,
                    "parsing with -it should set tty=true, got false"
                );
                prop_assert!(
                    *stdin,
                    "parsing with -it should set stdin=true, got false"
                );
                prop_assert!(
                    !*no_tty,
                    "parsing with -it should not set no_tty"
                );
                prop_assert!(
                    !*no_stdin,
                    "parsing with -it should not set no_stdin"
                );
            }
            other => {
                prop_assert!(false, "expected Exec variant, got {:?}", other);
            }
        }

        // Build command line with separate `-i -t` flags
        let mut args_separate: Vec<String> = vec![
            "rhop".to_string(),
            "exec".to_string(),
            "-i".to_string(),
            "-t".to_string(),
            target.clone(),
            "--".to_string(),
        ];
        args_separate.extend(cmd.clone());

        // Parse with `-i -t`
        let cli_separate = ArunCli::try_parse_from(&args_separate);
        prop_assert!(
            cli_separate.is_ok(),
            "ArunCli::try_parse_from failed for -i -t args {:?}: {:?}",
            args_separate,
            cli_separate.err()
        );
        let cli_separate = cli_separate.unwrap();

        // Verify -i -t also produces both tty=true and stdin=true
        match &cli_separate.command {
            ArunCommand::Exec { tty, stdin, no_tty, no_stdin, .. } => {
                prop_assert!(
                    *tty,
                    "parsing with -i -t should set tty=true, got false"
                );
                prop_assert!(
                    *stdin,
                    "parsing with -i -t should set stdin=true, got false"
                );
                prop_assert!(
                    !*no_tty,
                    "parsing with -i -t should not set no_tty"
                );
                prop_assert!(
                    !*no_stdin,
                    "parsing with -i -t should not set no_stdin"
                );
            }
            other => {
                prop_assert!(false, "expected Exec variant, got {:?}", other);
            }
        }
    }
}
