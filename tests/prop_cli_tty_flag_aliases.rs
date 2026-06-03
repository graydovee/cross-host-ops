//! Property-based tests for CLI short flag equivalence.
//!
//! Verifies that `--tty` and `-t` produce identical results, and
//! `--stdin` and `-i` produce identical results for any valid exec command.

// Feature: exec-stdin-tty-refactor, Property 3: CLI short flag equivalence
// Validates: Requirements 2.1, 2.2, 3.1

use clap::Parser;
use proptest::prelude::*;

use rhop::cli::{ArunCli, ArunCommand};

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating valid TARGET strings (alphanumeric with dots/hyphens/underscores).
fn arb_target() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9._\\-]{0,30}"
}

/// Strategy for generating a simple command element (alphanumeric, no leading hyphen).
fn arb_cmd_element() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,20}"
}

/// Strategy for generating command vectors (1-5 elements).
fn arb_cmd() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(arb_cmd_element(), 1..=5)
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 2.1, 2.2, 3.1**
    ///
    /// For any valid exec command (target + cmd), parsing with `--tty` SHALL
    /// produce the same `tty=true` result as parsing with `-t`.
    #[test]
    fn prop_tty_long_flag_equals_short_flag(
        target in arb_target(),
        cmd in arb_cmd(),
    ) {
        // Build args with --tty (long form)
        let mut long_args: Vec<String> = vec![
            "rhop".to_string(),
            "exec".to_string(),
            "--tty".to_string(),
            target.clone(),
            "--".to_string(),
        ];
        long_args.extend(cmd.clone());

        // Build args with -t (short form)
        let mut short_args: Vec<String> = vec![
            "rhop".to_string(),
            "exec".to_string(),
            "-t".to_string(),
            target.clone(),
            "--".to_string(),
        ];
        short_args.extend(cmd.clone());

        // Parse both
        let long_cli = ArunCli::try_parse_from(&long_args);
        prop_assert!(
            long_cli.is_ok(),
            "ArunCli::try_parse_from failed for --tty args {:?}: {:?}",
            long_args,
            long_cli.err()
        );

        let short_cli = ArunCli::try_parse_from(&short_args);
        prop_assert!(
            short_cli.is_ok(),
            "ArunCli::try_parse_from failed for -t args {:?}: {:?}",
            short_args,
            short_cli.err()
        );

        let long_cli = long_cli.unwrap();
        let short_cli = short_cli.unwrap();

        // Extract Exec variants and compare tty field
        match (&long_cli.command, &short_cli.command) {
            (
                ArunCommand::Exec { tty: long_tty, no_tty: long_no_tty, .. },
                ArunCommand::Exec { tty: short_tty, no_tty: short_no_tty, .. },
            ) => {
                prop_assert_eq!(
                    *long_tty, true,
                    "--tty should set tty=true"
                );
                prop_assert_eq!(
                    *short_tty, true,
                    "-t should set tty=true"
                );
                prop_assert_eq!(
                    long_tty, short_tty,
                    "--tty and -t should produce identical tty value"
                );
                prop_assert_eq!(
                    long_no_tty, short_no_tty,
                    "--tty and -t should produce identical no_tty value"
                );
            }
            _ => {
                prop_assert!(false, "expected Exec variant from both parses");
            }
        }
    }

    /// **Validates: Requirements 2.1, 2.2, 3.1**
    ///
    /// For any valid exec command (target + cmd), parsing with `--stdin` SHALL
    /// produce the same `stdin=true` result as parsing with `-i`.
    #[test]
    fn prop_stdin_long_flag_equals_short_flag(
        target in arb_target(),
        cmd in arb_cmd(),
    ) {
        // Build args with --stdin (long form)
        let mut long_args: Vec<String> = vec![
            "rhop".to_string(),
            "exec".to_string(),
            "--stdin".to_string(),
            target.clone(),
            "--".to_string(),
        ];
        long_args.extend(cmd.clone());

        // Build args with -i (short form)
        let mut short_args: Vec<String> = vec![
            "rhop".to_string(),
            "exec".to_string(),
            "-i".to_string(),
            target.clone(),
            "--".to_string(),
        ];
        short_args.extend(cmd.clone());

        // Parse both
        let long_cli = ArunCli::try_parse_from(&long_args);
        prop_assert!(
            long_cli.is_ok(),
            "ArunCli::try_parse_from failed for --stdin args {:?}: {:?}",
            long_args,
            long_cli.err()
        );

        let short_cli = ArunCli::try_parse_from(&short_args);
        prop_assert!(
            short_cli.is_ok(),
            "ArunCli::try_parse_from failed for -i args {:?}: {:?}",
            short_args,
            short_cli.err()
        );

        let long_cli = long_cli.unwrap();
        let short_cli = short_cli.unwrap();

        // Extract Exec variants and compare stdin field
        match (&long_cli.command, &short_cli.command) {
            (
                ArunCommand::Exec { stdin: long_stdin, no_stdin: long_no_stdin, .. },
                ArunCommand::Exec { stdin: short_stdin, no_stdin: short_no_stdin, .. },
            ) => {
                prop_assert_eq!(
                    *long_stdin, true,
                    "--stdin should set stdin=true"
                );
                prop_assert_eq!(
                    *short_stdin, true,
                    "-i should set stdin=true"
                );
                prop_assert_eq!(
                    long_stdin, short_stdin,
                    "--stdin and -i should produce identical stdin value"
                );
                prop_assert_eq!(
                    long_no_stdin, short_no_stdin,
                    "--stdin and -i should produce identical no_stdin value"
                );
            }
            _ => {
                prop_assert!(false, "expected Exec variant from both parses");
            }
        }
    }
}
