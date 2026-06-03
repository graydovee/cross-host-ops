//! Property-based tests for CLI command parsing correctness.
//!
//! These tests verify that the current CLI commands (`rhop ls`, `rhop host add/remove/list`)
//! parse correctly with arbitrary valid inputs.
//!
//! Note: The deprecated command forms (`rhop server list`, `rhop remote connect/remove/list`)
//! have been fully removed from the CLI. These tests now validate the current command tree.
//!
//! Feature: cli-command-tree-refactor

use clap::Parser;
use proptest::prelude::*;

use rhop::cli::{ArunCli, ArunCommand, HostCommand};

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating valid NAME strings.
/// Alphanumeric, 1-50 chars, starting with a letter.
fn arb_name() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9]{0,49}"
}

/// Strategy for generating valid ADDRESS strings.
/// Formats: user@host:port, host:port, or just host.
fn arb_address() -> impl Strategy<Value = String> {
    let host_part = "[a-z][a-z0-9]{0,19}(\\.[a-z][a-z0-9]{0,9}){0,3}";
    let port_part = prop::num::u16::ANY.prop_map(|p| {
        // Use ports in a reasonable range for display
        let port = (p % 65534) + 1;
        port.to_string()
    });

    let user_part = "[a-z][a-z0-9_]{0,14}";

    // Three address formats
    prop_oneof![
        // Format: just host
        host_part.prop_map(|h| h),
        // Format: host:port
        (host_part, port_part.clone()).prop_map(|(h, p)| format!("{}:{}", h, p)),
        // Format: user@host:port
        (user_part, host_part, port_part).prop_map(|(u, h, p)| format!("{}@{}:{}", u, h, p)),
    ]
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// `rhop ls` parses to ArunCommand::Ls { refresh: false }.
    #[test]
    fn prop_ls_parses_correctly(_dummy in 0u8..1) {
        let args = vec!["rhop", "ls"];
        let cli = ArunCli::try_parse_from(&args).unwrap();

        match &cli.command {
            ArunCommand::Ls { refresh } => {
                prop_assert_eq!(*refresh, false, "ls without --refresh should have refresh=false");
            }
            other => {
                prop_assert!(false, "expected Ls, got {:?}", other);
            }
        }
    }

    /// `rhop ls --refresh` parses to ArunCommand::Ls { refresh: true }.
    #[test]
    fn prop_ls_refresh_parses_correctly(_dummy in 0u8..1) {
        let args = vec!["rhop", "ls", "--refresh"];
        let cli = ArunCli::try_parse_from(&args).unwrap();

        match &cli.command {
            ArunCommand::Ls { refresh } => {
                prop_assert_eq!(*refresh, true, "ls --refresh should have refresh=true");
            }
            other => {
                prop_assert!(false, "expected Ls {{ refresh: true }}, got {:?}", other);
            }
        }
    }

    /// For random NAME and ADDRESS, `rhop host add <NAME> <ADDRESS>` parses correctly.
    #[test]
    fn prop_host_add_parses_correctly(
        name in arb_name(),
        address in arb_address(),
    ) {
        let args = vec![
            "rhop".to_string(),
            "host".to_string(),
            "add".to_string(),
            name.clone(),
            address.clone(),
        ];
        let cli = ArunCli::try_parse_from(&args);
        prop_assert!(
            cli.is_ok(),
            "Failed to parse 'host add' form: {:?}",
            cli.err()
        );
        let cli = cli.unwrap();

        match &cli.command {
            ArunCommand::Host { command: HostCommand::Add { name: parsed_name, address: parsed_address, .. } } => {
                prop_assert_eq!(parsed_name, &name, "NAME mismatch in host add");
                prop_assert_eq!(parsed_address, &address, "ADDRESS mismatch in host add");
            }
            other => {
                prop_assert!(false, "expected Host {{ Add }}, got {:?}", other);
            }
        }
    }

    /// For random NAME, `rhop host remove <NAME>` parses correctly.
    #[test]
    fn prop_host_remove_parses_correctly(
        name in arb_name(),
    ) {
        let args = vec![
            "rhop".to_string(),
            "host".to_string(),
            "remove".to_string(),
            name.clone(),
        ];
        let cli = ArunCli::try_parse_from(&args);
        prop_assert!(
            cli.is_ok(),
            "Failed to parse 'host remove' form: {:?}",
            cli.err()
        );
        let cli = cli.unwrap();

        match &cli.command {
            ArunCommand::Host { command: HostCommand::Remove { name: parsed_name } } => {
                prop_assert_eq!(parsed_name, &name, "NAME mismatch in host remove");
            }
            other => {
                prop_assert!(false, "expected Host {{ Remove }}, got {:?}", other);
            }
        }
    }

    /// `rhop host list` parses to ArunCommand::Host { command: HostCommand::List }.
    #[test]
    fn prop_host_list_parses_correctly(_dummy in 0u8..1) {
        let args = vec!["rhop", "host", "list"];
        let cli = ArunCli::try_parse_from(&args).unwrap();

        match &cli.command {
            ArunCommand::Host { command: HostCommand::List } => {}
            other => {
                prop_assert!(false, "expected Host {{ List }}, got {:?}", other);
            }
        }
    }
}
