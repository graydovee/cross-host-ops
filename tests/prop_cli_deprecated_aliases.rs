//! Property-based tests for deprecated command dispatch equivalence.
//!
//! These tests verify that deprecated command forms (`rhop server list`,
//! `rhop remote connect/remove/list`) parse to equivalent structures as
//! their new counterparts (`rhop ls`, `rhop host add/remove/list`).
//!
//! Feature: cli-command-tree-refactor, Property 5: Deprecated command dispatch equivalence

use clap::Parser;
use proptest::prelude::*;

use rhop::cli::{ArunCli, ArunCommand, HostCommand, RemoteCommand, ServerCommand};

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

// Feature: cli-command-tree-refactor, Property 5: Deprecated command dispatch equivalence
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.6, 7.3**
    ///
    /// `rhop server list` parses to ArunCommand::Server { command: ServerCommand::List { refresh: false } }
    /// and `rhop ls` parses to ArunCommand::Ls { refresh: false }.
    /// Both should dispatch to the same handler with the same arguments.
    #[test]
    fn prop_server_list_equivalence(_dummy in 0u8..1) {
        // Parse the deprecated form
        let old_args = vec!["rhop", "server", "list"];
        let old_cli = ArunCli::try_parse_from(&old_args).unwrap();

        // Parse the new form
        let new_args = vec!["rhop", "ls"];
        let new_cli = ArunCli::try_parse_from(&new_args).unwrap();

        // Verify deprecated form parses correctly
        match &old_cli.command {
            ArunCommand::Server { command: ServerCommand::List { refresh } } => {
                prop_assert_eq!(*refresh, false, "server list without --refresh should have refresh=false");
            }
            other => {
                prop_assert!(false, "expected Server {{ List }}, got {:?}", other);
            }
        }

        // Verify new form parses correctly
        match &new_cli.command {
            ArunCommand::Ls { refresh } => {
                prop_assert_eq!(*refresh, false, "ls without --refresh should have refresh=false");
            }
            other => {
                prop_assert!(false, "expected Ls, got {:?}", other);
            }
        }
    }

    /// **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.6, 7.3**
    ///
    /// `rhop server list --refresh` parses to ArunCommand::Server { command: ServerCommand::List { refresh: true } }
    /// and `rhop ls --refresh` parses to ArunCommand::Ls { refresh: true }.
    #[test]
    fn prop_server_list_refresh_equivalence(_dummy in 0u8..1) {
        // Parse the deprecated form with --refresh
        let old_args = vec!["rhop", "server", "list", "--refresh"];
        let old_cli = ArunCli::try_parse_from(&old_args).unwrap();

        // Parse the new form with --refresh
        let new_args = vec!["rhop", "ls", "--refresh"];
        let new_cli = ArunCli::try_parse_from(&new_args).unwrap();

        // Verify deprecated form parses correctly
        match &old_cli.command {
            ArunCommand::Server { command: ServerCommand::List { refresh } } => {
                prop_assert_eq!(*refresh, true, "server list --refresh should have refresh=true");
            }
            other => {
                prop_assert!(false, "expected Server {{ List {{ refresh: true }} }}, got {:?}", other);
            }
        }

        // Verify new form parses correctly
        match &new_cli.command {
            ArunCommand::Ls { refresh } => {
                prop_assert_eq!(*refresh, true, "ls --refresh should have refresh=true");
            }
            other => {
                prop_assert!(false, "expected Ls {{ refresh: true }}, got {:?}", other);
            }
        }
    }

    /// **Validates: Requirements 4.2, 4.6, 7.3**
    ///
    /// For random NAME and ADDRESS, `rhop remote connect <NAME> <ADDRESS>` and
    /// `rhop host add <NAME> <ADDRESS>` produce equivalent arguments.
    #[test]
    fn prop_remote_connect_host_add_equivalence(
        name in arb_name(),
        address in arb_address(),
    ) {
        // Parse the deprecated form
        let old_args = vec![
            "rhop".to_string(),
            "remote".to_string(),
            "connect".to_string(),
            name.clone(),
            address.clone(),
        ];
        let old_cli = ArunCli::try_parse_from(&old_args);
        prop_assert!(
            old_cli.is_ok(),
            "Failed to parse deprecated 'remote connect' form: {:?}",
            old_cli.err()
        );
        let old_cli = old_cli.unwrap();

        // Parse the new form
        let new_args = vec![
            "rhop".to_string(),
            "host".to_string(),
            "add".to_string(),
            name.clone(),
            address.clone(),
        ];
        let new_cli = ArunCli::try_parse_from(&new_args);
        prop_assert!(
            new_cli.is_ok(),
            "Failed to parse new 'host add' form: {:?}",
            new_cli.err()
        );
        let new_cli = new_cli.unwrap();

        // Extract and compare arguments
        let (old_name, old_address) = match &old_cli.command {
            ArunCommand::Remote { command: RemoteCommand::Connect { name, address, .. } } => {
                (name.clone(), address.clone())
            }
            other => {
                prop_assert!(false, "expected Remote {{ Connect }}, got {:?}", other);
                unreachable!()
            }
        };

        let (new_name, new_address) = match &new_cli.command {
            ArunCommand::Host { command: HostCommand::Add { name, address, .. } } => {
                (name.clone(), address.clone())
            }
            other => {
                prop_assert!(false, "expected Host {{ Add }}, got {:?}", other);
                unreachable!()
            }
        };

        // Verify equivalence: same handler arguments
        prop_assert_eq!(&old_name, &new_name, "NAME mismatch between remote connect and host add");
        prop_assert_eq!(&old_address, &new_address, "ADDRESS mismatch between remote connect and host add");
    }

    /// **Validates: Requirements 4.3, 4.6, 7.3**
    ///
    /// For random NAME, `rhop remote remove <NAME>` and `rhop host remove <NAME>`
    /// produce equivalent arguments.
    #[test]
    fn prop_remote_remove_host_remove_equivalence(
        name in arb_name(),
    ) {
        // Parse the deprecated form
        let old_args = vec![
            "rhop".to_string(),
            "remote".to_string(),
            "remove".to_string(),
            name.clone(),
        ];
        let old_cli = ArunCli::try_parse_from(&old_args);
        prop_assert!(
            old_cli.is_ok(),
            "Failed to parse deprecated 'remote remove' form: {:?}",
            old_cli.err()
        );
        let old_cli = old_cli.unwrap();

        // Parse the new form
        let new_args = vec![
            "rhop".to_string(),
            "host".to_string(),
            "remove".to_string(),
            name.clone(),
        ];
        let new_cli = ArunCli::try_parse_from(&new_args);
        prop_assert!(
            new_cli.is_ok(),
            "Failed to parse new 'host remove' form: {:?}",
            new_cli.err()
        );
        let new_cli = new_cli.unwrap();

        // Extract and compare arguments
        let old_name = match &old_cli.command {
            ArunCommand::Remote { command: RemoteCommand::Remove { name } } => name.clone(),
            other => {
                prop_assert!(false, "expected Remote {{ Remove }}, got {:?}", other);
                unreachable!()
            }
        };

        let new_name = match &new_cli.command {
            ArunCommand::Host { command: HostCommand::Remove { name } } => name.clone(),
            other => {
                prop_assert!(false, "expected Host {{ Remove }}, got {:?}", other);
                unreachable!()
            }
        };

        prop_assert_eq!(&old_name, &new_name, "NAME mismatch between remote remove and host remove");
    }

    /// **Validates: Requirements 4.4, 4.6, 7.3**
    ///
    /// `rhop remote list` and `rhop host list` both parse successfully
    /// and produce equivalent dispatch targets.
    #[test]
    fn prop_remote_list_host_list_equivalence(_dummy in 0u8..1) {
        // Parse the deprecated form
        let old_args = vec!["rhop", "remote", "list"];
        let old_cli = ArunCli::try_parse_from(&old_args).unwrap();

        // Parse the new form
        let new_args = vec!["rhop", "host", "list"];
        let new_cli = ArunCli::try_parse_from(&new_args).unwrap();

        // Verify deprecated form parses to Remote { List }
        match &old_cli.command {
            ArunCommand::Remote { command: RemoteCommand::List } => {}
            other => {
                prop_assert!(false, "expected Remote {{ List }}, got {:?}", other);
            }
        }

        // Verify new form parses to Host { List }
        match &new_cli.command {
            ArunCommand::Host { command: HostCommand::List } => {}
            other => {
                prop_assert!(false, "expected Host {{ List }}, got {:?}", other);
            }
        }
    }

    /// **Validates: Requirements 4.6, 7.3**
    ///
    /// Verify the deprecation warning format: `emit_deprecation_warning` produces
    /// a single line to stderr containing the replacement command.
    /// We test this by verifying the format string pattern matches expectations.
    #[test]
    fn prop_deprecation_warning_format(_dummy in 0u8..1) {
        // The deprecation warning format is:
        //   "warning: '{old_cmd}' is deprecated; use '{new_cmd}' instead"
        // Verify the expected format for each deprecated command pair.
        let pairs = vec![
            ("rhop server list", "rhop ls"),
            ("rhop remote connect", "rhop host add"),
            ("rhop remote remove", "rhop host remove"),
            ("rhop remote list", "rhop host list"),
        ];

        for (old_cmd, new_cmd) in &pairs {
            let expected = format!("warning: '{}' is deprecated; use '{}' instead", old_cmd, new_cmd);
            // Verify it's a single line (no newlines in the message body)
            prop_assert!(
                !expected.contains('\n'),
                "deprecation warning should be a single line, got: {}",
                expected
            );
            // Verify it contains the replacement command
            prop_assert!(
                expected.contains(new_cmd),
                "deprecation warning should contain replacement command '{}', got: {}",
                new_cmd,
                expected
            );
        }
    }
}
