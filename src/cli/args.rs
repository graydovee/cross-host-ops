use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// Output format for CLI responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text output (default).
    Text,
    /// NDJSON output (one JSON object per line).
    Json,
}

#[derive(Debug, Parser)]
#[command(name = "xho")]
#[command(
    about = "Cross Host Ops command runner with a local or remote daemon",
    version
)]
pub struct ArunCli {
    /// Output format: text (default) or json (NDJSON).
    #[arg(long = "output", default_value = "text")]
    pub output_format: OutputFormat,

    /// Disable all interactive prompts; fail instead of waiting for human input.
    #[arg(long = "non-interactive")]
    pub non_interactive: bool,

    #[command(subcommand)]
    pub command: ArunCommand,
}

#[derive(Debug, Subcommand)]
pub enum ArunCommand {
    #[command(
        about = "Execute a remote command: xho exec <TARGET> [--] <CMD> [ARGS...]",
        long_about = "Execute a remote command on the target host.\n\n\
            Use -- to separate xho options from remote arguments when remote \
            arguments begin with a hyphen that could conflict with xho options.",
        trailing_var_arg = true
    )]
    Exec {
        /// Allocate a TTY for the remote command.
        #[arg(short = 't', long = "tty", conflicts_with = "no_tty")]
        tty: bool,
        /// Do not allocate a TTY for the remote command.
        #[arg(long = "no-tty")]
        no_tty: bool,
        /// Forward local stdin to the remote command's stdin.
        #[arg(short = 'i', long = "stdin", conflicts_with = "no_stdin")]
        stdin: bool,
        /// Do not forward stdin (overrides config default).
        #[arg(long = "no-stdin")]
        no_stdin: bool,
        /// Abort the operation after this duration (e.g. 30s, 2m).
        #[arg(long = "timeout", value_name = "DURATION")]
        timeout: Option<String>,
        /// Wrap the remote command in a shell to source rc files.
        /// Use --shell bash to wrap with bash, --no-shell to disable.
        #[arg(long = "shell")]
        shell: Option<String>,
        /// Disable shell wrapping regardless of config.
        #[arg(long = "no-shell", conflicts_with = "shell")]
        no_shell: bool,
        /// Remote target name.
        #[arg(value_name = "TARGET")]
        target: String,
        /// Remote command and arguments (use -- to separate from xho options).
        #[arg(
            value_name = "CMD",
            trailing_var_arg = true,
            allow_hyphen_values = true
        )]
        cmd: Vec<String>,
    },
    #[command(about = "Copy files between local and remote host")]
    Cp {
        #[arg(short = 'r', long = "recursive")]
        recursive: bool,
        /// Suppress progress bars and non-error copy messages.
        #[arg(short = 'q', long = "quiet")]
        quiet: bool,
        /// Abort the operation after this duration (e.g. 30s, 2m).
        #[arg(long = "timeout", value_name = "DURATION")]
        timeout: Option<String>,
        #[arg(value_name = "SOURCE")]
        source: String,
        #[arg(value_name = "DEST")]
        dest: String,
    },
    #[command(about = "Show daemon and connection pool status")]
    Status,
    #[command(about = "List reachable servers from all configured sources")]
    Ls {
        /// Re-fetch every server list source bypassing the in-memory cache.
        #[arg(long, alias = "no-cache")]
        refresh: bool,
    },
    #[command(about = "Manage jump host entries")]
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
    #[command(about = "Manage the local daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    #[command(about = "Manage encrypted secrets in the local vault")]
    Secret {
        /// Daemon config file to operate on. Defaults to ~/.xho/config.toml.
        /// The vault is looked up next to this config (<config_dir>/secrets)
        /// unless [secret].vault_path overrides it.
        #[arg(long = "config", value_name = "FILE", global = true)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        command: SecretCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum SecretCommand {
    /// Encrypt all plaintext secrets in config.toml and server.toml,
    /// replacing them in place with `vault:` references.
    #[command(about = "Encrypt plaintext secrets in config files into the vault")]
    Encrypt {
        /// Show what would change without modifying any files.
        #[arg(long = "dry-run")]
        dry_run: bool,
    },
    /// Store a single secret in the vault (value read interactively, hidden).
    #[command(about = "Store a secret in the vault under <NAME>")]
    Set {
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// List entry names stored in the vault (values are never shown).
    #[command(about = "List secret names stored in the vault")]
    List,
    /// Re-encrypt the vault under a different identity file.
    #[command(about = "Re-encrypt the vault under a new identity file")]
    Rekey {
        /// Identity file the vault is currently encrypted under.
        #[arg(long = "old", value_name = "FILE")]
        old: String,
        /// Identity file to re-encrypt the vault under.
        #[arg(long = "new", value_name = "FILE")]
        new: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    #[command(about = "Start the daemon in background mode")]
    Start {
        #[arg(short = 'c', long = "config", value_name = "FILE")]
        config: Option<PathBuf>,
        #[arg(long = "log-level", value_name = "LEVEL")]
        log_level: Option<String>,
    },
    #[command(about = "Stop the daemon")]
    Stop,
    #[command(about = "Restart the daemon")]
    Restart,
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    /// Add a new jump host entry.
    #[command(about = "Add a new jump host entry")]
    Add {
        #[arg(value_name = "NAME")]
        name: String,
        #[arg(value_name = "ADDRESS", help = "[user@]host[:port]")]
        address: String,
        #[arg(long = "identity-file", value_name = "FILE")]
        identity_file: Option<String>,
        #[arg(long = "known-hosts", value_name = "FILE")]
        known_hosts: Option<String>,
        #[arg(long = "accept-new-host-key", conflicts_with = "fingerprint")]
        accept_new_host_key: bool,
        #[arg(
            long = "fingerprint",
            value_name = "SHA256",
            conflicts_with = "accept_new_host_key"
        )]
        fingerprint: Option<String>,
    },
    /// Remove a jump host entry.
    #[command(about = "Remove a jump host entry")]
    Remove {
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// List all configured jump hosts.
    #[command(about = "List all configured jump hosts")]
    List,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use proptest::prelude::*;

    fn argv_element_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            "[a-zA-Z0-9_./]{1,20}",
            Just("--non-interactive".to_string()),
            Just("--tty".to_string()),
            Just("--no-tty".to_string()),
            Just("--stdin".to_string()),
            Just("--output".to_string()),
            Just("--output=json".to_string()),
            Just("--timeout".to_string()),
            Just("--timeout=30s".to_string()),
            Just("--".to_string()),
            Just("-v".to_string()),
            Just("-n".to_string()),
            Just("-e".to_string()),
            "[a-z]{1,8}=[a-z0-9]{1,8}".prop_map(|s| format!("--{}", s)),
            Just("-".to_string()),
            Just("---".to_string()),
        ]
    }

    #[test]
    fn cp_quiet_short_flag_parses() {
        let parsed =
            ArunCli::try_parse_from(["xho", "cp", "-q", "local.txt", "host1:/tmp/local.txt"])
                .unwrap();
        match parsed.command {
            ArunCommand::Cp {
                quiet,
                source,
                dest,
                ..
            } => {
                assert!(quiet);
                assert_eq!(source, "local.txt");
                assert_eq!(dest, "host1:/tmp/local.txt");
            }
            _ => panic!("expected cp command"),
        }
    }

    #[test]
    fn cp_quiet_long_flag_parses_with_recursive_and_timeout() {
        let parsed = ArunCli::try_parse_from([
            "xho",
            "cp",
            "--quiet",
            "-r",
            "--timeout",
            "30s",
            "dir",
            "host1:/tmp/dir",
        ])
        .unwrap();
        match parsed.command {
            ArunCommand::Cp {
                quiet,
                recursive,
                timeout,
                source,
                dest,
            } => {
                assert!(quiet);
                assert!(recursive);
                assert_eq!(timeout.as_deref(), Some("30s"));
                assert_eq!(source, "dir");
                assert_eq!(dest, "host1:/tmp/dir");
            }
            _ => panic!("expected cp command"),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        #[test]
        fn prop_argv_passthrough_transparency(
            argv in proptest::collection::vec(argv_element_strategy(), 1..10),
        ) {
            let target = "my-target";
            let mut args = vec!["xho".to_string(), "exec".to_string(), target.to_string(), "--".to_string()];
            args.extend(argv.clone());

            let parsed = ArunCli::try_parse_from(&args).unwrap();
            match parsed.command {
                ArunCommand::Exec { target: parsed_target, cmd, .. } => {
                    prop_assert_eq!(&parsed_target, target);
                    prop_assert_eq!(&cmd, &argv);
                }
                _ => panic!("expected Exec command"),
            }
        }
    }
}
