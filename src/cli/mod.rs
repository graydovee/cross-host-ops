mod args;
mod client;
mod copy;
mod daemon;
mod exec;
mod host;
mod output;
mod progress;
mod prompt;

use std::io::IsTerminal;

use anyhow::Result;

pub use args::{ArunCli, ArunCommand, DaemonCommand, HostCommand, OutputFormat};
pub use exec::{RawModeGuard, set_raw_mode};
pub use output::print_version_json;

use crate::config::{AppConfig, parse_duration};
use crate::types::{
    ExecStdinFlags, ExecTtyFlags, effective_stdin_decision, effective_tty_decision,
    stdin_intent_from_flags, tty_intent_from_flags,
};

use self::copy::run_copy;
use self::daemon::run_daemon_command;
use self::exec::{detect_double_dash_separator, run_command};
use self::host::run_host_command;
use self::output::{list_servers, status};

pub async fn run_cli(cli: ArunCli) -> Result<i32> {
    match cli.command {
        ArunCommand::Exec {
            target,
            cmd,
            timeout,
            tty,
            no_tty,
            stdin,
            no_stdin,
            shell,
            no_shell,
        } => {
            let Some(timeout_ms) = parse_timeout_ms(timeout.as_deref())? else {
                return Ok(125);
            };

            if cmd.is_empty() {
                eprintln!("error: at least one command argument is required");
                return Ok(125);
            }

            let has_separator = detect_double_dash_separator(&target);
            if !has_separator && cmd.len() > 1 {
                eprintln!(
                    "error: multiple command arguments require a `--` separator or quoting as a single string"
                );
                return Ok(125);
            }

            let argv = if !has_separator && cmd.len() == 1 {
                vec!["sh".to_string(), "-c".to_string(), cmd[0].clone()]
            } else {
                cmd
            };

            let stdout_is_tty = std::io::stdout().is_terminal();
            let config = AppConfig::load(None).unwrap_or_default();
            let tty_flags = ExecTtyFlags {
                force_tty: tty,
                force_no_tty: no_tty,
            };
            let stdin_flags = ExecStdinFlags {
                force_stdin: stdin,
                force_no_stdin: no_stdin,
            };
            let resolved_tty = effective_tty_decision(&tty_flags, &config.ssh, stdout_is_tty);
            let resolved_stdin = effective_stdin_decision(&stdin_flags, &config.ssh);
            let tty_intent = tty_intent_from_flags(&tty_flags);
            let stdin_intent = stdin_intent_from_flags(&stdin_flags);

            run_command(
                target,
                argv,
                resolved_tty,
                tty_intent,
                resolved_stdin,
                stdin_intent,
                timeout_ms,
                shell,
                no_shell,
                &config,
            )
            .await
        }
        ArunCommand::Cp {
            recursive,
            quiet,
            source,
            dest,
            timeout,
        } => {
            let Some(timeout_ms) = parse_timeout_ms(timeout.as_deref())? else {
                return Ok(125);
            };
            run_copy(recursive, quiet, source, dest, timeout_ms).await
        }
        ArunCommand::Status => status().await,
        ArunCommand::Ls { refresh } => list_servers(refresh).await,
        ArunCommand::Host { command } => run_host_command(command).await,
        ArunCommand::Daemon { command } => run_daemon_command(command).await,
    }
}

fn parse_timeout_ms(timeout: Option<&str>) -> Result<Option<u64>> {
    let Some(timeout_str) = timeout else {
        return Ok(Some(0));
    };
    match parse_duration(timeout_str) {
        Ok(dur) => {
            let secs = dur.as_secs();
            if secs < 1 || secs > 86400 {
                eprintln!("error: timeout must be between 1s and 86400s");
                return Ok(None);
            }
            Ok(Some(dur.as_millis() as u64))
        }
        Err(e) => {
            eprintln!("error: invalid timeout '{}': {}", timeout_str, e);
            Ok(None)
        }
    }
}
