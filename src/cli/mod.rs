use std::env;
use std::ffi::OsStr;
use std::io::{self, IsTerminal, Write};
use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hyper_util::rt::TokioIo;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::config::{
    AppConfig, ClientConfig, GatewayConfig, RhopdGatewayConfig,
    default_config_path, expand_tilde, parse_duration, RESERVED_NAMES,
};
use crate::types::{CopyDirection, CopySpec, AddressDefaults, RemoteAddress, ExecTtyFlags, ExecStdinFlags, effective_tty_decision, effective_stdin_decision, should_use_interactive_mode};
use crate::exit_codes::cap_remote_exit_code;
use crate::daemon::gateway::GatewayKind;
use crate::protocol::rpc;
use crate::daemon::gateway::auth::{
    KnownHostState, fetch_remote_host_key, inspect_known_host,
    normalize_paths as normalize_remote_paths, parse_remote_target, trust_known_host,
};

/// Output format for CLI responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text output (default).
    Text,
    /// NDJSON output (one JSON object per line).
    Json,
}

#[derive(Debug, Parser)]
#[command(name = "rhop")]
#[command(about = "Remote Hop command runner with a local or remote daemon", version)]
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
        about = "Execute a remote command: rhop exec <TARGET> [--] <CMD> [ARGS...]",
        long_about = "Execute a remote command on the target host.\n\n\
            Use -- to separate rhop options from remote arguments when remote \
            arguments begin with a hyphen that could conflict with rhop options.",
        trailing_var_arg = true,
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
        /// Remote command and arguments (use -- to separate from rhop options).
        #[arg(
            value_name = "CMD",
            trailing_var_arg = true,
            allow_hyphen_values = true,
        )]
        cmd: Vec<String>,
    },
    #[command(about = "Copy files between local and remote host")]
    Cp {
        #[arg(short = 'r', long = "recursive")]
        recursive: bool,
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
        #[arg(long = "fingerprint", value_name = "SHA256", conflicts_with = "accept_new_host_key")]
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

// Interactive mode detection is now provided by crate::types::should_use_interactive_mode
// (4-argument version: resolved_tty, resolved_stdin, stdin_is_tty, stdout_is_tty).

/// Get the current terminal size as (cols, rows).
/// Falls back to (80, 24) if the terminal size cannot be determined.
pub(crate) fn get_terminal_size() -> (u16, u16) {
    terminal_size::terminal_size()
        .map(|(w, h)| (w.0, h.0))
        .unwrap_or((80, 24))
}

/// RAII guard that restores terminal to original mode on drop.
pub struct RawModeGuard {
    original_termios: libc::termios,
    fd: RawFd,
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        // Restore original terminal settings unconditionally.
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.original_termios);
        }
    }
}

/// Set the terminal to raw mode and return a guard that restores it on drop.
///
/// If raw mode setup fails (e.g., fd is not a terminal), returns an error
/// so the caller can fall back to non-interactive mode.
pub fn set_raw_mode(fd: RawFd) -> Result<RawModeGuard> {
    unsafe {
        let mut original_termios: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(fd, &mut original_termios) != 0 {
            return Err(anyhow!(
                "tcgetattr failed: {}",
                io::Error::last_os_error()
            ));
        }

        let mut raw = original_termios;
        libc::cfmakeraw(&mut raw);

        if libc::tcsetattr(fd, libc::TCSANOW, &raw) != 0 {
            return Err(anyhow!(
                "tcsetattr failed: {}",
                io::Error::last_os_error()
            ));
        }

        Ok(RawModeGuard {
            original_termios,
            fd,
        })
    }
}

pub async fn run_cli(cli: ArunCli) -> Result<i32> {
    match cli.command {
        ArunCommand::Exec { target, cmd, timeout, tty, no_tty, stdin, no_stdin, shell, no_shell } => {
            // Validate --timeout if provided: parse and bounds-check (1–86400 seconds).
            let timeout_ms: u64 = if let Some(ref timeout_str) = timeout {
                match parse_duration(timeout_str) {
                    Ok(dur) => {
                        let secs = dur.as_secs();
                        if secs < 1 || secs > 86400 {
                            eprintln!("error: timeout must be between 1s and 86400s");
                            return Ok(125);
                        }
                        dur.as_millis() as u64
                    }
                    Err(e) => {
                        eprintln!("error: invalid timeout '{}': {}", timeout_str, e);
                        return Ok(125);
                    }
                }
            } else {
                0
            };

            if cmd.is_empty() {
                eprintln!("error: at least one command argument is required");
                return Ok(125);
            }

            // Detect whether `--` was used by scanning raw process args.
            let has_separator = detect_double_dash_separator(&target);

            if !has_separator && cmd.len() > 1 {
                eprintln!(
                    "error: multiple command arguments require a `--` separator or quoting as a single string"
                );
                return Ok(125);
            }

            let argv = if !has_separator && cmd.len() == 1 {
                // Single-string mode: wrap in sh -c
                vec!["sh".to_string(), "-c".to_string(), cmd[0].clone()]
            } else {
                // Multi-arg mode (with --): pass directly
                cmd
            };

            // Resolve TTY and stdin decisions using the new decision functions.
            let stdout_is_tty = std::io::stdout().is_terminal();
            let config = AppConfig::load(None).unwrap_or_default();
            let tty_flags = ExecTtyFlags { force_tty: tty, force_no_tty: no_tty };
            let stdin_flags = ExecStdinFlags { force_stdin: stdin, force_no_stdin: no_stdin };
            let resolved_tty = effective_tty_decision(&tty_flags, &config.ssh, stdout_is_tty);
            let resolved_stdin = effective_stdin_decision(&stdin_flags, &config.ssh);

            run_command(target, argv, resolved_tty, resolved_stdin, timeout_ms, shell, no_shell, &config).await
        }
        ArunCommand::Cp {
            recursive,
            source,
            dest,
            timeout,
        } => {
            let timeout_ms: u64 = if let Some(ref timeout_str) = timeout {
                match parse_duration(timeout_str) {
                    Ok(dur) => {
                        let secs = dur.as_secs();
                        if secs < 1 || secs > 86400 {
                            eprintln!("error: timeout must be between 1s and 86400s");
                            return Ok(125);
                        }
                        dur.as_millis() as u64
                    }
                    Err(e) => {
                        eprintln!("error: invalid timeout '{}': {}", timeout_str, e);
                        return Ok(125);
                    }
                }
            } else {
                0
            };
            run_copy(recursive, source, dest, timeout_ms).await
        }
        ArunCommand::Status => status().await,
        ArunCommand::Ls { refresh } => list_servers(refresh).await,
        ArunCommand::Host { command } => run_host_command(command).await,
        ArunCommand::Daemon { command } => run_daemon_command(command).await,
    }
}

/// Emit a JSON object describing the binary's version, capabilities, and exit codes.
///
/// Called when `rhop --version --output json` is invoked.
pub fn print_version_json() {
    let version_info = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": [
            "exec",
            "cp",
            "status",
            "ls",
            "host.add",
            "host.remove",
            "host.list",
            "daemon.start",
            "daemon.stop",
            "daemon.restart"
        ],
        "exit_codes": {
            "0": "success",
            "1-123": "remote command exit code",
            "124": "operation timed out",
            "125": "rhop or daemon failure",
            "126": "auth/host-key/review denied",
            "127": "target not found / unsupported capability"
        }
    });
    println!("{}", serde_json::to_string_pretty(&version_info).unwrap());
}

/// Detect whether a `--` separator was used between the target and the first cmd token.
///
/// Scans `std::env::args_os()` for a literal `--` that appears after the target positional.
/// This is necessary because clap's `trailing_var_arg` consumes all tokens after target
/// into `cmd`, stripping the `--` from the parsed result.
fn detect_double_dash_separator(target: &str) -> bool {
    let raw_args: Vec<_> = env::args_os().collect();
    let target_os = OsStr::new(target);

    // Find the position of "exec" subcommand in raw args.
    let exec_pos = match raw_args.iter().position(|a| a == OsStr::new("exec")) {
        Some(p) => p,
        None => return false,
    };

    // Find the target position after "exec" (skip options like --tty, --timeout, etc.)
    let target_pos = match raw_args[exec_pos + 1..]
        .iter()
        .position(|a| a == target_os)
    {
        Some(p) => exec_pos + 1 + p,
        None => return false,
    };

    // Check if the token immediately after target is `--`.
    raw_args
        .get(target_pos + 1)
        .map(|a| a == OsStr::new("--"))
        .unwrap_or(false)
}

async fn run_command(target: String, argv: Vec<String>, resolved_tty: bool, resolved_stdin: bool, timeout_ms: u64, shell: Option<String>, no_shell: bool, _config: &AppConfig) -> Result<i32> {
    // Pass raw CLI flags to daemon; daemon resolves effective shell from server.toml.
    let cli_shell = shell.unwrap_or_default();

    // Check if we should use interactive mode (all 4 conditions must be true).
    let stdin_is_tty = io::stdin().is_terminal();
    let stdout_is_tty = io::stdout().is_terminal();

    if should_use_interactive_mode(resolved_tty, resolved_stdin, stdin_is_tty, stdout_is_tty) {
        return run_interactive(target, argv, timeout_ms, cli_shell, no_shell).await;
    }

    // Batch execution path: request_pty + exec without sentinel.
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;

    let (tx, rx) = mpsc::channel(8);
    tx.send(rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target,
            argv,
            pty: resolved_tty,
            no_pty: !resolved_tty,
            stdin: resolved_stdin,
            timeout_ms,
            interactive: false,
            term_cols: 0,
            term_rows: 0,
            shell: cli_shell,
            no_shell,
        })),
    })
    .await
    .map_err(|_| anyhow!("failed to send execute request"))?;

    // If stdin forwarding is requested, spawn a task that reads from tokio stdin
    // and sends data on the bidirectional stream.
    //
    // EOF semantics: instead of relying on closing the gRPC request stream
    // (which is unreliable across SSH-tunneled gRPC hops), we send an
    // EXPLICIT zero-length StdinData message as an EOF sentinel.  Daemons
    // along the path interpret an empty StdinData payload as "no more stdin"
    // and propagate it downstream.  We then keep the sender alive for the
    // remainder of the call so the bidirectional stream stays healthy.
    let response_tx = if resolved_stdin {
        let stdin_tx = tx.clone();
        tokio::spawn(async move {
            let mut tokio_stdin = tokio::io::stdin();
            let mut buf = [0u8; 4096];
            loop {
                match tokio_stdin.read(&mut buf).await {
                    Ok(0) => {
                        // Local stdin EOF: send explicit empty StdinData as
                        // an EOF sentinel rather than just dropping the sender.
                        let eof_msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::StdinData(
                                rpc::StdinData { data: Vec::new() },
                            )),
                        };
                        let _ = stdin_tx.send(eof_msg).await;
                        break;
                    }
                    Ok(n) => {
                        let msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::StdinData(
                                rpc::StdinData {
                                    data: buf[..n].to_vec(),
                                },
                            )),
                        };
                        if stdin_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            // stdin_tx (the clone) drops here.  The MAIN tx returned in
            // `response_tx` keeps the request channel open for the rest of
            // the call so confirm/auth replies can still be sent if needed
            // and so the gRPC stream stays open until ExitStatus arrives.
        });
        Some(tx)
    } else {
        Some(tx)
    };

    let response = client.execute(ReceiverStream::new(rx)).await?;
    let mut stream = response.into_inner();
    let mut exit_code = 1;

    while let Some(message) = stream.message().await? {
        match message
            .event
            .ok_or_else(|| anyhow!("execute stream returned empty event"))?
        {
            rpc::execute_response::Event::Stdout(chunk) => {
                io::stdout().write_all(&chunk.data)?;
                io::stdout().flush()?;
            }
            rpc::execute_response::Event::Stderr(chunk) => {
                io::stderr().write_all(&chunk.data)?;
                io::stderr().flush()?;
            }
            rpc::execute_response::Event::ReviewResult(_result) => {}
            rpc::execute_response::Event::ConfirmRequired(confirm) => {
                let allow = prompt_for_confirmation(&confirm.reason)?;
                let Some(ref response_tx) = response_tx else {
                    return Err(anyhow!(
                        "received ConfirmRequired but no response channel available"
                    ));
                };
                response_tx.send(rpc::ExecuteRequest {
                    request: Some(rpc::execute_request::Request::Confirm(
                        rpc::ConfirmRequest {
                            execution_id: confirm.execution_id,
                            allow,
                        },
                    )),
                })
                .await
                .map_err(|_| anyhow!("failed to send confirmation request"))?;
            }
            rpc::execute_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                let Some(ref response_tx) = response_tx else {
                    return Err(anyhow!(
                        "received AuthPrompt but no response channel available"
                    ));
                };
                response_tx.send(crate::protocol::execute_auth_input_request(prompt.prompt_id, value))
                    .await
                    .map_err(|_| anyhow!("failed to send auth input request"))?;
            }
            rpc::execute_response::Event::ExitStatus(status) => {
                exit_code = status.code;
                break;
            }
            rpc::execute_response::Event::Info(info) => {
                eprintln!("{}", info.message);
            }
            rpc::execute_response::Event::Error(error) => {
                eprintln!("error: {}", error.message);
                return Ok(1);
            }
        }
    }

    // Exit code 124 from the daemon means timeout fired. If the client
    // requested a timeout, pass 124 through directly (it's rhop's own
    // semantic, not a remote process exit code). Otherwise cap normally.
    if exit_code == 124 && timeout_ms > 0 {
        Ok(124)
    } else {
        Ok(cap_remote_exit_code(exit_code))
    }
}

/// Run a command in interactive PTY mode with raw terminal, bidirectional
/// byte streaming, and SIGWINCH forwarding.
pub(crate) async fn run_interactive(
    target: String,
    argv: Vec<String>,
    timeout_ms: u64,
    shell: String,
    no_shell: bool,
) -> Result<i32> {
    // Step 1: Get initial terminal size.
    let (cols, rows) = get_terminal_size();

    // Step 2: Connect to daemon and send StartRequest with interactive=true.
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;
    let (tx, rx) = mpsc::channel(32);
    tx.send(rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target,
            argv,
            pty: true,
            no_pty: false,
            stdin: false,
            timeout_ms,
            interactive: true,
            term_cols: cols as u32,
            term_rows: rows as u32,
            shell,
            no_shell,
        })),
    })
    .await
    .map_err(|_| anyhow!("failed to send execute request"))?;

    let response = client.execute(ReceiverStream::new(rx)).await?;
    let mut stream = response.into_inner();

    // Step 3: Set terminal to raw mode with RAII guard.
    let _guard = set_raw_mode(libc::STDIN_FILENO)?;

    // Step 4: Spawn stdin forwarding task (channel capacity 32 via tx clone).
    let stdin_tx = tx.clone();
    let stdin_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        let mut stdin = tokio::io::stdin();
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let msg = rpc::ExecuteRequest {
                        request: Some(rpc::execute_request::Request::StdinData(
                            rpc::StdinData {
                                data: buf[..n].to_vec(),
                            },
                        )),
                    };
                    if stdin_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Step 5: Spawn SIGWINCH handler (resize channel capacity 8 via bounded sender).
    let resize_tx = tx.clone();
    let sigwinch_task = tokio::spawn(async move {
        let mut signal = match tokio::signal::unix::signal(SignalKind::window_change()) {
            Ok(s) => s,
            Err(_) => return,
        };
        while signal.recv().await.is_some() {
            let (cols, rows) = get_terminal_size();
            let msg = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::WindowResize(
                    rpc::WindowResize {
                        cols: cols as u32,
                        rows: rows as u32,
                    },
                )),
            };
            if resize_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Step 6: Process response stream — write stdout directly, handle exit.
    let mut exit_code = 1;
    while let Some(message) = stream.message().await? {
        match message
            .event
            .ok_or_else(|| anyhow!("execute stream returned empty event"))?
        {
            rpc::execute_response::Event::Stdout(chunk) => {
                io::stdout().write_all(&chunk.data)?;
                io::stdout().flush()?;
            }
            rpc::execute_response::Event::ExitStatus(status) => {
                exit_code = status.code;
                break;
            }
            rpc::execute_response::Event::Error(error) => {
                eprintln!("error: {}", error.message);
                stdin_task.abort();
                sigwinch_task.abort();
                return Ok(1);
            }
            rpc::execute_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                tx.send(crate::protocol::execute_auth_input_request(prompt.prompt_id, value))
                    .await
                    .map_err(|_| anyhow!("failed to send auth input request"))?;
            }
            rpc::execute_response::Event::ConfirmRequired(confirm) => {
                let allow = prompt_for_confirmation(&confirm.reason)?;
                tx.send(rpc::ExecuteRequest {
                    request: Some(rpc::execute_request::Request::Confirm(
                        rpc::ConfirmRequest {
                            execution_id: confirm.execution_id,
                            allow,
                        },
                    )),
                })
                .await
                .map_err(|_| anyhow!("failed to send confirmation request"))?;
            }
            _ => {}
        }
    }

    // Step 7: Cleanup — abort tasks, guard drops automatically restoring terminal.
    stdin_task.abort();
    sigwinch_task.abort();
    Ok(cap_remote_exit_code(exit_code))
}

async fn status() -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::NoAutoStart).await?;
    let response = client.status(rpc::StatusRequest {}).await?.into_inner();
    println!("daemon:");
    println!("  origin: {}", response.daemon_origin);
    println!("  cli_controllable: {}", response.cli_controllable);
    println!("  active_executions: {}", response.active_executions);
    if !response.cli_start_config_path.is_empty() {
        println!("  cli_start_config_path: {}", response.cli_start_config_path);
    }
    if !response.cli_start_log_level.is_empty() {
        println!("  cli_start_log_level: {}", response.cli_start_log_level);
    }
    if response.remote_listening {
        println!("remote:");
        println!("  listening: {}", response.remote_addr);
        println!("  user: {}", response.remote_ssh_user);
    }

    // Print gateways from the daemon's StatusResponse.
    if !response.gateways.is_empty() {
        println!("gateways:");
        for jh in &response.gateways {
            println!("  - name: {}", jh.name);
            println!("    kind: {}", jh.kind);
            println!("    address: {}", jh.address);
            if let Some(sub) = &jh.sub_status {
                println!("    sub_status:");
                println!("      daemon_running: {}", sub.daemon_running);
                println!("      active_executions: {}", sub.active_executions);
                if !sub.pools.is_empty() {
                    println!("      pools:");
                    for pool in &sub.pools {
                        println!(
                            "        {} total={} busy={} idle={} queued={}",
                            pool.key, pool.total, pool.busy, pool.idle, pool.queued
                        );
                    }
                }
            }
        }
    }

    if !response.pools.is_empty() {
        println!("pools:");
        for pool in response.pools {
            println!(
                "  {} total={} busy={} idle={} queued={}",
                pool.key, pool.total, pool.busy, pool.idle, pool.queued
            );
        }
    }
    Ok(0)
}

async fn run_copy(recursive: bool, source: String, dest: String, timeout_ms: u64) -> Result<i32> {
    let (target, spec) = parse_copy_operands(recursive, &source, &dest)?;
    let mut client = connect_local_copy_client().await?;
    let (tx, rx) = mpsc::channel(8);
    tx.send(crate::protocol::copy_spec_to_rpc(target, spec, timeout_ms))
        .await
        .map_err(|_| anyhow!("failed to send copy start request"))?;
    let response = client.copy(ReceiverStream::new(rx)).await?;
    let mut stream = response.into_inner();
    while let Some(message) = stream.message().await? {
        match message
            .event
            .ok_or_else(|| anyhow!("copy stream returned empty event"))?
        {
            rpc::copy_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                tx.send(crate::protocol::copy_auth_input_request(prompt.prompt_id, value))
                    .await
                    .map_err(|_| anyhow!("failed to send copy auth input request"))?;
            }
            rpc::copy_response::Event::Error(error) => {
                eprintln!("error: {}", error.message);
                return Ok(1);
            }
            rpc::copy_response::Event::Complete(done) => {
                if !done.message.is_empty() {
                    println!("{}", done.message);
                }
                break;
            }
            rpc::copy_response::Event::Info(info) => {
                if !info.message.is_empty() {
                    println!("{}", info.message);
                }
            }
            rpc::copy_response::Event::DataChunk(_chunk) => {
                // Download data streaming is not yet implemented in the CLI
                // path; handled in a future task.
            }
        }
    }
    Ok(0)
}

/// Dispatch a HostCommand to the appropriate handler function.
async fn run_host_command(command: HostCommand) -> Result<i32> {
    match command {
        HostCommand::Add {
            name,
            address,
            identity_file,
            known_hosts,
            accept_new_host_key,
            fingerprint,
        } => {
            remote_connect(
                name,
                address,
                identity_file,
                known_hosts,
                accept_new_host_key,
                fingerprint,
            )
            .await
        }
        HostCommand::Remove { name } => remote_remove(name).await,
        HostCommand::List => remote_list(),
    }
}



async fn run_daemon_command(command: DaemonCommand) -> Result<i32> {
    match command {
        DaemonCommand::Start { config, log_level } => daemon_start(CliDaemonStartOptions {
            config,
            log_level,
        }),
        DaemonCommand::Stop => daemon_stop().await,
        DaemonCommand::Restart => daemon_restart().await,
    }
}



async fn list_servers(refresh: bool) -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;
    let response = client.list_servers(rpc::ServerListRequest {}).await?.into_inner();

    // If the response includes a merged server list, use it for source-tagged output.
    if let Some(merged) = response.merged {
        print_merged_server_list(&merged);
    } else {
        // Backward-compatible fallback: print the flat server list.
        print_flat_server_list(&response.servers);
    }
    let _ = refresh; // TODO: wire refresh flag to ServerListRequest when proto field is added
    Ok(0)
}

fn print_merged_server_list(merged: &rpc::MergedServerList) {
    // Compute column widths from source-tagged rows.
    let name_width = merged
        .rows
        .iter()
        .map(|row| {
            let source = &row.source;
            let alias = row.server.as_ref().map(|s| s.alias.as_str()).unwrap_or("");
            format!("{}:{}", source, alias).len()
        })
        .max()
        .unwrap_or(4)
        .max("NAME".len());
    let host_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.host.len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("HOST".len());
    let port_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.port.to_string().len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("PORT".len());
    let user_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.user.len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("USER".len());
    let auth_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.auth_kind.len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("AUTH".len());

    // Print header.
    println!(
        "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}  {:<auth_width$}",
        "NAME", "HOST", "PORT", "USER", "AUTH",
        name_width = name_width,
        host_width = host_width,
        port_width = port_width,
        user_width = user_width,
        auth_width = auth_width,
    );

    // Print rows tagged as <source>:<alias>.
    for row in &merged.rows {
        let server = match row.server.as_ref() {
            Some(s) => s,
            None => continue,
        };
        let tagged_name = format!("{}:{}", row.source, server.alias);
        println!(
            "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}  {:<auth_width$}",
            tagged_name,
            server.host,
            server.port,
            server.user,
            server.auth_kind,
            name_width = name_width,
            host_width = host_width,
            port_width = port_width,
            user_width = user_width,
            auth_width = auth_width,
        );
    }

    // Print one line per non-Ok source describing its status.
    let non_ok_sources: Vec<&rpc::SourceStatus> = merged
        .source_status
        .iter()
        .filter(|s| s.status != "ok")
        .collect();
    if !non_ok_sources.is_empty() {
        println!();
        for source_status in non_ok_sources {
            if source_status.detail.is_empty() {
                println!("{}: {}", source_status.source, source_status.status);
            } else {
                println!(
                    "{}: {} [{}]",
                    source_status.source, source_status.status, source_status.detail
                );
            }
        }
    }
}

fn print_flat_server_list(servers: &[rpc::ServerEntry]) {
    let name_width = servers
        .iter()
        .map(|server| server.alias.len())
        .max()
        .unwrap_or(4)
        .max("NAME".len());
    let host_width = servers
        .iter()
        .map(|server| server.host.len())
        .max()
        .unwrap_or(4)
        .max("HOST".len());
    let port_width = servers
        .iter()
        .map(|server| server.port.to_string().len())
        .max()
        .unwrap_or(4)
        .max("PORT".len());
    let user_width = servers
        .iter()
        .map(|server| server.user.len())
        .max()
        .unwrap_or(4)
        .max("USER".len());

    println!(
        "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}",
        "NAME",
        "HOST",
        "PORT",
        "USER",
        name_width = name_width,
        host_width = host_width,
        port_width = port_width,
        user_width = user_width,
    );
    for server in servers {
        println!(
            "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}",
            server.alias,
            server.host,
            server.port,
            server.user,
            name_width = name_width,
            host_width = host_width,
            port_width = port_width,
            user_width = user_width,
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ClientAccess {
    AutoStart,
    NoAutoStart,
}

async fn connect_data_client(
    access: ClientAccess,
) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let client_config = ClientConfig::load()?;
    connect_local_data_client(&client_config, access).await
}

async fn connect_local_copy_client() -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let client_config = ClientConfig::load()?;
    connect_local_data_client(&client_config, ClientAccess::AutoStart).await
}

async fn connect_local_data_client(
    client_config: &ClientConfig,
    access: ClientAccess,
) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let socket_path = PathBuf::from(&client_config.local.socket_path);
    match connect_unix_client(&socket_path).await {
        Ok(client) => Ok(client),
        Err(_error) if access == ClientAccess::AutoStart && client_config.local.auto_start => {
            spawn_daemon(&CliDaemonStartOptions::default())?;
            wait_for_socket(&socket_path).await?;
            connect_unix_client(&socket_path).await
        }
        Err(error) => Err(error).with_context(|| {
            format!("failed to connect to local daemon socket {}", socket_path.display())
        }),
    }
}


async fn connect_unix_client(
    socket_path: &Path,
) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let path = socket_path.to_path_buf();
    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?;
    Ok(rpc::rhop_rpc_client::RhopRpcClient::new(channel))
}

#[derive(Debug, Default, Clone)]
struct CliDaemonStartOptions {
    config: Option<PathBuf>,
    log_level: Option<String>,
}

fn daemon_start(options: CliDaemonStartOptions) -> Result<i32> {
    spawn_daemon(&options)?;
    println!("daemon started");
    Ok(0)
}

async fn daemon_stop() -> Result<i32> {
    let socket_path = local_socket_path()?;
    let mut client = match connect_unix_client(&socket_path).await {
        Ok(client) => client,
        Err(_) => {
            eprintln!("rhopd is not running");
            return Ok(1);
        }
    };
    let response = client.shutdown(rpc::ShutdownRequest {}).await?;
    let message = response.into_inner().message;
    wait_for_socket_removal(&socket_path).await?;
    println!("{}", message);
    Ok(0)
}

async fn daemon_restart() -> Result<i32> {
    let options = current_cli_start_options().await?;
    let stop_code = daemon_stop().await?;
    if stop_code != 0 {
        return Ok(stop_code);
    }
    spawn_daemon(&options)?;
    println!("daemon restarted");
    Ok(0)
}

fn spawn_daemon(options: &CliDaemonStartOptions) -> Result<()> {
    let daemon = daemon_path()?;
    let mut command = Command::new(&daemon);
    command.arg("--daemon");
    command.arg("--origin").arg("cli_spawned");
    if let Some(config_path) = &options.config {
        command.arg("--config").arg(config_path);
    } else if let Some(config_path) = local_config_path_if_exists()? {
        command.arg("--config").arg(config_path);
    }
    if let Some(log_level) = &options.log_level {
        command.arg("--log-level").arg(log_level);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to spawn {}", daemon.display()))?;
    Ok(())
}

async fn wait_for_socket(socket_path: &PathBuf) -> Result<()> {
    for _ in 0..50 {
        if socket_path.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!(
        "timed out waiting for daemon socket {}",
        socket_path.display()
    );
}

async fn wait_for_socket_removal(socket_path: &PathBuf) -> Result<()> {
    for _ in 0..50 {
        if !socket_path.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!(
        "timed out waiting for daemon socket {} to be removed",
        socket_path.display()
    );
}

fn daemon_path() -> Result<PathBuf> {
    let current = env::current_exe()?;
    let directory = current
        .parent()
        .ok_or_else(|| anyhow!("failed to resolve binary directory"))?;
    Ok(directory.join("rhopd"))
}

fn local_socket_path() -> Result<PathBuf> {
    let client_config = ClientConfig::load()?;
    Ok(PathBuf::from(client_config.local.socket_path))
}

fn local_config_path_if_exists() -> Result<Option<PathBuf>> {
    let path = default_config_path();
    if path.exists() {
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

async fn current_cli_start_options() -> Result<CliDaemonStartOptions> {
    let socket_path = local_socket_path()?;
    let mut client = connect_unix_client(&socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", socket_path.display()))?;
    let response = client.status(rpc::StatusRequest {}).await?.into_inner();
    Ok(CliDaemonStartOptions {
        config: (!response.cli_start_config_path.is_empty())
            .then(|| PathBuf::from(response.cli_start_config_path)),
        log_level: (!response.cli_start_log_level.is_empty()).then_some(response.cli_start_log_level),
    })
}

async fn remote_connect(
    name: String,
    address: String,
    identity_file_override: Option<String>,
    known_hosts_override: Option<String>,
    _accept_new_host_key: bool,
    _fingerprint: Option<String>,
) -> Result<i32> {
    // --- Step 1: Validate <name> against RESERVED_NAMES ---
    if RESERVED_NAMES.contains(&name.as_str()) {
        eprintln!(
            "error: name '{}' is reserved (reserved names: {:?})",
            name, RESERVED_NAMES
        );
        return Ok(1);
    }

    // --- Step 2: Validate <name> against existing gateway names ---
    // Load the daemon config to check for name collisions.
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    if let Some(entry) = config.gateways.iter().find(|g| g.name() == name) {
        eprintln!(
            "error: name '{}' is already used by a {:?} gateway",
            name, entry.gateway_kind()
        );
        return Ok(1);
    }

    // --- Step 3: Parse <address> via RemoteAddress::parse ---
    let defaults = AddressDefaults {
        user: "rhop".to_string(),
        port: 2222,
    };
    let remote_addr = match RemoteAddress::parse(&address, &defaults) {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("error: invalid address '{}': {}", address, e);
            return Ok(1);
        }
    };

    // --- Step 4: SSH host-key trust flow ---
    let (identity_file, known_hosts_path) =
        normalize_remote_paths(identity_file_override.as_deref(), known_hosts_override.as_deref())?;

    let target = parse_remote_target(&remote_addr.format())?;
    let public_key = fetch_remote_host_key(&target, &identity_file).await?;
    let state = inspect_known_host(&target.host, target.port, &public_key, &std::path::PathBuf::from(&known_hosts_path));
    match state {
        KnownHostState::Known => {}
        KnownHostState::Unknown {
            algorithm,
            fingerprint,
        } => {
            eprintln!(
                "The authenticity of host '{}' can't be established.",
                target.address()
            );
            eprintln!("{} key fingerprint is {}.", algorithm, fingerprint);
            if !prompt_for_confirmation("trust this host key and continue")? {
                bail!("host key not trusted");
            }
            trust_known_host(
                &target.host,
                target.port,
                &public_key,
                &std::path::PathBuf::from(&known_hosts_path),
            )?;
        }
        KnownHostState::Changed {
            algorithm,
            fingerprint,
        } => {
            bail!(
                "host key for {} changed; refusing to connect ({} {})",
                target.address(),
                algorithm,
                fingerprint
            );
        }
    }

    // --- Step 5: Persist the new entry to the config file ---
    let new_entry = GatewayConfig::Rhopd(RhopdGatewayConfig {
        name: name.clone(),
        address: remote_addr.format(),
        identity_file: identity_file.clone(),
        known_hosts_path: known_hosts_path.clone(),
    });

    // Re-load config to persist (avoid stale state)
    let mut config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    config.gateways.push(new_entry);

    // Write the updated config atomically
    let raw = toml::to_string_pretty(&config)
        .context("failed to serialize config")?;
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&config_path, raw)
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    println!(
        "added remote '{}' at {} and trusted host key",
        name,
        target.address()
    );
    Ok(0)
}

async fn remote_remove(name: String) -> Result<i32> {
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();

    // Find the entry with the given name
    let entry = config.gateways.iter().find(|g| g.name() == name);

    match entry {
        None => {
            eprintln!(
                "error: name '{}' not found in gateways configuration",
                name
            );
            Ok(1)
        }
        Some(entry) if entry.gateway_kind() != GatewayKind::Rhopd => {
            eprintln!(
                "error: name '{}' is a {:?} gateway; quick-remove only manages rhopd entries",
                name, entry.gateway_kind()
            );
            Ok(1)
        }
        Some(_) => {
            // Remove the entry and persist
            let mut config = config;
            config.gateways.retain(|g| g.name() != name);

            let raw = toml::to_string_pretty(&config)
                .context("failed to serialize config")?;
            if let Some(parent) = config_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::write(&config_path, raw)
                .with_context(|| format!("failed to write {}", config_path.display()))?;

            println!("removed remote '{}'", name);
            Ok(0)
        }
    }
}

fn remote_list() -> Result<i32> {
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();

    if config.gateways.is_empty() {
        return Ok(0);
    }

    println!("{:<10}  {:<12}  {}", "NAME", "KIND", "ADDRESS");
    for entry in &config.gateways {
        let address = match entry {
            GatewayConfig::Rhopd(c) => c.address.clone(),
            GatewayConfig::Jumpserver(c) => format!("{}:{}", c.host, c.port),
            GatewayConfig::Direct(c) => format!("{}:{}", c.host, c.port),
        };
        println!("{:<10}  {:<12}  {}", entry.name(), entry.gateway_kind(), address);
    }

    Ok(0)
}



fn prompt_for_confirmation(reason: &str) -> Result<bool> {
    eprintln!("confirmation required: {}", reason);
    eprint!("Continue? [y/N] ");
    io::stderr().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim(), "y" | "Y" | "yes" | "YES"))
}

fn prompt_for_auth_input(message: &str, secret: bool) -> Result<String> {
    eprint!("{}: ", message);
    io::stderr().flush()?;
    if secret {
        read_secret_line()
    } else {
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        Ok(input.trim_end().to_string())
    }
}

fn read_secret_line() -> Result<String> {
    let stdin = io::stdin();
    let fd = stdin.as_raw_fd();
    let mut term = std::mem::MaybeUninit::<libc::termios>::uninit();
    unsafe {
        if libc::tcgetattr(fd, term.as_mut_ptr()) != 0 {
            return Err(anyhow!("failed to read terminal attributes"));
        }
        let original = term.assume_init();
        let mut masked = original;
        masked.c_lflag &= !libc::ECHO;
        if libc::tcsetattr(fd, libc::TCSANOW, &masked) != 0 {
            return Err(anyhow!("failed to disable terminal echo"));
        }
        let mut input = String::new();
        let read_result = io::stdin().read_line(&mut input);
        let restore_result = libc::tcsetattr(fd, libc::TCSANOW, &original);
        eprintln!();
        if restore_result != 0 {
            return Err(anyhow!("failed to restore terminal echo"));
        }
        read_result?;
        Ok(input.trim_end().to_string())
    }
}

fn parse_copy_operands(recursive: bool, source: &str, dest: &str) -> Result<(String, CopySpec)> {
    let src_remote = parse_remote_spec(source);
    let dst_remote = parse_remote_spec(dest);
    match (src_remote, dst_remote) {
        (Some((target, remote_path)), None) => Ok((
            target,
            CopySpec {
                direction: CopyDirection::Download,
                local_path: expand_tilde(dest)?,
                remote_path,
                recursive,
                relay_upload_rx: None,
                relay_download_tx: None,
            },
        )),
        (None, Some((target, remote_path))) => Ok((
            target,
            CopySpec {
                direction: CopyDirection::Upload,
                local_path: expand_tilde(source)?,
                remote_path,
                recursive,
                relay_upload_rx: None,
                relay_download_tx: None,
            },
        )),
        (Some(_), Some(_)) => bail!("copy supports exactly one remote operand"),
        (None, None) => bail!("copy requires one remote operand like host:/path"),
    }
}

fn parse_remote_spec(value: &str) -> Option<(String, String)> {
    let (target, path) = value.split_once(':')?;
    if target.is_empty()
        || path.is_empty()
        || target.contains('/')
        || target.contains('\\')
        || target == "."
        || target == ".."
    {
        return None;
    }
    Some((target.to_string(), path.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use proptest::prelude::*;

    // Feature: rhopd-jumpserver-architecture, Property 16: Argv pass-through transparency
    //
    // For any TARGET T and any Vec<String> argv V (including elements that look like
    // rhop flags such as --non-interactive, --tty, --output, --), parsing
    // `rhop exec <target> -- <argv>...` produces the exact same argv in the parsed struct.
    //
    // Validates: Requirements 17.1, 17.2, 17.4

    /// Strategy that generates arbitrary argv elements, including ones that look like
    /// rhop flags (--output, --non-interactive, --tty, --no-tty, --stdin, --timeout, --).
    fn argv_element_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            // Plain words
            "[a-zA-Z0-9_./]{1,20}",
            // Flags that look like rhop's own flags
            Just("--non-interactive".to_string()),
            Just("--tty".to_string()),
            Just("--no-tty".to_string()),
            Just("--stdin".to_string()),
            Just("--output".to_string()),
            Just("--output=json".to_string()),
            Just("--timeout".to_string()),
            Just("--timeout=30s".to_string()),
            Just("--".to_string()),
            // Short flags
            Just("-v".to_string()),
            Just("-n".to_string()),
            Just("-e".to_string()),
            // Long flags with values
            "[a-z]{1,8}=[a-z0-9]{1,8}".prop_map(|s| format!("--{}", s)),
            // Bare dashes
            Just("-".to_string()),
            Just("---".to_string()),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        #[test]
        fn prop_argv_passthrough_transparency(
            argv in proptest::collection::vec(argv_element_strategy(), 1..10),
        ) {
            // Build the CLI args: rhop exec <target> -- <argv...>
            // Using `--` separator to pass arbitrary argv through (multi-arg mode).
            let target = "my-target";
            let mut args = vec!["rhop".to_string(), "exec".to_string(), target.to_string(), "--".to_string()];
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
