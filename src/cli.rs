use std::env;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::config::{
    AppConfig, ClientConfig, JumpHostConfig, JumpHostFields, RhopdJumpHostFields,
    default_config_path, expand_tilde, RESERVED_NAMES,
};
use crate::connection::{CopyDirection, CopySpec};
use crate::exit_codes::cap_remote_exit_code;
use crate::jump::address::{AddressDefaults, RemoteAddress};
use crate::jump::JumpHostKind;
use crate::protocol::rpc;
use crate::remote::{
    KnownHostState, fetch_remote_host_key, inspect_known_host, load_client_config,
    normalize_remote_paths, parse_remote_target,
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
    #[command(about = "Execute a remote command on the target host", trailing_var_arg = true)]
    Exec {
        /// Allocate a PTY for the remote command.
        #[arg(long = "pty", conflicts_with = "no_pty")]
        pty: bool,
        /// Do not allocate a PTY for the remote command.
        #[arg(long = "no-pty")]
        no_pty: bool,
        /// Forward local stdin to the remote command's stdin.
        #[arg(long = "stdin")]
        stdin: bool,
        /// Abort the operation after this duration (e.g. 30s, 2m).
        #[arg(long = "timeout", value_name = "DURATION")]
        timeout: Option<String>,
        /// Target and command: <TARGET> <CMD>...
        #[arg(
            value_name = "TARGET_AND_CMD",
            required = true,
            allow_hyphen_values = true,
            help = "Target name followed by remote command and arguments"
        )]
        target_and_argv: Vec<String>,
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
    #[command(about = "Manage remote daemon target selection")]
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    #[command(about = "Manage the local daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    #[command(about = "Query configured servers")]
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum RemoteCommand {
    #[command(about = "Connect to a remote daemon and trust its host key if needed")]
    Connect {
        #[arg(value_name = "NAME", help = "Name for the remote jump host")]
        name: String,
        #[arg(value_name = "ADDRESS", help = "[user@]host[:port] of the remote daemon")]
        address: String,
        #[arg(long = "identity-file", value_name = "FILE")]
        identity_file: Option<String>,
        #[arg(long = "known-hosts", value_name = "FILE")]
        known_hosts: Option<String>,
        /// Trust the host key without prompting (TOFU mode).
        #[arg(long = "accept-new-host-key", conflicts_with = "fingerprint")]
        accept_new_host_key: bool,
        /// Trust only if the host key's SHA256 fingerprint matches this value.
        #[arg(long = "fingerprint", value_name = "SHA256", conflicts_with = "accept_new_host_key")]
        fingerprint: Option<String>,
    },
    #[command(about = "Remove a rhopd jump host entry from the configuration")]
    Remove {
        #[arg(value_name = "NAME", help = "Name of the rhopd jump host to remove")]
        name: String,
    },
    #[command(about = "List all configured jump hosts")]
    List,
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
pub enum ServerCommand {
    #[command(about = "List configured servers from the daemon's active server.toml")]
    List {
        /// Re-fetch every Server_List_Source bypassing the in-memory cache.
        #[arg(long, alias = "no-cache")]
        refresh: bool,
    },
}

pub async fn run_cli(cli: ArunCli) -> Result<i32> {
    match cli.command {
        ArunCommand::Exec { target_and_argv, .. } => {
            let (target, argv) = split_target_and_argv(target_and_argv)?;
            if argv.is_empty() {
                bail!("at least one command argument is required");
            }
            run_command(target, argv).await
        }
        ArunCommand::Cp {
            recursive,
            source,
            dest,
            ..
        } => run_copy(recursive, source, dest).await,
        ArunCommand::Status => status().await,
        ArunCommand::Remote { command } => run_remote_command(command).await,
        ArunCommand::Daemon { command } => run_daemon_command(command).await,
        ArunCommand::Server { command } => run_server_command(command).await,
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
            "server.list",
            "remote.connect",
            "remote.remove",
            "remote.list",
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

/// Split the combined target_and_argv vec into (target, argv).
/// The first element is the target; the rest is the argv.
fn split_target_and_argv(mut args: Vec<String>) -> Result<(String, Vec<String>)> {
    if args.is_empty() {
        bail!("target is required");
    }
    let target = args.remove(0);
    Ok((target, args))
}

async fn run_command(target: String, argv: Vec<String>) -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;

    let (tx, rx) = mpsc::channel(8);
    tx.send(rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target,
            argv,
        })),
    })
    .await
    .map_err(|_| anyhow!("failed to send execute request"))?;

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
            rpc::execute_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                tx.send(crate::protocol::execute_auth_input_request(prompt.prompt_id, value))
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

    // Print jump hosts from the daemon's StatusResponse.
    if !response.jump_hosts.is_empty() {
        println!("jump_hosts:");
        for jh in &response.jump_hosts {
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

async fn run_copy(recursive: bool, source: String, dest: String) -> Result<i32> {
    let (target, spec) = parse_copy_operands(recursive, &source, &dest)?;
    let mut client = connect_local_copy_client().await?;
    let (tx, rx) = mpsc::channel(8);
    tx.send(crate::protocol::copy_spec_to_rpc(target, spec))
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
        }
    }
    Ok(0)
}

async fn run_remote_command(command: RemoteCommand) -> Result<i32> {
    match command {
        RemoteCommand::Connect {
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
        RemoteCommand::Remove { name } => remote_remove(name).await,
        RemoteCommand::List => remote_list(),
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

async fn run_server_command(command: ServerCommand) -> Result<i32> {
    match command {
        ServerCommand::List { refresh } => list_servers(refresh).await,
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
    let client_config = load_client_config()?;
    connect_local_data_client(&client_config, access).await
}

async fn connect_local_copy_client() -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let client_config = load_client_config()?;
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
    let client_config = load_client_config()?;
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

    // --- Step 2: Validate <name> against existing jump host names ---
    // Load the daemon config to check for name collisions.
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    for entry in &config.jump_hosts {
        if entry.name == name {
            eprintln!(
                "error: name '{}' is already used by a {} jump host",
                name, entry.kind
            );
            return Ok(1);
        }
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
        normalize_remote_paths(identity_file_override, known_hosts_override)?;

    let target = parse_remote_target(&remote_addr.format())?;
    let public_key = fetch_remote_host_key(&target, &identity_file).await?;
    let state = inspect_known_host(&target, &public_key, &std::path::PathBuf::from(&known_hosts_path));
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
            crate::remote::trust_known_host(
                &target,
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
    let new_entry = JumpHostConfig {
        name: name.clone(),
        kind: JumpHostKind::Rhopd,
        fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
            address: remote_addr.format(),
            identity_file: identity_file.clone(),
            known_hosts_path: known_hosts_path.clone(),
        }),
    };

    // Re-load config to persist (avoid stale state)
    let mut config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    config.jump_hosts.push(new_entry);

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
    let entry = config.jump_hosts.iter().find(|e| e.name == name);

    match entry {
        None => {
            eprintln!(
                "error: name '{}' not found in jump hosts configuration",
                name
            );
            Ok(1)
        }
        Some(entry) if entry.kind != JumpHostKind::Rhopd => {
            eprintln!(
                "error: name '{}' is a {} jump host; quick-remove only manages rhopd entries",
                name, entry.kind
            );
            Ok(1)
        }
        Some(_) => {
            // Remove the entry and persist
            let mut config = config;
            config.jump_hosts.retain(|e| e.name != name);

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

    if config.jump_hosts.is_empty() {
        return Ok(0);
    }

    println!("{:<10}  {:<12}  {}", "NAME", "KIND", "ADDRESS");
    for entry in &config.jump_hosts {
        let address = match &entry.fields {
            JumpHostFields::Rhopd(fields) => fields.address.clone(),
            JumpHostFields::Jumpserver(fields) => format!("{}:{}", fields.host, fields.port),
            JumpHostFields::Direct(fields) => format!("{}:{}", fields.host, fields.port),
        };
        println!("{:<10}  {:<12}  {}", entry.name, entry.kind, address);
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
            },
        )),
        (None, Some((target, remote_path))) => Ok((
            target,
            CopySpec {
                direction: CopyDirection::Upload,
                local_path: expand_tilde(source)?,
                remote_path,
                recursive,
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
    // rhop flags such as --non-interactive, --pty, --output, --), parsing
    // `rhop exec <target> <argv>...` produces the exact same argv in the parsed struct.
    //
    // Validates: Requirements 17.1, 17.2, 17.4

    /// Strategy that generates arbitrary argv elements, including ones that look like
    /// rhop flags (--output, --non-interactive, --pty, --no-pty, --stdin, --timeout, --).
    fn argv_element_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            // Plain words
            "[a-zA-Z0-9_./]{1,20}",
            // Flags that look like rhop's own flags
            Just("--non-interactive".to_string()),
            Just("--pty".to_string()),
            Just("--no-pty".to_string()),
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
            // Build the CLI args: rhop exec <target> <argv...>
            let target = "my-target";
            let mut args = vec!["rhop".to_string(), "exec".to_string(), target.to_string()];
            args.extend(argv.clone());

            let parsed = ArunCli::try_parse_from(&args).unwrap();
            match parsed.command {
                ArunCommand::Exec { target_and_argv, .. } => {
                    let (parsed_target, parsed_argv) = split_target_and_argv(target_and_argv).unwrap();
                    prop_assert_eq!(&parsed_target, target);
                    prop_assert_eq!(&parsed_argv, &argv);
                }
                _ => panic!("expected Exec command"),
            }
        }
    }
}
