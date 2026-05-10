use std::env;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::config::{ClientConfig, ClientMode, default_config_path};
use crate::connection::{CopyDirection, CopySpec};
use crate::protocol::rpc;
use crate::remote::{
    KnownHostState, apply_remote_target, client_mode, connect_remote_client, disable_remote_mode,
    enable_remote_mode, fetch_remote_host_key, inspect_known_host, load_client_config,
    normalize_remote_paths, parse_remote_target, save_client_config,
};

#[derive(Debug, Parser)]
#[command(name = "rhop")]
#[command(about = "Remote Hop command runner with a local or remote daemon", version)]
pub struct ArunCli {
    #[command(subcommand)]
    pub command: ArunCommand,
}

#[derive(Debug, Subcommand)]
pub enum ArunCommand {
    #[command(about = "Execute a remote command on the target host")]
    Exec {
        #[arg(
            value_name = "TARGET",
            help = "Target name used to derive the remote IP"
        )]
        target: String,
        #[arg(
            value_name = "CMD",
            trailing_var_arg = true,
            allow_hyphen_values = true,
            help = "Remote command and arguments"
        )]
        argv: Vec<String>,
    },
    #[command(about = "Copy files between local and remote host")]
    Cp {
        #[arg(short = 'r', long = "recursive")]
        recursive: bool,
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
        #[arg(value_name = "TARGET")]
        target: String,
        #[arg(long = "identity-file", value_name = "FILE")]
        identity_file: Option<String>,
        #[arg(long = "known-hosts", value_name = "FILE")]
        known_hosts: Option<String>,
    },
    #[command(about = "Use the saved remote daemon as the default target")]
    Enable,
    #[command(about = "Switch the default target back to the local daemon")]
    Disable,
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
    List,
}

pub async fn run_cli(cli: ArunCli) -> Result<i32> {
    match cli.command {
        ArunCommand::Exec { target, argv } => {
            if argv.is_empty() {
                bail!("at least one command argument is required");
            }
            run_command(target, argv).await
        }
        ArunCommand::Cp {
            recursive,
            source,
            dest,
        } => run_copy(recursive, source, dest).await,
        ArunCommand::Status => status().await,
        ArunCommand::Remote { command } => run_remote_command(command).await,
        ArunCommand::Daemon { command } => run_daemon_command(command).await,
        ArunCommand::Server { command } => run_server_command(command).await,
    }
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

    Ok(exit_code)
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

    println!("local:");
    println!("  enabled: {}", response.local_enabled);
    if response.local_enabled {
        println!("  socket_path: {}", response.local_socket_path);
    }

    if response.remote_enabled {
        println!("remote:");
        println!("  enabled: true");
        if !response.remote_listen_addr.is_empty() {
            println!("  listen_addr: {}", response.remote_listen_addr);
        }
        if !response.remote_user.is_empty() {
            println!("  user: {}", response.remote_user);
        }
    }

    for pool in response.pools {
        println!(
            "{} total={} busy={} idle={} queued={}",
            pool.key, pool.total, pool.busy, pool.idle, pool.queued
        );
    }
    Ok(0)
}

async fn run_copy(recursive: bool, source: String, dest: String) -> Result<i32> {
    let (target, spec) = parse_copy_operands(recursive, &source, &dest)?;
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;
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
            target,
            identity_file,
            known_hosts,
        } => remote_connect(target, identity_file, known_hosts).await,
        RemoteCommand::Enable => remote_enable(),
        RemoteCommand::Disable => remote_disable(),
    }
}

async fn run_daemon_command(command: DaemonCommand) -> Result<i32> {
    ensure_local_mode("daemon commands")?;
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
        ServerCommand::List => list_servers().await,
    }
}

async fn list_servers() -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;
    let response = client.list_servers(rpc::ServerListRequest {}).await?.into_inner();
    let name_width = response
        .servers
        .iter()
        .map(|server| server.alias.len())
        .max()
        .unwrap_or(4)
        .max("NAME".len());
    let host_width = response
        .servers
        .iter()
        .map(|server| server.host.len())
        .max()
        .unwrap_or(4)
        .max("HOST".len());
    let port_width = response
        .servers
        .iter()
        .map(|server| server.port.to_string().len())
        .max()
        .unwrap_or(4)
        .max("PORT".len());
    let user_width = response
        .servers
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
    for server in response.servers {
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
    Ok(0)
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
    match client_mode(&client_config) {
        ClientMode::Local => connect_local_data_client(&client_config, access).await,
        ClientMode::Remote => connect_remote_data_client(&client_config, access).await,
    }
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

async fn connect_remote_data_client(
    client_config: &ClientConfig,
    access: ClientAccess,
) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    if access == ClientAccess::AutoStart {
        connect_remote_client(&client_config.remote).await
    } else {
        connect_remote_client(&client_config.remote).await
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
    target: String,
    identity_file_override: Option<String>,
    known_hosts_override: Option<String>,
) -> Result<i32> {
    let target = parse_remote_target(&target)?;
    let mut client_config = load_client_config()?;
    let (identity_file, known_hosts_path) =
        normalize_remote_paths(identity_file_override, known_hosts_override)?;
    let public_key = fetch_remote_host_key(&target, &identity_file).await?;
    let state = inspect_known_host(&target, &public_key, &PathBuf::from(&known_hosts_path));
    match state {
        KnownHostState::Known => {}
        KnownHostState::Unknown {
            algorithm,
            fingerprint,
        } => {
            eprintln!("The authenticity of host '{}' can't be established.", target.address());
            eprintln!("{} key fingerprint is {}.", algorithm, fingerprint);
            if !prompt_for_confirmation("trust this host key and continue")? {
                bail!("host key not trusted");
            }
            crate::remote::trust_known_host(
                &target,
                &public_key,
                &PathBuf::from(&known_hosts_path),
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

    apply_remote_target(&mut client_config, &target);
    client_config.remote.identity_file = identity_file;
    client_config.remote.known_hosts_path = known_hosts_path;
    save_client_config(&client_config)?;
    println!("saved remote daemon target {}", target.address());
    Ok(0)
}

fn remote_enable() -> Result<i32> {
    let mut client_config = load_client_config()?;
    enable_remote_mode(&mut client_config)?;
    save_client_config(&client_config)?;
    println!("default daemon target switched to remote");
    Ok(0)
}

fn remote_disable() -> Result<i32> {
    let mut client_config = load_client_config()?;
    disable_remote_mode(&mut client_config);
    save_client_config(&client_config)?;
    println!("default daemon target switched to local");
    Ok(0)
}

fn ensure_local_mode(what: &str) -> Result<()> {
    let client_config = load_client_config()?;
    if matches!(client_mode(&client_config), ClientMode::Remote) {
        bail!("{} are only supported in local mode; run `rhop remote disable` first", what);
    }
    Ok(())
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
                local_path: dest.to_string(),
                remote_path,
                recursive,
            },
        )),
        (None, Some((target, remote_path))) => Ok((
            target,
            CopySpec {
                direction: CopyDirection::Upload,
                local_path: source.to_string(),
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
