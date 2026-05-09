use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
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

use crate::config::AppConfig;
use crate::connection::{CopyDirection, CopySpec};
use crate::protocol::rpc;

#[derive(Debug, Parser)]
#[command(name = "rhop")]
#[command(about = "Remote Hop command runner with a local daemon", version)]
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
    #[command(name = "daemon-start")]
    #[command(about = "Start the daemon in background mode")]
    DaemonStart,
    #[command(name = "reload-config")]
    #[command(about = "Reload daemon configuration")]
    ReloadConfig,
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
        ArunCommand::DaemonStart => daemon_start(),
        ArunCommand::ReloadConfig => reload_config().await,
    }
}

async fn run_command(target: String, argv: Vec<String>) -> Result<i32> {
    let socket_path = socket_path()?;
    let mut client = match connect_client(&socket_path).await {
        Ok(client) => client,
        Err(_) => {
            spawn_daemon()?;
            wait_for_socket(&socket_path).await?;
            connect_client(&socket_path).await?
        }
    };

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
    let socket_path = socket_path()?;
    let mut client = match connect_client(&socket_path).await {
        Ok(client) => client,
        Err(_) => {
            eprintln!("rhopd is not running");
            return Ok(1);
        }
    };
    let response = client.status(rpc::StatusRequest {}).await?.into_inner();
    println!("socket: {}", response.socket_path);
    println!("active executions: {}", response.active_executions);
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
    let socket_path = socket_path()?;
    let mut client = match connect_client(&socket_path).await {
        Ok(client) => client,
        Err(_) => {
            spawn_daemon()?;
            wait_for_socket(&socket_path).await?;
            connect_client(&socket_path).await?
        }
    };
    let request = crate::protocol::copy_spec_to_rpc(target, spec);
    let response = client.copy(request).await?.into_inner();
    if !response.message.is_empty() {
        println!("{}", response.message);
    }
    Ok(0)
}

async fn reload_config() -> Result<i32> {
    let socket_path = socket_path()?;
    let mut client = connect_client(&socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", socket_path.display()))?;
    let response = client.reload_config(rpc::ReloadConfigRequest {}).await?;
    println!("{}", response.into_inner().message);
    Ok(0)
}

async fn connect_client(
    socket_path: &PathBuf,
) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let path = socket_path.clone();
    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?;
    Ok(rpc::rhop_rpc_client::RhopRpcClient::new(channel))
}

fn daemon_start() -> Result<i32> {
    spawn_daemon()?;
    Ok(0)
}

fn spawn_daemon() -> Result<()> {
    let daemon = daemon_path()?;
    let mut command = Command::new(&daemon);
    command.arg("--daemon");
    if let Some(config_path) = local_config_path_if_exists()? {
        command.arg("--config").arg(config_path);
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

fn daemon_path() -> Result<PathBuf> {
    let current = env::current_exe()?;
    let directory = current
        .parent()
        .ok_or_else(|| anyhow!("failed to resolve binary directory"))?;
    Ok(directory.join("rhopd"))
}

fn socket_path() -> Result<PathBuf> {
    let mut config = AppConfig::load(None)?;
    config.expand_paths()?;
    Ok(PathBuf::from(config.server.socket_path))
}

fn local_config_path_if_exists() -> Result<Option<PathBuf>> {
    let path = crate::config::default_config_path();
    if path.exists() {
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

fn prompt_for_confirmation(reason: &str) -> Result<bool> {
    eprintln!("command requires confirmation: {}", reason);
    eprint!("Continue? [y/N] ");
    io::stderr().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim(), "y" | "Y" | "yes" | "YES"))
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
