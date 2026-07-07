use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::time::sleep;

use crate::config::{ClientConfig, default_config_path};
use crate::protocol::rpc;

use super::args::DaemonCommand;
use super::local_conn::{LocalEndpoint, connect};

pub(crate) async fn run_daemon_command(command: DaemonCommand) -> Result<i32> {
    match command {
        DaemonCommand::Start { config, log_level } => {
            daemon_start(CliDaemonStartOptions { config, log_level })
        }
        DaemonCommand::Stop => daemon_stop().await,
        DaemonCommand::Restart => daemon_restart().await,
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CliDaemonStartOptions {
    config: Option<PathBuf>,
    log_level: Option<String>,
}

fn daemon_start(options: CliDaemonStartOptions) -> Result<i32> {
    spawn_daemon(&options)?;
    println!("daemon started");
    Ok(0)
}

async fn daemon_stop() -> Result<i32> {
    let endpoint = local_endpoint()?;
    let mut client = match connect(&endpoint).await {
        Ok(client) => client,
        Err(_) => {
            eprintln!("xhod is not running");
            return Ok(1);
        }
    };
    let response = client.shutdown(rpc::ShutdownRequest {}).await?;
    let message = response.into_inner().message;
    wait_for_endpoint_removal(&endpoint).await?;
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

pub(crate) fn spawn_daemon(options: &CliDaemonStartOptions) -> Result<()> {
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
    // On Windows, detach from the parent's console window.
    #[cfg(windows)]
    {
        // CREATE_NO_WINDOW = 0x08000000
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to spawn {}", daemon.display()))?;
    Ok(())
}

/// Poll until the daemon endpoint's on-disk marker is gone (socket file for
/// Unix transport, lock file for TCP), used after `shutdown` to confirm exit.
async fn wait_for_endpoint_removal(endpoint: &LocalEndpoint) -> Result<()> {
    let marker = endpoint_marker_path(endpoint);
    for _ in 0..50 {
        if !marker.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!(
        "timed out waiting for daemon endpoint {} to be removed",
        marker.display()
    );
}

/// Filesystem path whose presence/absence signals daemon liveness.
fn endpoint_marker_path(endpoint: &LocalEndpoint) -> &std::path::Path {
    match endpoint {
        #[cfg(unix)]
        LocalEndpoint::Unix(p) => p,
        LocalEndpoint::Tcp(p) => p,
    }
}

fn daemon_path() -> Result<PathBuf> {
    let current = env::current_exe()?;
    let directory = current
        .parent()
        .ok_or_else(|| anyhow!("failed to resolve binary directory"))?;
    Ok(directory.join("xhod"))
}

fn local_endpoint() -> Result<LocalEndpoint> {
    let client_config = ClientConfig::load()?;
    LocalEndpoint::from_config(&client_config)
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
    let endpoint = local_endpoint()?;
    let mut client = connect(&endpoint)
        .await
        .with_context(|| format!("failed to connect to {}", endpoint.describe_internal()))?;
    let response = client.status(rpc::StatusRequest {}).await?.into_inner();
    Ok(CliDaemonStartOptions {
        config: (!response.cli_start_config_path.is_empty())
            .then(|| PathBuf::from(response.cli_start_config_path)),
        log_level: (!response.cli_start_log_level.is_empty())
            .then_some(response.cli_start_log_level),
    })
}
