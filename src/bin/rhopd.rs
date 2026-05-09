use clap::Parser;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use rhop::daemon::{CliStartOptions, DaemonOrigin};

#[derive(Debug, Parser)]
#[command(name = "rhopd")]
#[command(about = "rhop daemon", version)]
struct RhopdCli {
    #[arg(short = 'c', long = "config", value_name = "FILE")]
    config: Option<PathBuf>,
    #[arg(long = "log-level", value_name = "LEVEL")]
    log_level: Option<String>,
    #[arg(long)]
    daemon: bool,
    #[arg(long = "origin", value_name = "ORIGIN", default_value = "external")]
    origin: String,
}

#[tokio::main]
async fn main() {
    let cli = RhopdCli::parse();
    let origin = parse_origin(&cli.origin);
    let cli_start_options = CliStartOptions {
        config_path: cli.config.as_ref().map(|value| value.display().to_string()),
        log_level: cli.log_level.clone(),
    };
    if cli.daemon {
        if let Err(error) = spawn_background(cli.config.clone(), cli.log_level.clone(), origin) {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(error) = rhop::daemon::run_with_overrides(
        cli.config.clone(),
        cli.log_level,
        origin,
        cli_start_options,
    )
    .await
    {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn spawn_background(
    config: Option<PathBuf>,
    log_level: Option<String>,
    origin: DaemonOrigin,
) -> anyhow::Result<()> {
    let current = std::env::current_exe()?;
    let mut command = Command::new(current);
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    if let Some(log_level) = log_level {
        command.arg("--log-level").arg(log_level);
    }
    command.arg("--origin").arg(origin.as_str());
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(())
}

fn parse_origin(value: &str) -> DaemonOrigin {
    match value {
        "cli_spawned" => DaemonOrigin::CliSpawned,
        _ => DaemonOrigin::External,
    }
}
