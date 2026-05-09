use clap::Parser;
use std::path::PathBuf;
use std::process::{Command, Stdio};

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
}

#[tokio::main]
async fn main() {
    let cli = RhopdCli::parse();
    if cli.daemon {
        if let Err(error) = spawn_background(cli.config.clone(), cli.log_level.clone()) {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(error) = rhop::daemon::run_with_overrides(cli.config.clone(), cli.log_level).await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn spawn_background(config: Option<PathBuf>, log_level: Option<String>) -> anyhow::Result<()> {
    let current = std::env::current_exe()?;
    let mut command = Command::new(current);
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    if let Some(log_level) = log_level {
        command.arg("--log-level").arg(log_level);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(())
}
