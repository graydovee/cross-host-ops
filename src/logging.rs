use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;

pub fn init_logging(log_path: Option<String>, log_level: &str) -> Result<Option<WorkerGuard>> {
    let level = parse_level(log_level)?;
    if let Some(path) = log_path.filter(|path| !path.trim().is_empty()) {
        let path = PathBuf::from(path);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("invalid log path {}", path.display()))?;
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow!("invalid log path {}", path.display()))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create log directory {}", parent.display()))?;
        let appender = tracing_appender::rolling::never(parent, file_name);
        let (writer, guard) = tracing_appender::non_blocking(appender);
        fmt()
            .with_max_level(level)
            .with_target(false)
            .with_writer(writer)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;
        return Ok(Some(guard));
    }

    fmt()
        .with_max_level(level)
        .with_target(false)
        .with_writer(std::io::stdout)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;
    Ok(None)
}

fn parse_level(value: &str) -> Result<Level> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(Level::TRACE),
        "debug" => Ok(Level::DEBUG),
        "info" => Ok(Level::INFO),
        "warn" => Ok(Level::WARN),
        "error" => Ok(Level::ERROR),
        other => Err(anyhow!("invalid log level {}", other)),
    }
}
