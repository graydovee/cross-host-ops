use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result, anyhow};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;

type FmtLayer = fmt::Layer<
    tracing_subscriber::Registry,
    fmt::format::DefaultFields,
    fmt::format::Format,
    BoxMakeWriter,
>;
type FilteredFmtLayer =
    tracing_subscriber::filter::Filtered<FmtLayer, LevelFilter, tracing_subscriber::Registry>;
type ReloadHandle = reload::Handle<FilteredFmtLayer, tracing_subscriber::Registry>;

struct LoggingState {
    handle: ReloadHandle,
    guard: Option<WorkerGuard>,
    log_path: Option<PathBuf>,
    log_level: Level,
}

static LOGGING_STATE: OnceLock<Mutex<LoggingState>> = OnceLock::new();

pub fn init_logging(log_path: Option<String>, log_level: &str) -> Result<Option<WorkerGuard>> {
    let level = parse_level(log_level)?;
    let (writer, guard, normalized_path) = build_writer(log_path)?;

    let layer = fmt::layer()
        .with_target(false)
        .with_ansi(normalized_path.is_none())
        .with_writer(writer)
        .with_filter(LevelFilter::from_level(level));
    let (layer, handle) = reload::Layer::new(layer);

    tracing_subscriber::registry()
        .with(layer)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;

    let mut returned_guard = guard;
    let state = LoggingState {
        handle,
        guard: returned_guard.take(),
        log_path: normalized_path,
        log_level: level,
    };
    let _ = LOGGING_STATE.set(Mutex::new(state));
    Ok(returned_guard)
}

pub fn reopen_log_output() -> Result<()> {
    let state = LOGGING_STATE
        .get()
        .ok_or_else(|| anyhow!("logging is not initialized"))?;
    let mut state = state
        .lock()
        .map_err(|_| anyhow!("logging state mutex is poisoned"))?;

    let (writer, guard, normalized_path) = build_writer(
        state
            .log_path
            .as_ref()
            .map(|path| path.display().to_string()),
    )?;

    state
        .handle
        .modify(|layer| {
            *layer.inner_mut().writer_mut() = writer;
            layer.inner_mut().set_ansi(normalized_path.is_none());
            *layer.filter_mut() = LevelFilter::from_level(state.log_level);
        })
        .map_err(|error| anyhow!("failed to reload logging writer: {error}"))?;

    state.guard = guard;
    state.log_path = normalized_path;
    Ok(())
}

fn build_writer(
    log_path: Option<String>,
) -> Result<(BoxMakeWriter, Option<WorkerGuard>, Option<PathBuf>)> {
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
        return Ok((BoxMakeWriter::new(writer), Some(guard), Some(path)));
    }

    Ok((BoxMakeWriter::new(std::io::stdout), None, None))
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
