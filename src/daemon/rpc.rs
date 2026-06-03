// Daemon RPC handler module.
// Contains process_execute, process_copy, and process_list_servers functions
// that dispatch to the Gateway-based architecture.

use anyhow::{Result, anyhow};
use tracing::warn;

use crate::config::{ServerEntry, load_server_config};
use crate::types::CopySpec;
use crate::types::ServerListSource;
use crate::protocol::ServerListSourceStatus;


use super::gateway::{ErrorKind, ExecRequest};
use super::DaemonState;
use super::resolver::Resolver;

/// Apply path prefix logic to a remote source string.
///
/// Rules:
/// - remote_source == "local" || remote_source.is_empty() → ServerListSource::Gateway(gateway_name)
/// - otherwise → ServerListSource::Gateway(format!("{}:{}", gateway_name, remote_source))
pub fn prefix_source(gateway_name: &str, remote_source: &str) -> ServerListSource {
    if remote_source == "local" || remote_source.is_empty() {
        ServerListSource::Gateway(gateway_name.to_string())
    } else {
        ServerListSource::Gateway(format!("{}:{}", gateway_name, remote_source))
    }
}

/// Process an execute request by resolving routes and iterating candidates.
///
/// Multi-candidate fallback logic:
/// - Resolution error → continue to next candidate
/// - Execution/Transport error → return immediately
/// - All candidates fail → return the last error
pub async fn process_execute(
    state: &DaemonState,
    target: &str,
    request: &ExecRequest,
) -> Result<i32> {
    let config = state.config.read().await.clone();
    let server_config =
        load_server_config(std::path::Path::new(&config.ssh.server_config_path))
            .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.gateways);
    let routes = resolver.resolve(target)?;

    let mut last_error: Option<anyhow::Error> = None;

    for route in &routes {
        let gateway = state
            .find_gateway(&route.gateway_name)
            .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;

        match gateway.exec(&route.end_target, request).await {
            Ok(code) => return Ok(code),
            Err(e) if e.kind == ErrorKind::Resolution => {
                last_error = Some(e.into());
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no routes for target '{}'", target)))
}

/// Process a copy request by resolving routes and iterating candidates.
///
/// Same multi-candidate fallback logic as process_execute.
pub async fn process_copy(
    state: &DaemonState,
    target: &str,
    spec: CopySpec,
) -> Result<()> {
    let config = state.config.read().await.clone();
    let server_config =
        load_server_config(std::path::Path::new(&config.ssh.server_config_path))
            .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.gateways);
    let routes = resolver.resolve(target)?;

    let mut last_error: Option<anyhow::Error> = None;

    for route in &routes {
        let gateway = state
            .find_gateway(&route.gateway_name)
            .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;

        match gateway.copy(&route.end_target, spec.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) if e.kind == ErrorKind::Resolution => {
                last_error = Some(e.into());
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no routes for target '{}'", target)))
}

/// Process a list_servers request by iterating all gateways, merging results,
/// and skipping gateways that return Unsupported errors.
///
/// Returns a tuple of (rows with source tags, source status) for building the RPC response.
pub async fn process_list_servers(
    state: &DaemonState,
) -> (Vec<(ServerEntry, ServerListSource)>, Vec<(ServerListSource, ServerListSourceStatus)>) {
    let mut results: Vec<(ServerEntry, ServerListSource)> = Vec::new();
    let mut source_status: Vec<(ServerListSource, ServerListSourceStatus)> = Vec::new();

    // Iterate in Vec order (config declaration order)
    for (name, gateway) in &state.gateways {
        match gateway.list_servers().await {
            Ok(rows) => {
                let source_tag = if name == "local" {
                    ServerListSource::Local
                } else {
                    ServerListSource::Gateway(name.clone())
                };
                for row in rows {
                    // Use source directly from the row (gateway already tagged it)
                    results.push((row.server, row.source));
                }
                source_status.push((source_tag, ServerListSourceStatus::Ok));
            }
            Err(e) if e.kind == ErrorKind::Unsupported => {
                let source_tag = if name == "local" {
                    ServerListSource::Local
                } else {
                    ServerListSource::Gateway(name.clone())
                };
                source_status.push((source_tag, ServerListSourceStatus::Unsupported));
            }
            Err(e) => {
                let source_tag = if name == "local" {
                    ServerListSource::Local
                } else {
                    ServerListSource::Gateway(name.clone())
                };
                warn!(gateway = name.as_str(), error = %e, "list_servers failed");
                source_status.push((source_tag, ServerListSourceStatus::Error(e.to_string())));
            }
        }
    }

    (results, source_status)
}


