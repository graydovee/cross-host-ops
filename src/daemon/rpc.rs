// Daemon RPC handler module.
// Contains process_execute, process_copy, and process_list_servers functions
// that dispatch to the Gateway-based architecture.

use anyhow::{Result, anyhow};
use tracing::warn;

use crate::config::{ServerEntry, load_server_config};
use crate::connection::CopySpec;
use crate::jump::ServerListSource;
use crate::protocol::ServerListSourceStatus;

use super::gateway::{ErrorKind, ExecRequest};
use super::gateway_daemon::GatewayDaemonState;
use super::resolver::Resolver;

/// Process an execute request by resolving routes and iterating candidates.
///
/// Multi-candidate fallback logic:
/// - Resolution error → continue to next candidate
/// - Execution/Transport error → return immediately
/// - All candidates fail → return the last error
pub async fn process_execute(
    state: &GatewayDaemonState,
    target: &str,
    request: &ExecRequest,
) -> Result<i32> {
    let config = state.config.read().await.clone();
    let server_config =
        load_server_config(std::path::Path::new(&config.ssh.server_config_path))
            .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.jump_hosts);
    let routes = resolver.resolve(target)?;

    let mut last_error: Option<anyhow::Error> = None;

    for route in &routes {
        let gateway = state
            .gateways
            .get(&route.gateway_name)
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
    state: &GatewayDaemonState,
    target: &str,
    spec: &CopySpec,
) -> Result<()> {
    let config = state.config.read().await.clone();
    let server_config =
        load_server_config(std::path::Path::new(&config.ssh.server_config_path))
            .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.jump_hosts);
    let routes = resolver.resolve(target)?;

    let mut last_error: Option<anyhow::Error> = None;

    for route in &routes {
        let gateway = state
            .gateways
            .get(&route.gateway_name)
            .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;

        match gateway.copy(&route.end_target, spec).await {
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
/// Returns a tuple of (server entries, source status) for building the RPC response.
pub async fn process_list_servers(
    state: &GatewayDaemonState,
) -> (Vec<ServerEntry>, Vec<(ServerListSource, ServerListSourceStatus)>) {
    let mut results: Vec<ServerEntry> = Vec::new();
    let mut source_status: Vec<(ServerListSource, ServerListSourceStatus)> = Vec::new();

    for (name, gateway) in &state.gateways {
        let source = if name == "local" {
            ServerListSource::Local
        } else {
            ServerListSource::JumpHost(name.clone())
        };

        match gateway.list_servers().await {
            Ok(entries) => {
                results.extend(entries);
                source_status.push((source, ServerListSourceStatus::Ok));
            }
            Err(e) if e.kind == ErrorKind::Unsupported => {
                source_status.push((source, ServerListSourceStatus::Unsupported));
                continue;
            }
            Err(e) => {
                warn!(
                    gateway = name.as_str(),
                    error = %e,
                    "list_servers failed"
                );
                source_status.push((source, ServerListSourceStatus::Error(e.to_string())));
                continue;
            }
        }
    }

    (results, source_status)
}


