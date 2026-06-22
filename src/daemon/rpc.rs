// Daemon RPC handler module.
// Contains process_execute, process_copy, and process_list_servers functions
// that dispatch to the Gateway-based architecture.

use std::time::Duration;

use anyhow::{Result, anyhow};
use tracing::warn;

use crate::config::{DirectAuth, ServerEntry, load_server_config};
use crate::protocol::ServerListSourceStatus;
use crate::types::{CopySpec, ServerListSource};

use super::DaemonState;
use super::gateway::{ErrorKind, ExecRequest, GatewayKind};
use super::resolver::Resolver;

/// Timeout for each gateway's list_servers call.
const LIST_SERVERS_TIMEOUT: Duration = Duration::from_secs(5);

/// Apply path prefix logic to a remote source string.
pub fn prefix_source(gateway_name: &str, remote_source: &str) -> ServerListSource {
    if remote_source == "local" || remote_source.is_empty() {
        ServerListSource::Gateway(gateway_name.to_string())
    } else {
        ServerListSource::Gateway(format!("{}:{}", gateway_name, remote_source))
    }
}

/// Process an execute request by resolving routes and iterating candidates.
pub async fn process_execute(
    state: &DaemonState,
    target: &str,
    request: &ExecRequest,
) -> Result<i32> {
    let config = state.config.read().await.clone();
    let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
        .unwrap_or_default();
    let dynamic_names = state.reverse_proxy_registry.list_names().await;
    let resolver = Resolver::new(&config, &server_config, &config.gateways)
        .with_dynamic_gateways(&dynamic_names);
    let routes = resolver.resolve(target)?;

    let mut last_error: Option<anyhow::Error> = None;

    for route in &routes {
        let gateway = state
            .find_gateway_any(&route.gateway_name)
            .await
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
pub async fn process_copy(state: &DaemonState, target: &str, spec: CopySpec) -> Result<()> {
    let config = state.config.read().await.clone();
    let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
        .unwrap_or_default();
    let dynamic_names = state.reverse_proxy_registry.list_names().await;
    let resolver = Resolver::new(&config, &server_config, &config.gateways)
        .with_dynamic_gateways(&dynamic_names);
    let routes = resolver.resolve(target)?;

    let mut last_error: Option<anyhow::Error> = None;

    for route in &routes {
        let gateway = state
            .find_gateway_any(&route.gateway_name)
            .await
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

/// Process a list_servers request by iterating all gateways, merging results.
///
/// `no_recurse`: when true (set by XhodGateway/ReverseProxyGateway outgoing
/// calls), skip forward XhodGateway connections to prevent recursive loops.
/// Reverse proxy gateways are always queried — they propagate the flag on
/// their own outgoing calls, so the receiving side skips its forward gateways.
pub async fn process_list_servers(
    state: &DaemonState,
    no_recurse: bool,
) -> (
    Vec<(ServerEntry, ServerListSource)>,
    Vec<(ServerListSource, ServerListSourceStatus)>,
) {
    let mut results: Vec<(ServerEntry, ServerListSource)> = Vec::new();
    let mut source_status: Vec<(ServerListSource, ServerListSourceStatus)> = Vec::new();

    let make_source = |name: &str| {
        if name == "local" {
            ServerListSource::Local
        } else {
            ServerListSource::Gateway(name.to_string())
        }
    };

    // Iterate static gateways (config declaration order).
    for (name, gateway) in &state.gateways {
        // When no_recurse is set, skip forward XhodGateway connections.
        // They would call back to a remote that has a reverse proxy to us,
        // creating an infinite loop.
        if no_recurse && gateway.kind() == GatewayKind::Xhod {
            continue;
        }

        let source_tag = make_source(name);
        match tokio::time::timeout(LIST_SERVERS_TIMEOUT, gateway.list_servers()).await {
            Ok(Ok(rows)) => {
                for row in rows {
                    results.push((row.server, row.source));
                }
                source_status.push((source_tag, ServerListSourceStatus::Ok));
            }
            Ok(Err(e)) if e.kind == ErrorKind::Unsupported => {
                source_status.push((source_tag, ServerListSourceStatus::Unsupported));
            }
            Ok(Err(e)) => {
                warn!(gateway = name.as_str(), error = %e, "list_servers failed");
                source_status.push((source_tag, ServerListSourceStatus::Error(e.to_string())));
            }
            Err(_) => {
                warn!(gateway = name.as_str(), "list_servers timed out");
                source_status
                    .push((source_tag, ServerListSourceStatus::Error("timeout".to_string())));
            }
        }
    }

    // Iterate dynamic reverse proxy gateways (always — they propagate no_recurse).
    let rp_names = state.reverse_proxy_registry.list_names().await;
    for name in &rp_names {
        let source_tag = ServerListSource::Gateway(name.clone());

        // Synthetic entry: the node itself, visible with HOST = <None>.
        results.push((
            ServerEntry {
                alias: name.clone(),
                host: "<None>".to_string(),
                port: 0,
                user: String::new(),
                auth: DirectAuth::None,
            },
            // Use Local source so the XhodGateway prefix yields just
            // "ali-xhod" (not "ali-xhod:dev-local"), giving display
            // name "ali-xhod:dev-local" instead of doubled.
            ServerListSource::Local,
        ));

        if let Some(gateway) = state.reverse_proxy_registry.get(name).await {
            match tokio::time::timeout(LIST_SERVERS_TIMEOUT, gateway.list_servers()).await {
                Ok(Ok(rows)) => {
                    for row in rows {
                        results.push((row.server, row.source));
                    }
                    source_status.push((source_tag, ServerListSourceStatus::Ok));
                }
                Ok(Err(e)) => {
                    warn!(gateway = name.as_str(), error = %e, "reverse proxy list_servers failed");
                    source_status
                        .push((source_tag, ServerListSourceStatus::Error(e.to_string())));
                }
                Err(_) => {
                    warn!(gateway = name.as_str(), "reverse proxy list_servers timed out");
                    source_status
                        .push((source_tag, ServerListSourceStatus::Error("timeout".to_string())));
                }
            }
        } else {
            source_status.push((source_tag, ServerListSourceStatus::Ok));
        }
    }

    (results, source_status)
}
