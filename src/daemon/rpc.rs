// Daemon RPC handler module.
// Contains process_list_servers (the gateway list aggregator). The old
// route-dispatch process_execute/process_copy were removed once Execute/Copy
// RPC handlers drove the unified TargetSession abstraction directly.

use std::time::Duration;

use tracing::warn;

use crate::config::{DirectAuth, ServerEntry};
use crate::protocol::ServerListSourceStatus;
use crate::types::ServerListSource;

use super::DaemonState;
use super::gateway::{Capabilities, ErrorKind, ListServersOptions};

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

/// Process a list_servers request by iterating all gateways, merging results.
///
/// `no_recurse`: when true, pass the recursion context into each gateway so
/// forwarding backends can suppress loops inside their own implementation.
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
        // Skip gateways that don't declare LIST (e.g. Jumpserver bastion) —
        // calling list_servers would just return Unsupported, so we never
        // surface it to the client in the first place.
        if !gateway.capabilities().contains(Capabilities::LIST) {
            continue;
        }

        let source_tag = make_source(name);
        match tokio::time::timeout(
            LIST_SERVERS_TIMEOUT,
            gateway.list_servers_with_options(ListServersOptions { no_recurse }),
        )
        .await
        {
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
                source_status.push((
                    source_tag,
                    ServerListSourceStatus::Error("timeout".to_string()),
                ));
            }
        }
    }

    // Iterate dynamic reverse proxy gateways (always — they propagate no_recurse).
    let rp_nodes = state.reverse_proxy_registry.list_nodes().await;
    for node in &rp_nodes {
        let source_tag = ServerListSource::Gateway(node.name.clone());

        // Synthetic entry with real node info (hostname, user from health check).
        results.push((
            ServerEntry {
                alias: node.name.clone(),
                host: if node.hostname.is_empty() {
                    node.name.clone()
                } else {
                    node.hostname.clone()
                },
                port: 0,
                user: node.user.clone(),
                auth: DirectAuth::ReverseProxy,
            },
            ServerListSource::Local,
        ));

        if let Some(gateway) = state.reverse_proxy_registry.get(&node.name).await {
            match tokio::time::timeout(
                LIST_SERVERS_TIMEOUT,
                gateway.list_servers_with_options(ListServersOptions { no_recurse }),
            )
            .await
            {
                Ok(Ok(rows)) => {
                    for row in rows {
                        results.push((row.server, row.source));
                    }
                    source_status.push((source_tag, ServerListSourceStatus::Ok));
                }
                Ok(Err(e)) => {
                    warn!(gateway = node.name.as_str(), error = %e, "reverse proxy list_servers failed");
                    source_status.push((source_tag, ServerListSourceStatus::Error(e.to_string())));
                }
                Err(_) => {
                    warn!(
                        gateway = node.name.as_str(),
                        "reverse proxy list_servers timed out"
                    );
                    source_status.push((
                        source_tag,
                        ServerListSourceStatus::Error("timeout".to_string()),
                    ));
                }
            }
        } else {
            source_status.push((source_tag, ServerListSourceStatus::Ok));
        }
    }

    (results, source_status)
}
