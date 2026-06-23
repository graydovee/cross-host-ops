use std::env;

use anyhow::Result;

use crate::protocol::rpc;

use super::client::{ClientAccess, connect_data_client};

/// Emit a JSON object describing the binary's version, capabilities, and exit codes.
///
/// Called when `xho --version --output json` is invoked.
pub fn print_version_json() {
    let version_info = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": [
            "exec",
            "cp",
            "status",
            "ls",
            "host.add",
            "host.remove",
            "host.list",
            "daemon.start",
            "daemon.stop",
            "daemon.restart"
        ],
        "exit_codes": {
            "0": "success",
            "1-123": "remote command exit code",
            "124": "operation timed out",
            "125": "xho or daemon failure",
            "126": "auth/host-key/review denied",
            "127": "target not found / unsupported capability"
        }
    });
    println!("{}", serde_json::to_string_pretty(&version_info).unwrap());
}

pub(crate) async fn status() -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::NoAutoStart).await?;
    let response = client.status(rpc::StatusRequest {}).await?.into_inner();
    println!("daemon:");
    println!("  origin: {}", response.daemon_origin);
    println!("  cli_controllable: {}", response.cli_controllable);
    println!("  active_executions: {}", response.active_executions);
    if !response.cli_start_config_path.is_empty() {
        println!(
            "  cli_start_config_path: {}",
            response.cli_start_config_path
        );
    }
    if !response.cli_start_log_level.is_empty() {
        println!("  cli_start_log_level: {}", response.cli_start_log_level);
    }
    if response.remote_listening {
        println!("remote:");
        println!("  listening: {}", response.remote_addr);
        println!("  user: {}", response.remote_ssh_user);
    }

    // Print gateways from the daemon's StatusResponse.
    if !response.gateways.is_empty() {
        println!("gateways:");
        for jh in &response.gateways {
            println!("  - name: {}", jh.name);
            println!("    kind: {}", jh.kind);
            println!("    address: {}", jh.address);
            if let Some(sub) = &jh.sub_status {
                println!("    sub_status:");
                println!("      daemon_running: {}", sub.daemon_running);
                println!("      active_executions: {}", sub.active_executions);
                if !sub.pools.is_empty() {
                    println!("      pools:");
                    for pool in &sub.pools {
                        println!(
                            "        {} total={} busy={} idle={} queued={}",
                            pool.key, pool.total, pool.busy, pool.idle, pool.queued
                        );
                    }
                }
            }
        }
    }

    // Reverse proxy server info.
    if response.reverse_proxy_server_enabled {
        println!("reverse_proxy_server:");
        println!("  enabled: true");
        if !response.reverse_proxy_nodes.is_empty() {
            println!("  nodes:");
            for node in &response.reverse_proxy_nodes {
                println!("    - name: {}", node.name);
                if !node.peer_addr.is_empty() {
                    println!("      peer: {}", node.peer_addr);
                }
                if !node.fingerprint.is_empty() {
                    println!("      key: {}", node.fingerprint);
                }
            }
        }
    }

    // Reverse proxy client info.
    if response.reverse_proxy_client_enabled {
        println!("reverse_proxy_client:");
        println!("  target: {}", response.reverse_proxy_client_target);
        println!("  status: {}", response.reverse_proxy_client_status);
    }

    if !response.pools.is_empty() {
        println!("pools:");
        for pool in response.pools {
            println!(
                "  {} total={} busy={} idle={} queued={}",
                pool.key, pool.total, pool.busy, pool.idle, pool.queued
            );
        }
    }
    Ok(0)
}

pub(crate) async fn list_servers(refresh: bool) -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;
    let response = client
        .list_servers(rpc::ServerListRequest {})
        .await?
        .into_inner();

    // If the response includes a merged server list, use it for source-tagged output.
    if let Some(merged) = response.merged {
        print_merged_server_list(&merged);
    } else {
        // Backward-compatible fallback: print the flat server list.
        print_flat_server_list(&response.servers);
    }
    let _ = refresh; // TODO: wire refresh flag to ServerListRequest when proto field is added
    Ok(0)
}

fn print_merged_server_list(merged: &rpc::MergedServerList) {
    // Compute the display name for a row: local entries show bare alias,
    // gateway entries show <source>:<alias>.
    let display_name = |source: &str, alias: &str| -> String {
        if source == "local" {
            alias.to_string()
        } else {
            format!("{}:{}", source, alias)
        }
    };

    // Compute column widths from display names.
    let name_width = merged
        .rows
        .iter()
        .map(|row| {
            let alias = row.server.as_ref().map(|s| s.alias.as_str()).unwrap_or("");
            display_name(&row.source, alias).len()
        })
        .max()
        .unwrap_or(4)
        .max("NAME".len());
    let host_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.host.len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("HOST".len());
    let port_width = merged
        .rows
        .iter()
        .map(|row| {
            row.server
                .as_ref()
                .map(|s| s.port.to_string().len())
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(4)
        .max("PORT".len());
    let user_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.user.len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("USER".len());
    let auth_width = merged
        .rows
        .iter()
        .map(|row| row.server.as_ref().map(|s| s.auth_kind.len()).unwrap_or(0))
        .max()
        .unwrap_or(4)
        .max("AUTH".len());

    // Print header.
    println!(
        "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}  {:<auth_width$}",
        "NAME",
        "HOST",
        "PORT",
        "USER",
        "AUTH",
        name_width = name_width,
        host_width = host_width,
        port_width = port_width,
        user_width = user_width,
        auth_width = auth_width,
    );

    // Print rows — local entries show bare alias, gateway entries show <source>:<alias>.
    // Skip _self entries (internal, not for display).
    for row in &merged.rows {
        let server = match row.server.as_ref() {
            Some(s) => s,
            None => continue,
        };
        if server.alias == "_self" {
            continue;
        }
        let tagged_name = display_name(&row.source, &server.alias);
        println!(
            "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}  {:<auth_width$}",
            tagged_name,
            server.host,
            server.port,
            server.user,
            server.auth_kind,
            name_width = name_width,
            host_width = host_width,
            port_width = port_width,
            user_width = user_width,
            auth_width = auth_width,
        );
    }

    // Skip non-Ok source status output — unsupported gateways (e.g. jumpserver)
    // are expected and don't need to clutter the listing.
}

fn print_flat_server_list(servers: &[rpc::ServerEntry]) {
    let name_width = servers
        .iter()
        .map(|server| server.alias.len())
        .max()
        .unwrap_or(4)
        .max("NAME".len());
    let host_width = servers
        .iter()
        .map(|server| server.host.len())
        .max()
        .unwrap_or(4)
        .max("HOST".len());
    let port_width = servers
        .iter()
        .map(|server| server.port.to_string().len())
        .max()
        .unwrap_or(4)
        .max("PORT".len());
    let user_width = servers
        .iter()
        .map(|server| server.user.len())
        .max()
        .unwrap_or(4)
        .max("USER".len());

    println!(
        "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}",
        "NAME",
        "HOST",
        "PORT",
        "USER",
        name_width = name_width,
        host_width = host_width,
        port_width = port_width,
        user_width = user_width,
    );
    for server in servers {
        println!(
            "{:<name_width$}  {:<host_width$}  {:<port_width$}  {:<user_width$}",
            server.alias,
            server.host,
            server.port,
            server.user,
            name_width = name_width,
            host_width = host_width,
            port_width = port_width,
            user_width = user_width,
        );
    }
}
