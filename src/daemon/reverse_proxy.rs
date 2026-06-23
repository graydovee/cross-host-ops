// Reverse proxy registry — manages dynamic gateways from reverse proxy nodes.
//
// When a node xhod (without a public IP) connects to this server xhod via
// the `xho-reverse` SSH subsystem, it registers here as a dynamic gateway.
// The registered gateway wraps a pre-established gRPC client over the SSH
// channel from the node.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use russh::ChannelStream;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{info, warn};

use crate::daemon::connection::xhod::XhodConnection;
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest, InteractiveRequest as ConnInteractiveRequest,
};
use crate::daemon::gateway::{
    ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle, InteractiveRequest,
    is_transport_error,
};
use crate::daemon::rpc::prefix_source;
use crate::daemon::ssh_server::ReverseProxyHandshake;
use crate::protocol::{self, ServerListRow, rpc};
use crate::types::{CopySpec, ServerListSource};

/// Subsystem name for reverse proxy connections.
pub const REVERSE_SUBSYSTEM_NAME: &str = "xho-reverse";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Metadata about a connected reverse proxy node.
#[derive(Clone, Debug)]
pub struct ReverseProxyNodeInfo {
    pub name: String,
    pub peer_addr: Option<SocketAddr>,
    pub fingerprint: String,
    pub connected_at: SystemTime,
    /// Hostname reported by the node (from list_servers).
    pub hostname: String,
    /// Execution user reported by the node.
    pub user: String,
}

/// A registered reverse proxy node: its gateway + metadata.
struct RegisteredNode {
    gateway: Arc<dyn Gateway>,
    info: ReverseProxyNodeInfo,
}

/// Snapshot of a node's status for RPC responses.
#[derive(Clone, Debug)]
pub struct ReverseProxyNodeStatus {
    pub name: String,
    pub peer_addr: String,
    pub fingerprint: String,
    pub connected_at: u64,
    pub hostname: String,
    pub user: String,
}

// ---------------------------------------------------------------------------
// ReverseProxyRegistry
// ---------------------------------------------------------------------------

/// Thread-safe registry of dynamically registered reverse proxy nodes.
///
/// Each node connects via the `xho-reverse` SSH subsystem and registers
/// under a unique name. The name becomes a dynamic gateway name usable
/// in target strings (e.g. `node-2:web01`).
pub struct ReverseProxyRegistry {
    inner: Arc<RwLock<HashMap<String, RegisteredNode>>>,
}

impl ReverseProxyRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns `true` if a node with this name is currently registered.
    pub async fn contains(&self, name: &str) -> bool {
        self.inner.read().await.contains_key(name)
    }

    /// Look up a registered node's gateway by name.
    pub async fn get(&self, name: &str) -> Option<Arc<dyn Gateway>> {
        self.inner.read().await.get(name).map(|n| n.gateway.clone())
    }

    /// List all registered node names.
    pub async fn list_names(&self) -> Vec<String> {
        self.inner.read().await.keys().cloned().collect()
    }

    /// List all registered nodes with their status info.
    pub async fn list_nodes(&self) -> Vec<ReverseProxyNodeStatus> {
        self.inner
            .read()
            .await
            .values()
            .map(|n| ReverseProxyNodeStatus {
                name: n.info.name.clone(),
                peer_addr: n.info.peer_addr.map(|a| a.to_string()).unwrap_or_default(),
                fingerprint: n.info.fingerprint.clone(),
                connected_at: n
                    .info
                    .connected_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                hostname: n.info.hostname.clone(),
                user: n.info.user.clone(),
            })
            .collect()
    }

    /// Try to register a node. Returns an error if the name is already taken.
    pub async fn register(
        &self,
        name: &str,
        gateway: Arc<dyn Gateway>,
        info: ReverseProxyNodeInfo,
    ) -> Result<()> {
        let mut map = self.inner.write().await;
        if map.contains_key(name) {
            bail!(
                "reverse proxy node name '{}' is already registered; refusing new connection",
                name
            );
        }
        info!(node = %name, "registered reverse proxy node");
        map.insert(name.to_string(), RegisteredNode { gateway, info });
        Ok(())
    }

    /// Unregister a node by name (e.g. after connection drop or CLI disconnect).
    pub async fn unregister(&self, name: &str) {
        let mut map = self.inner.write().await;
        if map.remove(name).is_some() {
            info!(node = %name, "unregistered reverse proxy node");
        }
    }

    /// Update a node's hostname and user from health check data.
    pub async fn update_node_info(&self, name: &str, hostname: String, user: String) {
        if let Some(node) = self.inner.write().await.get_mut(name) {
            if !hostname.is_empty() {
                node.info.hostname = hostname;
            }
            if !user.is_empty() {
                node.info.user = user;
            }
        }
    }

    /// Number of registered nodes.
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}

impl Default for ReverseProxyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Handshake processing
// ---------------------------------------------------------------------------

/// Process a reverse proxy connection on the server side.
///
/// The node name is parsed from the SSH subsystem name (`xho-reverse:<name>`).
/// The channel stream goes directly to gRPC — no text handshake.
/// 3. Send ack (`{"status":"ok"}\n` or error).
/// 4. Create a gRPC client over the channel.
/// 5. Register the node in the registry.
///
/// This function blocks until the connection ends (the gRPC client stays
/// alive as long as the channel is open). When the channel drops, the
/// caller should `unregister` the node.
pub(super) async fn process_reverse_handshake(
    registry: &Arc<ReverseProxyRegistry>,
    handshake: ReverseProxyHandshake,
) -> Result<()> {
    let ReverseProxyHandshake {
        stream,
        node_name,
        info,
    } = handshake;

    // Node name was parsed from the subsystem name: "xho-reverse:<name>".
    if node_name.is_empty() {
        bail!("reverse proxy node name is empty (subsystem must be xho-reverse:<name>)");
    }

    // Check for name conflict before proceeding.
    if registry.contains(&node_name).await {
        bail!(
            "reverse proxy node name '{}' is already registered; refusing new connection",
            node_name
        );
    }

    info!(node = %node_name, "reverse proxy handshake accepted");

    // Stream goes directly to gRPC — no text handshake, no BufReader.

    // Create a tonic gRPC client over the SSH channel stream (lazy connection).
    let client = create_grpc_client_over_stream(stream);

    // Clone for health monitoring (tonic Channel is cheaply cloneable).
    let health_client = client.clone();

    // Create the gateway wrapping this gRPC client.
    let gateway = Arc::new(ReverseProxyGateway::new(
        node_name.clone(),
        client,
        registry.clone(),
    ));

    let node_info = ReverseProxyNodeInfo {
        name: node_name.clone(),
        peer_addr: info.peer_addr,
        fingerprint: info.public_key_fingerprint,
        connected_at: SystemTime::now(),
        hostname: String::new(),
        user: String::new(),
    };

    // Register the node.
    registry.register(&node_name, gateway, node_info).await?;

    // Health monitoring: periodically poll the node. When the SSH channel
    // breaks, the RPC will fail and we deregister the node. The first poll
    // also collects the node's hostname and user from list_servers.
    let monitor_registry = registry.clone();
    let monitor_name = node_name.clone();
    let mut first_check = true;
    loop {
        if !first_check {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        }
        first_check = false;
        if !monitor_registry.contains(&monitor_name).await {
            break;
        }
        let mut hc_request = tonic::Request::new(rpc::ServerListRequest {});
        hc_request
            .metadata_mut()
            .insert("xho-no-recurse", "true".parse().unwrap());
        let result = health_client.clone().list_servers(hc_request).await;
        match result {
            Ok(resp) => {
                // Parse node info from the _self entry in the response.
                if let Some(ref merged) = resp.into_inner().merged {
                    if let Some(row) = merged
                        .rows
                        .iter()
                        .find(|r| r.server.as_ref().is_some_and(|s| s.alias == "_self"))
                    {
                        if let Some(srv) = &row.server {
                            monitor_registry
                                .update_node_info(&monitor_name, srv.host.clone(), srv.user.clone())
                                .await;
                        }
                    }
                }
            }
            Err(_) => {
                info!(
                    node = %monitor_name,
                    "reverse proxy health check failed; deregistering"
                );
                monitor_registry.unregister(&monitor_name).await;
                break;
            }
        }
    }

    Ok(())
}

/// Create a tonic gRPC client (`XhoRpcClient<Channel>`) over an SSH channel
/// stream. Uses `connect_with_connector_lazy` so the HTTP/2 handshake is
/// deferred to the first RPC — avoids timing issues where the client-side
/// tonic server hasn't started yet.
fn create_grpc_client_over_stream(
    stream: ChannelStream<russh::server::Msg>,
) -> rpc::xho_rpc_client::XhoRpcClient<Channel> {
    let io = TokioIo::new(stream);

    let cell = Arc::new(tokio::sync::Mutex::new(Some(io)));
    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel = endpoint.connect_with_connector_lazy(service_fn(move |_: Uri| {
        let cell = cell.clone();
        async move {
            match cell.lock().await.take() {
                Some(io) => Ok(io),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "reverse proxy stream already consumed",
                )),
            }
        }
    }));
    rpc::xho_rpc_client::XhoRpcClient::new(channel)
}

// ---------------------------------------------------------------------------
// ReverseProxyGateway — wraps a pre-established gRPC client as a Gateway
// ---------------------------------------------------------------------------

/// A Gateway backed by a reverse proxy connection.
///
/// Unlike `XhodGateway` (which initiates SSH connections), this gateway
/// wraps a gRPC client that was created over a node-initiated SSH channel.
/// On transport errors, it triggers deregistration from the registry
/// instead of attempting to reconnect.
pub struct ReverseProxyGateway {
    name: String,
    /// The pre-established gRPC client. Wrapped in a mutex so it can be
    /// taken/cloned for each operation.
    client: tokio::sync::Mutex<rpc::xho_rpc_client::XhoRpcClient<Channel>>,
    registry: Arc<ReverseProxyRegistry>,
}

impl ReverseProxyGateway {
    pub fn new(
        name: String,
        client: rpc::xho_rpc_client::XhoRpcClient<Channel>,
        registry: Arc<ReverseProxyRegistry>,
    ) -> Self {
        Self {
            name,
            client: tokio::sync::Mutex::new(client),
            registry,
        }
    }

    /// On a transport error, deregister this node from the registry.
    async fn handle_transport_error(&self) {
        warn!(
            node = %self.name,
            "transport error on reverse proxy gateway; deregistering node"
        );
        self.registry.unregister(&self.name).await;
    }
}

#[async_trait]
impl Gateway for ReverseProxyGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let client = {
            let guard = self.client.lock().await;
            (*guard).clone()
        };

        let mut conn = XhodConnection::new(client, target.to_string());

        let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());

        let mut conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            tty: request.tty,
            cols: request.cols,
            rows: request.rows,
            shell: request.shell.clone(),
            no_shell: request.no_shell,
            timeout_ms: request.timeout_ms,
            stdin: request.stdin,
            stdin_intent: request.stdin_intent,
            stdin_rx,
        };

        let result = conn.exec(&mut conn_request).await;

        match result {
            Ok(exit_code) => Ok(exit_code),
            Err(e) if is_transport_error(&e) => {
                self.handle_transport_error().await;
                Err(GatewayError::transport(e))
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn copy(&self, target: &str, spec: CopySpec) -> Result<(), GatewayError> {
        let client = {
            let guard = self.client.lock().await;
            (*guard).clone()
        };

        let mut conn = XhodConnection::new(client, target.to_string());

        let result = conn.copy(spec).await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if is_transport_error(&e) => {
                self.handle_transport_error().await;
                Err(GatewayError::transport(e))
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let client = {
            let guard = self.client.lock().await;
            (*guard).clone()
        };

        let mut conn = XhodConnection::new(client, target.to_string());
        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: request.shell.clone(),
            no_shell: request.no_shell,
        };

        let result = conn.exec_interactive(&conn_request).await;

        match result {
            Ok(handle) => Ok(InteractiveHandle {
                stdin_tx: handle.stdin_tx,
                resize_tx: handle.resize_tx,
                stdout_rx: handle.stdout_rx,
                exit_rx: handle.exit_rx,
                abort_handles: handle.abort_handles,
            }),
            Err(e) if is_transport_error(&e) => {
                self.handle_transport_error().await;
                Err(GatewayError::transport(e))
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let mut client = {
            let guard = self.client.lock().await;
            (*guard).clone()
        };

        let mut request = tonic::Request::new(rpc::ServerListRequest {});
        request
            .metadata_mut()
            .insert("xho-no-recurse", "true".parse().unwrap());

        let response = client
            .list_servers(request)
            .await
            .map_err(|e| GatewayError::transport(anyhow!("list_servers RPC failed: {}", e)))?
            .into_inner();

        // Prefer merged.rows if available and non-empty.
        if let Some(ref merged) = response.merged {
            if !merged.rows.is_empty() {
                let rows = merged
                    .rows
                    .iter()
                    .filter_map(|rpc_row| {
                        let server = protocol::server_entry_from_rpc(rpc_row.server.clone()?);
                        let source = prefix_source(&self.name, &rpc_row.source);
                        Some(ServerListRow { source, server })
                    })
                    .collect();
                return Ok(rows);
            }
        }

        // Fallback: flat servers field.
        let rows = response
            .servers
            .into_iter()
            .map(|s| {
                let server = protocol::server_entry_from_rpc(s);
                ServerListRow {
                    source: ServerListSource::Gateway(self.name.clone()),
                    server,
                }
            })
            .collect();
        Ok(rows)
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::ReverseProxy
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn prune_idle(&self) {
        // No-op: the reverse proxy connection lifetime is managed by the
        // SSH channel, not by idle pruning.
    }
}
