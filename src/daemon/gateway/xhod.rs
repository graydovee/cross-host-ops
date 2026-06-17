// XhodGateway implementation.
// Manages a single gRPC-over-SSH connection to a remote xhod daemon.
// All requests are multiplexed over the shared gRPC channel.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use russh::ChannelStream;
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{debug, info};

use crate::daemon::connection::xhod::XhodConnection;
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest, InteractiveRequest as ConnInteractiveRequest,
};
use crate::daemon::connection_manager::{ManagedSingleton, SingletonLease};
use crate::daemon::rpc::prefix_source;
use crate::daemon::ssh_server::remote_subsystem_name;
use crate::protocol::{self, ServerListRow, rpc};
use crate::types::CopySpec;
use crate::types::ServerListSource;

use super::auth::{AuthPrompter, ClientHandler, normalize_paths, parse_remote_target};
use super::{
    ErrorKind, ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle,
    InteractiveRequest, is_transport_error,
};

type XhoRpcClient = rpc::xho_rpc_client::XhoRpcClient<Channel>;
type XhoRpcStream = TokioIo<ChannelStream<client::Msg>>;

#[derive(Clone)]
struct XhodConnectorConfig {
    gateway_name: String,
    address: String,
    identity_file: String,
    known_hosts_path: String,
    auth_prompter: Arc<AuthPrompter>,
}

// ---------------------------------------------------------------------------
// XhodGateway
// ---------------------------------------------------------------------------

/// A Gateway that forwards operations to a remote xhod daemon over a
/// gRPC channel multiplexed on an SSH `xho-rpc` subsystem.
///
/// The gRPC client is lazily established on first use and shared across
/// all concurrent operations. On transport errors, the client is discarded
/// and re-established on the next operation.
pub struct XhodGateway {
    gateway_name: String,
    address: String,
    identity_file: String,
    known_hosts_path: String,
    auth_prompter: Arc<AuthPrompter>,
    max_idle_time: Duration,
    /// Single shared gRPC client (lazily connected).
    client: ManagedSingleton<XhoRpcClient>,
}

impl XhodGateway {
    /// Construct a new XhodGateway. No connections are established.
    pub fn new(
        gateway_name: String,
        address: String,
        identity_file: String,
        known_hosts_path: String,
        auth_prompter: Arc<AuthPrompter>,
        max_idle_time: Duration,
    ) -> Self {
        Self {
            gateway_name,
            address,
            identity_file,
            known_hosts_path,
            auth_prompter,
            max_idle_time,
            client: ManagedSingleton::new(),
        }
    }

    async fn ensure_client(&self) -> Result<SingletonLease<XhoRpcClient>, GatewayError> {
        for attempt in 0..=1 {
            let result = self
                .client
                .checkout_or_insert_with(|| async {
                    self.connect_client().await.map_err(GatewayError::transport)
                })
                .await;
            match result {
                Ok(lease) => return Ok(lease),
                Err(e) if attempt == 0 && matches!(e.kind, ErrorKind::Transport) => {
                    debug!(
                        gateway = %self.gateway_name,
                        "transport error creating xhod client, retrying: {}",
                        e
                    );
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("xhod client checkout loop is bounded")
    }

    fn connector_config(&self) -> XhodConnectorConfig {
        XhodConnectorConfig {
            gateway_name: self.gateway_name.clone(),
            address: self.address.clone(),
            identity_file: self.identity_file.clone(),
            known_hosts_path: self.known_hosts_path.clone(),
            auth_prompter: self.auth_prompter.clone(),
        }
    }

    /// Create a tonic gRPC client. The connector opens a fresh SSH
    /// `xho-rpc` subsystem every time tonic needs a new underlying transport.
    async fn connect_client(&self) -> Result<XhoRpcClient> {
        let connector_config = Arc::new(self.connector_config());

        let endpoint = Endpoint::from_static("http://[::]:50051");
        let tonic_channel: Channel = endpoint
            .connect_with_connector(service_fn(move |_: Uri| {
                let connector_config = connector_config.clone();
                async move { Self::open_rpc_stream(connector_config).await }
            }))
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to establish gRPC channel over SSH for {}: {}",
                    self.gateway_name,
                    e
                )
            })?;

        info!(
            gateway = %self.gateway_name,
            "gRPC-over-SSH channel established"
        );

        Ok(rpc::xho_rpc_client::XhoRpcClient::new(tonic_channel))
    }

    async fn open_rpc_stream(config: Arc<XhodConnectorConfig>) -> std::io::Result<XhoRpcStream> {
        Self::open_rpc_stream_inner(config)
            .await
            .map(TokioIo::new)
            .map_err(std::io::Error::other)
    }

    async fn open_rpc_stream_inner(
        config: Arc<XhodConnectorConfig>,
    ) -> Result<ChannelStream<client::Msg>> {
        // Parse address to get host, port, user.
        let target = parse_remote_target(&config.address)
            .map_err(|e| anyhow!("failed to parse xhod address {:?}: {}", config.address, e))?;

        // Normalize identity_file and known_hosts_path with defaults.
        let id_opt = if config.identity_file.is_empty() {
            None
        } else {
            Some(config.identity_file.as_str())
        };
        let kh_opt = if config.known_hosts_path.is_empty() {
            None
        } else {
            Some(config.known_hosts_path.as_str())
        };
        let (identity_file, _known_hosts_path) = normalize_paths(id_opt, kh_opt).map_err(|e| {
            anyhow!(
                "failed to normalize paths for {}: {}",
                config.gateway_name,
                e
            )
        })?;

        info!(
            gateway = %config.gateway_name,
            host = %target.host,
            port = %target.port,
            user = %target.user,
            "opening xho-rpc subsystem"
        );

        // Open SSH connection with the "xho" user from the parsed address
        // (parse_remote_target defaults to "xho" user if not specified).
        let client_config = client::Config::default();
        let mut handle = client::connect(
            Arc::new(client_config),
            (target.host.as_str(), target.port),
            ClientHandler,
        )
        .await
        .map_err(|e| {
            anyhow!(
                "SSH connection to {}:{} failed: {}",
                target.host,
                target.port,
                e
            )
        })?;

        // Authenticate with public key (user "xho"), AuthPrompter fallback.
        let auth_result = Self::authenticate_ssh(&mut handle, &target.user, &identity_file).await;

        if let Err(e) = auth_result {
            // Fallback: use AuthPrompter for password
            debug!(
                gateway = %config.gateway_name,
                "publickey auth failed, trying password via AuthPrompter: {}",
                e
            );
            let password = (config.auth_prompter)(super::auth::AuthPrompt {
                gateway_name: config.gateway_name.clone(),
                message: format!(
                    "Password for {}@{}:{}",
                    target.user, target.host, target.port
                ),
                secret: true,
            })
            .await
            .map_err(|e| {
                anyhow!(
                    "AuthPrompter failed for {}@{}:{}: {}",
                    target.user,
                    target.host,
                    target.port,
                    e
                )
            })?;

            super::auth::authenticate_with_password(&mut handle, &target.user, &password)
                .await
                .map_err(|e| {
                    anyhow!(
                        "password auth failed for {}@{}:{}: {}",
                        target.user,
                        target.host,
                        target.port,
                        e
                    )
                })?;
        }

        // Open session channel and request the "xho-rpc" subsystem.
        let ssh_channel = handle.channel_open_session().await.map_err(|e| {
            anyhow!(
                "failed to open session channel on {}:{}: {}",
                target.host,
                target.port,
                e
            )
        })?;
        ssh_channel
            .request_subsystem(true, remote_subsystem_name())
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to start xho-rpc subsystem on {}:{}: {}",
                    target.host,
                    target.port,
                    e
                )
            })?;

        Ok(ssh_channel.into_stream())
    }

    /// Attempt publickey authentication. Returns Ok(()) on success, Err on failure.
    async fn authenticate_ssh(
        handle: &mut client::Handle<ClientHandler>,
        user: &str,
        identity_file: &str,
    ) -> Result<()> {
        let key = load_secret_key(identity_file, None)
            .map_err(|e| anyhow!("failed to load key {}: {}", identity_file, e))?;
        let hash_alg = handle.best_supported_rsa_hash().await?.flatten();
        let auth = handle
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg))
            .await?;
        if auth.success() {
            Ok(())
        } else {
            Err(anyhow!(
                "publickey authentication failed for {} using {}",
                user,
                identity_file
            ))
        }
    }

    async fn invalidate_client(&self, generation: u64) {
        if self.client.invalidate_generation(generation).await {
            debug!(
                gateway = %self.gateway_name,
                generation = %generation,
                "discarded xhod gRPC client, will reconnect on next use"
            );
        }
    }

    async fn list_servers_with_client(
        &self,
        mut client: rpc::xho_rpc_client::XhoRpcClient<Channel>,
    ) -> Result<Vec<ServerListRow>, GatewayError> {
        let response = client
            .list_servers(rpc::ServerListRequest {})
            .await
            .map_err(|e| GatewayError::transport(anyhow!("list_servers RPC failed: {}", e)))?
            .into_inner();

        // Prefer merged.rows if available and non-empty
        if let Some(ref merged) = response.merged {
            if !merged.rows.is_empty() {
                let rows = merged
                    .rows
                    .iter()
                    .filter_map(|rpc_row| {
                        let server = protocol::server_entry_from_rpc(rpc_row.server.clone()?);
                        let source = prefix_source(&self.gateway_name, &rpc_row.source);
                        Some(ServerListRow { source, server })
                    })
                    .collect();
                return Ok(rows);
            }
        }

        // Fallback: use flat servers field, treat as source="local"
        let rows = response
            .servers
            .into_iter()
            .map(|s| {
                let server = protocol::server_entry_from_rpc(s);
                ServerListRow {
                    source: ServerListSource::Gateway(self.gateway_name.clone()),
                    server,
                }
            })
            .collect();
        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// Gateway trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Gateway for XhodGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let lease = self.ensure_client().await?;

        // Create a XhodConnection and delegate exec to it.
        let mut conn = XhodConnection::new((*lease.resource()).clone(), target.to_string());

        // Take stdin_rx from the gateway request (consuming it so the channel
        // is owned by the connection layer for forwarding).
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
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    generation = %lease.generation(),
                    "transport error on xhod exec; discarding client without replay: {}",
                    e
                );
                self.invalidate_client(lease.generation()).await;
                Err(GatewayError::transport(e))
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn copy(&self, target: &str, spec: CopySpec) -> Result<(), GatewayError> {
        let lease = self.ensure_client().await?;

        let mut conn = XhodConnection::new((*lease.resource()).clone(), target.to_string());

        let result = conn.copy(spec).await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    generation = %lease.generation(),
                    "transport error on xhod copy; discarding client without replay: {}",
                    e
                );
                self.invalidate_client(lease.generation()).await;
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
        let lease = self.ensure_client().await?;

        let mut conn = XhodConnection::new((*lease.resource()).clone(), target.to_string());
        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: request.shell.clone(),
            no_shell: request.no_shell,
        };

        let result = conn.exec_interactive(&conn_request).await;
        let handle = match result {
            Ok(handle) => handle,
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    generation = %lease.generation(),
                    "transport error on xhod interactive exec; discarding client without replay: {}",
                    e
                );
                self.invalidate_client(lease.generation()).await;
                return Err(GatewayError::transport(e));
            }
            Err(e) => return Err(GatewayError::execution(e)),
        };

        Ok(InteractiveHandle {
            stdin_tx: handle.stdin_tx,
            resize_tx: handle.resize_tx,
            stdout_rx: handle.stdout_rx,
            exit_rx: handle.exit_rx,
            abort_handles: handle.abort_handles,
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let lease = self.ensure_client().await?;
        let client = (*lease.resource()).clone();

        match self.list_servers_with_client(client).await {
            Ok(rows) => Ok(rows),
            Err(e) if matches!(e.kind, ErrorKind::Transport) => {
                debug!(
                    gateway = %self.gateway_name,
                    "transport error on list_servers, retrying: {}",
                    e
                );
                self.invalidate_client(lease.generation()).await;

                let new_lease = self.ensure_client().await?;
                let new_client = (*new_lease.resource()).clone();
                match self.list_servers_with_client(new_client).await {
                    Ok(rows) => Ok(rows),
                    Err(e) => {
                        if matches!(e.kind, ErrorKind::Transport) {
                            self.invalidate_client(new_lease.generation()).await;
                        }
                        Err(e)
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Xhod
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {
        let _ = self.client.prune_idle(self.max_idle_time).await;
    }
}
