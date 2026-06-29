// XhodGateway implementation.
//
// Forwards operations to a remote xhod daemon over a gRPC channel multiplexed
// on an SSH `xho-rpc` subsystem. The gRPC client is lazily established on first
// use and shared across all concurrent operations (so a second `xho exec`
// through this gateway reuses the channel). On transport errors the client is
// discarded and re-established on the next operation.
//
// Both `open_session` and `open_exec_session` build a `TunneledSession` that
// drives the remote daemon's `OpenSession` RPC; the remote xhod recursively
// opens its own `TargetSession` to the end machine, so multi-hop is uniform.

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

use crate::daemon::connection_manager::{ManagedSingleton, SingletonLease};
use crate::daemon::rpc::prefix_source;
use crate::daemon::session::TargetSession;
use crate::daemon::session::tunnel::TunneledSession;
use crate::daemon::shell::build_remote_command;
use crate::daemon::ssh_server::remote_subsystem_name;
use crate::protocol::{self, ServerListRow, rpc};
use crate::types::ServerListSource;

use super::auth::{AuthPrompter, ClientHandler, normalize_paths, parse_remote_target};
use super::{Capabilities, ErrorKind, Gateway, GatewayError, GatewayKind};

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
                    debug!(gateway = %self.gateway_name,
                        "transport error creating xhod client, retrying: {}", e);
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("xhod client checkout loop is bounded")
    }

    /// Get a cloned gRPC client, used to construct a `TunneledSession`.
    async fn client(&self) -> Result<XhoRpcClient, GatewayError> {
        let lease = self.ensure_client().await?;
        Ok((*lease.resource()).clone())
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

    /// Create a tonic gRPC client. The connector opens a fresh SSH `xho-rpc`
    /// subsystem every time tonic needs a new underlying transport.
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

        info!(gateway = %self.gateway_name, "gRPC-over-SSH channel established");
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
        let target = parse_remote_target(&config.address)
            .map_err(|e| anyhow!("failed to parse xhod address {:?}: {}", config.address, e))?;

        let id_opt = (!config.identity_file.is_empty()).then_some(config.identity_file.as_str());
        let kh_opt =
            (!config.known_hosts_path.is_empty()).then_some(config.known_hosts_path.as_str());
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

        let auth_result = Self::authenticate_ssh(&mut handle, &target.user, &identity_file).await;
        if let Err(e) = auth_result {
            debug!(gateway = %config.gateway_name,
                "publickey auth failed, trying password via AuthPrompter: {}", e);
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
            debug!(gateway = %self.gateway_name, generation = %generation,
                "discarded xhod gRPC client, will reconnect on next use");
        }
    }

    async fn list_servers_with_client(
        &self,
        mut client: XhoRpcClient,
    ) -> Result<Vec<ServerListRow>, GatewayError> {
        let mut request = tonic::Request::new(rpc::ServerListRequest {});
        request
            .metadata_mut()
            .insert("xho-no-recurse", "true".parse().unwrap());

        let response = client
            .list_servers(request)
            .await
            .map_err(|e| GatewayError::transport(anyhow!("list_servers RPC failed: {}", e)))?
            .into_inner();

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
    fn name(&self) -> &str {
        &self.gateway_name
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Xhod
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EXEC | Capabilities::COPY | Capabilities::PROXY | Capabilities::LIST
    }

    async fn open_exec_session(
        &self,
        target: &str,
        argv: &[String],
        _shell: &str,
        _no_shell: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError> {
        // The remote xhod builds the final command for its own end target via
        // the OpenSession tunnel, so we send raw (quoted) argv and let the next
        // hop resolve the shell.
        let client = self.client().await?;
        let command = build_remote_command(argv);
        let session =
            Box::new(TunneledSession::new(client, target.to_string())) as Box<dyn TargetSession>;
        Ok((session, command))
    }

    async fn open_session(&self, target: &str) -> Result<Box<dyn TargetSession>, GatewayError> {
        let client = self.client().await?;
        Ok(Box::new(TunneledSession::new(client, target.to_string())) as Box<dyn TargetSession>)
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let lease = self.ensure_client().await?;
        let client = (*lease.resource()).clone();

        match self.list_servers_with_client(client).await {
            Ok(rows) => Ok(rows),
            Err(e) if matches!(e.kind, ErrorKind::Transport) => {
                debug!(gateway = %self.gateway_name,
                    "transport error on list_servers, retrying: {}", e);
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

    async fn pool_status(
        &self,
    ) -> Vec<crate::daemon::connection_manager::ConnectionStatusSnapshot> {
        vec![self.client.status_snapshot(self.gateway_name.clone()).await]
    }

    async fn prune_idle(&self) {
        let _ = self.client.prune_idle(self.max_idle_time).await;
    }
}
