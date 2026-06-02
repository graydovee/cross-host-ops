// RhopdGateway implementation.
// Manages a single gRPC-over-SSH connection to a remote rhopd daemon.
// All requests are multiplexed over the shared gRPC channel.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use russh::client;
use russh::keys::{load_secret_key, PrivateKeyWithHashAlg};
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{debug, info};

use crate::config::ServerEntry;
use crate::types::CopySpec;
use crate::protocol::rpc;
use crate::daemon::ssh_server::remote_subsystem_name;

use super::auth::{normalize_paths, parse_remote_target, AuthPrompter, ClientHandler};
use super::{
    ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle, InteractiveRequest,
    is_transport_error,
};
use crate::daemon::connection::rhopd::RhopdConnection;
use crate::daemon::connection::{
    Connection, ExecRequest as ConnExecRequest, InteractiveRequest as ConnInteractiveRequest,
};

// ---------------------------------------------------------------------------
// RhopdGateway
// ---------------------------------------------------------------------------

/// A Gateway that forwards operations to a remote rhopd daemon over a
/// gRPC channel multiplexed on an SSH `rhop-rpc` subsystem.
///
/// The gRPC client is lazily established on first use and shared across
/// all concurrent operations. On transport errors, the client is discarded
/// and re-established on the next operation.
pub struct RhopdGateway {
    gateway_name: String,
    address: String,
    identity_file: String,
    known_hosts_path: String,
    auth_prompter: Arc<AuthPrompter>,
    /// Single shared gRPC client (lazily connected).
    client: AsyncMutex<Option<rpc::rhop_rpc_client::RhopRpcClient<Channel>>>,
}

impl RhopdGateway {
    /// Construct a new RhopdGateway. No connections are established.
    pub fn new(
        gateway_name: String,
        address: String,
        identity_file: String,
        known_hosts_path: String,
        auth_prompter: Arc<AuthPrompter>,
    ) -> Self {
        Self {
            gateway_name,
            address,
            identity_file,
            known_hosts_path,
            auth_prompter,
            client: AsyncMutex::new(None),
        }
    }

    /// Ensure the gRPC client is connected. If not, establish a new
    /// SSH connection → authenticate → open subsystem → create tonic Channel.
    /// Returns a clone of the gRPC client.
    async fn ensure_client(
        &self,
    ) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>, GatewayError> {
        let mut guard = self.client.lock().await;
        if let Some(ref client) = *guard {
            return Ok(client.clone());
        }

        // Connect and store the new client.
        let client = self.connect_client().await.map_err(GatewayError::transport)?;
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Establish the SSH connection and create a gRPC client.
    async fn connect_client(&self) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
        // Parse address to get host, port, user.
        let target = parse_remote_target(&self.address)
            .map_err(|e| anyhow!("failed to parse rhopd address {:?}: {}", self.address, e))?;

        // Normalize identity_file and known_hosts_path with defaults.
        let id_opt = if self.identity_file.is_empty() {
            None
        } else {
            Some(self.identity_file.as_str())
        };
        let kh_opt = if self.known_hosts_path.is_empty() {
            None
        } else {
            Some(self.known_hosts_path.as_str())
        };
        let (identity_file, _known_hosts_path) = normalize_paths(id_opt, kh_opt)
            .map_err(|e| anyhow!("failed to normalize paths for {}: {}", self.gateway_name, e))?;

        info!(
            gateway = %self.gateway_name,
            host = %target.host,
            port = %target.port,
            user = %target.user,
            "connecting to remote rhopd"
        );

        // Open SSH connection with the "rhop" user from the parsed address
        // (parse_remote_target defaults to "rhop" user if not specified).
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

        // Authenticate with public key (user "rhop"), AuthPrompter fallback.
        let auth_result = self
            .authenticate_ssh(&mut handle, &target.user, &identity_file)
            .await;

        if let Err(e) = auth_result {
            // Fallback: use AuthPrompter for password
            debug!(
                gateway = %self.gateway_name,
                "publickey auth failed, trying password via AuthPrompter: {}",
                e
            );
            let password = (self.auth_prompter)(super::auth::AuthPrompt {
                gateway_name: self.gateway_name.clone(),
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

        // Open session channel and request the "rhop-rpc" subsystem.
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
                    "failed to start rhop-rpc subsystem on {}:{}: {}",
                    target.host,
                    target.port,
                    e
                )
            })?;

        // Wrap the SSH channel stream into a tonic Channel via a one-shot connector.
        let stream = ssh_channel.into_stream();
        let stream_slot: Arc<std::sync::Mutex<Option<_>>> =
            Arc::new(std::sync::Mutex::new(Some(stream)));
        let connector_slot = stream_slot.clone();

        let endpoint = Endpoint::from_static("http://[::]:50051");
        let tonic_channel: Channel = endpoint
            .connect_with_connector(service_fn(move |_: Uri| {
                let slot = connector_slot.clone();
                async move {
                    let stream = slot
                        .lock()
                        .expect("rhopd stream slot mutex poisoned")
                        .take()
                        .ok_or_else(|| {
                            std::io::Error::other("rhop-rpc subsystem connector already consumed")
                        })?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to establish gRPC channel over SSH to {}:{}: {}",
                    target.host,
                    target.port,
                    e
                )
            })?;

        info!(
            gateway = %self.gateway_name,
            "gRPC-over-SSH connection established to {}:{}",
            target.host,
            target.port
        );

        Ok(rpc::rhop_rpc_client::RhopRpcClient::new(tonic_channel))
    }

    /// Attempt publickey authentication. Returns Ok(()) on success, Err on failure.
    async fn authenticate_ssh(
        &self,
        handle: &mut client::Handle<ClientHandler>,
        user: &str,
        identity_file: &str,
    ) -> Result<()> {
        let key = load_secret_key(identity_file, None)
            .map_err(|e| anyhow!("failed to load key {}: {}", identity_file, e))?;
        let hash_alg = handle
            .best_supported_rsa_hash()
            .await?
            .flatten();
        let auth = handle
            .authenticate_publickey(
                user,
                PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg),
            )
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

    /// Discard the cached client so the next operation reconnects.
    async fn discard_client(&self) {
        let mut guard = self.client.lock().await;
        *guard = None;
        debug!(gateway = %self.gateway_name, "discarded gRPC client, will reconnect on next use");
    }
}

// ---------------------------------------------------------------------------
// Gateway trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Gateway for RhopdGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let client = self.ensure_client().await?;

        // Create a RhopdConnection and delegate exec to it.
        let mut conn = RhopdConnection::new(client, target.to_string());
        let conn_request = ConnExecRequest {
            argv: request.argv.clone(),
            sender: request.sender.clone(),
            pty: request.pty,
            cols: request.cols,
            rows: request.rows,
            shell: request.shell.clone(),
        };

        // First attempt
        let result = conn.exec(&conn_request).await;

        match result {
            Ok(exit_code) => Ok(exit_code),
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    "transport error on first exec attempt, retrying: {}",
                    e
                );
                // Discard client and retry once.
                self.discard_client().await;

                let new_client = self.ensure_client().await?;
                let mut retry_conn = RhopdConnection::new(new_client, target.to_string());
                let retry_result = retry_conn.exec(&conn_request).await;

                match retry_result {
                    Ok(exit_code) => Ok(exit_code),
                    Err(e) => {
                        // Discard on any further transport error too.
                        self.discard_client().await;
                        if is_transport_error(&e) {
                            Err(GatewayError::transport(e))
                        } else {
                            Err(GatewayError::execution(e))
                        }
                    }
                }
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn copy(&self, target: &str, spec: &CopySpec) -> Result<(), GatewayError> {
        let client = self.ensure_client().await?;

        let mut conn = RhopdConnection::new(client, target.to_string());

        // First attempt
        let result = conn.copy(spec).await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if is_transport_error(&e) => {
                debug!(
                    gateway = %self.gateway_name,
                    target = %target,
                    "transport error on copy, retrying: {}",
                    e
                );
                self.discard_client().await;

                let new_client = self.ensure_client().await?;
                let mut retry_conn = RhopdConnection::new(new_client, target.to_string());
                let retry_result = retry_conn.copy(spec).await;

                match retry_result {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        self.discard_client().await;
                        if is_transport_error(&e) {
                            Err(GatewayError::transport(e))
                        } else {
                            Err(GatewayError::execution(e))
                        }
                    }
                }
            }
            Err(e) => Err(GatewayError::execution(e)),
        }
    }

    async fn exec_interactive(
        &self,
        target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let client = self.ensure_client().await?;

        let mut conn = RhopdConnection::new(client, target.to_string());
        let conn_request = ConnInteractiveRequest {
            argv: request.argv.clone(),
            cols: request.cols,
            rows: request.rows,
            sender: request.sender.clone(),
            shell: request.shell.clone(),
        };

        let handle = conn.exec_interactive(&conn_request).await.map_err(|e| {
            if is_transport_error(&e) {
                GatewayError::transport(e)
            } else {
                GatewayError::execution(e)
            }
        })?;

        Ok(InteractiveHandle {
            stdin_tx: handle.stdin_tx,
            resize_tx: handle.resize_tx,
            stdout_rx: handle.stdout_rx,
            exit_rx: handle.exit_rx,
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerEntry>, GatewayError> {
        let mut client = self.ensure_client().await?;

        let response = client
            .list_servers(rpc::ServerListRequest {})
            .await
            .map_err(|e| {
                // On transport error from list_servers, discard the client.
                // We can't call discard_client() here directly (borrow issues),
                // so we classify and handle below.
                GatewayError::transport(anyhow!("list_servers RPC failed: {}", e))
            })?
            .into_inner();

        let entries = response
            .servers
            .into_iter()
            .map(|s| {
                let auth = if s.auth_kind == "password" {
                    crate::config::DirectAuth::Password {
                        password: String::new(),
                    }
                } else {
                    crate::config::DirectAuth::Key {
                        identity_file: String::new(),
                    }
                };
                ServerEntry {
                    alias: s.alias,
                    host: s.host,
                    port: s.port as u16,
                    user: s.user,
                    auth,
                }
            })
            .collect();

        Ok(entries)
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Rhopd
    }

    fn name(&self) -> &str {
        &self.gateway_name
    }

    async fn prune_idle(&self) {
        // No-op: RhopdGateway maintains a single persistent gRPC connection.
        // Connections are only discarded on transport errors, not idleness.
    }
}
