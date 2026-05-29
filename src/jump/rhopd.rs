use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use russh::client;
use russh::keys::{ssh_key, HashAlg, PrivateKeyWithHashAlg, load_secret_key};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::warn;

use crate::config::{AppConfig, ServerEntry};
use crate::connection::CopySpec;
use crate::protocol::{rpc, AuthPromptMessage, ServerEvent};
use crate::remote::{
    inspect_known_host, normalize_remote_paths, parse_remote_target, remote_subsystem_name,
    KnownHostState, RemoteTarget,
};

use super::auth::AuthPromptRouter;
use super::error::UnsupportedCapability;
use super::{JumpHost, JumpHostKind};

/// Owned SSH plumbing that backs the `rhop-rpc` byte stream.
///
/// Field declaration order is load-bearing because Rust drops struct fields
/// top-to-bottom. `stream_slot` is declared before `handle` so that any
/// `RhopdSubsystemStream` still parked in the slot (i.e. tonic never drove
/// the connector) is torn down before the SSH `Handle`, guaranteeing the
/// russh subsystem channel reaches EOF while the SSH transport is still
/// alive. Once tonic has consumed the slot, the live stream is owned by
/// the tonic `Channel` (and ultimately by `RhopdJumpHost::client`), whose
/// drop order is enforced at the outer struct level (see `RhopdJumpHost`).
#[allow(dead_code)]
pub struct RhopdTransport {
    /// One-shot slot handed to the tonic connector closure on its first
    /// invocation. After tonic takes the stream it stays empty for the
    /// lifetime of the transport.
    stream_slot: Arc<Mutex<Option<RhopdSubsystemStream>>>,
    /// Authenticated SSH session backing every channel opened on this
    /// rhopd hop. Dropped last so the underlying TCP/SSH transport stays
    /// up while the byte stream and gRPC channel finish unwinding.
    handle: client::Handle<RhopdAuthClientHandler>,
}

impl RhopdTransport {
    /// Build a transport from already-established russh and stream-slot
    /// components. Constructed exclusively by `RhopdJumpHost::connect`
    /// after authentication and subsystem negotiation succeed.
    #[allow(dead_code)]
    pub(crate) fn from_components(
        handle: client::Handle<RhopdAuthClientHandler>,
        stream_slot: Arc<Mutex<Option<RhopdSubsystemStream>>>,
    ) -> Self {
        Self {
            stream_slot,
            handle,
        }
    }
}

/// A [`JumpHost`] that reaches end targets through a remote `rhopd` daemon
/// over a gRPC channel multiplexed on the SSH `rhop-rpc` subsystem.
///
/// One gRPC channel is shared per `rhopd` alias; tonic supports concurrent
/// RPCs on a single channel.
///
/// Field declaration order is load-bearing for `Drop`. Rust drops struct
/// fields top-to-bottom, so the order below produces the teardown sequence
/// `client` -> `transport.stream_slot` -> `transport.handle`:
///
/// 1. `client` drops first, releasing the tonic `Channel` and (through it)
///    the live `RhopdSubsystemStream` owned by tonic's connection pool.
/// 2. `transport` drops next; inside it `stream_slot` is declared before
///    `handle`, so any unconsumed `RhopdSubsystemStream` is torn down
///    before the SSH `Handle`.
/// 3. `transport.handle` drops last, closing the SSH session only after
///    the subsystem byte stream has reached EOF on both ends.
#[allow(dead_code)]
pub struct RhopdJumpHost {
    alias: String,
    address: String,
    /// The end-target server alias that this jump host instance routes to.
    /// Sent as the `target` field in Execute/Copy RPCs to the remote daemon.
    target_label: String,
    // NOTE: `client` is intentionally declared before `transport` to enforce
    // the drop order documented above. Do not reorder these two fields.
    client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
    // The transport is `Option<RhopdTransport>` rather than a bare
    // `RhopdTransport` so in-process tests that drive the gRPC client
    // directly through a duplex stream can construct a host without a
    // real SSH session. Production paths (`connect`) always store
    // `Some(...)`. Drop order is unaffected: `Option::drop` runs the
    // inner `RhopdTransport`'s `Drop` first, after `client` has been
    // dropped above, preserving the documented teardown sequence.
    transport: Option<RhopdTransport>,
    auth_router: Option<Arc<AuthPromptRouter>>,
}

impl RhopdJumpHost {
    /// Establish a real connection to the remote `rhopd` daemon. This is the
    /// new 4-parameter signature that takes `address`, `identity_file`, and
    /// `known_hosts_path` as raw strings; subsequent steps inside this
    /// function are responsible for parsing/normalising them.
    ///
    /// Step 4.1 — address parsing & short-circuit. Any failure from
    /// `parse_remote_target` is mapped to `RhopdConnectError::Parse` and
    /// returned before any network resource is created (no
    /// `russh::client::connect`, no DNS lookup, nothing).
    #[allow(dead_code)]
    pub async fn connect(
        alias: String,
        address: String,
        identity_file: String,
        known_hosts_path: String,
        target_label: String,
    ) -> Result<Self> {
        // Step 4.1: parse the address up-front so a malformed `address`
        // never causes an outbound connection attempt.
        let target: RemoteTarget = parse_remote_target(&address).map_err(|e| {
            anyhow::Error::from(RhopdConnectError::Parse {
                address: address.clone(),
                reason: format!("{e}"),
            })
        })?;

        // Step 4.2: normalise default paths. Empty strings (from
        // `RhopdJumpHostFields` defaults) must fall back to
        // `~/.ssh/id_ed25519` and `default_known_hosts_path()` per
        // requirements 2.6/2.7. `normalize_remote_paths` only treats `None`
        // as "use default", so we collapse empty strings to `None` first.
        let identity_opt = if identity_file.is_empty() {
            None
        } else {
            Some(identity_file.clone())
        };
        let known_hosts_opt = if known_hosts_path.is_empty() {
            None
        } else {
            Some(known_hosts_path.clone())
        };
        let (identity_file, known_hosts_path) =
            normalize_remote_paths(identity_opt, known_hosts_opt)?;

        // Step 4.3: open the SSH transport and run host-key validation. The
        // handler stashes the outcome of `check_server_key` into a shared
        // `Arc<Mutex<...>>` so that, after `russh::client::connect` returns
        // (potentially with a transport error), we can inspect whether the
        // failure was caused by an unknown / changed host key versus a
        // generic TCP / SSH transport error.
        let last_seen: Arc<Mutex<Option<HostKeyOutcome>>> = Arc::new(Mutex::new(None));
        let handler = RhopdAuthClientHandler {
            target: target.clone(),
            known_hosts_path: PathBuf::from(known_hosts_path.clone()),
            last_seen: last_seen.clone(),
        };
        let client_config = Arc::new(client::Config::default());
        let mut handle = match client::connect(
            client_config,
            (target.host.as_str(), target.port),
            handler,
        )
        .await
        {
            Ok(h) => h,
            Err(e) => {
                // If the handler observed an Unknown / Changed host key, the
                // russh transport error is really a host-key failure; lift
                // it into the dedicated `HostKey` variant so requirement 7.2
                // can surface the fingerprint. Otherwise treat it as a
                // generic TCP / SSH transport failure (requirement 7.1).
                let outcome = last_seen
                    .lock()
                    .expect("rhopd host key mutex poisoned")
                    .take();
                let err: RhopdConnectError = match outcome {
                    Some(HostKeyOutcome::Unknown { fingerprint }) => {
                        RhopdConnectError::HostKey {
                            host: target.host.clone(),
                            port: target.port,
                            state: HostKeyStateKind::Unknown,
                            fingerprint,
                        }
                    }
                    Some(HostKeyOutcome::Changed { fingerprint }) => {
                        RhopdConnectError::HostKey {
                            host: target.host.clone(),
                            port: target.port,
                            state: HostKeyStateKind::Changed,
                            fingerprint,
                        }
                    }
                    _ => RhopdConnectError::Tcp {
                        host: target.host.clone(),
                        port: target.port,
                        reason: format!("{e}"),
                    },
                };
                return Err(err.into());
            }
        };

        // Step 4.4: publickey authentication. Any failure (key load,
        // best-supported RSA hash negotiation, transport error during the
        // userauth round-trip, or `auth.success() == false`) is mapped to
        // a single `RhopdConnectError::Auth` variant carrying the
        // user/host/port/identity_file triple required by requirement 7.3.
        let auth_err = || RhopdConnectError::Auth {
            user: target.user.clone(),
            host: target.host.clone(),
            port: target.port,
            identity_file: identity_file.clone(),
        };
        let key = load_secret_key(&identity_file, None).map_err(|_| auth_err())?;
        let hash_alg: Option<HashAlg> = handle
            .best_supported_rsa_hash()
            .await
            .map_err(|_| auth_err())?
            .flatten();
        let auth = handle
            .authenticate_publickey(
                &target.user,
                PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg),
            )
            .await
            .map_err(|_| auth_err())?;
        if !auth.success() {
            return Err(auth_err().into());
        }

        // Step 4.5: open a fresh SSH session channel and request the
        // `rhop-rpc` subsystem on it. Both `channel_open_session` and
        // `request_subsystem` failures are funnelled into a single
        // `RhopdConnectError::Subsystem` per requirement 7.4 so the
        // operator sees one variant regardless of which RPC actually
        // returned the error.
        let subsystem_err = |e: russh::Error| RhopdConnectError::Subsystem {
            host: target.host.clone(),
            port: target.port,
            reason: format!("{e}"),
        };
        let ssh_channel = handle
            .channel_open_session()
            .await
            .map_err(subsystem_err)?;
        ssh_channel
            .request_subsystem(true, remote_subsystem_name())
            .await
            .map_err(subsystem_err)?;

        // Step 4.6: wrap the SSH subsystem byte stream and build a tonic
        // `Channel`. The wrapped stream is parked in a one-shot slot so
        // tonic's connector closure consumes it on the first call. Any
        // subsequent invocation (which would only happen if tonic decided
        // to redial) returns an error, because the rhop-rpc subsystem is
        // single-use per SSH session and re-entry would silently swap in
        // a stale stream. Per requirement 7.4 / design.md, any failure on
        // the subsystem-stream-backed path stays in the `Subsystem`
        // variant so error categories remain tidy.
        let stream = RhopdSubsystemStream::new(ssh_channel.into_stream());
        let stream_slot: Arc<Mutex<Option<RhopdSubsystemStream>>> =
            Arc::new(Mutex::new(Some(stream)));
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
                            std::io::Error::other(
                                "rhop-rpc subsystem connector already consumed",
                            )
                        })?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|e| RhopdConnectError::Subsystem {
                host: target.host.clone(),
                port: target.port,
                reason: format!("{e}"),
            })?;

        // Step 4.7: assemble the final `RhopdJumpHost`. The tonic
        // `Channel` becomes the backing transport for the gRPC client, and
        // the SSH `Handle` plus the (now-empty after first dial) stream
        // slot are wrapped into `RhopdTransport` so their drop order
        // remains tied to the gRPC client's lifetime via field ordering on
        // `RhopdJumpHost`.
        let client = rpc::rhop_rpc_client::RhopRpcClient::new(tonic_channel);
        let transport = RhopdTransport::from_components(handle, stream_slot);
        Ok(RhopdJumpHost {
            alias,
            address,
            target_label,
            client,
            transport: Some(transport),
            auth_router: None,
        })
    }

    /// Build from pre-constructed components (useful for testing and for the
    /// pool/factory once the transport layer is wired).
    ///
    /// `transport` is `Option<RhopdTransport>` so in-process tests that
    /// connect the gRPC `client` over a `tokio::io::duplex` pair can pass
    /// `None` — they have no real SSH session to attach. Production code
    /// must always pass `Some(...)`.
    #[allow(dead_code)]
    pub fn from_parts(
        alias: String,
        address: String,
        transport: Option<RhopdTransport>,
        client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
    ) -> Self {
        // When constructed via `from_parts` without an explicit target_label,
        // default to the alias. This preserves backward compatibility for
        // tests that use `from_parts` directly. Production code always goes
        // through `connect` which sets `target_label` explicitly.
        Self {
            target_label: alias.clone(),
            alias,
            address,
            client,
            transport,
            auth_router: None,
        }
    }

    /// Set the auth prompt router used to forward authentication prompts
    /// received from the inner gRPC stream upstream to the CLI.
    #[allow(dead_code)]
    pub fn set_auth_router(&mut self, router: Arc<AuthPromptRouter>) {
        self.auth_router = Some(router);
    }
}

#[async_trait]
impl JumpHost for RhopdJumpHost {
    async fn exec(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
    ) -> Result<i32> {
        // Build the initial StartRequest and send it as the first message on
        // the Execute streaming RPC. We use an mpsc channel so we can send
        // AuthInputRequest messages back into the stream when auth prompts arrive.
        let start = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: self.target_label.clone(),
                argv: argv.to_vec(),
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::ExecuteRequest>(4);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send start request into stream")
        })?;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self
            .client
            .execute(request_stream)
            .await?
            .into_inner();

        let mut exit_code: Option<i32> = None;
        let connect_timeout = config.ssh.connect_timeout;

        // Bridge ExecuteResponse events 1:1 back to the sender.
        while let Some(response) = response_stream.message().await? {
            if let Some(event) = response.event {
                match event {
                    rpc::execute_response::Event::Stdout(chunk) => {
                        let _ = sender.send(ServerEvent::Stdout { data: chunk.data });
                    }
                    rpc::execute_response::Event::Stderr(chunk) => {
                        let _ = sender.send(ServerEvent::Stderr { data: chunk.data });
                    }
                    rpc::execute_response::Event::ExitStatus(status) => {
                        exit_code = Some(status.code);
                    }
                    rpc::execute_response::Event::Error(err) => {
                        let _ = sender.send(ServerEvent::Error {
                            message: err.message,
                        });
                    }
                    rpc::execute_response::Event::ReviewResult(review) => {
                        let risk_level = match review.risk_level.as_str() {
                            "safe" => crate::config::RiskLevel::Safe,
                            "risky" => crate::config::RiskLevel::Risky,
                            _ => crate::config::RiskLevel::Dangerous,
                        };
                        let action = match review.action.as_str() {
                            "allow" => crate::config::ReviewAction::Allow,
                            "warn" => crate::config::ReviewAction::Warn,
                            "confirm" => crate::config::ReviewAction::Confirm,
                            _ => crate::config::ReviewAction::Deny,
                        };
                        let _ = sender.send(ServerEvent::ReviewResult {
                            execution_id: uuid::Uuid::parse_str(&review.execution_id)
                                .unwrap_or_default(),
                            risk_level,
                            action,
                            reason: review.reason,
                            matched_whitelist_reason: if review.matched_whitelist_reason.is_empty()
                            {
                                None
                            } else {
                                Some(review.matched_whitelist_reason)
                            },
                        });
                    }
                    rpc::execute_response::Event::ConfirmRequired(confirm) => {
                        let _ = sender.send(ServerEvent::ConfirmRequired {
                            execution_id: uuid::Uuid::parse_str(&confirm.execution_id)
                                .unwrap_or_default(),
                            reason: confirm.reason,
                        });
                    }
                    rpc::execute_response::Event::Info(_info) => {
                        // Info messages are informational; no action needed.
                    }
                    rpc::execute_response::Event::AuthPrompt(prompt) => {
                        if let Some(router) = &self.auth_router {
                            // Forward the auth prompt upstream via the router and
                            // wait for the response, then send AuthInputRequest back.
                            let prompt_id = prompt.prompt_id.clone();
                            let target_label = prompt.target_label.clone();
                            let msg = AuthPromptMessage {
                                prompt_id: prompt.prompt_id,
                                target_label: prompt.target_label,
                                kind: prompt.kind,
                                secret: prompt.secret,
                                message: prompt.message,
                            };

                            let ask_result = tokio::time::timeout(
                                connect_timeout,
                                router.ask(msg),
                            )
                            .await;

                            match ask_result {
                                Ok(Ok(value)) => {
                                    // Send AuthInputRequest back into the gRPC stream
                                    let auth_input = rpc::ExecuteRequest {
                                        request: Some(
                                            rpc::execute_request::Request::AuthInput(
                                                rpc::AuthInputRequest {
                                                    prompt_id,
                                                    value,
                                                },
                                            ),
                                        ),
                                    };
                                    if req_tx.send(auth_input).await.is_err() {
                                        return Err(anyhow::anyhow!(
                                            "failed to send auth input into gRPC stream"
                                        ));
                                    }
                                }
                                Ok(Err(e)) => {
                                    warn!(
                                        prompt_id = %prompt_id,
                                        error = %e,
                                        "auth prompt ask failed"
                                    );
                                    return Err(e);
                                }
                                Err(_elapsed) => {
                                    return Err(anyhow::anyhow!(
                                        "auth prompt timed out for {}",
                                        target_label
                                    ));
                                }
                            }
                        } else {
                            // No router available; forward as ServerEvent for the
                            // daemon's event loop to handle (legacy path).
                            let _ = sender.send(ServerEvent::AuthPrompt {
                                prompt_id: prompt.prompt_id,
                                target_label: prompt.target_label,
                                kind: prompt.kind,
                                secret: prompt.secret,
                                message: prompt.message,
                            });
                        }
                    }
                }
            }
        }

        exit_code.ok_or_else(|| anyhow::anyhow!("remote daemon closed stream without exit status"))
    }

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        // Build CopyStartRequest with local_path intentionally set to "" for
        // rhopd hops — the remote daemon must not touch local paths.
        let direction = match spec.direction {
            crate::connection::CopyDirection::Upload => rpc::CopyDirection::Upload as i32,
            crate::connection::CopyDirection::Download => rpc::CopyDirection::Download as i32,
        };

        let start = rpc::CopyRequest {
            request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
                target: self.target_label.clone(),
                local_path: String::new(), // intentionally empty for rhopd hops
                remote_path: spec.remote_path.clone(),
                recursive: spec.recursive,
                direction,
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::CopyRequest>(4);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send copy start request into stream")
        })?;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self.client.copy(request_stream).await?.into_inner();

        let connect_timeout = config.ssh.connect_timeout;

        // Bridge CopyResponse events 1:1.
        while let Some(response) = response_stream.message().await? {
            if let Some(event) = response.event {
                match event {
                    rpc::copy_response::Event::Complete(_complete) => {
                        return Ok(());
                    }
                    rpc::copy_response::Event::Error(err) => {
                        return Err(anyhow::anyhow!("remote copy error: {}", err.message));
                    }
                    rpc::copy_response::Event::Info(_info) => {
                        // Informational; no action needed.
                    }
                    rpc::copy_response::Event::AuthPrompt(prompt) => {
                        if let Some(router) = &self.auth_router {
                            // Forward the auth prompt upstream via the router and
                            // wait for the response, then send AuthInputRequest back.
                            let prompt_id = prompt.prompt_id.clone();
                            let target_label = prompt.target_label.clone();
                            let msg = AuthPromptMessage {
                                prompt_id: prompt.prompt_id,
                                target_label: prompt.target_label,
                                kind: prompt.kind,
                                secret: prompt.secret,
                                message: prompt.message,
                            };

                            let ask_result = tokio::time::timeout(
                                connect_timeout,
                                router.ask(msg),
                            )
                            .await;

                            match ask_result {
                                Ok(Ok(value)) => {
                                    // Send AuthInputRequest back into the gRPC stream
                                    let auth_input = rpc::CopyRequest {
                                        request: Some(
                                            rpc::copy_request::Request::AuthInput(
                                                rpc::AuthInputRequest {
                                                    prompt_id,
                                                    value,
                                                },
                                            ),
                                        ),
                                    };
                                    if req_tx.send(auth_input).await.is_err() {
                                        return Err(anyhow::anyhow!(
                                            "failed to send auth input into gRPC copy stream"
                                        ));
                                    }
                                }
                                Ok(Err(e)) => {
                                    warn!(
                                        prompt_id = %prompt_id,
                                        error = %e,
                                        "copy auth prompt ask failed"
                                    );
                                    return Err(e);
                                }
                                Err(_elapsed) => {
                                    return Err(anyhow::anyhow!(
                                        "auth prompt timed out for {}",
                                        target_label
                                    ));
                                }
                            }
                        }
                        // If no router, auth prompts are silently ignored (no
                        // way to respond without a router).
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "remote daemon closed copy stream without completion"
        ))
    }

    async fn tui_shell(&mut self, _config: &AppConfig) -> Result<()> {
        // Until interactive shell ships, return UnsupportedCapability per Req 4.5.
        Err(UnsupportedCapability {
            kind: JumpHostKind::Rhopd,
            name: self.alias.clone(),
            method: "tui_shell",
        }
        .into())
    }

    async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
        // Call the Remote_Daemon's ListServers RPC over the pooled gRPC channel.
        let response = self
            .client
            .list_servers(rpc::ServerListRequest {})
            .await?
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

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Rhopd
    }

    fn name(&self) -> &str {
        &self.alias
    }
}

// ---------------------------------------------------------------------------
// Connect-time error type
// ---------------------------------------------------------------------------

/// Categorical state of a server host key as observed during the SSH handshake.
///
/// Mirrors the failure-relevant subset of `KnownHostState` that needs to leak
/// out of `check_server_key` and into the user-facing error message.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostKeyStateKind {
    /// Host key is not present in `known_hosts`.
    Unknown,
    /// Host key is present in `known_hosts` but does not match what the
    /// server presented (potential MITM or legitimate key rotation).
    Changed,
}

impl std::fmt::Display for HostKeyStateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::Changed => f.write_str("changed"),
        }
    }
}

/// Failures that can occur while building an `RhopdJumpHost`.
///
/// Each variant's `Display` rendering is contractually required by
/// requirements 7.1‒7.4 to carry enough context (target address, fingerprint,
/// user, identity file, etc.) for an operator to triage the error from a
/// single line of output.
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum RhopdConnectError {
    /// The configured `address` could not be parsed by `parse_remote_target`.
    /// The original (unparsed) string is preserved verbatim in `address` so
    /// the operator can spot typos.
    #[error("failed to parse rhopd address {address:?}: {reason}")]
    Parse { address: String, reason: String },

    /// DNS resolution, TCP connection, or the SSH transport-level handshake
    /// failed. `reason` is the `Display` form of the underlying
    /// `russh::Error` / `std::io::Error`.
    #[error("failed to connect to {host}:{port}: {reason}")]
    Tcp {
        host: String,
        port: u16,
        reason: String,
    },

    /// The remote host key was either not present in `known_hosts`
    /// (`state == Unknown`) or did not match the recorded entry
    /// (`state == Changed`). The SHA-256 fingerprint of the key the server
    /// actually presented is included so the operator can compare against
    /// out-of-band records.
    #[error(
        "host key for {host}:{port} is {state} (sha256 fingerprint: {fingerprint})"
    )]
    HostKey {
        host: String,
        port: u16,
        state: HostKeyStateKind,
        fingerprint: String,
    },

    /// Publickey authentication did not succeed against the remote rhopd.
    /// The full `user@host:port` triple plus the on-disk identity file path
    /// are surfaced so the operator can quickly check the wrong-key case.
    #[error(
        "publickey authentication failed for {user}@{host}:{port} using identity_file={identity_file}"
    )]
    Auth {
        user: String,
        host: String,
        port: u16,
        identity_file: String,
    },

    /// The SSH session was established and authenticated, but the remote
    /// daemon refused to start the `rhop-rpc` subsystem on the freshly
    /// opened channel (or the channel closed during the request).
    #[error("failed to open rhop-rpc subsystem on {host}:{port}: {reason}")]
    Subsystem {
        host: String,
        port: u16,
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Host-key inspection handler
// ---------------------------------------------------------------------------

/// Outcome of a single `check_server_key` invocation.
///
/// The handler stores this in shared state so that, after the handshake
/// completes, `RhopdJumpHost::connect` can lift `Unknown` / `Changed` results
/// into a `RhopdConnectError::HostKey` carrying the captured fingerprint.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum HostKeyOutcome {
    Known,
    Unknown { fingerprint: String },
    Changed { fingerprint: String },
}

/// `russh::client::Handler` used by `RhopdJumpHost::connect` to validate the
/// remote `rhopd`'s host key against a `known_hosts` file.
///
/// This handler is intentionally tiny and stateless from russh's perspective:
/// it consults `inspect_known_host`, records the outcome (including the
/// SHA-256 fingerprint for failure variants) into `last_seen`, and returns
/// `Ok(true)` only when the key matches an existing entry. Any other state
/// returns `Ok(false)`, which causes russh to abort the handshake with an
/// error that `connect` then rewrites into `RhopdConnectError::HostKey`.
#[allow(dead_code)]
pub(crate) struct RhopdAuthClientHandler {
    pub(crate) target: RemoteTarget,
    pub(crate) known_hosts_path: PathBuf,
    pub(crate) last_seen: Arc<Mutex<Option<HostKeyOutcome>>>,
}

impl client::Handler for RhopdAuthClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &ssh_key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        let outcome = match inspect_known_host(
            &self.target,
            server_public_key,
            &self.known_hosts_path,
        ) {
            KnownHostState::Known => HostKeyOutcome::Known,
            KnownHostState::Unknown { fingerprint, .. } => {
                HostKeyOutcome::Unknown { fingerprint }
            }
            KnownHostState::Changed { fingerprint, .. } => {
                HostKeyOutcome::Changed { fingerprint }
            }
        };
        let accept = matches!(outcome, HostKeyOutcome::Known);
        *self
            .last_seen
            .lock()
            .expect("rhopd host key mutex poisoned") = Some(outcome);
        Ok(accept)
    }
}

// ---------------------------------------------------------------------------
// rhop-rpc subsystem byte stream adapter
// ---------------------------------------------------------------------------

/// Adapter that owns the byte stream backing the `rhop-rpc` subsystem and
/// forwards `AsyncRead` / `AsyncWrite` to the inner stream, so it can be
/// handed to `TokioIo` for tonic.
///
/// Production builds wrap a real `russh::ChannelStream<russh::client::Msg>`.
/// The test-only `Boxed` variant exists so property tests (Property 3:
/// byte-transparency) can drive the adapter through a `tokio::io::duplex`
/// half without spinning up a real SSH session.
#[allow(dead_code)]
pub(crate) struct RhopdSubsystemStream {
    inner: InnerStream,
}

#[allow(dead_code)]
enum InnerStream {
    Real(Box<russh::ChannelStream<russh::client::Msg>>),
    #[cfg(test)]
    Boxed(Box<dyn DuplexStream>),
}

#[cfg(test)]
trait DuplexStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}

#[cfg(test)]
impl<T> DuplexStream for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin
{
}

impl RhopdSubsystemStream {
    /// Wrap a real russh subsystem channel stream.
    #[allow(dead_code)]
    pub(crate) fn new(inner: russh::ChannelStream<russh::client::Msg>) -> Self {
        Self {
            inner: InnerStream::Real(Box::new(inner)),
        }
    }

    /// Test-only constructor that wraps any compatible duplex stream so
    /// property tests can drive the adapter without a real SSH session.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn from_async_stream<S>(stream: S) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        Self {
            inner: InnerStream::Boxed(Box::new(stream)),
        }
    }
}

impl tokio::io::AsyncRead for RhopdSubsystemStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            InnerStream::Real(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            #[cfg(test)]
            InnerStream::Boxed(stream) => std::pin::Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for RhopdSubsystemStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.inner {
            InnerStream::Real(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            #[cfg(test)]
            InnerStream::Boxed(stream) => std::pin::Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            InnerStream::Real(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            #[cfg(test)]
            InnerStream::Boxed(stream) => std::pin::Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            InnerStream::Real(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            #[cfg(test)]
            InnerStream::Boxed(stream) => std::pin::Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

// Compile-time assertion that `RhopdSubsystemStream` implements the trio of
// traits required by tonic's `TokioIo` wrapper. If any forwarding impl above
// is removed or broken, this fails to compile.
const _: fn() = || {
    fn assert_impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send>() {}
    assert_impl::<RhopdSubsystemStream>();
};

// ---------------------------------------------------------------------------
// Unit tests for path defaults and connect-time error rendering
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{default_known_hosts_path, expand_tilde};
    use crate::remote::normalize_remote_paths;

    /// Empty / `None` inputs MUST fall back to `~/.ssh/id_ed25519` and
    /// `default_known_hosts_path()` (both expanded). This mirrors the
    /// behaviour `RhopdJumpHost::connect` relies on after collapsing
    /// empty strings to `None`.
    #[test]
    fn connect_normalizes_default_paths() {
        let (identity_file, known_hosts_path) =
            normalize_remote_paths(None, None).expect("default normalisation succeeds");
        let expected_identity = expand_tilde("~/.ssh/id_ed25519").unwrap();
        // `default_known_hosts_path()` may itself be tilde-prefixed depending
        // on the host config; expand it the same way `normalize_remote_paths`
        // does so the comparison is like-for-like.
        let expected_known_hosts =
            expand_tilde(&default_known_hosts_path().display().to_string()).unwrap();
        assert_eq!(identity_file, expected_identity);
        assert_eq!(known_hosts_path, expected_known_hosts);
    }

    /// Mirrors the empty-string -> `None` collapse `RhopdJumpHost::connect`
    /// performs before delegating to `normalize_remote_paths`. Verifies the
    /// downstream path-defaulting still kicks in when the caller-supplied
    /// strings are present but empty.
    #[test]
    fn connect_normalizes_empty_strings_via_caller_collapse() {
        let identity_in: String = String::new();
        let known_hosts_in: String = String::new();
        let identity_opt = if identity_in.is_empty() {
            None
        } else {
            Some(identity_in)
        };
        let known_hosts_opt = if known_hosts_in.is_empty() {
            None
        } else {
            Some(known_hosts_in)
        };
        let (identity_file, known_hosts_path) =
            normalize_remote_paths(identity_opt, known_hosts_opt).expect("ok");
        let expected_identity = expand_tilde("~/.ssh/id_ed25519").unwrap();
        let expected_known_hosts =
            expand_tilde(&default_known_hosts_path().display().to_string()).unwrap();
        assert_eq!(identity_file, expected_identity);
        assert_eq!(known_hosts_path, expected_known_hosts);
    }

    /// `RhopdConnectError::Parse` MUST surface the original (unparsed)
    /// address verbatim along with the underlying reason so the operator
    /// can spot typos in the configured `address` field.
    #[test]
    fn connect_error_display_parse() {
        let e = RhopdConnectError::Parse {
            address: "  ".to_string(),
            reason: "empty host".to_string(),
        };
        let msg = format!("{e}");
        assert!(msg.contains("\"  \""), "missing original address: {msg}");
        assert!(msg.contains("empty host"), "missing reason: {msg}");
    }

    /// `RhopdConnectError::Tcp` MUST surface `host:port` and the underlying
    /// reason.
    #[test]
    fn connect_error_display_tcp() {
        let e = RhopdConnectError::Tcp {
            host: "example.com".to_string(),
            port: 2222,
            reason: "connection refused".to_string(),
        };
        let msg = format!("{e}");
        assert!(msg.contains("example.com:2222"), "missing host:port: {msg}");
        assert!(msg.contains("connection refused"), "missing reason: {msg}");
    }

    /// `RhopdConnectError::HostKey` MUST surface `host:port`, the
    /// `unknown` / `changed` state token, and the SHA-256 fingerprint.
    #[test]
    fn connect_error_display_hostkey() {
        let unknown = RhopdConnectError::HostKey {
            host: "h".to_string(),
            port: 22,
            state: HostKeyStateKind::Unknown,
            fingerprint: "SHA256:abcdef".to_string(),
        };
        let msg = format!("{unknown}");
        assert!(msg.contains("h:22"), "missing host:port: {msg}");
        assert!(msg.contains("unknown"), "missing state token: {msg}");
        assert!(msg.contains("SHA256:abcdef"), "missing fingerprint: {msg}");

        let changed = RhopdConnectError::HostKey {
            host: "h".to_string(),
            port: 22,
            state: HostKeyStateKind::Changed,
            fingerprint: "SHA256:abcdef".to_string(),
        };
        let msg = format!("{changed}");
        assert!(msg.contains("changed"), "missing state token: {msg}");
    }

    /// `RhopdConnectError::Auth` MUST surface `user@host:port` and the
    /// configured identity file path so the operator can quickly check
    /// the wrong-key case.
    #[test]
    fn connect_error_display_auth() {
        let e = RhopdConnectError::Auth {
            user: "alice".to_string(),
            host: "h".to_string(),
            port: 2222,
            identity_file: "/tmp/id_ed25519".to_string(),
        };
        let msg = format!("{e}");
        assert!(msg.contains("alice@h:2222"), "missing user@host:port: {msg}");
        assert!(
            msg.contains("/tmp/id_ed25519"),
            "missing identity_file: {msg}"
        );
    }

    /// `RhopdConnectError::Subsystem` MUST surface `host:port`, the
    /// subsystem name `rhop-rpc`, and the underlying reason.
    #[test]
    fn connect_error_display_subsystem() {
        let e = RhopdConnectError::Subsystem {
            host: "h".to_string(),
            port: 2222,
            reason: "channel closed".to_string(),
        };
        let msg = format!("{e}");
        assert!(msg.contains("h:2222"), "missing host:port: {msg}");
        assert!(msg.contains("rhop-rpc"), "missing subsystem name: {msg}");
        assert!(msg.contains("channel closed"), "missing reason: {msg}");
    }

    use proptest::prelude::*;

    /// Strategy producing rhopd address strings that `parse_remote_target`
    /// is guaranteed to reject. Three sources are unioned:
    ///
    /// 1. Hand-curated literals that are known-rejected today (empty /
    ///    whitespace-only, dangling `user@`, whitespace-only host with a
    ///    valid port, well-formed host with explicitly-bad port suffixes).
    /// 2. Programmatically generated `host:nonNumericPort` where the port
    ///    component is non-empty but provably not a `u16`.
    /// 3. Programmatically generated out-of-range numeric ports
    ///    (`host:N` with `N > u16::MAX`).
    ///
    /// All three sources are then funnelled through a `prop_filter` that
    /// double-checks `parse_remote_target` actually returns `Err`, so the
    /// property test never accidentally exercises an accepted address.
    fn arb_invalid_rhopd_address() -> impl Strategy<Value = String> {
        // (1) Curated literals: every entry has been manually verified
        // against `parse_remote_target` and produces an `Err`.
        let literals = prop_oneof![
            Just("".to_string()),
            Just("   ".to_string()),
            Just("\t".to_string()),
            Just("\n".to_string()),
            Just("user@".to_string()),
            Just("alice@".to_string()),
            Just("   :2222".to_string()),
            Just("\t:80".to_string()),
            Just(" :22".to_string()),
            Just("host:abc".to_string()),
            Just("host:-1".to_string()),
            Just("host:1.5".to_string()),
            Just("host: 22".to_string()),
            Just("host:22 ".to_string()),
            Just("host:65536".to_string()),
            Just("host:99999999".to_string()),
            Just("@:port".to_string()),
            Just("foo.com:bar".to_string()),
        ];

        // (2) Non-numeric port component. The port body is at least one
        // ASCII letter so `u16::from_str` is guaranteed to fail; the host
        // body is alphanumeric and non-empty so the host-empty branch is
        // not taken first.
        let nonnumeric_port = (
            "[a-zA-Z0-9.\\-]{1,16}",
            "[a-zA-Z][a-zA-Z0-9]{0,7}",
        )
            .prop_map(|(host, port)| format!("{host}:{port}"));

        // (3) Out-of-range numeric port. We append `99999` to a non-empty
        // numeric tail so the resulting decimal is always > u16::MAX.
        let out_of_range_port = (
            "[a-zA-Z0-9.\\-]{1,16}",
            "[0-9]{0,4}",
        )
            .prop_map(|(host, tail)| format!("{host}:99999{tail}"));

        prop_oneof![literals, nonnumeric_port, out_of_range_port]
            // Defence-in-depth: even if the generators above ever drift,
            // the filter guarantees we only feed `connect` strings that
            // `parse_remote_target` truly rejects.
            .prop_filter(
                "address must be rejected by parse_remote_target",
                |s| parse_remote_target(s).is_err(),
            )
    }

    /// One slot in the (key, expected-state) assignment driven by Property 2.
    ///
    /// We materialise the three `KnownHostState` outcomes as a tagged enum
    /// so the test body can populate the temporary `known_hosts` file
    /// deterministically (write the key / write a different key / write
    /// nothing) before running `check_server_key` against it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ExpectedKnownHostState {
        Known,
        Unknown,
        Changed,
    }

    /// Strategy producing a uniformly random `ExpectedKnownHostState` for
    /// each generated key. Each variant is equally likely so the property
    /// gets reasonable coverage of all three branches inside 100 cases.
    fn arb_expected_known_host_state() -> impl Strategy<Value = ExpectedKnownHostState> {
        prop_oneof![
            Just(ExpectedKnownHostState::Known),
            Just(ExpectedKnownHostState::Unknown),
            Just(ExpectedKnownHostState::Changed),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        // Feature: rhopd-connect-and-server-list, Property 1: invalid address short-circuits before any network attempt
        ///
        /// **Validates: Requirements 1.2, 7.1**
        ///
        /// For every address string that `parse_remote_target` rejects,
        /// `RhopdJumpHost::connect` MUST return `Err` whose downcast is
        /// `RhopdConnectError::Parse`. Because `parse_remote_target` is
        /// the very first call inside `connect` and every other variant
        /// (`Tcp` / `HostKey` / `Auth` / `Subsystem`) can only be produced
        /// after the parse step has succeeded, observing the `Parse`
        /// variant is itself a structural witness that no DNS lookup, no
        /// TCP socket, and no SSH handshake was attempted.
        ///
        /// The error's `Display` text MUST also carry the original input
        /// verbatim so an operator can spot typos in the configured
        /// `address` field (Requirement 7.1, surfaced via the error map).
        #[test]
        fn prop_invalid_address_no_network(address in arb_invalid_rhopd_address()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let original = address.clone();
            let result = rt.block_on(async move {
                RhopdJumpHost::connect(
                    "alias-under-test".to_string(),
                    address,
                    String::new(),
                    String::new(),
                    "target-under-test".to_string(),
                )
                .await
            });
            let err = match result {
                Err(e) => e,
                Ok(_) => {
                    return Err(TestCaseError::fail(format!(
                        "connect unexpectedly succeeded for invalid address {original:?}"
                    )));
                }
            };

            // Variant check: must be `RhopdConnectError::Parse`. Any other
            // variant would imply network work happened, contradicting
            // Property 1.
            let connect_err = err.downcast_ref::<RhopdConnectError>().ok_or_else(|| {
                TestCaseError::fail(format!(
                    "expected RhopdConnectError, got: {err}"
                ))
            })?;
            prop_assert!(
                matches!(connect_err, RhopdConnectError::Parse { .. }),
                "expected RhopdConnectError::Parse, got: {:?}",
                connect_err
            );

            // Display text must contain the original input (debug-quoted),
            // matching the contract validated by `connect_error_display_parse`.
            let rendered = format!("{err}");
            let quoted = format!("{:?}", original);
            prop_assert!(
                rendered.contains(&quoted),
                "error display {rendered:?} must contain original address {quoted}",
            );
        }

        // Feature: rhopd-connect-and-server-list, Property 2: auth handler matches inspect_known_host
        ///
        /// **Validates: Requirements 2.2, 2.3, 7.2**
        ///
        /// For every (key, expected-state) pair in a 5..=10-element
        /// assignment vector, `RhopdAuthClientHandler::check_server_key`
        /// MUST agree with `inspect_known_host` on whether the key is
        /// `Known` (and thus accepted). When the inspection result is
        /// `Unknown` or `Changed`, the SHA-256 fingerprint stashed in
        /// `last_seen` MUST equal `key.fingerprint(HashAlg::Sha256)
        /// .to_string()`, which is exactly the value
        /// `RhopdJumpHost::connect` then surfaces in
        /// `RhopdConnectError::HostKey`.
        ///
        /// The test materialises three populations on a single
        /// `tempfile::NamedTempFile`:
        /// * `Known`: write the matching public key via
        ///   `learn_known_hosts_path` so `check_known_hosts_path`
        ///   resolves to `Ok(true)`.
        /// * `Unknown`: leave the key absent so
        ///   `check_known_hosts_path` resolves to `Ok(false)`.
        /// * `Changed`: write a *different* (decoy) public key under the
        ///   same `host:port` so `check_known_hosts_path` resolves to
        ///   `Err(KeyChanged { .. })`.
        #[test]
        fn prop_auth_handler_known_host_equivalence(
            assignment in proptest::collection::vec(arb_expected_known_host_state(), 5..=10),
        ) {
            use ssh_key::{Algorithm, PrivateKey};
            use russh::keys::known_hosts::learn_known_hosts_path;

            // Generate one fresh Ed25519 keypair per slot. We use the
            // same SysRng pattern as `daemon.rs` so the test exercises the
            // production-grade RNG rather than a deterministic shim.
            let mut rng = rand_core::UnwrapErr(getrandom::SysRng);
            let private_keys: Vec<PrivateKey> = (0..assignment.len())
                .map(|_| {
                    PrivateKey::random(&mut rng, Algorithm::Ed25519)
                        .expect("Ed25519 keygen succeeds")
                })
                .collect();

            // One temp known_hosts file per case. NamedTempFile auto-cleans
            // on drop so each proptest iteration leaves the FS pristine.
            let temp = tempfile::NamedTempFile::new()
                .expect("create temp known_hosts file");
            let known_hosts_path: PathBuf = temp.path().to_path_buf();

            // Each slot uses its own logical host so populations do not
            // interfere with one another inside the shared known_hosts
            // file. Within a single slot the host:port is reused so the
            // `Changed` branch (decoy key under same host:port as the
            // checked key) remains structurally reachable.
            let targets: Vec<RemoteTarget> = (0..assignment.len())
                .map(|slot| RemoteTarget {
                    host: format!("rhopd-prop-host-{slot}"),
                    port: 2222,
                    user: "rhop".to_string(),
                })
                .collect();

            // Pre-seed the file according to the assignment. For `Changed`
            // slots we synthesise a decoy key (distinct from the real one)
            // and learn it under the same host:port; the real key is what
            // the handler will then be asked to check, triggering
            // `KeyChanged`.
            for (slot, expected) in assignment.iter().enumerate() {
                let target = &targets[slot];
                let real_pub = private_keys[slot].public_key();
                match expected {
                    ExpectedKnownHostState::Known => {
                        learn_known_hosts_path(
                            &target.host,
                            target.port,
                            real_pub,
                            &known_hosts_path,
                        )
                        .expect("learn real key for Known slot");
                    }
                    ExpectedKnownHostState::Unknown => {
                        // Intentionally write nothing: absent key is the
                        // signal `check_known_hosts_path` reports as
                        // `Ok(false)`.
                    }
                    ExpectedKnownHostState::Changed => {
                        // Generate a decoy key that is guaranteed to
                        // differ from `private_keys[slot]` and learn it
                        // under the same host:port so the on-disk record
                        // diverges from what the handler will be shown.
                        let decoy = PrivateKey::random(&mut rng, Algorithm::Ed25519)
                            .expect("decoy Ed25519 keygen succeeds");
                        learn_known_hosts_path(
                            &target.host,
                            target.port,
                            decoy.public_key(),
                            &known_hosts_path,
                        )
                        .expect("learn decoy key for Changed slot");
                    }
                }
            }

            let rt = tokio::runtime::Runtime::new().unwrap();

            // Walk the assignment again, this time querying the handler
            // for each (key, expected-state) pair and cross-checking
            // against `inspect_known_host`.
            for (slot, expected) in assignment.iter().enumerate() {
                let target = targets[slot].clone();
                let real_priv = &private_keys[slot];
                let real_pub = real_priv.public_key().clone();

                let last_seen: Arc<Mutex<Option<HostKeyOutcome>>> =
                    Arc::new(Mutex::new(None));
                let mut handler = RhopdAuthClientHandler {
                    target: target.clone(),
                    known_hosts_path: known_hosts_path.clone(),
                    last_seen: last_seen.clone(),
                };

                // 1. Reference truth: what `inspect_known_host` reports.
                let state = inspect_known_host(&target, &real_pub, &known_hosts_path);

                // 2. Observed: what the handler returns. `check_server_key`
                // is async (russh trait method), so wrap with a runtime
                // block_on like `prop_invalid_address_no_network` does.
                let accepted = rt.block_on(async {
                    use client::Handler;
                    handler
                        .check_server_key(&real_pub)
                        .await
                        .expect("check_server_key never returns Err in our handler")
                });

                // 3. Equivalence: handler accepts iff the inspection
                // result is `Known`.
                let inspected_known = matches!(state, KnownHostState::Known);
                prop_assert_eq!(
                    accepted, inspected_known,
                    "slot {} expected {:?}: accepted={} but inspect_known_host returned {:?}",
                    slot, expected, accepted, state
                );

                // 4. Sanity: the handler's reported acceptance must agree
                // with the assignment we used to seed the file.
                let expected_known = matches!(expected, ExpectedKnownHostState::Known);
                prop_assert_eq!(
                    accepted, expected_known,
                    "slot {} expected {:?} but accepted={}",
                    slot, expected, accepted
                );

                // 5. Fingerprint capture: on Unknown/Changed paths the
                // handler must persist the SHA-256 fingerprint of the
                // *presented* key into `last_seen`, exactly equal to
                // `public_key.fingerprint(HashAlg::Sha256).to_string()`.
                let stash = last_seen
                    .lock()
                    .expect("rhopd host key mutex poisoned (test)")
                    .clone();
                match (expected, stash) {
                    (ExpectedKnownHostState::Known, Some(HostKeyOutcome::Known)) => {
                        // Known path: `last_seen` records `Known` with no
                        // fingerprint, matching design.md.
                    }
                    (
                        ExpectedKnownHostState::Unknown,
                        Some(HostKeyOutcome::Unknown { fingerprint }),
                    ) => {
                        let expected_fp =
                            real_pub.fingerprint(HashAlg::Sha256).to_string();
                        prop_assert_eq!(
                            fingerprint.clone(),
                            expected_fp.clone(),
                            "slot {} Unknown fingerprint mismatch: got {:?} expected {:?}",
                            slot,
                            fingerprint,
                            expected_fp
                        );
                    }
                    (
                        ExpectedKnownHostState::Changed,
                        Some(HostKeyOutcome::Changed { fingerprint }),
                    ) => {
                        let expected_fp =
                            real_pub.fingerprint(HashAlg::Sha256).to_string();
                        prop_assert_eq!(
                            fingerprint.clone(),
                            expected_fp.clone(),
                            "slot {} Changed fingerprint mismatch: got {:?} expected {:?}",
                            slot,
                            fingerprint,
                            expected_fp
                        );
                    }
                    (expected, stash) => {
                        return Err(TestCaseError::fail(format!(
                            "slot {slot} mismatch: expected {expected:?}, last_seen={stash:?}"
                        )));
                    }
                }
            }
        }

        // Feature: rhopd-connect-and-server-list, Property 3: RhopdSubsystemStream is byte-transparent
        ///
        /// **Validates: Requirements 3.3**
        ///
        /// For arbitrary byte sequences (length 0..=65535) generated in
        /// both directions, writing them through a `RhopdSubsystemStream`
        /// adapter MUST surface unchanged on the peer side of a
        /// `tokio::io::duplex` pair, and vice-versa. This guarantees the
        /// adapter is a transparent byte channel for tonic, with no
        /// chunking, reordering, dropping, or padding.
        ///
        /// Two independent duplex pairs are used, one per direction, so
        /// the `shutdown()` that signals EOF to `read_to_end` on one
        /// pair never interferes with the second direction's pair.
        #[test]
        fn prop_subsystem_stream_byte_transparent(
            client_to_server in proptest::collection::vec(any::<u8>(), 0..=65535),
            server_to_client in proptest::collection::vec(any::<u8>(), 0..=65535),
        ) {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Direction 1: client -> server. The duplex buffer is
                // sized to 64 KiB which strictly exceeds the maximum
                // generated payload (65535 bytes); using a multi-threaded
                // runtime also lets the reader drain concurrently with
                // the writer, so even shorter buffers would suffice.
                let (client_side, mut server_side) = tokio::io::duplex(64 * 1024);
                let mut client_stream = RhopdSubsystemStream::from_async_stream(client_side);

                let writer_payload = client_to_server.clone();
                let writer = tokio::spawn(async move {
                    client_stream.write_all(&writer_payload).await.unwrap();
                    client_stream.flush().await.unwrap();
                    // shutdown() closes only the write half of the duplex,
                    // signalling EOF to `read_to_end` on `server_side`.
                    client_stream.shutdown().await.unwrap();
                });

                let mut received_at_server = Vec::new();
                server_side.read_to_end(&mut received_at_server).await.unwrap();
                writer.await.unwrap();
                prop_assert_eq!(
                    received_at_server,
                    client_to_server,
                    "client -> server payload was not transparent"
                );

                // Direction 2: server -> client. Build a fresh duplex
                // pair so the half-closed state from direction 1 cannot
                // contaminate the read path here.
                let (client_side2, mut server_side2) = tokio::io::duplex(64 * 1024);
                let mut client_stream2 = RhopdSubsystemStream::from_async_stream(client_side2);

                let server_payload = server_to_client.clone();
                let writer2 = tokio::spawn(async move {
                    server_side2.write_all(&server_payload).await.unwrap();
                    server_side2.flush().await.unwrap();
                    server_side2.shutdown().await.unwrap();
                });

                let mut received_at_client = Vec::new();
                client_stream2.read_to_end(&mut received_at_client).await.unwrap();
                writer2.await.unwrap();
                prop_assert_eq!(
                    received_at_client,
                    server_to_client,
                    "server -> client payload was not transparent"
                );

                Ok::<(), TestCaseError>(())
            })?;
        }
    }
}
