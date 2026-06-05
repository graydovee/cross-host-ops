// DirectConnection implementation.
// Wraps a russh SSH client handle and implements the Connection trait
// for SSH channel-based exec/copy/interactive operations.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use russh::ChannelMsg;
use russh::client::{self, Handle};
use russh_sftp::client::SftpSession;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::config::{AppConfig, DirectAuth};
use crate::protocol::ServerEvent;
use crate::types::{CopyDirection, CopySpec};

use super::shared::build_final_command;
use super::{Connection, ExecRequest, InteractiveHandle, InteractiveRequest};

// ---------------------------------------------------------------------------
// SSH client handler (accepts all host keys — identity verification happens
// via known_hosts in the auth layer above us).
// ---------------------------------------------------------------------------

pub(crate) struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// DirectConnection
// ---------------------------------------------------------------------------

/// A direct SSH connection to an end target.
/// Wraps a russh client handle and provides exec/copy/interactive operations.
pub(crate) struct DirectConnection {
    handle: Handle<ClientHandler>,
}

impl DirectConnection {
    /// Create a new DirectConnection from an already-authenticated SSH handle.
    pub(crate) fn new(handle: Handle<ClientHandler>) -> Self {
        Self { handle }
    }

    /// Establish a new SSH connection and authenticate.
    pub(crate) async fn connect(
        host: &str,
        port: u16,
        user: &str,
        auth: &DirectAuth,
        config: &AppConfig,
        pubkey_accepted_algorithms: Option<&str>,
    ) -> Result<Self> {
        let mut handle = connect_handle(host, port, config).await?;
        match auth {
            DirectAuth::Key { identity_file } => {
                authenticate_with_key(&mut handle, user, identity_file, pubkey_accepted_algorithms)
                    .await?;
            }
            DirectAuth::Password { password } => {
                authenticate_with_password(&mut handle, user, password).await?;
            }
        }
        // Probe: open and immediately close a session channel to verify
        // the connection is fully established.
        probe_session(&mut handle).await?;
        Ok(Self { handle })
    }
}

#[async_trait::async_trait]
impl Connection for DirectConnection {
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32> {
        let command = build_final_command(&request.argv, &request.shell);
        let mut channel = self.handle.channel_open_session().await?;
        if request.pty {
            channel
                .request_pty(
                    true,
                    "xterm-256color",
                    request.cols,
                    request.rows,
                    0,
                    0,
                    &[],
                )
                .await?;
        }
        channel.exec(true, command.as_str()).await?;

        // Take ownership of the stdin receiver so we can forward bytes to the
        // SSH channel. When None, behavior is identical to the original loop.
        let mut stdin_rx = request.stdin_rx.take();

        let mut exit_code = None;
        let mut stdin_done = stdin_rx.is_none(); // true means no stdin branch to service
        loop {
            tokio::select! {
                // Read from stdin_rx and forward to the SSH channel.
                // This branch is only active when stdin_rx is Some and not yet exhausted.
                stdin_result = async {
                    if stdin_done {
                        // Park this branch permanently — return pending to disable it.
                        std::future::pending::<Option<Vec<u8>>>().await
                    } else {
                        // Safety: stdin_rx is Some when stdin_done is false.
                        stdin_rx.as_mut().unwrap().recv().await
                    }
                } => {
                    match stdin_result {
                        Some(data) => {
                            // Forward stdin chunk to the remote process via the SSH channel.
                            channel.data(Cursor::new(data)).await?;
                        }
                        None => {
                            // Stdin sender has been dropped — signal EOF to the remote process.
                            channel.eof().await?;
                            stdin_done = true;
                        }
                    }
                }
                // Read messages from the SSH channel (stdout, stderr, exit status).
                msg = channel.wait() => {
                    let Some(message) = msg else {
                        break;
                    };
                    match message {
                        ChannelMsg::Data { data } => {
                            let _ = request.sender.send(ServerEvent::Stdout {
                                data: data.to_vec(),
                            });
                        }
                        ChannelMsg::ExtendedData { data, .. } => {
                            let _ = request.sender.send(ServerEvent::Stderr {
                                data: data.to_vec(),
                            });
                        }
                        ChannelMsg::ExitStatus { exit_status } => {
                            exit_code = Some(exit_status as i32);
                        }
                        ChannelMsg::ExitSignal { .. } => {
                            exit_code = Some(255);
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(exit_code.unwrap_or(255))
    }

    async fn copy(&mut self, spec: CopySpec) -> Result<()> {
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(&spec).await,
            CopyDirection::Download => self.copy_download(&spec).await,
        }
    }

    async fn exec_interactive(
        &mut self,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle> {
        let mut channel = self.handle.channel_open_session().await?;

        // Request PTY with caller-specified dimensions
        channel
            .request_pty(
                true,
                "xterm-256color",
                request.cols,
                request.rows,
                0,
                0,
                &[],
            )
            .await?;

        // Build and execute the command
        let command = build_final_command(&request.argv, &request.shell);
        channel.exec(true, command.as_str()).await?;

        // Set up forwarding channels
        let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(32);
        let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(8);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();

        // Spawn channel I/O task
        tokio::spawn(async move {
            let mut exit_code: Option<i32> = None;
            loop {
                tokio::select! {
                    // Write stdin to channel
                    Some(data) = stdin_rx.recv() => {
                        let _ = channel.data(Cursor::new(data)).await;
                    }
                    // Handle resize
                    Some((cols, rows)) = resize_rx.recv() => {
                        let _ = channel.window_change(cols, rows, 0, 0).await;
                    }
                    // Read channel messages
                    msg = channel.wait() => {
                        match msg {
                            Some(ChannelMsg::Data { data }) => {
                                if stdout_tx.send(data.to_vec()).is_err() {
                                    break;
                                }
                            }
                            Some(ChannelMsg::ExitStatus { exit_status }) => {
                                exit_code = Some(exit_status as i32);
                            }
                            Some(ChannelMsg::ExitSignal { .. }) => {
                                exit_code = Some(255);
                            }
                            Some(ChannelMsg::Eof) | None => {
                                let _ = exit_tx.send(exit_code.unwrap_or(0));
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
        })
    }

    fn is_alive(&self) -> bool {
        // The russh Handle is alive as long as it has not been disconnected.
        // We check this by verifying the handle's internal sender is not closed.
        !self.handle.is_closed()
    }
}

// ---------------------------------------------------------------------------
// SSH connection and authentication helpers
// ---------------------------------------------------------------------------

async fn connect_handle(
    host: &str,
    port: u16,
    config: &AppConfig,
) -> Result<Handle<ClientHandler>> {
    let client_config = client::Config {
        inactivity_timeout: Some(config.ssh.keepalive_interval * 2),
        ..Default::default()
    };
    let handle = timeout(
        config.ssh.connect_timeout,
        client::connect(Arc::new(client_config), (host, port), ClientHandler),
    )
    .await
    .context("timed out opening SSH connection")??;
    Ok(handle)
}

async fn authenticate_with_key(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    identity_file: &str,
    pubkey_accepted_algorithms: Option<&str>,
) -> Result<()> {
    use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};

    let key = load_secret_key(identity_file, None)
        .with_context(|| format!("failed to load key {}", identity_file))?;
    let hash_alg = preferred_rsa_hash(pubkey_accepted_algorithms, handle).await?;
    let auth = handle
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg))
        .await?;
    if auth.success() {
        return Ok(());
    }
    bail!("SSH publickey authentication failed for {}", user)
}

async fn authenticate_with_password(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    password: &str,
) -> Result<()> {
    let auth = handle.authenticate_password(user, password).await?;
    if auth.success() {
        return Ok(());
    }
    bail!("SSH password authentication failed for {}", user)
}

async fn preferred_rsa_hash(
    pubkey_accepted_algorithms: Option<&str>,
    handle: &Handle<ClientHandler>,
) -> Result<Option<russh::keys::HashAlg>> {
    if wants_legacy_ssh_rsa(pubkey_accepted_algorithms) {
        return Ok(None);
    }
    Ok(handle.best_supported_rsa_hash().await?.flatten())
}

fn wants_legacy_ssh_rsa(pubkey_accepted_algorithms: Option<&str>) -> bool {
    let Some(value) = pubkey_accepted_algorithms else {
        return false;
    };
    value
        .split(',')
        .map(str::trim)
        .any(|item| item == "ssh-rsa" || item == "+ssh-rsa")
}

async fn probe_session(handle: &mut Handle<ClientHandler>) -> Result<()> {
    let channel = handle.channel_open_session().await?;
    drop(channel);
    Ok(())
}

// ---------------------------------------------------------------------------
// SFTP copy operations
// ---------------------------------------------------------------------------

impl DirectConnection {
    async fn copy_upload(&mut self, spec: &CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let local = PathBuf::from(&spec.local_path);
        let remote_path = self
            .normalize_remote_upload_path(spec, &local, &sftp)
            .await?;
        if spec.recursive {
            copy_local_dir_to_remote(&sftp, &local, Path::new(&remote_path)).await
        } else {
            copy_local_file_to_remote(&sftp, &local, Path::new(&remote_path)).await
        }
    }

    async fn copy_download(&mut self, spec: &CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        let local = PathBuf::from(maybe_local_download_target(
            Path::new(&spec.local_path),
            &remote_path,
        )?);
        let remote = Path::new(&remote_path);
        let metadata = sftp
            .metadata(path_to_string(remote)?)
            .await
            .with_context(|| format!("failed to stat remote path {}", remote.display()))?;
        if metadata.is_dir() {
            if !spec.recursive {
                bail!("copying a remote directory requires -r");
            }
            copy_remote_dir_to_local(&sftp, remote, &local).await
        } else {
            copy_remote_file_to_local(&sftp, remote, &local).await
        }
    }

    async fn open_sftp(&mut self) -> Result<SftpSession> {
        let channel = self.handle.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;
        Ok(sftp)
    }

    async fn expand_remote_copy_path(&mut self, remote_path: &str) -> Result<String> {
        if !remote_path_needs_expansion(remote_path) {
            return Ok(remote_path.to_string());
        }
        let (user, suffix) = split_tilde_path(remote_path)
            .ok_or_else(|| anyhow!("invalid remote path {}", remote_path))?;
        let home = match user {
            Some(user) => self.remote_home_for_user(user).await?,
            None => self.remote_home_for_current_user().await?,
        };
        Ok(join_remote_path(&home, suffix))
    }

    async fn normalize_remote_upload_path(
        &mut self,
        spec: &CopySpec,
        local_path: &Path,
        sftp: &SftpSession,
    ) -> Result<String> {
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        if spec.recursive {
            return Ok(remote_path);
        }
        match sftp.metadata(remote_path.clone()).await {
            Ok(metadata) if metadata.is_dir() => {
                upload_destination_for_directory(local_path, &remote_path)
            }
            Ok(_) => Ok(remote_path),
            Err(_) => Ok(remote_path),
        }
    }

    async fn remote_home_for_current_user(&mut self) -> Result<String> {
        let home = self.run_probe_command("printf %s \"$HOME\"").await?;
        if !home.is_empty() && home.starts_with('/') {
            return Ok(home);
        }
        self.run_probe_command("getent passwd \"$(id -un)\" | cut -d: -f6")
            .await
    }

    async fn remote_home_for_user(&mut self, user: &str) -> Result<String> {
        self.run_probe_command(&format!(
            "getent passwd {} | cut -d: -f6",
            super::shared::shell_quote(user)
        ))
        .await
    }

    async fn run_probe_command(&mut self, command: &str) -> Result<String> {
        let mut channel = self.handle.channel_open_session().await?;
        channel.exec(true, command).await?;
        let mut stdout = Vec::new();
        let mut exit_code = None;
        while let Some(message) = channel.wait().await {
            match message {
                ChannelMsg::Data { data } => stdout.extend_from_slice(&data),
                ChannelMsg::ExitStatus { exit_status } => exit_code = Some(exit_status as i32),
                ChannelMsg::ExitSignal { .. } => exit_code = Some(255),
                _ => {}
            }
        }
        let output = String::from_utf8_lossy(&stdout).trim().to_string();
        if exit_code.unwrap_or(255) != 0 || output.is_empty() || !output.starts_with('/') {
            bail!("failed to resolve remote path via `{}`", command);
        }
        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

fn remote_path_needs_expansion(path: &str) -> bool {
    path == "~" || path.starts_with("~/") || is_tilde_user_path(path)
}

fn is_tilde_user_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix('~') else {
        return false;
    };
    !rest.is_empty() && !rest.starts_with('/') && rest.contains('/')
}

fn split_tilde_path(path: &str) -> Option<(Option<&str>, &str)> {
    let rest = path.strip_prefix('~')?;
    if rest.is_empty() {
        return Some((None, ""));
    }
    if let Some(stripped) = rest.strip_prefix('/') {
        return Some((None, stripped));
    }
    let (user, suffix) = rest.split_once('/')?;
    Some((Some(user), suffix))
}

fn join_remote_path(home: &str, suffix: &str) -> String {
    if suffix.is_empty() {
        home.to_string()
    } else {
        format!("{}/{}", home.trim_end_matches('/'), suffix)
    }
}

fn upload_destination_for_directory(local_path: &Path, remote_dir: &str) -> Result<String> {
    let basename = local_path
        .file_name()
        .ok_or_else(|| {
            anyhow!(
                "failed to derive local basename from {}",
                local_path.display()
            )
        })?
        .to_string_lossy()
        .to_string();
    Ok(format!("{}/{}", remote_dir.trim_end_matches('/'), basename))
}

fn maybe_local_download_target(local_path: &Path, remote_path: &str) -> Result<String> {
    if local_path.exists() && local_path.is_dir() {
        let basename = Path::new(remote_path)
            .file_name()
            .ok_or_else(|| anyhow!("failed to derive remote basename from {}", remote_path))?
            .to_string_lossy()
            .to_string();
        return Ok(local_path.join(basename).display().to_string());
    }
    Ok(local_path.display().to_string())
}

fn validate_copy_spec(spec: &CopySpec) -> Result<()> {
    if spec.local_path.is_empty() || spec.remote_path.is_empty() {
        bail!("local_path and remote_path must not be empty");
    }
    let local = Path::new(&spec.local_path);
    if matches!(spec.direction, CopyDirection::Upload) && local.is_dir() && !spec.recursive {
        bail!("copying a directory requires -r");
    }
    Ok(())
}

fn path_to_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", path.display()))
}

// ---------------------------------------------------------------------------
// SFTP file/directory copy helpers
// ---------------------------------------------------------------------------

async fn copy_local_file_to_remote(sftp: &SftpSession, local: &Path, remote: &Path) -> Result<()> {
    let bytes = tokio::fs::read(local)
        .await
        .with_context(|| format!("failed to read {}", local.display()))?;
    if let Some(parent) = remote.parent() {
        create_remote_dirs(sftp, parent).await?;
    }
    let mut file = sftp
        .create(path_to_string(remote)?)
        .await
        .with_context(|| format!("failed to create remote {}", remote.display()))?;
    file.write_all(&bytes).await?;
    file.shutdown().await?;
    Ok(())
}

async fn copy_remote_file_to_local(sftp: &SftpSession, remote: &Path, local: &Path) -> Result<()> {
    let mut file = sftp
        .open(path_to_string(remote)?)
        .await
        .with_context(|| format!("failed to open remote {}", remote.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    if let Some(parent) = local.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    tokio::fs::write(local, bytes)
        .await
        .with_context(|| format!("failed to write {}", local.display()))?;
    Ok(())
}

async fn copy_local_dir_to_remote(
    sftp: &SftpSession,
    local_root: &Path,
    remote_root: &Path,
) -> Result<()> {
    create_remote_dirs(sftp, remote_root).await?;
    copy_local_dir_to_remote_recursive(sftp, local_root, remote_root).await
}

async fn copy_local_dir_to_remote_recursive(
    sftp: &SftpSession,
    local_dir: &Path,
    remote_dir: &Path,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(local_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        let local_path = entry.path();
        let remote_path = remote_dir.join(entry.file_name());
        if file_type.is_dir() {
            create_remote_dirs(sftp, &remote_path).await?;
            Box::pin(copy_local_dir_to_remote_recursive(
                sftp,
                &local_path,
                &remote_path,
            ))
            .await?;
        } else if file_type.is_file() {
            copy_local_file_to_remote(sftp, &local_path, &remote_path).await?;
        }
    }
    Ok(())
}

async fn copy_remote_dir_to_local(
    sftp: &SftpSession,
    remote_root: &Path,
    local_root: &Path,
) -> Result<()> {
    tokio::fs::create_dir_all(local_root).await?;
    Box::pin(copy_remote_dir_to_local_recursive(
        sftp,
        remote_root,
        local_root,
    ))
    .await
}

async fn copy_remote_dir_to_local_recursive(
    sftp: &SftpSession,
    remote_dir: &Path,
    local_dir: &Path,
) -> Result<()> {
    let mut entries = sftp
        .read_dir(path_to_string(remote_dir)?)
        .await
        .with_context(|| format!("failed to read remote dir {}", remote_dir.display()))?;
    while let Some(entry) = entries.next() {
        let file_name = entry.file_name();
        if file_name == "." || file_name == ".." {
            continue;
        }
        let remote_path = remote_dir.join(&file_name);
        let local_path = local_dir.join(&file_name);
        let metadata = entry.metadata();
        if metadata.is_dir() {
            tokio::fs::create_dir_all(&local_path).await?;
            Box::pin(copy_remote_dir_to_local_recursive(
                sftp,
                &remote_path,
                &local_path,
            ))
            .await?;
        } else {
            copy_remote_file_to_local(sftp, &remote_path, &local_path).await?;
        }
    }
    Ok(())
}

async fn create_remote_dirs(sftp: &SftpSession, remote_path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in remote_path.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() {
            continue;
        }
        let current_str = path_to_string(&current)?;
        if !sftp.try_exists(current_str.clone()).await? {
            sftp.create_dir(current_str.clone())
                .await
                .with_context(|| format!("failed to create remote dir {}", current.display()))?;
        }
    }
    Ok(())
}
