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
use russh_sftp::client::fs::{File as SftpFile, Metadata as SftpMetadata};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::config::{AppConfig, DirectAuth};
use crate::copy_frames::{
    copy_entry_name, join_relative_path, path_to_string, relative_path_to_string,
};
use crate::protocol::ServerEvent;
use crate::types::{CopyDirection, CopyFrame, CopySpec};

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
        if request.tty {
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

    async fn copy(&mut self, mut spec: CopySpec) -> Result<()> {
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(&mut spec).await,
            CopyDirection::Download => self.copy_download(&mut spec).await,
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
        let task = tokio::spawn(async move {
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
        let abort_handles = vec![task.abort_handle()];

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
            abort_handles,
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
    async fn copy_upload(&mut self, spec: &mut CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let remote_root = PathBuf::from(self.expand_remote_copy_path(&spec.remote_path).await?);
        let remote_root_is_dir = remote_path_is_dir(&sftp, &remote_root).await;
        let mut upload_rx = spec
            .upload_rx
            .take()
            .ok_or_else(|| anyhow!("upload copy frame stream missing"))?;
        let mut current_file: Option<SftpFile> = None;

        while let Some(frame) = upload_rx.recv().await {
            match frame {
                CopyFrame::BeginFile { relative_path, .. } => {
                    if current_file.is_some() {
                        bail!("copy stream began a new file before ending the previous file");
                    }
                    let remote_path = if spec.recursive {
                        join_relative_path(&remote_root, &relative_path)?
                    } else if remote_root_is_dir {
                        remote_root.join(copy_entry_name(&relative_path, &spec.source_name, "copy"))
                    } else {
                        remote_root.clone()
                    };
                    if let Some(parent) = remote_path.parent() {
                        create_remote_dirs(&sftp, parent).await?;
                    }
                    current_file = Some(
                        sftp.create(path_to_string(&remote_path)?)
                            .await
                            .with_context(|| {
                                format!("failed to create remote {}", remote_path.display())
                            })?,
                    );
                }
                CopyFrame::FileData { data } => {
                    let file = current_file
                        .as_mut()
                        .ok_or_else(|| anyhow!("copy stream sent file data before BeginFile"))?;
                    file.write_all(&data).await?;
                }
                CopyFrame::EndFile => {
                    let mut file = current_file
                        .take()
                        .ok_or_else(|| anyhow!("copy stream sent EndFile before BeginFile"))?;
                    file.shutdown().await?;
                }
                CopyFrame::BeginDirectory { relative_path, .. } => {
                    if !spec.recursive {
                        bail!("remote directory frame requires recursive copy");
                    }
                    let remote_path = join_relative_path(&remote_root, &relative_path)?;
                    create_remote_dirs(&sftp, &remote_path).await?;
                }
                CopyFrame::Symlink {
                    relative_path,
                    target,
                } => {
                    let remote_path = if spec.recursive {
                        join_relative_path(&remote_root, &relative_path)?
                    } else if remote_root_is_dir {
                        remote_root.join(copy_entry_name(&relative_path, &spec.source_name, "copy"))
                    } else {
                        remote_root.clone()
                    };
                    if let Some(parent) = remote_path.parent() {
                        create_remote_dirs(&sftp, parent).await?;
                    }
                    let _ = sftp.remove_file(path_to_string(&remote_path)?).await;
                    sftp.symlink(path_to_string(&remote_path)?, target)
                        .await
                        .with_context(|| {
                            format!("failed to create remote symlink {}", remote_path.display())
                        })?;
                }
                CopyFrame::EndOfStream => break,
            }
        }

        if current_file.is_some() {
            bail!("copy stream ended before EndFile");
        }
        Ok(())
    }

    async fn copy_download(&mut self, spec: &mut CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        let remote = Path::new(&remote_path);
        let metadata = sftp
            .symlink_metadata(path_to_string(remote)?)
            .await
            .with_context(|| format!("failed to stat remote path {}", remote.display()))?;
        let tx = spec
            .download_tx
            .take()
            .ok_or_else(|| anyhow!("download copy frame stream missing"))?;
        if metadata.is_dir() {
            if !spec.recursive {
                bail!("copying a remote directory requires -r");
            }
            send_remote_dir_frames(&sftp, remote, Path::new(""), &tx).await?;
        } else {
            let relative_path = copy_entry_name("", &remote_source_name(&remote_path), "copy");
            send_remote_entry_frame(&sftp, remote, Path::new(&relative_path), &metadata, &tx)
                .await?;
        }
        tx.send(CopyFrame::EndOfStream)
            .await
            .map_err(|_| anyhow!("download copy frame stream closed"))?;
        Ok(())
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

fn validate_copy_spec(spec: &CopySpec) -> Result<()> {
    if spec.remote_path.is_empty() {
        bail!("remote_path must not be empty");
    }
    Ok(())
}

fn remote_source_name(remote_path: &str) -> String {
    Path::new(remote_path.trim_end_matches('/'))
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("download")
        .to_string()
}

async fn remote_path_is_dir(sftp: &SftpSession, remote_path: &Path) -> bool {
    sftp.metadata(path_to_string(remote_path).unwrap_or_default())
        .await
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// SFTP file/directory copy helpers
// ---------------------------------------------------------------------------

async fn send_remote_dir_frames(
    sftp: &SftpSession,
    remote_root: &Path,
    relative_root: &Path,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    tx.send(CopyFrame::BeginDirectory {
        relative_path: relative_path_to_string(relative_root)?,
        mode: 0,
        mtime: 0,
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;

    let mut entries = sftp
        .read_dir(path_to_string(remote_root)?)
        .await
        .with_context(|| format!("failed to read remote dir {}", remote_root.display()))?;
    while let Some(entry) = entries.next() {
        let file_name = entry.file_name();
        if file_name == "." || file_name == ".." {
            continue;
        }
        let remote_path = remote_root.join(&file_name);
        let relative_path = relative_root.join(&file_name);
        let metadata = entry.metadata();
        send_remote_entry_frame(sftp, &remote_path, &relative_path, &metadata, tx).await?;
    }
    Ok(())
}

async fn send_remote_entry_frame(
    sftp: &SftpSession,
    remote_path: &Path,
    relative_path: &Path,
    metadata: &SftpMetadata,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    if metadata.is_dir() {
        return Box::pin(send_remote_dir_frames(sftp, remote_path, relative_path, tx)).await;
    }
    if metadata.is_symlink() {
        let target = sftp
            .read_link(path_to_string(remote_path)?)
            .await
            .with_context(|| format!("failed to read remote symlink {}", remote_path.display()))?;
        tx.send(CopyFrame::Symlink {
            relative_path: relative_path_to_string(relative_path)?,
            target,
        })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
        return Ok(());
    }
    if !metadata.is_regular() {
        bail!(
            "unsupported remote file type for copy: {}",
            remote_path.display()
        );
    }

    tx.send(CopyFrame::BeginFile {
        relative_path: relative_path_to_string(relative_path)?,
        mode: metadata.permissions.unwrap_or(0),
        size: metadata.len(),
        mtime: metadata.mtime.map(i64::from).unwrap_or(0),
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;

    let mut file = sftp
        .open(path_to_string(remote_path)?)
        .await
        .with_context(|| format!("failed to open remote {}", remote_path.display()))?;
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        tx.send(CopyFrame::FileData {
            data: buf[..n].to_vec(),
        })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    }
    tx.send(CopyFrame::EndFile)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
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
