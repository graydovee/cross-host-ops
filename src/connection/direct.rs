use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use russh::ChannelMsg;
use russh::client::Handle;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use russh_sftp::protocol::FileAttributes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::UnboundedSender;

use crate::config::{AppConfig, DirectAuth};
use crate::protocol::ServerEvent;

use super::{Connection, InteractiveSession};
use super::shared::{
    ClientHandler, apply_local_mode, authenticate_with_key, authenticate_with_password,
    build_final_command, connect_handle,
    join_remote_path, local_mode, maybe_local_download_target, remote_mode,
    remote_path_needs_expansion, split_tilde_path,
    upload_destination_for_directory,
};
use super::AuthPrompter;
use super::types::{CopyDirection, CopySpec, DirectTarget};

pub struct DirectSshConnection {
    handle: Handle<ClientHandler>,
}

impl DirectSshConnection {
    pub async fn connect(
        target: &DirectTarget,
        config: &AppConfig,
        _auth_prompter: &AuthPrompter,
    ) -> Result<Self> {
        let mut handle = connect_handle(&target.host_name, target.port, config).await?;
        match &target.auth {
            DirectAuth::Key { identity_file } => {
                authenticate_with_key(
                    &mut handle,
                    &target.user,
                    identity_file,
                    &target.host,
                    None,
                    target.pubkey_accepted_algorithms.as_deref(),
                    None,
                )
                .await?;
            }
            DirectAuth::Password { password } => {
                authenticate_with_password(&mut handle, &target.user, password).await?;
            }
        }
        probe_session(&mut handle).await?;
        Ok(Self { handle })
    }
}

#[tonic::async_trait]
impl Connection for DirectSshConnection {
    async fn execute(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        _config: &AppConfig,
        pty: bool,
        cols: u32,
        rows: u32,
        shell: &str,
    ) -> Result<i32> {
        let command = build_final_command(argv, shell);
        let mut channel = self.handle.channel_open_session().await?;
        if pty {
            channel
                .request_pty(
                    true,
                    "xterm-256color",
                    cols,
                    rows,
                    0,
                    0,
                    &[],
                )
                .await?;
        }
        channel.exec(true, command.as_str()).await?;
        let mut exit_code = None;
        loop {
            let Some(message) = channel.wait().await else {
                break;
            };
            match message {
                ChannelMsg::Data { data } => {
                    let _ = sender.send(ServerEvent::Stdout {
                        data: data.to_vec(),
                    });
                }
                ChannelMsg::ExtendedData { data, .. } => {
                    let _ = sender.send(ServerEvent::Stderr {
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
        Ok(exit_code.unwrap_or(255))
    }

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(spec, config).await,
            CopyDirection::Download => self.copy_download(spec, config).await,
        }
    }

    async fn execute_interactive(
        &mut self,
        argv: &[String],
        cols: u32,
        rows: u32,
        _config: &AppConfig,
        shell: &str,
    ) -> Result<InteractiveSession> {
        let mut channel = self.handle.channel_open_session().await?;

        // Request PTY with caller-specified dimensions
        channel
            .request_pty(true, "xterm-256color", cols, rows, 0, 0, &[])
            .await?;

        // Execute command directly — no sentinel wrapping
        let command = build_final_command(argv, shell);
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

        Ok(InteractiveSession {
            stdin_tx,
            stdout_rx,
            resize_tx,
            exit_rx,
        })
    }
}

async fn probe_session(handle: &mut Handle<ClientHandler>) -> Result<()> {
    let channel = handle.channel_open_session().await?;
    drop(channel);
    Ok(())
}

impl DirectSshConnection {
    async fn copy_upload(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        validate_direct_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let local = PathBuf::from(&spec.local_path);
        let remote_path = self
            .normalize_remote_upload_path(spec, &local, &sftp)
            .await?;
        if spec.recursive {
            copy_local_dir_to_remote(&sftp, &local, Path::new(&remote_path), config.copy.preserve_mode).await
        } else {
            copy_local_file_to_remote(&sftp, &local, Path::new(&remote_path), config.copy.preserve_mode).await
        }
    }

    async fn copy_download(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        validate_direct_copy_spec(spec)?;
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
            copy_remote_dir_to_local(&sftp, remote, &local, config.copy.preserve_mode).await
        } else {
            copy_remote_file_to_local(&sftp, remote, &local, config.copy.preserve_mode).await
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
            Ok(metadata) if metadata.is_dir() => upload_destination_for_directory(local_path, &remote_path),
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

fn validate_direct_copy_spec(spec: &CopySpec) -> Result<()> {
    if spec.local_path.is_empty() || spec.remote_path.is_empty() {
        bail!("local_path and remote_path must not be empty");
    }
    let local = Path::new(&spec.local_path);
    if matches!(spec.direction, CopyDirection::Upload) && local.is_dir() && !spec.recursive {
        bail!("copying a directory requires -r");
    }
    Ok(())
}

async fn copy_local_file_to_remote(
    sftp: &SftpSession,
    local: &Path,
    remote: &Path,
    preserve_mode: bool,
) -> Result<()> {
    let bytes = tokio::fs::read(local)
        .await
        .with_context(|| format!("failed to read {}", local.display()))?;
    if let Some(parent) = remote.parent() {
        create_remote_dirs(sftp, parent).await?;
    }
    let mut file = if preserve_mode {
        let mut attrs = FileAttributes::empty();
        attrs.permissions = Some(local_mode(local)?);
        sftp.open_with_flags_and_attributes(
            path_to_string(remote)?,
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
            attrs,
        )
        .await
        .with_context(|| format!("failed to create remote {}", remote.display()))?
    } else {
        sftp.create(path_to_string(remote)?)
            .await
            .with_context(|| format!("failed to create remote {}", remote.display()))?
    };
    file.write_all(&bytes).await?;
    if preserve_mode {
        let mut attrs = file.metadata().await?;
        attrs.permissions = Some(local_mode(local)?);
        file.set_metadata(attrs).await?;
    }
    file.shutdown().await?;
    Ok(())
}

async fn copy_remote_file_to_local(
    sftp: &SftpSession,
    remote: &Path,
    local: &Path,
    preserve_mode: bool,
) -> Result<()> {
    let mut file = sftp
        .open(path_to_string(remote)?)
        .await
        .with_context(|| format!("failed to open remote {}", remote.display()))?;
    let mode = if preserve_mode {
        Some(remote_mode(&file.metadata().await?)?)
    } else {
        None
    };
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
    if let Some(mode) = mode {
        apply_local_mode(local, mode)?;
    }
    Ok(())
}

async fn copy_local_dir_to_remote(
    sftp: &SftpSession,
    local_root: &Path,
    remote_root: &Path,
    preserve_mode: bool,
) -> Result<()> {
    create_remote_dirs(sftp, remote_root).await?;
    if preserve_mode {
        set_remote_path_mode(sftp, remote_root, local_mode(local_root)?).await?;
    }
    copy_local_dir_to_remote_recursive(sftp, local_root, remote_root, preserve_mode).await
}

async fn copy_local_dir_to_remote_recursive(
    sftp: &SftpSession,
    local_dir: &Path,
    remote_dir: &Path,
    preserve_mode: bool,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(local_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        let local_path = entry.path();
        let remote_path = remote_dir.join(entry.file_name());
        if file_type.is_dir() {
            create_remote_dirs(sftp, &remote_path).await?;
            if preserve_mode {
                set_remote_path_mode(sftp, &remote_path, local_mode(&local_path)?).await?;
            }
            Box::pin(copy_local_dir_to_remote_recursive(
                sftp,
                &local_path,
                &remote_path,
                preserve_mode,
            ))
            .await?;
        } else if file_type.is_file() {
            copy_local_file_to_remote(sftp, &local_path, &remote_path, preserve_mode).await?;
        }
    }
    Ok(())
}

async fn copy_remote_dir_to_local(
    sftp: &SftpSession,
    remote_root: &Path,
    local_root: &Path,
    preserve_mode: bool,
) -> Result<()> {
    tokio::fs::create_dir_all(local_root).await?;
    if preserve_mode {
        let mode = remote_mode(&sftp.metadata(path_to_string(remote_root)?).await?)?;
        apply_local_mode(local_root, mode)?;
    }
    Box::pin(copy_remote_dir_to_local_recursive(
        sftp,
        remote_root,
        local_root,
        preserve_mode,
    ))
    .await
}

async fn copy_remote_dir_to_local_recursive(
    sftp: &SftpSession,
    remote_dir: &Path,
    local_dir: &Path,
    preserve_mode: bool,
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
            if preserve_mode {
                let mode = remote_mode(&metadata)?;
                apply_local_mode(&local_path, mode)?;
            }
            Box::pin(copy_remote_dir_to_local_recursive(
                sftp,
                &remote_path,
                &local_path,
                preserve_mode,
            ))
            .await?;
        } else {
            copy_remote_file_to_local(sftp, &remote_path, &local_path, preserve_mode).await?;
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

async fn set_remote_path_mode(sftp: &SftpSession, remote_path: &Path, mode: u32) -> Result<()> {
    let remote = path_to_string(remote_path)?;
    let mut attrs = sftp.metadata(remote.clone()).await?;
    attrs.permissions = Some(mode);
    sftp.set_metadata(remote, attrs).await?;
    Ok(())
}

fn path_to_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("path is not valid UTF-8: {}", path.display()))
}
