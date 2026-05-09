use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use russh::ChannelMsg;
use russh::client::Handle;
use russh_sftp::client::SftpSession;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;

use crate::config::{AppConfig, DirectAuth};
use crate::protocol::ServerEvent;

use super::Connection;
use super::shared::{
    ClientHandler, PtyShell, authenticate_with_key, authenticate_with_password,
    build_interactive_shell_command, build_remote_command, connect_handle, request_default_pty,
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
        config: &AppConfig,
    ) -> Result<i32> {
        let command = if config.ssh.pty {
            build_interactive_shell_command(argv)
        } else {
            build_remote_command(argv)
        };
        if config.ssh.pty {
            return self.execute_with_pty(&command, sender, config).await;
        }
        self.execute_without_pty(&command, sender).await
    }

    async fn copy(&mut self, spec: &CopySpec, _config: &AppConfig) -> Result<()> {
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(spec).await,
            CopyDirection::Download => self.copy_download(spec).await,
        }
    }
}

impl DirectSshConnection {
    async fn execute_without_pty(
        &mut self,
        command: &str,
        sender: &UnboundedSender<ServerEvent>,
    ) -> Result<i32> {
        let mut channel = self.handle.channel_open_session().await?;
        channel.exec(true, command).await?;
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

    async fn execute_with_pty(
        &mut self,
        command: &str,
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
    ) -> Result<i32> {
        let channel = self.handle.channel_open_session().await?;
        request_default_pty(&channel).await?;
        let mut shell = PtyShell::new(
            channel,
            vec!["$ ".to_string(), "# ".to_string()],
            config.ssh.connect_timeout,
        );
        shell.request_shell().await?;
        shell.wait_for_prompt().await?;
        shell.clear_prompt_remainder();
        shell.write_line("stty -echo").await?;
        shell.wait_for_prompt().await?;
        shell.clear_pending();
        shell.clear_prompt_remainder();
        let marker = shell.make_marker("__RHOP_EXEC__");
        let wrapped = shell.wrap_shell_command(command, marker.as_ref());
        shell.write_line(&wrapped).await?;
        let (status, _) = shell.read_until_sentinel(marker.as_ref(), Some(sender)).await?;
        shell.finish_roundtrip().await?;
        Ok(status)
    }
}

async fn probe_session(handle: &mut Handle<ClientHandler>) -> Result<()> {
    let channel = handle.channel_open_session().await?;
    drop(channel);
    Ok(())
}

impl DirectSshConnection {
    async fn copy_upload(&mut self, spec: &CopySpec) -> Result<()> {
        validate_direct_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let local = PathBuf::from(&spec.local_path);
        if spec.recursive {
            copy_local_dir_to_remote(&sftp, &local, Path::new(&spec.remote_path)).await
        } else {
            copy_local_file_to_remote(&sftp, &local, Path::new(&spec.remote_path)).await
        }
    }

    async fn copy_download(&mut self, spec: &CopySpec) -> Result<()> {
        validate_direct_copy_spec(spec)?;
        let sftp = self.open_sftp().await?;
        let local = PathBuf::from(&spec.local_path);
        let remote = Path::new(&spec.remote_path);
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
            sftp.create_dir(current_str)
                .await
                .with_context(|| format!("failed to create remote dir {}", current.display()))?;
        }
    }
    Ok(())
}

fn path_to_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("path is not valid UTF-8: {}", path.display()))
}
