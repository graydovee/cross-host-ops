use std::io::{self, IsTerminal};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::expand_tilde;
use crate::copy_frames::{
    copy_entry_name, join_relative_path, local_basename, non_empty_name, path_is_existing_dir,
    relative_path_to_string, validate_relative_path, validate_upload_source,
};
use crate::protocol::rpc;
use crate::types::{CopyDirection, CopyFrame, CopySpec};

use super::client::connect_local_copy_client;
use super::progress::CopyProgressReporter;
use super::prompt::prompt_for_auth_input;

pub(crate) async fn run_copy(
    recursive: bool,
    quiet: bool,
    source: String,
    dest: String,
    timeout_ms: u64,
) -> Result<i32> {
    let CopyCliPlan {
        target,
        spec,
        local_path,
    } = parse_copy_operands(recursive, &source, &dest)?;
    if spec.direction == CopyDirection::Upload {
        validate_upload_source(Path::new(&local_path), recursive).await?;
    }
    let mut client = connect_local_copy_client().await?;
    let (tx, rx) = mpsc::channel(8);
    tx.send(crate::protocol::copy_spec_to_rpc(target, &spec, timeout_ms))
        .await
        .map_err(|_| anyhow!("failed to send copy start request"))?;

    let response = client.copy(ReceiverStream::new(rx)).await?;
    let show_progress = !quiet && io::stderr().is_terminal();
    if spec.direction == CopyDirection::Upload {
        spawn_copy_upload_frames(
            tx.clone(),
            PathBuf::from(&local_path),
            recursive,
            CopyProgressReporter::new(show_progress),
        );
    }
    let mut stream = response.into_inner();
    let mut download_writer = if spec.direction == CopyDirection::Download {
        Some(CopyDownloadWriter::new(
            PathBuf::from(&local_path),
            recursive,
            spec.source_name.clone(),
            CopyProgressReporter::new(show_progress),
        ))
    } else {
        None
    };
    while let Some(message) = stream.message().await? {
        match message
            .event
            .ok_or_else(|| anyhow!("copy stream returned empty event"))?
        {
            rpc::copy_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                tx.send(crate::protocol::copy_auth_input_request(
                    prompt.prompt_id,
                    value,
                ))
                .await
                .map_err(|_| anyhow!("failed to send copy auth input request"))?;
            }
            rpc::copy_response::Event::Error(error) => {
                eprintln!("error: {}", error.message);
                return Ok(1);
            }
            rpc::copy_response::Event::Complete(done) => {
                if !quiet && !done.message.is_empty() {
                    println!("{}", done.message);
                }
                break;
            }
            rpc::copy_response::Event::Info(info) => {
                if !quiet && !info.message.is_empty() {
                    eprintln!("{}", info.message);
                }
            }
            rpc::copy_response::Event::Frame(frame) => {
                let frame = crate::protocol::copy_frame_from_rpc(frame)?;
                if let Some(writer) = download_writer.as_mut() {
                    writer.apply(frame).await?;
                }
            }
        }
    }
    Ok(0)
}

fn spawn_copy_upload_frames(
    tx: mpsc::Sender<rpc::CopyRequest>,
    local_path: PathBuf,
    recursive: bool,
    progress: CopyProgressReporter,
) {
    tokio::spawn(async move {
        if let Err(error) = send_path_copy_frames(&tx, &local_path, recursive, progress).await {
            tracing::warn!(error = %error, path = %local_path.display(), "failed to stream copy upload frames");
        }
    });
}

async fn send_path_copy_frames(
    tx: &mpsc::Sender<rpc::CopyRequest>,
    local_path: &Path,
    recursive: bool,
    mut progress: CopyProgressReporter,
) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(local_path)
        .await
        .with_context(|| format!("failed to inspect {}", local_path.display()))?;

    if metadata.is_dir() {
        if !recursive {
            bail!(
                "{} is a directory; use -r to copy directories",
                local_path.display()
            );
        }
        tx.send(crate::protocol::copy_frame_request(
            CopyFrame::BeginDirectory {
                relative_path: String::new(),
                mode: metadata.permissions().mode(),
                mtime: metadata.mtime(),
            },
        ))
        .await
        .map_err(|_| anyhow!("failed to send root directory copy frame"))?;
        send_directory_contents_frames(tx, local_path, local_path, &mut progress).await?;
    } else {
        let relative_path = local_basename(local_path)?;
        send_path_entry_frame(tx, local_path, Path::new(&relative_path), &mut progress).await?;
    }

    tx.send(crate::protocol::copy_frame_request(CopyFrame::EndOfStream))
        .await
        .map_err(|_| anyhow!("failed to send copy end-of-stream frame"))?;
    Ok(())
}

async fn send_directory_contents_frames(
    tx: &mpsc::Sender<rpc::CopyRequest>,
    root: &Path,
    dir: &Path,
    progress: &mut CopyProgressReporter,
) -> Result<()> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current_dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current_dir)
            .await
            .with_context(|| format!("failed to read directory {}", current_dir.display()))?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let relative = path.strip_prefix(root).with_context(|| {
                format!("failed to derive relative path for {}", path.display())
            })?;
            let metadata = tokio::fs::symlink_metadata(&path)
                .await
                .with_context(|| format!("failed to inspect {}", path.display()))?;
            send_path_entry_frame_with_metadata(tx, &path, relative, &metadata, progress).await?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                stack.push(path);
            }
        }
    }
    Ok(())
}

async fn send_path_entry_frame(
    tx: &mpsc::Sender<rpc::CopyRequest>,
    path: &Path,
    relative_path: &Path,
    progress: &mut CopyProgressReporter,
) -> Result<()> {
    validate_relative_path(relative_path)?;
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    send_path_entry_frame_with_metadata(tx, path, relative_path, &metadata, progress).await
}

async fn send_path_entry_frame_with_metadata(
    tx: &mpsc::Sender<rpc::CopyRequest>,
    path: &Path,
    relative_path: &Path,
    metadata: &std::fs::Metadata,
    progress: &mut CopyProgressReporter,
) -> Result<()> {
    validate_relative_path(relative_path)?;
    let relative_path = relative_path_to_string(relative_path)?;
    if metadata.file_type().is_symlink() {
        let target = tokio::fs::read_link(path)
            .await
            .with_context(|| format!("failed to read symlink {}", path.display()))?;
        tx.send(crate::protocol::copy_frame_request(CopyFrame::Symlink {
            relative_path,
            target: target.to_string_lossy().to_string(),
        }))
        .await
        .map_err(|_| anyhow!("failed to send symlink copy frame"))?;
        return Ok(());
    }

    if metadata.is_dir() {
        tx.send(crate::protocol::copy_frame_request(
            CopyFrame::BeginDirectory {
                relative_path: relative_path.clone(),
                mode: metadata.permissions().mode(),
                mtime: metadata.mtime(),
            },
        ))
        .await
        .map_err(|_| anyhow!("failed to send directory copy frame"))?;
        return Ok(());
    }

    if !metadata.is_file() {
        bail!("unsupported file type for copy: {}", path.display());
    }

    progress.begin_file(relative_path.clone(), metadata.len());
    tx.send(crate::protocol::copy_frame_request(CopyFrame::BeginFile {
        relative_path: relative_path.clone(),
        mode: metadata.permissions().mode(),
        size: metadata.len(),
        mtime: metadata.mtime(),
    }))
    .await
    .map_err(|_| anyhow!("failed to send file copy frame"))?;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file
            .read(&mut buf)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        if n == 0 {
            break;
        }
        tx.send(crate::protocol::copy_frame_request(CopyFrame::FileData {
            data: buf[..n].to_vec(),
        }))
        .await
        .map_err(|_| anyhow!("failed to send file data copy frame"))?;
        progress.add_bytes(n);
    }
    tx.send(crate::protocol::copy_frame_request(CopyFrame::EndFile))
        .await
        .map_err(|_| anyhow!("failed to send end-file copy frame"))?;
    progress.finish_file();
    Ok(())
}

struct CopyDownloadWriter {
    dest: PathBuf,
    recursive: bool,
    source_name: String,
    root: Option<PathBuf>,
    current_file: Option<tokio::fs::File>,
    progress: CopyProgressReporter,
}

impl CopyDownloadWriter {
    fn new(
        dest: PathBuf,
        recursive: bool,
        source_name: String,
        progress: CopyProgressReporter,
    ) -> Self {
        Self {
            dest,
            recursive,
            source_name,
            root: None,
            current_file: None,
            progress,
        }
    }

    async fn apply(&mut self, frame: CopyFrame) -> Result<()> {
        match frame {
            CopyFrame::BeginFile {
                relative_path,
                mode,
                size,
                ..
            } => {
                let path = self.destination_for_file(&relative_path).await?;
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }
                let file = tokio::fs::File::create(&path)
                    .await
                    .with_context(|| format!("failed to create {}", path.display()))?;
                if mode != 0 {
                    let permissions = std::fs::Permissions::from_mode(mode);
                    tokio::fs::set_permissions(&path, permissions)
                        .await
                        .with_context(|| {
                            format!("failed to set permissions on {}", path.display())
                        })?;
                }
                self.progress
                    .begin_file(download_progress_name(&path, &relative_path), size);
                self.current_file = Some(file);
            }
            CopyFrame::FileData { data } => {
                let file = self
                    .current_file
                    .as_mut()
                    .ok_or_else(|| anyhow!("copy stream sent file data before BeginFile"))?;
                file.write_all(&data).await?;
                self.progress.add_bytes(data.len());
            }
            CopyFrame::EndFile => {
                if let Some(mut file) = self.current_file.take() {
                    file.flush().await?;
                }
                self.progress.finish_file();
            }
            CopyFrame::BeginDirectory {
                relative_path,
                mode,
                ..
            } => {
                if !self.recursive {
                    bail!("remote source is a directory; use -r to copy directories");
                }
                let root = self.download_root().await?;
                let path = join_relative_path(&root, &relative_path)?;
                tokio::fs::create_dir_all(&path)
                    .await
                    .with_context(|| format!("failed to create directory {}", path.display()))?;
                if mode != 0 {
                    let permissions = std::fs::Permissions::from_mode(mode);
                    tokio::fs::set_permissions(&path, permissions)
                        .await
                        .with_context(|| {
                            format!("failed to set permissions on {}", path.display())
                        })?;
                }
            }
            CopyFrame::Symlink {
                relative_path,
                target,
            } => {
                let path = if self.recursive {
                    let root = self.download_root().await?;
                    join_relative_path(&root, &relative_path)?
                } else {
                    self.destination_for_single_entry(&relative_path).await?
                };
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }
                let _ = tokio::fs::remove_file(&path).await;
                std::os::unix::fs::symlink(target, &path)
                    .with_context(|| format!("failed to create symlink {}", path.display()))?;
            }
            CopyFrame::EndOfStream => {
                if let Some(mut file) = self.current_file.take() {
                    file.flush().await?;
                }
                self.progress.finish_file();
            }
        }
        Ok(())
    }

    async fn destination_for_file(&mut self, relative_path: &str) -> Result<PathBuf> {
        if self.recursive {
            let root = self.download_root().await?;
            join_relative_path(&root, relative_path)
        } else {
            self.destination_for_single_entry(relative_path).await
        }
    }

    async fn destination_for_single_entry(&self, relative_path: &str) -> Result<PathBuf> {
        if path_is_existing_dir(&self.dest).await? {
            let name = copy_entry_name(relative_path, &self.source_name, "download");
            Ok(self.dest.join(name))
        } else {
            Ok(self.dest.clone())
        }
    }

    async fn download_root(&mut self) -> Result<PathBuf> {
        if let Some(root) = &self.root {
            return Ok(root.clone());
        }
        let root = if path_is_existing_dir(&self.dest).await? {
            self.dest
                .join(non_empty_name(&self.source_name, "download"))
        } else {
            self.dest.clone()
        };
        self.root = Some(root.clone());
        Ok(root)
    }
}

fn download_progress_name(path: &Path, relative_path: &str) -> String {
    if !relative_path.is_empty() {
        return relative_path.to_string();
    }
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("download")
        .to_string()
}

/// Dispatch a HostCommand to the appropriate handler function.

struct CopyCliPlan {
    target: String,
    spec: CopySpec,
    local_path: String,
}

fn parse_copy_operands(recursive: bool, source: &str, dest: &str) -> Result<CopyCliPlan> {
    let src_remote = parse_remote_spec(source);
    let dst_remote = parse_remote_spec(dest);
    match (src_remote, dst_remote) {
        (Some((target, remote_path)), None) => Ok(CopyCliPlan {
            target,
            spec: CopySpec {
                direction: CopyDirection::Download,
                remote_path: remote_path.clone(),
                recursive,
                source_name: remote_source_name(&remote_path),
                upload_rx: None,
                download_tx: None,
            },
            local_path: expand_tilde(dest)?,
        }),
        (None, Some((target, remote_path))) => {
            let local_path = expand_tilde(source)?;
            Ok(CopyCliPlan {
                target,
                spec: CopySpec {
                    direction: CopyDirection::Upload,
                    remote_path,
                    recursive,
                    source_name: local_basename(Path::new(&local_path))?,
                    upload_rx: None,
                    download_tx: None,
                },
                local_path,
            })
        }
        (Some(_), Some(_)) => bail!("copy supports exactly one remote operand"),
        (None, None) => bail!("copy requires one remote operand like host:/path"),
    }
}

fn remote_source_name(remote_path: &str) -> String {
    let trimmed = remote_path.trim_end_matches('/');
    Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("download")
        .to_string()
}

fn parse_remote_spec(value: &str) -> Option<(String, String)> {
    let colon_pos = value.rfind(':')?;
    let target = &value[..colon_pos];
    let path = &value[colon_pos + 1..];
    if target.is_empty()
        || path.is_empty()
        || target.contains('/')
        || target.contains('\\')
        || target == "."
        || target == ".."
    {
        return None;
    }
    Some((target.to_string(), path.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_spec_supports_xhod_qualified_targets() {
        assert_eq!(
            parse_remote_spec("remote-xhod:host1:/tmp/x"),
            Some(("remote-xhod:host1".to_string(), "/tmp/x".to_string()))
        );
    }

    #[test]
    fn parse_remote_spec_keeps_single_hop_behavior() {
        assert_eq!(
            parse_remote_spec("host1:/tmp/x"),
            Some(("host1".to_string(), "/tmp/x".to_string()))
        );
    }
}
