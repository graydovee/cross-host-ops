use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::types::CopyFrame;

pub(crate) fn path_to_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", path.display()))
}

pub(crate) fn relative_path_to_string(path: &Path) -> Result<String> {
    validate_relative_path(path)?;
    path_to_string(path)
}

pub(crate) fn validate_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute() {
        bail!(
            "copy frame relative path must not be absolute: {}",
            path.display()
        );
    }
    for component in path.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!(
                    "copy frame relative path contains invalid component: {}",
                    path.display()
                );
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }
    Ok(())
}

pub(crate) fn join_relative_path(root: &Path, relative_path: &str) -> Result<PathBuf> {
    if relative_path.is_empty() {
        return Ok(root.to_path_buf());
    }
    let relative = Path::new(relative_path);
    validate_relative_path(relative)?;
    Ok(root.join(relative))
}

pub(crate) fn non_empty_name(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn copy_entry_name(relative_path: &str, source_name: &str, fallback: &str) -> String {
    Path::new(relative_path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .or_else(|| (!source_name.trim().is_empty()).then_some(source_name.trim()))
        .unwrap_or(fallback)
        .to_string()
}

pub(crate) fn local_basename(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("failed to derive basename from {}", path.display()))
}

pub(crate) async fn path_is_existing_dir(path: &Path) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_dir()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

pub(crate) async fn validate_upload_source(path: &Path, recursive: bool) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to inspect upload source {}", path.display()))?;
    if metadata.is_dir() && !recursive {
        bail!(
            "{} is a directory; use -r to copy directories",
            path.display()
        );
    }
    Ok(())
}

pub(crate) async fn materialize_frames_to_dir(
    root: &Path,
    upload_rx: &mut mpsc::Receiver<CopyFrame>,
) -> Result<()> {
    let mut current_file: Option<tokio::fs::File> = None;
    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginDirectory {
                relative_path,
                mode,
                ..
            } => {
                let path = join_relative_path(root, &relative_path)?;
                tokio::fs::create_dir_all(&path)
                    .await
                    .with_context(|| format!("failed to create {}", path.display()))?;
                if mode != 0 {
                    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                        .await
                        .with_context(|| format!("failed to chmod {}", path.display()))?;
                }
            }
            CopyFrame::BeginFile {
                relative_path,
                mode,
                ..
            } => {
                if current_file.is_some() {
                    bail!("copy stream began a new file before ending the previous file");
                }
                let path = join_relative_path(root, &relative_path)?;
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }
                let file = tokio::fs::File::create(&path)
                    .await
                    .with_context(|| format!("failed to create {}", path.display()))?;
                if mode != 0 {
                    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                        .await
                        .with_context(|| format!("failed to chmod {}", path.display()))?;
                }
                current_file = Some(file);
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
                file.flush().await?;
            }
            CopyFrame::Symlink {
                relative_path,
                target,
            } => {
                let path = join_relative_path(root, &relative_path)?;
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }
                let _ = tokio::fs::remove_file(&path).await;
                std::os::unix::fs::symlink(target, &path)
                    .with_context(|| format!("failed to create symlink {}", path.display()))?;
            }
            CopyFrame::EndOfStream => break,
        }
    }
    if current_file.is_some() {
        bail!("copy stream ended before EndFile");
    }
    Ok(())
}

pub(crate) async fn collect_single_file_upload(
    upload_rx: &mut mpsc::Receiver<CopyFrame>,
) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    let mut in_file = false;
    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginFile { .. } if !in_file => {
                in_file = true;
            }
            CopyFrame::FileData { data: chunk } if in_file => {
                data.extend_from_slice(&chunk);
            }
            CopyFrame::EndFile if in_file => {
                in_file = false;
            }
            CopyFrame::EndOfStream => break,
            CopyFrame::BeginDirectory { .. } => {
                bail!("non-recursive jumpserver upload received a directory frame");
            }
            CopyFrame::Symlink { .. } => {
                bail!("non-recursive jumpserver symlink upload is not supported");
            }
            other => bail!("unexpected copy frame in single-file upload: {:?}", other),
        }
    }
    if in_file {
        bail!("copy stream ended before EndFile");
    }
    Ok(data)
}

pub(crate) async fn emit_local_path_frames(
    root: &Path,
    relative_root: &Path,
    include_root: bool,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(root)
        .await
        .with_context(|| format!("failed to inspect {}", root.display()))?;
    if include_root {
        send_local_entry_frame(root, relative_root, &metadata, tx).await?;
    }
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        let mut stack = vec![(root.to_path_buf(), relative_root.to_path_buf())];
        while let Some((dir, rel_dir)) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .with_context(|| format!("failed to read {}", dir.display()))?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let rel = rel_dir.join(entry.file_name());
                let metadata = tokio::fs::symlink_metadata(&path)
                    .await
                    .with_context(|| format!("failed to inspect {}", path.display()))?;
                send_local_entry_frame(&path, &rel, &metadata, tx).await?;
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    stack.push((path, rel));
                }
            }
        }
    }
    Ok(())
}

async fn send_local_entry_frame(
    path: &Path,
    relative_path: &Path,
    metadata: &std::fs::Metadata,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    validate_relative_path(relative_path)?;
    let relative_path = relative_path_to_string(relative_path)?;
    if metadata.file_type().is_symlink() {
        let target = tokio::fs::read_link(path)
            .await
            .with_context(|| format!("failed to read symlink {}", path.display()))?;
        tx.send(CopyFrame::Symlink {
            relative_path,
            target: target.to_string_lossy().to_string(),
        })
        .await
        .map_err(|_| anyhow!("copy frame stream closed"))?;
        return Ok(());
    }
    if metadata.is_dir() {
        tx.send(CopyFrame::BeginDirectory {
            relative_path,
            mode: metadata.permissions().mode(),
            mtime: metadata.mtime(),
        })
        .await
        .map_err(|_| anyhow!("copy frame stream closed"))?;
        return Ok(());
    }
    if !metadata.is_file() {
        bail!("unsupported file type for copy: {}", path.display());
    }
    tx.send(CopyFrame::BeginFile {
        relative_path,
        mode: metadata.permissions().mode(),
        size: metadata.len(),
        mtime: metadata.mtime(),
    })
    .await
    .map_err(|_| anyhow!("copy frame stream closed"))?;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        tx.send(CopyFrame::FileData {
            data: buf[..n].to_vec(),
        })
        .await
        .map_err(|_| anyhow!("copy frame stream closed"))?;
    }
    tx.send(CopyFrame::EndFile)
        .await
        .map_err(|_| anyhow!("copy frame stream closed"))?;
    Ok(())
}
