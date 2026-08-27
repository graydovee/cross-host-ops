// Copy (`xho cp`) over a `TargetSession`'s sftp subsystem.
//
// The session's sftp subsystem is exposed as an `AsyncRead + AsyncWrite` via a
// duplex bridge, and a `russh_sftp` client runs over it. This unifies copy for
// direct (real sftp), local (spawned sftp-server), and tunnel (sftp forwarded
// over OpenSession) targets. Logic is ported from the legacy
// `DirectConnection` copy path; remote `~`-expansion is omitted (SFTP paths are
// used as-is), which only affects literal `~` remote paths.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use russh_sftp::client::SftpSession;
use russh_sftp::client::fs::Metadata as SftpMetadata;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::copy_frames::{
    copy_entry_name, join_relative_path, path_to_string, relative_path_to_string,
};
use crate::types::{CopyDirection, CopyFrame, CopySpec};

use super::TargetSession;

/// Open an SFTP client over a `TargetSession`'s sftp subsystem.
pub(crate) async fn open_sftp(sess: Box<dyn TargetSession>) -> Result<SftpSession> {
    let (writer, mut stream) = sess.split();
    writer.subsystem("sftp").await?;
    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let (mut rd, mut wr) = tokio::io::split(server);
        let mut buf = vec![0u8; 8192];
        loop {
            tokio::select! {
                ev = stream.next() => match ev {
                    Some(super::SessionEvent::Stdout(d)) | Some(super::SessionEvent::Stderr(d)) => {
                        if wr.write_all(&d).await.is_err() { break; }
                    }
                    Some(super::SessionEvent::ExitStatus(_))
                    | Some(super::SessionEvent::ExitSignal(_))
                    | Some(super::SessionEvent::Eof)
                    | None => {
                        let _ = wr.shutdown().await;
                        break;
                    }
                },
                n = rd.read(&mut buf) => match n {
                    Ok(0) => { let _ = writer.eof().await; break; }
                    Ok(n) => {
                        if writer.write_stdin(&buf[..n]).await.is_err() { break; }
                    }
                    Err(_) => break,
                },
            }
        }
    });
    SftpSession::new(client).await.context("sftp init")
}

/// Run a copy (upload or download) over an already-open SFTP session.
/// Consumes the frame channels from `spec`; on failure the download frame
/// sender is restored so a transport retry can stream into the same relay.
pub(crate) async fn run(sftp: &SftpSession, spec: &mut CopySpec) -> Result<()> {
    match spec.direction {
        CopyDirection::Upload => upload(sftp, spec).await,
        CopyDirection::Download => download(sftp, spec).await,
    }
}

async fn upload(sftp: &SftpSession, spec: &mut CopySpec) -> Result<()> {
    let remote_root = PathBuf::from(&spec.remote_path);
    let remote_root_is_dir = remote_path_is_dir(sftp, &remote_root).await;
    let mut upload_rx = spec
        .upload_rx
        .take()
        .ok_or_else(|| anyhow!("upload copy frame stream missing"))?;
    let mut current_file: Option<russh_sftp::client::fs::File> = None;
    // Final path of the file being written. Data goes to `<dest>.xho_tmp`
    // and is published by rename on EndFile — a failed transfer never leaves
    // a partial at the destination, and a `--resume` retry can continue the
    // tmp partial via a positioned write.
    let mut current_dest: Option<String> = None;

    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginFile {
                relative_path,
                size,
                start_offset,
                ..
            } => {
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
                    create_remote_dirs(sftp, parent).await?;
                }
                let dest = path_to_string(&remote_path)?;
                let tmp = format!("{}.xho_tmp", dest);
                // Resume offset comes from the frame (the client verified
                // the partial against its source prefix via the ack
                // handshake); cross-check the partial still has exactly that
                // many bytes before appending, else transfer fresh.
                let mut skip = start_offset.min(size);
                if skip > 0 {
                    match sftp.metadata(&tmp).await {
                        Ok(meta) if meta.len() == skip => {}
                        _ => skip = 0,
                    }
                }
                let file = if skip > 0 {
                    use tokio::io::AsyncSeekExt;
                    let mut file = sftp
                        .open_with_flags(
                            &tmp,
                            russh_sftp::protocol::OpenFlags::CREATE
                                | russh_sftp::protocol::OpenFlags::WRITE,
                        )
                        .await
                        .with_context(|| format!("failed to open remote partial {tmp}"))?;
                    file.seek(std::io::SeekFrom::Start(skip))
                        .await
                        .with_context(|| format!("failed to seek remote partial {tmp}"))?;
                    file
                } else {
                    sftp.create(&tmp)
                        .await
                        .with_context(|| format!("failed to create remote {tmp}"))?
                };
                current_dest = Some(dest);
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
                file.shutdown().await?;
                let dest = current_dest
                    .take()
                    .ok_or_else(|| anyhow!("copy stream sent EndFile before BeginFile"))?;
                let tmp = format!("{}.xho_tmp", dest);
                // SFTP rename does not overwrite: drop a stale destination
                // first (ignore absence), then publish the completed file.
                let _ = sftp.remove_file(&dest).await;
                sftp.rename(&tmp, &dest)
                    .await
                    .with_context(|| format!("failed to publish remote {dest}"))?;
            }
            CopyFrame::BeginDirectory { relative_path, .. } => {
                if !spec.recursive {
                    bail!("remote directory frame requires recursive copy");
                }
                let remote_path = join_relative_path(&remote_root, &relative_path)?;
                create_remote_dirs(sftp, &remote_path).await?;
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
                    create_remote_dirs(sftp, parent).await?;
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

async fn download(sftp: &SftpSession, spec: &mut CopySpec) -> Result<()> {
    let remote = Path::new(&spec.remote_path);
    let metadata = sftp
        .symlink_metadata(path_to_string(remote)?)
        .await
        .with_context(|| format!("failed to stat remote path {}", remote.display()))?;
    let download_tx = spec
        .download_tx
        .take()
        .ok_or_else(|| anyhow!("download copy frame stream missing"))?;
    let result = download_inner(sftp, spec, &metadata, &download_tx).await;
    if result.is_err() {
        // Restore the sender so a gateway retry reuses the same relay.
        spec.download_tx = Some(download_tx);
    }
    result
}

async fn download_inner(
    sftp: &SftpSession,
    spec: &mut CopySpec,
    metadata: &SftpMetadata,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    let remote = Path::new(&spec.remote_path);
    if metadata.is_dir() {
        if !spec.recursive {
            bail!("copying a remote directory requires -r");
        }
        send_remote_dir_frames(sftp, remote, Path::new(""), tx).await?;
    } else {
        let relative_path = copy_entry_name("", &spec.source_name, "copy");
        // Resume hint honored only when the remote source is unchanged.
        let start = spec
            .resume
            .iter()
            .find(|e| e.relative_path == relative_path)
            .filter(|e| {
                e.size == metadata.len()
                    && e.mtime == metadata.mtime.map(i64::from).unwrap_or(0)
                    && e.offset > 0
                    && e.offset <= metadata.len()
            })
            .map(|e| e.offset)
            .unwrap_or(0);
        send_remote_entry_frame(sftp, remote, Path::new(&relative_path), metadata, tx, start)
            .await?;
    }
    tx.send(CopyFrame::EndOfStream)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

/// Report remote partial-upload state (`<dest>.xho_tmp`) for the CLI's
/// resume decision: the partial's size and FULL sha256 (streamed through
/// the hasher — the bytes are read once and never buffered whole). The
/// client compares against its own source prefix and chooses the offset it
/// streams from. Single-file uploads only in v1.
pub(crate) async fn probe_upload_resume(
    sftp: &SftpSession,
    spec: &mut CopySpec,
) -> Vec<crate::types::ResumeEntry> {
    // Same dest semantics as the upload itself: a directory root means the
    // file (and its `.xho_tmp` partial) live INSIDE under the SOURCE name
    // (idempotent: the resolved path is a file, so re-resolution during the
    // copy after the probe is a no-op).
    if !spec.recursive {
        let root = PathBuf::from(&spec.remote_path);
        if remote_path_is_dir(sftp, &root).await {
            let name = spec
                .source_name
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .filter(|n| !n.is_empty())
                .unwrap_or("upload");
            spec.remote_path = path_to_string(&root.join(name)).unwrap_or(spec.remote_path.clone());
        }
    }
    let mut out = Vec::with_capacity(spec.resume.len());
    for entry in &spec.resume {
        let mut effective = entry.clone();
        effective.offset = 0;
        effective.partial_sha256 = String::new();
        if !spec.recursive {
            let tmp = format!("{}.xho_tmp", spec.remote_path);
            if let Ok(meta) = sftp.metadata(&tmp).await {
                let tmp_size = meta.len();
                if tmp_size > 0 && tmp_size <= entry.size {
                    if let Ok(mut file) = sftp.open(&tmp).await {
                        use sha2::{Digest, Sha256};
                        let mut hasher = Sha256::new();
                        let mut buf = vec![0u8; 64 * 1024];
                        let mut read = 0u64;
                        loop {
                            match file.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    hasher.update(&buf[..n]);
                                    read += n as u64;
                                }
                                Err(_) => {
                                    read = u64::MAX;
                                    break;
                                }
                            }
                        }
                        if read == tmp_size {
                            effective.offset = tmp_size;
                            effective.partial_sha256 = hasher
                                .finalize()
                                .iter()
                                .map(|b| format!("{b:02x}"))
                                .collect();
                        }
                    }
                }
            }
        }
        out.push(effective);
    }
    out
}

async fn remote_path_is_dir(sftp: &SftpSession, remote_path: &Path) -> bool {
    sftp.metadata(path_to_string(remote_path).unwrap_or_default())
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
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
        send_remote_entry_frame(sftp, &remote_path, &relative_path, &metadata, tx, 0).await?;
    }
    Ok(())
}

async fn send_remote_entry_frame(
    sftp: &SftpSession,
    remote_path: &Path,
    relative_path: &Path,
    metadata: &SftpMetadata,
    tx: &mpsc::Sender<CopyFrame>,
    start_offset: u64,
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
        start_offset,
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;

    let mut file = sftp
        .open(path_to_string(remote_path)?)
        .await
        .with_context(|| format!("failed to open remote {}", remote_path.display()))?;
    if start_offset > 0 {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(start_offset))
            .await
            .with_context(|| format!("failed to seek remote {}", remote_path.display()))?;
    }
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
