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

/// Sidecar for a resumable download: JSON kept next to the `.part` file,
/// fingerprinting the remote source so the next `--resume` run can validate
/// that nothing changed before appending.
#[derive(serde::Deserialize, serde::Serialize)]
struct DownloadSidecar {
    target: String,
    remote_path: String,
    relative_path: String,
    offset: u64,
    size: u64,
    mtime: i64,
}

/// Sidecar for a resumable upload: JSON kept next to the local source,
/// fingerprinting the local file (the remote partial is probed by the
/// daemon and answered via `resume_ack`).
#[derive(serde::Deserialize, serde::Serialize)]
struct UploadSidecar {
    target: String,
    remote_path: String,
    size: u64,
    mtime: i64,
}

/// Paths for a resumable download's partial data and sidecar.
fn download_part_paths(dest: &str) -> (PathBuf, PathBuf) {
    let part = PathBuf::from(format!("{dest}.part"));
    let meta = PathBuf::from(format!("{}.meta", part.display()));
    (part, meta)
}

/// Load a download sidecar when it matches this transfer and the local
/// `.part` really holds the recorded prefix.
async fn read_download_sidecar(
    dest: &str,
    target: &str,
    remote_path: &str,
) -> Option<(DownloadSidecar, PathBuf, PathBuf)> {
    let (part, meta) = download_part_paths(dest);
    let text = tokio::fs::read_to_string(&meta).await.ok()?;
    let sidecar: DownloadSidecar = serde_json::from_str(&text).ok()?;
    if sidecar.target != target || sidecar.remote_path != remote_path || sidecar.offset == 0 {
        return None;
    }
    let part_len = tokio::fs::metadata(&part).await.ok()?.len();
    if part_len != sidecar.offset {
        return None;
    }
    Some((sidecar, part, meta))
}

/// Load an upload sidecar when the local source is unchanged.
async fn read_upload_sidecar(
    local_path: &str,
    target: &str,
    remote_path: &str,
) -> Option<(UploadSidecar, PathBuf)> {
    let meta = PathBuf::from(format!("{local_path}.xho_resume"));
    let text = tokio::fs::read_to_string(&meta).await.ok()?;
    let sidecar: UploadSidecar = serde_json::from_str(&text).ok()?;
    if sidecar.target != target || sidecar.remote_path != remote_path {
        return None;
    }
    let stat = tokio::fs::metadata(local_path).await.ok()?;
    if stat.len() != sidecar.size || stat.mtime() != sidecar.mtime {
        return None;
    }
    Some((sidecar, meta))
}

/// sha256 (hex) of the first `upto` bytes of `path`; None when the file is
/// shorter (a truncated prefix never matches a full-length hash).
async fn prefix_sha256(path: &str, upto: u64) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await.ok()?;
    let mut remaining = upto;
    let mut buf = vec![0u8; 64 * 1024];
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        file.read_exact(&mut buf[..want]).await.ok()?;
        hasher.update(&buf[..want]);
        remaining -= want as u64;
    }
    Some(
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}

/// State the download writer needs to honor a resume hint.
#[derive(Clone)]
struct DownloadResumeCtx {
    part: PathBuf,
    meta: PathBuf,
    expected_offset: u64,
    target: String,
    remote_path: String,
}

pub(crate) async fn run_copy(
    recursive: bool,
    resume: bool,
    quiet: bool,
    yes: bool,
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

    // Resume preparation (single-file transfers only in v1): collect hints
    // from previous `--resume` sidecars; the daemon validates them against
    // remote state before use.
    let mut resume_entries: Vec<crate::types::ResumeEntry> = Vec::new();
    let mut dl_resume: Option<DownloadResumeCtx> = None;
    let mut upload_sidecar: Option<(UploadSidecar, PathBuf)> = None;
    if resume && !recursive {
        match spec.direction {
            CopyDirection::Download => {
                // `.part` bookkeeping engages whenever the flag is set, so the
                // FIRST interrupted run already leaves a resumable partial;
                // an existing valid sidecar additionally seeds the offset.
                let (part, meta) = download_part_paths(&local_path);
                let expected_offset =
                    read_download_sidecar(&local_path, &target, &spec.remote_path)
                        .await
                        .map(|(sidecar, _, _)| {
                            resume_entries.push(crate::types::ResumeEntry {
                                relative_path: sidecar.relative_path.clone(),
                                offset: sidecar.offset,
                                size: sidecar.size,
                                mtime: sidecar.mtime,
                                partial_sha256: String::new(),
                            });
                            sidecar.offset
                        })
                        .unwrap_or(0);
                dl_resume = Some(DownloadResumeCtx {
                    part,
                    meta,
                    expected_offset,
                    target: target.clone(),
                    remote_path: spec.remote_path.clone(),
                });
            }
            CopyDirection::Upload => {
                // Ask the daemon for the remote partial's state; the resume
                // decision (append vs fresh) is made HERE after the ack,
                // by verifying the partial's full hash against the local
                // source prefix.
                if let Ok(stat) = tokio::fs::metadata(&local_path).await {
                    let sidecar = UploadSidecar {
                        target: target.clone(),
                        remote_path: spec.remote_path.clone(),
                        size: stat.len(),
                        mtime: stat.mtime(),
                    };
                    resume_entries.push(crate::types::ResumeEntry {
                        relative_path: spec.source_name.clone(),
                        offset: 0,
                        size: sidecar.size,
                        mtime: sidecar.mtime,
                        partial_sha256: String::new(),
                    });
                    let meta = PathBuf::from(format!("{local_path}.xho_resume"));
                    if let Ok(text) = serde_json::to_string(&sidecar) {
                        let _ = tokio::fs::write(&meta, text).await;
                    }
                    upload_sidecar = Some((sidecar, meta));
                }
            }
        }
    }

    tx.send(crate::protocol::copy_spec_to_rpc(
        target.clone(),
        &spec,
        timeout_ms,
        &resume_entries,
    ))
    .await
    .map_err(|_| anyhow!("failed to send copy start request"))?;

    let response = client.copy(ReceiverStream::new(rx)).await?;
    let show_progress = !quiet && io::stderr().is_terminal();

    // The upload feeder spawns immediately without resume hints, or lazily
    // when the daemon's resume_ack carries the effective offset (an old
    // daemon never acks — the first Frame event triggers a fresh spawn).
    let mut upload_spawned = false;
    if spec.direction == CopyDirection::Upload && upload_sidecar.is_none() {
        spawn_copy_upload_frames(
            tx.clone(),
            PathBuf::from(&local_path),
            recursive,
            CopyProgressReporter::new(show_progress),
            0,
        );
        upload_spawned = true;
    }
    let mut stream = response.into_inner();
    let mut download_writer = if spec.direction == CopyDirection::Download {
        Some(CopyDownloadWriter::new(
            PathBuf::from(&local_path),
            recursive,
            spec.source_name.clone(),
            CopyProgressReporter::new(show_progress),
            dl_resume,
        ))
    } else {
        None
    };
    let outcome = async {
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
                    return Err(super::classify_daemon_error(&error.message).into());
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
                    // Frames on an upload before any ack: old daemon treating
                    // the request as a fresh transfer.
                    if spec.direction == CopyDirection::Upload && !upload_spawned {
                        spawn_copy_upload_frames(
                            tx.clone(),
                            PathBuf::from(&local_path),
                            recursive,
                            CopyProgressReporter::new(show_progress),
                            0,
                        );
                        upload_spawned = true;
                    }
                    let frame = crate::protocol::copy_frame_from_rpc(frame)?;
                    if let Some(writer) = download_writer.as_mut() {
                        writer.apply(frame).await?;
                    }
                }
                rpc::copy_response::Event::ResumeAck(ack) => {
                    // Resume only when the remote partial's FULL hash equals
                    // the local source prefix of the same length — the only
                    // sound proof that appending reproduces the source (a
                    // head fingerprint would pass a same-header rewrite).
                    if spec.direction == CopyDirection::Upload && !upload_spawned {
                        let candidate = ack
                            .entries
                            .iter()
                            .find(|e| e.relative_path == spec.source_name)
                            .filter(|e| e.offset > 0)
                            .filter(|e| !e.partial_sha256.is_empty());
                        let skip = match candidate {
                            Some(e) => {
                                let local_hash = prefix_sha256(&local_path, e.offset).await;
                                if local_hash.as_deref() == Some(e.partial_sha256.as_str()) {
                                    eprintln!("resuming upload at byte {}", e.offset);
                                    e.offset
                                } else {
                                    eprintln!(
                                        "remote partial does not match this source; transferring fresh"
                                    );
                                    0
                                }
                            }
                            None => 0,
                        };
                        spawn_copy_upload_frames(
                            tx.clone(),
                            PathBuf::from(&local_path),
                            recursive,
                            CopyProgressReporter::new(show_progress),
                            skip,
                        );
                        upload_spawned = true;
                    }
                }
                rpc::copy_response::Event::ReviewResult(_result) => {
                    // Review outcome surfaced for structured-output consumers; the
                    // human copy path relies on ConfirmRequired below for interaction.
                }
                rpc::copy_response::Event::ConfirmRequired(confirm) => {
                    let allow = super::prompt::prompt_for_confirmation(&confirm.reason, yes)?;
                    tx.send(crate::protocol::copy_confirm_request(
                        crate::protocol::parse_execution_id(&confirm.execution_id)?,
                        allow,
                    ))
                    .await
                    .map_err(|_| anyhow!("failed to send copy confirmation request"))?;
                    if !allow {
                        break;
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    match (&outcome, download_writer.as_mut()) {
        (Err(_), Some(writer)) => {
            // Resume mode: keep the `.part` and record how much of it is
            // valid so the next `--resume` run appends from here. Without
            // resume the truncated in-progress file is removed.
            writer.handle_failure().await;
        }
        (Ok(()), Some(writer)) => {
            // Publish the completed `.part` over the destination.
            writer.publish().await?;
        }
        _ => {}
    }
    if outcome.is_ok() {
        if let Some((_, meta)) = &upload_sidecar {
            let _ = tokio::fs::remove_file(meta).await;
        }
    }
    outcome?;
    Ok(0)
}

fn spawn_copy_upload_frames(
    tx: mpsc::Sender<rpc::CopyRequest>,
    local_path: PathBuf,
    recursive: bool,
    progress: CopyProgressReporter,
    skip: u64,
) {
    tokio::spawn(async move {
        if let Err(error) = send_path_copy_frames(&tx, &local_path, recursive, progress, skip).await
        {
            tracing::warn!(error = %error, path = %local_path.display(), "failed to stream copy upload frames");
        }
    });
}

async fn send_path_copy_frames(
    tx: &mpsc::Sender<rpc::CopyRequest>,
    local_path: &Path,
    recursive: bool,
    mut progress: CopyProgressReporter,
    skip: u64,
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
        if skip > 0 {
            send_path_entry_frame_from(
                tx,
                local_path,
                Path::new(&relative_path),
                &mut progress,
                skip,
            )
            .await?;
        } else {
            send_path_entry_frame(tx, local_path, Path::new(&relative_path), &mut progress).await?;
        }
    }

    tx.send(crate::protocol::copy_frame_request(CopyFrame::EndOfStream))
        .await
        .map_err(|_| anyhow!("failed to send copy end-of-stream frame"))?;
    Ok(())
}

/// Upload a single file starting at byte `skip` (resume): the BeginFile
/// still declares the FULL size (the daemon expects only the remaining
/// suffix and appends remotely); the local read seeks past the prefix.
async fn send_path_entry_frame_from(
    tx: &mpsc::Sender<rpc::CopyRequest>,
    path: &Path,
    relative_path: &Path,
    progress: &mut CopyProgressReporter,
    skip: u64,
) -> Result<()> {
    use tokio::io::AsyncSeekExt;
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    let size = metadata.len();
    anyhow::ensure!(
        skip <= size,
        "resume offset {skip} beyond local file size {size}"
    );

    progress.begin_file(
        crate::copy_frames::relative_path_to_string(relative_path)?,
        size,
        skip,
    );
    tx.send(crate::protocol::copy_frame_request(CopyFrame::BeginFile {
        relative_path: relative_path_to_string(relative_path)?,
        mode: metadata.permissions().mode(),
        size,
        mtime: metadata.mtime(),
        start_offset: skip,
    }))
    .await
    .map_err(|_| anyhow!("failed to send file copy frame"))?;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.seek(std::io::SeekFrom::Start(skip))
        .await
        .with_context(|| format!("failed to seek {}", path.display()))?;
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

    progress.begin_file(relative_path.clone(), metadata.len(), 0);
    tx.send(crate::protocol::copy_frame_request(CopyFrame::BeginFile {
        relative_path: relative_path.clone(),
        mode: metadata.permissions().mode(),
        size: metadata.len(),
        mtime: metadata.mtime(),
        start_offset: 0,
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
    /// Path of the file currently being written; cleared on EndFile. Used to
    /// remove a truncated transfer when the copy fails mid-file (non-resume
    /// mode) or to record resume state.
    incomplete_path: Option<PathBuf>,
    /// Resume context (single-file `--resume`): data goes to `.part`, a
    /// failure keeps it with a sidecar, success renames over the dest.
    resume: Option<DownloadResumeCtx>,
    /// Total valid bytes of the current file (resume baseline + received).
    applied: u64,
    /// BeginFile fingerprint of the current file for the failure sidecar.
    current_size: u64,
    current_mtime: i64,
    current_relative_path: String,
    progress: CopyProgressReporter,
}

impl CopyDownloadWriter {
    fn new(
        dest: PathBuf,
        recursive: bool,
        source_name: String,
        progress: CopyProgressReporter,
        resume: Option<DownloadResumeCtx>,
    ) -> Self {
        Self {
            dest,
            recursive,
            source_name,
            root: None,
            current_file: None,
            incomplete_path: None,
            resume,
            applied: 0,
            current_size: 0,
            current_mtime: 0,
            current_relative_path: String::new(),
            progress,
        }
    }

    /// Handle a failed transfer: in resume mode keep the `.part` and record
    /// how much of it is valid (sidecar) so the next `--resume` run appends;
    /// otherwise remove the truncated file so no corrupt look-alike remains.
    async fn handle_failure(&mut self) {
        if self.resume.is_some() {
            if let Some(mut file) = self.current_file.take() {
                let _ = file.flush().await;
            }
            if self.applied > 0 {
                if let Some(ctx) = self.resume.clone() {
                    let sidecar = DownloadSidecar {
                        target: ctx.target,
                        remote_path: ctx.remote_path,
                        relative_path: self.current_relative_path.clone(),
                        offset: self.applied,
                        size: self.current_size,
                        mtime: self.current_mtime,
                    };
                    if let Ok(text) = serde_json::to_string(&sidecar) {
                        let _ = tokio::fs::write(&ctx.meta, text).await;
                    }
                }
            }
            self.incomplete_path = None;
        } else {
            self.cleanup_incomplete().await;
        }
    }

    /// Publish a completed transfer: in resume mode rename the `.part` over
    /// the destination and drop the sidecar.
    async fn publish(&mut self) -> Result<()> {
        if let Some(ctx) = self.resume.take() {
            tokio::fs::rename(&ctx.part, &self.dest)
                .await
                .with_context(|| {
                    format!(
                        "failed to publish {} to {}",
                        ctx.part.display(),
                        self.dest.display()
                    )
                })?;
            let _ = tokio::fs::remove_file(&ctx.meta).await;
        }
        Ok(())
    }

    /// Remove the in-progress (truncated) file, if any. Files that already
    /// received EndFile are complete and are kept.
    async fn cleanup_incomplete(&mut self) {
        self.current_file = None;
        if let Some(path) = self.incomplete_path.take() {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    async fn apply(&mut self, frame: CopyFrame) -> Result<()> {
        match frame {
            CopyFrame::BeginFile {
                relative_path,
                mode,
                size,
                mtime,
                start_offset,
            } => {
                let (path, file, baseline) = if let Some(ctx) = self.resume.as_ref() {
                    // Resume mode: data lands in `.part`. A non-zero start
                    // offset must equal the recorded prefix length — the
                    // daemon validated our hint against the same source.
                    let path = ctx.part.clone();
                    let file = if start_offset > 0 {
                        anyhow::ensure!(
                            start_offset == ctx.expected_offset,
                            "daemon resumed at byte {start_offset} but local partial holds {}",
                            ctx.expected_offset
                        );
                        tokio::fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&path)
                            .await
                            .with_context(|| format!("failed to append {}", path.display()))?
                    } else {
                        tokio::fs::File::create(&path)
                            .await
                            .with_context(|| format!("failed to create {}", path.display()))?
                    };
                    (path, file, start_offset)
                } else {
                    let path = self.destination_for_file(&relative_path).await?;
                    if let Some(parent) = path.parent() {
                        if !parent.as_os_str().is_empty() {
                            tokio::fs::create_dir_all(parent).await?;
                        }
                    }
                    let file = tokio::fs::File::create(&path)
                        .await
                        .with_context(|| format!("failed to create {}", path.display()))?;
                    (path, file, 0)
                };
                if mode != 0 && self.resume.is_none() {
                    let permissions = std::fs::Permissions::from_mode(mode);
                    tokio::fs::set_permissions(&path, permissions)
                        .await
                        .with_context(|| {
                            format!("failed to set permissions on {}", path.display())
                        })?;
                }
                self.progress.begin_file(
                    download_progress_name(&path, &relative_path),
                    size,
                    baseline,
                );
                self.applied = baseline;
                self.current_size = size;
                self.current_mtime = mtime;
                self.current_relative_path = relative_path;
                self.current_file = Some(file);
                self.incomplete_path = Some(path);
            }
            CopyFrame::FileData { data } => {
                let file = self
                    .current_file
                    .as_mut()
                    .ok_or_else(|| anyhow!("copy stream sent file data before BeginFile"))?;
                file.write_all(&data).await?;
                self.applied += data.len() as u64;
                self.progress.add_bytes(data.len());
            }
            CopyFrame::EndFile => {
                if let Some(mut file) = self.current_file.take() {
                    file.flush().await?;
                }
                self.incomplete_path = None;
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
                resume: Vec::new(),
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
                    resume: Vec::new(),
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

    #[tokio::test]
    async fn download_sidecar_rejects_stale_or_mismatched_state() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("file.bin");
        let dest = dest.to_str().unwrap().to_string();
        let (part, meta) = download_part_paths(&dest);

        // No sidecar at all.
        assert!(read_download_sidecar(&dest, "t1", "/r/p").await.is_none());

        // Sidecar for a different transfer target.
        let sidecar = DownloadSidecar {
            target: "other".into(),
            remote_path: "/r/p".into(),
            relative_path: "p".into(),
            offset: 10,
            size: 100,
            mtime: 5,
        };
        tokio::fs::write(&meta, serde_json::to_string(&sidecar).unwrap())
            .await
            .unwrap();
        assert!(read_download_sidecar(&dest, "t1", "/r/p").await.is_none());

        // Matching sidecar but the .part length disagrees with the offset.
        let sidecar = DownloadSidecar {
            target: "t1".into(),
            remote_path: "/r/p".into(),
            relative_path: "p".into(),
            offset: 10,
            size: 100,
            mtime: 5,
        };
        tokio::fs::write(&meta, serde_json::to_string(&sidecar).unwrap())
            .await
            .unwrap();
        tokio::fs::write(&part, b"short").await.unwrap();
        assert!(read_download_sidecar(&dest, "t1", "/r/p").await.is_none());

        // Valid: part holds exactly `offset` bytes.
        tokio::fs::write(&part, vec![0u8; 10]).await.unwrap();
        let (loaded, _, _) = read_download_sidecar(&dest, "t1", "/r/p")
            .await
            .expect("valid sidecar");
        assert_eq!(loaded.offset, 10);
        assert_eq!(loaded.relative_path, "p");
    }

    #[tokio::test]
    async fn upload_sidecar_rejects_changed_local_source() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("up.bin");
        let local_str = local.to_str().unwrap().to_string();
        tokio::fs::write(&local, b"0123456789").await.unwrap();
        let stat = tokio::fs::metadata(&local).await.unwrap();

        let sidecar = UploadSidecar {
            target: "t1".into(),
            remote_path: "/r/up".into(),
            size: stat.len(),
            mtime: stat.mtime(),
        };
        let meta = PathBuf::from(format!("{local_str}.xho_resume"));
        tokio::fs::write(&meta, serde_json::to_string(&sidecar).unwrap())
            .await
            .unwrap();
        let (loaded, _) = read_upload_sidecar(&local_str, "t1", "/r/up")
            .await
            .expect("valid sidecar");
        assert_eq!(loaded.size, stat.len());

        // Rewrite the source: size/mtime no longer match → no resume.
        tokio::fs::write(&local, b"changed-content").await.unwrap();
        assert!(
            read_upload_sidecar(&local_str, "t1", "/r/up")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn prefix_sha256_covers_exactly_upto_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.bin");
        let path_str = path.to_str().unwrap().to_string();
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&path, &data).await.unwrap();
        use sha2::{Digest, Sha256};
        let expect = |n: usize| -> String {
            Sha256::digest(&data[..n])
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        };
        // Full length and arbitrary prefixes (incl. non-multiples of the
        // 64 KiB read buffer).
        assert_eq!(prefix_sha256(&path_str, 5000).await.unwrap(), expect(5000));
        assert_eq!(prefix_sha256(&path_str, 1234).await.unwrap(), expect(1234));
        assert_eq!(prefix_sha256(&path_str, 1).await.unwrap(), expect(1));
        // Asking for more than the file has yields None (never a partial
        // read that could falsely match).
        assert!(prefix_sha256(&path_str, 5001).await.is_none());
    }

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
