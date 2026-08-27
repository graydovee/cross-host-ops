//! In-process end-to-end copy tests against the `_self` (localhost) gateway.
//!
//! These drive the REAL daemon copy path — RPC handler, review/audit wiring,
//! gateway dispatch, `LocalSession` sftp subsystem (a genuine `sftp-server`
//! process), `sftp_copy` frame logic, and the resume protocol — with no SSH
//! and no network beyond the harness's in-memory duplex.

mod support;

use std::path::{Path, PathBuf};

use support::in_process_rpc::InProcessRpcHarness;
use xho::protocol::rpc;

/// Deterministic pseudo-random payload (LCG) — no rand dependency needed.
/// `seed` differentiates payloads so prefix-collision tests stay honest.
fn payload(len: usize) -> Vec<u8> {
    payload_seeded(len, 0x2545F491)
}

fn payload_seeded(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        })
        .collect()
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

struct DownloadOutcome {
    begin: rpc::CopyBeginFile,
    data: Vec<u8>,
    ended_cleanly: bool,
    errored: Option<String>,
}

fn parse_download_events(events: Vec<rpc::CopyResponse>) -> DownloadOutcome {
    let mut begin = None;
    let mut data = Vec::new();
    let mut ended_cleanly = false;
    let mut errored = None;
    for event in events {
        match event.event.expect("non-empty event") {
            rpc::copy_response::Event::Frame(frame) => match frame.frame.expect("frame") {
                rpc::copy_frame::Frame::BeginFile(b) => begin = Some(b),
                rpc::copy_frame::Frame::FileData(d) => data.extend_from_slice(&d.data),
                rpc::copy_frame::Frame::EndFile(_) => {}
                rpc::copy_frame::Frame::EndOfStream(_) => ended_cleanly = true,
                _ => {}
            },
            rpc::copy_response::Event::Complete(_) => ended_cleanly = true,
            rpc::copy_response::Event::Error(e) => errored = Some(e.message),
            _ => {}
        }
    }
    DownloadOutcome {
        begin: begin.expect("BeginFile frame"),
        data,
        ended_cleanly,
        errored,
    }
}

fn start_request(target: &str, remote_path: &str, source_name: &str) -> rpc::CopyStartRequest {
    rpc::CopyStartRequest {
        target: target.to_string(),
        remote_path: remote_path.to_string(),
        recursive: false,
        direction: rpc::CopyDirection::Download as i32,
        timeout_ms: 0,
        source_name: source_name.to_string(),
        ..Default::default()
    }
}

/// Client-side resume decision, mirroring the CLI: verify the ack's partial
/// hash against the local source prefix, then stream from the chosen offset.
/// Returns (skip, ack events decision message).
async fn client_decide_upload_skip(events: &[rpc::CopyResponse], name: &str, source: &[u8]) -> u64 {
    let candidate = events.iter().find_map(|e| match e.event.as_ref() {
        Some(rpc::copy_response::Event::ResumeAck(ack)) => ack
            .entries
            .iter()
            .find(|e| e.relative_path == name)
            .filter(|e| e.offset > 0 && !e.partial_sha256.is_empty()),
        _ => None,
    });
    match candidate {
        Some(e) => {
            use sha2::{Digest, Sha256};
            let local: String = Sha256::digest(&source[..e.offset as usize])
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            if local == e.partial_sha256 {
                e.offset
            } else {
                0
            }
        }
        None => 0,
    }
}

fn upload_frame_stream(name: &str, size: u64, mtime: i64, bytes: &[u8]) -> Vec<rpc::CopyFrame> {
    let mut frames = vec![rpc::CopyFrame {
        frame: Some(rpc::copy_frame::Frame::BeginFile(rpc::CopyBeginFile {
            relative_path: name.to_string(),
            mode: 0o644,
            size,
            mtime,
            start_offset: 0,
        })),
    }];
    for chunk in bytes.chunks(32 * 1024) {
        frames.push(rpc::CopyFrame {
            frame: Some(rpc::copy_frame::Frame::FileData(rpc::CopyFileData {
                data: chunk.to_vec(),
            })),
        });
    }
    frames.push(rpc::CopyFrame {
        frame: Some(rpc::copy_frame::Frame::EndFile(rpc::CopyEndFile {})),
    });
    frames.push(rpc::CopyFrame {
        frame: Some(rpc::copy_frame::Frame::EndOfStream(rpc::CopyEndOfStream {})),
    });
    frames
}

/// Scratch dir for source/destination files (shared filesystem with the
/// in-process daemon — the `_self` gateway's "remote" IS this host).
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("xho-e2e-copy-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn file_name(path: &Path) -> String {
    path.file_name().unwrap().to_str().unwrap().to_string()
}

fn stat_pair(path: &Path) -> (u64, i64) {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path).expect("stat");
    (meta.len(), meta.mtime())
}

// ---------------------------------------------------------------------------
// Download e2e
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_download_via_self_gateway_transfers_whole_file() {
    let dir = scratch("dl-whole");
    let src = dir.join("data.bin");
    let original = payload(200_000);
    std::fs::write(&src, &original).unwrap();
    let name = file_name(&src);

    let mut harness = InProcessRpcHarness::new().await;
    let events = harness
        .copy_download(start_request("_self", src.to_str().unwrap(), &name))
        .await;

    let outcome = parse_download_events(events);
    assert!(outcome.errored.is_none(), "error: {:?}", outcome.errored);
    assert!(outcome.ended_cleanly);
    assert_eq!(outcome.begin.start_offset, 0);
    assert_eq!(outcome.begin.size, original.len() as u64);
    assert_eq!(outcome.data, original);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn e2e_download_resume_continues_from_recorded_offset() {
    let dir = scratch("dl-resume");
    let src = dir.join("data.bin");
    let original = payload(150_000);
    std::fs::write(&src, &original).unwrap();
    let name = file_name(&src);
    let (size, mtime) = stat_pair(&src);
    let skip = 90_000u64;

    let mut start = start_request("_self", src.to_str().unwrap(), &name);
    start.resume.push(rpc::CopyResumeEntry {
        relative_path: name.clone(),
        offset: skip,
        size,
        mtime,
        partial_sha256: String::new(),
    });

    let mut harness = InProcessRpcHarness::new().await;
    let outcome = parse_download_events(harness.copy_download(start).await);
    assert!(outcome.errored.is_none(), "error: {:?}", outcome.errored);
    assert_eq!(
        outcome.begin.start_offset, skip,
        "must resume at the offset"
    );
    assert_eq!(outcome.data, original[skip as usize..]);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn e2e_download_resume_restarts_when_source_changed() {
    let dir = scratch("dl-stale");
    let src = dir.join("data.bin");
    let original = payload(100_000);
    std::fs::write(&src, &original).unwrap();
    let name = file_name(&src);
    let (size, mtime) = stat_pair(&src);

    let mut start = start_request("_self", src.to_str().unwrap(), &name);
    start.resume.push(rpc::CopyResumeEntry {
        relative_path: name.clone(),
        offset: 50_000,
        size,
        mtime: mtime + 3600, // source fingerprint mismatch
        partial_sha256: String::new(),
    });

    let mut harness = InProcessRpcHarness::new().await;
    let outcome = parse_download_events(harness.copy_download(start).await);
    assert!(outcome.errored.is_none(), "error: {:?}", outcome.errored);
    assert_eq!(
        outcome.begin.start_offset, 0,
        "stale hint must fall back to a fresh transfer"
    );
    assert_eq!(outcome.data, original);
    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Upload e2e
// ---------------------------------------------------------------------------

/// scp semantics: uploading to an existing directory destination must land
/// INSIDE it under the source name (regression: the shell path targeted the
/// directory itself, producing an unwritable `<dir>.xho_tmp` and a dead
/// receiver that hung the upload).
#[tokio::test]
async fn e2e_upload_into_directory_destination_lands_inside() {
    let dir = scratch("up-dir");
    let remote_dir = dir.join("incoming");
    std::fs::create_dir_all(&remote_dir).unwrap();
    let original = payload(80_000);
    let name = "payload.bin";

    let mut start = rpc::CopyStartRequest {
        target: "_self".to_string(),
        remote_path: remote_dir.to_str().unwrap().to_string(),
        recursive: false,
        direction: rpc::CopyDirection::Upload as i32,
        timeout_ms: 0,
        source_name: name.to_string(),
        ..Default::default()
    };
    start.resume.push(rpc::CopyResumeEntry {
        relative_path: name.to_string(),
        offset: 0,
        size: original.len() as u64,
        mtime: 1_700_000_000,
        partial_sha256: String::new(),
    });

    let mut harness = InProcessRpcHarness::new().await;
    let probe_events = harness.copy_upload(start.clone(), vec![]).await;
    let errored = first_error(&probe_events);
    assert!(errored.is_none(), "probe error: {errored:?}");

    let events = harness
        .copy_upload(
            start,
            upload_frame_stream_from(name, original.len() as u64, 1_700_000_000, &original, 0),
        )
        .await;
    let errored = first_error(&events);
    assert!(errored.is_none(), "upload error: {errored:?}");
    assert_eq!(
        std::fs::read(remote_dir.join(name)).unwrap(),
        original,
        "file must land inside the directory under the source name"
    );
    assert!(
        !dir.join(format!("{}.xho_tmp", remote_dir.to_str().unwrap()))
            .exists()
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn e2e_upload_via_self_gateway_writes_file_atomically() {
    let dir = scratch("up-whole");
    let dest = dir.join("uploaded.bin");
    let original = payload(120_000);
    let name = file_name(&dest);
    let mtime = 1_700_000_000i64;

    let mut start = start_request("_self", dest.to_str().unwrap(), &name);
    start.direction = rpc::CopyDirection::Upload as i32;
    let frames = upload_frame_stream(&name, original.len() as u64, mtime, &original);

    let mut harness = InProcessRpcHarness::new().await;
    let events = harness.copy_upload(start, frames).await;

    let errored: Option<String> = events
        .into_iter()
        .filter_map(|e| match e.event.expect("event") {
            rpc::copy_response::Event::Error(err) => Some(err.message),
            _ => None,
        })
        .next();
    assert!(errored.is_none(), "error: {errored:?}");
    assert_eq!(std::fs::read(&dest).unwrap(), original, "file content");
    assert!(
        !dir.join(format!("{name}.xho_tmp")).exists(),
        "tmp must be renamed away"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn e2e_upload_resume_appends_to_validated_partial() {
    let dir = scratch("up-resume");
    let dest = dir.join("uploaded.bin");
    let original = payload(128_000);
    let name = file_name(&dest);
    let mtime = 1_700_000_000i64;
    let skip = 77_000usize;

    // A previous interrupted attempt left the first `skip` bytes in the tmp.
    let tmp = dir.join(format!("{name}.xho_tmp"));
    std::fs::write(&tmp, &original[..skip]).unwrap();

    let mut harness = InProcessRpcHarness::new().await;

    // Phase 1: probe the remote partial (no frames yet, like the CLI).
    let probe_events = harness
        .copy_upload(upload_start(&dest, &name, &original, mtime), vec![])
        .await;
    let skip = client_decide_upload_skip(&probe_events, &name, &original).await;
    assert_eq!(skip, 77_000, "full-prefix hash must approve the partial");

    // Phase 2: stream only the suffix from the verified offset.
    let events = harness
        .copy_upload(
            upload_start(&dest, &name, &original, mtime),
            upload_frame_stream_from(
                &name,
                original.len() as u64,
                mtime,
                &original,
                skip as usize,
            ),
        )
        .await;
    let errored = first_error(&events);
    assert!(errored.is_none(), "error: {errored:?}");
    assert_eq!(
        std::fs::read(&dest).unwrap(),
        original,
        "prefix + appended suffix must equal the source"
    );
    assert!(!tmp.exists(), "tmp must be published");
    std::fs::remove_dir_all(&dir).ok();
}

/// The user-reported case: a file that only GREW (append) shares its whole
/// prefix with the old partial — resume must stay correct.
#[tokio::test]
async fn e2e_upload_resume_append_grown_file_resumes_correctly() {
    let dir = scratch("up-append");
    let dest = dir.join("growing.bin");
    let name = file_name(&dest);
    let mtime = 1_700_000_000i64;

    // v1 (80 KB) uploaded halfway in a previous attempt.
    let v1 = payload(80_000);
    let skip = 50_000usize;
    let tmp = dir.join(format!("{name}.xho_tmp"));
    std::fs::write(&tmp, &v1[..skip]).unwrap();

    // v2 = v1 with 40 KB appended (the first bytes — the WHOLE prefix — are
    // unchanged). A head fingerprint alone could not distinguish this from a
    // same-header rewrite; the full-prefix hash must.
    let mut v2 = v1.clone();
    v2.extend_from_slice(&payload_seeded(40_000, 0xABCDEF));

    let mut harness = InProcessRpcHarness::new().await;
    let probe_events = harness
        .copy_upload(upload_start(&dest, &name, &v2, mtime), vec![])
        .await;
    let skip = client_decide_upload_skip(&probe_events, &name, &v2).await;
    assert_eq!(skip, 50_000, "append-grown prefix must verify and resume");

    let events = harness
        .copy_upload(
            upload_start(&dest, &name, &v2, mtime),
            upload_frame_stream_from(&name, v2.len() as u64, mtime, &v2, skip as usize),
        )
        .await;
    let errored = first_error(&events);
    assert!(errored.is_none(), "error: {errored:?}");
    assert_eq!(
        std::fs::read(&dest).unwrap(),
        v2,
        "old prefix + new suffix must equal the grown source"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The corruption case a head fingerprint cannot catch: the source was
/// REWRITTEN keeping the first 512 bytes (same-header file format) but
/// differing in the middle, before the old partial's end.
#[tokio::test]
async fn e2e_upload_resume_rejects_same_header_rewrite() {
    let dir = scratch("up-rewrite");
    let dest = dir.join("arch.bin");
    let name = file_name(&dest);
    let mtime = 1_700_000_000i64;

    // v1: 90 KB; a previous attempt left 60 KB in the tmp.
    let v1 = payload(90_000);
    let tmp = dir.join(format!("{name}.xho_tmp"));
    std::fs::write(&tmp, &v1[..60_000]).unwrap();

    // v2: same first 512 bytes (fixed header), diverging content afterwards.
    let mut v2 = payload_seeded(90_000, 0x77AA_77AA);
    v2[..512].copy_from_slice(&v1[..512]);

    let mut harness = InProcessRpcHarness::new().await;
    let probe_events = harness
        .copy_upload(upload_start(&dest, &name, &v2, mtime), vec![])
        .await;
    let skip = client_decide_upload_skip(&probe_events, &name, &v2).await;
    assert_eq!(skip, 0, "same-header rewrite must NOT resume");

    // Fresh full transfer then publishes the correct content.
    let events = harness
        .copy_upload(
            upload_start(&dest, &name, &v2, mtime),
            upload_frame_stream_from(&name, v2.len() as u64, mtime, &v2, 0),
        )
        .await;
    let errored = first_error(&events);
    assert!(errored.is_none(), "error: {errored:?}");
    assert_eq!(std::fs::read(&dest).unwrap(), v2);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn e2e_upload_resume_discards_partial_larger_than_source() {
    let dir = scratch("up-oversize");
    let dest = dir.join("uploaded.bin");
    let original = payload(64_000);
    let name = file_name(&dest);
    let mtime = 1_700_000_000i64;

    // A partial from a LARGER previous version of the file.
    let tmp = dir.join(format!("{name}.xho_tmp"));
    std::fs::write(&tmp, &payload(100_000)).unwrap();

    let mut harness = InProcessRpcHarness::new().await;
    let probe_events = harness
        .copy_upload(upload_start(&dest, &name, &original, mtime), vec![])
        .await;
    let skip = client_decide_upload_skip(&probe_events, &name, &original).await;
    assert_eq!(skip, 0, "oversized partial must not be resumable");
    std::fs::remove_dir_all(&dir).ok();
}

fn upload_start(dest: &Path, name: &str, source: &[u8], mtime: i64) -> rpc::CopyStartRequest {
    rpc::CopyStartRequest {
        target: "_self".to_string(),
        remote_path: dest.to_str().unwrap().to_string(),
        recursive: false,
        direction: rpc::CopyDirection::Upload as i32,
        timeout_ms: 0,
        source_name: name.to_string(),
        resume: vec![rpc::CopyResumeEntry {
            relative_path: name.to_string(),
            offset: 0,
            size: source.len() as u64,
            mtime,
            partial_sha256: String::new(),
        }],
        ..Default::default()
    }
}

fn upload_frame_stream_from(
    name: &str,
    size: u64,
    mtime: i64,
    bytes: &[u8],
    skip: usize,
) -> Vec<rpc::CopyFrame> {
    let mut frames = vec![rpc::CopyFrame {
        frame: Some(rpc::copy_frame::Frame::BeginFile(rpc::CopyBeginFile {
            relative_path: name.to_string(),
            mode: 0o644,
            size,
            mtime,
            start_offset: skip as u64,
        })),
    }];
    for chunk in bytes[skip..].chunks(32 * 1024) {
        frames.push(rpc::CopyFrame {
            frame: Some(rpc::copy_frame::Frame::FileData(rpc::CopyFileData {
                data: chunk.to_vec(),
            })),
        });
    }
    frames.push(rpc::CopyFrame {
        frame: Some(rpc::copy_frame::Frame::EndFile(rpc::CopyEndFile {})),
    });
    frames.push(rpc::CopyFrame {
        frame: Some(rpc::copy_frame::Frame::EndOfStream(rpc::CopyEndOfStream {})),
    });
    frames
}

fn first_error(events: &[rpc::CopyResponse]) -> Option<String> {
    events.iter().find_map(|e| match e.event.as_ref() {
        Some(rpc::copy_response::Event::Error(err)) => Some(err.message.clone()),
        _ => None,
    })
}
