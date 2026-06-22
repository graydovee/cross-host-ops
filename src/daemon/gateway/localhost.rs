// LocalhostGateway — executes operations on the local machine directly.
//
// Registered under the reserved name `_self` when `reverse_proxy.allow_host_access`
// is enabled. Allows upstream clients to operate the node's own machine
// without SSH (e.g. `xho exec node-1:node-2 uname` executes on node-2).

use std::path::Path;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::copy_frames;
use crate::daemon::connection::shared::build_final_command;
use crate::protocol::{ServerEvent, ServerListRow};
use crate::types::{CopyDirection, CopyFrame, CopySpec};

use super::{
    ExecRequest, Gateway, GatewayError, GatewayKind, InteractiveHandle, InteractiveRequest,
};

/// The reserved gateway name for local host access.
pub const SELF_GATEWAY_NAME: &str = "_self";

/// Gateway that executes operations on the local machine directly.
pub struct LocalhostGateway;

impl LocalhostGateway {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Gateway for LocalhostGateway {
    async fn exec(&self, _target: &str, request: &ExecRequest) -> Result<i32, GatewayError> {
        let argv = &request.argv;
        if argv.is_empty() {
            return Err(GatewayError::execution(anyhow!("empty argv")));
        }

        // Build the command: use shell if specified, otherwise direct exec.
        let (program, args) = if request.no_shell {
            (argv[0].clone(), argv[1..].to_vec())
        } else {
            let shell = if request.shell.is_empty() {
                "/bin/sh"
            } else {
                &request.shell
            };
            let cmd_str = build_final_command(argv, shell);
            debug!(cmd = %cmd_str, shell = %shell, "host exec");
            (shell.to_string(), vec!["-c".to_string(), cmd_str])
        };

        let mut child = Command::new(&program)
            .args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| GatewayError::execution(anyhow!("failed to spawn process: {}", e)))?;

        // Forward stdout.
        let stdout_sender = request.sender.clone();
        let mut stdout = child.stdout.take();
        let stdout_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            if let Some(ref mut stdout) = stdout {
                let mut buf = vec![0u8; 8192];
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let data = buf[..n].to_vec();
                            if stdout_sender.send(ServerEvent::Stdout { data }).is_err() {
                                return Err(anyhow!("client stream closed"));
                            }
                        }
                        Err(e) => return Err(anyhow!("stdout read error: {}", e)),
                    }
                }
            }
            Ok(())
        });

        // Forward stderr.
        let stderr_sender = request.sender.clone();
        let mut stderr = child.stderr.take();
        let stderr_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            if let Some(ref mut stderr) = stderr {
                let mut buf = vec![0u8; 8192];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let data = buf[..n].to_vec();
                            if stderr_sender.send(ServerEvent::Stderr { data }).is_err() {
                                return Err(anyhow!("client stream closed"));
                            }
                        }
                        Err(e) => return Err(anyhow!("stderr read error: {}", e)),
                    }
                }
            }
            Ok(())
        });

        // Forward stdin if requested.
        let stdin_task: Option<JoinHandle<()>> = if request.stdin {
            let stdin_rx = request.stdin_rx.lock().ok().and_then(|mut g| g.take());
            if let Some(mut stdin_rx) = stdin_rx {
                let mut child_stdin = child.stdin.take();
                Some(tokio::spawn(async move {
                    if let Some(ref mut stdin) = child_stdin {
                        while let Some(data) = stdin_rx.recv().await {
                            if data.is_empty() {
                                break;
                            }
                            if stdin.write_all(&data).await.is_err() {
                                break;
                            }
                            let _ = stdin.flush().await;
                        }
                        let _ = stdin.shutdown().await;
                    }
                }))
            } else {
                // Drop stdin to signal EOF.
                drop(child.stdin.take());
                None
            }
        } else {
            // Drop stdin to signal EOF.
            drop(child.stdin.take());
            None
        };

        // Wait for the process with optional timeout.
        let exit_code = if request.timeout_ms > 0 {
            let timeout = tokio::time::timeout(
                std::time::Duration::from_millis(request.timeout_ms),
                child.wait(),
            )
            .await;
            match timeout {
                Ok(Ok(status)) => status.code().unwrap_or(1),
                Ok(Err(e)) => {
                    let _ = child.kill().await;
                    return Err(GatewayError::execution(anyhow!(
                        "process wait error: {}",
                        e
                    )));
                }
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = request.sender.send(ServerEvent::Stderr {
                        data: b"timed out\n".to_vec(),
                    });
                    124 // timeout exit code
                }
            }
        } else {
            child
                .wait()
                .await
                .map_err(|e| GatewayError::execution(anyhow!("process wait error: {}", e)))?
                .code()
                .unwrap_or(1)
        };

        // Wait for I/O tasks to finish draining.
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        if let Some(task) = stdin_task {
            task.abort();
        }

        Ok(exit_code)
    }

    async fn copy(&self, _target: &str, mut spec: CopySpec) -> Result<(), GatewayError> {
        let remote_path = Path::new(&spec.remote_path);
        match spec.direction {
            CopyDirection::Upload => {
                let mut upload_rx = spec
                    .upload_rx
                    .take()
                    .ok_or_else(|| GatewayError::execution(anyhow!("missing upload stream")))?;

                if spec.recursive {
                    // Recursive upload: create the destination directory and
                    // materialize frames relative to it.
                    tokio::fs::create_dir_all(remote_path)
                        .await
                        .ok();
                    copy_frames::materialize_frames_to_dir(remote_path, &mut upload_rx)
                        .await
                        .map_err(|e| GatewayError::execution(e))?;
                } else {
                    // Single file: write directly to the requested remote path.
                    let data = copy_frames::collect_single_file_upload(&mut upload_rx)
                        .await
                        .map_err(|e| GatewayError::execution(e))?;
                    if let Some(parent) = remote_path.parent() {
                        if !parent.as_os_str().is_empty() {
                            tokio::fs::create_dir_all(parent).await.ok();
                        }
                    }
                    tokio::fs::write(remote_path, data)
                        .await
                        .map_err(|e| GatewayError::execution(anyhow!("failed to write {}: {}", remote_path.display(), e)))?;
                }
            }
            CopyDirection::Download => {
                let download_tx = spec
                    .download_tx
                    .take()
                    .ok_or_else(|| GatewayError::execution(anyhow!("missing download stream")))?;

                copy_frames::emit_local_path_frames(remote_path, Path::new(""), true, &download_tx)
                    .await
                    .map_err(|e| GatewayError::execution(e))?;

                let _ = download_tx.send(CopyFrame::EndOfStream).await;
            }
        }
        Ok(())
    }

    async fn exec_interactive(
        &self,
        _target: &str,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle, GatewayError> {
        let argv = &request.argv;
        if argv.is_empty() {
            return Err(GatewayError::execution(anyhow!("empty argv")));
        }

        // Build command (same logic as exec).
        let shell = "/bin/sh";
        let cmd_str = build_final_command(argv, shell);
        debug!(cmd = %cmd_str, "host interactive exec");

        let mut child = Command::new(shell)
            .arg("-c")
            .arg(&cmd_str)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| GatewayError::execution(anyhow!("failed to spawn process: {}", e)))?;

        let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(8);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();

        // stdin forwarder.
        let mut child_stdin = child.stdin.take();
        let stdin_task = tokio::spawn(async move {
            if let Some(ref mut stdin) = child_stdin {
                while let Some(data) = stdin_rx.recv().await {
                    if stdin.write_all(&data).await.is_err() {
                        break;
                    }
                    let _ = stdin.flush().await;
                }
                let _ = child_stdin.as_mut().map(|_| {
                    // can't call shutdown on Option, so just let it drop
                });
            }
        });

        // stdout/stderr forwarder.
        let mut child_stdout = child.stdout.take();
        let mut child_stderr = child.stderr.take();
        let stdout_forward_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            if let Some(ref mut stdout) = child_stdout {
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let _ = stdout_tx.send(buf[..n].to_vec());
                        }
                        Err(_) => break,
                    }
                }
            }
            // Also drain stderr into stdout stream (merged for non-PTY).
            if let Some(ref mut stderr) = child_stderr {
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let _ = stdout_tx.send(buf[..n].to_vec());
                        }
                        Err(_) => break,
                    }
                }
            }
        });

        // Window resize handler (no-op for non-PTY, but consume messages).
        let resize_task = tokio::spawn(async move {
            while resize_rx.recv().await.is_some() {
                // No PTY; resize is a no-op.
            }
        });

        // Process waiter.
        let wait_task = tokio::spawn(async move {
            let code = child
                .wait()
                .await
                .map(|s| s.code().unwrap_or(1))
                .unwrap_or(1);
            let _ = exit_tx.send(code);
        });

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
            abort_handles: vec![
                stdin_task.abort_handle(),
                stdout_forward_task.abort_handle(),
                resize_task.abort_handle(),
                wait_task.abort_handle(),
            ],
        })
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        // _self is an internal mechanism for reverse proxy host access.
        // It is not shown in server listings — users reach it implicitly
        // via `xho exec node-1:node-2 <cmd>` (bare gateway name → _self).
        Ok(Vec::new())
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Localhost
    }

    fn name(&self) -> &str {
        SELF_GATEWAY_NAME
    }

    async fn prune_idle(&self) {
        // No-op: no pooled connections.
    }
}
