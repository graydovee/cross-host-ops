use std::env;
use std::ffi::OsStr;
use std::io::{self, IsTerminal, Write};
use std::os::unix::io::RawFd;

use anyhow::{Result, anyhow};
use tokio::io::AsyncReadExt;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::AppConfig;
use crate::exit_codes::cap_remote_exit_code;
use crate::protocol::rpc;
use crate::types::{FlagIntent, should_use_interactive_mode};

use super::client::{ClientAccess, connect_data_client};
use super::prompt::{prompt_for_auth_input, prompt_for_confirmation};

/// Get the current terminal size as (cols, rows).
/// Falls back to (80, 24) if the terminal size cannot be determined.
pub(crate) fn get_terminal_size() -> (u16, u16) {
    terminal_size::terminal_size()
        .map(|(w, h)| (w.0, h.0))
        .unwrap_or((80, 24))
}

/// RAII guard that restores terminal to original mode on drop.
pub struct RawModeGuard {
    original_termios: libc::termios,
    fd: RawFd,
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        // Restore original terminal settings unconditionally.
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.original_termios);
        }
    }
}

/// Set the terminal to raw mode and return a guard that restores it on drop.
///
/// If raw mode setup fails (e.g., fd is not a terminal), returns an error
/// so the caller can fall back to non-interactive mode.
pub fn set_raw_mode(fd: RawFd) -> Result<RawModeGuard> {
    unsafe {
        let mut original_termios: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(fd, &mut original_termios) != 0 {
            return Err(anyhow!("tcgetattr failed: {}", io::Error::last_os_error()));
        }

        let mut raw = original_termios;
        libc::cfmakeraw(&mut raw);

        if libc::tcsetattr(fd, libc::TCSANOW, &raw) != 0 {
            return Err(anyhow!("tcsetattr failed: {}", io::Error::last_os_error()));
        }

        Ok(RawModeGuard {
            original_termios,
            fd,
        })
    }
}

fn raw_mode_diagnostic_bytes(message: &str) -> Vec<u8> {
    let message = message.trim_end_matches(['\r', '\n']);
    if message.is_empty() {
        return Vec::new();
    }

    let normalized = message.replace("\r\n", "\n").replace('\r', "\n");
    let mut bytes = normalized.replace('\n', "\r\n").into_bytes();
    bytes.extend_from_slice(b"\r\n");
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_mode_diagnostic_bytes_uses_carriage_return_newline() {
        assert_eq!(
            raw_mode_diagnostic_bytes("warning: target matched"),
            b"warning: target matched\r\n".to_vec()
        );
    }

    #[test]
    fn raw_mode_diagnostic_bytes_normalizes_multiline_messages() {
        assert_eq!(
            raw_mode_diagnostic_bytes("first\nsecond\r\nthird\r"),
            b"first\r\nsecond\r\nthird\r\n".to_vec()
        );
    }

    #[test]
    fn raw_mode_diagnostic_bytes_skips_empty_messages() {
        assert!(raw_mode_diagnostic_bytes("").is_empty());
        assert!(raw_mode_diagnostic_bytes("\n\r").is_empty());
    }
}

fn write_raw_mode_diagnostic(message: &str) -> Result<()> {
    let bytes = raw_mode_diagnostic_bytes(message);
    if bytes.is_empty() {
        return Ok(());
    }

    let mut stderr = io::stderr();
    stderr.write_all(&bytes)?;
    stderr.flush()?;
    Ok(())
}

/// Detect whether a `--` separator was used between the target and the first cmd token.
///
/// Scans `std::env::args_os()` for a literal `--` that appears after the target positional.
/// This is necessary because clap's `trailing_var_arg` consumes all tokens after target
/// into `cmd`, stripping the `--` from the parsed result.
pub(crate) fn detect_double_dash_separator(target: &str) -> bool {
    let raw_args: Vec<_> = env::args_os().collect();
    let target_os = OsStr::new(target);

    // Find the position of "exec" subcommand in raw args.
    let exec_pos = match raw_args.iter().position(|a| a == OsStr::new("exec")) {
        Some(p) => p,
        None => return false,
    };

    // Find the target position after "exec" (skip options like --tty, --timeout, etc.)
    let target_pos = match raw_args[exec_pos + 1..].iter().position(|a| a == target_os) {
        Some(p) => exec_pos + 1 + p,
        None => return false,
    };

    // Check if the token immediately after target is `--`.
    raw_args
        .get(target_pos + 1)
        .map(|a| a == OsStr::new("--"))
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_command(
    target: String,
    argv: Vec<String>,
    resolved_tty: bool,
    tty_intent: FlagIntent,
    resolved_stdin: bool,
    stdin_intent: FlagIntent,
    timeout_ms: u64,
    shell: Option<String>,
    no_shell: bool,
    _config: &AppConfig,
) -> Result<i32> {
    // Pass raw CLI flags to daemon; daemon resolves effective shell from server.toml.
    let cli_shell = shell.unwrap_or_default();

    // Check if we should use interactive mode (all 4 conditions must be true).
    let stdin_is_tty = io::stdin().is_terminal();
    let stdout_is_tty = io::stdout().is_terminal();

    if should_use_interactive_mode(resolved_tty, resolved_stdin, stdin_is_tty, stdout_is_tty) {
        return run_interactive(
            target,
            argv,
            timeout_ms,
            cli_shell,
            no_shell,
            tty_intent,
            stdin_intent,
        )
        .await;
    }

    // Batch execution path: request_pty + exec without sentinel.
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;

    let (tx, rx) = mpsc::channel(8);
    tx.send(rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target,
            argv,
            tty: resolved_tty,
            tty_intent: rpc::FlagIntent::from(tty_intent) as i32,
            stdin: resolved_stdin,
            stdin_intent: rpc::FlagIntent::from(stdin_intent) as i32,
            timeout_ms,
            interactive: false,
            term_cols: 0,
            term_rows: 0,
            shell: cli_shell,
            no_shell,
        })),
    })
    .await
    .map_err(|_| anyhow!("failed to send execute request"))?;

    // If stdin forwarding is requested, spawn a task that reads from tokio stdin
    // and sends data on the bidirectional stream.
    //
    // EOF semantics: instead of relying on closing the gRPC request stream
    // (which is unreliable across SSH-tunneled gRPC hops), we send an
    // EXPLICIT zero-length StdinData message as an EOF sentinel.  Daemons
    // along the path interpret an empty StdinData payload as "no more stdin"
    // and propagate it downstream.  We then keep the sender alive for the
    // remainder of the call so the bidirectional stream stays healthy.
    let response_tx = if resolved_stdin {
        let stdin_tx = tx.clone();
        tokio::spawn(async move {
            let mut tokio_stdin = tokio::io::stdin();
            let mut buf = [0u8; 4096];
            loop {
                match tokio_stdin.read(&mut buf).await {
                    Ok(0) => {
                        // Local stdin EOF: send explicit empty StdinData as
                        // an EOF sentinel rather than just dropping the sender.
                        let eof_msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::StdinData(
                                rpc::StdinData { data: Vec::new() },
                            )),
                        };
                        let _ = stdin_tx.send(eof_msg).await;
                        break;
                    }
                    Ok(n) => {
                        let msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::StdinData(
                                rpc::StdinData {
                                    data: buf[..n].to_vec(),
                                },
                            )),
                        };
                        if stdin_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            // stdin_tx (the clone) drops here.  The MAIN tx returned in
            // `response_tx` keeps the request channel open for the rest of
            // the call so confirm/auth replies can still be sent if needed
            // and so the gRPC stream stays open until ExitStatus arrives.
        });
        Some(tx)
    } else {
        Some(tx)
    };

    let response = client.execute(ReceiverStream::new(rx)).await?;
    let mut stream = response.into_inner();
    let mut exit_code = 1;

    while let Some(message) = stream.message().await? {
        match message
            .event
            .ok_or_else(|| anyhow!("execute stream returned empty event"))?
        {
            rpc::execute_response::Event::Stdout(chunk) => {
                io::stdout().write_all(&chunk.data)?;
                io::stdout().flush()?;
            }
            rpc::execute_response::Event::Stderr(chunk) => {
                io::stderr().write_all(&chunk.data)?;
                io::stderr().flush()?;
            }
            rpc::execute_response::Event::ReviewResult(_result) => {}
            rpc::execute_response::Event::ConfirmRequired(confirm) => {
                let allow = prompt_for_confirmation(&confirm.reason)?;
                let Some(ref response_tx) = response_tx else {
                    return Err(anyhow!(
                        "received ConfirmRequired but no response channel available"
                    ));
                };
                response_tx
                    .send(rpc::ExecuteRequest {
                        request: Some(rpc::execute_request::Request::Confirm(
                            rpc::ConfirmRequest {
                                execution_id: confirm.execution_id,
                                allow,
                            },
                        )),
                    })
                    .await
                    .map_err(|_| anyhow!("failed to send confirmation request"))?;
            }
            rpc::execute_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                let Some(ref response_tx) = response_tx else {
                    return Err(anyhow!(
                        "received AuthPrompt but no response channel available"
                    ));
                };
                response_tx
                    .send(crate::protocol::execute_auth_input_request(
                        prompt.prompt_id,
                        value,
                    ))
                    .await
                    .map_err(|_| anyhow!("failed to send auth input request"))?;
            }
            rpc::execute_response::Event::ExitStatus(status) => {
                exit_code = status.code;
                break;
            }
            rpc::execute_response::Event::Info(info) => {
                eprintln!("{}", info.message);
            }
            rpc::execute_response::Event::Error(error) => {
                eprintln!("error: {}", error.message);
                return Ok(1);
            }
        }
    }

    // Exit code 124 from the daemon means timeout fired. If the client
    // requested a timeout, pass 124 through directly (it's xho's own
    // semantic, not a remote process exit code). Otherwise cap normally.
    if exit_code == 124 && timeout_ms > 0 {
        Ok(124)
    } else {
        Ok(cap_remote_exit_code(exit_code))
    }
}

/// Run a command in interactive PTY mode with raw terminal, bidirectional
/// byte streaming, and SIGWINCH forwarding.
pub(crate) async fn run_interactive(
    target: String,
    argv: Vec<String>,
    timeout_ms: u64,
    shell: String,
    no_shell: bool,
    tty_intent: FlagIntent,
    stdin_intent: FlagIntent,
) -> Result<i32> {
    // Step 1: Get initial terminal size.
    let (cols, rows) = get_terminal_size();

    // Step 2: Connect to daemon and send StartRequest with interactive=true.
    let mut client = connect_data_client(ClientAccess::AutoStart).await?;
    let (tx, rx) = mpsc::channel(32);
    tx.send(rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target,
            argv,
            tty: true,
            tty_intent: rpc::FlagIntent::from(tty_intent) as i32,
            stdin: true,
            stdin_intent: rpc::FlagIntent::from(stdin_intent) as i32,
            timeout_ms,
            interactive: true,
            term_cols: cols as u32,
            term_rows: rows as u32,
            shell,
            no_shell,
        })),
    })
    .await
    .map_err(|_| anyhow!("failed to send execute request"))?;

    let response = client.execute(ReceiverStream::new(rx)).await?;
    let mut stream = response.into_inner();

    // Step 3: Set terminal to raw mode with RAII guard.
    let _guard = set_raw_mode(libc::STDIN_FILENO)?;

    // Step 4: Spawn stdin forwarding task (channel capacity 32 via tx clone).
    let stdin_tx = tx.clone();
    let stdin_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        let mut stdin = tokio::io::stdin();
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let msg = rpc::ExecuteRequest {
                        request: Some(rpc::execute_request::Request::StdinData(rpc::StdinData {
                            data: buf[..n].to_vec(),
                        })),
                    };
                    if stdin_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Step 5: Spawn SIGWINCH handler (resize channel capacity 8 via bounded sender).
    let resize_tx = tx.clone();
    let sigwinch_task = tokio::spawn(async move {
        let mut signal = match tokio::signal::unix::signal(SignalKind::window_change()) {
            Ok(s) => s,
            Err(_) => return,
        };
        while signal.recv().await.is_some() {
            let (cols, rows) = get_terminal_size();
            let msg = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::WindowResize(
                    rpc::WindowResize {
                        cols: cols as u32,
                        rows: rows as u32,
                    },
                )),
            };
            if resize_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Step 6: Process response stream — write stdout directly, handle exit.
    let mut exit_code = 1;
    while let Some(message) = stream.message().await? {
        match message
            .event
            .ok_or_else(|| anyhow!("execute stream returned empty event"))?
        {
            rpc::execute_response::Event::Stdout(chunk) => {
                io::stdout().write_all(&chunk.data)?;
                io::stdout().flush()?;
            }
            rpc::execute_response::Event::ExitStatus(status) => {
                exit_code = status.code;
                break;
            }
            rpc::execute_response::Event::Error(error) => {
                write_raw_mode_diagnostic(&format!("error: {}", error.message))?;
                stdin_task.abort();
                sigwinch_task.abort();
                return Ok(1);
            }
            rpc::execute_response::Event::Info(info) => {
                write_raw_mode_diagnostic(&info.message)?;
            }
            rpc::execute_response::Event::AuthPrompt(prompt) => {
                let value = prompt_for_auth_input(&prompt.message, prompt.secret)?;
                tx.send(crate::protocol::execute_auth_input_request(
                    prompt.prompt_id,
                    value,
                ))
                .await
                .map_err(|_| anyhow!("failed to send auth input request"))?;
            }
            rpc::execute_response::Event::ConfirmRequired(confirm) => {
                let allow = prompt_for_confirmation(&confirm.reason)?;
                tx.send(rpc::ExecuteRequest {
                    request: Some(rpc::execute_request::Request::Confirm(
                        rpc::ConfirmRequest {
                            execution_id: confirm.execution_id,
                            allow,
                        },
                    )),
                })
                .await
                .map_err(|_| anyhow!("failed to send confirmation request"))?;
            }
            _ => {}
        }
    }

    // Step 7: Cleanup — abort tasks, guard drops automatically restoring terminal.
    stdin_task.abort();
    sigwinch_task.abort();
    Ok(cap_remote_exit_code(exit_code))
}
