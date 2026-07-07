//! Unix terminal raw-mode via termios + SIGWINCH window-resize signaling.

use std::io;
use std::os::unix::io::RawFd;

use anyhow::{Result, anyhow};
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;

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

/// Spawn a SIGWINCH watcher that sends the new (cols, rows) terminal size on
/// each window change. The task ends when the sender is dropped.
pub fn spawn_resize_watcher(resize_tx: mpsc::Sender<(u16, u16)>) {
    tokio::spawn(async move {
        let mut signal = match tokio::signal::unix::signal(SignalKind::window_change()) {
            Ok(s) => s,
            Err(_) => return,
        };
        while signal.recv().await.is_some() {
            let (cols, rows) = crate::cli::exec::get_terminal_size();
            if resize_tx.send((cols, rows)).await.is_err() {
                break;
            }
        }
    });
}
