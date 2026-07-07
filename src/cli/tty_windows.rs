//! Windows terminal raw-mode via the Win32 Console API + polling resize watcher.
//!
//! Raw mode is implemented by toggling the stdin console mode flags: we clear
//! `ENABLE_ECHO_INPUT`, `ENABLE_LINE_INPUT`, `ENABLE_PROCESSED_INPUT` and
//! `ENABLE_EXTENDED_FLAGS`-driven quick-edit, and enable
//! `ENABLE_VIRTUAL_TERMINAL_INPUT` so ANSI key sequences pass through unfiltered
//! (matching the Unix termios "raw" behavior the remote PTY expects).

use std::io;

use anyhow::{Result, anyhow};
use tokio::sync::mpsc;
use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Console::{
    ENABLE_ECHO_INPUT, ENABLE_EXTENDED_FLAGS, ENABLE_INSERT_MODE, ENABLE_LINE_INPUT,
    ENABLE_PROCESSED_INPUT, ENABLE_QUICK_EDIT_MODE, ENABLE_VIRTUAL_TERMINAL_INPUT,
    GetConsoleMode, GetStdHandle, SetConsoleMode, STD_INPUT_HANDLE, CONSOLE_MODE,
};

/// RAII guard that restores the original console input mode on drop.
pub struct RawModeGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
    original_mode: CONSOLE_MODE,
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        // Restore original console settings unconditionally.
        unsafe {
            SetConsoleMode(self.handle, self.original_mode);
        }
    }
}

/// Set the console's stdin to raw mode and return a guard that restores it.
///
/// Returns an error if stdin is not a console (e.g. piped) — callers fall back
/// to non-interactive mode in that case.
pub fn set_raw_mode() -> Result<RawModeGuard> {
    unsafe {
        let handle = GetStdHandle(STD_INPUT_HANDLE);
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(anyhow!("failed to get stdin handle: {}", io::Error::last_os_error()));
        }

        let mut original_mode: CONSOLE_MODE = 0;
        if GetConsoleMode(handle, &mut original_mode) == 0 {
            // Not a console (piped/redirected) — can't enter raw mode.
            return Err(anyhow!("stdin is not a console (mode unavailable)"));
        }

        // Build raw input mode: disable echo, line buffering, signal processing
        // and quick-edit/insert (these flags live under ENABLE_EXTENDED_FLAGS);
        // enable virtual-terminal input so escape sequences arrive intact.
        let mut raw = original_mode;
        raw &= !(ENABLE_ECHO_INPUT | ENABLE_LINE_INPUT | ENABLE_PROCESSED_INPUT);
        // If extended flags are in use, also clear quick-edit + insert so mouse
        // selection doesn't hijack keystrokes during an interactive session.
        if raw & ENABLE_EXTENDED_FLAGS != 0 {
            raw &= !(ENABLE_QUICK_EDIT_MODE | ENABLE_INSERT_MODE);
        }
        raw |= ENABLE_VIRTUAL_TERMINAL_INPUT;

        if SetConsoleMode(handle, raw) == 0 {
            return Err(anyhow!("SetConsoleMode failed: {}", io::Error::last_os_error()));
        }

        Ok(RawModeGuard {
            handle,
            original_mode,
        })
    }
}

/// Spawn a watcher that polls `terminal_size` and sends (cols, rows) when the
/// console window changes. Windows has no SIGWINCH equivalent; polling every
/// 200ms is the conventional approach (matches `terminal_size`-based watchers).
pub fn spawn_resize_watcher(resize_tx: mpsc::Sender<(u16, u16)>) {
    tokio::spawn(async move {
        let mut last = crate::cli::exec::get_terminal_size();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let current = crate::cli::exec::get_terminal_size();
            if current != last {
                last = current;
                if resize_tx.send(current).await.is_err() {
                    break;
                }
            }
        }
    });
}
