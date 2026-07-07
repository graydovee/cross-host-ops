//! Terminal raw-mode + window-resize abstraction, platform-split.
//!
//! Two public entry points:
//! - [`set_raw_mode_stdin`]: put *this process's stdin* into raw mode, returning
//!   a guard that restores it on drop. Internally selects the Unix (termios) or
//!   Windows (Console API) implementation.
//! - [`spawn_resize_watcher`]: deliver terminal size changes to a channel.
//!   Unix uses SIGWINCH; Windows polls `terminal_size`.
//!
//! The underlying guard type is platform-specific; callers hold it via the
//! re-exported concrete `RawModeGuard`.
//!
//! Implementations live in `tty_unix` / `tty_windows`.

#[cfg(unix)]
#[path = "tty_unix.rs"]
pub(crate) mod tty_unix;
#[cfg(unix)]
pub use tty_unix::RawModeGuard;

#[cfg(windows)]
#[path = "tty_windows.rs"]
pub(crate) mod tty_windows;
#[cfg(windows)]
pub use tty_windows::RawModeGuard;

/// Put this process's stdin into raw mode; the returned guard restores the
/// original mode on drop.
pub fn set_raw_mode_stdin() -> anyhow::Result<RawModeGuard> {
    #[cfg(unix)]
    {
        tty_unix::set_raw_mode(libc::STDIN_FILENO)
    }
    #[cfg(windows)]
    {
        tty_windows::set_raw_mode()
    }
}

/// Spawn a terminal-resize watcher that sends (cols, rows) on each change.
///
/// Unix listens for SIGWINCH; Windows polls `terminal_size` periodically.
/// The task ends when the sender is dropped.
pub fn spawn_resize_watcher(resize_tx: tokio::sync::mpsc::Sender<(u16, u16)>) {
    #[cfg(unix)]
    {
        tty_unix::spawn_resize_watcher(resize_tx);
    }
    #[cfg(windows)]
    {
        tty_windows::spawn_resize_watcher(resize_tx);
    }
}
