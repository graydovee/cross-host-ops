use std::io::{self, Write};

#[cfg(unix)]
use std::os::fd::AsRawFd;

#[cfg(unix)]
use anyhow::anyhow;
use anyhow::Result;

pub(crate) fn prompt_for_confirmation(reason: &str) -> Result<bool> {
    eprintln!("confirmation required: {}", reason);
    eprint!("Continue? [y/N] ");
    io::stderr().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim(), "y" | "Y" | "yes" | "YES"))
}

pub(crate) fn prompt_for_auth_input(message: &str, secret: bool) -> Result<String> {
    eprint!("{}: ", message);
    io::stderr().flush()?;
    if secret {
        read_secret_line()
    } else {
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        Ok(input.trim_end().to_string())
    }
}

#[cfg(unix)]
fn read_secret_line() -> Result<String> {
    let stdin = io::stdin();
    let fd = stdin.as_raw_fd();
    let mut term = std::mem::MaybeUninit::<libc::termios>::uninit();
    unsafe {
        if libc::tcgetattr(fd, term.as_mut_ptr()) != 0 {
            return Err(anyhow!("failed to read terminal attributes"));
        }
        let original = term.assume_init();
        let mut masked = original;
        masked.c_lflag &= !libc::ECHO;
        if libc::tcsetattr(fd, libc::TCSANOW, &masked) != 0 {
            return Err(anyhow!("failed to disable terminal echo"));
        }
        let mut input = String::new();
        let read_result = io::stdin().read_line(&mut input);
        let restore_result = libc::tcsetattr(fd, libc::TCSANOW, &original);
        eprintln!();
        if restore_result != 0 {
            return Err(anyhow!("failed to restore terminal echo"));
        }
        read_result?;
        Ok(input.trim_end().to_string())
    }
}

/// Windows fallback for secret input: terminal echo is not masked.
///
/// The Win32 Console API path (GetConsoleMode/SetConsoleMode toggling
/// ENABLE_ECHO_INPUT) lands alongside the full raw-mode implementation. For
/// now the secret is read plainly; users who need echo suppression can pipe
/// the value in or set it via the secret vault.
#[cfg(not(unix))]
fn read_secret_line() -> Result<String> {
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}
