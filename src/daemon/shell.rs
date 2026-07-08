// Shell command construction utilities.
//
// Backend-agnostic helpers for quoting argv and wrapping commands in a shell.
// Used by every gateway that builds a command string to run on a target
// (direct, localhost, xhod, jumpserver). Contains no transport logic.

/// Shell-quote a single argument using single-quote wrapping.
/// Empty strings produce `''`. Internal single-quotes are escaped via `'\''`.
pub fn shell_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

/// Build a remote command string by shell-quoting every argument.
pub fn build_remote_command(argv: &[String]) -> String {
    let mut result = String::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            result.push(' ');
        }
        result.push_str(&shell_quote(arg));
    }
    result
}

/// Build an interactive shell command where the first word (if "safe") is
/// left unquoted so that shell aliases can expand.
pub fn build_interactive_shell_command(argv: &[String]) -> String {
    let mut result = String::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            result.push(' ');
        }
        if index == 0 && is_safe_shell_command_word(arg) {
            result.push_str(arg);
        } else {
            result.push_str(&shell_quote(arg));
        }
    }
    result
}

fn is_safe_shell_command_word(arg: &str) -> bool {
    !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | '+'))
}

/// Determine the flags for a given shell name.
/// bash, zsh: use -ic (interactive)
/// sh, fish, others: use -c only
///
/// Matches on the basename so `C:\...\bash.exe` is recognized as bash.
fn shell_flags(shell_name: &str) -> &'static str {
    let basename = shell_name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(shell_name)
        .to_ascii_lowercase();
    match basename.as_str() {
        "bash" | "bash.exe" | "zsh" | "zsh.exe" => "-ic",
        _ => "-c",
    }
}

/// Wrap a command string in the specified shell invocation.
/// Single quotes in the input are escaped using the `'\''` technique.
///
/// Only the shell *basename* is embedded in the wrapped command (e.g. `bash`,
/// not `C:\Program Files\Git\bin\bash.exe`), so the inner shell is resolved via
/// PATH and the embedded path can't break on backslashes/spaces.
pub fn wrap_in_shell(inner_cmd: &str, shell_name: &str) -> String {
    let escaped = inner_cmd.replace('\'', "'\\''");
    let flags = shell_flags(shell_name);
    let basename = shell_basename(shell_name);
    format!("{basename} {flags} '{escaped}'")
}

/// Reduce a shell path to its basename for embedding in a wrapped command.
/// `/usr/bin/bash` → `bash`; `C:\dir\cmd.exe` → `cmd.exe`; `sh` → `sh`.
fn shell_basename(shell_name: &str) -> &str {
    shell_name
        .rsplit(['/', '\\'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(shell_name)
}

/// Build the final remote command, optionally wrapping in a shell.
/// If shell is empty, no wrapping is applied.
pub fn build_final_command(argv: &[String], shell: &str) -> String {
    let inner = build_remote_command(argv);
    if shell.is_empty() {
        inner
    } else {
        // When wrapping in a shell, we join argv with spaces but leave the
        // first word (command name) unquoted so that shell aliases can expand.
        // Subsequent arguments are still shell-quoted to preserve semantics.
        let shell_inner = build_shell_inner_command(argv);
        wrap_in_shell(&shell_inner, shell)
    }
}

/// Build a command string for use inside a shell wrapper.
/// The first argument (command name) is left unquoted so aliases expand.
/// Remaining arguments are shell-quoted to preserve word boundaries.
fn build_shell_inner_command(argv: &[String]) -> String {
    let mut result = String::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            result.push(' ');
        }
        if index == 0 {
            // Leave command name unquoted for alias expansion
            result.push_str(arg);
        } else {
            result.push_str(&shell_quote(arg));
        }
    }
    result
}

/// Resolve the effective shell-wrapping decision.
///
/// Priority (highest to lowest):
/// 1. --no-shell or --shell=false → None (disable)
/// 2. --shell <name> → Some(name) (CLI override)
/// 3. server_shell (per-server from server.toml) → Some(name) or None if empty
/// 4. defaults_shell (from server.toml [defaults]) → Some(name) or None if empty
pub fn resolve_shell(
    cli_shell: Option<&str>,
    no_shell: bool,
    server_shell: Option<&str>,
    defaults_shell: &str,
) -> Option<String> {
    if no_shell {
        return None;
    }
    if let Some(name) = cli_shell {
        if name == "false" {
            return None;
        }
        return Some(name.to_string());
    }
    if let Some(shell) = server_shell {
        if shell.is_empty() {
            return None;
        }
        return Some(shell.to_string());
    }
    if defaults_shell.is_empty() {
        None
    } else {
        Some(defaults_shell.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_final_command, build_remote_command, resolve_shell, shell_flags, shell_quote,
        wrap_in_shell,
    };

    #[test]
    fn wrap_in_shell_uses_only_basename() {
        // A Windows absolute path with spaces/backslashes must reduce to its
        // basename so the embedded command stays parseable.
        assert_eq!(
            wrap_in_shell("echo hi", r"C:\Program Files\Git\bin\bash.exe"),
            "bash.exe -ic 'echo hi'"
        );
        assert_eq!(wrap_in_shell("echo hi", "/usr/bin/bash"), "bash -ic 'echo hi'");
        assert_eq!(wrap_in_shell("echo hi", "sh"), "sh -c 'echo hi'");
    }

    #[test]
    fn shell_quotes_arguments() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn builds_remote_command() {
        let argv = vec!["echo".to_string(), "hello world".to_string()];
        assert_eq!(build_remote_command(&argv), "'echo' 'hello world'");
    }

    #[test]
    fn shell_flags_returns_ic_for_bash_zsh() {
        assert_eq!(shell_flags("bash"), "-ic");
        assert_eq!(shell_flags("zsh"), "-ic");
    }

    #[test]
    fn shell_flags_returns_c_for_others() {
        assert_eq!(shell_flags("sh"), "-c");
        assert_eq!(shell_flags("fish"), "-c");
        assert_eq!(shell_flags("ksh"), "-c");
        assert_eq!(shell_flags("unknown"), "-c");
    }

    #[test]
    fn wrap_in_shell_basic() {
        assert_eq!(wrap_in_shell("echo hello", "bash"), "bash -ic 'echo hello'");
        assert_eq!(wrap_in_shell("echo hello", "zsh"), "zsh -ic 'echo hello'");
        assert_eq!(wrap_in_shell("echo hello", "sh"), "sh -c 'echo hello'");
        assert_eq!(wrap_in_shell("echo hello", "fish"), "fish -c 'echo hello'");
    }

    #[test]
    fn wrap_in_shell_escapes_single_quotes() {
        assert_eq!(
            wrap_in_shell("echo 'hi'", "bash"),
            "bash -ic 'echo '\\''hi'\\'''",
        );
    }

    #[test]
    fn wrap_in_shell_empty_command() {
        assert_eq!(wrap_in_shell("", "bash"), "bash -ic ''");
    }

    #[test]
    fn build_final_command_with_shell() {
        let argv = vec!["ls".to_string(), "-la".to_string()];
        let result = build_final_command(&argv, "bash");
        let expected = r#"bash -ic 'ls '\''-la'\'''"#;
        assert_eq!(result, expected);

        let argv2 = vec!["ls".to_string()];
        let result2 = build_final_command(&argv2, "bash");
        assert_eq!(result2, "bash -ic 'ls'");
    }

    #[test]
    fn build_final_command_without_shell() {
        let argv = vec!["echo".to_string(), "hello".to_string()];
        let result = build_final_command(&argv, "");
        assert_eq!(result, build_remote_command(&argv));
    }

    #[test]
    fn resolve_shell_no_shell_flag_wins() {
        assert_eq!(resolve_shell(Some("bash"), true, Some("zsh"), "sh"), None);
    }

    #[test]
    fn resolve_shell_cli_shell_wins() {
        assert_eq!(
            resolve_shell(Some("bash"), false, Some("zsh"), "sh"),
            Some("bash".to_string())
        );
    }

    #[test]
    fn resolve_shell_cli_false_disables() {
        assert_eq!(resolve_shell(Some("false"), false, Some("zsh"), "sh"), None);
    }

    #[test]
    fn resolve_shell_server_shell_used() {
        assert_eq!(
            resolve_shell(None, false, Some("zsh"), "bash"),
            Some("zsh".to_string())
        );
    }

    #[test]
    fn resolve_shell_server_empty_disables() {
        assert_eq!(resolve_shell(None, false, Some(""), "bash"), None);
    }

    #[test]
    fn resolve_shell_defaults_used() {
        assert_eq!(
            resolve_shell(None, false, None, "bash"),
            Some("bash".to_string())
        );
    }

    #[test]
    fn resolve_shell_defaults_empty_returns_none() {
        assert_eq!(resolve_shell(None, false, None, ""), None);
    }
}
