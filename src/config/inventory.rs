use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::path::expand_tilde;
use super::secret::{Secret, SecretResolver};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfigFile {
    pub defaults: ServerDefaults,
    pub servers: HashMap<String, ServerHostConfig>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerDefaults {
    pub identity_file: Option<String>,
    pub shell: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerHostConfig {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub identity_file: Option<String>,
    pub password: Option<Secret>,
    pub shell: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectAuth {
    Key { identity_file: String },
    Password { password: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerEntry {
    pub alias: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub auth: DirectAuth,
}

impl ServerEntry {
    pub fn auth_kind(&self) -> &'static str {
        match self.auth {
            DirectAuth::Key { .. } => "key",
            DirectAuth::Password { .. } => "password",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SshHostEntry {
    pub patterns: Vec<String>,
    pub host_name: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub identity_file: Option<String>,
    pub proxy_command: Option<String>,
    pub pubkey_accepted_algorithms: Option<String>,
}

impl SshHostEntry {
    pub fn matches(&self, host: &str) -> bool {
        self.patterns
            .iter()
            .any(|pattern| glob_match(pattern, host))
    }
}

pub fn parse_ssh_config(path: &Path) -> Result<Vec<SshHostEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut entries = Vec::new();
    let mut current = SshHostEntry::default();

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = parts.next().unwrap_or_default();
        let value = parts.next().unwrap_or_default().trim();
        if key.eq_ignore_ascii_case("Host") {
            if !current.patterns.is_empty() {
                entries.push(current);
            }
            current = SshHostEntry {
                patterns: value.split_whitespace().map(str::to_string).collect(),
                ..Default::default()
            };
            continue;
        }
        match key.to_ascii_lowercase().as_str() {
            "hostname" => current.host_name = Some(value.to_string()),
            "port" => current.port = value.parse::<u16>().ok(),
            "user" => current.user = Some(value.to_string()),
            "identityfile" => current.identity_file = Some(expand_tilde(value)?),
            "proxycommand" => current.proxy_command = Some(value.to_string()),
            "pubkeyacceptedalgorithms" => {
                current.pubkey_accepted_algorithms = Some(value.to_string())
            }
            _ => {}
        }
    }
    if !current.patterns.is_empty() {
        entries.push(current);
    }
    Ok(entries)
}

pub fn load_server_config(path: &Path) -> Result<ServerConfigFile> {
    if !path.exists() {
        return Ok(ServerConfigFile::default());
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut config: ServerConfigFile =
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
    expand_server_config_paths(&mut config)?;
    validate_server_config(&config)?;
    Ok(config)
}

fn expand_server_config_paths(config: &mut ServerConfigFile) -> Result<()> {
    if let Some(identity_file) = &config.defaults.identity_file {
        config.defaults.identity_file = Some(expand_tilde(identity_file)?);
    }
    for server in config.servers.values_mut() {
        if let Some(identity_file) = &server.identity_file {
            server.identity_file = Some(expand_tilde(identity_file)?);
        }
    }
    Ok(())
}

fn validate_server_config(config: &ServerConfigFile) -> Result<()> {
    for (alias, server) in &config.servers {
        if server.host.trim().is_empty() {
            bail!("server entry {} is missing host", alias);
        }
        if server.user.trim().is_empty() {
            bail!("server entry {} is missing user", alias);
        }
        if server.password.is_some() && server.identity_file.is_some() {
            bail!(
                "server entry {} cannot set both password and identity_file",
                alias
            );
        }
        // Validate auth shape without resolving secrets (no env/file/vault
        // access at load time).
        resolve_server_entry(alias, server, &config.defaults, None)?;
    }
    Ok(())
}

/// Resolve a server entry to host/port/user/auth.
///
/// When `resolver` is `Some`, a `password` secret is resolved to plaintext (the
/// connection path). When `None`, password entries yield a placeholder so that
/// listing and validation never touch env vars, files, or the vault — only the
/// auth *kind* is meaningful in that case.
pub fn resolve_server_entry(
    alias: &str,
    server: &ServerHostConfig,
    defaults: &ServerDefaults,
    resolver: Option<&SecretResolver>,
) -> Result<ServerEntry> {
    let auth = if let Some(password) = &server.password {
        let plaintext = match resolver {
            Some(resolver) => password.resolve(resolver).with_context(|| {
                format!("failed to resolve password for server entry {alias}")
            })?,
            None => Default::default(),
        };
        DirectAuth::Password {
            password: plaintext.to_string(),
        }
    } else if let Some(identity_file) = server.identity_file.clone() {
        DirectAuth::Key { identity_file }
    } else if let Some(identity_file) = defaults.identity_file.clone() {
        DirectAuth::Key { identity_file }
    } else {
        bail!(
            "server entry {} is missing authentication; set password, identity_file, or defaults.identity_file",
            alias
        );
    };

    Ok(ServerEntry {
        alias: alias.to_string(),
        host: server.host.clone(),
        port: server.port.unwrap_or(22),
        user: server.user.clone(),
        auth,
    })
}

pub fn list_server_entries(path: &Path) -> Result<Vec<ServerEntry>> {
    let config = load_server_config(path)?;
    let mut entries = config
        .servers
        .iter()
        .map(|(alias, server)| resolve_server_entry(alias, server, &config.defaults, None))
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by(|a, b| a.alias.cmp(&b.alias));
    Ok(entries)
}

pub fn resolve_ssh_host(entries: &[SshHostEntry], host: &str) -> Option<SshHostEntry> {
    let mut resolved = SshHostEntry {
        patterns: vec![host.to_string()],
        ..Default::default()
    };
    let mut matched = false;
    for entry in entries.iter().filter(|entry| entry.matches(host)) {
        matched = true;
        if resolved.host_name.is_none() {
            resolved.host_name = entry.host_name.clone();
        }
        if resolved.port.is_none() {
            resolved.port = entry.port;
        }
        if resolved.user.is_none() {
            resolved.user = entry.user.clone();
        }
        if resolved.identity_file.is_none() {
            resolved.identity_file = entry.identity_file.clone();
        }
        if resolved.proxy_command.is_none() {
            resolved.proxy_command = entry.proxy_command.clone();
        }
        if resolved.pubkey_accepted_algorithms.is_none() {
            resolved.pubkey_accepted_algorithms = entry.pubkey_accepted_algorithms.clone();
        }
    }
    matched.then_some(resolved)
}

pub fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_inner(
        &pattern.chars().collect::<Vec<_>>(),
        &text.chars().collect::<Vec<_>>(),
        0,
        0,
    )
}

fn glob_match_inner(pattern: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }
    match pattern[pi] {
        '*' => {
            for next_ti in ti..=text.len() {
                if glob_match_inner(pattern, text, pi + 1, next_ti) {
                    return true;
                }
            }
            false
        }
        '?' => ti < text.len() && glob_match_inner(pattern, text, pi + 1, ti + 1),
        ch => ti < text.len() && ch == text[ti] && glob_match_inner(pattern, text, pi + 1, ti + 1),
    }
}
