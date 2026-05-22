use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use home::home_dir;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use crate::jump::JumpHostKind;

#[allow(deprecated)]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub ssh: SshConfig,
    pub copy: CopyConfig,
    /// Deprecated: use `[[jump_hosts]]` entries with `kind = "jumpserver"` instead.
    pub jumpserver: JumpserverConfig,
    pub review: ReviewConfig,
    #[serde(default)]
    pub jump_hosts: Vec<JumpHostConfig>,
}

#[allow(deprecated)]
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            ssh: SshConfig::default(),
            copy: CopyConfig::default(),
            jumpserver: JumpserverConfig::default(),
            review: ReviewConfig::default(),
            jump_hosts: Vec::new(),
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = path.map(PathBuf::from).unwrap_or_else(default_config_path);
        if !path.exists() {
            let mut config = Self::default();
            config.expand_paths()?;
            config.validate()?;
            return Ok(config);
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let mut config: AppConfig =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        config.expand_paths()?;
        config.validate()?;
        Ok(config)
    }

    #[allow(deprecated)]
    pub fn expand_paths(&mut self) -> Result<()> {
        if let Some(log_path) = &self.server.log_path {
            self.server.log_path = Some(expand_tilde(log_path)?);
        }
        self.server.local.socket_path = expand_tilde(&self.server.local.socket_path)?;
        self.server.remote.host_key_path = expand_tilde(&self.server.remote.host_key_path)?;
        self.server.remote.authorized_keys_path =
            expand_tilde(&self.server.remote.authorized_keys_path)?;
        self.ssh.ssh_config_path = expand_tilde(&self.ssh.ssh_config_path)?;
        self.ssh.server_config_path = expand_tilde(&self.ssh.server_config_path)?;
        if let Some(identity) = &self.jumpserver.identity_file {
            self.jumpserver.identity_file = Some(expand_tilde(identity)?);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.server.validate()
    }
}

pub fn default_config_path() -> PathBuf {
    home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".rhop/config.toml")
}

pub fn default_client_config_path() -> PathBuf {
    default_root_dir().join("client.toml")
}

pub fn default_root_dir() -> PathBuf {
    home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".rhop")
}

pub fn default_known_hosts_path() -> PathBuf {
    default_root_dir().join("known_hosts")
}

pub fn expand_tilde(value: &str) -> Result<String> {
    if value == "~" {
        return Ok(home_dir()
            .ok_or_else(|| anyhow!("home directory not found"))?
            .display()
            .to_string());
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return Ok(home_dir()
            .ok_or_else(|| anyhow!("home directory not found"))?
            .join(rest)
            .display()
            .to_string());
    }
    Ok(value.to_string())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    pub log_path: Option<String>,
    pub log_level: String,
    #[serde(deserialize_with = "deserialize_duration")]
    pub reaper_interval: Duration,
    pub local: LocalServerConfig,
    pub remote: RemoteServerConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            log_path: None,
            log_level: "info".to_string(),
            reaper_interval: Duration::from_secs(30),
            local: LocalServerConfig::default(),
            remote: RemoteServerConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.local.enable && !self.remote.enable {
            bail!("at least one of server.local.enable or server.remote.enable must be true");
        }
        if self.local.enable && self.local.socket_path.trim().is_empty() {
            bail!("server.local.socket_path must not be empty");
        }
        if self.remote.enable {
            if self.remote.user.trim().is_empty() {
                bail!("server.remote.user must not be empty");
            }
            if self.remote.listen_addr.parse::<SocketAddr>().is_err() {
                bail!(
                    "server.remote.listen_addr is invalid: {}",
                    self.remote.listen_addr
                );
            }
            if self.remote.host_key_path.trim().is_empty() {
                bail!("server.remote.host_key_path must not be empty");
            }
            if self.remote.authorized_keys_path.trim().is_empty() {
                bail!("server.remote.authorized_keys_path must not be empty");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LocalServerConfig {
    pub enable: bool,
    pub socket_path: String,
}

impl Default for LocalServerConfig {
    fn default() -> Self {
        Self {
            enable: true,
            socket_path: "~/.rhop/rhopd.sock".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct RemoteServerConfig {
    pub enable: bool,
    pub listen_addr: String,
    pub user: String,
    pub host_key_path: String,
    pub authorized_keys_path: String,
}

impl Default for RemoteServerConfig {
    fn default() -> Self {
        Self {
            enable: false,
            listen_addr: "0.0.0.0:2222".to_string(),
            user: "rhop".to_string(),
            host_key_path: "~/.rhop/host_key".to_string(),
            authorized_keys_path: "~/.rhop/authorized_keys".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SshConfig {
    pub ssh_config_path: String,
    pub server_config_path: String,
    pub fallback: Vec<FallbackTransport>,
    pub pty: bool,
    pub auto_pty_detect: bool,
    #[serde(deserialize_with = "deserialize_duration")]
    pub connect_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub keepalive_interval: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub max_idle_time: Duration,
    pub max_connections_per_ip: usize,
}

impl Default for SshConfig {
    fn default() -> Self {
        Self {
            ssh_config_path: "~/.ssh/config".to_string(),
            server_config_path: "~/.rhop/server.toml".to_string(),
            fallback: vec![
                FallbackTransport::SshConfig,
                FallbackTransport::Jumpserver,
            ],
            pty: true,
            auto_pty_detect: true,
            connect_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(30),
            max_idle_time: Duration::from_secs(600),
            max_connections_per_ip: 10,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct CopyConfig {
    pub preserve_mode: bool,
}

impl Default for CopyConfig {
    fn default() -> Self {
        Self {
            preserve_mode: true,
        }
    }
}

#[deprecated(
    note = "Use [[jump_hosts]] entries with kind = \"jumpserver\" instead. This top-level [jumpserver] block will be removed in a future release."
)]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct JumpserverConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub identity_file: Option<String>,
    pub pubkey_accepted_algorithms: Option<String>,
    pub menu_prompt_contains: String,
    pub mfa_prompt_contains: String,
    pub shell_prompt_suffixes: Vec<String>,
    pub mfa: MfaConfig,
}

#[allow(deprecated)]
impl Default for JumpserverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: String::new(),
            port: 22,
            user: String::new(),
            identity_file: None,
            pubkey_accepted_algorithms: None,
            menu_prompt_contains: "Opt".to_string(),
            mfa_prompt_contains: "MFA".to_string(),
            shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
            mfa: MfaConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct MfaConfig {
    pub totp_secret_base32: String,
    pub digits: u32,
    pub period: u64,
    pub digest: String,
}

impl Default for MfaConfig {
    fn default() -> Self {
        Self {
            totp_secret_base32: String::new(),
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FallbackTransport {
    SshConfig,
    Jumpserver,
}

impl fmt::Display for FallbackTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FallbackTransport::SshConfig => write!(f, "ssh_config"),
            FallbackTransport::Jumpserver => write!(f, "jumpserver"),
        }
    }
}

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
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerHostConfig {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub identity_file: Option<String>,
    pub password: Option<String>,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewConfig {
    pub enable: bool,
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    #[serde(deserialize_with = "deserialize_duration")]
    pub timeout: Duration,
    pub failure_action: ReviewAction,
    pub headers: HashMap<String, String>,
    pub prompts: ReviewPrompts,
    pub policy: ReviewPolicy,
    pub fast_allowlist: FastAllowlistConfig,
    pub semantic_whitelist: Vec<SemanticWhitelistEntry>,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            enable: false,
            endpoint: default_review_endpoint(),
            model: default_review_model(),
            api_key: default_review_api_key(),
            timeout: Duration::from_secs(10),
            failure_action: ReviewAction::Deny,
            headers: HashMap::new(),
            prompts: ReviewPrompts::default(),
            policy: ReviewPolicy::default(),
            fast_allowlist: FastAllowlistConfig::default(),
            semantic_whitelist: default_semantic_whitelist(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewPrompts {
    pub system: String,
    pub template: String,
}

impl Default for ReviewPrompts {
    fn default() -> Self {
        Self {
            system: default_review_system_prompt(),
            template: default_review_template(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewPolicy {
    pub safe: ReviewAction,
    pub risky: ReviewAction,
    pub dangerous: ReviewAction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct FastAllowlistConfig {
    pub enable: bool,
    pub commands: Vec<String>,
}

impl Default for FastAllowlistConfig {
    fn default() -> Self {
        Self {
            enable: true,
            commands: Vec::new(),
        }
    }
}

impl Default for ReviewPolicy {
    fn default() -> Self {
        Self {
            safe: ReviewAction::Allow,
            risky: ReviewAction::Confirm,
            dangerous: ReviewAction::Deny,
        }
    }
}

impl ReviewPolicy {
    pub fn action_for(&self, risk: RiskLevel) -> ReviewAction {
        match risk {
            RiskLevel::Safe => self.safe,
            RiskLevel::Risky => self.risky,
            RiskLevel::Dangerous => self.dangerous,
        }
    }
}

pub fn default_review_endpoint() -> String {
    "https://api.openai.com/v1/chat/completions".to_string()
}

pub fn default_review_model() -> String {
    "gpt-4.1-mini".to_string()
}

pub fn default_review_api_key() -> Option<String> {
    env::var("RHOP_REVIEW_API_KEY")
        .ok()
        .or_else(|| env::var("OPENAI_API_KEY").ok())
}

pub fn default_review_system_prompt() -> String {
    [
        "You are a Unix command safety reviewer for a remote execution tool.",
        "Your job is to classify whether a command is operationally safe before execution.",
        "Be conservative and judge the overall effect of the full command, not isolated substrings.",
        "Return JSON only, with no markdown and no extra text.",
    ]
    .join(" ")
}

pub fn default_review_template() -> String {
    [
        "Classify the command into exactly one risk level: safe, risky, or dangerous.",
        "safe: read-only inspection or diagnostics that should not modify files, system state, services, users, packages, network policy, or remote data.",
        "risky: bounded changes to application or working state with moderate blast radius, where human confirmation is reasonable.",
        "dangerous: deletion, overwrite, download-and-execute, package install, service restart/stop, privilege escalation, system configuration change, cluster mutation, credential access, data exfiltration, or any broad/uncertain shell script.",
        "Judge the whole command after shell operators, pipes, redirects, subshells, and scripts are considered together.",
        "Semantic whitelist entries are only hints. They can justify safe only when the entire command is actually read-only or otherwise clearly within the allowed intent.",
        "If a command mixes a benign subcommand with any mutating or unclear behavior, do not whitelist it.",
        "Return compact JSON with keys: risk_level, reason, matched_whitelist_reason.",
        "matched_whitelist_reason must be null when no whitelist intent applies.",
    ]
    .join("\n")
}

pub fn default_semantic_whitelist() -> Vec<SemanticWhitelistEntry> {
    vec![
        SemanticWhitelistEntry {
            name: "read-only inspection".to_string(),
            description: "Read-only inspection of files, logs, process state, sockets, environment, or system metadata.".to_string(),
            examples: vec![
                "cat /etc/hosts".to_string(),
                "journalctl -u nginx".to_string(),
                "ps aux | grep kubelet".to_string(),
            ],
        },
        SemanticWhitelistEntry {
            name: "source and git inspection".to_string(),
            description: "Read-only inspection of source code or git history/status without checkout, reset, clean, apply, or commit.".to_string(),
            examples: vec![
                "grep -R TODO src".to_string(),
                "git status --short".to_string(),
                "git log --oneline -20".to_string(),
            ],
        },
        SemanticWhitelistEntry {
            name: "kubernetes read-only inspection".to_string(),
            description: "Cluster inspection commands that only get, describe, or view logs and do not patch, edit, apply, delete, scale, or exec.".to_string(),
            examples: vec![
                "kubectl get pods -A".to_string(),
                "kubectl describe pod my-pod -n prod".to_string(),
                "kubectl logs deploy/api -n prod --since=10m".to_string(),
            ],
        },
    ]
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct SemanticWhitelistEntry {
    pub name: String,
    pub description: String,
    pub examples: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ClientConfig {
    pub local: LocalClientConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            local: LocalClientConfig::default(),
        }
    }
}

impl ClientConfig {
    pub fn load() -> Result<Self> {
        let path = default_client_config_path();
        Self::load_from_path(&path)
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        if !path.exists() {
            let mut config = Self::default();
            config.expand_paths()?;
            return Ok(config);
        }
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let mut config: ClientConfig =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        config.expand_paths()?;
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        let path = default_client_config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let raw = toml::to_string_pretty(self).context("failed to serialize client config")?;
        fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    pub fn expand_paths(&mut self) -> Result<()> {
        self.local.socket_path = expand_tilde(&self.local.socket_path)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LocalClientConfig {
    pub socket_path: String,
    pub auto_start: bool,
}

impl Default for LocalClientConfig {
    fn default() -> Self {
        Self {
            socket_path: "~/.rhop/rhopd.sock".to_string(),
            auto_start: true,
        }
    }
}



#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAction {
    Allow,
    Warn,
    Confirm,
    Deny,
}

impl fmt::Display for ReviewAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReviewAction::Allow => write!(f, "allow"),
            ReviewAction::Warn => write!(f, "warn"),
            ReviewAction::Confirm => write!(f, "confirm"),
            ReviewAction::Deny => write!(f, "deny"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Safe,
    Risky,
    Dangerous,
}

impl fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RiskLevel::Safe => write!(f, "safe"),
            RiskLevel::Risky => write!(f, "risky"),
            RiskLevel::Dangerous => write!(f, "dangerous"),
        }
    }
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    struct DurationVisitor;

    impl<'de> Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an integer number of seconds or a duration string like 10s")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 {
                return Err(E::custom("duration must be positive"));
            }
            Ok(Duration::from_secs(value as u64))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_duration(value).map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(DurationVisitor)
}

pub fn parse_duration(value: &str) -> Result<Duration> {
    if value.is_empty() {
        bail!("duration is empty");
    }
    if let Ok(seconds) = u64::from_str(value) {
        return Ok(Duration::from_secs(seconds));
    }
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| anyhow!("invalid duration {}", value))?;
    let amount = value[..split]
        .parse::<u64>()
        .with_context(|| format!("invalid duration number {}", value))?;
    let unit = &value[split..];
    let seconds = match unit {
        "s" => amount,
        "m" => amount * 60,
        "h" => amount * 60 * 60,
        "d" => amount * 60 * 60 * 24,
        _ => bail!("invalid duration unit {}", unit),
    };
    Ok(Duration::from_secs(seconds))
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
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
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
        resolve_server_entry(alias, server, &config.defaults)?;
    }
    Ok(())
}

pub fn resolve_server_entry(
    alias: &str,
    server: &ServerHostConfig,
    defaults: &ServerDefaults,
) -> Result<ServerEntry> {
    let auth = if let Some(password) = server.password.clone() {
        DirectAuth::Password { password }
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
        .map(|(alias, server)| resolve_server_entry(alias, server, &config.defaults))
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

// ---------------------------------------------------------------------------
// Jump host configuration types (task 7.1)
// ---------------------------------------------------------------------------

/// Aliases that are reserved by the system and cannot be assigned to any
/// Jump_Host entry. Currently only `"local"` is reserved (it names the
/// Local_Daemon's own server.toml source).
pub const RESERVED_ALIASES: &[&str] = &["local"];

/// Validation errors for the `[[jump_hosts]]` configuration section.
#[derive(Debug, thiserror::Error)]
pub enum JumpHostValidationError {
    #[error("jump host alias must not be empty")]
    EmptyAlias,

    #[error("jump host alias '{alias}' is reserved (reserved aliases: {reserved:?})")]
    ReservedAlias {
        alias: String,
        reserved: &'static [&'static str],
    },

    #[error("jump host alias '{alias}' is already used by a {existing_kind} jump host")]
    AliasCollision {
        alias: String,
        existing_kind: JumpHostKind,
    },

    #[error("jump host '{alias}' has kind '{kind}' but fields do not match that kind")]
    KindFieldsMismatch { alias: String, kind: JumpHostKind },
}

/// Validates the `[[jump_hosts]]` entries in the daemon configuration.
///
/// Checks:
/// - alias must not be empty
/// - alias must not be in `RESERVED_ALIASES`
/// - alias must not duplicate any other entry's alias (regardless of kind)
/// - fields variant must match kind
pub fn validate_jump_hosts(jump_hosts: &[JumpHostConfig]) -> Result<(), JumpHostValidationError> {
    let mut seen: HashMap<&str, JumpHostKind> = HashMap::new();

    for entry in jump_hosts {
        // Empty alias check
        if entry.alias.is_empty() {
            return Err(JumpHostValidationError::EmptyAlias);
        }

        // Reserved alias check
        if RESERVED_ALIASES.contains(&entry.alias.as_str()) {
            return Err(JumpHostValidationError::ReservedAlias {
                alias: entry.alias.clone(),
                reserved: RESERVED_ALIASES,
            });
        }

        // Duplicate alias check
        if let Some(&existing_kind) = seen.get(entry.alias.as_str()) {
            return Err(JumpHostValidationError::AliasCollision {
                alias: entry.alias.clone(),
                existing_kind,
            });
        }
        seen.insert(&entry.alias, entry.kind);

        // Kind-fields mismatch check
        let fields_match = match (&entry.kind, &entry.fields) {
            (JumpHostKind::Rhopd, JumpHostFields::Rhopd(_)) => true,
            (JumpHostKind::Jumpserver, JumpHostFields::Jumpserver(_)) => true,
            (JumpHostKind::Direct, JumpHostFields::Direct(_)) => true,
            _ => false,
        };
        if !fields_match {
            return Err(JumpHostValidationError::KindFieldsMismatch {
                alias: entry.alias.clone(),
                kind: entry.kind,
            });
        }
    }

    Ok(())
}

fn default_port() -> u16 {
    22
}

fn default_menu_prompt_contains() -> String {
    "Opt".to_string()
}

fn default_mfa_prompt_contains() -> String {
    "MFA".to_string()
}

fn default_shell_prompt_suffixes() -> Vec<String> {
    vec!["$ ".to_string(), "# ".to_string()]
}

/// Configuration for a single jump host entry in `[[jump_hosts]]`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JumpHostConfig {
    pub alias: String,
    pub kind: JumpHostKind,
    #[serde(flatten)]
    pub fields: JumpHostFields,
}

/// Kind-specific fields for a jump host, dispatched by `kind`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JumpHostFields {
    Rhopd(RhopdJumpHostFields),
    Jumpserver(JumpserverJumpHostFields),
    Direct(DirectJumpHostFields),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RhopdJumpHostFields {
    pub address: String,
    #[serde(default)]
    pub identity_file: String,
    #[serde(default)]
    pub known_hosts_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JumpserverJumpHostFields {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub identity_file: String,
    #[serde(default)]
    pub pubkey_accepted_algorithms: Option<String>,
    #[serde(default = "default_menu_prompt_contains")]
    pub menu_prompt_contains: String,
    #[serde(default = "default_mfa_prompt_contains")]
    pub mfa_prompt_contains: String,
    #[serde(default = "default_shell_prompt_suffixes")]
    pub shell_prompt_suffixes: Vec<String>,
    #[serde(default)]
    pub mfa: MfaConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectJumpHostFields {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    pub auth: DirectAuth,
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::{
        AppConfig, ClientConfig, DirectAuth, DirectJumpHostFields, JumpHostConfig,
        JumpHostFields, JumpHostValidationError, JumpserverJumpHostFields, RhopdJumpHostFields,
        SshHostEntry, default_client_config_path, default_config_path, default_known_hosts_path,
        glob_match, parse_duration, resolve_ssh_host, validate_jump_hosts, RESERVED_ALIASES,
    };
    use crate::jump::JumpHostKind;
    use proptest::prelude::*;

    #[test]
    fn parses_duration() {
        assert_eq!(parse_duration("30s").unwrap().as_secs(), 30);
        assert_eq!(parse_duration("10m").unwrap().as_secs(), 600);
        assert_eq!(parse_duration("2h").unwrap().as_secs(), 7200);
    }

    #[test]
    fn matches_glob() {
        assert!(glob_match("192.0.2.*", "192.0.2.163"));
        assert!(glob_match("192.0.2.?.163", "192.0.2.163"));
        assert!(!glob_match("192.0.2.?.163", "192.0.2.163"));
    }

    #[test]
    fn resolves_first_matching_values() {
        let entries = vec![
            SshHostEntry {
                patterns: vec!["192.0.2.*".into()],
                user: Some("root".into()),
                ..Default::default()
            },
            SshHostEntry {
                patterns: vec!["192.0.2.163".into()],
                port: Some(2222),
                identity_file: Some("/tmp/key".into()),
                ..Default::default()
            },
        ];
        let resolved = resolve_ssh_host(&entries, "192.0.2.163").unwrap();
        assert_eq!(resolved.user.as_deref(), Some("root"));
        assert_eq!(resolved.port, Some(2222));
        assert_eq!(resolved.identity_file.as_deref(), Some("/tmp/key"));
    }

    #[test]
    fn defaults_use_rhop_paths() {
        assert!(default_config_path().ends_with(".rhop/config.toml"));
        assert!(default_client_config_path().ends_with(".rhop/client.toml"));
        assert!(default_known_hosts_path().ends_with(".rhop/known_hosts"));
        let config = AppConfig::default();
        assert_eq!(config.server.local.socket_path, "~/.rhop/rhopd.sock");
        assert_eq!(config.server.remote.host_key_path, "~/.rhop/host_key");
        assert_eq!(
            config.server.remote.authorized_keys_path,
            "~/.rhop/authorized_keys"
        );
        assert!(config.copy.preserve_mode);
        let client = ClientConfig::default();
        assert_eq!(client.local.socket_path, "~/.rhop/rhopd.sock");
    }

    #[test]
    fn validates_at_least_one_server_listener() {
        let mut config = AppConfig::default();
        config.server.local.enable = false;
        config.server.remote.enable = false;
        assert!(config.validate().is_err());
    }

    // --- validate_jump_hosts tests ---

    fn make_rhopd_entry(alias: &str) -> JumpHostConfig {
        JumpHostConfig {
            alias: alias.to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.1:2222".to_string(),
                identity_file: "/tmp/key".to_string(),
                known_hosts_path: "/tmp/known_hosts".to_string(),
            }),
        }
    }

    fn make_jumpserver_entry(alias: &str) -> JumpHostConfig {
        JumpHostConfig {
            alias: alias.to_string(),
            kind: JumpHostKind::Jumpserver,
            fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                host: "jump.example.com".to_string(),
                port: 22,
                user: "admin".to_string(),
                identity_file: "/tmp/key".to_string(),
                pubkey_accepted_algorithms: None,
                menu_prompt_contains: "Opt".to_string(),
                mfa_prompt_contains: "MFA".to_string(),
                shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                mfa: super::MfaConfig::default(),
            }),
        }
    }

    fn make_direct_entry(alias: &str) -> JumpHostConfig {
        JumpHostConfig {
            alias: alias.to_string(),
            kind: JumpHostKind::Direct,
            fields: JumpHostFields::Direct(DirectJumpHostFields {
                host: "10.0.0.2".to_string(),
                port: 22,
                user: "root".to_string(),
                auth: DirectAuth::Password {
                    password: "secret".to_string(),
                },
            }),
        }
    }

    #[test]
    fn validate_jump_hosts_accepts_valid_entries() {
        let entries = vec![
            make_rhopd_entry("prod"),
            make_jumpserver_entry("staging"),
            make_direct_entry("dev"),
        ];
        assert!(validate_jump_hosts(&entries).is_ok());
    }

    #[test]
    fn validate_jump_hosts_accepts_empty_list() {
        assert!(validate_jump_hosts(&[]).is_ok());
    }

    #[test]
    fn validate_jump_hosts_rejects_empty_alias() {
        let entries = vec![make_rhopd_entry("")];
        let err = validate_jump_hosts(&entries).unwrap_err();
        assert!(matches!(err, JumpHostValidationError::EmptyAlias));
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn validate_jump_hosts_rejects_reserved_alias() {
        let entries = vec![make_rhopd_entry("local")];
        let err = validate_jump_hosts(&entries).unwrap_err();
        match &err {
            JumpHostValidationError::ReservedAlias { alias, reserved } => {
                assert_eq!(alias, "local");
                assert_eq!(*reserved, RESERVED_ALIASES);
            }
            other => panic!("expected ReservedAlias, got: {:?}", other),
        }
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn validate_jump_hosts_rejects_duplicate_alias_across_kinds() {
        let entries = vec![make_rhopd_entry("shared"), make_jumpserver_entry("shared")];
        let err = validate_jump_hosts(&entries).unwrap_err();
        match &err {
            JumpHostValidationError::AliasCollision {
                alias,
                existing_kind,
            } => {
                assert_eq!(alias, "shared");
                assert_eq!(*existing_kind, JumpHostKind::Rhopd);
            }
            other => panic!("expected AliasCollision, got: {:?}", other),
        }
        assert!(err.to_string().contains("already used"));
    }

    #[test]
    fn validate_jump_hosts_rejects_kind_fields_mismatch() {
        // kind is Rhopd but fields are Jumpserver
        let entry = JumpHostConfig {
            alias: "bad".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                host: "jump.example.com".to_string(),
                port: 22,
                user: "admin".to_string(),
                identity_file: "/tmp/key".to_string(),
                pubkey_accepted_algorithms: None,
                menu_prompt_contains: "Opt".to_string(),
                mfa_prompt_contains: "MFA".to_string(),
                shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                mfa: super::MfaConfig::default(),
            }),
        };
        let err = validate_jump_hosts(&[entry]).unwrap_err();
        match &err {
            JumpHostValidationError::KindFieldsMismatch { alias, kind } => {
                assert_eq!(alias, "bad");
                assert_eq!(*kind, JumpHostKind::Rhopd);
            }
            other => panic!("expected KindFieldsMismatch, got: {:?}", other),
        }
        assert!(err.to_string().contains("do not match"));
    }

    // -----------------------------------------------------------------------
    // Property-based tests
    // -----------------------------------------------------------------------

    // Feature: rhopd-jumpserver-architecture, Property 10: Alias-uniqueness validation rejects collisions deterministically

    /// Strategy to generate a non-empty alphanumeric alias string (1–20 chars).
    fn arb_alias_string() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9]{1,20}"
    }

    /// Strategy to generate a valid `JumpHostConfig` with `JumpHostFields::Rhopd`
    /// and the given alias.
    fn arb_jump_host_config_with_alias(alias: String) -> JumpHostConfig {
        JumpHostConfig {
            alias,
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.1:2222".to_string(),
                identity_file: "/tmp/key".to_string(),
                known_hosts_path: "/tmp/known_hosts".to_string(),
            }),
        }
    }

    /// Strategy to generate a `Vec<JumpHostConfig>` of 0–5 entries with unique,
    /// non-reserved aliases (used as a valid prefix before injecting a collision).
    fn arb_valid_jump_host_vec() -> impl Strategy<Value = Vec<JumpHostConfig>> {
        proptest::collection::hash_set(arb_alias_string(), 0..=5)
            .prop_filter("aliases must not be reserved", |aliases| {
                aliases.iter().all(|a| !RESERVED_ALIASES.contains(&a.as_str()))
            })
            .prop_map(|aliases| {
                aliases
                    .into_iter()
                    .map(arb_jump_host_config_with_alias)
                    .collect::<Vec<_>>()
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 10.6, 10.7, 14.2, 14.3, 14.5, 14.6**
        ///
        /// For any `Vec<JumpHostConfig>` containing two entries with equal `alias`,
        /// `validate_jump_hosts` returns `Err(AliasCollision { .. })`.
        #[test]
        fn prop_alias_collision_rejected(
            prefix in arb_valid_jump_host_vec(),
            dup_alias in arb_alias_string().prop_filter("not reserved", |a| {
                !RESERVED_ALIASES.contains(&a.as_str())
            }),
        ) {
            // Build a config with two entries sharing the same alias
            let entry1 = arb_jump_host_config_with_alias(dup_alias.clone());
            let entry2 = arb_jump_host_config_with_alias(dup_alias.clone());

            // Filter out any prefix entry that already uses dup_alias to avoid
            // the collision being triggered by prefix vs entry1 instead of entry1 vs entry2
            let mut entries: Vec<JumpHostConfig> = prefix
                .into_iter()
                .filter(|e| e.alias != dup_alias)
                .collect();
            entries.push(entry1);
            entries.push(entry2);

            let result = validate_jump_hosts(&entries);
            prop_assert!(result.is_err(), "expected Err for duplicate alias '{}'", dup_alias);
            match result.unwrap_err() {
                JumpHostValidationError::AliasCollision { alias, .. } => {
                    prop_assert_eq!(alias, dup_alias);
                }
                other => {
                    prop_assert!(false, "expected AliasCollision, got: {:?}", other);
                }
            }
        }

        /// **Validates: Requirements 10.6, 10.7, 14.2, 14.3, 14.5, 14.6**
        ///
        /// For any `Vec<JumpHostConfig>` containing an entry with `alias == "local"`,
        /// `validate_jump_hosts` returns `Err(ReservedAlias { .. })`.
        #[test]
        fn prop_reserved_alias_rejected(
            prefix in arb_valid_jump_host_vec(),
        ) {
            // Insert an entry with the reserved alias "local"
            let reserved_entry = arb_jump_host_config_with_alias("local".to_string());

            let mut entries = prefix;
            entries.push(reserved_entry);

            let result = validate_jump_hosts(&entries);
            prop_assert!(result.is_err(), "expected Err for reserved alias 'local'");
            match result.unwrap_err() {
                JumpHostValidationError::ReservedAlias { alias, reserved } => {
                    prop_assert_eq!(alias, "local");
                    prop_assert_eq!(reserved, RESERVED_ALIASES);
                }
                other => {
                    prop_assert!(false, "expected ReservedAlias, got: {:?}", other);
                }
            }
        }
    }
}
