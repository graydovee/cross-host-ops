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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub ssh: SshConfig,
    pub copy: CopyConfig,
    pub review: ReviewConfig,
    #[serde(default)]
    pub gateways: Vec<GatewayConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            ssh: SshConfig::default(),
            copy: CopyConfig::default(),
            review: ReviewConfig::default(),
            gateways: Vec::new(),
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

        for gw in &mut self.gateways {
            match gw {
                GatewayConfig::Rhopd(c) => {
                    c.identity_file = expand_tilde(&c.identity_file)?;
                    c.known_hosts_path = expand_tilde(&c.known_hosts_path)?;
                }
                GatewayConfig::Jumpserver(c) => {
                    c.identity_file = expand_tilde(&c.identity_file)?;
                }
                GatewayConfig::Direct(c) => {
                    c.identity_file = expand_tilde(&c.identity_file)?;
                }
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.server.validate()?;
        validate_gateways(&self.gateways)?;
        validate_fallback_references(&self.ssh.fallback, &self.gateways)?;
        Ok(())
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
    #[serde(deserialize_with = "deserialize_duration", serialize_with = "serialize_duration")]
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
    pub fallback: Vec<FallbackEntry>,
    /// When true, allocate PTY by default unless --no-tty overrides.
    pub tty: bool,
    /// When true, forward stdin by default unless --no-stdin overrides.
    pub stdin: bool,
    /// When true, auto-detect TTY based on stdout. If stdout is not a TTY, disable TTY.
    pub auto_tty_detect: bool,
    #[serde(deserialize_with = "deserialize_duration", serialize_with = "serialize_duration")]
    pub connect_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration", serialize_with = "serialize_duration")]
    pub keepalive_interval: Duration,
    #[serde(deserialize_with = "deserialize_duration", serialize_with = "serialize_duration")]
    pub max_idle_time: Duration,
    pub max_connections_per_ip: usize,
}

impl Default for SshConfig {
    fn default() -> Self {
        Self {
            ssh_config_path: "~/.ssh/config".to_string(),
            server_config_path: "~/.rhop/server.toml".to_string(),
            fallback: vec![FallbackEntry::Local],
            tty: true,
            stdin: false,
            auto_tty_detect: true,
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

/// A single entry in the `ssh.fallback` list.
///
/// - `"local"` deserializes to `FallbackEntry::Local` (resolve via ~/.ssh/config)
/// - Any other string deserializes to `FallbackEntry::Gateway(name)` (route through named gateway)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FallbackEntry {
    /// Resolve via local ~/.ssh/config
    Local,
    /// Route through the named gateway
    Gateway(String),
}

impl<'de> Deserialize<'de> for FallbackEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "local" {
            Ok(FallbackEntry::Local)
        } else {
            Ok(FallbackEntry::Gateway(s))
        }
    }
}

impl Serialize for FallbackEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            FallbackEntry::Local => serializer.serialize_str("local"),
            FallbackEntry::Gateway(name) => serializer.serialize_str(name),
        }
    }
}

impl fmt::Display for FallbackEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FallbackEntry::Local => write!(f, "local"),
            FallbackEntry::Gateway(name) => write!(f, "{}", name),
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
    pub shell: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerHostConfig {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub identity_file: Option<String>,
    pub password: Option<String>,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewConfig {
    pub enable: bool,
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    #[serde(deserialize_with = "deserialize_duration", serialize_with = "serialize_duration")]
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

fn serialize_duration<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let secs = duration.as_secs();
    let s = format!("{}s", secs);
    serializer.serialize_str(&s)
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
// ---------------------------------------------------------------------------
// Gateway configuration
// ---------------------------------------------------------------------------

/// Names reserved by the system that cannot be assigned to any gateway entry.
/// Currently only `"local"` is reserved (it names the local daemon's own
/// server.toml source).
pub const RESERVED_NAMES: &[&str] = &["local"];

// ---------------------------------------------------------------------------
// New gateway validation (task 2.2)
// ---------------------------------------------------------------------------

/// Validation errors for the `[[gateways]]` configuration section.
#[derive(Debug, thiserror::Error)]
pub enum GatewayValidationError {
    #[error("gateway name must not be empty")]
    EmptyName,

    #[error("gateway name '{name}' is reserved (reserved names: {reserved:?})")]
    ReservedName {
        name: String,
        reserved: &'static [&'static str],
    },

    #[error("gateway name '{name}' is already used by a {existing_kind:?} gateway")]
    NameCollision {
        name: String,
        existing_kind: crate::daemon::gateway::GatewayKind,
    },

    #[error("gateway '{name}' has empty required field '{field}'")]
    EmptyRequiredField { name: String, field: String },
}

/// Validates the `[[gateways]]` entries in the daemon configuration.
///
/// Checks:
/// - name must not be empty
/// - name must not be in `RESERVED_NAMES`
/// - name must not duplicate any other entry's name (regardless of kind)
/// - kind-specific required fields must not be empty
pub fn validate_gateways(gateways: &[GatewayConfig]) -> Result<(), GatewayValidationError> {
    use crate::daemon::gateway::GatewayKind;
    let mut seen: HashMap<&str, GatewayKind> = HashMap::new();

    for entry in gateways {
        let name = entry.name();

        // Empty name check
        if name.is_empty() {
            return Err(GatewayValidationError::EmptyName);
        }

        // Reserved name check
        if RESERVED_NAMES.contains(&name) {
            return Err(GatewayValidationError::ReservedName {
                name: name.to_string(),
                reserved: RESERVED_NAMES,
            });
        }

        // Duplicate name check
        if let Some(&existing_kind) = seen.get(name) {
            return Err(GatewayValidationError::NameCollision {
                name: name.to_string(),
                existing_kind,
            });
        }
        seen.insert(name, entry.gateway_kind());

        // Kind-specific required field validation
        match entry {
            GatewayConfig::Rhopd(c) => {
                if c.address.is_empty() {
                    return Err(GatewayValidationError::EmptyRequiredField {
                        name: c.name.clone(),
                        field: "address".to_string(),
                    });
                }
            }
            GatewayConfig::Jumpserver(c) => {
                if c.host.is_empty() {
                    return Err(GatewayValidationError::EmptyRequiredField {
                        name: c.name.clone(),
                        field: "host".to_string(),
                    });
                }
            }
            GatewayConfig::Direct(c) => {
                if c.host.is_empty() {
                    return Err(GatewayValidationError::EmptyRequiredField {
                        name: c.name.clone(),
                        field: "host".to_string(),
                    });
                }
            }
        }
    }

    Ok(())
}

/// Validates that all `ssh.fallback` entries of type `Gateway(name)` reference
/// either `"local"` or a name present in the gateways list.
pub fn validate_fallback_references(
    fallback: &[FallbackEntry],
    gateways: &[GatewayConfig],
) -> Result<()> {
    for entry in fallback {
        if let FallbackEntry::Gateway(name) = entry {
            // "local" is always valid as a fallback reference
            if name == "local" {
                continue;
            }
            // Check if the name matches any gateway entry
            let found = gateways.iter().any(|g| g.name() == name);
            if !found {
                bail!(
                    "ssh.fallback references gateway '{}' which is not defined in [[gateways]]",
                    name
                );
            }
        }
    }
    Ok(())
}

fn default_port() -> u16 {
    22
}

fn default_totp_digits() -> u32 {
    6
}

fn default_totp_period() -> u64 {
    30
}

// ---------------------------------------------------------------------------
// New gateway configuration types (task 2.1)
// ---------------------------------------------------------------------------

/// Tagged dispatch on `kind` field for gateway configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GatewayConfig {
    Rhopd(RhopdGatewayConfig),
    Jumpserver(JumpserverGatewayConfig),
    Direct(DirectGatewayConfig),
}

impl GatewayConfig {
    /// Returns the name of this gateway entry.
    pub fn name(&self) -> &str {
        match self {
            Self::Rhopd(c) => &c.name,
            Self::Jumpserver(c) => &c.name,
            Self::Direct(c) => &c.name,
        }
    }

    /// Returns the GatewayKind for this config variant.
    pub fn gateway_kind(&self) -> crate::daemon::gateway::GatewayKind {
        use crate::daemon::gateway::GatewayKind;
        match self {
            Self::Rhopd(_) => GatewayKind::Rhopd,
            Self::Jumpserver(_) => GatewayKind::Jumpserver,
            Self::Direct(_) => GatewayKind::Direct,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RhopdGatewayConfig {
    pub name: String,
    pub address: String,
    #[serde(default)]
    pub identity_file: String,
    #[serde(default)]
    pub known_hosts_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct JumpserverGatewayConfig {
    pub name: String,
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub identity_file: String,
    #[serde(default)]
    pub pubkey_accepted_algorithms: Option<String>,
    // Flat MFA/TOTP fields (no nested sub-table)
    #[serde(default)]
    pub totp_secret_base32: String,
    #[serde(default = "default_totp_digits")]
    pub totp_digits: u32,
    #[serde(default = "default_totp_period")]
    pub totp_period: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DirectGatewayConfig {
    pub name: String,
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub identity_file: String,
    #[serde(default)]
    pub password: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        AppConfig, ClientConfig, FallbackEntry,
        SshHostEntry, default_client_config_path, default_config_path, default_known_hosts_path,
        glob_match, parse_duration, resolve_ssh_host,
    };
    use proptest::prelude::*;
    use serde::{Deserialize, Serialize};

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

    // -----------------------------------------------------------------------
    // FallbackEntry property-based tests
    // -----------------------------------------------------------------------

    /// Wrapper struct for TOML round-trip testing since TOML requires a top-level table.
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct FallbackWrapper {
        fallback: Vec<FallbackEntry>,
    }

    /// Strategy to generate a valid `FallbackEntry`.
    fn arb_fallback_entry() -> impl Strategy<Value = FallbackEntry> {
        prop_oneof![
            Just(FallbackEntry::Local),
            // Non-empty alphanumeric strings that are not "local"
            "[a-zA-Z][a-zA-Z0-9_-]{0,19}"
                .prop_filter("must not be 'local'", |s| s != "local")
                .prop_map(FallbackEntry::Gateway),
        ]
    }

    /// Strategy to generate a `Vec<FallbackEntry>` of 0–8 entries.
    fn arb_fallback_vec() -> impl Strategy<Value = Vec<FallbackEntry>> {
        proptest::collection::vec(arb_fallback_entry(), 0..=8)
    }

    /// Strategy to generate a non-empty string that is not "local".
    fn arb_non_local_string() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9_-]{0,19}"
            .prop_filter("must not be 'local'", |s| s != "local")
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        // Feature: remove-deprecated-jumpserver-config, Property 1: FallbackEntry serialization round-trip
        /// **Validates: Requirements 1.3, 1.7**
        ///
        /// For any valid `Vec<FallbackEntry>`, serializing to TOML then deserializing
        /// produces an equivalent vector.
        #[test]
        fn prop_fallback_entry_round_trip(entries in arb_fallback_vec()) {
            let wrapper = FallbackWrapper { fallback: entries.clone() };
            let toml_str = toml::to_string(&wrapper).expect("serialize to TOML");
            let deserialized: FallbackWrapper = toml::from_str(&toml_str).expect("deserialize from TOML");
            prop_assert_eq!(deserialized.fallback, entries);
        }

        // Feature: config-and-legacy-cleanup, Property 2: Non-"local" strings deserialize as Gateway
        /// **Validates: Requirements 1.3, 1.7**
        ///
        /// For any non-empty string that is not "local", deserializing it as a
        /// `FallbackEntry` produces `FallbackEntry::Gateway(value)`.
        #[test]
        fn prop_non_local_string_deserializes_as_gateway(value in arb_non_local_string()) {
            // Wrap in a TOML table with a single-element array
            let toml_str = format!("fallback = [\"{}\"]", value);
            let deserialized: FallbackWrapper = toml::from_str(&toml_str).expect("deserialize from TOML");
            prop_assert_eq!(deserialized.fallback.len(), 1);
            prop_assert_eq!(&deserialized.fallback[0], &FallbackEntry::Gateway(value));
        }
    }
}
