mod client;
mod copy;
mod duration;
mod gateway;
mod inventory;
mod path;
mod reverse_proxy;
mod review;
mod secret;
mod server;
mod ssh;

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub use self::client::{ClientConfig, LocalClientConfig};
pub use self::copy::CopyConfig;
pub use self::duration::parse_duration;
pub use self::gateway::{
    DirectGatewayConfig, GatewayConfig, GatewayValidationError, JumpserverGatewayConfig,
    RESERVED_NAMES, XhodGatewayConfig, validate_fallback_references, validate_gateways,
};
pub use self::inventory::{
    DirectAuth, ServerConfigFile, ServerDefaults, ServerEntry, ServerHostConfig, SshHostEntry,
    glob_match, list_server_entries, load_server_config, parse_ssh_config, resolve_server_entry,
    resolve_ssh_host,
};
pub use self::path::{
    default_client_config_path, default_config_path, default_known_hosts_path, default_root_dir,
    default_vault_path, expand_tilde,
};
pub use self::reverse_proxy::ReverseProxyClientConfig;
pub use self::review::{
    FastAllowlistConfig, MfaConfig, ReviewAction, ReviewConfig, ReviewPolicy, ReviewPrompts,
    RiskLevel, SemanticWhitelistEntry, default_review_api_key, default_review_endpoint,
    default_review_model, default_review_system_prompt, default_review_template,
    default_semantic_whitelist,
};
pub use self::secret::{Secret, SecretConfig, SecretResolver, SecretSource};
pub use self::server::{LocalServerConfig, RemoteServerConfig, ServerConfig};
pub use self::ssh::FallbackEntry;
pub use self::ssh::SshConfig;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub ssh: SshConfig,
    pub copy: CopyConfig,
    pub review: ReviewConfig,
    #[serde(default)]
    pub secret: SecretConfig,
    #[serde(default)]
    pub gateways: Vec<GatewayConfig>,
    #[serde(default)]
    pub reverse_proxy: ReverseProxyClientConfig,
    /// Directory the config was loaded from (not serialized). The vault lives
    /// here by default so it follows the config file — e.g. `/etc/xho/secrets`
    /// when loaded from `/etc/xho/config.toml`.
    #[serde(skip)]
    pub config_dir: Option<PathBuf>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            ssh: SshConfig::default(),
            copy: CopyConfig::default(),
            review: ReviewConfig::default(),
            secret: SecretConfig::default(),
            gateways: Vec::new(),
            reverse_proxy: ReverseProxyClientConfig::default(),
            config_dir: None,
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = path.map(PathBuf::from).unwrap_or_else(default_config_path);
        if !path.exists() {
            let mut config = Self::default();
            config.config_dir = path.parent().map(PathBuf::from);
            config.expand_paths()?;
            config.validate()?;
            return Ok(config);
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let mut config: AppConfig =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        config.config_dir = path.parent().map(PathBuf::from);
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
        if let Some(vault_path) = &self.secret.vault_path {
            self.secret.vault_path = Some(expand_tilde(vault_path)?);
        }
        if let Some(key_source) = &self.secret.key_source {
            self.secret.key_source = Some(expand_tilde(key_source)?);
        }

        for gw in &mut self.gateways {
            match gw {
                GatewayConfig::Xhod(c) => {
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

        self.reverse_proxy.identity_file = expand_tilde(&self.reverse_proxy.identity_file)?;
        self.reverse_proxy.known_hosts_path = expand_tilde(&self.reverse_proxy.known_hosts_path)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.server.validate()?;
        validate_gateways(&self.gateways)?;
        validate_fallback_references(&self.ssh.fallback, &self.gateways)?;
        self.reverse_proxy.validate()?;
        Ok(())
    }

    /// Build a [`SecretResolver`] for resolving `vault:` secrets.
    ///
    /// Key source precedence:
    /// 1. `[secret].key_source` (explicit)
    /// 2. the daemon's own SSH host key (`[server.remote].host_key_path`) when
    ///    remote is enabled — it always exists, is unencrypted, and is
    ///    daemon-owned, so vault works with zero extra configuration
    /// 3. `fallback_identity` (typically `server.toml`'s `[defaults]
    ///    .identity_file`) for local-only setups with no host key
    ///
    /// Paths are expected to be already expanded.
    pub fn secret_resolver(&self, fallback_identity: Option<&str>) -> SecretResolver {
        let vault_path = self.vault_path();
        let key_source = self
            .secret
            .key_source
            .clone()
            .or_else(|| {
                if self.server.remote.enable {
                    Some(self.server.remote.host_key_path.clone())
                } else {
                    None
                }
            })
            .or_else(|| fallback_identity.map(str::to_string));
        SecretResolver::new(Some(vault_path), key_source)
    }

    /// Resolve the vault file path. Uses `[secret].vault_path` if set, else
    /// defaults to `<config_dir>/secrets` so the vault follows the config file
    /// (e.g. `/etc/xho/secrets` for `/etc/xho/config.toml`), falling back to
    /// `~/.xho/secrets` when the config directory is unknown.
    pub fn vault_path(&self) -> PathBuf {
        if let Some(vault_path) = &self.secret.vault_path {
            return PathBuf::from(vault_path);
        }
        if let Some(dir) = &self.config_dir {
            return dir.join("secrets");
        }
        crate::config::path::default_vault_path()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppConfig, ClientConfig, FallbackEntry, SshHostEntry, default_client_config_path,
        default_config_path, default_known_hosts_path, glob_match, parse_duration,
        resolve_ssh_host,
    };
    use crate::config::path::default_socket_path;
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
        assert!(glob_match("192.0.2.16?", "192.0.2.163"));
        assert!(!glob_match("192.0.2.16?", "192.0.2.16"));
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
    fn defaults_use_xho_paths() {
        assert!(default_config_path().ends_with(".xho/config.toml"));
        assert!(default_client_config_path().ends_with(".xho/client.toml"));
        assert!(default_known_hosts_path().ends_with(".xho/known_hosts"));
        let config = AppConfig::default();
        assert_eq!(config.server.local.socket_path, default_socket_path());
        assert_eq!(config.server.remote.host_key_path, "~/.xho/host_key");
        assert_eq!(
            config.server.remote.authorized_keys_path,
            "~/.xho/authorized_keys"
        );
        assert!(config.copy.preserve_mode);
        let client = ClientConfig::default();
        assert_eq!(client.local.socket_path, default_socket_path());
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
        "[a-zA-Z][a-zA-Z0-9_-]{0,19}".prop_filter("must not be 'local'", |s| s != "local")
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
