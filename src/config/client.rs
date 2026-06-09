use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::path::{default_client_config_path, expand_tilde};

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
            socket_path: "~/.xho/xhod.sock".to_string(),
            auto_start: true,
        }
    }
}
