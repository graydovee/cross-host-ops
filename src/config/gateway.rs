use std::collections::HashMap;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::secret::Secret;
use super::ssh::FallbackEntry;

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
            GatewayConfig::Xhod(c) => {
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
    Xhod(XhodGatewayConfig),
    Jumpserver(JumpserverGatewayConfig),
    Direct(DirectGatewayConfig),
}

impl GatewayConfig {
    /// Returns the name of this gateway entry.
    pub fn name(&self) -> &str {
        match self {
            Self::Xhod(c) => &c.name,
            Self::Jumpserver(c) => &c.name,
            Self::Direct(c) => &c.name,
        }
    }

    /// Returns the GatewayKind for this config variant.
    pub fn gateway_kind(&self) -> crate::daemon::gateway::GatewayKind {
        use crate::daemon::gateway::GatewayKind;
        match self {
            Self::Xhod(_) => GatewayKind::Xhod,
            Self::Jumpserver(_) => GatewayKind::Jumpserver,
            Self::Direct(_) => GatewayKind::Direct,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct XhodGatewayConfig {
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
    pub password: Option<Secret>,
}
