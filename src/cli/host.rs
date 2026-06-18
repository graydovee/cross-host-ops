use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::config::{
    AppConfig, GatewayConfig, RESERVED_NAMES, XhodGatewayConfig, default_config_path,
};
use crate::daemon::gateway::GatewayKind;
use crate::daemon::gateway::auth::{
    KnownHostState, fetch_remote_host_key, inspect_known_host,
    normalize_paths as normalize_remote_paths, parse_remote_target, trust_known_host,
};

use super::args::HostCommand;
use super::bootstrap::bootstrap_authorize;
use super::prompt::{prompt_for_auth_input, prompt_for_confirmation};
use crate::types::{AddressDefaults, RemoteAddress};

pub(crate) async fn run_host_command(command: HostCommand) -> Result<i32> {
    match command {
        HostCommand::Add {
            name,
            address,
            identity_file,
            known_hosts,
            accept_new_host_key,
            fingerprint,
            token,
        } => {
            remote_connect(
                name,
                address,
                identity_file,
                known_hosts,
                accept_new_host_key,
                fingerprint,
                token,
            )
            .await
        }
        HostCommand::Login { name, token } => remote_login(name, token).await,
        HostCommand::Remove { name } => remote_remove(name).await,
        HostCommand::List => remote_list(),
    }
}

/// Resolve a token: use `--token` if given, otherwise prompt interactively.
/// Empty / whitespace-only input maps to `None` (skip bootstrap).
fn resolve_token(token_arg: Option<String>, prompt_label: &str) -> Result<Option<String>> {
    match token_arg {
        Some(t) => {
            let trimmed = t.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        None => {
            let raw = prompt_for_auth_input(prompt_label, true)?;
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
    }
}

async fn remote_connect(
    name: String,
    address: String,
    identity_file_override: Option<String>,
    known_hosts_override: Option<String>,
    _accept_new_host_key: bool,
    _fingerprint: Option<String>,
    token_arg: Option<String>,
) -> Result<i32> {
    // --- Step 1: Validate <name> against RESERVED_NAMES ---
    if RESERVED_NAMES.contains(&name.as_str()) {
        eprintln!(
            "error: name '{}' is reserved (reserved names: {:?})",
            name, RESERVED_NAMES
        );
        return Ok(1);
    }

    // --- Step 2: Validate <name> against existing gateway names ---
    // Load the daemon config to check for name collisions.
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    if let Some(entry) = config.gateways.iter().find(|g| g.name() == name) {
        eprintln!(
            "error: name '{}' is already used by a {:?} gateway",
            name,
            entry.gateway_kind()
        );
        return Ok(1);
    }

    // --- Step 3: Parse <address> via RemoteAddress::parse ---
    let defaults = AddressDefaults {
        user: "xho".to_string(),
        port: 2222,
    };
    let remote_addr = match RemoteAddress::parse(&address, &defaults) {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("error: invalid address '{}': {}", address, e);
            return Ok(1);
        }
    };

    // --- Step 4: SSH host-key trust flow ---
    let (identity_file, known_hosts_path) = normalize_remote_paths(
        identity_file_override.as_deref(),
        known_hosts_override.as_deref(),
    )?;

    let target = parse_remote_target(&remote_addr.format())?;
    let public_key = fetch_remote_host_key(&target, &identity_file).await?;
    let state = inspect_known_host(
        &target.host,
        target.port,
        &public_key,
        &std::path::PathBuf::from(&known_hosts_path),
    );
    match state {
        KnownHostState::Known => {}
        KnownHostState::Unknown {
            algorithm,
            fingerprint,
        } => {
            eprintln!(
                "The authenticity of host '{}' can't be established.",
                target.address()
            );
            eprintln!("{} key fingerprint is {}.", algorithm, fingerprint);
            if !prompt_for_confirmation("trust this host key and continue")? {
                bail!("host key not trusted");
            }
            trust_known_host(
                &target.host,
                target.port,
                &public_key,
                &std::path::PathBuf::from(&known_hosts_path),
            )?;
        }
        KnownHostState::Changed {
            algorithm,
            fingerprint,
        } => {
            bail!(
                "host key for {} changed; refusing to connect ({} {})",
                target.address(),
                algorithm,
                fingerprint
            );
        }
    }

    // --- Step 5: Bootstrap (auto-append the local public key) ---
    // The bootstrap happens BEFORE we persist the gateway so a failed token
    // leaves the local config untouched (no half-configured entry).
    let token = resolve_token(
        token_arg,
        "token for remote xhod (empty to skip bootstrap)",
    )?;
    if let Some(token) = token {
        bootstrap_authorize(&remote_addr.format(), &token, Path::new(&identity_file)).await?;
    } else {
        eprintln!(
            "warning: no token provided; skipping authorized_keys bootstrap. \
             Future `xho exec`/`xho ls` calls will be rejected until the key is \
             added manually or via `xho host login`."
        );
    }

    // --- Step 6: Persist the new entry to the config file ---
    let new_entry = GatewayConfig::Xhod(XhodGatewayConfig {
        name: name.clone(),
        address: remote_addr.format(),
        identity_file: identity_file.clone(),
        known_hosts_path: known_hosts_path.clone(),
    });

    // Re-load config to persist (avoid stale state)
    let mut config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    config.gateways.push(new_entry);

    // Write the updated config atomically
    let raw = toml::to_string_pretty(&config).context("failed to serialize config")?;
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&config_path, raw)
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    println!(
        "added remote '{}' at {} and trusted host key",
        name,
        target.address()
    );
    Ok(0)
}

/// `xho host login <name>` — re-run the bootstrap against an existing gateway.
/// Reads the gateway's address/identity_file from config, prompts for a token
/// (or accepts `--token`), and calls `bootstrap_authorize`. Does not modify
/// the config file.
async fn remote_login(name: String, token_arg: Option<String>) -> Result<i32> {
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();
    let entry = config.gateways.iter().find(|g| g.name() == name);
    let Some(entry) = entry else {
        eprintln!("error: no gateway named '{name}'");
        return Ok(127);
    };
    if entry.gateway_kind() != GatewayKind::Xhod {
        eprintln!(
            "error: '{}' is a {:?} gateway; login only supports xhod",
            name,
            entry.gateway_kind()
        );
        return Ok(1);
    }
    let GatewayConfig::Xhod(c) = entry else {
        unreachable!("gateway_kind == Xhod implies Xhod variant");
    };
    let address = c.address.clone();
    let identity_file = c.identity_file.clone();
    let token = resolve_token(token_arg, "token for remote xhod")?;
    let Some(token) = token else {
        eprintln!("error: a token is required for `host login` (empty input aborted)");
        return Ok(1);
    };
    bootstrap_authorize(&address, &token, Path::new(&identity_file)).await?;
    Ok(0)
}

async fn remote_remove(name: String) -> Result<i32> {
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();

    // Find the entry with the given name
    let entry = config.gateways.iter().find(|g| g.name() == name);

    match entry {
        None => {
            eprintln!("error: name '{}' not found in gateways configuration", name);
            Ok(1)
        }
        Some(entry) if entry.gateway_kind() != GatewayKind::Xhod => {
            eprintln!(
                "error: name '{}' is a {:?} gateway; quick-remove only manages xhod entries",
                name,
                entry.gateway_kind()
            );
            Ok(1)
        }
        Some(_) => {
            // Remove the entry and persist
            let mut config = config;
            config.gateways.retain(|g| g.name() != name);

            let raw = toml::to_string_pretty(&config).context("failed to serialize config")?;
            if let Some(parent) = config_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::write(&config_path, raw)
                .with_context(|| format!("failed to write {}", config_path.display()))?;

            println!("removed remote '{}'", name);
            Ok(0)
        }
    }
}

fn remote_list() -> Result<i32> {
    let config_path = default_config_path();
    let config = AppConfig::load(Some(&config_path)).unwrap_or_default();

    if config.gateways.is_empty() {
        return Ok(0);
    }

    println!("{:<10}  {:<12}  {}", "NAME", "KIND", "ADDRESS");
    for entry in &config.gateways {
        let address = match entry {
            GatewayConfig::Xhod(c) => c.address.clone(),
            GatewayConfig::Jumpserver(c) => format!("{}:{}", c.host, c.port),
            GatewayConfig::Direct(c) => format!("{}:{}", c.host, c.port),
        };
        println!(
            "{:<10}  {:<12}  {}",
            entry.name(),
            entry.gateway_kind(),
            address
        );
    }

    Ok(0)
}
