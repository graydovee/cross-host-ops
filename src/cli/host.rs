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
use super::prompt::prompt_for_confirmation;
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
        } => {
            remote_connect(
                name,
                address,
                identity_file,
                known_hosts,
                accept_new_host_key,
                fingerprint,
            )
            .await
        }
        HostCommand::Remove { name } => remote_remove(name).await,
        HostCommand::List => remote_list(),
    }
}

async fn remote_connect(
    name: String,
    address: String,
    identity_file_override: Option<String>,
    known_hosts_override: Option<String>,
    _accept_new_host_key: bool,
    _fingerprint: Option<String>,
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

    // --- Step 5: Persist the new entry to the config file ---
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
