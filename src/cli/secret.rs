//! `xho secret` — manage the local encrypted vault.
//!
//! All operations are local file operations on the daemon host; nothing goes
//! through the xhod RPC. Encryption requires the configuration files and the
//! identity file used to derive the vault key to be present on the same host,
//! so run this where the config lives (locally, or over SSH on a remote host).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use toml_edit::{DocumentMut, Item, TableLike};

use crate::config::{
    AppConfig, ServerConfigFile, default_config_path, expand_tilde, load_server_config,
};
use crate::secret::Vault;

use super::args::SecretCommand;
use super::prompt::prompt_for_auth_input;

pub(crate) fn run_secret_command(config_path: Option<&Path>, command: SecretCommand) -> Result<i32> {
    match command {
        SecretCommand::Encrypt { dry_run } => encrypt(config_path, dry_run),
        SecretCommand::Set { name } => set(config_path, &name),
        SecretCommand::List => list(config_path),
        SecretCommand::Rekey { old, new } => rekey(config_path, &old, &new),
    }
}

/// Load the daemon config (and its sibling server.toml) from the given path,
/// or the default `~/.xho/config.toml` when `config_path` is `None`.
fn load_app(config_path: Option<&Path>) -> Result<AppConfig> {
    Ok(AppConfig::load(config_path).unwrap_or_default())
}

/// Backends that indicate a value is already an indirection reference and
/// should be left untouched by `encrypt`.
const REFERENCE_PREFIXES: &[&str] = &["env:", "file:", "vault:"];

fn is_reference(value: &str) -> bool {
    REFERENCE_PREFIXES
        .iter()
        .any(|prefix| value.starts_with(prefix))
}

/// Resolve the vault path from the loaded config: `[secret].vault_path` if
/// set, else `<config_dir>/secrets` (so the vault follows the config file).
fn vault_path(config: &AppConfig) -> PathBuf {
    config.vault_path()
}

/// Resolve the identity file used to derive the vault key.
///
/// Priority: `[secret].key_source` → the daemon's own host key (when remote is
/// enabled) → `server.toml`'s `[defaults].identity_file`. Returns an error with
/// guidance if none is available.
fn key_source(config: &AppConfig, server_config: &ServerConfigFile) -> Result<String> {
    if let Some(key_source) = &config.secret.key_source {
        return expand_tilde(key_source);
    }
    if config.server.remote.enable {
        return Ok(config.server.remote.host_key_path.clone());
    }
    if let Some(identity) = &server_config.defaults.identity_file {
        // server.toml defaults are already tilde-expanded by load_server_config.
        return Ok(identity.clone());
    }
    bail!(
        "no vault key source: set [secret].key_source in config.toml, enable \
         [server.remote] (to reuse the host key), or set [defaults].identity_file in server.toml"
    )
}

fn set(config_path: Option<&Path>, name: &str) -> Result<i32> {
    let config = load_app(config_path)?;
    let server_config = load_server_config(Path::new(&config.ssh.server_config_path))?;
    let key = key_source(&config, &server_config)?;
    let path = vault_path(&config);

    let value = prompt_for_auth_input(&format!("Enter secret for '{name}'"), true)?;
    if value.is_empty() {
        bail!("empty secret; nothing stored");
    }

    let mut vault = Vault::open_or_init(&path, &key)?;
    let existed = vault.contains(name);
    vault.set(name, &value, &key)?;
    vault.save()?;

    if existed {
        println!("updated secret '{name}' in {}", path.display());
    } else {
        println!("stored secret '{name}' in {}", path.display());
    }
    Ok(0)
}

fn list(config_path: Option<&Path>) -> Result<i32> {
    let config = load_app(config_path)?;
    let path = vault_path(&config);
    if !path.exists() {
        println!("no vault at {}", path.display());
        return Ok(0);
    }
    let vault = Vault::open(&path)?;
    let names = vault.list();
    if names.is_empty() {
        println!("vault {} is empty", path.display());
        return Ok(0);
    }
    for name in names {
        println!("{name}");
    }
    Ok(0)
}

fn rekey(config_path: Option<&Path>, old: &str, new: &str) -> Result<i32> {
    let config = load_app(config_path)?;
    let path = vault_path(&config);
    let old = expand_tilde(old)?;
    let new = expand_tilde(new)?;

    let mut vault = Vault::open(&path)?;
    vault.rekey(&old, &new)?;
    vault.save()?;
    println!("re-encrypted vault {} under {}", path.display(), new);
    Ok(0)
}

/// A plaintext secret found in a config file, with its location for rewriting.
struct Found {
    /// Vault entry name to store it under (also the `vault:` reference suffix).
    entry: String,
    /// The plaintext value.
    plaintext: String,
    /// Which file it came from, for reporting.
    file: &'static str,
}

fn encrypt(config_path_arg: Option<&Path>, dry_run: bool) -> Result<i32> {
    let config = load_app(config_path_arg)?;
    let server_config = load_server_config(Path::new(&config.ssh.server_config_path))?;
    // Resolve the config file path actually loaded, so rewriting targets the
    // same file `--config` pointed at (not always the default).
    let config_path = config_path_arg
        .map(PathBuf::from)
        .unwrap_or_else(default_config_path);
    let server_path = PathBuf::from(&config.ssh.server_config_path);

    // Parse both files with toml_edit so comments/formatting survive rewriting.
    let mut config_doc = read_doc(&config_path)?;
    let mut server_doc = read_doc(&server_path)?;

    let mut found: Vec<Found> = Vec::new();
    if let Some(doc) = server_doc.as_mut() {
        collect_server_secrets(doc, &mut found);
    }
    if let Some(doc) = config_doc.as_mut() {
        collect_config_secrets(doc, &mut found);
    }

    if found.is_empty() {
        println!("no plaintext secrets found; nothing to encrypt");
        return Ok(0);
    }

    if dry_run {
        println!("would encrypt {} secret(s):", found.len());
        for f in &found {
            println!("  {} ({}) -> vault:{}", f.entry, f.file, f.entry);
        }
        println!("(dry run; no files modified)");
        return Ok(0);
    }

    let key = key_source(&config, &server_config)?;
    let vault_path = vault_path(&config);
    let mut vault = Vault::open_or_init(&vault_path, &key)?;
    for f in &found {
        vault.set(&f.entry, &f.plaintext, &key)?;
    }
    vault.save()?;

    // Rewrite the documents in place now that the vault is persisted.
    if let Some(doc) = server_doc.as_mut() {
        rewrite_server_secrets(doc);
        backup_and_write(&server_path, &doc.to_string())?;
    }
    if let Some(doc) = config_doc.as_mut() {
        rewrite_config_secrets(doc);
        backup_and_write(&config_path, &doc.to_string())?;
    }

    println!(
        "encrypted {} secret(s) into {}",
        found.len(),
        vault_path.display()
    );
    for f in &found {
        println!("  {} ({}) -> vault:{}", f.entry, f.file, f.entry);
    }
    Ok(0)
}

fn read_doc(path: &Path) -> Result<Option<DocumentMut>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let doc = raw
        .parse::<DocumentMut>()
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(doc))
}

/// Extract a plaintext string value at `table[key]`, if present and not already
/// an indirection reference.
fn plaintext_at(table: &dyn TableLike, key: &str) -> Option<String> {
    let value = table.get(key)?.as_str()?;
    (!is_reference(value)).then(|| value.to_string())
}

fn collect_server_secrets(doc: &DocumentMut, found: &mut Vec<Found>) {
    let Some(servers) = doc.get("servers").and_then(Item::as_table) else {
        return;
    };
    for (alias, entry) in servers.iter() {
        if let Some(table) = entry.as_table_like() {
            if let Some(plaintext) = plaintext_at(table, "password") {
                found.push(Found {
                    entry: format!("server.{alias}.password"),
                    plaintext,
                    file: "server.toml",
                });
            }
        }
    }
}

fn rewrite_server_secrets(doc: &mut DocumentMut) {
    let Some(servers) = doc.get_mut("servers").and_then(Item::as_table_mut) else {
        return;
    };
    let aliases: Vec<String> = servers.iter().map(|(k, _)| k.to_string()).collect();
    for alias in aliases {
        let Some(entry) = servers.get_mut(&alias).and_then(Item::as_table_like_mut) else {
            continue;
        };
        if entry
            .get("password")
            .and_then(Item::as_str)
            .is_some_and(|v| !is_reference(v))
        {
            set_str_preserving_decor(entry, "password", &format!("vault:server.{alias}.password"));
        }
    }
}

fn collect_config_secrets(doc: &DocumentMut, found: &mut Vec<Found>) {
    // review.api_key
    if let Some(review) = doc.get("review").and_then(Item::as_table) {
        if let Some(plaintext) = plaintext_at(review, "api_key") {
            found.push(Found {
                entry: "review.api_key".to_string(),
                plaintext,
                file: "config.toml",
            });
        }
        // review.headers.* values
        if let Some(headers) = review.get("headers").and_then(Item::as_table) {
            for (header, _) in headers.iter() {
                if let Some(plaintext) = plaintext_at(headers, header) {
                    found.push(Found {
                        entry: format!("review.header.{header}"),
                        plaintext,
                        file: "config.toml",
                    });
                }
            }
        }
    }

    // [[gateways]] password / totp_secret_base32
    if let Some(gateways) = doc.get("gateways").and_then(Item::as_array_of_tables) {
        for gw in gateways.iter() {
            let name = gw.get("name").and_then(Item::as_str).unwrap_or("unnamed");
            if let Some(plaintext) = plaintext_at(gw, "password") {
                found.push(Found {
                    entry: format!("gateway.{name}.password"),
                    plaintext,
                    file: "config.toml",
                });
            }
            // totp uses an empty string to mean "no MFA"; skip empties.
            if let Some(value) = gw.get("totp_secret_base32").and_then(Item::as_str) {
                if !value.is_empty() && !is_reference(value) {
                    found.push(Found {
                        entry: format!("gateway.{name}.totp"),
                        plaintext: value.to_string(),
                        file: "config.toml",
                    });
                }
            }
        }
    }
}

fn rewrite_config_secrets(doc: &mut DocumentMut) {
    if let Some(review) = doc.get_mut("review").and_then(Item::as_table_mut) {
        if let Some(value) = review.get("api_key").and_then(Item::as_str) {
            if !is_reference(value) {
                set_str_preserving_decor(review, "api_key", "vault:review.api_key");
            }
        }
        if let Some(headers) = review.get_mut("headers").and_then(Item::as_table_mut) {
            let keys: Vec<String> = headers.iter().map(|(k, _)| k.to_string()).collect();
            for header in keys {
                if let Some(value) = headers.get(&header).and_then(Item::as_str) {
                    if !is_reference(value) {
                        set_str_preserving_decor(
                            headers,
                            &header,
                            &format!("vault:review.header.{header}"),
                        );
                    }
                }
            }
        }
    }

    if let Some(gateways) = doc.get_mut("gateways").and_then(Item::as_array_of_tables_mut) {
        for gw in gateways.iter_mut() {
            let name = gw
                .get("name")
                .and_then(Item::as_str)
                .unwrap_or("unnamed")
                .to_string();
            if let Some(value) = gw.get("password").and_then(Item::as_str) {
                if !is_reference(value) {
                    set_str_preserving_decor(
                        gw,
                        "password",
                        &format!("vault:gateway.{name}.password"),
                    );
                }
            }
            if let Some(value) = gw.get("totp_secret_base32").and_then(Item::as_str) {
                if !value.is_empty() && !is_reference(value) {
                    set_str_preserving_decor(
                        gw,
                        "totp_secret_base32",
                        &format!("vault:gateway.{name}.totp"),
                    );
                }
            }
        }
    }
}

/// Replace the string value at `table[key]`, preserving the surrounding decor
/// (whitespace and any trailing inline comment). Using `toml_edit::value(...)`
/// would discard the decor and drop inline comments on the line.
fn set_str_preserving_decor(table: &mut dyn TableLike, key: &str, new_value: &str) {
    let Some(item) = table.get_mut(key) else {
        return;
    };
    let decor = item
        .as_value()
        .map(|v| v.decor().clone())
        .unwrap_or_default();
    let mut new = toml_edit::Value::from(new_value);
    *new.decor_mut() = decor;
    *item = Item::Value(new);
}

/// Back up `path` to `path.bak`, then atomically write `contents` with the
/// original file's permissions preserved.
fn backup_and_write(path: &Path, contents: &str) -> Result<()> {
    let backup = path.with_extension(format!(
        "{}bak",
        path.extension()
            .and_then(|e| e.to_str())
            .map(|e| format!("{e}."))
            .unwrap_or_default()
    ));
    std::fs::copy(path, &backup)
        .with_context(|| format!("failed to back up {} to {}", path.display(), backup.display()))?;

    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("config path has no parent directory"))?;
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("config")
    ));
    std::fs::write(&tmp, contents).with_context(|| format!("failed to write {}", tmp.display()))?;
    copy_permissions(path, &tmp);
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    println!("backed up {} -> {}", path.display(), backup.display());
    Ok(())
}

#[cfg(unix)]
fn copy_permissions(from: &Path, to: &Path) {
    if let Ok(meta) = std::fs::metadata(from) {
        let _ = std::fs::set_permissions(to, meta.permissions());
    }
}

#[cfg(not(unix))]
fn copy_permissions(_from: &Path, _to: &Path) {}
