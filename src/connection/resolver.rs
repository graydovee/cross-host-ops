use std::path::Path;

use anyhow::{Result, anyhow, bail};

use crate::config::{
    AppConfig, FallbackTransport, load_server_config, parse_ssh_config, resolve_server_entry,
    resolve_ssh_host,
};

use super::types::{DirectTarget, ResolvedTarget, TargetTransport};

pub fn resolve_target(input: &str, config: &AppConfig) -> Result<Vec<ResolvedTarget>> {
    let ip = derive_target_ip(input);
    let mut candidates = Vec::new();

    append_server_candidates(&mut candidates, input, &ip, config)?;
    append_fallback_candidates(&mut candidates, input, &ip, config)?;

    if candidates.is_empty() {
        bail!(
            "target {} does not match server.toml or ~/.ssh/config and jumpserver is disabled",
            ip
        );
    }
    Ok(candidates)
}

fn append_server_candidates(
    candidates: &mut Vec<ResolvedTarget>,
    input: &str,
    ip: &str,
    config: &AppConfig,
) -> Result<()> {
    let server_config = load_server_config(Path::new(&config.ssh.server_config_path))?;

    if let Some(server) = server_config.servers.get(input) {
        let entry = resolve_server_entry(input, server, &server_config.defaults)?;
        candidates.push(make_server_target(input, ip, &entry));
        return Ok(());
    }

    if let Some((alias, server)) = server_config
        .servers
        .iter()
        .find(|(_, server)| server.host == input)
    {
        let entry = resolve_server_entry(alias, server, &server_config.defaults)?;
        candidates.push(make_server_target(input, ip, &entry));
        return Ok(());
    }

    if let Some((alias, server)) = server_config.servers.iter().find(|(_, server)| server.host == ip)
    {
        let entry = resolve_server_entry(alias, server, &server_config.defaults)?;
        candidates.push(make_server_target(input, ip, &entry));
    }
    Ok(())
}

fn append_fallback_candidates(
    candidates: &mut Vec<ResolvedTarget>,
    input: &str,
    ip: &str,
    config: &AppConfig,
) -> Result<()> {
    for transport in &config.ssh.fallback {
        match transport {
            FallbackTransport::SshConfig => {
                if let Some(target) = resolve_ssh_config_target(input, ip, config)? {
                    candidates.push(target);
                }
            }
            FallbackTransport::Jumpserver if config.jumpserver.enabled => {
                candidates.push(ResolvedTarget {
                    input: input.to_string(),
                    ip: ip.to_string(),
                    key: format!("target:{}", input),
                    transport: TargetTransport::Jump,
                    direct: None,
                    target_label: input.to_string(),
                });
            }
            FallbackTransport::Jumpserver => {}
        }
    }
    Ok(())
}

fn resolve_ssh_config_target(
    input: &str,
    ip: &str,
    config: &AppConfig,
) -> Result<Option<ResolvedTarget>> {
    let ssh_path = Path::new(&config.ssh.ssh_config_path);
    let entries = parse_ssh_config(ssh_path)?;
    if let Some(entry) = resolve_ssh_host(&entries, &ip) {
        if entry.proxy_command.is_some() {
            bail!("ProxyCommand is not supported for direct SSH targets");
        }
        let direct = DirectTarget {
            host: ip.to_string(),
            host_name: entry.host_name.unwrap_or_else(|| ip.to_string()),
            port: entry.port.unwrap_or(22),
            user: entry
                .user
                .ok_or_else(|| anyhow!("missing User for SSH host {}", ip))?,
            auth: crate::config::DirectAuth::Key {
                identity_file: entry
                    .identity_file
                    .ok_or_else(|| anyhow!("missing IdentityFile for SSH host {}", ip))?,
            },
            proxy_command: entry.proxy_command,
            pubkey_accepted_algorithms: entry.pubkey_accepted_algorithms,
        };
        return Ok(Some(ResolvedTarget {
            input: input.to_string(),
            key: format!("target:{}", input),
            ip: ip.to_string(),
            transport: TargetTransport::Direct,
            direct: Some(direct),
            target_label: input.to_string(),
        }));
    }
    Ok(None)
}

fn make_server_target(input: &str, ip: &str, entry: &crate::config::ServerEntry) -> ResolvedTarget {
    ResolvedTarget {
        input: input.to_string(),
        ip: ip.to_string(),
        key: format!("target:{}", input),
        transport: TargetTransport::Direct,
        direct: Some(DirectTarget {
            host: entry.host.clone(),
            host_name: entry.host.clone(),
            port: entry.port,
            user: entry.user.clone(),
            auth: entry.auth.clone(),
            proxy_command: None,
            pubkey_accepted_algorithms: None,
        }),
        target_label: entry.alias.clone(),
    }
}

pub fn derive_target_ip(input: &str) -> String {
    let parts = input.split('-').collect::<Vec<_>>();
    if parts.len() >= 4 {
        let tail = &parts[parts.len() - 4..];
        if tail
            .iter()
            .all(|segment| segment.chars().all(|ch| ch.is_ascii_digit()))
        {
            return tail.join(".");
        }
    }
    input.to_string()
}

#[cfg(test)]
mod tests {
    use super::derive_target_ip;

    #[test]
    fn derives_target_ip_from_suffix() {
        assert_eq!(derive_target_ip("foo-192-0-2-163"), "192.0.2.163");
        assert_eq!(derive_target_ip("192.0.2.163"), "192.0.2.163");
    }
}
