use std::path::Path;

use anyhow::{Result, anyhow, bail};

use crate::config::{AppConfig, parse_ssh_config, resolve_ssh_host};

use super::types::{DirectTarget, ResolvedTarget, TargetTransport};

pub fn resolve_target(input: &str, config: &AppConfig) -> Result<ResolvedTarget> {
    let ip = derive_target_ip(input);
    let ssh_path = Path::new(&config.ssh.ssh_config_path);
    let entries = parse_ssh_config(ssh_path)?;
    if let Some(entry) = resolve_ssh_host(&entries, &ip) {
        if entry.proxy_command.is_some() {
            bail!("ProxyCommand is not supported for direct SSH targets");
        }
        let direct = DirectTarget {
            host: ip.clone(),
            host_name: entry.host_name.unwrap_or_else(|| ip.clone()),
            port: entry.port.unwrap_or(22),
            user: entry
                .user
                .ok_or_else(|| anyhow!("missing User for SSH host {}", ip))?,
            identity_file: entry
                .identity_file
                .ok_or_else(|| anyhow!("missing IdentityFile for SSH host {}", ip))?,
            proxy_command: entry.proxy_command,
            pubkey_accepted_algorithms: entry.pubkey_accepted_algorithms,
        };
        return Ok(ResolvedTarget {
            input: input.to_string(),
            key: format!("direct:{}", ip),
            ip,
            transport: TargetTransport::Direct,
            direct: Some(direct),
        });
    }
    if !config.jumpserver.enabled {
        bail!(
            "target {} does not match ~/.ssh/config and jumpserver is disabled",
            ip
        );
    }
    Ok(ResolvedTarget {
        input: input.to_string(),
        key: format!("jump:{}", ip),
        ip,
        transport: TargetTransport::Jump,
        direct: None,
    })
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
