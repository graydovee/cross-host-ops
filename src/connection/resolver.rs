use std::path::Path;

use anyhow::{Result, anyhow, bail};

use crate::config::{
    AppConfig, DirectAuth, FallbackEntry, JumpHostConfig, ServerConfigFile,
    load_server_config, parse_ssh_config, resolve_server_entry, resolve_ssh_host,
};
use crate::jump::JumpHostKind;
use crate::jump::types::{EndTarget, EndTargetId, JumpHopRef, ServerListSource, TargetRoute};
use crate::protocol::ServerListRow;

// ---------------------------------------------------------------------------
// Resolver — new struct producing Vec<TargetRoute> directly (task 6.2)
// ---------------------------------------------------------------------------

/// The reserved alias representing the local daemon's own `server.toml`.
const LOCAL_SOURCE_ALIAS: &str = "local";

/// Pure resolver that maps a CLI target string into an ordered list of
/// `TargetRoute` candidates. The same inputs always produce the same outputs
/// (idempotence, Req 7.1).
pub struct Resolver<'a> {
    config: &'a AppConfig,
    server_config: &'a ServerConfigFile,
    jump_hosts: &'a [JumpHostConfig],
    /// Optional pre-computed merged server-list view from the aggregator.
    /// When provided, bare-alias resolution checks all sources for uniqueness
    /// and explicit `<jump_name>:<server_alias>` lookups verify the alias
    /// exists in the named source.
    merged_rows: &'a [ServerListRow],
}

impl<'a> Resolver<'a> {
    pub fn new(
        config: &'a AppConfig,
        server_config: &'a ServerConfigFile,
        jump_hosts: &'a [JumpHostConfig],
    ) -> Self {
        Self {
            config,
            server_config,
            jump_hosts,
            merged_rows: &[],
        }
    }

    /// Create a resolver with a pre-computed merged server-list view.
    /// This enables cross-source bare-alias disambiguation (Req 15.6, 15.7).
    pub fn with_merged_view(
        config: &'a AppConfig,
        server_config: &'a ServerConfigFile,
        jump_hosts: &'a [JumpHostConfig],
        merged_rows: &'a [ServerListRow],
    ) -> Self {
        Self {
            config,
            server_config,
            jump_hosts,
            merged_rows,
        }
    }

    /// Resolve a CLI target string into candidate `TargetRoute`s.
    ///
    /// Parsing rules:
    /// 1. `<jump_name>:<server_alias>` — explicit qualification.
    /// 2. `<server_alias>` — bare, merged-view lookup.
    /// 3. `<host_or_ip>` — legacy SSH-config / IP fallback.
    ///
    /// Candidate ordering:
    /// 1. Server-config matches against the named source.
    /// 2. `ssh.fallback`-driven candidates in declared order.
    /// 3. No implicit fan-out to all `rhopd` jump hosts.
    pub fn resolve(&self, input: &str) -> Result<Vec<TargetRoute>> {
        // Try explicit `<jump_name>:<server_alias>` form first.
        if let Some((jump_name, server_alias)) = parse_explicit_qualified(input) {
            return self.resolve_explicit(jump_name, server_alias);
        }

        // Try bare `<server_alias>` against the merged server-list view.
        let mut candidates = Vec::new();

        // If a merged view is available, use it for cross-source disambiguation.
        if !self.merged_rows.is_empty() {
            match self.resolve_bare_from_merged_view(input) {
                Ok(Some(routes)) => return Ok(routes),
                Ok(None) => {
                    // Not found in merged view, fall through to legacy path.
                }
                Err(e) => return Err(e), // Ambiguous — propagate error.
            }
        } else {
            // No merged view: fall back to local server config only.
            self.append_server_config_routes(&mut candidates, input);

            // If we found server-config matches, return them without fallback.
            if !candidates.is_empty() {
                return Ok(candidates);
            }
        }

        // Fall through to `ssh.fallback`-driven candidates (legacy path).
        self.append_fallback_routes(&mut candidates, input)?;

        if candidates.is_empty() {
            bail!(
                "target '{}' does not match any server config entry or ssh.fallback source",
                input
            );
        }
        Ok(candidates)
    }

    /// Resolve an explicitly qualified `<jump_name>:<server_alias>`.
    fn resolve_explicit(&self, jump_name: &str, server_alias: &str) -> Result<Vec<TargetRoute>> {
        if jump_name == LOCAL_SOURCE_ALIAS {
            // Look up in the local server config only.
            if let Some(route) = self.lookup_local_server(server_alias) {
                return Ok(vec![route]);
            }
            bail!(
                "server alias '{}' not found in local server config",
                server_alias
            );
        }

        // Look up the jump host by name.
        let jh = self
            .jump_hosts
            .iter()
            .find(|jh| jh.name == jump_name)
            .ok_or_else(|| anyhow!("jump host name '{}' not found", jump_name))?;

        // If we have a merged view, verify the server alias exists on this source.
        if !self.merged_rows.is_empty() {
            let source = ServerListSource::JumpHost(jump_name.to_string());
            let found = self
                .merged_rows
                .iter()
                .any(|row| row.source == source && row.server.alias == server_alias);
            if !found {
                bail!(
                    "server alias '{}' not found on jump host '{}'",
                    server_alias,
                    jump_name
                );
            }
        }

        // Build a route through this jump host to the named server alias.
        let route = TargetRoute {
            hops: vec![JumpHopRef {
                name: jh.name.clone(),
                kind: jh.kind,
            }],
            end_target: EndTarget {
                id: EndTargetId(format!("target:{}", server_alias)),
                alias: server_alias.to_string(),
            },
        };
        Ok(vec![route])
    }

    /// Resolve a bare `<server_alias>` against the merged server-list view.
    ///
    /// Returns:
    /// - `Ok(Some(routes))` if the alias is found in exactly one source.
    /// - `Ok(None)` if the alias is not found in any source.
    /// - `Err(...)` if the alias is ambiguous (found in multiple sources).
    fn resolve_bare_from_merged_view(&self, alias: &str) -> Result<Option<Vec<TargetRoute>>> {
        // Collect all sources that contain this server alias.
        let matching_rows: Vec<&ServerListRow> = self
            .merged_rows
            .iter()
            .filter(|row| row.server.alias == alias)
            .collect();

        if matching_rows.is_empty() {
            return Ok(None);
        }

        // Deduplicate by source — we only care about unique sources.
        let mut unique_sources: Vec<&ServerListSource> = Vec::new();
        for row in &matching_rows {
            if !unique_sources.iter().any(|s| *s == &row.source) {
                unique_sources.push(&row.source);
            }
        }

        if unique_sources.len() == 1 {
            // Unique: build the appropriate route based on the source.
            let source = unique_sources[0];
            let route = match source {
                ServerListSource::Local => {
                    // Direct route (no hops) for local server config entries.
                    TargetRoute {
                        hops: vec![],
                        end_target: EndTarget {
                            id: EndTargetId(format!("target:{}", alias)),
                            alias: alias.to_string(),
                        },
                    }
                }
                ServerListSource::JumpHost(jump_alias) => {
                    // Route through the named jump host.
                    let kind = self
                        .jump_hosts
                        .iter()
                        .find(|jh| &jh.name == jump_alias)
                        .map(|jh| jh.kind)
                        .unwrap_or(JumpHostKind::Rhopd);
                    TargetRoute {
                        hops: vec![JumpHopRef {
                            name: jump_alias.clone(),
                            kind,
                        }],
                        end_target: EndTarget {
                            id: EndTargetId(format!("target:{}", alias)),
                            alias: alias.to_string(),
                        },
                    }
                }
            };
            return Ok(Some(vec![route]));
        }

        // Ambiguous: found in multiple sources. Build the candidate list.
        let candidates: Vec<String> = unique_sources
            .iter()
            .map(|source| match source {
                ServerListSource::Local => format!("local:{}", alias),
                ServerListSource::JumpHost(jump_alias) => format!("{}:{}", jump_alias, alias),
            })
            .collect();

        bail!(
            "server alias '{}' is ambiguous; found in: {}",
            alias,
            candidates.join(", ")
        );
    }

    /// Append routes from the local server config for a bare alias.
    fn append_server_config_routes(&self, candidates: &mut Vec<TargetRoute>, input: &str) {
        // Check by alias first.
        if let Some(route) = self.lookup_local_server(input) {
            candidates.push(route);
            return;
        }

        // Check by host match.
        if let Some(route) = self.lookup_local_server_by_host(input) {
            candidates.push(route);
            return;
        }

        // Check by derived IP match.
        let ip = derive_target_ip(input);
        if ip != input {
            if let Some(route) = self.lookup_local_server_by_host(&ip) {
                candidates.push(route);
            }
        }
    }

    /// Append fallback-driven candidates in the order declared in `ssh.fallback`.
    /// When `ssh.fallback` is empty or all entries are disabled, this contributes
    /// zero candidates.
    fn append_fallback_routes(
        &self,
        candidates: &mut Vec<TargetRoute>,
        input: &str,
    ) -> Result<()> {
        let ip = derive_target_ip(input);

        for entry in &self.config.ssh.fallback {
            match entry {
                FallbackEntry::Local => {
                    if let Some(route) = self.resolve_ssh_config_route(input, &ip)? {
                        candidates.push(route);
                    }
                }
                FallbackEntry::JumpHost(name) => {
                    // Look up the named [[jump_hosts]] entry
                    let jh = self
                        .jump_hosts
                        .iter()
                        .find(|jh| jh.name == *name)
                        .ok_or_else(|| anyhow!(
                            "ssh.fallback references jump host '{}' which is not defined in [[jump_hosts]]",
                            name
                        ))?;
                    candidates.push(TargetRoute {
                        hops: vec![JumpHopRef {
                            name: jh.name.clone(),
                            kind: jh.kind,
                        }],
                        end_target: EndTarget {
                            id: EndTargetId(format!("target:{}", input)),
                            alias: input.to_string(),
                        },
                    });
                }
            }
        }
        Ok(())
    }

    /// Look up a server alias in the local `ServerConfigFile` by alias name.
    fn lookup_local_server(&self, alias: &str) -> Option<TargetRoute> {
        let server = self.server_config.servers.get(alias)?;
        let entry = resolve_server_entry(alias, server, &self.server_config.defaults).ok()?;
        Some(TargetRoute {
            hops: vec![],
            end_target: EndTarget {
                id: EndTargetId(format!("target:{}", entry.alias)),
                alias: entry.alias,
            },
        })
    }

    /// Look up a server in the local `ServerConfigFile` by host field.
    fn lookup_local_server_by_host(&self, host: &str) -> Option<TargetRoute> {
        let (alias, server) = self
            .server_config
            .servers
            .iter()
            .find(|(_, s)| s.host == host)?;
        let entry = resolve_server_entry(alias, server, &self.server_config.defaults).ok()?;
        Some(TargetRoute {
            hops: vec![],
            end_target: EndTarget {
                id: EndTargetId(format!("target:{}", entry.alias)),
                alias: entry.alias,
            },
        })
    }

    /// Resolve via SSH config as a fallback, producing a direct (zero-hop) route.
    fn resolve_ssh_config_route(&self, input: &str, ip: &str) -> Result<Option<TargetRoute>> {
        let ssh_path = Path::new(&self.config.ssh.ssh_config_path);
        let entries = parse_ssh_config(ssh_path)?;
        if let Some(entry) = resolve_ssh_host(&entries, ip) {
            if entry.proxy_command.is_some() {
                bail!("ProxyCommand is not supported for direct SSH targets");
            }
            // Verify minimum required fields exist.
            if entry.user.is_none() {
                return Ok(None);
            }
            if entry.identity_file.is_none() {
                return Ok(None);
            }
            return Ok(Some(TargetRoute {
                hops: vec![],
                end_target: EndTarget {
                    id: EndTargetId(format!("target:{}", input)),
                    alias: input.to_string(),
                },
            }));
        }
        Ok(None)
    }
}

/// Parse an input string as `<jump_name>:<server_alias>`.
/// Returns `None` if the input does not contain exactly one colon that is not
/// at the start or end, or if either part is empty.
fn parse_explicit_qualified(input: &str) -> Option<(&str, &str)> {
    // Avoid matching bare IPv6 addresses or port-like patterns.
    // A valid explicit form has exactly one colon with non-empty parts on both sides.
    let colon_pos = input.find(':')?;
    let jump_name = &input[..colon_pos];
    let server_alias = &input[colon_pos + 1..];

    // Both parts must be non-empty.
    if jump_name.is_empty() || server_alias.is_empty() {
        return None;
    }

    // If the part after the colon is purely numeric, treat it as a port (e.g.
    // "host:22") rather than an explicit qualification.
    if server_alias.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    // If there are multiple colons, this might be an IPv6 address — don't treat
    // it as explicit qualification.
    if input.matches(':').count() > 1 {
        return None;
    }

    Some((jump_name, server_alias))
}

/// Resolve a direct target (for pool use) by looking up the end target alias
/// in server.toml or SSH config. Returns a `DirectTarget` suitable for
/// `DirectSshConnection::connect`.
pub fn resolve_direct_target_for_route(
    input: &str,
    config: &AppConfig,
) -> anyhow::Result<super::types::DirectTarget> {
    let ip = derive_target_ip(input);

    // Try server.toml first
    let server_config = load_server_config(Path::new(&config.ssh.server_config_path))?;
    if let Some(server) = server_config.servers.get(input) {
        let entry = resolve_server_entry(input, server, &server_config.defaults)?;
        return Ok(super::types::DirectTarget {
            host: entry.host.clone(),
            host_name: entry.host.clone(),
            port: entry.port,
            user: entry.user.clone(),
            auth: entry.auth.clone(),
            proxy_command: None,
            pubkey_accepted_algorithms: None,
        });
    }
    if let Some((alias, server)) = server_config.servers.iter().find(|(_, s)| s.host == input) {
        let entry = resolve_server_entry(alias, server, &server_config.defaults)?;
        return Ok(super::types::DirectTarget {
            host: entry.host.clone(),
            host_name: entry.host.clone(),
            port: entry.port,
            user: entry.user.clone(),
            auth: entry.auth.clone(),
            proxy_command: None,
            pubkey_accepted_algorithms: None,
        });
    }
    if ip != input {
        if let Some((alias, server)) = server_config.servers.iter().find(|(_, s)| s.host == ip) {
            let entry = resolve_server_entry(alias, server, &server_config.defaults)?;
            return Ok(super::types::DirectTarget {
                host: entry.host.clone(),
                host_name: entry.host.clone(),
                port: entry.port,
                user: entry.user.clone(),
                auth: entry.auth.clone(),
                proxy_command: None,
                pubkey_accepted_algorithms: None,
            });
        }
    }

    // Try SSH config
    let ssh_path = Path::new(&config.ssh.ssh_config_path);
    let entries = parse_ssh_config(ssh_path)?;
    if let Some(entry) = resolve_ssh_host(&entries, &ip) {
        if entry.proxy_command.is_some() {
            anyhow::bail!("ProxyCommand is not supported for direct SSH targets");
        }
        return Ok(super::types::DirectTarget {
            host: ip.to_string(),
            host_name: entry.host_name.unwrap_or_else(|| ip.to_string()),
            port: entry.port.unwrap_or(22),
            user: entry
                .user
                .ok_or_else(|| anyhow!("missing User for SSH host {}", ip))?,
            auth: DirectAuth::Key {
                identity_file: entry
                    .identity_file
                    .ok_or_else(|| anyhow!("missing IdentityFile for SSH host {}", ip))?,
            },
            proxy_command: None,
            pubkey_accepted_algorithms: entry.pubkey_accepted_algorithms,
        });
    }

    anyhow::bail!("no direct target details found for '{}'", input)
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
    use super::*;
    use crate::config::{
        AppConfig, FallbackEntry, JumpHostConfig, JumpHostFields, RhopdJumpHostFields,
        JumpserverJumpHostFields, ServerConfigFile, ServerDefaults, ServerHostConfig,
    };
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn derives_target_ip_from_suffix() {
        assert_eq!(derive_target_ip("foo-192-0-2-163"), "192.0.2.163");
        assert_eq!(derive_target_ip("192.0.2.163"), "192.0.2.163");
    }

    #[test]
    fn parse_explicit_qualified_valid() {
        assert_eq!(
            parse_explicit_qualified("myjump:myserver"),
            Some(("myjump", "myserver"))
        );
        assert_eq!(
            parse_explicit_qualified("local:web01"),
            Some(("local", "web01"))
        );
    }

    #[test]
    fn parse_explicit_qualified_rejects_port_like() {
        // "host:22" should not be treated as explicit qualification
        assert_eq!(parse_explicit_qualified("host:22"), None);
    }

    #[test]
    fn parse_explicit_qualified_rejects_empty_parts() {
        assert_eq!(parse_explicit_qualified(":server"), None);
        assert_eq!(parse_explicit_qualified("jump:"), None);
        assert_eq!(parse_explicit_qualified(":"), None);
    }

    #[test]
    fn parse_explicit_qualified_rejects_ipv6() {
        assert_eq!(parse_explicit_qualified("::1"), None);
        assert_eq!(parse_explicit_qualified("fe80::1"), None);
    }

    #[test]
    fn parse_explicit_qualified_rejects_no_colon() {
        assert_eq!(parse_explicit_qualified("bareserver"), None);
    }

    fn make_server_config_with(entries: Vec<(&str, &str)>) -> ServerConfigFile {
        let mut servers = HashMap::new();
        for (alias, host) in entries {
            servers.insert(
                alias.to_string(),
                ServerHostConfig {
                    host: host.to_string(),
                    port: Some(22),
                    user: "testuser".to_string(),
                    identity_file: Some("/tmp/test_key".to_string()),
                    password: None,
                    shell: None,
                },
            );
        }
        ServerConfigFile {
            defaults: ServerDefaults {
                identity_file: None,
                shell: String::new(),
            },
            servers,
        }
    }

    #[test]
    fn resolver_explicit_local_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("local:web01").unwrap();

        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty()); // direct route
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    #[test]
    fn resolver_explicit_local_not_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let result = resolver.resolve("local:nonexistent");

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not found"));
    }

    #[test]
    fn resolver_explicit_jump_host() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![]);
        let jump_hosts = vec![JumpHostConfig {
            name: "remote1".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.99:2222".to_string(),
                identity_file: String::new(),
                known_hosts_path: String::new(),
            }),
        }];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("remote1:db01").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops.len(), 1);
        assert_eq!(routes[0].hops[0].name, "remote1");
        assert_eq!(routes[0].hops[0].kind, JumpHostKind::Rhopd);
        assert_eq!(routes[0].end_target.alias, "db01");
    }

    #[test]
    fn resolver_explicit_unknown_jump_host() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let result = resolver.resolve("unknown:db01");

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not found"));
    }

    #[test]
    fn resolver_bare_alias_found_in_server_config() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("web01").unwrap();

        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty());
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    #[test]
    fn resolver_bare_host_found_in_server_config() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("10.0.0.1").unwrap();

        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty());
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    #[test]
    fn resolver_fallback_jumpserver_enabled() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::JumpHost("test-jump".to_string())];
        // Use a non-existent path so server config is empty
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        // Use a non-existent ssh config path
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let jump_hosts = vec![JumpHostConfig {
            name: "test-jump".to_string(),
            kind: JumpHostKind::Jumpserver,
            fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                host: "jump.example.com".to_string(),
                port: 22,
                user: "admin".to_string(),
                identity_file: String::new(),
                pubkey_accepted_algorithms: None,
                menu_prompt_contains: "Opt".to_string(),
                mfa_prompt_contains: "MFA".to_string(),
                shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                mfa: crate::config::MfaConfig::default(),
            }),
        }];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("somehost").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops.len(), 1);
        assert_eq!(routes[0].hops[0].kind, JumpHostKind::Jumpserver);
        assert_eq!(routes[0].end_target.alias, "somehost");
    }

    #[test]
    fn resolver_fallback_jumpserver_disabled() {
        let mut config = AppConfig::default();
        // Reference a jump host name that doesn't exist in jump_hosts
        config.ssh.fallback = vec![FallbackEntry::JumpHost("nonexistent-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let result = resolver.resolve("somehost");

        // Should fail because the referenced jump host doesn't exist and no other candidates
        assert!(result.is_err());
    }

    #[test]
    fn resolver_empty_fallback_contributes_zero_candidates() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let result = resolver.resolve("somehost");

        assert!(result.is_err());
    }

    #[test]
    fn resolver_server_config_takes_priority_over_fallback() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::JumpHost("test-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts = vec![JumpHostConfig {
            name: "test-jump".to_string(),
            kind: JumpHostKind::Jumpserver,
            fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                host: "jump.example.com".to_string(),
                port: 22,
                user: "admin".to_string(),
                identity_file: String::new(),
                pubkey_accepted_algorithms: None,
                menu_prompt_contains: "Opt".to_string(),
                mfa_prompt_contains: "MFA".to_string(),
                shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                mfa: crate::config::MfaConfig::default(),
            }),
        }];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("web01").unwrap();

        // Server config match should be returned, not the jumpserver fallback
        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty()); // direct route from server config
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    #[test]
    fn resolver_no_implicit_fanout_to_rhopd_hosts() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let jump_hosts = vec![
            JumpHostConfig {
                name: "remote1".to_string(),
                kind: JumpHostKind::Rhopd,
                fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                    address: "10.0.0.99:2222".to_string(),
                    identity_file: String::new(),
                    known_hosts_path: String::new(),
                }),
            },
            JumpHostConfig {
                name: "remote2".to_string(),
                kind: JumpHostKind::Rhopd,
                fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                    address: "10.0.0.100:2222".to_string(),
                    identity_file: String::new(),
                    known_hosts_path: String::new(),
                }),
            },
        ];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let result = resolver.resolve("somehost");

        // Should NOT fan out to rhopd hosts — bare names don't implicitly
        // route through all rhopd jump hosts.
        assert!(result.is_err());
    }

    #[test]
    fn resolver_idempotent() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes1 = resolver.resolve("web01").unwrap();
        let routes2 = resolver.resolve("web01").unwrap();

        assert_eq!(routes1.len(), routes2.len());
        for (r1, r2) in routes1.iter().zip(routes2.iter()) {
            assert_eq!(r1.end_target.alias, r2.end_target.alias);
            assert_eq!(r1.end_target.id, r2.end_target.id);
            assert_eq!(r1.hops.len(), r2.hops.len());
        }
    }

    #[test]
    fn resolver_derived_ip_matches_server_host() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "192.0.2.163")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        // Input with IP-derivable suffix
        let routes = resolver.resolve("foo-192-0-2-163").unwrap();

        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty());
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    // --- Tests for merged-view resolution (Req 15.5, 15.6, 15.7) ---

    fn make_merged_rows() -> Vec<ServerListRow> {
        use crate::config::{DirectAuth, ServerEntry};
        vec![
            ServerListRow {
                source: ServerListSource::Local,
                server: ServerEntry {
                    alias: "web01".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 22,
                    user: "deploy".to_string(),
                    auth: DirectAuth::Key {
                        identity_file: "/tmp/key".to_string(),
                    },
                },
            },
            ServerListRow {
                source: ServerListSource::JumpHost("remote1".to_string()),
                server: ServerEntry {
                    alias: "db01".to_string(),
                    host: "192.168.1.10".to_string(),
                    port: 22,
                    user: "admin".to_string(),
                    auth: DirectAuth::Key {
                        identity_file: "/tmp/key".to_string(),
                    },
                },
            },
            ServerListRow {
                source: ServerListSource::Local,
                server: ServerEntry {
                    alias: "shared".to_string(),
                    host: "10.0.0.5".to_string(),
                    port: 22,
                    user: "deploy".to_string(),
                    auth: DirectAuth::Key {
                        identity_file: "/tmp/key".to_string(),
                    },
                },
            },
            ServerListRow {
                source: ServerListSource::JumpHost("remote1".to_string()),
                server: ServerEntry {
                    alias: "shared".to_string(),
                    host: "192.168.1.5".to_string(),
                    port: 22,
                    user: "admin".to_string(),
                    auth: DirectAuth::Key {
                        identity_file: "/tmp/key".to_string(),
                    },
                },
            },
        ]
    }

    #[test]
    fn resolver_merged_view_bare_alias_unique() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts = vec![JumpHostConfig {
            name: "remote1".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.99:2222".to_string(),
                identity_file: String::new(),
                known_hosts_path: String::new(),
            }),
        }];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // "db01" is unique (only in remote1)
        let routes = resolver.resolve("db01").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops.len(), 1);
        assert_eq!(routes[0].hops[0].name, "remote1");
        assert_eq!(routes[0].end_target.alias, "db01");
    }

    #[test]
    fn resolver_merged_view_bare_alias_unique_local() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts = vec![JumpHostConfig {
            name: "remote1".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.99:2222".to_string(),
                identity_file: String::new(),
                known_hosts_path: String::new(),
            }),
        }];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // "web01" is unique (only in local)
        let routes = resolver.resolve("web01").unwrap();
        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty()); // direct route
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    #[test]
    fn resolver_merged_view_bare_alias_ambiguous() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("shared", "10.0.0.5")]);
        let jump_hosts = vec![JumpHostConfig {
            name: "remote1".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.99:2222".to_string(),
                identity_file: String::new(),
                known_hosts_path: String::new(),
            }),
        }];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // "shared" exists in both local and remote1 — should be ambiguous
        let result = resolver.resolve("shared");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("ambiguous"), "error should mention ambiguous: {}", msg);
        assert!(msg.contains("local:shared"), "error should list local:shared: {}", msg);
        assert!(
            msg.contains("remote1:shared"),
            "error should list remote1:shared: {}",
            msg
        );
    }

    #[test]
    fn resolver_merged_view_explicit_jump_host_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts = vec![JumpHostConfig {
            name: "remote1".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.99:2222".to_string(),
                identity_file: String::new(),
                known_hosts_path: String::new(),
            }),
        }];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // Explicit "remote1:db01" should work
        let routes = resolver.resolve("remote1:db01").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops.len(), 1);
        assert_eq!(routes[0].hops[0].name, "remote1");
        assert_eq!(routes[0].end_target.alias, "db01");
    }

    #[test]
    fn resolver_merged_view_explicit_jump_host_not_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts = vec![JumpHostConfig {
            name: "remote1".to_string(),
            kind: JumpHostKind::Rhopd,
            fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                address: "10.0.0.99:2222".to_string(),
                identity_file: String::new(),
                known_hosts_path: String::new(),
            }),
        }];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // Explicit "remote1:nonexistent" should fail
        let result = resolver.resolve("remote1:nonexistent");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not found"), "error should mention not found: {}", msg);
    }

    #[test]
    fn resolver_merged_view_explicit_local_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // Explicit "local:web01" should work
        let routes = resolver.resolve("local:web01").unwrap();
        assert_eq!(routes.len(), 1);
        assert!(routes[0].hops.is_empty());
        assert_eq!(routes[0].end_target.alias, "web01");
    }

    #[test]
    fn resolver_merged_view_bare_not_found_falls_through() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::JumpHost("test-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let jump_hosts = vec![JumpHostConfig {
            name: "test-jump".to_string(),
            kind: JumpHostKind::Jumpserver,
            fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                host: "jump.example.com".to_string(),
                port: 22,
                user: "admin".to_string(),
                identity_file: String::new(),
                pubkey_accepted_algorithms: None,
                menu_prompt_contains: "Opt".to_string(),
                mfa_prompt_contains: "MFA".to_string(),
                shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                mfa: crate::config::MfaConfig::default(),
            }),
        }];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &jump_hosts, &merged_rows);

        // "unknown_host" is not in the merged view — should fall through to fallback
        let routes = resolver.resolve("unknown_host").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops[0].kind, JumpHostKind::Jumpserver);
    }

    // Feature: rhopd-jumpserver-architecture, Property 7: Resolver idempotence and ordering

    /// Strategy to generate CLI input strings: a mix of bare aliases, explicit
    /// `jump:server` forms, and host-like strings.
    fn arb_cli_input() -> impl Strategy<Value = String> {
        prop_oneof![
            // Bare alias (matches server config entries)
            prop_oneof![
                Just("web01".to_string()),
                Just("db01".to_string()),
                Just("cache01".to_string()),
            ],
            // Explicit jump:server form
            "[a-z][a-z0-9]{0,8}:[a-z][a-z0-9]{0,8}".prop_map(|s| s),
            // Host-like strings (IP addresses, hostnames)
            prop_oneof![
                Just("10.0.0.1".to_string()),
                Just("192.168.1.100".to_string()),
                Just("myhost.example.com".to_string()),
                "foo-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}".prop_map(|s| s),
            ],
            // Random alphanumeric strings
            "[a-z][a-z0-9\\-]{0,15}".prop_map(|s| s),
        ]
    }

    /// Build a fixed AppConfig suitable for property testing (no filesystem access).
    fn make_prop_app_config() -> AppConfig {
        let mut config = AppConfig::default();
        // Disable ssh config fallback to avoid filesystem access
        config.ssh.ssh_config_path = "/tmp/nonexistent_prop_ssh_config".to_string();
        config.ssh.server_config_path = "/tmp/nonexistent_prop_server.toml".to_string();
        // Use a jump host fallback so some inputs produce routes
        config.ssh.fallback = vec![FallbackEntry::JumpHost("test-jump".to_string())];
        config
    }

    /// Build a fixed ServerConfigFile with some entries for property testing.
    fn make_prop_server_config() -> ServerConfigFile {
        make_server_config_with(vec![
            ("web01", "10.0.0.1"),
            ("db01", "10.0.0.2"),
            ("cache01", "10.0.0.3"),
        ])
    }

    /// Build a fixed Vec<JumpHostConfig> with some jump host entries.
    fn make_prop_jump_hosts() -> Vec<JumpHostConfig> {
        vec![
            JumpHostConfig {
                name: "test-jump".to_string(),
                kind: JumpHostKind::Jumpserver,
                fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                    host: "jump.example.com".to_string(),
                    port: 22,
                    user: "admin".to_string(),
                    identity_file: String::new(),
                    pubkey_accepted_algorithms: None,
                    menu_prompt_contains: "Opt".to_string(),
                    mfa_prompt_contains: "MFA".to_string(),
                    shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                    mfa: crate::config::MfaConfig::default(),
                }),
            },
            JumpHostConfig {
                name: "remote1".to_string(),
                kind: JumpHostKind::Rhopd,
                fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                    address: "10.0.0.99:2222".to_string(),
                    identity_file: String::new(),
                    known_hosts_path: String::new(),
                }),
            },
            JumpHostConfig {
                name: "bastion".to_string(),
                kind: JumpHostKind::Jumpserver,
                fields: JumpHostFields::Jumpserver(JumpserverJumpHostFields {
                    host: "bastion.example.com".to_string(),
                    port: 22,
                    user: "admin".to_string(),
                    identity_file: String::new(),
                    pubkey_accepted_algorithms: None,
                    menu_prompt_contains: "Opt".to_string(),
                    mfa_prompt_contains: "MFA".to_string(),
                    shell_prompt_suffixes: vec!["$ ".to_string(), "# ".to_string()],
                    mfa: crate::config::MfaConfig::default(),
                }),
            },
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.8, 7.1, 7.2, 7.5**
        ///
        /// For arbitrary CLI input `s` and a fixed (AppConfig, ServerConfigFile,
        /// Vec<JumpHostConfig>), two calls to `Resolver::resolve(s)` return equal
        /// `Vec<TargetRoute>`; the order matches the deterministic ordering
        /// function; every `JumpHopRef` has non-empty `alias` and a populated
        /// `JumpHostKind`.
        #[test]
        fn prop_resolver_idempotence_and_ordering(input in arb_cli_input()) {
            let config = make_prop_app_config();
            let server_config = make_prop_server_config();
            let jump_hosts = make_prop_jump_hosts();

            let resolver = Resolver::new(&config, &server_config, &jump_hosts);

            let result1 = resolver.resolve(&input);
            let result2 = resolver.resolve(&input);

            match (result1, result2) {
                (Ok(routes1), Ok(routes2)) => {
                    // Idempotence: both calls return the same number of routes
                    prop_assert_eq!(
                        routes1.len(),
                        routes2.len(),
                        "Two resolve calls returned different number of routes for input '{}'",
                        input
                    );

                    // Idempotence: each route matches field-by-field
                    for (i, (r1, r2)) in routes1.iter().zip(routes2.iter()).enumerate() {
                        prop_assert_eq!(
                            &r1.end_target.alias, &r2.end_target.alias,
                            "Route {} end_target.alias differs for input '{}'", i, input
                        );
                        prop_assert_eq!(
                            &r1.end_target.id, &r2.end_target.id,
                            "Route {} end_target.id differs for input '{}'", i, input
                        );
                        prop_assert_eq!(
                            r1.hops.len(), r2.hops.len(),
                            "Route {} hops count differs for input '{}'", i, input
                        );
                        for (j, (h1, h2)) in r1.hops.iter().zip(r2.hops.iter()).enumerate() {
                            prop_assert_eq!(
                                &h1.name, &h2.name,
                                "Route {} hop {} name differs for input '{}'", i, j, input
                            );
                            prop_assert_eq!(
                                h1.kind, h2.kind,
                                "Route {} hop {} kind differs for input '{}'", i, j, input
                            );
                        }
                    }

                    // Ordering is deterministic: the routes are already in the
                    // canonical order produced by the resolver. Verify that a
                    // third call also matches (transitivity of determinism).
                    let result3 = resolver.resolve(&input).unwrap();
                    prop_assert_eq!(
                        routes1.len(), result3.len(),
                        "Third resolve call returned different count for input '{}'", input
                    );

                    // Invariant: every JumpHopRef has non-empty name and a
                    // populated JumpHostKind.
                    for (i, route) in routes1.iter().enumerate() {
                        for (j, hop) in route.hops.iter().enumerate() {
                            prop_assert!(
                                !hop.name.is_empty(),
                                "Route {} hop {} has empty name for input '{}'", i, j, input
                            );
                            // JumpHostKind is an enum — any variant is "populated".
                            // Verify it matches one of the known variants.
                            let kind_valid = matches!(
                                hop.kind,
                                JumpHostKind::Direct
                                    | JumpHostKind::Jumpserver
                                    | JumpHostKind::Rhopd
                            );
                            prop_assert!(
                                kind_valid,
                                "Route {} hop {} has invalid kind for input '{}'", i, j, input
                            );
                        }
                    }
                }
                (Err(_), Err(_)) => {
                    // Both calls returned Err — idempotence holds for the error case.
                    // Nothing more to verify.
                }
                (Ok(_), Err(e)) => {
                    prop_assert!(
                        false,
                        "First call Ok but second call Err({}) for input '{}'", e, input
                    );
                }
                (Err(e), Ok(_)) => {
                    prop_assert!(
                        false,
                        "First call Err({}) but second call Ok for input '{}'", e, input
                    );
                }
            }
        }
    }

    // Feature: rhopd-jumpserver-architecture, Property 13: Bare server-alias ambiguity reporting

    /// Strategy to generate a valid server alias (lowercase alphanumeric, 1-12 chars).
    fn arb_server_alias() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9]{0,11}".prop_map(|s| s)
    }

    /// Strategy to generate a valid jump host alias (lowercase alphanumeric, 1-8 chars),
    /// distinct from "local".
    fn arb_jump_alias() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9]{1,7}"
            .prop_filter("must not be 'local'", |s| s != "local")
            .prop_map(|s| s)
    }

    /// Strategy to generate a non-empty set of jump host aliases (1-4 aliases).
    fn arb_jump_alias_set() -> impl Strategy<Value = Vec<String>> {
        proptest::collection::hash_set(arb_jump_alias(), 1..=4)
            .prop_map(|set| set.into_iter().collect::<Vec<_>>())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 15.7**
        ///
        /// For arbitrary configs in which a `<server_alias>` appears in two or
        /// more `Server_List_Source` values, resolving the bare `<server_alias>`
        /// returns an error whose message contains every
        /// `<jump_name>:<server_alias>` form (including `local:`) where the
        /// server appears.
        #[test]
        fn prop_bare_alias_ambiguity_reporting(
            server_alias in arb_server_alias(),
            jump_aliases in arb_jump_alias_set(),
        ) {
            // Build the set of sources where the ambiguous alias will appear.
            // We need at least 2 sources for ambiguity.
            let mut all_sources: Vec<ServerListSource> = vec![ServerListSource::Local];
            for alias in &jump_aliases {
                all_sources.push(ServerListSource::JumpHost(alias.clone()));
            }

            // Use a deterministic selection: always include local + first jump host
            // to guarantee at least 2 sources.
            let ambiguous_sources: Vec<ServerListSource> = if all_sources.len() >= 2 {
                // Take at least 2 sources (local + at least one jump host)
                all_sources.iter().take(2.max(all_sources.len())).cloned().collect()
            } else {
                all_sources.clone()
            };

            // Build merged rows: one row per source containing the ambiguous alias.
            let merged_rows: Vec<ServerListRow> = ambiguous_sources
                .iter()
                .map(|source| ServerListRow {
                    source: source.clone(),
                    server: crate::config::ServerEntry {
                        alias: server_alias.clone(),
                        host: "10.0.0.1".to_string(),
                        port: 22,
                        user: "testuser".to_string(),
                        auth: crate::config::DirectAuth::Key {
                            identity_file: "/tmp/key".to_string(),
                        },
                    },
                })
                .collect();

            // Build jump host configs for all jump aliases.
            let jump_host_configs: Vec<JumpHostConfig> = jump_aliases
                .iter()
                .map(|alias| JumpHostConfig {
                    name: alias.clone(),
                    kind: JumpHostKind::Rhopd,
                    fields: JumpHostFields::Rhopd(RhopdJumpHostFields {
                        address: "10.0.0.99:2222".to_string(),
                        identity_file: String::new(),
                        known_hosts_path: String::new(),
                    }),
                })
                .collect();

            // Build a minimal server config (empty — we rely on merged view).
            let server_config = ServerConfigFile::default();
            let config = AppConfig::default();

            let resolver = Resolver::with_merged_view(
                &config,
                &server_config,
                &jump_host_configs,
                &merged_rows,
            );

            // Resolve the bare alias — should return an error because it's ambiguous.
            let result = resolver.resolve(&server_alias);
            prop_assert!(
                result.is_err(),
                "Expected Err for ambiguous alias '{}' present in {} sources, got Ok",
                server_alias,
                ambiguous_sources.len()
            );

            let err_msg = result.unwrap_err().to_string();

            // Verify the error mentions "ambiguous".
            prop_assert!(
                err_msg.contains("ambiguous"),
                "Error message should contain 'ambiguous', got: {}",
                err_msg
            );

            // Verify every candidate form appears in the error message.
            for source in &ambiguous_sources {
                let candidate = match source {
                    ServerListSource::Local => format!("local:{}", server_alias),
                    ServerListSource::JumpHost(jump_alias) => {
                        format!("{}:{}", jump_alias, server_alias)
                    }
                };
                prop_assert!(
                    err_msg.contains(&candidate),
                    "Error message should contain candidate '{}', got: {}",
                    candidate,
                    err_msg
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Feature: remove-deprecated-jumpserver-config, Property 3: Resolver fallback ordering preservation
    // Feature: remove-deprecated-jumpserver-config, Property 4: Resolver idempotence
    // Feature: remove-deprecated-jumpserver-config, Property 5: Server.toml match short-circuits fallback
    // Feature: remove-deprecated-jumpserver-config, Property 9: Missing jump host in fallback produces error
    // -----------------------------------------------------------------------

    /// Strategy to generate a permutation of jump host names for fallback ordering tests.
    /// Returns a subset (1..=count) of the provided names in a random order.
    fn arb_fallback_jump_entries(names: Vec<String>) -> impl Strategy<Value = Vec<FallbackEntry>> {
        let len = names.len();
        proptest::collection::vec(proptest::sample::select(names), 1..=len)
            .prop_map(|selected| {
                // Deduplicate while preserving order
                let mut seen = std::collections::HashSet::new();
                selected
                    .into_iter()
                    .filter(|n| seen.insert(n.clone()))
                    .map(|n| FallbackEntry::JumpHost(n))
                    .collect()
            })
    }

    /// Strategy to generate a non-empty target input string that won't match
    /// server config or trigger explicit qualification parsing.
    fn arb_bare_target() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9]{2,10}".prop_filter(
            "must not contain colon or match server entries",
            |s| !s.contains(':') && s != "web01" && s != "db01" && s != "cache01",
        )
    }

    /// Strategy to generate a server alias that matches one of the prop server config entries.
    fn arb_server_alias_match() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("web01".to_string()),
            Just("db01".to_string()),
            Just("cache01".to_string()),
        ]
    }

    /// Strategy to generate a jump host name that does NOT exist in the prop jump hosts.
    fn arb_nonexistent_jump_name() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9]{3,12}".prop_filter(
            "must not match any existing jump host name",
            |s| s != "test-jump" && s != "remote1" && s != "bastion",
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        // Feature: remove-deprecated-jumpserver-config, Property 3: Resolver fallback ordering preservation
        /// **Validates: Requirements 1.6, 6.3**
        ///
        /// For any permutation of valid `FallbackEntry::JumpHost` entries (all
        /// referencing existing jump hosts), the resolver output order matches
        /// the fallback declaration order.
        #[test]
        fn prop_resolver_fallback_ordering_preservation(
            fallback in arb_fallback_jump_entries(vec![
                "test-jump".to_string(),
                "remote1".to_string(),
                "bastion".to_string(),
            ]),
            target in arb_bare_target(),
        ) {
            let mut config = AppConfig::default();
            config.ssh.ssh_config_path = "/tmp/nonexistent_prop_ssh_config".to_string();
            config.ssh.server_config_path = "/tmp/nonexistent_prop_server.toml".to_string();
            config.ssh.fallback = fallback.clone();

            let server_config = ServerConfigFile::default();
            let jump_hosts = make_prop_jump_hosts();

            let resolver = Resolver::new(&config, &server_config, &jump_hosts);
            let routes = resolver.resolve(&target).unwrap();

            // The number of routes should equal the number of fallback entries
            // (all are JumpHost entries referencing valid hosts, no Local entries
            // that might not produce a route).
            prop_assert_eq!(
                routes.len(),
                fallback.len(),
                "Expected {} routes for {} fallback entries, got {} for target '{}'",
                fallback.len(),
                fallback.len(),
                routes.len(),
                target
            );

            // Verify ordering: each route's first hop name matches the
            // corresponding fallback entry's jump host name.
            for (i, (route, entry)) in routes.iter().zip(fallback.iter()).enumerate() {
                if let FallbackEntry::JumpHost(expected_name) = entry {
                    prop_assert_eq!(
                        routes[i].hops.len(),
                        1,
                        "Route {} should have exactly 1 hop for target '{}'",
                        i,
                        target
                    );
                    prop_assert_eq!(
                        &route.hops[0].name,
                        expected_name,
                        "Route {} hop name should be '{}' but got '{}' for target '{}'",
                        i,
                        expected_name,
                        route.hops[0].name,
                        target
                    );
                }
            }
        }

        // Feature: remove-deprecated-jumpserver-config, Property 4: Resolver idempotence
        /// **Validates: Requirements 6.8**
        ///
        /// For any target input string and configuration, calling
        /// `Resolver::resolve(input)` twice with the same config produces
        /// identical `Vec<TargetRoute>` results.
        #[test]
        fn prop_resolver_idempotence(input in arb_cli_input()) {
            let config = make_prop_app_config();
            let server_config = make_prop_server_config();
            let jump_hosts = make_prop_jump_hosts();

            let resolver = Resolver::new(&config, &server_config, &jump_hosts);

            let result1 = resolver.resolve(&input);
            let result2 = resolver.resolve(&input);

            match (result1, result2) {
                (Ok(routes1), Ok(routes2)) => {
                    prop_assert_eq!(
                        routes1.len(),
                        routes2.len(),
                        "Idempotence violated: different route counts for input '{}'",
                        input
                    );
                    for (i, (r1, r2)) in routes1.iter().zip(routes2.iter()).enumerate() {
                        prop_assert_eq!(
                            &r1.end_target.alias, &r2.end_target.alias,
                            "Route {} end_target.alias differs for input '{}'", i, input
                        );
                        prop_assert_eq!(
                            &r1.end_target.id, &r2.end_target.id,
                            "Route {} end_target.id differs for input '{}'", i, input
                        );
                        prop_assert_eq!(
                            r1.hops.len(), r2.hops.len(),
                            "Route {} hops count differs for input '{}'", i, input
                        );
                        for (j, (h1, h2)) in r1.hops.iter().zip(r2.hops.iter()).enumerate() {
                            prop_assert_eq!(
                                &h1.name, &h2.name,
                                "Route {} hop {} name differs for input '{}'", i, j, input
                            );
                            prop_assert_eq!(
                                h1.kind, h2.kind,
                                "Route {} hop {} kind differs for input '{}'", i, j, input
                            );
                        }
                    }
                }
                (Err(_), Err(_)) => {
                    // Both calls returned Err — idempotence holds for the error case.
                }
                (Ok(_), Err(e)) => {
                    prop_assert!(
                        false,
                        "First call Ok but second call Err({}) for input '{}'", e, input
                    );
                }
                (Err(e), Ok(_)) => {
                    prop_assert!(
                        false,
                        "First call Err({}) but second call Ok for input '{}'", e, input
                    );
                }
            }
        }

        // Feature: remove-deprecated-jumpserver-config, Property 5: Server.toml match short-circuits fallback
        /// **Validates: Requirements 6.4**
        ///
        /// For any target that matches an entry in server.toml, the Resolver
        /// returns that match without producing any fallback candidates (the
        /// result contains only direct routes from server config).
        #[test]
        fn prop_server_toml_match_short_circuits_fallback(
            server_alias in arb_server_alias_match(),
        ) {
            let mut config = AppConfig::default();
            config.ssh.ssh_config_path = "/tmp/nonexistent_prop_ssh_config".to_string();
            config.ssh.server_config_path = "/tmp/nonexistent_prop_server.toml".to_string();
            // Configure fallback with jump hosts that would produce routes
            config.ssh.fallback = vec![
                FallbackEntry::JumpHost("test-jump".to_string()),
                FallbackEntry::JumpHost("remote1".to_string()),
            ];

            let server_config = make_prop_server_config();
            let jump_hosts = make_prop_jump_hosts();

            let resolver = Resolver::new(&config, &server_config, &jump_hosts);
            let routes = resolver.resolve(&server_alias).unwrap();

            // Server config match should short-circuit: only direct routes returned
            prop_assert_eq!(
                routes.len(),
                1,
                "Expected exactly 1 route for server config match '{}', got {}",
                server_alias,
                routes.len()
            );

            // The route should be direct (zero hops)
            prop_assert!(
                routes[0].hops.is_empty(),
                "Server config match '{}' should produce a direct route (0 hops), got {} hops",
                server_alias,
                routes[0].hops.len()
            );

            // The end target alias should match the server alias
            prop_assert_eq!(
                &routes[0].end_target.alias,
                &server_alias,
                "End target alias should be '{}' but got '{}'",
                server_alias,
                routes[0].end_target.alias
            );
        }

        // Feature: remove-deprecated-jumpserver-config, Property 9: Missing jump host in fallback produces error
        /// **Validates: Requirements 3.4**
        ///
        /// For any `FallbackEntry::JumpHost(name)` where `name` does not match
        /// any `[[jump_hosts]]` entry's `name` field, the Resolver returns an error.
        #[test]
        fn prop_missing_jump_host_in_fallback_produces_error(
            missing_name in arb_nonexistent_jump_name(),
            target in arb_bare_target(),
        ) {
            let mut config = AppConfig::default();
            config.ssh.ssh_config_path = "/tmp/nonexistent_prop_ssh_config".to_string();
            config.ssh.server_config_path = "/tmp/nonexistent_prop_server.toml".to_string();
            config.ssh.fallback = vec![FallbackEntry::JumpHost(missing_name.clone())];

            let server_config = ServerConfigFile::default();
            // Use jump hosts that do NOT contain the missing_name
            let jump_hosts = make_prop_jump_hosts();

            let resolver = Resolver::new(&config, &server_config, &jump_hosts);
            let result = resolver.resolve(&target);

            prop_assert!(
                result.is_err(),
                "Expected error for missing jump host '{}' in fallback, got Ok for target '{}'",
                missing_name,
                target
            );

            let err_msg = result.unwrap_err().to_string();
            prop_assert!(
                err_msg.contains(&missing_name),
                "Error message should mention the missing jump host name '{}', got: {}",
                missing_name,
                err_msg
            );
        }
    }
}
