// Daemon target resolver module.
// Maps CLI targets to Vec<Route> for gateway dispatch.

use std::path::Path;

use anyhow::{Result, anyhow, bail};

use crate::config::{
    AppConfig, FallbackEntry, JumpHostConfig, ServerConfigFile,
    parse_ssh_config, resolve_server_entry, resolve_ssh_host,
};
use crate::jump::ServerListSource;
use crate::protocol::ServerListRow;

use super::gateway::Route;

// ---------------------------------------------------------------------------
// Resolver — produces Vec<Route> for daemon gateway dispatch
// ---------------------------------------------------------------------------

/// The reserved alias representing the local daemon's own `server.toml`.
const LOCAL_SOURCE_ALIAS: &str = "local";

/// Pure resolver that maps a CLI target string into an ordered list of
/// `Route` candidates. Each Route contains a gateway_name and an end_target.
/// The same inputs always produce the same outputs (idempotence, Req 7.1).
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
    /// This enables cross-source bare-alias disambiguation.
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

    /// Resolve a CLI target string into candidate `Route`s.
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
    pub fn resolve(&self, input: &str) -> Result<Vec<Route>> {
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
    fn resolve_explicit(&self, jump_name: &str, server_alias: &str) -> Result<Vec<Route>> {
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
        let route = Route {
            gateway_name: jh.name.clone(),
            end_target: server_alias.to_string(),
        };
        Ok(vec![route])
    }

    /// Resolve a bare `<server_alias>` against the merged server-list view.
    ///
    /// Returns:
    /// - `Ok(Some(routes))` if the alias is found in exactly one source.
    /// - `Ok(None)` if the alias is not found in any source.
    /// - `Err(...)` if the alias is ambiguous (found in multiple sources).
    fn resolve_bare_from_merged_view(&self, alias: &str) -> Result<Option<Vec<Route>>> {
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
                    // Direct route — gateway_name = "local"
                    Route {
                        gateway_name: "local".to_string(),
                        end_target: alias.to_string(),
                    }
                }
                ServerListSource::JumpHost(jump_alias) => {
                    // Route through the named jump host — gateway_name = jump_host.name
                    Route {
                        gateway_name: jump_alias.clone(),
                        end_target: alias.to_string(),
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
    fn append_server_config_routes(&self, candidates: &mut Vec<Route>, input: &str) {
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
        candidates: &mut Vec<Route>,
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
                    candidates.push(Route {
                        gateway_name: jh.name.clone(),
                        end_target: input.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Look up a server alias in the local `ServerConfigFile` by alias name.
    fn lookup_local_server(&self, alias: &str) -> Option<Route> {
        let server = self.server_config.servers.get(alias)?;
        let entry = resolve_server_entry(alias, server, &self.server_config.defaults).ok()?;
        Some(Route {
            gateway_name: "local".to_string(),
            end_target: entry.alias,
        })
    }

    /// Look up a server in the local `ServerConfigFile` by host field.
    fn lookup_local_server_by_host(&self, host: &str) -> Option<Route> {
        let (alias, server) = self
            .server_config
            .servers
            .iter()
            .find(|(_, s)| s.host == host)?;
        let entry = resolve_server_entry(alias, server, &self.server_config.defaults).ok()?;
        Some(Route {
            gateway_name: "local".to_string(),
            end_target: entry.alias,
        })
    }

    /// Resolve via SSH config as a fallback, producing a direct (local) route.
    fn resolve_ssh_config_route(&self, input: &str, ip: &str) -> Result<Option<Route>> {
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
            return Ok(Some(Route {
                gateway_name: "local".to_string(),
                end_target: input.to_string(),
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

/// Derive an IP address from a hostname suffix pattern like "foo-192-0-2-163".
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
    use crate::jump::JumpHostKind;
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
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
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
        assert_eq!(routes[0].gateway_name, "remote1");
        assert_eq!(routes[0].end_target, "db01");
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
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    #[test]
    fn resolver_bare_host_found_in_server_config() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("10.0.0.1").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    #[test]
    fn resolver_fallback_jumpserver_enabled() {
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

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("somehost").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "test-jump");
        assert_eq!(routes[0].end_target, "somehost");
    }

    #[test]
    fn resolver_fallback_jumpserver_disabled() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::JumpHost("nonexistent-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let result = resolver.resolve("somehost");

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
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
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
            assert_eq!(r1.gateway_name, r2.gateway_name);
            assert_eq!(r1.end_target, r2.end_target);
        }
    }

    #[test]
    fn resolver_derived_ip_matches_server_host() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "192.0.2.163")]);
        let jump_hosts: Vec<JumpHostConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &jump_hosts);
        let routes = resolver.resolve("foo-192-0-2-163").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    // --- Tests for merged-view resolution ---

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
        assert_eq!(routes[0].gateway_name, "remote1");
        assert_eq!(routes[0].end_target, "db01");
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
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
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
        assert_eq!(routes[0].gateway_name, "remote1");
        assert_eq!(routes[0].end_target, "db01");
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
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
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
        assert_eq!(routes[0].gateway_name, "test-jump");
        assert_eq!(routes[0].end_target, "unknown_host");
    }
}
