// Daemon target resolver module.
// Maps CLI targets to Vec<Route> for gateway dispatch.

use std::path::Path;

use anyhow::{Result, anyhow, bail};

use crate::config::{
    AppConfig, FallbackEntry, GatewayConfig, ServerConfigFile,
    parse_ssh_config, resolve_server_entry, resolve_ssh_host,
};
use crate::types::ServerListSource;
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
    gateways: &'a [GatewayConfig],
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
        gateways: &'a [GatewayConfig],
    ) -> Self {
        Self {
            config,
            server_config,
            gateways,
            merged_rows: &[],
        }
    }

    /// Create a resolver with a pre-computed merged server-list view.
    /// This enables cross-source bare-alias disambiguation.
    pub fn with_merged_view(
        config: &'a AppConfig,
        server_config: &'a ServerConfigFile,
        gateways: &'a [GatewayConfig],
        merged_rows: &'a [ServerListRow],
    ) -> Self {
        Self {
            config,
            server_config,
            gateways,
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
    /// 3. No implicit fan-out to all `rhopd` gateways.
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

        // Look up the gateway by name.
        let gc = self
            .gateways
            .iter()
            .find(|gc| gc.name() == jump_name)
            .ok_or_else(|| anyhow!("gateway name '{}' not found", jump_name))?;

        // If we have a merged view, verify the server alias exists on this source.
        if !self.merged_rows.is_empty() {
            let source = ServerListSource::Gateway(jump_name.to_string());
            let found = self
                .merged_rows
                .iter()
                .any(|row| row.source == source && row.server.alias == server_alias);
            if !found {
                bail!(
                    "server alias '{}' not found on gateway '{}'",
                    server_alias,
                    jump_name
                );
            }
        }

        // Build a route through this gateway to the named server alias.
        let route = Route {
            gateway_name: gc.name().to_string(),
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
                ServerListSource::Gateway(jump_alias) => {
                    // Route through the named gateway — gateway_name = jump_alias
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
                ServerListSource::Gateway(jump_alias) => format!("{}:{}", jump_alias, alias),
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
                FallbackEntry::Gateway(name) => {
                    // Look up the named [[gateways]] entry
                    let gc = self
                        .gateways
                        .iter()
                        .find(|gc| gc.name() == name.as_str())
                        .ok_or_else(|| anyhow!(
                            "ssh.fallback references gateway '{}' which is not defined in [[gateways]]",
                            name
                        ))?;
                    candidates.push(Route {
                        gateway_name: gc.name().to_string(),
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

/// Parse an input string as `<gateway_name>:<end_target>`.
/// Splits on the FIRST colon only, so multi-colon targets like
/// `"ali-rhopd:sub-gw:server1"` parse as gateway="ali-rhopd", end_target="sub-gw:server1".
/// Returns `None` if there is no colon, if either part is empty, or if the
/// part after the first colon is purely numeric (port-like, e.g. "host:22").
fn parse_explicit_qualified(input: &str) -> Option<(&str, &str)> {
    // Split on the first colon only.
    let colon_pos = input.find(':')?;
    let gateway_name = &input[..colon_pos];
    let end_target = &input[colon_pos + 1..];

    // Both parts must be non-empty.
    if gateway_name.is_empty() || end_target.is_empty() {
        return None;
    }

    // Reject if end_target starts with ':' (e.g. IPv6 "fe80::1" splits to
    // gateway="fe80", end_target=":1" — the leading colon signals IPv6).
    if end_target.starts_with(':') {
        return None;
    }

    // If the part after the first colon is purely numeric, treat it as a port
    // (e.g. "host:22") rather than an explicit qualification.
    if end_target.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    Some((gateway_name, end_target))
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
        AppConfig, FallbackEntry, GatewayConfig, RhopdGatewayConfig,
        JumpserverGatewayConfig, ServerConfigFile, ServerDefaults, ServerHostConfig,
    };
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

    #[test]
    fn parse_explicit_qualified_multi_colon_two_levels() {
        assert_eq!(
            parse_explicit_qualified("ali-rhopd:sub-gw:server1"),
            Some(("ali-rhopd", "sub-gw:server1"))
        );
    }

    #[test]
    fn parse_explicit_qualified_multi_colon_three_levels() {
        assert_eq!(
            parse_explicit_qualified("gw:sub:deep:server1"),
            Some(("gw", "sub:deep:server1"))
        );
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
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let routes = resolver.resolve("local:web01").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    #[test]
    fn resolver_explicit_local_not_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let result = resolver.resolve("local:nonexistent");

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not found"));
    }

    #[test]
    fn resolver_explicit_gateway() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![]);
        let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
            name: "remote1".to_string(),
            address: "10.0.0.99:2222".to_string(),
            identity_file: String::new(),
            known_hosts_path: String::new(),
        })];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let routes = resolver.resolve("remote1:db01").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "remote1");
        assert_eq!(routes[0].end_target, "db01");
    }

    #[test]
    fn resolver_explicit_unknown_gateway() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![]);
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let result = resolver.resolve("unknown:db01");

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not found"));
    }

    #[test]
    fn resolver_bare_alias_found_in_server_config() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let routes = resolver.resolve("web01").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    #[test]
    fn resolver_bare_host_found_in_server_config() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let routes = resolver.resolve("10.0.0.1").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    #[test]
    fn resolver_fallback_jumpserver_enabled() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::Gateway("test-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let gateways = vec![GatewayConfig::Jumpserver(JumpserverGatewayConfig {
            name: "test-jump".to_string(),
            host: "jump.example.com".to_string(),
            port: 22,
            user: "admin".to_string(),
            identity_file: String::new(),
            pubkey_accepted_algorithms: None,
            totp_secret_base32: String::new(),
            totp_digits: 6,
            totp_period: 30,
        })];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let routes = resolver.resolve("somehost").unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "test-jump");
        assert_eq!(routes[0].end_target, "somehost");
    }

    #[test]
    fn resolver_fallback_jumpserver_disabled() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::Gateway("nonexistent-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
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
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
        let result = resolver.resolve("somehost");

        assert!(result.is_err());
    }

    #[test]
    fn resolver_server_config_takes_priority_over_fallback() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::Gateway("test-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let gateways = vec![GatewayConfig::Jumpserver(JumpserverGatewayConfig {
            name: "test-jump".to_string(),
            host: "jump.example.com".to_string(),
            port: 22,
            user: "admin".to_string(),
            identity_file: String::new(),
            pubkey_accepted_algorithms: None,
            totp_secret_base32: String::new(),
            totp_digits: 6,
            totp_period: 30,
        })];

        let resolver = Resolver::new(&config, &server_config, &gateways);
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
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
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
        let gateways: Vec<GatewayConfig> = vec![];

        let resolver = Resolver::new(&config, &server_config, &gateways);
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
                source: ServerListSource::Gateway("remote1".to_string()),
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
                source: ServerListSource::Gateway("remote1".to_string()),
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
        let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
            name: "remote1".to_string(),
            address: "10.0.0.99:2222".to_string(),
            identity_file: String::new(),
            known_hosts_path: String::new(),
        })];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

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
        let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
            name: "remote1".to_string(),
            address: "10.0.0.99:2222".to_string(),
            identity_file: String::new(),
            known_hosts_path: String::new(),
        })];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

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
        let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
            name: "remote1".to_string(),
            address: "10.0.0.99:2222".to_string(),
            identity_file: String::new(),
            known_hosts_path: String::new(),
        })];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

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
    fn resolver_merged_view_explicit_gateway_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
            name: "remote1".to_string(),
            address: "10.0.0.99:2222".to_string(),
            identity_file: String::new(),
            known_hosts_path: String::new(),
        })];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

        // Explicit "remote1:db01" should work
        let routes = resolver.resolve("remote1:db01").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "remote1");
        assert_eq!(routes[0].end_target, "db01");
    }

    #[test]
    fn resolver_merged_view_explicit_gateway_not_found() {
        let config = AppConfig::default();
        let server_config = make_server_config_with(vec![("web01", "10.0.0.1")]);
        let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
            name: "remote1".to_string(),
            address: "10.0.0.99:2222".to_string(),
            identity_file: String::new(),
            known_hosts_path: String::new(),
        })];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

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
        let gateways: Vec<GatewayConfig> = vec![];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

        // Explicit "local:web01" should work
        let routes = resolver.resolve("local:web01").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "local");
        assert_eq!(routes[0].end_target, "web01");
    }

    #[test]
    fn resolver_merged_view_bare_not_found_falls_through() {
        let mut config = AppConfig::default();
        config.ssh.fallback = vec![FallbackEntry::Gateway("test-jump".to_string())];
        config.ssh.server_config_path = "/tmp/nonexistent_server.toml".to_string();
        config.ssh.ssh_config_path = "/tmp/nonexistent_ssh_config".to_string();

        let server_config = ServerConfigFile::default();
        let gateways = vec![GatewayConfig::Jumpserver(JumpserverGatewayConfig {
            name: "test-jump".to_string(),
            host: "jump.example.com".to_string(),
            port: 22,
            user: "admin".to_string(),
            identity_file: String::new(),
            pubkey_accepted_algorithms: None,
            totp_secret_base32: String::new(),
            totp_digits: 6,
            totp_period: 30,
        })];
        let merged_rows = make_merged_rows();

        let resolver =
            Resolver::with_merged_view(&config, &server_config, &gateways, &merged_rows);

        // "unknown_host" is not in the merged view — should fall through to fallback
        let routes = resolver.resolve("unknown_host").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].gateway_name, "test-jump");
        assert_eq!(routes[0].end_target, "unknown_host");
    }
}
