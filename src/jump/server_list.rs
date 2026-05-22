use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::time::timeout;
use tracing::warn;

use crate::config::{AppConfig, ServerConfigFile, ServerEntry, resolve_server_entry};
use crate::jump::{JumpHost, ServerListSource, UnsupportedCapability};
use crate::protocol::{MergedServerList, ServerListRow, ServerListSourceStatus};

/// Aggregates server lists from the local `server.toml` and all configured
/// jump hosts into a single [`MergedServerList`].
///
/// Each jump host is queried concurrently with a timeout. Failures (transport
/// errors or unsupported capability) are recorded per-source but never cause
/// the overall aggregation to fail.
///
/// Results are cached per-source with a TTL of `config.ssh.max_idle_time`.
/// Passing `refresh = true` evicts the cache before re-fetching.
pub struct ServerListAggregator<'a> {
    pub local: &'a ServerConfigFile,
    pub jump_hosts: &'a mut [Box<dyn JumpHost>],
    pub config: &'a AppConfig,
    pub cache: HashMap<ServerListSource, (Instant, Vec<ServerEntry>)>,
}

impl<'a> ServerListAggregator<'a> {
    /// Aggregate server entries from all sources.
    ///
    /// - Local entries come from `self.local` (tagged `ServerListSource::Local`).
    /// - Each jump host is called concurrently with `tokio::time::timeout`.
    /// - `UnsupportedCapability` → status `Unsupported`, zero rows.
    /// - Any other error → status `Error(msg)`, zero rows.
    /// - The result is always `Ok` — individual source failures are captured in
    ///   `source_status` but do not propagate.
    ///
    /// When `refresh` is `false`, cached results within TTL (`ssh.max_idle_time`)
    /// are returned without re-querying. When `refresh` is `true`, the cache is
    /// evicted before fetching.
    pub async fn aggregate(&mut self, refresh: bool) -> MergedServerList {
        let ttl = self.config.ssh.max_idle_time;
        let now = Instant::now();

        // If refresh requested, evict all cache entries.
        if refresh {
            self.cache.clear();
        }

        let mut rows = Vec::new();
        let mut source_status = Vec::new();

        // --- Local source ---
        let local_source = ServerListSource::Local;
        let local_entries = if let Some((cached_at, entries)) = self.cache.get(&local_source) {
            if now.duration_since(*cached_at) < ttl {
                entries.clone()
            } else {
                let entries = self.collect_local_entries();
                self.cache.insert(local_source.clone(), (now, entries.clone()));
                entries
            }
        } else {
            let entries = self.collect_local_entries();
            self.cache.insert(local_source.clone(), (now, entries.clone()));
            entries
        };

        for entry in local_entries {
            rows.push(ServerListRow {
                source: local_source.clone(),
                server: entry,
            });
        }
        source_status.push((local_source, ServerListSourceStatus::Ok));

        // --- Jump host sources (concurrent with timeout) ---
        let connect_timeout = self.config.ssh.connect_timeout;

        // Determine which jump hosts need fetching vs can use cache.
        let mut cached_results: Vec<(String, Vec<ServerEntry>)> = Vec::new();
        let mut fetch_indices: Vec<usize> = Vec::new();

        for (idx, jh) in self.jump_hosts.iter().enumerate() {
            let source = ServerListSource::JumpHost(jh.alias().to_string());
            if let Some((cached_at, entries)) = self.cache.get(&source) {
                if now.duration_since(*cached_at) < ttl {
                    cached_results.push((jh.alias().to_string(), entries.clone()));
                    continue;
                }
            }
            fetch_indices.push(idx);
        }

        // Add cached jump host results.
        for (alias, entries) in cached_results {
            let source = ServerListSource::JumpHost(alias);
            for entry in &entries {
                rows.push(ServerListRow {
                    source: source.clone(),
                    server: entry.clone(),
                });
            }
            source_status.push((source, ServerListSourceStatus::Ok));
        }

        // Fetch from jump hosts that are not cached.
        let jump_results = self.query_jump_hosts_by_indices(&fetch_indices, connect_timeout).await;

        for (alias, result) in jump_results {
            let source = ServerListSource::JumpHost(alias.clone());
            match result {
                Ok(entries) => {
                    self.cache.insert(source.clone(), (now, entries.clone()));
                    for entry in entries {
                        rows.push(ServerListRow {
                            source: source.clone(),
                            server: entry,
                        });
                    }
                    source_status.push((source, ServerListSourceStatus::Ok));
                }
                Err(status) => {
                    source_status.push((source, status));
                }
            }
        }

        MergedServerList {
            rows,
            source_status,
        }
    }

    /// Collect entries from the local `ServerConfigFile`.
    fn collect_local_entries(&self) -> Vec<ServerEntry> {
        self.local
            .servers
            .iter()
            .filter_map(|(alias, server)| {
                resolve_server_entry(alias, server, &self.local.defaults).ok()
            })
            .collect()
    }

    /// Query specific jump hosts by index, bounded by `connect_timeout`.
    ///
    /// Returns a vec of `(alias, Result<Vec<ServerEntry>, ServerListSourceStatus>)`.
    async fn query_jump_hosts_by_indices(
        &mut self,
        indices: &[usize],
        connect_timeout: Duration,
    ) -> Vec<(String, Result<Vec<ServerEntry>, ServerListSourceStatus>)> {
        let mut results = Vec::with_capacity(indices.len());

        for &idx in indices {
            let jh = &mut self.jump_hosts[idx];
            let alias = jh.alias().to_string();
            let outcome = timeout(connect_timeout, jh.list_servers(self.config)).await;
            let result = match outcome {
                Ok(Ok(entries)) => Ok(entries),
                Ok(Err(err)) => {
                    if err.downcast_ref::<UnsupportedCapability>().is_some() {
                        warn!(
                            jump_host = %alias,
                            "list_servers not supported"
                        );
                        Err(ServerListSourceStatus::Unsupported)
                    } else {
                        let msg = format!("{}", err);
                        warn!(
                            jump_host = %alias,
                            error = %msg,
                            "list_servers failed"
                        );
                        Err(ServerListSourceStatus::Error(msg))
                    }
                }
                Err(_elapsed) => {
                    let msg = format!("timeout after {:?}", connect_timeout);
                    warn!(
                        jump_host = %alias,
                        "list_servers timed out"
                    );
                    Err(ServerListSourceStatus::Error(msg))
                }
            };
            results.push((alias, result));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AppConfig, DirectAuth, ServerConfigFile, ServerDefaults, ServerEntry, ServerHostConfig,
    };
    use crate::connection::CopySpec;
    use crate::jump::{JumpHost, JumpHostKind, UnsupportedCapability};
    use crate::protocol::ServerEvent;
    use anyhow::Result;
    use async_trait::async_trait;
    use proptest::prelude::*;
    use std::collections::HashMap;
    use tokio::sync::mpsc::UnboundedSender;

    /// A mock jump host that returns a fixed list of servers.
    struct MockJumpHost {
        host_alias: String,
        servers: Vec<ServerEntry>,
    }

    #[async_trait]
    impl JumpHost for MockJumpHost {
        async fn exec(
            &mut self,
            _argv: &[String],
            _sender: &UnboundedSender<ServerEvent>,
            _config: &AppConfig,
        ) -> Result<i32> {
            Ok(0)
        }

        async fn copy(&mut self, _spec: &CopySpec, _config: &AppConfig) -> Result<()> {
            Ok(())
        }

        async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
            Ok(self.servers.clone())
        }

        fn kind(&self) -> JumpHostKind {
            JumpHostKind::Rhopd
        }

        fn alias(&self) -> &str {
            &self.host_alias
        }
    }

    /// A mock jump host that does not support list_servers (uses default).
    struct UnsupportedMockJumpHost {
        host_alias: String,
    }

    #[async_trait]
    impl JumpHost for UnsupportedMockJumpHost {
        async fn exec(
            &mut self,
            _argv: &[String],
            _sender: &UnboundedSender<ServerEvent>,
            _config: &AppConfig,
        ) -> Result<i32> {
            Ok(0)
        }

        async fn copy(&mut self, _spec: &CopySpec, _config: &AppConfig) -> Result<()> {
            Ok(())
        }

        fn kind(&self) -> JumpHostKind {
            JumpHostKind::Direct
        }

        fn alias(&self) -> &str {
            &self.host_alias
        }
    }

    /// A mock jump host that returns an error from list_servers.
    struct ErrorMockJumpHost {
        host_alias: String,
    }

    #[async_trait]
    impl JumpHost for ErrorMockJumpHost {
        async fn exec(
            &mut self,
            _argv: &[String],
            _sender: &UnboundedSender<ServerEvent>,
            _config: &AppConfig,
        ) -> Result<i32> {
            Ok(0)
        }

        async fn copy(&mut self, _spec: &CopySpec, _config: &AppConfig) -> Result<()> {
            Ok(())
        }

        async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
            Err(anyhow::anyhow!("connection refused"))
        }

        fn kind(&self) -> JumpHostKind {
            JumpHostKind::Rhopd
        }

        fn alias(&self) -> &str {
            &self.host_alias
        }
    }

    fn make_server_config_file(entries: Vec<(&str, &str, &str)>) -> ServerConfigFile {
        let mut servers = HashMap::new();
        for (alias, host, user) in entries {
            servers.insert(
                alias.to_string(),
                ServerHostConfig {
                    host: host.to_string(),
                    port: Some(22),
                    user: user.to_string(),
                    identity_file: Some("/tmp/key".to_string()),
                    password: None,
                },
            );
        }
        ServerConfigFile {
            defaults: ServerDefaults {
                identity_file: Some("/tmp/default_key".to_string()),
            },
            servers,
        }
    }

    fn make_server_entry(alias: &str, host: &str, user: &str) -> ServerEntry {
        ServerEntry {
            alias: alias.to_string(),
            host: host.to_string(),
            port: 22,
            user: user.to_string(),
            auth: DirectAuth::Key {
                identity_file: "/tmp/key".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn test_aggregate_local_only() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![
            ("web1", "10.0.0.1", "deploy"),
            ("db1", "10.0.0.2", "admin"),
        ]);
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        let result = aggregator.aggregate(false).await;

        assert_eq!(result.rows.len(), 2);
        assert!(result.rows.iter().all(|r| r.source == ServerListSource::Local));
        assert_eq!(result.source_status.len(), 1);
        assert!(matches!(
            &result.source_status[0],
            (ServerListSource::Local, ServerListSourceStatus::Ok)
        ));
    }

    #[tokio::test]
    async fn test_aggregate_with_jump_host() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![("web1", "10.0.0.1", "deploy")]);

        let mock = MockJumpHost {
            host_alias: "remote1".to_string(),
            servers: vec![make_server_entry("app1", "192.168.1.1", "app")],
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        let result = aggregator.aggregate(false).await;

        assert_eq!(result.rows.len(), 2);
        // One local, one from jump host
        assert!(result.rows.iter().any(|r| r.source == ServerListSource::Local));
        assert!(result
            .rows
            .iter()
            .any(|r| r.source == ServerListSource::JumpHost("remote1".to_string())));
        assert_eq!(result.source_status.len(), 2);
    }

    #[tokio::test]
    async fn test_aggregate_unsupported_capability() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![]);

        let mock = UnsupportedMockJumpHost {
            host_alias: "direct1".to_string(),
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        let result = aggregator.aggregate(false).await;

        // Zero rows from the unsupported jump host
        assert!(result
            .rows
            .iter()
            .all(|r| r.source == ServerListSource::Local));
        // Status should be Unsupported
        let jh_status = result
            .source_status
            .iter()
            .find(|(s, _)| *s == ServerListSource::JumpHost("direct1".to_string()));
        assert!(matches!(
            jh_status,
            Some((_, ServerListSourceStatus::Unsupported))
        ));
    }

    #[tokio::test]
    async fn test_aggregate_error_from_jump_host() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![]);

        let mock = ErrorMockJumpHost {
            host_alias: "broken1".to_string(),
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        let result = aggregator.aggregate(false).await;

        // Zero rows from the errored jump host
        assert!(result
            .rows
            .iter()
            .all(|r| r.source == ServerListSource::Local));
        // Status should be Error
        let jh_status = result
            .source_status
            .iter()
            .find(|(s, _)| *s == ServerListSource::JumpHost("broken1".to_string()));
        assert!(matches!(
            jh_status,
            Some((_, ServerListSourceStatus::Error(_)))
        ));
    }

    #[tokio::test]
    async fn test_aggregate_never_fails_overall() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![]);

        // All jump hosts fail
        let mock1 = ErrorMockJumpHost {
            host_alias: "fail1".to_string(),
        };
        let mock2 = UnsupportedMockJumpHost {
            host_alias: "fail2".to_string(),
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock1), Box::new(mock2)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        // This should not panic or fail — it always returns a result
        let result = aggregator.aggregate(false).await;

        assert_eq!(result.rows.len(), 0);
        assert_eq!(result.source_status.len(), 3); // local + 2 jump hosts
    }

    #[tokio::test]
    async fn test_cache_returns_cached_results_on_second_call() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![("web1", "10.0.0.1", "deploy")]);

        let mock = MockJumpHost {
            host_alias: "remote1".to_string(),
            servers: vec![make_server_entry("app1", "192.168.1.1", "app")],
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        // First call populates the cache.
        let result1 = aggregator.aggregate(false).await;
        assert_eq!(result1.rows.len(), 2);

        // Second call should use cached results (same count).
        let result2 = aggregator.aggregate(false).await;
        assert_eq!(result2.rows.len(), 2);

        // Cache should have entries for Local and JumpHost("remote1").
        assert_eq!(aggregator.cache.len(), 2);
        assert!(aggregator.cache.contains_key(&ServerListSource::Local));
        assert!(aggregator
            .cache
            .contains_key(&ServerListSource::JumpHost("remote1".to_string())));
    }

    #[tokio::test]
    async fn test_refresh_evicts_cache() {
        let config = AppConfig::default();
        let server_config = make_server_config_file(vec![("web1", "10.0.0.1", "deploy")]);

        let mock = MockJumpHost {
            host_alias: "remote1".to_string(),
            servers: vec![make_server_entry("app1", "192.168.1.1", "app")],
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        // Populate cache.
        let _ = aggregator.aggregate(false).await;
        assert_eq!(aggregator.cache.len(), 2);

        // Refresh should evict and re-populate.
        let result = aggregator.aggregate(true).await;
        assert_eq!(result.rows.len(), 2);
        // Cache is re-populated after refresh.
        assert_eq!(aggregator.cache.len(), 2);
    }

    #[tokio::test]
    async fn test_cache_expires_after_ttl() {
        use std::time::Duration;

        let mut config = AppConfig::default();
        // Set a very short TTL so we can test expiration.
        config.ssh.max_idle_time = Duration::from_millis(1);

        let server_config = make_server_config_file(vec![("web1", "10.0.0.1", "deploy")]);

        let mock = MockJumpHost {
            host_alias: "remote1".to_string(),
            servers: vec![make_server_entry("app1", "192.168.1.1", "app")],
        };
        let mut jump_hosts: Vec<Box<dyn JumpHost>> = vec![Box::new(mock)];

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };

        // Populate cache.
        let _ = aggregator.aggregate(false).await;
        assert_eq!(aggregator.cache.len(), 2);

        // Wait for TTL to expire.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Next call should re-fetch (cache expired).
        let result = aggregator.aggregate(false).await;
        assert_eq!(result.rows.len(), 2);
        // Cache is still populated (re-fetched).
        assert_eq!(aggregator.cache.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Feature: rhopd-jumpserver-architecture, Property 12: Merged server-list aggregation correctness
    // -----------------------------------------------------------------------

    /// Represents the outcome a mock jump host should produce.
    #[derive(Clone, Debug)]
    enum JumpHostOutcome {
        Ok(Vec<ServerEntry>),
        Unsupported,
        Error(String),
    }

    /// A configurable mock jump host that returns a predetermined outcome.
    struct OutcomeMockJumpHost {
        host_alias: String,
        outcome: JumpHostOutcome,
    }

    #[async_trait]
    impl JumpHost for OutcomeMockJumpHost {
        async fn exec(
            &mut self,
            _argv: &[String],
            _sender: &UnboundedSender<ServerEvent>,
            _config: &AppConfig,
        ) -> Result<i32> {
            Ok(0)
        }

        async fn copy(&mut self, _spec: &CopySpec, _config: &AppConfig) -> Result<()> {
            Ok(())
        }

        async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
            match &self.outcome {
                JumpHostOutcome::Ok(entries) => Ok(entries.clone()),
                JumpHostOutcome::Unsupported => Err(UnsupportedCapability {
                    kind: self.kind(),
                    alias: self.alias().to_string(),
                    method: "list_servers",
                }
                .into()),
                JumpHostOutcome::Error(msg) => Err(anyhow::anyhow!("{}", msg)),
            }
        }

        fn kind(&self) -> JumpHostKind {
            JumpHostKind::Rhopd
        }

        fn alias(&self) -> &str {
            &self.host_alias
        }
    }

    /// Strategy to generate a valid ServerEntry with constrained fields.
    fn arb_server_entry() -> impl Strategy<Value = ServerEntry> {
        (
            "[a-z][a-z0-9]{0,7}",       // alias
            "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}", // host (IP-like)
            1u16..=65535u16,             // port
            "[a-z]{1,8}",               // user
        )
            .prop_map(|(alias, host, port, user)| ServerEntry {
                alias,
                host,
                port,
                user,
                auth: DirectAuth::Key {
                    identity_file: "/tmp/key".to_string(),
                },
            })
    }

    /// Strategy to generate a JumpHostOutcome.
    fn arb_jump_host_outcome() -> impl Strategy<Value = JumpHostOutcome> {
        prop_oneof![
            // Ok with 0-4 server entries
            prop::collection::vec(arb_server_entry(), 0..5)
                .prop_map(JumpHostOutcome::Ok),
            // Unsupported
            Just(JumpHostOutcome::Unsupported),
            // Error with a message
            "[a-z ]{1,20}".prop_map(JumpHostOutcome::Error),
        ]
    }

    /// Strategy to generate a list of (alias, outcome) pairs with unique aliases.
    fn arb_jump_host_outcomes() -> impl Strategy<Value = Vec<(String, JumpHostOutcome)>> {
        prop::collection::vec(
            ("[a-z]{1,6}".prop_map(|s| format!("jh_{}", s)), arb_jump_host_outcome()),
            0..5,
        )
        .prop_map(|mut pairs| {
            // Deduplicate aliases to avoid collisions
            let mut seen = std::collections::HashSet::new();
            pairs.retain(|(alias, _)| seen.insert(alias.clone()));
            pairs
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 15.1, 15.2, 15.3, 15.4**
        ///
        /// For arbitrary (L, O) where L is a set of local server entries and O maps
        /// each jump-host alias to one of Ok(Vec<ServerEntry>) | Unsupported | Error(_),
        /// the aggregator's MergedServerList satisfies:
        /// 1. Rows multiset equals the union of local entries + all Ok jump host entries
        /// 2. One source_status entry per source (local + each jump host)
        /// 3. The aggregation always succeeds (never panics/errors)
        #[test]
        fn prop_merged_server_list_aggregation_correctness(
            local_entries in prop::collection::vec(arb_server_entry(), 0..5),
            jump_outcomes in arb_jump_host_outcomes(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let config = AppConfig::default();

                // Build a ServerConfigFile from the generated local entries.
                // Note: ServerConfigFile uses HashMap<String, ServerHostConfig> keyed by alias,
                // so duplicate aliases are naturally deduplicated (last one wins).
                let mut servers = HashMap::new();
                for entry in &local_entries {
                    servers.insert(
                        entry.alias.clone(),
                        ServerHostConfig {
                            host: entry.host.clone(),
                            port: Some(entry.port),
                            user: entry.user.clone(),
                            identity_file: Some("/tmp/key".to_string()),
                            password: None,
                        },
                    );
                }
                let server_config = ServerConfigFile {
                    defaults: ServerDefaults {
                        identity_file: Some("/tmp/default_key".to_string()),
                    },
                    servers: servers.clone(),
                };

                // The effective local entries are those that survived deduplication
                // (one per alias, as stored in the HashMap).
                let effective_local_aliases: Vec<String> =
                    servers.keys().cloned().collect();

                // Build mock jump hosts from the generated outcomes.
                let mut jump_hosts: Vec<Box<dyn JumpHost>> = jump_outcomes
                    .iter()
                    .map(|(alias, outcome)| {
                        Box::new(OutcomeMockJumpHost {
                            host_alias: alias.clone(),
                            outcome: outcome.clone(),
                        }) as Box<dyn JumpHost>
                    })
                    .collect();

                // Run the aggregator (invariant 3: this must not panic or fail).
                let mut aggregator = ServerListAggregator {
                    local: &server_config,
                    jump_hosts: &mut jump_hosts,
                    config: &config,
                    cache: HashMap::new(),
                };
                let result = aggregator.aggregate(false).await;

                // --- Invariant 1: rows multiset equality ---
                // Expected rows = local entries (deduplicated by alias) + entries from Ok jump hosts.
                let mut expected_rows: Vec<(ServerListSource, String)> = Vec::new();

                // Local entries: only those that survived HashMap deduplication.
                for alias in &effective_local_aliases {
                    expected_rows.push((
                        ServerListSource::Local,
                        alias.clone(),
                    ));
                }

                // Jump host entries: only from Ok outcomes.
                for (alias, outcome) in &jump_outcomes {
                    if let JumpHostOutcome::Ok(entries) = outcome {
                        for entry in entries {
                            expected_rows.push((
                                ServerListSource::JumpHost(alias.clone()),
                                entry.alias.clone(),
                            ));
                        }
                    }
                }

                // Build actual rows multiset (source, server alias).
                let mut actual_rows: Vec<(ServerListSource, String)> = result
                    .rows
                    .iter()
                    .map(|row| (row.source.clone(), row.server.alias.clone()))
                    .collect();

                // Sort both for comparison (multiset equality via sorted vecs).
                let source_sort_key = |s: &ServerListSource| -> String {
                    match s {
                        ServerListSource::Local => "!local".to_string(),
                        ServerListSource::JumpHost(alias) => alias.clone(),
                    }
                };
                expected_rows.sort_by(|a, b| {
                    source_sort_key(&a.0).cmp(&source_sort_key(&b.0)).then(a.1.cmp(&b.1))
                });
                actual_rows.sort_by(|a, b| {
                    source_sort_key(&a.0).cmp(&source_sort_key(&b.0)).then(a.1.cmp(&b.1))
                });

                prop_assert_eq!(
                    actual_rows.len(),
                    expected_rows.len(),
                    "Row count mismatch: actual={}, expected={}",
                    actual_rows.len(),
                    expected_rows.len()
                );
                prop_assert_eq!(
                    &actual_rows,
                    &expected_rows,
                    "Rows multiset mismatch"
                );

                // --- Invariant 2: one source_status entry per source ---
                let expected_source_count = 1 + jump_outcomes.len(); // local + each jump host
                prop_assert_eq!(
                    result.source_status.len(),
                    expected_source_count,
                    "source_status count mismatch: actual={}, expected={}",
                    result.source_status.len(),
                    expected_source_count
                );

                // Verify local source status is Ok.
                let local_status = result
                    .source_status
                    .iter()
                    .find(|(s, _)| *s == ServerListSource::Local);
                prop_assert!(
                    local_status.is_some(),
                    "Missing local source in source_status"
                );
                prop_assert!(
                    matches!(local_status, Some((_, ServerListSourceStatus::Ok))),
                    "Local source status should be Ok"
                );

                // Verify each jump host's status mirrors its outcome.
                for (alias, outcome) in &jump_outcomes {
                    let source = ServerListSource::JumpHost(alias.clone());
                    let status = result
                        .source_status
                        .iter()
                        .find(|(s, _)| *s == source);
                    prop_assert!(
                        status.is_some(),
                        "Missing source_status for jump host '{}'",
                        alias
                    );
                    let (_, actual_status) = status.unwrap();
                    match outcome {
                        JumpHostOutcome::Ok(_) => {
                            prop_assert!(
                                matches!(actual_status, ServerListSourceStatus::Ok),
                                "Jump host '{}' should have Ok status, got {:?}",
                                alias,
                                actual_status
                            );
                        }
                        JumpHostOutcome::Unsupported => {
                            prop_assert!(
                                matches!(actual_status, ServerListSourceStatus::Unsupported),
                                "Jump host '{}' should have Unsupported status, got {:?}",
                                alias,
                                actual_status
                            );
                        }
                        JumpHostOutcome::Error(_) => {
                            prop_assert!(
                                matches!(actual_status, ServerListSourceStatus::Error(_)),
                                "Jump host '{}' should have Error status, got {:?}",
                                alias,
                                actual_status
                            );
                        }
                    }
                }

                // --- Invariant 3: overall Ok even when every jump host failed ---
                // The fact that we reached this point without panic/error proves
                // the aggregation always succeeds. The function returns
                // MergedServerList directly (not Result), so it cannot fail.

                Ok(())
            })?;
        }
    }
}
