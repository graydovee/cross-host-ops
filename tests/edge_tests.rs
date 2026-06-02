//! Example/edge tests from the coverage matrix (task 13.6).
//!
//! These tests exercise specific code paths identified in the spec's coverage
//! matrix. Each test is tagged with its matrix ID for traceability.
#![allow(clippy::useless_vec)]

use std::collections::HashMap;

use rhop::config::{
    AppConfig, GatewayConfig, GatewayValidationError, RhopdGatewayConfig,
    ServerConfigFile, ServerDefaults, ServerHostConfig, RESERVED_NAMES, validate_gateways,
};
use rhop::daemon::resolver::Resolver;

// ---------------------------------------------------------------------------
// 14.2: Reserved alias rejection
// Verify validate_gateways rejects "local" as an alias.
// ---------------------------------------------------------------------------

#[test]
fn edge_14_2_reserved_alias_local_rejected() {
    let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
        name: "local".to_string(),
        address: "10.0.0.1:2222".to_string(),
        identity_file: String::new(),
        known_hosts_path: String::new(),
    })];

    let result = validate_gateways(&gateways);
    assert!(
        result.is_err(),
        "validate_gateways should reject 'local' name"
    );

    match result.unwrap_err() {
        GatewayValidationError::ReservedName { name, reserved } => {
            assert_eq!(name, "local");
            assert_eq!(reserved, RESERVED_NAMES);
        }
        other => panic!(
            "expected ReservedName error, got: {:?}",
            other
        ),
    }
}

#[test]
fn edge_14_2_reserved_names_constant_contains_local() {
    assert!(
        RESERVED_NAMES.contains(&"local"),
        "RESERVED_NAMES should contain 'local'"
    );
}

// ---------------------------------------------------------------------------
// 15.5: explicit `<jump>:<server>` lookup
// Verify resolver handles explicit form correctly.
// ---------------------------------------------------------------------------

#[test]
fn edge_15_5_explicit_jump_server_lookup() {
    let config = AppConfig::default();
    let server_config = ServerConfigFile {
        defaults: ServerDefaults {
            identity_file: None,
            shell: String::new(),
        },
        servers: HashMap::new(),
    };
    let gateways = vec![GatewayConfig::Rhopd(RhopdGatewayConfig {
        name: "prod-jump".to_string(),
        address: "10.0.0.99:2222".to_string(),
        identity_file: String::new(),
        known_hosts_path: String::new(),
    })];

    let resolver = Resolver::new(&config, &server_config, &gateways);

    // Explicit form "prod-jump:web01" should resolve to a route through prod-jump
    let routes = resolver.resolve("prod-jump:web01").unwrap();

    assert_eq!(routes.len(), 1, "should produce exactly one route");
    assert_eq!(routes[0].gateway_name, "prod-jump");
    assert_eq!(routes[0].end_target, "web01");
}

#[test]
fn edge_15_5_explicit_unknown_gateway_errors() {
    let config = AppConfig::default();
    let server_config = ServerConfigFile {
        defaults: ServerDefaults {
            identity_file: None,
            shell: String::new(),
        },
        servers: HashMap::new(),
    };
    let gateways: Vec<GatewayConfig> = vec![];

    let resolver = Resolver::new(&config, &server_config, &gateways);

    // Explicit form with unknown jump host should error
    let result = resolver.resolve("unknown-jump:web01");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("not found"),
        "error should mention 'not found', got: {}",
        msg
    );
}

#[test]
fn edge_15_5_explicit_local_source_lookup() {
    let config = AppConfig::default();
    let mut servers = HashMap::new();
    servers.insert(
        "db01".to_string(),
        ServerHostConfig {
            host: "10.0.0.5".to_string(),
            port: Some(22),
            user: "dbadmin".to_string(),
            identity_file: Some("/tmp/key".to_string()),
            password: None,
            shell: None,
        },
    );
    let server_config = ServerConfigFile {
        defaults: ServerDefaults {
            identity_file: None,
            shell: String::new(),
        },
        servers,
    };
    let gateways: Vec<GatewayConfig> = vec![];

    let resolver = Resolver::new(&config, &server_config, &gateways);

    // "local:db01" should resolve to a direct route (no hops)
    let routes = resolver.resolve("local:db01").unwrap();

    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].gateway_name, "local", "local source should route through 'local' gateway");
    assert_eq!(routes[0].end_target, "db01");
}
