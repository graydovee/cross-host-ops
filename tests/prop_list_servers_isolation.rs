//! Property test: list_servers handler is per-source isolated and order-preserving
//!
//! Feature: xhod-connect-and-server-list, Property 5
//!
//! NOTE: This test originally tested the `Gateway` trait + `ServerListAggregator`
//! architecture which was removed as part of the config-and-legacy-cleanup spec.
//! The equivalent functionality is now in `daemon::rpc::process_list_servers`
//! which uses the Gateway trait. The gateway-level list_servers isolation is
//! tested in `prop_gateway_list_servers_merge.rs`.
//!
//! This file is intentionally left as a placeholder to avoid breaking test
//! discovery infrastructure. The original property is validated by the gateway
//! merge test.
