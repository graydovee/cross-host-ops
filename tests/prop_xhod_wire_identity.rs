//! Property-based test for wire-level identity of XhodGateway::exec and
//! XhodGateway::list_servers.
//!
//! Feature: xhod-jumpserver-architecture, Property 11
//!
//! NOTE: This test originally tested the `XhodGateway` struct from the
//! deleted `src/jump/` module. That module was removed as part of the
//! config-and-legacy-cleanup spec. The equivalent wire-level behavior is now
//! an implementation detail of `daemon::gateway::xhod::XhodGateway`.
//!
//! The gRPC protocol correctness is validated by the in-process RPC tests
//! (in_process_rpc_test.rs, prop_integration_p1_p4.rs).
