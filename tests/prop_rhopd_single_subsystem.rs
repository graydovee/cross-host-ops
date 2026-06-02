//! Property test: exactly one rhop-rpc subsystem per RhopdJumpHost.
//!
//! Feature: rhopd-connect-and-server-list, Property 4
//!
//! NOTE: This test originally tested the `RhopdJumpHost` struct from the
//! deleted `src/jump/` module. That module was removed as part of the
//! config-and-legacy-cleanup spec. The equivalent functionality (single
//! connection per gateway) is now an implementation detail of
//! `daemon::gateway::rhopd::RhopdGateway` which manages its own connection
//! pool internally.
//!
//! The connection reuse behavior is validated at the gateway level by the
//! gateway transport retry test (prop_gateway_transport_retry.rs).
