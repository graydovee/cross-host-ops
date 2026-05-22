pub mod address;
pub mod auth;
pub mod auth_resolution;
pub mod direct;
pub mod error;
pub mod factory;
pub mod jumpserver;
pub mod pty;
pub mod rhopd;
pub mod server_list;
pub mod types;

pub use types::{EndTarget, EndTargetId, JumpHopRef, ServerListSource, TargetRoute};

use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use crate::config::{AppConfig, ServerEntry};
use crate::connection::CopySpec;
use crate::protocol::ServerEvent;

pub use error::UnsupportedCapability;

/// Identifies the concrete kind of a jump host for pool keying, diagnostics,
/// and configuration dispatch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JumpHostKind {
    Direct,
    Jumpserver,
    Rhopd,
}

impl fmt::Display for JumpHostKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JumpHostKind::Direct => write!(f, "direct"),
            JumpHostKind::Jumpserver => write!(f, "jumpserver"),
            JumpHostKind::Rhopd => write!(f, "rhopd"),
        }
    }
}

/// A unified abstraction over the different ways the daemon can reach an end
/// target. Concrete implementations exist for direct SSH, interactive
/// jumpserver shells, and remote `rhopd` daemons.
#[async_trait]
pub trait JumpHost: Send {
    /// Required. Run a command on the end target reachable through this hop.
    async fn exec(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
    ) -> Result<i32>;

    /// Required. Carry out the remote-side half of a copy. The local-side I/O
    /// is the responsibility of whoever called this method (the local daemon).
    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()>;

    /// Optional. Open a fully interactive PTY shell session to the end target.
    /// Default returns an `UnsupportedCapability` error.
    async fn tui_shell(&mut self, _config: &AppConfig) -> Result<()> {
        Err(UnsupportedCapability {
            kind: self.kind(),
            name: self.name().to_string(),
            method: "tui_shell",
        }
        .into())
    }

    /// Optional. Enumerate the end-target catalog reachable through this hop.
    /// Default returns an `UnsupportedCapability` error.
    async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
        Err(UnsupportedCapability {
            kind: self.kind(),
            name: self.name().to_string(),
            method: "list_servers",
        }
        .into())
    }

    /// Identity for pool keying and diagnostics.
    fn kind(&self) -> JumpHostKind;
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Feature: rhopd-jumpserver-architecture, Property 5: UnsupportedCapability error contract

    /// A mock JumpHost that only implements the required methods (exec, copy,
    /// kind, name) and does NOT override tui_shell or list_servers, so the
    /// default implementations fire.
    struct MockJumpHost {
        host_name: String,
        host_kind: JumpHostKind,
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

        fn kind(&self) -> JumpHostKind {
            self.host_kind
        }

        fn name(&self) -> &str {
            &self.host_name
        }
    }

    /// Strategy to generate arbitrary JumpHostKind values.
    fn arb_jump_host_kind() -> impl Strategy<Value = JumpHostKind> {
        prop_oneof![
            Just(JumpHostKind::Direct),
            Just(JumpHostKind::Jumpserver),
            Just(JumpHostKind::Rhopd),
        ]
    }

    /// Strategy to generate non-empty alias strings (the Display format always
    /// includes the name, so we need at least one character to verify containment).
    fn arb_alias() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_][a-zA-Z0-9_\\-]{0,30}".prop_map(|s| s)
    }

    /// Strategy to generate method names from the set of optional trait methods.
    fn arb_method() -> impl Strategy<Value = &'static str> {
        prop_oneof![Just("tui_shell"), Just("list_servers"),]
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.4, 3.6, 4.5, 16.3, 16.4**
        ///
        /// For arbitrary alias strings and method names in {"tui_shell", "list_servers"},
        /// calling the default trait method on a synthesized JumpHost returns Err,
        /// the error downcasts to UnsupportedCapability, and its Display rendering
        /// contains the name, the textual JumpHostKind, and the method name.
        #[test]
        fn prop_unsupported_capability_error_contract(
            alias in arb_alias(),
            kind in arb_jump_host_kind(),
            method in arb_method(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let config = AppConfig::default();
                let mut mock = MockJumpHost {
                    host_name: alias.clone(),
                    host_kind: kind,
                };

                let result = match method {
                    "tui_shell" => mock.tui_shell(&config).await.map(|_| ()),
                    "list_servers" => mock.list_servers(&config).await.map(|_| ()),
                    _ => unreachable!(),
                };

                // The result must be an error
                let err = result.expect_err(
                    "default tui_shell/list_servers should return Err"
                );

                // The error must downcast to UnsupportedCapability
                let unsupported = err
                    .downcast_ref::<UnsupportedCapability>()
                    .expect("error should downcast to UnsupportedCapability");

                // Verify the fields match
                prop_assert_eq!(unsupported.kind, kind);
                prop_assert_eq!(&unsupported.name, &alias);
                prop_assert_eq!(unsupported.method, method);

                // Verify Display rendering contains name, kind name, and method name
                let display = format!("{}", err);
                prop_assert!(
                    display.contains(&alias),
                    "Display should contain name '{}', got: {}",
                    alias,
                    display
                );
                prop_assert!(
                    display.contains(&kind.to_string()),
                    "Display should contain kind '{}', got: {}",
                    kind,
                    display
                );
                prop_assert!(
                    display.contains(method),
                    "Display should contain method '{}', got: {}",
                    method,
                    display
                );

                Ok(())
            })?;
        }
    }
}
