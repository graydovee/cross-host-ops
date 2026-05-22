use super::JumpHostKind;

/// Error returned by optional `JumpHost` trait methods when the concrete
/// implementation does not support the requested capability.
#[derive(Debug, thiserror::Error)]
#[error("jump host {name} (kind={kind}) does not support method {method}")]
pub struct UnsupportedCapability {
    pub kind: JumpHostKind,
    pub name: String,
    pub method: &'static str,
}
