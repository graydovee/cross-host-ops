use crate::jump::JumpHostKind;

/// Identifies an end target for pool keying and routing purposes.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EndTargetId(pub String);

/// Represents an ordered list of jump hops followed by an end target.
/// Produced by the resolver from configuration plus a CLI target string.
#[derive(Clone, Debug)]
pub struct TargetRoute {
    /// The jump hops to traverse (empty for direct routes).
    pub hops: Vec<JumpHopRef>,
    /// The end target to reach.
    pub end_target: EndTarget,
}

/// A reference to a jump host in a target route.
#[derive(Clone, Debug)]
pub struct JumpHopRef {
    pub name: String,
    pub kind: JumpHostKind,
}

/// The final destination in a target route.
#[derive(Clone, Debug)]
pub struct EndTarget {
    pub id: EndTargetId,
    pub alias: String,
}

/// Identifies the source of server-list entries.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ServerListSource {
    /// Entries from the local daemon's own server.toml.
    Local,
    /// Entries from a configured jump host.
    JumpHost(String), // the jump host alias
}
