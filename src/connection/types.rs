use std::fmt;

use crate::config::DirectAuth;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyDirection {
    Upload,
    Download,
}

#[derive(Clone, Debug)]
pub struct CopySpec {
    pub direction: CopyDirection,
    pub local_path: String,
    pub remote_path: String,
    pub recursive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TargetTransport {
    Direct,
    Jump,
}

impl fmt::Display for TargetTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetTransport::Direct => write!(f, "direct"),
            TargetTransport::Jump => write!(f, "jump"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedTarget {
    pub input: String,
    pub ip: String,
    pub key: String,
    pub transport: TargetTransport,
    pub direct: Option<DirectTarget>,
    pub target_label: String,
}

#[derive(Clone, Debug)]
pub struct DirectTarget {
    pub host: String,
    pub host_name: String,
    pub port: u16,
    pub user: String,
    pub auth: DirectAuth,
    pub proxy_command: Option<String>,
    pub pubkey_accepted_algorithms: Option<String>,
}
