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
