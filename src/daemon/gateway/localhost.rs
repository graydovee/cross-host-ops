// LocalhostGateway — executes operations on the local machine directly.
//
// Backed by `LocalSession`: shell/exec run on a pseudo-terminal (or pipes), and
// the sftp subsystem is served by spawning the OS `sftp-server`. This gives the
// transparent proxy and `xho cp` a uniform path to the local host (`_self`).

use anyhow::Result;
use async_trait::async_trait;

use crate::config::DirectAuth;
use crate::daemon::session::TargetSession;
use crate::daemon::session::local::LocalSession;
use crate::daemon::shell::{build_final_command, resolve_shell};
use crate::protocol::ServerListRow;
use crate::types::ServerListSource;

use super::{Capabilities, Gateway, GatewayError, GatewayKind};

/// The reserved gateway name for local host access.
pub const SELF_GATEWAY_NAME: &str = "_self";

pub struct LocalhostGateway {
    /// Default shell (from config or $SHELL).
    shell: String,
    /// Execution user (from config or $USER).
    user: String,
    /// This machine's hostname.
    hostname: String,
    /// Optional explicit sftp-server path (from config); probed when None.
    sftp_server_path: Option<String>,
}

impl LocalhostGateway {
    pub fn new(
        shell: Option<String>,
        user: Option<String>,
        sftp_server_path: Option<String>,
    ) -> Self {
        Self {
            shell: shell
                .or_else(|| std::env::var("SHELL").ok())
                .unwrap_or_else(|| "/bin/sh".to_string()),
            user: user
                .or_else(|| std::env::var("USER").ok())
                .or_else(|| std::env::var("LOGNAME").ok())
                .unwrap_or_else(|| "unknown".to_string()),
            hostname: get_hostname(),
            sftp_server_path,
        }
    }

    fn new_session(&self) -> Box<dyn TargetSession> {
        Box::new(LocalSession::new(
            self.shell.clone(),
            self.sftp_server_path.clone(),
        )) as Box<dyn TargetSession>
    }
}

fn get_hostname() -> String {
    let mut buf = [0u8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret == 0 {
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..len]).to_string()
    } else {
        "localhost".to_string()
    }
}

#[async_trait]
impl Gateway for LocalhostGateway {
    fn name(&self) -> &str {
        SELF_GATEWAY_NAME
    }

    fn kind(&self) -> GatewayKind {
        GatewayKind::Localhost
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::EXEC | Capabilities::COPY | Capabilities::PROXY | Capabilities::LIST
    }

    async fn open_exec_session(
        &self,
        _target: &str,
        argv: &[String],
        shell: &str,
        no_shell: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError> {
        let cli_shell = (!shell.is_empty()).then_some(shell);
        let eff_shell =
            resolve_shell(cli_shell, no_shell, Some(self.shell.as_str()), "").unwrap_or_default();
        let command = build_final_command(argv, &eff_shell);
        Ok((self.new_session(), command))
    }

    async fn open_session(&self, _target: &str) -> Result<Box<dyn TargetSession>, GatewayError> {
        Ok(self.new_session())
    }

    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        Ok(vec![ServerListRow {
            source: ServerListSource::Local,
            server: crate::config::ServerEntry {
                alias: SELF_GATEWAY_NAME.to_string(),
                host: self.hostname.clone(),
                port: 0,
                user: self.user.clone(),
                auth: DirectAuth::None,
            },
        }])
    }
}
