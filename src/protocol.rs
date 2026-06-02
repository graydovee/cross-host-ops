use anyhow::{Result, anyhow};
use uuid::Uuid;

use crate::config::{ReviewAction, RiskLevel, ServerEntry};
use crate::connection::{CopyDirection, CopySpec};
use crate::jump::ServerListSource;

pub mod rpc {
    tonic::include_proto!("rhop.rpc");
}

#[derive(Clone, Debug)]
pub struct ExecRequest {
    pub target: String,
    pub argv: Vec<String>,
    pub pty: bool,
    pub no_pty: bool,
    pub stdin: bool,
    pub timeout_ms: u64,
    pub interactive: bool,
    pub term_cols: u32,
    pub term_rows: u32,
    pub shell: String,
}

#[derive(Clone, Debug)]
pub struct AuthPromptMessage {
    pub prompt_id: String,
    pub target_label: String,
    pub kind: String,
    pub secret: bool,
    pub message: String,
}

pub fn copy_spec_to_rpc(target: String, spec: CopySpec, timeout_ms: u64) -> rpc::CopyRequest {
    rpc::CopyRequest {
        request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
            target,
            local_path: spec.local_path,
            remote_path: spec.remote_path,
            recursive: spec.recursive,
            direction: match spec.direction {
                CopyDirection::Upload => rpc::CopyDirection::Upload as i32,
                CopyDirection::Download => rpc::CopyDirection::Download as i32,
            },
            timeout_ms,
        })),
    }
}

pub fn copy_spec_from_rpc(request: rpc::CopyStartRequest) -> Result<(String, CopySpec, u64)> {
    let direction = match rpc::CopyDirection::try_from(request.direction)
        .unwrap_or(rpc::CopyDirection::Unspecified)
    {
        rpc::CopyDirection::Upload => CopyDirection::Upload,
        rpc::CopyDirection::Download => CopyDirection::Download,
        rpc::CopyDirection::Unspecified => {
            return Err(anyhow!("copy direction is required"));
        }
    };
    Ok((
        request.target,
        CopySpec {
            direction,
            local_path: request.local_path,
            remote_path: request.remote_path,
            recursive: request.recursive,
        },
        request.timeout_ms,
    ))
}

#[derive(Debug)]
pub enum ServerEvent {
    ReviewResult {
        execution_id: Uuid,
        risk_level: RiskLevel,
        action: ReviewAction,
        reason: String,
        matched_whitelist_reason: Option<String>,
    },
    ConfirmRequired {
        execution_id: Uuid,
        reason: String,
    },
    AuthPrompt {
        prompt_id: String,
        target_label: String,
        kind: String,
        secret: bool,
        message: String,
    },
    Stdout {
        data: Vec<u8>,
    },
    Stderr {
        data: Vec<u8>,
    },
    ExitStatus {
        code: i32,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct PoolStatus {
    pub key: String,
    pub total: usize,
    pub busy: usize,
    pub idle: usize,
    pub queued: usize,
}

pub fn parse_execution_id(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| anyhow!("invalid execution_id {}: {}", value, error))
}

pub fn server_event_to_rpc(event: ServerEvent) -> rpc::ExecuteResponse {
    use rpc::execute_response::Event;
    let event = match event {
        ServerEvent::ReviewResult {
            execution_id,
            risk_level,
            action,
            reason,
            matched_whitelist_reason,
        } => Event::ReviewResult(rpc::ReviewResult {
            execution_id: execution_id.to_string(),
            risk_level: risk_level.to_string(),
            action: action.to_string(),
            reason,
            matched_whitelist_reason: matched_whitelist_reason.unwrap_or_default(),
        }),
        ServerEvent::ConfirmRequired {
            execution_id,
            reason,
        } => Event::ConfirmRequired(rpc::ConfirmRequired {
            execution_id: execution_id.to_string(),
            reason,
        }),
        ServerEvent::AuthPrompt {
            prompt_id,
            target_label,
            kind,
            secret,
            message,
        } => Event::AuthPrompt(rpc::AuthPrompt {
            prompt_id,
            target_label,
            kind,
            secret,
            message,
        }),
        ServerEvent::Stdout { data } => Event::Stdout(rpc::OutputChunk { data }),
        ServerEvent::Stderr { data } => Event::Stderr(rpc::OutputChunk { data }),
        ServerEvent::ExitStatus { code } => Event::ExitStatus(rpc::ExitStatus { code }),
        ServerEvent::Error { message } => Event::Error(rpc::ErrorResponse { message }),
    };
    rpc::ExecuteResponse { event: Some(event) }
}

pub fn error_response(message: impl Into<String>) -> rpc::ExecuteResponse {
    rpc::ExecuteResponse {
        event: Some(rpc::execute_response::Event::Error(rpc::ErrorResponse {
            message: message.into(),
        })),
    }
}

pub fn pool_status_to_rpc(status: PoolStatus) -> rpc::PoolStatus {
    rpc::PoolStatus {
        key: status.key,
        total: status.total as u64,
        busy: status.busy as u64,
        idle: status.idle as u64,
        queued: status.queued as u64,
    }
}

pub fn execute_auth_input_request(prompt_id: String, value: String) -> rpc::ExecuteRequest {
    rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::AuthInput(
            rpc::AuthInputRequest { prompt_id, value },
        )),
    }
}

pub fn copy_auth_input_request(prompt_id: String, value: String) -> rpc::CopyRequest {
    rpc::CopyRequest {
        request: Some(rpc::copy_request::Request::AuthInput(
            rpc::AuthInputRequest { prompt_id, value },
        )),
    }
}

pub fn auth_prompt_message_to_rpc(message: AuthPromptMessage) -> rpc::AuthPrompt {
    rpc::AuthPrompt {
        prompt_id: message.prompt_id,
        target_label: message.target_label,
        kind: message.kind,
        secret: message.secret,
        message: message.message,
    }
}

pub fn copy_auth_prompt_response(message: AuthPromptMessage) -> rpc::CopyResponse {
    rpc::CopyResponse {
        event: Some(rpc::copy_response::Event::AuthPrompt(
            auth_prompt_message_to_rpc(message),
        )),
    }
}

pub fn copy_complete_response(message: impl Into<String>) -> rpc::CopyResponse {
    rpc::CopyResponse {
        event: Some(rpc::copy_response::Event::Complete(rpc::CopyComplete {
            message: message.into(),
        })),
    }
}

pub fn copy_error_response(message: impl Into<String>) -> rpc::CopyResponse {
    rpc::CopyResponse {
        event: Some(rpc::copy_response::Event::Error(rpc::ErrorResponse {
            message: message.into(),
        })),
    }
}

// --- JumpHostStatus helpers ---

/// Domain representation of a jump host's status, including optional nested
/// sub-status from a remote daemon.
#[derive(Clone, Debug)]
pub struct JumpHostStatus {
    pub name: String,
    pub kind: String,
    pub address: String,
    pub sub_status: Option<Box<rpc::StatusResponse>>,
}

pub fn jump_host_status_to_rpc(status: JumpHostStatus) -> rpc::JumpHostStatus {
    rpc::JumpHostStatus {
        name: status.name,
        kind: status.kind,
        address: status.address,
        sub_status: status.sub_status.map(|s| *s),
    }
}

pub fn jump_host_status_from_rpc(rpc_status: rpc::JumpHostStatus) -> JumpHostStatus {
    JumpHostStatus {
        name: rpc_status.name,
        kind: rpc_status.kind,
        address: rpc_status.address,
        sub_status: rpc_status.sub_status.map(Box::new),
    }
}

// --- ServerEntry helpers ---

pub fn server_entry_to_rpc(entry: ServerEntry) -> rpc::ServerEntry {
    let auth_kind = entry.auth_kind().to_string();
    rpc::ServerEntry {
        alias: entry.alias,
        host: entry.host,
        port: entry.port as u32,
        user: entry.user,
        auth_kind,
    }
}

pub fn server_entry_from_rpc(entry: rpc::ServerEntry) -> ServerEntry {
    use crate::config::DirectAuth;
    ServerEntry {
        alias: entry.alias,
        host: entry.host,
        port: entry.port as u16,
        user: entry.user,
        auth: match entry.auth_kind.as_str() {
            "password" => DirectAuth::Password {
                password: String::new(),
            },
            _ => DirectAuth::Key {
                identity_file: String::new(),
            },
        },
    }
}

// --- MergedServerList / ServerListRow / SourceStatus helpers ---

/// Domain representation of a source's status in the merged server list.
#[derive(Clone, Debug)]
pub enum ServerListSourceStatus {
    Ok,
    Unsupported,
    Error(String),
}

/// Domain representation of a single row in the merged server list.
#[derive(Clone, Debug)]
pub struct ServerListRow {
    pub source: ServerListSource,
    pub server: ServerEntry,
}

/// Domain representation of the merged server list response.
#[derive(Clone, Debug)]
pub struct MergedServerList {
    pub rows: Vec<ServerListRow>,
    pub source_status: Vec<(ServerListSource, ServerListSourceStatus)>,
}

pub fn server_list_source_to_string(source: &ServerListSource) -> String {
    match source {
        ServerListSource::Local => "local".to_string(),
        ServerListSource::JumpHost(alias) => alias.clone(),
    }
}

pub fn server_list_source_from_string(s: &str) -> ServerListSource {
    if s == "local" {
        ServerListSource::Local
    } else {
        ServerListSource::JumpHost(s.to_string())
    }
}

pub fn source_status_to_rpc(
    source: &ServerListSource,
    status: &ServerListSourceStatus,
) -> rpc::SourceStatus {
    let (status_str, detail) = match status {
        ServerListSourceStatus::Ok => ("ok".to_string(), String::new()),
        ServerListSourceStatus::Unsupported => ("unsupported".to_string(), String::new()),
        ServerListSourceStatus::Error(msg) => ("error".to_string(), msg.clone()),
    };
    rpc::SourceStatus {
        source: server_list_source_to_string(source),
        status: status_str,
        detail,
    }
}

pub fn source_status_from_rpc(
    rpc_status: rpc::SourceStatus,
) -> (ServerListSource, ServerListSourceStatus) {
    let source = server_list_source_from_string(&rpc_status.source);
    let status = match rpc_status.status.as_str() {
        "ok" => ServerListSourceStatus::Ok,
        "unsupported" => ServerListSourceStatus::Unsupported,
        _ => ServerListSourceStatus::Error(rpc_status.detail),
    };
    (source, status)
}

pub fn server_list_row_to_rpc(row: ServerListRow) -> rpc::ServerListRow {
    rpc::ServerListRow {
        server: Some(server_entry_to_rpc(row.server)),
        source: server_list_source_to_string(&row.source),
    }
}

pub fn server_list_row_from_rpc(rpc_row: rpc::ServerListRow) -> Option<ServerListRow> {
    let server_entry = rpc_row.server?;
    Some(ServerListRow {
        source: server_list_source_from_string(&rpc_row.source),
        server: server_entry_from_rpc(server_entry),
    })
}

pub fn merged_server_list_to_rpc(list: MergedServerList) -> rpc::MergedServerList {
    rpc::MergedServerList {
        rows: list.rows.into_iter().map(server_list_row_to_rpc).collect(),
        source_status: list
            .source_status
            .iter()
            .map(|(source, status)| source_status_to_rpc(source, status))
            .collect(),
    }
}

pub fn merged_server_list_from_rpc(rpc_list: rpc::MergedServerList) -> MergedServerList {
    MergedServerList {
        rows: rpc_list
            .rows
            .into_iter()
            .filter_map(server_list_row_from_rpc)
            .collect(),
        source_status: rpc_list
            .source_status
            .into_iter()
            .map(source_status_from_rpc)
            .collect(),
    }
}
