use anyhow::{Result, anyhow};
use uuid::Uuid;

use crate::config::{ReviewAction, RiskLevel};
use crate::connection::{CopyDirection, CopySpec};

pub mod rpc {
    tonic::include_proto!("rhop.rpc");
}

#[derive(Clone, Debug)]
pub struct ExecRequest {
    pub target: String,
    pub argv: Vec<String>,
}

pub fn copy_spec_to_rpc(target: String, spec: CopySpec) -> rpc::CopyRequest {
    rpc::CopyRequest {
        target,
        local_path: spec.local_path,
        remote_path: spec.remote_path,
        recursive: spec.recursive,
        direction: match spec.direction {
            CopyDirection::Upload => rpc::CopyDirection::Upload as i32,
            CopyDirection::Download => rpc::CopyDirection::Download as i32,
        },
    }
}

pub fn copy_spec_from_rpc(request: rpc::CopyRequest) -> Result<(String, CopySpec)> {
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
