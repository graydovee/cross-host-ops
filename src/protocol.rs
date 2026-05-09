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

#[derive(Clone, Debug)]
pub struct AuthPromptMessage {
    pub prompt_id: String,
    pub target_label: String,
    pub kind: String,
    pub secret: bool,
    pub message: String,
}

pub fn copy_spec_to_rpc(target: String, spec: CopySpec) -> rpc::CopyRequest {
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
        })),
    }
}

pub fn copy_spec_from_rpc(request: rpc::CopyStartRequest) -> Result<(String, CopySpec)> {
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
