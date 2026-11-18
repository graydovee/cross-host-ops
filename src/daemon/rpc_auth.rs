//! Caller-scoped authorization for the daemon control-plane RPC service.

use tonic::{Request, Status};
use tracing::warn;

use super::ssh_server::RemoteConnectInfo;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallerScope {
    Local,
    RemoteKey,
    Token,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RpcClass {
    Business,
    Admin,
    Bootstrap,
}

pub(crate) fn caller_scope<T>(request: &Request<T>) -> CallerScope {
    let info = request
        .extensions()
        .get::<Option<RemoteConnectInfo>>()
        .and_then(Option::as_ref);

    match info {
        None => CallerScope::Local,
        Some(info) if info.via_token => CallerScope::Token,
        Some(_) => CallerScope::RemoteKey,
    }
}

pub(crate) fn authorize<T>(request: &Request<T>, class: RpcClass) -> Result<(), Status> {
    let scope = caller_scope(request);
    let allowed = match scope {
        CallerScope::Local => true,
        CallerScope::RemoteKey => matches!(class, RpcClass::Business),
        CallerScope::Token => matches!(class, RpcClass::Bootstrap),
    };

    if allowed {
        return Ok(());
    }

    warn!(
        caller_scope = ?scope,
        rpc_class = ?class,
        "control-plane RPC authorization denied"
    );
    Err(Status::permission_denied(format!(
        "{class:?} RPCs are not allowed for {scope:?} callers"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn request(scope: CallerScope) -> Request<()> {
        let mut request = Request::new(());
        match scope {
            CallerScope::Local => {}
            CallerScope::RemoteKey | CallerScope::Token => {
                request.extensions_mut().insert(Some(RemoteConnectInfo {
                    peer_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12222)),
                    ssh_user: "xho".to_string(),
                    public_key_fingerprint: "SHA256:test".to_string(),
                    via_token: scope == CallerScope::Token,
                }));
            }
        }
        request
    }

    #[test]
    fn caller_scope_reads_connection_extension() {
        assert_eq!(
            caller_scope(&request(CallerScope::Local)),
            CallerScope::Local
        );
        assert_eq!(
            caller_scope(&request(CallerScope::RemoteKey)),
            CallerScope::RemoteKey
        );
        assert_eq!(
            caller_scope(&request(CallerScope::Token)),
            CallerScope::Token
        );
    }

    #[test]
    fn authorization_matrix_is_enforced() {
        for class in [RpcClass::Business, RpcClass::Admin, RpcClass::Bootstrap] {
            assert!(authorize(&request(CallerScope::Local), class).is_ok());
        }

        assert!(authorize(&request(CallerScope::RemoteKey), RpcClass::Business).is_ok());
        assert!(authorize(&request(CallerScope::RemoteKey), RpcClass::Admin).is_err());
        assert!(authorize(&request(CallerScope::RemoteKey), RpcClass::Bootstrap).is_err());

        assert!(authorize(&request(CallerScope::Token), RpcClass::Business).is_err());
        assert!(authorize(&request(CallerScope::Token), RpcClass::Admin).is_err());
        assert!(authorize(&request(CallerScope::Token), RpcClass::Bootstrap).is_ok());
    }
}
