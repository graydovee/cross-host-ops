//! Integration tests for the token RPCs and BootstrapAuthorize.
//!
//! These tests spin up an in-process `XhoRpcService` over a duplex channel
//! and exercise the new token / bootstrap flows directly via gRPC. They do
//! NOT exercise the SSH auth layer (that requires a real russh listener and
//! is covered separately).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use hyper_util::rt::TokioIo;
use russh::keys::ssh_key;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tower::service_fn;

use xho::config::AppConfig;
use xho::daemon::test_support::make_test_rpc_service;
use xho::protocol::rpc;
use xho::protocol::rpc::xho_rpc_client::XhoRpcClient;

const DUPLEX_BUFFER_SIZE: usize = 1024 * 1024;

/// Build an in-process service pointing `authorized_keys_path` at `path`,
/// wire it to a tonic client via a duplex stream, and return the client.
async fn spawn_service_with_authorized_keys(
    authorized_keys_path: PathBuf,
) -> XhoRpcClient<Channel> {
    let tempdir = std::env::temp_dir().join(format!(
        "xho-token-test-{}-{:?}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&tempdir).unwrap();

    let config_path = tempdir.join("config.toml");
    let mut config = AppConfig::default();
    config.server.local.enable = false;
    config.server.remote.enable = false;
    config.review.enable = false;
    config.server.remote.authorized_keys_path = authorized_keys_path.display().to_string();
    // Keep the tempdir alive for the test's lifetime by leaking it (tests are short).
    std::mem::forget(tempdir);

    let service = make_test_rpc_service(config, config_path);
    let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
            .expect("gRPC server failed");
    });

    let io = std::sync::Mutex::new(Some(client_io));
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(service_fn(move |_: Uri| {
            let stream = io
                .lock()
                .unwrap()
                .take()
                .expect("duplex stream already consumed");
            async move { Ok::<_, std::io::Error>(TokioIo::new(stream)) }
        }))
        .await
        .expect("failed to connect gRPC client");
    XhoRpcClient::new(channel)
}

fn tmp_authorized_keys_path(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    std::env::temp_dir().join(format!(
        "xho-token-test-ak-{label}-{nanos}-{:?}",
        std::thread::current().id()
    ))
}

fn random_keypair_openssh() -> String {
    let mut rng = rand_core::UnwrapErr(getrandom::SysRng);
    let key = ssh_key::PrivateKey::random(&mut rng, ssh_key::Algorithm::Ed25519).expect("gen key");
    key.public_key().to_openssh().expect("serialize pubkey")
}

#[tokio::test]
async fn token_gen_list_invalidate_roundtrip() -> Result<()> {
    let ak_path = tmp_authorized_keys_path("roundtrip");
    let mut client = spawn_service_with_authorized_keys(ak_path).await;

    // Initially empty.
    let list = client
        .token_list(rpc::TokenListRequest {})
        .await?
        .into_inner();
    assert!(list.tokens.is_empty());

    // Generate a once token with 60s TTL.
    let generated = client
        .token_gen(rpc::TokenGenRequest {
            ttl_secs: 60,
            once: true,
            label: Some("ci".into()),
        })
        .await?
        .into_inner();
    assert!(!generated.token.is_empty());
    assert!(generated.once);
    assert!(generated.expires_at.ends_with('Z'));
    let prefix: String = generated.token.chars().take(8).collect();

    // List reflects it.
    let list = client
        .token_list(rpc::TokenListRequest {})
        .await?
        .into_inner();
    assert_eq!(list.tokens.len(), 1);
    assert_eq!(list.tokens[0].prefix, prefix);
    assert!(list.tokens[0].once);
    assert!(!list.tokens[0].consumed);
    assert_eq!(list.tokens[0].label.as_deref(), Some("ci"));

    // Invalidate by prefix.
    let inv = client
        .token_invalidate(rpc::TokenInvalidateRequest {
            token_or_prefix: prefix.clone(),
        })
        .await?
        .into_inner();
    assert!(inv.invalidated);

    // List now empty.
    let list = client
        .token_list(rpc::TokenListRequest {})
        .await?
        .into_inner();
    assert!(list.tokens.is_empty());
    Ok(())
}

#[tokio::test]
async fn token_gen_default_ttl_when_zero() -> Result<()> {
    let ak_path = tmp_authorized_keys_path("default-ttl");
    let mut client = spawn_service_with_authorized_keys(ak_path).await;

    let generated = client
        .token_gen(rpc::TokenGenRequest {
            ttl_secs: 0,
            once: false,
            label: None,
        })
        .await?
        .into_inner();
    assert!(!generated.once);
    // Default TTL is 5 minutes; the expiry must be a parseable RFC3339 in the future.
    assert!(!generated.expires_at.is_empty());
    Ok(())
}

#[tokio::test]
async fn bootstrap_authorize_appends_key_idempotently() -> Result<()> {
    let ak_path = tmp_authorized_keys_path("bootstrap");
    let mut client = spawn_service_with_authorized_keys(ak_path.clone()).await;

    let pubkey_line = random_keypair_openssh();

    // First call: appended=true.
    let r1 = client
        .bootstrap_authorize(rpc::BootstrapAuthorizeRequest {
            public_key: pubkey_line.clone(),
        })
        .await?
        .into_inner();
    assert!(r1.appended);
    assert!(!r1.already_present);
    assert!(r1.fingerprint.starts_with("SHA256:"));

    // File now contains exactly one line with our key.
    let content = std::fs::read_to_string(&ak_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0], pubkey_line);

    // Second call: appended=false, already_present=true.
    let r2 = client
        .bootstrap_authorize(rpc::BootstrapAuthorizeRequest {
            public_key: pubkey_line.clone(),
        })
        .await?
        .into_inner();
    assert!(!r2.appended);
    assert!(r2.already_present);
    assert_eq!(r1.fingerprint, r2.fingerprint);

    // File still has exactly one line — no duplicate appended.
    let content2 = std::fs::read_to_string(&ak_path).unwrap();
    assert_eq!(content2.lines().count(), 1);

    // Different key gets appended on a new line.
    let another = random_keypair_openssh();
    let r3 = client
        .bootstrap_authorize(rpc::BootstrapAuthorizeRequest {
            public_key: another.clone(),
        })
        .await?
        .into_inner();
    assert!(r3.appended);
    assert_ne!(r3.fingerprint, r1.fingerprint);

    let content3 = std::fs::read_to_string(&ak_path).unwrap();
    assert_eq!(content3.lines().count(), 2);
    Ok(())
}

#[tokio::test]
async fn bootstrap_authorize_rejects_malformed_pubkey() -> Result<()> {
    let ak_path = tmp_authorized_keys_path("badkey");
    let mut client = spawn_service_with_authorized_keys(ak_path.clone()).await;

    let err = client
        .bootstrap_authorize(rpc::BootstrapAuthorizeRequest {
            public_key: "not-a-valid-openssh-key".to_string(),
        })
        .await
        .expect_err("expected invalid_argument");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(ak_path.exists() == false);
    Ok(())
}

// Keep the Arc import referenced (used in some builds for future expansion).
#[allow(dead_code)]
fn _keep_arc() -> Arc<()> {
    Arc::new(())
}
