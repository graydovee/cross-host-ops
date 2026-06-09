use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::config::ClientConfig;
use crate::protocol::rpc;

use super::daemon::{CliDaemonStartOptions, spawn_daemon, wait_for_socket};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientAccess {
    AutoStart,
    NoAutoStart,
}

pub(crate) async fn connect_data_client(
    access: ClientAccess,
) -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>> {
    let client_config = ClientConfig::load()?;
    connect_local_data_client(&client_config, access).await
}

pub(crate) async fn connect_local_copy_client() -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>>
{
    let client_config = ClientConfig::load()?;
    connect_local_data_client(&client_config, ClientAccess::AutoStart).await
}

async fn connect_local_data_client(
    client_config: &ClientConfig,
    access: ClientAccess,
) -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>> {
    let socket_path = PathBuf::from(&client_config.local.socket_path);
    match connect_unix_client(&socket_path).await {
        Ok(client) => Ok(client),
        Err(_error) if access == ClientAccess::AutoStart && client_config.local.auto_start => {
            spawn_daemon(&CliDaemonStartOptions::default())?;
            wait_for_socket(&socket_path).await?;
            connect_unix_client(&socket_path).await
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to connect to local daemon socket {}",
                socket_path.display()
            )
        }),
    }
}

pub(crate) async fn connect_unix_client(
    socket_path: &Path,
) -> Result<rpc::xho_rpc_client::XhoRpcClient<Channel>> {
    let path = socket_path.to_path_buf();
    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?;
    Ok(rpc::xho_rpc_client::XhoRpcClient::new(channel))
}
