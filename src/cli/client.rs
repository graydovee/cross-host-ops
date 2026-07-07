use anyhow::{Context, Result};

use crate::config::ClientConfig;
use crate::protocol::rpc;

use super::daemon::spawn_daemon;
use super::local_conn::{LocalEndpoint, connect, wait_for_ready};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientAccess {
    AutoStart,
    NoAutoStart,
}

pub(crate) async fn connect_data_client(
    access: ClientAccess,
) -> Result<rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>> {
    let client_config = ClientConfig::load()?;
    connect_local_data_client(&client_config, access).await
}

pub(crate) async fn connect_local_copy_client() -> Result<rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>>
{
    let client_config = ClientConfig::load()?;
    connect_local_data_client(&client_config, ClientAccess::AutoStart).await
}

async fn connect_local_data_client(
    client_config: &ClientConfig,
    access: ClientAccess,
) -> Result<rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>> {
    let endpoint = LocalEndpoint::from_config(client_config)?;
    match connect(&endpoint).await {
        Ok(client) => Ok(client),
        Err(_error) if access == ClientAccess::AutoStart && client_config.local.auto_start => {
            spawn_daemon(&super::daemon::CliDaemonStartOptions::default())?;
            wait_for_ready(&endpoint).await?;
            connect(&endpoint).await
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to connect to local daemon via {}",
                endpoint.describe_internal()
            )
        }),
    }
}
