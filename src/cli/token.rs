//! `xho token` subcommand — manage short-lived bootstrap tokens on the local
//! daemon. Runs against the local Unix socket (the daemon on THIS host); to
//! issue a token for a remote xhod, run `xho token gen` on that host.

use anyhow::{Context, Result};

use crate::config::parse_duration;
use crate::protocol::rpc;

use super::args::TokenCommand;
use super::client::{ClientAccess, connect_data_client};

pub(crate) async fn run_token_command(command: TokenCommand) -> Result<i32> {
    let mut client = connect_data_client(ClientAccess::AutoStart)
        .await
        .context("failed to connect to local daemon")?;
    match command {
        TokenCommand::Gen {
            ttl,
            reusable,
            label,
        } => generate(&mut client, ttl.as_deref(), reusable, label).await,
        TokenCommand::List => list(&mut client).await,
        TokenCommand::Invalidate { token } => invalidate(&mut client, &token).await,
    }
}

async fn generate(
    client: &mut rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>,
    ttl: Option<&str>,
    reusable: bool,
    label: Option<String>,
) -> Result<i32> {
    let ttl_secs = match ttl {
        Some(s) => {
            let dur = parse_duration(s).with_context(|| format!("invalid --ttl {s:?}"))?;
            if dur.as_secs() == 0 {
                anyhow::bail!("--ttl must be greater than zero");
            }
            dur.as_secs()
        }
        None => 300, // 5 minutes
    };
    let request = rpc::TokenGenRequest {
        ttl_secs,
        once: !reusable,
        label: label
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
    };
    let response = client
        .token_gen(request)
        .await
        .context("TokenGen RPC failed")?
        .into_inner();
    println!("token:        {}", response.token);
    println!("expires_at:   {}", response.expires_at);
    println!(
        "reusable:     {}",
        if response.once {
            "no (single-use)"
        } else {
            "yes"
        }
    );
    eprintln!();
    eprintln!("warning: this token is shown only once; copy it now.");
    eprintln!(
        "        pass it to `xho host add <name> <addr> --token <TOKEN>` (or `xho host login`)."
    );
    Ok(0)
}

async fn list(
    client: &mut rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>,
) -> Result<i32> {
    let response = client
        .token_list(rpc::TokenListRequest {})
        .await
        .context("TokenList RPC failed")?
        .into_inner();
    if response.tokens.is_empty() {
        println!("(no active tokens)");
        return Ok(0);
    }
    println!(
        "{:<10} {:<22} {:<8} {:<10} {}",
        "PREFIX", "EXPIRES_AT", "ONCE", "CONSUMED", "LABEL"
    );
    for t in response.tokens {
        println!(
            "{:<10} {:<22} {:<8} {:<10} {}",
            t.prefix,
            t.expires_at,
            if t.once { "yes" } else { "no" },
            if t.consumed { "yes" } else { "no" },
            t.label.unwrap_or_default(),
        );
    }
    Ok(0)
}

async fn invalidate(
    client: &mut rpc::xho_rpc_client::XhoRpcClient<tonic::transport::Channel>,
    token: &str,
) -> Result<i32> {
    let response = client
        .token_invalidate(rpc::TokenInvalidateRequest {
            token_or_prefix: token.trim().to_string(),
        })
        .await
        .context("TokenInvalidate RPC failed")?
        .into_inner();
    if response.invalidated {
        println!("invalidated token matching {:?}", token);
    } else {
        println!("no active token matched {:?}", token);
    }
    Ok(0)
}
