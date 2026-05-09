use clap::Parser;

#[tokio::main]
async fn main() {
    let cli = rhop::cli::ArunCli::parse();
    match rhop::cli::run_cli(cli).await {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }
}
