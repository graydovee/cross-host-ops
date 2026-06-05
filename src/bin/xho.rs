use clap::Parser;

use xho::cli::ArunCli;
use xho::exit_codes::{EXIT_INTERNAL, XhoError};

#[tokio::main]
async fn main() {
    // Try parsing; if --version was requested, clap returns a DisplayVersion error.
    // We intercept it to support `--output json` version output.
    let cli = match ArunCli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            match e.kind() {
                clap::error::ErrorKind::DisplayVersion => {
                    // Check if --output json was passed by inspecting raw args
                    let args: Vec<String> = std::env::args().collect();
                    let is_json = args
                        .windows(2)
                        .any(|w| w[0] == "--output" && w[1] == "json");
                    if is_json {
                        xho::cli::print_version_json();
                    } else {
                        // Default text version output
                        print!("{}", e);
                    }
                    std::process::exit(0);
                }
                _ => {
                    e.exit();
                }
            }
        }
    };

    match xho::cli::run_cli(cli).await {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            // Attempt to extract a typed XhoError for its exit code.
            let exit_code = error
                .downcast_ref::<XhoError>()
                .map(|e| e.exit_code())
                .unwrap_or(EXIT_INTERNAL);
            eprintln!("{error:#}");
            std::process::exit(exit_code);
        }
    }
}
