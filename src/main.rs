use std::{env, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use omp_discord_bridge::{config::AppConfig, discord, omp::OmpTransport, store::Store};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[arg(
        long,
        env = "OMP_DISCORD_BRIDGE_CONFIG",
        default_value = "~/.config/omp-discord-bridge/config.toml"
    )]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum Command {
    /// Start the Discord gateway.
    Run,
    /// Validate configuration, database access, token presence, and OMP RPC startup.
    Check,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match execute(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "bridge failed");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<()> {
    let config_path = expand_cli_home(cli.config)?;
    let config = AppConfig::load(&config_path)?;
    let store = Store::open(&config.runtime.database_path).await?;

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => {
            let recovered = store.recover_running().await?;
            if recovered > 0 {
                tracing::warn!(
                    recovered,
                    "marked interrupted OMP turns uncertain; they will not be replayed"
                );
            }
            discord::run(config, store).await
        }
        Command::Check => {
            drop(config.discord.load_token()?);
            let transport = OmpTransport::new(
                config.omp,
                Duration::from_secs(config.runtime.turn_timeout_seconds),
                config.runtime.max_response_bytes,
                config.runtime.discord_history_max_days,
            );
            transport.probe().await?;
            println!("configuration, SQLite, Discord token, and OMP RPC startup are valid");
            Ok(())
        }
    }
}

fn expand_cli_home(path: PathBuf) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME").context("HOME is not set; cannot expand config path")?;
        let mut expanded = PathBuf::from(home);
        if text.len() > 2 {
            expanded.push(&text[2..]);
        }
        Ok(expanded)
    } else {
        Ok(path)
    }
}
