use std::{env, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use omp_client_bridge::{
    clients,
    config::AppConfig,
    live_config::{LiveConfig, spawn_watcher},
    omp::OmpTransport,
    store::Store,
    threadwatch,
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version = env!("BRIDGE_BUILD_VERSION"), about)]
struct Cli {
    #[arg(
        long,
        env = "OMP_CLIENT_BRIDGE_CONFIG",
        default_value = "~/.config/omp-client-bridge/config.toml"
    )]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum Command {
    /// Start all configured client adapters.
    Run,
    /// Validate configuration, database access, token presence, and OMP RPC startup.
    Check,
    /// Validate a replacement without opening or migrating the live database.
    #[command(hide = true)]
    CheckUpdate,
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

fn collector_config(
    config: &AppConfig,
    command_sender: tokio::sync::mpsc::Sender<threadwatch::ControlCommand>,
) -> Result<Option<threadwatch::Config>> {
    let Some(reporting) = &config.threadwatch else {
        return Ok(None);
    };
    let token = threadwatch::load_token(&reporting.ingest_token_file)?;
    ensure!(
        token.len() >= 32 && !token.chars().any(char::is_whitespace),
        "Threadwatch ingest token must contain at least 32 non-whitespace characters"
    );
    Ok(Some(threadwatch::Config {
        server_url: reporting.server_url.clone(),
        token,
        bridge: threadwatch::Bridge {
            id: reporting.bot_id.clone(),
            name: reporting.bot_name.clone(),
            database_path: config.runtime.database_path.clone(),
            session_dir: config.omp.session_dir.clone(),
            bot_token_file: config.discord.as_ref().map(|d| d.token_file.clone()),
        },
        data_dir: reporting.data_dir.clone(),
        collect_interval: Duration::from_millis(reporting.collect_interval_ms),
        heartbeat_interval: Duration::from_millis(reporting.heartbeat_interval_ms),
        request_timeout: Duration::from_secs(10),
        command_sender,
    }))
}
async fn execute(cli: Cli) -> Result<()> {
    let config_path = expand_cli_home(cli.config)?;
    let config = AppConfig::load(&config_path)?;
    if config.discord.is_none() && config.matrix.is_none() {
        tracing::warn!(
            "no Discord or Matrix client is configured; the bridge will start idle and will not respond to any client"
        );
    }
    let (command_sender, command_receiver) = tokio::sync::mpsc::channel(32);
    let collector = collector_config(&config, command_sender)?;
    if matches!(cli.command, Some(Command::CheckUpdate)) {
        validate_client_tokens(&config)?;
        let transport = OmpTransport::new(
            config.omp,
            Duration::from_secs(config.runtime.turn_timeout_seconds),
            config.runtime.max_response_bytes,
            config.runtime.discord_history_max_days,
        );
        tokio::time::timeout(Duration::from_secs(30), transport.probe())
            .await
            .context("replacement OMP probe timed out")??;
        println!("replacement configuration, token, and OMP RPC startup are valid");
        return Ok(());
    }
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
            let reporting = collector.map(|config| {
                tokio::spawn(async move {
                    if let Err(error) = threadwatch::run(config, false).await {
                        tracing::error!(error = %format!("{error:#}"), "Threadwatch reporting stopped");
                    }
                })
            });
            let live_config = LiveConfig::new(config);
            let watcher = spawn_watcher(config_path.clone(), live_config.clone())?;
            let result = clients::run(live_config, store, command_receiver, &config_path).await;
            watcher.abort();
            if let Some(task) = reporting {
                task.abort();
            }
            result
        }
        Command::Check => {
            validate_client_tokens(&config)?;
            let transport = OmpTransport::new(
                config.omp,
                Duration::from_secs(config.runtime.turn_timeout_seconds),
                config.runtime.max_response_bytes,
                config.runtime.discord_history_max_days,
            );
            transport.probe().await?;
            println!("configuration, SQLite, client tokens, and OMP RPC startup are valid");
            Ok(())
        }
        Command::CheckUpdate => unreachable!("handled before opening the database"),
    }
}

fn validate_client_tokens(config: &AppConfig) -> Result<()> {
    if let Some(discord) = &config.discord {
        drop(discord.load_token()?);
    }
    if let Some(matrix) = &config.matrix {
        drop(matrix.load_access_token()?);
    }
    Ok(())
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
