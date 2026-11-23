pub mod discord;
pub mod matrix;

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context as _, Result};

use crate::{live_config::LiveConfig, store::Store};
use tokio::sync::{Semaphore, mpsc};

pub async fn run(
    live_config: LiveConfig,
    store: Store,
    command_receiver: mpsc::Receiver<crate::threadwatch::ControlCommand>,
    config_path: &Path,
) -> Result<()> {
    let config = live_config.load();
    let permits = Arc::new(Semaphore::new(config.runtime.max_concurrency));
    let matrix = config.matrix.as_ref().map(|_| {
        let config = Arc::clone(&config);
        let permits = Arc::clone(&permits);
        let matrix_store = store.clone();
        tokio::spawn(async move {
            loop {
                if let Err(error) = matrix::run(Arc::clone(&config), matrix_store.clone(), Arc::clone(&permits)).await {
                    tracing::error!(error = %format!("{error:#}"), "Matrix adapter stopped; restarting");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    });
    let result = match config.discord {
        Some(_) => discord::run(live_config, store, command_receiver, config_path, permits).await,
        None => {
            let _ = command_receiver;
            shutdown_signal().await
        }
    };
    if let Some(task) = matrix {
        task.abort();
    }
    result
}

pub(crate) async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to listen for SIGTERM")?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for shutdown signal")
            }
            received = terminate.recv() => {
                received.context("SIGTERM listener closed unexpectedly")?;
                Ok(())
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for shutdown signal")
    }
}
