pub mod discord;
pub mod matrix;

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Result;

use crate::{config::AppConfig, store::Store};
use tokio::sync::{Semaphore, mpsc};

pub async fn run(
    config: AppConfig,
    store: Store,
    command_receiver: mpsc::Receiver<crate::threadwatch::ControlCommand>,
    config_path: &Path,
) -> Result<()> {
    let permits = Arc::new(Semaphore::new(config.runtime.max_concurrency));
    let matrix = config.matrix.as_ref().map(|_| {
        let config = Arc::new(config.clone());
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
    let result = discord::run(config, store, command_receiver, config_path, permits).await;
    if let Some(task) = matrix {
        task.abort();
    }
    result
}
