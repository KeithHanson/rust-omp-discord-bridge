use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

use crate::config::AppConfig;

#[derive(Debug, Clone)]
pub struct LiveConfig {
    current: Arc<ArcSwap<AppConfig>>,
}

impl LiveConfig {
    pub fn new(config: AppConfig) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(config)),
        }
    }

    pub fn load(&self) -> Arc<AppConfig> {
        self.current.load_full()
    }

    pub fn reload(&self, path: &Path) -> Result<()> {
        let candidate = AppConfig::load(path)?;
        let current = self.load();
        ensure_restart_fields_unchanged(&current, &candidate)?;
        self.current.store(Arc::new(candidate));
        Ok(())
    }
}

fn ensure_restart_fields_unchanged(current: &AppConfig, candidate: &AppConfig) -> Result<()> {
    let mut changed = Vec::new();
    if current.discord != candidate.discord {
        changed.push("discord");
    }
    if current.matrix != candidate.matrix {
        changed.push("matrix");
    }
    if current.omp.executable != candidate.omp.executable {
        changed.push("omp.executable");
    }
    if current.omp.session_dir != candidate.omp.session_dir {
        changed.push("omp.session_dir");
    }
    if current.omp.allowed_root != candidate.omp.allowed_root {
        changed.push("omp.allowed_root");
    }
    if current.runtime.database_path != candidate.runtime.database_path
        || current.runtime.max_concurrency != candidate.runtime.max_concurrency
        || current.runtime.discord_message_limit != candidate.runtime.discord_message_limit
        || current.runtime.max_attachment_bytes != candidate.runtime.max_attachment_bytes
        || current.runtime.max_attachments != candidate.runtime.max_attachments
        || current.runtime.discord_history_max_messages
            != candidate.runtime.discord_history_max_messages
        || current.runtime.discord_history_max_bytes != candidate.runtime.discord_history_max_bytes
        || current.runtime.discord_history_timeout_seconds
            != candidate.runtime.discord_history_timeout_seconds
    {
        changed.push("runtime fields other than max_response_bytes, discord_history_max_days, and turn_timeout_seconds");
    }
    if current.threadwatch.is_some() != candidate.threadwatch.is_some() {
        changed.push("threadwatch");
    } else if let (Some(old), Some(new)) = (&current.threadwatch, &candidate.threadwatch)
        && (old.server_url != new.server_url
            || old.ingest_token_file != new.ingest_token_file
            || old.bot_id != new.bot_id
            || old.bot_name != new.bot_name
            || old.data_dir != new.data_dir
            || old.collect_interval_ms != new.collect_interval_ms
            || old.heartbeat_interval_ms != new.heartbeat_interval_ms)
    {
        changed.push("threadwatch");
    }
    if changed.is_empty() {
        Ok(())
    } else {
        bail!(
            "restart-required configuration changed: {}",
            changed.join(", ")
        )
    }
}

pub fn spawn_watcher(path: PathBuf, config: LiveConfig) -> Result<tokio::task::JoinHandle<()>> {
    let watched_path = path.clone();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<notify::Result<Event>>();
    let mut watcher = RecommendedWatcher::new(
        move |event| {
            let _ = sender.send(event);
        },
        notify::Config::default(),
    )
    .context("failed to create configuration watcher")?;
    let parent = path
        .parent()
        .context("configuration path has no parent directory")?;
    watcher
        .watch(parent, RecursiveMode::NonRecursive)
        .with_context(|| {
            format!(
                "failed to watch configuration directory {}",
                parent.display()
            )
        })?;

    Ok(tokio::spawn(async move {
        let _watcher = watcher;
        while let Some(event) = receiver.recv().await {
            let relevant = match event {
                Ok(event) => event.paths.iter().any(|changed| changed == &watched_path),
                Err(error) => {
                    tracing::warn!(error = %error, "configuration watcher error");
                    false
                }
            };
            if !relevant {
                continue;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            while receiver.try_recv().is_ok() {}
            match config.reload(&watched_path) {
                Ok(()) => {
                    tracing::info!(path = %watched_path.display(), "reloaded live configuration")
                }
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), path = %watched_path.display(), "kept last-known-good configuration")
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copied_config() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let root = directory.path().display();
        let source = std::fs::read_to_string("config.example.toml")
            .unwrap()
            .replace("~/src/project", &root.to_string())
            .replace(
                "allowed_root = \"~/src\"",
                &format!("allowed_root = \"{root}\""),
            );
        std::fs::write(&path, source).unwrap();
        (directory, path)
    }

    #[tokio::test]
    async fn watcher_publishes_valid_external_change() {
        let (_directory, path) = copied_config();
        let live = LiveConfig::new(AppConfig::load(&path).unwrap());
        let watcher = spawn_watcher(path.clone(), live.clone()).unwrap();
        AppConfig::write_default_model(&path, "rtx-inkling/inkling-2-mini").unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if live.load().omp.model.as_deref() == Some("rtx-inkling/inkling-2-mini") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("watcher did not publish the changed model");
        watcher.abort();
    }

    #[test]
    fn reload_rejects_restart_required_change_and_keeps_last_good_snapshot() {
        let (_directory, path) = copied_config();
        let live = LiveConfig::new(AppConfig::load(&path).unwrap());
        let source = std::fs::read_to_string(&path).unwrap();
        std::fs::write(
            &path,
            source.replace("executable = \"omp\"", "executable = \"other-omp\""),
        )
        .unwrap();
        let error = live.reload(&path).unwrap_err();
        assert!(error.to_string().contains("omp.executable"), "{error:#}");
        assert_eq!(live.load().omp.executable, PathBuf::from("omp"));
    }
}
