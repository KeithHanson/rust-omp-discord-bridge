mod bridge;
mod collector;
mod sanitize;

pub use bridge::Bridge;
pub use collector::{Config, run};

pub fn load_token(path: &std::path::Path) -> anyhow::Result<String> {
    bridge::protected_token(path)
}

fn valid_avatar_url(url: &str) -> bool {
    let Some(path) = url.strip_prefix("https://cdn.discordapp.com/avatars/") else {
        return false;
    };
    let Some((user, image)) = path.split_once('/') else {
        return false;
    };
    let image = image.strip_suffix("?size=96").unwrap_or(image);
    let Some(hash) = image.strip_suffix(".png") else {
        return false;
    };
    let hash = hash.strip_prefix("a_").unwrap_or(hash);
    (1..=20).contains(&user.len())
        && user.bytes().all(|byte| byte.is_ascii_digit())
        && hash.len() == 32
        && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::{Bridge, collector::DeliveryQueue};
    use rusqlite::Connection;
    use serde_json::json;
    use tempfile::tempdir;

    fn bridge(id: &str) -> Bridge {
        Bridge {
            id: id.into(),
            name: "Test bot".into(),
            database_path: "/state/bridge.sqlite".into(),
            session_dir: "/state/sessions".into(),
            bot_token_file: None,
        }
    }

    #[test]
    fn reuses_legacy_queue_without_rewriting_pending_delivery() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("collector-queue.sqlite");
        let db = Connection::open(&database).unwrap();
        db.execute_batch(
            "CREATE TABLE identity (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
             CREATE TABLE deliveries (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL, payload TEXT NOT NULL);
             CREATE TABLE captured_threads (id TEXT PRIMARY KEY, revision INTEGER NOT NULL, generation INTEGER NOT NULL, metadata TEXT NOT NULL);
             CREATE TABLE captured_events (thread_id TEXT NOT NULL, event_id TEXT NOT NULL, hash TEXT NOT NULL, parts INTEGER NOT NULL, PRIMARY KEY(thread_id,event_id));",
        )
        .unwrap();
        db.execute(
            "INSERT INTO identity VALUES(1,?)",
            [json!(["bot", "/state/bridge.sqlite", "/state/sessions"]).to_string()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO deliveries(id,payload) VALUES('delivery-1','{\"legacy\":true}')",
            [],
        )
        .unwrap();
        drop(db);

        let queue = DeliveryQueue::open(directory.path(), &bridge("bot")).unwrap();
        assert_eq!(
            queue.peek().unwrap(),
            Some(("delivery-1".into(), "{\"legacy\":true}".into()))
        );
        assert_eq!(queue.count().unwrap(), 1);
    }

    #[test]
    fn rejects_state_owned_by_another_bridge() {
        let directory = tempdir().unwrap();
        DeliveryQueue::open(directory.path(), &bridge("bot-a")).unwrap();
        let error = DeliveryQueue::open(directory.path(), &bridge("bot-b"))
            .err()
            .expect("mismatched identity must fail");
        assert!(error.to_string().contains("another bridge"));
    }
}
