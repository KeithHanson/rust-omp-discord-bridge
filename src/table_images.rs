use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

use crate::render::chunk_message;

const RENDERER: &str = include_str!("../scripts/markdown-table-image.py");
const RENDER_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_PARTS: usize = 32;
const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";

#[derive(Debug)]
pub(crate) enum AssistantPart {
    Text(String),
    Image { data: Vec<u8>, filename: String },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ManifestPart {
    Text(ManifestText),
    Image(ManifestImage),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestText {
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestImage {
    image: String,
}

/// Prepare every attachment before the caller starts any Discord delivery.
pub(crate) async fn prepare(content: &str, limit: usize) -> Result<Vec<AssistantPart>> {
    ensure!(limit >= 100, "Discord message limit must be at least 100");
    if !might_contain_table(content) {
        return Ok(text_parts(content, limit));
    }

    let directory = tempfile::tempdir().context("creating table renderer directory")?;
    let directory_path = tokio::fs::canonicalize(directory.path())
        .await
        .context("resolving table renderer directory")?;
    let script = directory_path.join("markdown-table-image.py");
    let input = directory_path.join("input.md");
    let output = directory_path.join("table.png");
    tokio::fs::write(&script, RENDERER)
        .await
        .context("writing embedded table renderer")?;
    tokio::fs::write(&input, content)
        .await
        .context("writing table renderer input")?;

    let mut command = Command::new("uv");
    command
        .arg("run")
        .arg("--no-project")
        .arg("--with")
        .arg("markdown-it-py==4.0.0")
        .arg("--with")
        .arg("playwright==1.56.0")
        .arg("python")
        .arg(&script)
        .arg(&input)
        .arg(&output)
        .arg("--manifest")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // The bridge runs on Linux. A separate group lets cancellation notify the
    // Python renderer and Playwright driver, not just uv's immediate process.
    command.process_group(0);
    let child = command.spawn().context("starting table renderer with uv")?;
    let mut process = RendererProcess::new(child)?;
    let stdout = process.child.stdout.take().context("missing renderer stdout")?;
    let stderr = process.child.stderr.take().context("missing renderer stderr")?;
    let (status, manifest, diagnostics) = tokio::time::timeout(RENDER_TIMEOUT, async {
        tokio::try_join!(
            async { process.child.wait().await.context("waiting for table renderer") },
            read_bounded(stdout, MAX_MANIFEST_BYTES, "table renderer manifest"),
            read_bounded(stderr, 64 * 1024, "table renderer diagnostics"),
        )
    })
    .await
    .context("table renderer exceeded its 45-second deadline")??;
    // The renderer and both output pipes have finished; do not retain a stale
    // process-group ID while loading attachments.
    process.group = None;
    drop(process);
    ensure!(
        status.success(),
        "table renderer failed ({status}): {}",
        String::from_utf8_lossy(&diagnostics).trim()
    );
    let parts = load_manifest(&manifest, &directory_path, limit).await?;
    Ok(parts)
}

fn might_contain_table(content: &str) -> bool {
    content.contains('|') && content.contains('-')
}

fn text_parts(content: &str, limit: usize) -> Vec<AssistantPart> {
    chunk_message(content, limit)
        .into_iter()
        .map(AssistantPart::Text)
        .collect()
}

async fn read_bounded(
    reader: impl AsyncRead + Unpin,
    maximum: usize,
    description: &str,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .with_context(|| format!("reading {description}"))?;
    ensure!(bytes.len() <= maximum, "{description} exceeds {maximum} bytes");
    Ok(bytes)
}

async fn load_manifest(manifest: &[u8], directory: &Path, limit: usize) -> Result<Vec<AssistantPart>> {
    let entries: Vec<ManifestPart> =
        serde_json::from_slice(manifest).context("invalid table renderer manifest")?;
    ensure!(!entries.is_empty(), "table renderer returned an empty manifest");
    ensure!(entries.len() <= MAX_PARTS, "table renderer exceeds {MAX_PARTS} parts");
    let mut parts = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            ManifestPart::Text(entry) => {
                // Discord rejects whitespace-only text between adjacent tables.
                if !entry.text.trim().is_empty() {
                    parts.extend(text_parts(&entry.text, limit));
                }
            }
            ManifestPart::Image(entry) => {
                let path = Path::new(&entry.image);
                ensure!(path.is_absolute(), "table image path must be absolute");
                let metadata = tokio::fs::symlink_metadata(path)
                    .await
                    .context("reading table image metadata")?;
                ensure!(metadata.file_type().is_file(), "table image must be a regular file");
                ensure!(
                    metadata.len() <= MAX_ATTACHMENT_BYTES as u64,
                    "table image exceeds the 10 MiB attachment limit"
                );
                let canonical = tokio::fs::canonicalize(path)
                    .await
                    .context("resolving table image path")?;
                ensure!(canonical.starts_with(directory), "table image escapes renderer directory");
                ensure!(canonical.extension().is_some_and(|ext| ext == "png"), "table image must be a PNG");
                let file = tokio::fs::File::open(&canonical)
                    .await
                    .context("opening table image")?;
                let data = read_bounded(file, MAX_ATTACHMENT_BYTES, "table image").await?;
                ensure!(data.starts_with(PNG_SIGNATURE), "table image has an invalid PNG signature");
                parts.push(AssistantPart::Image {
                    data,
                    filename: format!("table-{}.png", parts.len() + 1),
                });
            }
        }
        ensure!(parts.len() <= MAX_PARTS, "assistant output exceeds {MAX_PARTS} parts");
    }
    if parts.is_empty() {
        bail!("table renderer returned no deliverable parts");
    }
    Ok(parts)
}

struct RendererProcess {
    child: Child,
    group: Option<u32>,
}

impl RendererProcess {
    fn new(child: Child) -> Result<Self> {
        let group = child.id().context("table renderer has no process ID")?;
        Ok(Self { child, group: Some(group) })
    }
}

impl Drop for RendererProcess {
    fn drop(&mut self) {
        let Some(group) = self.group else {
            return;
        };
        // Safe std/Tokio APIs do not expose killpg. Arch supplies /usr/bin/kill;
        // invoke it directly (without a shell) to notify the entire private group.
        // Python handles TERM by unwinding Playwright and closing Chromium, which
        // can have its own process group. Child's kill_on_drop is the uv backstop.
        if let Err(error) = std::process::Command::new("/usr/bin/kill")
            .args(["-TERM", "--", &format!("-{group}")])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            tracing::warn!(%error, "could not signal table renderer process group");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_text_uses_existing_chunker() {
        let content = "ordinary text with Unicode 🦀\n".repeat(200);
        let parts = prepare(&content, 100).await.unwrap();
        assert!(parts.len() > MAX_PARTS);
        let actual: Vec<_> = parts
            .into_iter()
            .map(|part| match part {
                AssistantPart::Text(text) => text,
                AssistantPart::Image { .. } => panic!("plain text became an image"),
            })
            .collect();
        assert_eq!(actual, chunk_message(&content, 100));
    }

    #[test]
    fn candidate_filter_accepts_tables_without_requiring_outer_pipes() {
        assert!(might_contain_table("a | b\n- | -\nx | y"));
        assert!(might_contain_table("| a | b |\n| :--- | ---: |"));
        assert!(might_contain_table("> | a | b |\n> | --- | --- |"));
        assert!(might_contain_table("- | a | b |\n  | --- | --- |"));
        assert!(!might_contain_table("ordinary prose\n---\nmore prose"));
    }

    #[tokio::test]
    async fn manifest_preserves_text_and_image_order() {
        let directory = tempfile::tempdir().unwrap();
        let root = tokio::fs::canonicalize(directory.path()).await.unwrap();
        let image = root.join("test.png");
        tokio::fs::write(&image, PNG_SIGNATURE).await.unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!([
            {"text": "before"}, {"text": "\n \t"}, {"image": image},
            {"text": "\n\n"}, {"text": "after"}
        ]))
        .unwrap();
        let parts = load_manifest(&manifest, &root, 100).await.unwrap();
        assert!(matches!(&parts[..], [AssistantPart::Text(before), AssistantPart::Image { .. }, AssistantPart::Text(after)] if before == "before" && after == "after"));
    }

    #[tokio::test]
    async fn manifest_rejects_ambiguous_entries_and_escaping_paths() {
        let directory = tempfile::tempdir().unwrap();
        let root = tokio::fs::canonicalize(directory.path()).await.unwrap();
        for manifest in [
            br#"[{"text":"a","image":"/tmp/a.png"}]"#.as_slice(),
            br#"[{"unknown":"a"}]"#,
            br#"[{"image":"relative.png"}]"#,
            br#"[]"#,
        ] {
            assert!(load_manifest(manifest, &root, 100).await.is_err());
        }
        let outside = tempfile::Builder::new().suffix(".png").tempfile().unwrap();
        tokio::fs::write(outside.path(), PNG_SIGNATURE).await.unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!([{"image": outside.path()}])).unwrap();
        assert!(load_manifest(&manifest, &root, 100).await.is_err());
    }

    #[tokio::test]
    async fn manifest_rejects_invalid_png_and_excess_parts() {
        let directory = tempfile::tempdir().unwrap();
        let root = tokio::fs::canonicalize(directory.path()).await.unwrap();
        let image = root.join("invalid.png");
        tokio::fs::write(&image, "not a PNG").await.unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!([{"image": image}])).unwrap();
        assert!(load_manifest(&manifest, &root, 100).await.is_err());
        let manifest = serde_json::to_vec(&vec![serde_json::json!({"text": "a"}); MAX_PARTS + 1]).unwrap();
        assert!(load_manifest(&manifest, &root, 100).await.is_err());
        let manifest = serde_json::to_vec(&serde_json::json!([{"text": "x".repeat(100 * (MAX_PARTS + 1))}])).unwrap();
        assert!(load_manifest(&manifest, &root, 100).await.is_err());
    }

    #[tokio::test]
    #[ignore = "requires local uv, Chromium, and renderer Python dependencies"]
    async fn local_renderer_smoke() {
        let content = "Before\n\n| Name | Value |\n| --- | --- |\n| Example | 42 |\n\nAfter";
        let parts = prepare(content, 1900).await.unwrap();
        assert!(matches!(&parts[..], [AssistantPart::Text(before), AssistantPart::Image { data, .. }, AssistantPart::Text(after)] if before.contains("Before") && data.starts_with(PNG_SIGNATURE) && data.len() > PNG_SIGNATURE.len() && after.contains("After")));
        let fenced = "```markdown\n| A | B |\n| --- | --- |\n| 1 | 2 |\n```";
        let parts = prepare(fenced, 1900).await.unwrap();
        assert!(matches!(&parts[..], [AssistantPart::Text(text)] if text == fenced));
    }
}
