use std::{fmt::Write as _, path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::{io::AsyncReadExt, process::Command, time::timeout};

const MAX_OUTPUT_BYTES: u64 = 1_048_576;

pub(crate) async fn fetch(executable: &Path, cwd: &Path) -> Result<String> {
    let mut child = Command::new(executable)
        .args([
            "usage",
            "--json",
            "--provider",
            "openai-codex",
            "--no-extensions",
        ])
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("could not start OMP usage query")?;
    let result = timeout(Duration::from_secs(20), async {
        let stdout = child.stdout.take().context("usage stdout unavailable")?;
        let mut bytes = Vec::new();
        stdout
            .take(MAX_OUTPUT_BYTES + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 > MAX_OUTPUT_BYTES {
            bail!("usage response exceeded size limit");
        }
        if !child.wait().await?.success() {
            bail!("OMP usage query failed");
        }
        let snapshot: Value = serde_json::from_slice(&bytes).context("invalid usage response")?;
        Ok(render(&snapshot))
    })
    .await
    .context("OMP usage query timed out")
    .and_then(|result| result);
    if result.is_err() {
        let _ = child.kill().await;
    }
    result
}

fn render(snapshot: &Value) -> String {
    let mut output = String::from("OpenAI/Codex quota usage (authenticated accounts):");
    let mut accounts = 0;
    if let Some(reports) = snapshot.get("reports").and_then(Value::as_array) {
        for report in reports {
            if report.get("provider").and_then(Value::as_str) != Some("openai-codex") {
                continue;
            }
            accounts += 1;
            let _ = write!(output, "\nAccount {accounts}:");
            let mut windows = 0;
            if let Some(limits) = report.get("limits").and_then(Value::as_array) {
                for limit in limits {
                    let window = match limit.pointer("/window/id").and_then(Value::as_str) {
                        Some("7d") => "Weekly",
                        Some("5h") => "5-hour",
                        _ => match limit.pointer("/window/durationMs").and_then(Value::as_u64) {
                            Some(604_800_000) => "Weekly",
                            Some(18_000_000) => "5-hour",
                            _ => "Usage window",
                        },
                    };
                    let percent = limit
                        .pointer("/amount/usedFraction")
                        .and_then(Value::as_f64)
                        .map(|fraction| fraction * 100.0)
                        .or_else(|| {
                            (limit.pointer("/amount/unit").and_then(Value::as_str)
                                == Some("percent"))
                            .then(|| limit.pointer("/amount/used").and_then(Value::as_f64))
                            .flatten()
                        })
                        .filter(|value| value.is_finite() && *value >= 0.0);
                    let _ = write!(output, "\n- {window}: ");
                    if let Some(percent) = percent {
                        let _ = write!(output, "{percent:.1}% used");
                    } else {
                        output.push_str("usage unavailable");
                    }
                    if let Some(reset) = timestamp(limit.pointer("/window/resetsAt")) {
                        let _ = write!(output, "; resets <t:{reset}:F> (<t:{reset}:R>)");
                    } else {
                        output.push_str("; reset time unavailable");
                    }
                    windows += 1;
                }
            }
            if windows == 0 {
                output.push_str(" quota usage unavailable");
            }
            if let Some(fetched) = timestamp(report.get("fetchedAt")) {
                let _ = write!(output, "\nUpdated <t:{fetched}:R>.");
            }
        }
    }
    if accounts == 0 {
        output.push_str("\nUnavailable: OMP returned no OpenAI/Codex quota reports.");
    }
    output
}

fn timestamp(value: Option<&Value>) -> Option<u64> {
    value
        .and_then(Value::as_u64)
        .map(|millis| millis / 1_000)
        .filter(|seconds| *seconds > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn weekly_usage_preserves_reset_and_excludes_account_metadata() {
        let text = render(&json!({"reports": [{
            "provider": "openai-codex", "fetchedAt": 1794541635667u64,
            "metadata": {"email": "private@example.com", "accountId": "private-id", "orgId": "private-org"},
            "limits": [{"window": {"id": "7d", "resetsAt": 1795021679000u64}, "amount": {"usedFraction": 0.2}}]
        }]}));
        assert!(text.contains("Weekly: 20.0% used"));
        assert!(text.contains("<t:1795021679:F> (<t:1795021679:R>)"));
        assert!(text.contains("Updated <t:1794541635:R>"));
        assert!(!text.contains("private"));
    }

    #[test]
    fn missing_usage_is_not_reported_as_zero_or_an_invented_reset() {
        let text = render(
            &json!({"reports": [{"provider": "openai-codex", "limits": [{"window": {"id": "7d"}}]}]}),
        );
        assert!(text.contains("Weekly: usage unavailable; reset time unavailable"));
        assert!(!text.contains("0.0%"));
        assert!(render(&json!({"reports": []})).contains("no OpenAI/Codex quota reports"));
    }

    #[test]
    fn accounts_and_windows_remain_separate_and_other_providers_are_omitted() {
        let text = render(&json!({"reports": [
            {"provider": "other", "limits": [{"amount": {"usedFraction": 0.9}}]},
            {"provider": "openai-codex", "limits": [{"window": {"id": "5h"}, "amount": {"usedFraction": 0.4}}]},
            {"provider": "openai-codex", "limits": [{"window": {"durationMs": 604800000}, "amount": {"unit": "percent", "used": 75}}]}
        ]}));
        assert!(text.contains("Account 1:\n- 5-hour: 40.0% used"));
        assert!(text.contains("Account 2:\n- Weekly: 75.0% used"));
        assert!(!text.contains("90.0%"));
    }
}
