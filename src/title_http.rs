use std::{sync::Mutex, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{
    Client, Method, StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde_json::Value;
use serenity::all::{Channel, Http};
use tokio::time::Instant;

use crate::domain::ThreadId;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const UNKNOWN_RETRY: Duration = Duration::from_secs(2);

/// Held by the messenger, not by a request future: cancellation never erases a cooldown.
#[derive(Default)]
pub(super) struct TitleState {
    get_until: Option<Instant>,
    patch_until: Option<Instant>,
    pub working: bool,
}

pub(super) enum Reply<T> {
    Ready(T),
    Limited,
}

pub(super) struct TitleHttp {
    client: Client,
    base_url: String,
    global_until: Mutex<Option<Instant>>,
}

impl TitleHttp {
    pub fn new(http: &Http) -> Result<Self> {
        let mut authorization =
            HeaderValue::from_str(http.token()).context("invalid Discord authorization header")?;
        authorization.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);
        Ok(Self {
            client: Client::builder()
                .default_headers(headers)
                .user_agent("omp-client-bridge")
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .timeout(REQUEST_TIMEOUT)
                .build()
                .context("failed to create Discord title HTTP client")?,
            base_url: http
                .proxy
                .as_deref()
                .unwrap_or("https://discord.com/api/v10")
                .trim_end_matches('/')
                .to_owned(),
            global_until: Mutex::new(None),
        })
    }

    pub fn global_deadline(&self) -> Option<Instant> {
        *self
            .global_until
            .lock()
            .expect("title global cooldown poisoned")
    }

    /// Always called before reading the session, reviewing, or fetching the current title.
    pub async fn wait(&self, state: &TitleState) {
        loop {
            let deadline = state
                .get_until
                .max(state.patch_until)
                .max(self.global_deadline());
            match deadline {
                Some(deadline) if deadline > Instant::now() => {
                    tokio::time::sleep_until(deadline).await
                }
                _ => return,
            }
        }
    }

    pub fn patch_blocked(&self, state: &TitleState) -> bool {
        state
            .patch_until
            .max(self.global_deadline())
            .is_some_and(|until| until > Instant::now())
    }

    pub async fn channel(
        &self,
        thread_id: ThreadId,
        state: &mut TitleState,
    ) -> Result<Reply<Channel>> {
        match self
            .request(thread_id, Method::GET, None, &mut state.get_until)
            .await?
        {
            Reply::Ready(value) => Ok(Reply::Ready(
                serde_json::from_value(value).context("invalid Discord thread title response")?,
            )),
            Reply::Limited => Ok(Reply::Limited),
        }
    }

    pub async fn patch(
        &self,
        thread_id: ThreadId,
        name: &str,
        state: &mut TitleState,
    ) -> Result<Reply<()>> {
        match self
            .request(
                thread_id,
                Method::PATCH,
                Some(serde_json::json!({"name": name})),
                &mut state.patch_until,
            )
            .await?
        {
            Reply::Ready(_) => Ok(Reply::Ready(())),
            Reply::Limited => Ok(Reply::Limited),
        }
    }

    // No retry middleware: a 429 returns control to the caller, which discards its old inference.
    async fn request(
        &self,
        thread_id: ThreadId,
        method: Method,
        body: Option<Value>,
        until: &mut Option<Instant>,
    ) -> Result<Reply<Value>> {
        if self
            .global_deadline()
            .is_some_and(|deadline| deadline > Instant::now())
        {
            return Ok(Reply::Limited);
        }
        let mut request = self
            .client
            .request(method, format!("{}/channels/{thread_id}", self.base_url));
        if let Some(body) = body {
            request = request.json(&body);
        }
        // A cancelled/timed-out request may have reached Discord. Keep a conservative
        // reservation until its request deadline, unless response headers supersede it.
        *until = Some(Instant::now() + REQUEST_TIMEOUT);
        let response = request
            .send()
            .await
            .context("Discord title HTTP request failed")?;
        let status = response.status();
        let headers = response.headers();
        let reset = seconds_header(headers, "x-ratelimit-reset-after");
        let retry = seconds_header(headers, "retry-after").max(reset);
        let global = headers
            .get("x-ratelimit-global")
            .is_some_and(|value| value == "true")
            || headers
                .get("x-ratelimit-scope")
                .is_some_and(|value| value == "global");
        let limited = status == StatusCode::TOO_MANY_REQUESTS;
        let exhausted = headers
            .get("x-ratelimit-remaining")
            .is_some_and(|value| value == "0");
        *until = if limited || exhausted {
            Some(Instant::now() + retry.unwrap_or(UNKNOWN_RETRY))
        } else {
            None
        };
        if global && limited {
            self.extend_global(until.expect("rate limited request has a deadline"));
        }
        if limited {
            // Preserve the header/default reservation even if reading JSON fails or is cancelled.
            let value: Value = response
                .json()
                .await
                .context("invalid Discord title rate-limit response")?;
            let delay = value
                .get("retry_after")
                .and_then(Value::as_f64)
                .and_then(seconds)
                .max(retry)
                .unwrap_or(UNKNOWN_RETRY);
            let deadline = Instant::now() + delay;
            *until = Some(deadline);
            if global || value.get("global").and_then(Value::as_bool) == Some(true) {
                self.extend_global(until.expect("rate limited request has a deadline"));
            }
            return Ok(Reply::Limited);
        }
        if !status.is_success() {
            // Never include request headers, Discord bodies, or credentials in errors.
            bail!("Discord title HTTP returned {status}");
        }
        let value = response
            .json()
            .await
            .context("invalid Discord title HTTP response")?;
        Ok(Reply::Ready(value))
    }

    fn extend_global(&self, deadline: Instant) {
        let mut global = self
            .global_until
            .lock()
            .expect("title global cooldown poisoned");
        *global = Some(global.unwrap_or(deadline).max(deadline));
    }
}

fn seconds_header(headers: &HeaderMap, name: &str) -> Option<Duration> {
    headers
        .get(name)?
        .to_str()
        .ok()?
        .parse()
        .ok()
        .and_then(seconds)
}

fn seconds(value: f64) -> Option<Duration> {
    let duration = Duration::try_from_secs_f64(value).ok()?;
    // Zero/malformed retry values must not create a busy retry loop.
    (duration > Duration::ZERO && Instant::now().checked_add(duration).is_some())
        .then_some(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cooldowns_survive_cancelled_waits_and_global_extensions() {
        let http = TitleHttp::new(&Http::new("test-token")).unwrap();
        let start = Instant::now();
        let state = TitleState {
            patch_until: Some(start + Duration::from_secs(10)),
            ..TitleState::default()
        };
        assert!(
            tokio::time::timeout(Duration::from_secs(2), http.wait(&state))
                .await
                .is_err()
        );
        http.extend_global(start + Duration::from_secs(20));
        http.wait(&state).await;
        assert_eq!(Instant::now(), start + Duration::from_secs(20));
    }

    #[test]
    fn retry_seconds_accept_fractional_values_and_reject_invalid_delays() {
        assert_eq!(seconds(0.25), Some(Duration::from_millis(250)));
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::MAX] {
            assert_eq!(seconds(value), None);
        }
    }
}
