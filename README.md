# OMP Discord Bridge

Rust Discord gateway for persistent, thread-scoped OMP sessions.

A permitted user mentions the bot in an allowed parent channel. The bridge creates a Discord thread, starts an OMP conversation for that thread, interrupts active work when a newer message arrives, and resumes the same saved OMP session across idle periods and daemon restarts.

## Current behavior

- mention-to-thread session creation;
- explicit guild, parent-channel, and user allowlists;
- durable SQLite session and turn state;
- ordered, durable message admission with immediate interruption of active work and bounded global concurrency;
- real OMP resume through its native RPC protocol;
- one-shot Discord approval buttons for OMP writes and executable actions;
- one editable activity message per active conversation run, including replacement turns, with assistant messages delivered as they complete;
- `status`, `stop`, and `new` controls inside managed threads;
- duplicate Discord message rejection;
- restart recovery that marks interrupted turns uncertain instead of replaying them;
- bounded attachment metadata and Discord-safe response chunking;
- on-demand OMP access to bounded parent-channel message history.

Attachment bytes are not downloaded by the bridge. OMP receives the Discord CDN URL, filename, content type, and byte count as untrusted prompt context.

Activity messages show fixed, public labels for thinking, file inspection, tool
execution, response writing, approval waits, interruption, retries, and compaction.
Updates are coalesced every five seconds, including elapsed-time refreshes during
quiet work. The activity message remains live while newer messages replace earlier
turns; completion, cancellation, failure, and uncertain outcomes finalize it.
Tool arguments, tool output, and reasoning content are never included.

Activity delivery is best-effort: a failed or timed-out create/edit is logged and
disables further status updates for that turn without interrupting OMP or replaying
work. Each request has a ten-second deadline. Assistant text is delivered in separate
messages. After a process crash, an old activity message can remain stale; use
`status` to inspect durable turn state. Activity message IDs are not persisted.

## Prerequisites

Install Rust with `rustup`:

```bash
rustup toolchain install stable --profile minimal --component rustfmt,clippy
```

Install and authenticate OMP **18.20.0 or newer**, then verify `omp --version` succeeds. The bridge uses `omp --mode rpc-ui` and correlated `prompt_result` completion events; it does not use ACP.

Create a Discord application and bot with:

- Message Content Intent enabled;
- OAuth scopes `bot` and `applications.commands`;
- permissions to view channels, read message history, send messages, create public threads, send messages in threads, and use external emoji only if desired.

## Configuration

### Local `.env` setup (Linux/Unix)

For a single server, channel, and user, use the local launcher:

```bash
cp .env.example .env
chmod 600 .env
# Edit .env: token, server/channel/user IDs, and OMP working directory.
cargo build --release --bins
./target/release/bridge-local check
./target/release/bridge-local run
```

If `.env` already exists, edit it rather than copying over it. Enable Discord
Developer Mode under User Settings → Advanced, then right-click the server,
parent text channel, and your profile to copy their respective IDs. Obtain the
bot token from the application's Bot page in the Discord Developer Portal.

The launcher parses `.env` without executing shell commands or exporting its
token to OMP. It rejects group/world-readable environment files and writes
generated configuration and a mode-600 token file beneath
`~/.config/omp-discord-bridge/local-env/`. Edit `.env`, not these generated files.
Use `--env-file /absolute/path/to/.env` when launching from another directory.
Blank required fields fail before starting the bridge. `OMP_MODEL` may remain
blank to retain OMP's configured default.

`check` performs a real OMP startup probe but does not authenticate to Discord.
Use `run` and mention the bot in the allowed channel to verify Discord access.
The working-directory allowlist is not a sandbox; OMP retains your user's access
and the project's approval policy. The launcher stays in the foreground.

### Direct TOML setup

```bash
mkdir -p ~/.config/omp-discord-bridge
cp config.example.toml ~/.config/omp-discord-bridge/config.toml
```

Edit every guild, channel, user, and path ID. Access lists may not be empty: configuration is default-deny. Each guild working directory must already exist and resolve beneath `omp.allowed_root`.

The Discord history tool accepts a per-call `days` value up to `runtime.discord_history_max_days`. A request fails rather than returning partial history if it exceeds `runtime.discord_history_max_messages`, `runtime.discord_history_max_bytes`, or `runtime.discord_history_timeout_seconds`.

Store the Discord token in the dedicated file referenced by `discord.token_file`. Enter it interactively so it does not appear in shell history:

```bash
umask 077
cat > ~/.config/omp-discord-bridge/discord-token
```

Paste the token, press Enter, then `Ctrl-D`. Enforce the required permissions:

```bash
chmod 600 ~/.config/omp-discord-bridge/discord-token
```

The bridge rejects empty token files, files larger than 4096 bytes, tokens containing whitespace, and—on Unix—files that grant group or other access. Keep the token file outside the repository; never put the token itself in TOML or a command-line argument.

## Validate and run
The bridge launches the configured OMP executable and uses its normal authentication sources, including `~/.omp/agent/agent.db`, OMP `.env` files, and environment variables inherited from the bridge process. Leave `omp.model` unset to use OMP's configured default. If set, use an exact provider/model ID shown by `omp models`; `openai-codex` subscription authentication is separate from the API-key-based `openai` provider.


The check command validates configuration and SQLite access, loads the protected token file, and performs a real OMP RPC startup/clean-shutdown probe without sending a model prompt:

```bash
cargo run -- --config ~/.config/omp-discord-bridge/config.toml check
```

Start the gateway:

```bash
RUST_LOG=info cargo run --release -- \
  --config ~/.config/omp-discord-bridge/config.toml run
```

`run` is the default subcommand and may be omitted. Use a service manager for unattended operation.

### Boot service on this workstation

`omp-discord-bridge.service` is configured for user `hephaestus` and this checkout.
Its installed copy is `/etc/systemd/system/omp-discord-bridge.service`, enabled
for boot without requiring a login. It runs as the regular user, reads
`~/.config/omp-discord-bridge/config.toml`, and restarts after failures.
It uses the release binary in this checkout; keep that path in place.
The service does not read the repository's `.env` launcher configuration.

```bash
systemctl status omp-discord-bridge
journalctl -u omp-discord-bridge -f
sudo systemctl restart omp-discord-bridge
sudo systemctl disable --now omp-discord-bridge
```

After editing TOML or rebuilding the binary, restart the service. After editing
the unit, reinstall it with `sudo install -m 644 omp-discord-bridge.service
/etc/systemd/system/omp-discord-bridge.service`, run `sudo systemctl daemon-reload`,
then restart. Do not launch a second bridge manually against the same database.

When this service carries your current conversation, build a candidate separately
so the running release stays untouched:

```bash
cargo build --locked --release --bins --target-dir target/activity-release
```

Wait for all activity messages to finalize and for all active turns to finish before
deploying from a local terminal. A streamed final answer can arrive before its turn
has settled. Stopping the service during a turn interrupts its OMP process;
recovery marks that turn uncertain rather than replaying it.

```bash
sudo systemctl stop omp-discord-bridge
install -m 755 target/activity-release/release/omp-discord-bridge target/release/
install -m 755 target/activity-release/release/bridge-local target/release/
sudo systemctl start omp-discord-bridge
```

### Scheduled improvement reviews

The separate `omp-session-review.timer` runs a proposal-only session review every
eight hours, without connecting to Discord or restarting the bridge. Empty windows
exit without a model call. Reports and suggested instruction/documentation changes
remain local under `~/docs/session-reviews/`; nothing is applied automatically.
See [scheduled session review](docs/session-review.md) for installation, privacy
limits, coverage, and operating commands.

## Docker

`Dockerfile` builds the bridge with Rust 1.99.1 and installs the checksum-verified OMP 18.20.0 binary. `compose.yaml` runs it as a non-root host UID/GID with bind-mounted configuration, durable data, and workspace directories:

```bash
export HOST_UID="$(id -u)" HOST_GID="$(id -g)"
mkdir -p container/config container/data
cp config.container.example.toml container/config/config.toml
# Add container/config/discord-token with mode 600, then edit config.toml.
docker compose build bridge
docker compose run --rm bridge check
docker compose up -d bridge
```

The default mounts are `./container/config:/config:ro`, `./container/data:/data`, and the repository root at `/workspace`. Set `BRIDGE_CONFIG_DIR`, `BRIDGE_DATA_DIR`, and `OMP_WORKSPACE_DIR` to use other local paths. OMP authentication persists below `/data/.omp`. See [`docs/docker.md`](docs/docker.md) for permissions, authentication, validation, backup, and upgrade procedures.

## Discord workflow

1. In an allowed parent channel, mention the bot and include a prompt.
2. The bot creates a thread and starts the prompt when a concurrency slot is available.
3. Continue sending messages in that thread without mentioning the bot. A newer message immediately requests interruption with OMP's native `abort_and_prompt`, then runs in the same live process and conversation.
4. When the OMP project configuration requires approval for a write or executable action, an authorized user chooses **Approve once** or **Deny**. Requests expire after five minutes; interruption also expires the old request immediately.

Interruption is cooperative: a running tool may need time to stop, and completed
file changes or external actions are not rolled back. Rapid messages are retained
in admission order, not combined or silently replaced. The bridge allows one active
prompt plus one pending interrupt command; further messages are dispatched as soon
as an earlier prompt resolves, without waiting for natural turn completion.
There is a hard limit of 64 queued/running messages per thread; excess messages are
explicitly rejected, while duplicate Discord deliveries remain harmless.

Each completed assistant message is sent immediately, including commentary before
tool calls, rather than waiting for the whole turn to finish. This is message-level
delivery, not token-by-token editing. Long messages are split for Discord.
A bounded, ordered delivery worker keeps Discord requests off the RPC loop so
they do not block interruption. Failed or ambiguous sends are not automatically
retried; a failed message does not suppress later independent messages.

Already-posted messages remain visible if their turn is interrupted. Unfinished
assistant text stays in SQLite rather than being posted as a finished answer.
Full turn output is retained in SQLite but is not sent again at completion.
Interruption notices share the delivery queue with assistant messages. The live
activity message remains visible throughout. Messages arriving during final
delivery are picked up by the next run.
The `interrupted_by_turn_id` database column records which message requested each
interruption; the column is added automatically when opening an existing database.

The bridge does not override OMP's approval mode, remove tools, or sandbox an approved call. OMP loads the project configuration from the configured working directory, including `tools.approvalMode`. When that policy asks for approval, the bridge translates OMP's one-shot request into Discord buttons. Persistent session-wide approval is intentionally unavailable through Discord.

OMP can call `discord_channel_history` to read messages from the allowed parent channel that created the session. The tool accepts `days`, returns chronological JSON containing message text, author metadata, embeds, and attachment metadata, and never exposes the Discord token. Discord history is untrusted input. Requests outside the configured day window or resource limits fail explicitly; history is never silently truncated.

Thread controls must be the complete message:

- `status` — show session, working directory, queue, uncertain-turn state, and OpenAI/Codex provider quota usage with reset times;
- `stop` — stop the thread and cancel its active turn;
- `new` — after the active turn stops, discard queued turns and start a fresh OMP session.

`status` queries the configured OMP executable's `usage --json --provider openai-codex`
interface without interrupting the conversation or making a model call. It shows
weekly and any other reported quota windows for each authenticated Codex account,
not this thread's token spending. Accounts are numbered without exposing emails,
account IDs, or organization metadata. Discord renders reset timestamps in your
local timezone, with a relative countdown and the snapshot's reported update age.
Missing usage/reset data is explicitly unavailable, never estimated as zero.
The query has a 20-second deadline and a 1 MiB output limit; failures leave the
ordinary session/queue status available. Provider caching may affect freshness.

An unexpected process or protocol loss after prompt dispatch is recorded as uncertain, even if its acknowledgement was lost. Neither the original prompt nor a replacement with an unresolved outcome is automatically replayed. Inspect the working tree and OMP session before manually retrying it.

## Development checks

```bash
cargo fmt -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --locked
cargo run -- --version
cargo run -- --help
```

`Cargo.lock` belongs in version control because this repository builds an application. Runtime secrets, SQLite state, logs, and Rust build output are ignored by Git.

See `AGENTS.md` for engineering invariants and `docs/omp-rpc.md` for the transport contract.
