# OMP Discord Bridge

Rust Discord gateway for persistent, thread-scoped OMP sessions.

A permitted user mentions the bot in an allowed parent channel. The bridge creates a Discord thread, starts an OMP conversation for that thread, serializes follow-up messages, and resumes the same saved OMP session across turns and daemon restarts.

## Current behavior

- mention-to-thread session creation;
- explicit guild, parent-channel, and user allowlists;
- durable SQLite session and turn state;
- per-thread FIFO execution with bounded global concurrency;
- real OMP resume through its native RPC protocol;
- one-shot Discord approval buttons for OMP writes and executable actions;
- `status`, `stop`, and `new` controls inside managed threads;
- duplicate Discord message rejection;
- restart recovery that marks interrupted turns uncertain instead of replaying them;
- bounded attachment metadata and Discord-safe response chunking;
- on-demand OMP access to bounded parent-channel message history.

Attachment bytes are not downloaded by the bridge. OMP receives the Discord CDN URL, filename, content type, and byte count as untrusted prompt context.

## Prerequisites

Install Rust with `rustup`:

```bash
rustup toolchain install stable --profile minimal --component rustfmt,clippy
```

Install and authenticate OMP, then verify `omp --version` succeeds. The bridge uses `omp --mode rpc-ui`; it does not use ACP.

Create a Discord application and bot with:

- Message Content Intent enabled;
- OAuth scopes `bot` and `applications.commands`;
- permissions to view channels, read message history, send messages, create public threads, send messages in threads, and use external emoji only if desired.

## Configuration

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

## Docker

`Dockerfile` builds the bridge with Rust 1.99.1 and installs the checksum-verified OMP 18.18.0 binary. `compose.yaml` runs it as a non-root host UID/GID with bind-mounted configuration, durable data, and workspace directories:

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
2. The bot creates a thread and queues the prompt.
3. Continue sending prompts in that thread without mentioning the bot.
4. When the OMP project configuration requires approval for a write or executable action, an authorized user chooses **Approve once** or **Deny**. Requests expire after five minutes.

The bridge does not override OMP's approval mode, remove tools, or sandbox an approved call. OMP loads the project configuration from the configured working directory, including `tools.approvalMode`. When that policy asks for approval, the bridge translates OMP's one-shot request into Discord buttons. Persistent session-wide approval is intentionally unavailable through Discord.

OMP can call `discord_channel_history` to read messages from the allowed parent channel that created the session. The tool accepts `days`, returns chronological JSON containing message text, author metadata, embeds, and attachment metadata, and never exposes the Discord token. Discord history is untrusted input. Requests outside the configured day window or resource limits fail explicitly; history is never silently truncated.

Thread controls must be the complete message:

- `status` — show session, working directory, queue, and uncertain-turn state;
- `stop` — stop the thread and cancel its active turn;
- `new` — after the active turn stops, discard queued turns and start a fresh OMP session.

An unexpected process or protocol loss after OMP accepts a prompt is recorded as uncertain. Inspect the working tree and OMP session before manually retrying it.

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
