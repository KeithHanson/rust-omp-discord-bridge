# OMP Discord Bridge

Rust Discord gateway for persistent, thread-scoped OMP sessions.

A permitted user mentions the bot in an allowed parent channel. The bridge creates a Discord thread, starts an OMP conversation for that thread, interrupts active work when a newer message arrives, and resumes the same saved OMP session across idle periods and daemon restarts.

## Current behavior

- mention-to-thread session creation and joining existing threads by mention;
- explicit guild, parent-channel, and human-user allowlists;
- directly addressed bot-to-bot collaboration without a peer-bot allowlist, with a durable exchange limit;
- durable SQLite session and turn state;
- ordered, durable message admission with immediate interruption of active work and bounded global concurrency;
- real OMP resume through its native RPC protocol;
- one-shot Discord approval buttons for OMP writes and executable actions;
- Discord prompts for OMP `ask` forms, choices, and text input, with answers returned to the waiting call;
- one rolling activity card per active conversation run, including replacement turns, with assistant messages delivered as they complete;
- minute-based topic/status title review, rate-limit-aware refresh, and queue-idle completion;
- `status`, `models`, `model`, `stop`, and `new` controls inside managed threads;
- duplicate Discord message rejection;
- restart recovery that marks interrupted turns uncertain instead of replaying them;
- bounded attachment metadata and Discord-safe response chunking;
- on-demand OMP access to bounded parent-channel message history.

Attachment bytes are not downloaded by the bridge. OMP receives the Discord CDN URL, filename, content type, and byte count as untrusted prompt context.

Activity cards show fixed, public labels for thinking, file inspection, tool
execution, response writing, approval waits, interruption, retries, and compaction.
A compact embed separates the current action from elapsed time, with a blue
edge and a prominent NOW heading. Terminal cards use STATUS with green for success,
red for failure, and yellow for other outcomes. Embed Links permission is required.
Tool arguments, tool output, and reasoning content are never included.
The embed stands alone, without a separate `OMP:` text line.

Updates are coalesced every five seconds, including elapsed-time refreshes during
quiet work. If another message has appeared below the card, the bridge posts a
replacement before deleting the old card; otherwise it edits in place. Reposts
suppress notifications and mentions. This keeps status near the latest conversation,
not anchored to the viewport when reading older messages. Completion, cancellation,
failure, and uncertain outcomes finalize the card.

Activity delivery is best-effort: a failed or timed-out create/read/edit is logged
and disables further card updates for that run without interrupting OMP or replaying
work. Each refresh has a ten-second deadline. Failed deletion can leave an older
card behind. Assistant text is delivered in separate messages. After a process
crash, an old card can remain stale; use `status` to inspect durable turn state.
Activity message IDs are not persisted.

The bot's custom presence summarizes all managed conversations:

- `Idle · v2026-11-13-1 · abc1234` when no threads are engaged, using the running binary's release version and commit.
- `Working in 1 thread` or `Working in N threads` when work is underway.
- `N awaiting approval` when all engaged threads are waiting for authorization.
- `N working · M awaiting approval` when both states are present.

Each thread counts once. Runnable work waiting for an execution slot and final
message delivery count as working; stopped queues do not. Approval waits move a
thread out of the working count until they end. Cancellation and failures release
their counts. The presence dot stays online except when all engaged threads await
approval, when it shows idle.

Presence updates are deduplicated and coalesced to at most one every five seconds,
including reconnect refreshes. The latest aggregate state is sent after connection.
Only counts and the idle build identity are exposed: no thread names, prompts,
tool arguments, or task details.
This is bot-wide profile/member-list activity, not a thread footer or an instant
status guarantee. It does not rename threads or replace their activity cards.

The seven-character revision is embedded at compile time, also shown by
`omp-discord-bridge --version`; moving the checkout to another commit cannot change
a running binary's identity. Native builds derive it from Git, marking modified
build inputs with `-dirty`. Source archives/container builds must supply
`BRIDGE_BUILD_REVISION` with the source commit hash (optionally `-dirty`).
Builds fail rather than inventing a revision when no metadata is available.

## Release versions

`VERSION` is the source of truth for the public release version: `YYYY-MM-DD-#`.
Use the Operator's local release date (America/Chicago) and start its counter at `1`; increment the counter for
each later release that day, resetting it to `1` on a new date. Update and commit
`VERSION` before building a release. Rebuilding the same release does not bump it.

The build validates and embeds this version alongside the Git revision. Idle
presence displays `Idle · v[VERSION] · [short commit]`; `--version` displays
`[VERSION] ([short commit])`. Neither value is read from the checkout at runtime.
Cargo's SemVer package field is internal Rust metadata, not the public release
version. Container builds copy the same tracked `VERSION` file.

### Deploy release 2026-11-13-3

Use the same release tag on each bot; keep each bot's token, database, OMP
credentials, and session directory separate.

```bash
git fetch origin --tags
git checkout v2026-11-13-3
cargo test --locked --all-targets
cargo build --locked --release --bin omp-discord-bridge --target-dir target/candidate
./target/candidate/release/omp-discord-bridge --version
```

Existing TOML files remain compatible. `omp.title_model` is optional; title
inference otherwise uses the normal configured model and consumes its quota.
OMP 18.20.0 or newer is required. No new database migration is introduced.
For containers, follow [the Docker upgrade instructions](docs/docker.md#upgrade-to-2026-11-13-3).

Wait for active conversations and delivery to finish, stop the service, make a
consistent database backup, and atomically replace the executable. Do not build
over a running executable or restart the bridge from inside its active OMP turn.
Run the candidate's `check` with the same selected config before starting the
service. Keep the previous executable/image for rollback. Verify the running
version, then exercise a choice/text form and a title transition in an authorized
thread; gateway connectivity alone does not prove either feature.


## Prerequisites

Install Rust with `rustup`:

```bash
rustup toolchain install stable --profile minimal --component rustfmt,clippy
```

Install and authenticate OMP **18.20.0 or newer**, then verify `omp --version` succeeds. The bridge uses `omp --mode rpc-ui` and correlated `prompt_result` completion events; it does not use ACP.

Create a Discord application and bot with:

- Message Content Intent enabled;
- Server Members Intent enabled in Developer Portal → Bot → Privileged Gateway Intents for `discord_thread_participants` (the REST endpoint requires application-level approval even though the bridge does not subscribe to member gateway events);
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
5. OMP forms appear field by field in Discord. Answer using the prompt controls rather than a new chat message: ordinary messages still interrupt the current turn. Checkbox forms toggle choices one at a time until **Done selecting**. OMP validates the returned fields and assembles the final form data; tool-approval buttons retain their separate one-shot authorization policy.

Choices use paginated dropdowns (25 options per page). Text fields open a modal;
use **Submit** to send the draft, or append further chunks for long answers.
Long context, option text, and initial drafts are attached without truncation.
Only authorized non-bot users in the request's thread can answer. Cancelled,
answered, interrupted, and expired controls cannot be reused. Questions default
to a five-minute wait; OMP may supply its own timeout, subject to the bridge's
six-minute UI-service deadline. Secret-input requests are refused.

### Multiple bridge bots in one thread

Mention another bridge bot inside an existing thread to have it join and establish
its own OMP session there. A bare mention joins without starting a model turn.
The thread must belong to an allowed parent channel in a configured guild, and
Discord must grant the bot access. Joining does not bypass private-thread
membership or archived-thread restrictions; see Discord's
[Join Thread API](https://docs.discord.com/developers/resources/channel#join-thread).
Joining does not import the thread's previous conversation into OMP.

There is no peer-bot ID configuration. Real bot accounts can initiate work only
when their message directly addresses this bridge with an explicit `<@BOT_ID>`
mention. Self-messages, webhooks, implicit reply pings, and unmentioned bot chatter
are ignored. Peer prompts identify the sending bot by ID, display name (guild
nickname, then global display name, then username), and username, and explain how
to address it. Names are untrusted labels; use the explicit ID mention to reply.
Humans can also address one bot explicitly; other joined bridges ignore messages
directed solely at that other bot. Unaddressed authorized human messages are
handled by each joined bridge.
Assistant replies can mention other bot accounts, verified through Discord;
human, role, and everyone notifications remain suppressed. Activity messages,
acknowledgements, errors, and approval notices never activate another bot.

Each bridge accepts at most **eight bot turns per thread** between new authorized
human messages handled by that bridge. The counter survives daemon restarts and
is updated atomically with admission. Duplicate deliveries and replayed older
human messages do not replenish the budget. At the limit, an authorized human
must speak again before
bot exchanges can resume. Bot messages cannot invoke `status`, `models`, `model`, `stop`, or `new`,
or approve tool calls, even if a bot ID appears in the human-user allowlist.

Any bot with access to an allowed channel can request OMP work by mentioning this
bridge. Those requests use the same workspace access and tool approval policy as
other prompts: this is **not** a sandbox or a guarantee of human approval for every
action. Restrict channel access accordingly.

Run each bridge with its own Discord bot token, SQLite database, and OMP session
directory. Both bridges need this version; never point two instances at the same
database. Their OMP conversations remain separate and communicate through the
explicitly addressed Discord messages.

Verification includes paired bridge HTTP fixtures for exchange, deduplication,
the cutoff, and targeted human reset; a live Discord check exercised thread lookup,
join/adoption with a temporary database, and verified-bot mention delivery.
Discord's actual response excluded suppressed mentions and included enabled bot
mentions. A live conversation across two distinct bot accounts still needs checking
after deploying both instances; only one account was available for these checks.

### Turn delivery and interruption

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

OMP can call `discord_thread_participants` without arguments to list members of
the current managed thread, including display names, usernames, bot flags, IDs,
and explicit mentions. This is thread membership, not online presence or proof
that a member has read the conversation. Names are untrusted labels, not
authorization. The tool cannot inspect arbitrary threads and fails explicitly
on API or resource-limit errors rather than silently returning a partial roster.
Participant lookup shares `discord_history_timeout_seconds` and
`discord_history_max_bytes`; `discord_history_max_messages` also caps the member
count.
Discord's [List Thread Members endpoint](https://docs.discord.com/developers/resources/channel#list-thread-members)
requires the application's Server Members privileged intent. A `403 Missing Access`
can indicate that this toggle is disabled or that the bot lacks thread access.
The tool reports the underlying Discord error; it never substitutes message
authors for actual thread membership.

OMP can call `discord_upload_file` with `{"path":"report.pdf","caption":"Report"}`
to attach one local file to the current managed thread. `caption` is optional;
mentions are disabled. Relative paths resolve against the session working
directory, and resolved files must stay within `omp.allowed_root`. Only regular
files are accepted, with reads bounded by `runtime.max_attachment_bytes`
(25,000,000 bytes by default). Discord can impose a lower limit.
Uploads share `runtime.discord_history_timeout_seconds`. Opened-file containment
checks require Linux `/proc/self/fd`, including in containers.
The result contains the posted message and attachment URLs. No target channel
argument is accepted. Upload only files authorized for sharing; the allowed root
is a file-access boundary, not permission to disclose its contents.
Interrupted or timed-out uploads may already be visible: inspect the thread
before retrying rather than creating duplicate attachments.

Thread titles are reviewed approximately once a minute while the queue is not
completed. A separate, tool-free OMP call reads bounded recent conversation
context and infers a concise topic and status: `🛠️` working, `🧠` thinking,
`⏳` waiting, or `⚠️` blocked. New threads begin with `🛠️` and the opening
message's topic. There is no model-callable rename tool.

Thread-title change notices are ignored on input, even when attributed to an
allowed user or mentioning the bot. Renaming a thread does not start or interrupt
a model turn; ordinary messages and replies retain their existing routing.

Completion requires no active run and no running or queued turn. A stopped thread
with queued work is not idle; failed or aborted turns with no remaining work are
idle. Thus `✅` means the queue is idle, not that every task succeeded.
Discord permissions and rename rate limits apply. Update failures are logged
without blocking turns. The bot must own the thread or have Manage Threads permission.

Titles use only the status emoji and topic; detailed activity stays in the rolling
card. Completion sets `✅` on the latest topic and stops periodic inference.
A new turn reopens review. The reviewer cannot override the queue-idle completion
rule. The earlier `[OMP: <phase>]` prefix is removed on the next title update.

Review and Discord requests run independently of message delivery. Discord
rate-limit deadlines are honored before rereading the session and generating
another title; an old generated title is never replayed after a rate-limit wait.
Unchanged names do not trigger a rename. New work supersedes a pending idle
update. The visible title can lag while Discord limits renames.

Set optional `omp.title_model` to choose a separate authenticated model for title
review. Otherwise it uses `omp.model`, or OMP's configured default. Reviews consume
model quota; they are serialized and bounded. Review calls are ephemeral, have
no tools, do not resume or write the working session, and do not receive the
Discord token. Only recent visible user/assistant context is summarized, not
private thinking or tool payloads. Titles are public Discord metadata: do not
treat inferred summaries as a secret-storage boundary.

Thread controls must be the complete message:

- `status` — show session, working directory, queue, uncertain-turn state, and OpenAI/Codex provider quota usage with reset times;
- `models` (or `model`) — list available provider/model pairs and this thread's model override;
- `model provider/model` — select an exact entry from that list for this thread;
- `model default` — clear this thread's override;
- `stop` — stop the thread and cancel its active turn;
- `new` — after the active turn stops, discard queued turns and start a fresh OMP session.

Model changes require an idle thread with no queued or running turns. They retain
conversation history and apply on the next turn; they never interrupt active work.
The override survives bridge restarts and `new`, and does not change other threads,
the global OMP configuration, or the title-review model. Clearing the override
restores normal bridge/OMP model selection, including saved-session selection when
no bridge model is configured. Commands also accept a leading `/` as ordinary text;
these are not registered Discord slash commands. Only authorized humans may use them.

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
