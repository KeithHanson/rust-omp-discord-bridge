# OMP Discord Bridge

Rust Discord gateway for persistent, thread-scoped OMP sessions.

A permitted user mentions the bot in an allowed parent channel. The bridge creates a Discord thread, starts an OMP conversation for that thread, interrupts active work when a newer message arrives, and resumes the same saved OMP session across idle periods and daemon restarts.

## Private voice-to-thread trial

Build the optional companion with `cargo build --locked --features local-voice
--bin voice-watch`. The normal bridge binary has no audio dependency unless that
feature is selected. This is an opt-in local experiment, not a published release.

The bridge accepts companion input only when `OMP_DISCORD_BRIDGE_VOICE_SOCKET`
names a Unix socket in an existing, owner-only directory (mode 0700). The socket
is mode 0600 and accepts only same-user peers. Do not expose it over TCP. The
configured guild, parent-channel and human allowlists and thread exclusions still
apply. A companion cannot create a session through this interface.

Start the bridge with that environment setting, then the companion:

```sh
voice-watch --token-file /path/to/discord-token \
  --bot-id BOT_ID --guild-id GUILD_ID --channel-id GENERAL_VOICE_ID \
  --user-id OPERATOR_ID \
  --stt-url http://PRIVATE_ASR_HOST:18086 --stt-key-file /path/to/asr-key \
  --bridge-socket /private/directory/voice.sock
```

Use `/voice on` in an existing bridge thread, then speak in the configured General
voice channel. The companion registers this guild-only slash command without
replacing other commands. Only the configured human can operate it. Confirmation
identifies the destination; enabling another thread replaces the previous binding.
`/voice off` or leaving General disarms it. Restarting the companion also disarms.
Already submitted turns are not canceled by turning voice off; use the existing
`stop` control for an active agent turn.

Recognized speech is visibly attributed as a transcription, then admitted as a
human turn in the thread's existing OMP session. Replies remain text-only. Voice
transcripts are prompts, not bridge control commands. Transcripts remain in
Discord and session history; microphone audio is memory-only. There is no TTS.
An acknowledgement failure is not retried because admission may already have
occurred; check the thread before repeating the utterance.

Capture retains only the configured human's attributed audio, decoded at stereo
16 kHz. Earshot VAD uses mono 16 ms frames, 640 ms pre-roll, 800 ms trailing
non-speech and a 160 ms minimum speech duration. Utterances cap at 20 seconds with
640 ms overlap; two completed utterances may wait behind a request. Backlog
overflow drops new chunks with a warning. Quiet/brief speech can be rejected and
overlap can repeat words. Unknown-SSRC audio remains excluded. Model and VAD
quality still require live microphone testing.

`scripts/voice-asr.Dockerfile` adds pinned audio decoders to the vLLM image.
Build it as `hephaestus/qwen3-asr:trial` on the authorized GPU host;
`scripts/run-local-asr.sh` launches pinned Whisper large-v3-turbo as `voice-asr`.
The image/container/cache names retain the original Qwen trial names. The launcher
requires a mode-0600 `~/hephaestus-qwen3-asr/api.env` containing `VLLM_API_KEY`.
It binds port 18086 on all interfaces: restrict firewall access to approved
LAN/ZeroTier ranges. Its 4% GPU setting is not a hard VRAM isolation boundary.
Stop the previous ASR container before switching models on the shared GPU.
Plain LAN HTTP is unencrypted; the companion uses the private overlay endpoint.

Omitting `--bridge-socket` retains the standalone log-only transcription trial;
it does not deliver agent turns. Keep automatic binary updates disabled during
private deployment (`OMP_DISCORD_BRIDGE_AUTO_UPDATE=false`). Activate a changed
bridge only through the normal idle-safe handoff; never restart it from an active
agent conversation.

## Current behavior

- mention-to-thread session creation and joining existing threads by mention;
- explicit guild, parent-channel, and human-user allowlists;
- directly addressed bot-to-bot collaboration without a peer-bot allowlist, with a durable exchange limit;
- durable SQLite session and turn state;
- ordered, durable message admission with immediate interruption of active work and bounded global concurrency;
- real OMP resume through its native RPC protocol;
- one-shot Discord approval buttons for OMP writes and executable actions;
- Discord prompts for OMP `ask` forms, choices, and text input, with answers returned to the waiting call;
- one stationary activity card per active conversation run, including replacement turns, with assistant messages delivered as they complete;
- minute-based topic/status title review, rate-limit-aware refresh, and queue-idle completion;
- `status`, `models`, `model`, `stop`, `new`, and targeted `@Bot kick` controls inside threads;
- duplicate Discord message rejection;
- restart recovery that marks interrupted turns uncertain instead of replaying them;
- bounded attachment metadata and Discord-safe response chunking;
- on-demand OMP access to bounded parent-channel and same-parent thread message history.

Attachment bytes are not downloaded by the bridge. OMP receives the Discord CDN URL, filename, content type, and byte count as untrusted prompt context.

Activity cards show fixed, public labels for thinking, file inspection, tool
execution, response writing, approval waits, interruption, retries, and compaction.
A compact embed separates the current action from elapsed time, with a blue
edge and a prominent NOW heading. Terminal cards use STATUS with green for success,
red for failure, and yellow for other outcomes. Embed Links permission is required.
Tool arguments, tool output, and reasoning content are never included.
The embed stands alone, without a separate `OMP:` text line.

Updates are coalesced every five seconds, including elapsed-time refreshes during
quiet work. The bridge edits the original card in place, even when newer messages
appear below it. It never relocates the card to the bottom of the conversation.
Cards suppress notifications and mentions and can scroll out of view.
Completion, cancellation, failure, and uncertain outcomes finalize the same card.

Activity delivery is best-effort: a failed or timed-out create/edit is logged
and disables further card updates for that run without interrupting OMP or replaying
work. Each refresh has a ten-second deadline.
Assistant text is delivered in separate messages. After a process
crash, an old card can remain stale; use `status` to inspect durable turn state.
Activity message IDs are not persisted.

### Automatic table images

Assistant Markdown tables are rendered to dark-theme PNG attachments before
Discord message chunking. Surrounding prose stays in order; table hyperlinks
are posted separately as clickable URLs. Fenced Markdown examples stay text.
Wide tables repeat the first column across images, and long tables repeat their
headers. Images suppress mentions and obey the same thread-exclusion gate as
text delivery. The original Markdown remains in the saved conversation.

The bridge embeds `scripts/markdown-table-image.py` and requires `uv`, system
`chromium`, and `/usr/bin/kill` on the Linux host. The first table render may
download the pinned Python dependencies into uv's cache. Grant the bot Attach
Files permission. The renderer disables raw HTML, JavaScript, and network
resource loading. Rendering failures produce an explicit notice rather than
silently substituting an unrendered table; failed HTTP sends are not retried.

For standalone use:
`uv run --script scripts/markdown-table-image.py input.md output.png --title 'Comparison'`.
`--manifest` emits an ordered JSON list of text and image paths for integration.
No Discord token or message content is sent to an external rendering service.


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

### Release 2026-11-16-2

Adds bridge-backed Threadwatch chat control. The integrated reporter polls
operator commands, admits them through the existing durable per-thread turn
queue, acknowledges acceptance or rejection, and deduplicates retries.

### Release 2026-11-16-1

Moves Threadwatch collection into the bridge process with a durable queue,
checkpoint compatibility, heartbeats, Discord metadata lookup, and reporting
failure isolation. Existing standalone collector state directories and bot IDs
remain the migration boundary; stop the standalone collector before enabling
integrated reporting.

### Release 2026-11-14-4

Adds persistent per-thread provider/model selection through `models`,
`model provider/model`, and `model default`. Changes require an idle thread,
preserve conversation history, and leave other threads and title review unchanged.
Includes the table images, automatic updates, and targeted bot kicks from earlier releases.

### Release 2026-11-14-3

Adds automatic Markdown-table PNG delivery and verified GitHub binary updates
every seven minutes. Updates wait for idle turns, delivery, and title workers.
Table rendering requires `uv`, system Chromium, and Attach Files permission;
the Python renderer is embedded in the binary.

The first installation of this release needs a controlled idle-time replacement.
Subsequent clean release builds update themselves unless explicitly disabled.
Keep the previous executable for rollback; gateway reconnection is not message
backfill, and post-startup failures still require manual rollback.

### Build and publish a GitHub release

Run `scripts/release.sh` on **Linux x86-64** with Rust installed through rustup,
`musl-gcc`, GNU coreutils/tar/gzip, binutils (`readelf`), and Git. On Arch, install
`musl binutils github-cli`; on Debian/Ubuntu, install `musl-tools binutils gh`.
Publishing additionally requires GitHub CLI authentication with release-write
permission (`gh auth login --hostname github.com`).

Commit all release changes, including `VERSION`, and tag that exact commit:

```bash
rustup target add x86_64-unknown-linux-musl
version=$(cat VERSION)
git tag -a "v$version" -m "v$version"
scripts/release.sh --dry-run
git push origin HEAD "refs/tags/v$version"
scripts/release.sh
```

The checkout must be clean, including untracked files; the local tag must point
at `HEAD`. Publishing also checks that GitHub's tag resolves to the same commit.
`--dry-run` performs the local build, static-link/version checks, and packaging
without contacting GitHub. `--help` prints usage without building. Builds use a
snapshot of the committed tree, locked Cargo dependencies, and static musl linking;
neither local secrets nor ignored source files enter the package.
The final link uses the selected Rust toolchain's bundled `rust-lld`; `musl-gcc`
compiles C dependencies. This avoids host linker specs injecting an ELF interpreter
into an otherwise static executable.

Artifacts are retained in a fresh `target/releases/vVERSION.XXXXXX/` directory:

- `omp-discord-bridge-VERSION-linux-x86_64.tar.gz`: a versioned directory with
  `omp-discord-bridge`, `bridge-local`, `.env.example`, example TOML configs,
  `omp-discord-bridge.service`, README, and license.
- `omp-discord-bridge-linux-x86_64`: the raw static executable used for updates.
- `SHA256SUMS`: SHA-256 checksums of both assets, using those exact basenames.

The fixed repository is `KeithHanson/rust-omp-discord-bridge` and the tag is
`vVERSION`. The script creates a **draft**, uploads all three assets without
overwriting, and only then publishes it as the latest release. Any existing
release, including a draft, is refused. Failed uploads leave the draft for manual
inspection; remove an abandoned draft only after checking its state, then retry.
Never overwrite or retag a published release: commit a new `VERSION` instead.
No service is installed or restarted by the release script.

### Automatic binary updates

Clean Linux x86-64 release builds check the latest GitHub release in
`KeithHanson/rust-omp-discord-bridge` every **420 seconds (7 minutes)** by default.
Dirty builds never enable updates. Clean debug builds require
`OMP_DISCORD_BRIDGE_AUTO_UPDATE=true`. Set
`OMP_DISCORD_BRIDGE_AUTO_UPDATE=false` to opt out of release-build updates.
The optional dedicated `OMP_DISCORD_BRIDGE_UPDATE_TOKEN` authenticates GitHub requests; it is not the
Discord bot token.

The updater requires a `vYYYY-MM-DD-N` tag, the raw
`omp-discord-bridge-linux-x86_64` asset, and its matching entry in `SHA256SUMS`.
It verifies the checksum and candidate's `--version`, then runs a bounded
`check-update` preflight to validate the selected configuration, token, and OMP
probe **without migrating or otherwise modifying the live database**. Gateway
admission remains open throughout preflight. The updater then waits until turns,
message/activity deliveries, title OMP workers, and admitted handlers are all
idle. Continuous activity can defer an update indefinitely; updates never
interrupt a turn to meet a deadline.

New work admission is held exclusively only across the final idle recheck and
synchronous atomic executable replacement/`exec`. The replacement runs with the
same arguments, environment, and PID.
The previous binary is retained with a `.previous` suffix and restored if `exec`
fails. A later application/startup failure does **not** trigger automatic rollback;
stop the service and restore the previous executable manually in that case.

The executable's parent directory must be writable by the service account.
Containers should opt out and use image rollouts instead. Rollover briefly
disconnects the Discord gateway; messages during that gap may be missed because
the bridge does not backfill them. This is safe work draining, not guaranteed
zero-downtime message ingestion.

### Deploy release 2026-11-14-2

Use the same release tag on each bot; keep each bot's token, database, OMP
credentials, and session directory separate.

```bash
git fetch origin --tags
git checkout v2026-11-14-2
cargo test --locked --all-targets
cargo build --locked --release --bin omp-discord-bridge --target-dir target/candidate
./target/candidate/release/omp-discord-bridge --version
```

Existing TOML files remain compatible. OMP 18.20.0 or newer is required.
This release adds a persistent `excluded_threads` SQLite table. Keep the database
when upgrading: it contains the bot's thread exclusions as well as session history.
For containers, follow [the Docker upgrade instructions](docs/docker.md#upgrade-to-2026-11-14-2).

The GitHub release also includes a static **Linux x86-64** bundle containing
`omp-discord-bridge`, `bridge-local`, example configs, and the systemd unit.
OMP must still be installed and authenticated separately. Other platforms should
build from the tag above; this bundle is not an ARM or macOS executable.

```bash
gh release download v2026-11-14-2 --repo KeithHanson/rust-omp-discord-bridge \
  --pattern 'omp-discord-bridge-2026-11-14-2-linux-x86_64.tar.gz' --pattern SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf omp-discord-bridge-2026-11-14-2-linux-x86_64.tar.gz
cd omp-discord-bridge-2026-11-14-2-linux-x86_64
./omp-discord-bridge --version
```

For first installation, follow the configuration section below and adapt the
included service unit to your user, executable, configuration, and OMP paths.
For upgrades, wait for active conversations and delivery to finish, stop the
service, make a consistent database backup, and atomically replace the executable.
Do not overwrite a running executable or restart from inside an active OMP turn.
Run the candidate's `check` with the same selected config before starting the
service. Keep the previous executable/image for rollback, but older versions do
not enforce kicks; restoring an old binary weakens the exclusion policy.
Never restore an old database over newly admitted work.

Verify the running version. In a disposable authorized thread, send `@Bot kick`,
then mention it again and send `new`; neither should produce a reply or rejoin.
Repeat after a controlled restart. Do not kick a bot from a thread you want it
to keep using: there is deliberately no chat-based undo command.


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

The Discord history tools require a per-call integer `days` value from 1 through `runtime.discord_history_max_days`. A request fails rather than returning partial history if it exceeds `runtime.discord_history_max_messages`, `runtime.discord_history_max_bytes`, or `runtime.discord_history_timeout_seconds`.

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

### Integrated Threadwatch reporting

The optional `[threadwatch]` table runs bidirectional reporting inside the bridge process. It reads the bridge's configured SQLite database and OMP session directory, sends sanitized records and heartbeats through a durable queue, and polls for operator chat commands addressed to this bridge:

```toml
[threadwatch]
server_url = "http://192.168.192.183:8787"
ingest_token_file = "~/.config/omp-discord-bridge/threadwatch-ingest-token"
bot_id = "hephaestus"
bot_name = "Hephaestus"
data_dir = "~/.local/state/threadwatch-collector"
collect_interval_ms = 2000
heartbeat_interval_ms = 10000
```

`server_url` must be a plain HTTP origin. `bot_id` is a stable ASCII identifier; never reuse one for a different bridge. The ingestion token file follows the same protected-file rules as the Discord token and must contain at least 32 non-whitespace characters. It authenticates both reporting and command polling; browser operators use a separate Threadwatch control token. Accepted commands enter the same durable turn queue as authorized human Discord messages, including interruption and queue-limit semantics. `check` validates this configuration and credential without contacting Threadwatch. Reporting and command-polling outages are logged and retried without taking Discord offline.

For migration from a standalone Threadwatch collector:

1. Preserve its collector state directory, bot ID, and shared ingestion token.
2. Stop and disable `threadwatch-collector` before starting an integrated reporter; never let both own the same state.
3. Set `threadwatch.data_dir` to that existing state directory. This preserves checkpoints, pending batches, and stable delivery IDs.
4. Give the bridge's startup identity read access to the ingestion token. With Infisical, grant the bridge identity only the required Threadwatch token and render it as a mode-600 runtime file.
5. Start or roll over the bridge using the normal idle-safe procedure. Verify a fresh ready heartbeat and zero queued batches before deleting the retired collector unit/configuration.

Rollback reverses configuration, not data: stop the bridge reporter, restore the standalone collector against the same authoritative state directory, and never restore a stale queue snapshot. The bridge's existing Discord token supplies optional read-only title/avatar lookup; no separate collector Discord credential or bridge JSON file is used.

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
bot exchanges can resume. Bot messages cannot invoke `status`, `models`, `model`, `stop`, `new`, or `kick`,
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

OMP can call `discord_thread_history` with `{"thread_id":"123456789012345678","days":7}` to read a particular thread. Both arguments are required: `thread_id` must be a nonzero decimal u64 **string**, not a JSON number, and `days` must be an integer within the configured window. Extra arguments are rejected. Discord channel metadata must identify the target as a thread in the current session's guild and the same authorized parent channel; other parents, guilds, and non-thread channels are rejected. The target does not need its own bridge session. Results use the same chronological JSON, pagination, and resource limits as parent-channel history; inaccessible threads and Discord API errors fail explicitly.

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

Titles use only the status emoji and topic; detailed activity stays in the stationary
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
- `new` — after the active turn stops, discard queued turns and start a fresh OMP session;
- `@Bot kick` — an authorized human mentions the specific bot and sends `kick`
  (or `/kick`) as the rest of the message. The bridge permanently excludes itself
  from this thread, cancels active work, discards queued work, and leaves without
  posting a farewell. This also works before the bot has adopted the thread.

Exclusions survive restarts. Human/peer mentions, `new`, and re-adding the bot to
the Discord thread do not clear them. There is no chat command to undo a kick.
Other bots and other threads are unaffected; each bot must run this bridge version
to honor its own exclusion. Messages, uploads, approvals, forms, activity edits,
and title updates are blocked after exclusion commits. HTTP requests already in
flight finish before that commit; a kick cannot retract something Discord already
received. A failed Discord leave does not undo local exclusion.

Discord's native member-removal event also persists exclusion when the bridge
receives an event removing its own user. Removal while offline may not deliver
that event: use the explicit command while connected for durable enforcement.
Native removal is not a Discord-level ban; persistence belongs to this bridge's
SQLite database. Keep that database when restarting or migrating the bot.

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
