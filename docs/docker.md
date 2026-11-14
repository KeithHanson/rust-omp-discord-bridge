# Docker deployment

The image contains the Rust bridge and a pinned OMP binary. It runs as the host UID/GID selected through Compose, keeps its root filesystem read-only, and does not publish a network port. Discord and model-provider connections are outbound.

## Local bind mounts

| Host path | Container path | Access | Contents |
|---|---|---|---|
| `BRIDGE_CONFIG_DIR` | `/config` | read-only | `config.toml` and `discord-token` |
| `BRIDGE_DATA_DIR` | `/data` | read-write | SQLite state, OMP sessions, OMP credentials, caches, and home directory |
| `OMP_WORKSPACE_DIR` | `/workspace` | read-write | Allowed project tree used by OMP |

The defaults are `./container/config`, `./container/data`, and the repository root. Set `OMP_WORKSPACE_DIR` to the directory containing every configured guild working directory. Paths in `config.toml` must use container paths, not host paths.

## Initial setup

From the repository root:

```bash
export HOST_UID="$(id -u)"
export HOST_GID="$(id -g)"
export BRIDGE_CONFIG_DIR="$PWD/container/config"
export BRIDGE_DATA_DIR="$PWD/container/data"
export OMP_WORKSPACE_DIR="$HOME/src"

install -d -m 700 "$BRIDGE_CONFIG_DIR" "$BRIDGE_DATA_DIR"
cp config.container.example.toml "$BRIDGE_CONFIG_DIR/config.toml"
chmod 600 "$BRIDGE_CONFIG_DIR/config.toml"

umask 077
cat > "$BRIDGE_CONFIG_DIR/discord-token"
```

Paste the Discord token, press Enter, then `Ctrl-D`. Edit `config.toml` with the real Discord IDs. Set each guild `cwd` beneath `/workspace`; set `omp.allowed_root = "/workspace"`. Runtime state belongs beneath `/data`.

Compose runs the container with `HOST_UID:HOST_GID`. This lets the process read a mode-`600` token owned by the current user and write the local data and workspace mounts without making them world-writable. Export these variables in the shell that runs every Compose command, or put their numeric values and mount paths in a local `.env` file.

## OMP authentication

OMP stores its normal account database under `/data/.omp`, so authentication survives container replacement:

```bash
docker compose run --rm --entrypoint omp bridge login
```

Provider API-key environment variables may instead be added to the Compose service or supplied by the deployment secret manager. Do not put credentials in `config.toml`, the image, or Compose command arguments.

## Validate and run

Build the pinned Rust and OMP versions, then exercise configuration, SQLite, the protected token, and real OMP RPC startup:

```bash
BRIDGE_BUILD_REVISION="$(git rev-parse HEAD)" docker compose build bridge
docker compose run --rm bridge check
docker compose up -d bridge
docker compose logs -f bridge
```

Build from a clean checkout. `.git` is excluded from the image context, so the
build requires `BRIDGE_BUILD_REVISION`: the source commit hash, optionally suffixed
with `-dirty` for modified sources. Source archives must supply their known commit.
This value is embedded in the binary's idle presence and `--version`; it is not
read from the mounted workspace at runtime.

Stop cleanly with:

```bash
docker compose down
```

The bridge handles both `SIGINT` and Docker's `SIGTERM`. Compose allows 45 seconds for gateway shutdown and OMP child cleanup before forcing termination.

## Persistence and backup

Back up `BRIDGE_DATA_DIR` together with the workspace. The data directory contains bridge SQLite state, saved OMP conversations, and OMP authentication material; protect backups as secrets. The config mount is read-only, while `/data`, `/workspace`, and an in-memory `/tmp` are the only writable locations in the default deployment.

To change pinned build versions for a deliberate upgrade:

```bash
BRIDGE_BUILD_REVISION="$(git rev-parse HEAD)" RUST_VERSION=1.99.1 OMP_VERSION=18.20.0 docker compose build --pull bridge
```

Run `docker compose run --rm bridge check` after every image, configuration, credential, or mount change before restarting the gateway.

## Upgrade to 2026-11-14-2

This release adds persistent per-thread bot exclusion with `@Bot kick`.
Existing configuration remains valid and OMP 18.20.0 remains the minimum version.
Startup adds an `excluded_threads` SQLite table. Preserve the data mount to retain
exclusions across upgrades. Older bridge images do not enforce these exclusions.

Build from the release tag in a clean checkout:

```bash
git fetch origin --tags
git checkout v2026-11-14-2
BRIDGE_BUILD_REVISION="$(git rev-parse HEAD)" docker compose build bridge
docker compose run --rm bridge --version
```

Wait until all conversations and final message delivery are idle before replacing
the gateway. Back up the data directory consistently while the bridge is stopped;
do not copy a live SQLite main file without its WAL. Keep the old image for rollback.
Then run `docker compose run --rm bridge check` and `docker compose up -d bridge`.
Use the same config/data mounts for the check and the service. Never run two
gateways against the same SQLite database.

Acceptance: in a disposable authorized thread, send `@Bot kick`. Mention the bot
again and send `new`; it must not reply or rejoin. Restart the bridge cleanly and
repeat. Other threads must remain usable. Only use a disposable thread: no chat
command clears an exclusion. Gateway connectivity alone does not prove this path.
