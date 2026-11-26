#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

usage() {
cat <<'EOF'
Usage: scripts/test-candidate.sh [--debug] [--build-only] [--help]

Build the current checkout and deploy it to an experimental test bot.

The test bot lives in its own dedicated setup, separate from the production
bot:

    ~/.config/omp-client-bridge-test/config.toml   (mode 600)
    ~/.config/omp-client-bridge-test/<token file>  (mode 600)
    ~/.local/state/omp-client-bridge-test/          (0700, persistent)

The script:
  1. builds the checkout into target/candidate/<profile>/
  2. validates the test bot's config, token, and state layout
  3. stops the running test bot (if any)
  4. replaces the test bot's stable binary with the fresh build
  5. starts the test bot, leaving it running

The production bridge, its database, and its control socket are never
touched: the test bot uses its own config, state directory, socket, and
token. The state directory is persistent across runs; inspect it as needed.

  --debug       Build in debug mode instead of release (faster, bigger binary).
  --build-only  Build the binary without deploying it to the test bot.
  --help        Show this help without building or testing.

No test bot can be assumed to exist yet: the script fails early with the
required layout if the setup is missing. See the README section
"Deploy a candidate build to the test bot" for details.
EOF
}

fail() {
    printf 'test-candidate: %s\n' "$*" >&2
    exit 1
}

profile=release
build_only=false
for argument in "$@"; do
    case "$argument" in
        --debug) profile=debug ;;
        --build-only) build_only=true ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; fail "Unknown argument: $argument" ;;
    esac
done

for utility in cargo git omp rm cat chmod stat pgrep kill nohup; do
    command -v "$utility" >/dev/null 2>&1 || fail "Missing $utility."
done

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
root=$(pwd -P)
[[ $(git rev-parse --show-toplevel) == "$root" ]] || fail 'Run from a Git checkout of this repository.'

printf 'Building (%s) into target/candidate/%s ...\n' "$profile" "$profile"
if [[ $profile == release ]]; then
    cargo build --locked --release --bins --target-dir target/candidate
else
    cargo build --locked --bins --target-dir target/candidate
fi
candidate_binary="target/candidate/$profile/omp-client-bridge"
version=$("$candidate_binary" --version) || fail 'Built binary failed to report its version.'
printf 'Candidate: %s\n' "$version"

if "$build_only"; then
    printf 'Build-only mode; candidate left at %s\n' "$candidate_binary"
    exit 0
fi

# --- Test bot setup -------------------------------------------------------------

config_dir="$HOME/.config/omp-client-bridge-test"
config_file="$config_dir/config.toml"
state_dir="$HOME/.local/state/omp-client-bridge-test"
stable_binary="$state_dir/omp-client-bridge"

config_required="The test bot setup is missing or invalid: $config_file

Create it for the experimental test bot (see README.md \"Deploy a candidate
build to the test bot\"). A minimal config:

    [discord]
    token_file = \"$config_dir/discord-token\"
    [[discord.guilds]]
    id = \"<test guild id>\"
    allowed_channels = [\"<test channel id>\"]
    allowed_users = [\"<test user id>\"]
    cwd = \"~/\"

    [omp]
    executable = \"omp\"
    session_dir = \"$state_dir/omp-sessions\"
    allowed_root = \"~/\"

    [runtime]
    database_path = \"$state_dir/bridge.sqlite\"

    [control]
    socket = \"$state_dir/control.sock\"

The token file must exist, be a regular file, mode 600, and non-empty."

[[ -f $config_file ]] || fail "$config_required"
[[ ! -L $config_file ]] || fail 'test bot config must not be a symlink'
[[ ! -L $config_dir ]] || fail 'test bot config directory must not be a symlink'

parse_tmp=$(mktemp "${TMPDIR:-/tmp}/omp-client-bridge-parse.XXXXXX")
parse_error=$(mktemp "${TMPDIR:-/tmp}/omp-client-bridge-parse-err.XXXXXX")
python3 - "$config_file" "$parse_tmp" > "$parse_error" <<'PY'
import sys, tomllib
from pathlib import Path
config_file = Path(sys.argv[1])
out_file = Path(sys.argv[2])
try:
    config = tomllib.loads(config_file.read_text())
except tomllib.TOMLDecodeError as error:
    print(f"invalid TOML: {error}", file=sys.stderr)
    sys.exit(1)
discord = config.get("discord") or {}
token_file = discord.get("token_file") or ""
guilds = discord.get("guilds") or []
channel = None
if guilds:
    channels = guilds[0].get("allowed_channels") or []
    channel = str(channels[0]) if channels else None
runtime = config.get("runtime") or {}
db_path = runtime.get("database_path") or ""
control = config.get("control") or {}
socket = control.get("socket") or ""
omp = config.get("omp") or {}
session_dir = omp.get("session_dir") or ""
def expand(p):
    return str(Path(p).expanduser()) if p else ""
out_file.write_text("\n".join([
    expand(token_file),
    channel or "",
    expand(db_path),
    expand(socket),
    expand(session_dir),
]))
PY
parse_code=$?
if [[ $parse_code != 0 ]]; then
    rm -f -- "$parse_tmp" "$parse_error"
    printf 'test-candidate: %s\n' "$(cat "$parse_error")" >&2
    fail "$config_required"
fi
mapfile -t parsed < "$parse_tmp"
rm -f -- "$parse_tmp" "$parse_error"

token_file="${parsed[0]}"
db_path="${parsed[2]}"
socket_path="${parsed[3]}"
session_dir="${parsed[4]}"

[[ -n $token_file ]] || fail "$config_required"

token_mode=$(stat -c '%a' "$token_file" 2>/dev/null) || fail "token file not found: $token_file\n$config_required"
[[ $token_mode == "600" ]] || fail "token file $token_file must be mode 600, found mode $token_mode"
[[ -s $token_file ]] || fail "token file is empty: $token_file"
command -v omp >/dev/null || fail 'omp not found in PATH (required for OMP sessions).'

[[ ! -L $state_dir ]] || fail "test state directory must not be a symlink: $state_dir"

# The config must keep all mutable state inside the test directory.
case $db_path in
    "$state_dir"/*) ;;
    *) fail "database_path $db_path must live under the test state directory $state_dir" ;;
esac
case $socket_path in
    "$state_dir"/control.sock) ;;
    *) fail "control socket $socket_path must be $state_dir/control.sock" ;;
esac
case $session_dir in
    "$state_dir"/*) ;;
    *) fail "session_dir $session_dir must live under the test state directory $state_dir" ;;
esac

# --- Stop the test bot -----------------------------------------------------------

pids=$(pgrep -f "^$stable_binary --config $config_file run$" 2>/dev/null || true)
if [[ -n $pids ]]; then
    for pid in $pids; do
        printf 'Stopping test bot (PID %s) ...\n' "$pid"
        kill -TERM "$pid" 2>/dev/null || true
        for _ in $(seq 1 30); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            printf 'Test bot (PID %s) ignored SIGTERM; sending SIGKILL\n' "$pid"
            kill -KILL "$pid" 2>/dev/null || true
            sleep 1
        fi
    done
    printf 'Test bot stopped.\n'
else
    printf 'Test bot is not running.\n'
fi

# --- Replace the binary ----------------------------------------------------------

mkdir -p -m 700 -- "$state_dir"
chmod 700 -- "$state_dir"
cp -f -- "$candidate_binary" "$stable_binary"
chmod 700 -- "$stable_binary"
printf 'Binary replaced: %s\n' "$stable_binary"

# --- Start the test bot ----------------------------------------------------------

nohup "$stable_binary" --config "$config_file" run > "$state_dir/bridge.log" 2>&1 &
test_pid=$!
printf 'Starting test bot (PID %s) ...\n' "$test_pid"

ready=false
for _ in $(seq 1 120); do
    if [[ -S $socket_path ]]; then
        ready=true
        break
    fi
    if ! kill -0 "$test_pid" 2>/dev/null; then
        printf '\nTest bot exited during startup:\n' >&2
        tail -n 30 "$state_dir/bridge.log" >&2 || true
        fail 'test bot failed to start'
    fi
    sleep 0.5
done
[[ $ready == true ]] || {
    printf '\nTest bot did not come up within 60s:\n' >&2
    tail -n 30 "$state_dir/bridge.log" >&2 || true
    fail 'test bot failed to start'
}
printf 'Test bot running (PID %s); socket %s; log %s\n' "$test_pid" "$socket_path" "$state_dir/bridge.log"
