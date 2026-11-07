# OMP RPC transport

The bridge owns one short-lived OMP RPC process per queued turn. A new Discord thread starts OMP without `--resume`; after the first completed turn, the bridge stores OMP's canonical `sessionFile`. Every later turn starts OMP with `--resume <sessionFile>`, so daemon restarts continue the same saved conversation rather than merely preserving a Discord mapping.

## Launch contract

The bridge launches OMP directly, without a shell:

```text
omp --mode rpc-ui \
  --session-dir <configured-session-dir> \
  --cwd <authorized-project-directory> \
  --no-title \
  [--resume <canonical-session-file>] \
  [--model <configured-model>] \
  [--thinking <configured-level>]
```

The child inherits the bridge process environment, including `HOME` and provider API-key variables. OMP therefore uses its normal credential database at `~/.omp/agent/agent.db`, its normal `.env` lookup, and the project configuration in `<cwd>/.omp`. `session-dir` relocates conversation files only; it does not create a separate OMP installation. When `omp.model` is omitted, OMP selects its configured default. An explicit model must use an authenticated provider ID: `openai-codex` account access and `openai` API-key access are distinct routes.

`rpc-ui` is required because ordinary headless RPC cannot satisfy approval prompts. OMP 18.18.0 sends tool approvals as `extension_ui_request` `select` frames. The bridge recognizes only the exact one-shot `Approve` and `Deny` choices and presents them as Discord buttons; it deliberately does not expose OMP's persistent “approve for this session” choice. Only users in the guild policy's allowlist can decide them. Unrelated select, input, editor, or URL UI requests are cancelled rather than guessed.

The bridge does not pass `--approval-mode` or `--auto-approve`. OMP's project configuration in the session working directory governs `tools.approvalMode`; when the project does not set it, OMP's normal configuration layering and default apply. Read-only calls and approved actions retain their normal OMP capabilities.

Before each prompt, the bridge registers one RPC host tool named `discord_channel_history` with `set_host_tools`. OMP calls it with an integer `days` argument. The bridge—not the OMP process—uses the Discord token to page backward through the managed session's authorized parent channel, stopping at the requested cutoff. Results are chronological JSON and include message text, author data, embeds, and attachment metadata.

The host tool enforces the configured maximum days, message count, encoded bytes, and request timeout. Crossing any limit returns a tool error and no partial history. Discord content and metadata remain untrusted; the tool result labels them as data. OMP receives neither the Discord token nor a general Discord API capability.

## Protocol behavior

Verified locally with OMP `18.18.0`:

- stdout starts with a `ready` JSON frame advertising protocol versions 1 and 2;
- commands and events are newline-delimited JSON;
- `prompt` acknowledgment means accepted, not finished;
- a turn finishes only at terminal `agent_end`, or a local command reports `agentInvoked: false`;
- `abort` must be followed through terminal `agent_end`;
- `get_state` returns the authoritative `sessionId` and `sessionFile`.

The current client deliberately uses protocol v1 and enforces OMP's advertised 1 MiB physical-frame ceiling. It accumulates only assistant text and caps that output with `runtime.max_response_bytes`; unknown events are ignored. A malformed frame, oversized frame, unexpected EOF, or process loss after prompt acceptance marks the turn **uncertain** and never replays it automatically.

## Cancellation and shutdown

`stop` first marks the Discord session stopped, then sends RPC `abort` to an active process. The bridge waits up to 15 seconds for terminal completion. A missing terminal event is uncertain, because tools may already have changed external state.

On normal completion the bridge requests state, validates that the returned session file resolves beneath the configured session directory, closes stdin, drains stdout through EOF, and then reaps OMP. Keeping stdout open is required because OMP emits disposal frames while shutting down. If output or process shutdown exceeds ten seconds, the bridge kills and reaps the child and reports a protocol failure.

## Startup probe

`check` starts a real OMP RPC process, waits for `ready`, closes stdin, and verifies a clean process exit. It does not send a model prompt or mutate a project:

```bash
cargo run -- --config ./config.toml check
```
