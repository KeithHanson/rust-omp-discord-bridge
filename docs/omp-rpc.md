# OMP RPC transport

The bridge owns one OMP RPC process per active conversation run, which may contain several interrupted/replacement turns. An idle thread starts OMP with its saved `--resume <sessionFile>` when available, or without `--resume` for a new conversation. Messages arriving while OMP is active use `abort_and_prompt` in that same process, not a second child or a synthetic new session. The validated canonical `sessionFile` is saved as early as the protocol allows and on completion.

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

`rpc-ui` is required because ordinary headless RPC cannot satisfy approval prompts. OMP 18.20.0 sends tool approvals as `extension_ui_request` `select` frames. The bridge recognizes only the exact one-shot `Approve` and `Deny` choices and presents them as Discord buttons; it deliberately does not expose OMP's persistent “approve for this session” choice. Only users in the guild policy's allowlist can decide them. Unrelated select, input, editor, or URL UI requests are cancelled rather than guessed. UI/history waits remain interruptible; cancelling an approval wait invalidates its Discord buttons rather than leaving an actionable stale request.

The bridge does not pass `--approval-mode` or `--auto-approve`. OMP's project configuration in the session working directory governs `tools.approvalMode`; when the project does not set it, OMP's normal configuration layering and default apply. Read-only calls and approved actions retain their normal OMP capabilities.

Before the first prompt in each process, the bridge registers one RPC host tool named `discord_channel_history` with `set_host_tools`. OMP calls it with an integer `days` argument. The bridge—not the OMP process—uses the Discord token to page backward through the managed session's authorized parent channel, stopping at the requested cutoff. Results are chronological JSON and include message text, author data, embeds, and attachment metadata.

The host tool enforces the configured maximum days, message count, encoded bytes, and request timeout. Crossing any limit returns a tool error and no partial history. Discord content and metadata remain untrusted; the tool result labels them as data. OMP receives neither the Discord token nor a general Discord API capability.

## Protocol behavior

Protocol contract for OMP `18.20.0` (minimum supported version):

- stdout starts with a `ready` JSON frame advertising protocol versions 1 and 2;
- commands and events are newline-delimited JSON;
- `prompt` acknowledgment means accepted, not finished;
- every prompt/replacement has a unique command ID; responses and `prompt_result` frames are correlated by that ID;
- `prompt_result.status` distinguishes completed, aborted, and failed work; local-only commands may complete in the response with `agentInvoked: false`;
- generic `agent_end` does not finish a correlated prompt or close the process underneath a replacement;
- process shutdown waits until all dispatched prompt outcomes are known and the session is settled;
- a newer message uses `abort_and_prompt`; `stop` uses `abort`, with a bounded cancellation grace period;
- `get_state` returns the authoritative `sessionId` and `sessionFile`.

The current client deliberately uses protocol v1 and enforces OMP's advertised 1 MiB physical-frame ceiling. It accumulates assistant text and caps that output with `runtime.max_response_bytes`. Known lifecycle, tool execution, retry, compaction, and assistant-stream events also update a bounded, latest-value activity channel; unknown events are ignored. Activity labels are fixed strings, never raw tool names, arguments, results, or thinking text. Activity create/edit requests run separately from RPC processing and assistant-message delivery; intermediate updates are coalesced every five seconds. A malformed frame, oversized frame, unexpected EOF, or process loss after prompt dispatch marks unresolved turns **uncertain**, even if acceptance was not acknowledged, and never replays them automatically.

The event mapping was exercised with OMP `18.20.0` using a real read-only turn in
a temporary working directory/session and a live Discord create/edit/final-edit
smoke test. Activity delivery failures are logged, not promoted into turn failures.
The status message is ephemeral operational state: its ID is not saved in SQLite,
so crash recovery still relies on the durable turn status, not the last visible edit.

Live interruption was also exercised with OMP `18.20.0`, temporary workspaces,
session files, and SQLite databases, with labeled status/reply messages in Discord:
interrupting a foreground command with two rapid replacements, and interrupting
an unanswered approval. The old turns were recorded aborted, the newest answers
completed with the original conversation marker intact, and the unapproved
command did not execute. Replacement answers arrived roughly 4.4 and 5.1 seconds
after interruption in that run; these are observations, not latency guarantees.

Message-level delivery was exercised with real OMP and labeled Discord messages
using disposable workspaces, sessions, and SQLite databases. The first commentary
message was acknowledged by Discord while its turn was still running, roughly
10.6 seconds before normal completion. An interrupted run retained that already
posted message, sent one interruption notice, then one replacement answer with the
remembered context marker roughly 2.7 seconds later. Neither run repeated its
aggregate response at completion. These timings are observations, not guarantees.

## Durable interruption admission

The gateway commits the Discord message before waking the active RPC loop, and
wakes that loop before awaiting any Discord HTTP acknowledgement. The runner holds
one concurrency permit for the live process. A transaction claims each replacement
and records `interrupted_by_turn_id` on its predecessor before dispatch. This is
interruption intent, not proof that a tool stopped.

At most two prompt tickets are unresolved in the RPC client: the current prompt
and its pending replacement. More inputs remain durable in SQLite and are claimed
in order as tickets resolve. No input is coalesced or automatically resent.
The per-thread queued/running limit is 64; overflow is rejected explicitly.
Terminal outcomes are persisted individually. Remaining running rows become
uncertain after transport failure or daemon restart, including a write whose
acknowledgement may have been lost. Undispatched queued inputs remain pending.
Replacement acknowledgements and interrupted predecessors have independent
15-second deadlines. Missing confirmation fails uncertain instead of blocking
later inputs for the normal turn timeout. Assistant text without a correlated
prompt owner also fails uncertain rather than being silently dropped or attached
to the wrong message, including uncorrelated background-agent output.

The activity message spans the live process. Completed visible assistant messages
are enqueued at `message_end`, including commentary before tools, without waiting
for `prompt_result` or session settlement. Stream deltas and the final message
snapshot are deduplicated; thinking, tool results, and unfinished aborted/error
messages are not published. A completed prompt can flush remaining text if its
stream omitted `message_end`. Full aggregate output is still persisted, but never
resent as a second final answer.

A FIFO worker queues up to 64 messages, chunks each message, and sends independently
of the RPC reader, with a ten-second deadline per send. Interruption/failure notices
join that same queue after persistence. A failed or timed-out send is ambiguous:
the remaining chunks of that message
are not retried, and later independent messages can still be delivered. Queue
saturation is an explicit error rather than silent message loss. Already-posted
messages are not retracted on interruption; unfinished text remains in SQLite.

A message arriving after the RPC loop commits to shutdown is picked up by the next
process using the saved session. The stopped-session check and durable claim are
atomic, so a stale Discord handler cannot admit new work after `stop`.

## Cancellation and shutdown

`stop` first marks the Discord session stopped, then sends RPC `abort` to an active process. The bridge waits up to 15 seconds for all correlated prompt outcomes and a settled session. A missing outcome or unsettled session is uncertain, because tools may already have changed external state; an uncorrelated `agent_end` alone is not completion.

On normal completion the bridge requests state, validates that the returned session file resolves beneath the configured session directory, closes stdin, drains stdout through EOF, and then reaps OMP. Keeping stdout open is required because OMP emits disposal frames while shutting down. If output or process shutdown exceeds ten seconds, the bridge kills and reaps the child and reports a protocol failure.

## Startup probe

`check` starts a real OMP RPC process, waits for `ready`, closes stdin, and verifies a clean process exit. It does not send a model prompt or mutate a project:

```bash
cargo run -- --config ./config.toml check
```
