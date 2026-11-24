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

Managed threads can select a persistent, nullable provider/model override using
`model provider/model`, with choices listed by `models`. The bridge validates the
exact pair against OMP's available-model catalog before saving it. Selection is
idle-only and does not create a new conversation. The override takes precedence
over `omp.model` and saved-session selection for subsequent conversation runs.
`model default` removes the override; title review retains its separate selection
policy. SQLite migration adds the nullable session field without modifying prior
conversation files.

`model default provider/model` sets the bridge-wide default by rewriting the
`omp.model` line in the loaded config file (preserving comments and formatting),
validating the pair against the catalog first. It does not change the session's own
override or the running process; new sessions and override-cleared sessions pick up the
new default on their next spawn, and title review (which already honors `omp.model` when
`title_model` is unset) follows the same change on its next call. Changing the default
still requires an idle session, since it writes a file shared by every thread.

Verification of this feature used OMP 18.20.0 with the host authentication profile
and isolated temporary workspace, SQLite, and session directory. The real catalog
returned eight available models. A new conversation selected
`openai-codex/gpt-5.6-luna`; its resumed conversation selected
`openai-codex/gpt-5.6-sol` and recalled the original marker. Both exact RPC selections
were acknowledged. All 191 library tests passed, including model persistence,
idle guards, provider-ID validation, and the local Discord HTTP control route.
Only one authenticated provider was available for this live smoke; cross-provider
selection is covered by deterministic tests, not a live provider-to-provider turn.
This does not establish deployment or Discord-client appearance.

`rpc-ui` is required for interactive requests. OMP 18.20.0 sends tool approvals as `extension_ui_request` `select` frames. The bridge recognizes only the exact one-shot `Approve` and `Deny` choices and presents them as Discord buttons; persistent session-wide approval remains unavailable. Other selections and text/editor requests are routed to Discord prompts rather than silently cancelled. OMP's HTML `ask` tool uses these requests field by field: checkbox groups repeatedly select one toggle until “Done selecting,” and OMP performs form validation and assembles the final typed answer object. The bridge returns exact option strings and text, not display labels truncated to Discord's component limits.

UI waits remain interruptible. An OMP `cancel` request targets the original request ID; dropping its wait invalidates the Discord controls. Responses to a host-side UI timeout include `cancelled: true` and `timedOut: true`, so OMP can distinguish elapsed time from explicit cancellation. Secret-input requests are refused rather than collected in a public thread. Unsupported UI methods produce a visible explanation rather than pretending the user cancelled.

A local OMP 18.20.0 smoke probe exercised the real HTML `ask` tool with a radio field and a defaulted text field. OMP emitted `select` and `editor` requests, accepted the returned option label and text, and produced `{"mode":"safe","note":"round-trip verified"}` before completing the turn. This verifies the upstream RPC form contract; it is not proof of Discord client rendering or a deployed bridge.

The bridge does not pass `--approval-mode` or `--auto-approve`. OMP's project configuration in the session working directory governs `tools.approvalMode`; when the project does not set it, OMP's normal configuration layering and default apply. Read-only calls and approved actions retain their normal OMP capabilities.

Before the first prompt in each process, the bridge registers `discord_channel_history`,
`discord_thread_history`, `discord_thread_participants`, and `discord_upload_file`
with `set_host_tools`, and requires OMP to acknowledge all four.
Both history tools require integer `days` from 1 through
`runtime.discord_history_max_days`. The channel tool reads the managed session's
authorized parent channel. The thread tool additionally requires `thread_id` as
a nonzero decimal u64 string (never a JSON number), for example
`{"thread_id":"123456789012345678","days":7}`; unknown arguments are rejected.
Discord channel metadata must identify a thread in the current session's guild
and the same authorized parent channel. A target bridge session is not required.
Non-thread channels, other guilds or parents, and inaccessible targets fail.
The bridge—not the OMP process—uses the Discord token to page backward through
the selected channel or thread, stopping at the requested cutoff. Results are
chronological JSON containing message text, author data, embeds, and attachment metadata.

The participants tool accepts no arguments and lists the current managed
thread's members with their display names, usernames, bot flags, IDs, and
explicit mentions. Membership is not online presence, access authorization, or
proof of having read the thread. Thread history's explicit ID does not grant arbitrary channel access.

History enforces the configured maximum days, message count, encoded bytes,
and request timeout. Participant lookup shares `discord_history_timeout_seconds`,
`discord_history_max_bytes`, and the `discord_history_max_messages` count cap.
Crossing a limit returns a tool error rather than a partial result. Discord
content and identity labels remain untrusted data. OMP receives neither the
Discord token nor a general Discord API capability.

The upload tool accepts `path` and an optional `caption`. It sends one local regular
file to the current session thread, with mentions disabled, and returns a JSON
delivery receipt. Relative paths use the session working directory; resolved files
must remain inside `omp.allowed_root`. File reads are bounded by
`runtime.max_attachment_bytes`; Discord may enforce a lower upload limit.
Uploads are external side effects: cancellation or a transport error can leave an
already-posted attachment. Inspect the thread before retrying an uncertain upload.
File access is not permission to publish secrets or unrelated private information.

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

## Missing-final-message nudge

A terminal assistant message is an assistant `message_end` with `stopReason == "stop"`
and nonempty visible text. Tool-use commentary (`toolUse`) and unfinished
aborted/error messages never count as final.

If a durable turn completes (`prompt_result` status `completed`) or aborts
(`prompt_result` status `aborted`) without producing a terminal assistant message, the
bridge may not settle the run. With the OMP process still alive, it sends one synthetic
internal prompt — never a durable queue row — to the same OMP session:

> You just stopped without sending a final assistant message. Send the final assistant response now.

The nudge rides the same delivery path as normal assistant text: its output reaches
the Discord/Matrix thread as an assistant message. The nudge is never recorded through
`TurnControl`, never becomes a durable turn, and never becomes the run's last result:
the durable record and the run's result keep the triggering turn's own outcome.

The nudge is one-shot per run, and only when the run is about to settle and no
cancellation has been requested: if another durable turn is dispatched
(interruption), the guard is re-armed and the previous nudge is discarded. Aborted
turns are nudged exactly like completed turns; failed, uncertain, and
pre-dispatch-ended runs are not. If the nudge itself completes without a terminal
message, no further nudge is sent; the run settles as before.

Whether a message actually reaches the thread remains best-effort, as with all
assistant output; delivery failures are logged, never promoted into turn failures.

## Cancellation and shutdown

`stop` first marks the Discord session stopped, then sends RPC `abort` to an active process. The bridge waits up to 15 seconds for all correlated prompt outcomes and a settled session. A missing outcome or unsettled session is uncertain, because tools may already have changed external state; an uncorrelated `agent_end` alone is not completion.

On normal completion the bridge requests state, validates that the returned session file resolves beneath the configured session directory, closes stdin, drains stdout through EOF, and then reaps OMP. Keeping stdout open is required because OMP emits disposal frames while shutting down. If output or process shutdown exceeds ten seconds, the bridge kills and reaps the child and reports a protocol failure.

## Startup probe

`check` starts a real OMP RPC process, waits for `ready`, closes stdin, and verifies a clean process exit. It does not send a model prompt or mutate a project:

```bash
cargo run -- --config ./config.toml check
```

## Background title review

Title review does not prompt or resume the working RPC process. It starts a
separate ephemeral OMP JSON-print call with tools, extensions, skills, rules,
context-file discovery, LSP, automatic title generation, and session persistence
disabled. Input is a bounded projection of recent visible conversation text plus
queue/session state; it is explicitly untrusted evidence, not executable instructions.
The reviewer returns a topic and one of `working`, `thinking`, `waiting`, or
`blocked`. Completion is determined by the bridge's post-delivery queue state.

The reviewer uses `omp.title_model`, falling back to `omp.model` or the OMP
default. One inference runs at a time per bridge, with a 45-second deadline and
bounded output. Cancellation kills the review child. It neither owns a turn nor
changes the source session, and Discord credentials are not forwarded.

Discord title HTTP retries differ from ordinary message delivery: the bridge
retains server-provided cooldowns and waits before taking fresh context and
generating a new title. It does not let the HTTP client automatically replay a
stale title body. Periodic review continues only while work remains; terminal
idle reconciliation preserves the topic and applies `✅`.
