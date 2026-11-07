# OMP Discord Bridge engineering instructions

## Product invariant

A permitted Discord user mentions the bot in an allowed parent channel and receives one managed Discord thread backed by one OMP session. Messages accepted in that thread execute against the same OMP conversation, in order, until the user explicitly starts a new session or stops it.

Do not claim session persistence merely because a Discord thread mapping survives. Persistence means a daemon restart resumes the same OMP conversation with its prior context. A Discord message must never execute twice because of gateway redelivery, reconnect, or restart recovery.

## Current source of truth

This repository is the Rust implementation. The JavaScript `CarterMcAlister/omp-discord-bridge` repository is behavioral reference material only. Do not copy its implementation claims without proving them against OMP:

- its SQLite state survives restart, but its original ACP adapter starts a new OMP session after restart;
- its production adapter does not implement the advertised steer or compact behavior;
- it buffers agent text rather than streaming live updates;
- it automatically accepts OMP permission requests.

No OMP transport has been selected here yet. Do not silently make ACP, terminal scraping, session-file mutation, or the collaboration protocol the permanent boundary. Select a transport from current OMP capabilities, record the contract, and prove new-session, resume, cancel, steering, compaction, tool events, and approval behavior with the real OMP process before building higher layers on it.

## Required behavior

Preserve these invariants as the implementation grows:

- A mention in an allowed parent channel creates exactly one thread and one durable session record.
- Only configured guilds, parent channels, and users may create or control sessions.
- Every accepted Discord message is keyed by its Discord message ID and processed at most once.
- Turns for one OMP session are serialized in acceptance order.
- Independent sessions may run concurrently up to one explicit global limit.
- Queue, session, and delivery state survive clean and unclean daemon restarts.
- Recovery distinguishes work known not to have started from work with an uncertain outcome. Never blindly replay an uncertain coding-agent turn.
- `status`, `stop`, and `new` report the state transition they actually completed.
- Long Discord output is split without corrupting code fences or silently dropping content.
- Attachment handling has explicit count, size, content-type, download, and timeout limits.
- Failures remain visible and retryable; they are not converted into apparent successful replies.

## Security gates

- Read the Discord token from an environment variable or secret provider. Never accept it in a checked-in config file or command-line argument.
- Resolve and validate configured working directories before starting OMP. Prevent path traversal and symlink escape from an allowed root.
- Treat Discord message text, embeds, filenames, attachment metadata, URLs, slash-command values, and OMP output as untrusted input.
- Do not log tokens, authorization headers, message bodies, attachment URLs with access signatures, or OMP secrets. Diagnostic content logging must be explicit and off by default.
- Bound queues, messages, downloaded bytes, response bytes, subprocess output, and concurrent work.
- Run OMP without a shell. Pass arguments directly to the child process and define its environment explicitly.

## Rust requirements

- Use Rust edition 2024 and stable Rust.
- Keep `Cargo.lock` committed; this repository builds an application, not a reusable library.
- Production application code and operational tooling are Rust. Do not introduce a parallel JavaScript or Python runtime.
- Keep Discord, OMP transport, persistence, and application policy behind explicit boundaries. Core queue and session transitions must not depend on Discord SDK types.
- Model IDs and states with domain types instead of interchangeable strings. Model queue/session lifecycle with enums and exhaustive matches.
- Do not block an async executor with SQLite, filesystem, or child-process waits. Use the selected runtime's blocking boundary deliberately.
- Avoid allocation, copying, and formatting in streaming and message-chunking hot paths. Retain bounded buffers and apply backpressure.
- Every fixed limit needs an explicit error path and a boundary test. Never truncate prompts, tool output, or attachments silently.
- Handle integer conversion and Discord snowflake parsing without unchecked casts.
- `unsafe` is forbidden by the workspace manifest. Reconsider that policy only for a demonstrated requirement and document the local safety invariant.
- Add dependencies deliberately, disable unused default features, and prefer maintained crates with narrow responsibilities.
- Do not opportunistically update unrelated dependencies while implementing behavior.
- Keep formatting and Clippy clean. Fix warnings rather than suppressing them globally.

## Durable state and concurrency

Persist enough information to answer, after any restart:

- which Discord thread maps to which OMP session;
- which working directory and authorization policy apply;
- which Discord messages were accepted;
- each turn's state and ordering key;
- whether an attempted turn has a known or uncertain external outcome;
- which response chunks were delivered to Discord.

Use database constraints and transactions for uniqueness and state transitions. In-memory mutexes may optimize execution but must not be the source of truth. State changes must be monotonic and invalid transitions must fail visibly.

Cancellation is a state transition, not only an abort signal. Record the request, attempt transport cancellation, observe the process result, and report whether cancellation completed or the outcome is uncertain.

## Behavior-first development

Work on one observable behavior at a time:

1. State the user-visible behavior and failure boundary.
2. Add a failing test at the narrowest durable boundary when practical.
3. Implement the smallest production change that satisfies it.
4. Run the focused test, then the complete local quality gate.
5. Exercise the changed path through the real executable.
6. For Discord or OMP integration, exercise a test guild and real OMP process when credentials are available.

Tests must assert consumer-visible behavior, state transitions, ordering, idempotency, authorization, limits, and recovery. Do not test copied constants, source text, mock echoes, simple forwarding, or incidental SDK calls. Use deterministic fakes only at true external boundaries such as Discord, time, storage faults, and the OMP process.

For bugs, retain a failing-before/passing-after regression test when the behavior can be isolated. For restart behavior, terminate the process between durable state transitions and prove the observed recovery path.

## Canonical development commands

Install Rust through `rustup`, then install the repository toolchain components:

```bash
rustup toolchain install stable --profile minimal --component rustfmt,clippy
```

Run from the repository root:

```bash
cargo fmt -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --locked
cargo run -- --version
```

Run the narrowest relevant test first while iterating. Do not hide failures with output filtering or `|| true`. An unchanged failure requires investigation, not retries.

## Runtime verification

Compilation and mocked tests are not proof of Discord or OMP behavior.

For a Discord-facing change:

1. Start the actual bot with a test token and restrictive allowlist.
2. Exercise the changed command or message flow in a test guild.
3. Observe the created thread, replies, permissions, and durable state.
4. Restart during the relevant lifecycle when persistence or recovery changed.

For an OMP-facing change:

1. Start the real configured OMP executable in a temporary allowed working directory.
2. Exercise the exact operation: create, resume, prompt, cancel, steer, compact, tool event, or approval.
3. Observe both the protocol result and resulting OMP session state.
4. Confirm child-process cleanup after success, failure, cancellation, and daemon shutdown.

If credentials or an OMP capability are unavailable, finish all host-testable work and state the missing runtime proof precisely. Never replace it with a mock claim.

## Documentation boundaries

Documentation changes are part of behavior changes:

- `AGENTS.md`: product invariants, security rules, workflow, canonical commands, and completion gates;
- `README.md`: concise developer setup and normal operation;
- `docs/`: transport contracts, persistence design, operational deployment, and detailed troubleshooting;
- code comments: local invariants that cannot be expressed by types or APIs.

Update documentation when setup, configuration, commands, permissions, limits, storage schema, recovery semantics, or verified OMP behavior changes. Delete superseded instructions instead of accumulating alternatives.

## Completion gate

A change is complete only when:

- requested behavior and all affected call sites are implemented;
- focused tests and the complete local quality gate pass;
- the actual executable exercises the changed host path;
- Discord or OMP integration is exercised when the changed surface requires it, or the unavailable credential/capability is stated precisely;
- authorization, bounded-resource, restart, cancellation, and duplicate-delivery effects were considered;
- documentation matches verified behavior;
- no temporary diagnostics, placeholders, compatibility shims, obsolete paths, or generated artifacts remain.
