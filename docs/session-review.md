# Scheduled session review

The companion `scripts/session-review.py` worker uses Python 3.11+ standard libraries
and the bridge TOML's `omp.executable`, `omp.model` (if set), and `omp.session_dir`.
It does not open the bridge SQLite database, connect to Discord, or restart the
bridge. OMP uses the workstation's existing provider authentication; model calls
consume the usual provider quota.

The system timer runs at 00:00, 08:00, and 16:00 UTC. Each invocation snapshots
activity in the preceding eight hours. It scans `~/.omp/agent/sessions` and the
configured bridge session directory recursively, including nested JSONL sessions.
It also reads `~/.omp/agent/custom-session-files`: each OMP breadcrumb contains an
absolute session filename, not session JSON or a directory. Registered files need
not have a `.jsonl` extension. For `.jsonl` parents, nested sessions are discovered
under the sibling directory obtained by removing that suffix. Canonical paths
deduplicate overlaps and symlink aliases. Stale paths, empty breadcrumbs, and
non-file registry entries are ignored; unregistered sibling files are not added.
The report output tree is excluded even when registered, aliased, or under a scan root.
Repeated `--session-root` arguments replace both default roots and registry discovery.
Named OMP profiles, alternate `PI_CODING_AGENT_DIR`/XDG layouts, and unregistered
external storage require explicit roots. Automatic discovery targets this workstation's
default layout, not every possible OMP profile or storage layout.

Selection uses message/custom-message timestamps, not file modification times or
session start times. The window is `[invocation time minus eight hours, invocation
time)`: the start is inclusive and the end exclusive. Earlier records from selected
sessions supply task context. Every timestamped evidence type, including compaction
and branch summaries, is excluded at or after the end, even in mixed-record files.
Records without timestamps can provide context but cannot select a session.
Complete records are read from a fixed byte snapshot; unfinished trailing records
are excluded. Concurrent sessions are not stopped. An empty window exits successfully
without invoking OMP or creating a report/manifest.

This is a strict rolling-window review, not a lossless audit/checkpoint system.
A record completed or appended after one snapshot can be missed if its timestamp
is older than the next window and its session has no newer message activity.
With newer activity, that record can reappear as earlier context, not new activity.
A tail timestamped exactly at one window's end is eligible at the next window's
start if complete by then. `partial_tail` flags describe selected sessions only:
an unselected file with only an incomplete tail does not get a manifest entry.
The limitation is included in every published report and manifest. There is no
checkpoint, automatic overlap recovery, backfill, or cross-run deduplication.
Invocation timing can create gaps or overlapping windows; repeated/manual runs can
review activity again. Timer downtime does not expand the next eight-hour window.

The worker reviews every selected session, segmenting large logs without silently
truncating them. Each segment receives the error/recovery/shortcut/outcome rubric;
a final synthesis proposes precise rule changes or procedural documents. Large
syntheses are reduced hierarchically, retaining their intermediate notes. Source
paths and line numbers identify evidence. Model judgments remain fallible: check
the evidence before approving proposals.

Reports live in `~/docs/session-reviews/<UTC timestamp>/report.md`, alongside
per-session notes, synthesis notes when needed, and `manifest.json`. A hidden
`.TIMESTAMP.in-progress` directory preserves partial work on failure; only successful
runs are renamed into completed report directories. No automatic retries or cleanup
are performed. An advisory file lock prevents overlapping manual and scheduled
workers sharing the same output directory; separate output directories do not
share that lock. A contender exits before reading session snapshots or invoking OMP.
Publication is a same-filesystem directory rename after every model call succeeds;
it is not a power-loss durability guarantee.

## Safety and privacy

The model runs with `--no-session --no-tools --no-extensions --no-skills --no-rules
--no-context-files --no-title --no-lsp`; it cannot execute its proposals or post them
to Discord through tools. It receives a dedicated review prompt rather than project
instructions. Source records, current `~/AGENTS.md`, and intermediate review notes
are untrusted evidence, never authority to apply proposals. Its sessions are
ephemeral, preventing newly generated reviewer sessions from entering discovery.
The worker excludes its report output tree, but cannot identify arbitrary historical
review transcripts manually saved elsewhere as ordinary work sessions.
The worker itself writes only review artifacts; OMP may update runtime/auth state.

The system service runs as `hephaestus`, not root. Its filesystem is read-only except
for reports, OMP runtime/authentication state, cache, and private temporary storage.
Reports are private (directory mode 700, file mode 600). The service does not load
the Discord token file or bridge `.env`; inherited `DISCORD_*` variables are removed.

Hidden reasoning, image bodies, credential-pin metadata, usage accounting, and
native replay/archive payloads are not included. Records are projected onto
visible text, tool calls/results, and compaction/branch summaries before redaction.
Common credential keys, quoted JSON assignments, secret-bearing environment
assignments, and token patterns are redacted before model calls and report writes.
This is best-effort, not a general secret detector: remaining session content is
sent to the configured OMP model provider. Reports can contain sensitive operational
details and must not be published automatically.

No `AGENTS.md` changes or proposed procedural documents are applied automatically.
Approve them separately. This worker does not deliver notifications to Discord.

## Install and operate

These workstation-specific units use the same checkout and user as the bridge.
They require no bridge rebuild or restart:

```sh
install -d -m 700 ~/docs/session-reviews
sudo install -m 644 omp-session-review.service omp-session-review.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now omp-session-review.timer
systemctl list-timers omp-session-review.timer
sudo systemctl start omp-session-review.service
journalctl -u omp-session-review.service
```

`Persistent=true` causes one run after a missed schedule, not one per missed slot.
It still reviews only the immediately preceding eight hours: downtime older than
that is not backfilled. The schedule does not wait eight hours after review completion.
A running service is not launched again. Each model call is limited to 15 minutes;
the service has a six-hour deadline. Failures are visible in systemd/journald, never
published as successful reviews. No automatic model-output content guarantee exists.

The installed service executes this checkout's script directly: source edits take
effect at its next invocation without a bridge rebuild or restart. A successful
historical run does not verify subsequent source changes.

The test suite uses temporary roots and fake provider responses/executables, not
real session logs or model calls. Consumer-visible assertions cover:

- Registered-only external parents/children, extensionless registered files,
  canonical-path deduplication, stale/empty entries, and explicit-root replacement.
- Mixed evidence at the exclusive end; successive-window delayed, incomplete,
  and boundary tails; later recovery as context; empty-window/no-model behavior.
- Output-tree recursion exclusion, including registered files and symlink aliases.
- Visible-evidence projection/redaction and private report permissions.
- A fake CLI boundary exercising disabled tools/context/extensions, ephemeral
  sessions, isolated working directory, separated evidence/system prompt, and
  removal of inherited `DISCORD_*` variables. This does not prove real model
  obedience to untrusted-evidence instructions or independently sandbox manual runs.
- Actual lock contention, staged publication visibility, successful rename, and
  partial-work retention when synthesis fails.
- Provider completion/abort/error/timeout handling and lossless prompt splitting.

Run the focused suite after source changes; listing a scenario here is not evidence
that the current checkout has passed. Service sandbox and actual installed OMP flag
semantics require separate verification. The worker still reads complete snapshots
and selected histories into memory; prompt segmentation is not an overall memory
bound.

```sh
sudo systemctl disable --now omp-session-review.timer
# If a review is currently running, stop it separately:
sudo systemctl stop omp-session-review.service
python -m unittest discover -s scripts -p 'test_session_review.py'
```
