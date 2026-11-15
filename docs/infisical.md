# Services-host Infisical file credentials

These are deployment assets, not an installer. Hephaestus owns repository changes;
services-bot owns provisioning, installation, configuration, restart and runtime
proof on services. Nothing here deploys, enables or restarts automatically. The
note-retrieval worktree at `decb147` matches installed release `2026-11-15-1`;
local `main` is older and must not be substituted for that release.

## Contract and selected files

Keep the installed binary, `ExecStart`, selected TOML filename, User/Group, HOME,
PATH, working directory, models, notebook IDs/endpoints and SQLite database
unchanged. No Rust SDK, wrapper, environment credentials or `JEV_KEY` are needed.
The existing three TOML settings already read protected files:

| TOML setting | Infisical secret name | Rendered file |
| --- | --- | --- |
| `discord.token_file` | `DISCORD_BOT_TOKEN` | `/run/omp-discord-bridge/discord-token` |
| `omp.note_retrieval.password_file` | `NOTEBOOK_PASSWORD` | `/run/omp-discord-bridge/notebook-password` |
| `omp.note_retrieval.decision_key_file` | `DECISION_KEY` | `/run/omp-discord-bridge/decision-key` |

Deployment selections (all paths are on services):

- `deploy/infisical/agent.example.yaml`: generator input only, never install raw.
- Generated YAML → `/etc/omp-discord-bridge/infisical/agent.yaml`, root-owned 0644;
  it contains identifiers and paths, not secret values.
  Ensure both `/etc/omp-discord-bridge` and its `infisical` subdirectory are
  traversable by the service account (0755); keep only `credentials` root-only.
  Agent 0.45.8 can panic rather than report an unreadable configuration path.
- `deploy/infisical/omp-discord-bridge-infisical.service` →
  `/etc/systemd/system/omp-discord-bridge-infisical.service`, root-owned 0644.
- `deploy/infisical/20-infisical.conf` →
  `/etc/systemd/system/omp-discord-bridge.service.d/20-infisical.conf`, root-owned 0644.
- Root-only bootstrap files → `/etc/omp-discord-bridge/credentials/client-id` and
  `/etc/omp-discord-bridge/credentials/client-secret` (0600, parent 0700).
- Change only the three table entries in the TOML already selected by installed
  `ExecStart`; do not assume the workstation example's TOML path is installed.

The fetch unit runs as `services`, matching the selected services-host bridge
account. Adapt both User and Group when installing on another host.
Systemd loads root bootstrap through `LoadCredential`, exposes it privately to
the Agent, and creates `/run/omp-discord-bridge` owned by that account with 0700.
The selected CLI is `/usr/local/bin/infisical` (Agent 0.45.8); verify that path
and version on the deployment host before installing the unit.

Each bridge start synchronously starts the oneshot from `ExecStartPre`, including
automatic failure restarts. It has no `RemainAfterExit` and must not be enabled as
a separate boot service or started concurrently by a rotation timer. The existing
bridge `ExecStart` and other pre-start commands are preserved. Fresh fetch removes
all old targets, precreates each 0600, renders with umask 0077, enforces 0600 and
requires all three regular, non-symlink files to be nonempty. A failed render,
validation or timeout fails bridge startup; failure cleanup removes partial
outputs. A setup failure also blocks startup, even if an old runtime file remains
inaccessible to cleanup. There is no fallback to old local files. Agent exits
and revokes its access token (`exit-after-auth` and
`revoke-credentials-on-shutdown` are true), with no token sink or persistent cache.
The runtime directory is preserved after successful oneshot exit so the bridge
can read it; `/run` is ephemeral at reboot. Rotation takes effect on the next
controlled bridge restart, not by editing files under a running bridge.

## Bootstrap and explicit mappings

Services provisions a dedicated Universal Auth machine identity with organization
No Access and read-only Viewer membership on only the required projects. This
instance's built-in Viewer role spans all environments in each assigned project;
selecting `dev` in a template does not enforce a `dev`-only access boundary.
Use a dedicated services Discord project to isolate its bot token. Do not grant
write/admin or bootstrap-project access. Use short-lived access tokens and rotate
long-lived client secrets deliberately. Bootstrap must remain recoverable while
the bridge is stopped.
If Infisical is self-hosted, its own database/encryption/bootstrap credentials
must not depend on this Agent or bridge. Never put bootstrap values in repository,
chat, shell arguments/history, environment exports or logs; services delivers
root-only files through its approved secret channel.

Use the direct reachable Infisical API origin, not a public human SSO/login
endpoint. Prefer TLS with trusted certificates; HTTP is only for an explicitly
approved private transport. Do not disable TLS verification. The generator
requires an explicit endpoint to avoid silently sending bootstrap to a cloud
service. It accepts DNS/IPv4 origins, optional port/base path; it rejects URL
userinfo, query strings, fragments, whitespace and template injection characters.

The services Discord token belongs to its own newly provisioned project and bot.
Never default to or copy the Hephaestus bot token/project. Existing retrieval
secrets may remain in their current project: grant this identity only the needed
read access rather than duplicating secrets into the Discord project.

Set the following **nonsecret** shell variables from services' provisioning record:
`INFISICAL_ENDPOINT`, `SERVICES_DISCORD_PROJECT_ID`, `RETRIEVAL_PROJECT_ID`.
Then, from this checkout:

```bash
bash deploy/infisical/generate-config.sh \
  --endpoint "${INFISICAL_ENDPOINT:?supply direct API endpoint}" \
  --discord-project "${SERVICES_DISCORD_PROJECT_ID:?supply services bot project}" \
  --notebook-project "${RETRIEVAL_PROJECT_ID:?supply retrieval project}" \
  --decision-project "${RETRIEVAL_PROJECT_ID:?supply retrieval project}" \
  --notebook-path /file-credentials --decision-path /file-credentials \
  --environment dev > agent.yaml
```

The existing retrieval mapping at handoff is project
`a7a7d3ea-6bce-4968-a2c8-1907883e8b96`, environment `dev`, path
`/file-credentials`; services must confirm the mapping and authorization. There
is intentionally no default project ID. `--project ID` supplies a common fallback
for mappings not explicitly supplied. `--discord-project`, `--notebook-project`
and `--decision-project` override it individually. Each secret path can be set
with `--discord-path`, `--notebook-path`, `--decision-path` (default `/`). Secret
names are the three exact names in the table. `--environment` defaults to `dev`.
`--runtime-dir` defaults to `/run/omp-discord-bridge`; `--unit-name` defaults to
`omp-discord-bridge-infisical.service` and selects the systemd credential directory.
The generator validates nonsecret substitutions and prints deterministic YAML
only; it never contacts Infisical, fetches/stores secrets or installs anything.

## Isolated proof, then idle-safe activation

1. Services records the installed release/version and selected unit/TOML paths,
   account, HOME/PATH/model and database path. Save protected copies of the
   selected TOML and any existing same-name drop-in; retain old credential files
   for rollback. Do not print credentials when inspecting configurations.
2. Provision bootstrap and permissions, generate YAML, and review mappings.
   Before touching bridge configuration, prove a fetch in isolation. Generate a
   second YAML with `--runtime-dir /run/omp-discord-bridge-infisical-proof` and
   `--unit-name omp-discord-bridge-infisical-proof.service`. Stage a copy of the
   fetch unit under that exact name, changing `RuntimeDirectory` and every render
   target in pre/post commands to the proof directory, and `--config` to a
   separate proof YAML. Keep the same `LoadCredential` sources. Install/reload and
   start **only** the proof unit. Never launch the bridge, gateway or live SQLite
   database as a credential test. A local proof using local mappings does not
   prove the new services identity/project.
3. Confirm oneshot completion, private owner/modes, all three nonempty files and
   absence of an access-token sink, without `cat`, shell tracing or secret logs.
   Exercise denied/missing-secret and unreachable-endpoint failure cases on the
   isolated unit: it must fail and leave no usable partial set; restore correct
   proof configuration afterward. Agent version-specific render/revoke behavior
   requires actual proof, not YAML inspection alone.
4. Before activation, services calls Discord `GET https://discord.com/api/v10/users/@me`
   using the fetched token as a `Bot` Authorization header, loaded directly from
   its file by a trusted client (not a token-bearing command-line argument).
   Record only returned bot ID/name and compare against the expected **services**
   bot/application ID. This HTTP request does not open a gateway. Never accept
   the Hephaestus account merely because authentication succeeds. Independently
   prove authenticated Notebook access and an authorized Decision request using
   the fetched files, keeping credential bodies/headers out of output.
5. Services installs the reviewed production YAML/fetch unit and drop-in, with
   correct account and CLI path. Do not replace the installed base bridge unit.
   Once all active turns and final activity messages have settled, stop the
   existing bridge, update only the three TOML file paths above, daemon-reload,
   and start the existing bridge once. No parallel bridge/gateway session, no
   alternate database, no live database replay or manual recovery experiments.
   Verify release, expected bot identity, normal gateway connection and retrieval
   with the original database intact. A restart during active work can mark a
   turn uncertain and is not safe validation.
6. Only after real services proof and an agreed rollback window remove obsolete
   local credential copies. Delete the isolated proof files/unit/YAML through
   services' secure procedure; do not leave extra rendered secrets in `/run`.

## Rollback

Services waits for idle, stops the bridge, restores the previous selected TOML
and exact previous drop-in (or removes only this new drop-in if none existed),
then daemon-reloads and starts the same installed release once. Keep the database,
session data, HOME, PATH and models untouched. Confirm the original expected bot
identity, not just process liveness. Do not delete or replay database rows.
Remove the fetch/proof units and runtime credentials only when no process uses
them; revoke the machine bootstrap identity after deciding migration is abandoned.
If old credential files were already retired, securely reprovision the previous
sources before restarting; never silently fall back or print fetched values.

## Narrow repository checks (not deployment)

```bash
bash -n deploy/infisical/generate-config.sh
bash deploy/infisical/generate-config.sh --help
bash deploy/infisical/generate-config.sh --endpoint https://infisical.example \
  --discord-project sample-discord --notebook-project sample-retrieval \
  --decision-project sample-retrieval --notebook-path /file-credentials \
  --decision-path /file-credentials
systemd-analyze verify deploy/infisical/omp-discord-bridge-infisical.service
```

The sample IDs are for generation checks, not an installable services mapping.
Also check rejection of missing mappings/endpoint and unsafe quoted input, and
parse generated YAML with the chosen tooling. Verify the unit against the selected
`/usr/local/bin/infisical` installation. These checks do not replace isolated fetch
or services activation.

Repository proof on 2026-11-15: generator validation and systemd unit validation
passed. An isolated oneshot using the installed Agent and existing local
read-only identity fetched all three values exactly, with files 0600 and directory
0700. Missing-secret and unreachable-endpoint starts failed and removed the
rendered credential set. Bootstrap credential files were inaccessible after the
oneshot stopped. No live bridge was restarted. This proves the loading mechanism,
not services-bot's new identity or deployment.

Reference: [Infisical Agent configuration and templates](https://infisical.com/docs/integrations/platforms/infisical-agent).
