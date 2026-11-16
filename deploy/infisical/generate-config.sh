#!/usr/bin/env bash
# Nonsecret configuration only. No network, credentials, installation or restart.
set -euo pipefail
usage() {
  printf '%s\n' 'Usage: bash generate-config.sh --endpoint URL [--environment dev] [--project ID]' \
    '       [--discord-project ID] [--notebook-project ID] [--decision-project ID]' \
    '       [--discord-only] (omit Notebook/decision access when retrieval is disabled)' \
    '       [--discord-path /] [--notebook-path /] [--decision-path /]' \
    '       [--runtime-dir /run/omp-discord-bridge] [--unit-name omp-discord-bridge-infisical.service]' \
    '       For user services, use --runtime-dir /run/user/UID/omp-discord-bridge.' \
    'Each mapping uses its explicit ID or falls back to --project. YAML goes to stdout.'
}
fail() { printf '%s\n' "$1" >&2; exit 2; }
project= discord= notebook= decision= endpoint= environment=dev
discord_path=/ notebook_path=/ decision_path=/
discord_only=false
runtime_dir=/run/omp-discord-bridge unit_name=omp-discord-bridge-infisical.service
while (($#)); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --discord-only) discord_only=true; shift ;;
    --project|--discord-project|--notebook-project|--decision-project|--endpoint|--environment|--discord-path|--notebook-path|--decision-path|--runtime-dir|--unit-name)
      (($# >= 2)) || fail "Missing value for $1"
      [[ -n $2 ]] || fail "Empty value for $1"
      case "$1" in
        --project) project=$2 ;;
        --discord-project) discord=$2 ;;
        --notebook-project) notebook=$2 ;;
        --decision-project) decision=$2 ;;
        --endpoint) endpoint=$2 ;;
        --environment) environment=$2 ;;
        --discord-path) discord_path=$2 ;;
        --notebook-path) notebook_path=$2 ;;
        --decision-path) decision_path=$2 ;;
        --runtime-dir) runtime_dir=$2 ;;
        --unit-name) unit_name=$2 ;;
      esac
      shift 2 ;;
    *) fail "Unknown option: $1" ;;
  esac
done
discord=${discord:-$project}
notebook=${notebook:-$project}
decision=${decision:-$project}
ids=("$discord")
if ! "$discord_only"; then ids+=("$notebook" "$decision"); fi
for id in "${ids[@]}"; do
  [[ $id =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || fail 'Supply the required project mappings (letters, digits, underscore, hyphen only).'
done
[[ $environment =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || fail 'Invalid environment slug.'
# Deliberately excludes userinfo, queries, fragments, quoting and template syntax.
[[ $endpoint =~ ^https?://[A-Za-z0-9][A-Za-z0-9.-]*(:[0-9]+)?(/[A-Za-z0-9._/-]*)?$ ]] || fail 'Supply a direct API origin/base URL with --endpoint (http[s], DNS/IPv4, optional port/path).'
[[ $endpoint != *'/../'* && $endpoint != */.. ]] || fail 'Endpoint must not contain parent path components.'
for secret_path in "$discord_path" "$notebook_path" "$decision_path"; do
  [[ $secret_path =~ ^/([A-Za-z0-9_-]+/)*[A-Za-z0-9_-]*$ ]] || fail 'Secret paths must be absolute and contain letters, digits, underscores, hyphens and slashes only.'
done
[[ $runtime_dir =~ ^/run/(user/[0-9]+/)?[A-Za-z0-9_-]+$ ]] || fail 'Runtime directory must be a named child of /run or /run/user/UID.'
[[ $unit_name =~ ^[A-Za-z0-9_-]+[.]service$ ]] || fail 'Unit name must be a simple .service name.'
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
template=$(<"$script_dir/agent.example.yaml")
if "$discord_only"; then template=${template%%$'\n# Optional notebook retrieval'*}; fi
template=${template//@@ENDPOINT@@/$endpoint}
template=${template//@@ENVIRONMENT@@/$environment}
template=${template//@@DISCORD_PROJECT@@/$discord}
template=${template//@@NOTEBOOK_PROJECT@@/$notebook}
template=${template//@@DECISION_PROJECT@@/$decision}
template=${template//@@DISCORD_PATH@@/$discord_path}
template=${template//@@NOTEBOOK_PATH@@/$notebook_path}
template=${template//@@DECISION_PATH@@/$decision_path}
template=${template//@@RUNTIME_DIR@@/$runtime_dir}
template=${template//@@RUNTIME_ROOT@@/${runtime_dir%/*}}
template=${template//@@UNIT_NAME@@/$unit_name}
# Remove only the example-only first comment.
printf '%s\n' "${template#*$'\n'}"
