#!/usr/bin/env bash
# Run on the authorized GPU host. No changes to existing model services.
set -euo pipefail
image=hephaestus/qwen3-asr:trial
cache="$HOME/hephaestus-qwen3-asr/cache"
test -s "$HOME/hephaestus-qwen3-asr/api.env"
mkdir -p "$cache"
chmod 700 "$(dirname "$cache")" "$cache"
exec docker run -d --name hephaestus-qwen3-asr \
  --gpus device=0 --network host --memory 8g --cpus 4 --shm-size 1g \
  --env-file "$HOME/hephaestus-qwen3-asr/api.env" \
  --ulimit core=0 --user "$(id -u):$(id -g)" \
  --env HOME=/cache --env XDG_CACHE_HOME=/cache \
  --env LD_LIBRARY_PATH=/usr/local/cuda/compat/lib.real:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64 \
  --mount "type=bind,src=$cache,dst=/cache" \
  --entrypoint python "$image" -m vllm.entrypoints.openai.api_server \
  --model openai/whisper-large-v3-turbo \
  --revision 41f01f3fe87f28c78e2fbf8b568835947dd65ed9 \
  --served-model-name voice-asr \
  --dtype float16 --gpu-memory-utilization 0.04 \
  --host 0.0.0.0 --port 18086 \
  --max-model-len 448 --max-num-seqs 1 --max-num-batched-tokens 2048 \
  --limit-mm-per-prompt '{"audio":1}' \
  --enforce-eager --no-enable-log-requests
