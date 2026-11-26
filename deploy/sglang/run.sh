#!/usr/bin/env bash
set -euo pipefail

bind_ip=${1:?usage: bash deploy/sglang/run.sh PRIVATE_IP [HF_CACHE_DIR]}
cache_dir=${2:-"$HOME/.cache/huggingface"}
image='lmsysorg/sglang@sha256:f7876bef696ccf9cf75a2bf7ccb82970f5fac577f082fb8ca95c9a59d2160eb5'
revision='b968826d9c46dd6066d109eabc6255188de91218'
mkdir -p "$cache_dir"
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv
docker pull --platform linux/amd64 "$image"
docker run --detach --name agentbox-sglang --gpus device=0 --shm-size=8g \
  --publish "$bind_ip:30000:30000" \
  --volume "$cache_dir:/root/.cache/huggingface" \
  --entrypoint python3 "$image" -m sglang.launch_server \
  --host 0.0.0.0 --port 30000 \
  --model-path Qwen/Qwen3-8B --revision "$revision" --served-model-name Qwen/Qwen3-8B \
  --dtype bfloat16 --context-length 4096 --max-running-requests 1 \
  --mem-fraction-static 0.88 --attention-backend triton \
  --cuda-graph-backend-decode disabled --cuda-graph-backend-prefill disabled \
  --log-requests --log-requests-level 0 --enable-metrics \
  --tool-call-parser qwen25 --default-chat-template-kwargs '{"enable_thinking":false}'
