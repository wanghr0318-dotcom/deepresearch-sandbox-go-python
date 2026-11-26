#!/usr/bin/env bash
set -euo pipefail

usage='usage: bash deploy/sglang/run.sh PRIVATE_IP [HF_CACHE_DIR] | --autodl VENV_DIR MODEL_DIR EVIDENCE_DIR'
model='Qwen/Qwen3-8B'
revision='b968826d9c46dd6066d109eabc6255188de91218'
launch_args=(
  -m sglang.launch_server --port 30000 --served-model-name "$model"
  --dtype bfloat16 --max-running-requests 1 --mem-fraction-static 0.88
  --attention-backend triton --cuda-graph-backend-decode disabled
  --cuda-graph-backend-prefill disabled --log-requests --log-requests-level 0
  --enable-metrics --tool-call-parser qwen25
  --default-chat-template-kwargs '{"enable_thinking":false}'
)

run_autodl() {
  local venv=${1:?$usage} model_dir=${2:?$usage} evidence=${3:?$usage}
  local python="$venv/bin/python" pid sampler attempt
  local -a args=("${launch_args[@]}" --host 127.0.0.1 --model-path "$model_dir" --context-length 24576)
  test -r "$model_dir/config.json"
  test -r "$model_dir/tokenizer_config.json"
  if curl -sS --connect-timeout 1 --max-time 2 http://127.0.0.1:30000/v1/models >/dev/null 2>&1; then
    echo 'An HTTP service is already answering on port 30000.' >&2
    return 1
  fi
  umask 077
  mkdir "$evidence"
  date -Is > "$evidence/started-at.txt"
  printf '%s\n' "$revision" > "$evidence/expected-model-revision.txt"
  printf '%q ' "$python" "${args[@]}" > "$evidence/command.txt"
  printf '\n' >> "$evidence/command.txt"
  find "$model_dir" -maxdepth 1 -type f -printf '%f\t%s\n' | sort > "$evidence/model-files.tsv"
  nvidia-smi --query-gpu=name,driver_version,memory.total,memory.used --format=csv > "$evidence/gpu-before.csv"
  "$python" - <<'PY' > "$evidence/runtime.txt"
import platform
from importlib.metadata import distributions, version

sglang = version("sglang")
if sglang != "0.5.23":
    raise SystemExit(f"expected SGLang 0.5.23, found {sglang}")
print("Python", platform.python_version())
for package in sorted({f'{d.metadata["Name"]}=={d.version}' for d in distributions()}):
    print(package)
PY

  nohup nvidia-smi --query-gpu=timestamp,memory.used,utilization.gpu --format=csv -l 1 \
    > "$evidence/gpu-samples.csv" 2> "$evidence/gpu-samples.stderr" </dev/null &
  sampler=$!
  printf '%s\n' "$sampler" > "$evidence/sampler.pid"
  nohup "$python" "${args[@]}" > "$evidence/sglang.log" 2>&1 </dev/null &
  pid=$!
  printf '%s\n' "$pid" > "$evidence/sglang.pid"

  for ((attempt=0; attempt<90; attempt++)); do
    if ! kill -0 "$pid" "$sampler" 2>/dev/null; then
      break
    fi
    if curl -fsS --connect-timeout 1 --max-time 3 http://127.0.0.1:30000/v1/models > "$evidence/models.json" 2>/dev/null &&
      "$python" -c 'import json, sys; sys.exit(not any(row["id"] == sys.argv[2] for row in json.load(open(sys.argv[1]))["data"]))' "$evidence/models.json" "$model"; then
      date -Is > "$evidence/ready-at.txt"
      printf 'Model ready: %s\nSGLang PID: %s\nSampler PID: %s\n' "$model" "$pid" "$sampler"
      return 0
    fi
    sleep 2
  done
  kill "$pid" "$sampler" 2>/dev/null || true
  tail -40 "$evidence/sglang.log" "$evidence/gpu-samples.stderr" >&2
  return 1
}

if [[ ${1:-} == --autodl ]]; then
  run_autodl "${@:2}"
  exit
fi

bind_ip=${1:?$usage}
cache_dir=${2:-"$HOME/.cache/huggingface"}
image='lmsysorg/sglang@sha256:f7876bef696ccf9cf75a2bf7ccb82970f5fac577f082fb8ca95c9a59d2160eb5'
mkdir -p "$cache_dir"
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv
docker pull --platform linux/amd64 "$image"
docker run --detach --name agentbox-sglang --gpus device=0 --shm-size=8g \
  --publish "$bind_ip:30000:30000" \
  --volume "$cache_dir:/root/.cache/huggingface" \
  --entrypoint python3 "$image" "${launch_args[@]}" \
  --host 0.0.0.0 --model-path "$model" --revision "$revision" --context-length 4096
