# shellcheck shell=bash
# 模型网关脚本（scripts/demo-gateway.sh、scripts/loadtest-gateway.sh、scripts/fault-gateway.sh）的共用函数：
# docker（原生，或 WSL2 中 Docker Desktop 的 docker.exe）、全新的演示数据库、fake upstream 进程、带 --model-api 的
# agentbox server 的启动与停止、psql。全部在本机，不访问外网，没有模型费用；不需要 root（server 以 --exec-slots 0
# 启动，这些脚本不运行沙箱任务）。由调用者 source；调用者先设置 GW_NAME（用于库名、目录名与容器名）。
#
# 环境变量（三个脚本共用）：
#   AGENTBOX_DATABASE_URL     PostgreSQL 管理连接串（默认 postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable；
#                             脚本在其中新建并重建自己的库）
#   AGENTBOX_GW_BIN           二进制目录（默认 <仓库>/bin）；AGENTBOX_GW_SKIP_BUILD=1 时不重新构建
#   AGENTBOX_GW_WORK          工作目录（数据目录、日志、结果；默认 /tmp/agentbox-<GW_NAME>，每次清空；须在 Linux 文件系统上，
#                             因为 server 要求数据目录中的 api.token 为 0600）
#   AGENTBOX_GW_LISTEN        server 监听（默认 127.0.0.1:18480）；AGENTBOX_GW_METRICS_LISTEN /metrics（默认 127.0.0.1:19464）

GW_REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
GW_BIN="${AGENTBOX_GW_BIN:-$GW_REPO/bin}"
GW_PG_ADMIN_URL="${AGENTBOX_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
GW_WORK="${AGENTBOX_GW_WORK:-/tmp/agentbox-$GW_NAME}"
GW_LISTEN="${AGENTBOX_GW_LISTEN:-127.0.0.1:18480}"
GW_METRICS="${AGENTBOX_GW_METRICS_LISTEN:-127.0.0.1:19464}"
GW_ADDR="http://$GW_LISTEN"
GW_DATA="$GW_WORK/data"
GW_SERVER_PID=""
GW_FAKE_PIDS=()
GW_SERVER_N=0

gw_say() { printf '%s\n' "$*"; }
gw_fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

gw_wait_for() {
  local what=$1 limit=$2 i
  shift 2
  for ((i = 0; i < limit * 10; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  gw_fail "等待超时（${limit}s）：$what"
}

# ---- docker ----

GW_DOCKER=""
GW_WINPATH=""
gw_detect_docker() {
  if command -v docker >/dev/null 2>&1 && timeout 20 docker version >/dev/null 2>&1; then
    GW_DOCKER=docker
  elif [ -x "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe" ]; then
    GW_DOCKER="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
    GW_WINPATH=1
  else
    gw_fail "需要 docker（WSL2 中可使用 Docker Desktop）"
  fi
}
gw_hostpath() { if [ -n "$GW_WINPATH" ]; then wslpath -w "$1"; else printf '%s' "$1"; fi; }
gw_docker() { "$GW_DOCKER" "$@"; }
gw_compose() { "$GW_DOCKER" compose -f "$(gw_hostpath "$GW_REPO/deploy/docker-compose.yml")" "$@"; }

# ---- PostgreSQL ----

gw_db_url() { python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(path='/'+sys.argv[2])))" "$GW_PG_ADMIN_URL" "$1"; }

# gw_psql DB SQL：以 -At 执行，打印结果（psql 不存在时经 compose 的 postgres 服务执行）。
gw_psql() {
  local db=$1 sql=$2
  if command -v psql >/dev/null 2>&1; then
    psql -v ON_ERROR_STOP=1 -At "$(gw_db_url "$db")" -c "$sql"
  else
    gw_compose exec -T postgres psql -v ON_ERROR_STOP=1 -U agentbox -d "$db" -At -c "$sql"
  fi
}

# gw_fresh_db NAME：删除并重建库 NAME，导出 AGENTBOX_DATABASE_URL 指向它。
gw_fresh_db() {
  gw_psql postgres "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null || gw_fail "无法删除库 $1（需要 psql 或 deploy/docker-compose.yml 的 postgres 服务）"
  gw_psql postgres "CREATE DATABASE $1" >/dev/null || gw_fail "无法创建库 $1"
  GW_DB=$1
  AGENTBOX_DATABASE_URL="$(gw_db_url "$1")"
  export AGENTBOX_DATABASE_URL
}

gw_sql() { gw_psql "$GW_DB" "$1"; }

# ---- 构建 ----

gw_build() {
  mkdir -p "$GW_BIN"
  if [ "${AGENTBOX_GW_SKIP_BUILD:-}" = 1 ]; then
    for b in agentbox fakeupstream agentbox-loadtest; do [ -x "$GW_BIN/$b" ] || gw_fail "缺少 $GW_BIN/$b（去掉 AGENTBOX_GW_SKIP_BUILD）"; done
    return
  fi
  command -v go >/dev/null 2>&1 || gw_fail "需要 Go（构建 server、fake upstream 与 agentbox-loadtest）"
  (cd "$GW_REPO" && CGO_ENABLED=0 go build -o "$GW_BIN/" ./cmd/agentbox ./cmd/agentbox-loadtest ./tests/e2e/fakeupstream/cmd/fakeupstream)
}

# ---- 进程 ----

gw_stop_pid() {
  local pid=$1
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

gw_cleanup() {
  gw_stop_pid "$GW_SERVER_PID"
  local p
  for p in "${GW_FAKE_PIDS[@]}"; do gw_stop_pid "$p"; done
  GW_FAKE_PIDS=()
}

# gw_start_fake NAME [fakeupstream 参数…]：启动 fake upstream，设置 FAKE_<NAME>_URL（根地址，Anthropic 供应商的
# base_url）、FAKE_<NAME>_V1（OpenAI 供应商的 base_url）、FAKE_<NAME>_HOSTPORT 与 FAKE_<NAME>_PID。
gw_start_fake() {
  local name=$1 log="$GW_WORK/fake-$1.log" url v1 hp
  shift
  "$GW_BIN/fakeupstream" "$@" >"$log" 2>&1 &
  local pid=$!
  GW_FAKE_PIDS+=("$pid")
  gw_wait_for "fake upstream $name 打印地址" 30 grep -q '^url=' "$log"
  read -r url v1 hp < <(sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p' "$log")
  printf -v "FAKE_${name}_URL" '%s' "$url"
  printf -v "FAKE_${name}_V1" '%s' "$v1"
  printf -v "FAKE_${name}_HOSTPORT" '%s' "$hp"
  printf -v "FAKE_${name}_PID" '%s' "$pid"
}

gw_server_ready() { curl -fsS "$GW_ADDR/status" | grep -q '"normal"'; }

# gw_new_data_dir：清空数据目录并写入运维 token（0600，不打印）。
gw_new_data_dir() {
  rm -rf "$GW_DATA"
  mkdir -p "$GW_DATA"
  (umask 077; od -An -N24 -tx1 /dev/urandom | tr -d ' \n' >"$GW_DATA/api.token")
  GW_TOKEN=$(cat "$GW_DATA/api.token")
}

# gw_start_server [额外 server 参数…]：以 GW_SERVER_FLAGS 加额外参数启动 server，等它进入 normal 模式。
# 每次启动的 stderr 写到 $GW_WORK/server-<n>.log。供应商 Key 是随机的假值（fake upstream 不校验 OpenAI 的 Key）。
gw_start_server() {
  GW_SERVER_N=$((GW_SERVER_N + 1))
  GW_SERVER_LOG="$GW_WORK/server-$GW_SERVER_N.log"
  if curl -fsS "$GW_ADDR/status" >/dev/null 2>&1; then gw_fail "$GW_LISTEN 上已有服务在运行"; fi
  AGENTBOX_MODEL_API_KEY="${GW_MODEL_KEY:-fake-key-$RANDOM$RANDOM}" "$GW_BIN/agentbox" server "${GW_SERVER_FLAGS[@]}" "$@" >"$GW_SERVER_LOG" 2>&1 &
  GW_SERVER_PID=$!
  local i
  for ((i = 0; i < 300; i++)); do
    if gw_server_ready >/dev/null 2>&1; then return 0; fi
    kill -0 "$GW_SERVER_PID" 2>/dev/null || { tail -n 20 "$GW_SERVER_LOG" >&2; gw_fail "server 退出"; }
    sleep 0.1
  done
  tail -n 20 "$GW_SERVER_LOG" >&2
  gw_fail "server 30 s 内没有进入 normal 模式"
}

gw_stop_server() { gw_stop_pid "$GW_SERVER_PID"; GW_SERVER_PID=""; }

# gw_kill9_server：模拟崩溃（SIGKILL，不做任何清理）。
gw_kill9_server() {
  kill -KILL "$GW_SERVER_PID" 2>/dev/null || true
  wait "$GW_SERVER_PID" 2>/dev/null || true
  GW_SERVER_PID=""
}

# gw_base_flags OPENAI_V1 HOSTPORTS FALLBACK_FILE MODELS：标准的 server 参数（账号 + 模型 API + 降级链文件）。
gw_base_flags() {
  local v1=$1 hostports=$2 fallback=$3 models=$4 first=${4%%,*}
  GW_SERVER_FLAGS=(--data-dir "$GW_DATA" --listen "$GW_LISTEN" --exec-slots 0
    --model-base-url "$v1" --model-name "$first" --models "$models"
    --user-orchestrator-model "$first" --user-worker-model "$first"
    --model-price-in-micro-per-mtok 1000000 --model-price-out-micro-per-mtok 2000000
    --search-provider fake --search-base-url "${v1%/v1}" --upstream-allow-private "$hostports"
    --model-api --model-fallback-file "$fallback" --metrics-listen "$GW_METRICS")
}

# gw_api METHOD PATH [JSON]：以运维 token 调用 API，打印正文。
gw_api() {
  local m=$1 p=$2
  shift 2
  curl -sS -X "$m" -H "Authorization: Bearer $GW_TOKEN" -H 'Content-Type: application/json' "$GW_ADDR$p" "$@"
}

gw_json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

gw_verify() { "$GW_BIN/agentbox" verify-invariants --data-dir "$GW_DATA" "$@"; }

# gw_reconcile：重新汇总并对账本月（usage_daily、model_requests、quota_holds 与 quota_budgets）；有差异时非零。
gw_reconcile() { "$GW_BIN/agentbox" bill reconcile "$(date -u +%Y-%m)"; }
