#!/usr/bin/env bash
# M2 真实研究演示：真实沙箱 + 真实 Gateway 运行一个 DeepResearch 任务，逐步打印每一步及其结果。以 root 在
# Linux（cgroup v2）或 WSL2 中运行。
#
#   真实模型（OpenAI 兼容端点，默认 Moonshot；Key 只从环境变量或仓库根的 .env 读取 AGENTBOX_MODEL_API_KEY 这一行）：
#     sudo bash scripts/demo-m2.sh
#   演练（fake upstream 扮演模型、搜索与网页，不访问外网，其余流程完全相同）：
#     sudo AGENTBOX_DEMO_FAKE=1 bash scripts/demo-m2.sh
#
# 模型路由（multi-agent）：主 agent（编排：计划与报告）用 AGENTBOX_DEMO_ORCHESTRATOR_MODEL（默认 kimi-k3），任务内的
# worker（总结）用 AGENTBOX_DEMO_WORKER_MODEL（默认 kimi-k2.6）。server 以 --model-name <worker 模型>
# --models <白名单> 启动，任务 config 带 orchestrator_model 与 worker_model；inspect 明细显示每个调用的模型（取自
# 结果 blob 中上游回复的 model 字段；fake upstream 原样回显请求的 model）并检查路由。费用按配置的单价估算与结算，
# 不代表供应商的实际计费。
#
# 步骤：环境检查（root、cgroup v2、PostgreSQL、python3、模型与搜索配置、Key 已设置——只报告"已设置"，从不打印）
# → 全新的数据目录与数据库 → 安装 worker 包（含 deepresearch）到 /opt/agentbox → 构建、doctor → 启动 server
# （--worker-argv python3,-m,deepresearch）→ 提交研究任务 → G3：Worker 运行中从宿主读取沙箱内每个进程的
# /proc/<pid>/environ 与 cmdline，不含 Key 的值与变量名 → 等待 task_terminal → inspect 的调用明细（端点、模型、
# tries、费用、延迟；不含正文）与总费用 → 报告前 40 行与证据列表（报告从 BlobStore 读取并校验 sha256）→ 环境清理 →
# 正常停止 → verify-invariants --quiescent → 泄漏检查（cgroup、环境目录、沙箱进程）→ Key 不出现在 server 日志、
# 事件流与 inspect 输出中。任何一步失败即以非零状态退出，并打印失败的步骤名。
#
# 环境变量：
#   AGENTBOX_DEMO_FAKE=1                 演练模式（启动 tests/e2e/fakeupstream 的独立进程；不读取 .env，未设置
#                                        AGENTBOX_MODEL_API_KEY 时使用随机生成的假 Key）
#   AGENTBOX_DEMO_MODEL_BASE_URL         真实模式：OpenAI 兼容模型上游地址（--model-base-url；默认
#                                        https://api.moonshot.cn/v1）
#   AGENTBOX_DEMO_ORCHESTRATOR_MODEL     编排模型（计划与报告；默认 kimi-k3）
#   AGENTBOX_DEMO_WORKER_MODEL           worker 模型（任务内总结；默认 kimi-k2.6；也是 server 的 --model-name）
#   AGENTBOX_DEMO_MODELS                 声明的模型白名单（--models，逗号分隔；默认
#                                        kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3；须包含上面两个）
#   AGENTBOX_DEMO_SEARCH_PROVIDER        ddg_lite（默认，无 Key）| tavily（需要 AGENTBOX_SEARCH_API_KEY）|
#                                        serper（Google 结果，需要 AGENTBOX_SERPER_API_KEY；脚本把它作为
#                                        AGENTBOX_SEARCH_API_KEY 交给 server，因此 Tavily 与 Serper 的 Key 可并存于 .env）
#   AGENTBOX_DEMO_TOPIC                  研究题目（默认见下方 TOPIC）
#   AGENTBOX_DEMO_PRICE_IN_MICRO_PER_MTOK / AGENTBOX_DEMO_PRICE_OUT_MICRO_PER_MTOK
#                                        可选：默认模型输入、输出单价（每百万 token 的微美元）
#   AGENTBOX_DEMO_MODEL_PRICES           可选：按模型的单价（--model-price，model=IN:OUT，逗号分隔；演练模式默认
#                                        <编排模型>=2000000:2000000，其余模型 1000000:1000000）
#   AGENTBOX_DEMO_BUDGET_MICRO           可选：该任务的 limits.budget_micro（默认取 server 的 2000000）
#   AGENTBOX_DEMO_UPSTREAM_ALLOW_PRIVATE 可选：显式放行的私有上游（例如本机模型服务的 host:port）
#   AGENTBOX_DEMO_ENV_FILE               Key 文件（默认 <仓库根>/.env；只读取 AGENTBOX_MODEL_API_KEY 与所选搜索供应商
#                                        的 Key 行（AGENTBOX_SEARCH_API_KEY 或 AGENTBOX_SERPER_API_KEY），不 source 整个文件）
#   AGENTBOX_DEMO_TIMEOUT                等待任务结束的秒数（默认 1800）
#   AGENTBOX_DATABASE_URL、AGENTBOX_DEMO_DB、AGENTBOX_DEMO_DATA_DIR、AGENTBOX_DEMO_DATABASE_URL、
#   AGENTBOX_DEMO_LISTEN                 与 scripts/demo-m1.sh 相同（默认库 agentbox_demo_m2、数据目录
#                                        /var/lib/agentbox-demo-m2、监听 127.0.0.1:8080）
#   AGENTBOX_DEMO_WEB_DIR                可选：工作台构建产物目录（例如 <仓库>/web/dist）；设置后 server 以
#                                        --web-dir 同源提供工作台，可在浏览器中观察研究任务；不设置时行为不变
#   AGENTBOX_DEMO_REDIS_ADDR             可选：共享缓存的 Redis 地址（例如 127.0.0.1:6379）→ --redis-addr；设置后
#                                        inspect 一步打印 /status 的缓存计数与各调用的来源（upstream|cache|coalesced）
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
BIN="$REPO/bin/agentbox"
FU_BIN="$REPO/bin/fakeupstream"
PG_ADMIN_URL="${AGENTBOX_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
DEMO_DB="${AGENTBOX_DEMO_DB:-agentbox_demo_m2}"
DATA="${AGENTBOX_DEMO_DATA_DIR:-/var/lib/agentbox-demo-m2}"
LISTEN="${AGENTBOX_DEMO_LISTEN:-127.0.0.1:8080}"
export AGENTBOX_ADDR="http://$LISTEN"
FAKE="${AGENTBOX_DEMO_FAKE:-}"
TOPIC="${AGENTBOX_DEMO_TOPIC:-固态电池的产业化进展与主要技术瓶颈}"
SEARCH_PROVIDER="${AGENTBOX_DEMO_SEARCH_PROVIDER:-ddg_lite}"
MODEL_BASE_URL="${AGENTBOX_DEMO_MODEL_BASE_URL:-https://api.moonshot.cn/v1}"
ORCH_MODEL="${AGENTBOX_DEMO_ORCHESTRATOR_MODEL:-kimi-k3}"
WORKER_MODEL="${AGENTBOX_DEMO_WORKER_MODEL:-kimi-k2.6}"
MODELS="${AGENTBOX_DEMO_MODELS:-kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3}"
MODEL_PRICES="${AGENTBOX_DEMO_MODEL_PRICES:-}"
TIMEOUT="${AGENTBOX_DEMO_TIMEOUT:-1800}"
ENV_FILE="${AGENTBOX_DEMO_ENV_FILE:-$REPO/.env}"
WORKER_DIR=/opt/agentbox
CGROOT=/sys/fs/cgroup
LOGDIR=$(mktemp -d /tmp/agentbox-demo-m2.XXXXXX)
SERVER_LOG="$LOGDIR/server.log"
EVENTS="$LOGDIR/events.jsonl"
SERVER_PID=""
FU_PID=""
WATCH_PID=""
STEP="初始化"
N=0

step() { N=$((N + 1)); STEP="$1"; printf '\n[%02d] %s\n' "$N" "$1"; }
ok() { printf '     OK  %s\n' "$*"; }
info() { printf '         %s\n' "$*"; }
fail() { printf '     FAIL %s\n' "$*" >&2; exit 1; }

stop_pid() {
  local pid=$1
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

on_exit() {
  local rc=$?
  stop_pid "$WATCH_PID"
  stop_pid "$SERVER_PID"
  stop_pid "$FU_PID"
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%02d] %s（退出码 %d）。日志目录：%s\n' "$N" "$STEP" "$rc" "$LOGDIR" >&2
    # 只在 Key 未出现在日志中时回显日志尾部（检查本身不打印 Key）。
    if [ -f "$SERVER_LOG" ] && no_secret "server 日志" "$SERVER_LOG" >/dev/null 2>&1; then
      tail -n 20 "$SERVER_LOG" >&2 || true
    fi
  else
    printf '\n演示完成：全部 %d 步通过。日志目录：%s\n' "$N" "$LOGDIR"
  fi
}
trap on_exit EXIT

# json <python 表达式>：从标准输入读取 JSON 为 d，打印表达式的值。
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
# jsonl <python 表达式>：标准输入每行一个 JSON（d），逐行打印表达式的值。
jsonl() { python3 -c "import json,sys; [print($1) for d in (json.loads(l) for l in sys.stdin if l.strip())]"; }

# wait_for <描述> <秒> <命令...>：轮询命令直到成功；超时失败。
wait_for() {
  local what=$1 limit=$2 i
  shift 2
  for ((i = 0; i < limit * 10; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  fail "等待超时（${limit}s）：$what"
}

# ---- Key：只保存在本 shell 的非导出变量中；只经环境变量前缀交给 server；检查时经标准输入交给 python ----

MODEL_KEY=""
SEARCH_KEY=""

# env_file_value <变量名>：从 ENV_FILE 读取该变量最后一次赋值的值（去掉 export 前缀、首尾空白、引号与 CR）；
# 不 source 文件，不读取其他变量。
env_file_value() {
  local name=$1 line val
  [ -r "$ENV_FILE" ] || return 0
  line=$(grep -E "^[[:space:]]*(export[[:space:]]+)?$name=" "$ENV_FILE" | tail -n 1) || return 0
  val=${line#*=}
  val=${val%$'\r'}
  val=$(printf '%s' "$val" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')
  case "$val" in
    \"*\") val=${val#\"}; val=${val%\"} ;;
    \'*\') val=${val#\'}; val=${val%\'} ;;
  esac
  printf '%s' "$val"
}

# secrets：每行一个非空 Key（printf 是 shell 内建命令，Key 不出现在任何进程的命令行中）。
secrets() {
  [ -z "$MODEL_KEY" ] || printf '%s\n' "$MODEL_KEY"
  [ -z "$SEARCH_KEY" ] || printf '%s\n' "$SEARCH_KEY"
}

# no_secret <描述> <文件...>：文件中不含任何 Key 的值（只报告位置，不回显 Key）。
no_secret() {
  local where=$1
  shift
  secrets | python3 -c '
import sys
keys = [k.encode() for k in sys.stdin.read().split("\n") if k]
if not keys:
    sys.exit("没有可检查的 Key")
bad = [p for p in sys.argv[2:] if any(k in open(p, "rb").read() for k in keys)]
if bad:
    sys.exit("Key 出现在%s中：%s" % (sys.argv[1], " ".join(bad)))
' "$where" "$@"
}

# ---------------------------------------------------------------------------

inspect_task() { "$BIN" task inspect "$1"; }
task_field() { inspect_task "$1" | json "d['task']['$2']"; }
install_cgroup() { echo "$CGROOT/agentbox-$(cat "$DATA/install_id")"; }
server_ready() { curl -fsS "$AGENTBOX_ADDR/status" | grep -q '"normal"'; }
cleaned() {
  [ "$(inspect_task "$1" | json "all(a['cleanup_state'] == 'done' and a.get('stopped_at') for a in d['attempts'])")" = True ] &&
    [ -z "$(ls -A "$DATA/envs" 2>/dev/null)" ]
}

# env_procs：本安装全部环境 cgroup 中的进程，每行 "pid cmdline"。
env_procs() {
  local cg p
  for cg in "$(install_cgroup)"/env-*; do
    [ -d "$cg" ] || continue
    for p in $(find "$cg" -name cgroup.procs -exec cat {} +); do
      printf '%s %s\n' "$p" "$(tr '\0' ' ' <"/proc/$p/cmdline" 2>/dev/null)"
    done
  done
}
worker_running() { env_procs | grep -qE '^[0-9]+ python3 -m deepresearch'; }

# g3_probe：宿主读取环境 cgroup 中每个进程的 /proc/<pid>/environ 与 cmdline：不含 Key 的值，也不含
# AGENTBOX_MODEL_API_KEY / AGENTBOX_SEARCH_API_KEY / AGENTBOX_SERPER_API_KEY 变量名；须包含 Worker 进程。打印每个进程的 pid、uid、
# cmdline 与环境变量名（不打印值）。
g3_probe() {
  local pids
  pids=$(env_procs | awk '{print $1}')
  [ -n "$pids" ] || fail "环境 cgroup 中没有进程"
  # shellcheck disable=SC2086 # pids 按空白拆分为参数
  secrets | python3 -c '
import sys
keys = [k.encode() for k in sys.stdin.read().split("\n") if k]
if not keys:
    sys.exit("G3 须在配置了非空 Key 时验证")
names = (b"AGENTBOX_MODEL_API_KEY", b"AGENTBOX_SEARCH_API_KEY", b"AGENTBOX_SERPER_API_KEY")
worker = False
for pid in sys.argv[1:]:
    try:
        env = open(f"/proc/{pid}/environ", "rb").read()
        cmd = open(f"/proc/{pid}/cmdline", "rb").read()
        uid = next(l.split()[1] for l in open(f"/proc/{pid}/status") if l.startswith("Uid:"))
    except OSError as e:
        print(f"pid {pid}：已退出（{e.strerror}）")
        continue
    if any(k in env or k in cmd for k in keys):
        sys.exit(f"Key 的值出现在沙箱进程 {pid} 的 environ 或 cmdline 中")
    if any(n in env for n in names):
        sys.exit(f"Key 的变量名出现在沙箱进程 {pid} 的 environ 中")
    argv = cmd.replace(b"\0", b" ").decode("utf-8", "replace").strip()
    vars_ = [e.split(b"=", 1)[0].decode("utf-8", "replace") for e in env.split(b"\0") if e]
    worker = worker or argv.startswith("python3 -m deepresearch")
    listed = ",".join(vars_) or "（无）"
    print(f"pid {pid} uid {uid}：{argv}；环境变量 {len(vars_)} 个：{listed}")
if not worker:
    sys.exit("检查时 Worker（python3 -m deepresearch）已不在环境中")
' $pids
}

# ---------------------------------------------------------------------------

step "环境检查：root、cgroup v2、PostgreSQL、python3、模型与搜索配置"
[ "$(id -u)" = 0 ] || fail "需要 root（sudo bash scripts/demo-m2.sh）"
[ "$(stat -fc %T $CGROOT)" = cgroup2fs ] || fail "$CGROOT 不是 cgroup v2"
ok "root，$CGROOT 为 cgroup2fs，内核 $(uname -r) $(uname -m)"
hostport=$(python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(p.hostname, p.port or 5432)" "$PG_ADMIN_URL")
read -r pghost pgport <<<"$hostport"
timeout 3 bash -c "</dev/tcp/$pghost/$pgport" 2>/dev/null || fail "PostgreSQL $pghost:$pgport 不可达（docker compose -f deploy/docker-compose.yml up -d --wait）"
ok "PostgreSQL $pghost:$pgport 可达"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' || fail "需要 python3 ≥ 3.11（$(python3 --version)）"
ok "$(command -v python3)：$(python3 --version)"
if curl -fsS "$AGENTBOX_ADDR/status" >/dev/null 2>&1; then fail "$LISTEN 上已有服务在运行"; fi
if [ -n "$FAKE" ]; then
  # 演练：不读取 .env；Key 取环境变量，未设置时随机生成（假 Key 只发给本机 fake upstream）。
  MODEL_KEY="${AGENTBOX_MODEL_API_KEY:-fake-demo-key-$(od -An -tx1 -N12 /dev/urandom | tr -d ' \n')}"
  SEARCH_KEY="${AGENTBOX_SEARCH_API_KEY:-}"
  ok "演练模式：模型与搜索上游为本机 fake upstream（不访问外网）；AGENTBOX_MODEL_API_KEY 已设置（演练用假 Key）"
else
  MODEL_KEY="${AGENTBOX_MODEL_API_KEY:-}"
  [ -n "$MODEL_KEY" ] || MODEL_KEY=$(env_file_value AGENTBOX_MODEL_API_KEY)
  [ -n "$MODEL_KEY" ] || fail "AGENTBOX_MODEL_API_KEY 未设置（环境变量或 $ENV_FILE 中的 AGENTBOX_MODEL_API_KEY=...）"
  ok "AGENTBOX_MODEL_API_KEY 已设置"
  ok "模型上游：$MODEL_BASE_URL"
  case "$SEARCH_PROVIDER" in
    ddg_lite) ok "搜索：ddg_lite（无 Key）" ;;
    tavily)
      SEARCH_KEY="${AGENTBOX_SEARCH_API_KEY:-}"
      [ -n "$SEARCH_KEY" ] || SEARCH_KEY=$(env_file_value AGENTBOX_SEARCH_API_KEY)
      [ -n "$SEARCH_KEY" ] || fail "--search-provider tavily 需要 AGENTBOX_SEARCH_API_KEY"
      ok "搜索：tavily；AGENTBOX_SEARCH_API_KEY 已设置"
      ;;
    serper)
      SEARCH_KEY="${AGENTBOX_SERPER_API_KEY:-}"
      [ -n "$SEARCH_KEY" ] || SEARCH_KEY=$(env_file_value AGENTBOX_SERPER_API_KEY)
      [ -n "$SEARCH_KEY" ] || fail "--search-provider serper 需要 AGENTBOX_SERPER_API_KEY（环境变量或 $ENV_FILE 中的 AGENTBOX_SERPER_API_KEY=...）"
      ok "搜索：serper；AGENTBOX_SERPER_API_KEY 已设置（作为 AGENTBOX_SEARCH_API_KEY 交给 server）"
      ;;
    *) fail "AGENTBOX_DEMO_SEARCH_PROVIDER 须为 ddg_lite、tavily 或 serper，得到 $SEARCH_PROVIDER" ;;
  esac
fi
# Key 不留在导出的环境中：只经环境变量前缀交给 server。
unset AGENTBOX_MODEL_API_KEY AGENTBOX_SEARCH_API_KEY AGENTBOX_SERPER_API_KEY
for m in "$ORCH_MODEL" "$WORKER_MODEL"; do
  case ",$MODELS," in *",$m,"*) ;; *) fail "模型 $m 不在白名单 AGENTBOX_DEMO_MODELS=$MODELS 中" ;; esac
done
if [ -n "$FAKE" ] && [ -z "$MODEL_PRICES" ]; then
  MODEL_PRICES="$ORCH_MODEL=2000000:2000000"
fi
ok "模型路由：编排（计划、报告）= $ORCH_MODEL；worker（总结）= $WORKER_MODEL（server 默认模型）；白名单 $MODELS"
[ -z "$MODEL_PRICES" ] || ok "按模型的单价（配置值，不代表供应商实际计费）：$MODEL_PRICES"
ok "题目：$TOPIC"

step "准备全新的数据目录 $DATA 与数据库 $DEMO_DB"
rm -rf "$DATA"
if [ -n "${AGENTBOX_DEMO_DATABASE_URL:-}" ]; then
  export AGENTBOX_DATABASE_URL="$AGENTBOX_DEMO_DATABASE_URL"
  ok "使用给定的数据库连接串（须为空库）"
else
  # with_db <库名>：把 PG_ADMIN_URL 的库名换成给定值（用户、口令、主机、端口与参数不变；口令不写死在脚本里）。
  with_db() { python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(path='/'+sys.argv[2])))" "$PG_ADMIN_URL" "$1"; }
  # 依次尝试：本机 psql（凭据取自 PG_ADMIN_URL）→ docker（compose 服务 postgres，或容器 agentbox-pg）→ Docker Desktop 的 docker.exe。
  pg_sql() {
    local sql="$1" dk
    if command -v psql >/dev/null 2>&1; then
      psql -v ON_ERROR_STOP=1 "$(with_db postgres)" -qc "$sql" >/dev/null 2>&1 && return 0
    fi
    for dk in docker "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"; do
      { command -v "$dk" >/dev/null 2>&1 || [ -x "$dk" ]; } || continue
      "$dk" compose -f "$REPO/deploy/docker-compose.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
      "$dk" exec agentbox-pg psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
    done
    return 1
  }
  pg_sql "DROP DATABASE IF EXISTS $DEMO_DB WITH (FORCE)" || fail "无法新建演示数据库：需要 psql 或 docker（PostgreSQL 由 deploy/docker-compose.yml 启动），或设置 AGENTBOX_DEMO_DATABASE_URL 指向一个空库"
  pg_sql "CREATE DATABASE $DEMO_DB" || fail "无法创建 $DEMO_DB"
  AGENTBOX_DATABASE_URL="$(with_db "$DEMO_DB")"
  export AGENTBOX_DATABASE_URL
  ok "已新建空库 $DEMO_DB"
fi
ok "数据目录已清空：$DATA"

step "安装 worker 包到 $WORKER_DIR（agentbox_worker、deepresearch）"
install -d -m 0755 "$WORKER_DIR"
rm -rf "$WORKER_DIR/agentbox_worker" "$WORKER_DIR/sim_worker" "$WORKER_DIR/deepresearch"
cp -r "$REPO/worker/agentbox_worker" "$REPO/worker/sim_worker" "$REPO/worker/deepresearch" "$WORKER_DIR/"
find "$WORKER_DIR" -name __pycache__ -prune -exec rm -rf {} +
chmod -R u=rwX,go=rX "$WORKER_DIR"
[ -f "$WORKER_DIR/deepresearch/__main__.py" ] || fail "$WORKER_DIR 中没有 deepresearch"
ok "$(find "$WORKER_DIR" -name '*.py' | wc -l) 个 .py 文件；$WORKER_DIR/deepresearch 存在"

step "构建（CGO_ENABLED=0）并运行 doctor"
(cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/agentbox)
ok "$BIN"
if [ -n "$FAKE" ]; then
  (cd "$REPO" && CGO_ENABLED=0 go build -o "$FU_BIN" ./tests/e2e/fakeupstream/cmd/fakeupstream)
  ok "$FU_BIN（演练用 fake upstream）"
fi
"$BIN" doctor | sed 's/^/         /'

FLAGS=(--data-dir "$DATA" --listen "$LISTEN" --worker-argv python3,-m,deepresearch)
if [ -n "$FAKE" ]; then
  step "启动 fake upstream（模型、搜索与网页；第一次 chat 挂起到 G3 检查完成）"
  "$FU_BIN" -hold-first-chat >"$LOGDIR/fakeupstream.log" 2>&1 &
  FU_PID=$!
  wait_for "fake upstream 打印地址" 30 grep -q '^url=' "$LOGDIR/fakeupstream.log"
  read -r fu_url fu_model fu_hostport < <(sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p' "$LOGDIR/fakeupstream.log")
  ok "pid $FU_PID：$fu_url"
  FLAGS+=(--model-base-url "$fu_model" --model-name "$WORKER_MODEL" --models "$MODELS"
    --model-price-in-micro-per-mtok "${AGENTBOX_DEMO_PRICE_IN_MICRO_PER_MTOK:-1000000}"
    --model-price-out-micro-per-mtok "${AGENTBOX_DEMO_PRICE_OUT_MICRO_PER_MTOK:-1000000}"
    --search-provider fake --search-base-url "$fu_url" --upstream-allow-private "$fu_hostport")
else
  FLAGS+=(--model-base-url "$MODEL_BASE_URL" --model-name "$WORKER_MODEL" --models "$MODELS"
    --search-provider "$SEARCH_PROVIDER")
  [ -z "${AGENTBOX_DEMO_PRICE_IN_MICRO_PER_MTOK:-}" ] || FLAGS+=(--model-price-in-micro-per-mtok "$AGENTBOX_DEMO_PRICE_IN_MICRO_PER_MTOK")
  [ -z "${AGENTBOX_DEMO_PRICE_OUT_MICRO_PER_MTOK:-}" ] || FLAGS+=(--model-price-out-micro-per-mtok "$AGENTBOX_DEMO_PRICE_OUT_MICRO_PER_MTOK")
  [ -z "${AGENTBOX_DEMO_UPSTREAM_ALLOW_PRIVATE:-}" ] || FLAGS+=(--upstream-allow-private "$AGENTBOX_DEMO_UPSTREAM_ALLOW_PRIVATE")
fi
[ -z "$MODEL_PRICES" ] || FLAGS+=(--model-price "$MODEL_PRICES")
[ -z "${AGENTBOX_DEMO_WEB_DIR:-}" ] || FLAGS+=(--web-dir "$AGENTBOX_DEMO_WEB_DIR")
[ -z "${AGENTBOX_DEMO_REDIS_ADDR:-}" ] || FLAGS+=(--redis-addr "$AGENTBOX_DEMO_REDIS_ADDR")

step "启动 agentbox server（生产启动器、Gateway、Worker = python3 -m deepresearch）"
info "agentbox server ${FLAGS[*]}"
AGENTBOX_MODEL_API_KEY="$MODEL_KEY" AGENTBOX_SEARCH_API_KEY="$SEARCH_KEY" "$BIN" server "${FLAGS[@]}" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_for "server 进入 normal 模式" 60 server_ready
ok "server pid $SERVER_PID 已就绪（$AGENTBOX_ADDR，日志 $SERVER_LOG）"
[ -z "${AGENTBOX_DEMO_WEB_DIR:-}" ] || info "工作台：浏览器打开 $AGENTBOX_ADDR/（远程主机经 ssh -L 8080:$LISTEN 隧道访问）"

step "提交研究任务"
SPEC=$(python3 -c 'import json,sys; print(json.dumps({"topic": sys.argv[1], "orchestrator_model": sys.argv[2], "worker_model": sys.argv[3]}, ensure_ascii=False))' "$TOPIC" "$ORCH_MODEL" "$WORKER_MODEL")
SUBMIT=(task submit --spec "$SPEC")
[ -z "${AGENTBOX_DEMO_BUDGET_MICRO:-}" ] || SUBMIT+=(--limits "{\"budget_micro\":$AGENTBOX_DEMO_BUDGET_MICRO}")
T=$("$BIN" "${SUBMIT[@]}" | json "d['task_id']")
ok "task_id $T，spec $SPEC"
"$BIN" task watch "$T" >"$EVENTS" 2>"$LOGDIR/watch.err" &
WATCH_PID=$!

step "G3：Worker 运行中，宿主读取沙箱内每个进程的 environ 与 cmdline"
wait_for "Worker（python3 -m deepresearch）在环境中运行" 120 worker_running
g3_probe | sed 's/^/         /' # 检查失败时 pipefail 使脚本在此退出
ok "沙箱进程的 environ 与 cmdline 不含 Key 的值，也没有 AGENTBOX_MODEL_API_KEY / AGENTBOX_SEARCH_API_KEY / AGENTBOX_SERPER_API_KEY"
if [ -n "$FAKE" ]; then
  kill -USR1 "$FU_PID"
  info "已放行 fake upstream 挂起的第一次 chat"
fi

step "等待 task_terminal（最长 ${TIMEOUT}s）"
for ((i = 0; i < TIMEOUT; i++)); do
  kill -0 "$WATCH_PID" 2>/dev/null || break
  sleep 1
done
kill -0 "$WATCH_PID" 2>/dev/null && fail "任务 $T 在 ${TIMEOUT}s 内未结束"
rc=0
wait "$WATCH_PID" || rc=$?
WATCH_PID=""
[ "$rc" = 0 ] || fail "task watch 退出码 $rc：$(cat "$LOGDIR/watch.err")"
jsonl "'%s: %s' % (d['type'], (lambda s: s if len(s) <= 140 else s[:140] + '…')(json.dumps(d.get('payload'), ensure_ascii=False)))" <"$EVENTS" | sed 's/^/         /'
status=$(task_field "$T" status)
[ "$status" = succeeded ] || fail "任务结束为 $status（$(task_field "$T" status_reason 2>/dev/null || true)）"
ok "succeeded；checkpoints：$(inspect_task "$T" | json "' → '.join(c['step_id'] for c in sorted(d['checkpoints'], key=lambda c: c['commit_seq']))")"

step "inspect：Gateway 调用明细（端点、模型、tries、费用、延迟；不含请求与响应正文）"
inspect_task "$T" >"$LOGDIR/inspect.json"
# 模型取自 chat 结果 blob（上游回复的 model 字段，只读这一个字段，不打印正文）；检查路由：计划与报告调用为编排
# 模型，任务内的 chat 为 worker 模型（允许上游在模型名后附加 "-<版本>"）。
python3 - "$LOGDIR/inspect.json" "$DATA/blobs/sha256" "$ORCH_MODEL" "$WORKER_MODEL" <<'PY' | sed 's/^/         /'
import json, os, sys
d = json.load(open(sys.argv[1]))
blobs, orch, worker = sys.argv[2:5]
total, bad = 0, []
for c in d["calls"]:
    lat = sum(t["latency_ms"] for t in c["tries"])
    outs = ",".join(t.get("outcome") or t["state"] for t in c["tries"])
    total += c["cost_charged_micro"]
    model = "-"
    if c["endpoint"] == "/v1/chat/completions":
        # journal 记录的模型（未完成的调用也有）；没有时退回结果 blob 中上游回复的 model 字段
        model = c.get("model") or ""
        ref = c.get("result_ref") or ""
        if not model:
            try:
                model = json.load(open(os.path.join(blobs, ref[:2], ref[2:]))).get("model") or "?"
            except (OSError, ValueError, AttributeError):
                model = "?"
        step = c["call_id"].split("/")[1]
        want = orch if step in ("plan", "report") else worker
        if model != want and not model.startswith(want + "-"):
            bad.append(f"{c['call_id']}: 期望 {want}，得到 {model}")
    print(f"{c['call_id']:<24} {c['endpoint']:<22} {model:<12} {c['state']:<10} tries={c['tries_used']}({outs}) "
          f"cost={c['cost_charged_micro']}µ$ latency={lat}ms")
print(f"调用 {len(d['calls'])} 个；总费用 {total} 微美元（{total / 1e6:.6f} USD，按配置价格估算）")
if bad:
    sys.exit("模型路由不符：" + "；".join(bad))
PY
ok "模型路由：plan/report = $ORCH_MODEL，任务内 chat = $WORKER_MODEL"
# 每个调用都处于终态：completed、failed（例如网页不可达）或 unknown（结果不确定、可能已计费，例如超过调用
# 期限）；不存在 resolving/in_flight。每个模型步骤（call_id 去掉末尾序号）的最后一次调用必须 completed——
# worker 对超时的调用以新 ID 重做，重做成功即该步骤完成。
[ "$(json "len(d['calls']) > 0 and all(c['state'] in ('completed', 'failed', 'unknown') for c in d['calls'])" <"$LOGDIR/inspect.json")" = True ] ||
  fail "存在未结算的 Gateway 调用（resolving/in_flight）"
[ "$(json "all(c['state'] == 'completed' for c in {c['call_id'].rsplit('/', 1)[0]: c for c in sorted((c for c in d['calls'] if c['endpoint'] == '/v1/chat/completions'), key=lambda c: (c['call_id'].rsplit('/', 1)[0], int(c['call_id'].rsplit('/', 1)[1])))}.values())" <"$LOGDIR/inspect.json")" = True ] ||
  fail "存在最终未完成的模型步骤"
ok "全部调用处于终态：$(json "', '.join(f'{s} {n}' for s, n in sorted(__import__('collections').Counter(c['state'] for c in d['calls']).items()))" <"$LOGDIR/inspect.json")；每个模型步骤的最后一次调用 completed"
if [ -n "${AGENTBOX_DEMO_REDIS_ADDR:-}" ]; then
  ok "调用来源：$(json "', '.join(f'{s} {n}' for s, n in sorted(__import__('collections').Counter(c.get('source', 'upstream') for c in d['calls']).items()))" <"$LOGDIR/inspect.json")"
  ok "缓存计数（/status）：$(curl -fsS "$AGENTBOX_ADDR/status" | json "d.get('cache')")"
fi

step "研究报告（从 BlobStore 读取并校验 sha256）：前 40 行与证据列表"
REPORT_SHA=$(jsonl "d['payload']['sha256'] if d['type'] == 'artifact_saved' and d['payload'].get('artifact_id') == 'report' else ''" <"$EVENTS" | grep -v '^$' | tail -n 1 || true)
[ -n "$REPORT_SHA" ] || fail "事件中没有 artifact_saved(report)"
REPORT="$DATA/blobs/sha256/${REPORT_SHA:0:2}/${REPORT_SHA:2}"
[ "$(sha256sum "$REPORT" | awk '{print $1}')" = "$REPORT_SHA" ] || fail "报告 blob 的内容哈希不是 $REPORT_SHA"
info "report sha256:$REPORT_SHA（$(wc -c <"$REPORT") 字节，$(wc -l <"$REPORT") 行）"
info "---- 前 40 行 ----"
head -n 40 "$REPORT" | sed 's/^/         | /'
info "---- 证据 ----"
sed -n '/^## 证据$/,$p' "$REPORT" | sed 's/^/         | /'
evidence=$(sed -n '/^## 证据$/,$p' "$REPORT" | grep -oE 'sha256:[0-9a-f]{64}' | cut -d: -f2 || true)
[ -n "$evidence" ] || fail "报告没有证据列表"
for s in $evidence; do
  [ -f "$DATA/blobs/sha256/${s:0:2}/${s:2}" ] || fail "证据 blob $s 不在 BlobStore 中"
done
ok "证据 $(echo "$evidence" | wc -l) 条，每条的 blob 均在 BlobStore 中"

step "清理：环境停止、销毁，环境目录删除"
wait_for "任务 $T 的环境清理完成" 60 cleaned "$T"
ok "attempt 的环境 cleanup_state=done，$DATA/envs 为空"

step "正常停止 server（SIGTERM）"
kill -TERM "$SERVER_PID"
rc=0
wait "$SERVER_PID" || rc=$?
SERVER_PID=""
[ "$rc" = 0 ] || fail "server 退出码 $rc"
ok "退出码 0"
if [ -n "$FAKE" ]; then
  kill -TERM "$FU_PID"
  wait "$FU_PID" 2>/dev/null || true
  FU_PID=""
  info "fake upstream：$(grep '^counts ' "$LOGDIR/fakeupstream.log" || echo '未打印计数')"
fi

step "verify-invariants --quiescent"
"$BIN" verify-invariants --quiescent --data-dir "$DATA" | sed 's/^/         /'
ok "通过"

step "泄漏检查：agentbox cgroup、环境目录、沙箱进程"
own=$(install_cgroup)
[ -z "$(ls -A "$DATA/envs" 2>/dev/null)" ] || fail "残留环境目录：$(ls "$DATA/envs")"
leaks=0
for cg in "$CGROOT"/agentbox-*; do
  [ -d "$cg" ] || continue
  procs=$(find "$cg" -name cgroup.procs -exec cat {} + | wc -l)
  envs=$(find "$cg" -mindepth 1 -maxdepth 2 -type d -name 'env-*' | wc -l)
  if [ "$procs" -ne 0 ] || [ "$envs" -ne 0 ]; then
    info "残留：$cg（进程 $procs 个，环境 cgroup $envs 个）"
    leaks=$((leaks + 1))
  elif [ "$cg" = "$own" ]; then
    rmdir "$cg" || fail "无法删除本安装的空 cgroup $cg"
  else
    info "注意：其他安装或测试留下的空 cgroup $cg（不属于本次演示）"
  fi
done
[ "$leaks" = 0 ] || fail "$leaks 个 agentbox cgroup 仍有进程或环境"
# 只匹配命令行以之开头的真实沙箱进程，避免把包含这些字样的外层 shell 误判为残留。
sandbox_procs() { pgrep -af '^python3 -m (sim_worker|deepresearch)|^/proc/self/exe (init|sandbox-launch)|^agentbox-helper( |$)' || true; }
if [ -n "$(sandbox_procs)" ]; then
  sandbox_procs | sed 's/^/         /'
  fail "仍有沙箱进程"
fi
ok "没有环境目录、没有带进程或环境的 agentbox cgroup（本安装的空 cgroup 已删除）、没有沙箱进程"

step "Key 泄漏检查：server 日志、事件流、inspect 输出与演示日志目录"
no_secret "演示日志目录" "$LOGDIR"/* || fail "Key 出现在演示日志中"
ok "$(ls "$LOGDIR" | tr '\n' ' ')均不含 Key 的值"
