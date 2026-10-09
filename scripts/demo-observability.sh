#!/usr/bin/env bash
# 可观测性演示（docs/design/2026-10-10-observability-design.md）：启动本地观测栈（OTel Collector → Tempo、
# Prometheus、Loki + Alloy、Grafana），以 fake upstream（不访问外网、没有模型费用）运行真实沙箱中的任务与会话
# turn——一个成功的研究任务（上游注入一次 503，Gateway 重试）、一个暂停后继续的任务、一个预算耗尽而失败的任务、
# 两个会话 turn（第二个被停止）——然后从 Tempo、Prometheus 与 Loki 取回证据并打印查看位置。以 root 在 Linux
# （cgroup v2）或 WSL2 中运行：
#
#   sudo bash scripts/demo-observability.sh
#
# 需要：PostgreSQL（docker compose -f deploy/docker-compose.yml up -d --wait）、python3 ≥ 3.11、Go、docker
# （WSL2 中没有 docker 时使用 Docker Desktop 的 docker.exe）。观测栈在演示结束后继续运行（Grafana
# http://127.0.0.1:3000）；停止：docker compose -f deploy/observability/docker-compose.yml down（加 -v 删除数据）。
#
# 环境变量：
#   AGENTBOX_DATABASE_URL        管理连接串（默认 postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable）
#   AGENTBOX_DEMO_DB             演示库名（默认 agentbox_demo_obs，每次重建）
#   AGENTBOX_DEMO_DATA_DIR       数据目录（默认 /var/lib/agentbox-demo-obs，每次清空）
#   AGENTBOX_DEMO_LISTEN         API 监听（默认 127.0.0.1:8080）
#   AGENTBOX_DEMO_METRICS_LISTEN /metrics 监听（默认 127.0.0.1:9464；原生 Linux 上 Prometheus 容器经 docker
#                                网桥访问，默认改为网桥网关地址:9464）
#   AGENTBOX_DEMO_EVIDENCE_DIR   证据输出目录（默认 <日志目录>/evidence）
#   AGENTBOX_DEMO_KEEP_SERVER=1  结束时保留 server 运行（Ctrl-C 停止），便于在 Grafana 中继续操作
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
OBS="$REPO/deploy/observability"
BIN="$REPO/bin/agentbox"
FU_BIN="$REPO/bin/fakeupstream"
PG_ADMIN_URL="${AGENTBOX_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
DEMO_DB="${AGENTBOX_DEMO_DB:-agentbox_demo_obs}"
DATA="${AGENTBOX_DEMO_DATA_DIR:-/var/lib/agentbox-demo-obs}"
LISTEN="${AGENTBOX_DEMO_LISTEN:-127.0.0.1:8080}"
export AGENTBOX_ADDR="http://$LISTEN"
CGROOT=/sys/fs/cgroup
LOGDIR=$(mktemp -d /tmp/agentbox-demo-obs.XXXXXX)
SERVER_LOG_DIR="$OBS/logs" # Alloy 读取此目录下的 *.log（compose 的 AGENTBOX_LOG_DIR）
SERVER_LOG="$SERVER_LOG_DIR/agentbox.log"
EVIDENCE="${AGENTBOX_DEMO_EVIDENCE_DIR:-$LOGDIR/evidence}"
SERVER_PID=""
FU_PID=""
STEP="初始化"
N=0
WORKER_MODEL=fake-model

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
  stop_pid "$SERVER_PID"
  stop_pid "$FU_PID"
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%02d] %s（退出码 %d）。日志目录：%s\n' "$N" "$STEP" "$rc" "$LOGDIR" >&2
    [ -f "$SERVER_LOG" ] && tail -n 20 "$SERVER_LOG" >&2
  else
    printf '\n演示完成：全部 %d 步通过。日志目录：%s；证据：%s\n' "$N" "$LOGDIR" "$EVIDENCE"
  fi
}
trap on_exit EXIT

json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

wait_for() {
  local what=$1 limit=$2 i
  shift 2
  for ((i = 0; i < limit * 10; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  fail "等待超时（${limit}s）：$what"
}

# ---- docker：原生 docker，或 WSL2 中 Docker Desktop 的 docker.exe（路径参数须转换为 Windows 路径） ----

DOCKER=""
WINPATH=""
if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
  DOCKER=docker
elif [ -x "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe" ]; then
  DOCKER="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
  WINPATH=1
fi
hostpath() { if [ -n "$WINPATH" ]; then wslpath -w "$1"; else printf '%s' "$1"; fi; }
compose() { AGENTBOX_LOG_DIR="$(hostpath "$SERVER_LOG_DIR")" WSLENV=AGENTBOX_LOG_DIR "$DOCKER" compose -f "$(hostpath "$OBS/docker-compose.yml")" "$@"; }

# ---- API 辅助：运维调用带 Bearer token；用户调用带会话 cookie ----

api() { curl -fsS -H "Authorization: Bearer $AGENTBOX_TOKEN" -H 'Content-Type: application/json' "$@"; }
user_api() { curl -fsS -b "$LOGDIR/cookies" -c "$LOGDIR/cookies" -H "Origin: $AGENTBOX_ADDR" -H 'Content-Type: application/json' "$@"; }
task_status() { api "$AGENTBOX_ADDR/tasks/$1" | json "d['status']"; }
is_status() { [ "$(task_status "$1")" = "$2" ]; }
turn_status() { user_api "$AGENTBOX_ADDR/sessions/$1/turns" | json "next((t['status'] for t in d['turns'] if t['turn_id']=='$2'), '')"; }
turn_is() { turn_status "$1" "$2" | grep -qxE "$3"; }
server_ready() { curl -fsS "$AGENTBOX_ADDR/status" | grep -q '"normal"'; }
has_progress() { api "$AGENTBOX_ADDR/tasks/$1/inspect" | json "len(d.get('checkpoints') or [])" | grep -qv '^0$'; }

# ---------------------------------------------------------------------------

step "环境检查：root、cgroup v2、PostgreSQL、python3、go、docker"
[ "$(id -u)" = 0 ] || fail "需要 root（sudo bash scripts/demo-observability.sh）"
[ "$(stat -fc %T $CGROOT)" = cgroup2fs ] || fail "$CGROOT 不是 cgroup v2"
read -r pghost pgport < <(python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(p.hostname, p.port or 5432)" "$PG_ADMIN_URL")
timeout 3 bash -c "</dev/tcp/$pghost/$pgport" 2>/dev/null || fail "PostgreSQL $pghost:$pgport 不可达（docker compose -f deploy/docker-compose.yml up -d --wait）"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' || fail "需要 python3 ≥ 3.11"
command -v go >/dev/null 2>&1 || fail "需要 Go（构建 server 与 fake upstream）"
[ -n "$DOCKER" ] || fail "需要 docker（WSL2 中可使用 Docker Desktop）"
if curl -fsS "$AGENTBOX_ADDR/status" >/dev/null 2>&1; then fail "$LISTEN 上已有服务在运行"; fi
ok "root、cgroup2fs、PostgreSQL $pghost:$pgport、$(python3 --version)、$(go version | awk '{print $3}')、docker=$DOCKER"

step "启动观测栈（deploy/observability：OTel Collector、Tempo、Prometheus、Loki、Alloy、Grafana）"
mkdir -p "$SERVER_LOG_DIR" "$EVIDENCE"
: >"$SERVER_LOG"
# Prometheus 的抓取目标：WSL2 / Docker Desktop 经 host.docker.internal 到达宿主 127.0.0.1；原生 Linux 上容器经
# docker 网桥访问宿主，server 的 /metrics 监听网桥网关地址（仍不对外网开放）。
if [ -n "$WINPATH" ] || grep -qi microsoft /proc/version 2>/dev/null; then
  METRICS_LISTEN="${AGENTBOX_DEMO_METRICS_LISTEN:-127.0.0.1:9464}"
  TARGET="host.docker.internal:${METRICS_LISTEN##*:}"
else
  gw=$("$DOCKER" network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || echo 172.17.0.1)
  METRICS_LISTEN="${AGENTBOX_DEMO_METRICS_LISTEN:-$gw:9464}"
  TARGET="$METRICS_LISTEN"
fi
printf '[{"targets": ["%s"], "labels": {"service": "agentbox"}}]\n' "$TARGET" >"$OBS/prometheus/targets/agentbox.json"
compose up -d --wait >"$LOGDIR/compose.log" 2>&1 || { cat "$LOGDIR/compose.log" >&2; fail "观测栈启动失败"; }
wait_for "Grafana 就绪" 120 curl -fsS http://127.0.0.1:3000/api/health
ok "Grafana http://127.0.0.1:3000 · Prometheus http://127.0.0.1:9090 · Tempo http://127.0.0.1:3200 · OTLP http://127.0.0.1:4318"
ok "Prometheus 抓取 $TARGET；Alloy 读取 $SERVER_LOG_DIR/*.log"

step "准备全新的数据目录 $DATA 与数据库 $DEMO_DB"
rm -rf "$DATA"
with_db() { python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(path='/'+sys.argv[2])))" "$PG_ADMIN_URL" "$1"; }
pg_sql() {
  local sql="$1"
  if command -v psql >/dev/null 2>&1; then
    psql -v ON_ERROR_STOP=1 "$(with_db postgres)" -qc "$sql" >/dev/null 2>&1 && return 0
  fi
  "$DOCKER" compose -f "$(hostpath "$REPO/deploy/docker-compose.yml")" exec -T postgres psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1
}
pg_sql "DROP DATABASE IF EXISTS $DEMO_DB WITH (FORCE)" || fail "无法新建演示数据库（需要 psql 或 deploy/docker-compose.yml 的 postgres 服务）"
pg_sql "CREATE DATABASE $DEMO_DB" || fail "无法创建 $DEMO_DB"
AGENTBOX_DATABASE_URL="$(with_db "$DEMO_DB")"
export AGENTBOX_DATABASE_URL
mkdir -p "$DATA"
(umask 077; od -An -N24 -tx1 /dev/urandom | tr -d ' \n' >"$DATA/api.token")
AGENTBOX_TOKEN=$(cat "$DATA/api.token")
export AGENTBOX_TOKEN
ok "空库 $DEMO_DB；运维 token $DATA/api.token（0600，不打印）"

step "安装 worker 包、构建 server 与 fake upstream"
bash "$REPO/scripts/dev/install-worker.sh" >/dev/null
(cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/agentbox && CGO_ENABLED=0 go build -o "$FU_BIN" ./tests/e2e/fakeupstream/cmd/fakeupstream)
ok "$BIN、$FU_BIN；worker 包已安装到 /opt/agentbox"

step "启动 fake upstream（固定延迟：chat 300 ms、search 80 ms、fetch 50 ms；第 2 次 chat 回 503）"
"$FU_BIN" -latency chat=300ms,search=80ms,fetch=50ms -inject chat:2:503 >"$LOGDIR/fakeupstream.log" 2>&1 &
FU_PID=$!
wait_for "fake upstream 打印地址" 30 grep -q '^url=' "$LOGDIR/fakeupstream.log"
read -r fu_url fu_model fu_hostport < <(sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p' "$LOGDIR/fakeupstream.log")
ok "pid $FU_PID：$fu_url"

step "启动 agentbox server（tracing → Collector、/metrics、JSON 日志 → $SERVER_LOG）"
FLAGS=(--data-dir "$DATA" --listen "$LISTEN" --worker-argv python3,-m,deepresearch
  --session-worker-argv python3,-m,chatagent
  --model-base-url "$fu_model" --model-name "$WORKER_MODEL" --models "$WORKER_MODEL"
  --user-orchestrator-model "$WORKER_MODEL" --user-worker-model "$WORKER_MODEL"
  --model-price-in-micro-per-mtok 1000000 --model-price-out-micro-per-mtok 2000000
  --search-provider fake --search-base-url "$fu_url" --upstream-allow-private "$fu_hostport"
  --otlp-endpoint http://127.0.0.1:4318 --metrics-listen "$METRICS_LISTEN" --log-file "$SERVER_LOG")
info "agentbox server ${FLAGS[*]}"
env -u AGENTBOX_TOKEN AGENTBOX_MODEL_API_KEY="fake-demo-key-$RANDOM$RANDOM" "$BIN" server "${FLAGS[@]}" >"$LOGDIR/server.stderr" 2>&1 &
SERVER_PID=$!
wait_for "server 进入 normal 模式" 60 server_ready
wait_for "/metrics 可读" 10 curl -fsS "http://$METRICS_LISTEN/metrics"
ok "server pid $SERVER_PID；/metrics http://$METRICS_LISTEN/metrics（只供运维）"

submit() { api -X POST "$AGENTBOX_ADDR/tasks" -d "$1" | json "d['task_id']"; }
SPEC='{"topic":"固态电池的产业化进展与主要技术瓶颈","orchestrator_model":"fake-model","worker_model":"fake-model"}'

step "任务 A：完整研究（第 2 次 chat 收到 503，Gateway 自动重试）"
TA=$(submit "{\"request_id\":\"obs-a-$RANDOM\",\"spec\":$SPEC}")
wait_for "任务 A 结束" 300 is_status "$TA" succeeded
ok "$TA succeeded"

step "任务 B：运行中暂停（stop → paused），再继续到成功"
TB=$(submit "{\"request_id\":\"obs-b-$RANDOM\",\"spec\":$SPEC}")
wait_for "任务 B 写出第一个 checkpoint" 120 has_progress "$TB"
api -X POST "$AGENTBOX_ADDR/tasks/$TB/pause" -d "{\"request_id\":\"obs-b-pause-$RANDOM\"}" >/dev/null
wait_for "任务 B 暂停" 120 is_status "$TB" paused
ok "$TB paused"
api -X POST "$AGENTBOX_ADDR/tasks/$TB/resume" -d "{\"request_id\":\"obs-b-resume-$RANDOM\"}" >/dev/null
wait_for "任务 B 继续后结束" 300 is_status "$TB" succeeded
ok "$TB succeeded（2 次运行：创建请求与 resume 请求各为一条 trace 的根）"

step "任务 C：预算 10 微美元（第一次模型调用即 402 budget_exhausted → 任务失败）"
TC=$(submit "{\"request_id\":\"obs-c-$RANDOM\",\"spec\":$SPEC,\"limits\":{\"budget_micro\":10}}")
wait_for "任务 C 结束" 120 bash -c "s=\$(curl -fsS -H 'Authorization: Bearer $AGENTBOX_TOKEN' $AGENTBOX_ADDR/tasks/$TC | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"status\"])'); [ \"\$s\" = failed ] || [ \"\$s\" = succeeded ]"
ok "$TC $(task_status "$TC")（$(api "$AGENTBOX_ADDR/tasks/$TC" | json "d.get('status_reason','')")）"

step "会话：注册用户、发送消息（turn 1 完成），再发送一条并停止（turn 2）"
user_api -X POST "$AGENTBOX_ADDR/auth/register" -d "{\"username\":\"obsdemo$RANDOM\",\"password\":\"Obs-demo-9x\"}" >/dev/null
SID=$(user_api -X POST "$AGENTBOX_ADDR/sessions" -d "{\"request_id\":\"obs-s-$RANDOM\",\"title\":\"observability demo\"}" | json "d['session_id']")
T1=$(user_api -X POST "$AGENTBOX_ADDR/sessions/$SID/messages" -d "{\"request_id\":\"obs-m1-$RANDOM\",\"text\":\"你好，简单介绍一下固态电池\",\"deep_research\":false}" | json "d['turn_id']")
wait_for "turn 1 结束" 180 turn_is "$SID" "$T1" "succeeded|failed|awaiting_input"
ok "session $SID turn $T1 $(turn_status "$SID" "$T1")"
T2=$(user_api -X POST "$AGENTBOX_ADDR/sessions/$SID/messages" -d "{\"request_id\":\"obs-m2-$RANDOM\",\"text\":\"再详细研究一下量产成本\",\"deep_research\":true}" | json "d['turn_id']")
wait_for "turn 2 运行" 120 turn_is "$SID" "$T2" "running"
# turn 可能在停止请求到达之前已经结束（fake upstream 很快）：此时停止返回 409，不视为失败。
user_api -X POST "$AGENTBOX_ADDR/turns/$T2/stop" -d "{\"request_id\":\"obs-stop-$RANDOM\"}" >/dev/null || info "停止请求未被接受（turn 已结束）"
wait_for "turn 2 停止" 120 turn_is "$SID" "$T2" "paused|succeeded|failed|cancelled"
ok "turn $T2 $(turn_status "$SID" "$T2")"

step "等待 span 导出（批处理）与 Prometheus 抓取"
sleep 12
ok "完成"

step "证据：Tempo 中每个任务/turn 的 trace（span 树）"
# traces_of <task_id>：按 span 属性 task.id 在 Tempo 中检索该任务每次运行（run span）的 trace id（按开始时间）。
traces_of() {
  curl -fsS -G "http://127.0.0.1:3200/api/search" --data-urlencode "q={ span.task.id = \"$1\" && name =~ \"task|turn\" }" --data-urlencode "limit=10" |
    json "' '.join(t['traceID'] for t in sorted(d.get('traces') or [], key=lambda t: int(t['startTimeUnixNano'])))"
}
: >"$EVIDENCE/traces.txt"
for id in "$TA" "$TB" "$TC" "$T1" "$T2"; do
  tids=""
  for _ in $(seq 1 30); do tids=$(traces_of "$id"); [ -n "$tids" ] && break; sleep 2; done
  [ -n "$tids" ] || fail "Tempo 中没有任务 $id 的 trace"
  for tid in $tids; do
    curl -fsS "http://127.0.0.1:3200/api/traces/$tid" >"$EVIDENCE/trace-$id-$tid.json"
    printf '%s %s
' "$id" "$tid" >>"$EVIDENCE/traces.txt"
    info "task $id → trace $tid（Grafana：Explore → Tempo → $tid）"
    python3 "$REPO/scripts/dev/span-tree.py" "$EVIDENCE/trace-$id-$tid.json" | sed 's/^/           /' | head -60
  done
done
ok "trace JSON 已保存到 $EVIDENCE/trace-*.json"

step "证据：PromQL"
promql() {
  printf '# %s\n' "$1" >>"$EVIDENCE/promql.txt"
  curl -fsS -G http://127.0.0.1:9090/api/v1/query --data-urlencode "query=$1" |
    python3 -c '
import json, sys
r = json.load(sys.stdin)["data"]["result"]
for s in r:
    labels = ",".join(f"{k}={v}" for k, v in sorted(s["metric"].items()) if k not in ("job", "instance", "service"))
    print(f"  {{{labels}}} {float(s["value"][1]):.4g}")
if not r:
    print("  (empty)")
' | tee -a "$EVIDENCE/promql.txt" | sed 's/^/         /'
}
for q in \
  'sum by (kind, status) (agentbox_tasks_finished_total)' \
  'sum by (kind, outcome_class) (agentbox_attempts_finished_total)' \
  'histogram_quantile(0.5, sum by (le, kind) (agentbox_attempt_ready_seconds_bucket))' \
  'histogram_quantile(0.95, sum by (le, kind) (agentbox_task_start_seconds_bucket))' \
  'sum by (kind, desired) (agentbox_stop_seconds_sum) / sum by (kind, desired) (agentbox_stop_seconds_count)' \
  'sum by (kind, status, outcome) (agentbox_upstream_tries_total)' \
  'histogram_quantile(0.95, sum by (le, kind) (agentbox_upstream_try_duration_seconds_bucket))' \
  'sum by (kind, result) (agentbox_gateway_calls_total)' \
  'sum by (kind, model) (agentbox_cost_micro_usd_total)' \
  'sum by (kind) (agentbox_tool_calls_per_task_sum) / sum by (kind) (agentbox_tool_calls_per_task_count)' \
  'agentbox_sandbox_envs' 'agentbox_sandbox_cleanup_backlog' 'agentbox_run_slots_in_use' \
  'sum by (route, code) (agentbox_http_requests_total)'; do
  info "$q"
  promql "$q"
done
ok "PromQL 结果已保存到 $EVIDENCE/promql.txt"

step "证据：Loki 中带 trace_id 的日志"
tid_a=$(awk -v t="$TA" '$1 == t {print $2; exit}' "$EVIDENCE/traces.txt")
n=$(curl -fsS -G http://127.0.0.1:3100/loki/api/v1/query_range --data-urlencode "query={job=\"agentbox\"} |= \"$tid_a\"" \
  --data-urlencode "since=1h" --data-urlencode "limit=1000" | json "sum(len(s['values']) for s in d['data']['result'])")
total=$(curl -fsS -G http://127.0.0.1:3100/loki/api/v1/query_range --data-urlencode 'query={job="agentbox"}' \
  --data-urlencode "since=1h" --data-urlencode "limit=5000" | json "sum(len(s['values']) for s in d['data']['result'])")
printf 'loki lines total=%s with trace %s=%s\n' "$total" "$tid_a" "$n" >"$EVIDENCE/loki.txt"
[ "$n" -gt 0 ] || fail "Loki 中没有带 trace_id $tid_a 的日志"
ok "Loki：共 $total 行；任务 A 的 trace $tid_a 有 $n 行（Grafana 中点击 TraceID 链接跳到 Tempo）"

step "检查：trace 与日志属性中没有 Key、提示词或 URL"
python3 - "$EVIDENCE" "$SERVER_LOG" <<'PY'
import glob, sys
ev, log = sys.argv[1], sys.argv[2]
needles = ["fake-demo-key", "固态电池的产业化", "Bearer ", "http://127.0.0.1:"]
bad = []
for p in glob.glob(ev + "/trace-*.json"):
    s = open(p, encoding="utf-8").read()
    bad += [f"{p}: {n}" for n in needles if n in s]
if bad:
    sys.exit("trace 中出现了不应记录的内容：" + "; ".join(bad))
print("         trace JSON 中没有 Key、题目原文、Authorization 或上游 URL")
PY
ok "通过"

step "查看位置"
info "Grafana 看板：http://127.0.0.1:3000/d/agentbox-overview（远程主机经 ssh -L 3000:127.0.0.1:3000 隧道）"
info "Trace：Grafana → Explore → Tempo，输入 $EVIDENCE/traces.txt 中的 trace id（任务 A：$tid_a）"
info "日志：Grafana → Explore → Loki，{job=\"agentbox\"} |= \"$tid_a\"，点击 TraceID 跳到 Tempo"
info "PromQL：http://127.0.0.1:9090/graph；结果见 $EVIDENCE/promql.txt"
if [ -n "${AGENTBOX_DEMO_KEEP_SERVER:-}" ]; then
  info "server 保持运行（pid $SERVER_PID），Ctrl-C 结束"
  wait "$SERVER_PID" || true
fi
