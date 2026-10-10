#!/usr/bin/env bash
# 轨迹诊断闭环演示（docs/design/2026-10-10-trace-doctor-design.md，零模型费用）：真实沙箱 + 真实 Gateway，两个 fake
# upstream 扮演模型供应商（primary 不稳定：每第 4 个 chat 请求挂起；backup 健康）与搜索/网页（抓取延迟 1.5 s）。
# 以 root 在 Linux（cgroup v2）或 WSL2 中运行：
#
#   sudo bash scripts/demo-doctor.sh
#
# 步骤：环境检查 → 全新的数据目录与数据库 → 安装 worker 包 → 构建 → 启动两个 fake upstream → 写出"before"配置
# （降级链 primary → backup，但没有 --model-try-timeout；--call-deadline 1s 短于抓取延迟）→ server A → eval 运行 A
# → 停止 A → agentbox doctor-traces 读运行 A 的轨迹，给出根因、proposal.patch 与 experiment.flags → 以
# experiment.flags 启动 server B → 同一 suite、同一 seed 的运行 B → eval compare A B（N 与 Wilson 95% 区间；
# 区间重叠时如实说明差异在噪声内）→ 停止 → verify-invariants --quiescent → 泄漏检查。任何一步失败即以非零状态退出。
#
# 环境变量：
#   AGENTBOX_DATABASE_URL     PostgreSQL 管理连接串（默认 postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable）
#   AGENTBOX_DEMO_DB          演示库名（默认 agentbox_demo_doctor）
#   AGENTBOX_DEMO_DATA_DIR    数据目录（默认 /var/lib/agentbox-demo-doctor）
#   AGENTBOX_DEMO_LISTEN      监听地址（默认 127.0.0.1:8092）
#   AGENTBOX_EVAL_OUT         运行结果目录（默认 <仓库>/eval-runs）
#   AGENTBOX_DOCTOR_REPEAT    每个任务的重复次数（默认 8；8 个任务 × 8 = 64 次/运行）
#   AGENTBOX_DOCTOR_CONC      eval 并发（默认 8；server 的 --run-slots 与之相同）
#   AGENTBOX_DOCTOR_OBS=1     同时导出 trace/指标/日志到已在运行的观测栈（deploy/observability；见
#                             scripts/demo-observability.sh），doctor 从 Tempo、Prometheus、Loki 补充证据
#   AGENTBOX_OBS_LOG_DIR      观测栈 Alloy 读取的日志目录（默认 <仓库>/deploy/observability/logs）
#   AGENTBOX_DEMO_PREBUILT=1  不构建，直接使用 bin/agentbox 与 bin/fakeupstream
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
BIN="$REPO/bin/agentbox"
FU_BIN="$REPO/bin/fakeupstream"
PG_ADMIN_URL="${AGENTBOX_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
DEMO_DB="${AGENTBOX_DEMO_DB:-agentbox_demo_doctor}"
DATA="${AGENTBOX_DEMO_DATA_DIR:-/var/lib/agentbox-demo-doctor}"
LISTEN="${AGENTBOX_DEMO_LISTEN:-127.0.0.1:8092}"
SUITE="$REPO/eval/suites/doctor.yaml"
OUT="${AGENTBOX_EVAL_OUT:-$REPO/eval-runs}"
REPEAT="${AGENTBOX_DOCTOR_REPEAT:-8}"
CONC="${AGENTBOX_DOCTOR_CONC:-8}"
OBS="${AGENTBOX_DOCTOR_OBS:-}"
OBS_LOG_DIR="${AGENTBOX_OBS_LOG_DIR:-$REPO/deploy/observability/logs}"
export AGENTBOX_ADDR="http://$LISTEN"
WORKER_DIR=/opt/agentbox
CGROOT=/sys/fs/cgroup
LOGDIR=$(mktemp -d /tmp/agentbox-demo-doctor.XXXXXX)
SERVER_PID=""
PRIMARY_PID=""
BACKUP_PID=""
STEP="初始化"
N=0
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

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
  stop_pid "$PRIMARY_PID"
  stop_pid "$BACKUP_PID"
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%02d] %s（退出码 %d）。日志目录：%s\n' "$N" "$STEP" "$rc" "$LOGDIR" >&2
    for f in "$LOGDIR"/server-*.log; do [ -f "$f" ] && { echo "== $f" >&2; tail -n 20 "$f" >&2; }; done
  else
    printf '\n演示完成：全部 %d 步通过。运行结果：%s；日志目录：%s\n' "$N" "$OUT" "$LOGDIR"
  fi
}
trap on_exit EXIT

wait_for() {
  local what=$1 limit=$2 i
  shift 2
  for ((i = 0; i < limit * 10; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  fail "等待超时（${limit}s）：$what"
}
server_ready() { curl -fsS -H "Authorization: Bearer $AGENTBOX_TOKEN" "$AGENTBOX_ADDR/status" | grep -q '"normal"'; }
summary_field() { python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print($2)" "$1/summary.json"; }

step "环境检查：root、cgroup v2、PostgreSQL、python3"
[ "$(id -u)" = 0 ] || fail "需要 root（sudo bash scripts/demo-doctor.sh）"
[ "$(stat -fc %T $CGROOT)" = cgroup2fs ] || fail "$CGROOT 不是 cgroup v2"
ok "root，$CGROOT 为 cgroup2fs，内核 $(uname -r)"
hostport=$(python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(p.hostname, p.port or 5432)" "$PG_ADMIN_URL")
read -r pghost pgport <<<"$hostport"
timeout 3 bash -c "</dev/tcp/$pghost/$pgport" 2>/dev/null || fail "PostgreSQL $pghost:$pgport 不可达（docker compose -f deploy/docker-compose.yml up -d --wait）"
ok "PostgreSQL $pghost:$pgport 可达；$(python3 --version)"
if curl -fsS "$AGENTBOX_ADDR/status" >/dev/null 2>&1; then fail "$LISTEN 上已有服务在运行"; fi
if [ -n "$OBS" ]; then
  curl -fsS http://127.0.0.1:3200/api/status/buildinfo >/dev/null 2>&1 || fail "AGENTBOX_DOCTOR_OBS=1 但 Tempo（127.0.0.1:3200）未就绪：先启动 deploy/observability"
  mkdir -p "$OBS_LOG_DIR"
  ok "观测栈就绪；server 日志写入 $OBS_LOG_DIR"
fi

step "准备全新的数据目录 $DATA 与数据库 $DEMO_DB"
rm -rf "$DATA"
with_db() { python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(path='/'+sys.argv[2])))" "$PG_ADMIN_URL" "$1"; }
pg_sql() {
  local sql="$1" dk c
  if command -v psql >/dev/null 2>&1; then
    psql -v ON_ERROR_STOP=1 "$(with_db postgres)" -qc "$sql" >/dev/null 2>&1 && return 0
  fi
  for dk in docker "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"; do
    { command -v "$dk" >/dev/null 2>&1 || [ -x "$dk" ]; } || continue
    "$dk" compose -f "$REPO/deploy/docker-compose.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
    for c in $("$dk" ps --filter "publish=$pgport" --format '{{.Names}}' 2>/dev/null | tr -d '\r'); do
      "$dk" exec "$c" psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
    done
  done
  return 1
}
pg_sql "DROP DATABASE IF EXISTS $DEMO_DB WITH (FORCE)" || fail "无法新建演示数据库（需要 psql 或 docker）"
pg_sql "CREATE DATABASE $DEMO_DB" || fail "无法创建 $DEMO_DB"
AGENTBOX_DATABASE_URL="$(with_db "$DEMO_DB")"
export AGENTBOX_DATABASE_URL
mkdir -p "$DATA"
(umask 077; od -An -N24 -tx1 /dev/urandom | tr -d ' \n' >"$DATA/api.token")
AGENTBOX_TOKEN=$(cat "$DATA/api.token")
export AGENTBOX_TOKEN
ok "空库 $DEMO_DB；运维 token 经环境变量交给 CLI，不打印"

step "安装 worker 包到 $WORKER_DIR（agentbox_worker、deepresearch、evalworker）"
install -d -m 0755 "$WORKER_DIR"
for d in agentbox_worker deepresearch evalworker; do
  rm -rf "${WORKER_DIR:?}/$d"
  cp -r "$REPO/worker/$d" "$WORKER_DIR/"
done
find "$WORKER_DIR" -name __pycache__ -prune -exec rm -rf {} +
chmod -R u=rwX,go=rX "$WORKER_DIR"
ok "$(find "$WORKER_DIR" -name '*.py' | wc -l) 个 .py 文件"

if [ -n "${AGENTBOX_DEMO_PREBUILT:-}" ]; then
  step "使用预先构建的 $BIN 与 $FU_BIN"
  [ -x "$BIN" ] && [ -x "$FU_BIN" ] || fail "缺少 $BIN 或 $FU_BIN"
else
  step "构建 agentbox 与 fakeupstream"
  (cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/agentbox && CGO_ENABLED=0 go build -o "$FU_BIN" ./tests/e2e/fakeupstream/cmd/fakeupstream)
fi
ok "$BIN、$FU_BIN"

step "启动两个 fake upstream：primary（每第 4 个 chat 挂起，chat 200 ms，抓取 1.5 s）与 backup（健康，chat 400 ms）"
"$FU_BIN" -eval-suite "$SUITE" -every chat:4:hang -latency chat=200ms,fetch=1500ms >"$LOGDIR/fu-primary.log" 2>&1 &
PRIMARY_PID=$!
"$FU_BIN" -eval-suite "$SUITE" -latency chat=400ms >"$LOGDIR/fu-backup.log" 2>&1 &
BACKUP_PID=$!
wait_for "primary 打印地址" 30 grep -q '^url=' "$LOGDIR/fu-primary.log"
wait_for "backup 打印地址" 30 grep -q '^url=' "$LOGDIR/fu-backup.log"
read -r p_url p_model p_hp < <(sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p' "$LOGDIR/fu-primary.log")
read -r _ b_model b_hp < <(sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p' "$LOGDIR/fu-backup.log")
ok "primary pid $PRIMARY_PID $p_url；backup pid $BACKUP_PID $b_model"

step "写出 before 配置（有意的错误配置）"
CFG="$OUT/doctor-$STAMP"
mkdir -p "$CFG"
cat >"$CFG/fallback.json" <<EOF
{"providers": [{"name": "backup", "base_url": "$b_model", "price": {"in": 1000000, "out": 2000000}}]}
EOF
cat >"$CFG/before.flags" <<EOF
# agentbox server flags for the doctor demo ("before": mis-configured on purpose)
--data-dir=$DATA
--listen=$LISTEN
--worker-argv=python3,-m,evalworker
--run-slots=$CONC
--model-base-url=$p_model
--model-name=kimi-k2.6
--models=kimi-k2.6,kimi-k3
--model-price-in-micro-per-mtok=1000000
--model-price-out-micro-per-mtok=2000000
--model-fallback-file=$CFG/fallback.json
--search-provider=fake
--search-base-url=$p_url
--upstream-allow-private=$p_hp,$b_hp
# fetches take 1.5 s at the fake site, but the search/fetch call deadline is 1 s
--call-deadline=1s
# no --model-try-timeout: a hung primary try holds the model call until the eval timeout
EOF
if [ -n "$OBS" ]; then
  cat >>"$CFG/before.flags" <<EOF
--otlp-endpoint=http://127.0.0.1:4318
--metrics-listen=127.0.0.1:9464
--log-file=$OBS_LOG_DIR/agentbox-doctor.log
EOF
fi
sed 's/^/         /' "$CFG/before.flags"

start_server() { # start_server <flags 文件> <名字>
  local args
  mapfile -t args < <(grep -vE '^\s*(#|$)' "$1")
  AGENTBOX_MODEL_API_KEY="fake-doctor-key-$(od -An -tx1 -N8 /dev/urandom | tr -d ' \n')" \
    "$BIN" server "${args[@]}" >"$LOGDIR/server-$2.log" 2>&1 &
  SERVER_PID=$!
  wait_for "server $2 就绪" 90 server_ready
}
stop_server() {
  kill -TERM "$SERVER_PID"
  local rc=0
  wait "$SERVER_PID" || rc=$?
  SERVER_PID=""
  [ "$rc" = 0 ] || fail "server 退出码 $rc"
}
run_eval() { # run_eval <run-id>
  "$BIN" eval run --suite "$SUITE" --out "$OUT" --run-id "$1" --agent model --repeat "$REPEAT" \
    --concurrency "$CONC" --seed 7 | tee "$LOGDIR/$1.log" | sed 's/^/         /'
}

step "server A（before 配置）与运行 A：$REPEAT 次 × 8 个任务，并发 $CONC"
start_server "$CFG/before.flags" a
A="doctor-$STAMP-before"
run_eval "$A"
ok "A：成功率 $(summary_field "$OUT/$A" "d['success_rate']")，N=$(summary_field "$OUT/$A" "d['runs']")，P95 $(summary_field "$OUT/$A" "d['latency_ms']['p95']") ms；失败类别 $(summary_field "$OUT/$A" "d['failure_categories']")"
stop_server

step "agentbox doctor-traces：读运行 A 的轨迹，给出根因与提议"
DOC_ARGS=(--run "$OUT/$A" --apply-to "$CFG/before.flags" --out "$CFG/doctor")
if [ -n "$OBS" ]; then
  sleep 10 # 等 trace 批量导出与日志采集
  DOC_ARGS+=(--tempo http://127.0.0.1:3200 --prometheus http://127.0.0.1:9090 --loki http://127.0.0.1:3100)
fi
"$BIN" doctor-traces "${DOC_ARGS[@]}" | tee "$CFG/doctor-stdout.md" | sed 's/^/         /'
[ -s "$CFG/doctor/experiment.flags" ] || fail "doctor 没有写出 experiment.flags"
cmp -s "$CFG/before.flags" "$CFG/doctor/experiment.flags" && fail "doctor 没有提出任何可应用的修改"
echo "         ---- proposal.patch ----"
sed 's/^/         /' "$CFG/doctor/proposal.patch"
if command -v patch >/dev/null 2>&1; then
  cp "$CFG/before.flags" "$LOGDIR/patched.flags"
  patch -s "$LOGDIR/patched.flags" <"$CFG/doctor/proposal.patch" || fail "proposal.patch 无法应用"
  cmp -s "$LOGDIR/patched.flags" "$CFG/doctor/experiment.flags" || fail "proposal.patch 应用结果与 experiment.flags 不同"
  ok "proposal.patch 可用 patch(1) 应用，结果与 experiment.flags 相同"
fi

step "server B（experiment.flags）与运行 B：同一 suite、seed、重复与并发"
[ -n "$OBS" ] && sleep 15 # 让 B 的指标与日志落在 A 的分析窗口（A 结束后 10 s）之外
start_server "$CFG/doctor/experiment.flags" b
B="doctor-$STAMP-after"
run_eval "$B"
ok "B：成功率 $(summary_field "$OUT/$B" "d['success_rate']")，N=$(summary_field "$OUT/$B" "d['runs']")，P95 $(summary_field "$OUT/$B" "d['latency_ms']['p95']") ms；失败类别 $(summary_field "$OUT/$B" "d['failure_categories']")"
stop_server

step "eval compare A → B"
"$BIN" eval compare "$OUT/$A" "$OUT/$B" | tee "$CFG/compare.md" | sed 's/^/         /'
"$BIN" eval compare "$OUT/$A" "$OUT/$B" --json >"$CFG/compare.json"
python3 - "$CFG/compare.json" <<'PY'
import json, sys
c = json.load(open(sys.argv[1]))
a, b = c["success_ci95_a"], c["success_ci95_b"]
m = {x["name"]: x for x in c["metrics"]}
sr = m.get("success_rate", {})
print("         成功率 %.3f → %.3f（N %d → %d；Wilson 95%% [%.3f, %.3f] → [%.3f, %.3f]）"
      % (sr.get("a", 0), sr.get("b", 0), c["n_a"], c["n_b"], a[0], a[1], b[0], b[1]))
for k in ("latency_p50", "latency_p95", "wall_clock"):
    if k in m:
        print("         %s %.0f ms → %.0f ms" % (k, m[k]["a"], m[k]["b"]))
if b[0] > a[1]:
    print("         结论：区间不重叠，改进超出抽样噪声。")
elif a[0] > b[1]:
    print("         结论：区间不重叠，B 更差（提议有害）。")
else:
    print("         结论：区间重叠，成功率的差异在噪声内（不据此宣称改进）。")
PY

step "verify-invariants --quiescent"
"$BIN" verify-invariants --quiescent --data-dir "$DATA" | sed 's/^/         /'
ok "通过"

step "泄漏检查：本安装的 cgroup、环境目录、token 不在运行结果中"
own="$CGROOT/agentbox-$(cat "$DATA/install_id")"
[ -z "$(ls -A "$DATA/envs" 2>/dev/null)" ] || fail "残留环境目录：$(ls "$DATA/envs")"
if [ -d "$own" ]; then
  procs=$(find "$own" -name cgroup.procs -exec cat {} + | wc -l)
  envs=$(find "$own" -mindepth 1 -maxdepth 2 -type d -name 'env-*' | wc -l)
  [ "$procs" -eq 0 ] && [ "$envs" -eq 0 ] || fail "$own 仍有 $procs 个进程、$envs 个环境 cgroup"
  rmdir "$own" || fail "无法删除本安装的空 cgroup $own"
fi
if grep -rqF "$AGENTBOX_TOKEN" "$OUT/$A" "$OUT/$B" "$CFG"; then fail "token 出现在运行结果中"; fi
ok "没有残留环境与进程；运行结果与 doctor 输出不含 token；结果目录 $CFG"
