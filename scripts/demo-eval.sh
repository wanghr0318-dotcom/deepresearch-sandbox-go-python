#!/usr/bin/env bash
# 评测平台演示（agentbox eval，零模型费用）：真实沙箱 + 真实 Gateway + 独立 exec 沙箱，fake upstream 扮演模型、
# 搜索与网页。以 root 在 Linux（cgroup v2）或 WSL2 中运行：
#
#   sudo bash scripts/demo-eval.sh
#
# 步骤：环境检查 → 全新的数据目录与数据库 → 安装 worker 包（agentbox_worker、deepresearch、evalworker）到
# /opt/agentbox → 构建 agentbox 与 fakeupstream → 启动 fake upstream（-eval-suite：编码任务按 suite 的 fake_reply
# 作答）→ 启动 server（--worker-argv python3,-m,evalworker，exec 默认开启）→ 运行 A：--agent reference
# （验证评测链路：参考解应全部通过）→ 运行 B：--agent model（fake 模型，其中 4 个回复故意有错）→ 运行 C：同 B，
# --concurrency 1 → compare A B、compare C B → 正常停止 → verify-invariants --quiescent → 泄漏检查。
# 任何一步失败即以非零状态退出。
#
# 环境变量：
#   AGENTBOX_DATABASE_URL     PostgreSQL 管理连接串（默认 postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable）
#   AGENTBOX_DEMO_DB          演示库名（默认 agentbox_demo_eval）
#   AGENTBOX_DEMO_DATA_DIR    数据目录（默认 /var/lib/agentbox-demo-eval）
#   AGENTBOX_DEMO_LISTEN      监听地址（默认 127.0.0.1:8090）
#   AGENTBOX_EVAL_SUITE       suite（默认 eval/suites/demo.yaml）
#   AGENTBOX_EVAL_OUT         运行结果目录（默认 <仓库>/eval-runs）
#   AGENTBOX_EVAL_CONCURRENCY 运行 A、B 的并发（默认 4）
#   AGENTBOX_DEMO_PREBUILT=1  不构建，直接使用 bin/agentbox 与 bin/fakeupstream（例如在带 VCS 信息的环境中预先
#                             构建：manifest 的 server.build 才有 vcs.revision）
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
BIN="$REPO/bin/agentbox"
FU_BIN="$REPO/bin/fakeupstream"
PG_ADMIN_URL="${AGENTBOX_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
DEMO_DB="${AGENTBOX_DEMO_DB:-agentbox_demo_eval}"
DATA="${AGENTBOX_DEMO_DATA_DIR:-/var/lib/agentbox-demo-eval}"
LISTEN="${AGENTBOX_DEMO_LISTEN:-127.0.0.1:8090}"
SUITE="${AGENTBOX_EVAL_SUITE:-$REPO/eval/suites/demo.yaml}"
OUT="${AGENTBOX_EVAL_OUT:-$REPO/eval-runs}"
CONC="${AGENTBOX_EVAL_CONCURRENCY:-4}"
export AGENTBOX_ADDR="http://$LISTEN"
WORKER_DIR=/opt/agentbox
CGROOT=/sys/fs/cgroup
LOGDIR=$(mktemp -d /tmp/agentbox-demo-eval.XXXXXX)
SERVER_PID=""
FU_PID=""
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
  stop_pid "$FU_PID"
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%02d] %s（退出码 %d）。日志目录：%s\n' "$N" "$STEP" "$rc" "$LOGDIR" >&2
    [ -f "$LOGDIR/server.log" ] && tail -n 20 "$LOGDIR/server.log" >&2
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
[ "$(id -u)" = 0 ] || fail "需要 root（sudo bash scripts/demo-eval.sh）"
[ "$(stat -fc %T $CGROOT)" = cgroup2fs ] || fail "$CGROOT 不是 cgroup v2"
ok "root，$CGROOT 为 cgroup2fs，内核 $(uname -r) $(uname -m)"
hostport=$(python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(p.hostname, p.port or 5432)" "$PG_ADMIN_URL")
read -r pghost pgport <<<"$hostport"
timeout 3 bash -c "</dev/tcp/$pghost/$pgport" 2>/dev/null || fail "PostgreSQL $pghost:$pgport 不可达（docker compose -f deploy/docker-compose.yml up -d --wait）"
ok "PostgreSQL $pghost:$pgport 可达；$(python3 --version)"
if curl -fsS "$AGENTBOX_ADDR/status" >/dev/null 2>&1; then fail "$LISTEN 上已有服务在运行"; fi
[ -f "$SUITE" ] || fail "没有 suite：$SUITE"

step "准备全新的数据目录 $DATA 与数据库 $DEMO_DB"
rm -rf "$DATA"
with_db() { python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(path='/'+sys.argv[2])))" "$PG_ADMIN_URL" "$1"; }
pg_sql() {
  local sql="$1" dk
  if command -v psql >/dev/null 2>&1; then
    psql -v ON_ERROR_STOP=1 "$(with_db postgres)" -qc "$sql" >/dev/null 2>&1 && return 0
  fi
  for dk in docker "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"; do
    { command -v "$dk" >/dev/null 2>&1 || [ -x "$dk" ]; } || continue
    "$dk" compose -f "$REPO/deploy/docker-compose.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
    "$dk" exec agentbox-pg psql -v ON_ERROR_STOP=1 -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
    # 任何发布了 PostgreSQL 端口的容器（例如另一份检出启动的 compose 项目）
    local c
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
ok "空库 $DEMO_DB；运维 token $DATA/api.token（0600，经环境变量交给 CLI，不打印）"

step "安装 worker 包到 $WORKER_DIR（agentbox_worker、deepresearch、evalworker）"
install -d -m 0755 "$WORKER_DIR"
for d in agentbox_worker deepresearch evalworker; do
  rm -rf "${WORKER_DIR:?}/$d"
  cp -r "$REPO/worker/$d" "$WORKER_DIR/"
done
find "$WORKER_DIR" -name __pycache__ -prune -exec rm -rf {} +
chmod -R u=rwX,go=rX "$WORKER_DIR"
[ -f "$WORKER_DIR/evalworker/__main__.py" ] || fail "$WORKER_DIR 中没有 evalworker"
ok "$(find "$WORKER_DIR" -name '*.py' | wc -l) 个 .py 文件"

if [ -n "${AGENTBOX_DEMO_PREBUILT:-}" ]; then
  step "使用预先构建的 $BIN 与 $FU_BIN"
  [ -x "$BIN" ] && [ -x "$FU_BIN" ] || fail "AGENTBOX_DEMO_PREBUILT 已设置，但缺少 $BIN 或 $FU_BIN"
else
  step "构建 agentbox（CGO_ENABLED=0）与 fakeupstream"
  (cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/agentbox && CGO_ENABLED=0 go build -o "$FU_BIN" ./tests/e2e/fakeupstream/cmd/fakeupstream)
fi
ok "$BIN、$FU_BIN"

step "启动 fake upstream（编码任务按 suite 的 fake_reply 作答；研究为固定脚本）"
"$FU_BIN" -eval-suite "$SUITE" >"$LOGDIR/fakeupstream.log" 2>&1 &
FU_PID=$!
wait_for "fake upstream 打印地址" 30 grep -q '^url=' "$LOGDIR/fakeupstream.log"
read -r fu_url fu_model fu_hostport < <(sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p' "$LOGDIR/fakeupstream.log")
ok "pid $FU_PID：$fu_url；$(grep -o '[0-9]* scripted code replies' "$LOGDIR/fakeupstream.log")"

step "启动 agentbox server（Worker = python3 -m evalworker；exec 沙箱默认开启）"
# 假 Key 只发给本机 fake upstream。
AGENTBOX_MODEL_API_KEY="fake-eval-key-$(od -An -tx1 -N8 /dev/urandom | tr -d ' \n')" \
  "$BIN" server --data-dir "$DATA" --listen "$LISTEN" --worker-argv python3,-m,evalworker \
  --model-base-url "$fu_model" --model-name kimi-k2.6 --models kimi-k2.6,kimi-k3 \
  --model-price-in-micro-per-mtok 1000000 --model-price-out-micro-per-mtok 2000000 \
  --search-provider fake --search-base-url "$fu_url" --upstream-allow-private "$fu_hostport" \
  >"$LOGDIR/server.log" 2>&1 &
SERVER_PID=$!
wait_for "server 就绪" 90 server_ready
ok "pid $SERVER_PID，$AGENTBOX_ADDR"
curl -fsS -H "Authorization: Bearer $AGENTBOX_TOKEN" "$AGENTBOX_ADDR/server-info" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("         server-info：revision", (d.get("build") or {}).get("vcs.revision", "-")[:12],
      "；worker", " ".join(d["worker_argv"]), "；models", d["models"].get("declared"),
      "；exec", d["exec"]["enabled"], d["exec"]["image_digest"][:19])'

mkdir -p "$OUT"
run_eval() { # run_eval <run-id> <标志...>
  local id=$1
  shift
  "$BIN" eval run --suite "$SUITE" --out "$OUT" --run-id "$id" "$@" | tee "$LOGDIR/$id.log" | sed 's/^/         /'
}

step "运行 A：--agent reference（参考解；验证评测链路），并发 $CONC"
A="demo-$STAMP-reference"
run_eval "$A" --agent reference --concurrency "$CONC"
[ "$(summary_field "$OUT/$A" "d['by_kind']['coding']['passed']")" = 10 ] || fail "参考解没有全部通过：评测链路有问题"
ok "编码任务 10/10 通过；研究 $(summary_field "$OUT/$A" "d['by_kind']['research']['passed']")/3"

step "运行 B：--agent model（fake 模型），并发 $CONC"
B="demo-$STAMP-model-c$CONC"
run_eval "$B" --agent model --concurrency "$CONC"
ok "成功率 $(summary_field "$OUT/$B" "d['success_rate']")；失败类别 $(summary_field "$OUT/$B" "d['failure_categories']")"

step "运行 C：--agent model，并发 1"
C="demo-$STAMP-model-c1"
run_eval "$C" --agent model --concurrency 1
ok "墙钟 $(summary_field "$OUT/$C" "d['wall_ms']") ms（B：$(summary_field "$OUT/$B" "d['wall_ms']") ms）"

step "compare A → B（参考解 → 模型）与 C → B（并发 1 → $CONC）"
"$BIN" eval compare "$OUT/$A" "$OUT/$B" | tee "$OUT/compare-reference-vs-model.md" | sed 's/^/         /'
"$BIN" eval compare "$OUT/$C" "$OUT/$B" | tee "$OUT/compare-c1-vs-c$CONC.md" | sed 's/^/         /'
ok "对比已写入 $OUT/compare-*.md"

step "正常停止 server（SIGTERM）与 fake upstream"
kill -TERM "$SERVER_PID"
rc=0
wait "$SERVER_PID" || rc=$?
SERVER_PID=""
[ "$rc" = 0 ] || fail "server 退出码 $rc"
kill -TERM "$FU_PID"
wait "$FU_PID" 2>/dev/null || true
FU_PID=""
ok "server 退出码 0；fake upstream $(grep '^counts ' "$LOGDIR/fakeupstream.log" || echo '未打印计数')"

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
if grep -rqF "$AGENTBOX_TOKEN" "$OUT/$A" "$OUT/$B" "$OUT/$C"; then fail "token 出现在运行结果中"; fi
ok "没有残留环境与进程；运行结果不含 token"
