#!/usr/bin/env bash
# M1 真实沙箱演示：以 root 在 Linux（cgroup v2）或 WSL2 中运行，逐步打印每一步及其结果。
#
#   sudo AGENTBOX_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable' \
#     bash scripts/demo-m1.sh
#
# 步骤：环境检查（PostgreSQL 可达、python3 ≥ 3.11、安装 worker 包到 /opt/agentbox、构建、doctor）→ 启动
# server → 首个切片（提交 → checkpoint → 杀死 Worker → 恢复 → result → 清理）→ 运行中的任务遇到 server
# SIGKILL → 重启并恢复 → 正常停止 → verify-invariants --quiescent → 泄漏检查（没有残留的 agentbox cgroup、
# 环境目录与沙箱进程）。任何一步失败即以非零状态退出，并打印失败的步骤名。
#
# 每次运行都使用全新的数据目录（默认 /var/lib/agentbox-demo，先清空）与全新的数据库（默认 agentbox_demo，经
# docker compose 的 postgres 服务 DROP + CREATE），因此可重复执行、与 README 快速开始的 /var/lib/agentbox 互不影响。
# 可选环境变量：AGENTBOX_DATABASE_URL（用于探测 PostgreSQL 可达性；默认 127.0.0.1:5432）、AGENTBOX_DEMO_DB、
# AGENTBOX_DEMO_DATA_DIR、AGENTBOX_DEMO_DATABASE_URL（自备空库时设置，跳过新建）、AGENTBOX_DEMO_LISTEN（默认 127.0.0.1:8080）。
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
BIN="$REPO/bin/agentbox"
# 演示每次都在全新的数据目录与数据库上运行（数据目录与数据库以安装身份一一绑定，复用会把上次的状态带进来）：
# 数据库由 deploy/docker-compose.yml 的 postgres 服务新建（DROP + CREATE）；若自行提供 AGENTBOX_DEMO_DATABASE_URL，
# 则按原样使用（须为空库）。PG_ADMIN_URL 只用于探测可达性与定位 docker 服务。
PG_ADMIN_URL="${AGENTBOX_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
DEMO_DB="${AGENTBOX_DEMO_DB:-agentbox_demo}"
DATA="${AGENTBOX_DEMO_DATA_DIR:-/var/lib/agentbox-demo}"
LISTEN="${AGENTBOX_DEMO_LISTEN:-127.0.0.1:8080}"
export AGENTBOX_ADDR="http://$LISTEN"
WORKER_DIR=/opt/agentbox
CGROOT=/sys/fs/cgroup
LOGDIR=$(mktemp -d /tmp/agentbox-demo.XXXXXX)
SERVER_PID=""
STEP="初始化"
N=0

step() { N=$((N + 1)); STEP="$1"; printf '\n[%02d] %s\n' "$N" "$1"; }
ok() { printf '     OK  %s\n' "$*"; }
info() { printf '         %s\n' "$*"; }
fail() { printf '     FAIL %s\n' "$*" >&2; exit 1; }

on_exit() {
  local rc=$?
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%02d] %s（退出码 %d）。server 日志：%s\n' "$N" "$STEP" "$rc" "$LOGDIR" >&2
    tail -n 20 "$LOGDIR"/server-*.log 2>/dev/null >&2 || true
  else
    printf '\n演示完成：全部 %d 步通过。server 日志：%s\n' "$N" "$LOGDIR"
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

inspect() { "$BIN" task inspect "$1"; }
task_field() { inspect "$1" | json "d['task']['$2']"; }
is_terminal() { case "$(task_field "$1" status)" in succeeded | failed | cancelled) return 0 ;; esac; return 1; }
checkpoints() { inspect "$1" | json "len(d['checkpoints'])"; }
has_checkpoint() { [ "$(checkpoints "$1")" -ge 1 ]; }
install_cgroup() { echo "$CGROOT/agentbox-$(cat "$DATA/install_id")"; }
env_of() { inspect "$1" | json "[a for a in d['attempts'] if a['attempt_no'] == $2][0]['env_id']"; }

server_ready() { curl -fsS "$AGENTBOX_ADDR/status" | grep -q '"normal"'; }
start_server() {
  local log="$LOGDIR/server-$1.log"
  "$BIN" server --data-dir "$DATA" --listen "$LISTEN" >"$log" 2>&1 &
  SERVER_PID=$!
  wait_for "server 进入 normal 模式" 60 server_ready
  ok "server pid $SERVER_PID 已就绪（$AGENTBOX_ADDR，日志 $log）"
}

# 环境全部停止并清理：所有 attempt 的 cleanup_state 为 done，数据目录中没有环境目录。
cleaned() {
  [ "$(inspect "$1" | json "all(a['cleanup_state'] == 'done' and a.get('stopped_at') for a in d['attempts'])")" = True ] &&
    [ -z "$(ls -A "$DATA/envs" 2>/dev/null)" ]
}

submit() { "$BIN" task submit --spec "$1" | json "d['task_id']"; }

# ---------------------------------------------------------------------------

step "环境检查：root、cgroup v2、PostgreSQL、python3"
[ "$(id -u)" = 0 ] || fail "需要 root（sudo bash scripts/demo-m1.sh）"
[ "$(stat -fc %T $CGROOT)" = cgroup2fs ] || fail "$CGROOT 不是 cgroup v2"
ok "root，$CGROOT 为 cgroup2fs，内核 $(uname -r) $(uname -m)"
hostport=$(python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(p.hostname, p.port or 5432)" "$PG_ADMIN_URL")
read -r pghost pgport <<<"$hostport"
timeout 3 bash -c "</dev/tcp/$pghost/$pgport" 2>/dev/null || fail "PostgreSQL $pghost:$pgport 不可达（docker compose -f deploy/docker-compose.yml up -d --wait）"
ok "PostgreSQL $pghost:$pgport 可达"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' || fail "需要 python3 ≥ 3.11（$(python3 --version)）"
ok "$(command -v python3)：$(python3 --version)"
if curl -fsS "$AGENTBOX_ADDR/status" >/dev/null 2>&1; then fail "$LISTEN 上已有服务在运行"; fi

step "准备全新的数据目录 $DATA 与数据库 $DEMO_DB"
rm -rf "$DATA"
if [ -n "${AGENTBOX_DEMO_DATABASE_URL:-}" ]; then
  export AGENTBOX_DATABASE_URL="$AGENTBOX_DEMO_DATABASE_URL"
  ok "使用给定的数据库连接串（须为空库）"
else
  # 依次尝试：本机 psql → docker（compose 服务 postgres，或容器 agentbox-pg）→ Docker Desktop 的 docker.exe
  # （WSL2 未开启 WSL 集成时经 interop 调用）。
  pg_sql() {
    local sql="$1" dk
    if command -v psql >/dev/null 2>&1; then
      PGPASSWORD=agentbox psql -v ON_ERROR_STOP=1 -h "$pghost" -p "$pgport" -U agentbox -d postgres -qc "$sql" >/dev/null 2>&1 && return 0
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
  export AGENTBOX_DATABASE_URL="postgres://agentbox:agentbox@$pghost:$pgport/$DEMO_DB?sslmode=disable"
  ok "已新建空库 $DEMO_DB（docker compose 的 postgres 服务）"
fi
ok "数据目录已清空：$DATA"

step "安装 worker 包到 $WORKER_DIR（默认 rootfs 模板的一部分）"
install -d -m 0755 "$WORKER_DIR"
rm -rf "$WORKER_DIR/agentbox_worker" "$WORKER_DIR/sim_worker"
cp -r "$REPO/worker/agentbox_worker" "$REPO/worker/sim_worker" "$WORKER_DIR/"
find "$WORKER_DIR" -name __pycache__ -prune -exec rm -rf {} +
chmod -R u=rwX,go=rX "$WORKER_DIR"
ok "$(find "$WORKER_DIR" -name '*.py' | wc -l) 个 .py 文件"

step "构建（CGO_ENABLED=0）并运行 doctor"
(cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/agentbox)
ok "$BIN"
"$BIN" doctor | sed 's/^/         /'

step "启动 agentbox server（生产启动器、默认模板 $WORKER_DIR）"
start_server 1

step "首个切片：提交任务（progress → 产物 → checkpoint → sleep → 产物 → result）"
SPEC1='{"steps":[{"op":"progress","message":"collecting"},{"op":"artifact","artifact_id":"notes","path":"notes.txt","content":"draft notes"},{"op":"checkpoint","step_id":"collect"},{"op":"sleep","ms":3000},{"op":"artifact","artifact_id":"report","path":"report/final.md","content":"# final report"}],"summary":"report ready","outputs":["notes","report"]}'
T1=$(submit "$SPEC1")
ok "task_id $T1"

step "等待 checkpoint 提交"
wait_for "checkpoint collect 已提交" 60 has_checkpoint "$T1"
ok "$(inspect "$T1" | json "d['checkpoints'][0]['step_id'] + ' commit_seq=' + str(d['checkpoints'][0]['commit_seq'])")"

step "杀死沙箱中的 Worker（宿主向环境 cgroup 中的 sim_worker 发 SIGKILL）"
ENV1=$(env_of "$T1" 1)
CG1="$(install_cgroup)/env-$ENV1"
pids=$(cat "$CG1/cgroup.procs")
info "环境 cgroup $CG1：进程 $(echo $pids)"
killed=0
for p in $pids; do
  if tr '\0' ' ' <"/proc/$p/cmdline" 2>/dev/null | grep -q sim_worker; then
    info "pid $p：$(tr '\0' ' ' <"/proc/$p/cmdline")（uid $(awk '/^Uid:/{print $2}' "/proc/$p/status")）"
    kill -KILL "$p"
    killed=$((killed + 1))
  fi
done
[ "$killed" -ge 1 ] || fail "环境中没有 sim_worker 进程"
ok "已杀死 $killed 个 Worker 进程"

step "等待恢复：新 attempt 从 checkpoint 继续并产生 result"
wait_for "任务 $T1 到达终态" 90 is_terminal "$T1"
inspect "$T1" | json "'\n'.join('attempt %d：%s（exit_code=%s exit_signal=%s）' % (a['attempt_no'], a['outcome_class'], a.get('exit_code'), a.get('exit_signal')) for a in d['attempts'])" | sed 's/^/         /'
[ "$(task_field "$T1" status)" = succeeded ] || fail "任务结束为 $(task_field "$T1" status)"
[ "$(inspect "$T1" | json "[a['outcome_class'] for a in d['attempts']] == ['crashed_signal', 'succeeded']")" = True ] || fail "attempt 分类不符"
ok "succeeded：attempt 1 crashed_signal（SIGKILL），attempt 2 恢复后 succeeded"

step "result"
"$BIN" task watch "$T1" | jsonl "'%s: %s' % (d['type'], json.dumps(d['payload'], ensure_ascii=False))" | grep -E '^(checkpoint_committed|result|task_terminal|artifact_saved):' | sed 's/^/         /'
ok "result 的 summary 与固定输出见上（产物下载端点在 M1 为 501）"

step "清理：环境停止、销毁，环境目录删除"
wait_for "任务 $T1 的环境清理完成" 60 cleaned "$T1"
[ ! -e "$CG1" ] || fail "环境 cgroup $CG1 仍存在"
ok "两个 attempt 的环境 cleanup_state=done，$DATA/envs 为空，环境 cgroup 已删除"

step "运行中的任务：提交（checkpoint s1 → sleep 5 s → 产物 → result）"
SPEC2='{"steps":[{"op":"artifact","artifact_id":"a1","path":"a1.txt","content":"one"},{"op":"checkpoint","step_id":"s1"},{"op":"sleep","ms":5000},{"op":"artifact","artifact_id":"a2","path":"a2.txt","content":"two"}],"summary":"survived restart","outputs":["a1","a2"]}'
T2=$(submit "$SPEC2")
wait_for "checkpoint s1 已提交" 60 has_checkpoint "$T2"
ENV2=$(env_of "$T2" 1)
CG2="$(install_cgroup)/env-$ENV2"
ok "task_id $T2，checkpoint s1 已提交，Worker 在环境 $ENV2 中运行（$(wc -l <"$CG2/cgroup.procs") 个进程）"

step "以 SIGKILL 杀死 server"
kill -KILL "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
sleep 1
left=$(cat "$CG2/cgroup.procs" 2>/dev/null | wc -l)
[ -d "$DATA/envs/$ENV2" ] && [ -d "$CG2" ] || fail "server 被杀死后应留下孤儿环境（目录与 cgroup）"
ok "server 已被 SIGKILL；孤儿环境：目录 $DATA/envs/$ENV2、cgroup $CG2（存活进程 $left 个）"

step "重启 server：启动恢复"
start_server 2
grep -E '"msg":"(恢复扫描|启动恢复完成)"' "$LOGDIR/server-2.log" |
  jsonl "'%s %s' % (d['msg'], json.dumps({k: v for k, v in d.items() if k not in ('time', 'level', 'msg')}, ensure_ascii=False))" |
  sed 's/^/         /'

step "等待运行中的任务恢复并完成"
wait_for "任务 $T2 到达终态" 90 is_terminal "$T2"
inspect "$T2" | json "'\n'.join('attempt %d：%s' % (a['attempt_no'], a['outcome_class']) for a in d['attempts'])" | sed 's/^/         /'
[ "$(task_field "$T2" status)" = succeeded ] || fail "任务结束为 $(task_field "$T2" status)"
[ "$(inspect "$T2" | json "[a['outcome_class'] for a in d['attempts']] == ['lost_on_restart', 'succeeded']")" = True ] || fail "attempt 分类不符"
wait_for "任务 $T2 的环境清理完成" 60 cleaned "$T2"
[ ! -e "$CG2" ] || fail "孤儿环境 cgroup $CG2 未被回收"
ok "succeeded：attempt 1 lost_on_restart，attempt 2 从 s1 恢复；孤儿环境已回收"
[ "$(curl -fsS "$AGENTBOX_ADDR/tasks" | json "len(d['tasks'])")" -ge 2 ] || fail "GET /tasks"
"$BIN" status | sed 's/^/         /'

step "正常停止 server（SIGTERM）"
kill -TERM "$SERVER_PID"
rc=0
wait "$SERVER_PID" || rc=$?
SERVER_PID=""
[ "$rc" = 0 ] || fail "server 退出码 $rc"
ok "退出码 0"

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
if pgrep -f 'sim_worker' >/dev/null || pgrep -f '^/proc/self/exe (init|sandbox-launch)' >/dev/null; then
  pgrep -af 'sim_worker|^/proc/self/exe (init|sandbox-launch)' | sed 's/^/         /'
  fail "仍有沙箱进程"
fi
ok "没有环境目录、没有带进程或环境的 agentbox cgroup（本安装的空 cgroup 已删除）、没有 sim_worker / init / sandbox-launch 进程"
