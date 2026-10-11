#!/usr/bin/env bash
# 模型网关负载测试（docs/design/2026-10-11-model-gateway-design.md §10）：在本机启动 PostgreSQL 中的一个新库、
# 一个专用的 Redis 容器（停止它不影响别的服务）、两个 fake upstream（OpenAI 格式与 Anthropic 格式的供应商，
# 不访问外网，没有模型费用）和带 --model-api 的 agentbox server，然后以 cmd/agentbox-loadtest 依次运行场景，
# 结果写入工作目录（results.jsonl 与 summary.md）。不需要 root。
#
#   bash scripts/loadtest-gateway.sh                       # 全部场景，各一次
#   AGENTBOX_LT_REPEAT=3 bash scripts/loadtest-gateway.sh latency ladder budget lockwait
#
# 场景：latency（网关开销与首字开销）、ladder（最大 RPS）、ratelimit（限流精度，60 s）、budget（并发下的超支，
# 期间每 5 s 运行一次 verify-invariants）、lockwait（同一 org 的预算行锁等待）、redis（运行中停止 Redis：
# 限流器降级，客户端没有错误；之后重新启动 Redis）。结束时运行 verify-invariants，有 bill 子命令时运行
# bill reconcile。每个场景不超过约 2 分钟。
#
# 环境变量（另见 scripts/lib/gateway.sh）：
#   AGENTBOX_LT_REPEAT        每个场景重复次数（默认 1）
#   AGENTBOX_LT_FAKE_LATENCY  fake upstream 回复前的固定延迟（默认 0，测量的是网关自身的开销）
#   AGENTBOX_LT_REDIS_PORT    专用 Redis 容器的端口（默认 6390；容器名 agentbox-loadtest-redis）
#   AGENTBOX_LT_SEED          随机种子（默认 1）
#   AGENTBOX_LT_PG_OPTIONS    附加到 server 连接串的 PostgreSQL options（例如 "-c synchronous_commit=off"，只用于分解开销）
set -euo pipefail

GW_NAME=loadtest
# shellcheck source=scripts/lib/gateway.sh
source "$(dirname "$0")/lib/gateway.sh"

REPEAT="${AGENTBOX_LT_REPEAT:-1}"
LATENCY="${AGENTBOX_LT_FAKE_LATENCY:-0s}"
REDIS_PORT="${AGENTBOX_LT_REDIS_PORT:-6390}"
REDIS_NAME=agentbox-loadtest-redis
SEED="${AGENTBOX_LT_SEED:-1}"
SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -gt 0 ] || SCENARIOS=(latency ladder ratelimit budget lockwait redis)
RESULTS="$GW_WORK/results.jsonl"
SUMMARY="$GW_WORK/summary.md"

on_exit() {
  local rc=$?
  gw_cleanup
  [ -n "$GW_DOCKER" ] && gw_docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
  if [ "$rc" -ne 0 ]; then
    printf '\n负载测试失败（退出码 %d）。工作目录：%s\n' "$rc" "$GW_WORK" >&2
  else
    printf '\n负载测试完成。结果：%s；汇总：%s\n' "$RESULTS" "$SUMMARY"
  fi
}
trap on_exit EXIT

rm -rf "$GW_WORK"
mkdir -p "$GW_WORK"
: >"$RESULTS"
gw_detect_docker

{
  echo "# 模型网关负载测试 $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "- 主机：$(uname -sr)；$(uname -m)；$(nproc) 个逻辑 CPU（$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')）；内存 $(free -g | awk '/^Mem:/{print $2}') GiB"
  echo "- 负载：开始前 1/5/15 分钟负载 $(cut -d' ' -f1-3 /proc/loadavg)（CPU 与其他进程共享，数字含噪声）"
  echo "- $(go version 2>/dev/null || echo 'go: 未构建')；PostgreSQL 与 Redis：docker（$GW_DOCKER）"
  echo "- fake upstream 回复前延迟：$LATENCY；种子：$SEED；重复：$REPEAT"
  echo
} >"$SUMMARY"

gw_say "== 构建"
gw_build
gw_say "== 新库 agentbox_loadtest、专用 Redis（127.0.0.1:$REDIS_PORT）、fake upstream"
gw_fresh_db agentbox_loadtest
if [ -n "${AGENTBOX_LT_PG_OPTIONS:-}" ]; then # 例如 "-c synchronous_commit=off"：只用于分解开销，不是推荐配置
  AGENTBOX_DATABASE_URL="$AGENTBOX_DATABASE_URL&options=$(python3 -c "import sys,urllib.parse as u; print(u.quote(sys.argv[1]))" "$AGENTBOX_LT_PG_OPTIONS")"
  export AGENTBOX_DATABASE_URL
fi
gw_docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
gw_docker run -d --name "$REDIS_NAME" -p "127.0.0.1:$REDIS_PORT:6379" redis:7-alpine redis-server --save '' --appendonly no >/dev/null
gw_wait_for "Redis 就绪" 30 bash -c "exec 3<>/dev/tcp/127.0.0.1/$REDIS_PORT"
gw_start_fake OA -latency "chat=$LATENCY"
gw_start_fake AN -latency "messages=$LATENCY"
# 慢供应商：流式事件之间 20 ms（budget 场景中客户端在首字后放弃的流发往它，结算为 charged_unknown）。
gw_start_fake SLOW -chunk-delay chat=20ms
cat >"$GW_WORK/fallback.json" <<EOF
{"providers": [
  {"name": "claude", "protocol": "anthropic", "base_url": "$FAKE_AN_URL", "models": {"m-an": "fake-model"},
   "price": {"in": 1000000, "out": 2000000}},
  {"name": "slow", "base_url": "$FAKE_SLOW_V1", "models": {"m-slow": "fake-model"}, "price": {"in": 1000000, "out": 2000000}}],
 "routing": {"primary_models": {"m-oa": "fake-model"}}}
EOF
gw_new_data_dir
gw_base_flags "$FAKE_OA_V1" "$FAKE_OA_HOSTPORT,$FAKE_AN_HOSTPORT,$FAKE_SLOW_HOSTPORT" "$GW_WORK/fallback.json" m-oa,m-an,m-slow
gw_say "== 启动 server（--model-api，Redis 127.0.0.1:$REDIS_PORT）"
gw_start_server --redis-addr "127.0.0.1:$REDIS_PORT"
gw_say "   server pid $GW_SERVER_PID，$GW_ADDR；OpenAI 供应商 $FAKE_OA_URL，Anthropic 供应商 $FAKE_AN_URL"

LT=("$GW_BIN/agentbox-loadtest")
COMMON=(--addr "$GW_ADDR" --token-file "$GW_DATA/api.token" --database-url "$AGENTBOX_DATABASE_URL" --seed "$SEED"
  --fake-openai "$FAKE_OA_V1" --fake-anthropic "$FAKE_AN_URL" --out "$RESULTS")

run_lt() { # run_lt SCENARIO LABEL [参数…]
  local s=$1 label=$2
  shift 2
  {
    echo "## $s（$label）"
    echo
    "${LT[@]}" "$s" "${COMMON[@]}" --label "$label" "$@"
    echo
  } | tee -a "$SUMMARY"
}

verify_loop() { # 每 5 s 运行一次 verify-invariants，直到 $GW_WORK/stop-verify 出现
  local n=0 bad=0
  while [ ! -e "$GW_WORK/stop-verify" ]; do
    if gw_verify >>"$GW_WORK/verify-during.log" 2>&1; then n=$((n + 1)); else bad=$((bad + 1)); fi
    sleep 5
  done
  echo "verify-invariants during the run: $n passed, $bad failed" >"$GW_WORK/verify-during.txt"
}

for s in "${SCENARIOS[@]}"; do
  for ((r = 1; r <= REPEAT; r++)); do
    gw_say "== 场景 $s（第 $r/$REPEAT 次）"
    case "$s" in
      latency) run_lt latency "run $r" --concurrency 1 --duration 6s --warmup 1s ;;
      ladder) run_lt ladder "run $r" --duration 8s --orgs 16 ;;
      ratelimit) run_lt ratelimit "run $r" --duration 60s --levels 60,600,3000 ;;
      budget)
        rm -f "$GW_WORK/stop-verify"
        verify_loop &
        vpid=$!
        run_lt budget "run $r" --concurrency 64 --users 4 --duration 60s --budget-usd 0.5 --key-budget-usd 0.1
        touch "$GW_WORK/stop-verify"
        wait "$vpid" || true
        { cat "$GW_WORK/verify-during.txt"; echo; } | tee -a "$SUMMARY"
        ;;
      lockwait) run_lt lockwait "run $r" --concurrency 64 --keys 16 --org-counts 1,4,16 --duration 12s ;;
      redis)
        run_lt steady "run $r" --concurrency 8 --duration 45s --metrics-url "http://$GW_METRICS/metrics" \
          --hook "15s=$(printf '%q' "$GW_DOCKER") stop $REDIS_NAME" --hook "30s=$(printf '%q' "$GW_DOCKER") start $REDIS_NAME"
        ;;
      *) gw_fail "未知场景 $s" ;;
    esac
  done
done

gw_say "== 结束检查"
{
  echo "## 结束检查"
  echo
  echo '```'
  gw_verify 2>&1 || echo "verify-invariants 退出码 $?"
  if gw_has_bill; then
    "$GW_BIN/agentbox" bill reconcile "$(date -u +%Y-%m)" 2>&1 || echo "bill reconcile 退出码 $?"
  else
    echo "bill reconcile：此版本没有 bill 子命令（G5），跳过"
  fi
  echo '```'
  echo
  echo "model_requests：$(gw_sql "SELECT sum(n) || ' 行，状态 ' || string_agg(s || '×' || n, ' ') FROM (SELECT status::text s, count(*) n FROM model_requests GROUP BY 1 ORDER BY 1) x")"
  echo "quota_holds：$(gw_sql "SELECT string_agg(state || '×' || n, ' ') FROM (SELECT state, count(*) n FROM quota_holds GROUP BY 1 ORDER BY 1) x")"
} | tee -a "$SUMMARY"
