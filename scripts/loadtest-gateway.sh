#!/usr/bin/env bash
# 模型网关负载测试（docs/design/2026-10-11-model-gateway-design.md §10）：在本机启动 PostgreSQL 中的一个新库、
# 一个专用的 Redis 容器（停止它不影响别的服务）、两个 fake upstream（OpenAI 格式与 Anthropic 格式的供应商，
# 不访问外网，没有模型费用）和带 --model-api 的 agentbox server，然后以 cmd/agentbox-loadtest 依次运行场景，
# 结果写入工作目录（results.jsonl 与 summary.md）。不需要 root。需要：docker（未设置 AGENTBOX_DATABASE_URL 时以
# deploy/docker-compose.yml 启动 PostgreSQL；另起一个专用 Redis 容器）、Go、python3、curl。
#
#   bash scripts/loadtest-gateway.sh                       # 全部场景，各一次
#   AGENTBOX_LT_REPEAT=3 bash scripts/loadtest-gateway.sh latency ladder budget lockwait
#
# 场景：latency（网关开销与首字开销）、ladder（最大 RPS）、latency-limits 与 ladder-limits（同上，但三级都设了很高的
# rpm/tpm，限流器在路径上）、ratelimit（限流精度，60 s）、budget（并发下的超支，
# 期间每 5 s 运行一次 verify-invariants）、lockwait（同一 org 的预算行锁等待）、redis（运行中停止 Redis：
# 限流器降级，客户端没有错误；之后重新启动 Redis）。结束时运行 verify-invariants 与
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

gw_reset_work
: >"$RESULTS"
gw_prepare

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
# 直接请求的基线（latency 场景）发往单独的 fake，供应商 fake 只收到经网关的请求（结束时据此独立核对）。
gw_start_fake DIRECT -latency "chat=$LATENCY,messages=$LATENCY"
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
  --fake-openai "$FAKE_DIRECT_V1" --fake-anthropic "$FAKE_DIRECT_URL" --out "$RESULTS")
# latency-limits / ladder-limits：在 org、用户、key 三级设很高的 rpm 与 tpm——每个请求都经过 Redis 上的 Lua 限流
# 脚本，但没有请求被拒绝；与 latency / ladder（不设限额，限流器不在路径上）对照。
LIMITS_ON=(--rpm 1000000 --tpm 1000000000)

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
      latency-limits) run_lt latency "run $r, limits on" --concurrency 1 --duration 6s --warmup 1s "${LIMITS_ON[@]}" ;;
      ladder) run_lt ladder "run $r" --duration 8s --orgs 16 ;;
      ladder-limits) run_lt ladder "run $r, limits on" --duration 8s --orgs 16 "${LIMITS_ON[@]}" ;;
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
  "$GW_BIN/agentbox" bill reconcile "$(date -u +%Y-%m)" 2>&1 || echo "bill reconcile 退出码 $?"
  echo '```'
  echo
  echo "model_requests：$(gw_sql "SELECT sum(n) || ' 行，状态 ' || string_agg(s || '×' || n, ' ') FROM (SELECT status::text s, count(*) n FROM model_requests GROUP BY 1 ORDER BY 1) x")"
  echo "quota_holds：$(gw_sql "SELECT string_agg(state || '×' || n, ' ') FROM (SELECT state, count(*) n FROM quota_holds GROUP BY 1 ORDER BY 1) x")"
} | tee -a "$SUMMARY"

# 独立核对：停止 server 与供应商 fake，用 fake 自己的计数（收到的请求、回复中报告的 token）对照网关的记录——
# 每个到达供应商的请求都有一条 try、一个未释放的 hold；网关记录的 token 等于供应商报告的 token。
gw_say "== 独立核对（fake upstream 的计数 vs 网关的记录）"
gw_stop_server
for p in "$FAKE_OA_PID" "$FAKE_AN_PID" "$FAKE_SLOW_PID"; do gw_stop_pid "$p"; done
fake_field() { sed -nE "s/^$2 .*$3=([0-9+]+).*/\1/p" "$GW_WORK/fake-$1.log" | tail -n1; }
oa_n=$(fake_field OA counts chat) an_n=$(fake_field AN counts messages) slow_n=$(fake_field SLOW counts chat)
oa_u=$(fake_field OA usage chat) an_u=$(fake_field AN usage messages)
tries() { gw_sql "SELECT count(*) FROM model_request_tries WHERE provider = '$1'"; }
check_eq() { # check_eq 描述 期望 实际
  if [ "$2" = "$3" ]; then echo "PASS $1：$2"; else echo "FAIL $1：fake $2，网关 $3"; fi
}
{
  echo "## 独立核对（fake upstream 的计数 vs 网关的记录）"
  echo
  check_eq "OpenAI 供应商收到的请求 = 网关记录的 primary try" "$oa_n" "$(tries primary)"
  check_eq "Anthropic 供应商收到的请求 = 网关记录的 claude try" "$an_n" "$(tries claude)"
  check_eq "慢供应商收到的请求 = 网关记录的 slow try" "$slow_n" "$(tries slow)"
  sent=$((oa_n + an_n + slow_n))
  holds=$(gw_sql "SELECT count(*) FROM quota_holds WHERE source = 'model_request' AND state <> 'released'")
  rows=$(gw_sql "SELECT count(*) FROM model_requests")
  echo "供应商共收到 $sent 个请求；model_requests $rows 行；未释放的 model_request hold $holds 个"
  if [ "$holds" -ge "$sent" ]; then echo "PASS 每个到达供应商的请求都有 hold（hold $holds ≥ 请求 $sent）"; else echo "FAIL hold $holds < 请求 $sent"; fi
  gw_usage=$(gw_sql "SELECT coalesce(sum(prompt_tokens), 0) || '+' || coalesce(sum(completion_tokens), 0) FROM model_requests WHERE provider IN ('primary', 'claude')")
  fake_p=$(( ${oa_u%+*} + ${an_u%+*} )) fake_c=$(( ${oa_u#*+} + ${an_u#*+} ))
  check_eq "OpenAI 与 Anthropic 供应商报告的 token（输入+输出）= model_requests 的合计" "$fake_p+$fake_c" "$gw_usage"
  echo "（慢供应商只接收被客户端放弃的流，用量没有到达网关，记为 charged_unknown，不计入 token 核对。）"
  echo
} | tee -a "$SUMMARY"
if grep -q '^FAIL' "$SUMMARY"; then gw_fail "独立核对有差异（见 $SUMMARY）"; fi
