#!/usr/bin/env bash
# 缓存收益测量（规格 §16.6；Plan 9 Task 5）。
#
# 两组（共享缓存开、关）各 N 次重复同一组查询（1 个搜索 + 3 个抓取）：每次重复是新的任务、新的调用 ID（测跨任务的
# 共享缓存，而不是 journal 重放）。上游是进程内 fake upstream，正常回复前固定延迟；记录命中率、命中与上游延迟分布、
# 上游请求减少量与环境。实现是 tests/e2e 的 TestCacheBenefit（进程内装置：真实 PostgreSQL、真实 Redis、sim_worker）。
#
# 在 Linux（或 WSL2）中运行（脚本先切换到仓库根目录）：
#   bash bench/cache/run.sh
# 环境变量（括号内为默认值，与 deploy/docker-compose.yml 的本地服务一致）：
#   AGENTBOX_TEST_DATABASE_URL（postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable）
#   AGENTBOX_TEST_REDIS_ADDR（127.0.0.1:6379）
#   AGENTBOX_CACHE_BENCH_RUNS（10）          每组重复次数
#   AGENTBOX_CACHE_BENCH_LATENCY_MS（200）   fake upstream 每个回复前的延迟
#   AGENTBOX_CACHE_BENCH_OUT（$TMPDIR/agentbox-cache-bench-<UTC 时间>.md）  结果（Markdown：环境、汇总、原始数据）
# 需要 go 与 python3（sim_worker）。结果是本机 fake upstream 上的测量，不代表真实网络。
set -euo pipefail

cd "$(dirname "$0")/../.."

export AGENTBOX_TEST_DATABASE_URL="${AGENTBOX_TEST_DATABASE_URL:-postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable}"
export AGENTBOX_TEST_REDIS_ADDR="${AGENTBOX_TEST_REDIS_ADDR:-127.0.0.1:6379}"
export AGENTBOX_CACHE_BENCH_RUNS="${AGENTBOX_CACHE_BENCH_RUNS:-10}"
export AGENTBOX_CACHE_BENCH_LATENCY_MS="${AGENTBOX_CACHE_BENCH_LATENCY_MS:-200}"
export AGENTBOX_CACHE_BENCH_OUT="${AGENTBOX_CACHE_BENCH_OUT:-${TMPDIR:-/tmp}/agentbox-cache-bench-$(date -u +%Y%m%dT%H%M%SZ).md}"
# 缺少 PostgreSQL、Redis 或 python3 时失败而不是跳过。
export CI=true

echo "缓存收益测量：每组 ${AGENTBOX_CACHE_BENCH_RUNS} 次，上游延迟 ${AGENTBOX_CACHE_BENCH_LATENCY_MS} ms，Redis ${AGENTBOX_TEST_REDIS_ADDR}"
log="${AGENTBOX_CACHE_BENCH_OUT%.md}.log"
status=0
go test -count=1 -run '^TestCacheBenefit$' -v ./tests/e2e/ >"$log" 2>&1 || status=$?
grep -E '^(--- |ok|FAIL)' "$log" || true
if [ "$status" -ne 0 ] || [ ! -s "$AGENTBOX_CACHE_BENCH_OUT" ]; then
	tail -n 60 "$log" >&2
	echo "测量失败（go test 退出码 $status），完整日志：$log" >&2
	exit 1
fi
cat "$AGENTBOX_CACHE_BENCH_OUT"
echo
echo "结果已写入 $AGENTBOX_CACHE_BENCH_OUT"
