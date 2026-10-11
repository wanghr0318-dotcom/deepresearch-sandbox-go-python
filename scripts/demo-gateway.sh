#!/usr/bin/env bash
# 模型网关演示（docs/design/2026-10-11-model-gateway-design.md §10）：多租户、平台 API key、OpenAI ⇄ Anthropic
# 双向转换、加权路由、限流与月度预算、吊销、用量与账单、Prometheus 指标。供应商是本机的 fake upstream（不访问
# 外网，没有模型费用）；不需要 root。可重复运行（每次重建演示库与数据目录），构建之后约 1 分钟。
#
#   bash scripts/demo-gateway.sh
#
# 需要：docker（PostgreSQL 与 Redis 由 deploy/docker-compose.yml 启动；WSL2 中可用 Docker Desktop）、Go、curl、
# python3，以及 uv（官方 openai 与 anthropic SDK 由 uv 按 tests/sdk 的锁文件安装）。WSL2 中没有 uv 时可用 Windows
# 的 uv.exe：AGENTBOX_DEMO_UV=/mnt/c/.../uv.exe（路径与环境变量自动转换）。
#
# 步骤：
#   1. 启动 PostgreSQL/Redis、两个 fake upstream（OpenAI 格式与 Anthropic 格式）与 agentbox server（账号 +
#      --model-api + 降级链文件：权重与 primary_models）
#   2. 两个用户注册；运维建 org acme（alice 为 owner）；alice 邀请 bob，bob 接受；各自建 models scope 的 key
#   3. 官方 openai 与 anthropic SDK 调用：同格式原样转发、OpenAI 客户端 → Anthropic 供应商、反向，含流式与工具调用；
#      加权路由（同一个逻辑模型按 3:1 分给两个供应商）
#   4. 限流 429（带 x-ratelimit-* 头），然后预算用尽 429 insufficient_quota
#   5. 吊销 key → 401
#   6. 用量、账单与对账（有 usage / bill 子命令时；否则打印 model_requests 汇总）与 verify-invariants
#   7. Prometheus 指标：成功率、首字时间、流完整性、按原因的拒绝数
#
# 环境变量另见 scripts/lib/gateway.sh；AGENTBOX_DEMO_UV 为 uv 可执行文件（默认 uv）。
set -euo pipefail

GW_NAME=demo-gateway
# shellcheck source=scripts/lib/gateway.sh
source "$(dirname "$0")/lib/gateway.sh"

UV="${AGENTBOX_DEMO_UV:-uv}"
N=0
STEP="初始化"
step() { N=$((N + 1)); STEP="$1"; printf '\n━━ [%d] %s\n' "$N" "$1"; }
ok() { printf '   ✓ %s\n' "$*"; }
info() { printf '     %s\n' "$*"; }

on_exit() {
  local rc=$?
  gw_cleanup
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%d] %s（退出码 %d）。工作目录：%s\n' "$N" "$STEP" "$rc" "$GW_WORK" >&2
    [ -n "${GW_SERVER_LOG:-}" ] && [ -f "$GW_SERVER_LOG" ] && tail -n 15 "$GW_SERVER_LOG" >&2
  else
    printf '\n演示完成：%d 步全部通过（工作目录 %s）。\n' "$N" "$GW_WORK"
  fi
}
trap on_exit EXIT

# 用户以会话 cookie 调用（每个用户一个 cookie 文件）；key 调用带 Bearer。
user_api() { # user_api USER METHOD PATH [curl 参数…]
  local u=$1 m=$2 p=$3
  shift 3
  curl -sS -X "$m" -b "$GW_WORK/$u.cookies" -c "$GW_WORK/$u.cookies" -H "Origin: $GW_ADDR" -H 'Content-Type: application/json' "$GW_ADDR$p" "$@"
}
chat() { # chat KEY MODEL [max_tokens]：一个 OpenAI 格式的非流式请求，打印 HTTP 状态
  curl -sS -o "$GW_WORK/last.json" -D "$GW_WORK/last.h" -w '%{http_code}' "$GW_ADDR/v1/chat/completions" \
    -H "Authorization: Bearer $1" -H 'Content-Type: application/json' \
    -d "{\"model\":\"$2\",\"max_tokens\":${3:-32},\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}]}"
}
header() { tr -d '\r' <"$GW_WORK/last.h" | grep -i "^$1:" | head -n1; }

# ---------------------------------------------------------------------------

step "启动 PostgreSQL/Redis、两个 fake upstream 与 agentbox server"
for c in curl python3; do command -v "$c" >/dev/null || gw_fail "需要 $c"; done
command -v "$UV" >/dev/null 2>&1 || [ -x "$UV" ] || gw_fail "需要 uv（或 AGENTBOX_DEMO_UV 指向 uv 可执行文件）"
rm -rf "$GW_WORK"
mkdir -p "$GW_WORK"
gw_detect_docker
gw_compose up -d --wait postgres redis >"$GW_WORK/compose.log" 2>&1 || { cat "$GW_WORK/compose.log" >&2; gw_fail "docker compose up 失败"; }
ok "PostgreSQL 127.0.0.1:5432、Redis 127.0.0.1:6379（deploy/docker-compose.yml）"
gw_build
gw_fresh_db agentbox_demo_gateway
gw_new_data_dir
gw_start_fake OA -tool-calls
gw_start_fake AN -tool-calls
ok "fake upstream：OpenAI 格式 $FAKE_OA_URL，Anthropic 格式 $FAKE_AN_URL（-tool-calls：带工具的请求回复 tool call）"
cat >"$GW_WORK/model-fallback.json" <<EOF
{"providers": [
  {"name": "claude", "protocol": "anthropic", "base_url": "$FAKE_AN_URL", "anthropic_version": "2023-06-01", "weight": 1,
   "models": {"m-an": "fake-model", "m-both": "fake-model"},
   "price": {"in": 3000000, "out": 15000000}}],
 "routing": {"mode": "weighted", "primary_weight": 3, "primary_models": {"m-oa": "fake-model", "m-both": "fake-model"}}}
EOF
info "降级链文件：claude（Anthropic 格式，权重 1）提供 m-an 与 m-both；主供应商（OpenAI 格式，权重 3）提供 m-oa 与 m-both"
gw_base_flags "$FAKE_OA_V1" "$FAKE_OA_HOSTPORT,$FAKE_AN_HOSTPORT" "$GW_WORK/model-fallback.json" m-oa,m-an,m-both
gw_start_server --redis-addr 127.0.0.1:6379
ok "agentbox server pid $GW_SERVER_PID：$GW_ADDR（--model-api），指标 http://$GW_METRICS/metrics"

step "租户：两个用户、org acme、邀请与接受、平台 API key"
for u in alice bob; do
  code=$(user_api "$u" POST /auth/register -o /dev/null -w '%{http_code}' -d "{\"username\":\"$u\",\"password\":\"Demo-Pass1\"}")
  [ "$code" = 201 ] || gw_fail "注册 $u：$code"
done
ok "alice、bob 注册（各自先在自己的个人 org 中）"
gw_api POST /v1/orgs -d '{"name":"acme","owner":"alice"}' -o /dev/null -w '' || true
[ "$(gw_sql "SELECT count(*) FROM orgs WHERE name = 'acme'")" = 1 ] || gw_fail "建 org acme"
ok "运维建 org acme，alice 为 owner"
inv=$(user_api alice POST /v1/orgs/invites -d '{"username":"bob","role":"member"}' | gw_json "d['id']")
ok "alice 邀请 bob（邀请 $inv）"
[ "$(user_api bob GET /v1/orgs/invites | gw_json "d['invites'][0]['org']")" = acme ] || gw_fail "bob 看不到邀请"
user_api bob POST "/v1/orgs/invites/$inv/accept" -o /dev/null
[ "$(user_api bob GET /v1/me | gw_json "d['org']['name']")" = acme ] || gw_fail "bob 没有加入 acme"
ok "bob 接受邀请，成为 acme 的 member（用户只有同意才会迁入另一个 org）"
k=$(user_api alice POST /v1/keys -d '{"name":"alice-sdk","scopes":["models"]}')
ALICE_KEY=$(printf '%s' "$k" | gw_json "d['key']")
k=$(user_api bob POST /v1/keys -d '{"name":"bob-limited","scopes":["models"]}')
BOB_KEY=$(printf '%s' "$k" | gw_json "d['key']")
BOB_KEY_ID=$(printf '%s' "$k" | gw_json "d['id']")
k=$(user_api bob POST /v1/keys -d '{"name":"bob-budget","scopes":["models"]}')
BOB_KEY2=$(printf '%s' "$k" | gw_json "d['key']")
BOB_KEY2_ID=$(printf '%s' "$k" | gw_json "d['id']")
ok "key：alice-sdk、bob-limited（$BOB_KEY_ID）、bob-budget（$BOB_KEY2_ID）；完整 key 只在创建时返回一次（这里不打印）"
info "GET /v1/me（以 alice 的 key）：$(curl -sS -H "Authorization: Bearer $ALICE_KEY" "$GW_ADDR/v1/me")"

step "官方 openai 与 anthropic SDK：同格式原样转发与双向转换，流式与工具调用"
sdk_env=(AGENTBOX_DEMO_BASE="$GW_ADDR" AGENTBOX_DEMO_KEY="$ALICE_KEY" PYTHONIOENCODING=utf-8)
project="$GW_REPO/tests/sdk"
script="$GW_REPO/scripts/demo-gateway-sdk.py"
if [[ "$UV" == *.exe ]]; then # WSL2 中的 Windows uv：路径转为 Windows 路径，环境变量经 WSLENV 传入
  project=$(wslpath -w "$project")
  script=$(wslpath -w "$script")
  export WSLENV="AGENTBOX_DEMO_BASE:AGENTBOX_DEMO_KEY:PYTHONIOENCODING${UV_CACHE_DIR:+:UV_CACHE_DIR}${WSLENV:+:$WSLENV}"
fi
env "${sdk_env[@]}" "$UV" run --quiet --locked --project "$project" python "$script"
ok "16 个 SDK 调用全部成功（2 个 SDK × 2 种供应商格式 × 非流式、流式、工具调用与工具结果）"
gw_sql "SELECT client_protocol || ' → ' || provider_protocol || CASE WHEN converted THEN '（转换）' ELSE '（原样转发）' END || ': ' || count(*) || ' 个请求，流式 ' || count(*) FILTER (WHERE stream) FROM model_requests GROUP BY client_protocol, provider_protocol, converted ORDER BY 1" | sed 's/^/     /'
for i in 1 2 3 4 5 6 7 8; do [ "$(chat "$ALICE_KEY" m-both)" = 200 ] || gw_fail "m-both 请求 $i"; done
info "加权路由：m-both 的 8 个请求 → $(gw_sql "SELECT string_agg(provider || '×' || n, '，') FROM (SELECT provider, count(*) n FROM model_requests WHERE model = 'm-both' GROUP BY 1 ORDER BY 1) x")（权重 3:1，平滑加权轮询）"

step "限流：owner 给 bob 的 key 设 rpm 3，第 4 个请求 429；然后月度预算用尽"
user_api alice PUT "/v1/limits/key/$BOB_KEY_ID" -d '{"rpm":3}' -o /dev/null
for i in 1 2 3 4; do
  code=$(chat "$BOB_KEY" m-oa)
  info "请求 $i：$code  $(header x-ratelimit-remaining-requests)"
done
[ "$code" = 429 ] || gw_fail "第 4 个请求应为 429，得到 $code"
ok "429 $(gw_json "d['error']['code']" <"$GW_WORK/last.json")：$(header retry-after)；$(header x-ratelimit-limit-requests)；$(header x-ratelimit-reset-requests)"
user_api alice PUT "/v1/budgets/key/$BOB_KEY2_ID" -d '{"limit_micro":6000}' -o /dev/null
info "owner 给 bob-budget 设本月预算 6000 µUSD（0.006 USD）；每个请求预留最坏费用（输入估算 × 最高输入单价 + max_tokens × 最高输出单价）"
n=0
while :; do
  n=$((n + 1))
  code=$(chat "$BOB_KEY2" m-an 100)
  [ "$code" = 200 ] || break
  [ "$n" -lt 60 ] || gw_fail "60 个请求后预算仍未用尽"
done
[ "$code" = 429 ] || gw_fail "预算用尽应为 429，得到 $code：$(cat "$GW_WORK/last.json")"
ok "第 $n 个请求：429 $(gw_json "d['error']['code'] + '：' + d['error']['message']" <"$GW_WORK/last.json")"
info "key 预算：$(user_api alice GET "/v1/budgets/key/$BOB_KEY2_ID" | gw_json "'limit %s, spent %s, reserved %s, unknown %s, available %s µUSD' % (d['limit_micro'], d['spent_micro'], d['reserved_micro'], d['unknown_micro'], d['available_micro'])")"
code=$(curl -sS -o "$GW_WORK/last.json" -w '%{http_code}' "$GW_ADDR/v1/messages" -H "x-api-key: $BOB_KEY2" -H 'anthropic-version: 2023-06-01' \
  -H 'Content-Type: application/json' -d '{"model":"m-an","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}')
ok "同一个 key 以 Anthropic 格式调用：$code $(cat "$GW_WORK/last.json")"

step "吊销 key → 401"
code=$(user_api bob DELETE "/v1/keys/$BOB_KEY_ID" -o /dev/null -w '%{http_code}')
ok "bob 吊销 bob-limited：HTTP $code（NOTIFY apikey_changed：每个 server 立即从缓存中逐出）"
code=$(chat "$BOB_KEY" m-oa)
[ "$code" = 401 ] || gw_fail "吊销后应为 401，得到 $code"
ok "下一个请求：401 $(cat "$GW_WORK/last.json")"

step "用量、账单与对账"
month=$(date -u +%Y-%m)
if gw_has_bill; then
  "$GW_BIN/agentbox" usage --org acme --group-by model,key --rollup | sed 's/^/     /'
  "$GW_BIN/agentbox" bill show "$month" --org acme | sed 's/^/     /'
  "$GW_BIN/agentbox" bill reconcile "$month" | sed 's/^/     /'
  ok "bill reconcile $month：usage_daily、model_requests、quota_holds 与 quota_budgets 一致"
else
  info "此版本没有 usage / bill 子命令（G5），改为直接汇总 model_requests："
  gw_sql "SELECT u.username || ' · ' || r.model || ': ' || count(*) || ' 个请求，' || sum(r.prompt_tokens) || '+' || sum(r.completion_tokens) || ' token，' || sum(r.cost_micro) || ' µUSD' FROM model_requests r JOIN users u ON u.id = r.user_id GROUP BY u.username, r.model ORDER BY 1" | sed 's/^/     /'
fi
gw_verify | sed 's/^/     /'
ok "verify-invariants：预算行与 hold 一致（G1）、未超限额（G2）、每个 model_requests 行恰有一个已结算的 hold（G3）"

step "Prometheus 指标（$GW_METRICS/metrics）"
curl -fsS "http://$GW_METRICS/metrics" >"$GW_WORK/metrics.txt"
grep -E '^agentbox_model_(requests_total|streams_total|rejections_total|ttft_seconds_(sum|count))' "$GW_WORK/metrics.txt" | sed 's/^/     /'
python3 - "$GW_WORK/metrics.txt" <<'EOF'
import re, sys
tot = ok = streams = complete = 0
ttft_sum = ttft_n = 0.0
rej = {}
for line in open(sys.argv[1], encoding="utf-8"):
    m = re.match(r'^(agentbox_model_\w+)\{([^}]*)\} (\S+)$', line) or re.match(r'^(agentbox_model_\w+)() (\S+)$', line)
    if not m:
        continue
    name, labels, v = m.group(1), dict(re.findall(r'(\w+)="([^"]*)"', m.group(2))), float(m.group(3))
    if name == "agentbox_model_requests_total":
        tot += v; ok += v if labels.get("status") == "200" else 0
    elif name == "agentbox_model_streams_total":
        streams += v; complete += v if labels.get("complete") == "true" else 0
    elif name == "agentbox_model_rejections_total":
        rej[labels.get("reason")] = rej.get(labels.get("reason"), 0) + v
    elif name == "agentbox_model_ttft_seconds_sum":
        ttft_sum += v
    elif name == "agentbox_model_ttft_seconds_count":
        ttft_n += v
print(f"   ✓ 成功率 {ok:.0f}/{tot:.0f}（到达路由的请求）；平均首字时间 {1000 * ttft_sum / max(ttft_n, 1):.1f} ms（{ttft_n:.0f} 个）；"
      f"流完整 {complete:.0f}/{streams:.0f}；拒绝 {', '.join(f'{k} {v:.0f}' for k, v in sorted(rej.items()))}")
EOF
info "Grafana 面板：deploy/observability（bash scripts/demo-observability.sh 启动观测栈）的 "Agentbox model gateway" 仪表盘（deploy/observability/grafana/dashboards/agentbox-model-gateway.json）"
