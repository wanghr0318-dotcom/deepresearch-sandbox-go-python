#!/usr/bin/env bash
# 模型网关故障注入（docs/design/2026-10-11-model-gateway-design.md §7.8、§10）：在本机以 fake upstream 为供应商
# （不访问外网，没有模型费用）、带 --model-api 的 agentbox server 运行四个故障场景，逐项检查并打印 PASS/FAIL：
#
#   1. kill -9：流进行中 server 被 SIGKILL，重启后 hold 在期限（--model-api-max-duration + 5 min）之后由 sweep 转为
#      charged_unknown；verify-invariants 通过（有 bill 子命令时 bill reconcile 也通过）。约 6 分钟（等待期限）。
#   2. 首字节之后供应商失败：客户端收到其格式的错误事件，model_requests.stream_complete = false。四条路径。
#   3. 流进行中吊销 key：这个流正常结束，下一个请求 401 invalid_api_key。
#   4. 熔断：主供应商连续 503 → 换后备供应商；熔断打开期间跳过主供应商（circuit_open）；打开时长过后半开试探
#      成功，回到主供应商。
#
#   bash scripts/fault-gateway.sh            # 全部场景
#   bash scripts/fault-gateway.sh 2 3 4      # 指定场景
#
# 不需要 root。环境变量见 scripts/lib/gateway.sh。结果写入工作目录的 faults.md。
set -euo pipefail

GW_NAME=fault
# shellcheck source=scripts/lib/gateway.sh
source "$(dirname "$0")/lib/gateway.sh"

SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -gt 0 ] || SCENARIOS=(1 2 3 4)
REPORT="$GW_WORK/faults.md"
FAILS=0

pass() { printf 'PASS %s\n' "$*" | tee -a "$REPORT"; }
check_fail() { printf 'FAIL %s\n' "$*" | tee -a "$REPORT"; FAILS=$((FAILS + 1)); }
check() { # check DESCRIPTION 命令…
  local what=$1
  shift
  if "$@" >/dev/null 2>&1; then pass "$what"; else check_fail "$what"; fi
}
section() { printf '\n## %s\n\n' "$*" | tee -a "$REPORT"; }
note() { printf '%s\n' "$*" | tee -a "$REPORT"; }

on_exit() {
  local rc=$?
  gw_cleanup
  if [ "$rc" -ne 0 ] || [ "$FAILS" -ne 0 ]; then
    printf '\n故障注入：%d 项检查失败（退出码 %d）。工作目录：%s\n' "$FAILS" "$rc" "$GW_WORK" >&2
    [ "$rc" -ne 0 ] || exit 1
  else
    printf '\n故障注入：全部检查通过。报告：%s\n' "$REPORT"
  fi
}
trap on_exit EXIT

rm -rf "$GW_WORK"
mkdir -p "$GW_WORK"
echo "# 模型网关故障注入 $(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$REPORT"
gw_detect_docker
gw_build
gw_fresh_db agentbox_fault
gw_new_data_dir

# restart_fakes [OA 参数…] -- [AN 参数…]：停止上一个场景的 fake upstream 并启动新的一对（OpenAI 与 Anthropic 格式）。
restart_fakes() {
  local oa=() an=()
  while [ $# -gt 0 ] && [ "$1" != -- ]; do oa+=("$1"); shift; done
  [ $# -gt 0 ] && shift
  an=("$@")
  gw_cleanup
  gw_start_fake OA "${oa[@]}"
  gw_start_fake AN "${an[@]}"
}

# start_split_server [server 参数…]：m-oa 由 OpenAI 格式的主供应商提供，m-an 由 Anthropic 格式的后备供应商提供。
start_split_server() {
  cat >"$GW_WORK/fallback-split.json" <<EOF
{"providers": [{"name": "claude", "protocol": "anthropic", "base_url": "$FAKE_AN_URL", "models": {"m-an": "fake-model"},
  "price": {"in": 1000000, "out": 2000000}}],
 "routing": {"primary_models": {"m-oa": "fake-model"}}}
EOF
  gw_base_flags "$FAKE_OA_V1" "$FAKE_OA_HOSTPORT,$FAKE_AN_HOSTPORT" "$GW_WORK/fallback-split.json" m-oa,m-an
  gw_start_server "$@"
}

KEY=""
KEY_ID=""
USER_NAME=fault-user
ensure_user() {
  gw_api GET /v1/me >/dev/null
  if [ -z "$(gw_sql "SELECT 1 FROM users WHERE username = '$USER_NAME'")" ]; then
    curl -fsS -o /dev/null -H 'Content-Type: application/json' -H "Origin: $GW_ADDR" -d "{\"username\":\"$USER_NAME\",\"password\":\"Fault-Pass1\"}" "$GW_ADDR/auth/register"
  fi
}
new_key() { # new_key NAME：为 fault-user 建一个 models scope 的 key，设置 KEY 与 KEY_ID（不打印 key）
  local out
  out=$(gw_api POST /v1/keys -d "{\"name\":\"$1\",\"scopes\":[\"models\"],\"username\":\"$USER_NAME\"}")
  KEY=$(printf '%s' "$out" | gw_json "d['key']")
  KEY_ID=$(printf '%s' "$out" | gw_json "d['id']")
}

# stream_req OUT PROTO MODEL：后台发出一个流式请求，正文写入 OUT、响应头写入 OUT.h；设置 REQ_PID。
stream_req() {
  local out=$1 proto=$2 model=$3
  if [ "$proto" = openai ]; then
    curl -sN -D "$out.h" -o "$out" "$GW_ADDR/v1/chat/completions" -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
      -d "{\"model\":\"$model\",\"stream\":true,\"max_tokens\":32,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" &
  else
    curl -sN -D "$out.h" -o "$out" "$GW_ADDR/v1/messages" -H "x-api-key: $KEY" -H 'anthropic-version: 2023-06-01' -H 'Content-Type: application/json' \
      -d "{\"model\":\"$model\",\"stream\":true,\"max_tokens\":32,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" &
  fi
  REQ_PID=$!
}
req_id() { tr -d '\r' <"$1.h" | sed -n 's/^x-request-id: //Ip' | head -n1; }
has_content() { grep -q 'fake 回复' "$1"; }

# ---------------------------------------------------------------------------

scenario_1() {
  section "1. server 在流进行中被 kill -9，重启后 sweep 把 hold 转为 charged_unknown"
  restart_fakes -chunk-delay chat=1s --
  start_split_server --model-api-max-duration 10s
  ensure_user
  new_key fault-kill
  stream_req "$GW_WORK/f1.out" openai m-oa
  gw_wait_for "第一个内容事件" 30 has_content "$GW_WORK/f1.out"
  local rid
  rid=$(req_id "$GW_WORK/f1.out")
  note "请求 $rid 已收到第一个内容事件；kill -9 server（pid $GW_SERVER_PID）"
  gw_kill9_server
  wait "$REQ_PID" 2>/dev/null || true
  check "客户端的流没有正常结束（没有 [DONE]）" bash -c "! grep -q '\[DONE\]' '$GW_WORK/f1.out'"
  check "崩溃后 hold 仍是 held" test "$(gw_sql "SELECT state FROM quota_holds WHERE ref = '$rid'")" = held
  check "崩溃后没有 model_requests 行" test "$(gw_sql "SELECT count(*) FROM model_requests WHERE id = '$rid'")" = 0
  note "hold：$(gw_sql "SELECT 'amount_micro=' || amount_micro || ' deadline_at=' || to_char(deadline_at AT TIME ZONE 'UTC', 'HH24:MI:SS') || 'Z（现在 ' || to_char(now() AT TIME ZONE 'UTC', 'HH24:MI:SS') || 'Z）' FROM quota_holds WHERE ref = '$rid'")"
  gw_start_server --model-api-max-duration 10s
  note "server 已重启（pid $GW_SERVER_PID）；等待 hold 期限过去与下一次 sweep（每 30 s）"
  local t0=$SECONDS state=""
  while [ $((SECONDS - t0)) -lt 480 ]; do
    state=$(gw_sql "SELECT state FROM quota_holds WHERE ref = '$rid'")
    [ "$state" = charged_unknown ] && break
    sleep 5
  done
  check "hold 转为 charged_unknown（重启后 $((SECONDS - t0)) s）" test "$state" = charged_unknown
  note "hold：$(gw_sql "SELECT 'state=' || state || ' amount_micro=' || amount_micro || ' charged_micro=' || charged_micro FROM quota_holds WHERE ref = '$rid'")"
  note "org 预算行：$(gw_sql "SELECT 'reserved=' || reserved_micro || ' spent=' || spent_micro || ' unknown=' || unknown_micro FROM quota_budgets b JOIN org_members m ON b.scope_kind = 'org' AND b.scope_id = m.org_id::text JOIN users u ON u.id = m.user_id WHERE u.username = '$USER_NAME'")"
  check "verify-invariants 通过" gw_verify
  if gw_has_bill; then
    check "bill reconcile $(date -u +%Y-%m) 通过" "$GW_BIN/agentbox" bill reconcile "$(date -u +%Y-%m)"
  else
    note "bill reconcile：此版本没有 bill 子命令（G5），跳过"
  fi
  gw_stop_server
}

scenario_2() {
  section "2. 供应商在首字节之后失败：客户端收到其格式的错误事件，stream_complete = false"
  # OpenAI 格式：第 1 个流 3 个事件（含内容）后发错误事件，第 2 个 3 个事件后断开；Anthropic 格式：4 个事件（message_start、
  # content_block_start、ping 与一个 text_delta）后第 1 个断开、第 2 个发错误事件。内容之前的失败会换供应商或回 502，不是本场景。
  restart_fakes -fail-after chat:1:3:error,chat:2:3 -- -fail-after messages:1:4,messages:2:4:error
  start_split_server
  ensure_user
  new_key fault-midstream
  local i=0 c
  for c in "openai m-oa 原样转发，供应商的错误事件" "anthropic m-oa 转换，供应商断开" "anthropic m-an 原样转发，供应商断开" "openai m-an 转换，供应商的错误事件"; do
    i=$((i + 1))
    set -- $c
    local out="$GW_WORK/f2-$i.out" rid row
    stream_req "$out" "$1" "$2"
    wait "$REQ_PID" || true
    rid=$(req_id "$out")
    if [ "$1" = openai ]; then
      check "路径 $i（$1 客户端 → $2，${c#* * }）：OpenAI 格式的错误对象 data: {\"error\":…}，没有 [DONE]" \
        bash -c "grep -q '^data: {\"error\"' '$out' && ! grep -q '\[DONE\]' '$out'"
    else
      check "路径 $i（$1 客户端 → $2，${c#* * }）：Anthropic 格式的 event: error，没有 message_stop" \
        bash -c "grep -q '^event: error' '$out' && ! grep -q 'message_stop' '$out'"
    fi
    row=$(gw_sql "SELECT status || ' ' || coalesce(error_code, '-') || ' stream_complete=' || coalesce(stream_complete::text, 'null') || ' provider=' || coalesce(provider, '-') || ' converted=' || converted FROM model_requests WHERE id = '$rid'")
    check "路径 $i：model_requests $rid → $row" bash -c "[[ '$row' == *'stream_complete=false'* ]]"
    note "    错误事件：$(grep -m1 -A1 -E '^(event: error|data: \{"error")' "$out" | tr '\n' ' ' | cut -c1-200)"
  done
  check "verify-invariants 通过" gw_verify
  gw_stop_server
}

scenario_3() {
  section "3. 流进行中吊销 key：这个流正常结束，下一个请求 401"
  restart_fakes -chunk-delay chat=1s --
  start_split_server
  ensure_user
  new_key fault-revoke
  local out="$GW_WORK/f3.out" t0 code
  stream_req "$out" openai m-oa
  gw_wait_for "第一个内容事件" 30 has_content "$out"
  t0=$SECONDS
  code=$(gw_api DELETE "/v1/keys/$KEY_ID" -o /dev/null -w '%{http_code}')
  check "吊销 key $KEY_ID：HTTP $code" test "${code:0:1}" = 2
  wait "$REQ_PID" || true
  check "流在吊销之后正常结束（[DONE]，吊销后又持续 $((SECONDS - t0)) s）" grep -q '\[DONE\]' "$out"
  local rid
  rid=$(req_id "$out")
  check "model_requests：200，stream_complete = true" test "$(gw_sql "SELECT status || ' ' || stream_complete FROM model_requests WHERE id = '$rid'")" = "200 true"
  curl -sS -o "$GW_WORK/f3-next.out" -w '%{http_code}' "$GW_ADDR/v1/chat/completions" -H "Authorization: Bearer $KEY" \
    -H 'Content-Type: application/json' -d '{"model":"m-oa","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}' >"$GW_WORK/f3-next.code"
  check "下一个请求 401 invalid_api_key：$(cat "$GW_WORK/f3-next.code") $(cat "$GW_WORK/f3-next.out")" \
    bash -c "[ \"\$(cat '$GW_WORK/f3-next.code')\" = 401 ] && grep -q invalid_api_key '$GW_WORK/f3-next.out'"
  check "verify-invariants 通过" gw_verify
  gw_stop_server
}

scenario_4() {
  section "4. 熔断：主供应商 503 → 后备供应商；熔断打开时跳过；打开时长过后半开试探成功"
  restart_fakes -inject chat:1:503,chat:2:503,chat:3:503 --
  cat >"$GW_WORK/fallback-breaker.json" <<EOF
{"providers": [{"name": "claude", "protocol": "anthropic", "base_url": "$FAKE_AN_URL", "price": {"in": 1000000, "out": 2000000}}]}
EOF
  gw_base_flags "$FAKE_OA_V1" "$FAKE_OA_HOSTPORT,$FAKE_AN_HOSTPORT" "$GW_WORK/fallback-breaker.json" m-1
  gw_start_server --model-breaker-failures 3 --model-breaker-open 5s
  ensure_user
  new_key fault-breaker
  local i code ids=()
  one() {
    local out="$GW_WORK/f4-$1.out"
    code=$(curl -sS -D "$out.h" -o "$out" -w '%{http_code}' "$GW_ADDR/v1/chat/completions" -H "Authorization: Bearer $KEY" \
      -H 'Content-Type: application/json' -d '{"model":"m-1","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}')
    ids+=("$(req_id "$out")")
    [ "$code" = 200 ] || check_fail "请求 $1：$code"
  }
  for i in 1 2 3 4 5 6; do one "$i"; done
  note "（等待 6 s：熔断打开时长 5 s）"
  sleep 6
  for i in 7 8; do one "$i"; done
  note ""
  note "| 请求 | 状态 | 供应商 | tries（供应商/结局/状态/跳过原因） |"
  note "|---|---|---|---|"
  local id n=0 row tries
  local -a provs trs
  for id in "${ids[@]}"; do
    n=$((n + 1))
    row=$(gw_sql "SELECT status || '|' || coalesce(provider, '-') FROM model_requests WHERE id = '$id'")
    tries=$(gw_sql "SELECT string_agg(provider || '/' || outcome || '/' || status || coalesce(nullif('/' || skipped_reason, '/'), ''), ', ' ORDER BY try_no) FROM model_request_tries WHERE request_id = '$id'")
    provs+=("${row#*|}")
    trs+=("$tries")
    note "| $n | ${row%%|*} | ${row#*|} | $tries |"
  done
  note ""
  check "请求 1–3：主供应商 503 后由后备供应商 claude 回答" test "${provs[0]}${provs[1]}${provs[2]}" = claudeclaudeclaude
  check "请求 4–6：熔断打开，仍由 claude 回答" test "${provs[3]}${provs[4]}${provs[5]}" = claudeclaudeclaude
  check "请求 1–3 各有一次主供应商的 503 try" bash -c "for t in '${trs[0]}' '${trs[1]}' '${trs[2]}'; do [[ \$t == *'/retryable/503'* ]] || exit 1; done"
  check "请求 4–6 没有发往主供应商的 try（熔断打开）" bash -c "for t in '${trs[3]}' '${trs[4]}' '${trs[5]}'; do [[ \$t != *'/503'* ]] || exit 1; done"
  check "请求 7–8：打开时长过后回到主供应商（不是 claude）" bash -c "[ -n '${provs[6]}' ] && [ '${provs[6]}' != claude ] && [ '${provs[7]}' != claude ]"
  note ""
  note '```'
  curl -fsS "http://$GW_METRICS/metrics" | grep -E '^agentbox_(breaker_transitions_total|model_requests_total)' | tee -a "$REPORT" || true
  note '```'
  check "verify-invariants 通过" gw_verify
  gw_stop_server
}

for s in "${SCENARIOS[@]}"; do
  case "$s" in
    1 | 2 | 3 | 4) "scenario_$s" ;;
    *) gw_fail "未知场景 $s（1–4）" ;;
  esac
done
