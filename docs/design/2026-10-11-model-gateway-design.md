# Multi-tenant model gateway: API keys, limits, quotas, OpenAI ⇄ Anthropic

Status: approved design, 2026-10-11. Implementation is split into tracks A, B, C (§11).

## 1. Goal

Today the Gateway is internal: only sandboxed workers reach it, over one Unix socket per attempt, identified by the attempt, with a per-task budget, one operator-owned provider key, OpenAI-compatible upstreams only and no streaming.

This design turns the platform into something many users can use at once, each with their own credentials and limits:

- **Platform API keys** (`abx_live_…`). The platform keeps the provider keys; every user's usage, limits and budgets are tracked per key.
- **Tenancy org → user → key.** Limits and monthly budgets can be set at each level, and a request must pass all of them.
- **Two surfaces behind the same key, auth, limits and metering:**
  - the existing runtime API (sessions, tasks, sandboxes);
  - a model API: OpenAI `/v1/chat/completions` and Anthropic `/v1/messages`, with streaming, any client format to any provider format, weighted routing, fallback and breakers.
- **Usage, bills and reconciliation** per org, user, key and model.

### Non-goals

- Images, audio, embeddings.
- Payment.
- SSO / OAuth.
- Multiple regions.
- A separate gateway process (the boundaries below allow one later).
- Bring-your-own provider keys.
- More than one org per user (the schema allows it later; the API enforces one).

## 2. Constraints that do not change

- PostgreSQL is the single source of truth. Redis stays a disposable accelerator: losing it never loses state or lets anyone overspend.
- A model call never runs without a money reservation. When PostgreSQL is unavailable, the model API fails closed.
- Provider keys never enter sandboxes, responses, logs or errors. Platform key secrets are never stored or logged.
- Default configuration behaves exactly as today:
  - without `--model-api`, no `/v1/chat/completions` or `/v1/messages` is served;
  - cookie login and the operator token keep working;
  - sandboxes keep their Unix-socket Gateway.
- Existing invariants keep holding; new ones are added (§9).

## 3. Architecture

```
client ──Authorization: Bearer abx_live_… | x-api-key | session cookie | operator token──►
  ┌─ auth middleware → tenant.Principal{org, user, key, scopes, role}
  ├─ ratelimit  (requests/min, tokens/min per key/user/org; concurrent streams)
  ├─ quota      (monthly money budget per key/user/org, all levels in one transaction)
  │
  ├─ /v1/sessions, /v1/tasks …    → existing runtime → sandbox → internal Gateway ─┐
  └─ /v1/chat/completions,                                                         │
     /v1/messages, /v1/models     → modelapi ──────────────────────────────────────┤
                                                                                   ▼
          llmproto (neutral format, codecs) → router (weights + breakers + fallback) → providers
          (openai | anthropic protocol each)    model_requests / calls → usage_daily → bills
```

New packages:

| Package | Responsibility | Depends on |
|---|---|---|
| `internal/tenant` | Orgs, members, roles; `Principal` in the request context | — |
| `internal/apikey` | Key format, HMAC verification, scopes, expiry, revocation, cache with NOTIFY invalidation | `tenant` |
| `internal/ratelimit` | Token buckets: Redis Lua script with in-process fallback; concurrency counters | — |
| `internal/quota` | Monthly budgets per level; holds reserved against all levels at once; settle, release, sweep | `tenant` |
| `internal/llmproto` | Neutral message format; OpenAI and Anthropic codecs (request, response, stream) | stdlib only |
| `internal/modelapi` | HTTP handlers for the model API; ties auth, limits, quota, routing, streaming, logging together | all of the above, `gateway/upstream`, `gateway/call` routing |
| `internal/usage` | Daily rollup, bills, reconciliation | `tenant` |

Persistence follows the existing pattern: each package declares the small `Store` interface it needs, and `internal/persistence/postgres` implements it (`tenancy.go`, `quota.go`, `modelrequests.go`, `usage.go`).

## 4. Data model

Migrations are allocated per track to avoid collisions:

| Migration | Track | Contents |
|---|---|---|
| `0012_tenancy.sql` | A | `orgs`, `org_members`, `api_keys`, `limits`, owner columns on tasks and sessions |
| `0013_quota.sql` | A | `quota_budgets`, `quota_holds` |
| `0014_model_requests.sql` | C | `model_requests`, `model_request_tries` |
| `0015_usage.sql` | C | `usage_daily` |

The tables:

- **`orgs`:** `id bigint identity`, `name text unique`, `disabled bool`, `created_at`.
- **`org_members`:** `org_id`, `user_id unique` (one org per user for now), `role in ('owner','member')`.
- **`api_keys`:**
  - columns: `id text primary key` (`k_` + 16 base32 chars), `org_id`, `user_id`, `name`, `secret_hmac bytea`, `scopes text[]` (subset of `runtime`, `models`), `created_at`, `expires_at null`, `revoked_at null`, `last_used_at null`;
  - `last_used_at` is updated at most once a minute per key.
- **`limits`:**
  - columns: `scope_kind in ('org','user','key')`, `scope_id text`, `rpm int null`, `tpm int null`, `max_sandboxes int null`, `max_streams int null`;
  - primary key `(scope_kind, scope_id)`;
  - NULL means no limit at that level.
- **`quota_budgets`:**
  - columns: `scope_kind`, `scope_id`, `period date` (first day of the UTC month), `limit_micro bigint null` (NULL = unlimited), `reserved_micro`, `spent_micro`, `unknown_micro`;
  - primary key `(scope_kind, scope_id, period)`;
  - rows are created lazily from the configured monthly limit.
- **`quota_holds`:**
  - columns: `hold_id text primary key`, `org_id`, `user_id`, `key_id null`, `period`, `source in ('task_call','model_request')`, `ref text`, `amount_micro`, `state in ('held','settled','released','charged_unknown')`, `charged_micro`, `deadline_at`, `created_at`;
  - one hold reserves the same amount at every level that exists for its principal.
- **Columns added to `tasks` and `sessions`:** `org_id bigint`, `key_id text null`. The migration creates a personal org `user-<id>` for every existing user (that user is its owner) and fills the columns. Tasks created by the operator token get the reserved org `operator`.
- **`model_requests`** (range-partitioned by month on `created_at`):
  - identity and attribution: `id text`, `org_id`, `user_id`, `key_id`;
  - request: `client_protocol in ('openai','anthropic')`, `model`, `stream bool`;
  - routing: `provider`, `provider_protocol`, `converted bool`;
  - outcome: `status int`, `error_code`;
  - usage and timing: `prompt_tokens`, `completion_tokens`, `cache_read_tokens`, `cache_write_tokens`, `cost_micro`, `ttfb_ms`, `latency_ms`;
  - `stream_complete bool null`, `hold_id`, `created_at`.
- **`model_request_tries`:** `request_id`, `try_no`, `provider`, `outcome`, `status`, `latency_ms`, `cost_micro`, `skipped_reason`.
- **`usage_daily`:** `day`, `org_id`, `user_id`, `key_id`, `model`, `source in ('runtime','model_api')`, `requests`, `prompt_tokens`, `completion_tokens`, `cost_micro`, `unknown_micro`; primary key over the dimension columns.

## 5. Authentication and authorization

- **Key format:** `abx_live_<id>_<secret>`, where `<secret>` is 32 random bytes in base32.
  - Stored: `HMAC-SHA256(pepper, secret)`. The pepper is a server secret file `<data>/apikey.pepper`, created with 0600 permissions on first start.
  - The full key is returned once, at creation.
- **Accepted credentials:**
  - `Authorization: Bearer abx_live_…` (OpenAI style);
  - `x-api-key: abx_live_…` (Anthropic style);
  - the session cookie (web UI);
  - the operator token (unchanged).
  - A Bearer value that is not an `abx_` key is checked as the operator token, as today.
- **Lookup:**
  - by `id`, with a constant-time comparison of the HMAC;
  - verified keys are cached in memory for 30 s;
  - revocation and expiry changes send `NOTIFY apikey_changed, '<id>'`, which every server uses to evict the key at once.
- **Principal:** `{OrgID, UserID, KeyID (empty for cookie), Scopes, Role, Operator}` in the request context.
- **Scopes:** `runtime` (sessions, tasks and their events) and `models` (model API). An API key never manages keys, limits, budgets or members; those need the cookie (member/owner) or the operator token. A leaked key therefore cannot mint new keys or raise its own limits.
- **Error responses:**
  - invalid, revoked or expired key → `401 invalid_api_key`;
  - missing scope → `403 permission_denied`.

  Errors are returned in the caller's format (OpenAI error object or Anthropic error object, §7.5).

## 6. Rate limits and quotas

### 6.1 Rate limits (`internal/ratelimit`)

- **Request buckets:** one token bucket per level (key, user, org) for requests per minute. A single Lua script checks and takes from all levels atomically, and takes nothing if any level is short.
- **Token buckets:** a request takes its estimated tokens up front (prompt estimate + `max_tokens`). After completion the unused part is refunded.
- **Redis client:** the existing stdlib RESP2 client in `internal/gateway/cache` gains `EVALSHA`/`EVAL` (or a shared minimal client moves to `internal/resp`). Keys look like `rl:{scope}:{id}:rpm` and expire after twice the window.
- **Fallback:** if Redis errors or times out (50 ms), the limiter switches to an in-process bucket per server and raises `agentbox_ratelimit_degraded`. Limits stay enforced per server instead of globally. This is acceptable because money is protected by quotas, not by rate limits.
- **Concurrency:** concurrent model streams per key/user/org use an in-process counter, released when the stream ends. Concurrent sandboxes per org are counted in PostgreSQL at admission (live environments whose task or session belongs to the org).
- **Rejection:** `429 rate_limit_exceeded` with `Retry-After`, `x-ratelimit-limit-requests`, `x-ratelimit-remaining-requests`, `x-ratelimit-reset-requests` and the same three for tokens.

### 6.2 Quotas (`internal/quota`)

- **`Reserve(principal, source, ref, amount, deadline)`:** in one PostgreSQL transaction, locks the budget rows for this month at every level in a fixed order (org, user, key, to avoid deadlocks). For each level it checks that `spent + reserved + unknown + amount ≤ limit`, then raises `reserved` everywhere and inserts the hold. If any level is short it returns `ErrInsufficientQuota{Level}` and changes nothing.
- **`Settle(hold, charged, unknown bool)`:** moves the amount from `reserved` to `spent`, or to `unknown` when the outcome is unknown, at every level, and marks the hold.
- **`Release(hold)`:** returns the reservation when no try was sent.
- **Sweep:** a periodic job turns holds past `deadline_at` into `charged_unknown`, charging the full amount — the same conservative rule the task ledger already uses.
- **Runtime calls:** every model, search or fetch call made by a sandbox places one `quota_holds` row next to its existing per-task reservation. Both must succeed. Settlement follows the call's existing settlement. The per-task budget keeps working unchanged.
- **Rejection:** `429 insufficient_quota` (OpenAI convention; Anthropic callers get a `rate_limit_error`-shaped object with the same message) naming the level that ran out.
- **Known limit:** at very high request rates the org row becomes a hot lock. The load test measures the lock wait (§10). Sharded counters and pre-allocated budget slices are recorded as future work.

## 7. Model API (`internal/modelapi`, `internal/llmproto`)

### 7.1 Endpoints

- `POST /v1/chat/completions` (OpenAI format).
- `POST /v1/messages` (Anthropic format; the `anthropic-version` header is accepted and echoed).
- `GET /v1/models`: the logical models the key may use, in OpenAI list format; with `anthropic-version` present, in Anthropic format.

All are enabled by `--model-api` and require the `models` scope.

### 7.2 Neutral format

`llmproto.Request` contains:
- a system prompt;
- messages, each with a role and typed blocks: `text`, `tool_call{id,name,arguments_json}`, `tool_result{call_id,content,is_error}`, `thinking{text,signature}`;
- tools (`name`, `description`, `parameters`);
- `tool_choice` (`auto|none|required|{name}`);
- `max_tokens`, `temperature`, `top_p`, `stop`, `stream`;
- extras that do not translate (§7.4).

`llmproto.Response` carries the same blocks, a stop reason (`end|max_tokens|tool_use|stop_sequence|error`) and usage (input, output, cache read, cache write).

The stream is a sequence of `llmproto.Event`:
- `message_start`;
- `block_start{index, type, tool id/name}`;
- `block_delta{index, text | arguments fragment | thinking}`;
- `block_stop`;
- `message_delta{stop reason, usage}`;
- `message_stop`;
- `error`.

The OpenAI and Anthropic codecs each implement: decode request, encode request, decode response, encode response, decode stream chunk, encode stream event.

### 7.3 Four paths

| Client → provider | Mode |
|---|---|
| OpenAI → OpenAI | **Pass-through**: the validated body is forwarded with only `model` rewritten; the stream is forwarded chunk by chunk; usage is read from the final chunk (`stream_options.include_usage` is added) |
| Anthropic → Anthropic | **Pass-through**: the body is forwarded with `model` rewritten; usage is read from `message_start` and `message_delta` |
| OpenAI → Anthropic | Decoded to the neutral format, encoded as Anthropic; the response or stream is converted back to OpenAI chunks |
| Anthropic → OpenAI | Decoded to the neutral format, encoded as OpenAI; the response or stream is converted back to Anthropic events (`message_start`, per-block `content_block_start` / `content_block_delta` (`text_delta`, `input_json_delta`) / `content_block_stop`, `message_delta`, `message_stop`) |

Field mapping:

| Anthropic | OpenAI |
|---|---|
| `system` | first `system` message |
| `text` block | `content` |
| `tool_use` block | `tool_calls[]` |
| `tool_result` block | `role: tool` message |
| `input_schema` | `parameters` |
| `tool_choice` `auto` / `any` / `{name}` | `auto` / `required` / `{function:{name}}` |
| `stop_sequences` | `stop` |
| `stop_reason` `end_turn` / `max_tokens` / `tool_use` / `stop_sequence` | `finish_reason` `stop` / `length` / `tool_calls` / `stop` |

Consecutive OpenAI tool messages become one Anthropic user turn with several `tool_result` blocks, and the reverse.

### 7.4 Features only one format has

| Feature | When converting |
|---|---|
| `cache_control`, `metadata.user_id` | Dropped; the drop is counted in `model_requests` and metrics |
| `thinking` blocks / `reasoning_effort`, images, `n > 1`, `logprobs`, `response_format` json_schema (to Anthropic) | `400 unsupported_field`, naming the field |

A request that uses a feature only one format has is routed only to providers of that format. If no such provider serves the model, the result is `400 unsupported_field`.

### 7.5 Errors

Errors are written in the client's format. OpenAI:

```
{"error":{"message","type","code"}}
```

Anthropic:

```
{"type":"error","error":{"type","message"}}
```

| Situation | Status | OpenAI `code` | Anthropic `type` |
|---|---|---|---|
| bad key | 401 | `invalid_api_key` | `authentication_error` |
| missing scope | 403 | `permission_denied` | `permission_error` |
| bad request or unsupported field | 400 | `invalid_request_error` / `unsupported_field` | `invalid_request_error` |
| rate limit | 429 | `rate_limit_exceeded` | `rate_limit_error` |
| budget exhausted | 429 | `insufficient_quota` | `rate_limit_error` |
| all providers open | 503 | `model_degraded` | `overloaded_error` |
| provider failed | 502 | `upstream_error` | `api_error` |

### 7.6 Providers and routing

The existing `--model-fallback-file` gains optional fields (old files stay valid):

```json
{"providers": [
  {"name": "claude", "protocol": "anthropic", "base_url": "https://api.anthropic.com", "key_env": "AGENTBOX_MODEL_CLAUDE_API_KEY",
   "anthropic_version": "2023-06-01", "weight": 1,
   "models": {"smart": "claude-model-id"},
   "price": {"in": 3000000, "out": 15000000, "cache_read": 300000, "cache_write": 3750000}},
  {"name": "kimi", "protocol": "openai", "base_url": "https://api.moonshot.cn/v1", "key_env": "AGENTBOX_MODEL_API_KEY",
   "weight": 3, "models": {"smart": "kimi-k3"}, "price": {"in": 2000000, "out": 8000000}}
],
 "routing": {"mode": "weighted"}}
```

- **Fields:** `protocol` defaults to `openai`. Keys are sent as `Authorization: Bearer` (openai) or `x-api-key` plus `anthropic-version` (anthropic).
- **`weighted` mode (default when any `weight` is given):** smooth weighted round-robin (the nginx algorithm) over providers serving the model whose breaker is closed. The rest are the fallback order, by weight.
- **`latency` mode:** effective weight = weight / EWMA(time to first token), floored so a slow provider still gets probe traffic.
- **`ordered` mode (default with no weights):** today's behaviour, unchanged.
- **Failover:** a retryable failure before the first byte has been written to the client fails over to the next provider. After the first byte there is no failover: two streams cannot be spliced, so the client gets an error event and `stream_complete=false`.
- **Breakers, per-try timeouts and hedging** keep their current semantics. Hedging applies only to non-stream requests.
- **Sandbox workers** keep sending OpenAI-format calls. With an Anthropic provider in the chain, the Gateway converts (Anthropic → OpenAI path above), so the research agent can use Anthropic-format models without worker changes. Runtime calls keep their journal and replay semantics; streaming stays off for them.

### 7.7 Request flow

1. Authenticate (§5).
2. Decode the request in the client format.
3. Rate limit: requests/min and estimated tokens/min at all levels; acquire a stream slot if `stream`.
4. Resolve the logical model to candidate providers (format-only features filter them).
5. Estimate the worst-case cost: input estimate × the highest input price among the candidates + `max_tokens` × the highest output price. Reserve a quota hold for it.
6. Pick a provider (§7.6). Encode or pass through. Send. On a retryable failure before the first byte, try the next provider (each try recorded).
7. Stream or return the response in the client format; measure time to first token.
8. Settle the hold with the actual cost: unknown → conservative estimate, `charged_unknown`. Refund unused tokens to the tokens/min bucket. Release the stream slot. Write `model_requests` and its tries. Update metrics.

### 7.8 Failure handling

| Failure | Behaviour |
|---|---|
| Redis unavailable | In-process limiter on each server; metric and warning; quotas unaffected |
| PostgreSQL unavailable | `503`; no model call without a hold. Cached keys do not bypass this |
| Client disconnects mid-stream | Cancel the upstream; settle the tokens seen, or the estimate as unknown if usage never arrived |
| Provider fails mid-stream | Error event to the client in its format; `stream_complete=false`; settle what was seen |
| Server crash between reserve and settle | The sweep turns expired holds into `charged_unknown` |
| Key revoked during a stream | The stream completes (bounded by `--model-api-max-duration`, default 10 min); the next request gets `401` |

## 8. Management API, CLI, web

| Endpoint | Who |
|---|---|
| `GET /v1/me` (principal, org, role, scopes) | any authenticated caller |
| `POST /v1/keys` `{name, scopes, expires_at?}` → key shown once; `GET /v1/keys`; `DELETE /v1/keys/{id}` (revoke) | cookie: members manage their own, owners the org's; operator: all |
| `GET/PUT /v1/limits/{kind}/{id}`, `GET/PUT /v1/budgets/{kind}/{id}` | owners for their org and its users and keys (cannot exceed the org's own limit); operator for orgs |
| `GET /v1/usage?from&to&group_by=day,model,key,user`, `GET /v1/bills/{yyyy-mm}` | members: their own; owners: the org's; operator: all |
| `POST /v1/orgs/members` `{username, role}`, `DELETE /v1/orgs/members/{user}` | owners |
| `POST /v1/orgs` | operator |

**CLI:** `agentbox org create|list|add-member`, `agentbox key create|list|revoke`, `agentbox limits set`, `agentbox budget set`, `agentbox usage`, `agentbox bill show|reconcile <yyyy-mm>`.

**Web (Vue), three pages:**
- **API Keys:** create with scope checkboxes; a dialog that shows the key once with copy; list with last used; revoke with confirmation.
- **Usage:** daily cost and tokens by model and key; budget progress per level.
- **Members:** owners only.

**Bills and reconciliation:**
- A rollup job aggregates settled `model_requests` and runtime holds into `usage_daily`.
- A bill is the month's `usage_daily` for an org, with per-model token totals and cost.
- `bill reconcile` recomputes the month from the raw `model_requests` and `quota_holds` and compares it with `usage_daily` and with `quota_budgets.spent_micro + unknown_micro`. Any difference is reported per org, user, key and model, and the command exits non-zero.

## 9. Invariants added to `verify-invariants`

1. For every budget row:
   - `reserved_micro` = the sum of `held` holds at that level and period;
   - `spent_micro + unknown_micro` = the sum of `charged_micro` of settled and `charged_unknown` holds.
2. For every level with a limit: `spent + reserved + unknown ≤ limit`. A hold may overshoot only through `charged_unknown` settlement above its estimate, and the checker reports that separately.
3. Every `model_requests` row is linked to exactly one hold. Every completed request's hold is not `held`.
4. Every API key's `org_id` matches its user's org.
5. Every task and session with a `key_id` belongs to that key's org.

## 10. Testing and evidence

- **Unit:**
  - key generation, parsing and HMAC check;
  - scope checks;
  - the Lua script against real Redis (as in CI) and the in-process fallback;
  - smooth weighted round-robin distribution over 10 000 picks;
  - codec test fixtures for all four paths, with and without streaming, including tool arguments split across chunks, several tool calls, `tool_result` grouping and stop-reason mapping.
- **Concurrency:** 200 parallel reservations against one org budget with random settle, release and unknown outcomes. At every point and at the end, invariants 1 and 2 hold. Run with `-race`.
- **Compatibility:** the official `openai` and `anthropic` Python SDKs, unmodified, against the server with `fakeupstream` in OpenAI mode and in Anthropic mode (new). Each covers non-stream, stream and tool calls — 2 SDKs × 2 provider formats.
- **Fault injection:**
  - Redis stopped during a run (degraded limiter, no errors to clients);
  - server killed mid-stream (the hold becomes `charged_unknown`; reconcile passes);
  - provider fails after the first byte (`stream_complete=false`);
  - key revoked during a stream.
- **Load test** (`cmd/agentbox-loadtest`, Go, against `fakeupstream` with set latency), each reported with P50/P99 and the hardware:
  - gateway overhead (fake latency 0);
  - maximum requests per second;
  - time-to-first-token overhead;
  - rate-limiter accuracy (allowed / configured over 60 s);
  - overspend (must be 0);
  - the Redis-down run;
  - lock wait on the org budget row.
- **Demo** `scripts/demo-gateway.sh` (zero model cost):
  1. create an org, users and keys;
  2. call with both SDKs, including OpenAI client → Anthropic provider and the reverse;
  3. show `429` with rate-limit headers, then `insufficient_quota`;
  4. revoke a key and get `401`;
  5. show the bill and run reconcile;
  6. Grafana panels: success rate, time to first token, stream completeness, rejections by reason.
- **Evidence** goes to `docs/evidence/2026-10-11-model-gateway.md`.
- **Real providers:** no Anthropic key is available, so the Anthropic provider path is verified against `fakeupstream` only. If Moonshot's Anthropic-compatible endpoint accepts the existing key, one cheap real call per direction is recorded.

## 11. Delivery

| Track | PR | Content |
|---|---|---|
| A | G1 | `tenant`, `apikey`, auth middleware, `/v1/me`, keys, members and orgs endpoints, CLI, migration `0012` with backfill, invariants 4–5 |
| A | G2 | `ratelimit` (Lua + fallback, concurrency), `quota` (holds, settle, sweep), runtime calls place holds, limits and budgets endpoints and CLI, migration `0013`, invariants 1–2 |
| B | B1 | `llmproto` neutral format and both codecs (request, response, stream) with fixtures; `fakeupstream` Anthropic mode and streaming in both formats |
| B | B2 | Provider `protocol`, Anthropic upstream adapter, streaming `DoStream` in `gateway/upstream`, weighted and latency routing, fallback-file fields |
| C | G3 | `modelapi`: `/v1/chat/completions`, `/v1/messages`, `/v1/models`, pass-through and converted paths, `model_requests` (`0014`), metrics, invariant 3, SDK compatibility tests |
| C | G5 | `usage` rollup (`0015`), bills, reconcile, usage endpoints and CLI, web pages |
| C | G6 | Load test, fault-injection runs, evidence, demo script, README (zh/en), `docs/usage.zh-CN.md`, CHANGELOG |

Ordering:
- A (G1 → G2) and B (B1 → B2) run in parallel.
- G3 needs G2 and B2.
- G5 needs G3.
- G6 comes last.

### Interfaces between tracks

Fixed here so the tracks can work in parallel:

```go
// internal/tenant
type Principal struct {
    OrgID, UserID int64
    KeyID         string // empty for cookie and operator callers
    Scopes        Scopes // Runtime, Models
    Role          Role   // Owner, Member
    Operator      bool
}
func WithPrincipal(ctx context.Context, p Principal) context.Context
func FromContext(ctx context.Context) (Principal, bool)

// internal/quota
type Scope struct{ OrgID, UserID int64; KeyID string }
type Holder interface {
    Reserve(ctx context.Context, s Scope, source Source, ref string, amountMicro int64, deadline time.Time) (HoldID, error) // *InsufficientError{Level}
    Settle(ctx context.Context, id HoldID, chargedMicro int64, unknown bool) error
    Release(ctx context.Context, id HoldID) error
}

// internal/ratelimit
type Bucket struct{ Scope, Kind string; Limit int64; Window time.Duration }
type Decision struct{ Allowed bool; RetryAfter time.Duration; Limit, Remaining int64; Reset time.Duration; Degraded bool }
type Limiter interface {
    Take(ctx context.Context, buckets []Bucket, n int64) (Decision, error) // all-or-nothing
    Refund(ctx context.Context, buckets []Bucket, n int64)
}

// internal/llmproto: Request, Response, Event, Usage, StopReason; codecs
func DecodeOpenAIRequest([]byte) (Request, error); func EncodeOpenAIRequest(Request) ([]byte, error)
func DecodeAnthropicRequest([]byte) (Request, error); func EncodeAnthropicRequest(Request) ([]byte, error)
// plus Decode/Encode for responses and NewOpenAIStreamDecoder / NewAnthropicStreamEncoder etc.

// internal/gateway/upstream: streaming chat on a route
type Stream interface {
    Next() (llmproto.Event, error) // io.EOF at the end
    Usage() (llmproto.Usage, bool)
    Close() error
}
// ChatAdapter gains DoStream(ctx, route int, body []byte, proto llmproto.Protocol) (Stream, *Error)
// and RouteProtocol(route int) llmproto.Protocol.
```
