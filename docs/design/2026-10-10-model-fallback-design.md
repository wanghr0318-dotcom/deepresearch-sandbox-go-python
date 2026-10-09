# Model provider fallback, circuit breakers, hedging and degraded mode — design

Status: implemented on branch `s4-model-fallback` (2026-10-10). Scope: the Gateway's model endpoint
(`POST /v1/chat/completions`). Search and fetch are unchanged.

## 1. Problem

Before this change a logical model (`kimi-k3`, `deepseek-chat`, …) was served by exactly one OpenAI-compatible
upstream. When that upstream was down the Gateway retried the same upstream with exponential backoff
(2 s, 4 s, … up to `MaxTries` = 3 and the 300 s call deadline). A dead provider therefore cost each model call
tens of seconds to minutes before the Worker saw an error, and a provider outage was a product outage.

## 2. Goals

1. **Provider chain per logical model.** An ordered list of upstreams (base URL, key env var, model-name
   mapping, price). On a retryable failure (connect error, per-try timeout, 429, 5xx, invalid/empty
   response) the next try goes to the next provider *immediately* (no backoff between providers).
2. **Journal semantics unchanged.** Every provider attempt is one try (one reservation, one `call_tries` row).
   Replay of a completed call returns the journaled blob without contacting any provider. The fingerprint is
   that of the *logical* call (logical model), so it is stable no matter which provider served it. Budget
   reservation and settlement use the price of the provider that actually ran the try. `unknown` outcomes
   keep their meaning and accounting (estimate charged to the unknown bucket).
3. **Circuit breaker per provider** (closed / open / half-open with one probe), generalised from the Redis
   cache breaker, so a dead provider is skipped without spending a try on it.
4. **Optional hedged requests** (off by default).
5. **Degraded mode.** When every provider that serves the model is open, fail fast with
   `503 model_degraded` instead of waiting for timeouts.
6. **Visibility.** `inspect` shows, per try, which provider ran it, whether it was a hedge, and which
   providers were skipped and why. Log attributes are stable names that observability (S1) can turn into
   metrics/spans.
7. **Default configuration behaves exactly as before**: with no fallback providers configured there is no
   breaker, no routing, no hedging, and the try rows carry an empty provider.

## 3. Non-goals

- Failing over on `fatal` outcomes (400/401/403/404 …). A request the primary rejects as invalid would be
  rejected by any provider; an auth failure is a configuration bug that should be fixed, not hidden.
- Streaming. The Gateway's model endpoint is non-streaming.
- Cross-process breaker state. Breakers are in-memory per Gateway process; after a restart every provider
  starts closed (the first failing call re-learns the outage in at most `failures` tries).
- Health-check pings. The half-open probe is a real request; there is no synthetic traffic.
- Search/fetch fallback (the same mechanism would apply, but those adapters have one provider each today).

## 4. Architecture

```
Worker ──► edge ──► call.Coordinator ──────────────► upstream.chatAdapter
                    │  Tx1 BeginCall (fingerprint      │  Routes(): ["primary","backup",…]
                    │   = logical model)               │  EstimateOn(i), DoOn(i), PricingOn(i)
                    │  try loop:                       │  route i rewrites "model" to the provider's
                    │   pick route ◄── breakers[i]     │  name and uses its base URL + key
                    │   Tx2 ReserveTry(provider,       │
                    │        skipped, est_i)           │
                    │   DoOn(i) (per-try timeout)      │
                    │   [hedge: after delay, Tx2 for   │
                    │    route j, DoOn(j); first ok    │
                    │    wins, loser cancelled]        │
                    │   SettleTry (cost at price_i)    │
                    │   retryable/unknown → next route │
                    │   now; all tried → backoff       │
                    │   all open → 503 model_degraded  │
```

### 4.1 Where things live

| Concern | Package | Why |
|---|---|---|
| Provider protocol, model mapping, per-provider price, empty-response check | `internal/gateway/upstream` (`ChatRoute`, `Router` interface) | upstream owns protocols and keys; it does not account |
| Route choice, breakers, hedging, try accounting | `internal/gateway/call` (`routing.go`) | call is the only owner of tries, reservations and the journal |
| Breaker state machine | `internal/gateway/breaker` (new; `cache.Breaker` is now an alias) | one implementation for Redis and model providers |
| `provider`, `skipped`, `hedge` per try | migration `0011_call_try_route.sql`, `ReserveTry`, `LoadCall`, inspect | audit metadata next to outcome/latency/cost |
| Flags / provider file | `cmd/agentbox`, `internal/app` | assembly |

### 4.2 `upstream.Router`

```go
type Router interface {
    Routes() []string                                    // provider names, index 0 = primary
    Serves(route int, model string) bool                 // false → skipped as model_not_served
    EstimateOn(route int, resolved []byte) (int64, error) // reservation at this provider's price
    DoOn(ctx context.Context, route int, resolved []byte) (Response, *Error)
    PricingOn(route int, model string) Pricing           // settlement at this provider's price
}
```

The chat adapter always implements it; the Coordinator only routes when `len(Routes()) > 1`. Route 0 is the
existing configuration (`--model-base-url`, `AGENTBOX_MODEL_API_KEY`, `--model-price*`); its estimate and
cost are computed exactly as before. A fallback route serves a logical model only if its `models` map lists
it (an empty map means "serves every declared model under the same name"). `DoOn(i)` replaces the `model`
field of the resolved body with the provider's model name; the rest of the body is identical, so the logical
request — and its fingerprint — does not depend on the route.

In chain mode the adapter — **including the primary route** — also treats a 2xx response without a non-empty `choices` array as
`unknown / upstream_bad_response` (the provider may have billed it, so it is charged like any other unknown),
which makes "empty response" fail over. In single-provider mode this check is off so behaviour is unchanged.

### 4.3 The try loop with routes

For each iteration of the existing try loop (deadline, cancellation, pause, `MaxTries` all unchanged):

1. **Pick.** Walk routes in order. Skip a route if it does not serve the model (`model_not_served`), if it
   already failed in the current round (`tried`), or if its breaker refuses (`circuit_open`). The first route
   whose breaker allows is used. The skipped list (`backup:circuit_open,c:model_not_served`) is stored on the
   try row.
2. **Nothing allowed.** If some route was skipped as `tried`, the round is over: reset the round, back off
   (same formula as today, honouring `Retry-After`) and pick again. If every route was skipped for
   `circuit_open`/`model_not_served`, no provider can be asked: the call fails with
   `model_degraded` (503, retryable with `X-Agentbox-Retry`). No reservation is made, so it costs nothing and
   returns in microseconds.
3. **Reserve.** `ReserveTry` with the route's estimate, `provider`, `skipped`. Slot accounting uses a
   per-provider semaphore key (`openai_compat/<route>`).
4. **Do.** `DoOn(route)` under a per-try timeout (`--model-try-timeout`, default off). A timeout after the
   request was sent is `unknown` (as today); before it was sent it is `retryable`.
5. **Report to the breaker.** ok and request-rejecting fatal (e.g. 400) → success (the provider answered);
   retryable, unknown and provider-side fatal (401/403/404, egress blocked, invalid URL) → failure; cancellation by the Coordinator (task cancel, deadline, shutdown, hedge loser) → abort (no
   verdict, releases a half-open probe slot).
6. **Settle** exactly as before; cost uses `PricingOn(route, model)`.
7. **Next.** On retryable/unknown, mark the route `tried`. If another route is ready, the next try starts
   immediately; otherwise back off.

`MaxTries` still bounds the whole logical call, across providers. With two providers and the default 3 the
sequence is A → B → (backoff) → A. Chains longer than `MaxTries` (default 3) are never fully walked in one call; keep chains short.

### 4.4 Breakers

`internal/gateway/breaker.Breaker` is the former cache breaker with a configurable threshold and open
duration (zero values keep the cache defaults, 5 failures / 30 s) plus a non-mutating `Ready()` used to decide
whether to skip the backoff. Model routes use `--model-breaker-failures` (default 3) and
`--model-breaker-open` (default 30 s). State machine: closed —N consecutive failures→ open —open duration
elapsed, next Allow→ half-open (exactly one probe in flight) —probe ok→ closed / probe fails→ open.

Breakers exist only for routed adapters; a single provider never gets one (so a single dead provider still
behaves exactly as before rather than turning into fast `model_degraded` errors).

### 4.5 Hedged requests (`--model-hedge-delay`, off by default)

When enabled, if the primary leg has not finished after the delay and another route is ready, the Coordinator
reserves a **second, concurrent try** for that route (`ReserveTry{Hedge: true}`) and sends the same logical
request there. Decisive results are an `ok` from either leg or a `fatal` from the **first** leg; a `fatal`
from the hedge leg is not decisive (a wrong backup key, model name or base URL must not cancel a slow but
healthy primary) — the Coordinator keeps waiting for the first leg. On a decisive result the other leg is
cancelled; if it nevertheless answers `ok` before the cancellation takes effect, that `ok` is preferred over a
first-leg `fatal`.

Why this is safe for the journal:

- The model endpoint has no external side effects except cost; it is never cached or coalesced.
- Each leg is a normal try with its own reservation. Persistence allows a second held reservation only for an
  explicit hedge request for a *different provider* while exactly one try is held. The ReserveTry
  idempotency rule (lost COMMIT, E11b) now also matches on provider, so a hedge request is never mistaken for
  a retry of the primary's reservation and vice versa.
- **Only the final leg decides the call.** Every other leg is settled first with a *charge-only* (`Sibling`)
  settlement: its reservation, the ledger and its try row are settled normally (released if the request had
  not been sent, `unknown` with the full estimate if it had, the actual cost if it had already answered `ok`;
  an `ok` loser's blob is stored for provenance), `calls.cost_charged` and `possible_external_duplicate` are
  updated, but `calls.state`, `result_ref`, `upstream_request_id` and `fail_reason` are not touched. The final
  leg is then settled normally. So two `ok` legs never both write `result_ref`, and a `fatal` leg never marks
  the call failed when the other leg succeeds. A crash between the two settlements leaves the final leg's
  reservation held: the startup ledger conversion charges it as unknown and the call becomes `unknown` →
  retryable, never a stray `failed`. A try cancelled because the other leg won keeps its own error code and
  is marked `hedge_lost`.
- **A slow first leg counts as a failure.** If the leg that triggered the hedge loses after running longer
  than the hedge delay, its breaker records a failure (a cancelled hedge leg is only an abort), so a
  persistently hanging primary is eventually skipped instead of being hedged around forever.
- **Cost is accounted conservatively.** A loser that was already sent is charged its full estimate to the
  `unknown` bucket (the provider may bill tokens generated before the cancel arrived), and the call is marked
  `possible_external_duplicate`. Hedging therefore trades money for tail latency: in the worst case a hedged
  call costs up to two estimates. That is why it is off by default and why the delay should be set near the
  provider's p95 latency, not near zero.
- Hedging needs a free try (`MaxTries`), budget for the second estimate and an inflight slot; if any is
  missing the Coordinator simply waits for the primary leg.

### 4.6 Visibility

Per try (`call_tries`, `inspect` JSON `calls[].tries[]`, CLI `inspect` "MODEL CALL" table):

| field | meaning |
|---|---|
| `provider` | route name that ran the try (`primary`, `backup`, …); empty in single-provider mode |
| `skipped` | providers passed over before this try, `name:reason`, reasons `circuit_open`, `tried`, `model_not_served` |
| `hedge` | true for the second leg of a hedged pair |
| `hedge_lost` | true for a leg cancelled because the other leg of the pair produced the decisive result (its `error` keeps its own code) |

The serving provider of a completed call is the `provider` of its `ok` try. Log attributes on the existing
`gateway: try` line: `route`, `route_skipped`, `hedge`, `breaker`; new lines `gateway: breaker` (state
transitions: `route`, `from`, `to`) and `gateway: degraded` (`routes` with each route's state).
`Coordinator.RouteStates()` returns the current breaker state per route for a future metrics endpoint.

## 5. Failure handling summary

| Situation | Result |
|---|---|
| A connect error / 5xx / 429 | try settled retryable on A, next try on B immediately |
| A hangs | per-try timeout → unknown on A (estimate charged), next try on B |
| A returns 200 with no choices (chain mode) | unknown / upstream_bad_response, next try on B |
| A returns 400 | fatal, call failed (no failover) |
| A open, B closed | A skipped (`A:circuit_open`), try on B; no try spent on A |
| all open | `503 model_degraded` in < 1 ms, no reservation |
| crash after try on B was sent | startup conversion: try → unknown, call → unknown; next request with the same call id makes a new try (routing picks the healthy provider); later replays return the blob |
| replay of a completed call | journal blob, no provider contacted, regardless of breaker state |

## 6. Testing

- `breaker`: state machine with injected clock, configurable thresholds, `Ready()` is non-mutating.
- `upstream`: route model mapping, per-route key/base URL, estimate/pricing per route, empty-choices check
  only in chain mode, `Serves`.
- `call` (in-memory store, scripted fake routes): A down → B serves; A flapping → breaker opens, skips A,
  half-open probe closes it; all down → `model_degraded` fast; fingerprint stable across providers; replay
  after completion does not contact providers; crash mid-fallback (ledger conversion simulated) → retry on
  the healthy route → replay; hedging: winner/loser settlement order and accounting, hedge off by default;
  single-provider configuration unchanged (no provider recorded, backoff unchanged).
- `persistence/postgres`: migration, provider/skipped/hedge round trip, hedge reservation rules, idempotency
  match on provider, ledger conversion with two held tries; invariants hold.
- Chaos test with the real HTTP chat adapter against two `fakeupstream` servers measuring the latency of a
  model call with failover vs. without (evidence `docs/evidence/2026-10-10-model-fallback.md`).

## 7. How it is demonstrated

`go test ./internal/gateway/call -run 'TestChaos' -v` prints the measured latencies (fake upstream, zero cost):
primary down with failover vs. the same outage without a fallback provider, breaker-open fast path, and the
all-down degraded error. `agentbox inspect <task>` shows the provider and skip reasons per try.
