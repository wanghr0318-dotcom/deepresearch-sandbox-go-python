# 模型降级链的混沌测试——WSL2 + fake upstream（2026-10-10）

> **范围声明**：本记录在开发机的 **WSL2** 上测得，供应商是**进程内 fake upstream**（127.0.0.1，正常回复前固定延迟
> 50 ms），不是真实模型服务，费用为零。它说明的是"Gateway 在供应商故障时额外花了多少时间"，不是对真实供应商
> 可用性或真实延迟的结论。数字是端到端的调用延迟：`Coordinator.Invoke` 从发出到返回，含 Tx1、预留、上游 HTTP 请求、
> 结算与结果 blob 保存（journal 为内存实现）。

- 设计：[2026-10-10-model-fallback-design.md](../design/2026-10-10-model-fallback-design.md)。
- 命令：`AGENTBOX_CHAOS_RUNS=5 go test -count=1 -run TestChaos -v ./internal/gateway/call`（WSL2，普通用户）。退出码 0。
  实现见 `internal/gateway/call/chaos_test.go`：生产用的 OpenAI 兼容 chat adapter 经 HTTP 访问
  `tests/e2e/fakeupstream`（`SetAlways` 注入持续故障，`SetLatency` 注入慢回复）；Coordinator 用生产默认限额
  （退避基数 2 s 翻倍并抖动、每调用 3 次 try、模型调用期限 300 s），只有"挂起"一组把期限调成 3 s 以便测量。
- 环境：Windows 11 主机上的 WSL2（内核 6.6.87.2-microsoft-standard-WSL2），linux/amd64，24 个逻辑 CPU，Go 1.27.1。
- 每个场景都是新的逻辑调用（新的 call ID），不是 journal 重放。
- 对冲一组把熔断阈值设得很高：触发对冲后落败的慢主供应商按设计计为一次熔断失败，默认阈值 3 次后主供应商会被
  熔断、之后的调用直接走后备供应商（那是熔断而不是对冲的效果）；这里只测对冲本身。修复轮 1 之后复测，数字不变
  （中位数 251.89 ms，5 次）。

## 结果

| 场景 | 结果 | 次数 | 中位数 | 最大 |
|---|---|---|---|---|
| 健康的单供应商（基线） | 200 | 5 | 50.91 ms | 52.02 ms |
| 主供应商持续 503，**无降级链**（之前） | 429 tries_exhausted | 5 | 3.69 s | 5.06 s |
| 主供应商持续 503，有降级链，熔断器关闭 | 200（后备供应商） | 3 | 51.42 ms | 52.24 ms |
| 主供应商持续 503，有降级链，熔断器打开 | 200（后备供应商，主供应商不再被请求） | 5 | 50.79 ms | 50.98 ms |
| 主供应商端口拒绝连接，有降级链 | 200（后备供应商） | 5 | 51.02 ms | 51.93 ms |
| 主供应商挂起，**无降级链**，期限 3 s（之前；生产期限 300 s） | 504 call_deadline_exceeded | 1 | 3.00 s | 3.00 s |
| 主供应商挂起，有降级链，`--model-try-timeout 1s` | 200（后备供应商） | 5 | 1.051 s | 1.052 s |
| 全部供应商 503，熔断器关闭（第一个调用） | 429 tries_exhausted | 1 | 3.87 s | 3.87 s |
| 全部供应商 503，熔断器打开（**降级模式**） | 503 model_degraded | 5 | 40 µs | 100 µs |
| 主供应商慢（每次 1 s），不对冲 | 200 | 5 | 1.001 s | 1.002 s |
| 主供应商慢（每次 1 s），`--model-hedge-delay 200ms` | 200（后备供应商胜出） | 5 | 252.14 ms | 253.21 ms |

另一次只跑降级场景（20 次）：中位数 30 µs，最大 110 µs。

## 读法

- **故障转移几乎不加延迟**：503 与拒绝连接都在几毫秒内返回，转到后备供应商是立即的（不退避），所以有降级链时
  延迟与健康供应商相同（约 51 ms）。没有降级链时同一故障要用完 3 次 try 与两次退避（1–2 s、2–4 s，带抖动），
  3.7–5.1 s 后才以 429 失败。
- **熔断器打开后不再请求故障供应商**：测试断言打开期间主供应商的 fake upstream 请求数不变；try 行记录
  `skipped = primary:circuit_open`。
- **挂起需要每 try 超时**：不设 `--model-try-timeout` 时，挂起的供应商占满整个调用期限（生产 300 s）；设 1 s 时
  1.05 s 完成（1 s 超时 + 50 ms 后备回复）。超时的 try 已发出，按 unknown 结算（估算计入 unknown）。
- **降级模式是快速失败**：所有供应商都熔断后，Gateway 不预留、不发请求，约 40 µs 返回 `503 model_degraded`；第一个
  遇到全部故障的调用仍要花几秒把熔断器打开。
- **对冲以费用换尾延迟**：5 次对冲调用实际花费合计 160 µUSD，被取消的主供应商腿（已发出）按估算保守计入 unknown
  合计 685 µUSD——估算按 `max_tokens` 计，远高于实际用量，这正是默认关闭对冲的原因。

## 没有证明什么

- 不是真实供应商：真实模型的延迟分布、限流行为与部分失败（例如流式中断）不在这里。
- journal 是内存实现；PostgreSQL 上的同一语义（每 try 的 provider/skipped/hedge 列、对冲预留规则、崩溃后两条
  在途腿的账本转换）由 `internal/persistence/postgres/route_test.go` 在真实数据库上验证，崩溃中途换供应商后的重放由
  `TestRouteCrashMidFallbackReplay` 验证，二者都不计时。
- 熔断状态只在进程内：重启后所有供应商从关闭开始。

## 原始输出

```
| scenario | result | runs | median | max |
|---|---|---|---|---|
| healthy single provider (baseline) | 200 | 5 | 50.91ms | 52.02ms |
| primary 503, no fallback (before) | 429 tries_exhausted | 5 | 3.69373s | 5.05772s |
| primary 503, fallback, breaker closed | 200 | 3 | 51.42ms | 52.24ms |
| primary 503, fallback, breaker open | 200 | 5 | 50.79ms | 50.98ms |
--- PASS: TestChaosPrimaryDown (21.10s)
| primary connection refused, fallback | 200 | 5 | 51.02ms | 51.93ms |
--- PASS: TestChaosPrimaryUnreachable (0.27s)
| primary hangs, no fallback, deadline 3 s (before) | 504 call_deadline_exceeded | 1 | 3.00218s | 3.00218s |
| primary hangs, fallback, try timeout 1 s | 200 | 5 | 1.05127s | 1.052s |
--- PASS: TestChaosPrimaryHangs (6.27s)
| all providers 503, breakers closed (first call) | 429 tries_exhausted | 1 | 3.87299s | 3.87299s |
| all providers 503, breakers open (degraded mode) | 503 model_degraded | 5 | 40µs | 100µs |
--- PASS: TestChaosAllDown (5.86s)
| slow primary (1 s), no hedging | 200 | 5 | 1.00071s | 1.00213s |
| slow primary (1 s), hedge delay 200 ms | 200 | 5 | 252.14ms | 253.21ms |
hedged calls: spent 160 µUSD, charged as unknown (cancelled primary legs) 685 µUSD
--- PASS: TestChaosHedge (6.28s)
ok  	github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call	39.778s
```
