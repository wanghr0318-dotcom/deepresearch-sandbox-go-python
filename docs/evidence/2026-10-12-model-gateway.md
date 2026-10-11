# 模型网关：负载测试、故障注入、演示与一次真实供应商调用（2026-10-12）

> **范围声明**：除最后一节外，本记录全部在开发机的 **WSL2** 上测得，供应商是 **fake upstream**（127.0.0.1，回复前
> 延迟 0，不是真实模型服务），费用为零。客户端、server、fake upstream、PostgreSQL 与 Redis **在同一台机器上**，
> PostgreSQL 与 Redis 运行在 Docker Desktop 的容器中；CPU 同时被其他开发任务使用（开始时 1 分钟负载 6–8），
> 数字含噪声，关键场景各跑 3 次。数字说明的是"网关自身花了多少时间、在这台机器上能做多少、限额与预算在并发下
> 是否守住"，**不是**生产环境的容量结论。

- 设计：[2026-10-11-model-gateway-design.md](../design/2026-10-11-model-gateway-design.md) §10。分支 `g6-evidence`，基于
  main 4275b5c（G1、G2、B1、B2、G3 已合并；用量、账单与对账 G5 尚未合并，因此 `bill reconcile` 在本记录中被跳过）。
- 环境：Windows 11 主机上的 WSL2（内核 6.6.87.2-microsoft-standard-WSL2，镜像网络），x86_64，24 个逻辑 CPU（Intel
  Core Ultra 9 275HX），15 GiB；Go 1.27.1；PostgreSQL 16 与 Redis 7 在 Docker Desktop 中，经 127.0.0.1 访问。从 WSL2
  到 PostgreSQL 的一次 `SELECT 1` 往返 p50 0.21–0.23 ms、p99 0.42–0.91 ms。server 的连接池为 pgx 默认的 24 个连接。
- server 以普通用户运行：`agentbox server --model-api --exec-slots 0 --redis-addr <专用 Redis>`，账号开启，
  `--model-fallback-file` 让逻辑模型 `m-oa` 只由 OpenAI 格式的主供应商提供、`m-an` 只由 Anthropic 格式的后备供应商
  `claude` 提供（`routing.primary_models`），因此四条路径（客户端格式 > 供应商格式）可以分别测量。单价 1 / 2 USD 每百万
  输入 / 输出 token（只决定预留与结算的数额）。
- 工具：`cmd/agentbox-loadtest`（Go；每个场景自建用户、org 与 models scope 的 key；`--seed`；结果为 JSON 行），由
  `scripts/loadtest-gateway.sh` 启动 server、两个 fake upstream、一个专用 Redis 容器后运行。命令：

  ```bash
  AGENTBOX_LT_REPEAT=3 bash scripts/loadtest-gateway.sh latency ladder budget lockwait
  bash scripts/loadtest-gateway.sh ratelimit redis
  AGENTBOX_LT_PG_OPTIONS='-c synchronous_commit=off' bash scripts/loadtest-gateway.sh latency lockwait   # 只用于分解开销
  bash scripts/fault-gateway.sh
  bash scripts/demo-gateway.sh
  ```

  三条负载命令的退出码都是 0；每次结束时 `verify-invariants` 都通过（`不变量检查通过`）。

## 结果一览

| 测量 | 结果 | 说明 |
|---|---|---|
| 网关开销（并发 1，非流式，四条路径，3 次中位数） | P50 15.7–16.7 ms，P95 21.7–23.7 ms，P99 24.7–27.4 ms | 网关延迟减去直接请求 fake upstream 的延迟（0.2–0.3 ms） |
| 首字时间（流式，网关） | P50 6.4–6.6 ms，P99 11.1–14.4 ms（直接请求 0.3 ms / 0.6–0.7 ms） | 首个内容事件；预留在首字之前，结算在流结束之后 |
| 同上，`synchronous_commit=off` | 开销 P50 5.5–5.9 ms，首字 2.6–2.7 ms | 每个请求约 4 个事务；约三分之二的开销是 WAL 落盘（Docker Desktop 的虚拟磁盘） |
| 最大吞吐（16 个 org，非流式） | 1033–1082 RPS，并发 32 时；P99 42–60 ms | 并发 64、128 时吞吐不再增长（880–1027 RPS），延迟随并发线性增长；0 错误 |
| 限流精度（rpm 60 / 600 / 3000，60 s） | 放行 / 配置 = 1.000 / 1.000 / 1.001 | 先排空桶，再以 2 倍限额的速率开环发送 |
| 并发下的预算超支（并发 64，3 次） | **0 µUSD**；每 100 ms 的快照中 0 次超限，最高用量 / 限额 0.9998 | 含 15% 首字后被客户端放弃的流（`charged_unknown`） |
| 运行中停止 Redis（并发 8，45 s） | 20 749 个请求 **0 错误**；Redis 重启后约 4 s 恢复 | 修复连接池问题之前，恢复要等到池中失效连接逐个试完（见下文） |
| 同一 org 的预算行锁（并发 64，16 个 key） | 1 个 org：134–139 RPS，P99 1.18–1.27 s；16 个 org：1027–1058 RPS，P99 81–86 ms | 设计 §6.2 预见的热点行；锁等待的后端平均 21.8 个（共约 23 个活动连接） |
| 故障注入（4 个场景，26 项检查） | 全部 PASS | kill -9、首字节后失败、流中吊销、熔断与恢复 |
| 真实供应商（Moonshot，6 次调用） | 全部 200；四条路径各一次，加两个流 | 经 Windows 侧的本机转发；共约 260 个 token |

## 1. 网关开销与首字开销

方法：`agentbox-loadtest latency --concurrency 1`。每行先预热 1 s，再测 6 s；同一请求体（200 字节的用户消息，
`max_tokens` 32）另外直接发给 fake upstream（供应商格式），作为基线。开销 = 网关的分位数 − 直接请求的分位数。首字时间是
请求发出到第一个内容事件（OpenAI 的非空 `delta.content`、Anthropic 的 `content_block_delta`）。"db tx/req" 是测量期间
`pg_stat_database` 的事务数增量除以请求数。

3 次运行的中位数（ms；第 1 次运行开始时负载 7.7，P50 偏高 5–13 ms，见原始数据）：

| 路径 | 流式 | 开销 P50 | 开销 P95 | 开销 P99 | 网关首字 P50 / P99（直接约 0.3） | db tx/req |
|---|---|---|---|---|---|---|
| oa>oa 原样转发 | 否 | 16.66 | 23.69 | 26.46 | — | 4.0–4.5 |
| oa>oa 原样转发 | 是 | 15.91 | 23.50 | 25.36 | 6.54 / 11.70 | 3.6–4.3 |
| an>oa 转换 | 否 | 15.75 | 21.77 | 27.44 | — | 3.4–4.4 |
| an>oa 转换 | 是 | 16.63 | 27.67 | 35.17 | 6.63 / 14.40 | 4.1–4.5 |
| oa>an 转换 | 否 | 15.67 | 21.65 | 24.68 | — | 3.9–4.7 |
| oa>an 转换 | 是 | 15.71 | 21.39 | 24.50 | 6.56 / 11.07 | 4.1–4.4 |
| an>an 原样转发 | 否 | 15.72 | 22.04 | 26.44 | — | 3.4–4.1 |
| an>an 原样转发 | 是 | 15.62 | 20.45 | 24.94 | 6.42 / 11.20 | 4.0–4.8 |

`synchronous_commit=off`（1 次运行，只为分解开销，**不是推荐配置**：它允许崩溃时丢失最近提交的预算变化）：八行的开销
P50 5.52–5.87 ms、P99 7.6–9.9 ms，首字 P50 2.57–2.72 ms，事务数不变（4.0–4.3）。

读法：

- 转换（OpenAI ⇄ Anthropic）与原样转发的开销没有可分辨的差别；开销几乎全部在 PostgreSQL 上。每个请求约 4 个事务（读
  限额、预留 hold、结算、写 `model_requests` 与 tries），关掉同步提交后开销从约 16 ms 降到约 5.7 ms，所以约三分之二是
  这台机器上 WAL 落盘的时间（Docker Desktop 的虚拟磁盘），其余是数据库往返与网关自身。
- 首字之前只有读限额与预留（结算与记录在流结束之后），所以首字开销约是总开销的 40%。
- 在 WSL2 + Docker Desktop 上 fsync 偏慢；在本地 NVMe 上的原生 PostgreSQL 预计更低，但这里没有测。

## 2. 最大吞吐

方法：`agentbox-loadtest ladder`，oa>oa 非流式，16 个 org 各 1 个 key（避开第 5 节的热点行），闭环并发 1、2、4、…，
每级预热 1 s、测 8 s；错误率超过 1% 或吞吐连续两级增长不足 5% 时停止。

| 并发 | 第 1 次 RPS / P99 ms | 第 2 次 | 第 3 次 |
|---|---|---|---|
| 1 | 76.0 / 16.8 | 77.0 / 17.4 | 74.6 / 19.1 |
| 8 | 456.6 / 24.9 | 487.5 / 22.7 | 482.3 / 23.3 |
| 16 | 852.2 / 26.0 | 781.9 / 55.2 | 785.8 / 46.6 |
| **32** | **1062.2 / 42.7** | **1082.2 / 41.7** | **1033.4 / 60.5** |
| 64 | 881.9 / 141.6 | 1003.9 / 86.9 | 985.0 / 89.4 |
| 128 | 1027.5 / 150.4 | 977.7 / 183.6 | 1004.9 / 159.8 |

全部级别 0 错误。拐点在并发 32 附近：之后排队只增加延迟。瓶颈没有单独定位；24 个连接的连接池、每请求约 4 次提交
与同机的 24 个 CPU（同时运行 PostgreSQL、Redis、两个 fake upstream、负载工具与其他任务）都可能是原因。

## 3. 限流精度

方法：`agentbox-loadtest ratelimit --levels 60,600,3000 --duration 60s --factor 2`。每个级别一个 key，只在 key 层级设
`rpm`。先用 16 个并发请求排空令牌桶（直到第一个 429），再以 2 倍限额的速率开环发送 60 s；桶从空开始时期望放行
`rpm × 60 s / 60 s`。三个级别同时运行，限流器在 Redis（Lua 脚本）上。

| rpm | 发送速率 /s | 放行 | 期望 | 放行 / 期望 | 429 rate_limit_exceeded | 其他 |
|---|---|---|---|---|---|---|
| 60 | 2 | 60 | 60 | 1.000 | 60 | 0 |
| 600 | 20 | 600 | 600 | 1.000 | 600 | 0 |
| 3000 | 100 | 3002 | 3000 | 1.001 | 2995 | 3 |

此前一次相同的运行：60 / 600 / 3005（1.002）。第一个 429 的头：`retry-after: 1`、`x-ratelimit-limit-requests: 60`、
`x-ratelimit-remaining-requests: 0`、`x-ratelimit-reset-requests: 1m0s`。

"其他" 3 个是 `503 service_unavailable`：同一秒内三个请求的 `QuotaReserve` 超过 2 s 的语句期限
（`canceling statement due to statement timeout`）。这台 PostgreSQL 同时被其他开发任务的测试使用，原因没有确定。
它们按设计失败关闭——没有 hold 就不调用供应商，客户端得到其格式的 503——但这说明一次 2 s 的数据库停顿会直接变成
客户端错误。

## 4. 并发下的预算超支

方法：`agentbox-loadtest budget --concurrency 64 --users 4 --budget-usd 0.5 --key-budget-usd 0.1 --seed 1`。一个 org
（预算 500 000 µUSD）中 4 个用户各一个 key，第一个 key 另有 100 000 µUSD 的预算。按种子抽签：60% 非流式、25% 完整的
流、15% 发往慢供应商（事件间隔 20 ms）且客户端在首字后断开的流——这些在用量到达前被放弃，按预留全额记为
`charged_unknown`。请求在 OpenAI 与 Anthropic 格式之间交替。采样器每 100 ms 用一条语句（一个快照）读取该 org 与该
key 本月的预算行，检查 `spent + reserved + unknown ≤ limit`；运行期间脚本每 5 s 运行一次 `verify-invariants`；org 预算
耗尽 3 s 后停止，所有请求结束后再读一次。

| 运行 | 请求（200 / 放弃 / 429 insufficient_quota） | 快照 | 快照中超限 | 最高 用量/限额 | 结束时 org：spent + unknown | 超支 | 运行中 verify-invariants |
|---|---|---|---|---|---|---|---|
| 1 | 1560 / 279 / 6263 | 160 | 0 | 0.9998 | 149 688 + 350 145 = 499 833 | 0 | 3 次通过 |
| 2 | 1569 / 278 / 6281 | 162 | 0 | 0.9998 | 150 550 + 348 890 = 499 440 | 0 | 3 次通过 |
| 3 | 1603 / 275 / 6121 | 164 | 0 | 0.9998 | 153 810 + 345 125 = 498 935 | 0 | 4 次通过 |

结束时 `reserved` 在每一级都是 0；key 级预算同样没有超出（结束时 98 846–99 729 / 100 000）。第一个
`insufficient_quota` 出现在 12.5–12.9 s。

## 5. 同一 org 的预算行锁

方法：`agentbox-loadtest lockwait --concurrency 64 --keys 16 --org-counts 1,4,16`。同样的闭环负载（oa>oa 非流式，每个 org
有一个很大的预算），16 个 key 分别放在 1、4、16 个 org 中。采样器每 20 ms 读 `pg_stat_activity` 中该库
`wait_event_type = 'Lock'` 的后端数。

| org 数 | RPS（3 次） | P50 ms | P99 ms | 平均锁等待后端 | 有锁等待的采样 |
|---|---|---|---|---|---|
| 1 | 134.0 / 139.0 / 135.9 | 352–374 | 1178–1272 | 21.7–21.8（最多 23） | 99.8–100% |
| 4 | 438.6 / 434.1 / 437.5 | 137–140 | 237–242 | 15.8–16.0 | 100% |
| 16 | 1027.1 / 1048.1 / 1058.0 | 59–61 | 81–86 | 2.3–2.5 | 74–78% |

`synchronous_commit=off`（1 次）：1 / 4 / 16 个 org 为 358 / 1089 / 1661 RPS。

读法：每个请求在预留与结算两个事务中锁 org 的预算行，一个 org 的吞吐上限约为 1 ÷（持锁时间 × 2），持锁时间里有一次
提交落盘，所以关掉同步提交后上限约提高 2.7 倍。这是设计 §6.2 记下的已知限制：一个 org 在这台机器上约 135 RPS。
分片计数器或预分配的预算片可以解除它，作为后续工作。全部 200：没有请求触发 1 s 的 `lock_timeout`（P99 约 1.3 s 是排队的总时间）；更高的并发可能触发它。

## 6. 运行中停止 Redis

方法：`agentbox-loadtest steady --concurrency 8 --duration 45s --hook '15s=docker stop …' --hook '30s=docker start …'`，8 个
org 各一个 key，每个 key 的 rpm 为 1 000 000（每个请求都经过 Redis 上的限流脚本），一半的工作者用流式；每秒抓取
`agentbox_ratelimit_degraded` 等指标。

- 20 749 个请求，**0 错误**；每秒吞吐与 P99 在停止前、降级中、恢复后没有可见的变化（P50 15.7–18.3 ms，P99 21–59 ms）。
- `degraded` 在第 15 s 变为 1（第一个失败的 EVALSHA），之后每 5 s 的冷却后试一次 Redis（`redis_failures` 1 → 4），
  在第 35 s 回到 0：Redis 在 30.6 s 重新启动，下一次试探成功。降级期间 9449 次决定由进程内令牌桶作出。

**发现并修复的问题。** 第一次运行（修复之前）中，Redis 重新启动之后 `degraded` 一直保持为 1 直到运行结束
（14 s 后），`redis_failures` 每 5 s 加 1。原因：`internal/gateway/cache` 的 Redis 客户端在一次 I/O 错误后只关闭当前连接，
池中其余空闲连接（这次约 8 个，限流器的池上限 32 个）都已被重启的 Redis 关闭，每次试探取出一个失效连接再失败一次，
恢复被推迟"空闲连接数 × 5 s"（最多约 160 s）。修复：表示服务端关闭了连接的错误（EOF、连接被重置、管道断开）之后丢弃
全部空闲连接；回归测试 `TestStalePoolDroppedAfterIOError`。上面的数字来自修复之后。

## 7. 故障注入（`scripts/fault-gateway.sh`，全部 PASS）

1. **流进行中 `kill -9` server。** fake upstream 每个事件间隔 1 s；收到第一个内容事件后 SIGKILL server。客户端的流没有
   `[DONE]`；hold 仍是 `held`（1135 µUSD），没有 `model_requests` 行。以同样的参数重启（`--model-api-max-duration 10s`，hold
   期限 = 开始 + 10 s + 5 min）：重启后 331 s，sweep 把它转为 `charged_unknown`，`charged_micro = amount_micro = 1135`；
   org 预算行 `reserved 0, unknown 1135`；`verify-invariants` 通过。`bill reconcile` 在 G5 合并之后补测。
2. **首字节之后供应商失败。** 四条路径各一个流：
   - oa>oa 原样转发：供应商的错误对象原样转发；
   - an>oa 转换：供应商断开，网关发出 Anthropic 的 `event: error`（`api_error`）；
   - an>an 原样转发：同上；
   - oa>an 转换：供应商的 `overloaded_error` 事件，网关发出 OpenAI 格式的 `{"error":{…,"code":"upstream_error"}}`。

   四个客户端都没有收到完成标记（`[DONE]` / `message_stop`）；`model_requests` 四行都是
   `200 stream_error stream_complete=false`。
3. **流进行中吊销 key。** 第一个内容事件之后经运维 API 吊销：这个流在吊销之后又持续约 4 s，以 `[DONE]` 正常结束，记录为
   `200, stream_complete = true`；下一个请求 `401 invalid_api_key`。
4. **熔断。** 主供应商的前 3 个请求回 503，`--model-breaker-failures 3 --model-breaker-open 5s`：请求 1–3 的 tries 为
   `primary/retryable/503, claude/ok/200`；请求 4–6 为 `claude/ok/200`，跳过原因 `primary:circuit_open`；等待 6 s 后请求 7–8
   回到主供应商（`primary/ok/200`）。主供应商的 fake upstream 一共只收到 5 个请求（3 个 503 + 2 个成功）；指标
   `agentbox_breaker_transitions_total{route="primary"}` 的 `open`、`half_open`、`closed` 各 1。

## 8. 演示（`scripts/demo-gateway.sh`，7 步全部通过，构建之后不到 1 分钟）

- 租户：alice、bob 注册；运维建 org `acme`（alice 为 owner）；alice 邀请，bob 接受；各自创建 models scope 的 key。
- 未修改的官方 SDK（openai 3.29.0、anthropic 1.13.0，`uv` 按 `tests/sdk/uv.lock` 安装）：2 个 SDK × 2 种供应商格式 ×
  （非流式、流式、工具调用、工具结果）共 16 个调用全部成功；`model_requests` 中四条路径各 4 个请求。
- 加权路由：`m-both` 由两个供应商按权重 3:1 提供，8 个请求 → `primary×6, claude×2`。
- 限流：owner 给 bob 的 key 设 `rpm 3`，第 4 个请求 `429 rate_limit_exceeded`，`Retry-After: 20`、
  `X-Ratelimit-Limit-Requests: 3`；key 预算 6000 µUSD 时第 7 个请求 `429 insufficient_quota`（"the monthly budget at the key
  level is exhausted"），同一个 key 以 Anthropic 格式调用得到 `rate_limit_error` 形状的同一消息。
- 吊销 key 后 `401 invalid_api_key`；`verify-invariants` 通过；指标：成功率 33/33（到达路由的请求），平均首字 6.7 ms，流完整
  4/4，拒绝 `auth 1, quota 2, rate_limit 1`。

演示中发现一个兼容性限制：Anthropic SDK 1.13.0 的回复对象经 `model_dump()` 后带 `caller: null` 字段，原样放回下一个请求得到
`400 unsupported field messages[1].content[0].caller`；按 Anthropic 文档直接放回 `response.content` 没有问题（SDK 只发送
已知字段）。已写入 `docs/usage.zh-CN.md` 的已知限制。

## 9. 真实供应商（Moonshot，可选检查）

WSL2 不能访问外网。在 Windows 上运行一个只转发的本机代理（127.0.0.1:18700 → `https://api.moonshot.cn`，不记录头与正文，
不入库），WSL2 中的 server 以它为主供应商（`/v1`，OpenAI 兼容）与 Anthropic 格式的后备供应商（`/anthropic`）；Key 只经
环境变量从 `.env` 读取，没有出现在命令行、日志或回复中（server 日志与 6 个回复中 Key 出现 0 次）。逻辑模型 `r-oa`、`r-an`
都映射到 `kimi-k2.7-code-highspeed`，提示 "Reply with exactly one word: pong"，`max_tokens` 400。

| 调用 | 路径 | 状态 | 回复 | 用量（输入 + 输出） | 耗时 |
|---|---|---|---|---|---|
| 1 | OpenAI 客户端 → OpenAI 兼容端点（原样转发） | 200 | `pong` | 14 + 27 | 0.89 s |
| 2 | Anthropic 客户端 → OpenAI 兼容端点（转换） | 200 | `pong` | 14 + 23 | 0.97 s |
| 3 | Anthropic 客户端 → Anthropic 兼容端点（原样转发） | 200 | `pong` | 14 + 26 | 0.91 s |
| 4 | OpenAI 客户端 → Anthropic 兼容端点（转换） | 200 | `pong` | 14 + 26 | 0.99 s |
| 5 | OpenAI 流式 → OpenAI 兼容端点 | 200 | 流完整 | 14 + 20 | 0.91 s |
| 6 | Anthropic 流式 → Anthropic 兼容端点 | 200 | 流完整（含 `thinking` 块） | 14 + 50 | 1.11 s |

Moonshot 的 Anthropic 兼容端点接受这个 Key。6 行 `model_requests` 都记录了用量（两个流 `stream_complete = true`），`verify-invariants` 通过。
共约 260 个 token，费用远低于 0.01 USD（按配置的单价记账 1982 µUSD，那不是 Moonshot 的实际计费）。只是单次连通性检查，
不涉及延迟或可靠性。

## 没有证明什么

- **不是真实供应商的性能。** fake upstream 立即回复；真实模型的首字时间以秒计（第 9 节约 0.9 s），网关的约 16 ms 在其中
  占比很小，但没有在真实负载下测过。
- **单机、共享 CPU。** 客户端、server、数据库与 fake upstream 争用同一组 CPU，其他开发任务同时运行；吞吐与拐点只对这台
  机器成立。多个 server 时限流按设计在 Redis 上全局生效（降级时按 server 生效），这里只有一个 server。
- **数据库在 Docker Desktop 中。** 提交落盘偏慢，决定了开销与 org 热点行的上限；没有在原生 PostgreSQL 或云数据库上测。
- **预算超支为 0 只覆盖这里的结局组合**（完整、流式、客户端放弃、数据库超时）；供应商报告的用量超过预留时按实际结算，
  超出由 `verify-invariants` 的 G2 单独报告（设计 §9），这里的 fake 用量远低于预留，没有触发。
- **kill -9 只测了一次、一个流**；hold 的转换依赖期限（这里 5 min 10 s）与 30 s 的 sweep 周期。
- `bill reconcile` 尚未运行（G5 合并后补测）。

## 原始数据

结果文件不入库：`results.jsonl`（每次运行一行 JSON，含参数与分位数）与 `summary.md` 由上面的命令重新生成。下面是第
2 次 latency 运行的原始输出，未作修改：

```
latency: concurrency 1, 6s per row (warm-up 1s), max_tokens 32, prompt 200 B; database round trip (SELECT 1) p50 0.224 ms, p99 0.911 ms
| path | stream | n | rps | gw p50 | gw p95 | gw p99 | direct p50 | direct p99 | overhead p50 | overhead p95 | overhead p99 | TTFT p50 / p99 ms | db tx/req | outcomes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| oa>oa | false | 350 | 58.2 | 16.00 | 24.00 | 26.90 | 0.23 | 0.45 | 15.77 | 23.69 | 26.46 | — | 4.0 | 200×350 |
| oa>oa | true | 390 | 64.9 | 15.00 | 18.30 | 22.08 | 0.30 | 0.60 | 14.70 | 17.89 | 21.48 | 5.63 / 9.63 (direct 0.29 / 0.56) | 3.8 | 200×390 |
| an>oa | false | 353 | 58.7 | 15.99 | 22.08 | 27.88 | 0.23 | 0.45 | 15.75 | 21.77 | 27.44 | — | 3.5 | 200×353 |
| an>oa | true | 355 | 59.2 | 15.98 | 21.99 | 25.86 | 0.30 | 0.60 | 15.68 | 21.58 | 25.26 | 6.51 / 10.70 (direct 0.29 / 0.56) | 4.5 | 200×355 |
| oa>an | false | 368 | 61.2 | 15.91 | 21.96 | 25.14 | 0.24 | 0.46 | 15.67 | 21.65 | 24.68 | — | 4.7 | 200×368 |
| oa>an | true | 365 | 60.6 | 15.95 | 21.86 | 25.29 | 0.30 | 0.79 | 15.64 | 21.39 | 24.50 | 6.48 / 11.07 (direct 0.28 / 0.73) | 4.1 | 200×365 |
| an>an | false | 355 | 59.2 | 16.01 | 22.35 | 26.91 | 0.24 | 0.46 | 15.78 | 22.04 | 26.44 | — | 4.1 | 200×355 |
| an>an | true | 367 | 61.1 | 15.92 | 20.92 | 28.11 | 0.30 | 0.79 | 15.62 | 20.45 | 27.32 | 6.42 / 12.51 (direct 0.28 / 0.73) | 4.2 | 200×367 |
```
