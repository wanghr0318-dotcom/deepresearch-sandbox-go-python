# M3 缓存收益测量——WSL2 + fake upstream（2026-10-05）

> **范围声明**：本记录在开发机的 **WSL2** 上测得，上游是**进程内 fake upstream**（127.0.0.1，每个回复前固定延迟
> 200 ms），不是真实网络。它说明的是"命中路径相对一次 200 ms 上游请求的开销"与"跨任务重复查询时上游请求减少多少"，
> 不是对真实搜索与抓取的收益结论。真实网络下的数字以后在服务器（上海 CVM）上用同一脚本补测。

- 规格：§16.6 缓存收益（"用新的逻辑任务与 call ID 重复相同查询，避免测到 journal 重放"）；计划：Plan 9 Task 5。
- 命令：`bash bench/cache/run.sh`（WSL2，普通用户；实现为 `tests/e2e` 的 `TestCacheBenefit`）。退出码 0。
- 代码：分支 `m3-p9-t5`（基于 `m3-batch` 的 `a918233`）。
- 方法：
  - 两组：`cache_on`（`app.Config.RedisAddr` = 本机 Redis）与 `cache_off`（不配置 Redis，等价于 `--cache=off`）。
    每组都是全新的数据目录、全新的 PostgreSQL 库与全新的 fake upstream。
  - 每组 10 次重复。每次重复提交一个**新任务**（Worker 是 sim_worker，只睡眠以保持 attempt 存活）；测试在该
    attempt 的 Gateway socket 上依次发出 1 个搜索（同一查询）和 3 个抓取（同 3 个 URL，`Cache-Control: max-age=3600`）。
    调用 ID 每次重复都不同（`bench/r<n>/...`），因此第 2 次起的命中来自共享缓存，不是 journal 重放。
  - 延迟是测试在 Unix socket 上看到的端到端时间：请求发出到响应体读完，包含访问检查、Tx1、Redis 查找、blob 复核和
    Tx2（命中），或者 Tx1、预留、上游请求和结算（未命中）。来源取自 `calls.source`。
  - 搜索查询与页面路径带随机后缀，不会读到之前运行留下的条目。
- 环境：
  - Windows 11 主机上的 WSL2（内核 6.6.87.2-microsoft-standard-WSL2），linux/amd64，24 个逻辑 CPU，Go 1.27.1。
  - PostgreSQL 16 与 Redis 7 都运行在 Docker Desktop 容器中，WSL2 经 127.0.0.1 访问（5432、6379）。
    因此每次命中都多一次 WSL2 → Windows 的 Redis 往返，PostgreSQL 事务也一样。
- 结论（只针对这个环境）：
  - 第 1 次重复的 4 个调用全部未命中，之后 36 个调用全部命中：命中率 90%（36/40）。
  - 上游请求从 40 次降到 4 次，减少 90%。
  - 命中的 p50 是 12.4 ms，未命中或关闭缓存时上游路径的 p50 约为 230 ms（其中 200 ms 是注入的上游延迟）。
  - 正确性另由 E21–E25 与不变量 I14（缓存部分）、I15 验证，这里只记录收益。

脚本输出的完整结果如下，未作修改。

### 环境

- 开始时间（UTC）：2026-10-05T16:03:48Z
- 主机：LAPTOP-HQ5E1RG1；内核 6.6.87.2-microsoft-standard-WSL2；linux/amd64，24 个逻辑 CPU；go1.27.1
- Redis：127.0.0.1:6379；PostgreSQL：AGENTBOX_TEST_DATABASE_URL（本机）
- 上游：进程内 fake upstream（127.0.0.1），搜索与抓取的每个正常回复前固定延迟 200ms
- 每组 10 次重复；每次重复 = 新任务（新的逻辑任务与调用 ID）依次发出 1 个搜索 + 3 个抓取（抓取页面 Cache-Control: max-age=3600）；延迟为测试在 attempt 的 Gateway socket 上看到的端到端时间

### 汇总

| 组 | 调用数 | 命中（source = cache） | 命中率 | 上游请求数 | /status 缓存指标 |
|---|---|---|---|---|---|
| cache_on | 40 | 36 | 90% | 4 | breaker_open=0 bypass=0 coalesced=0 error=0 hit=36 integrity_failure=0 miss=4 |
| cache_off | 40 | 0 | 0% | 40 | （缓存关闭） |

上游请求减少：40 → 4（90%）。

### 延迟分布（毫秒）

| 样本 | n | 最小 | p50 | p90 | 最大 | 平均 |
|---|---|---|---|---|---|---|
| 缓存开：命中（cache） | 36 | 10.3 | 12.4 | 16.5 | 22.2 | 13.4 |
| 缓存开：未命中（upstream） | 4 | 229.5 | 233.9 | 243.1 | 243.1 | 235.1 |
| 缓存关：全部（upstream） | 40 | 227.3 | 230.4 | 235.8 | 245.1 | 231.8 |
| 命中：搜索 | 9 | 11.6 | 13.1 | 22.2 | 22.2 | 15.2 |
| 命中：抓取 | 27 | 10.3 | 12.4 | 15.7 | 16.5 | 12.9 |
| 上游：搜索 | 11 | 227.8 | 234.1 | 243.4 | 245.1 | 236.1 |
| 上游：抓取 | 33 | 227.3 | 229.8 | 234.4 | 237.0 | 230.8 |

### 原始数据

| 组 | 重复 | 调用 ID | 类别 | 来源 | 延迟（ms） |
|---|---|---|---|---|---|
| cache_on | 1 | bench/r1/search/1 | search | upstream | 243.1 |
| cache_on | 1 | bench/r1/fetch/2 | fetch | upstream | 234.0 |
| cache_on | 1 | bench/r1/fetch/3 | fetch | upstream | 233.9 |
| cache_on | 1 | bench/r1/fetch/4 | fetch | upstream | 229.5 |
| cache_on | 2 | bench/r2/search/1 | search | cache | 13.1 |
| cache_on | 2 | bench/r2/fetch/2 | fetch | cache | 12.2 |
| cache_on | 2 | bench/r2/fetch/3 | fetch | cache | 11.4 |
| cache_on | 2 | bench/r2/fetch/4 | fetch | cache | 12.0 |
| cache_on | 3 | bench/r3/search/1 | search | cache | 22.2 |
| cache_on | 3 | bench/r3/fetch/2 | fetch | cache | 15.7 |
| cache_on | 3 | bench/r3/fetch/3 | fetch | cache | 15.1 |
| cache_on | 3 | bench/r3/fetch/4 | fetch | cache | 13.5 |
| cache_on | 4 | bench/r4/search/1 | search | cache | 12.3 |
| cache_on | 4 | bench/r4/fetch/2 | fetch | cache | 12.0 |
| cache_on | 4 | bench/r4/fetch/3 | fetch | cache | 12.4 |
| cache_on | 4 | bench/r4/fetch/4 | fetch | cache | 12.1 |
| cache_on | 5 | bench/r5/search/1 | search | cache | 11.6 |
| cache_on | 5 | bench/r5/fetch/2 | fetch | cache | 10.7 |
| cache_on | 5 | bench/r5/fetch/3 | fetch | cache | 12.1 |
| cache_on | 5 | bench/r5/fetch/4 | fetch | cache | 14.1 |
| cache_on | 6 | bench/r6/search/1 | search | cache | 12.4 |
| cache_on | 6 | bench/r6/fetch/2 | fetch | cache | 12.5 |
| cache_on | 6 | bench/r6/fetch/3 | fetch | cache | 10.3 |
| cache_on | 6 | bench/r6/fetch/4 | fetch | cache | 11.3 |
| cache_on | 7 | bench/r7/search/1 | search | cache | 18.3 |
| cache_on | 7 | bench/r7/fetch/2 | fetch | cache | 12.0 |
| cache_on | 7 | bench/r7/fetch/3 | fetch | cache | 13.5 |
| cache_on | 7 | bench/r7/fetch/4 | fetch | cache | 12.1 |
| cache_on | 8 | bench/r8/search/1 | search | cache | 11.6 |
| cache_on | 8 | bench/r8/fetch/2 | fetch | cache | 12.2 |
| cache_on | 8 | bench/r8/fetch/3 | fetch | cache | 11.5 |
| cache_on | 8 | bench/r8/fetch/4 | fetch | cache | 12.9 |
| cache_on | 9 | bench/r9/search/1 | search | cache | 21.4 |
| cache_on | 9 | bench/r9/fetch/2 | fetch | cache | 16.5 |
| cache_on | 9 | bench/r9/fetch/3 | fetch | cache | 15.7 |
| cache_on | 9 | bench/r9/fetch/4 | fetch | cache | 13.4 |
| cache_on | 10 | bench/r10/search/1 | search | cache | 13.6 |
| cache_on | 10 | bench/r10/fetch/2 | fetch | cache | 12.4 |
| cache_on | 10 | bench/r10/fetch/3 | fetch | cache | 13.8 |
| cache_on | 10 | bench/r10/fetch/4 | fetch | cache | 14.4 |
| cache_off | 1 | bench/r1/search/1 | search | upstream | 242.7 |
| cache_off | 1 | bench/r1/fetch/2 | fetch | upstream | 229.3 |
| cache_off | 1 | bench/r1/fetch/3 | fetch | upstream | 232.5 |
| cache_off | 1 | bench/r1/fetch/4 | fetch | upstream | 233.0 |
| cache_off | 2 | bench/r2/search/1 | search | upstream | 243.4 |
| cache_off | 2 | bench/r2/fetch/2 | fetch | upstream | 230.7 |
| cache_off | 2 | bench/r2/fetch/3 | fetch | upstream | 227.3 |
| cache_off | 2 | bench/r2/fetch/4 | fetch | upstream | 229.0 |
| cache_off | 3 | bench/r3/search/1 | search | upstream | 230.4 |
| cache_off | 3 | bench/r3/fetch/2 | fetch | upstream | 229.1 |
| cache_off | 3 | bench/r3/fetch/3 | fetch | upstream | 237.0 |
| cache_off | 3 | bench/r3/fetch/4 | fetch | upstream | 232.8 |
| cache_off | 4 | bench/r4/search/1 | search | upstream | 235.8 |
| cache_off | 4 | bench/r4/fetch/2 | fetch | upstream | 230.6 |
| cache_off | 4 | bench/r4/fetch/3 | fetch | upstream | 227.9 |
| cache_off | 4 | bench/r4/fetch/4 | fetch | upstream | 229.0 |
| cache_off | 5 | bench/r5/search/1 | search | upstream | 232.1 |
| cache_off | 5 | bench/r5/fetch/2 | fetch | upstream | 228.6 |
| cache_off | 5 | bench/r5/fetch/3 | fetch | upstream | 235.6 |
| cache_off | 5 | bench/r5/fetch/4 | fetch | upstream | 235.3 |
| cache_off | 6 | bench/r6/search/1 | search | upstream | 232.9 |
| cache_off | 6 | bench/r6/fetch/2 | fetch | upstream | 231.5 |
| cache_off | 6 | bench/r6/fetch/3 | fetch | upstream | 229.3 |
| cache_off | 6 | bench/r6/fetch/4 | fetch | upstream | 234.4 |
| cache_off | 7 | bench/r7/search/1 | search | upstream | 234.1 |
| cache_off | 7 | bench/r7/fetch/2 | fetch | upstream | 231.4 |
| cache_off | 7 | bench/r7/fetch/3 | fetch | upstream | 231.2 |
| cache_off | 7 | bench/r7/fetch/4 | fetch | upstream | 229.4 |
| cache_off | 8 | bench/r8/search/1 | search | upstream | 245.1 |
| cache_off | 8 | bench/r8/fetch/2 | fetch | upstream | 232.4 |
| cache_off | 8 | bench/r8/fetch/3 | fetch | upstream | 229.2 |
| cache_off | 8 | bench/r8/fetch/4 | fetch | upstream | 229.3 |
| cache_off | 9 | bench/r9/search/1 | search | upstream | 227.8 |
| cache_off | 9 | bench/r9/fetch/2 | fetch | upstream | 229.8 |
| cache_off | 9 | bench/r9/fetch/3 | fetch | upstream | 228.4 |
| cache_off | 9 | bench/r9/fetch/4 | fetch | upstream | 230.3 |
| cache_off | 10 | bench/r10/search/1 | search | upstream | 229.7 |
| cache_off | 10 | bench/r10/fetch/2 | fetch | upstream | 228.0 |
| cache_off | 10 | bench/r10/fetch/3 | fetch | upstream | 227.9 |
| cache_off | 10 | bench/r10/fetch/4 | fetch | upstream | 227.6 |
