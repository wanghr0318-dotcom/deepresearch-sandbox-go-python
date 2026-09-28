# go-agentbox 设计稿：Agent 工具沙箱运行时

> 状态：设计稿 · 2026-09-29
> 范围：沙箱管控面 + Provider 层 + ReAct 执行框架

---

## 1. 项目定位

**一句话**：给 Agent 用的工具沙箱运行时 —— 隔离执行、资源限额、自动回收、产物持久化；外挂一层可按场景装配的 ReAct 执行框架。

主体是沙箱运行时（约 70% 工作量与绝大部分技术深度），框架外壳负责把沙箱放进真实使用场景，并支撑"多场景可迁移"这个命题。

### 1.1 与既有方案的关系

| 方案 | 关系 |
|---|---|
| CloudWeGo Eino（字节，Go LLM/Agent 框架） | **依赖**。ReAct 主循环采用其 graph 编排（见 §5.1） |
| 腾讯云 AGS / Cube Sandbox | 对标物。Provider 接口按其能力面定义，并提供一个可选的 AGS provider |
| E2B | 接口语义参考。沙箱内 daemon + Connect 风格 API 的模型来自这里 |
| runc | 参考。re-exec init 模式来自这里 |

对 Eino 是依赖，对其余是参考 —— **参考的判据：关掉源码能否从头写出来并解释每个决定**。

### 1.2 非目标

- 不实现通用 DAG 编排引擎（用 Eino）
- 不实现 microVM 级隔离（见 §6）
- 不实现进程级快照（CRIU）
- 第一版不支持浏览器沙箱的独立隔离（browser 走沙箱内 CDP）

---

## 2. 整体架构

```
入口层（按 scenario 装配 workflow，一次性完成）
   │
   ▼
Template 节点 ──► ReAct Graph（Eino）──► 流式事件输出
                      │
                      ▼
                  ToolsNode（自实现）
                      └─ 中间件链（洋葱）
                           └─ 工具执行（按 binding 分流）
                                ├─ loop      进程内
                                ├─ 无 sandbox 进程内直调
                                ├─ service   → sandbox exec service → envd
                                └─ reception → 沙箱内
                                                  │
                          ┌───────────────────────┴──────────┐
                          │  Sandbox 管控面                   │
                          │  Naming / Locking / Lifecycle /   │
                          │  Pool / Reaper / Snapshot / Limiter│
                          └───────────────┬───────────────────┘
                                          │
                                   Provider 接口
                          ┌───────────────┴───────────────┐
                    LocalProvider                  TencentProvider
              （namespace + cgroup，零外部依赖）    （AGS 控制面 HTTP）
```

---

## 3. 沙箱管控面

### 3.1 命名与绑定

```
主沙箱      sb:{chatID}
子 agent    sb:{chatID}:{sha256(chatID:agentID)[:16]}
```

用 SHA256 截断而非直接拼 agentID：agentID 可能含非法字符或过长；定长便于 key 规划；不泄露业务标识。截 16 hex（64 bit），单会话内碰撞可忽略。

沙箱名的三重身份：**全局复用标识 / 分布式锁键 / 绑定记录键**。

绑定记录：

```
binding:{name} → { sandbox_id, provider, state, created_at, last_active_at, ttl_deadline }
```

放 Redis 而非 DB：每次工具调用都要 ensure，属热路径。绑定丢失不致命 —— 沙箱变孤儿，provider 侧 TTL 兜底回收，对账任务扫出。DB 作为最终一致的账本用于对账。

**sandboxID 不从工具入参传入**，由执行层按 chatID（子 agent 再叠 agentID）自行绑定。工具无从指定要用哪个箱，这是防止越权访问他人沙箱的第一道闸。

### 3.2 Store 抽象

管控面的全部外部状态收敛到一个接口，使单机与分布式两种形态可切换：

```go
type Store interface {
    // 绑定
    GetBinding(ctx context.Context, name string) (*Binding, error)
    PutBinding(ctx context.Context, name string, b *Binding) error
    DelBinding(ctx context.Context, name string, expectSandboxID string) error // CAS
    ListBindings(ctx context.Context, prefix string) ([]*Binding, error)

    // 分布式锁
    Lock(ctx context.Context, key string, ttl time.Duration) (token string, err error)
    Unlock(ctx context.Context, key, token string) error
    Renew(ctx context.Context, key, token string, ttl time.Duration) error

    // 延迟队列
    Enqueue(ctx context.Context, queue string, item []byte, runAt time.Time) error
    Dequeue(ctx context.Context, queue string, now time.Time, n int) ([][]byte, error)

    // 信号量
    SemAcquire(ctx context.Context, key, token string, limit int, timeout time.Duration) (bool, error)
    SemRelease(ctx context.Context, key, token string) error
}
```

| 实现 | 用途 | 语义 |
|---|---|---|
| `MemStore` | 默认，零外部依赖，`go run . demo` 即可跑 | 单进程内正确；锁与信号量退化为进程内原语 |
| `RedisStore` | 生产 | 完整分布式语义，Lua 保证原子性 |

`MemStore` 下分布式特性退化为本地语义，因此 `RedisStore` 的正确性必须由 §7 的多进程并发测试单独证明。

### 3.3 ensure 流程

```
ensure(ctx, chatID, agentID) → Sandbox

① name := buildName(chatID, agentID)

② 快路径：读 binding:{name}
     命中 && state ∈ {running, hibernated}
        → 探活
           活 → 续 TTL →（hibernated 则 resume）→ 返回
           死 → 落慢路径，并标记 recreated=true
     未命中 → 落慢路径

③ 慢路径：抢分布式锁 lock:{name}（带 token、TTL、watchdog）
     抢不到 → 阻塞等待，带超时上限，不无限重试

④ 双检：拿到锁后重读 binding（其他实例可能刚建好）

⑤ 建箱：
     a. 从 ready pool 取预热实例（池空则直接 create）
     b. 按 chatID 挂 workspace
     c. 随创建请求注入 env 凭证
     d. 有快照则从快照起，失败降级用模板

⑥ 写 binding → 释放锁 → 返回
```

**双检（④）必须有**。缺失会导致并发 ensure 建出两个箱，其中一个立刻成为孤儿。

**释放锁必须带 token**，走 Lua 原子比对：

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```

否则：A 持锁超时自动释放 → B 拿锁 → A 完成后删掉 B 的锁 → C 也拿到锁 → 两个持有者同时在临界区。

**watchdog 续期**必须绑定 context，业务结束立即停。

**重建告知**：探活失败导致重建时（`recreated=true`），该轮**首个工具结果**须附加提示 `sandbox was recreated`，让模型知道沙箱内的临时状态（进程、未落 workspace 的文件）已丢失。不告知会导致模型基于过时假设继续操作。

### 3.4 一致性模型

> **Provider 是真相源，binding 是缓存。**

Provider 才真正持有资源；binding 存在只为避免每次查 provider。因此快路径乐观信任 binding，**但执行前必须探活**，探活失败即以 provider 为准。

| 不一致情况 | 成因 | 处理 |
|---|---|---|
| binding 有 / provider 无 | 被 provider TTL 回收 | 解绑 + 重建 |
| binding 无 / provider 有 | 孤儿箱（绑定写失败或 Redis 丢数据） | 对账任务扫出后关闭 |
| 两边状态不同 | 例如 binding=running 实际 hibernated | 以 provider 为准，刷新 binding |

孤儿箱靠 `Provider.List(prefix)` + 统一命名前缀扫出。**命名规范是可对账性的前提，不是美观问题。**

### 3.5 状态机与三层 TTL

```
creating → running ⇄ hibernated → closed
              ↘      error      ↙
```

`hibernated` 仍视为可复用，ensure 时直接 resume。`closed / error / notfound` 一律解绑重建。

| TTL 层 | 作用 | 参考值 |
|---|---|---|
| 锁 TTL | 防持锁者崩溃死锁 | 30s + watchdog |
| 业务 TTL（binding） | 管控面据此回收 | 按场景，15–60 min idle |
| Provider TTL | 管控面整体故障时的最终保险 | **严格大于**业务 TTL |

Provider TTL 写反的后果：管控面以为箱还在、provider 早已回收，每次 ensure 都要重建，预热池形同虚设 —— 且表现为偶发变慢，极难定位。

续期时机：ensure 成功后、每次 Exec 完成后。

### 3.6 Reaper：幂等优先于选主

每秒运行的维护任务。多实例部署会撞车：同时扫到同一过期箱、同时打快照、同时 Close。

常规解法是 Redis 选主，但选主必然存在脑裂窗口。**正确性由幂等保证，选主只是优化**：

- `Close` 一个已关闭的箱返回成功，不报错
- `Snapshot` 带版本号，重复打直接跳过
- 删 binding 用 CAS（比对 sandbox_id）

回收走延迟队列：

```
Reaper 扫描 → 标记过期 → 延迟队列（score = 执行时间）
                            ↓
       Worker：取到期项 → 最终快照 → Close → 删 binding
```

三个理由：打快照耗时不能阻塞扫描；失败可重新入队带退避；批量过期时削峰，不打爆 provider。多消费者取任务须用 Lua 保证"取出 + 删除"原子。

### 3.7 并发控制：两级信号量与嵌套顺序

系统里有**两级**并发限制，两者都必要，但嵌套顺序写反会死锁。

| 级别 | 键 | 限什么 | 默认 |
|---|---|---|---|
| 会话级 | `sem:{chatID}` | 该会话**总的**在途工具执行数（跨轮、跨 subagent） | 按场景 |
| 轮内 | 进程内 | 单轮内并行执行的 tool_call 数 | 10 |

**强制顺序：先抢会话级，再进轮内并发。**

```
for each tool_call in round:
    ① SemAcquire(sem:{chatID})        ← 会话级，可能阻塞
    ② 提交到轮内并发池（上限 10）
    ③ 执行（含 sandbox ensure）
    ④ SemRelease(sem:{chatID})        ← defer，覆盖整个执行
```

顺序反过来的后果：轮内先放行 10 个，它们再去抢只允许 5 个的会话级信号量 —— 5 个占住轮内槽位却在阻塞，另 5 个拿不到会话级也进不来，若彼此有依赖即死锁。

**会话级信号量的持有必须覆盖整个工具执行（含 sandbox ensure）**，不能只包住调用那一瞬间，否则限的不是并发而是 QPS。

会话信号量用带自动过期的 ZSET 实现，避免持有者崩溃后计数不归零导致会话永久卡死：

```lua
-- KEYS[1]=sem:{chatID}   ARGV: now, timeout, limit, token
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[2])
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
    return 1
end
return 0
```

超限任务在 DB 保持 `pending`，release 时通过 Pub/Sub 唤醒。

### 3.8 预热池与快照

**预热池**：后台补水位。池空时必须有并发创建上限，否则流量尖峰会惊群 —— 大量请求同时发现池空并同时 create，打爆 provider。

预热省下的是什么随 provider 而变：

| Provider | 建箱耗时主要来自 | 预热省下的部分 |
|---|---|---|
| Local | mount overlay + 建 cgroup + fork + pivot_root + init 就绪 | 全部（仅剩挂 workspace） |
| Tencent | 云侧实例调度与启动 | 云侧启动延迟 |

采用 overlayfs 后已无需预解压 rootfs（见 §4.4），预热池的价值转为省去 mount / cgroup / fork 这一串系统调用与 init 就绪等待。因此 **Local 侧预热池收益显著小于云侧**，是否启用应可配置，以 §7 的冷启动压测数据决定。

**快照**：建箱时恢复 → 执行后限频异步刷新 → 回收时最终快照 → 失败则降级用模板。

已有持久化 workspace 仍需快照的原因：workspace 只有文件，快照含整个 rootfs 状态（安装的包、修改的系统配置）。代价是快照大、慢、贵，故必须限频。

---

## 4. Provider 层

### 4.1 接口

```go
type Provider interface {
    Name() string
    Capabilities() Capabilities
    Create(ctx context.Context, spec CreateSpec) (*Instance, error)
    Get(ctx context.Context, id string) (*Instance, error)
    List(ctx context.Context, prefix string) ([]*Instance, error) // 对账
    Resume(ctx context.Context, id string) error
    Close(ctx context.Context, id string) error                   // 幂等
    Snapshot(ctx context.Context, id string) (string, error)
    Exec(ctx context.Context, id string, req ExecRequest) (*ExecResult, error)
}

type CreateSpec struct {
    Name       string
    Template   string
    SnapshotID string
    Env        map[string]string  // 凭证随创建注入，不长期传递
    Workspace  WorkspaceMount
    TTL        time.Duration
    Limits     ResourceLimits
    Network    NetworkPolicy      // 可出网 / 禁止入站
}

type ExecRequest struct {
    Cmd     []string
    Env     []string
    WorkDir string             // 沙箱内路径，默认 /workspace
    Stdin   io.Reader
    Stdout  io.Writer          // 流式直出，接事件流
    Stderr  io.Writer
    Timeout time.Duration
}

type ExecResult struct {
    ExitCode int
    Duration time.Duration
    Killed   bool              // 超时被杀
    OOM      bool              // 被 cgroup OOM killer 杀
}
```

`Stdout/Stderr` 用 `io.Writer` 而非在结果里返回，因为需要边跑边推给前端。

`OOM` 单列：被内存限额杀掉与命令自身失败对 agent 是不同信号，应触发不同反应。

### 4.2 能力差异显式化

```go
type Capabilities struct {
    HibernateFreesMemory   bool  // Local=false, Tencent=true
    SnapshotIncludesMemory bool  // Local=false, Tencent=true
    MaxConcurrent          int
}
```

接口统一，但能力不对等，**策略必须感知差异**。例如 LocalProvider 的 hibernate 不释放内存，其 idle TTL 应显著短于云 provider。

### 4.3 fail-closed 注册

```go
func Register(name string, p Provider)
func Resolve(scenario string) (Provider, error)  // 未注册 → error，绝不 fallback
```

宁可整条请求失败，也不让本该跑在受限环境的工具悄悄跑到别处。

### 4.4 LocalProvider

#### 目录布局

```
/var/lib/agentbox/
  ├─ templates/default/     # 模板 rootfs（alpine minirootfs），只读共享
  ├─ instances/{id}/
  │    ├─ upper/            # overlayfs 可写层
  │    ├─ work/             # overlayfs 工作目录
  │    ├─ merged/           # 挂载点 = 沙箱的 /
  │    └─ state.json        # 实例元数据，List 依此
  ├─ ws/{chatID}/           # 持久化 workspace → bind mount 到 /workspace
  └─ snapshots/{sid}.tar.gz
```

**overlayfs 是关键**：template 作为 lowerdir 只读共享，每实例仅有空 upper 层。建箱从"拷贝几十 MB"降为"mkdir 三个目录 + 一次 mount"，快照只需打包 upper 层。原理同 Docker 镜像分层。

#### 创建流程

```
① mkdir instances/{id}/{upper,work,merged}
② mount -t overlay overlay -o lowerdir=templates/default,upperdir=...,workdir=... merged
③ bind mount ws/{chatID} → merged/workspace
④ 建 cgroup /sys/fs/cgroup/agentbox/{id}/ ：cpu.max / memory.max / pids.max
⑤ fork：Cloneflags = NEWNS|NEWPID|NEWUTS|NEWIPC|NEWNET|NEWUSER, Setpgid
⑥ 子进程内：写 cgroup.procs → pivot_root → 挂 /proc /sys /dev → 降权 → exec init
⑦ 写 state.json
```

#### re-exec 处理线程敏感 syscall

`pivot_root` / `setns` 是线程级调用，而 goroutine 会在 OS 线程间迁移。解法是 re-exec 自身（runc 同款）：

```go
// 父进程
cmd := exec.Command("/proc/self/exe", "init")
cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: ...}

// main() 开头，早于任何初始化
func main() {
    if len(os.Args) > 1 && os.Args[1] == "init" {
        runInit()   // pivot_root、挂载、降权，然后 exec 目标 init
        return
    }
    // 正常启动路径
}
```

#### 沙箱内 mini-envd

1 号进程职责：

1. `wait()` 循环收割孤儿进程 —— 不做则僵尸堆满 `pids.max`，沙箱自我饿死
2. 通过 unix socket 接收 exec 请求并 fork
3. 转发信号

选择"和沙箱内 daemon 通信"而非"管控面 setns 后 fork"，使 LocalProvider 与 TencentProvider 的 Exec 语义完全一致，Provider 抽象才真正成立；同时避开 `LockOSThread` 问题。

#### Hibernate = cgroup v2 freezer

```bash
echo 1 > /sys/fs/cgroup/agentbox/{id}/cgroup.freeze   # 冻结
echo 0 > /sys/fs/cgroup/agentbox/{id}/cgroup.freeze   # 恢复
```

边界：仅停 CPU 调度，**内存不释放**。故 `HibernateFreesMemory = false`。

#### Snapshot 降级实现

```bash
tar czf snapshots/{sid}.tar.gz -C instances/{id}/upper .
```

能恢复：安装的包、改动的配置、写入的文件。
不能恢复：运行中的进程、内存状态。进程级快照需 CRIU，划至范围外。

#### Close 顺序

```
① 冻结 cgroup                    ← 防止清理期间继续 fork
② 向 PID ns 1 号进程发 SIGTERM，等 grace period
③ 超时则 SIGKILL 1 号进程 → 内核连带清理整个 PID namespace
④ 轮询 cgroup.procs 直到为空     ← 内核回收异步，必须确认
⑤ umount merged/workspace       （EBUSY → MNT_DETACH lazy umount）
⑥ umount merged（overlayfs）
⑦ rmdir cgroup 目录             （非空则失败，故 ④ 不可跳过）
⑧ 删 instances/{id}，保留 ws/ 与 snapshots/
```

### 4.5 TencentProvider

通过 AGS 控制面 HTTP 接口实现，执行面走沙箱内 envd Connect API。第一版可仅实现骨架，用途是证明 Provider 抽象确实面向两种形态完全不同的后端。

---

## 5. ReAct 执行框架

### 5.1 主循环：Eino Graph

主循环用 Eino 的 graph 编排，独立在 `react.go`。**pipeline 只做装配，不含循环逻辑。**

```
START
  │
  ▼
ChatModel 节点 ──branch──► 有 tool_calls ──► ToolsNode（并发执行）
  ▲                    │                            │
  │                    └──► 无 tool_calls ──► END   │
  └────────────────────── branch ◄─────────────────┘
```

每轮：

```
curRound++
  → 上一轮结果 append 到 state.Messages
  → 跑 compactor 链（§5.5）
  → 调 ChatModel
```

### 5.2 终止条件

命中任一即终止：

| # | 条件 | 处理 |
|---|---|---|
| 1 | 模型不再发 tool_call（`finish_reason=stop`） | 正常终止 |
| 2 | `curRound >= maxRound` | 见下 |
| 3 | 用户软取消 / 中断 | 收尾并落 trace |
| 4 | 超时或 fatal 错误 | 报错终止 |

**默认 maxRound：主 agent 50，子 agent 30。** `ScenarioConfig.React.MaxRound` 可覆盖（§5.10 三场景表给的就是覆盖值）。

超上限的收尾有两种模式，由场景配置选择：

- **报错终止**
- **"无工具总结"收尾** —— 再调一次模型，但不带工具目录，强制它产出最终文本

两种模式都须向模型注入 `max_round_tool_hit` 标记，让它知道是被截断而非自然结束。

### 5.3 模型调用层（buildChatModel）

装配期一次性构建，运行期只读。react graph 拿到的只是一个 `ChatModel` 接口，**不感知降级与续写**。

#### 配置装配

配置来自 workflow yaml，用 [koanf](https://github.com/knadh/koanf) 分层合并，产出一份最终的 `ModelChainConfig`：

```
workflow.yaml ──koanf 分层合并──► ModelChainConfig ──► buildChatModel
   pipelines/config.go                                pipelines/workflow.go
```

```go
type ModelChainConfig struct {
    BaseURL       string
    APIKey        string
    Model         string
    Protocol      string             // 原生协议标识
    MaxTokens     int
    ContextLength int
    Temperature   float64
    Fallbacks     []ModelRef         // 降级链
    ExtraHeaders  map[string]string  // 透传给推理网关的上行头，默认空
}
```

`pipelines/config.go` 只负责把 yaml 合并成 `ModelChainConfig`；`pipelines/workflow.go` 的 `buildChatModel` 只负责把 config 组装成运行期链路。

**不做 A/B 分桶与灰度变体**。理由：本项目无生产流量，分桶通道没有使用场景；而 koanf 本身即支持分层覆盖，将来若需要变体，是新增一层 yaml 而非改代码，**延迟的成本为零**。同理 `ExtraHeaders` 默认为空，推理网关侧若需要打标，配 yaml 即可。

#### 运行期链路

```
ChatModel（对外接口）
 └─ Aggregator     tool_call 按 index 聚合             ← 必须在最外层
     └─ AutoContinue  finish_reason 续写，独立预算
         └─ Fallback   主模型故障 → 降级链
             └─ Adapter（原生协议 + SSE 解码 + 归一）
```

**Fallback 与分桶是两回事，只去掉后者**：

| | 触发条件 | 去留 |
|---|---|---|
| 分桶 / variant | 实验、灰度 —— 主动按规则分流 | 去掉 |
| Fallback 降级 | 故障 —— 主模型超时 / 限流 / 5xx 才走 | 保留 |

降级是可用性机制，属运行期行为，无法靠配置分层替代。

#### 归一化下沉到 Adapter

各厂商 SSE 格式不同，Adapter 解析成统一 chunk，上层不感知协议差异：

```go
type Chunk struct {
    TextDelta      string
    ReasoningDelta string
    ToolCallDelta  *ToolCallDelta   // {Index, ID, Name, ArgsDelta}
    FinishReason   FinishReason     // 已归一
    Usage          *Usage
}
```

`finish_reason` 也必须归一（各家用 `stop` / `end_turn` / `max_tokens` / `length` 表达同一含义），否则 auto-continue 的触发条件要在每个 adapter 里重写一遍。

#### tool_call 聚合

按 OpenAI function-calling schema 聚合：

```go
type ToolCallBucket struct {
    ID   string          // 首次出现时写入，后续到达一律忽略
    Name string          // 同上
    Args strings.Builder // 增量拼接
}
```

**Aggregator 必须位于 AutoContinue 之外（更外层）。**

原因：`arguments` 可能在拼到一半时被 `length` 截断，续写后要接着拼进同一个桶。而**续写请求返回的 SSE，其 `tool_call.index` 通常会重新从 0 开始** —— 若直接按 index 分桶，第二段会覆盖第一段而非追加，产出半截 JSON。这个 bug 只在长 arguments 时偶发，极难定位。

解法：Aggregator 以 `(continueSeq 归一后的全局 index)` 为键，Adapter 在续写时提供 index offset 映射。AutoContinue 对上游只暴露一条连续的 chunk 流。

#### auto-continue

触发：`finish_reason ∈ {length, server_interrupt, repeat}`。**与 react 轮次无关**，不消耗 `curRound`。

**必须有硬上限**，否则模型持续返回 `length` 即无限续写：

```go
type AutoContinueConfig struct {
    MaxContinues int            // 硬上限 3
    TokenBudget  int            // 单次模型调用内累计 token 上限
    TriggerOn    []FinishReason
}
```

达到上限后：**返回已累积的截断内容 + 最后一轮 tool_calls，循环终止**。

**截断的 tool_call 必须转为错误 tool 消息，不得交给 ToolsNode 执行。** 在 `arguments` 拼到一半被截断时，那段 JSON 是不完整的 —— 直接送去执行会在反序列化处炸，而且错误信息毫无指向性。正确处理是：进 ToolsNode 前校验每个 tool_call 的 `arguments` 能否解析，不能解析的按 §5.4 产出 `is_error` 的 tool 消息。这样既满足了"每个 tool_call 必须有对应 tool 消息"的协议约束，又让模型知道自己的输出被截断了。

#### maxTokens 的实际生效值

`MaxTokens` 不是定值，每轮按剩余窗口收缩：

```
available = ContextLength - input_tokens - tools_tokens

if available <= 0:
    return 1                        // 兜底，避免完全失败
if MaxTokens <= available:
    return MaxTokens                // 配置值够用
return max(available * 0.9, 1)      // 留 10% buffer
```

默认 fallback 值 4096。

**配置过小的三个后果**（都不是立即可见的错误，而是缓慢劣化）：

| 后果 | 机理 |
|---|---|
| 延迟与成本 × n | 每次回答都要续写数轮，每轮都重传完整上下文 |
| 加速触顶 | 续写产生的中间内容也进 history，压缩触发得更频繁，形成恶性循环 |
| tool_call 截断 | 长 `arguments` 更容易被腰斩，落到上面那条错误路径 |

因此 `Validate()` 除 §5.3 已有的检查外，还须加一条：**`available` 低于某个下限时不应发起模型调用**。返回 1 只是防止程序崩溃，一个被要求产出 1 个 token 的请求在业务上必然无用 —— 此时正确的动作是先强制压缩，压缩后 `available` 仍不足则显式报错，而不是发一个注定无意义的请求出去。

#### 超时

| 层 | 值 |
|---|---|
| SSE 空闲（两个 chunk 之间） | 5 min |
| HTTP 总时长 | 1 h |

两者都到期即断开并按 fatal 错误处理。

#### 配置一致性校验

`Validate()` 须在启动时检查两处，它们配错都不会立刻报错，而是表现为线上偶发异常：

| 检查 | 原因 |
|---|---|
| `ctx_tools` 压缩阈值须由 `ContextLength` 推导，且留足输出预留 | 两者独立配置且不一致时，会出现"压缩没触发、但请求已超长被服务端拒绝" |
| `MaxTokens` 低于下限时告警，并与 `AutoContinue.MaxContinues` 联合校验 | `finish_reason=length` 正是撞 `MaxTokens` 产生的；配得过于保守会导致每次回答都续写数轮，token 成本与延迟同时翻倍 |

### 5.4 工具层

#### 工具目录

模型可见的 schema 来自 preset yaml：

| 组 | 工具 |
|---|---|
| 任务 | `todo_read` / `todo_write` |
| 执行 | `ipython` |
| 文件 | `read_file` / `edit_file` / `write_file` |
| 浏览 | `browser` |
| 检索 | `web_search` / `web_open_url` / `search_image` |

#### 裁剪

按 `rule.yaml` 依 workflow 裁剪，每个工具声明：

```yaml
tools:
  ipython:
    enabled: true
    optional: false      # 场景不可关闭
  browser:
    enabled: false
    optional: true       # 场景可开
```

#### 执行双轨（binding）

| binding | 落点 | 工具 |
|---|---|---|
| `loop` | 进程内循环 | `todo_*`、子 agent（`task`） |
| 无 sandbox | 进程内直调 | `web_search`、`web_open_url`、`search_image` |
| `service` | tools → sandbox exec service → envd | 文件类 |
| `reception`（或留空） | 沙箱内 | ssh(user)；legacy：`ipython` → kernel server、`browser` → CDP |

`sandboxID` 不从入参传入（§3.1）。

#### 并发与容错

单轮多个 tool_call 并发执行，并发限制见 §3.7 的两级信号量。

**协议硬约束：每个 `tool_call` 必须有一条 `tool_call_id` 对应的 `role=tool` 消息。** 缺一条，下一轮请求会被模型服务端拒绝（400）。因此所有"容错"都必须是**产出错误内容的 tool 消息**，绝不能是"跳过该 call"：

| 情况 | 处理 |
|---|---|
| 工具名未知 | 产出 `role=tool` 提示文本，不中断整体流程 |
| 单轮 tool_call 数超 `maxToolCall` | 超出部分**仍须补齐** `role=tool` 的"已截断"错误消息 |
| 工具执行失败 | 转成 `is_error` 结果给模型看，而非向上抛传输失败 |
| 工具超时 / 被取消 | 同上，按 §5.4 错误分类标注 |

**进下一轮前必须断言**：`len(tool_calls) == len(tool_messages)` 且 id 集合相等。不等即为框架 bug，应 fail fast 而非带病进入下一轮。

#### 错误分类

工具错误统一分类，随 tool 消息回传，便于模型区分该重试还是该换路：

```
tool_not_found / timeout / cancelled / internal
```

#### MCP 动态接入

每轮按用户设置动态接入。工具改名避免与内置工具冲突：

```
mcp_plugin_{name}_{server}_{tool}
```

连续失败的 MCP 由 Template 节点注入 `plugin_status` 提示，让模型知道该插件当前不可用。

### 5.5 上下文压缩

每次调用模型**之前**跑 compactor 链。阈值一律表达为 `ContextLength` 的比例，不写绝对值 —— 换模型时只改 `ContextLength` 一处。

#### 两种触发模式

**默认模式**：

```
history_tokens > ContextLength * 0.9
```

**reserve 模式**（可选开关）—— 按剩余可用空间触发：

```
available <= clamp(ContextLength * (1 - 0.9), FLOOR, CAP)

FLOOR = MaxSummaryLength(4096) + Instructions(2048) + Margin
CAP   使 1M 级窗口在约 95% 处才触发
```

两种模式在中等窗口上等价；差别在两端：

| 窗口 | 默认模式触发点 | reserve 模式 |
|---|---|---|
| 小窗口 | `0.9 * L`，剩余空间可能不足以放下 summary + buffer | `FLOOR` 保证剩余空间**至少**够放 summary + instructions + margin |
| 1M 级 | `0.9 * L` 会白白空出 100k | `CAP` 把触发点推到约 95% |

`FLOOR` 的意义：小窗口下 10% 可能连 summary 都放不下，压缩完立刻又超阈值。`CAP` 的意义：大窗口下 10% 是巨大的浪费。两个 clamp 边界各自解决一端的问题。

#### 压缩后的空间预算

| 项 | 上限 |
|---|---|
| summary | 4096 tokens |
| buffer | 2048 tokens |
| skill | 单条 ≤ 5000，总计 < 25000 |

**压缩目标**：把 history 压回阈值线以下，**且**留出 summary + buffer 的空间。只满足前者不满足后者，会出现"刚压完就又触发"的抖动。

#### 单条消息截断

| 规则 | 值 |
|---|---|
| 单条 replay 超长即截断 | 8000 tokens |
| 截断保底保留 | 4000 tokens |

#### 不变量

压缩不得破坏 `tool_call` 与 `tool` 消息的配对关系 —— **压缩粒度以"完整的一轮"为单位**，不能把一轮从中间切开。切开的后果见 §5.4 的协议硬约束。

压缩发生时推送 `history_compaction` 事件（§5.9），让前端可见。

#### 待核对

`ContextLength * 0.9` 在 256k 窗口上算得 230.4k，但实测触发点被记为约 239k（相当于 0.934）。两者不一致，实现前需确认：是另有一个常量参与，还是 reserve 模式的 `CAP` 在此生效。**这个数不对齐，压缩会比预期晚触发约 9k tokens**，在接近窗口上限时足以导致请求被拒。

### 5.6 准入与内容审核

两层，**顺序固定：先准入，后审核**。

| 层 | 管什么 | 时机 | 结果 |
|---|---|---|---|
| **Admission** | 权限 —— 该场景允不允许调该工具 | 工具执行前 | `Allow` / `Confirm` / `Deny` |
| **Moderation（入参）** | 内容 —— 入参是否违规 | 准入通过后、执行前 | 放行 / 拦截 |
| **Moderation（回传）** | 内容 —— 产出的图片/文件是否违规 | 结果回传前 | 放行 / **替换为 blocked 图** |

顺序不能反：无权调用的工具，其入参根本不该被送去审核。

```go
type Decision int
const (
    Allow Decision = iota
    Confirm   // 挂起，等用户确认
    Deny      // 拒绝，理由作为 tool 消息回传模型
)
```

`Confirm` 推送 `tool_confirm_required` 事件挂起循环，等前端回传决定。**即便是 `Deny`，也必须产出对应的 `role=tool` 消息**（见 §5.4 协议约束）。

Admission 与 Moderation 均为强制层，场景可替换实现但不可关闭。

### 5.7 产出物落地

- 输出目录按约定路径组织
- portal fs 挂载，TOS 为后端存储
- 渲染时清洗占位符，**拒绝路径穿越**
- 凭证经创建请求注入，沙箱内 daemon 仅内存缓存，**不写入日志**

### 5.8 子 agent

`task` 工具在进程内起独立 ReAct 循环，最终文本作为 tool result 回主循环。

| 项 | 规则 |
|---|---|
| 嵌套深度 | 主 agent `depth=0`，子 agent `depth=1`；**`depth >= 2` 禁止嵌套** |
| 沙箱 | 独立沙箱：同 chatID + 不同 agentID（命名见 §3.1） |
| workspace | **共享同一 chat 的 TOS workspace** |
| 并发 | 最多 **4** |
| 回收 | 主循环通过 `wait_for_message` / `check_subagent_status` 收取结果 |
| maxRound | 默认 30 |

**共享 workspace 的写入隔离**：4 个子 agent 并发写同一 workspace 会互相覆盖。约定：

```
workspace/               ← 共享只读区，子 agent 可读
workspace/.agents/{agentID}/   ← 各子 agent 专属写入区
```

子 agent 默认写入自己的专属目录，主 agent 负责汇总。不做这个约定会出现"报告写了一半被另一个子 agent 覆盖"这类极难复现的问题。

### 5.9 事件流

推送给前端的事件类型：

| 事件 | 含义 |
|---|---|
| `start` / `end` | 一次请求的起止 |
| `model_text` | 模型文本增量 |
| `reasoning` | 推理内容增量 |
| `tool_detect` | 检测到 tool_call |
| `args_delta` | tool_call 参数增量 |
| `progress` | 工具执行进度 |
| `agent_start` / `agent_end` | 子 agent 起止 |
| `tool_confirm_required` | 准入判定为 `Confirm`，等待用户确认 |
| `history_compaction` | 触发了上下文压缩 |
| `steer_message` | 用户插话 |
| `block_revoke` / `message_revoke` | 撤回（§5.11） |

#### steer_message 的排队规则

用户插话**下轮生效，不中断当轮**。但落地受协议约束：当轮若正在执行工具，`assistant(tool_calls)` 之后**必须紧跟全部对应的 tool 消息**，插话不能插在中间。

规则：

```
插话到达 → 进待定队列
         → 等当轮所有 tool 消息回填完毕
         → append 到消息尾部
         → 进入下一轮
```

违反此顺序会破坏消息序列，下一轮请求直接被服务端拒绝。

### 5.10 场景装配

```go
type ScenarioConfig struct {
    Name        string
    Model       ModelConfig      // Primary + Fallbacks + Variants + AutoContinue + Timeout
    Prompt      PromptConfig     // system prompt / skill 注入
    Tools       ToolPolicy       // rule.yaml 裁剪结果 + 风险等级表 + 用户 MCP
    React       ReactConfig      // MaxRound（覆盖默认）、压缩阈值、压缩策略、超限收尾模式
    Sandbox     SandboxPolicy    // 模板、限额、网络策略、TTL
    Memory      MemoryOptions
    Concurrency ConcurrencyConfig // 会话级信号量上限、轮内并发上限
    ErrorPolicy ErrorPolicy      // 撤回档位
}

func (c *ScenarioConfig) Validate() error   // 启动时对所有 scenario 全量跑一遍
```

装配在入口层一次性完成，之后链路只读不改。配置错误在装配期暴露，而非第 8 轮才炸。

#### 三场景对照

| | 代码助手 | 数据分析 | 深度研究 |
|---|---|---|---|
| MaxRound（覆盖默认 50） | 40 | 15 | 60 |
| 会话级并发 | 5 | 3 | 8 |
| 沙箱模板 | 带 git / 编译器 | 带 python + 科学计算 | 最小 |
| 内存限额 | 1 GB | 4 GB | 512 MB |
| 网络策略 | 出站受限（仅包管理源） | 禁止出站 | 出站放开 |
| 沙箱 idle TTL | 60 min | 30 min | 15 min |
| 压缩阈值 | 高 | 中 | 低 |
| 压缩策略 | 保留最近 + 文件摘要 | 保留最近 | 摘要为主 |
| `ipython`（Admission） | Allow | Confirm | Deny |
| `web_search`（Admission） | Confirm | Deny | Allow |
| `browser` | 关 | 关 | 开 |
| 快照 | 开 | 开 | 关 |
| 超限收尾 | 无工具总结 | 报错 | 无工具总结 |
| 撤回档位 | RevokeMessage | RevokeMessage | RevokeBlock |

三列差异是行为质变而非参数微调：同一个二进制，三种安全模型。

### 5.11 撤回语义

流式输出按内容块推送：

```
event: block_start     {block_id, type}
event: block_delta     {block_id, delta}
event: block_end       {block_id}
event: block_revoke    {block_id}
event: message_revoke  {message_id}
```

| 档位 | 行为 |
|---|---|
| `RevokeBlock` | 只撤单块，允许静默恢复；后台重试成功即续推新块 |
| `RevokeMessage` | 整条作废，用户重发 |

**判据是该场景的工具副作用是否可逆**：深度研究的失败通常幂等（检索失败），重试无副作用；代码助手的失败可能已改动文件，静默重试会造成"用户以为没发生但已发生一半"。

---

## 6. 能力边界

明确声明，不假装覆盖。

| 项 | 本项目 | 生产级方案 |
|---|---|---|
| 隔离强度 | 容器级（namespace + cgroup），共享宿主内核 | microVM（独立内核 + VT-x） |
| 逃逸面 | 内核漏洞即逃逸 | 显著更小 |
| 适用租户模型 | **单租户可信场景** | 多租户不可信 |
| Hibernate | 冻结进程，不释放内存 | 内存落盘并释放 |
| Snapshot | 文件系统层 | 含内存与进程状态 |
| 浏览器 | 沙箱内 CDP，无独立隔离 | 独立浏览器沙箱 |
| 沙箱内用户 | 默认 root（rootless 为待定项） | 非特权用户 |

---

## 7. 验证方式

| 验证项 | 方法 | 产出证据 |
|---|---|---|
| 分布式正确性 | 8 进程并发对同一 chatID 调 ensure | 仅创建 1 个沙箱、无孤儿、无死锁 |
| 两级信号量 | 会话级限 3、轮内限 10，构造 20 个 tool_call | 无死锁，在途数始终 ≤ 3 |
| 消息配对 | 注入未知工具、超 maxToolCall、工具超时 | `tool_calls` 与 `tool` 消息数恒等，下一轮请求不被拒 |
| auto-continue 预算 | 构造持续返回 `length` 的 mock 模型 | 达 MaxContinues 后停止，标记截断 |
| tool_call 跨续写聚合 | 构造在 arguments 中途截断的 mock | 拼接结果为完整 JSON，非半截 |
| 回收正确性 | 注入 fork 炸弹、OOM、超时 | 限额生效、进程清零、cgroup 与挂载点无残留 |
| 一致性 | 强杀 provider 侧实例后重新 ensure | 自动解绑重建，workspace 完整，首个工具结果含 `sandbox was recreated` |
| 子 agent 写隔离 | 4 个子 agent 并发写同名文件 | 各写各的专属目录，无覆盖 |
| 冷启动 | 压测建箱延迟分布 | P50 / P99，预热池开关对比 |
| 并发容量 | 逐步加压 | 单机并发沙箱数上限、内存曲线 |
| 多场景 | 三场景各跑一条完整链路 | 同一二进制，三种安全姿态实测 |

**零外部依赖可演示**是硬要求：`Store` 默认内存实现，`go run . demo` 即可跑通，无需 Redis 与云账号。分布式语义由上表前两项的多进程测试保证。

---

## 8. 里程碑

| 阶段 | 内容 | 出口条件 |
|---|---|---|
| M1 | LocalProvider 最小可用 | 能建箱、执行命令、正确销毁，无残留 |
| M2 | 管控面单机版 | 命名复用、状态机、TTL、Reaper，MemStore |
| M3 | 管控面分布式版 | RedisStore、分布式锁、延迟队列、两级信号量；多进程并发测试通过 |
| M4 | ReAct 循环 | Eino graph 打通、模型层四层装配、ToolsNode 并发与容错、消息配对断言 |
| M5 | 工具层与子 agent | 双轨 binding、MCP 接入、准入与审核、子 agent 与写隔离 |
| M6 | 场景装配 | 三场景配置跑通，对照表实测 |
| M7 | 压测与文档 | 验证表全部有数据，README 含能力边界 |

---

## 9. 待定项

| # | 待定 | 影响 | 定夺时点 |
|---|---|---|---|
| 1 | ~~编排层自研 vs Eino~~ | **已定：用 Eino graph** | — |
| 2 | ~~Variant 分桶依据~~ | **已定：不做分桶**，koanf 分层使其延迟成本为零（§5.3） | — |
| 3 | Rust 的落点 | 目前设计中无必须项，可能不引入 | 按需 |
| 4 | 持久化存储后端（本地 FS / 对象存储） | 影响 workspace 与快照实现 | M1 |
| 5 | 维护任务选主实现 | 幂等已保证正确性，选主仅为优化 | M3 |
| 6 | TencentProvider 实现深度（骨架 vs 完整） | 取决于是否有 AGS 账号 | M5 |
| 7 | 沙箱内是否改 rootless | 影响隔离强度与兼容性 | M1 |
| 8 | 超限收尾"无工具总结"的 prompt 设计 | 影响截断时的输出质量 | M4 |
