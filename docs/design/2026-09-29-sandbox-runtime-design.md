# go-agentbox 设计稿：Agent 工具沙箱运行时

> 状态：设计稿 · 2026-09-29
> 范围：沙箱管控面 + Provider 层 + 框架外壳

---

## 1. 项目定位

**一句话**：给 Agent 用的工具沙箱运行时 —— 隔离执行、资源限额、自动回收、产物持久化；外挂一层可按场景装配的 ReAct 执行框架。

主体是沙箱运行时（约 70% 工作量与绝大部分技术深度），框架外壳只负责把沙箱放进一个真实的使用场景，并支撑"多场景可迁移"这个命题。

### 1.1 与既有方案的关系

| 方案 | 关系 |
|---|---|
| CloudWeGo Eino（字节，Go LLM/Agent 框架） | 对标物。本项目的编排层是薄外壳，不与其竞争通用图编排能力 |
| 腾讯云 AGS / Cube Sandbox | 对标物。Provider 接口按其能力面定义，并提供一个可选的 AGS provider |
| E2B | 接口语义参考。沙箱内 daemon + Connect 风格 API 的模型来自这里 |

**参考，不是依赖**。判据：关掉源码能否从头写出来并解释每个决定。

### 1.2 非目标

- 不实现通用 DAG 编排引擎
- 不实现 microVM 级隔离（见 §6 能力边界）
- 不实现进程级快照（CRIU）
- 第一版不支持浏览器沙箱

---

## 2. 整体架构

```
入口层（按 scenario 装配 workflow）
   │
   ▼
Template 节点 ──► General ReAct Agent ──► 流式输出
                      │
                      ▼
                  ToolNode
                      └─ 中间件链（洋葱）
                           └─ 工具执行
                                ├─ 纯函数工具 ──► 直接执行
                                └─ 副作用工具 ──► Sandbox Manager
                                                      │
                          ┌───────────────────────────┴──────────┐
                          │  管控面                               │
                          │  Naming / Locking / Lifecycle /       │
                          │  Pool / Reaper / Snapshot / Limiter   │
                          └───────────────┬───────────────────────┘
                                          │
                                   Provider 接口
                          ┌───────────────┴───────────────┐
                    LocalProvider                  TencentProvider
              （namespace + cgroup，零外部依赖）    （AGS 控制面 HTTP）
```

---

## 3. 管控面

### 3.1 命名与绑定

```
主沙箱      sb:{chatID}
子 agent    sb:{chatID}:{sha256(chatID:agentID)[:16]}
```

用 SHA256 截断而非直接拼 agentID：agentID 可能含非法字符或过长；定长便于 key 规划；不泄露业务标识。截 16 hex（64 bit），单会话内碰撞可忽略。

沙箱名的三重身份：**全局复用标识 / 分布式锁键 / 绑定记录键**。

绑定记录（Redis Hash）：

```
binding:{name} → { sandbox_id, provider, state, created_at, last_active_at, ttl_deadline }
```

放 Redis 而非 DB：每次工具调用都要 ensure，属热路径。绑定丢失不致命 —— 沙箱变孤儿，provider 侧 TTL 兜底回收，对账任务扫出。DB 作为最终一致的账本用于对账。

### 3.2 ensure 流程

```
ensure(ctx, chatID, agentID) → Sandbox

① name := buildName(chatID, agentID)

② 快路径：读 binding:{name}
     命中 && state ∈ {running, hibernated}
        → 探活
           活 → 续 TTL →（hibernated 则 resume）→ 返回
           死 → 落慢路径
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

否则：A 持锁超时自动释放 → B 拿锁 → A 完成后删掉了 B 的锁 → C 也拿到锁 → 两个持有者同时在临界区。

**watchdog 续期**必须绑定 context，业务结束立即停，避免"业务已完成、续期仍在跑"。

### 3.3 一致性模型

> **Provider 是真相源，binding 是缓存。**

Provider 才真正持有资源；binding 存在只为避免每次查 provider。因此快路径乐观信任 binding，**但执行前必须探活**，探活失败即以 provider 为准。

| 不一致情况 | 成因 | 处理 |
|---|---|---|
| binding 有 / provider 无 | 被 provider TTL 回收 | 解绑 + 重建 |
| binding 无 / provider 有 | 孤儿箱（绑定写失败或 Redis 丢数据） | 对账任务扫出后关闭 |
| 两边状态不同 | 例如 binding=running 实际 hibernated | 以 provider 为准，刷新 binding |

孤儿箱靠 `Provider.List(prefix)` + 统一命名前缀扫出。**命名规范是可对账性的前提，不是美观问题。**

### 3.4 状态机与三层 TTL

```
creating → running ⇄ hibernated → closed
              ↘      error      ↙
```

`hibernated` 仍视为可复用，ensure 时直接 resume。`closed / error / notfound` 一律解绑重建。

| TTL 层 | 作用 | 参考值 |
|---|---|---|
| 锁 TTL | 防持锁者崩溃死锁 | 30s + watchdog |
| 业务 TTL（binding） | 管控面据此回收 | 按场景，15–60 min idle |
| Provider TTL | 管控面整体故障时的最终保险 | 严格大于业务 TTL |

**Provider TTL 必须严格大于业务 TTL**。写反的后果是管控面以为箱还在、provider 早已回收，每次 ensure 都要重建，预热池形同虚设 —— 且表现为偶发变慢，极难定位。

续期时机：ensure 成功后、每次 Exec 完成后。

### 3.5 Reaper：幂等优先于选主

每秒运行的维护任务。多实例部署会撞车：同时扫到同一过期箱、同时打快照、同时 Close。

常规解法是 Redis 选主，但选主必然存在脑裂窗口。**正确性由幂等保证，选主只是优化**：

- `Close` 一个已关闭的箱返回成功，不报错
- `Snapshot` 带版本号，重复打直接跳过
- 删 binding 用 CAS（比对 sandbox_id）

回收走延迟队列：

```
Reaper 扫描 → 标记过期 → Redis ZSET（score = 执行时间）
                            ↓
       Worker：ZRANGEBYSCORE 0 now → 最终快照 → Close → 删 binding
```

三个理由：打快照耗时不能阻塞扫描；失败可重新入队带退避；批量过期时削峰，不打爆 provider。

多消费者从 ZSET 取任务须用 Lua 保证"取出 + 删除"原子。

### 3.6 会话信号量

普通计数器的致命问题：持有者崩溃后计数不归零，会话永久卡死。

改用 ZSET 记录 `持有者 token → 获取时间`，acquire 时先清理超时持有者：

```lua
-- KEYS[1]=sem:{chatID}   ARGV: now, timeout, limit, token
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[2])
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
    return 1
end
return 0
```

持有者崩溃自愈，不需要额外清理任务。超限任务在 DB 保持 `pending`，release 时通过 Pub/Sub 唤醒。

### 3.7 预热池与快照

**预热池**：后台补水位。池空时必须有并发创建上限，否则流量尖峰会惊群 —— 大量请求同时发现池空并同时 create，打爆 provider。

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
    Stdout  io.Writer          // 流式直出，接 SSE
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

`OOM` 单列，因为"被内存限额杀掉"与"命令自身失败"对 agent 是不同信号，应触发不同反应。

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

**overlayfs 是关键**：template 作为 lowerdir 只读共享，每实例仅有空 upper 层。建箱从"拷贝几十 MB"降为"mkdir 三个目录 + 一次 mount"，同时快照只需打包 upper 层。原理同 Docker 镜像分层。

#### 创建流程

```
① mkdir instances/{id}/{upper,work,merged}
② mount -t overlay overlay -o lowerdir=templates/default,upperdir=...,workdir=... merged
③ bind mount ws/{chatID} → merged/workspace
④ 建 cgroup /sys/fs/cgroup/agentbox/{id}/
       cpu.max / memory.max / pids.max
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

## 5. 框架外壳

### 5.1 沙箱接入点

沙箱不是工具，是工具的执行环境。ensure 发生在中间件链内，工具本身只做声明：

```go
type Tool interface {
    Name() string
    Schema() json.RawMessage
    NeedsSandbox() bool
    Invoke(ctx context.Context, args json.RawMessage) (any, error)
}

sb := sandbox.FromContext(ctx)   // 由中间件注入
```

好处：工具实现可单测，单测中注入 fake sandbox，无需真起 namespace。

### 5.2 中间件链

```go
type Handler func(ctx context.Context, call ToolCall) (ToolResult, error)
type Middleware func(next Handler) Handler
func (c *Chain) Use(name string, mw Middleware)   // 同名覆盖，异名追加
```

洋葱模型，执行顺序 = 注册顺序。内置：

| 序 | 中间件 | 职责 |
|---|---|---|
| 1 | Trace | 开 span，记录入参出参耗时，**不可关闭** |
| 2 | Admission | 准入判定，拒绝则直接返回 |
| 3 | Semaphore | 会话级并发信号量，超限转 pending |
| 4 | Timeout | 给 ctx 挂 deadline |
| 5 | SandboxBind | `NeedsSandbox` 的工具在此 ensure 并注入 context |
| 6 | Retry | 退避重试，**仅对幂等工具生效** |
| 7 | Truncate | 结果超长截断，防撑爆上下文 |

**覆盖语义**：同名中间件后注册者替换先注册者，槽位不变；异名追加至链尾。场景可替换内置实现，但不能打乱链路结构 —— 配置自由，不给编排自由。

### 5.3 准入控制

强制层，场景无权关闭。

```go
type Decision int
const (
    Allow Decision = iota
    Confirm                  // 挂起等用户确认
    Deny                     // 拒绝，理由回传模型
)

type Admission interface {
    Check(ctx context.Context, call ToolCall) (Decision, string)
}
```

`Confirm` 与流式协议配合：发出 `tool_confirm_required` 事件挂起循环，等前端回传决定。这是 human-in-the-loop 的落点。

### 5.4 场景装配

```go
type ScenarioConfig struct {
    Name        string
    Model       ModelConfig      // 主模型 + 降级链
    Prompt      PromptConfig     // system prompt / skill 注入
    Tools       ToolPolicy       // 白名单 + 风险等级表 + 用户 MCP
    React       ReactConfig      // MaxRound、压缩阈值、压缩策略
    Sandbox     SandboxPolicy    // 模板、限额、网络策略、TTL
    Memory      MemoryOptions
    ErrorPolicy ErrorPolicy
}

func (c *ScenarioConfig) Validate() error   // 启动时对所有 scenario 全量跑一遍
```

装配在入口层一次性完成，之后链路只读不改。配置错误在装配期暴露，而非第 8 轮才炸。

### 5.5 三场景对照

| | 代码助手 | 数据分析 | 深度研究 |
|---|---|---|---|
| MaxRound | 40 | 15 | 60 |
| 沙箱模板 | 带 git / 编译器 | 带 python + 科学计算 | 最小 |
| 内存限额 | 1 GB | 4 GB | 512 MB |
| 网络策略 | 出站受限（仅包管理源） | 禁止出站 | 出站放开 |
| 沙箱 idle TTL | 60 min | 30 min | 15 min |
| 压缩阈值 | 高 | 中 | 低 |
| 压缩策略 | 保留最近 + 文件摘要 | 保留最近 | 摘要为主 |
| shell 工具 | Allow | Confirm | Deny |
| 网络出站工具 | Confirm | Deny | Allow |
| 快照 | 开 | 开 | 关 |
| 异常策略 | RevokeMessage | RevokeMessage | RevokeBlock |

三列差异是行为质变而非参数微调：同一个二进制，三种安全模型。

### 5.6 流式协议与撤回语义

```
event: block_start     {block_id, type: "text" | "tool_call"}
event: block_delta     {block_id, delta}
event: block_end       {block_id}
event: block_revoke    {block_id}
event: message_revoke  {message_id}
event: tool_confirm_required {call_id, tool, args, reason}
```

| 策略 | 行为 |
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
| 浏览器沙箱 | 第一版不支持 | 支持 |

---

## 7. 验证方式

| 验证项 | 方法 | 产出证据 |
|---|---|---|
| 分布式正确性 | 8 进程并发对同一 chatID 调 ensure | 断言仅创建 1 个沙箱、无孤儿、无死锁 |
| 回收正确性 | 注入 fork 炸弹、OOM、超时 | 限额生效、进程清零、cgroup 与挂载点无残留 |
| 一致性 | 强杀 provider 侧实例后重新 ensure | 自动解绑重建，workspace 文件完整 |
| 冷启动 | 压测建箱延迟分布 | P50 / P99，预热池开关对比 |
| 并发容量 | 逐步加压 | 单机并发沙箱数上限、内存曲线 |
| 多场景 | 三场景各跑一条完整链路 | 同一二进制，三种安全姿态实测 |

**零外部依赖可演示**是硬要求：`Store` 接口默认内存实现，`go run . demo` 即可跑通，无需 Redis 与云账号。分布式语义由上表第一项的多进程测试保证。

---

## 8. 里程碑

| 阶段 | 内容 | 出口条件 |
|---|---|---|
| M1 | LocalProvider 最小可用 | 能建箱、执行命令、正确销毁，无残留 |
| M2 | 管控面单机版 | 命名复用、状态机、TTL、Reaper，内存 Store |
| M3 | 管控面分布式版 | Redis Store、分布式锁、延迟队列、信号量；多进程并发测试通过 |
| M4 | 框架外壳 | ReAct 循环、中间件链、准入控制打通 |
| M5 | 场景装配 | 三场景配置跑通，对照表实测 |
| M6 | 压测与文档 | 验证表全部有数据，README 含能力边界 |

---

## 9. 待定项

| # | 待定 | 影响 |
|---|---|---|
| 1 | 编排层自研 vs 直接用 Eino | 代码量与叙事，M4 前需定 |
| 2 | Rust 的落点（是否需要） | 目前设计中无必须项，可能不引入 |
| 3 | 持久化存储后端（本地 FS / 对象存储） | 影响 workspace 与快照实现 |
| 4 | 维护任务的选主实现（是否必要） | 幂等已保证正确性，选主仅为优化 |
| 5 | TencentProvider 实现深度（骨架 vs 完整） | 取决于是否有 AGS 账号 |
