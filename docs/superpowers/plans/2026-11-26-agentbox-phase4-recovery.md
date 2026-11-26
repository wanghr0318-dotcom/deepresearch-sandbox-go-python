# 阶段 4 实施计划 · 执行节点失联后的 Agent 恢复

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标**　在 B 已提交一次工具结果后使整台 B 离线，由 A 从已提交版本续跑同一任务；让仍持有旧凭据的 B 恢复连接，验证所有旧写入被拒绝。

**架构**　单控制面，K3s server、PostgreSQL、Gateway 保留在 A，B 只运行 K3s agent。COS 保存不可变快照，PG 是任务归属和已提交恢复点的唯一裁决者。新 Pod 复用现有 Gateway relay，领取新 attempt 的授权。

**技术栈**　Go、Python chatagent、K3s、PostgreSQL、腾讯云 CVM/COS。计划基于 `c6cd0c9`。

## 范围及当前证据

- [x] A 同节点编排 Pod 重建通过 4/4 次；每次独立评分 4/4。第 4 次恢复前后 6 条消息的摘要一致。[证据](../../evidence/2026-11-26-phase4-cvm-a/README.md)
- [ ] A+B 整机失联、存活旧进程迟到写入、COS 中断恢复分别取证。B 的实际节点身份及 COS 权限在执行时核对。
- GPU 保持关机。CPU 确定性回包只替代模型决策；使用真实 chatagent、Gateway、K3s、PG、COS。真实模型恢复安排 1 次必跑、1 次条件补跑，单列 GPU 窗口，阶段 5 的扩容及缓存对照另行开展。
- 继续使用无 hostPath 的独立执行 Pod。阶段 3 的 DeepSearch 三轮业务语料用于研究质量验收；本阶段固定故障位置，评价文件、消息、预算、调用序号和外部副作用的一致性。

## 三个 subagent 的写入边界

| 所有者 | 独占代码与测试 | 向其他人交付 | 权限边界 |
|---|---|---|---|
| 1 · 快照与工作区 | `internal/blob/`、`internal/gateway/workspace/`；COS 依赖若确需新增，由 1 独占 `go.mod` 和 `go.sum` | 从一个一致的工作区版本生成不可变 manifest；上传并校验后返回摘要；按指定摘要恢复，校验失败不发布 | 不更新 PG head，不实现租约，不接触云节点生命周期 |
| 2 · 归属与接管 | `internal/persistence/postgres/` 含迁移、`internal/task/`、`internal/runner/`、`internal/session/`、`internal/recovery/`、`internal/gateway/call/`、必要的 `internal/resource/` | PG 原子提交、恢复时选择 checkpoint、旧 attempt 拒写、未知调用的裁决与接管判据 | 不写 COS 客户端，不复制 Python 的消息序列化，不操作集群 |
| 3 · Pod 入口与云验收 | `internal/provider/k8s/`、`eval/phase4/`、本阶段云端原始证据 | 私网调度和新旧 relay 凭据的实测、故障驱动、按轮时间线和清理结果 | 唯一云故障操作人；不改 PG/COS 语义，不删除共享 namespace、PG 或 A 控制面 |

**主 agent 独占集成**：`internal/app/`、`cmd/agentbox/`、`internal/gateway/edge/` 的装配、必要的跨包接口变更、报告、合并和发布。现有 `scripts/run-cloud-agent-csv.py`、`worker/chatagent/` 原则上复用。任何人发现必须改另一方文件，提交调用点及失败用例，由文件所有者落实；每个测试只写在负责该断言的一层。

## 执行顺序与交付

### 0 · 固定接口和实验环境

- [ ] 主 agent 对照 `internal/gateway/workspace/state.go`、`internal/persistence/postgres/runner.go` 的 checkpoint 事务、`internal/provider/k8s/gatewayrelay.go` 和 `eval/phase4/rebuild.py`，画出一次写文件、保存消息、提交 checkpoint 的调用顺序。记录旧 checkpoint 无 manifest 时的读取路径。
- [ ] 约定 1→2 只传已校验的 manifest SHA-256；2→恢复执行者交付同一 checkpoint 的 inline state、manifest 摘要、新 attempt ID。控制面用 task ID + current attempt 校验发布权。共享字段由主 agent 一次接线。
- [ ] 1、2 独立开工。3 可同时整理 A 的现有回归并准备 B。B 首选匹配 A 的上海同 VPC、Ubuntu 24.04 x86_64、SA9.LARGE8、80 GiB SSD；先核对实例 ID、内网 IP、SSH 主机身份。COS 采用同地域实验前缀，凭据留在 A，日志只保存对象摘要和校验结果。

### 1 · 快照可移植且上传失败可回退（负责人 1）

- [ ] 在 `internal/gateway/workspace/manager_test.go` 构造版本 V1，含普通文件、空文件和可执行文件，再把一个文件改成 V2。在空目录恢复 V1，断言内容、权限、版本及工具去重记录与原版本相同。对路径、文件 SHA-256、字节数和执行位使用已有 workspace 表示。
- [ ] 固定同一逻辑版本的规范编码；先持久化引用的内容对象，核对大小与 SHA-256，再上传 manifest。只返回完整版本的摘要。下载按摘要校验后才对当前 attempt 可见。保留原有本机工作区作为热路径。
- [ ] 注入内容上传半途断开、manifest 已上传但事务未提交、读取时内容摘要不符。分别断言 PG 仍指向 V1、恢复只见完整 V1、损坏版本返回明确错误。COS 的 ETag 不作为 SHA-256 的替代。
- [ ] 运行 `go test ./internal/blob ./internal/gateway/workspace -count=1`；用真实 COS 把 V1 恢复进空目录并运行相同清单核对。将对象大小、摘要、manifest 及失败注入点交给 2、3。提交本任务代码与测试。

### 2 · PG 条件提交、租约与结果未知（负责人 2）

- [ ] 先在独立 PG 测试库写并发测试。旧 attempt X 保存 C1；X 过期后两个接管请求竞争，只允许一个获得 Y；Y 从 C1 继续，X 即使抢先恢复网络也无法续租或发布。
- [ ] 在已有 checkpoint 事务内绑定工作区摘要、消息、步骤、工具调用号和预算；版本迁移兼容历史记录。租期与续租使用数据库时间。检查并更新归属、head、checkpoint 和最终结果时持有同一事务保护；条件写影响行数必须为一。用同请求重复提交验证已有幂等语义。
- [ ] 审计所有能改变文件可见版本的路径，尤其是 workspace write 和 exec 回传。旧进程可以留下未引用的内容对象，但不能覆盖新 attempt 的 head。主 agent 在 Gateway 的接入和提交处接好身份检查，覆盖“授权后、提交前已经换主”的竞争窗口。
- [ ] 复用调用 journal 走三种分支：已完成返回已记结果；确认未发出可以调度；已发出且结果未知的非幂等动作进入核对或停止。用带计数的受控服务模拟“副作用已发生但响应丢失”，断言外部计数为 1。仅在持久化 fencing 已生效且旧进程无法再发布时允许绕过等待旧节点确认停止；补测租期内短暂失联不会提前接管。
- [ ] 在独立库配置 `AGENTBOX_TEST_DATABASE_URL`，执行 `go test ./internal/persistence/postgres ./internal/task ./internal/runner ./internal/session ./internal/recovery ./internal/gateway/call -count=1 -v`，核对上述 PG 用例实际运行。记录迁移前后恢复、并发接管胜者、拒绝原因和最终 head，提交代码与测试。

### 3 · 正常任务在 B 工作，迁移后入口可重绑（负责人 3）

- [ ] 将 B 作为 agent 加入 A，导入与 A 相同 digest 的 worker。执行 `sudo k3s kubectl get nodes -o wide`，预期两个不同内网 IP 均 Ready。用本次实验独立 namespace 的 Pod 规格/节点亲和性定向，避免更改其他负载的调度。
- [ ] 在 B 做一次无故障运行，用 Pod UID、nodeName、attempt ID 和成功工具调用证明编排、relay、执行链路确实工作。沿用 `eval/phase4/rebuild.py` 的提交后停点及已有独立评分器。
- [ ] 单测入口重绑：新 Pod 从私网连接 A，使用新授权完成一次工具调用；用旧身份请求同一入口，保存拒绝结果和未变化的 head；覆盖启用预热时的身份绑定。命令 `go test ./internal/provider/k8s -count=1` 只用于代码回归，真实证据单独登记。
- [ ] 排练失联注入与回滚。保留 B 的独立管理通道；网络分区只隔离 B 的实验数据链路，设置可自动恢复的短窗口。每次检查目标实例 ID、任务 ID 和 namespace，A 始终承担控制面。由 3 串行执行云端故障，杜绝两轮交叉。

### 4 · 合流与故障矩阵（主 agent 审核，3 操作）

先合入 1、2，再接线并复跑 A 单节点回归。进入整机实验前，必须能从空工作区仅凭 PG/COS 恢复，无需旧节点文件；用新目录排除误用 A 旧缓存。每次新建独立任务。历史 run-01 和 run-04 作为两条确定性基线（后者含完整消息摘要），run-02、03 保留为同点重复记录。后续两个 CPU 实验名额优先覆盖不同时间点：C2 内容上传被中断、C2 所有对象已校验但 PG 提交前中断。两次均预先提交 C1，恢复后逐项校验只见 C1。每个新场景先取一条可复核轨迹；断机接管、分区迟到写入及结果未知分别验收。复测用于定位不稳定性，单次观测只报告原值。

| 注入点 | 故障动作 | 必须同时观察到 |
|---|---|---|
| B 写完文件、C1 已在 PG 提交 | 核对 B 实例后关机；A 保持服务 | 原 B 上的 Pod/attempt 与 A 上新 Pod/attempt 不同；同一 task、C1；文件与完整消息摘要相同、预算与调用号延续；下一次工具成功；独立判分 4/4 |
| B 仍运行且持有旧身份 | 隔离 B，等 PG 确认换主后恢复网络；从仍存活的 X 主动请求续租、提交 checkpoint、发布文件 head 和最终结果 | 四类旧写均被拒绝；新 head 和结果保持；记录旧 relay 请求的响应及实际尝试数。若旧进程已死，本轮不能证明迟到拒写，重新注入 |
| 保存 C2 期间 | 分别中断对象上传，以及在对象已完整、PG 提交前停止发布者 | 新 attempt 只见完整 C1；无半文件；原 journal 的已完成调用不重复；保存 COS 与 PG 时间线 |
| 工具副作用返回前 | 受控服务已登记一次调用，切断回包后使执行者失联 | 出现“结果未知”；自动恢复不产生第二次外部副作用；保留核对/停止记录 |

如 B 长期 NotReady 而旧 Pod 仍绑定，保留实际检测与停顿时间，回到任务 2 修正接管判据。手动删除旧 Pod 另记为人工干预，不能计为整机自动恢复成功。

### 真实模型窗口（CPU 跨机自动接管和拒写均通过后开启）

- [ ] M1 必跑一次。单卡自托管 Qwen3-8B，经腾讯云 Gateway，关闭模型回退。沿用 CSV 题，以实际工具成功事件和已提交 checkpoint 为注入条件，在 B 离线后观察 A 恢复。模型继续自主选择工具；保存恢复前后的请求历史、实际模型和上游、下一次成功工具调用、最终独立判分。模型没走到注入点记为未注入；已注入但未恢复计入恢复失败。
- [ ] M2 按条件补一次。若 M1 没有得到可核验的真实恢复，修正原因后优先复跑 M1，保留首轮。若 M1 已通过，且 DeepSearch 真实检索可用、同会话三轮语料已能正常完成，在第二轮首次成功取证并提交 checkpoint 后注入故障，验证恢复后继续取证、第三轮能沿用同一研究对象和来源。记录研究评分与引用核验。最多安排两次真实模型尝试；不足以验收时如实保留缺口。
- [ ] B 按量租用从双机联调开始：先完成 A 上 PG/COS 空目录恢复、租约并发接管及旧写拒绝，再开同 VPC 的 B，验证正常任务落 B。真实双节点容错在阶段 4 完成；A 与 A+B 固定负载的吞吐和成本对照留到阶段 5。

### 5 · 验收证据与阶段出口（主 agent）

- [ ] 每轮归档 `fault.json`、`events.sse`、`solve-inspect.json`、`grade-inspect.json` 及脱敏后的 PG/COS 摘要、两台节点和 Pod 的身份。记录 commit、镜像 digest、K3s 版本、租约参数、时钟来源和回滚结果。沿用 A 的证据格式。
- [ ] 用 A 控制面同一时钟记录注入、检测、换主、新 Pod Ready、下一次有效工具调用和成功返回。分别计算检测与业务恢复延迟；同时列出重做步骤、重复外部调用、旧写尝试/拒绝数，以及 B 恢复后孤儿 Pod 和授权残留。逐轮记录提交数、触发注入数、恢复成功数。恢复率分母为实际注入数，注入后超时计失败；注入前超时单列。另列每个候选的评分用例通过数/已执行用例数，未产生候选记为未评分。消息恢复与下一次有效工具调用成功即可判定运行时续跑，候选做错单独报告。预期安全停止的未知副作用实验按安全性断言计数。只在同配置同故障点的重复实验内汇总延迟。
- [ ] 每个 agent 按独立通过的测试提交一次；主 agent 核对无重复实现、无凭据，审阅失败轮与清理记录，更新总计划勾选项并推送 GitHub。CPU 故障工程验收与真实模型补验分别标记。只有后续另开 GPU 并留下真实模型轨迹，才勾选总计划相应条目。
