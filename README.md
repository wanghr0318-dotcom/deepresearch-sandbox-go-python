# go-agentbox

单执行主机上的 Agent Runtime：Go 负责执行与权限边界，Python 负责研究编排。

> **状态：开发中（v0.2 内部里程碑 M1）。** 控制面（调度、恢复、API、CLI）已在测试环境中端到端验证，但**尚不能作为服务运行任务**：`agentbox server` 在沙箱生产启动器完成前拒绝启动。下文"已实现"只列出已经通过验收的部分；"这是什么"描述的是设计目标，每项保证在对应验收通过前都不成立。

---

## 当前状态

| 部分 | 状态 | 说明 |
|---|---|---|
| Worker 协议 v1（task 模式）与 Python Worker SDK、sim-worker（Plan 3） | **已验收** | Go 侧 `internal/protocol` 与 Python 侧 `worker/agentbox_worker` 共用 `protocol/fixtures` |
| PostgreSQL 持久化、BlobStore、安装身份引导（Plan 4） | **已验收** | `internal/persistence`、`internal/blob`、`internal/datadir`、`internal/ownership`；事务语义只在真实 PostgreSQL 上测试 |
| `internal/runtime` 改名为 `internal/sandbox`（Plan 1A） | **已验收** | 纯改名 |
| 控制面：准入、资源 coordinator 与清理、AttemptRunner、task actor 与调度（Plan 5） | **已验收**（控制面层） | `internal/admission`、`resource`、`runner`、`task`；E1、E4、E7、E8、E10 以进程型 fake provider 与真实 sim_worker 验证 |
| 启动恢复、REST/SSE API、CLI、不变量检查、故障注入（Plan 6） | **已验收**（控制面层） | `internal/reconcile`、`recovery`、`api`、`cli`、`app`、`invariants`、`faultinject`；E5、E6、E13–E16 |
| Provider 契约、cgroup 扩展、宿主自检、init 服务循环、环境生命周期（Plan 2 Task 1–8） | 已实现，所在计划未验收 | `internal/provider`、`provider/local`（以测试启动器验证）、`sandbox`、`cgroup`、`hostcheck`；`agentbox doctor` |
| 沙箱生产启动器（Plan 1B 结论 + Plan 2 后续任务） | spike 已完成，待审阅 | WSL2 上 §16.2 全部通过；规格 §4.6 回写提案待审阅，之后实现 |
| Gateway、DeepResearch、Redis、Vue、会话、exec 沙箱、sub-run | 未开始 | M2–M4 |

## 现在可以运行的命令

Go、Docker 与 PostgreSQL 命令已在 WSL2（内核 6.6）上验证；Python 测试已在 Windows（uv）与 CI 的 Linux 作业上验证。没有列出的命令尚不能使用：`agentbox server` 在生产启动器完成前拒绝启动，因此 `agentbox task …`、`agentbox status` 与 `verify-invariants` 还没有可连接的服务。

**依赖**：Linux（cgroup v2）或 Windows 下的 WSL2；Go 1.23+；Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)；Docker（运行 PostgreSQL）。

```bash
stat -fc %T /sys/fs/cgroup            # 应输出 cgroup2fs
```

**Go 单元测试**（不需要数据库；需要 root 的用例在非 root 下跳过）：

```bash
go vet ./...
go test ./...
```

**PostgreSQL 与数据库测试**（事务语义测试只在真实 PostgreSQL 上运行）：

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
export AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable'
go test ./internal/persistence/postgres/
docker compose -f deploy/docker-compose.yml down
```

`CI=true` 时未设置 `AGENTBOX_TEST_DATABASE_URL` 会使数据库测试失败而不是跳过（CI 即如此运行）。

**控制面端到端测试**（需要上面的 PostgreSQL 与 `python3`；以 `app.Run` 装配全部组件，用进程型测试 provider 运行真实的 `sim_worker`，覆盖崩溃重启恢复、取消竞争、数据库断连等；约 2 分钟）：

```bash
CI=true go test -count=1 ./tests/e2e/...
```

**需要 root 的测试**（cgroup、环境生命周期等）：以 root 运行 `go test ./internal/cgroup/ ./internal/rootfs/ ./internal/sandbox/ ./internal/provider/local/`（WSL 中可用 `wsl -u root`）。

**宿主自检**（需要 root）：

```bash
go build -o bin/agentbox ./cmd/agentbox
sudo ./bin/agentbox doctor             # 通过时输出"宿主环境检查通过"
```

**Python Worker SDK 测试**：

```bash
cd worker
uv run pytest -q
```

## 文档

| 文档 | 内容 |
|---|---|
| [路线图](docs/design/2026-10-03-roadmap.md) | v0.2 首发（M1–M4）→ v0.3 多节点 → v0.4 MicroVM；发布门槛 |
| [v0.2 首发设计规格](docs/design/2026-10-03-v0.2-first-release-design.md) | 架构、协议、数据模型、Gateway、会话、隔离、验证与发布门槛 |
| [代码组织设计](docs/design/2026-10-03-code-organization.md) | 模块职责、允许依赖、事务用例、所有权、测试布局 |
| [M1 计划索引](docs/plans/2026-10-03-m1-index.md) | M1 各计划、依赖与验收归属 |
| [持久化设计](docs/design/2026-10-04-m1-4-persistence-design.md)、[安装身份修订](docs/design/2026-10-05-installation-identity-amendment.md) | Plan 4 的设计依据 |
| [Provider 契约](docs/design/2026-10-05-provider-contract.md) | 环境生命周期的 Go 接口、错误、并发边界 |
| [Plan 2](docs/plans/2026-10-05-m1-2-local-provider.md)、[Plan 5](docs/plans/2026-10-05-m1-5-control-plane.md)、[Plan 6](docs/plans/2026-10-05-m1-6-recovery-entry.md) | M1 第 2 批计划、执行中修订与验收记录 |
| [REST API（OpenAPI）](api/openapi.yaml) | 任务提交、控制、查询与 SSE 事件流的契约 |
| [Plan 1B spike 记录](docs/experiments/2026-10-05-spike-1b.md)、[§4.6 回写提案](docs/design/2026-10-05-spec-4.6-writeback-proposal.md) | 降权启动序列的实验结论（待审阅） |
| [Worker 协议](protocol/README.md) | 协议 v1 的语义与 fixtures |

历史文档（不可直接执行）：[2026-09-29 设计稿](docs/design/2026-09-29-sandbox-runtime-design.md)、[v0.1 规格](docs/design/2026-10-03-v0.1-reliable-execution-design.md)、[旧 M1 计划](docs/plans/2026-09-29-m1-local-provider.md)。

## 这是什么（设计目标）

```
CLI / Vue ── REST + SSE ──► Go 控制面（任务/会话状态机、准入、事件续传、checkpoint、恢复）
                              ├─ Gateway：凭据隔离、调用 journal、预算、撤销、出网控制、exec 调度
                              ├─ PostgreSQL：唯一元数据存储      Redis：仅缓存
                              └─ Go Runtime：namespace + cgroup + user namespace + seccomp
                                     │
                    ┌────────────────┴────────────────┐
           编排环境：Python Worker（可信）         exec 环境：模型生成的代码
           经 Unix socket 访问 Gateway             无 Gateway 入口、无网络
```

- 长任务 Worker 故障后，从宿主已提交的 checkpoint 恢复。
- 供应商 API Key 不进入沙箱；所有外部访问经 Gateway 记录、计费、可撤销。
- 会话可冻结、驱逐并从已提交状态重建。
- 同一任务内的受限并行子研究共享预算、可取消。
- DeepResearch 与 sim-worker 通过同一套版本化协议接入。

## 能力边界（v0.2 设计）

| 项 | v0.2 |
|---|---|
| 隔离强度 | 容器级，共享宿主内核；权限边界测试是回归测试，不是无逃逸证明 |
| 编排 Worker | 可信平台代码；生成代码在独立 exec 沙箱中运行 |
| 外部调用 | 不保证 exactly-once；结果未知按全额预留保守计费 |
| 预算 | 保守估算，可出现赤字，不是严格上限 |
| 部署 | 单执行主机，无跨主机故障接管 |
| 冻结 | 保留进程与内存；驱逐才释放 |
| Workspace | 不随 checkpoint 回滚 |

完整清单见规格第 1.3 节与第 20 节。

## 参考与致谢

参考其设计，代码自行实现：

- [runc](https://github.com/opencontainers/runc)：re-exec init、降权与 `/proc` 掩蔽
- [Tencent Cloud Cube Sandbox](https://github.com/tencentcloud/CubeSandbox)：Agent 沙箱能力面
- E2B：沙箱内 daemon 与接口语义

## License

待定（v0.2 发布门槛包含 LICENSE、CONTRIBUTING、SECURITY 与版本说明）。
