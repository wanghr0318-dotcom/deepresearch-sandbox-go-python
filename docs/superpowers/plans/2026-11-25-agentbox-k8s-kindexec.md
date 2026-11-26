# Kubernetes KindExec Implementation Plan

> 使用 Superpowers `executing-plans` 按任务推进。每项先锁定契约，再在腾讯云验收。

**目标**　在腾讯云 CVM A 的单节点 Kubernetes 上跑通 Gateway 到独立执行 Pod，用现有 CSV 修复题验证输出和清理。

**边界**　阶段 1 的编排 Pod 和执行 Pod 都要求零 `hostPath`。首次集成测试直接在云上运行。题库扩充留给阶段 3，SGLang 留给阶段 2，跨节点租约和 COS 恢复留给阶段 4。阶段 1 预计 **1–2 周**。

## 数据与连接设计

| 边界 | 阶段 1 实现 |
|---|---|
| 执行输入与输出 | 每次调用一个 Pod，使用独立 `emptyDir`。Gateway 暂存目录由一处窄接口管理，保留当前 Create → Stage → Start 的调用顺序。K8s 实现通过 API server 的容器通信通道将已暂存输入流式写入 Pod，然后启动命令。命令容器只读挂载输入，使用独立输出卷。可信 helper 回收输出并将其交给 Gateway 的 BlobStore。 |
| 编排工作区与 Gateway | 将编排工作区映射到 Pod 自有卷。Python 仍通过 Unix socket 调用 Gateway，socket 由同 Pod 的代理与 Agent 共享 `emptyDir`。代理经私网认证转发，身份绑定 task 和 attempt。Gateway 在入口验证身份和归属。退役路径和跨节点恢复时分别验证旧身份撤销、新身份签发。 |
| 控制面权限 | Gateway 侧持有测试命名空间的必要 Pod 与容器通信权限。用户命令所在容器只拿本次输入与输出卷，关闭服务账户令牌自动挂载。代理凭据只挂给编排代理。 |
| 持久化 | 阶段 1 的 Pod 卷随 Pod 生命周期释放。工具返回后由 Gateway 登记产物。阶段 4 再将已提交 checkpoint 与工作区清单绑定到可跨节点读取的 COS 产物。 |

当前 `EnvInfo.InDir` 是 Gateway 进程本地的暂存目录，现有 `exec.go` 先 Create、再暂存、最后 Start。**允许 Gateway 本地暂存，Pod 通过 API 通道接收内容。** 编排 Pod 的历史挂载来自 `internal/provider/k8s/slots.go` 和 `pod.go`，因此完整验收要覆盖这条入口。初次实现先验证 API 通道可承载有界输入、标准输出及产物，并明确 input-ready 确认与超时。两阶段传输失败采用同一清理流程。

## CVM A 预检记录

- K3s v1.34.13+k3s1 节点 Ready。腾讯云内网镜像加速地址可拉取固定摘要的 BusyBox 和 Python 镜像。
- 文件探针通过 Kubernetes API 向非 root Pod 的 emptyDir 写入、读回固定字符串。删除测试 Pod 后确认无残留。
- 身份探针使用临时 Python Gateway 和 Pod 内代理。Agent 经 Unix socket 与私网 Service 发请求，正确 attempt 得到 HTTP 200，使用有效凭据但 attempt 错误得到 HTTP 403。两个 Pod 的 hostPath 数均为 0。测试 Pod、Service、ConfigMap 和 Secret 已清理。
- 身份探针走集群内 HTTP，验收范围是连接与身份判断。实际 Go Gateway 的认证传输、凭据生命周期、失败回收和 Provider 契约仍待实现及验收。

2026-11-26 用仓库源码编译 Linux podagent，在 CVM A 的双容器测试 Pod 中通过 Kubernetes API 把归档交给传输容器。stage-input 写入共享 emptyDir，非 root 沙箱从只读挂载读回相同内容。该探针覆盖实际 helper，生产 Provider 的 Create、Start、Stop 和 OpenOutputs 接线仍待验收。

2026-11-26 真实 Provider 契约在 CVM A 用镜像 `sha256:675ba2c270d747ef488295764ce8509d30b1f69dc752adc5e2e78acc89dfdc7a` 跑过 17 个通过事件，零跳过，耗时 21.06 秒。覆盖输入传输、同名工作区隔离、退出码 0/7、输出配额及清理。原始记录为 `E:/agentbox-bootstrap/kindexec-real-contract-final.jsonl`。

2026-11-26 在腾讯云 CVM A（SA9.LARGE8，4 vCPU、8 GiB）通过 CLI 提交 CSV 修复任务。Go Gateway 由命名空间范围的 ServiceAccount 管理 Pod。模拟 Worker 经同 Pod 的 Unix socket 与私网 TLS 代理调用 Gateway，后者为四次命令分别创建独立执行 Pod。Kubernetes 实际对象记录了 1 个编排 Pod、4 个执行 Pod、0 个 `hostPath`。基线、正确补丁、错误补丁退出码为 7 / 0 / 7，命令耗时为 34 / 35 / 36 ms。墙钟超时在 400 ms 触发，结果登记为 `timed_out`。正常下载接口返回报告，SHA-256 为 `b3d6c680c5943635ecb5ecf9ecabc83d4ec2c0418ccbe13e6ed30278070edb25`。收尾完成后，运行时 Pod 和代理 Secret 均为 0。

本轮任务 ID 为 `task_e5e11a3a21e551c20ff27d5081757a57`，Worker 镜像为 `sha256:a8fe9d08f0e7c376444769e8f84611d4ef223a7955ee3908e63c0779f9083353`。复跑命令、输入和证据分别在 `E:/agentbox-bootstrap/verify-cvm-csv.sh`、`E:/agentbox-bootstrap/cvm-csv-spec.json`、`E:/agentbox-bootstrap/cvm-csv-evidence/`。脚本使用专用命名空间 `agentbox-cvm-acceptance`。旧命名空间的准入规则只允许 emptyDir 和指定的 hostPath，部署私网代理时应使用专用命名空间及对应 RBAC。工具退出后无法从该容器读到实际 CPU 时间，当前账本按预留值结算，阶段 3 的 CPU 指标需要单独实测。上述结果来自脚本化 Worker，模型工具调用和 GPU 指标属于阶段 2。

超时请求从 Gateway 收到请求到返回共 2,002 ms，包含启动、执行、停止及结果收集。Gateway 记账后约 161 ms 的首个集群采样已经找不到该执行 Pod。采样只提供观测上界，不能据此推算命令被杀死到 Pod 删除的精确耗时。

同一 Worker 镜像和最新 Provider 再跑真实契约，测试耗时 21.46 秒，17 项通过、0 跳过，测试命名空间没有残留的受管 Pod。记录见 `E:/agentbox-bootstrap/kindexec-real-contract-scoped-image.jsonl`。Linux 的 Gateway 调用包测试也已通过。专用严格验收脚本已提供，GitHub Actions 的普通 CI 尚未接入真实 CVM 任务。

## Task 1　统一 Pod 规格、控制通道和清理

- [ ] 在 `internal/provider/k8s/pod.go` 复用现有规格构建逻辑，按 Kind 提供镜像、卷、启动方式和资源差异。共用 SecurityContext 与资源默认值。为 CPU、内存设置 requests/limits，并为临时卷配置容量预算和执行时限。
- [ ] 在 `internal/provider/k8s/provider.go` 及连接模块接入上述无宿主挂载通道。先做真实集群探针，验证可信端写入 Pod、Agent 容器可访问本 Pod 的 socket，以及 Gateway 拒绝错误 attempt 身份。
- [ ] 将临时目录、Pod 和绑定记录的收尾集中到一个带归属检查的幂等入口。删除前核对 installation、环境身份和 Pod UID。超时、取消、创建失败及重试均复用。Pod 已不存在视为完成，残留可在重试中回收。
- [ ] 在 `cmd/agentbox/server.go` 增加 K8s exec 的配置入口，登记实际执行镜像 digest。继续复用 `internal/app/exec.go` 和 Gateway 的调用顺序。

## Task 2　复用输出契约和测试矩阵

保留 Gateway 的 Stop → OpenOutputs → Destroy 顺序。KindExec 的 Stop 必须依据 Kubernetes API 状态确认沙箱容器已终止，同时让只读输出的传输容器继续存活。Destroy 按 Pod UID 校验归属并清理。如果在时限内无法确认沙箱已停止，应拒绝发布产物并强制回收。

- [ ] 以 `internal/provider/local/outputs.go` 为准核对输出行为。其枚举依赖本地文件描述符和 Linux `openat2`，直接复用整方法需要同等文件系统条件。将可共享的校验和限额规则放在共同入口，两种 Provider 沿用同一结果口径。可信 helper 必须安全打开输出，跳过符号链接和非普通文件，在传输层限制文件数和总字节数。
- [x] 扩展 `internal/provider/providertest` 的同一套表驱动契约。由现有 `TestConformanceOnFakeCluster` 和 `TestConformanceOnRealCluster` 分别提供环境，覆盖退出 0、退出 7、停止、同名文件并发隔离、输出上限与重复清理。墙钟超时由真实 Gateway 端到端用例覆盖。
- [ ] 用 `go test ./internal/provider/k8s -run '^TestConformanceOnFakeCluster$' -count=1` 做轻量回归。只写一套 KindExec 用例。

## Task 3　腾讯云正式验收与 CI

- [x] 在 CVM A 的专用命名空间运行真实集群契约。用交叉编译的测试二进制、`test2json` 与 `citestjson` 保留原始结果并检查跳过。装有 Go 的主机可用 `bash scripts/verify-k8s-contract.sh k8s-contract.jsonl` 运行同一套契约。
- [ ] 在要求跑真实集群的 CI 验收任务复用 `tools/citestjson`，验证目标测试确实运行且退出成功。缺环境、全部未运行或目标契约被跳过均标红。普通 fake CI 仍可独立运行。
- [x] 用现有 CSV 任务经正常 Gateway 请求完成独立 Pod 执行和评测。基线失败、正确补丁通过、错误补丁失败。保存两类 Pod UID、`hostPath` 数量、退出码、产物摘要、超时时间和清理后残留 Pod 数。
- [ ] 归档 CVM 规格、提交、镜像 digest、命令、JSON 测试流和 K8s 事件。阶段 3 再测 Pod 排队与启动容量，提前保留本阶段 requests/limits 的实际配置。
