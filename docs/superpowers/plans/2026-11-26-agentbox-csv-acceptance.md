# 阶段 2 收尾与阶段 3 单节点基线实施计划

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** 从一次真实 chatagent 会话取回模型修改后的 records.py，在独立工具 Pod 中评分，并使另一人能根据原始记录重算结果。

**Architecture:** 上海 CVM A 承载 Go Gateway、K3s 和执行 Pod。北京 AutoDL 单卡 A800 运行 SGLang，通过现有双 SSH 隧道接入 Gateway。控制端关联会话、候选文件、工具 Pod 与评测结果。

**Tech Stack:** Go、Python、K3s、PostgreSQL、SGLang 0.5.23、Qwen/Qwen3-8B BF16。

## 已验证的起点和约束

- 远端阶段 1 为 ccdf6f9。真实 K8s 契约 17 项通过、0 跳过；CSV 为脚本化 Worker。阶段 2 部署文件、探针和原始证据目前在工作树。
- 查看 docs/evidence/2026-11-26-sglang-a800/ 内的 direct.json、direct-from-cvm.json、gateway.json、events.sse 和 agent-calls.csv。前三份验证协议；后两份串起真实会话。现有显存记录 71646 MiB 为采样点。
- 冻结权重 revision b968826d9c46dd6066d109eabc6255188de91218、qwen25、thinking 关闭、24576 上下文、服务并发 1。AutoDL 使用 Python 安装路径；另存实际包清单及启动配置。保持零 hostPath。复用已有 workspace API、验证用例与负载客户端。
- 本轮并行任务是审阅与规划。下列是执行时的写入归属。主 agent 整合原计划、调度云机、串行发起正式测量、审查脱敏证据并提交。并发审阅不意味着并发占用同一 GPU。

## A　cloud_image · 部署和实际路由

**归属** deploy/sglang/README.md、deploy/sglang/run.sh（仅 Docker 路径有实际缺陷时改）；证据为本轮环境清单、服务启动参数、上游请求记录、设备采样。读取 scripts/probe-sglang-tools.py，不重复写探针。
- [ ] 在工作站确认隧道、在 GPU 进程所在环境记录 pip freeze、模型清单、实际启动参数、GPU 名称、驱动。比对已有 gpu-verification.txt。摘录 Gateway 单上游配置并去掉凭据。
- [ ] 在 CVM A 对同一请求顺序复跑直连与 Gateway。入口是 python3 scripts/probe-sglang-tools.py BASE_URL OUTPUT_JSON；BASE_URL 分别取已转发的模型 /v1 地址及已鉴权的 Gateway /v1 地址。验收核对 tool_call_id、第二轮原文、服务端请求时间与模型。中继存活需覆盖整轮测试。
- [ ] 在 GPU 端覆盖预热至末个请求完成，运行 nvidia-smi --query-gpu=timestamp,memory.used,utilization.gpu --format=csv -l 1 并保存输出。交付采样间隔和观测最大值，标明预分配影响及采样遗漏瞬时峰值的可能。
- [ ] 固定输出预算，单请求逐档增加实际输入 token，记录提交长度、服务接受长度、完成/截断/拒绝/OOM；先验证任务所需窗口。任何配置变更单独登记。将可用模型入口、配置指纹和时间窗口交给 B、C。

## B　provider_lifecycle · 一道真实修复题、独立评分

**归属** eval/fixtures/csv-first-row/、本轮任务证据；优先复用 worker/agentbox_worker/tools/workspace.py、既有 chatagent session、阶段 1 工具执行通道。用 eval/fixtures/csv-first-row/verify.py 的四组 CASES 作为唯一评分依据。
- [ ] CPU 预检运行 py -3.13 -B eval/fixtures/csv-first-row/verify.py（Linux 用 Python 3.10+）。预期 baseline 1/4、correct 4/4、incorrect 3/4；三组自检整体退出 0。
- [ ] 用参考候选在独立 KindExec 路径复现同样三组 verdict。评分端只接收候选 records.py 与固定 checker。记录候选 SHA-256、各用例 verdict、Pod UID、退出状态。已知模板不需要新增评分框架。
- [ ] 新建普通 chatagent 会话，通过验收入口使用现有 gateway.workspace 写入 repo/records.py 并核对初始摘要，然后调用原 chatagent.app.run。只在验收入口做 fixture 初始化，模型收到的第一步仍是读取原文件，向 POST /sessions/{id}/messages 发送 task.md 的题意。参考补丁、独立用例及期望输出留在评测端。公开冒烟测试可根据题面单行示例执行。
- [ ] 从事件中核实模型读原文件、写修改、调用 exec_shell 测试、消费结果。结束前用 exec_shell 执行 base64 -w0 records.py，并取得同一版本的 sha256sum。由 tool_result.raw.response_ref 定位 GET /turns/{turn_id}/raw/{sha256}，解码完整 stdout 并校验文件摘要；若输出截断，本次标为取件失败。由独立执行 Pod 在干净目录重跑评分并核对导出摘要。模型自报“成功”只作为会话输出保留。
- [ ] 各次都归档会话与 turn ID、工具与 Pod 记录、候选、上游调用及四项判定。目标是一条真实解对、一条正常结束但评分失败的轨迹；预先登记试跑上限（建议首批 3 次），全部通过或全部失败均照实报告缺项。参考错误补丁只用于验证评分器。
- [ ] 主 agent 审查 B 的证据链及 A 的实际路由；经脱敏检查后分别固定部署/探针与 CSV 验收的提交。阶段 2 关闭后才能发布正式 Agent 基线。

## C　exec_contract · 单节点基线与计量

**归属** cmd/agentbox-loadtest/ 的最小适配及阶段 3 统计产物；题库与评分归 B。首先复用 client.go、scenarios.go、stats.go。现有 evalworker 与 chatagent 入口不同，正式任务必须走 B 验证的同一入口。
- [ ] CPU 上预检已有客户端：go test ./cmd/agentbox-loadtest/... 。核实首个带内容 SSE、末 token 与输出计数。现有非流式工具探针仅记录整次耗时。若实际命令只能连接测试上游，先写针对真实端点配置/固定请求序列的失败用例，再最小修改，复跑同一测试。
- [ ] B 将首题扩至 5–8 题，每题固定初始仓库、参考补丁和评分规则，校准后冻结。C 保存请求序列和 seed，标注输入输出预算、预热、缓存设置、到达方式和重试。直连与 Gateway 均从 CVM A 出发，比较包含跨云隧道的观测值。
- [ ] 单执行节点从闭环并发 1 起跑。逐轮记录服务并发和客户端并发；提高负载时只改一个量。每档至少 3 轮，按轮列样本数、中位数、范围；廉价请求每档争取 100 次，少量仓库任务列逐题数字。
- [ ] 每次提交落一条 JSONL，含 run/session/turn、题号、候选摘要、终态、用例结果、模型调用数/解析错误数、输入/输出 token、耗时及证据位置。终态互斥：解对、可评分未解对、解析中止、基础设施中止；其他中止显式列出。核对各类总数等于提交数。
- [ ] 分层输出请求级流式 TTFT、输出 tokens/s 与队列，整任务可评分轨迹/h、Pod 就绪时间、CPU 秒与 GiB·秒/可评分轨迹。资源指标注明采样来源和分母，采不到的项标缺测。按账单单列 CPU、GPU、盘、流量、失败与空闲时段。每千条成本 = 对应窗口实际费用 × 1000 / 同窗口可评分数；分母为零时留空。
- [ ] 聚合前复核直接与网关两组实际样本一致，报告每轮统计及样本量。下一阶段的跨机恢复须单独验收工作区、消息和 Gateway socket 重绑。扩容、缓存开关实验保留到固定基线之后。
