# CVM A 阶段 1 验收记录

2026-11-26 在腾讯云 SA9.LARGE8（4 vCPU、8 GiB）、K3s v1.34.13+k3s1 验收。Go Gateway 使用专用命名空间的 ServiceAccount。Worker 镜像 digest 为 `sha256:a8fe9d08f0e7c376444769e8f84611d4ef223a7955ee3908e63c0779f9083353`。

- [CSV 评分结果](results.json)和[任务检查结果](inspect.json)记录了三个补丁状态和一次 400 ms 墙钟超时。任务 ID 为 `task_e5e11a3a21e551c20ff27d5081757a57`。
- [Pod 采样](pods-observed.jsonl)记录了 1 个编排 Pod 和 4 个独立执行 Pod，合计 0 个 hostPath 卷。最终集群检查无运行时 Pod 和临时 Secret 残留。
- [报告产物](report.txt)经 CLI 下载，SHA-256 为 `b3d6c680c5943635ecb5ecf9ecabc83d4ec2c0418ccbe13e6ed30278070edb25`。
- [真实 Provider 契约 JSONL](provider-contract.jsonl)记录 17 项通过、0 项跳过，耗时 21.46 秒。装有 Go 的验收机可使用 [严格契约脚本](../../../scripts/verify-k8s-contract.sh)复跑。

CSV 任务由脚本化 Worker 驱动，模型推理和多轮工具调用由阶段 2 验收。
