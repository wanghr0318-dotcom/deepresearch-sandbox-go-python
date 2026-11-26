# 阶段 3 · 云上单执行节点的 Agent 修复题基线

上海 CVM A 运行 K3s、Go Gateway 和 PostgreSQL。北京 AutoDL 的单卡 A800 运行 SGLang 和 Qwen3-8B BF16。使用单任务槽、单执行槽和单上游，经双 SSH 隧道接入，按固定顺序执行 6 题 × 3 轮。每题独立开启会话和工作区，产物交给新的 KindExec Pod 评分。六题各自的实际提示 SHA-256 在三轮内一致。

| 轮次 | 解对 | 可评分未解对 | 其他中止 |
| --- | ---: | ---: | ---: |
| 1 | 3 | 3 | 0 |
| 2 | 3 | 3 | 0 |
| 3 | 3 | 2 | 1 |
| 合计 | 9 | 8 | 1 |

总墙钟时间 671.183 秒，包含每题的启动、模型与工具执行、独立评分、关闭和单次超时，实际可评分轨迹为 91.182 条/小时。共 18 次提交、17 条可评分、0 解析中止、0 基础设施中止。[汇总](formal/summary.json) · [逐任务记录](formal/trajectories.jsonl)

解对以独立评分四项全过为准。9 条解对轨迹中，8 条自主选择了未安装的 `pytest` 或找不到测试的 `unittest` 命令，仍产出了可复评的正确候选文件。在 `result.json` 中可用 `public_workflow_ok` 区分公开冒烟执行结果，完整 CSV 读改测成功轨迹另见[阶段 2 第 5 次](../2026-11-26-csv-agent-gpu/attempt-5/result.json)。

第三轮 `money-rounding` 进入搜索分支，SGLang 仍在返回 200，外部搜索反复超时，最终用完 240 秒任务等待窗口。保留为其他中止。17 份候选均复核 SHA-256，各自记录评分与 Pod UID。[调用日志](formal/gpu-suite-timeout-gateway.jsonl) · [驱动日志](formal/gpu-suite-timeout-driver.log)

K3s 从 10:21:29.877 UTC 到 10:32:40.224 UTC 共记录 584 个采样点，包含本批 namespace 内 72 个不同 UID 的 684 条 Pod 观察。CPU 累计值和内存工作集各缺 41 条。17 条可评分任务的生命周期证据覆盖 70 个 Pod，均为零 `hostPath` 并收到删除事件。[K3s 原始采样](formal/k3s.jsonl)

首次启动批量驱动时，长证据路径导致 AF_UNIX socket 地址过长。停止批量进程、允许已启动的子任务清理后，将正式批移到由 `mktemp -d` 创建的短路径。首次批的三条汇总行和第四个独立结果保留在 `path-failed/`，正式 18 条分母从短路径完整复跑开始。[失败日志](path-failed/gpu-suite-path-error.log)
