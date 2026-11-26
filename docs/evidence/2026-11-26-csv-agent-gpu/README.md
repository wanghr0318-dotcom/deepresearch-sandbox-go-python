# 真实 GPU 的 CSV 修复验收

2026-11-26 在上海 CVM A 启动独立的 Go Gateway、K3s namespace 和 PostgreSQL 数据库。唯一模型上游为通过 SSH 转发接入的 AutoDL A800 上的 SGLang，模型为 `Qwen/Qwen3-8B`。会话入口沿用 [CPU 彩排](../2026-11-26-csv-agent-cpu/README.md) 的同一驱动。

| 运行 | 会话耗时 | 结果 |
| --- | ---: | --- |
| [第 1 次](attempt-1/result.json) | 10.139 s | 两次模型调用共 6 次上游尝试，均返回 502 `upstream_unreachable`，当时转发入口断开。 |
| [第 2 次](attempt-2/result.json) | 21.276 s | 两次模型调用共 6 次上游尝试，均返回 502 `upstream_unconfirmed`。GPU 日志定位到 FlashInfer JIT 子进程缺少 `ninja` 的 PATH。 |
| [第 3 次](attempt-3/result.json) | 13.181 s | A800 上的 SGLang 完成 4 次模型调用。Agent 错判相对路径，独立评分通过 1/4，计为可评分未解对。 |
| [第 4 次](attempt-4/result.json) | 25.334 s | 澄清工具路径。Agent 正确读取并写入 `records.py`，公开冒烟实际输出空列表，独立评分 1/4。 |
| [第 5 次](attempt-5/result.json) | 12.178 s | 同一澄清提示。Agent 自主读、改、测，4 次模型调用均由 SGLang 响应，独立评分 4/4。 |

前两次在首轮模型输出前中止，计为基础设施中止。第 3 次的 Agent 调用 `read_file` 读取 `repo/records.py` 收到 404，随后在该子目录写入了同名文件，并只针对该文件跑了公开冒烟。独立评分取到的工作区根 `records.py` 与基线摘要一致。原始提示见 [prompt.txt](attempt-3/prompt.txt)，SHA-256 为 `399114d76ae3b78d220b558579fd1ef08af7027d1ba96583c6e6766b1bd336c3`。

各次会话 Pod 正常启动，`hostPath` 均为 0，结束后托管资源余量均为 0。第 1 次采样传入了大小写不符的节点名，无有效资源样本。第 2 次取得 22 个采样点及 20 条 Pod 观察，CPU、内存值各缺 1 条。第 3 次取得 19 个采样点及 24 条 Pod 观察，CPU、内存值各缺 3 条。

第 4 和第 5 次使用相同提示，SHA-256 为 `b0a35cbdeb6b5688769d08062d014339b313f775873a5b23367a8404ff623cac`。公开提示只澄清工作区根和目标文件名，四项评分用例固定。第 4 次保留了跳过首条记录的缺陷，得到 29 个采样点、34 条 Pod 观察，CPU、内存值各缺 2 条。第 5 次取回的候选摘要为 `b6696a729418012d5ad463b2d0a12abe3ab07f06186e3abc4ae98d71968e4247`，工具执行和独立评分分别使用不同的 KindExec Pod，得到 17 个采样点、23 条 Pod 观察，CPU、内存值各缺 3 条。
