# 腾讯云 CPU 上的 CSV Agent 彩排

2026-11-26 在上海 CVM A 新起独立的 Gateway、PostgreSQL 数据库和 K3s namespace。固定应答器使用 OpenAI 兼容接口，真实 `chatagent` 依次读文件、写文件、调用 `exec_shell`，并将三次工具结果按原调用 ID 送回应答器。本轮使用固定应答，推理能力留给实际模型验收。

会话从提交到终态耗时 5.093 秒。候选从工具执行结果的 BlobStore 摘要取回，由新的 KindExec Pod 重跑四个 CSV 用例，全部通过。编排、工具、评分编排和评分执行各用了一个 Pod，四个 UID 均不同，`hostPath` 均为 0。关闭会话后查询该 namespace 的托管 Pod 和 Secret，均已清理。

在 CVM 上复跑时使用已有的 K3s、PostgreSQL 和镜像，并为本次运行选择新的本地证据目录。脚本只在指定的内网地址监听 Pod relay。

```bash
sudo python3 scripts/run-cloud-agent-csv.py \
  --fixture eval/fixtures/csv-first-row \
  --agentbox ~/agentbox-cvm-acceptance/agentbox \
  --worker-image localhost/agentbox-worker@sha256:a8fe9d08f0e7c376444769e8f84611d4ef223a7955ee3908e63c0779f9083353 \
  --relay-listen 172.17.0.6:19446 \
  --model csv-scripted --scripted --evidence csv-rehearsal
```

实际模型路径将 `--model csv-scripted --scripted` 换成 `--model Qwen/Qwen3-8B --model-url http://127.0.0.1:33562/v1`。使用另一证据目录，并在测试开始前确认模型转发可用。

归档包含 `result.json`、`events.sse`、`records.py`。各 Pod 的创建、就绪和删除记录汇总在 `result.json`。
