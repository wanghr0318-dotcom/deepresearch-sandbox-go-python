# 阶段 4 · 腾讯云 A 单机 Pod 故障恢复

A 的 K3s v1.34.13+k3s1 在节点 `vm-0-6-ubuntu` 上运行真实编排 Pod、Gateway relay、执行 Pod 和独立评分 Pod。模型端使用 CPU 确定性回包。每次实验新建数据库、命名空间、会话和工作区。

模型依次请求读文件、修复 CSV 首行丢失、运行 Python 冒烟测试。测试钩子在写文件后的 checkpoint 得到宿主确认时挂起 Agent。驱动在任务记录中找到同一 checkpoint 后强制删除该实验 Pod。宿主自动创建新的 attempt 和 Pod。恢复入口直接运行 chatagent，跳过初始化写入。模型收到两个历史工具结果后才会请求第三步。评分器独立检查候选文件的四个用例。

**恢复实验**：历史共注入 4 次 Pod 删除，成功续跑 4 次，覆盖 1 个故障时点。后续以 01 和 04 两轮作基线，02 和 03 作为同点重复观测保留。

**业务判分**：每轮恢复后分别对候选执行同一组 4 个 CSV 用例，各轮均通过 4 项。

| 运行 | 请求删除至下一次 exec_shell 事件（秒） | 请求删除至 exec_shell 成功返回（秒） | CSV 用例通过数/执行数 |
|---|---:|---:|---:|
| [01](run-01/result.json) | 5.223 | 6.914 | 4/4 |
| [02](run-02/result.json) | 5.343 | 7.031 | 4/4 |
| [03](run-03/result.json) | 3.736 | 5.418 | 4/4 |
| [04](run-04/result.json) | 4.825 | 7.490 | 4/4 |

时间起点取各轮 `fault.json` 的 `delete_utc_epoch`，终点取同一台机器产生的 `events.sse` 中 exec_shell 的 tool_call 或成功 tool_result。四次均出现两个 attempt（crashed_signal、succeeded），producer_exec 归属第二个 attempt。四轮写文件事件内容、执行产物和评分候选的 SHA-256 一致，为 `1b35e2b8fa0f2a85d5aa4efcf30aed6bd12e87139a0cb17c3a735c7aa1c13b64`，清理后实验内剩余的受管资源均为空。

第 04 轮额外在旧进程提交与新进程恢复处记录完整消息列表摘要。6 条消息的角色依次为 system、user、assistant、tool、assistant、tool，两侧摘要相同。详见[消息与 Pod UID 对照](run-04/fault.json)、[任务及调用归属](run-04/solve-inspect.json)、[评分记录](run-04/grade-inspect.json)。

本实验中 PG、Gateway、工作区索引和 blob 均留在 A。实测范围为同一执行节点上的编排 Pod 替换。B 整机失联、跨机入口重绑、租约过期拒写及 COS 上传中断均待后续验证。

## 在 A 复跑

在包含本次脚本的项目根目录运行。证据目录每次使用新名字，路径保持简短，以适配 Unix socket 的长度限制。

```sh
sudo -n python3 -u eval/phase4/rebuild.py \
  --agentbox /home/ubuntu/agentbox-cvm-acceptance/agentbox \
  --fixture eval/fixtures/csv-first-row \
  --worker-image localhost/agentbox-worker@sha256:a8fe9d08f0e7c376444769e8f84611d4ef223a7955ee3908e63c0779f9083353 \
  --relay-listen 172.17.0.6:19446 --model phase4-cpu --scripted \
  --evidence "/tmp/p4-$(date +%s)" --timeout 45
```
