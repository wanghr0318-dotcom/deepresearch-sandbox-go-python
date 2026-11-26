# 阶段 2 · SGLang 实机验收

## 部署

- 镜像为 [SGLang v0.5.23](https://github.com/sgl-project/sglang/releases/tag/v0.5.23)，启动脚本锁到 Docker Registry 返回的 Linux amd64 digest。
- 权重为 [Qwen/Qwen3-8B](https://huggingface.co/Qwen/Qwen3-8B/tree/b968826d9c46dd6066d109eabc6255188de91218)，保留该 revision 随附的聊天模板。SGLang 使用 `qwen25` 工具解析器，默认关闭 thinking。
- 单卡从 BF16、4096 上下文及 1 个运行中请求起步。4096 是含输出预留的启动配置，实际可用输入长度和显存由实测确定。
- GPU 主机使用 x86_64 Linux、适配所选镜像 CUDA 的 NVIDIA 驱动、Docker 与 NVIDIA Container Toolkit。阶段 1 的 CPU CVM 和 GPU CVM 同上海地域、同 VPC。GPU 的 TCP 30000 仅允许 CPU CVM 私网访问。

```bash
bash deploy/sglang/run.sh "$GPU_PRIVATE_IP" /mnt/models/huggingface
docker logs -f agentbox-sglang
```

先在 GPU 主机针对这两个固定制品检查下载链路，记录 GPU 型号、驱动、镜像 digest、模型 revision、启动参数及 `nvidia-smi` 显存。CVM A 的公网直连探针中，`huggingface.co` 和 `registry-1.docker.io` 超时，GPU 实例需独立验证。若必须换镜像仓库，核对镜像 digest。若转运权重缓存，保持同一 HF revision。

### AutoDL 单卡与跨云连接

本次使用北京 AutoDL A800 和上海腾讯云 CVM。AutoDL 容器内建立 Python 3.12 虚拟环境，通过 uv 安装 `sglang==0.5.23`，并限制 `aiohttp>=3.10,<4`。安装后的 PyTorch 为 `2.14.1+cu130`。固定的是 SGLang 包版本；上面的 Docker digest 只适用于 Docker 路径。

权重放在数据盘，按指定 revision 的文件清单核对各分片 SHA-256。启动时将 `--model-path` 指向本地权重目录，保留 `--served-model-name Qwen/Qwen3-8B`，服务绑定 `127.0.0.1:30000`。当前 chatagent 默认请求最多 16384 个输出 token，因此这次会话验收将上下文调为 24576，给输入和工具定义留出空间。

跨云实验通过本地 SSH 转发连接。工作站向 AutoDL 做本地转发（工作站回环端口 → GPU 回环端口 30000），再向 CVM 做远程转发（CVM 回环端口 → 工作站回环端口）。Gateway 只配置这个 CVM 回环上游；工作站须保持在线。CVM 测得的直连耗时包含这两段隧道。

## 三条探针

1. 在 CVM A 直连 GPU 私网的 `http://<GPU_PRIVATE_IP>:30000/v1`。同一脚本要求首轮返回 `read_file`，验证函数参数，并携带真实返回的 `tool_call_id` 回传测试字符串。第二轮必须消费该字符串。报告记录完整的测试请求、响应和耗时。
2. 用阶段 1 的 Gateway 启动配置接入下列选项。开启 `--model-api`，用测试用户的 `models` scope key 运行同一脚本，`base_url` 改为 Gateway 的 `/v1`。密钥放在 `AGENTBOX_PROBE_TOKEN` 环境变量中。保留实际 SGLang 请求日志并与 Gateway 请求时间和返回模型交叉核对。单上游模式下 Gateway 没有逐路由 provider 字段。
3. 用现有 `chatagent` 会话入口建立测试会话，向 `POST /sessions/{id}/messages` 提交一个普通工具任务。先让模型用 `write_file` 把固定文本写入 `probe.txt`，再通过 `read_file` 读取并返回内容。保存会话事件和模型调用记录，核对模型自行生成工具动作、真实工具的返回和下一次模型调用。然后用现有 [CSV 修复题](../../eval/fixtures/csv-first-row/task.md)扩展为读、改、测任务，交给独立验证器评分。脚本中的测试字符串只用于前两条协议探针。

```bash
python3 scripts/probe-sglang-tools.py "http://$GPU_PRIVATE_IP:30000/v1" direct.json
```

Gateway 启动参数增量如下。实验启动参数只声明 SGLang 这一个模型上游，原有 K8s 和工作区参数沿用阶段 1 的验收配置。

```bash
--model-base-url "http://$GPU_PRIVATE_IP:30000/v1" \
--upstream-allow-private "$GPU_PRIVATE_IP:30000" \
--model-name Qwen/Qwen3-8B \
--user-orchestrator-model Qwen/Qwen3-8B --user-worker-model Qwen/Qwen3-8B \
--model-api --session-worker-argv python3,-m,chatagent
```

三条探针通过后，再固定输出预算测单请求上下文长度和峰值显存。端到端报告分别记录解析中止、基础设施故障以及可评分但答错。
