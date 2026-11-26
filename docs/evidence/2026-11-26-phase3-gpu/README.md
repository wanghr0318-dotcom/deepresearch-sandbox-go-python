# A800 推理进程及显存采样

2026-11-26 在 AutoDL 单卡 NVIDIA A800-SXM4-80GB 上运行 SGLang 0.5.23，驱动 580.126.09，Python 3.12.3，实际导入的 PyTorch 为 2.14.1+cu130。模型为 Qwen3-8B revision `b968826d9c46dd6066d109eabc6255188de91218`，五个权重分片和 tokenizer 的 [SHA-256](weight-sha256.txt) 与 [前次验收](../2026-11-26-sglang-a800/gpu-verification.txt) 一致。

推理配置为 BF16、最大运行请求数 1、上下文 24576、静态显存比例 0.88、Triton attention，禁用 prefill 和 decode CUDA Graph，关闭 thinking，保留 qwen25 工具解析配置。见 [启动命令](command.txt) 和 [启动日志摘录](startup-events.txt)。SGLang 监听 GPU 主机回环地址，腾讯云 CVM 通过 SSH 转发接入。

第一轮 `/v1/models` 就绪后，18:02:15 的首次非贪心任务请求触发 FlashInfer JIT。服务进程的 `PATH` 缺少已安装 `ninja` 的目录，调度进程随后退出。将虚拟环境和 CUDA 编译工具的目录加入启动 `PATH` 后重启，仍使用相同的 sampling backend。18:04:03 接口就绪，后续非贪心 [探针请求](sampling-probe.request.json) 取得 [正常响应](sampling-probe.response.json)。见 [首次故障日志](scheduler-failure.txt) 和 [工具链版本](toolchain.txt)。

Gateway 请求层 R2 中，客户端第 90 条请求耗时约 30.7 秒。按时间和 1379 输入、10 输出 token 匹配，SGLang 在 18:11:07 收到与结束该请求，报告服务端耗时 0.284 s、排队 0.316 ms。后续 30 个每秒 GPU 样本为 0% 利用率，显存稳定在 71988 MiB。两层记录缺少共享请求 ID，无法据此把尾延迟归到具体传输环节。见 [服务端请求计时](gateway-r2-selected-requests.jsonl)。

## 正式请求回放窗口

将 [每秒 GPU 样本](gpu-samples.csv) 按 [CVM 开始与结束时间](../2026-11-26-phase3-request/README.md) 左闭右开切分，六组共 725 个样本。显存峰值是整张卡 `nvidia-smi` 采样值，包含模型、静态 KV cache 和运行时分配。GPU 利用率为每秒采样值的算术均值，覆盖请求间隔和网络等待。

| 组别 | 样本数 | 峰值显存（MiB） | 平均 GPU 利用率 | 峰值 GPU 利用率 |
| --- | ---: | ---: | ---: | ---: |
| 直连 R1 | 100 | 71988 | 17.75% | 50% |
| Gateway R1 | 100 | 71988 | 19.62% | 49% |
| Gateway R2 | 154 | 71988 | 11.53% | 49% |
| 直连 R2 | 147 | 71988 | 12.33% | 49% |
| 直连 R3 | 100 | 71988 | 20.13% | 47% |
| Gateway R3 | 124 | 71988 | 15.22% | 47% |

## 全程采样

修复后主机的 [原始采样](gpu-samples.csv) 覆盖北京时间 18:03:31.598 至 18:33:58.158，共 1827 点。相邻采样的最大间隔为 1.008 秒。覆盖成功探针、请求回放、Agent 任务和上下文边界探针。全程峰值为 73236 MiB，首次在 18:33:21.149 观测到。单秒 GPU 利用率最高为 100%。

[最终指标摘录](metrics.txt) 是进程级累计，包含上述负载。前缀命中计数为 916623 个 device-hit token，另有 52251 个 input token，host/storage 命中为 0。这组复用前缀的混合负载没有执行缓存开关对照。排队直方图总量为 18.530 s、741 个观测，整体均值约 25.0 ms。SGLang 访问日志另记录 736 条 `/v1/chat/completions` HTTP 200，两处按各自计数口径保留。见 [HTTP 200 摘录](chat-post-200-count.txt) 和 [采样截止](collected-at.txt)。
