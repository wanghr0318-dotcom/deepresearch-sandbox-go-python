# 阶段 3 · 单节点测量输入与复查

tasks.json 固定六题的次序。四道单文件修复、两道跨文件定位。跨文件题仍只提交 records.py，判题端复原原始只读 helper。参考补丁和评分输入留在可信评测端。先本地运行固定基线和参考补丁。

```bash
python3 -B eval/phase3/verify-fixtures.py > fixtures.jsonl
go test ./cmd/agentbox-loadtest -count=1
go build -o ./agentbox-loadtest ./cmd/agentbox-loadtest
```

预期每题基线至少一项失败，参考补丁全部四项通过。此验证只运行仓库里的已知输入和补丁，正式评分继续使用独立执行 Pod。

## 直连与 Gateway · 请求级回放

requests.json 是固定的合成负载，含两条短提示和两条长提示。从 CVM A 运行两组。直连地址使用同一模型的 CVM 端入口，Gateway 指向同一个上游。记录 GPU、模型 revision、引擎设置、thinking、缓存状态和隧道拓扑。每组按相同方式预热，预热数据另存。下面每轮 100 个请求，并发为 1。每档重复三轮，改变并发时使用同一组提示和输出预算。

```bash
mkdir -p evidence/phase3
for round in 1 2 3; do
  common=(--corpus eval/phase3/requests.json --model Qwen/Qwen3-8B --repeat 25 --concurrency 1 --max-tokens 64)
  env -u AGENTBOX_REPLAY_TOKEN ./agentbox-loadtest replay --role direct --base "$DIRECT_BASE_URL" "${common[@]}" \
    > "evidence/phase3/direct-c1-r${round}.jsonl"
  ./agentbox-loadtest replay --role gateway --base "$GATEWAY_BASE_URL" "${common[@]}" \
    > "evidence/phase3/gateway-c1-r${round}.jsonl"
done
```

两个 URL 均以 /v1 结尾。调用前安全地把 Gateway 模型 key 设置到环境变量 AGENTBOX_REPLAY_TOKEN。请求正文相同，认证头由各自入口处理。按 index 和 request_sha256 核对两组样本，summary 输出短/长组的样本数和分位数。并发大于 1 时请求的提交顺序受调度影响，样本集合与输出编号固定。

TTFT 是从提交到首个有效内容 SSE 事件的时间，空消息与 usage 块不算。token/s 采用成功且有 usage 的样本总 token 数除以该轮实际墙钟时间。有成功样本没报 usage 时省略该轮 token/s。输出 JSONL 留 HTTP 结果、缺测 TTFT、usage 和短长组。跨云直连和 Gateway 观测都包含隧道，差值只代表该次部署条件下的客户端差异。

## 完整 Agent · 任务回放与评分

阶段 2 的真实 CSV 自主读、改、测及独立判分通过后，从 CVM A 运行同一入口。批量驱动每题启动独立会话和 Gateway，串行占用同一个 relay 监听地址。使用短绝对路径放置实验数据，Linux 的 AF_UNIX 路径还需容纳每题目录和 Gateway socket 文件名。

```bash
run_dir=$(mktemp -d /tmp/abx-XXXXXX)
python3 -B eval/phase3/run-suite.py --evidence "$run_dir/e" --repeat 3 -- \
  --agentbox "$AGENTBOX_BIN" --worker-image "$PINNED_WORKER_IMAGE" \
  --relay-listen "$RELAY_LISTEN" --model Qwen/Qwen3-8B --model-url "$DIRECT_BASE_URL"
```

每次提交一条 trajectories.jsonl 记录。solved 要求判题结果覆盖本题全部固定用例。scored_unsolved 接受完整布尔评分或已完成的独立判题所记录的候选代码异常，异常须有非零退出码及判题产物引用。runner 报告 infrastructure_abort 时计为 infra_abort。parse_abort 采用 runner 明确提供的事件分类，缺证据归 other_abort。此批的可评分轨迹/h 含每题部署、关闭和独立评分。单题 runtime_ms 和调用事件留在原始记录。GPU 队列和显存、Pod 时间、CPU 秒和 GiB·秒由独立采样收集。
