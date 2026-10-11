//go:build linux

// Command fakeupstream 以独立进程运行 tests/e2e/fakeupstream（只供测试与演练，不访问外网）：scripts/demo-m2.sh
// 的演练模式（AGENTBOX_DEMO_FAKE=1）用它代替真实的模型与搜索上游。
//
// 启动后安装固定研究题目的脚本（与 Plan 8 Task 5 的业务验收相同：2 个任务、每次搜索 3 条结果、固定页面），
// 向标准输出打印一行 `url=<根地址> model_base_url=<根地址>/v1 hostport=<127.0.0.1:端口>`，然后运行到收到
// SIGTERM 或 SIGINT；退出前打印各类请求计数。
//
//   - -eval-suite FILE：读取 agentbox eval 的 suite（internal/eval），为每个编码任务安装 [stage:code] 的回复
//     （suite 中的 fake_reply）：评测的零成本模式。
//   - -hold-first-chat：第一次 chat 请求（研究的计划阶段）挂起，直到进程收到 SIGUSR1——演练脚本据此在 Worker
//     运行中完成沙箱进程的 G3 检查后再放行，不依赖时间。
//   - -latency chat=300ms,search=80ms：各类别正常回复前的固定延迟（可观测性演示：trace 与延迟直方图有可读的数字）。
//   - -inject chat:3:503,search:2:429：第 N 次该类请求以给定状态回复（演示 Gateway 的重试与上游错误指标）。
//   - -every chat:4:hang,fetch:5:503：该类请求中序号为 K 的倍数者挂起（直到客户端离开）、以给定状态回复或
//     （disconnect）中途断开：故障速率固定的不稳定供应商（trace doctor 演示）。
//
// 模型网关（2026-10-11 设计 §10）：类别 messages 是 Anthropic 的 /v1/messages（base_url 为打印的 url）；
// 以上各选项的 kind 都可以是 messages。
//   - -anthropic-key-env NAME：/v1/messages 要求 x-api-key 等于环境变量 NAME 的值（Key 不出现在命令行）。
//   - -fail-after chat:2:5,messages:1:3:error：该类第 N 次流式请求发出 K 个 SSE 事件后切断连接（加 :error 时
//     改为发出错误事件，加 :midline 时在下一个事件的一行中间切断）。
//   - -chunk-delay chat=50ms：流式回复相邻事件之间的延迟（首字延迟测量）。
//   - -cache-usage 300:20：模型回复用量带缓存读取 300、缓存写入 20 个 token。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// researchTasks 是固定研究题目的计划（与 tests/e2e 的业务验收相同）。
var researchTasks = []fakeupstream.ResearchTask{
	{Title: "电解质路线", Intent: "比较硫化物、氧化物与聚合物电解质", Query: "固态电解质 技术路线"},
	{Title: "量产与成本", Intent: "梳理量产时间表与成本结构", Query: "固态电池 量产 成本"},
}

func main() {
	hold := flag.Bool("hold-first-chat", false, "第一次 chat 请求挂起，直到收到 SIGUSR1")
	latency := flag.String("latency", "", "各类别的固定延迟，逗号分隔的 kind=duration（kind 为 chat、search、fetch）")
	inject := flag.String("inject", "", "注入的错误回复，逗号分隔的 kind:N:status（第 N 次该类请求以 status 回复）")
	every := flag.String("every", "", "周期故障，逗号分隔的 kind:K:action（序号为 K 的倍数的请求；action 为 hang、disconnect 或 4xx/5xx 状态）")
	suitePath := flag.String("eval-suite", "", "agentbox eval 的 suite 文件：编码任务的 fake_reply 作为 [stage:code] 回复")
	keyEnv := flag.String("anthropic-key-env", "", "/v1/messages 要求 x-api-key 等于该环境变量的值")
	failAfter := flag.String("fail-after", "", "流式故障，逗号分隔的 kind:N:K[:error]（第 N 次该类请求发出 K 个事件后切断或发出错误事件）")
	chunkDelay := flag.String("chunk-delay", "", "流式回复相邻事件之间的延迟，逗号分隔的 kind=duration")
	cacheUsage := flag.String("cache-usage", "", "模型回复用量中的缓存 token，read:write")
	flag.Parse()

	fu := fakeupstream.New()
	fu.SetResearch(researchTasks)
	if err := configure(fu, *latency, *inject, *every); err != nil {
		fmt.Fprintln(os.Stderr, "fakeupstream:", err)
		os.Exit(2)
	}
	if err := configureStreams(fu, *keyEnv, *failAfter, *chunkDelay, *cacheUsage); err != nil {
		fmt.Fprintln(os.Stderr, "fakeupstream:", err)
		os.Exit(2)
	}
	if *suitePath != "" {
		suite, err := eval.LoadSuite(*suitePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		n := 0
		for _, t := range suite.Tasks {
			if t.Kind == eval.KindCoding && t.FakeReply != "" {
				fu.SetCodeReply(t.ID, t.FakeReply)
				n++
			}
		}
		fmt.Fprintf(os.Stderr, "fakeupstream: %d scripted code replies from %s\n", n, *suitePath)
	}
	if *hold {
		fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	}
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)
	fmt.Printf("url=%s model_base_url=%s hostport=%s\n", fu.URL(), fu.ModelBaseURL(), fu.HostPort())
	for s := range sig {
		if s == syscall.SIGUSR1 {
			// 挂起的请求可能尚未到达：等它挂起后再放行（Release 只放行当前挂起的请求）。
			go func() {
				for fu.Hanging() == 0 {
					time.Sleep(20 * time.Millisecond)
				}
				fu.Release()
				fmt.Println("released")
			}()
			continue
		}
		break
	}
	fmt.Printf("counts chat=%d search=%d fetch=%d plan=%d summarize=%d report=%d code=%d messages=%d\n",
		fu.Count(fakeupstream.Chat), fu.Count(fakeupstream.Search), fu.Count(fakeupstream.Fetch),
		fu.StageCount(fakeupstream.StagePlan), fu.StageCount(fakeupstream.StageSummarize), fu.StageCount(fakeupstream.StageReport),
		fu.StageCount(fakeupstream.StageCode), fu.Count(fakeupstream.Messages))
	fu.Close()
}

// kinds 是命令行中的类别名。
var kinds = map[string]fakeupstream.Kind{"chat": fakeupstream.Chat, "search": fakeupstream.Search, "fetch": fakeupstream.Fetch,
	"messages": fakeupstream.Messages}

// configure 应用 -latency、-inject 与 -every。
func configure(fu *fakeupstream.Server, latency, inject, every string) error {
	for _, item := range strings.Split(latency, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		k, v, ok := strings.Cut(item, "=")
		d, err := time.ParseDuration(v)
		if kind, known := kinds[k]; ok && known && err == nil && d >= 0 {
			fu.SetLatency(kind, d)
			continue
		}
		return fmt.Errorf("-latency %q: 须为 kind=duration（kind 为 chat、search、fetch、messages）", item)
	}
	for _, item := range strings.Split(inject, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) == 3 {
			kind, known := kinds[parts[0]]
			n, nerr := strconv.Atoi(parts[1])
			status, serr := strconv.Atoi(parts[2])
			if known && nerr == nil && serr == nil && n >= 1 && status >= 400 && status <= 599 {
				fu.Inject(kind, n, fakeupstream.Action{Status: status})
				continue
			}
		}
		return fmt.Errorf("-inject %q: 须为 kind:N:status（N ≥ 1，status 为 4xx/5xx）", item)
	}
	for _, item := range strings.Split(every, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) == 3 {
			kind, known := kinds[parts[0]]
			k, kerr := strconv.Atoi(parts[1])
			if a, ok := periodicAction(parts[2]); known && kerr == nil && k >= 1 && ok {
				fu.SetEvery(kind, k, a)
				continue
			}
		}
		return fmt.Errorf("-every %q: 须为 kind:K:action（K ≥ 1，action 为 hang、disconnect 或 4xx/5xx 状态）", item)
	}
	return nil
}

// periodicAction 解析 -every 的动作。
func periodicAction(s string) (fakeupstream.Action, bool) {
	switch s {
	case "hang":
		return fakeupstream.Action{Hang: true}, true
	case "disconnect":
		return fakeupstream.Action{Disconnect: true}, true
	}
	status, err := strconv.Atoi(s)
	if err != nil || status < 400 || status > 599 {
		return fakeupstream.Action{}, false
	}
	return fakeupstream.Action{Status: status}, true
}

// configureStreams 应用 -anthropic-key-env、-fail-after、-chunk-delay 与 -cache-usage。
func configureStreams(fu *fakeupstream.Server, keyEnv, failAfter, chunkDelay, cacheUsage string) error {
	if keyEnv != "" {
		key := os.Getenv(keyEnv)
		if key == "" {
			return fmt.Errorf("-anthropic-key-env %s: 环境变量为空", keyEnv)
		}
		fu.SetAnthropicKey(key)
	}
	for _, item := range strings.Split(failAfter, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) == 3 || (len(parts) == 4 && (parts[3] == "error" || parts[3] == "midline")) {
			kind, known := kinds[parts[0]]
			n, nerr := strconv.Atoi(parts[1])
			k, kerr := strconv.Atoi(parts[2])
			if known && nerr == nil && kerr == nil && n >= 1 && k >= 1 {
				mode := ""
				if len(parts) == 4 {
					mode = parts[3]
				}
				fu.Inject(kind, n, fakeupstream.Action{FailAfterChunks: k, FailWithError: mode == "error", FailMidLine: mode == "midline"})
				continue
			}
		}
		return fmt.Errorf("-fail-after %q: 须为 kind:N:K、kind:N:K:error 或 kind:N:K:midline（N、K ≥ 1）", item)
	}
	for _, item := range strings.Split(chunkDelay, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		k, v, ok := strings.Cut(item, "=")
		d, err := time.ParseDuration(v)
		if kind, known := kinds[k]; ok && known && err == nil && d >= 0 {
			fu.SetChunkDelay(kind, d)
			continue
		}
		return fmt.Errorf("-chunk-delay %q: 须为 kind=duration", item)
	}
	if cacheUsage != "" {
		r, w, ok := strings.Cut(cacheUsage, ":")
		read, rerr := strconv.ParseInt(r, 10, 64)
		write, werr := strconv.ParseInt(w, 10, 64)
		if !ok || rerr != nil || werr != nil || read < 0 || write < 0 {
			return fmt.Errorf("-cache-usage %q: 须为 read:write（非负整数）", cacheUsage)
		}
		fu.SetCacheUsage(read, write)
	}
	return nil
}
