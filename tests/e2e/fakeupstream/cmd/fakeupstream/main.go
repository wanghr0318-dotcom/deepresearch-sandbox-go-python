//go:build linux

// Command fakeupstream 以独立进程运行 tests/e2e/fakeupstream（只供测试与演练，不访问外网）：scripts/demo-m2.sh
// 的演练模式（AGENTBOX_DEMO_FAKE=1）用它代替真实的模型与搜索上游。
//
// 启动后安装固定研究题目的脚本（与 Plan 8 Task 5 的业务验收相同：2 个任务、每次搜索 3 条结果、固定页面），
// 向标准输出打印一行 `url=<根地址> model_base_url=<根地址>/v1 hostport=<127.0.0.1:端口>`，然后运行到收到
// SIGTERM 或 SIGINT；退出前打印各类请求计数。
//
//   - -hold-first-chat：第一次 chat 请求（研究的计划阶段）挂起，直到进程收到 SIGUSR1——演练脚本据此在 Worker
//     运行中完成沙箱进程的 G3 检查后再放行，不依赖时间。
//   - -latency chat=300ms,search=80ms：各类别正常回复前的固定延迟（可观测性演示：trace 与延迟直方图有可读的数字）。
//   - -inject chat:3:503,search:2:429：第 N 次该类请求以给定状态回复（演示 Gateway 的重试与上游错误指标）。
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
	flag.Parse()

	fu := fakeupstream.New()
	fu.SetResearch(researchTasks)
	if err := configure(fu, *latency, *inject); err != nil {
		fmt.Fprintln(os.Stderr, "fakeupstream:", err)
		os.Exit(2)
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
	fmt.Printf("counts chat=%d search=%d fetch=%d plan=%d summarize=%d report=%d\n",
		fu.Count(fakeupstream.Chat), fu.Count(fakeupstream.Search), fu.Count(fakeupstream.Fetch),
		fu.StageCount(fakeupstream.StagePlan), fu.StageCount(fakeupstream.StageSummarize), fu.StageCount(fakeupstream.StageReport))
	fu.Close()
}

// configure 应用 -latency 与 -inject。
func configure(fu *fakeupstream.Server, latency, inject string) error {
	kinds := map[string]fakeupstream.Kind{"chat": fakeupstream.Chat, "search": fakeupstream.Search, "fetch": fakeupstream.Fetch}
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
		return fmt.Errorf("-latency %q: 须为 kind=duration（kind 为 chat、search、fetch）", item)
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
	return nil
}
