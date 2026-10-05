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
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/tests/e2e/fakeupstream"
)

// researchTasks 是固定研究题目的计划（与 tests/e2e 的业务验收相同）。
var researchTasks = []fakeupstream.ResearchTask{
	{Title: "电解质路线", Intent: "比较硫化物、氧化物与聚合物电解质", Query: "固态电解质 技术路线"},
	{Title: "量产与成本", Intent: "梳理量产时间表与成本结构", Query: "固态电池 量产 成本"},
}

func main() {
	hold := flag.Bool("hold-first-chat", false, "第一次 chat 请求挂起，直到收到 SIGUSR1")
	flag.Parse()

	fu := fakeupstream.New()
	fu.SetResearch(researchTasks)
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
