package main

import (
	"fmt"
	"os"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/hostcheck"
)

func main() {
	// init 分流必须是 main 的第一件事，早于任何其他初始化。
	// 这个分支是被 Spawn 以 /proc/self/exe init 启动的沙箱 1 号进程。
	if len(os.Args) > 1 && os.Args[1] == "init" {
		if err := runSandboxInit(); err != nil {
			fmt.Fprintln(os.Stderr, "sandbox init:", err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "doctor" {
		r := hostcheck.Check()
		if err := r.Err(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("宿主环境检查通过")
		return
	}
	fmt.Fprintln(os.Stderr, "用法: agentbox doctor")
	os.Exit(2)
}
