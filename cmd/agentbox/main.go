package main

import (
	"fmt"
	"os"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cli"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/hostcheck"
)

func main() {
	// init 分流必须是 main 的第一件事，早于任何其他初始化。
	// 这个分支是专用启动进程以 /proc/self/exe init 启动的沙箱 1 号进程。
	if len(os.Args) > 1 && os.Args[1] == "init" {
		if err := runSandboxInit(); err != nil {
			fmt.Fprintln(os.Stderr, "sandbox init:", err)
			os.Exit(1)
		}
		return
	}
	// 专用启动进程（每个环境一个，短生命周期）：server 以 /proc/self/exe sandbox-launch 启动它，
	// 它清空自身附加组后在命名空间中启动 init（规格 §4.6）。同样早于任何其他初始化。
	if len(os.Args) > 1 && os.Args[1] == "sandbox-launch" {
		if err := runSandboxLaunch(); err != nil {
			fmt.Fprintln(os.Stderr, "sandbox-launch:", err)
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
	if len(os.Args) > 1 && (os.Args[1] == "task" || os.Args[1] == "status") {
		os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "server" {
		os.Exit(runServer(os.Args[2:], os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "verify-invariants" {
		os.Exit(runVerify(os.Args[2:], os.Stdout, os.Stderr))
	}
	fmt.Fprintln(os.Stderr, "用法: agentbox doctor | server ... | verify-invariants [--quiescent] ... | task ... | status")
	os.Exit(2)
}
