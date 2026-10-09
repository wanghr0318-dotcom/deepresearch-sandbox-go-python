package main

import (
	"fmt"
	"os"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/cli"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/hostcheck"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/mcpdemo"
)

func main() {
	// stage-2 helper（沙箱 init 以 execveat 启动，降权后 execve workload）：同样早于任何其他初始化。
	// 成功时不返回；失败时已向 exec-status 管道报告并以 126 退出。
	if len(os.Args) > 1 && os.Args[1] == "exec-stage2" {
		runSandboxHelper()
		os.Exit(126)
	}
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
		for _, it := range r.Items {
			status := "通过"
			if !it.OK {
				status = "失败"
			}
			fmt.Printf("[%s] %s：%s\n", status, it.Name, it.Detail)
		}
		for _, w := range r.Warnings { // 警告不阻止启动（例如 aarch64 未经验证）
			fmt.Printf("[警告] %s\n", w)
		}
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
	if len(os.Args) > 1 && os.Args[1] == "cache" {
		os.Exit(runCache(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "eval" {
		os.Exit(eval.Main(os.Args[2:], os.Stdout, os.Stderr, os.Getenv))
	}
	if len(os.Args) > 1 && os.Args[1] == "user" {
		os.Exit(runUser(os.Args[2:], os.Stdout, os.Stderr))
	}
	// 演示用 MCP 服务器（stdio；docs/design/2026-10-10-shell-file-mcp-design.md）：由 Gateway 按 --mcp-config 启动。
	if len(os.Args) > 1 && os.Args[1] == "mcp-demo-server" {
		if err := mcpdemo.ServeStdio(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "mcp-demo-server:", err)
			os.Exit(1)
		}
		return
	}
	fmt.Fprintln(os.Stderr, "用法: agentbox doctor | server ... | verify-invariants [--quiescent] ... | cache rotate-key --data-dir DIR | user list|disable|enable ... | task ... | status | eval run|report|compare ... | mcp-demo-server")
	os.Exit(2)
}
