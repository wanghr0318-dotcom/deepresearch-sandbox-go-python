//go:build linux

package main

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"
)

func runSandboxInit() error { return sandbox.RunInit() }

func runSandboxLaunch() error { return sandbox.RunLaunch() }

// runSandboxHelper 运行 stage-2 helper：成功时 execve workload，不返回；失败时报告并以 126 退出。
func runSandboxHelper() { sandbox.RunHelper() }

// runCache 是 `agentbox cache rotate-key --data-dir DIR`（规格 §11.5）：当前缓存签名密钥成为上一密钥，
// 生成新的当前密钥。持有数据目录锁，因此只能在服务停止时轮换（服务启动时加载密钥，重启后生效）。
func runCache(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "rotate-key" {
		fmt.Fprintln(stderr, "用法: agentbox cache rotate-key --data-dir DIR")
		return 2
	}
	fs := flag.NewFlagSet("cache rotate-key", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", "", "数据目录（必填）")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *dataDir == "" || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "用法: agentbox cache rotate-key --data-dir DIR")
		return 2
	}
	dir, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox cache rotate-key:", err)
		return 1
	}
	lock, err := datadir.Acquire(dir)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox cache rotate-key: 取得数据目录锁失败（服务运行中须先停止）:", err)
		return 1
	}
	defer func() { _ = lock.Release() }() // 进程即将退出，释放错误无影响
	if err := cache.RotateKey(dir); err != nil {
		fmt.Fprintln(stderr, "agentbox cache rotate-key:", err)
		return 1
	}
	fmt.Fprintln(stdout, "缓存签名密钥已轮换；上一密钥仍可验证，重启服务后生效")
	return 0
}
