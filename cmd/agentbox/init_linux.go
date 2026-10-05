//go:build linux

package main

import "github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"

func runSandboxInit() error { return sandbox.RunInit() }

func runSandboxLaunch() error { return sandbox.RunLaunch() }

// runSandboxHelper 运行 stage-2 helper：成功时 execve workload，不返回；失败时报告并以 126 退出。
func runSandboxHelper() { sandbox.RunHelper() }
