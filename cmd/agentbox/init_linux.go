//go:build linux

package main

import "github.com/wanghr0318-dotcom/go-agentbox/internal/runtime"

func runSandboxInit() error { return runtime.RunInit() }
