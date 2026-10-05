//go:build linux

package main

import "github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"

func runSandboxInit() error { return sandbox.RunInit() }

func runSandboxLaunch() error { return sandbox.RunLaunch() }
