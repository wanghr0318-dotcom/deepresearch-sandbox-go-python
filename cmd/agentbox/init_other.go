//go:build !linux

package main

import "errors"

func runSandboxInit() error { return errors.New("沙箱 init 仅在 Linux 上可用") }

func runSandboxLaunch() error { return errors.New("沙箱启动进程仅在 Linux 上可用") }
