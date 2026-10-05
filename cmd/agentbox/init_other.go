//go:build !linux

package main

import (
	"errors"
	"fmt"
	"os"
)

func runSandboxInit() error { return errors.New("沙箱 init 仅在 Linux 上可用") }

func runSandboxLaunch() error { return errors.New("沙箱启动进程仅在 Linux 上可用") }

func runSandboxHelper() { fmt.Fprintln(os.Stderr, "stage-2 helper 仅在 Linux 上可用") }
