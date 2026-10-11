//go:build !linux

package main

import (
	"fmt"
	"io"
)

// org 与 key 管理与 user 一样只在执行主机（Linux）上提供；其他平台只为开发机能够编译。

func runOrg(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox org 仅在 Linux 上可用")
	return 1
}

func runKey(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox key 仅在 Linux 上可用")
	return 1
}

func runLimits(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox limits 仅在 Linux 上可用")
	return 1
}

func runBudget(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox budget 仅在 Linux 上可用")
	return 1
}
