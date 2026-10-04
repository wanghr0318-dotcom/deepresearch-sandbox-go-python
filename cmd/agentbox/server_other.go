//go:build !linux

package main

import (
	"fmt"
	"io"
)

// 执行主机只支持 Linux（provider/local 只在 Linux 上构建）；其他平台只为开发机能够编译。

func runServer(_ []string, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox server 仅在 Linux 上可用")
	return 1
}

func runVerify(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox verify-invariants 仅在 Linux 上可用")
	return 2
}
