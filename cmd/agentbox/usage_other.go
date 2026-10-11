//go:build !linux

package main

import (
	"fmt"
	"io"
)

// 用量与账单命令与 budget 一样只在执行主机（Linux）上提供；其他平台只为开发机能够编译。

func runUsage(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox usage 仅在 Linux 上可用")
	return 1
}

func runBill(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox bill 仅在 Linux 上可用")
	return 1
}
