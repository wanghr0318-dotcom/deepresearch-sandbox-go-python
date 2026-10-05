//go:build !linux

package main

import (
	"fmt"
	"io"
)

// 用户管理与 server 同样只在执行主机（Linux）上提供；其他平台只为开发机能够编译。

func runUser(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agentbox user 仅在 Linux 上可用")
	return 1
}
