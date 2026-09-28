// Package testutil 提供测试前置条件检查。
package testutil

import (
	"os"
	"runtime"
	"testing"
)

// RequireLinuxRoot 在当前环境不是 Linux 或不是 root 时跳过测试。
// 沙箱相关测试必须调用它——条件不满足时应跳过而非失败，
// 否则在开发者的 Windows/macOS 上跑 go test ./... 会全线报错。
func RequireLinuxRoot(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skipf("需要 Linux，当前为 %s", runtime.GOOS)
	}
	if os.Geteuid() != 0 {
		t.Skip("需要 root 权限")
	}
}
