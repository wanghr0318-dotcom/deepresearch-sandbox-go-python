//go:build linux && cgo

package sandbox

// 本文件只在 cgo 构建中编译（规格 §4.6 实现门槛 3）：helper 的身份与能力调用经 syscall.AllThreadsSyscall
// 作用于全部线程，cgo 下该调用返回 ENOTSUP（libc 创建的线程不受 Go 运行时控制）。生产二进制与 CI 的
// linux-integration 固定 CGO_ENABLED=0；若 helper 仍以 cgo 构建运行，它在做任何事之前以
// start_err{helper/cgo_enabled: …} 拒绝运行，workload 不运行（RunHelper 的第一步）。
//
// 只影响 helper：server 与测试进程不受影响（非 root 的 -race 套件需要 cgo，其中需要 root 的沙箱用例跳过）。
// init 的能力设置同样经 AllThreadsSyscall，cgo 下以 init_err{init/init_caps: …} 失败关闭。

func init() { cgoBuild = true }
