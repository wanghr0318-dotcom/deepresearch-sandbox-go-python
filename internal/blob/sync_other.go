//go:build !unix

package blob

// 执行主机只支持 Linux；其他平台只为开发机能够编译。
func syncDir(string) error { return nil }
