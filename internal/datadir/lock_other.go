//go:build !unix

package datadir

import (
	"errors"
	"os"
)

// 执行主机只支持 Linux；其他平台只为开发机能够编译。
func lockFile(*os.File) error { return errors.New("datadir: flock 只在 unix 上支持") }

func syncDir(string) error { return nil }
