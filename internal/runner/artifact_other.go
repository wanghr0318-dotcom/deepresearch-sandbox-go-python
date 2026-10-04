//go:build !linux

package runner

import (
	"errors"
	"os"
)

// errNoOpenat2 说明产物保存依赖 Linux 的 openat2(2)；其他平台只为能够编译。
var errNoOpenat2 = errors.New("runner: 产物保存需要 Linux openat2")

type outDir struct{}

func openOutDir(string) (*outDir, error) { return nil, errNoOpenat2 }

func (*outDir) open(string) (*os.File, int64, error) {
	return nil, 0, &artifactError{code: codePathInvalid, err: errNoOpenat2}
}

func (*outDir) Close() error { return nil }
