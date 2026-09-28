//go:build !linux

package hostcheck

import "errors"

func checkCgroupV2(path string) (bool, error) {
	return false, errors.New("cgroup v2 检查仅在 Linux 上可用")
}
