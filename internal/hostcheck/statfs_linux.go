//go:build linux

package hostcheck

import "syscall"

func checkCgroupV2(path string) (bool, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false, err
	}
	return int64(st.Type) == cgroup2Magic, nil
}
