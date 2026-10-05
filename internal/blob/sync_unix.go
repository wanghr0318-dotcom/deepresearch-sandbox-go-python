//go:build unix

package blob

import (
	"fmt"
	"os"
)

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("blob: 打开目录以 fsync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("blob: fsync 目录: %w", err)
	}
	return nil
}
