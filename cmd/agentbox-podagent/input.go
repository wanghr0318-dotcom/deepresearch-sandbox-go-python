package main

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const stageInputBytes = 256 << 20
const stageInputFiles = 1024

// stageInput materializes a bounded archive into a fresh workspace volume.
// Any failure leaves the execution unstarted; the caller discards the Pod.
func stageInput(dir string, src io.Reader) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()

	archive := tar.NewReader(src)
	remaining := int64(stageInputBytes)
	for count := 0; ; count++ {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read input archive: %w", err)
		}
		if count >= stageInputFiles || header.Size < 0 || header.Size > remaining {
			return errors.New("input archive exceeds file or byte limit")
		}
		if header.Typeflag != tar.TypeReg || !fs.ValidPath(header.Name) || header.Name == "." {
			return fmt.Errorf("invalid input entry %q", header.Name)
		}
		remaining -= header.Size
		mode := os.FileMode(0o444)
		if header.Mode&0o111 != 0 {
			mode = 0o555
		}
		name, err := filepath.Localize(header.Name)
		if err != nil {
			return err
		}
		if err := root.MkdirAll(filepath.Dir(name), 0o750); err != nil {
			return err
		}
		f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		_, copyErr := io.CopyN(f, archive, header.Size)
		if err := errors.Join(copyErr, f.Close()); err != nil {
			return fmt.Errorf("write input %q: %w", header.Name, err)
		}
	}
}
