package main

import (
	"archive/tar"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
)

func runCollectOutput(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("collect-output", flag.ContinueOnError)
	maxFiles := flags.Int("max", -1, "maximum number of regular files")
	maxBytes := flags.Int64("bytes", -1, "maximum number of payload bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *maxFiles < 0 || *maxBytes < 0 || flags.NArg() != 0 {
		return errors.New("collect-output requires nonnegative --max and --bytes")
	}
	return collectOutput("/out", stdout, *maxFiles, *maxBytes)
}

// collectOutput is called in the transfer container after Kubernetes confirms
// that the sandbox has terminated. Its /out mount is read-only.
func collectOutput(dir string, dst io.Writer, maxFiles int, maxBytes int64) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()

	type entry struct {
		path string
		kind fs.FileMode
	}
	var outputs []entry
	if err := fs.WalkDir(root.FS(), ".", func(name string, item fs.DirEntry, err error) error {
		if err == nil && !item.IsDir() {
			outputs = append(outputs, entry{name, item.Type()})
		}
		return err
	}); err != nil {
		return err
	}
	slices.SortFunc(outputs, func(a, b entry) int { return strings.Compare(a.path, b.path) })

	archive := tar.NewWriter(dst)
	count := 0
	for _, entry := range outputs {
		name := entry.path
		reason := ""
		switch {
		case entry.kind&fs.ModeSymlink != 0:
			reason = "symlink"
		case !entry.kind.IsRegular():
			reason = "not_regular"
		case count >= maxFiles:
			reason = "too_many"
		}
		if reason != "" {
			if err := skipOutput(archive, name, reason); err != nil {
				return err
			}
			continue
		}
		size, err := archiveOutputFile(root, archive, name, maxBytes)
		if err != nil {
			return err
		}
		if size >= 0 {
			count++
			maxBytes -= size
		}
	}
	return archive.Close()
}

func skipOutput(archive *tar.Writer, name, reason string) error {
	return archive.WriteHeader(&tar.Header{
		Name: name, Typeflag: tar.TypeReg,
		PAXRecords: map[string]string{"AGENTBOX.skip": reason},
	})
}

// archiveOutputFile returns -1 when a file could not be opened for collection.
func archiveOutputFile(root *os.Root, archive *tar.Writer, name string, maxBytes int64) (int64, error) {
	file, err := root.Open(name)
	if err != nil {
		return -1, skipOutput(archive, name, "open_failed")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return -1, skipOutput(archive, name, "open_failed")
	}
	if info.Size() > maxBytes {
		return 0, fmt.Errorf("output exceeds byte limit at %q", name)
	}
	if err := archive.WriteHeader(&tar.Header{
		Name: name, Typeflag: tar.TypeReg, Mode: 0o444 | int64(info.Mode().Perm()&0o111), Size: info.Size(),
	}); err != nil {
		return 0, err
	}
	_, err = io.CopyN(archive, file, info.Size())
	return info.Size(), err
}
