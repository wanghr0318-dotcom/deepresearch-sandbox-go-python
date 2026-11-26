package main

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStageInput(t *testing.T) {
	for _, tc := range []struct {
		name      string
		path      string
		size      int64
		data      string
		wantError bool
	}{
		{name: "nested input file", path: "ws/records.py", size: 4, data: "test"},
		{name: "incomplete transfer", path: "ws/records.py", size: 4, data: "te", wantError: true},
		{name: "over byte budget", path: "ws/records.py", size: stageInputBytes + 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire bytes.Buffer
			w := tar.NewWriter(&wire)
			if err := w.WriteHeader(&tar.Header{Name: tc.path, Size: tc.size, Mode: 0o644, Typeflag: tar.TypeReg}); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write([]byte(tc.data)); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil && !tc.wantError {
				t.Fatal(err)
			}
			dir := t.TempDir()
			if err := stageInput(dir, &wire); tc.wantError {
				if err == nil {
					t.Fatal("expected staging to fail")
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(tc.path)))
			if err != nil || string(contents) != tc.data {
				t.Fatalf("staged content %q, error %v", contents, err)
			}
		})
	}
}
