package main

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCollectOutput(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "report"), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{"report-early.txt": "hello", "report/result.txt": "world"} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(path)), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var wire bytes.Buffer
	if err := collectOutput(dir, &wire, 1, 5); err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(&wire)
	h, err := reader.Next()
	if err != nil || h.Name != "report-early.txt" || h.Size != 5 {
		t.Fatalf("first header = %+v, error %v", h, err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "hello" {
		t.Fatalf("contents = %q, error %v", data, err)
	}
	h, err = reader.Next()
	if err != nil || h.Name != "report/result.txt" || h.Size != 0 || h.PAXRecords["AGENTBOX.skip"] != "too_many" {
		t.Fatalf("skip header = %+v, error %v", h, err)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("archive terminator: %v", err)
	}
	if err := collectOutput(dir, io.Discard, 2, 4); err == nil {
		t.Fatal("expected payload budget error")
	}
}
