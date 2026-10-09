package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServerLoggerWritesStderrAndFile(t *testing.T) {
	var stderr bytes.Buffer
	path := filepath.Join(t.TempDir(), "server.log")
	log, closeFn, err := serverLogger(path, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("hello", "k", 1)
	closeFn()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), `"msg":"hello"`) || string(b) != stderr.String() {
		t.Errorf("stderr %q file %q", stderr.String(), b)
	}
	if strings.Contains(string(b), "trace_id") {
		t.Error("trace_id without a span")
	}
	if _, _, err := serverLogger(filepath.Join(t.TempDir(), "missing", "x.log"), &stderr); err == nil {
		t.Error("unwritable log file accepted")
	}
}
