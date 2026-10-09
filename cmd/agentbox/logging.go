package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// serverLogger returns the server's JSON logger: stderr, plus logFile (appended, 0600) when set. Records logged
// with a context carrying an active span get trace_id/span_id (obs.LogHandler); with tracing off the output is
// unchanged.
func serverLogger(logFile string, stderr io.Writer) (*slog.Logger, func(), error) {
	w, closeFn := stderr, func() {}
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			return nil, nil, fmt.Errorf("--log-file: %w", err)
		}
		w, closeFn = io.MultiWriter(stderr, f), func() { _ = f.Close() }
	}
	return slog.New(obs.LogHandler(slog.NewJSONHandler(w, nil))), closeFn, nil
}

// buildVersion is the main module version from the build info ("(devel)" for local builds).
func buildVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "(devel)"
}
