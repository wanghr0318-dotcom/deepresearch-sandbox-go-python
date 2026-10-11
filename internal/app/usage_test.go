package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

type countingUsage struct {
	usage.Store
	runs atomic.Int32
	fail bool
}

func (c *countingUsage) Rollup(context.Context, time.Time) (usage.RollupResult, error) {
	c.runs.Add(1)
	if c.fail {
		return usage.RollupResult{}, errors.New("db down")
	}
	return usage.RollupResult{}, nil
}

// The rollup runs once at start, then every interval (errors are logged and retried), until the context ends.
func TestRollupUsageLoop(t *testing.T) {
	for _, fail := range []bool{false, true} {
		s := &server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		u := &countingUsage{fail: fail}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.rollupUsage(ctx, u, 5*time.Millisecond)
		}()
		deadline := time.Now().Add(5 * time.Second)
		for u.runs.Load() < 3 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		cancel()
		<-done
		if u.runs.Load() < 3 {
			t.Fatalf("fail=%v: %d runs", fail, u.runs.Load())
		}
	}
}
