package k8s

import (
	"sort"
	"sync"
	"time"
)

// LatencySummary summarises start latencies (Create until the environment accepts StartExec).
type LatencySummary struct {
	Count    int
	P50, P95 time.Duration
}

type stats struct {
	mu      sync.Mutex
	samples map[string][]time.Duration
}

func newStats() *stats { return &stats{samples: map[string][]time.Duration{}} }

func (s *stats) add(path string, d time.Duration) {
	s.mu.Lock()
	s.samples[path] = append(s.samples[path], d)
	s.mu.Unlock()
}

func (s *stats) summary() map[string]LatencySummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]LatencySummary{}
	for k, v := range s.samples {
		out[k] = Summarize(v)
	}
	return out
}

// Summarize returns the nearest-rank P50 and P95 of ds.
func Summarize(ds []time.Duration) LatencySummary {
	if len(ds) == 0 {
		return LatencySummary{}
	}
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	rank := func(q float64) time.Duration {
		i := int(q*float64(len(c))+0.999999) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(c) {
			i = len(c) - 1
		}
		return c[i]
	}
	return LatencySummary{Count: len(c), P50: rank(0.50), P95: rank(0.95)}
}
