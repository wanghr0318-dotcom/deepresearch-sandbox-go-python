package main

import (
	"context"
	"sync"
	"time"
)

// gen builds the i-th request of worker w.
type gen func(w, i int) reqSpec

// closedLoop runs conc workers, each sending its next request as soon as the previous one is done, for dur or until
// ctx ends. Requests in flight then are completed (they do not inherit ctx's cancellation) and counted.
func closedLoop(ctx context.Context, conc int, dur time.Duration, run func(ctx context.Context, s reqSpec) result, g gen) ([]result, time.Duration) {
	var mu sync.Mutex
	var out []result
	start := time.Now()
	deadline := start.Add(dur)
	var wg sync.WaitGroup
	for w := range conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []result
			for i := 0; time.Now().Before(deadline) && ctx.Err() == nil; i++ {
				local = append(local, run(context.WithoutCancel(ctx), g(w, i)))
			}
			mu.Lock()
			out = append(out, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, time.Since(start)
}

// openLoop sends requests at a fixed rate (per second) for dur, whatever the latency, with at most maxInflight in
// flight (a request that would exceed it is counted as "skipped" by the caller through the returned count).
func openLoop(ctx context.Context, rate float64, dur time.Duration, maxInflight int, run func(ctx context.Context, s reqSpec) result,
	g gen) ([]result, int, time.Duration) {
	var mu sync.Mutex
	var out []result
	sem := make(chan struct{}, maxInflight)
	interval := time.Duration(float64(time.Second) / rate)
	start := time.Now()
	deadline := start.Add(dur)
	skipped := 0
	var wg sync.WaitGroup
	for i := 0; ; i++ {
		next := start.Add(time.Duration(i) * interval)
		if !next.Before(deadline) || ctx.Err() != nil {
			break
		}
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		select {
		case sem <- struct{}{}:
		default:
			skipped++
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := run(ctx, g(0, i))
			<-sem
			mu.Lock()
			out = append(out, r)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, skipped, time.Since(start)
}

// perSecond groups results by the second (since start) in which they started.
type secondStats struct {
	Second   int     `json:"t"`
	Requests int     `json:"requests"`
	Errors   int     `json:"errors"`
	P50      float64 `json:"p50_ms"`
	P99      float64 `json:"p99_ms"`
	Max      float64 `json:"max_ms"`
}

func perSecond(rs []result, start time.Time) []secondStats {
	buckets := map[int][]result{}
	last := 0
	for _, r := range rs {
		s := int(r.Start.Sub(start) / time.Second)
		buckets[s] = append(buckets[s], r)
		last = max(last, s)
	}
	out := make([]secondStats, 0, last+1)
	for s := 0; s <= last; s++ {
		b := buckets[s]
		st := secondStats{Second: s, Requests: len(b)}
		var ds []time.Duration
		for _, r := range b {
			if !r.ok() {
				st.Errors++
				continue
			}
			ds = append(ds, r.Total)
		}
		d := distOf(ds)
		st.P50, st.P99, st.Max = d.P50, d.P99, d.Max
		out = append(out, st)
	}
	return out
}
