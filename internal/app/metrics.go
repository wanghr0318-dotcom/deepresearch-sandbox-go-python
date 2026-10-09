package app

import (
	"context"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// envStatser is the optional Store capability behind the sandbox environment gauges (internal/persistence/postgres
// implements it; test stores need not).
type envStatser interface {
	EnvironmentStats(ctx context.Context) (running map[string]int64, cleanupBacklog int64, err error)
}

// envStatsTTL bounds how often a scrape queries the database; envStatsTimeout bounds one query.
const (
	envStatsTTL     = 5 * time.Second
	envStatsTimeout = 2 * time.Second
)

// envKinds are the environment kinds always reported (0 when absent) so dashboards see stable series.
var envKinds = []string{"task", "session", "exec"}

// registerGauges registers the scrape-time gauges (only when metrics are on): run slots and memory from the
// admission gate, exec slots from the exec gate, live sandbox environments by kind and the cleanup backlog from
// the store (cached for envStatsTTL; on error the previous values are kept).
func (s *server) registerGauges() {
	if !obs.Metrics() {
		return
	}
	var (
		mu         sync.Mutex
		at         time.Time
		refreshing bool
		running    = map[string]int64{}
		backlog    int64
	)
	es, _ := s.store.(envStatser)
	// envs returns the cached values; at most one scrape at a time refreshes them, without holding mu during the
	// database query (concurrent scrapes get the previous values).
	envs := func() (map[string]int64, int64) {
		mu.Lock()
		stale := es != nil && !refreshing && time.Since(at) >= envStatsTTL
		if stale {
			refreshing = true
		}
		mu.Unlock()
		if stale {
			ctx, cancel := context.WithTimeout(context.Background(), envStatsTimeout)
			r, b, err := es.EnvironmentStats(ctx)
			cancel()
			mu.Lock()
			refreshing, at = false, time.Now()
			if err == nil {
				running, backlog = r, b
			}
			mu.Unlock()
			if err != nil {
				s.log.Debug("metrics: environment stats unavailable, keeping previous values", "error", err.Error())
			}
		}
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]int64, len(running))
		for k, v := range running {
			out[k] = v
		}
		return out, backlog
	}
	obs.M().RegisterGauges(func() []obs.Gauge {
		u := s.adm.Snapshot()
		g := []obs.Gauge{
			{Name: "run_slots_in_use", Help: "Run slots granted (running task attempts and session incarnations).", Value: float64(u.RunSlotsUsed)},
			{Name: "run_slots_capacity", Help: "Configured run slots (--run-slots).", Value: float64(u.Capacity.RunSlots)},
			{Name: "admission_queued", Help: "Requests waiting for a run slot or memory.", Value: float64(u.Queued)},
			{Name: "memory_reserved_bytes", Help: "Memory reserved by granted environments.", Value: float64(u.MemoryUsed)},
			{Name: "memory_capacity_bytes", Help: "Configured memory for all environments (--memory-bytes).", Value: float64(u.Capacity.MemoryBytes)},
		}
		if s.execGate != nil {
			e := s.execGate.Snapshot()
			g = append(g,
				obs.Gauge{Name: "exec_slots_in_use", Help: "Exec slots in use (/v1/exec sandboxes).", Value: float64(e.Used)},
				obs.Gauge{Name: "exec_slots_capacity", Help: "Configured exec slots (--exec-slots).", Value: float64(e.Capacity.Slots)},
				obs.Gauge{Name: "exec_queued", Help: "Exec calls waiting for a slot.", Value: float64(e.Queued)})
		}
		if es != nil {
			r, b := envs()
			for _, k := range envKinds {
				g = append(g, obs.Gauge{Name: "sandbox_envs", Help: "Sandbox environments not yet recorded as stopped, by kind.",
					Labels: map[string]string{"kind": k}, Value: float64(r[k])})
			}
			g = append(g, obs.Gauge{Name: "sandbox_cleanup_backlog", Help: "Stopped environments whose cleanup is not done.", Value: float64(b)})
		}
		if s.calls != nil {
			for _, rs := range s.calls.RouteStates() {
				g = append(g, obs.Gauge{Name: "breaker_state", Help: "Circuit breaker state of each model route: 0 closed, 1 half_open, 2 open.",
					Labels: map[string]string{"kind": string(rs.Kind), "route": rs.Route}, Value: breakerValue(rs.State)})
			}
		}
		return g
	})
}

// breakerValue maps a breaker state to the breaker_state gauge value.
func breakerValue(state string) float64 {
	switch state {
	case "open":
		return 2
	case "half_open":
		return 1
	}
	return 0
}
