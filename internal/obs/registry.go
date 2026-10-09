package obs

import (
	"context"
	"sync"
	"time"
)

// In-memory, best-effort registries that connect work happening in different goroutines/contexts:
//
//   - submissions: the API notes the traceparent and time of the request that starts a run of a task (create,
//     resume, continue, answer); the task actor takes it when the run starts, so the run's trace is rooted at that
//     request and submit→ready is measured;
//   - stops: the API notes when a pause/cancel/stop was accepted; the actor takes it when it observes the control,
//     so stop latency is measured from the accepted request;
//   - worker spans: the runner notes the traceparent it hands to the worker (init / task_start) for the attempt;
//     the Gateway edge accepts a worker's traceparent header only when it names exactly that span.
//
// Each is bounded (registryMax entries, older than registryTTL dropped) and empty after a restart.

const (
	registryMax = 4096
	registryTTL = time.Hour
)

type entry struct {
	value string
	at    time.Time
}

type registry struct {
	mu sync.Mutex
	m  map[string]entry
}

func newRegistry() *registry { return &registry{m: map[string]entry{}} }

func (r *registry) put(key, value string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[key]; !exists && len(r.m) >= registryMax {
		for k, v := range r.m {
			if now.Sub(v.at) > registryTTL {
				delete(r.m, k)
			}
		}
		if len(r.m) >= registryMax {
			return // full of fresh entries: drop (best-effort)
		}
	}
	r.m[key] = entry{value: value, at: now}
}

func (r *registry) take(key string) (entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[key]
	if !ok {
		return entry{}, false
	}
	delete(r.m, key)
	if time.Since(e.at) > registryTTL {
		return entry{}, false
	}
	return e, true
}

func (r *registry) get(key string) (entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[key]
	return e, ok
}

// dropBefore removes key when its entry was noted at or before t.
func (r *registry) dropBefore(key string, t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.m[key]; ok && !e.at.After(t) {
		delete(r.m, key)
	}
}

func (r *registry) remove(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.m[key]; ok && e.value == value {
		delete(r.m, key)
	}
}

var (
	submits = newRegistry()
	stops   = newRegistry()
	workers = newRegistry()
)

// NoteSubmit records that a run of taskID was started by the request in ctx. No-op when observability is off.
func NoteSubmit(ctx context.Context, taskID string) {
	if taskID != "" && Enabled() {
		submits.put(taskID, Traceparent(ctx), time.Now())
	}
}

// TakeSubmit removes and returns the submission noted for taskID.
func TakeSubmit(taskID string) (traceparent string, at time.Time, ok bool) {
	e, ok := submits.take(taskID)
	return e.value, e.at, ok
}

// DropSubmitBefore discards a submission of taskID noted at or before t (stale once the run it belonged to has
// ended).
func DropSubmitBefore(taskID string, t time.Time) { submits.dropBefore(taskID, t) }

// NoteStop records when a pause/cancel/stop of taskID was accepted. No-op when observability is off.
func NoteStop(taskID string) {
	if taskID != "" && Enabled() {
		stops.put(taskID, "", time.Now())
	}
}

// TakeStop removes and returns the accepted time of a stop of taskID.
func TakeStop(taskID string) (time.Time, bool) {
	e, ok := stops.take(taskID)
	return e.at, ok
}

// ExpectWorker records the traceparent handed to the worker of attemptID and returns a function removing it.
// No-op for an empty traceparent.
func ExpectWorker(attemptID, traceparent string) (done func()) {
	if attemptID == "" || traceparent == "" {
		return func() {}
	}
	workers.put(attemptID, traceparent, time.Now())
	return func() { workers.remove(attemptID, traceparent) }
}

// ExpectedWorker returns the traceparent handed to the worker of attemptID ("" when none).
func ExpectedWorker(attemptID string) string {
	e, _ := workers.get(attemptID)
	return e.value
}
