package obs

import (
	"context"
	"sync"
	"time"
)

// The submit registry links the API request that created a task (or turn) to the task actor that runs it
// later, asynchronously: the API notes the request's traceparent and the submit time under the task id, the
// actor takes it when it loads the task. It is in-memory and best-effort: bounded to registryMax entries,
// entries older than registryTTL are dropped, and it is empty after a restart (the task span is then a new
// root and the submit→ready latency is not observed).

const (
	registryMax = 4096
	registryTTL = time.Hour
)

type submitted struct {
	traceparent string
	at          time.Time
}

var reg = struct {
	sync.Mutex
	m map[string]submitted
}{m: map[string]submitted{}}

// NoteSubmit records that taskID was submitted by the request in ctx. No-op when observability is off.
func NoteSubmit(ctx context.Context, taskID string) {
	if taskID == "" || !Enabled() {
		return
	}
	now := time.Now()
	s := submitted{traceparent: Traceparent(ctx), at: now}
	reg.Lock()
	defer reg.Unlock()
	if len(reg.m) >= registryMax {
		for k, v := range reg.m {
			if now.Sub(v.at) > registryTTL {
				delete(reg.m, k)
			}
		}
		if len(reg.m) >= registryMax {
			return // full of fresh entries: drop (best-effort)
		}
	}
	reg.m[taskID] = s
}

// TakeSubmit removes and returns the submission noted for taskID.
func TakeSubmit(taskID string) (traceparent string, at time.Time, ok bool) {
	reg.Lock()
	defer reg.Unlock()
	s, ok := reg.m[taskID]
	if !ok {
		return "", time.Time{}, false
	}
	delete(reg.m, taskID)
	if time.Since(s.at) > registryTTL {
		return "", time.Time{}, false
	}
	return s.traceparent, s.at, true
}
