package ratelimit

import "sync"

// Slot is one concurrency limit: at most Limit concurrent holders of Scope (for example "key:k_abc…:streams").
type Slot struct {
	Scope string
	Limit int64
}

// Concurrency counts concurrent holders per scope in process (model streams, §6.1). Acquire is all-or-nothing over
// all slots.
type Concurrency struct {
	mu sync.Mutex
	n  map[string]int64
}

// NewConcurrency returns an empty counter set.
func NewConcurrency() *Concurrency { return &Concurrency{n: map[string]int64{}} }

// Acquire takes one unit of every slot, or none: when a slot is full it returns ok false and that slot. The returned
// release function gives the units back; it is idempotent. A scope listed more than once counts once, against the
// smallest of its limits.
func (c *Concurrency) Acquire(slots []Slot) (release func(), full Slot, ok bool) {
	slots = dedupe(slots)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range slots {
		if c.n[s.Scope] >= s.Limit {
			return func() {}, s, false
		}
	}
	for _, s := range slots {
		c.n[s.Scope]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			for _, s := range slots {
				if c.n[s.Scope]--; c.n[s.Scope] <= 0 {
					delete(c.n, s.Scope)
				}
			}
		})
	}, Slot{}, true
}

// InUse returns the current count of scope.
func (c *Concurrency) InUse(scope string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[scope]
}

// dedupe copies slots with each scope once (the smallest limit wins), so a scope listed twice counts one unit.
func dedupe(slots []Slot) []Slot {
	out := make([]Slot, 0, len(slots))
	at := make(map[string]int, len(slots))
	for _, s := range slots {
		if i, ok := at[s.Scope]; ok {
			out[i].Limit = min(out[i].Limit, s.Limit)
			continue
		}
		at[s.Scope] = len(out)
		out = append(out, s)
	}
	return out
}
