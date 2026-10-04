// Package admission is the in-memory capacity gate: run slots and environment
// memory are granted to tasks in strict FIFO order. A stopped-but-unconfirmed
// environment stays occupied; the caller releases only after stopped_at is
// durable (spec section 14.5). Exec slots belong to M4 and are not modeled.
package admission

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrExceedsCapacity is returned when a request can never be satisfied.
var ErrExceedsCapacity = errors.New("admission: request exceeds total capacity")

// Capacity is the total capacity of the gate.
type Capacity struct {
	RunSlots    int
	MemoryBytes int64
}

// Request asks for one run slot plus MemoryBytes.
type Request struct {
	TaskID      string
	MemoryBytes int64
}

// Grant is a held slice of capacity. Release is keyed by ID.
type Grant struct {
	ID          uint64
	TaskID      string
	MemoryBytes int64
}

// Usage is a point-in-time view of the gate.
type Usage struct {
	Capacity     Capacity
	RunSlotsUsed int
	MemoryUsed   int64
	Queued       int
}

type waiter struct {
	req     Request
	ready   chan struct{} // closed when granted
	granted bool
	grant   Grant
}

// Admission grants capacity in strict FIFO order.
type Admission struct {
	mu     sync.Mutex
	cap    Capacity
	held   map[uint64]Grant
	memory int64
	nextID uint64
	queue  *list.List // of *waiter
}

// New returns an empty gate with the given capacity.
func New(c Capacity) *Admission {
	return &Admission{cap: c, held: map[uint64]Grant{}, queue: list.New()}
}

func (a *Admission) fits(r Request) bool {
	return len(a.held) < a.cap.RunSlots && a.memory+r.MemoryBytes <= a.cap.MemoryBytes
}

func (a *Admission) grantLocked(r Request) Grant {
	a.nextID++
	g := Grant{ID: a.nextID, TaskID: r.TaskID, MemoryBytes: r.MemoryBytes}
	a.held[g.ID] = g
	a.memory += g.MemoryBytes
	return g
}

// dispatchLocked grants to queue heads while they fit; a head that does not
// fit blocks everything behind it.
func (a *Admission) dispatchLocked() {
	for e := a.queue.Front(); e != nil; e = a.queue.Front() {
		w := e.Value.(*waiter)
		if !a.fits(w.req) {
			return
		}
		a.queue.Remove(e)
		w.grant = a.grantLocked(w.req)
		w.granted = true
		close(w.ready)
	}
}

// Acquire blocks until the request is granted in FIFO order. If ctx ends first
// it returns the ctx error and holds nothing. A request that can never fit the
// total capacity fails immediately with ErrExceedsCapacity.
func (a *Admission) Acquire(ctx context.Context, r Request) (Grant, error) {
	if err := ctx.Err(); err != nil {
		return Grant{}, err
	}
	if r.MemoryBytes < 0 {
		return Grant{}, fmt.Errorf("admission: negative memory request %d", r.MemoryBytes)
	}
	a.mu.Lock()
	if a.cap.RunSlots < 1 || r.MemoryBytes > a.cap.MemoryBytes {
		a.mu.Unlock()
		return Grant{}, ErrExceedsCapacity
	}
	if a.queue.Len() == 0 && a.fits(r) {
		g := a.grantLocked(r)
		a.mu.Unlock()
		return g, nil
	}
	w := &waiter{req: r, ready: make(chan struct{})}
	e := a.queue.PushBack(w)
	a.mu.Unlock()

	select {
	case <-w.ready:
		return w.grant, nil // w.grant written before close(ready)
	case <-ctx.Done():
		a.mu.Lock()
		defer a.mu.Unlock()
		if w.granted { // lost the race: give the capacity back
			a.releaseLocked(w.grant.ID)
			a.dispatchLocked()
		} else {
			a.queue.Remove(e)
			a.dispatchLocked() // the head may have changed
		}
		return Grant{}, ctx.Err()
	}
}

func (a *Admission) releaseLocked(id uint64) {
	g, ok := a.held[id]
	if !ok {
		return
	}
	delete(a.held, id)
	a.memory -= g.MemoryBytes
}

// Release returns a grant's capacity. Idempotent: unknown or already released
// IDs are ignored. Amounts come from the recorded grant, not the argument.
func (a *Admission) Release(g Grant) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releaseLocked(g.ID)
	a.dispatchLocked()
}

// Rebuild replaces the held set with the occupancy reported by recovery.
// Queued waiters are kept and re-evaluated. Later grants get IDs above every
// rebuilt ID. Occupancy may exceed capacity; nothing new is granted until it
// drains.
func (a *Admission) Rebuild(occupied []Grant) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.held = make(map[uint64]Grant, len(occupied))
	a.memory = 0
	for _, g := range occupied {
		if _, dup := a.held[g.ID]; dup {
			continue
		}
		a.held[g.ID] = g
		a.memory += g.MemoryBytes
		if g.ID > a.nextID {
			a.nextID = g.ID
		}
	}
	a.dispatchLocked()
}

// Snapshot returns the current usage.
func (a *Admission) Snapshot() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Usage{Capacity: a.cap, RunSlotsUsed: len(a.held), MemoryUsed: a.memory, Queued: a.queue.Len()}
}
