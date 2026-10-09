// Package breaker is the Gateway's consecutive-failure circuit breaker, shared by the Redis cache
// (internal/gateway/cache, spec §11.5) and the model provider chain (internal/gateway/call routing,
// docs/design/2026-10-10-model-fallback-design.md §4.4).
//
// State machine: closed —FailureThreshold consecutive failures→ open —OpenDuration elapsed, next Allow→
// half-open (exactly one probe in flight) —probe succeeds→ closed / probe fails→ open again.
//
// The package depends only on the standard library.
package breaker

import (
	"sync"
	"time"
)

// Defaults (spec §11.5, §19): the values the Redis cache has always used.
const (
	DefaultFailureThreshold = 5
	DefaultOpenDuration     = 30 * time.Second
)

// State is a breaker state.
type State int

const (
	StateClosed   State = iota // normal: everything is allowed
	StateOpen                  // everything is refused until the open duration has elapsed
	StateHalfOpen              // after the open duration: a single probe is allowed
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Config configures a Breaker. Zero fields take the defaults; Now nil means time.Now.
// OnTransition, if set, is called (outside the breaker's lock) after every state change.
type Config struct {
	FailureThreshold int
	OpenDuration     time.Duration
	Now              func() time.Time
	OnTransition     func(from, to State)
}

// Breaker is a consecutive-failure circuit breaker. The zero value is usable (defaults, time.Now).
// Every Allow that returns true must be followed by exactly one Success, Failure or Abort, otherwise a
// half-open probe never ends.
type Breaker struct {
	cfg Config

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool
}

// New creates a breaker with the given configuration.
func New(cfg Config) *Breaker { return &Breaker{cfg: cfg} }

func (b *Breaker) clock() time.Time {
	if b.cfg.Now == nil {
		return time.Now()
	}
	return b.cfg.Now()
}

func (b *Breaker) threshold() int {
	if b.cfg.FailureThreshold <= 0 {
		return DefaultFailureThreshold
	}
	return b.cfg.FailureThreshold
}

func (b *Breaker) openDuration() time.Duration {
	if b.cfg.OpenDuration <= 0 {
		return DefaultOpenDuration
	}
	return b.cfg.OpenDuration
}

// set changes the state under the lock and returns the notification to run after unlocking.
func (b *Breaker) set(to State) func() {
	from := b.state
	b.state = to
	if from == to || b.cfg.OnTransition == nil {
		return func() {}
	}
	f := b.cfg.OnTransition
	return func() { f(from, to) }
}

// Allow reports whether an operation may be issued now. Once the open duration has elapsed the breaker
// turns half-open and lets exactly one probe through.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	notify := func() {}
	defer func() { b.mu.Unlock(); notify() }()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.clock().Sub(b.openedAt) < b.openDuration() {
			return false
		}
		notify = b.set(StateHalfOpen)
	}
	if b.probing {
		return false
	}
	b.probing = true
	return true
}

// Ready reports, without changing any state, whether Allow would currently return true.
func (b *Breaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		return b.clock().Sub(b.openedAt) >= b.openDuration() && !b.probing
	}
	return !b.probing
}

// Success reports a success: closes a half-open breaker; resets the failure count when closed.
// A late success while open (an operation issued before the breaker opened) changes nothing.
func (b *Breaker) Success() {
	b.mu.Lock()
	notify := func() {}
	defer func() { b.mu.Unlock(); notify() }()
	switch b.state {
	case StateHalfOpen:
		b.probing, b.failures = false, 0
		notify = b.set(StateClosed)
	case StateClosed:
		b.failures = 0
	}
}

// Failure reports a failure: re-opens a half-open breaker; when closed, counts it and opens after
// FailureThreshold consecutive failures. Late failures while open are ignored.
func (b *Breaker) Failure() {
	b.mu.Lock()
	notify := func() {}
	defer func() { b.mu.Unlock(); notify() }()
	switch b.state {
	case StateHalfOpen:
		b.probing, b.openedAt = false, b.clock()
		notify = b.set(StateOpen)
	case StateClosed:
		b.failures++
		if b.failures >= b.threshold() {
			b.failures, b.openedAt = 0, b.clock()
			notify = b.set(StateOpen)
		}
	}
}

// Abort ends an operation that was neither a success nor a failure (e.g. the caller cancelled it):
// state and counters are unchanged; a half-open probe slot is released.
func (b *Breaker) Abort() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen {
		b.probing = false
	}
}

// State returns the current state (an open breaker whose duration has elapsed still reports open until
// the next Allow).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
