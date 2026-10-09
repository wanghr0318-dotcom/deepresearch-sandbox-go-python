package breaker

import (
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// TestConfiguredThresholdAndDuration: a breaker with threshold 2 / 5 s opens after 2 failures, refuses for
// 5 s, then lets exactly one probe through; a failing probe re-opens it, a succeeding probe closes it.
func TestConfiguredThresholdAndDuration(t *testing.T) {
	clk := &clock{t: time.Unix(1000, 0)}
	var transitions []string
	b := New(Config{FailureThreshold: 2, OpenDuration: 5 * time.Second, Now: clk.now,
		OnTransition: func(from, to State) { transitions = append(transitions, from.String()+">"+to.String()) }})
	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("closed breaker refused attempt %d", i)
		}
		b.Failure()
	}
	if b.State() != StateOpen || b.Allow() || b.Ready() {
		t.Fatalf("after 2 failures: state %s", b.State())
	}
	clk.advance(5*time.Second - time.Nanosecond)
	if b.Allow() || b.Ready() {
		t.Fatal("allowed before the open duration elapsed")
	}
	clk.advance(time.Nanosecond)
	if !b.Ready() {
		t.Fatal("Ready false after open duration")
	}
	if b.State() != StateOpen {
		t.Fatal("Ready changed the state")
	}
	if !b.Allow() || b.State() != StateHalfOpen {
		t.Fatalf("probe not allowed: %s", b.State())
	}
	if b.Allow() || b.Ready() {
		t.Fatal("second probe allowed while the first is in flight")
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("failed probe: %s", b.State())
	}
	clk.advance(5 * time.Second)
	if !b.Allow() {
		t.Fatal("second probe refused")
	}
	b.Success()
	if b.State() != StateClosed || !b.Allow() {
		t.Fatalf("successful probe: %s", b.State())
	}
	want := []string{"closed>open", "open>half_open", "half_open>open", "open>half_open", "half_open>closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions %v, want %v", transitions, want)
		}
	}
}

// TestZeroValueDefaults: the zero value uses 5 failures / 30 s (the cache's historical values).
func TestZeroValueDefaults(t *testing.T) {
	var b Breaker
	for i := 0; i < DefaultFailureThreshold-1; i++ {
		b.Allow()
		b.Failure()
	}
	if b.State() != StateClosed {
		t.Fatal("opened before 5 failures")
	}
	b.Success() // resets the consecutive count
	for i := 0; i < DefaultFailureThreshold-1; i++ {
		b.Failure()
	}
	if b.State() != StateClosed {
		t.Fatal("success did not reset the count")
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatal("not open after 5 consecutive failures")
	}
}

// TestAbortReleasesProbe: an aborted probe leaves the breaker half-open and frees the probe slot.
func TestAbortReleasesProbe(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	b := New(Config{FailureThreshold: 1, OpenDuration: time.Second, Now: clk.now})
	b.Failure()
	clk.advance(time.Second)
	if !b.Allow() {
		t.Fatal("probe refused")
	}
	b.Abort()
	if b.State() != StateHalfOpen || !b.Ready() || !b.Allow() {
		t.Fatal("abort did not release the probe slot")
	}
}
