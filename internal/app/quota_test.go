package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeSandboxes struct {
	mu       sync.Mutex
	limit    *int64
	live     int64 // sandboxes on other servers
	err      error
	limitQs  int
	countQs  int
	selfLive map[string]bool
}

func (f *fakeSandboxes) OrgSandboxLimit(context.Context, string) (int64, *int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limitQs++
	return 7, f.limit, f.err
}

func (f *fakeSandboxes) OrgLiveSandboxes(_ context.Context, _ int64, task string, pending []string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.countQs++
	return f.live + int64(len(pending)), f.selfLive[task], f.err
}

func (f *fakeSandboxes) set(fn func(f *fakeSandboxes)) { f.mu.Lock(); fn(f); f.mu.Unlock() }

func admitAsync(g *sandboxGate, ctx context.Context, task string) chan error {
	ch := make(chan error, 1)
	go func() { ch <- g.admit(ctx, task) }()
	return ch
}

func expectBlocked(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("admitted while the org is full: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
}

func expectAdmitted(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("not admitted")
	}
}

// TestSandboxGate: without a limit admission reads the limit once and never counts (review F3); at the limit tasks
// wait in strict FIFO order and the head is woken by a release on this server, not by the poll (review F2); a later
// arrival cannot overtake a waiter; a task whose own environment is live is never held; a cancelled wait leaves the
// queue; a failing query lets the task through.
func TestSandboxGate(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &fakeSandboxes{selfLive: map[string]bool{}}
	g := newSandboxGate(f, log)
	g.poll = time.Hour // only releases wake waiters in this test
	ctx := context.Background()
	if err := g.admit(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if f.limitQs != 1 || f.countQs != 0 {
		t.Fatalf("unlimited org: %d limit reads, %d counts", f.limitQs, f.countQs)
	}
	two := int64(2)
	f.set(func(f *fakeSandboxes) { f.limit = &two })
	expectAdmitted(t, admitAsync(g, ctx, "b")) // a (pending) + b's count 1 < 2
	c := admitAsync(g, ctx, "c")               // a + b = 2: waits at the head
	expectBlocked(t, c)
	d := admitAsync(g, ctx, "d") // queued behind c
	expectBlocked(t, d)
	if g.Waiting() != 2 {
		t.Fatalf("waiting = %d", g.Waiting())
	}
	g.release("a") // frees one: only the head (c) may take it
	expectAdmitted(t, c)
	expectBlocked(t, d)
	// A newcomer while d waits does not overtake it even when a slot frees.
	e := admitAsync(g, ctx, "e")
	expectBlocked(t, e)
	g.release("b")
	expectAdmitted(t, d)
	expectBlocked(t, e)
	// A task whose own environment is live does not add a sandbox.
	f.set(func(f *fakeSandboxes) { f.selfLive["s"] = true })
	ectx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := g.admit(ectx, "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled wait: %v", err) // x waits behind e and gives up
	}
	g.release("c")
	expectAdmitted(t, e)
	if err := g.admit(ctx, "s"); err != nil {
		t.Fatal(err)
	}
	if g.Waiting() != 0 {
		t.Fatalf("queue should be empty: %d", g.Waiting())
	}
	f.set(func(f *fakeSandboxes) { f.err = errors.New("db down") })
	if err := g.admit(ctx, "y"); err != nil {
		t.Fatalf("a failing query lets the task through: %v", err)
	}
}

type fakeStaleLedger struct {
	ids       []string
	converted []string
}

func (f *fakeStaleLedger) StaleHeldReservations(context.Context, time.Duration) ([]string, error) {
	return f.ids, nil
}
func (f *fakeStaleLedger) ConvertStaleReservations(_ context.Context, ids []string) (int, error) {
	f.converted = append(f.converted, ids...)
	return len(ids), nil
}

// TestConvertStaleLedgerSkipsInflight (review F7): reservations whose try is still in flight in this process are
// never converted; once the settlement was attempted (even if it failed) they are.
func TestConvertStaleLedgerSkipsInflight(t *testing.T) {
	inflight := newInflightTries()
	inflight.add("r1")
	f := &fakeStaleLedger{ids: []string{"r1", "r2"}}
	n, err := convertStaleLedgerOnce(context.Background(), f, inflight)
	if err != nil || n != 1 || len(f.converted) != 1 || f.converted[0] != "r2" {
		t.Fatalf("%d %v %v", n, err, f.converted)
	}
	inflight.remove("r1")
	f.converted, f.ids = nil, []string{"r1"}
	if n, _ := convertStaleLedgerOnce(context.Background(), f, inflight); n != 1 {
		t.Fatal("after the settlement attempt the reservation is converted")
	}
}
