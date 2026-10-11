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
	mu      sync.Mutex
	limit   *int64
	live    int64
	err     error
	pending []string
}

func (f *fakeSandboxes) OrgSandboxes(_ context.Context, _ string, pending []string) (int64, *int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending = pending
	return 7, f.limit, f.live + int64(len(pending)), f.err
}

func (f *fakeSandboxes) set(live int64) { f.mu.Lock(); f.live = live; f.mu.Unlock() }

// TestSandboxGate: without a limit admission is immediate; at the limit the task waits until a sandbox of the org
// stops; tasks this server admitted count until released; a cancelled wait returns the ctx error; a failing count
// lets the task through.
func TestSandboxGate(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &fakeSandboxes{}
	g := newSandboxGate(f, log)
	g.poll = 5 * time.Millisecond
	ctx := context.Background()
	if err := g.admit(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	two := int64(2)
	f.limit = &two
	f.set(0)
	if err := g.admit(ctx, "b"); err != nil { // a (pending) + 0 live < 2
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.admit(ctx, "c") }() // a + b = 2: waits
	select {
	case err := <-done:
		t.Fatalf("admitted at the limit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.release("a")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := g.admit(cctx, "d"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled wait: %v", err)
	}
	f.mu.Lock()
	f.err = errors.New("db down")
	f.mu.Unlock()
	if err := g.admit(ctx, "e"); err != nil {
		t.Fatalf("a failing count lets the task through: %v", err)
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
