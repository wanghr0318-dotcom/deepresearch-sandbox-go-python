package admission

import (
	"context"
	"errors"
	"testing"
	"time"
)

const deadline = 5 * time.Second

type result struct {
	g   Grant
	err error
}

// startAcquire runs Acquire in a goroutine and returns once it is queued
// (Snapshot().Queued reaches want), so FIFO order is deterministic.
func startAcquire(t *testing.T, a *Admission, ctx context.Context, r Request, wantQueued int) <-chan result {
	t.Helper()
	ch := make(chan result, 1)
	go func() {
		g, err := a.Acquire(ctx, r)
		ch <- result{g, err}
	}()
	waitFor(t, func() bool { return a.Snapshot().Queued == wantQueued })
	return ch
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !cond() {
		if time.Now().After(end) {
			t.Fatal("condition not reached before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

func recv(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(deadline):
		t.Fatal("no result before deadline")
		return result{}
	}
}

func notReady(t *testing.T, ch <-chan result) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("unexpected result: %+v", r)
	default:
	}
}

func TestImmediateGrantWithinCapacity(t *testing.T) {
	a := New(Capacity{RunSlots: 2, MemoryBytes: 100})
	g1, err := a.Acquire(context.Background(), Request{TaskID: "a", MemoryBytes: 40})
	if err != nil {
		t.Fatal(err)
	}
	g2, err := a.Acquire(context.Background(), Request{TaskID: "b", MemoryBytes: 60})
	if err != nil {
		t.Fatal(err)
	}
	if g1.ID == g2.ID || g1.TaskID != "a" || g1.MemoryBytes != 40 {
		t.Fatalf("bad grants: %+v %+v", g1, g2)
	}
	u := a.Snapshot()
	if u.RunSlotsUsed != 2 || u.MemoryUsed != 100 || u.Queued != 0 {
		t.Fatalf("usage: %+v", u)
	}
}

func TestQueuesAndGrantsInFIFOOrder(t *testing.T) {
	a := New(Capacity{RunSlots: 1, MemoryBytes: 100})
	g0, _ := a.Acquire(context.Background(), Request{TaskID: "0", MemoryBytes: 10})
	c1 := startAcquire(t, a, context.Background(), Request{TaskID: "1", MemoryBytes: 10}, 1)
	c2 := startAcquire(t, a, context.Background(), Request{TaskID: "2", MemoryBytes: 10}, 2)
	notReady(t, c1)
	a.Release(g0)
	r1 := recv(t, c1)
	if r1.err != nil || r1.g.TaskID != "1" {
		t.Fatalf("first: %+v", r1)
	}
	notReady(t, c2)
	a.Release(r1.g)
	r2 := recv(t, c2)
	if r2.err != nil || r2.g.TaskID != "2" {
		t.Fatalf("second: %+v", r2)
	}
}

func TestHeadBlocksSmallerFollowers(t *testing.T) {
	a := New(Capacity{RunSlots: 5, MemoryBytes: 100})
	g0, _ := a.Acquire(context.Background(), Request{TaskID: "0", MemoryBytes: 60})
	big := startAcquire(t, a, context.Background(), Request{TaskID: "big", MemoryBytes: 80}, 1)
	// A small request would fit now (40 free) but must wait behind the head.
	small := startAcquire(t, a, context.Background(), Request{TaskID: "small", MemoryBytes: 10}, 2)
	notReady(t, small)
	a.Release(g0)
	rb := recv(t, big)
	if rb.err != nil {
		t.Fatal(rb.err)
	}
	rs := recv(t, small)
	if rs.err != nil {
		t.Fatal(rs.err)
	}
}

func TestCancelHeadDequeuesAndAdvancesFollowers(t *testing.T) {
	a := New(Capacity{RunSlots: 5, MemoryBytes: 100})
	g0, _ := a.Acquire(context.Background(), Request{TaskID: "0", MemoryBytes: 60})
	ctx, cancel := context.WithCancel(context.Background())
	head := startAcquire(t, a, ctx, Request{TaskID: "head", MemoryBytes: 80}, 1)
	next := startAcquire(t, a, context.Background(), Request{TaskID: "next", MemoryBytes: 30}, 2)
	notReady(t, next)
	cancel()
	rh := recv(t, head)
	if !errors.Is(rh.err, context.Canceled) {
		t.Fatalf("head err = %v", rh.err)
	}
	rn := recv(t, next) // fits in the 40 free bytes once the head is gone
	if rn.err != nil || rn.g.TaskID != "next" {
		t.Fatalf("next: %+v", rn)
	}
	u := a.Snapshot()
	if u.MemoryUsed != 90 || u.RunSlotsUsed != 2 || u.Queued != 0 {
		t.Fatalf("usage: %+v", u)
	}
	a.Release(g0)
	a.Release(rn.g)
	if u := a.Snapshot(); u.MemoryUsed != 0 || u.RunSlotsUsed != 0 {
		t.Fatalf("leak: %+v", u)
	}
}

func TestAcquireWithDoneContextDoesNotOccupy(t *testing.T) {
	a := New(Capacity{RunSlots: 1, MemoryBytes: 100})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Acquire(ctx, Request{TaskID: "x", MemoryBytes: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if u := a.Snapshot(); u.RunSlotsUsed != 0 || u.MemoryUsed != 0 || u.Queued != 0 {
		t.Fatalf("usage: %+v", u)
	}
}

func TestDoubleReleaseDoesNotOverGrant(t *testing.T) {
	a := New(Capacity{RunSlots: 1, MemoryBytes: 100})
	g1, _ := a.Acquire(context.Background(), Request{TaskID: "1", MemoryBytes: 10})
	a.Release(g1)
	g2, err := a.Acquire(context.Background(), Request{TaskID: "2", MemoryBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	a.Release(g1) // stale duplicate must not free g2's slot
	c3 := startAcquire(t, a, context.Background(), Request{TaskID: "3", MemoryBytes: 10}, 1)
	notReady(t, c3)
	if u := a.Snapshot(); u.RunSlotsUsed != 1 || u.MemoryUsed != 10 {
		t.Fatalf("usage: %+v", u)
	}
	a.Release(g2)
	if r := recv(t, c3); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestOversizedRequestRejectedImmediately(t *testing.T) {
	a := New(Capacity{RunSlots: 2, MemoryBytes: 100})
	if _, err := a.Acquire(context.Background(), Request{TaskID: "x", MemoryBytes: 101}); !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("err = %v", err)
	}
	if _, err := a.Acquire(context.Background(), Request{TaskID: "x", MemoryBytes: -1}); err == nil {
		t.Fatal("negative memory accepted")
	}
	if u := a.Snapshot(); u.Queued != 0 {
		t.Fatalf("queued: %+v", u)
	}
	z := New(Capacity{RunSlots: 0, MemoryBytes: 100})
	if _, err := z.Acquire(context.Background(), Request{TaskID: "x"}); !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("zero slots err = %v", err)
	}
}

func TestRebuildSetsOccupancyAndWakesWaiters(t *testing.T) {
	a := New(Capacity{RunSlots: 2, MemoryBytes: 100})
	a.Rebuild([]Grant{{ID: 7, TaskID: "old1", MemoryBytes: 50}, {ID: 9, TaskID: "old2", MemoryBytes: 30}})
	if u := a.Snapshot(); u.RunSlotsUsed != 2 || u.MemoryUsed != 80 {
		t.Fatalf("usage: %+v", u)
	}
	c := startAcquire(t, a, context.Background(), Request{TaskID: "new", MemoryBytes: 10}, 1)
	a.Release(Grant{ID: 7})
	r := recv(t, c)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.g.ID <= 9 {
		t.Fatalf("new grant ID %d collides with rebuilt IDs", r.g.ID)
	}
	if u := a.Snapshot(); u.RunSlotsUsed != 2 || u.MemoryUsed != 40 {
		t.Fatalf("usage: %+v", u)
	}
	// Rebuild replaces prior state entirely.
	a.Rebuild(nil)
	if u := a.Snapshot(); u.RunSlotsUsed != 0 || u.MemoryUsed != 0 {
		t.Fatalf("usage after empty rebuild: %+v", u)
	}
}
