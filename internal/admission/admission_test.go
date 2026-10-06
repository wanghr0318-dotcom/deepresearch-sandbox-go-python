package admission

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
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

// ---- ExecGate (exec slots, §10.3) ----

type execResult struct {
	g   ExecGrant
	err error
}

// execWaitQueued yields until the gate reports n queued waiters; no sleeping.
func execWaitQueued(t *testing.T, g *ExecGate, n int) {
	t.Helper()
	end := time.Now().Add(deadline)
	for g.Snapshot().Queued != n {
		if time.Now().After(end) {
			t.Fatalf("queued never reached %d: %+v", n, g.Snapshot())
		}
		runtime.Gosched()
	}
}

// startExec runs Acquire in a goroutine and returns once it is queued as the
// wantQueued-th waiter, so queue order is deterministic.
func startExec(t *testing.T, g *ExecGate, ctx context.Context, task string, wantQueued int) <-chan execResult {
	t.Helper()
	ch := make(chan execResult, 1)
	go func() {
		gr, err := g.Acquire(ctx, task)
		ch <- execResult{gr, err}
	}()
	execWaitQueued(t, g, wantQueued)
	return ch
}

func recvExec(t *testing.T, ch <-chan execResult) execResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(deadline):
		t.Fatal("no exec result before deadline")
		return execResult{}
	}
}

func notReadyExec(t *testing.T, ch <-chan execResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("unexpected exec result: %+v", r)
	default:
	}
}

func mustExec(t *testing.T, g *ExecGate, task string) ExecGrant {
	t.Helper()
	gr, err := g.Acquire(context.Background(), task)
	if err != nil {
		t.Fatalf("acquire %s: %v", task, err)
	}
	if gr.TaskID != task || gr.ID == 0 {
		t.Fatalf("bad grant for %s: %+v", task, gr)
	}
	return gr
}

func checkExecUsage(t *testing.T, g *ExecGate, used, queued int, perTask map[string]int) {
	t.Helper()
	u := g.Snapshot()
	if u.Used != used || u.Queued != queued || len(u.PerTask) != len(perTask) {
		t.Fatalf("usage = %+v, want used=%d queued=%d perTask=%v", u, used, queued, perTask)
	}
	for k, v := range perTask {
		if u.PerTask[k] != v {
			t.Fatalf("usage = %+v, want perTask=%v", u, perTask)
		}
	}
}

func TestExecGatePerTaskCapDoesNotBlockOtherTasks(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 4, PerTask: 2})
	mustExec(t, g, "a")
	mustExec(t, g, "a")
	a3 := startExec(t, g, context.Background(), "a", 1) // third for task a waits
	b1 := mustExec(t, g, "b")                           // other task is granted past the queued head
	notReadyExec(t, a3)
	checkExecUsage(t, g, 3, 1, map[string]int{"a": 2, "b": 1})
	if s := g.Snapshot(); s.Capacity != (ExecCapacity{Slots: 4, PerTask: 2}) {
		t.Fatalf("capacity = %+v", s.Capacity)
	}
	g.Release(b1)
	notReadyExec(t, a3) // freeing another task's slot does not lift a's cap
}

func TestExecGateDispatchSkipsCappedHead(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 3, PerTask: 2})
	a1 := mustExec(t, g, "a")
	mustExec(t, g, "a")
	b1 := mustExec(t, g, "b")                           // global full
	a3 := startExec(t, g, context.Background(), "a", 1) // head: a at cap
	b2 := startExec(t, g, context.Background(), "b", 2) // behind it
	c1 := startExec(t, g, context.Background(), "c", 3) // behind b2
	g.Release(b1)                                       // one slot frees: a3 is skipped, b2 granted
	if r := recvExec(t, b2); r.err != nil || r.g.TaskID != "b" {
		t.Fatalf("b2: %+v", r)
	}
	notReadyExec(t, a3)
	notReadyExec(t, c1)
	checkExecUsage(t, g, 3, 2, map[string]int{"a": 2, "b": 1})
	g.Release(a1) // head now satisfiable and first in line
	if r := recvExec(t, a3); r.err != nil || r.g.TaskID != "a" {
		t.Fatalf("a3: %+v", r)
	}
	notReadyExec(t, c1)
	checkExecUsage(t, g, 3, 1, map[string]int{"a": 2, "b": 1})
}

func TestExecGateSameTaskFIFO(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 4, PerTask: 1})
	a1 := mustExec(t, g, "a")
	a2 := startExec(t, g, context.Background(), "a", 1)
	a3 := startExec(t, g, context.Background(), "a", 2)
	g.Release(a1)
	r2 := recvExec(t, a2)
	if r2.err != nil {
		t.Fatal(r2.err)
	}
	notReadyExec(t, a3)
	g.Release(r2.g)
	if r3 := recvExec(t, a3); r3.err != nil || r3.g.ID <= r2.g.ID {
		t.Fatalf("a3: %+v after %+v", r3, r2)
	}
}

func TestExecGateGlobalFullQueuesInFIFOOrder(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 1, PerTask: 2})
	x := mustExec(t, g, "x")
	ca := startExec(t, g, context.Background(), "a", 1)
	cb := startExec(t, g, context.Background(), "b", 2)
	notReadyExec(t, ca)
	g.Release(x)
	ra := recvExec(t, ca)
	if ra.err != nil || ra.g.TaskID != "a" {
		t.Fatalf("a: %+v", ra)
	}
	notReadyExec(t, cb)
	g.Release(ra.g)
	if rb := recvExec(t, cb); rb.err != nil || rb.g.TaskID != "b" {
		t.Fatalf("b: %+v", rb)
	}
	checkExecUsage(t, g, 1, 0, map[string]int{"b": 1})
}

func TestExecGateCancelWhileQueuedDoesNotOccupy(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 1, PerTask: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Acquire(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("done ctx err = %v", err)
	}
	x := mustExec(t, g, "x")
	ctx, cancel = context.WithCancel(context.Background())
	head := startExec(t, g, ctx, "a", 1)
	next := startExec(t, g, context.Background(), "b", 2)
	cancel()
	if r := recvExec(t, head); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("head err = %v", r.err)
	}
	checkExecUsage(t, g, 1, 1, map[string]int{"x": 1})
	g.Release(x)
	if r := recvExec(t, next); r.err != nil || r.g.TaskID != "b" {
		t.Fatalf("next: %+v", r)
	}
}

// The grant lands after the waiter has observed ctx.Done but before it takes the
// lock: the grant must be returned and passed to the next waiter.
func TestExecGateGrantCancelRaceBarrier(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 1, PerTask: 1})
	atBarrier := make(chan struct{})
	proceed := make(chan struct{})
	g.beforeCancelLock = func() {
		close(atBarrier)
		<-proceed
	}
	x := mustExec(t, g, "x")
	ctx, cancel := context.WithCancel(context.Background())
	head := startExec(t, g, ctx, "a", 1)
	cancel()
	<-atBarrier
	next := startExec(t, g, context.Background(), "b", 2)
	g.Release(x) // grants the head that is already leaving
	checkExecUsage(t, g, 1, 1, map[string]int{"a": 1})
	close(proceed)
	if r := recvExec(t, head); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("head: %+v", r)
	}
	r := recvExec(t, next)
	if r.err != nil || r.g.TaskID != "b" {
		t.Fatalf("next: %+v", r)
	}
	checkExecUsage(t, g, 1, 0, map[string]int{"b": 1})
	g.Release(r.g)
	checkExecUsage(t, g, 0, 0, map[string]int{})
}

func TestExecGateGrantCancelRaceRandom(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	g := NewExecGate(ExecCapacity{Slots: 1, PerTask: 1})
	for i := 0; i < 1000; i++ {
		x := mustExec(t, g, "x")
		ctx, cancel := context.WithCancel(context.Background())
		head := startExec(t, g, ctx, "a", 1)
		yields := rng.Intn(4)
		if rng.Intn(2) == 0 {
			cancel()
			for j := 0; j < yields; j++ {
				runtime.Gosched()
			}
			g.Release(x)
		} else {
			g.Release(x)
			for j := 0; j < yields; j++ {
				runtime.Gosched()
			}
			cancel()
		}
		r := recvExec(t, head)
		switch {
		case r.err == nil:
			checkExecUsage(t, g, 1, 0, map[string]int{"a": 1})
			g.Release(r.g)
		case errors.Is(r.err, context.Canceled):
		default:
			t.Fatalf("iteration %d (seed %d): err = %v", i, seed, r.err)
		}
		if u := g.Snapshot(); u.Used != 0 || u.Queued != 0 || len(u.PerTask) != 0 {
			t.Fatalf("iteration %d (seed %d): leak %+v", i, seed, u)
		}
	}
}

func TestExecGateReleaseIdempotentAndIgnoresUnknown(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 1, PerTask: 1})
	a := mustExec(t, g, "a")
	g.Release(a)
	b := mustExec(t, g, "b")
	g.Release(a)                               // stale duplicate must not free b's slot
	g.Release(ExecGrant{ID: 999, TaskID: "b"}) // unknown ID ignored
	g.Release(ExecGrant{})                     // zero grant ignored
	c := startExec(t, g, context.Background(), "c", 1)
	notReadyExec(t, c)
	checkExecUsage(t, g, 1, 1, map[string]int{"b": 1})
	g.Release(b)
	if r := recvExec(t, c); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestExecGateOccupyCountsAgainstCapacity(t *testing.T) {
	g := NewExecGate(ExecCapacity{Slots: 2, PerTask: 1})
	o1 := g.Occupy("a")
	o2 := g.Occupy("b")
	if o1.ID == 0 || o1.ID == o2.ID || o1.TaskID != "a" {
		t.Fatalf("occupy grants: %+v %+v", o1, o2)
	}
	o3 := g.Occupy("b") // recovery may exceed capacity; never refused
	checkExecUsage(t, g, 3, 0, map[string]int{"a": 1, "b": 2})
	cc := startExec(t, g, context.Background(), "c", 1) // global over capacity
	ca := startExec(t, g, context.Background(), "a", 2) // a at its cap
	g.Release(o2)
	notReadyExec(t, cc) // still 2 used of 2
	g.Release(o3)
	if r := recvExec(t, cc); r.err != nil || r.g.ID <= o3.ID {
		t.Fatalf("c: %+v", r)
	}
	notReadyExec(t, ca)
	g.Release(o1)
	if r := recvExec(t, ca); r.err != nil {
		t.Fatal(r.err)
	}
	checkExecUsage(t, g, 2, 0, map[string]int{"a": 1, "c": 1})
}

func TestExecGateUnusableCapacityRejected(t *testing.T) {
	for _, c := range []ExecCapacity{{Slots: 0, PerTask: 2}, {Slots: 4, PerTask: 0}} {
		g := NewExecGate(c)
		if _, err := g.Acquire(context.Background(), "a"); !errors.Is(err, ErrExceedsCapacity) {
			t.Fatalf("%+v: err = %v", c, err)
		}
		checkExecUsage(t, g, 0, 0, map[string]int{})
	}
}

// ---- M4 Plan 12：仅内存授予与内存压力回调（规格 §12.2） ----

// TestMemoryOnlyGrantAndPressure：MemoryOnly 授予不占 run slot（frozen 会话的环境内存仍计入内存）；队首因内存
// 不足等待时 pressure 回调以缺口调用，同一排队状态只调用一次、不持锁（回调中可读 Snapshot）；释放后队首被授予。
func TestMemoryOnlyGrantAndPressure(t *testing.T) {
	a := New(Capacity{RunSlots: 1, MemoryBytes: 100})
	type call struct {
		need int64
		used int64
	}
	calls := make(chan call, 8)
	a.SetPressureHandler(func(need int64) { calls <- call{need, a.Snapshot().MemoryUsed} })

	inc1, err := a.Acquire(context.Background(), Request{TaskID: "inc-1", MemoryBytes: 40, MemoryOnly: true})
	if err != nil || !inc1.MemoryOnly {
		t.Fatalf("MemoryOnly 授予 = %+v, %v", inc1, err)
	}
	inc2, err := a.Acquire(context.Background(), Request{TaskID: "inc-2", MemoryBytes: 30, MemoryOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	// 两个 incarnation 不占 run slot：普通任务仍能拿到唯一的 slot。
	task, err := a.Acquire(context.Background(), Request{TaskID: "t1", MemoryBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if u := a.Snapshot(); u.RunSlotsUsed != 1 || u.MemoryUsed != 80 {
		t.Fatalf("用量 = %+v，期望 1 个 slot、80 字节", u)
	}
	// 第三个 incarnation 需要 50：缺口 30。
	ch := startAcquire(t, a, context.Background(), Request{TaskID: "inc-3", MemoryBytes: 50, MemoryOnly: true}, 1)
	select {
	case c := <-calls:
		if c.need != 30 || c.used != 80 {
			t.Fatalf("pressure 回调 = %+v，期望缺口 30（已用 80）", c)
		}
	case <-time.After(deadline):
		t.Fatal("内存不足时没有 pressure 回调")
	}
	// 排队状态不变（释放一个不存在的授予）不再回调；归还 run slot 而内存仍不足时，缺口变化后再回调一次。
	a.Release(Grant{ID: 9999})
	a.Release(task)
	select {
	case c := <-calls:
		if c.need != 20 {
			t.Fatalf("释放 10 字节后的 pressure 回调 = %+v，期望缺口 20", c)
		}
	case <-time.After(deadline):
		t.Fatal("已用内存变化后没有再次回调")
	}
	select {
	case c := <-calls:
		t.Fatalf("同一排队状态重复回调：%+v", c)
	default:
	}
	notReady(t, ch)
	// 驱逐（归还 inc-1 的 40）后队首被授予。
	a.Release(inc1)
	if r := recv(t, ch); r.err != nil || r.g.TaskID != "inc-3" || !r.g.MemoryOnly {
		t.Fatalf("释放后队首授予 = %+v", r)
	}
	if u := a.Snapshot(); u.RunSlotsUsed != 0 || u.MemoryUsed != 80 || u.Queued != 0 {
		t.Fatalf("用量 = %+v", u)
	}
	a.Release(inc2)
	// 没有 run slot 的闸门仍可授予 MemoryOnly；普通请求永远无法满足。
	b := New(Capacity{RunSlots: 0, MemoryBytes: 10})
	if _, err := b.Acquire(context.Background(), Request{TaskID: "inc", MemoryBytes: 10, MemoryOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(context.Background(), Request{TaskID: "t", MemoryBytes: 1}); !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("没有 run slot 时普通请求 = %v", err)
	}
}
