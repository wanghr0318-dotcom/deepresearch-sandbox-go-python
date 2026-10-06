// Package admission 是内存中的容量闸门：run slots 与环境内存按严格 FIFO 授予任务（Admission）；
// exec slots 按全局上限与每任务上限授予（ExecGate，规格 §10.3）。
// 未确认停止的环境保持占用：调用方在 stopped_at 持久化之后才归还（规格 §14.5）。
package admission

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrExceedsCapacity 表示请求永远无法满足（超过总容量）。
var ErrExceedsCapacity = errors.New("admission: request exceeds total capacity")

// Capacity 是闸门的总容量。
type Capacity struct {
	RunSlots    int
	MemoryBytes int64
}

// Request 申请一个 run slot 与 MemoryBytes 内存。MemoryOnly 的请求只占内存、不占 run slot：会话 incarnation 的
// 环境内存（规格 §12.2 表：frozen 仍保留内存并计入环境内存容量；M4 Plan 12）。
type Request struct {
	TaskID      string
	MemoryBytes int64
	MemoryOnly  bool
}

// Grant 是一份已占用的容量；按 ID 归还。
type Grant struct {
	ID          uint64
	TaskID      string
	MemoryBytes int64
	MemoryOnly  bool
}

// Usage 是闸门某一时刻的用量。
type Usage struct {
	Capacity     Capacity
	RunSlotsUsed int
	MemoryUsed   int64
	Queued       int
}

type waiter struct {
	req     Request
	ready   chan struct{} // 授予时关闭
	granted bool
	grant   Grant
}

// pressureMark 是最近一次报告内存压力时的排队状态：同一状态只报告一次。
type pressureMark struct {
	head   *waiter
	memory int64
}

// Admission 按严格 FIFO 授予容量。
type Admission struct {
	mu     sync.Mutex
	cap    Capacity
	held   map[uint64]Grant
	memory int64
	slots  int // 占用 run slot 的授予数（不含 MemoryOnly）
	nextID uint64
	queue  *list.List // of *waiter

	pressure     func(needBytes int64)
	lastPressure pressureMark
}

// New 返回给定容量的空闸门。
func New(c Capacity) *Admission {
	return &Admission{cap: c, held: map[uint64]Grant{}, queue: list.New()}
}

// SetPressureHandler 设置内存压力回调（M4 Plan 12：session Scheduler 按 LRU 驱逐冻结与空闲的会话）：队首请求因
// 内存不足而等待时以缺口字节数调用 h。每个排队状态（队首与已用内存）至多调用一次，且不持锁调用。
// h 为 nil 时取消回调。
func (a *Admission) SetPressureHandler(h func(needBytes int64)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pressure = h
	a.lastPressure = pressureMark{}
}

func (a *Admission) fits(r Request) bool {
	return (r.MemoryOnly || a.slots < a.cap.RunSlots) && a.memory+r.MemoryBytes <= a.cap.MemoryBytes
}

func (a *Admission) grantLocked(r Request) Grant {
	a.nextID++
	g := Grant{ID: a.nextID, TaskID: r.TaskID, MemoryBytes: r.MemoryBytes, MemoryOnly: r.MemoryOnly}
	a.addLocked(g)
	return g
}

func (a *Admission) addLocked(g Grant) {
	a.held[g.ID] = g
	a.memory += g.MemoryBytes
	if !g.MemoryOnly {
		a.slots++
	}
}

// dispatchLocked 依次授予能满足的队首；队首不满足时其后全部等待（避免大请求饥饿）。返回须在解锁后
// 调用的内存压力通知（没有时为 nil）。
func (a *Admission) dispatchLocked() func() {
	for e := a.queue.Front(); e != nil; e = a.queue.Front() {
		w := e.Value.(*waiter)
		if !a.fits(w.req) {
			return a.pressureLocked(w)
		}
		a.queue.Remove(e)
		w.grant = a.grantLocked(w.req)
		w.granted = true
		close(w.ready)
	}
	return nil
}

// pressureLocked 在队首 head 因内存不足等待、且这一排队状态尚未报告时返回通知；否则 nil。
func (a *Admission) pressureLocked(head *waiter) func() {
	need := a.memory + head.req.MemoryBytes - a.cap.MemoryBytes
	mark := pressureMark{head: head, memory: a.memory}
	if a.pressure == nil || need <= 0 || mark == a.lastPressure {
		return nil
	}
	a.lastPressure = mark
	h := a.pressure
	return func() { h(need) }
}

func notify(f func()) {
	if f != nil {
		f()
	}
}

// Acquire 阻塞直到按 FIFO 授予。ctx 先结束时返回 ctx 错误且不占用任何容量；
// 永远无法满足的请求立即返回 ErrExceedsCapacity。
func (a *Admission) Acquire(ctx context.Context, r Request) (Grant, error) {
	if err := ctx.Err(); err != nil {
		return Grant{}, err
	}
	if r.MemoryBytes < 0 {
		return Grant{}, fmt.Errorf("admission: negative memory request %d", r.MemoryBytes)
	}
	a.mu.Lock()
	if (!r.MemoryOnly && a.cap.RunSlots < 1) || r.MemoryBytes > a.cap.MemoryBytes {
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
	var fire func()
	if a.queue.Front() == e { // 新的队首：可能因内存不足等待
		fire = a.pressureLocked(w)
	}
	a.mu.Unlock()
	notify(fire)

	select {
	case <-w.ready:
		return w.grant, nil // w.grant 在 close(ready) 之前写入
	case <-ctx.Done():
		a.mu.Lock()
		if w.granted { // 与授予竞争失败：归还刚得到的容量
			a.releaseLocked(w.grant.ID)
		} else {
			a.queue.Remove(e) // 队首可能已变化
		}
		fire = a.dispatchLocked()
		a.mu.Unlock()
		notify(fire)
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
	if !g.MemoryOnly {
		a.slots--
	}
}

// Release 归还一份容量。幂等：未知或已归还的 ID 被忽略；归还量取自记录的授予，不取自参数。
func (a *Admission) Release(g Grant) {
	a.mu.Lock()
	a.releaseLocked(g.ID)
	fire := a.dispatchLocked()
	a.mu.Unlock()
	notify(fire)
}

// Rebuild 以恢复报告的实际占用替换已占用集合（规格 §14.1 第 9 步）。排队者保留并重新评估；
// 之后的授予 ID 大于全部重建的 ID。占用可能超过容量，此时在回落之前不授予新的请求。
func (a *Admission) Rebuild(occupied []Grant) {
	a.mu.Lock()
	a.held = make(map[uint64]Grant, len(occupied))
	a.memory, a.slots = 0, 0
	for _, g := range occupied {
		if _, dup := a.held[g.ID]; dup {
			continue
		}
		a.addLocked(g)
		if g.ID > a.nextID {
			a.nextID = g.ID
		}
	}
	fire := a.dispatchLocked()
	a.mu.Unlock()
	notify(fire)
}

// Snapshot 返回当前用量。
func (a *Admission) Snapshot() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Usage{Capacity: a.cap, RunSlotsUsed: a.slots, MemoryUsed: a.memory, Queued: a.queue.Len()}
}
