// Package admission 是内存中的容量闸门：run slots 与环境内存按严格 FIFO 授予任务。
// 未确认停止的环境保持占用：调用方在 stopped_at 持久化之后才归还（规格 §14.5）。
// exec slots 属于 M4，本包不建模。
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

// Request 申请一个 run slot 与 MemoryBytes 内存。
type Request struct {
	TaskID      string
	MemoryBytes int64
}

// Grant 是一份已占用的容量；按 ID 归还。
type Grant struct {
	ID          uint64
	TaskID      string
	MemoryBytes int64
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

// Admission 按严格 FIFO 授予容量。
type Admission struct {
	mu     sync.Mutex
	cap    Capacity
	held   map[uint64]Grant
	memory int64
	nextID uint64
	queue  *list.List // of *waiter
}

// New 返回给定容量的空闸门。
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

// dispatchLocked 依次授予能满足的队首；队首不满足时其后全部等待（避免大请求饥饿）。
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
		return w.grant, nil // w.grant 在 close(ready) 之前写入
	case <-ctx.Done():
		a.mu.Lock()
		defer a.mu.Unlock()
		if w.granted { // 与授予竞争失败：归还刚得到的容量
			a.releaseLocked(w.grant.ID)
			a.dispatchLocked()
		} else {
			a.queue.Remove(e)
			a.dispatchLocked() // 队首可能已变化
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

// Release 归还一份容量。幂等：未知或已归还的 ID 被忽略；归还量取自记录的授予，不取自参数。
func (a *Admission) Release(g Grant) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releaseLocked(g.ID)
	a.dispatchLocked()
}

// Rebuild 以恢复报告的实际占用替换已占用集合（规格 §14.1 第 9 步）。排队者保留并重新评估；
// 之后的授予 ID 大于全部重建的 ID。占用可能超过容量，此时在回落之前不授予新的请求。
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

// Snapshot 返回当前用量。
func (a *Admission) Snapshot() Usage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Usage{Capacity: a.cap, RunSlotsUsed: len(a.held), MemoryUsed: a.memory, Queued: a.queue.Len()}
}
