package admission

import (
	"container/list"
	"context"
	"sync"
)

// ExecCapacity：全局 exec slots 与每任务上限（§10.3，默认 4、2）。
type ExecCapacity struct{ Slots, PerTask int }

// ExecGrant 是一个已占用的 exec slot；按 ID 归还。
type ExecGrant struct {
	ID     uint64
	TaskID string
}

// ExecUsage 是 exec 闸门某一时刻的用量。PerTask 只含占用数大于 0 的任务。
type ExecUsage struct {
	Capacity ExecCapacity
	Used     int
	PerTask  map[string]int
	Queued   int
}

type execWaiter struct {
	taskID  string
	ready   chan struct{} // 授予时关闭
	granted bool
	grant   ExecGrant
}

// ExecGate 按 FIFO 授予 exec slot：队首的任务已达每任务上限时，跳过它授予后面第一个可满足的等待者
// （同一任务内保持 FIFO）。全局 slot 不足时整个队列等待。
// 归还只在 exec 环境 stopped_at 已持久化之后（§14.5 同一原则）。
type ExecGate struct {
	mu      sync.Mutex
	cap     ExecCapacity
	held    map[uint64]ExecGrant
	perTask map[string]int
	nextID  uint64
	queue   *list.List // of *execWaiter

	// beforeCancelLock 仅供测试：ctx 结束分支在取锁之前调用，用于确定性地构造"授予与取消竞争"。
	beforeCancelLock func()
}

// NewExecGate 返回给定容量的空 exec 闸门。
func NewExecGate(c ExecCapacity) *ExecGate {
	return &ExecGate{cap: c, held: map[uint64]ExecGrant{}, perTask: map[string]int{}, queue: list.New()}
}

func (g *ExecGate) fitsLocked(taskID string) bool {
	return len(g.held) < g.cap.Slots && g.perTask[taskID] < g.cap.PerTask
}

func (g *ExecGate) grantLocked(taskID string) ExecGrant {
	g.nextID++
	gr := ExecGrant{ID: g.nextID, TaskID: taskID}
	g.held[gr.ID] = gr
	g.perTask[taskID]++
	return gr
}

// dispatchLocked 从队首起授予可满足的等待者：全局满即停止；任务已达上限则跳过。
// 被跳过的任务在本轮内占用只增不减，所以同一任务后面的等待者也被跳过（任务内 FIFO）。
// 返回后队列中没有可满足的等待者。
func (g *ExecGate) dispatchLocked() {
	for e := g.queue.Front(); e != nil && len(g.held) < g.cap.Slots; {
		next := e.Next()
		w := e.Value.(*execWaiter)
		if g.perTask[w.taskID] < g.cap.PerTask {
			g.queue.Remove(e)
			w.grant = g.grantLocked(w.taskID)
			w.granted = true
			close(w.ready)
		}
		e = next
	}
}

func (g *ExecGate) releaseLocked(id uint64) {
	gr, ok := g.held[id]
	if !ok {
		return
	}
	delete(g.held, id)
	if g.perTask[gr.TaskID]--; g.perTask[gr.TaskID] <= 0 {
		delete(g.perTask, gr.TaskID)
	}
}

// Acquire 等待一个 slot；ctx 结束返回 ctx.Err() 且不占用（已授予但调用方已离开时立即归还）。
// 容量配置使任何请求都无法满足（Slots 或 PerTask < 1）时立即返回 ErrExceedsCapacity。
func (g *ExecGate) Acquire(ctx context.Context, taskID string) (ExecGrant, error) {
	if err := ctx.Err(); err != nil {
		return ExecGrant{}, err
	}
	g.mu.Lock()
	if g.cap.Slots < 1 || g.cap.PerTask < 1 {
		g.mu.Unlock()
		return ExecGrant{}, ErrExceedsCapacity
	}
	// 每次状态变化后都已 dispatch，排队者此刻都不可满足；本请求可满足即不越过任何可满足的前序者。
	if g.fitsLocked(taskID) {
		gr := g.grantLocked(taskID)
		g.mu.Unlock()
		return gr, nil
	}
	w := &execWaiter{taskID: taskID, ready: make(chan struct{})}
	e := g.queue.PushBack(w)
	g.mu.Unlock()

	select {
	case <-w.ready:
		return w.grant, nil // w.grant 在 close(ready) 之前写入
	case <-ctx.Done():
		if g.beforeCancelLock != nil {
			g.beforeCancelLock()
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if w.granted { // 与授予竞争失败：归还刚得到的 slot
			g.releaseLocked(w.grant.ID)
		} else {
			g.queue.Remove(e)
		}
		g.dispatchLocked()
		return ExecGrant{}, ctx.Err()
	}
}

// Release 归还一个 slot。幂等：未知或已归还的 ID 被忽略；任务取自记录的授予，不取自参数。
// 调用方只在 exec 环境 stopped_at 已持久化后调用。
func (g *ExecGate) Release(gr ExecGrant) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.releaseLocked(gr.ID)
	g.dispatchLocked()
}

// Occupy 在启动恢复时登记仍被未确认停止的 exec 环境占用的 slot（stop_blocked），返回其授予。
// 不受容量限制（占用可超过容量，回落之前不授予新的请求）。
func (g *ExecGate) Occupy(taskID string) ExecGrant {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.grantLocked(taskID)
}

// Snapshot 返回当前用量（PerTask 为副本）。
func (g *ExecGate) Snapshot() ExecUsage {
	g.mu.Lock()
	defer g.mu.Unlock()
	per := make(map[string]int, len(g.perTask))
	for k, v := range g.perTask {
		per[k] = v
	}
	return ExecUsage{Capacity: g.cap, Used: len(g.held), PerTask: per, Queued: g.queue.Len()}
}
