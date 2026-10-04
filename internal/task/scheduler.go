package task

// 本文件是 task actor 的集合（规格 §3.2"每 task 一个 actor"、§14.1 第 10 步）：启动时按 ListActiveTasks
// 为每个非终态任务建立 actor，新任务提交后建立 actor，控制变化时通知对应 actor。Scheduler 不做决策；
// 容量排队由 admission 负责，任务的执行顺序由各 actor 申请 run slot 的先后决定。

import (
	"context"
	"fmt"
	"sync"
)

// StartOptions 是启动恢复交给 Scheduler 的结果（recovery.Report）。
type StartOptions struct {
	// StopBlocked 是当前 attempt 的环境未确认停止的任务及恢复为其重建的授予（见 WithStopBlocked）。
	StopBlocked map[string]SlotGrant
	// Excluded 是被隔离的任务：不为它们建立 actor。
	Excluded []string
}

// Scheduler 管理 actor 集合。方法并发安全。
type Scheduler struct {
	ctx      context.Context
	d        Deps
	mu       sync.Mutex
	actors   map[string]*Actor
	excluded map[string]bool
	wg       sync.WaitGroup
}

// NewScheduler 返回 Scheduler；ctx 结束时全部 actor 退出。
func NewScheduler(ctx context.Context, d Deps) *Scheduler {
	return &Scheduler{ctx: ctx, d: d, actors: map[string]*Actor{}, excluded: map[string]bool{}}
}

// Start 为 ListActiveTasks 返回的每个非终态任务建立 actor（启动恢复完成之后调用，§14.1 第 10 步）。
func (s *Scheduler) Start(opts StartOptions) error {
	ids, err := s.d.Store.ListActiveTasks(s.ctx)
	if err != nil {
		return fmt.Errorf("task: 列出活动任务: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range opts.Excluded {
		s.excluded[id] = true
	}
	for _, id := range ids {
		if s.excluded[id] || s.actors[id] != nil {
			continue
		}
		var o []SpawnOption
		if g, ok := opts.StopBlocked[id]; ok {
			o = append(o, WithStopBlocked(g))
		}
		s.spawnLocked(id, o...)
	}
	return nil
}

// Submit 在任务提交（或可能需要 actor）后调用：已有 actor 时通知它，否则建立 actor。被隔离的任务忽略。
func (s *Scheduler) Submit(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.excluded[taskID] {
		return
	}
	if a := s.actors[taskID]; a != nil {
		a.Notify()
		return
	}
	s.spawnLocked(taskID)
}

// Notify 在控制意图写入提交后调用（§8.1"提交后才确认接受，再尽力通知 actor"）：通知已有的 actor。
// 没有 actor 的任务已终态（或被隔离），控制写入不会被接受，无需处理。
func (s *Scheduler) Notify(taskID string) {
	s.mu.Lock()
	a := s.actors[taskID]
	s.mu.Unlock()
	if a != nil {
		a.Notify()
	}
}

// Actor 返回任务当前的 actor（没有时为 nil）。
func (s *Scheduler) Actor(taskID string) *Actor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.actors[taskID]
}

// Wait 等待全部 actor 退出（ctx 结束之后调用）。
func (s *Scheduler) Wait() { s.wg.Wait() }

func (s *Scheduler) spawnLocked(taskID string, opts ...SpawnOption) {
	a := Spawn(s.ctx, taskID, s.d, opts...)
	s.actors[taskID] = a
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-a.Done()
		s.mu.Lock()
		if s.actors[taskID] == a {
			delete(s.actors, taskID)
		}
		s.mu.Unlock()
	}()
}
