package session

// 本文件是 session actor 的集合（规格 §12.1"session actor 拥有 session 生命周期与 incarnation"）：启动时按
// ListOpenSessions 为每个未关闭的会话建立 actor；授予、交还与通知按需建立 actor；内存压力按 LRU 选出驱逐对象。
// Scheduler 不做决策。

import (
	"context"
	"fmt"
	"sync"
)

// Scheduler 管理 session actor。方法并发安全。
type Scheduler struct {
	ctx    context.Context
	d      Deps
	mu     sync.Mutex
	actors map[string]*Actor
	incs   map[string]IncarnationHandle // env_id → 存活 incarnation 的句柄
	envOf  map[*Actor]string
	wg     sync.WaitGroup
}

// NewScheduler 返回 Scheduler；ctx 结束时全部 actor 退出。
func NewScheduler(ctx context.Context, d Deps) *Scheduler {
	return &Scheduler{ctx: ctx, d: d.withDefaults(), actors: map[string]*Actor{}, incs: map[string]IncarnationHandle{},
		envOf: map[*Actor]string{}}
}

// Start 为 ListOpenSessions 返回的每个会话建立 actor（启动恢复之后调用：EvictSessionsOnRestart 已结束全部遗留
// incarnation）。
func (s *Scheduler) Start() error {
	ids, err := s.d.Store.ListOpenSessions(s.ctx)
	if err != nil {
		return fmt.Errorf("session: 列出未关闭的会话: %w", err)
	}
	for _, id := range ids {
		s.actor(id)
	}
	return nil
}

// Notify 在新 turn、turn 控制、wake、close 提交之后调用：actor 重新读取会话事实（没有 actor 时建立）。
func (s *Scheduler) Notify(sessionID string) {
	if a := s.actor(sessionID); a != nil {
		a.poke()
	}
}

// Grant 阻塞直到 taskID 是会话中可运行的下一个 turn 且 incarnation idle（必要时 thaw / 冷恢复 / 新建），返回授予；
// 会话关闭或恢复两次失败返回 ErrUnavailable。ctx 结束时撤回申请并返回 ctx 的错误。
//
// 授予只是"准许尝试"：attempt 创建事务（task.Store.CreateAttempt）在 sessions FOR UPDATE 下再次核对 incarnation
// 仍是当前且 idle（incarnation_not_idle）。授予之后 IdleFreeze 内未创建 attempt 的授予失效。
func (s *Scheduler) Grant(ctx context.Context, sessionID, taskID string) (GrantInfo, error) {
	reply := make(chan grantReply, 1)
	for {
		a := s.actor(sessionID)
		if a == nil {
			return GrantInfo{}, s.stopped()
		}
		select {
		case a.reqs <- grantReq{taskID: taskID, reply: reply}:
		case <-a.done:
			continue // actor 已退出（会话刚关闭）：新 actor 读取事实后答复
		case <-ctx.Done():
			return GrantInfo{}, ctx.Err()
		}
		select {
		case r := <-reply:
			return r.info, r.err
		case <-a.done:
			select { // 退出之前已答复
			case r := <-reply:
				return r.info, r.err
			default:
			}
			if s.ctx.Err() != nil {
				return GrantInfo{}, s.stopped()
			}
		case <-ctx.Done():
			select {
			case a.reqs <- withdrawReq{taskID: taskID, reply: reply}:
			case <-a.done:
			}
			return GrantInfo{}, ctx.Err()
		}
	}
}

// Handoff 在 turn 裁决提交后交还 incarnation：Destroy=false → Release（失败则销毁并记 incarnations.end_reason =
// release_timeout）；Destroy=true（故障、取消生效）→ 撤销入口、停止环境、结束 incarnation。返回时满足：Released 或
// 环境 stopped_at 已记录（StopReport.Recorded）——task actor 据此归还 run slot / 允许替代执行。
func (s *Scheduler) Handoff(ctx context.Context, sessionID string, h Handoff) (StopReport, error) {
	reply := make(chan StopReport, 1)
	for {
		a := s.actor(sessionID)
		if a == nil {
			return StopReport{}, s.stopped()
		}
		select {
		case a.reqs <- handoffReq{h: h, reply: reply}:
		case <-a.done:
			continue
		case <-ctx.Done():
			return StopReport{}, ctx.Err()
		}
		select {
		case r := <-reply:
			return r, nil
		case <-a.done:
			select {
			case r := <-reply:
				return r, nil
			default:
			}
			if s.ctx.Err() != nil {
				return StopReport{}, s.stopped()
			}
		case <-ctx.Done(): // 交还不撤回：actor 照常完成释放或销毁
			return StopReport{}, ctx.Err()
		}
	}
}

// Incarnation 返回 envID 对应的存活 incarnation（runner 适配用）。
func (s *Scheduler) Incarnation(envID string) (IncarnationHandle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.incs[envID]
	return h, ok
}

// OnMemoryPressure：按 LRUCandidates 先驱逐 frozen、再驱逐 idle，直至释放 ≥ need 或无候选（admission 回调；不阻塞调用方）。
func (s *Scheduler) OnMemoryPressure(need int64) {
	if need <= 0 {
		return
	}
	per := s.d.Config.IncarnationMemory
	n := 1
	if per > 0 {
		n = int((need + per - 1) / per)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		cands, err := retry(s.ctx, s.d, func(ctx context.Context) ([]LRUCandidate, error) { return s.d.Store.LRUCandidates(ctx, n) })
		if err != nil {
			return
		}
		for i, c := range cands {
			if i >= n {
				break
			}
			if a := s.actor(c.SessionID); a != nil {
				a.pressure()
			}
		}
	}()
}

// Wait 等待全部 actor 与后台 goroutine 退出（ctx 结束之后调用）。
func (s *Scheduler) Wait() { s.wg.Wait() }

func (s *Scheduler) stopped() error {
	return fmt.Errorf("%w: %w", ErrStopped, context.Cause(s.ctx))
}

// actor 返回会话的 actor，没有时建立；Scheduler 的 ctx 已结束时返回 nil。
func (s *Scheduler) actor(sessionID string) *Actor {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil
	}
	if a := s.actors[sessionID]; a != nil {
		select {
		case <-a.done:
		default:
			return a
		}
	}
	a := spawn(s.ctx, sessionID, s.d, s)
	s.actors[sessionID] = a
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-a.done
		s.mu.Lock()
		if s.actors[sessionID] == a {
			delete(s.actors, sessionID)
		}
		s.mu.Unlock()
	}()
	return a
}

// setIncarnation 登记 actor 持有的 incarnation 句柄（envID 为空表示清除）。
func (s *Scheduler) setIncarnation(a *Actor, envID string, h IncarnationHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.envOf[a]; ok {
		delete(s.incs, old)
		delete(s.envOf, a)
	}
	if envID != "" {
		s.incs[envID] = h
		s.envOf[a] = envID
	}
}
