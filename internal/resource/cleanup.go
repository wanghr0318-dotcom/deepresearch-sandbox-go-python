package resource

import (
	"context"
	"errors"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// RunCleanup 运行 cleanup loop，直到 ctx 结束（返回 ctx 的错误）或失去数据库所有权。
//
// 每轮只处理 ListCleanupCandidates 返回的环境（已停止、所属 attempt 已有判决、退避已到）：
// Destroy → 结束 intent → UpdateCleanup(done) → ReleaseUIDRange；Destroy 失败 →
// UpdateCleanup(pending, error, next_retry_at = 退避)。只写清理列、intent 与 UID 范围，
// 不写生命周期字段（规格 §3.2）。Store 故障不终止循环，下一轮重做（各操作幂等）。
func (c *Coordinator) RunCleanup(ctx context.Context) error {
	t := time.NewTicker(c.opt.CleanupInterval)
	defer t.Stop()
	for {
		err := c.cleanupPass(ctx)
		c.mu.Lock()
		hook := c.onCleanupRun
		c.mu.Unlock()
		if hook != nil {
			hook()
		}
		if errors.Is(err, persistence.ErrOwnershipLost) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// cleanupPass 执行一轮清理，返回第一个错误（仅用于判断是否失去所有权）。
func (c *Coordinator) cleanupPass(ctx context.Context) error {
	first := c.retryReleases(ctx)
	envs, err := c.store.ListCleanupCandidates(ctx, c.opt.Now(), c.opt.CleanupBatch)
	if err != nil {
		return err
	}
	for _, e := range envs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.cleanupEnv(ctx, e); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// cleanupEnv 在 envID 的串行执行者内清理一个环境。
func (c *Coordinator) cleanupEnv(ctx context.Context, e Environment) error {
	if e.StoppedAt == nil || e.CleanupState == CleanupDone { // 未确认停止的环境不清理（契约第 5 节）
		return nil
	}
	unlock, err := c.lock(ctx, e.EnvID)
	if err != nil {
		return err
	}
	defer unlock()

	if derr := c.p.Destroy(ctx, e.EnvID); derr != nil {
		if ctx.Err() != nil {
			return ctx.Err() // 被取消的尝试不计入 cleanup_tries
		}
		next := c.opt.Now().Add(c.opt.Backoff(int(e.CleanupTries)))
		_, err := c.store.UpdateCleanup(ctx, CleanupUpdate{
			EnvID: e.EnvID, ExpectedTries: e.CleanupTries, State: CleanupPending,
			Error: derr.Error(), NextRetryAt: &next,
		})
		return err
	}
	// Destroy 已逐层核对三层都不存在：创建意图可以结束。
	if err := c.finishIntent(ctx, e.EnvID); err != nil {
		return err
	}
	// cleanup_state = done 之后环境不再是候选：先登记待归还，使提交结果未知或归还失败时由
	// retryReleases 在后续轮次补做。若 done 实际未提交，补做的归还因前置条件冲突而放弃，
	// 环境仍是候选，下一轮重做。
	c.mu.Lock()
	c.pendingFree[e.EnvID] = struct{}{}
	c.mu.Unlock()
	if _, err := c.store.UpdateCleanup(ctx, CleanupUpdate{EnvID: e.EnvID, ExpectedTries: e.CleanupTries, State: CleanupDone}); err != nil {
		return err
	}
	return c.releaseUIDRange(ctx, e.EnvID)
}

// finishIntent 结束环境的创建意图：acquired → released；pending → failed（Destroy 成功且
// 串行执行者内没有在途创建，已确认无残留，规格 §8.3）。没有 intent 时跳过。
func (c *Coordinator) finishIntent(ctx context.Context, envID string) error {
	in, err := c.store.GetIntent(ctx, intentID(envID))
	if errors.Is(err, persistence.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	switch in.State {
	case IntentAcquired:
		_, err = c.store.ResolveIntent(ctx, in.IntentID, IntentReleased)
	case IntentPending:
		_, err = c.store.ResolveIntent(ctx, in.IntentID, IntentFailed)
	}
	return err
}

// releaseUIDRange 归还环境的 UID 范围（规格 §4.5：环境已停止且清理完成；文件与挂载由 Destroy
// 的逐层核对保证）。成功、没有范围或冲突（分配已变化，重试无意义，交给启动核对）时移出待归还集合。
func (c *Coordinator) releaseUIDRange(ctx context.Context, envID string) error {
	ur, err := c.store.GetUIDRange(ctx, envID)
	if err == nil {
		_, err = c.store.ReleaseUIDRange(ctx, ur.UIDRangeID, ur.AllocationID)
	}
	if errors.Is(err, persistence.ErrNotFound) {
		err = nil
	}
	if err != nil && !errors.Is(err, persistence.ErrConflict) {
		return err
	}
	c.mu.Lock()
	delete(c.pendingFree, envID)
	c.mu.Unlock()
	return err
}

// retryReleases 补做此前失败的 UID 范围归还（规格 §14.5 在线补偿）。
func (c *Coordinator) retryReleases(ctx context.Context) error {
	c.mu.Lock()
	ids := make([]string, 0, len(c.pendingFree))
	for id := range c.pendingFree {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	var first error
	for _, id := range ids {
		if err := c.releaseUIDRange(ctx, id); err != nil && first == nil {
			first = err
		}
	}
	return first
}
