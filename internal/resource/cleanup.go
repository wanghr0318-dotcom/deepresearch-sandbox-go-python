package resource

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/faultinject"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// RunCleanup 运行 cleanup loop，直到 ctx 结束（返回 ctx 的错误）或失去数据库所有权。
//
// 每轮只处理 ListCleanupCandidates 返回的环境（已停止、所属 attempt 已有判决、退避已到）：
// Destroy → 结束 intent → UpdateCleanup(done) → ReclaimUIDFiles → UIDFiles → ReleaseUIDRange（有残留文件则
// QuarantineUIDRange 并报警，不归还；M4 Plan 15 D13）；Destroy 失败 →
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
		case <-c.kick:
		}
	}
}

// cleanupPass 执行一轮清理，返回第一个错误（仅用于判断是否失去所有权）。
func (c *Coordinator) cleanupPass(ctx context.Context) error {
	first := c.retryReleases(ctx)
	if err := c.retryAlerts(ctx); err != nil && first == nil {
		first = err
	}
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

// cleanupEnv 在 envID 的串行执行者内清理一个环境。Destroy 失败已记入 cleanup 列（退避重试），不作为错误返回。
func (c *Coordinator) cleanupEnv(ctx context.Context, e Environment) error {
	_, err := c.cleanupOne(ctx, e)
	return err
}

// CleanupNow 立即清理一个已记录 stopped_at 的环境（M4 Plan 15 D8：exec 环境在收集之后同步尽力清理，不等待
// cleanup loop）：与 cleanup loop 相同的 cleanupEnv（Destroy → 结束 intent → cleanup_state = done → 回收、核查并
// 归还 UID 范围）。stopped_at 尚未记录时不清理并返回错误；Destroy 失败已按退避记入 cleanup 列（由 cleanup loop
// 接手，kind = exec 的候选不要求 attempt 判决），同时作为错误返回。已清理完成时为空操作。
func (c *Coordinator) CleanupNow(ctx context.Context, envID string) error {
	e, err := c.store.GetEnvironment(ctx, envID)
	if err != nil {
		return fmt.Errorf("resource: 清理环境 %s: %w", envID, err)
	}
	if e.StoppedAt == nil {
		return fmt.Errorf("resource: 清理环境 %s: stopped_at 尚未记录", envID)
	}
	derr, err := c.cleanupOne(ctx, e)
	if derr != nil {
		return errors.Join(fmt.Errorf("resource: 销毁环境 %s: %w", envID, derr), err)
	}
	return err
}

// cleanupOne 是 cleanupEnv 的实现；destroyErr 是 Destroy 的失败（已记入 cleanup 列）。
func (c *Coordinator) cleanupOne(ctx context.Context, e Environment) (destroyErr, err error) {
	if e.StoppedAt == nil || e.CleanupState == CleanupDone { // 未确认停止的环境不清理（契约第 5 节）
		return nil, nil
	}
	unlock, err := c.lock(ctx, e.EnvID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if derr := c.p.Destroy(ctx, e.EnvID); derr != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err() // 被取消的尝试不计入 cleanup_tries
		}
		next := c.opt.Now().Add(c.opt.Backoff(int(e.CleanupTries)))
		_, err := c.store.UpdateCleanup(ctx, CleanupUpdate{
			EnvID: e.EnvID, ExpectedTries: e.CleanupTries, State: CleanupPending,
			Error: derr.Error(), NextRetryAt: &next,
		})
		return derr, err
	}
	faultinject.Point(faultinject.CleanupDestroyed)
	// Destroy 已逐层核对三层都不存在：创建意图可以结束。
	if err := c.finishIntent(ctx, e.EnvID); err != nil {
		return nil, err
	}
	// cleanup_state = done 之后环境不再是候选：先登记待归还，使提交结果未知或归还失败时由
	// retryReleases 在后续轮次补做。若 done 实际未提交，补做的归还因前置条件冲突而放弃，
	// 环境仍是候选，下一轮重做。
	c.mu.Lock()
	c.pendingFree[e.EnvID] = struct{}{}
	c.mu.Unlock()
	if _, err := c.store.UpdateCleanup(ctx, CleanupUpdate{EnvID: e.EnvID, ExpectedTries: e.CleanupTries, State: CleanupDone}); err != nil {
		return nil, err
	}
	return nil, c.releaseUIDRange(ctx, e.EnvID)
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

// uidFilesReportLimit 是归还前核查残留文件时最多列出的路径数；隔离原因中写出其中前 uidFilesReasonPaths 个。
const (
	uidFilesReportLimit = 16
	uidFilesReasonPaths = 4
)

// releaseUIDRange 归还环境的 UID 范围（规格 §4.5：环境已停止且清理完成、范围拥有的文件已不存在、无残留挂载；
// 挂载与环境目录由 Destroy 的逐层核对保证）。归还前（M4 Plan 15 D13、E39）：
//
//  1. ReclaimUIDFiles：<data>/workspaces 下仍归该范围的文件改回 root 属主（任务 workspace 在任务结束后仍归
//     最后一个 attempt 的范围；下个 attempt 启动时会重新 chown，无损）；
//  2. UIDFiles：数据目录其他位置仍有归该范围的文件 → QuarantineUIDRange 并报警（I8 路径），不归还。
//
// 回收或核查出错时、环境的入口仍被持有时（Options.EntryHeld）本轮不归还，留在待归还集合中由下一轮重试。成功归还、已隔离、没有范围或冲突（分配已变化，
// 重试无意义，交给启动核对）时移出待归还集合。
func (c *Coordinator) releaseUIDRange(ctx context.Context, envID string) error {
	ur, err := c.store.GetUIDRange(ctx, envID)
	if err == nil && isOwnerRange(ur) {
		// 按 owner 保留的范围（session.go）不随环境清理归还，由 ReleaseOwnerUIDRange 归还。
		c.mu.Lock()
		delete(c.pendingFree, envID)
		c.mu.Unlock()
		return nil
	}
	if err == nil && c.opt.EntryHeld != nil && c.opt.EntryHeld(envID) {
		// 入口（Gateway socket）尚未撤销：它归该范围所有但不是残留。留在待归还集合中，撤销之后再核查归还
		// （见 Options.EntryHeld）。推迟过久（入口泄漏）时记一次 WARN，不让范围被静默占用。
		now := c.opt.Now()
		c.mu.Lock()
		first, seen := c.entryDeferred[envID]
		if !seen {
			first = now
			c.entryDeferred[envID] = now
		}
		warn := now.Sub(first) > EntryHeldWarnAfter && !c.entryWarned[envID]
		if warn {
			c.entryWarned[envID] = true
		}
		c.mu.Unlock()
		if warn {
			c.opt.Logger.Warn("环境的 Gateway 入口长时间未撤销，UID 范围推迟归还", "env_id", envID,
				"uid_range", ur.UIDRangeID, "deferred_for", now.Sub(first).Round(time.Second).String())
		}
		return nil
	}
	c.mu.Lock()
	delete(c.entryDeferred, envID)
	delete(c.entryWarned, envID)
	c.mu.Unlock()
	released := false
	if err == nil {
		released, err = c.checkAndRelease(ctx, ur, func(ctx context.Context) error {
			_, err := c.store.ReleaseUIDRange(ctx, ur.UIDRangeID, ur.AllocationID)
			return err
		})
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
	if released {
		c.notifyFreed()
	}
	return err
}

// ReleaseCheckedUIDRange 是启动核对（recovery 的 ReleaseUIDRange 步骤）的归还：与 cleanup loop 相同的回收与核查
// （ReclaimUIDFiles → provider.ScanUIDFiles → ReleaseUIDRange；数据目录中仍有归该范围的文件时 QuarantineUIDRange
// 并报警，不归还；M4 Plan 15 D13、E39）。released 为假且 err 为 nil 表示已隔离。分配代次不符为 persistence.ErrConflict。
func (c *Coordinator) ReleaseCheckedUIDRange(ctx context.Context, ur UIDRange) (released bool, err error) {
	released, err = c.checkAndRelease(ctx, ur, func(ctx context.Context) error {
		_, err := c.store.ReleaseUIDRange(ctx, ur.UIDRangeID, ur.AllocationID)
		return err
	})
	if released {
		c.notifyFreed()
	}
	return released, err
}

// checkAndRelease 回收 workspace 属主、以 provider.ScanUIDFiles 核查残留文件，然后经 release 归还（released 为真）
// 或隔离该范围。
func (c *Coordinator) checkAndRelease(ctx context.Context, ur UIDRange, release func(context.Context) error) (released bool, err error) {
	if ur.Base < 0 || ur.Size <= 0 || ur.Base+ur.Size-1 > math.MaxUint32 {
		return false, fmt.Errorf("resource: UID 范围 %s [%d, +%d) 超出 uint32", ur.UIDRangeID, ur.Base, ur.Size)
	}
	base, size := uint32(ur.Base), uint32(ur.Size)
	if _, err := c.p.ReclaimUIDFiles(ctx, base, size); err != nil {
		return false, fmt.Errorf("resource: 回收 UID 范围 %s 的 workspace 文件: %w", ur.UIDRangeID, err)
	}
	items, err := provider.ScanUIDFiles(ctx, c.p, []provider.UIDSpan{{ID: ur.UIDRangeID, Base: base, Size: size}}, uidFilesReportLimit)
	if err != nil {
		return false, fmt.Errorf("resource: 核查 UID 范围 %s 的残留文件: %w", ur.UIDRangeID, err)
	}
	left := make([]string, 0, len(items))
	for _, it := range items {
		left = append(left, it.Path)
	}
	if len(left) == 0 {
		err = release(ctx)
		return err == nil, err
	}
	count := fmt.Sprint(len(left))
	if len(left) >= uidFilesReportLimit {
		count = "至少 " + count
	}
	reason := fmt.Sprintf("归还前数据目录中仍有 %s 个条目归该范围所有：%s", count,
		strings.Join(left[:min(len(left), uidFilesReasonPaths)], ", "))
	if err := c.persist(ctx, func(ctx context.Context) error {
		return c.store.QuarantineUIDRange(ctx, ur.UIDRangeID, ur.AllocationID, reason)
	}); err != nil {
		return false, fmt.Errorf("resource: 隔离 UID 范围 %s: %w", ur.UIDRangeID, err)
	}
	q := Quarantine{Layer: provider.LayerUIDFiles, Path: UIDRangeQuarantinePath(ur.UIDRangeID), ObservedOwner: ur.OwnerID, Reason: reason}
	c.opt.Alert(q)
	c.mu.Lock()
	c.pendingAlert[q.Path] = struct{}{}
	c.mu.Unlock()
	return false, c.markAlerted(ctx, q.Path)
}

// markAlerted 记录报警已发出（I8）；失败时保留在待标记集合中，由后续轮次重试。
func (c *Coordinator) markAlerted(ctx context.Context, path string) error {
	err := c.persist(ctx, func(ctx context.Context) error { return c.store.MarkQuarantineAlerted(ctx, path) })
	if err != nil && !errors.Is(err, persistence.ErrNotFound) {
		return err
	}
	c.mu.Lock()
	delete(c.pendingAlert, path)
	c.mu.Unlock()
	return nil
}

// retryAlerts 补做此前失败的报警标记。
func (c *Coordinator) retryAlerts(ctx context.Context) error {
	c.mu.Lock()
	paths := make([]string, 0, len(c.pendingAlert))
	for p := range c.pendingAlert {
		paths = append(paths, p)
	}
	c.mu.Unlock()
	var first error
	for _, p := range paths {
		if err := c.markAlerted(ctx, p); err != nil && first == nil {
			first = err
		}
	}
	return first
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
