package resource

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// 本文件是会话环境的资源操作（M4 Plan 12 Task 5；规格 §12.1、§12.2、§12.4、§4.5）：按 owner 保留的 UID 范围，
// 以及经 env 串行执行者的冻结、解冻与进程表。cleanup loop 只在 releaseUIDRange 中跳过 owner 范围（cleanup.go）。

// OwnerPrefixSession 是会话 owner 的前缀：EnvRequest.UIDOwner = "session:<session_id>"。
const OwnerPrefixSession = "session:"

// OwnerUIDStore 是按 owner 保留 UID 范围所需的持久化用例（规格 §12.1"session 在整个生命周期保留其 UID 范围"）。
// 它不在 Store 中：只有支持会话的 Store 实现它；不支持时带 UIDOwner 的 CreateEnv 失败。
type OwnerUIDStore interface {
	// AssignOwnerUIDRange 为 envID 取得 owner 的 UID 范围：owner 已有 assigned 范围时返回它（同一 allocationID，
	// 否则为冲突），否则分配一段空闲范围给 owner（owner_id = owner）。两种情况都记录 envID 使用该范围
	// （environments.uid_range_id）。没有空闲范围为 ErrNoFreeUIDRange。
	AssignOwnerUIDRange(ctx context.Context, owner, envID, allocationID string) (UIDRange, error)
	// ReleaseOwnerUIDRange 归还 owner 的范围：使用过该范围的环境须全部已停止且清理完成（规格 §4.5），否则
	// 为 persistence.ErrConflict；owner 没有 assigned 范围为 persistence.ErrNotFound；allocationID 不符为冲突。
	ReleaseOwnerUIDRange(ctx context.Context, owner, allocationID string) (UIDRange, error)
	// OwnerUIDRange 读取 owner 的 assigned 范围，以及使用过它而尚未"已停止且清理完成"的环境数（归还前先确认这一
	// 条件，再核查文件；M4 Plan 15 Task 10）。owner 没有 assigned 范围为 persistence.ErrNotFound。
	OwnerUIDRange(ctx context.Context, owner string) (ur UIDRange, pendingEnvs int, err error)
}

// ownerAllocationID 是 owner 范围的分配代次：每个 owner 只分配一次（会话关闭后不再分配）。
func ownerAllocationID(owner string) string { return "owner-uid:" + owner }

// isOwnerRange 报告范围是否按 owner 分配（不随环境清理归还）。
func isOwnerRange(ur UIDRange) bool { return strings.HasPrefix(ur.OwnerID, OwnerPrefixSession) }

func (c *Coordinator) ownerStore() (OwnerUIDStore, error) {
	ost, ok := c.store.(OwnerUIDStore)
	if !ok {
		return nil, errors.New("resource: Store 不支持按 owner 保留的 UID 范围（UIDOwner）")
	}
	return ost, nil
}

// assignRange 按请求分配 UID 范围：UIDOwner 为空时按环境（assignUIDRange），否则按 owner。
func (c *Coordinator) assignRange(ctx context.Context, r EnvRequest) (UIDRange, error) {
	if r.UIDOwner == "" {
		return c.assignUIDRange(ctx, r.EnvID)
	}
	if !strings.HasPrefix(r.UIDOwner, OwnerPrefixSession) || r.UIDOwner == OwnerPrefixSession {
		return UIDRange{}, fmt.Errorf("resource: 非法的 UIDOwner %q（须为 %s<session_id>）", r.UIDOwner, OwnerPrefixSession)
	}
	ost, err := c.ownerStore()
	if err != nil {
		return UIDRange{}, err
	}
	return c.waitFreeRange(ctx, func(ctx context.Context) (UIDRange, error) {
		return ost.AssignOwnerUIDRange(ctx, r.UIDOwner, r.EnvID, ownerAllocationID(r.UIDOwner))
	})
}

// ReleaseOwnerUIDRange 在会话 workspace 删除后归还该 owner 的 UID 范围（规格 §4.5 回收条件：使用过它的环境
// 均已停止且清理完成）。幂等：owner 没有范围时返回 nil。条件未满足时返回 persistence.ErrConflict（调用方稍后重试）。
//
// 先确认使用过该范围的环境全部清理完成（否则存活会话的文件会被误判为残留），再与环境范围一样回收并核查文件
// （ReclaimUIDFiles → provider.ScanUIDFiles，M4 Plan 15 D13）：数据目录中仍有归该范围的文件时隔离该范围并报警、
// 不归还，返回 nil（范围不再分配，会话关闭照常完成）。
func (c *Coordinator) ReleaseOwnerUIDRange(ctx context.Context, owner string) error {
	if !strings.HasPrefix(owner, OwnerPrefixSession) || owner == OwnerPrefixSession {
		return fmt.Errorf("resource: 非法的 owner %q", owner)
	}
	ost, err := c.ownerStore()
	if err != nil {
		return err
	}
	var ur UIDRange
	var pending int
	err = c.persist(ctx, func(ctx context.Context) error {
		var err error
		ur, pending, err = ost.OwnerUIDRange(ctx, owner)
		return err
	})
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("resource: 读取 %s 的 UID 范围: %w", owner, err)
	case pending > 0:
		return fmt.Errorf("resource: 归还 %s 的 UID 范围: %w（%d 个环境尚未停止并完成清理）", owner, persistence.ErrConflict, pending)
	}
	released, err := c.checkAndRelease(ctx, ur, func(ctx context.Context) error {
		return c.persist(ctx, func(ctx context.Context) error {
			_, err := ost.ReleaseOwnerUIDRange(ctx, owner, ownerAllocationID(owner))
			return err
		})
	})
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("resource: 归还 %s 的 UID 范围: %w", owner, err)
	}
	if released {
		c.notifyFreed()
	}
	return nil
}

// FreezeEnv 在环境的串行执行者内冻结它（规格 §12.2）：provider 确认 frozen 1 后返回 nil；
// 未确认为 provider.ErrFreezeUnconfirmed（provider 已尝试解冻，调用方驱逐）。
func (c *Coordinator) FreezeEnv(ctx context.Context, envID string) error {
	unlock, err := c.lock(ctx, envID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := c.p.Freeze(ctx, envID); err != nil {
		return fmt.Errorf("resource: 冻结环境 %s: %w", envID, err)
	}
	return nil
}

// ThawEnv 在环境的串行执行者内解冻它。
func (c *Coordinator) ThawEnv(ctx context.Context, envID string) error {
	unlock, err := c.lock(ctx, envID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := c.p.Thaw(ctx, envID); err != nil {
		return fmt.Errorf("resource: 解冻环境 %s: %w", envID, err)
	}
	return nil
}

// EnvProcs 返回环境 cgroup.procs（规格 §12.4 释放核验的进程基线比较）。只读，不经串行执行者：
// 释放核验与同一环境的其他物理操作不并发（session actor 串行驱动），且读取本身不改变状态。
func (c *Coordinator) EnvProcs(ctx context.Context, envID string) ([]int, error) {
	pids, err := c.p.Procs(ctx, envID)
	if err != nil {
		return nil, fmt.Errorf("resource: 读取环境 %s 的进程: %w", envID, err)
	}
	return pids, nil
}
