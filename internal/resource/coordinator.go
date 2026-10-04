package resource

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// Provider 是 coordinator 使用的 provider 子集（Provider 契约第 1 节：消费者声明窄接口）。
// StartExec 由 runner 直接调用，不经 coordinator（契约第 5 节）。
type Provider interface {
	Create(ctx context.Context, spec provider.EnvSpec) (provider.EnvInfo, error)
	Stop(ctx context.Context, envID string) error
	Destroy(ctx context.Context, envID string) error
	List(ctx context.Context) ([]provider.EnvInfo, error)
	Scan(ctx context.Context) (provider.ScanReport, error)
}

// EnvRequest 描述要创建的环境。environments 行由调用方（task/session actor、gateway）先行创建；
// coordinator 补全 install_id 与 UID 范围后调用 provider。
type EnvRequest struct {
	EnvID     string
	AttemptID string
	Kind      provider.EnvKind
	Template  string
	Limits    provider.Limits
	Mounts    provider.Mounts
}

// StopResult 是 StopEnv 报告的事实。
//
//   - Stopped：provider.Stop 已确认执行树不存在；At 是停止时间（已记录时为持久化的 stopped_at）。
//   - Recorded：stopped_at 已持久化。Stopped 为真而 Recorded 为假表示停止已确认但 Store 写入
//     失败（规格 §14.5）：事实保留在 coordinator 中，再次调用 StopEnv 以原时间补提交，不再调用
//     provider。**调用方只在 Recorded 为真时归还 admission 容量**。
//   - Blocked：期限内未确认停止（ErrStopUnconfirmed，即 stop_blocked）；不写 stopped_at，
//     调用方继续占用容量并阻止替代执行（规格 §8.2）。
//
// StopEnv 只在停止本身不确定时返回错误（provider 错误，或确认前 ctx 结束）。
type StopResult struct {
	Stopped  bool
	Recorded bool
	At       time.Time
	Blocked  bool
}

// Options 配置 coordinator。零值字段取规格 §19 的默认值。
type Options struct {
	InstallID string

	// UID 范围池：UIDCount > 0 时，首次创建前幂等地登记 UIDCount 段 [UIDBase+i*UIDSize, ...)；
	// UIDCount 为 0 表示范围池已由他处登记。UIDSize 默认 4096。
	UIDBase  int64
	UIDSize  int64
	UIDCount int

	// Backoff 返回第 try 次（从 0 起）失败后的等待时间；默认基数 2 s、上限 60 s、带抖动。
	Backoff func(try int) time.Duration
	// Now 是时钟；默认 time.Now。
	Now func() time.Time
	// CleanupInterval 是 cleanup loop 两轮之间的间隔；默认 2 s。
	CleanupInterval time.Duration
	// CleanupBatch 是每轮最多处理的环境数；默认 16。
	CleanupBatch int
}

// ErrEnvStopped 表示环境已确认停止（stopped_at 已记录或待提交），不能再创建：停止之后到达的
// 创建请求是过期操作（规格 §3.2、§4.4）。
var ErrEnvStopped = errors.New("resource: 环境已停止，不能再创建")

// 资源意图的类型；每个环境恰有一个创建意图，身份由 env_id 决定，重试不产生第二份。
const intentKindEnvironment = "environment"

func intentID(envID string) string     { return "env-create:" + envID }
func allocationID(envID string) string { return "env-uid:" + envID }

// Coordinator 按 env_id 串行化物理操作（Create、Stop、Destroy），记录 intent、UID 范围与
// stopped_at，只报告事实，不决定任务状态（规格 §3.2）。
type Coordinator struct {
	store Store
	p     Provider
	opt   Options

	mu           sync.Mutex
	locks        map[string]*envLock
	pendingStop  map[string]time.Time // 已确认停止、stopped_at 尚未提交
	pendingFree  map[string]struct{}  // 清理已完成、UID 范围尚未归还
	seeded       bool
	onLockWait   func(envID string) // 测试钩子：串行执行者被占用、开始等待时调用
	onCleanupRun func()             // 测试钩子：cleanup loop 每轮结束时调用
}

// envLock 是一个环境的串行执行者：容量为 1 的信号量，等待可被 ctx 取消。
type envLock struct {
	sem  chan struct{}
	refs int
}

// NewCoordinator 返回 coordinator。
func NewCoordinator(store Store, p Provider, opt Options) *Coordinator {
	if opt.UIDSize == 0 {
		opt.UIDSize = 4096
	}
	if opt.Backoff == nil {
		opt.Backoff = DefaultBackoff
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.CleanupInterval <= 0 {
		opt.CleanupInterval = 2 * time.Second
	}
	if opt.CleanupBatch <= 0 {
		opt.CleanupBatch = 16
	}
	return &Coordinator{
		store: store, p: p, opt: opt,
		locks:       make(map[string]*envLock),
		pendingStop: make(map[string]time.Time),
		pendingFree: make(map[string]struct{}),
	}
}

// DefaultBackoff 是规格 §19 的重试退避：基数 2 s，每次翻倍，上限 60 s，在 [d/2, d] 内均匀抖动。
func DefaultBackoff(try int) time.Duration {
	const base, ceiling = 2 * time.Second, 60 * time.Second
	d := ceiling
	if try < 5 { // 2 s << 5 = 64 s 已超过上限
		d = min(base<<max(try, 0), ceiling)
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// lock 取得 envID 的串行执行者；ctx 结束时放弃等待。
func (c *Coordinator) lock(ctx context.Context, envID string) (func(), error) {
	c.mu.Lock()
	l := c.locks[envID]
	if l == nil {
		l = &envLock{sem: make(chan struct{}, 1)}
		c.locks[envID] = l
	}
	l.refs++
	hook := c.onLockWait
	c.mu.Unlock()

	select {
	case l.sem <- struct{}{}:
	default:
		if hook != nil {
			hook(envID)
		}
		select {
		case l.sem <- struct{}{}:
		case <-ctx.Done():
			c.unref(envID, l)
			return nil, ctx.Err()
		}
	}
	return func() {
		<-l.sem
		c.unref(envID, l)
	}, nil
}

func (c *Coordinator) unref(envID string, l *envLock) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l.refs--; l.refs == 0 {
		delete(c.locks, envID)
	}
}

// CreateEnv 创建环境：RecordIntent → AssignUIDRange → provider.Create → ResolveIntent(acquired)。
//
// provider 的结果按契约第 4 节处理：ErrIncomplete（本安装的残留）→ Stop → Destroy → 以新的创建
// 重建一次；ErrForeign → 记录隔离、不销毁；ErrConflict → 返回。失败时只有在独立原始扫描确认
// 该环境没有任何残留时才把 intent 置为 failed，否则保留 pending 交给清理（规格 §8.3）。
// 环境已确认停止时返回 ErrEnvStopped，不触碰 provider。
func (c *Coordinator) CreateEnv(ctx context.Context, r EnvRequest) (provider.EnvInfo, error) {
	if r.EnvID == "" {
		return provider.EnvInfo{}, errors.New("resource: CreateEnv 缺少 env_id")
	}
	unlock, err := c.lock(ctx, r.EnvID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	defer unlock()

	info, err := c.create(ctx, r)
	if err != nil {
		return provider.EnvInfo{}, fmt.Errorf("resource: 创建环境 %s: %w", r.EnvID, err)
	}
	return info, nil
}

func (c *Coordinator) create(ctx context.Context, r EnvRequest) (provider.EnvInfo, error) {
	// 停止之后不再创建：stopped_at 的条件是"无在途创建"（规格 §4.4）。
	if c.stopPending(r.EnvID) {
		return provider.EnvInfo{}, ErrEnvStopped
	}
	env, err := c.store.GetEnvironment(ctx, r.EnvID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	if env.StoppedAt != nil {
		return provider.EnvInfo{}, ErrEnvStopped
	}
	if err := c.seed(ctx); err != nil {
		return provider.EnvInfo{}, err
	}

	// intent 先于物理操作（契约第 6 节）。
	id := intentID(r.EnvID)
	if _, err := c.store.RecordIntent(ctx, Intent{IntentID: id, EnvID: r.EnvID, Kind: intentKindEnvironment, Name: r.EnvID}); err != nil {
		return provider.EnvInfo{}, err
	}
	ur, err := c.store.AssignUIDRange(ctx, r.EnvID, allocationID(r.EnvID))
	if err != nil {
		return provider.EnvInfo{}, err
	}
	if ur.Base < 0 || ur.Size <= 0 || ur.Base+ur.Size-1 > math.MaxUint32 {
		return provider.EnvInfo{}, fmt.Errorf("resource: UID 范围 %s [%d, +%d) 超出 uint32", ur.UIDRangeID, ur.Base, ur.Size)
	}
	spec := provider.EnvSpec{
		EnvID: r.EnvID, InstallID: c.opt.InstallID, Kind: r.Kind,
		UIDBase: uint32(ur.Base), UIDSize: uint32(ur.Size),
		Template: r.Template, Limits: r.Limits, Mounts: r.Mounts,
	}

	info, err := c.p.Create(ctx, spec)
	if errors.Is(err, provider.ErrIncomplete) {
		// 本安装的残留：不补完、不认领，停止并拆除后以新的创建重建一次（契约第 3 节）。
		if err = c.p.Stop(ctx, r.EnvID); err == nil {
			if err = c.p.Destroy(ctx, r.EnvID); err == nil {
				info, err = c.p.Create(ctx, spec)
			}
		}
	}
	switch {
	case err == nil:
		if err := c.persist(ctx, func(ctx context.Context) error {
			_, err := c.store.ResolveIntent(ctx, id, IntentAcquired)
			return err
		}); err != nil {
			// 环境已存在而 intent 仍为 pending：交给调用方按失败处理，残留由停止与清理回收。
			return provider.EnvInfo{}, fmt.Errorf("记录 intent acquired: %w", err)
		}
		return info, nil
	case errors.Is(err, provider.ErrForeign):
		// 不自动销毁；记录隔离并计入占用（规格 §14.1 扫描表）。intent 保留 pending。
		if qerr := c.quarantine(ctx, r.EnvID, "create: 同名环境不属于本安装或归属无法判定"); qerr != nil {
			return provider.EnvInfo{}, errors.Join(err, fmt.Errorf("记录隔离: %w", qerr))
		}
		return provider.EnvInfo{}, err
	case errors.Is(err, provider.ErrConflict):
		return provider.EnvInfo{}, err // 同名环境属于本安装：残留存在，intent 保留 pending
	}
	c.failIntentIfNoResidue(ctx, r.EnvID)
	return provider.EnvInfo{}, err
}

// quarantine 以独立原始扫描报告的资源记录隔离：每个 EnvID 匹配的扫描项按其 Layer 与 Path
// 原样记录，coordinator 不构造路径（资源归属以 provider 的报告为准）。
func (c *Coordinator) quarantine(ctx context.Context, envID, reason string) error {
	rep, err := c.p.Scan(ctx)
	if err != nil {
		return fmt.Errorf("扫描: %w", err)
	}
	found := false
	for _, it := range rep.Items {
		if it.EnvID != envID {
			continue
		}
		found = true
		q := Quarantine{Layer: it.Layer, Path: it.Path, Reason: reason}
		if err := c.persist(ctx, func(ctx context.Context) error { return c.store.RecordQuarantine(ctx, q) }); err != nil {
			return err
		}
	}
	if !found {
		return fmt.Errorf("扫描未报告环境 %s 的任何资源", envID)
	}
	return nil
}

// failIntentIfNoResidue 只在独立原始扫描确认 envID 没有任何一层残留时把 intent 置为 failed
// （规格 §8.3：failed 仅在确认无残留时设置；不能仅凭环境列表为空认定）。ctx 已结束、扫描失败
// 或发现残留时保留 pending，交给停止与清理。
func (c *Coordinator) failIntentIfNoResidue(ctx context.Context, envID string) {
	if ctx.Err() != nil {
		return
	}
	rep, err := c.p.Scan(ctx)
	if err != nil {
		return
	}
	for _, it := range rep.Items {
		if it.EnvID == envID {
			return
		}
	}
	_, _ = c.store.ResolveIntent(ctx, intentID(envID), IntentFailed) // 失败时保留 pending，同样安全
}

// seed 在首次创建前幂等地登记 UID 范围池。
func (c *Coordinator) seed(ctx context.Context) error {
	c.mu.Lock()
	done := c.seeded || c.opt.UIDCount == 0
	c.mu.Unlock()
	if done {
		return nil
	}
	if err := c.store.SeedUIDRanges(ctx, c.opt.UIDBase, c.opt.UIDSize, c.opt.UIDCount); err != nil {
		return fmt.Errorf("登记 UID 范围: %w", err)
	}
	c.mu.Lock()
	c.seeded = true
	c.mu.Unlock()
	return nil
}

// StopEnv 停止环境并报告事实：provider.Stop 成功 → MarkStopped → Stopped；ErrStopUnconfirmed →
// Blocked，不写 stopped_at。停止不等待 Store：先完成物理停止，再以退避重试提交 stopped_at，
// 直到成功（Recorded）或 ctx 结束；未提交的"已停止"事实以 Recorded=false 报告（不是错误），
// 保留在 coordinator 中，下次调用以原时间补提交（规格 §14.5）。
func (c *Coordinator) StopEnv(ctx context.Context, envID string) (StopResult, error) {
	unlock, err := c.lock(ctx, envID)
	if err != nil {
		return StopResult{}, err
	}
	defer unlock()

	c.mu.Lock()
	at, pending := c.pendingStop[envID]
	c.mu.Unlock()
	if !pending {
		switch err := c.p.Stop(ctx, envID); {
		case errors.Is(err, provider.ErrStopUnconfirmed):
			return StopResult{Blocked: true}, nil
		case err != nil:
			return StopResult{}, fmt.Errorf("resource: 停止环境 %s: %w", envID, err)
		}
		at = c.opt.Now()
		c.mu.Lock()
		c.pendingStop[envID] = at
		c.mu.Unlock()
	}

	var env Environment
	err = c.persist(ctx, func(ctx context.Context) error {
		var err error
		env, err = c.store.MarkStopped(ctx, envID, at)
		return err
	})
	if err != nil {
		// 停止已确认，只是事实尚未持久化：不是错误，以 Recorded=false 报告，事实保留待补提交。
		return StopResult{Stopped: true, At: at}, nil
	}
	c.mu.Lock()
	delete(c.pendingStop, envID)
	c.mu.Unlock()
	if env.StoppedAt != nil {
		at = *env.StoppedAt // 已记录时保持原值，重试不倒退
	}
	return StopResult{Stopped: true, Recorded: true, At: at}, nil
}

// ReclaimOrphan 回收属于本安装、没有对应记录的环境资源（规格 §14.1 扫描表）：在该环境的串行执行者内
// Stop，权威检查成立后 Destroy 逐层销毁。资源已不存在时幂等返回 nil。停止未确认时返回
// provider.ErrStopUnconfirmed（不销毁，由调用方记为阻塞）；外来资源返回 provider.ErrForeign（不触碰）。
// 不写数据库：孤儿没有 environments 行。
func (c *Coordinator) ReclaimOrphan(ctx context.Context, envID string) error {
	unlock, err := c.lock(ctx, envID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := c.p.Stop(ctx, envID); err != nil {
		return fmt.Errorf("resource: 回收孤儿环境 %s: 停止: %w", envID, err)
	}
	if err := c.p.Destroy(ctx, envID); err != nil {
		return fmt.Errorf("resource: 回收孤儿环境 %s: 销毁: %w", envID, err)
	}
	return nil
}

func (c *Coordinator) stopPending(envID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pendingStop[envID]
	return ok
}

// persist 以退避重试一次物理操作之后的事实提交，直到成功、遇到不可重试的错误或 ctx 结束。
func (c *Coordinator) persist(ctx context.Context, op func(context.Context) error) error {
	for try := 0; ; try++ {
		err := op(ctx)
		if err == nil || !transient(err) || ctx.Err() != nil {
			return err
		}
		t := time.NewTimer(c.opt.Backoff(try))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

// transient 报告 Store 错误是否值得以同一身份重试（各用例幂等）。
func transient(err error) bool {
	return errors.Is(err, persistence.ErrUnavailable) || errors.Is(err, persistence.ErrCommitUnknown) ||
		errors.Is(err, persistence.ErrContention)
}
