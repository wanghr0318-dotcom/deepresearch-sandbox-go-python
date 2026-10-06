package app

// 本文件装配独立 exec 沙箱（M4 Plan 15，规格 §10；计划 D4、D8、D11）：exec 配置的默认值与校验、admission.ExecGate
// 到 call.ExecSlots 的适配、resource coordinator 与 provider 到 call.ExecEnvs 的适配（execEnvAdapter），以及启动恢复后
// stop_blocked 的 exec 环境占用 exec slot。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
)

// ExecConfig 是 exec 的服务端策略（server 标志，计划 D4 的默认值）。Slots 为 0 时不启用 exec（/v1/exec 为 404
// endpoint_not_configured）；Slots > 0 时其余零值字段取默认值。exec 环境的内存不计入任务环境内存池（D11）：
// exec 内存总量 = Slots × MemoryMax，由运维与 Capacity.MemoryBytes 一并规划。
type ExecConfig struct {
	Slots   int // 全局 exec slots（--exec-slots，默认 4；0 关闭）
	PerTask int // 每任务并发 exec 上限（--exec-per-task，默认 2）

	// 每任务配额（exec_quotas 行在首次 exec 时按它建立）。
	CountLimit int64         // --exec-count-limit，默认 50
	CPUSeconds int64         // --exec-cpu-seconds，默认 600 CPU 秒
	WallLimit  time.Duration // --exec-wall-limit，默认 1800 s（每任务累计）

	// 每次 exec 的限制。
	WallDefault, WallMax     time.Duration // --exec-wall-default 60 s、--exec-wall-max 300 s
	MemoryDefault, MemoryMax int64         // --exec-memory-default 512 MiB、--exec-memory-max 1 GiB
	QueueTimeout             time.Duration // --exec-queue-timeout，默认 60 s
	PidsMax                  int64         // --exec-pids-max，默认 128
	CPUQuotaUs               int64         // --exec-cpu-quota-us（cpu.max quota，period 100000），默认 100000（1 核）
	TmpBytes                 int64         // --exec-tmp-bytes（/tmp tmpfs），默认 64 MiB
	OutBytes                 int64         // --exec-out-bytes（/out tmpfs），默认 64 MiB；也是 RLIMIT_FSIZE
	NoFile                   uint64        // RLIMIT_NOFILE，固定默认 256
}

// exec 的默认值（计划 D4、Global Constraints）。
const (
	DefaultExecSlots         = 4
	DefaultExecPerTask       = 2
	DefaultExecCountLimit    = 50
	DefaultExecCPUSeconds    = 600
	DefaultExecWallLimit     = 1800 * time.Second
	DefaultExecWallDefault   = 60 * time.Second
	DefaultExecWallMax       = 300 * time.Second
	DefaultExecMemoryDefault = 512 << 20
	DefaultExecMemoryMax     = 1 << 30
	DefaultExecQueueTimeout  = 60 * time.Second
	DefaultExecPidsMax       = 128
	DefaultExecCPUQuotaUs    = 100000
	DefaultExecTmpBytes      = 64 << 20
	DefaultExecOutBytes      = 64 << 20
	defaultExecNoFile        = 256
	execTemplate             = "exec" // rootfs.ExecTemplateName（exec 模板：不含 /opt/agentbox）
	cpuPeriodUs              = 100000
)

// Enabled 报告是否启用 exec。
func (e ExecConfig) Enabled() bool { return e.Slots > 0 }

func (e ExecConfig) withDefaults() ExecConfig {
	if !e.Enabled() {
		return e
	}
	def := func(p *int64, v int64) {
		if *p <= 0 {
			*p = v
		}
	}
	defDur := func(p *time.Duration, v time.Duration) {
		if *p <= 0 {
			*p = v
		}
	}
	if e.PerTask <= 0 {
		e.PerTask = DefaultExecPerTask
	}
	def(&e.CountLimit, DefaultExecCountLimit)
	def(&e.CPUSeconds, DefaultExecCPUSeconds)
	defDur(&e.WallLimit, DefaultExecWallLimit)
	defDur(&e.WallDefault, DefaultExecWallDefault)
	defDur(&e.WallMax, DefaultExecWallMax)
	def(&e.MemoryDefault, DefaultExecMemoryDefault)
	def(&e.MemoryMax, DefaultExecMemoryMax)
	defDur(&e.QueueTimeout, DefaultExecQueueTimeout)
	def(&e.PidsMax, DefaultExecPidsMax)
	def(&e.CPUQuotaUs, DefaultExecCPUQuotaUs)
	def(&e.TmpBytes, DefaultExecTmpBytes)
	def(&e.OutBytes, DefaultExecOutBytes)
	if e.NoFile == 0 {
		e.NoFile = defaultExecNoFile
	}
	return e
}

// Validate 校验 exec 配置（已取默认值之后）：每任务上限不超过全局 slots，默认值不超过上限，wall 以毫秒计为正。
func (e ExecConfig) Validate() error {
	switch {
	case e.Slots < 0:
		return fmt.Errorf("exec slots 不能为负数，得到 %d", e.Slots)
	case !e.Enabled():
		return nil
	case e.PerTask > e.Slots:
		return fmt.Errorf("exec 每任务上限 %d 超过全局 exec slots %d", e.PerTask, e.Slots)
	case e.WallDefault > e.WallMax:
		return fmt.Errorf("exec wall 默认值 %s 超过上限 %s", e.WallDefault, e.WallMax)
	case e.WallMax > e.WallLimit:
		return fmt.Errorf("exec 单次 wall 上限 %s 超过每任务累计 %s", e.WallMax, e.WallLimit)
	case e.WallDefault < time.Millisecond:
		return fmt.Errorf("exec wall 默认值 %s 须至少 1 ms", e.WallDefault)
	case e.MemoryDefault > e.MemoryMax:
		return fmt.Errorf("exec 内存默认值 %d 超过上限 %d", e.MemoryDefault, e.MemoryMax)
	case e.CPUSeconds > (1<<63-1)/1_000_000:
		return fmt.Errorf("exec CPU 配额 %d 秒过大", e.CPUSeconds)
	}
	return nil
}

// callConfig 构造 call.ExecConfig（Store、Envs、Slots、ImageDigest 由装配给出）。
func (e ExecConfig) callConfig(store call.ExecStore, envs call.ExecEnvs, slots call.ExecSlots, digest string) *call.ExecConfig {
	return &call.ExecConfig{
		Store: store, Envs: envs, Slots: slots, ImageDigest: digest,
		Policy:       call.ExecPolicy{CountLimit: e.CountLimit, CPULimitUsec: e.CPUSeconds * 1_000_000, WallLimitMs: e.WallLimit.Milliseconds()},
		Default:      call.ExecLimits{WallMs: e.WallDefault.Milliseconds(), MemoryBytes: e.MemoryDefault},
		Max:          call.ExecLimits{WallMs: e.WallMax.Milliseconds(), MemoryBytes: e.MemoryMax},
		QueueTimeout: e.QueueTimeout,
		CPURate:      float64(e.CPUQuotaUs) / cpuPeriodUs,
	}
}

// envLimits 是一次 exec 的环境限制：内存取请求的生效值，其余取策略。
func (e ExecConfig) envLimits(memory int64) provider.Limits {
	return provider.Limits{MemoryMax: memory, PidsMax: e.PidsMax, CPUQuotaUs: e.CPUQuotaUs, NoFile: e.NoFile,
		FSize: uint64(e.OutBytes), TmpBytes: e.TmpBytes}
}

// ---- admission.ExecGate → call.ExecSlots ----

// execSlots 把 ExecGate.Acquire（返回 ExecGrant）适配为 call 的窄接口（返回幂等的 release）。
type execSlots struct{ g *admission.ExecGate }

var _ call.ExecSlots = execSlots{}

func (x execSlots) Acquire(ctx context.Context, taskID string) (func(), error) {
	gr, err := x.g.Acquire(ctx, taskID)
	if err != nil {
		return nil, err
	}
	return sync.OnceFunc(func() { x.g.Release(gr) }), nil
}

// ---- resource.Coordinator + provider → call.ExecEnvs ----

// execCoordinator 是 resource.Coordinator 中 exec 使用的部分。
type execCoordinator interface {
	CreateEnv(ctx context.Context, r resource.EnvRequest) (provider.EnvInfo, error)
	StopEnv(ctx context.Context, envID string) (resource.StopResult, error)
	CleanupNow(ctx context.Context, envID string) error
}

var _ execCoordinator = (*resource.Coordinator)(nil)

// execProvider 是 provider 中 exec 直接使用的部分（启动、诊断与收集不经 coordinator，契约第 5 节）。
type execProvider interface {
	StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error)
	ResourceDiag(ctx context.Context, envID string) (provider.ResourceDiag, error)
	OpenOutputs(ctx context.Context, envID string, max int) ([]provider.OutputFile, []provider.SkippedOutput, error)
}

// execEnvAdapter 实现 call.ExecEnvs：Create 经 coordinator 创建 kind = exec 的环境（intent、UID 范围；环境行由
// ReserveExec 先行写入）并返回 /in 暂存目录；Stop 经 coordinator（stopped_at 已记录才可归还 slot）；Start、Diag、
// OpenOutputs 直达 provider；Cleanup 是 coordinator 对单个环境的同步清理（失败由 cleanup loop 接手，D8）。
type execEnvAdapter struct {
	c   execCoordinator
	p   execProvider
	cfg ExecConfig
}

var _ call.ExecEnvs = execEnvAdapter{}

func (x execEnvAdapter) Create(ctx context.Context, r call.ExecEnvRequest) (string, error) {
	info, err := x.c.CreateEnv(ctx, resource.EnvRequest{EnvID: r.EnvID, AttemptID: r.AttemptID, Kind: provider.KindExec,
		Template: execTemplate, Limits: x.cfg.envLimits(r.MemoryBytes), Mounts: provider.Mounts{OutBytes: x.cfg.OutBytes}})
	if err != nil {
		return "", err
	}
	if info.InDir == "" {
		return "", fmt.Errorf("app: exec 环境 %s 没有 /in 暂存目录", r.EnvID)
	}
	return info.InDir, nil
}

func (x execEnvAdapter) Start(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	return x.p.StartExec(ctx, envID, spec)
}

func (x execEnvAdapter) Stop(ctx context.Context, envID string) (call.ExecStop, error) {
	r, err := x.c.StopEnv(ctx, envID)
	return call.ExecStop{Stopped: r.Stopped, Recorded: r.Recorded, Blocked: r.Blocked}, err
}

func (x execEnvAdapter) Diag(ctx context.Context, envID string) (provider.ResourceDiag, error) {
	return x.p.ResourceDiag(ctx, envID)
}

func (x execEnvAdapter) OpenOutputs(ctx context.Context, envID string, max int) ([]provider.OutputFile, []provider.SkippedOutput, error) {
	return x.p.OpenOutputs(ctx, envID, max)
}

func (x execEnvAdapter) Cleanup(ctx context.Context, envID string) error {
	return x.c.CleanupNow(ctx, envID)
}

// ---- 启动恢复：stop_blocked 的 exec 环境 ----

// isExecEnv 报告环境是否是 exec 环境（env_id 由 call.ExecEnvID 生成，前缀 exec-）。
func isExecEnv(envID string) bool { return strings.HasPrefix(envID, "exec-") }

// occupyExecOrphans 处理没有 actor 负责的 stop_blocked 环境中的 exec 环境（§14.1、§14.5；D11）：exec 环境不占用任务
// run slot 与内存池，恢复为它重建的 run slot 占用立即归还，改以 ExecGate.Occupy 占用其任务的一个 exec slot，直到
// 后台重试确认停止（stopped_at 已记录）后归还。exec 关闭时保持恢复的 run slot 占用（保守）。
func (s *server) occupyExecOrphans(ctx context.Context, orphans []orphanStop) []orphanStop {
	if s.execGate == nil {
		return orphans
	}
	for i := range orphans {
		o := &orphans[i]
		if !isExecEnv(o.EnvID) {
			continue
		}
		taskID := ""
		if env, err := s.store.GetEnvironment(ctx, o.EnvID); err == nil && env.AttemptID != "" {
			if att, err := s.store.GetAttempt(ctx, env.AttemptID); err == nil {
				taskID = att.TaskID
			}
		}
		if o.HasGrant {
			s.adm.Release(o.Grant)
			o.HasGrant = false
		}
		g := s.execGate.Occupy(taskID)
		o.ExecGrant = &g
		s.log.Warn("stop_blocked 的 exec 环境占用 exec slot，确认停止后归还", "env_id", o.EnvID, "task_id", taskID)
	}
	return orphans
}

// errExecImage 包装 exec 模板检查的失败（Run 在取得任何锁之前返回它）。
var errExecImage = errors.New("app: exec 模板不可用")
