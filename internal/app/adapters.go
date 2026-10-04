package app

// 本文件是 task actor 窄接口的适配器（Plan 5 Task 7 报告"装配须写的适配器"）：task 不导入 runner、
// resource、admission 与 provider，装配代码把各包的实现转换到 task 的接口上。适配器不做决策：分类
// 只经 runner.Classify / runner.RetryOf，容量是否归还由 actor 依 StopReport.Recorded 决定。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// ---- 任务 limits ----

// taskLimits 是任务 limits_json 中装配代码识别的字段（规格未固定 limits 的结构；M1 取这些键，
// 其余键忽略）。0 表示未设置，取 Config 的默认值。
type taskLimits struct {
	MemoryMax    int64 `json:"memory_max"`      // 字节；也是 admission 申请的内存
	PidsMax      int64 `json:"pids_max"`        // pids.max
	CPUQuotaUs   int64 `json:"cpu_quota_us"`    // cpu.max 的 quota（period 固定 100000）
	NoFile       int64 `json:"nofile"`          // RLIMIT_NOFILE
	TmpBytes     int64 `json:"tmp_bytes"`       // /tmp、/run tmpfs 限额
	MaxRunTimeMs int64 `json:"max_run_time_ms"` // 累计运行时限（§14.4）
}

// parseLimits 解析 limits；空为全部未设置。字段类型不符或为负数是错误。
func parseLimits(raw json.RawMessage) (taskLimits, error) {
	var l taskLimits
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return l, nil
	}
	if err := json.Unmarshal(raw, &l); err != nil {
		return taskLimits{}, fmt.Errorf("limits 不合法: %w", err)
	}
	for name, v := range map[string]int64{"memory_max": l.MemoryMax, "pids_max": l.PidsMax, "cpu_quota_us": l.CPUQuotaUs,
		"nofile": l.NoFile, "tmp_bytes": l.TmpBytes, "max_run_time_ms": l.MaxRunTimeMs} {
		if v < 0 {
			return taskLimits{}, fmt.Errorf("limits.%s 不能为负数", name)
		}
	}
	if l.MaxRunTimeMs > math.MaxInt64/int64(time.Millisecond) {
		return taskLimits{}, errors.New("limits.max_run_time_ms 过大")
	}
	return l, nil
}

// memory 是任务申请的内存：limits.memory_max，未设置时取默认值（与 recovery 计算占用的规则相同）。
func (c Config) memory(l taskLimits) int64 {
	if l.MemoryMax > 0 {
		return l.MemoryMax
	}
	return c.DefaultMemoryBytes
}

// validateLimits 是 api.Config.ValidateLimits：拒绝格式错误与永远无法满足容量的 limits，使 actor 申请
// 槽位时不会遇到 admission.ErrExceedsCapacity（该错误在 actor 中是致命的）。
func (c Config) validateLimits(raw json.RawMessage) error {
	l, err := parseLimits(raw)
	if err != nil {
		return err
	}
	if m := c.memory(l); m > c.Capacity.MemoryBytes {
		return fmt.Errorf("limits.memory_max = %d 超过服务的总内存容量 %d，永远无法被授予", m, c.Capacity.MemoryBytes)
	}
	return nil
}

// runTimeLimit 是 task.Deps.RunTimeLimit：limits.max_run_time_ms，未设置时取 Config.MaxRunTime。
// limits 无法解析时（API 已校验，只可能来自外部改动）取默认值，不放开限额。
func (c Config) runTimeLimit(ts task.TaskState) time.Duration {
	l, err := parseLimits(ts.Limits)
	if err == nil && l.MaxRunTimeMs > 0 {
		return time.Duration(l.MaxRunTimeMs) * time.Millisecond
	}
	return c.MaxRunTime
}

// envLimits 是环境的资源限制：limits 中设置的字段覆盖 Config.DefaultLimits。
func (c Config) envLimits(l taskLimits) provider.Limits {
	out := c.DefaultLimits
	out.MemoryMax = c.memory(l)
	if l.PidsMax > 0 {
		out.PidsMax = l.PidsMax
	}
	if l.CPUQuotaUs > 0 {
		out.CPUQuotaUs = l.CPUQuotaUs
	}
	if l.NoFile > 0 {
		out.NoFile = uint64(l.NoFile)
	}
	if l.TmpBytes > 0 {
		out.TmpBytes = l.TmpBytes
	}
	return out
}

// ---- admission.Grant ↔ task.SlotGrant ----

type admissionAdapter struct {
	a   *admission.Admission
	cfg Config
}

var _ task.Admission = admissionAdapter{}

func (x admissionAdapter) Acquire(ctx context.Context, r task.SlotRequest) (task.SlotGrant, error) {
	l, err := parseLimits(r.Limits)
	if err != nil {
		return task.SlotGrant{}, err
	}
	g, err := x.a.Acquire(ctx, admission.Request{TaskID: r.TaskID, MemoryBytes: x.cfg.memory(l)})
	if err != nil {
		return task.SlotGrant{}, err
	}
	return task.SlotGrant{ID: g.ID, TaskID: g.TaskID, MemoryBytes: g.MemoryBytes}, nil
}

func (x admissionAdapter) Release(g task.SlotGrant) {
	x.a.Release(admission.Grant{ID: g.ID, TaskID: g.TaskID, MemoryBytes: g.MemoryBytes})
}

func slotGrant(g admission.Grant) task.SlotGrant {
	return task.SlotGrant{ID: g.ID, TaskID: g.TaskID, MemoryBytes: g.MemoryBytes}
}

// ---- resource.Coordinator → task.EnvController ----

// envCoordinator 是 resource.Coordinator 中 actor 使用的部分。
type envCoordinator interface {
	CreateEnv(ctx context.Context, r resource.EnvRequest) (provider.EnvInfo, error)
	StopEnv(ctx context.Context, envID string) (resource.StopResult, error)
}

type envAdapter struct {
	c       envCoordinator
	cfg     Config
	dataDir string
}

var _ task.EnvController = envAdapter{}

// CreateEnv 建立任务 workspace 并经 coordinator 创建环境。失败时返回非 nil 的已分类结果（actor 的约定：
// nil 会使 Decide 返回 ErrInvalid，成为致命错误）。
func (x envAdapter) CreateEnv(ctx context.Context, s task.EnvSpec) (*task.Outcome, error) {
	l, err := parseLimits(s.Limits)
	if err != nil {
		return classifyCreate(ctx, err), err
	}
	ws := workspaceDir(x.dataDir, s.TaskID)
	if err := os.MkdirAll(ws, 0o700); err != nil {
		err = fmt.Errorf("app: 建立任务 workspace: %w", err)
		return classifyCreate(ctx, err), err
	}
	_, err = x.c.CreateEnv(ctx, resource.EnvRequest{EnvID: s.EnvID, AttemptID: s.AttemptID, Kind: provider.KindTask,
		Template: x.cfg.Template, Limits: x.cfg.envLimits(l), Mounts: provider.Mounts{Workspace: ws}})
	if err != nil {
		return classifyCreate(ctx, err), err
	}
	return nil, nil
}

// StopEnv 原样转换 coordinator 报告的事实；只有 Recorded 时 actor 才归还容量（规格 §14.5）。
func (x envAdapter) StopEnv(ctx context.Context, envID string) (task.StopReport, error) {
	r, err := x.c.StopEnv(ctx, envID)
	return task.StopReport{Stopped: r.Stopped, Recorded: r.Recorded, Blocked: r.Blocked, At: r.At}, err
}

// classifyCreate 给出环境创建失败的分类（§14.3）：调用方取消按取消原因（runner.Classify 的启动失败
// 规则）；Store 暂时故障、提交结果未知、争用与本安装的残留（重建一次后仍不完整）是 create_failed_transient
// （有界重试）；其余（spec 冲突、外来资源、配置或 rootfs 问题）是 create_failed_env，不重试。
func classifyCreate(ctx context.Context, err error) *task.Outcome {
	var class string
	switch {
	case ctx.Err() != nil:
		class, _ = runner.Classify(runner.ClassifyInput{StartErr: err, PlatformKill: ctxKillReason(ctx)})
	case errors.Is(err, persistence.ErrUnavailable), errors.Is(err, persistence.ErrCommitUnknown),
		errors.Is(err, persistence.ErrContention), errors.Is(err, provider.ErrIncomplete):
		class = runner.ClassCreateFailedTransient
	default:
		class = runner.ClassCreateFailedEnv
	}
	return &task.Outcome{Class: class, Retry: task.RetryKind(runner.RetryOf(class))}
}

// ctxKillReason 与 runner 对 Run 的 ctx 的解释相同：累计运行时限（cause 为 DeadlineExceeded 或
// ErrRunTimeExceeded）为 timeout，其余取消为服务停止。
func ctxKillReason(ctx context.Context) string {
	if c := context.Cause(ctx); errors.Is(c, runner.ErrRunTimeExceeded) || errors.Is(c, context.DeadlineExceeded) {
		return runner.KillTimeout
	}
	return runner.KillShutdown
}

// workspaceDir 是任务的宿主侧 workspace（挂到环境的 /workspace）。M1 每个任务一个，attempt 的产物目录
// 为其下的 out/<attempt_id>（规格 §5.6）。
func workspaceDir(dataDir, taskID string) string {
	return filepath.Join(dataDir, "workspaces", taskID)
}

// ---- runner.Runner → task.AttemptRunner ----

// attemptRunner 是 runner.Runner 中 actor 使用的部分。
type attemptRunner interface {
	Run(ctx context.Context, a runner.Attempt, controls <-chan runner.Control) runner.Outcome
}

type runnerAdapter struct {
	r       attemptRunner
	cfg     Config
	dataDir string
}

var _ task.AttemptRunner = runnerAdapter{}

// Run 把 RunSpec 转换为 runner.Attempt（init 含 resume、宿主侧 out_dir、Worker 命令），把 actor 的控制
// 转发给 runner，并把 runner.Outcome 转换为 task.Outcome。StartErr 非 nil 时不启动 Worker，按启动失败分类。
func (x runnerAdapter) Run(ctx context.Context, s task.RunSpec, controls <-chan task.Control) task.Outcome {
	if s.StartErr != nil {
		return startFailure(ctx, s.StartErr)
	}
	ws := workspaceDir(x.dataDir, s.Task.TaskID)
	if err := os.MkdirAll(ws, 0o700); err != nil {
		return startFailure(ctx, fmt.Errorf("app: 建立任务 workspace: %w", err))
	}
	in := protocol.Init{Type: protocol.TypeInit, Bootstrap: protocol.BootstrapVersion,
		ProtocolVersions: []int64{protocol.Version}, Mode: protocol.ModeTask,
		TaskID: s.Task.TaskID, AttemptID: s.AttemptID, AttemptNo: s.AttemptNo,
		Config: s.Task.Spec, ConfigVersion: s.Task.ConfigVersion, OutDir: "/workspace/out/" + s.AttemptID}
	if cp := s.Task.Latest; cp != nil {
		in.Resume = &protocol.Resume{CheckpointID: cp.CheckpointID, StepID: cp.StepID, State: cp.State,
			StateRef: cp.StateRef, Refs: cp.Refs}
	}
	rc := make(chan runner.Control, 4)
	done := make(chan struct{})
	defer close(done)
	go forwardControls(controls, rc, done)
	out := x.r.Run(ctx, runner.Attempt{TaskID: s.Task.TaskID, AttemptID: s.AttemptID, AttemptNo: s.AttemptNo,
		EnvID: s.EnvID, Init: in, OutDir: filepath.Join(ws, "out", s.AttemptID),
		Exec:    provider.ExecSpec{ExecID: s.AttemptID, Argv: x.cfg.WorkerArgv, Env: x.cfg.WorkerEnv, Dir: "/workspace"},
		OnReady: s.OnReady}, rc)
	return outcome(out)
}

// forwardControls 把 actor 的控制转发给 runner，直到 Run 返回（done 关闭）。
func forwardControls(in <-chan task.Control, out chan<- runner.Control, done <-chan struct{}) {
	for {
		select {
		case c, ok := <-in:
			if !ok {
				return
			}
			select {
			case out <- runner.Control{Kind: c.Kind, GraceMs: c.GraceMs}:
			case <-done:
				return
			}
		case <-done:
			return
		}
	}
}

// startFailure：Worker 没有启动（读取任务或 Gateway 绑定失败、workspace 无法建立），经 runner.Classify
// 的启动失败规则分类。
func startFailure(ctx context.Context, err error) task.Outcome {
	in := runner.ClassifyInput{StartErr: err}
	if ctx.Err() != nil {
		in.PlatformKill = ctxKillReason(ctx)
	}
	class, retry := runner.Classify(in)
	return task.Outcome{Class: class, Retry: task.RetryKind(retry)}
}

// outcome 把 runner.Outcome 转换为 task.Outcome。退出状态只在已观察到（ExitErr 为空）时填写；result
// 内容只随 result 提议交给 Decide（有效 result 的判决内容）。
func outcome(o runner.Outcome) task.Outcome {
	out := task.Outcome{Class: o.Class, Retry: task.RetryKind(o.Retry), OOMKillDelta: int64(o.Diag.OOMKillDelta),
		PlatformKilled: o.PlatformKilled, OutputIncomplete: o.OutputIncomplete}
	if p := o.Proposal; p != nil {
		out.ProposalKind = p.Kind
		if p.Kind == "result" {
			out.Result = o.ResultPayload
		}
	}
	if o.ExitErr == nil {
		if o.Exit.Signal != 0 {
			sig := int64(o.Exit.Signal)
			out.ExitSignal = &sig
		} else {
			code := int64(o.Exit.Code)
			out.ExitCode = &code
		}
	}
	return out
}
