package app

// 本文件是会话（M4 Plan 12；规格 §12）的装配：session actor 的窄接口（session.Env、Workers、Gateway、Admission）
// 适配到 resource coordinator、runner、gateway/edge 与 admission；task actor 的会话授予（task.SessionGate）适配到
// session.Scheduler；会话 turn 的 Gateway 访问（task.Access）与执行（task.AttemptRunner）按 attempt 所属的
// incarnation 适配到 edge.Attach/Detach 与 runner.Incarnation.RunTask。适配器不做决策。

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/admission"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/edge"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/resource"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/session"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// 会话参数的默认值与范围（设计 D3、D4；规格 §19）。
const (
	DefaultTurnToolBudget    = 30
	MaxTurnToolBudget        = 1000
	DefaultSessionIdleFreeze = 10 * time.Minute
	DefaultSessionEvictAfter = time.Hour
)

// ReleaseReasonReleased 是 turn 释放（task_released 之后）时交给 edge.Detach 的离开原因：不是取消，在途 try
// 按期限结算（§9.1 表）。
const ReleaseReasonReleased = "released"

// sessionsEnabled：配置了会话 Worker 命令（--session-worker-argv）时启用会话；否则会话端点 503 sessions_unavailable。
func (c Config) sessionsEnabled() bool { return len(c.SessionWorkerArgv) > 0 }

// validateSessions 校验会话参数（withDefaults 之后）。
func (c Config) validateSessions() error {
	switch {
	case c.TurnToolBudget < 1 || c.TurnToolBudget > MaxTurnToolBudget:
		return fmt.Errorf("app: 每轮工具调用额度（--turn-tool-budget）须为 1–%d，得到 %d", MaxTurnToolBudget, c.TurnToolBudget)
	case c.SessionIdleFreeze <= 0:
		return fmt.Errorf("app: --session-idle-freeze 须 > 0，得到 %s", c.SessionIdleFreeze)
	case c.SessionEvictAfter <= c.SessionIdleFreeze:
		return fmt.Errorf("app: --session-evict-after（%s）须大于 --session-idle-freeze（%s）", c.SessionEvictAfter, c.SessionIdleFreeze)
	case c.sessionsEnabled() && !c.Accounts:
		return errors.New("app: 会话（--session-worker-argv）需要用户账号（--model-base-url）")
	}
	return nil
}

// turnSpec 是 api.Config.TurnSpec：模型由 server 配置固定（用户不能指定）；limits 为 server 默认值加 max_tool_calls
// （= --turn-tool-budget，CreateTurn 据此写入 budgets.tool_call_limit）。config.research 带编排与 worker 模型的输出
// 单价，供 Worker 估算报告预留（Plan 14 Task 9；单价只是配置）。
func (c Config) turnSpec(text string, deepResearch bool) (json.RawMessage, json.RawMessage, error) {
	spec, err := json.Marshal(struct {
		Kind              string `json:"kind"`
		Text              string `json:"text"`
		DeepResearch      bool   `json:"deep_research"`
		OrchestratorModel string `json:"orchestrator_model"`
		WorkerModel       string `json:"worker_model"`
		Research          struct {
			OrchestratorOutput int64 `json:"orchestrator_output_micro_per_mtok"`
			WorkerOutput       int64 `json:"worker_output_micro_per_mtok"`
		} `json:"research"`
	}{Kind: "turn", Text: text, DeepResearch: deepResearch, OrchestratorModel: c.UserOrchestratorModel,
		WorkerModel: c.UserWorkerModel, Research: struct {
			OrchestratorOutput int64 `json:"orchestrator_output_micro_per_mtok"`
			WorkerOutput       int64 `json:"worker_output_micro_per_mtok"`
		}{c.outputPrice(c.UserOrchestratorModel), c.outputPrice(c.UserWorkerModel)}})
	if err != nil {
		return nil, nil, err
	}
	limits, err := c.effectiveLimits(json.RawMessage(fmt.Sprintf(`{"max_tool_calls":%d}`, c.TurnToolBudget)))
	if err != nil {
		return nil, nil, err
	}
	return spec, limits, nil
}

// outputPrice 是模型的输出单价（每百万 token 的微美元）：按模型的单价，没有时取默认单价。
func (c Config) outputPrice(model string) int64 {
	if p, ok := c.Model.PricingByModel[model]; ok {
		return p.OutputMicroPerMTok
	}
	return c.Model.Pricing.OutputMicroPerMTok
}

// ---- 路径 ----

// sessionDir 是会话的宿主目录 <data>/sessions/<session_id>；workspace 在其下（挂到 incarnation 环境的 /workspace，
// 跨 incarnation 保留，会话关闭时删除）。
func sessionDir(dataDir, sessionID string) string {
	return filepath.Join(dataDir, "sessions", sessionID)
}

func sessionWorkspaceDir(dataDir, sessionID string) string {
	return filepath.Join(sessionDir(dataDir, sessionID), "workspace")
}

// restoreDir 是冷恢复暂存目录 <data>/restore/<env_id>（只读 bind 到 /run/agentbox/restore）。不能在环境目录内：
// provider 创建环境时要求环境目录不存在。
func restoreDir(dataDir, envID string) string { return filepath.Join(dataDir, "restore", envID) }

// makeSearchableDirs 逐级建立目录并补 o+x（沙箱 init 在 user namespace 中以映射 root 运行，须能经过它们）。
func makeSearchableDirs(dirs ...string) error {
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o711); err != nil {
			return err
		}
		fi, err := os.Stat(d)
		if err != nil {
			return err
		}
		if err := searchable(d, fi); err != nil {
			return err
		}
	}
	return nil
}

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ---- session.Env ← resource coordinator ----

// sessionCoordinator 是 resource.Coordinator 中会话使用的部分。
type sessionCoordinator interface {
	envCoordinator
	FreezeEnv(ctx context.Context, envID string) error
	ThawEnv(ctx context.Context, envID string) error
	EnvProcs(ctx context.Context, envID string) ([]int, error)
	ReleaseOwnerUIDRange(ctx context.Context, owner string) error
}

var _ sessionCoordinator = (*resource.Coordinator)(nil)

// sessionUIDStore 是记录会话 UID 范围所需的读写（environments.uid_range_id → sessions.uid_range_id）。
type sessionUIDStore interface {
	EnvUIDRangeID(ctx context.Context, envID string) (string, error)
	SetUIDRange(ctx context.Context, sessionID, uidRangeID string) error
}

type sessionEnv struct {
	c       sessionCoordinator
	store   sessionUIDStore
	blobs   blob.Store
	cfg     Config
	dataDir string
	log     *slog.Logger
}

var _ session.Env = sessionEnv{}

// CreateIncarnationEnv 建立会话 workspace 并经 coordinator 创建 kind = session 的环境（UID 范围按 owner
// "session:<id>" 分配并跨 incarnation 保留），然后把该范围写入 sessions.uid_range_id（P12-T7：actor 不调用 SetUIDRange）。
// 失败写日志（session actor 只把原因记入 sessions.last_error）。
func (x sessionEnv) CreateIncarnationEnv(ctx context.Context, s session.IncarnationEnv) (err error) {
	defer func() {
		if err != nil && x.log != nil {
			x.log.Warn("创建会话 incarnation 环境失败", "session_id", s.SessionID, "env_id", s.EnvID, "error", err.Error())
		}
	}()
	ws := sessionWorkspaceDir(x.dataDir, s.SessionID)
	if err := makeSearchableDirs(x.dataDir, filepath.Join(x.dataDir, "sessions"), sessionDir(x.dataDir, s.SessionID)); err != nil {
		return fmt.Errorf("app: 建立会话目录: %w", err)
	}
	if err := os.MkdirAll(ws, 0o700); err != nil {
		return fmt.Errorf("app: 建立会话 workspace: %w", err)
	}
	limits := x.cfg.envLimits(taskLimits{MemoryMax: s.MemoryBytes})
	if _, err := x.c.CreateEnv(ctx, resource.EnvRequest{EnvID: s.EnvID, Kind: provider.KindSession, Template: x.cfg.Template,
		Limits: limits, UIDOwner: resource.OwnerPrefixSession + s.SessionID,
		Mounts: provider.Mounts{Workspace: ws, GatewaySocket: s.GatewaySocket, RestoreDir: s.RestoreDir}}); err != nil {
		return err
	}
	rangeID, err := x.store.EnvUIDRangeID(ctx, s.EnvID)
	if err != nil {
		return fmt.Errorf("app: 读取会话环境 %s 的 UID 范围: %w", s.EnvID, err)
	}
	if err := x.store.SetUIDRange(ctx, s.SessionID, rangeID); err != nil {
		return fmt.Errorf("app: 记录会话 %s 的 UID 范围: %w", s.SessionID, err)
	}
	return nil
}

func (x sessionEnv) FreezeEnv(ctx context.Context, envID string) error {
	return x.c.FreezeEnv(ctx, envID)
}
func (x sessionEnv) ThawEnv(ctx context.Context, envID string) error { return x.c.ThawEnv(ctx, envID) }

// StopEnv 原样转换 coordinator 的事实。从未创建的环境（CreateIncarnation 已登记）同样以 provider.Stop 的空操作
// 停止并记录 stopped_at（Recorded）。
func (x sessionEnv) StopEnv(ctx context.Context, envID string) (session.StopReport, error) {
	r, err := x.c.StopEnv(ctx, envID)
	return session.StopReport{Stopped: r.Stopped, Recorded: r.Recorded, Blocked: r.Blocked}, err
}

// StageRestore 把 state_ref 的内容复制到 <data>/restore/<env_id>/<sha>（目录 root 0755、文件 0444，上级 o+x），边复制边
// 复算哈希：内容须与 sha 一致（state_ref 在会话 checkpoint 提交时已校验授权到会话 scope）。返回挂载的宿主目录。
func (x sessionEnv) StageRestore(_ context.Context, _, envID, sha string) (string, error) {
	if !sha256Hex.MatchString(sha) {
		return "", fmt.Errorf("app: 暂存的 state_ref %q 不是 sha256", sha)
	}
	dir := restoreDir(x.dataDir, envID)
	if err := makeSearchableDirs(x.dataDir, filepath.Join(x.dataDir, "restore")); err != nil {
		return "", fmt.Errorf("app: 建立恢复暂存目录: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("app: 建立恢复暂存目录: %w", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return "", err
	}
	rc, err := x.blobs.Open(sha)
	if err != nil {
		return "", fmt.Errorf("app: 读取 session state %s: %w", sha, err)
	}
	defer func() { _ = rc.Close() }()
	tmp, err := os.CreateTemp(dir, ".stage-*")
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, cerr := io.Copy(io.MultiWriter(tmp, h), rc)
	if err := errors.Join(cerr, tmp.Close()); err != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("app: 暂存 session state %s: %w", sha, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sha {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("app: session state %s 的内容哈希为 %s", sha, got)
	}
	if err := os.Chmod(tmp.Name(), 0o444); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, sha)); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	return dir, nil
}

// UnstageRestore 删除暂存目录（幂等）：bind 的是目录，删除其中的文件后沙箱内随即不可见；环境停止之后再次调用时
// 目录本身也被删除。
func (x sessionEnv) UnstageRestore(_ context.Context, envID string) error {
	return os.RemoveAll(restoreDir(x.dataDir, envID))
}

// DeleteWorkspace 删除会话目录（关闭的最后阶段：incarnation 已结束且其环境已记录停止，E32）。
func (x sessionEnv) DeleteWorkspace(_ context.Context, sessionID string) error {
	return os.RemoveAll(sessionDir(x.dataDir, sessionID))
}

// ReleaseUID 归还会话的 UID 范围：使用过它的环境须全部清理完成（否则冲突，actor 退避重试）。
func (x sessionEnv) ReleaseUID(ctx context.Context, sessionID string) error {
	return x.c.ReleaseOwnerUIDRange(ctx, resource.OwnerPrefixSession+sessionID)
}

// ---- Gateway：incarnation 入口与会话 turn 的访问 ----

// incEdge 是 edge.Edge 中会话使用的部分。
type incEdge interface {
	Bind(ctx context.Context, attemptID, envID string) (string, error)
	Revoke(ctx context.Context, attemptID, reason string) error
	BindIncarnation(ctx context.Context, incarnationID, envID string) (string, error)
	Attach(ctx context.Context, incarnationID, attemptID string) error
	Detach(ctx context.Context, incarnationID, attemptID, reason string) error
	RevokeIncarnation(ctx context.Context, incarnationID string) error
}

var _ incEdge = (*edge.Edge)(nil)

// accessRouter 是 task.Access 与 session.Gateway 的共同实现：独立任务的 attempt 经 edge.Bind/Revoke；会话 turn 的
// attempt 由 runner 适配在 RunTask 之前 Attach 到其 incarnation 的入口，Revoke 改为 Detach（按 attempt 所属
// incarnation）。Detach 的等待以 T_release 为界（P12-T5b：handler 依赖 Invoke 尊重 ctx）；RevokeIncarnation 之前先
// Detach 仍附着的 attempt（RevokeIncarnation 不转交离开原因）。
type accessRouter struct {
	e       incEdge
	release time.Duration // T_release

	mu       sync.Mutex
	attached map[string]string // attempt → incarnation
}

var (
	_ task.Access     = (*accessRouter)(nil)
	_ session.Gateway = (*accessRouter)(nil)
)

func newAccessRouter(e incEdge, release time.Duration) *accessRouter {
	if release <= 0 {
		release = 30 * time.Second
	}
	return &accessRouter{e: e, release: release, attached: map[string]string{}}
}

func (x *accessRouter) Bind(ctx context.Context, attemptID, envID string) (string, error) {
	return x.e.Bind(ctx, attemptID, envID)
}

func (x *accessRouter) Revoke(ctx context.Context, attemptID, reason string) error {
	x.mu.Lock()
	inc, ok := x.attached[attemptID]
	x.mu.Unlock()
	if !ok {
		return x.e.Revoke(ctx, attemptID, reason)
	}
	return x.detach(ctx, inc, attemptID, reason)
}

// attach 把会话 turn 的 attempt 附着到 incarnation 的入口。
func (x *accessRouter) attach(ctx context.Context, incID, attemptID string) error {
	if err := x.e.Attach(ctx, incID, attemptID); err != nil {
		return err
	}
	x.mu.Lock()
	x.attached[attemptID] = incID
	x.mu.Unlock()
	return nil
}

// detach 以 T_release 为界等待该 attempt 的连接与在途请求结束（幂等：未附着时只转交原因）。
func (x *accessRouter) detach(ctx context.Context, incID, attemptID, reason string) error {
	dctx, cancel := context.WithTimeout(ctx, x.release)
	defer cancel()
	err := x.e.Detach(dctx, incID, attemptID, reason)
	x.mu.Lock()
	if x.attached[attemptID] == incID {
		delete(x.attached, attemptID)
	}
	x.mu.Unlock()
	return err
}

func (x *accessRouter) BindIncarnation(ctx context.Context, incarnationID, envID string) (string, error) {
	return x.e.BindIncarnation(ctx, incarnationID, envID)
}

// RevokeIncarnation 先 Detach 仍附着在该 incarnation 上的 attempt（销毁路径：故障或取消），再关闭入口。
func (x *accessRouter) RevokeIncarnation(ctx context.Context, incarnationID string) error {
	x.mu.Lock()
	var pending []string
	for att, inc := range x.attached {
		if inc == incarnationID {
			pending = append(pending, att)
		}
	}
	x.mu.Unlock()
	var errs []error
	for _, att := range pending {
		if err := x.detach(ctx, incarnationID, att, ReleaseReasonReleased); err != nil {
			errs = append(errs, err)
		}
	}
	if err := x.e.RevokeIncarnation(ctx, incarnationID); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ---- session.Workers ← runner ----

// incHandle 是 session.IncarnationHandle 的实现（runner.Incarnation 的适配），并为会话 turn 的执行保留 incarnation
// 与其 ID。
type incHandle struct {
	inc   *runner.Incarnation
	incID string
}

var _ session.IncarnationHandle = (*incHandle)(nil)

func (h *incHandle) Release(ctx context.Context, attemptID, verdict, committed string) (bool, string) {
	r := h.inc.Release(ctx, runner.TaskOutcome{AttemptID: attemptID, Verdict: verdict, CommittedSessionCheckpointID: committed})
	return r.Released, r.Reason
}

func (h *incHandle) Quiesce(ctx context.Context, grace time.Duration) (string, error) {
	return h.inc.Quiesce(ctx, grace)
}

func (h *incHandle) Close(ctx context.Context, grace time.Duration) error {
	return h.inc.Close(ctx, grace)
}

func (h *incHandle) Exited() <-chan struct{} { return h.inc.Exited() }

// incarnationStarter 是 runner.Runner 中会话使用的部分。
type incarnationStarter interface {
	StartIncarnation(ctx context.Context, s runner.IncarnationSpec) (*runner.Incarnation, error)
}

type sessionWorkers struct {
	r       incarnationStarter
	coord   sessionCoordinator
	access  *accessRouter
	cfg     Config
	dataDir string
	log     *slog.Logger
}

var _ session.Workers = sessionWorkers{}

// Start 启动会话 Worker（--session-worker-argv）：释放核验的进程表取 coordinator 的 EnvProcs，Gateway 空闲取
// edge.Detach 完成的等待（以 T_release 为界）。
func (x sessionWorkers) Start(ctx context.Context, w session.WorkerStart) (session.IncarnationHandle, error) {
	incID, envID := w.IncarnationID, w.EnvID
	spec := runner.IncarnationSpec{SessionID: w.SessionID, IncarnationID: incID, EnvID: envID,
		Argv: x.cfg.SessionWorkerArgv, Env: x.cfg.WorkerEnv, WorkspaceRoot: sessionWorkspaceDir(x.dataDir, w.SessionID),
		Procs: func(ctx context.Context) ([]int, error) { return x.coord.EnvProcs(ctx, envID) },
		GatewayIdle: func(ctx context.Context, attemptID string) error {
			return x.access.detach(ctx, incID, attemptID, ReleaseReasonReleased)
		}, Extensions: x.cfg.workerExtensions()}
	if r := w.Resume; r != nil {
		spec.Resume = &protocol.SessionResume{CheckpointID: r.CheckpointID, State: r.State, StagedStatePath: r.StagedPath, Refs: r.Refs}
	}
	inc, err := x.r.StartIncarnation(ctx, spec)
	if err != nil {
		if x.log != nil {
			x.log.Warn("启动会话 Worker 失败", "session_id", w.SessionID, "incarnation_id", incID, "error", err.Error())
		}
		return nil, err
	}
	return &incHandle{inc: inc, incID: incID}, nil
}

// ---- session.Admission ← admission（MemoryOnly） ----

type sessionAdmission struct{ a *admission.Admission }

var _ session.Admission = sessionAdmission{}

func (x sessionAdmission) AcquireMemory(ctx context.Context, sessionID string, bytes int64) (session.Grant, error) {
	g, err := x.a.Acquire(ctx, admission.Request{TaskID: resource.OwnerPrefixSession + sessionID, MemoryBytes: bytes, MemoryOnly: true})
	if err != nil {
		return session.Grant{}, err
	}
	return session.Grant{ID: g.ID}, nil
}

func (x sessionAdmission) Release(g session.Grant) {
	x.a.Release(admission.Grant{ID: g.ID, MemoryOnly: true})
}

// ---- 会话运行时：task actor 的会话授予与会话 turn 的执行 ----

// turnFactsStore 是构造会话 turn 的 task_start 额外读取的事实（*postgres.Store 实现）。
type turnFactsStore interface {
	PendingQuestionID(ctx context.Context, taskID string) (string, error)
	TurnCarryover(ctx context.Context, taskID string) (srcTaskID, stateRef string, state json.RawMessage, err error)
	AuthorizeCarryover(ctx context.Context, taskID, srcTaskID, sha string, size int64) error
}

// sessionRuntime 持有 session Scheduler（启动执行时建立）与会话 turn 执行需要的依赖。
type sessionRuntime struct {
	sched  atomic.Pointer[session.Scheduler]
	access *accessRouter
	facts  turnFactsStore
	blobs  blob.Store
	log    *slog.Logger
	// subruns 是 sub-run 扩展的宿主实现（subruns.go）；会话 init 请求了 subruns 时每个 turn 的 attempt 都须提供。
	subruns runner.SubrunHost
}

// errSessionsOff：会话未启用或 session Scheduler 尚未启动（暂时性：turn 保持 queued 并退避重试，不失败）。
var errSessionsOff = errors.New("app: 会话未启用或 session Scheduler 尚未启动")

// sessionGate 是 task.SessionGate ← session.Scheduler：session.ErrUnavailable（会话已关闭或恢复两次失败）映射为
// task.ErrSessionUnavailable（turn 失败）；session.ErrStopped（Scheduler 停止或 actor 致命）保持为可重试的错误。
type sessionGate struct{ rt *sessionRuntime }

var _ task.SessionGate = sessionGate{}

func (x sessionGate) Grant(ctx context.Context, sessionID, taskID string) (task.SessionGrant, error) {
	sch := x.rt.sched.Load()
	if sch == nil {
		return task.SessionGrant{}, errSessionsOff
	}
	g, err := sch.Grant(ctx, sessionID, taskID)
	if errors.Is(err, session.ErrUnavailable) {
		return task.SessionGrant{}, fmt.Errorf("%w: %w", task.ErrSessionUnavailable, err)
	}
	if err != nil {
		return task.SessionGrant{}, err
	}
	return task.SessionGrant{IncarnationID: g.IncarnationID, EnvID: g.EnvID}, nil
}

func (x sessionGate) Handoff(ctx context.Context, sessionID string, h task.SessionHandoff) (task.StopReport, error) {
	sch := x.rt.sched.Load()
	if sch == nil {
		return task.StopReport{}, errSessionsOff
	}
	r, err := sch.Handoff(ctx, sessionID, session.Handoff{TaskID: h.TaskID, AttemptID: h.AttemptID, EnvID: h.EnvID,
		Verdict: h.Verdict, CommittedSessionCheckpointID: h.CommittedSessionCheckpointID, Destroy: h.Destroy})
	return task.StopReport{Stopped: r.Stopped, Recorded: r.Recorded, Blocked: r.Blocked}, err
}

// runTurn 在授予的 incarnation 中执行会话 turn 的 attempt：Attach 入口 → RunTask（task_start 带续跑指令、恢复
// 来源与 carryover）。incarnation 已不存在按控制连接丢失（control_lost，故障重试）分类。
func (rt *sessionRuntime) runTurn(ctx context.Context, s task.RunSpec, controls <-chan task.Control) task.Outcome {
	var h *incHandle
	if sch := rt.sched.Load(); sch != nil {
		if hh, ok := sch.Incarnation(s.EnvID); ok {
			h, _ = hh.(*incHandle)
		}
	}
	if h == nil {
		return startFailure(ctx, fmt.Errorf("%w: 环境 %s 的 incarnation 已不存在", provider.ErrControlLost, s.EnvID))
	}
	l, err := parseLimits(s.Task.Limits)
	if err != nil {
		return startFailure(ctx, err)
	}
	directive, err := rt.directive(ctx, s.Task)
	if err != nil {
		return startFailure(ctx, err)
	}
	if err := rt.access.attach(ctx, h.incID, s.AttemptID); err != nil {
		return startFailure(ctx, fmt.Errorf("app: 附着 attempt %s 到 incarnation %s 的入口: %w", s.AttemptID, h.incID, err))
	}
	in := protocol.Init{Config: s.Task.Spec, ConfigVersion: s.Task.ConfigVersion, BudgetLimits: budgetLimits(l), Resume: taskResume(s.Task)}
	rc := make(chan runner.Control, 4)
	done := make(chan struct{})
	defer close(done)
	go forwardControls(controls, rc, done)
	out := h.inc.RunTask(ctx, runner.SessionAttempt{
		Attempt: runner.Attempt{TaskID: s.Task.TaskID, AttemptID: s.AttemptID, AttemptNo: s.AttemptNo, EnvID: s.EnvID,
			Init: in, OnReady: s.OnReady, Subruns: rt.subruns},
		BaseSessionCheckpointID: s.Task.BaseSessionCheckpointID, Directive: directive,
		RestoredFromTaskID: s.Task.RestoredFromTaskID, Carryover: rt.carryover(ctx, s.Task),
	}, rc)
	return outcome(out)
}

// directive 把存储的续跑指令（{kind, answers?}）转换为 task_start.directive：continue 不发送（契约 B）；answer 补上
// 待答提问的 question_id（awaiting_input 提议中的，契约 A）。
func (rt *sessionRuntime) directive(ctx context.Context, ts task.TaskState) (json.RawMessage, error) {
	if len(ts.Directive) == 0 || string(ts.Directive) == "null" {
		return nil, nil
	}
	var d struct {
		Kind    string          `json:"kind"`
		Answers json.RawMessage `json:"answers,omitempty"`
	}
	if err := json.Unmarshal(ts.Directive, &d); err != nil {
		return nil, fmt.Errorf("app: 续跑指令不合法: %w", err)
	}
	switch d.Kind {
	case "continue":
		return nil, nil
	case protocol.DirectiveAnswer:
		qid, err := rt.facts.PendingQuestionID(ctx, ts.TaskID)
		if err != nil {
			return nil, fmt.Errorf("app: 读取待答提问: %w", err)
		}
		return json.Marshal(protocol.Directive{Kind: d.Kind, QuestionID: qid, Answers: d.Answers})
	}
	return json.Marshal(protocol.Directive{Kind: d.Kind})
}

// carryover 给出被取代 turn 最新 checkpoint 的 blob 引用（契约 D）：state_ref 直接使用（已授权到会话 scope）；
// inline 状态写入 BlobStore 并授权到本 turn 与会话 scope。任何失败只记日志：新一轮不带上下文继续。
func (rt *sessionRuntime) carryover(ctx context.Context, ts task.TaskState) *protocol.Carryover {
	src, ref, state, err := rt.facts.TurnCarryover(ctx, ts.TaskID)
	if err != nil || src == "" {
		if err != nil {
			rt.log.Warn("读取 carryover 失败，本轮不带上一轮的上下文", "task_id", ts.TaskID, "error", err.Error())
		}
		return nil
	}
	if ref == "" {
		b, err := rt.blobs.Put(ctx, bytes.NewReader(state))
		if err == nil {
			err = rt.facts.AuthorizeCarryover(ctx, ts.TaskID, src, b.SHA256, b.Size)
		}
		if err != nil {
			rt.log.Warn("登记 carryover 失败，本轮不带上一轮的上下文", "task_id", ts.TaskID, "error", err.Error())
			return nil
		}
		ref = b.SHA256
	}
	return &protocol.Carryover{TaskID: src, CheckpointRef: ref}
}

// ---- API：会话写操作提交后通知 task 与 session Scheduler ----

// notifyingSessions 包装 api.Sessions（§8.1"提交后才确认接受，再尽力通知 actor"）。
type notifyingSessions struct {
	api.Sessions
	s *server
}

func (n notifyingSessions) notifySession(sessionID string) {
	if sc := n.s.sessions.sched.Load(); sc != nil && sessionID != "" && n.s.workCtx.Err() == nil {
		sc.Notify(sessionID)
	}
}

func (n notifyingSessions) submit(taskID string) {
	if sc := n.s.sched.Load(); sc != nil && taskID != "" && n.s.workCtx.Err() == nil {
		sc.Submit(taskID)
	}
}

func (n notifyingSessions) CreateTurn(ctx context.Context, req api.CreateTurnRequest) (api.CreateTurnResult, error) {
	r, err := n.Sessions.CreateTurn(ctx, req)
	if err == nil {
		if !r.Replayed {
			obs.NoteSubmit(ctx, r.TurnID)
		}
		n.submit(r.SupersededTurnID)
		n.submit(r.TurnID)
		n.notifySession(req.SessionID)
	}
	return r, err
}

func (n notifyingSessions) TurnControl(ctx context.Context, req api.TurnControlRequest) (api.ControlResult, error) {
	r, err := n.Sessions.TurnControl(ctx, req)
	if err == nil {
		switch {
		case r.Replayed:
		case req.Action == "stop":
			obs.NoteStop(r.TaskID) // 停止延迟自接受停止请求起计
		default:
			obs.NoteSubmit(ctx, r.TaskID) // continue/finish/answer 开始 turn 的新一次运行
		}
		n.submit(r.TaskID)
		if sid, _, err := n.Sessions.TurnSession(ctx, r.TaskID); err == nil {
			n.notifySession(sid)
		}
	}
	return r, err
}

func (n notifyingSessions) CloseSession(ctx context.Context, sessionID string) (api.SessionView, error) {
	v, err := n.Sessions.CloseSession(ctx, sessionID)
	if err == nil {
		n.notifySession(sessionID)
		// 关闭同事务对非终态 turn 写了 cancel 控制：通知它们的 actor（actor 也会周期比较控制版本）。
		after := int64(-1)
		for {
			turns, err := n.Sessions.ListTurns(ctx, sessionID, after, 100)
			if err != nil || len(turns) == 0 {
				break
			}
			for _, t := range turns {
				if !task.IsTerminal(t.Status) {
					n.submit(t.TurnID)
				}
				after = t.TurnIndex
			}
		}
	}
	return v, err
}

func (n notifyingSessions) WakeSession(ctx context.Context, requestID string, bodyHash []byte, sessionID string) error {
	err := n.Sessions.WakeSession(ctx, requestID, bodyHash, sessionID)
	if err == nil {
		n.notifySession(sessionID)
	}
	return err
}
