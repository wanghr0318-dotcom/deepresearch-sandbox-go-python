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
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/edge"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// ---- 用户研究 ----

// researchSpec 由主题生成用户研究的 spec：模型由 server 配置固定（用户不能指定），其余取 Worker 的默认值。
func (c Config) researchSpec(topic string) (json.RawMessage, error) {
	return json.Marshal(struct {
		Topic             string `json:"topic"`
		OrchestratorModel string `json:"orchestrator_model"`
		WorkerModel       string `json:"worker_model"`
	}{topic, c.UserOrchestratorModel, c.UserWorkerModel})
}

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
	BudgetMicro  int64 `json:"budget_micro"`    // task 层预算上限（微美元，§9.6）；创建时写入 budgets.limit_micro
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
		"nofile": l.NoFile, "tmp_bytes": l.TmpBytes, "max_run_time_ms": l.MaxRunTimeMs, "budget_micro": l.BudgetMicro} {
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

// effectiveLimits 是 api.Config.EffectiveLimits：由请求中的 limits 得出随任务存储的有效 limits（规格 §14.4）。
//   - 拒绝格式错误与永远无法满足容量的 limits，使 actor 申请槽位时不会遇到 admission.ErrExceedsCapacity
//     （该错误在 actor 中是致命的）；
//   - max_run_time_ms：省略时补入 DefaultRunTime；显式值须为正整数且不超过 RunTimeCap（0 也是错误）；
//   - budget_micro（task 层预算，微美元，§9.6）：省略时补入 DefaultBudgetMicro；显式值须为正整数且不超过
//     BudgetCapMicro。随任务存储，CreateTask 在同一事务中据此写入 budgets.limit_micro。
//
// 其余键原样保留。
func (c Config) effectiveLimits(raw json.RawMessage) (json.RawMessage, error) {
	l, err := parseLimits(raw)
	if err != nil {
		return nil, err
	}
	if m := c.memory(l); m > c.Capacity.MemoryBytes {
		return nil, fmt.Errorf("limits.memory_max = %d 超过服务的总内存容量 %d，永远无法被授予", m, c.Capacity.MemoryBytes)
	}
	fields := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("limits 须为 JSON 对象: %w", err)
		}
	}
	capMs := c.RunTimeCap.Milliseconds()
	if _, set := fields["max_run_time_ms"]; set {
		if l.MaxRunTimeMs <= 0 || l.MaxRunTimeMs > capMs {
			return nil, fmt.Errorf("limits.max_run_time_ms 须为 1..%d 之间的整数（服务端上限 %s）", capMs, c.RunTimeCap)
		}
	} else {
		fields["max_run_time_ms"] = json.RawMessage(fmt.Sprint(c.DefaultRunTime.Milliseconds()))
	}
	if _, set := fields["budget_micro"]; set {
		if l.BudgetMicro <= 0 || l.BudgetMicro > c.BudgetCapMicro {
			return nil, fmt.Errorf("limits.budget_micro 须为 1..%d 之间的整数（微美元，服务端上限）", c.BudgetCapMicro)
		}
	} else {
		fields["budget_micro"] = json.RawMessage(fmt.Sprint(c.DefaultBudgetMicro))
	}
	return json.Marshal(fields)
}

// budgetLimits 是 init.budget_limits：{"budget_micro": <limits.budget_micro>}（创建时已补默认值并校验）。
func budgetLimits(l taskLimits) json.RawMessage {
	b, _ := json.Marshal(map[string]int64{"budget_micro": l.BudgetMicro}) // map[string]int64 的编码不会失败
	return b
}

// runTimeLimit 是 task.Deps.RunTimeLimit：创建时存储的 limits.max_run_time_ms。缺失或无法解析时
// （只可能来自外部改动）取默认值，不放开限额。
func (c Config) runTimeLimit(ts task.TaskState) time.Duration {
	l, err := parseLimits(ts.Limits)
	if err == nil && l.MaxRunTimeMs > 0 {
		return time.Duration(l.MaxRunTimeMs) * time.Millisecond
	}
	return c.DefaultRunTime
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
	if s.BindErr != nil { // Gateway 入口未建立：不创建环境（init 无法挂载不存在的 socket）
		return classifyCreate(ctx, s.BindErr), s.BindErr
	}
	l, err := parseLimits(s.Limits)
	if err != nil {
		return classifyCreate(ctx, err), err
	}
	// 已知的 M1 缺口：任务 workspace 没有清理所有者，任务结束后不会删除。
	ws, err := makeWorkspace(x.dataDir, s.TaskID)
	if err != nil {
		return classifyCreate(ctx, err), err
	}
	_, err = x.c.CreateEnv(ctx, resource.EnvRequest{EnvID: s.EnvID, AttemptID: s.AttemptID, Kind: provider.KindTask,
		Template: x.cfg.Template, Limits: x.cfg.envLimits(l), Mounts: provider.Mounts{Workspace: ws, GatewaySocket: s.GatewaySocket}})
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

// makeWorkspace 建立任务 workspace（0700）。数据目录与 workspaces 目录须对"其他人"可搜索（o+x，不可列出）：
// 沙箱 init 在 user namespace 中以映射 root 运行，只有经过它们才能到达 workspace；workspace 本身由
// provider 在启动环境时 chown 到映射 UID（规格 §4.5）。
func makeWorkspace(dataDir, taskID string) (string, error) {
	parent := filepath.Join(dataDir, "workspaces")
	if err := os.MkdirAll(parent, 0o711); err != nil {
		return "", fmt.Errorf("app: 建立任务 workspace: %w", err)
	}
	fi, err := os.Stat(parent)
	if err != nil {
		return "", fmt.Errorf("app: 建立任务 workspace: %w", err)
	}
	if err := searchable(parent, fi); err != nil {
		return "", err
	}
	ws := workspaceDir(dataDir, taskID)
	if err := os.MkdirAll(ws, 0o700); err != nil {
		return "", fmt.Errorf("app: 建立任务 workspace: %w", err)
	}
	return ws, nil
}

// searchable 为目录补上 o+x（已有时不变）。
func searchable(dir string, fi os.FileInfo) error {
	if fi.Mode().Perm()&0o001 != 0 {
		return nil
	}
	if err := os.Chmod(dir, fi.Mode().Perm()|0o001); err != nil {
		return fmt.Errorf("app: 使 %s 可被沙箱 init 搜索（o+x）: %w", dir, err)
	}
	return nil
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
	l, err := parseLimits(s.Task.Limits)
	if err != nil {
		return startFailure(ctx, err)
	}
	ws, err := makeWorkspace(x.dataDir, s.Task.TaskID)
	if err != nil {
		return startFailure(ctx, err)
	}
	// init 不含任何凭据：供应商 Key 只在 Gateway 的 upstream adapter 中（§9.9、G3）。
	in := protocol.Init{Type: protocol.TypeInit, Bootstrap: protocol.BootstrapVersion,
		ProtocolVersions: []int64{protocol.Version}, Mode: protocol.ModeTask,
		TaskID: s.Task.TaskID, AttemptID: s.AttemptID, AttemptNo: s.AttemptNo,
		Config: s.Task.Spec, ConfigVersion: s.Task.ConfigVersion, BudgetLimits: budgetLimits(l),
		OutDir: "/workspace/out/" + s.AttemptID}
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

// ---- Gateway ----

// attemptLookup 把 task.Store.LookupAttempt 适配为 edge.Attempts（入口绑定时核对 attempt 的任务与环境）。
type attemptLookup struct{ s task.Store }

var _ edge.Attempts = attemptLookup{}

func (x attemptLookup) Lookup(ctx context.Context, attemptID string) (string, string, error) {
	return x.s.LookupAttempt(ctx, attemptID)
}

// hostEvents 把 task.Store.AppendHostEvent 适配为 call.HostEvents：指纹分歧写入 host 事件 replay_divergence
// （§9.4），payload 只含调用 ID 与指纹摘要，不含请求正文。
type hostEvents struct{ s task.Store }

var _ call.HostEvents = hostEvents{}

// EventReplayDivergence 是指纹分歧的 host 事件类型（§9.4）。
const EventReplayDivergence = "replay_divergence"

func (x hostEvents) ReplayDivergence(ctx context.Context, taskID, attemptID, callID, detail string) error {
	payload, err := json.Marshal(map[string]string{"call_id": callID, "detail": detail})
	if err != nil {
		return err
	}
	return x.s.AppendHostEvent(ctx, taskID, attemptID, EventReplayDivergence, payload)
}

// searchPricing 是各搜索供应商的价格表（adapter 声明的按次计价，§9.6）。Tavily 按每次基础搜索 1 credit
// 约 0.008 USD 保守计价；Serper 按每次 2 credit（num 较大时可能计 2 credit）、每 credit 约 0.001 USD 保守计价
// （只是配置，未与供应商账单核对）；fake 与 ddg_lite 不收费。
func searchPricing(provider string) upstream.Pricing {
	switch provider {
	case upstream.SearchTavily:
		return upstream.Pricing{Version: "tavily/2026-10", SearchMicroPerRequest: 8_000}
	case upstream.SearchSerper:
		return upstream.Pricing{Version: "serper/2026-10", SearchMicroPerRequest: 2_000}
	}
	return upstream.Pricing{Version: provider + "/free"}
}

// gatewayAdapters 构造 Gateway 的上游 adapter、按类别的价格表与 chat 按模型的价格表（与 adapter 使用的
// 价格表相同，交给 call 由 usage 计算实际费用）。所有 adapter 共用一个验证 dialer（§9.8）。模型 adapter 只在
// 配置了 Model.BaseURL 时提供（否则 /v1/chat/completions 为 endpoint_not_configured）。供应商 Key 只交给
// adapter，由它放在 Authorization 头中，不进入日志、init 或沙箱环境（§9.9）。
func (c Config) gatewayAdapters(d *upstream.Dialer) ([]upstream.Adapter, map[upstream.Kind]upstream.Pricing, map[string]upstream.Pricing) {
	pricing := map[upstream.Kind]upstream.Pricing{
		upstream.KindSearch: searchPricing(c.SearchProvider),
		upstream.KindFetch:  {Version: "fetch/free"},
	}
	adapters := []upstream.Adapter{
		upstream.NewSearch(upstream.SearchConfig{Provider: c.SearchProvider, APIKey: c.SearchAPIKey, BaseURL: c.SearchBaseURL,
			Pricing: pricing[upstream.KindSearch], HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0)}),
		upstream.NewFetch(upstream.FetchConfig{Dialer: d, Pricing: pricing[upstream.KindFetch]}),
	}
	var chatPricing map[string]upstream.Pricing
	if c.Model.BaseURL != "" {
		pricing[upstream.KindChat] = c.Model.Pricing
		chatPricing = c.Model.PricingByModel
		adapters = append(adapters, upstream.NewChat(upstream.ChatConfig{BaseURL: c.Model.BaseURL, Model: c.Model.Name,
			Models: c.Model.Models, APIKey: c.Model.APIKey, Pricing: c.Model.Pricing, PricingByModel: chatPricing, MaxTokensCap: c.Model.MaxTokensCap,
			HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0)}))
	}
	return adapters, pricing, chatPricing
}
