package runner

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/subrun"
)

// Starter 是 runner 需要的 provider 子集（消费者窄接口）。
type Starter interface {
	StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error)
}

// Attempt 是一次执行所需的全部输入。
type Attempt struct {
	TaskID, AttemptID string
	AttemptNo         int64
	EnvID             string
	Init              protocol.Init // 由 actor 依 LoadTask 构造（含 resume），runner 原样发送
	// OutDir 是宿主侧 workspace 中的 out/<attempt_id>，即 <workspace>/out/<attempt_id>；
	// workspace 根必须已存在，out 与 <attempt_id> 不存在时由 runner 创建（规格 §5.6）。
	OutDir string
	Exec   provider.ExecSpec
	// OnReady 可选：合法的 ready 处理完毕后同步调用恰好一次，调用时不等待 Store。
	// 调用方据此知道 Worker 已能接收协作式 cancel/pause。
	OnReady func()
	// Subruns 是 sub-run 扩展的宿主实现（M4 Plan 14）。Init.Extensions（session 模式为 IncarnationSpec.Extensions）
	// 请求了 subruns 时必须提供，否则 Run / RunTask 在启动前失败；未请求时不使用。
	Subruns SubrunHost
}

// Control 是 actor 发给 runner 的控制请求。runner 向 Worker 发送协议 cancel/pause（grace_ms），
// grace 到期仍未退出则终止执行（规格 §5.9）。cancel 优先于 pause；重复的同类请求忽略。
type Control struct {
	Kind    string // cancel | pause
	GraceMs int64  // ≤ 0 取 Options.ControlGrace
	Reason  string // 协议消息的 reason；空取 "user"
}

// ErrRunTimeExceeded 是调用方以 context.WithCancelCause 取消 Run 的 ctx 时使用的 cause，表示累计
// 运行时限超限（规格 §14.4）；runner 据此按 task_deadline_exceeded 分类。ctx 到达 deadline 同样如此；
// 其余取消视为服务停止（lost_on_restart）。
var ErrRunTimeExceeded = errors.New("runner: 累计运行时限超限")

// Outcome 是一次执行的事实。Class 与 Retry 由 Classify 填写。
type Outcome struct {
	Proposal *TerminalProposal
	// ResultPayload 是终态提议被保存的内容（与 Proposal.Ref 指向的 blob 字节相同）：
	// result 为 {summary, outputs[{artifact_id, version, sha256}]}，error 为 {code, message, retryable}，
	// paused 为 {checkpoint_id}。最终事务提交的必须是这份内容（规格 §5.7）。
	ResultPayload json.RawMessage
	// SessionState 是 session 模式 result 携带的新 session 状态提议（原样，规格 §12.3）；其余为 nil。
	SessionState   *protocol.SessionState
	Exit           provider.ExitStatus
	ExitErr        error
	Diag           provider.ResourceDiag
	Class, Retry   string
	PlatformKilled bool // 平台在进程退出前主动终止了执行
	// PlatformKill 是平台主动终止的原因（Kill* 常量）；进程已退出后才出现的原因（例如屏障 B 期间
	// Store 不可用）同样记录，但不置 PlatformKilled。
	PlatformKill string
	Control      string // 宿主已请求的控制：""、cancel 或 pause
	// OutputIncomplete：屏障 A（管道收尾）或屏障 B（处理已接收事件）超时（规格 §5.7）。
	OutputIncomplete bool
	// Violation 非空表示协议违规：protocol 包的错误码（如 seq_invalid、after_terminal、
	// malformed_json、path_invalid），超出大小上限时为 output_limit_exceeded，Worker 以
	// handshake_error 结束握手时为 handshake_error，ready 的 mode 不是 task 时为 mode_mismatch。
	Violation string
	// StderrTail 是 stderr 的最后 Options.StderrTail 字节（诊断用）。
	StderrTail []byte
}

// Violation 中 runner 自己产生的取值。
const (
	ViolationOutputLimit    = "output_limit_exceeded"
	ViolationHandshakeError = "handshake_error"
	ViolationModeMismatch   = "mode_mismatch"
	// ViolationPausedNotLatest：paused.checkpoint_id 不是最新已提交的 checkpoint（规格 §5.9），
	// 或无法确认它是。
	ViolationPausedNotLatest = "paused_checkpoint_not_latest"
)

// 回复中使用的稳定错误码（规格 §5.4）。产物因存储暂时故障未能保存时使用 save_timeout（Worker 可
// 重试的那个码），因 attempt 正在结束而放弃保存时使用 cancelled。
const (
	codeMissingRef       = "missing_ref"
	codeStateTooLarge    = "state_too_large"
	codeCommitInFlight   = "commit_in_flight"
	codeArtifactTooLarge = "artifact_too_large"
	codeNotRegularFile   = "not_regular_file"
	codePathInvalid      = "path_invalid"
	codeHashMismatch     = "hash_mismatch"
	codeSaveTimeout      = "save_timeout"
	codeCancelled        = "cancelled"
)

// Options 配置 Runner。零值字段取默认值（规格 §5.7、§5.10、§14.5、§19）。
type Options struct {
	ReadyTimeout       time.Duration // T_ready，默认 30 s
	ExitGrace          time.Duration // 终态提议后须退出的期限，默认 10 s
	DrainTimeout       time.Duration // 屏障 A：进程退出后管道收尾，默认 5 s
	FinalizeTimeout    time.Duration // 屏障 B：处理全部已接收事件，默认 120 s
	ControlGrace       time.Duration // Control.GraceMs ≤ 0 时 cancel/pause 的 grace，默认 10 s
	StoreFailureCount  int           // Store 故障阈值：连续失败次数，默认 5
	StoreFailureWindow time.Duration // Store 故障阈值：连续失败持续时间，默认 30 s

	EventQueue       int           // stdout 行的有界队列容量，默认 256
	EventBatch       int           // AppendWorkerEvents 每批最多事件数，默认 64
	MaxAttemptEvents int64         // 每 attempt 事件合计字节，默认 64 MiB
	MaxArtifactBytes int64         // 单个产物，默认 256 MiB
	MaxStateRefBytes int64         // state_ref 指向的 blob，默认 16 MiB
	SaveTimeout      time.Duration // 单个产物的保存期限，默认 60 s
	WriteTimeout     time.Duration // stdin 每条消息的写期限（句柄支持 SetWriteDeadline 时），默认 5 s
	StderrTail       int           // 保留的 stderr 字节数，默认 64 KiB

	TaskAcceptTimeout time.Duration // session 模式 T_task_accept：task_start 之后等 task_accepted，默认 10 s
	ReleaseTimeout    time.Duration // session 模式 T_release：task_outcome 之后到释放核验完成，默认 30 s

	SubrunCancelTimeout time.Duration // T_subrun_cancel：宿主取消 sub-run 后等其 subrun_end，默认 10 s（§13.4）
}

func (o Options) withDefaults() Options {
	def := func(v *int64, d int64) {
		if *v <= 0 {
			*v = d
		}
	}
	dur := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	dur(&o.ReadyTimeout, 30*time.Second)
	dur(&o.ExitGrace, 10*time.Second)
	dur(&o.DrainTimeout, 5*time.Second)
	dur(&o.FinalizeTimeout, 120*time.Second)
	dur(&o.ControlGrace, 10*time.Second)
	dur(&o.StoreFailureWindow, 30*time.Second)
	dur(&o.TaskAcceptTimeout, 10*time.Second)
	dur(&o.ReleaseTimeout, 30*time.Second)
	dur(&o.SubrunCancelTimeout, subrun.DefaultCancelTimeout)
	if o.StoreFailureCount <= 0 {
		o.StoreFailureCount = 5
	}
	if o.EventQueue <= 0 {
		o.EventQueue = 256
	}
	if o.EventBatch <= 0 {
		o.EventBatch = 64
	}
	def(&o.MaxAttemptEvents, 64<<20)
	def(&o.MaxArtifactBytes, 256<<20)
	def(&o.MaxStateRefBytes, 16<<20)
	if o.SaveTimeout <= 0 {
		o.SaveTimeout = 60 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.StderrTail <= 0 {
		o.StderrTail = 64 << 10
	}
	return o
}

// Runner 执行 attempt：它是 Worker stdout 的唯一读取者，按 seq 处理事件、持久化、保存产物、
// 提交 checkpoint 并记录终态提议。Runner 不写生命周期字段（裁决由 task actor 提交）。
type Runner struct {
	store   Store
	blobs   blob.Store
	starter Starter
	diag    func(ctx context.Context, envID string) (provider.ResourceDiag, error)
	opt     Options
}

// New 返回 Runner。diag 可以为 nil。
func New(store Store, blobs blob.Store, starter Starter, diag func(ctx context.Context, envID string) (provider.ResourceDiag, error), opt Options) *Runner {
	return &Runner{store: store, blobs: blobs, starter: starter, diag: diag, opt: opt.withDefaults()}
}

// Run 启动 Worker、发送 init 并处理其事件，直到进程退出且 stdout 结束、全部已接收事件处理完毕。
// 启动前失败（init 不合法、out_dir 无法打开、StartExec 失败）时填写 ExitErr 与分类。返回的
// Outcome 总已由 Classify 分类。
func (r *Runner) Run(ctx context.Context, a Attempt, controls <-chan Control) (out Outcome) {
	ctx, span := obs.Start(ctx, "worker.run", runAttrs("task", a)...)
	defer func() { endRun(span, out) }()
	// init.traceparent（协议 v1 的可选字段）：沙箱内 Worker SDK 把它作为 Gateway 请求的 traceparent 头，使 Worker
	// 发起的调用加入同一 trace。tracing 关闭时为空，init 与此前完全相同。
	a.Init.Traceparent = obs.Traceparent(ctx)
	fail := func(err error) Outcome {
		in := ClassifyInput{StartErr: err}
		if ctx.Err() != nil {
			in.PlatformKill = ctxKillReason(ctx)
		}
		o := Outcome{ExitErr: err}
		o.Class, o.Retry = Classify(in)
		return o
	}
	initLine, err := protocol.EncodeLine(protocol.HostToWorker, &a.Init)
	if err != nil {
		return fail(fmt.Errorf("runner: init 不合法: %w", err))
	}
	if requestsSubruns(a.Init.Extensions) && a.Subruns == nil {
		return fail(errNoSubrunHost)
	}
	dir, err := openOutDir(a.OutDir)
	if err != nil {
		return fail(fmt.Errorf("runner: 打开 out_dir: %w", err))
	}
	defer func() { _ = dir.Close() }()
	sctx, sspan := obs.Start(ctx, "worker.start")
	h, err := r.starter.StartExec(sctx, a.EnvID, a.Exec)
	if err != nil {
		sspan.Fail("start_failed")
		sspan.End()
		return fail(err)
	}
	sspan.End()
	faultinject.Point(faultinject.WorkerStarted)
	at := &attemptRun{processor: newProcessor(r, a.TaskID, a.AttemptID, a.Init.Resume, dir), a: a, h: h,
		stdin: h.Stdin(), stderr: &tailBuffer{max: r.opt.StderrTail},
		readyCh: make(chan struct{}), finishing: make(chan struct{})}
	at.sendFn, at.appendFn = at.sendTask, r.store.AppendWorkerEvents
	at.ctlFn, at.killFn = at.sendTaskControl, at.kill
	at.stream.NegotiateExtensions(a.Init.Extensions)
	if requestsSubruns(a.Init.Extensions) {
		at.subs = newSubrunTracker(a.Subruns, r.opt.SubrunCancelTimeout)
	}
	return at.run(ctx, initLine, controls)
}

// ctxKillReason 是 Run 的 ctx 结束时的终止原因：累计运行时限（cause 为 ErrRunTimeExceeded 或到达
// deadline）为 timeout，其余为服务停止。
func ctxKillReason(ctx context.Context) string {
	if c := context.Cause(ctx); errors.Is(c, ErrRunTimeExceeded) || errors.Is(c, context.DeadlineExceeded) {
		return KillTimeout
	}
	return KillShutdown
}

// processor 是一个 attempt 的事件处理状态，task 模式（attemptRun）与 session 模式（sessionRun）共用：
// 持久化事件、保存产物、提交与查询 checkpoint、记录终态提议、执行 Store 故障阈值。除 storeDown 的
// 关闭（由读者 select）外只由处理器 goroutine 访问；其他 goroutine 只在处理器结束、或经通道得知
// 终态提议已处理之后读取 out。
type processor struct {
	r                  *Runner
	taskID, attemptID  string
	resumeCheckpointID string // init.resume / task_start.resume 的 checkpoint（paused 检查的候选）
	session            bool   // session 模式：result 的内容含 session_state
	dir                *outDir
	out                Outcome
	pctx               context.Context // 处理器的 ctx：屏障 B 超时或 attempt 结束后取消

	sendFn   func(protocol.Message) // 写出一条回复（编码方式与 0 字节/部分写出的处理由模式决定）
	appendFn func(ctx context.Context, attemptID string, events []WorkerEvent) (Watermark, error)
	// ctlFn 写出一条宿主发起的消息（subrun_cancel_requested），返回是否完整送达；killFn 终止执行（task 模式为 attempt，
	// session 模式为 incarnation）。
	ctlFn  func(protocol.Message) bool
	killFn func(reason string)
	subs   *subrunTracker // nil：未协商 sub-run 扩展

	storeDown       chan struct{} // Store 故障达阈值（§14.5），由处理器关闭一次
	storeDownClosed bool

	eventBytes int64
	pending    []WorkerEvent // 已接受但尚未持久化的 Worker 事件
	// unresolved 是提交结果未知的 checkpoint_id：它可能仍然生效，此时提交另一个 checkpoint
	// 会让指针顺序依赖竞争，因此以 commit_in_flight 拒绝，直到同一 ID 得到确定结果（规格 §5.5 第 4 条）。
	unresolved string
	saved      map[string]pinnedOutput // 本 attempt 已保存的产物：artifact_id → 最近一次保存的版本
	// committed 是本 attempt 中得知已提交的 checkpoint：checkpoint_id → commit_seq（提交或查询的结果），
	// 用于检查 paused/awaiting_input 的 checkpoint_id 是最新已提交者。
	committed map[string]int64
	// storeFailures 是连续计入故障阈值的 Store 失败次数，storeFailSince 是这一串失败的第一次时间。
	storeFailures  int
	storeFailSince time.Time
}

func newProcessor(r *Runner, taskID, attemptID string, resume *protocol.Resume, dir *outDir) processor {
	p := processor{r: r, taskID: taskID, attemptID: attemptID, dir: dir, saved: map[string]pinnedOutput{},
		committed: map[string]int64{}, storeDown: make(chan struct{})}
	if resume != nil {
		p.resumeCheckpointID = resume.CheckpointID
	}
	return p
}

// attemptRun 是一次 task 模式 Run 的状态。标明的字段之外只由处理器 goroutine 访问；主循环只在处理器结束
// （processed 关闭）之后读取处理器写入的 out 字段。
type attemptRun struct {
	processor
	a      Attempt
	h      provider.ExecHandle
	stderr *tailBuffer

	stdinMu     sync.Mutex
	stdin       io.WriteCloser
	stdinClosed bool

	// 处理器 → 主循环的通知，各由处理器关闭一次（关闭不会阻塞）。
	readyCh                      chan struct{} // 合法的 ready 已处理（OnReady 之后）
	finishing                    chan struct{} // 终态提议、handshake_error 或违规：Worker 应在 exit_grace 内退出
	readyClosed, finishingClosed bool

	killMu     sync.Mutex
	killReason string // 第一个终止原因
	killedLive bool   // 终止发生在进程退出之前
	exitSeen   bool

	stream      protocol.WorkerStream
	stopped     bool // 已出现违规：其后的行只读出丢弃
	readyCalled bool
}

type pinnedOutput struct {
	Version int64
	SHA256  string
}

type exitResult struct {
	status provider.ExitStatus
	err    error
}

// timer 是可停止、可为空的计时器；c 为 nil 时不参与 select。
type timer struct {
	t *time.Timer
	c <-chan time.Time
}

func newTimer(d time.Duration) timer {
	t := time.NewTimer(d)
	return timer{t: t, c: t.C}
}

func (t *timer) stop() {
	if t.t != nil {
		t.t.Stop()
	}
	t.t, t.c = nil, nil
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// run 是一次执行的主循环。处理器 goroutine 按 seq 处理事件；主循环独立执行取消、期限与退出
// （规格 §5.7"取消、超时、退出走独立路径"）：
//   - T_ready 内没有合法的 ready → 终止（ready_timeout）；
//   - 终态提议、handshake_error、违规或 stdout 关闭之后，exit_grace 内未退出 → 终止（exit_grace）；
//   - 控制：发送协议 cancel/pause，grace 到期或无法送达 → 终止；Worker 就绪前无法协作 → 直接终止；
//   - Store 故障达阈值 → 终止（store_unavailable，§14.5）；
//   - 进程退出后屏障 A：管道收尾，超时则终止执行树、标记 output_incomplete、关闭 stdout 读端
//     （readLines 丢弃末尾半行）；屏障 B：等处理器处理完全部已完整接收的事件，超时则取消处理器的
//     ctx、标记 output_incomplete，并等待它退出（其 Store 与 BlobStore 调用都受 ctx 约束）。
//
// 违规之后同样启动 exit_grace：违规的 Worker 不再被服务，但仍给它退出的机会，避免把仍在退出中的
// 进程误判为崩溃；规格未规定违规后是否立即终止，这里取有界等待。stdout 在进程退出前关闭时同样启动
// exit_grace：Worker 已不能再报告任何事件。
func (at *attemptRun) run(ctx context.Context, initLine []byte, controls <-chan Control) Outcome {
	opt := at.r.opt
	pctx, abandon := context.WithCancel(ctx)
	defer abandon()
	at.pctx = pctx
	go at.stderr.drain(at.h.Stderr())
	lines := make(chan lineItem, opt.EventQueue)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		readLines(ctx, at.h.Stdout(), lines, protocol.MaxEventBytes)
	}()
	exited := make(chan exitResult, 1) // 容量 1、唯一发送者：发送不会阻塞
	go func() {
		st, err := at.h.Wait()
		exited <- exitResult{st, err}
	}()
	// worker.handshake：init 写出 → 合法的 ready 处理完毕（或执行结束时仍未就绪）。
	_, handshake := obs.Start(ctx, "worker.handshake")
	handshakeOpen := true
	endHandshake := func(code string) {
		if handshakeOpen {
			handshakeOpen = false
			if code != "" {
				handshake.Fail(code)
			}
			handshake.End()
		}
	}
	defer endHandshake("not_ready")
	var finalize obs.Span
	defer func() {
		if finalize != nil {
			finalize.End()
		}
	}()
	if ok, partial := at.sendLine(initLine); !ok {
		at.closeStdin() // 未送达：Worker 等不到 init，由 T_ready 终止
		if partial {
			at.kill(KillStdinBroken)
		}
	}
	processed := make(chan struct{})
	go func() {
		defer close(processed)
		at.process(pctx, lines)
	}()

	var (
		exit                                *exitResult
		isProcessed, readerEOF              bool
		ready, finishing, storeDown         = at.readyCh, at.finishing, at.storeDown
		done                                = ctx.Done()
		ctrl                                string
		readyT                              = newTimer(opt.ReadyTimeout)
		graceT, exitGraceT, drainT, finishT timer
	)
	defer func() {
		for _, t := range []*timer{&readyT, &graceT, &exitGraceT, &drainT, &finishT} {
			t.stop()
		}
	}()
	startExitGrace := func() {
		if exit == nil && exitGraceT.c == nil {
			exitGraceT = newTimer(opt.ExitGrace)
		}
	}
	for exit == nil || !isProcessed {
		select {
		case <-processed:
			isProcessed, processed = true, nil
		case <-readerDone:
			readerEOF, readerDone = true, nil
			drainT.stop()
			if exit == nil {
				startExitGrace()
			} else {
				finishT = newTimer(opt.FinalizeTimeout)
			}
		case e := <-exited:
			exit = &e
			// worker.finalize：进程退出 → 已接收事件处理完毕、结果分类（屏障 A/B）。
			_, finalize = obs.Start(ctx, "worker.finalize")
			at.killMu.Lock()
			at.exitSeen = true
			at.killMu.Unlock()
			readyT.stop()
			graceT.stop()
			exitGraceT.stop()
			if readerEOF {
				finishT = newTimer(opt.FinalizeTimeout)
			} else {
				drainT = newTimer(opt.DrainTimeout)
			}
		case <-drainT.c: // 屏障 A 超时：执行树中仍有进程持有 stdout 写端
			drainT.stop()
			at.out.OutputIncomplete = true
			_ = at.h.Terminate(0)
			_ = at.h.Stdout().Close()
		case <-finishT.c: // 屏障 B 超时：放弃尚未处理的事件
			finishT.stop()
			at.out.OutputIncomplete = true
			abandon()
		case <-ready:
			ready = nil
			readyT.stop()
			endHandshake("")
		case <-finishing:
			finishing = nil
			readyT.stop()
			startExitGrace()
		case <-readyT.c:
			readyT.stop()
			if !isClosed(at.readyCh) {
				at.kill(KillReadyTimeout)
			}
		case <-exitGraceT.c:
			exitGraceT.stop()
			at.kill(KillExitGrace)
		case <-graceT.c:
			graceT.stop()
			at.kill(ctrl)
		case <-storeDown:
			storeDown = nil
			at.kill(KillStoreUnavailable)
		case <-done:
			done = nil
			at.kill(ctxKillReason(ctx))
		case c, ok := <-controls:
			if !ok {
				controls = nil
				continue
			}
			if (c.Kind != KillCancel && c.Kind != KillPause) || ctrl == KillCancel || ctrl == c.Kind {
				continue // 未知、重复，或 cancel 已生效（cancel 优先于 pause）
			}
			ctrl = c.Kind
			switch {
			case exit != nil || isClosed(at.finishing):
				// 已退出或正在结束：只记录宿主意图，由 exit_grace 约束退出
			case !isClosed(at.readyCh):
				at.kill(c.Kind) // Worker 尚未就绪，无法协作停止
			default:
				if ok, _ := at.sendLine(controlLine(at.a.AttemptID, c)); !ok {
					at.kill(c.Kind) // cancel/pause 无法送达 → 升级终止（§5.5 第 8 条）
					continue
				}
				grace := time.Duration(c.GraceMs) * time.Millisecond
				if grace <= 0 {
					grace = opt.ControlGrace
				}
				graceT.stop()
				graceT = newTimer(grace)
			}
		}
	}
	at.closeStdin()
	at.out.Exit, at.out.ExitErr = exit.status, exit.err
	if at.r.diag != nil {
		if d, err := at.r.diag(context.WithoutCancel(ctx), at.a.EnvID); err == nil {
			at.out.Diag = d
		}
	}
	at.out.StderrTail = at.stderr.bytes()
	at.killMu.Lock()
	at.out.PlatformKill, at.out.PlatformKilled = at.killReason, at.killedLive
	at.killMu.Unlock()
	at.out.Control = ctrl
	at.out.Class, at.out.Retry = Classify(ClassifyInput{
		Proposal: at.out.Proposal, Payload: at.out.ResultPayload, Exit: at.out.Exit, ExitErr: at.out.ExitErr,
		Diag: at.out.Diag, PlatformKill: at.out.PlatformKill, Control: ctrl, Violation: at.out.Violation,
		OutputIncomplete: at.out.OutputIncomplete, ControlLost: at.out.ExitErr != nil,
	})
	return at.out
}

// kill 记录终止原因（第一个原因为准）并终止执行。可由主循环与处理器调用。
func (at *attemptRun) kill(reason string) {
	at.killMu.Lock()
	if at.killReason == "" {
		at.killReason = reason
	}
	if !at.exitSeen {
		at.killedLive = true
	}
	at.killMu.Unlock()
	_ = at.h.Terminate(0)
}

func controlLine(attemptID string, c Control) []byte {
	reason := c.Reason
	if reason == "" {
		reason = "user"
	}
	grace := c.GraceMs
	var m protocol.Message
	if c.Kind == KillCancel {
		m = &protocol.Cancel{HostHeader: hostHeader(protocol.TypeCancel), AttemptID: attemptID, Reason: reason, GraceMS: grace}
	} else {
		m = &protocol.Pause{HostHeader: hostHeader(protocol.TypePause), AttemptID: attemptID, Reason: reason, GraceMS: grace}
	}
	line, err := protocol.EncodeLine(protocol.HostToWorker, m)
	if err != nil { // 由本包构造，编码失败是程序错误
		panic(fmt.Sprintf("runner: 编码 %s: %v", m.MessageType(), err))
	}
	return line
}

// process 是唯一的事件处理器：按到达顺序（即 seq 顺序）逐条处理，产物保存完成后才处理其后的
// 事件（含 checkpoint，规格 §5.7）。队列暂时为空或批次满时持久化已接受的事件。ctx 取消（屏障 B
// 超时或调用方取消）后其余的行只读出丢弃。
//
// sub-run 定时器的到期经 subrunFired 进入同一循环，与事件串行处理。
func (at *attemptRun) process(ctx context.Context, lines <-chan lineItem) {
	defer at.stopSubruns()
	fired := at.subrunFired()
	for {
		select {
		case item, ok := <-lines:
			if !ok {
				if ctx.Err() == nil {
					at.flush(ctx)
				}
				return
			}
			if at.stopped || ctx.Err() != nil {
				continue // 违规之后只读出丢弃，避免 Worker 因管道写满而阻塞
			}
			at.handle(ctx, item)
			if len(lines) == 0 || len(at.pending) >= at.r.opt.EventBatch {
				at.flush(ctx)
			}
		case f := <-fired:
			if !at.stopped && ctx.Err() == nil {
				at.subrunTimer(ctx, f)
			}
		}
	}
}

func (at *attemptRun) violate(code string) {
	at.out.Violation = code
	at.stopped = true
	at.stopSubruns()
	at.finish()
}

// finish 通知主循环 Worker 应当退出（启动 exit_grace）。
func (at *attemptRun) finish() {
	if !at.finishingClosed {
		at.finishingClosed = true
		close(at.finishing)
	}
}

func (at *attemptRun) handle(ctx context.Context, item lineItem) {
	at.eventBytes += int64(len(item.line))
	if item.tooLarge || at.eventBytes > at.r.opt.MaxAttemptEvents {
		at.violate(ViolationOutputLimit)
		return
	}
	m, err := protocol.DecodeLine(protocol.WorkerToHost, item.line)
	if err != nil {
		at.violate(violationCode(err))
		return
	}
	if err := at.stream.Observe(m); err != nil {
		at.violate(violationCode(err))
		return
	}
	if _, ok := m.(*protocol.HandshakeError); ok {
		// 不停止处理：其后若还有任何消息，Observe 判为 after_handshake_error 并覆盖此值
		at.out.Violation = ViolationHandshakeError
		at.finish()
		return
	}
	seq := eventSeq(m)
	at.pending = append(at.pending, WorkerEvent{Seq: seq, Type: m.MessageType(), Payload: json.RawMessage(item.line)})

	switch m := m.(type) {
	case *protocol.Ready:
		if m.Mode != protocol.ModeTask {
			at.violate(ViolationModeMismatch)
			return
		}
		// 先于 OnReady 关闭：调用方经 OnReady 得知就绪后发来的控制，主循环必然已视为就绪
		if !at.readyClosed {
			at.readyClosed = true
			close(at.readyCh)
		}
		if at.a.OnReady != nil && !at.readyCalled {
			at.readyCalled = true
			at.a.OnReady()
		}
	case *protocol.Artifact:
		at.flush(ctx)
		at.send(at.saveArtifact(ctx, m))
	case *protocol.Checkpoint:
		at.flush(ctx)
		at.send(at.commitCheckpoint(ctx, m))
	case *protocol.CheckpointQuery:
		at.flush(ctx)
		at.send(at.queryCheckpoint(ctx, m))
	case *protocol.SubrunStart, *protocol.SubrunEnd, *protocol.SubrunCancel:
		at.flush(ctx)
		if v := at.handleSubrun(ctx, m); v != "" {
			at.violate(v)
		}
	case *protocol.Result, *protocol.ErrorEvent:
		at.flush(ctx)
		at.stopSubruns()
		at.recordProposal(ctx, m)
		at.finish()
	case *protocol.Paused:
		at.flush(ctx)
		at.stopSubruns()
		at.recordProposal(ctx, m)
		if !at.pausedIsLatest(ctx, m.CheckpointID) {
			at.violate(ViolationPausedNotLatest)
		}
		at.finish()
	}
}

// pausedIsLatest 检查 paused.checkpoint_id 是最新已提交的 checkpoint（规格 §5.9）：它必须已提交，且
// commit_seq 不小于本 attempt 得知的任何已提交 checkpoint——包括 init.resume 指向的 checkpoint 与
// 提交结果未知的 checkpoint（尚未查询过的向 Store 查询）。无法确认（Store 不可用）时保守地返回 false。
func (at *processor) pausedIsLatest(ctx context.Context, id string) bool {
	candidates := []string{id}
	if at.resumeCheckpointID != "" {
		candidates = append(candidates, at.resumeCheckpointID)
	}
	if at.unresolved != "" {
		candidates = append(candidates, at.unresolved)
	}
	for _, c := range candidates {
		if _, ok := at.committed[c]; ok {
			continue
		}
		cc, err := at.r.store.QueryCheckpoint(ctx, Scope{Kind: protocol.ScopeTask, ID: at.taskID}, c)
		switch {
		case err == nil:
			at.noteStore(nil)
			at.committed[c] = cc.CommitSeq
		case errors.Is(err, persistence.ErrNotFound):
		default:
			at.noteStore(err)
			return false
		}
	}
	seq, ok := at.committed[id]
	if !ok {
		return false
	}
	for _, s := range at.committed {
		if s > seq {
			return false
		}
	}
	return true
}

// violationCode 把解码或顺序错误映射为 Outcome.Violation：大小与数量上限为 output_limit_exceeded
// （规格 §5.10），其余为 protocol 包的错误码。
func violationCode(err error) string {
	switch code := protocol.CodeOf(err); code {
	case protocol.CodeMessageTooLarge, protocol.CodeStateTooLarge, protocol.CodeTooManyRefs:
		return ViolationOutputLimit
	case "":
		return protocol.CodeMalformedJSON
	default:
		return code
	}
}

func eventSeq(m protocol.Message) int64 {
	switch m := m.(type) {
	case *protocol.Ready:
		return m.Seq
	case *protocol.Progress:
		return m.Seq
	case *protocol.Artifact:
		return m.Seq
	case *protocol.Checkpoint:
		return m.Seq
	case *protocol.CheckpointQuery:
		return m.Seq
	case *protocol.Paused:
		return m.Seq
	case *protocol.Result:
		return m.Seq
	case *protocol.ErrorEvent:
		return m.Seq
	case *protocol.SubrunStart:
		return m.Seq
	case *protocol.SubrunEnd:
		return m.Seq
	case *protocol.SubrunCancel:
		return m.Seq
	}
	return 0
}

// flush 以一批原子追加已接受的事件。失败时事件保留在 pending 中，下次 flush 以同样的内容重试
// （同序号同内容的重叠是幂等的）；处理不因事件日志暂时落后而停顿。
func (at *processor) flush(ctx context.Context) {
	for len(at.pending) > 0 {
		n := min(len(at.pending), at.r.opt.EventBatch)
		_, err := at.appendFn(ctx, at.attemptID, at.pending[:n])
		at.noteStore(err)
		if err != nil {
			return
		}
		at.pending = at.pending[n:]
	}
}

// noteStore 记录 Store 调用的结果并执行 Store 故障阈值（规格 §14.5、§19）：计入阈值的失败连续
// StoreFailureCount 次，或这一串失败持续达到 StoreFailureWindow，通知主循环以 store_unavailable 终止。
// 成功清零；不计入阈值的错误（冲突、拒绝、争用）不改变计数。处理器 ctx 已取消时的失败不计。
func (at *processor) noteStore(err error) {
	switch {
	case err == nil:
		at.storeFailures = 0
	case at.pctx.Err() != nil:
	case persistence.CountsTowardFailureThreshold(err):
		now := time.Now()
		if at.storeFailures == 0 {
			at.storeFailSince = now
		}
		at.storeFailures++
		if (at.storeFailures >= at.r.opt.StoreFailureCount || now.Sub(at.storeFailSince) >= at.r.opt.StoreFailureWindow) &&
			!at.storeDownClosed {
			at.storeDownClosed = true
			close(at.storeDown)
		}
	}
}

func hostHeader(typ string) protocol.HostHeader {
	return protocol.HostHeader{Type: typ, V: protocol.Version}
}

// ---- checkpoint ----

// commitCheckpoint 提交一个 checkpoint（span checkpoint.commit，属性只有 checkpoint.id 与结果状态）。
func (at *processor) commitCheckpoint(ctx context.Context, m *protocol.Checkpoint) *protocol.CheckpointResult {
	ctx, span := obs.Start(ctx, "checkpoint.commit", obs.Str("checkpoint.id", m.CheckpointID))
	res := at.commitCheckpointTx(ctx, m)
	span.SetAttrs(obs.Str("checkpoint.status", res.Status))
	if res.Status != protocol.CheckpointCommitted {
		span.Fail(res.Status)
	}
	span.End()
	return res
}

func (at *processor) commitCheckpointTx(ctx context.Context, m *protocol.Checkpoint) *protocol.CheckpointResult {
	res := &protocol.CheckpointResult{HostHeader: hostHeader(protocol.TypeCheckpointResult), CheckpointID: m.CheckpointID, Scope: m.Scope}
	if at.unresolved != "" && at.unresolved != m.CheckpointID {
		res.Status, res.Code = protocol.CheckpointRejected, codeCommitInFlight
		return res
	}
	if status, code := at.checkRefs(m); status != "" {
		res.Status, res.Code = status, code
		return res
	}
	faultinject.Point(faultinject.CheckpointCommitBefore)
	cc, err := at.r.store.CommitCheckpoint(ctx, Checkpoint{
		Scope:        Scope{Kind: protocol.ScopeTask, ID: at.taskID},
		CheckpointID: m.CheckpointID,
		AttemptID:    at.attemptID,
		StepID:       m.StepID,
		State:        stateOrNil(m.State),
		StateRef:     m.StateRef,
		Refs:         m.Refs,
		Subruns:      m.Subruns,
	})
	faultinject.Point(faultinject.CheckpointCommitAfter)
	at.noteStore(err)
	var rej *persistence.RejectedError
	switch {
	case err == nil:
		res.Status = protocol.CheckpointCommitted
		at.committed[m.CheckpointID] = cc.CommitSeq
	case errors.Is(err, persistence.ErrConflict):
		res.Status = protocol.CheckpointConflict
	case errors.As(err, &rej):
		res.Status, res.Code = protocol.CheckpointRejected, rej.Code
	default: // 提交结果未知、存储不可用、争用、取消：Worker 以同一 ID 查询或重发
		res.Status = protocol.CheckpointRetryableError
		if errors.Is(err, persistence.ErrCommitUnknown) {
			at.unresolved = m.CheckpointID
		}
		return res
	}
	if at.unresolved == m.CheckpointID {
		at.unresolved = ""
	}
	return res
}

// checkRefs 在提交前检查 refs、subruns[].result_ref 与 state_ref 指向的 blob 已完整保存（missing_ref）、state_ref
// 不超过上限（state_too_large）。授权到当前 scope 由 CommitCheckpoint 在事务内检查（ref_not_authorized）。
func (at *processor) checkRefs(m *protocol.Checkpoint) (status, code string) {
	refs := append([]string(nil), m.Refs...)
	for _, s := range m.Subruns {
		if s.ResultRef != "" {
			refs = append(refs, s.ResultRef)
		}
	}
	if m.StateRef != "" {
		refs = append(refs, m.StateRef)
	}
	for _, ref := range refs {
		b, err := at.r.blobs.Stat(ref)
		switch {
		case errors.Is(err, blob.ErrNotFound):
			return protocol.CheckpointRejected, codeMissingRef
		case err != nil:
			return protocol.CheckpointRetryableError, ""
		case ref == m.StateRef && b.Size > at.r.opt.MaxStateRefBytes:
			return protocol.CheckpointRejected, codeStateTooLarge
		}
	}
	return "", ""
}

func stateOrNil(s json.RawMessage) json.RawMessage {
	if t := bytes.TrimSpace(s); len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return nil
	}
	return s
}

func (at *processor) queryCheckpoint(ctx context.Context, m *protocol.CheckpointQuery) *protocol.CheckpointResult {
	res := &protocol.CheckpointResult{HostHeader: hostHeader(protocol.TypeCheckpointResult), CheckpointID: m.CheckpointID, Scope: m.Scope}
	cc, err := at.r.store.QueryCheckpoint(ctx, Scope{Kind: m.Scope, ID: at.taskID}, m.CheckpointID)
	if !errors.Is(err, persistence.ErrNotFound) {
		at.noteStore(err)
	}
	switch {
	case err == nil:
		res.Status = protocol.CheckpointCommitted
		at.committed[m.CheckpointID] = cc.CommitSeq
		if at.unresolved == m.CheckpointID {
			at.unresolved = ""
		}
	case errors.Is(err, persistence.ErrNotFound):
		res.Status = protocol.CheckpointNotFound // 提交结果未知时仍保持 unresolved：查不到不代表没有提交
	default:
		res.Status = protocol.CheckpointRetryableError
	}
	return res
}

// ---- 产物 ----

// artifactError 是产物被拒绝的原因，code 是回复中的稳定错误码。
type artifactError struct {
	code string
	err  error
}

func (e *artifactError) Error() string { return e.code + ": " + e.err.Error() }

// validArtifactPath 检查产物路径（规格 §5.6）：相对、非空、无 NUL、≤ 4 KiB、不含空、. 或 .. 分量。
// protocol 包在解码时已检查同一规则；这里在打开前再检查一次，不依赖调用方。
func validArtifactPath(p string) bool {
	if p == "" || len(p) > protocol.MaxArtifactPathBytes || strings.ContainsRune(p, 0) || strings.HasPrefix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// saveArtifact 按规格 §5.6 保存产物：从相对 out_dir 打开的 FD 边复制边哈希写入 BlobStore，
// 与声明值比较后登记，再答复。声明的哈希与大小只是待验证数据；已保存副本是权威内容。
func (at *processor) saveArtifact(ctx context.Context, m *protocol.Artifact) *protocol.ArtifactResult {
	res := &protocol.ArtifactResult{HostHeader: hostHeader(protocol.TypeArtifactResult), ArtifactID: m.ArtifactID}
	ref, version, err := at.storeArtifact(ctx, m)
	if err != nil {
		res.Status, res.Code = protocol.ArtifactRejected, err.code
		return res
	}
	at.saved[m.ArtifactID] = pinnedOutput{Version: version, SHA256: ref.SHA256}
	res.Status, res.Version, res.SHA256 = protocol.ArtifactSaved, version, ref.SHA256
	return res
}

func (at *processor) storeArtifact(ctx context.Context, m *protocol.Artifact) (blob.Ref, int64, *artifactError) {
	limit := at.r.opt.MaxArtifactBytes
	if !validArtifactPath(m.Path) {
		return blob.Ref{}, 0, &artifactError{code: codePathInvalid, err: errors.New(m.Path)}
	}
	if m.DeclaredSize > limit {
		return blob.Ref{}, 0, &artifactError{code: codeArtifactTooLarge, err: errors.New("声明大小超限")}
	}
	f, size, err := at.dir.open(m.Path)
	if err != nil {
		var ae *artifactError
		if errors.As(err, &ae) {
			return blob.Ref{}, 0, ae
		}
		return blob.Ref{}, 0, &artifactError{code: codePathInvalid, err: err}
	}
	defer func() { _ = f.Close() }()
	if size > limit {
		return blob.Ref{}, 0, &artifactError{code: codeArtifactTooLarge, err: fmt.Errorf("%d 字节", size)}
	}
	sctx, cancel := context.WithTimeout(ctx, at.r.opt.SaveTimeout)
	defer cancel()
	// 保存超时与 blob/Store 的暂时故障都答复 save_timeout（§5.4 中表示"未保存、可重试"的码）；
	// attempt 正在结束（ctx 已取消）时答复 cancelled。
	failed := func(err error) *artifactError {
		if ctx.Err() != nil {
			return &artifactError{code: codeCancelled, err: err}
		}
		return &artifactError{code: codeSaveTimeout, err: err}
	}
	// 读取上限多 1 字节：文件在打开后变长时能发现超限，而不是截断保存
	ref, err := at.r.blobs.Put(sctx, io.LimitReader(f, limit+1))
	if err != nil {
		return blob.Ref{}, 0, failed(err)
	}
	if ref.Size > limit {
		return blob.Ref{}, 0, &artifactError{code: codeArtifactTooLarge, err: fmt.Errorf("%d 字节", ref.Size)}
	}
	if ref.SHA256 != m.DeclaredSHA256 || ref.Size != m.DeclaredSize {
		return blob.Ref{}, 0, &artifactError{code: codeHashMismatch,
			err: fmt.Errorf("实际 %s/%d，声明 %s/%d", ref.SHA256, ref.Size, m.DeclaredSHA256, m.DeclaredSize)}
	}
	v, err := at.r.store.RegisterArtifact(sctx, Artifact{TaskID: at.taskID, AttemptID: at.attemptID, ArtifactID: m.ArtifactID,
		SHA256: ref.SHA256, Size: ref.Size, MediaType: m.MediaType, Visibility: m.Visibility})
	at.noteStore(err)
	var rej *persistence.RejectedError
	switch {
	case err == nil:
		return ref, v.Version, nil
	case errors.As(err, &rej):
		return blob.Ref{}, 0, &artifactError{code: rej.Code, err: err}
	default:
		return blob.Ref{}, 0, failed(err)
	}
}

// ---- 终态提议 ----

type pinnedRef struct {
	ArtifactID string `json:"artifact_id"`
	Version    int64  `json:"version,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
}

// recordProposal 在终态提议到达时保存其内容（BlobStore，Ref 为内容 sha256）并记录提议（规格 §5.7）。
// result 的 outputs 固定为本 attempt 最近一次保存的 (artifact_id, version, sha256)（规格 §5.6）；本 attempt
// 未保存的（恢复后的 attempt 不会重新登记之前保存的产物）固定为该任务中的最新版本；任务中不存在或
// 查询失败时只记录 ID。
func (at *processor) recordProposal(ctx context.Context, m protocol.Message) {
	var content any
	switch m := m.(type) {
	case *protocol.Result:
		outputs := make([]pinnedRef, 0, len(m.Outputs))
		for _, id := range m.Outputs {
			p, ok := at.saved[id]
			if !ok {
				v, err := at.r.store.LatestArtifact(ctx, at.taskID, id)
				if !errors.Is(err, persistence.ErrNotFound) {
					at.noteStore(err)
				}
				if err == nil {
					p = pinnedOutput{Version: v.Version, SHA256: v.SHA256}
				}
			}
			outputs = append(outputs, pinnedRef{ArtifactID: id, Version: p.Version, SHA256: p.SHA256})
		}
		var state *protocol.SessionState
		if at.session && m.SessionState != nil { // task 模式忽略 session_state（协议 README）
			state = m.SessionState
			at.out.SessionState = m.SessionState
		}
		// result.subruns[] 原样保存（供 inspect 与 API，规格 §13.6）；不据此改变 sub-run 状态
		content = struct {
			Summary      string                  `json:"summary"`
			Outputs      []pinnedRef             `json:"outputs"`
			SessionState *protocol.SessionState  `json:"session_state,omitempty"`
			Subruns      []protocol.ResultSubrun `json:"subruns,omitempty"`
		}{m.Summary, outputs, state, m.Subruns}
	case *protocol.AwaitingInput:
		content = struct {
			CheckpointID string `json:"checkpoint_id"`
			QuestionID   string `json:"question_id"`
		}{m.CheckpointID, m.QuestionID}
	case *protocol.ErrorEvent:
		content = struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			Retryable bool   `json:"retryable"`
		}{m.Code, m.Message, m.Retryable}
	case *protocol.Paused:
		content = struct {
			CheckpointID string `json:"checkpoint_id"`
		}{m.CheckpointID}
	}
	payload, err := json.Marshal(content)
	if err != nil { // 内容只含字符串、整数、布尔值与已校验的 JSON（session_state.state）
		panic(fmt.Sprintf("runner: 编码终态提议: %v", err))
	}
	sum := sha256.Sum256(payload)
	p := TerminalProposal{AttemptID: at.attemptID, Kind: m.MessageType(), Ref: hex.EncodeToString(sum[:])}
	at.out.Proposal, at.out.ResultPayload = &p, payload
	if _, err := at.r.blobs.Put(ctx, bytes.NewReader(payload)); err != nil {
		return // 内容仍在 Outcome 中，最终事务提交这份内容
	}
	rec, err := at.r.store.RecordTerminalProposal(ctx, p)
	at.noteStore(err)
	if err == nil {
		at.out.Proposal = &rec
	}
}

// ---- stdin ----

// send 写出一条回复（checkpoint_result、artifact_result 等），方式由模式决定。
func (at *processor) send(m protocol.Message) { at.sendFn(m) }

// sendTask 是 task 模式的写出：0 字节写出视为未送达，部分写出终止执行（§5.5 第 8 条）。
func (at *attemptRun) sendTask(m protocol.Message) {
	line, err := protocol.EncodeLine(protocol.HostToWorker, m)
	if err != nil { // 回复由本包构造，编码失败是程序错误
		panic(fmt.Sprintf("runner: 编码 %s: %v", m.MessageType(), err))
	}
	if _, partial := at.sendLine(line); partial {
		at.kill(KillStdinBroken)
	}
}

// sendTaskControl 写出宿主发起的消息（subrun_cancel_requested）并返回是否完整送达；部分写出终止执行（stdin_broken）。
func (at *attemptRun) sendTaskControl(m protocol.Message) bool {
	line, err := protocol.EncodeLine(protocol.HostToWorker, m)
	if err != nil { // 由本包构造，编码失败是程序错误
		panic(fmt.Sprintf("runner: 编码 %s: %v", m.MessageType(), err))
	}
	ok, partial := at.sendLine(line)
	if partial {
		at.kill(KillStdinBroken)
	}
	return ok
}

// sendLine 写一行到 stdin（规格 §5.5 第 8 条）：0 字节写出视为未送达（回复类消息由 Worker 重试或
// 查询）；部分写出意味着控制流损坏，关闭 stdin 并终止（调用方记录原因）。返回是否完整写出、是否部分写出。
func (at *attemptRun) sendLine(line []byte) (ok, partial bool) {
	at.stdinMu.Lock()
	defer at.stdinMu.Unlock()
	if at.stdinClosed {
		return false, false
	}
	type deadliner interface{ SetWriteDeadline(time.Time) error }
	d, hasDeadline := at.stdin.(deadliner)
	if hasDeadline {
		_ = d.SetWriteDeadline(time.Now().Add(at.r.opt.WriteTimeout))
		defer func() { _ = d.SetWriteDeadline(time.Time{}) }()
	}
	n, err := at.stdin.Write(append(line[:len(line):len(line)], '\n'))
	if err == nil {
		return true, false
	}
	if n > 0 {
		at.stdinClosed = true
		_ = at.stdin.Close()
		_ = at.h.Terminate(0)
		return false, true
	}
	return false, false
}

func (at *attemptRun) closeStdin() {
	at.stdinMu.Lock()
	defer at.stdinMu.Unlock()
	if !at.stdinClosed {
		at.stdinClosed = true
		_ = at.stdin.Close()
	}
}

// ---- stdout 与 stderr ----

type lineItem struct {
	line     []byte
	tooLarge bool
}

// readLines 把 stdout 按行读入有界队列，直到 EOF、读错误或 ctx 结束（发送在 ctx.Done 上 select，
// §14.5）。超过 max 字节的行不缓存，只报告 tooLarge 并读到行尾丢弃；末尾没有换行的半行丢弃。
func readLines(ctx context.Context, r io.Reader, out chan<- lineItem, max int) {
	defer close(out)
	send := func(it lineItem) bool {
		select {
		case out <- it:
			return true
		case <-ctx.Done():
			return false
		}
	}
	br := bufio.NewReaderSize(r, 64<<10)
	var buf []byte
	over := false
	for {
		chunk, err := br.ReadSlice('\n')
		if !over {
			if len(buf)+len(chunk) > max+1 { // +1 为换行符
				over, buf = true, nil
			} else {
				buf = append(buf, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return
		}
		item := lineItem{tooLarge: true}
		if !over {
			item = lineItem{line: bytes.Clone(buf[:len(buf)-1])}
		}
		if !send(item) {
			return
		}
		buf, over = buf[:0], false
	}
}

// tailBuffer 保留写入内容的最后 max 字节。
type tailBuffer struct {
	mu  sync.Mutex
	max int
	b   []byte
}

func (t *tailBuffer) drain(r io.Reader) {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			t.mu.Lock()
			t.b = append(t.b, buf[:n]...)
			if len(t.b) > t.max {
				t.b = append(t.b[:0], t.b[len(t.b)-t.max:]...)
			}
			t.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (t *tailBuffer) bytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return bytes.Clone(t.b)
}
