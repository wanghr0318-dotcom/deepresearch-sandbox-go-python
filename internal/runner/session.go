package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// 本文件是 session 模式（规格 §5.4 session 扩展、§5.7 session 屏障、§12.3–12.4）的 incarnation 驱动：
// 一个长期 Worker 进程按 task_start → task_accepted → 终态提议 → task_outcome → task_released 循环执行多个
// task attempt。读取 goroutine 与唯一的处理器 goroutine 贯穿整个 incarnation；每个 attempt 的事件处理复用
// task 模式的 processor（checkpoint、产物、提议、Store 故障阈值），事件以 incarnation seq 经
// AppendSessionWorkerEvents 入库。

// IncarnationSpec 是启动一个会话 incarnation 所需的全部输入。
type IncarnationSpec struct {
	SessionID, IncarnationID, EnvID string
	Argv, Env                       []string
	Config                          json.RawMessage
	Resume                          *protocol.SessionResume
	// WorkspaceRoot 是宿主侧 workspace 根：attempt 的产物目录为 <WorkspaceRoot>/out/<attempt_id>
	// （SessionAttempt.OutDir 为空时），以 openat2 逐级打开（规格 §5.6）。
	WorkspaceRoot string
	// Procs 返回编排环境 cgroup.procs（释放核验，§12.4）；nil 时不核验。就绪后的取值为基线。
	Procs func(ctx context.Context) ([]int, error)
	// GatewayIdle 等该 attempt 绑定的 Gateway 连接无在途请求且已关闭（edge.Detach）；nil 时不核验。
	GatewayIdle func(ctx context.Context, attemptID string) error
	// Extensions 是会话 init.extensions（规格 §5.2）：请求 subruns 时 ready 须回 subruns: 1，且每个 SessionAttempt
	// 须提供 Subruns。
	Extensions []string
}

// SessionAttempt 是会话中一次 task attempt 的输入。Attempt 中使用 TaskID、AttemptID、AttemptNo、
// Init 的 task 字段（Config、ConfigVersion、BudgetLimits、InputRefs、Traceparent、Resume、OutDir——为空时
// 取 /workspace/out/<attempt_id>）、OutDir（宿主侧；为空时取 <WorkspaceRoot>/out/<attempt_id>）与 OnReady
// （task_accepted 处理完毕后调用）；EnvID 与 Exec 不使用（进程属于 incarnation）。
type SessionAttempt struct {
	Attempt
	BaseSessionCheckpointID string
	// Directive 是 task_start.directive 的 JSON：finish_now 或 answer（契约 B）。空、null 或
	// {"kind":"continue"}（存储层 resume_directive 的取值）表示正常开始或继续，不发送 directive。
	Directive json.RawMessage
	// RestoredFromTaskID 仅用于展示（契约 C）；Carryover 指向被取代 turn 的最新 checkpoint（契约 D）。
	RestoredFromTaskID string
	Carryover          *protocol.Carryover
}

// TaskOutcome 是宿主对一个 attempt 的最终裁决（已提交），随 task_outcome 发给 Worker。
type TaskOutcome struct{ AttemptID, Verdict, CommittedSessionCheckpointID string }

// ReleaseResult 是释放流程的结果。Released 为 false 时调用方销毁 incarnation（§12.4，
// incarnations.end_reason = release_timeout）；已提交的任务终态与 session 指针不变。
type ReleaseResult struct {
	Released bool
	Reason   string // Release* 之一
}

// ReleaseResult.Reason 取值。
const (
	ReleaseTimeout           = "release_timeout"    // T_release 内未收到 task_released，或释放核验未通过
	ReleaseProtocolViolation = "protocol_violation" // 裁决（或终态提议）之后出现该 attempt 的协议违规（E49），或事件流已损坏
	ReleaseWorkerExited      = "worker_exited"      // incarnation 进程已退出
	ReleaseOutcomeUnresolved = "outcome_unresolved" // task_outcome 始终未能送达，或没有可释放的该 attempt
)

// EventPostVerdictViolation 是终态提议（或裁决）之后该 attempt 出现协议违规时写入的诊断 host 事件
// （规格 §5.8：不改写终态；记录诊断事件）。payload：{attempt_id, seq, type, code}。
const EventPostVerdictViolation = "post_verdict_violation"

// HostEventStore 是写诊断 host 事件的用例（postgres.Store.AppendHostEvent 实现）。Runner 的 Store
// 实现了它时，session 模式把 post_verdict_violation 写入任务事件流；未实现时只经 Release 与
// Incarnation.Violation 报告。
type HostEventStore interface {
	AppendHostEvent(ctx context.Context, taskID, attemptID, typ string, payload json.RawMessage) error
}

// StartError 是 StartIncarnation 的失败：Class/Retry 同 Classify 的取值，Violation 为启动阶段的协议违规码。
type StartError struct {
	Class, Retry string
	Violation    string
	Err          error
}

func (e *StartError) Error() string {
	if e.Violation != "" {
		return fmt.Sprintf("runner: incarnation 启动失败（%s，%s）: %v", e.Class, e.Violation, e.Err)
	}
	return fmt.Sprintf("runner: incarnation 启动失败（%s）: %v", e.Class, e.Err)
}

func (e *StartError) Unwrap() error { return e.Err }

var (
	// ErrIncarnationBusy：已有未释放的 attempt（或 RunTask 正在进行）时再调用 RunTask。
	ErrIncarnationBusy = errors.New("runner: incarnation 中已有未释放的 attempt")
	// ErrIncarnationGone：incarnation 进程已退出或事件流已损坏。
	ErrIncarnationGone = errors.New("runner: incarnation 已结束或事件流已损坏")
	errQuiesceTimeout  = errors.New("runner: quiesce 期限内没有 quiesced")
	errCloseTimeout    = errors.New("runner: session_close 期限内未结束，已终止")
)

// Incarnation 是一个运行中的会话 Worker 进程。
type Incarnation struct {
	r      *Runner
	spec   IncarnationSpec
	h      provider.ExecHandle
	stderr *tailBuffer
	lines  chan lineItem

	// mu 保护事件流状态机、stdin 写出与以下字段。宿主消息在持锁期间写出并经 HostSent 推进阶段，
	// 因此处理器 Accept Worker 的回应时一定已看到这条宿主消息。
	mu          sync.Mutex
	stream      *protocol.SessionStream
	stdin       io.WriteCloser
	stdinClosed bool
	att         *sessionRun // 当前 attempt：task_start 之后到释放成功
	running     bool        // RunTask 进行中
	violation   string      // 第一个协议违规码（含裁决之后的违规），诊断用
	broken      bool        // 事件流已损坏：之后的行只读出丢弃
	quiesced    string      // 最近一次 quiesced 报告的 session_checkpoint_id
	quiesceCh   chan string // Quiesce 等待中
	exitStatus  provider.ExitStatus
	exitErr     error

	// 处理器 → 等待者的通知，各关闭一次。
	readyCh   chan struct{} // 合法的 ready 已处理
	startFail chan startFailure
	brokenCh  chan struct{}
	closedCh  chan struct{} // closed 已处理
	exited    chan struct{} // 进程已退出（Wait 返回）
	procDone  chan struct{} // 处理器结束（stdout 已结束且全部行已处理）

	closeOnce                                  sync.Once
	readyClosed, startFailed, brokenC, closedC bool // 处理器私有
	killMu                                     sync.Mutex
	killReason                                 string
	killedLive, exitSeen                       bool
	baseline                                   []int // 就绪后的 Procs（已排序）
}

type startFailure struct {
	violation string
	payload   json.RawMessage // ready 之前的 error 事件内容
}

// sessionRun 是会话中一个 attempt 的状态。processor 只由处理器 goroutine 访问；标明的字段由 inc.mu 保护。
type sessionRun struct {
	processor
	inc     *Incarnation
	a       SessionAttempt
	abandon context.CancelFunc
	dirOnce sync.Once
	// handleMu 在处理器处理属于本 attempt 的一行期间持有：RunTask 在无提议时返回前取得它再设置 returned，
	// 之后的行必然看到 returned（不会有"已返回却又记录了提议"的竞争）。顺序：handleMu → inc.mu。
	handleMu sync.Mutex
	// handshake 是 worker.handshake span（只由 RunTask 的 goroutine 结束，见 endHandshake）。
	handshake     obs.Span
	handshakeDone bool

	// 处理器 → RunTask/Release 的通知。
	accepted chan struct{}
	proposed chan struct{} // 终态提议已处理（result 已快照），或提议之前出现违规
	released chan struct{} // task_released 已处理且该 attempt 的事件已全部入库（flushed）

	acceptedC, proposedC, releasedC bool // 处理器私有
	result                          Outcome
	flushed                         bool // 关闭 released 前写入

	// 以下由 inc.mu 保护。
	returned      bool                  // RunTask 已返回：之后的业务事件按裁决之后处理
	outcome       *protocol.TaskOutcome // Release 设置；task_outcome_query 时重发
	delivered     bool                  // task_outcome 至少完整写出一次
	postViolation string                // 终态提议或 RunTask 返回之后的第一个协议违规码
}

// StartIncarnation 启动会话 Worker：StartExec → init(session) → T_ready 内 ready（mode = session 且
// session_ext = 1）→ 记录进程基线。失败时终止进程并返回 *StartError（分类见 Classify / classifyStart）。
// incarnation 的生命周期不受 ctx 约束（ctx 只约束启动）。
func (r *Runner) StartIncarnation(ctx context.Context, s IncarnationSpec) (*Incarnation, error) {
	startErr := func(err error) error {
		in := ClassifyInput{StartErr: err}
		if ctx.Err() != nil {
			in.PlatformKill = ctxKillReason(ctx)
		}
		class, retry := Classify(in)
		return &StartError{Class: class, Retry: retry, Err: err}
	}
	init := &protocol.Init{Type: protocol.TypeInit, Bootstrap: protocol.BootstrapVersion,
		ProtocolVersions: []int64{protocol.Version}, Mode: protocol.ModeSession,
		SessionID: s.SessionID, IncarnationID: s.IncarnationID, Config: s.Config, SessionResume: s.Resume, Extensions: s.Extensions}
	if _, err := protocol.EncodeSessionLine(protocol.HostToWorker, init); err != nil {
		return nil, startErr(fmt.Errorf("runner: init 不合法: %w", err))
	}
	h, err := r.starter.StartExec(ctx, s.EnvID, provider.ExecSpec{ExecID: s.IncarnationID, Argv: s.Argv, Env: s.Env, Dir: "/workspace"})
	if err != nil {
		return nil, startErr(err)
	}
	inc := &Incarnation{r: r, spec: s, h: h, stderr: &tailBuffer{max: r.opt.StderrTail},
		lines: make(chan lineItem, r.opt.EventQueue), stream: protocol.NewSessionStream(), stdin: h.Stdin(),
		readyCh: make(chan struct{}), startFail: make(chan startFailure, 1), brokenCh: make(chan struct{}),
		closedCh: make(chan struct{}), exited: make(chan struct{}), procDone: make(chan struct{})}
	inc.loops()

	fail := func(in ClassifyInput, violation string, err error) (*Incarnation, error) {
		inc.kill(in.PlatformKill)
		<-inc.exited
		in.Exit, in.ExitErr = inc.exitStatus, inc.exitErr
		if in.ExitErr != nil {
			in.ControlLost = true
		}
		class, retry := Classify(in)
		return nil, &StartError{Class: class, Retry: retry, Violation: violation, Err: err}
	}
	if ok, partial, err := inc.sendHost(init); err != nil || !ok {
		if partial {
			return fail(ClassifyInput{PlatformKill: KillStdinBroken}, "", errors.New("runner: init 部分写出"))
		}
		// 未送达：Worker 等不到 init，由 T_ready 终止
	}
	startFailed := func(f startFailure) (*Incarnation, error) {
		if f.violation != "" {
			return fail(ClassifyInput{Violation: f.violation}, f.violation, fmt.Errorf("runner: 启动阶段协议违规 %s", f.violation))
		}
		// ready 之前的 error：Worker 报告启动失败（retryable 只是建议）
		in := ClassifyInput{Proposal: &TerminalProposal{Kind: protocol.TypeError}, Payload: f.payload}
		return fail(in, "", fmt.Errorf("runner: Worker 启动失败: %s", f.payload))
	}
	readyT := time.NewTimer(r.opt.ReadyTimeout)
	defer readyT.Stop()
	select {
	case <-inc.readyCh:
	case f := <-inc.startFail:
		return startFailed(f)
	case <-inc.exited:
		// 退出可能晚于（甚至由）处理器记录的启动违规 / ready 之前的 error / ready：处理器先发出通知再终止进程，
		// 但 select 在多路同时就绪时随机选择，Worker 也可能写完最后一行即退出而该行尚未处理。与 finishRun
		// 一样先等处理器处理完已接收的行（屏障 A 的期限内必然结束），再按处理器的结论判断，退出只作兜底。
		select {
		case <-inc.procDone:
		case <-ctx.Done():
			return fail(ClassifyInput{PlatformKill: ctxKillReason(ctx)}, "", ctx.Err())
		}
		select {
		case f := <-inc.startFail:
			return startFailed(f)
		default:
		}
		if !isClosed(inc.readyCh) {
			return fail(ClassifyInput{}, "", errors.New("runner: Worker 在 ready 之前退出"))
		}
		// 合法的 ready 已处理（之后才退出）：启动本身成功，进程退出由之后的 RunTask/Release 报告
	case <-readyT.C:
		return fail(ClassifyInput{PlatformKill: KillReadyTimeout}, "", errors.New("runner: T_ready 内没有 ready"))
	case <-ctx.Done():
		return fail(ClassifyInput{PlatformKill: ctxKillReason(ctx)}, "", ctx.Err())
	}
	if s.Procs != nil {
		pids, err := s.Procs(ctx)
		if err != nil {
			return fail(ClassifyInput{StartErr: err}, "", fmt.Errorf("runner: 读取进程基线: %w", err))
		}
		inc.baseline = sortedPids(pids)
	}
	return inc, nil
}

// loops 启动 stderr 读取、stdout 读取、处理器与退出等待。
func (inc *Incarnation) loops() {
	go inc.stderr.drain(inc.h.Stderr())
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		// 处理器总是读到 lines 关闭为止，读取者不会因 ctx 放弃发送
		readLines(context.Background(), inc.h.Stdout(), inc.lines, protocol.MaxEventBytes)
	}()
	go func() {
		defer close(inc.procDone)
		for {
			// 当前 attempt 的 sub-run 定时器到期与事件串行处理。定时器只在处理一行时登记，因此在阻塞前取到的
			// 通道总是最新的（attempt 切换之后的第一个定时器必然由之后的某一行登记）。
			inc.mu.Lock()
			att := inc.att
			inc.mu.Unlock()
			var fired <-chan subrunFire
			if att != nil {
				fired = att.subrunFired()
			}
			select {
			case item, ok := <-inc.lines:
				if !ok {
					inc.flushCurrent()
					return
				}
				inc.handle(item)
			case f := <-fired:
				inc.subrunFire(att, f)
			}
		}
	}()
	go func() {
		st, err := inc.h.Wait()
		inc.mu.Lock()
		inc.exitStatus, inc.exitErr = st, err
		inc.mu.Unlock()
		inc.killMu.Lock()
		inc.exitSeen = true
		inc.killMu.Unlock()
		close(inc.exited)
		inc.closeStdin()
		// 屏障 A：执行树中仍有进程持有 stdout 写端时，期限后关闭读端（readLines 丢弃末尾半行）
		t := time.NewTimer(inc.r.opt.DrainTimeout)
		defer t.Stop()
		select {
		case <-readerDone:
		case <-t.C:
			_ = inc.h.Stdout().Close()
		}
	}()
}

// Exited 在 incarnation 进程退出后关闭。
func (inc *Incarnation) Exited() <-chan struct{} { return inc.exited }

// ExitErr 是进程的退出情况：Wait 的错误，或非零退出码/信号；正常退出（0）或尚未退出时为 nil。
func (inc *Incarnation) ExitErr() error {
	select {
	case <-inc.exited:
	default:
		return nil
	}
	inc.mu.Lock()
	defer inc.mu.Unlock()
	switch {
	case inc.exitErr != nil:
		return inc.exitErr
	case inc.exitStatus.Signal != 0:
		return fmt.Errorf("runner: incarnation 被信号 %d 终止", inc.exitStatus.Signal)
	case inc.exitStatus.Code != 0:
		return fmt.Errorf("runner: incarnation 以退出码 %d 结束", inc.exitStatus.Code)
	}
	return nil
}

// Violation 返回该 incarnation 中出现的第一个协议违规码（含裁决之后的违规）；没有时为空。
func (inc *Incarnation) Violation() string {
	inc.mu.Lock()
	defer inc.mu.Unlock()
	return inc.violation
}

// ---- 宿主消息 ----

// sendHost 编码并写出一条宿主消息（规格 §5.5 第 8 条）：先在事件流状态机的副本上检查该消息在当前
// 阶段合法（不合法返回 err，什么也不写），完整写出后才推进阶段。0 字节写出 → 未送达（ok = false）；
// 部分写出 → 控制流损坏，关闭 stdin 并终止（partial = true）。
func (inc *Incarnation) sendHost(m protocol.Message) (ok, partial bool, err error) {
	line, err := protocol.EncodeSessionLine(protocol.HostToWorker, m)
	if err != nil {
		return false, false, err
	}
	broken := false
	defer func() { // 在释放 mu 之后执行（defer 后进先出）
		if broken {
			inc.kill(KillStdinBroken)
		}
	}()
	inc.mu.Lock()
	defer inc.mu.Unlock()
	next := *inc.stream
	if err := next.HostSent(m); err != nil {
		return false, false, err
	}
	if inc.stdinClosed {
		return false, false, nil
	}
	type deadliner interface{ SetWriteDeadline(time.Time) error }
	if d, ok := inc.stdin.(deadliner); ok {
		_ = d.SetWriteDeadline(time.Now().Add(inc.r.opt.WriteTimeout))
		defer func() { _ = d.SetWriteDeadline(time.Time{}) }()
	}
	n, werr := inc.stdin.Write(append(line[:len(line):len(line)], '\n'))
	if werr == nil {
		*inc.stream = next
		return true, false, nil
	}
	if n > 0 {
		inc.stdinClosed = true
		_ = inc.stdin.Close()
		broken = true
		return false, true, nil
	}
	return false, false, nil
}

func (inc *Incarnation) closeStdin() {
	inc.mu.Lock()
	defer inc.mu.Unlock()
	if !inc.stdinClosed {
		inc.stdinClosed = true
		_ = inc.stdin.Close()
	}
}

// kill 记录终止原因（第一个为准，空原因不记录）并终止执行。
func (inc *Incarnation) kill(reason string) {
	inc.killMu.Lock()
	if inc.killReason == "" {
		inc.killReason = reason
	}
	if !inc.exitSeen {
		inc.killedLive = true
	}
	inc.killMu.Unlock()
	_ = inc.h.Terminate(0)
}

// ---- 事件处理 ----

// eventHeader 是 Worker 事件的公共字段（解码成功之后再读取 seq 与 attempt_id）。
type eventHeader struct {
	Seq       int64  `json:"seq"`
	AttemptID string `json:"attempt_id"`
	Mode      string `json:"mode"`
	Type      string `json:"type"`
}

// handle 处理一行：解码 → 事件流检查 → 按类型分派。处理器 goroutine 独占调用。
func (inc *Incarnation) handle(item lineItem) {
	inc.mu.Lock()
	att, broken := inc.att, inc.broken
	inc.mu.Unlock()
	if broken {
		return // 事件流已损坏：只读出丢弃，避免 Worker 因管道写满而阻塞
	}
	if att != nil {
		att.handleMu.Lock()
		defer att.handleMu.Unlock()
		att.eventBytes += int64(len(item.line))
	}
	if item.tooLarge || (att != nil && att.eventBytes > inc.r.opt.MaxAttemptEvents) {
		inc.violate(att, ViolationOutputLimit, eventHeader{})
		return
	}
	var hdr eventHeader
	_ = json.Unmarshal(item.line, &hdr) // 只用于诊断与分类；解码失败时由 DecodeSessionLine 报告
	m, err := protocol.DecodeSessionLine(protocol.WorkerToHost, item.line)
	if err != nil {
		code := violationCode(err)
		if hdr.Type == protocol.TypeReady && hdr.Mode != "" && hdr.Mode != protocol.ModeSession {
			code = ViolationModeMismatch
		}
		inc.violate(att, code, hdr)
		return
	}
	inc.mu.Lock()
	err = inc.stream.Accept(m)
	phase := inc.stream.Phase()
	inc.mu.Unlock()
	if err != nil {
		code := violationCode(err)
		if att != nil && code == protocol.CodeAfterTerminal && phase == protocol.PhaseProposed && hdr.AttemptID == att.attemptID {
			// 终态提议或裁决之后该 attempt 的业务事件：事件流仍一致（阶段不变），task_outcome_query 与
			// task_released 照常处理（E49）
			att.postVerdict(code, hdr)
			return
		}
		inc.violate(att, code, hdr)
		return
	}
	switch m := m.(type) {
	case *protocol.HandshakeError:
		inc.failStart(startFailure{violation: ViolationHandshakeError})
	case *protocol.Ready:
		if !inc.readyClosed {
			inc.readyClosed = true
			close(inc.readyCh)
		}
	case *protocol.Quiesced:
		inc.mu.Lock()
		inc.quiesced = m.SessionCheckpointID
		ch := inc.quiesceCh
		inc.quiesceCh = nil
		inc.mu.Unlock()
		if ch != nil {
			ch <- m.SessionCheckpointID
		}
	case *protocol.Closed:
		if !inc.closedC {
			inc.closedC = true
			close(inc.closedCh)
		}
		inc.closeStdin() // Worker 已结束会话：stdin 的 EOF 让它退出
	case *protocol.ErrorEvent:
		if m.AttemptID == "" { // ready 之前的启动失败（事件流保证）
			inc.failStart(startFailure{payload: json.RawMessage(item.line)})
			return
		}
		att.handle(m, hdr.Seq, item.line)
	default:
		if att == nil { // 事件流保证带 attempt 的事件属于当前 attempt
			inc.violate(nil, protocol.CodeWrongAttempt, hdr)
			return
		}
		att.handle(m, hdr.Seq, item.line)
	}
}

func (inc *Incarnation) failStart(f startFailure) {
	if !inc.startFailed {
		inc.startFailed = true
		inc.startFail <- f
	}
}

// violate 记录事件流损坏：之后的行只读出丢弃，终止进程。当前 attempt 尚未提议时，违规成为它的结果
// （RunTask 返回 protocol_violation 类）；已提议或 RunTask 已返回时记为裁决之后的违规（Release 返回
// protocol_violation）。ready 之前的违规使启动失败。
func (inc *Incarnation) violate(att *sessionRun, code string, hdr eventHeader) {
	inc.mu.Lock()
	inc.broken = true
	if inc.violation == "" {
		inc.violation = code
	}
	returned := att != nil && att.returned
	inc.mu.Unlock()
	if !inc.brokenC {
		inc.brokenC = true
		close(inc.brokenCh)
	}
	switch {
	case att == nil:
		inc.failStart(startFailure{violation: code})
	case att.proposedC || returned:
		att.postVerdict(code, hdr)
	default:
		att.flush(att.pctx)
		att.out.Violation = code
		att.propose()
	}
	inc.kill("")
}

// flushCurrent 在 stdout 结束后持久化当前 attempt 尚未入库的事件，并关闭其产物目录（不再有事件）。
func (inc *Incarnation) flushCurrent() {
	inc.mu.Lock()
	att := inc.att
	inc.mu.Unlock()
	if att == nil {
		return
	}
	if att.pctx.Err() == nil {
		att.flush(att.pctx)
	}
	att.closeDir()
}

func (att *sessionRun) closeDir() { att.dirOnce.Do(func() { _ = att.dir.Close() }) }

// handle 处理属于本 attempt 的一条已通过事件流检查的事件（处理器 goroutine）。
func (att *sessionRun) handle(m protocol.Message, seq int64, line []byte) {
	ctx := att.pctx
	inc := att.inc
	inc.mu.Lock()
	returned := att.returned
	inc.mu.Unlock()
	if returned && !att.proposedC && isBusinessEvent(m) {
		// RunTask 已在无提议时返回（cancel 的 grace 到期）：结果已交给调用方，之后的业务事件按裁决之后处理
		att.postVerdict(protocol.CodeAfterTerminal, eventHeader{Seq: seq, Type: m.MessageType(), AttemptID: att.attemptID})
		return
	}
	att.pending = append(att.pending, WorkerEvent{Seq: seq, Type: m.MessageType(), Payload: json.RawMessage(line)})
	switch m := m.(type) {
	case *protocol.TaskAccepted:
		att.flush(ctx)
		if !att.acceptedC {
			att.acceptedC = true
			close(att.accepted)
		}
		if att.a.OnReady != nil {
			att.a.OnReady()
		}
	case *protocol.Artifact:
		att.flush(ctx)
		att.send(att.saveArtifact(ctx, m))
	case *protocol.Checkpoint:
		att.flush(ctx)
		att.send(att.commitCheckpoint(ctx, m))
	case *protocol.CheckpointQuery:
		att.flush(ctx)
		att.send(att.queryCheckpoint(ctx, m))
	case *protocol.SubrunStart, *protocol.SubrunEnd, *protocol.SubrunCancel:
		att.flush(ctx)
		if v := att.handleSubrun(ctx, m); v != "" {
			inc.violate(att, v, eventHeader{Seq: seq, Type: m.MessageType(), AttemptID: att.attemptID})
		}
	case *protocol.Result, *protocol.ErrorEvent:
		att.flush(ctx)
		att.recordProposal(ctx, m)
		att.propose()
	case *protocol.Paused:
		att.flush(ctx)
		att.recordProposal(ctx, m)
		if !att.pausedIsLatest(ctx, m.CheckpointID) {
			att.out.Violation = ViolationPausedNotLatest
		}
		att.propose()
	case *protocol.AwaitingInput:
		att.flush(ctx)
		att.recordProposal(ctx, m)
		if !att.pausedIsLatest(ctx, m.CheckpointID) { // 契约 A：checkpoint_id 须为最新已提交 task checkpoint
			att.out.Violation = ViolationPausedNotLatest
		}
		att.propose()
	case *protocol.TaskOutcomeQuery:
		att.flush(ctx)
		inc.mu.Lock()
		o := att.outcome
		inc.mu.Unlock()
		if o != nil { // 裁决尚未提交时不答复：Worker 超时后再次查询
			att.sendOutcome(o)
		}
	case *protocol.TaskReleased:
		// 释放核验的第一项：task_released 之前（含）的事件已全部处理并入库
		for att.flush(ctx); len(att.pending) > 0 && ctx.Err() == nil; att.flush(ctx) {
			select {
			case <-ctx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
		att.flushed = len(att.pending) == 0
		if !att.releasedC {
			att.releasedC = true
			close(att.released)
		}
	default: // progress 等：队列暂时为空或批次满时成批持久化
		if len(inc.lines) == 0 || len(att.pending) >= inc.r.opt.EventBatch {
			att.flush(ctx)
		}
	}
}

// isBusinessEvent：规格 §5.8 的业务事件（progress、artifact、checkpoint、subrun_* 与终态提议）。
func isBusinessEvent(m protocol.Message) bool {
	switch m.(type) {
	case *protocol.CheckpointQuery, *protocol.TaskOutcomeQuery, *protocol.TaskReleased, *protocol.TaskAccepted:
		return false
	}
	return true
}

// propose 快照结果并通知 RunTask（终态提议已处理，§5.7 session 屏障）。
func (att *sessionRun) propose() {
	if att.proposedC {
		return
	}
	att.stopSubruns()
	att.proposedC = true
	att.result = att.out
	close(att.proposed)
}

// subrunFire 处理当前 attempt 的一次 sub-run 定时器到期（处理器 goroutine）：终态提议之后或 RunTask 返回之后不再处理。
func (inc *Incarnation) subrunFire(att *sessionRun, f subrunFire) {
	att.handleMu.Lock()
	defer att.handleMu.Unlock()
	inc.mu.Lock()
	skip := inc.att != att || att.returned || inc.broken
	inc.mu.Unlock()
	if skip || att.proposedC {
		return
	}
	att.subrunTimer(att.pctx, f)
}

// sendControl 写出宿主发起的消息（subrun_cancel_requested），返回是否完整送达（部分写出时 sendHost 已终止）。
func (att *sessionRun) sendControl(m protocol.Message) bool {
	ok, _, err := att.inc.sendHost(m)
	return err == nil && ok
}

// postVerdict 记录终态提议（或裁决）之后该 attempt 的协议违规：不改写结果，写诊断 host 事件，
// 使 Release 返回 protocol_violation（规格 §5.8、E49）。
func (att *sessionRun) postVerdict(code string, hdr eventHeader) {
	inc := att.inc
	inc.mu.Lock()
	if att.postViolation == "" {
		att.postViolation = code
	}
	if inc.violation == "" {
		inc.violation = code
	}
	inc.mu.Unlock()
	hs, ok := inc.r.store.(HostEventStore)
	if !ok {
		return
	}
	payload, err := json.Marshal(struct {
		AttemptID string `json:"attempt_id"`
		Seq       int64  `json:"seq"`
		Type      string `json:"type"`
		Code      string `json:"code"`
	}{att.attemptID, hdr.Seq, hdr.Type, code})
	if err != nil { // 只含字符串与整数
		panic(fmt.Sprintf("runner: 编码诊断事件: %v", err))
	}
	// 诊断写入失败不影响判定（Release 仍返回 protocol_violation）
	_ = hs.AppendHostEvent(att.pctx, att.taskID, att.attemptID, EventPostVerdictViolation, payload)
}

// sendReply 是 session 模式的回复写出：带 attempt_id；0 字节写出视为未送达（Worker 重试或查询）。
func (att *sessionRun) sendReply(m protocol.Message) {
	switch m := m.(type) {
	case *protocol.CheckpointResult:
		m.AttemptID = att.attemptID
	case *protocol.ArtifactResult:
		m.AttemptID = att.attemptID
	}
	if _, _, err := att.inc.sendHost(m); err != nil {
		if _, ok := m.(*protocol.SubrunStarted); ok {
			return // 会话已在关闭（没有当前 attempt）：不再答复，Worker 随会话结束
		}
		panic(fmt.Sprintf("runner: 写出 %s: %v", m.MessageType(), err)) // 由本包构造且阶段必然允许：程序错误
	}
}

// sendOutcome 写出（或重发）task_outcome。
func (att *sessionRun) sendOutcome(o *protocol.TaskOutcome) {
	ok, _, err := att.inc.sendHost(o)
	if err != nil {
		return // 事件流已不允许（例如已 session_close）：由 Release 的期限处理
	}
	if ok {
		att.inc.mu.Lock()
		att.delivered = true
		att.inc.mu.Unlock()
	}
}

// ---- task ----

// buildTaskStart 构造 task_start（契约 B/C/D）。
func (inc *Incarnation) buildTaskStart(a SessionAttempt) (*protocol.TaskStart, error) {
	outDir := a.Init.OutDir
	if outDir == "" {
		outDir = "/workspace/out/" + a.AttemptID
	}
	ts := &protocol.TaskStart{HostHeader: hostHeader(protocol.TypeTaskStart), TaskID: a.TaskID, AttemptID: a.AttemptID,
		AttemptNo: a.AttemptNo, Traceparent: a.Init.Traceparent, Config: a.Init.Config, ConfigVersion: a.Init.ConfigVersion,
		BudgetLimits: a.Init.BudgetLimits, InputRefs: a.Init.InputRefs, OutDir: outDir,
		BaseSessionCheckpointID: a.BaseSessionCheckpointID, Resume: a.Init.Resume,
		RestoredFromTaskID: a.RestoredFromTaskID, Carryover: a.Carryover}
	if d := a.Directive; hasDirective(d) {
		var dir protocol.Directive
		if err := json.Unmarshal(d, &dir); err != nil {
			return nil, fmt.Errorf("runner: directive 不合法: %w", err)
		}
		if dir.Kind != "continue" {
			ts.Directive = &dir
		}
	}
	if _, err := protocol.EncodeSessionLine(protocol.HostToWorker, ts); err != nil {
		return nil, fmt.Errorf("runner: task_start 不合法: %w", err)
	}
	return ts, nil
}

func hasDirective(d json.RawMessage) bool { return stateOrNil(d) != nil }

// RunTask 在 incarnation 中执行一个 attempt：task_start → T_task_accept 内 task_accepted（否则终止 incarnation，
// ready_timeout）→ 按 seq 处理该 attempt 的事件 → 终态提议且其前事件全部处理完成后返回（session 屏障 §5.7）。
// 返回的 Outcome 已由 Classify 分类（session 模式的 result 不要求进程退出）。控制：
//   - pause：发送协议 pause，grace 内没有终态提议 → 终止 incarnation（paused）；
//   - cancel：发送协议 cancel，grace 内没有终态提议 → 返回 cancelled，不终止——Worker 取消后不发提议、
//     等待裁决（Python SDK），调用方以 Release(cancelled) 释放或销毁 incarnation；
//
// 进程退出 → 按退出原因分类；Store 故障达阈值或 ctx 结束 → 终止 incarnation。同一时刻至多一个
// RunTask，且上一个 attempt 释放成功之前不能开始下一个（ErrIncarnationBusy）。
func (inc *Incarnation) RunTask(ctx context.Context, a SessionAttempt, controls <-chan Control) (out Outcome) {
	ctx, span := obs.Start(ctx, "worker.run", runAttrs("session", a.Attempt)...)
	defer func() { endRun(span, out) }()
	a.Init.Traceparent = obs.Traceparent(ctx) // task_start.traceparent（见 Runner.Run）
	defer obs.ExpectWorker(a.AttemptID, a.Init.Traceparent)()
	return inc.runTask(ctx, a, controls)
}

func (inc *Incarnation) runTask(ctx context.Context, a SessionAttempt, controls <-chan Control) Outcome {
	fail := func(err error) Outcome {
		in := ClassifyInput{StartErr: err}
		if ctx.Err() != nil {
			in.PlatformKill = ctxKillReason(ctx)
		}
		o := Outcome{ExitErr: err}
		o.Class, o.Retry = Classify(in)
		return o
	}
	ts, err := inc.buildTaskStart(a)
	if err != nil {
		return fail(err)
	}
	negotiated := requestsSubruns(inc.spec.Extensions)
	if negotiated && a.Subruns == nil {
		return fail(errNoSubrunHost)
	}
	inc.mu.Lock()
	switch {
	case inc.running || inc.att != nil:
		inc.mu.Unlock()
		return fail(ErrIncarnationBusy)
	case inc.broken || isClosed(inc.exited):
		inc.mu.Unlock()
		// incarnation 在授予之后丢失：同控制连接断开（control_lost，按故障重试）
		return fail(fmt.Errorf("%w: %w", provider.ErrControlLost, ErrIncarnationGone))
	}
	inc.running = true
	inc.mu.Unlock()
	defer func() {
		inc.mu.Lock()
		inc.running = false
		inc.mu.Unlock()
	}()

	hostOut := a.OutDir
	if hostOut == "" {
		hostOut = filepath.Join(inc.spec.WorkspaceRoot, "out", a.AttemptID)
	}
	dir, err := openOutDir(hostOut)
	if err != nil {
		return fail(fmt.Errorf("runner: 打开 out_dir: %w", err))
	}
	pctx, abandon := context.WithCancel(obs.Carry(context.Background(), ctx)) // 只带 span，不继承取消
	att := &sessionRun{processor: newProcessor(inc.r, a.TaskID, a.AttemptID, a.Init.Resume, dir), inc: inc, a: a,
		abandon: abandon, accepted: make(chan struct{}), proposed: make(chan struct{}), released: make(chan struct{})}
	att.session, att.pctx = true, pctx
	att.sendFn, att.appendFn = att.sendReply, inc.r.store.AppendSessionWorkerEvents
	att.ctlFn, att.killFn = att.sendControl, inc.kill
	if negotiated {
		att.subs = newSubrunTracker(a.Subruns, inc.r.opt.SubrunCancelTimeout)
	}

	inc.mu.Lock()
	inc.att = att
	inc.mu.Unlock()
	// worker.handshake：task_start 写出 → task_accepted（或 RunTask 结束时仍未接受）。只由本 goroutine（wait）结束。
	_, att.handshake = obs.Start(ctx, "worker.handshake")
	defer att.endHandshake("not_accepted")
	ok, partial, err := inc.sendHost(ts)
	if err != nil { // 阶段不允许（例如已 session_close）：不写出
		inc.mu.Lock()
		inc.att = nil
		inc.mu.Unlock()
		abandon()
		_ = dir.Close()
		return fail(err)
	}
	if !ok && !partial {
		inc.kill(KillReadyTimeout) // task_start 未送达：Worker 不读 stdin，视为挂死（§5.4 T_task_accept）
	}
	return inc.wait(ctx, att, controls)
}

// wait 是 RunTask 的等待循环。
func (inc *Incarnation) wait(ctx context.Context, att *sessionRun, controls <-chan Control) Outcome {
	opt := inc.r.opt
	var (
		ctrl               string
		accepted, proposed = att.accepted, att.proposed
		exited, storeDown  = inc.exited, att.storeDown
		done               = ctx.Done()
		acceptT            = newTimer(opt.TaskAcceptTimeout)
		graceT             timer
		killed             bool
	)
	defer acceptT.stop()
	defer graceT.stop()
	for {
		select {
		case <-accepted:
			accepted = nil
			acceptT.stop()
			att.endHandshake("")
		case <-acceptT.c:
			acceptT.stop()
			inc.kill(KillReadyTimeout)
			killed = true
		case <-proposed:
			return inc.finishRun(ctx, att, ctrl, false)
		case <-exited:
			return inc.finishRun(ctx, att, ctrl, true)
		case <-storeDown:
			storeDown = nil
			inc.kill(KillStoreUnavailable)
			killed = true
		case <-done:
			done = nil
			inc.kill(ctxKillReason(ctx))
			killed = true
		case <-graceT.c:
			graceT.stop()
			if ctrl == KillCancel && !killed {
				return inc.finishRun(ctx, att, ctrl, false)
			}
			inc.kill(ctrl)
			killed = true
		case c, ok := <-controls:
			if !ok {
				controls = nil
				continue
			}
			if (c.Kind != KillCancel && c.Kind != KillPause) || ctrl == KillCancel || ctrl == c.Kind {
				continue // 未知、重复，或 cancel 已生效（cancel 优先于 pause）
			}
			ctrl = c.Kind
			if killed {
				continue // 已在终止中：只记录宿主意图
			}
			if ok, _, err := inc.sendHost(sessionControl(att.attemptID, c)); err != nil || !ok {
				inc.kill(c.Kind) // cancel/pause 无法送达 → 升级终止（§5.5 第 8 条）
				killed = true
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

func sessionControl(attemptID string, c Control) protocol.Message {
	reason := c.Reason
	if reason == "" {
		reason = "user"
	}
	if c.Kind == KillCancel {
		return &protocol.Cancel{HostHeader: hostHeader(protocol.TypeCancel), AttemptID: attemptID, Reason: reason, GraceMS: c.GraceMs}
	}
	return &protocol.Pause{HostHeader: hostHeader(protocol.TypePause), AttemptID: attemptID, Reason: reason, GraceMS: c.GraceMs}
}

// finishRun 汇总 RunTask 的结果。终态提议已处理时取其快照（进程未退出，退出状态为零值）；进程已退出时先等
// 处理器处理完已接收的事件（屏障 A/B 的期限内；提议可能就在其中），再按退出原因分类。
func (inc *Incarnation) finishRun(ctx context.Context, att *sessionRun, ctrl string, exited bool) Outcome {
	opt := inc.r.opt
	var out Outcome
	switch {
	case isClosed(att.proposed):
		out = att.result
	case exited:
		incomplete := false
		t := time.NewTimer(opt.DrainTimeout + opt.FinalizeTimeout)
		select {
		case <-att.proposed:
		case <-inc.procDone:
		case <-t.C: // 屏障 B 超时：放弃尚未处理的事件
			incomplete = true
			att.abandon()
			<-inc.procDone
		}
		t.Stop()
		if isClosed(att.proposed) {
			out = att.result
			break
		}
		out = att.out // 处理器已结束
		out.OutputIncomplete = incomplete
		inc.mu.Lock()
		out.Exit, out.ExitErr = inc.exitStatus, inc.exitErr
		inc.mu.Unlock()
	default:
		// cancel 的 grace 到期且没有提议：先等处理器处理完当前一行，再标记已返回，之后的业务事件按裁决之后处理
		att.handleMu.Lock()
		inc.mu.Lock()
		att.returned = true
		inc.mu.Unlock()
		att.handleMu.Unlock()
		if isClosed(att.proposed) { // 提议恰在此前处理完成：按"取消期间完成"
			out = att.result
		}
	}
	inc.mu.Lock()
	att.returned = true
	inc.mu.Unlock()
	if att.subs != nil {
		att.subs.stop() // 结果已交给调用方：之后到期的 sub-run 定时器不再处理（定时器本身由 stop 失效）
	}
	if inc.r.diag != nil {
		if d, err := inc.r.diag(context.WithoutCancel(ctx), inc.spec.EnvID); err == nil {
			out.Diag = d
		}
	}
	out.StderrTail = inc.stderr.bytes()
	inc.killMu.Lock()
	out.PlatformKill, out.PlatformKilled = inc.killReason, inc.killedLive
	inc.killMu.Unlock()
	out.Control = ctrl
	out.Class, out.Retry = Classify(ClassifyInput{
		Proposal: out.Proposal, Payload: out.ResultPayload, Exit: out.Exit, ExitErr: out.ExitErr, Diag: out.Diag,
		PlatformKill: out.PlatformKill, Control: ctrl, Violation: out.Violation, OutputIncomplete: out.OutputIncomplete,
		ControlLost: out.ExitErr != nil,
	})
	return out
}

// ---- 释放 ----

// Release 发送 task_outcome（可多次答复 task_outcome_query）并在 T_release 内等 task_released，再核验
// （§12.4）：其前事件已处理并入库、GatewayIdle(attemptID) 返回、Procs 等于基线。任何一项失败 → Released =
// false（调用方销毁 incarnation）。终态提议或裁决之后出现该 attempt 的协议违规 → protocol_violation（E49）。
// 成功后 incarnation 回到空闲，可开始下一个 RunTask。
func (inc *Incarnation) Release(ctx context.Context, o TaskOutcome) ReleaseResult {
	fail := func(reason string) ReleaseResult { return ReleaseResult{Reason: reason} }
	inc.mu.Lock()
	att := inc.att
	if att == nil || att.attemptID != o.AttemptID || inc.running {
		inc.mu.Unlock()
		if isClosed(inc.exited) {
			return fail(ReleaseWorkerExited)
		}
		return fail(ReleaseOutcomeUnresolved)
	}
	msg := &protocol.TaskOutcome{HostHeader: hostHeader(protocol.TypeTaskOutcome), AttemptID: o.AttemptID,
		Verdict: o.Verdict, CommittedSessionCheckpointID: o.CommittedSessionCheckpointID}
	att.outcome = msg // 先登记：此后到达的 task_outcome_query 由处理器重发
	inc.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, inc.r.opt.ReleaseTimeout)
	defer cancel()
	if _, err := protocol.EncodeSessionLine(protocol.HostToWorker, msg); err != nil {
		return fail(ReleaseOutcomeUnresolved) // verdict 不合法：调用方的程序错误
	}
	att.sendOutcome(msg) // 0 字节写出：等 Worker 查询（§5.5 第 8 条）
	result := func() ReleaseResult {
		inc.mu.Lock()
		post, delivered, broken := att.postViolation, att.delivered, inc.broken
		inc.mu.Unlock()
		switch {
		case post != "" || broken:
			return fail(ReleaseProtocolViolation)
		case isClosed(inc.exited):
			return fail(ReleaseWorkerExited)
		case !delivered:
			return fail(ReleaseOutcomeUnresolved)
		}
		return fail(ReleaseTimeout)
	}
	select {
	case <-att.released:
	case <-inc.brokenCh:
		return inc.releaseFailed(att, result())
	case <-inc.exited:
		return inc.releaseFailed(att, result())
	case <-rctx.Done():
		return inc.releaseFailed(att, result())
	}
	if r := result(); r.Reason == ReleaseProtocolViolation || r.Reason == ReleaseWorkerExited {
		return inc.releaseFailed(att, r)
	}
	if !att.flushed {
		return inc.releaseFailed(att, fail(ReleaseTimeout))
	}
	if g := inc.spec.GatewayIdle; g != nil {
		if err := g(rctx, o.AttemptID); err != nil {
			return inc.releaseFailed(att, fail(ReleaseTimeout))
		}
	}
	if !inc.procsAtBaseline(rctx) {
		return inc.releaseFailed(att, fail(ReleaseTimeout))
	}
	inc.mu.Lock()
	post := att.postViolation
	if post == "" {
		inc.att = nil
	}
	inc.mu.Unlock()
	if post != "" {
		return inc.releaseFailed(att, fail(ReleaseProtocolViolation))
	}
	att.abandon()
	att.closeDir()
	return ReleaseResult{Released: true}
}

// releaseFailed：释放失败的 attempt 保持为当前 attempt（incarnation 不再接受 RunTask，调用方销毁它）；
// 停止该 attempt 的 Store 调用。产物目录此后不再使用（RunTask 返回之后的业务事件不处理）。
func (inc *Incarnation) releaseFailed(att *sessionRun, r ReleaseResult) ReleaseResult {
	att.abandon()
	att.closeDir()
	return r
}

// procsAtBaseline 在期限内轮询 Procs，直到等于基线（释放中的子进程可能稍后才退出）。
func (inc *Incarnation) procsAtBaseline(ctx context.Context) bool {
	if inc.spec.Procs == nil {
		return true
	}
	for {
		pids, err := inc.spec.Procs(ctx)
		if err == nil && slices.Equal(sortedPids(pids), inc.baseline) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func sortedPids(p []int) []int {
	out := slices.Clone(p)
	slices.Sort(out)
	return out
}

// ---- quiesce / close ----

// Quiesce 发送 quiesce，grace 内等 quiesced，返回其 session_checkpoint_id（空串 = 会话尚无 checkpoint）。
// 上次 quiesced 之后没有运行任何 task 时直接返回上次的 ID，不再发送（quiesced 阶段不能再发 quiesce，
// 而 quiesce 不生成新 checkpoint，§5.5 第 7 条）。非空闲时返回事件流的 not_idle 错误。
func (inc *Incarnation) Quiesce(ctx context.Context, grace time.Duration) (string, error) {
	ch := make(chan string, 1)
	inc.mu.Lock()
	switch {
	case inc.stream.Phase() == protocol.PhaseQuiesced:
		id := inc.quiesced
		inc.mu.Unlock()
		return id, nil
	case inc.broken || isClosed(inc.exited):
		inc.mu.Unlock()
		return "", ErrIncarnationGone
	}
	inc.quiesceCh = ch
	inc.mu.Unlock()
	forget := func() {
		inc.mu.Lock()
		if inc.quiesceCh == ch {
			inc.quiesceCh = nil
		}
		inc.mu.Unlock()
	}
	ok, _, err := inc.sendHost(&protocol.Quiesce{HostHeader: hostHeader(protocol.TypeQuiesce), GraceMS: grace.Milliseconds()})
	if err != nil || !ok {
		forget()
		if err == nil {
			err = errors.New("runner: quiesce 未送达")
		}
		return "", err
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case id := <-ch:
		return id, nil
	case <-t.C:
		forget()
		return "", errQuiesceTimeout
	case <-inc.brokenCh:
		forget()
		return "", ErrIncarnationGone
	case <-inc.exited:
		forget()
		return "", ErrIncarnationGone
	case <-ctx.Done():
		forget()
		return "", ctx.Err()
	}
}

// Close 发送 session_close，grace 内等 closed 与进程退出；期限到（或 Worker 无法协作）则终止并等其退出，
// 返回错误说明是被终止的。
func (inc *Incarnation) Close(ctx context.Context, grace time.Duration) error {
	var err error
	inc.closeOnce.Do(func() {
		if isClosed(inc.exited) {
			return
		}
		ok, _, serr := inc.sendHost(&protocol.SessionClose{HostHeader: hostHeader(protocol.TypeSessionClose), GraceMS: grace.Milliseconds()})
		if serr == nil && ok {
			t := time.NewTimer(grace)
			defer t.Stop()
			select {
			case <-inc.exited:
				// closed 可能先于退出写出、尚未处理：等处理器读完 stdout（屏障 A 的期限内）
				select {
				case <-inc.procDone:
				case <-time.After(inc.r.opt.DrainTimeout):
				}
				if !isClosed(inc.closedCh) {
					err = errors.New("runner: Worker 未答复 closed 即退出")
				}
				return
			case <-t.C:
			case <-ctx.Done():
			}
		}
		err = errCloseTimeout
		inc.kill("")
		select {
		case <-inc.exited:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}
