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

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
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
}

// Control 是 actor 发给 runner 的控制请求（Task 6 处理）。
type Control struct {
	Kind    string // cancel | pause
	GraceMs int64
}

// Outcome 是一次执行的事实。Class 与 Retry 由 Classify（Task 6）填写。
type Outcome struct {
	Proposal *TerminalProposal
	// ResultPayload 是终态提议被保存的内容（与 Proposal.Ref 指向的 blob 字节相同）：
	// result 为 {summary, outputs[{artifact_id, version, sha256}]}，error 为 {code, message, retryable}，
	// paused 为 {checkpoint_id}。最终事务提交的必须是这份内容（规格 §5.7）。
	ResultPayload    json.RawMessage
	Exit             provider.ExitStatus
	ExitErr          error
	Diag             provider.ResourceDiag
	Class, Retry     string
	PlatformKilled   bool
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
)

// 回复中使用的稳定错误码（规格 §5.4）。store_unavailable 表示产物因存储暂时故障未能保存。
const (
	codeMissingRef       = "missing_ref"
	codeStateTooLarge    = "state_too_large"
	codeCommitInFlight   = "commit_in_flight"
	codeArtifactTooLarge = "artifact_too_large"
	codeNotRegularFile   = "not_regular_file"
	codePathInvalid      = "path_invalid"
	codeHashMismatch     = "hash_mismatch"
	codeSaveTimeout      = "save_timeout"
	codeStoreUnavailable = "store_unavailable"
)

// Options 配置 Runner。零值字段取默认值（规格 §5.10、§19）。
type Options struct {
	EventQueue       int           // stdout 行的有界队列容量，默认 256
	EventBatch       int           // AppendWorkerEvents 每批最多事件数，默认 64
	MaxAttemptEvents int64         // 每 attempt 事件合计字节，默认 64 MiB
	MaxArtifactBytes int64         // 单个产物，默认 256 MiB
	MaxStateRefBytes int64         // state_ref 指向的 blob，默认 16 MiB
	SaveTimeout      time.Duration // 单个产物的保存期限，默认 60 s
	WriteTimeout     time.Duration // stdin 每条消息的写期限（句柄支持 SetWriteDeadline 时），默认 5 s
	StderrTail       int           // 保留的 stderr 字节数，默认 64 KiB
}

func (o Options) withDefaults() Options {
	def := func(v *int64, d int64) {
		if *v <= 0 {
			*v = d
		}
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
// 启动前失败（init 不合法、out_dir 无法打开、StartExec 失败）时只填写 ExitErr。
func (r *Runner) Run(ctx context.Context, a Attempt, controls <-chan Control) Outcome {
	_ = controls // 控制消息、期限与屏障由 Task 6 加入主循环
	initLine, err := protocol.EncodeLine(protocol.HostToWorker, &a.Init)
	if err != nil {
		return Outcome{ExitErr: fmt.Errorf("runner: init 不合法: %w", err)}
	}
	dir, err := openOutDir(a.OutDir)
	if err != nil {
		return Outcome{ExitErr: fmt.Errorf("runner: 打开 out_dir: %w", err)}
	}
	defer func() { _ = dir.Close() }()
	h, err := r.starter.StartExec(ctx, a.EnvID, a.Exec)
	if err != nil {
		return Outcome{ExitErr: err}
	}
	at := &attemptRun{r: r, a: a, h: h, dir: dir, stdin: h.Stdin(), saved: map[string]pinnedOutput{},
		stderr: &tailBuffer{max: r.opt.StderrTail}}
	return at.run(ctx, initLine)
}

// attemptRun 是一次 Run 的状态。除 send 外的字段只由处理器 goroutine 访问。
type attemptRun struct {
	r      *Runner
	a      Attempt
	h      provider.ExecHandle
	dir    *outDir
	out    Outcome
	stderr *tailBuffer

	stdinMu     sync.Mutex
	stdin       io.WriteCloser
	stdinClosed bool

	stream      protocol.WorkerStream
	eventBytes  int64
	pending     []WorkerEvent // 已接受但尚未持久化的 Worker 事件
	stopped     bool          // 已出现违规：其后的行只读出丢弃
	readyCalled bool
	// unresolved 是提交结果未知的 checkpoint_id：它可能仍然生效，此时提交另一个 checkpoint
	// 会让指针顺序依赖竞争，因此以 commit_in_flight 拒绝，直到同一 ID 得到确定结果（规格 §5.5 第 4 条）。
	unresolved string
	saved      map[string]pinnedOutput // 本 attempt 已保存的产物：artifact_id → 最近一次保存的版本
	// storeFailures 是连续计入故障阈值的 Store 失败次数；阈值与终止由 Task 6 加入。
	storeFailures int
}

type pinnedOutput struct {
	Version int64
	SHA256  string
}

type exitResult struct {
	status provider.ExitStatus
	err    error
}

func (at *attemptRun) run(ctx context.Context, initLine []byte) Outcome {
	go at.stderr.drain(at.h.Stderr())
	lines := make(chan lineItem, at.r.opt.EventQueue)
	go readLines(at.h.Stdout(), lines, protocol.MaxEventBytes)
	exited := make(chan exitResult, 1)
	go func() {
		st, err := at.h.Wait()
		exited <- exitResult{st, err}
	}()
	if !at.sendLine(initLine) {
		at.closeStdin()
	}
	processed := make(chan struct{})
	go func() {
		defer close(processed)
		at.process(ctx, lines)
	}()

	done := ctx.Done()
	var exit *exitResult
	isProcessed := false
	for exit == nil || !isProcessed {
		select {
		case <-processed:
			isProcessed, processed = true, nil
		case e := <-exited:
			exit = &e
		case <-done:
			done = nil
			_ = at.h.Terminate(0)
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
	return at.out
}

// process 是唯一的事件处理器：按到达顺序（即 seq 顺序）逐条处理，产物保存完成后才处理其后的
// 事件（含 checkpoint，规格 §5.7）。队列暂时为空或批次满时持久化已接受的事件。
func (at *attemptRun) process(ctx context.Context, lines <-chan lineItem) {
	for item := range lines {
		if at.stopped {
			continue // 违规之后只读出丢弃，避免 Worker 因管道写满而阻塞
		}
		at.handle(ctx, item)
		if len(lines) == 0 || len(at.pending) >= at.r.opt.EventBatch {
			at.flush(ctx)
		}
	}
	at.flush(ctx)
}

func (at *attemptRun) violate(code string) {
	at.out.Violation = code
	at.stopped = true
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
	case *protocol.Result, *protocol.ErrorEvent, *protocol.Paused:
		at.flush(ctx)
		at.recordProposal(ctx, m)
	}
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
	}
	return 0
}

// flush 以一批原子追加已接受的事件。失败时事件保留在 pending 中，下次 flush 以同样的内容重试
// （同序号同内容的重叠是幂等的）；处理不因事件日志暂时落后而停顿。
func (at *attemptRun) flush(ctx context.Context) {
	for len(at.pending) > 0 {
		n := min(len(at.pending), at.r.opt.EventBatch)
		_, err := at.r.store.AppendWorkerEvents(ctx, at.a.AttemptID, at.pending[:n])
		at.noteStore(err)
		if err != nil {
			return
		}
		at.pending = at.pending[n:]
	}
}

// noteStore 记录 Store 调用的结果，供 Store 故障阈值（Task 6）使用。
func (at *attemptRun) noteStore(err error) {
	if persistence.CountsTowardFailureThreshold(err) {
		at.storeFailures++
	} else if err == nil {
		at.storeFailures = 0
	}
}

func hostHeader(typ string) protocol.HostHeader {
	return protocol.HostHeader{Type: typ, V: protocol.Version}
}

// ---- checkpoint ----

func (at *attemptRun) commitCheckpoint(ctx context.Context, m *protocol.Checkpoint) *protocol.CheckpointResult {
	res := &protocol.CheckpointResult{HostHeader: hostHeader(protocol.TypeCheckpointResult), CheckpointID: m.CheckpointID, Scope: m.Scope}
	if at.unresolved != "" && at.unresolved != m.CheckpointID {
		res.Status, res.Code = protocol.CheckpointRejected, codeCommitInFlight
		return res
	}
	if status, code := at.checkRefs(m); status != "" {
		res.Status, res.Code = status, code
		return res
	}
	_, err := at.r.store.CommitCheckpoint(ctx, Checkpoint{
		Scope:        Scope{Kind: protocol.ScopeTask, ID: at.a.TaskID},
		CheckpointID: m.CheckpointID,
		AttemptID:    at.a.AttemptID,
		StepID:       m.StepID,
		State:        stateOrNil(m.State),
		StateRef:     m.StateRef,
		Refs:         m.Refs,
	})
	at.noteStore(err)
	var rej *persistence.RejectedError
	switch {
	case err == nil:
		res.Status = protocol.CheckpointCommitted
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

// checkRefs 在提交前检查 refs 与 state_ref 指向的 blob 已完整保存（missing_ref）、state_ref 不超过
// 上限（state_too_large）。授权到当前 scope 由 CommitCheckpoint 在事务内检查（ref_not_authorized）。
func (at *attemptRun) checkRefs(m *protocol.Checkpoint) (status, code string) {
	refs := m.Refs
	if m.StateRef != "" {
		refs = append(append([]string(nil), m.Refs...), m.StateRef)
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

func (at *attemptRun) queryCheckpoint(ctx context.Context, m *protocol.CheckpointQuery) *protocol.CheckpointResult {
	res := &protocol.CheckpointResult{HostHeader: hostHeader(protocol.TypeCheckpointResult), CheckpointID: m.CheckpointID, Scope: m.Scope}
	_, err := at.r.store.QueryCheckpoint(ctx, Scope{Kind: m.Scope, ID: at.a.TaskID}, m.CheckpointID)
	if !errors.Is(err, persistence.ErrNotFound) {
		at.noteStore(err)
	}
	switch {
	case err == nil:
		res.Status = protocol.CheckpointCommitted
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
func (at *attemptRun) saveArtifact(ctx context.Context, m *protocol.Artifact) *protocol.ArtifactResult {
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

func (at *attemptRun) storeArtifact(ctx context.Context, m *protocol.Artifact) (blob.Ref, int64, *artifactError) {
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
	failed := func(err error) *artifactError {
		if errors.Is(sctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return &artifactError{code: codeSaveTimeout, err: err}
		}
		return &artifactError{code: codeStoreUnavailable, err: err}
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
	v, err := at.r.store.RegisterArtifact(sctx, Artifact{TaskID: at.a.TaskID, AttemptID: at.a.AttemptID, ArtifactID: m.ArtifactID,
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
// result 的 outputs 固定为本 attempt 最近一次保存的 (artifact_id, version, sha256)（规格 §5.6）；
// 本 attempt 未保存的 artifact_id 只记录 ID。
func (at *attemptRun) recordProposal(ctx context.Context, m protocol.Message) {
	var content any
	switch m := m.(type) {
	case *protocol.Result:
		outputs := make([]pinnedRef, 0, len(m.Outputs))
		for _, id := range m.Outputs {
			p := at.saved[id]
			outputs = append(outputs, pinnedRef{ArtifactID: id, Version: p.Version, SHA256: p.SHA256})
		}
		content = struct {
			Summary string      `json:"summary"`
			Outputs []pinnedRef `json:"outputs"`
		}{m.Summary, outputs}
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
	if err != nil { // 内容只含字符串、整数与布尔值
		panic(fmt.Sprintf("runner: 编码终态提议: %v", err))
	}
	sum := sha256.Sum256(payload)
	p := TerminalProposal{AttemptID: at.a.AttemptID, Kind: m.MessageType(), Ref: hex.EncodeToString(sum[:])}
	at.out.Proposal, at.out.ResultPayload = &p, payload
	if _, err := at.r.blobs.Put(ctx, bytes.NewReader(payload)); err != nil {
		return // 内容仍在 Outcome 中；记录失败由 Store 故障处理（Task 6）
	}
	rec, err := at.r.store.RecordTerminalProposal(ctx, p)
	at.noteStore(err)
	if err == nil {
		at.out.Proposal = &rec
	}
}

// ---- stdin ----

func (at *attemptRun) send(m protocol.Message) {
	line, err := protocol.EncodeLine(protocol.HostToWorker, m)
	if err != nil { // 回复由本包构造，编码失败是程序错误
		panic(fmt.Sprintf("runner: 编码 %s: %v", m.MessageType(), err))
	}
	at.sendLine(line)
}

// sendLine 写一行到 stdin（规格 §5.5 第 8 条）：0 字节写出视为未送达（回复类消息由 Worker 重试或
// 查询）；部分写出意味着控制流损坏，关闭 stdin 并终止。返回是否完整写出。
func (at *attemptRun) sendLine(line []byte) bool {
	at.stdinMu.Lock()
	defer at.stdinMu.Unlock()
	if at.stdinClosed {
		return false
	}
	type deadliner interface{ SetWriteDeadline(time.Time) error }
	d, hasDeadline := at.stdin.(deadliner)
	if hasDeadline {
		_ = d.SetWriteDeadline(time.Now().Add(at.r.opt.WriteTimeout))
		defer func() { _ = d.SetWriteDeadline(time.Time{}) }()
	}
	n, err := at.stdin.Write(append(line[:len(line):len(line)], '\n'))
	if err == nil {
		return true
	}
	if n > 0 {
		at.stdinClosed = true
		_ = at.stdin.Close()
		_ = at.h.Terminate(0)
	}
	return false
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

// readLines 把 stdout 按行读入有界队列，直到 EOF 或读错误。超过 max 字节的行不缓存，只报告 tooLarge
// 并读到行尾丢弃；末尾没有换行的半行丢弃。
func readLines(r io.Reader, out chan<- lineItem, max int) {
	defer close(out)
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
		if over {
			out <- lineItem{tooLarge: true}
		} else {
			out <- lineItem{line: bytes.Clone(buf[:len(buf)-1])}
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
