package call

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
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// ---- 内存 Store：语义对齐 internal/persistence/postgres 的 Gateway 事务用例，每次变更后断言 I3 ----

type fakeTry struct {
	rec            TryRecord
	amount, actual int64
	rstate         string // held | settled | released | charged_unknown
}

type fakeStore struct {
	t          *testing.T
	mu         sync.Mutex
	revoked    map[string]bool   // attempt_id → 访问已撤销
	desired    map[string]string // task_id → task_control.desired
	budgets    map[string]*Budget
	calls      map[callKey]*CallRecord
	tries      map[callKey][]*fakeTry
	nRes       int
	begun      chan string      // 每次新登记调用时发送 call_id
	settled    chan string      // 每次 SettleTry 改变账本后发送 call_id
	reserveErr error            // 下一次 ReserveTry 返回的存储故障（一次性）
	scoped     map[callKey]bool // (task_id, sha256) → 以其他来源登记到 scope_blobs(task)
	// sub-run（Plan 14 Task 5）：键为 (task_id, subrun_id)；默认 open、层账本不设上限。
	subClosed    map[callKey]bool
	subExhausted map[callKey]bool // ReserveTry 返回 subrun_budget_exhausted
	subBudgets   map[callKey]SubrunBudget
}

func newFakeStore(t *testing.T) *fakeStore {
	return &fakeStore{t: t, revoked: map[string]bool{}, desired: map[string]string{}, budgets: map[string]*Budget{},
		calls: map[callKey]*CallRecord{}, tries: map[callKey][]*fakeTry{}, begun: make(chan string, 64), settled: make(chan string, 64),
		scoped: map[callKey]bool{}, subClosed: map[callKey]bool{}, subExhausted: map[callKey]bool{}, subBudgets: map[callKey]SubrunBudget{}}
}

func (s *fakeStore) budget(taskID string) *Budget {
	b := s.budgets[taskID]
	if b == nil {
		b = &Budget{LimitMicro: 1_000_000}
		s.budgets[taskID] = b
	}
	return b
}

func (s *fakeStore) setLimit(taskID string, limit int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.budget(taskID).LimitMicro = limit
}

// checkI3 断言 I3：reserved = Σ held，unknown = Σ charged_unknown，spent = Σ settled 的实际费用；每笔 reservation 恰在一个桶中。
func (s *fakeStore) checkI3() {
	for task, b := range s.budgets {
		var held, unk, spent int64
		for k, ts := range s.tries {
			if k.taskID != task {
				continue
			}
			for _, tr := range ts {
				switch tr.rstate {
				case "held":
					held += tr.amount
				case "charged_unknown":
					unk += tr.amount
				case "settled":
					spent += tr.actual
				case "released":
				default:
					s.t.Errorf("违反 I3：reservation 状态 %q", tr.rstate)
				}
			}
		}
		if b.ReservedMicro != held || b.UnknownMicro != unk || b.SpentMicro != spent {
			s.t.Errorf("违反 I3：任务 %s 账本 %+v，held=%d unknown=%d spent=%d", task, *b, held, unk, spent)
		}
	}
}

func rejected(code string) error { return &persistence.RejectedError{Code: code, Detail: "fake"} }

func (s *fakeStore) facts(taskID, attemptID, subrunID string) AccessFacts {
	d := s.desired[taskID]
	if d == "" {
		d = "run"
	}
	return AccessFacts{TaskID: taskID, AttemptID: attemptID, Active: !s.revoked[attemptID], Current: true, Desired: d,
		SubrunID: subrunID, SubrunOpen: subrunID != "" && !s.subClosed[callKey{taskID, subrunID}]}
}

func (s *fakeStore) CheckAccess(_ context.Context, taskID, attemptID, subrunID string) (AccessFacts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.facts(taskID, attemptID, subrunID), nil
}

// closeSubrun 模拟宿主把 sub-run 置为 cancel_requested（之后的访问复查为 subrun_closed）。
func (s *fakeStore) closeSubrun(taskID, subrunID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subClosed[callKey{taskID, subrunID}] = true
}

// LoadSubrunBudget：fake 只返回测试登记的 sub-run 层账本（两层记账的真实语义见 postgres 测试）。
func (s *fakeStore) LoadSubrunBudget(_ context.Context, taskID, subrunID string) (SubrunBudget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.subBudgets[callKey{taskID, subrunID}]
	if !ok {
		return SubrunBudget{}, persistence.ErrNotFound
	}
	return b, nil
}

func (s *fakeStore) BeginCall(_ context.Context, r BeginCallRequest) (BeginCallResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if code := admit(s.facts(r.TaskID, r.AttemptID, r.SubrunID)); code != "" {
		return BeginCallResult{}, rejected(code)
	}
	k := callKey{r.TaskID, r.CallID}
	rec := s.calls[k]
	if rec != nil && rec.SubrunID != r.SubrunID { // 与 postgres 相同：归属不同的同 ID 调用是指纹分歧
		return BeginCallResult{}, rejected(persistence.CodeFingerprintMismatch)
	}
	now := time.Now()
	b := s.budget(r.TaskID)
	tool := r.Endpoint == "/v1/search" || r.Endpoint == "/v1/fetch"
	tb := func() *ToolBudget { // 与 postgres 相同：搜索与抓取在有上限时带额度，新登记时计数
		if !tool {
			return nil
		}
		return b.ToolBudget()
	}
	if rec == nil {
		if tool && b.ToolCallLimit != nil && b.ToolCallsUsed >= *b.ToolCallLimit {
			return BeginCallResult{ToolBudget: tb()}, rejected(persistence.CodeToolBudgetExhausted)
		}
		if tool {
			b.ToolCallsUsed++
		}
		rec = &CallRecord{TaskID: r.TaskID, CallID: r.CallID, Fingerprint: r.Fingerprint, Endpoint: r.Endpoint,
			State: StateResolving, Source: "upstream", CreatedAt: now, DeadlineAt: now.Add(r.Deadline),
			FirstAttemptID: r.AttemptID, SupersedesCallID: r.SupersedesCallID, SupersedeReason: r.SupersedeReason,
			ResolvingSince: &now, Model: r.Model, SubrunID: r.SubrunID}
		s.calls[k] = rec
		select {
		case s.begun <- r.CallID:
		default:
		}
		return BeginCallResult{Record: *rec, ToolBudget: tb()}, nil
	}
	if rec.State == StateResolving && rec.ResolvingSince == nil && rec.TriesUsed == 0 && rec.Fingerprint == r.Fingerprint {
		rec.ResolvingSince = &now
		return BeginCallResult{Record: *rec, ToolBudget: tb()}, nil
	}
	return BeginCallResult{Record: *rec, Existing: true, ToolBudget: tb()}, nil
}

// setToolLimit 设置任务的工具调用上限（budgets.tool_call_limit）。
func (s *fakeStore) setToolLimit(taskID string, limit int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.budget(taskID).ToolCallLimit = &limit
}

func (s *fakeStore) ReserveTry(_ context.Context, r ReserveTryRequest) (Try, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.checkI3()
	if err := s.reserveErr; err != nil {
		s.reserveErr = nil
		return Try{}, err
	}
	k := callKey{r.TaskID, r.CallID}
	rec := s.calls[k]
	if rec == nil {
		return Try{}, persistence.ErrNotFound
	}
	if ts := s.tries[k]; len(ts) > 0 {
		last := ts[len(ts)-1]
		if last.rstate == "held" {
			if last.rec.AttemptID == r.AttemptID && last.rec.EnvID == r.EnvID && last.amount == r.EstimateMicro {
				return Try{TaskID: r.TaskID, CallID: r.CallID, TryNo: last.rec.TryNo, ReservationID: last.rec.ReservationID,
					AttemptID: last.rec.AttemptID}, nil
			}
			return Try{}, rejected(persistence.CodeCallInProgress)
		}
	}
	if code := admit(s.facts(r.TaskID, r.AttemptID, r.SubrunID)); code != "" {
		return Try{}, rejected(code)
	}
	b := s.budget(r.TaskID)
	switch {
	case rec.SubrunID != r.SubrunID:
		return Try{}, rejected(persistence.CodeFingerprintMismatch)
	case r.SubrunID != "" && s.subExhausted[callKey{r.TaskID, r.SubrunID}]:
		return Try{}, rejected(persistence.CodeSubrunBudgetExhausted)
	case rec.State == StateCompleted:
		return Try{}, persistence.ErrConflict
	case !time.Now().Before(rec.DeadlineAt):
		return Try{}, rejected(persistence.CodeCallDeadlineExceeded)
	case rec.TriesUsed >= r.MaxTries:
		return Try{}, rejected(persistence.CodeTriesExhausted)
	case b.Available() <= 0:
		return Try{}, rejected(persistence.CodeBudgetExhausted)
	case b.Available() < r.EstimateMicro:
		return Try{}, rejected(persistence.CodeBudgetInsufficient)
	}
	s.nRes++
	tryNo := rec.TriesUsed + 1
	ft := &fakeTry{rec: TryRecord{TryNo: tryNo, AttemptID: r.AttemptID, EnvID: r.EnvID, State: "in_flight",
		ReservationID: fmt.Sprintf("rsv-%d", s.nRes)}, amount: r.EstimateMicro, rstate: "held"}
	s.tries[k] = append(s.tries[k], ft)
	b.ReservedMicro += r.EstimateMicro
	rec.State, rec.TriesUsed, rec.ResolvingSince = StateInFlight, tryNo, nil
	return Try{TaskID: r.TaskID, CallID: r.CallID, TryNo: tryNo, ReservationID: ft.rec.ReservationID, AttemptID: r.AttemptID}, nil
}

func (s *fakeStore) SettleTry(_ context.Context, st Settlement) (CallRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.checkI3()
	t := st.Try
	k := callKey{t.TaskID, t.CallID}
	rec := s.calls[k]
	ts := s.tries[k]
	if rec == nil || t.TryNo < 1 || t.TryNo > len(ts) {
		return CallRecord{}, persistence.ErrNotFound
	}
	ft := ts[t.TryNo-1]
	if ft.rec.ReservationID != t.ReservationID {
		return CallRecord{}, persistence.ErrConflict
	}
	if ft.rstate != "held" {
		return *rec, nil // 幂等：迟到的结算不改变记账
	}
	if st.Outcome == "ok" && len(st.ResultSHA256) != 64 {
		return CallRecord{}, persistence.ErrInvalid
	}
	b := s.budget(t.TaskID)
	b.ReservedMicro -= ft.amount
	ft.rec.State, ft.rec.Outcome, ft.rec.LatencyMs, ft.rec.Error = "settled", st.Outcome, st.LatencyMs, st.Error
	switch st.Outcome {
	case "ok":
		ft.rstate, ft.actual, ft.rec.CostMicro = "settled", st.ActualMicro, st.ActualMicro
		b.SpentMicro += st.ActualMicro
		rec.State, rec.ResultRef = StateCompleted, st.ResultSHA256
		rec.CostCharged += st.ActualMicro
	case "retryable":
		ft.rstate = "released"
	case "fatal":
		ft.rstate = "released"
		rec.State, rec.FailReason = StateFailed, st.Error
	case "unknown":
		ft.rstate = "charged_unknown"
		b.UnknownMicro += ft.amount
		rec.State, rec.PossibleExternalDuplicate = StateUnknown, true
	default:
		return CallRecord{}, persistence.ErrInvalid
	}
	if st.UpstreamRequestID != "" {
		rec.UpstreamRequestID = st.UpstreamRequestID
	}
	select {
	case s.settled <- t.CallID:
	default:
	}
	return *rec, nil
}

func (s *fakeStore) CompleteFromCache(_ context.Context, r CacheCompletion) (CallRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.checkI3()
	if code := admit(s.facts(r.TaskID, r.AttemptID, r.SubrunID)); code != "" {
		return CallRecord{}, rejected(code)
	}
	if rec := s.calls[callKey{r.TaskID, r.CallID}]; rec != nil && rec.SubrunID != r.SubrunID {
		return CallRecord{}, rejected(persistence.CodeFingerprintMismatch)
	}
	src := r.Source
	if src == "" {
		src = SourceCache
	}
	if src != SourceCache && src != SourceCoalesced {
		return CallRecord{}, persistence.ErrInvalid
	}
	rec := s.calls[callKey{r.TaskID, r.CallID}]
	switch {
	case rec == nil:
		return CallRecord{}, persistence.ErrNotFound
	case rec.State == StateCompleted && rec.Source == src && rec.ResultRef == r.ResultSHA256:
		return *rec, nil
	case rec.State != StateResolving || rec.ResolvingSince == nil || rec.TriesUsed != 0:
		return CallRecord{}, persistence.ErrConflict
	case !time.Now().Before(rec.DeadlineAt):
		return CallRecord{}, rejected(persistence.CodeCallDeadlineExceeded)
	}
	rec.State, rec.Source, rec.ResultRef, rec.ResolvingSince = StateCompleted, src, r.ResultSHA256, nil
	return *rec, nil
}

func (s *fakeStore) FailCall(_ context.Context, taskID, callID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.checkI3()
	rec := s.calls[callKey{taskID, callID}]
	if rec == nil {
		return persistence.ErrNotFound
	}
	switch rec.State {
	case StateFailed:
		return nil
	case StateCompleted:
		return persistence.ErrConflict
	}
	for _, ft := range s.tries[callKey{taskID, callID}] {
		if ft.rstate == "held" {
			return rejected(persistence.CodeCallInProgress)
		}
	}
	rec.State, rec.FailReason, rec.ResolvingSince = StateFailed, reason, nil
	return nil
}

func (s *fakeStore) ResetResolving(context.Context) (int, error) { return 0, nil }

func (s *fakeStore) LoadBudget(_ context.Context, taskID string) (Budget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.budget(taskID), nil
}

func (s *fakeStore) LoadCall(_ context.Context, taskID, callID string) (CallRecord, []TryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := callKey{taskID, callID}
	rec := s.calls[k]
	if rec == nil {
		return CallRecord{}, nil, persistence.ErrNotFound
	}
	var out []TryRecord
	for _, ft := range s.tries[k] {
		out = append(out, ft.rec)
	}
	return *rec, out, nil
}

// BlobAuthorized 模拟 scope_blobs(task)：本任务 completed 调用的结果，加上测试以 scope 登记的其他 blob
// （例如产物与 checkpoint 引用）。
func (s *fakeStore) BlobAuthorized(_ context.Context, taskID, sha string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scoped[callKey{taskID, sha}] {
		return true, nil
	}
	for k, r := range s.calls {
		if k.taskID == taskID && r.State == StateCompleted && r.ResultRef == sha {
			return true, nil
		}
	}
	return false, nil
}

func (s *fakeStore) scope(taskID, sha string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scoped[callKey{taskID, sha}] = true
}

func (s *fakeStore) ListCalls(_ context.Context, taskID string) ([]CallRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []CallRecord
	for k, r := range s.calls {
		if k.taskID == taskID {
			out = append(out, *r)
		}
	}
	return out, nil
}

// ---- 内存 blob 与宿主事件 ----

type fakeBlobs struct {
	mu      sync.Mutex
	m       map[string][]byte
	failPut bool
}

func (b *fakeBlobs) Put(_ context.Context, r io.Reader) (blob.Ref, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return blob.Ref{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failPut {
		return blob.Ref{}, errors.New("磁盘已满")
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	b.m[sha] = data
	return blob.Ref{SHA256: sha, Size: int64(len(data))}, nil
}

func (b *fakeBlobs) Open(sha string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.m[sha]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type fakeEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *fakeEvents) ReplayDivergence(_ context.Context, taskID, attemptID, callID, detail string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, taskID+"|"+attemptID+"|"+callID+"|"+detail)
	return nil
}

func (e *fakeEvents) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

// ---- 按脚本返回结果的 adapter ----

type step struct {
	body       string
	usage      upstream.Usage
	err        *upstream.Error
	retryAfter time.Duration
	gate       chan struct{} // 关闭前阻塞（ctx 结束则按 sent 归类）
	hold       bool          // 与 gate 合用：忽略 ctx，只在 gate 关闭时返回
	hang       bool          // 阻塞至 ctx 结束
	sent       bool          // ctx 结束时请求是否已发出：是 → unknown，否 → retryable
}

type fakeAdapter struct {
	kind     upstream.Kind
	provider string
	est      int64

	mu       sync.Mutex
	script   []step
	calls    int
	inflight int
	maxIn    int
	ctxErrs  []error       // 每次 try 结束时上下文的状态
	entered  chan struct{} // 每次 Do 开始时发送
}

func newAdapter(provider string, script ...step) *fakeAdapter {
	return &fakeAdapter{kind: upstream.KindChat, provider: provider, est: 100, script: script, entered: make(chan struct{}, 64)}
}

func (a *fakeAdapter) Kind() upstream.Kind { return a.kind }
func (a *fakeAdapter) Provider() string    { return a.provider }
func (a *fakeAdapter) Version() string     { return "fake/1" }

func (a *fakeAdapter) Resolve(body []byte) ([]byte, map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil || obj == nil {
		return nil, nil, &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 400, Code: upstream.CodeInvalidRequest}
	}
	defaults := map[string]any{}
	if _, ok := obj["max_tokens"]; !ok {
		obj["max_tokens"] = 64
		defaults["max_tokens"] = 64
	}
	if _, ok := obj["model"]; !ok {
		obj["model"] = "m1"
	}
	out, _ := json.Marshal(obj)
	return out, defaults, nil
}

func (a *fakeAdapter) Estimate([]byte) (int64, error) { return a.est, nil }

func ctxOutcome(sent bool) *upstream.Error {
	if sent {
		return &upstream.Error{Outcome: upstream.OutcomeUnknown, Status: 502, Code: upstream.CodeUpstreamUnconfirmed}
	}
	return &upstream.Error{Outcome: upstream.OutcomeRetryable, Status: 502, Code: upstream.CodeUpstreamUnreachable}
}

func (a *fakeAdapter) Do(ctx context.Context, _ []byte) (upstream.Response, *upstream.Error) {
	a.mu.Lock()
	s := step{body: `{"answer":"ok"}`}
	if a.calls < len(a.script) {
		s = a.script[a.calls]
	}
	a.calls++
	a.inflight++
	a.maxIn = max(a.maxIn, a.inflight)
	a.mu.Unlock()
	a.entered <- struct{}{}
	defer func() {
		a.mu.Lock()
		a.inflight--
		a.ctxErrs = append(a.ctxErrs, ctx.Err())
		a.mu.Unlock()
	}()
	if s.gate != nil && s.hold {
		<-s.gate // 不理会 ctx：测试显式释放前一直占用槽位
	} else if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return upstream.Response{}, ctxOutcome(s.sent)
		}
	}
	if s.hang {
		<-ctx.Done()
		return upstream.Response{}, ctxOutcome(s.sent)
	}
	if s.err != nil {
		return upstream.Response{RetryAfter: s.retryAfter}, s.err
	}
	return upstream.Response{Body: []byte(s.body), Usage: s.usage, UpstreamRequestID: "req-1"}, nil
}

func (a *fakeAdapter) stats() (calls, maxIn int, ctxErrs []error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls, a.maxIn, append([]error(nil), a.ctxErrs...)
}

func rateLimited() *upstream.Error {
	return &upstream.Error{Outcome: upstream.OutcomeRetryable, Status: 429, Code: upstream.CodeUpstreamRateLimited}
}

func unavailable() *upstream.Error {
	return &upstream.Error{Outcome: upstream.OutcomeRetryable, Status: 503, Code: upstream.CodeUpstreamUnavailable}
}

// ---- 装配 ----

type harness struct {
	c           *Coordinator
	store       *fakeStore
	ad          *fakeAdapter
	blobs       *fakeBlobs
	events      *fakeEvents
	logs        *syncBuffer
	chatPricing map[string]upstream.Pricing
	cache       CacheSource // 非 nil 时装配进 Coordinator（newCacheHarness）
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func testLimits() Limits {
	return Limits{CallDeadline: 5 * time.Second, ModelCallDeadline: 5 * time.Second, BackoffBase: time.Millisecond, BackoffMax: 4 * time.Millisecond}
}

func newHarness(t *testing.T, lim Limits, ad *fakeAdapter) *harness {
	t.Helper()
	h := &harness{store: newFakeStore(t), ad: ad, blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{},
		logs: &syncBuffer{}}
	h.c = h.newCoordinator(t, lim)
	return h
}

func (h *harness) newCoordinator(t *testing.T, lim Limits) *Coordinator {
	t.Helper()
	c, err := New(Config{
		Store: h.store, Adapters: []upstream.Adapter{h.ad}, Blobs: h.blobs, Events: h.events, Limits: lim,
		Pricing:     map[upstream.Kind]upstream.Pricing{upstream.KindChat: {InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}},
		ChatPricing: h.chatPricing,
		Logger:      slog.New(slog.NewJSONHandler(h.logs, nil)),
		Cache:       h.cache,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

const chatBody = `{"messages":[{"role":"user","content":"secret prompt text"}],"temperature":0.5}`

func inv(callID, body string) Invoke {
	return Invoke{TaskID: "t1", AttemptID: "a1", EnvID: "e1", CallID: callID, Kind: upstream.KindChat, Body: []byte(body)}
}

func (h *harness) invoke(t *testing.T, in Invoke) Result {
	t.Helper()
	r, err := h.c.Invoke(context.Background(), in)
	if err != nil {
		t.Fatalf("Invoke(%s)：%v", in.CallID, err)
	}
	return r
}

func (h *harness) call(t *testing.T, taskID, callID string) (CallRecord, []TryRecord) {
	t.Helper()
	rec, tries, err := h.store.LoadCall(context.Background(), taskID, callID)
	if err != nil {
		t.Fatal(err)
	}
	return rec, tries
}

func (h *harness) budget(t *testing.T, taskID string) Budget {
	t.Helper()
	b, _ := h.store.LoadBudget(context.Background(), taskID)
	return b
}

func waitEntered(t *testing.T, a *fakeAdapter) {
	t.Helper()
	select {
	case <-a.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("上游 try 没有开始")
	}
}

// waitBegun 等待 Tx1 登记指定调用。
func waitBegun(t *testing.T, s *fakeStore, callID string) {
	t.Helper()
	for {
		select {
		case id := <-s.begun:
			if id == callID {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("调用 %s 没有登记", callID)
		}
	}
}

// ---- 指纹 ----

// 指纹对请求体字段顺序与空白不敏感，对 applied_defaults、模型与端点敏感；重复属性名被拒绝。
func TestFingerprint(t *testing.T) {
	fp := func(defaults map[string]any, model, body string) string {
		t.Helper()
		s, err := Fingerprint("/v1/chat/completions", "fake/1", "p", model, defaults, "", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a := fp(map[string]any{"max_tokens": 64}, "m1", `{"a":1,"b":[1,2]}`)
	if b := fp(map[string]any{"max_tokens": 64}, "m1", ` { "b" : [1, 2], "a" : 1.0 } `); a != b {
		t.Fatal("字段顺序或数字写法改变了指纹")
	}
	if b := fp(map[string]any{"max_tokens": 128}, "m1", `{"a":1,"b":[1,2]}`); a == b {
		t.Fatal("applied_defaults 不同但指纹相同")
	}
	if b := fp(map[string]any{"max_tokens": 64}, "m2", `{"a":1,"b":[1,2]}`); a == b {
		t.Fatal("模型不同但指纹相同")
	}
	if _, err := Fingerprint("/v1/chat/completions", "fake/1", "p", "m1", nil, "", []byte(`{"a":1,"a":2}`)); !errors.Is(err, jcs.ErrDuplicateKey) {
		t.Fatalf("重复属性名：%v", err)
	}
	// 缓存指令计入指纹（§11.4）：no-cache 与无指令不同。
	nc, err := Fingerprint("/v1/chat/completions", "fake/1", "p", "m1", map[string]any{"max_tokens": 64}, CacheDirectiveNoCache, []byte(`{"a":1,"b":[1,2]}`))
	if err != nil || nc == a {
		t.Fatalf("no-cache 的指纹 %s / %v，不应等于无指令的 %s", nc, err, a)
	}
}

// 请求体含重复属性名 → 400 duplicate_json_key，不登记调用。
func TestInvokeRejectsDuplicateKey(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	r := h.invoke(t, inv("c1", `{"messages":[],"messages":[]}`))
	if r.Status != 400 || r.Code != CodeDuplicateJSONKey {
		t.Fatalf("得到 %+v", r)
	}
	if _, _, err := h.store.LoadCall(context.Background(), "t1", "c1"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不应登记调用：%v", err)
	}
}

// ---- journal：重放、分歧、进行中 ----

// 首次调用按用量结算并保存结果 blob；同 ID 同指纹的重复请求返回同一 blob 且 Replayed，不再访问上游。
func TestReplayReturnsSameBlob(t *testing.T) {
	ad := newAdapter("p", step{body: `{"answer":42}`, usage: upstream.Usage{InputTokens: 10, OutputTokens: 20}})
	h := newHarness(t, testLimits(), ad)
	first := h.invoke(t, inv("c1", chatBody))
	if first.Status != 200 || first.Replayed || string(first.Body) != `{"answer":42}` || first.BlobSHA256 == "" {
		t.Fatalf("首次结果 %+v", first)
	}
	again := h.invoke(t, inv("c1", `{"temperature":0.5,"messages":[{"content":"secret prompt text","role":"user"}]}`))
	if !again.Replayed || again.BlobSHA256 != first.BlobSHA256 || !bytes.Equal(again.Body, first.Body) {
		t.Fatalf("重放结果 %+v", again)
	}
	if calls, _, _ := ad.stats(); calls != 1 {
		t.Fatalf("上游被调用 %d 次", calls)
	}
	rec, _ := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.ResultRef != first.BlobSHA256 {
		t.Fatalf("调用记录 %+v", rec)
	}
	// 实际费用 = 10 × 1 + 20 × 2 微美元（按用量，而非估算 100）。
	if b := h.budget(t, "t1"); b.SpentMicro != 50 || b.ReservedMicro != 0 {
		t.Fatalf("账本 %+v", b)
	}
}

// 实际费用按解析后的模型取价格表（M2 按调用选择模型）：有专属价格的模型按它结算，其余模型用类别价格表。
func TestCostUsesPerModelPricing(t *testing.T) {
	usage := upstream.Usage{InputTokens: 10, OutputTokens: 20}
	h := newHarness(t, testLimits(), newAdapter("p", step{body: `{}`, usage: usage}, step{body: `{}`, usage: usage}))
	h.chatPricing = map[string]upstream.Pricing{"big": {InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 5_000_000}}
	h.c = h.newCoordinator(t, testLimits())
	if r := h.invoke(t, inv("c1", `{"model":"big","messages":[{"role":"user","content":"x"}]}`)); r.Status != 200 {
		t.Fatalf("c1 %+v", r)
	}
	// 10 × 3 + 20 × 5 = 130
	if b := h.budget(t, "t1"); b.SpentMicro != 130 {
		t.Fatalf("按模型 big 的价格结算：%+v", b)
	}
	if r := h.invoke(t, inv("c2", `{"messages":[{"role":"user","content":"x"}]}`)); r.Status != 200 {
		t.Fatalf("c2 %+v", r)
	}
	// 默认模型 m1 无专属价格 → 类别价格表：10 × 1 + 20 × 2 = 50
	if b := h.budget(t, "t1"); b.SpentMicro != 180 {
		t.Fatalf("默认模型按类别价格结算：%+v", b)
	}
	// journal 记录解析后的模型（请求指定的，或 adapter 补的默认值），供 inspect 展示。
	for callID, want := range map[string]string{"c1": "big", "c2": "m1"} {
		if rec, _ := h.call(t, "t1", callID); rec.Model != want {
			t.Fatalf("%s 的 journal 模型 = %q，期望 %q", callID, rec.Model, want)
		}
	}
}

// 同 ID 不同指纹 → 409 fingerprint_mismatch，并写 host 事件 replay_divergence。
func TestFingerprintMismatch(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.invoke(t, inv("c1", chatBody))
	r := h.invoke(t, inv("c1", `{"messages":[{"role":"user","content":"other"}]}`))
	if r.Status != 409 || r.Code != persistence.CodeFingerprintMismatch {
		t.Fatalf("得到 %+v", r)
	}
	ev := h.events.list()
	if len(ev) != 1 || !strings.HasPrefix(ev[0], "t1|a1|c1|") {
		t.Fatalf("事件 %v", ev)
	}
}

// 执行中的调用：同进程的重复请求与另一进程（共享数据库）的请求都得到 call_in_progress，上游只执行一次。
func TestInFlightCallInProgress(t *testing.T) {
	gate := make(chan struct{})
	ad := newAdapter("p", step{gate: gate, body: `{"answer":1}`})
	h := newHarness(t, testLimits(), ad)
	done := make(chan Result, 1)
	go func() { r, _ := h.c.Invoke(context.Background(), inv("c1", chatBody)); done <- r }()
	waitEntered(t, ad)
	if r := h.invoke(t, inv("c1", chatBody)); r.Code != persistence.CodeCallInProgress || r.Status != 409 {
		t.Fatalf("同进程：%+v", r)
	}
	other := h.newCoordinator(t, testLimits())
	if r, err := other.Invoke(context.Background(), inv("c1", chatBody)); err != nil || r.Code != persistence.CodeCallInProgress {
		t.Fatalf("另一进程：%+v %v", r, err)
	}
	close(gate)
	if r := <-done; r.Status != 200 {
		t.Fatalf("首个请求 %+v", r)
	}
	if calls, _, _ := ad.stats(); calls != 1 {
		t.Fatalf("上游被调用 %d 次", calls)
	}
}

// ---- 重试、上限与期限 ----

// 429 ×3：3 次 try 后 429 tries_exhausted，调用 failed；之后同 ID（即使带 Retry）返回持久化失败，不再访问上游。
func TestRetryableExhaustsTries(t *testing.T) {
	ad := newAdapter("p", step{err: rateLimited()}, step{err: rateLimited()}, step{err: rateLimited()})
	h := newHarness(t, testLimits(), ad)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 429 || r.Code != persistence.CodeTriesExhausted {
		t.Fatalf("得到 %+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateFailed || rec.FailReason != persistence.CodeTriesExhausted || rec.TriesUsed != 3 || len(tries) != 3 {
		t.Fatalf("调用记录 %+v，try %d", rec, len(tries))
	}
	if b := h.budget(t, "t1"); b.ReservedMicro != 0 || b.SpentMicro != 0 || b.UnknownMicro != 0 {
		t.Fatalf("账本 %+v", b)
	}
	in := inv("c1", chatBody)
	in.Retry = true
	if r := h.invoke(t, in); r.Status != 429 || r.Code != persistence.CodeTriesExhausted {
		t.Fatalf("重复请求 %+v", r)
	}
	if calls, _, _ := ad.stats(); calls != 3 {
		t.Fatalf("上游被调用 %d 次", calls)
	}
}

// unknown 只计一次：unknown 之后的可重试失败不再转 unknown；同一 try 的迟到结算不改变账本；累计仍 ≤ 3 次 try。
func TestUnknownChargedOnce(t *testing.T) {
	unconfirmed := &upstream.Error{Outcome: upstream.OutcomeUnknown, Status: 502, Code: upstream.CodeUpstreamUnconfirmed}
	ad := newAdapter("p", step{err: unconfirmed}, step{err: rateLimited()}, step{err: unavailable()})
	h := newHarness(t, testLimits(), ad)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Code != persistence.CodeTriesExhausted {
		t.Fatalf("得到 %+v", r)
	}
	b := h.budget(t, "t1")
	if b.UnknownMicro != 100 || b.ReservedMicro != 0 {
		t.Fatalf("账本 %+v", b)
	}
	rec, tries := h.call(t, "t1", "c1")
	if !rec.PossibleExternalDuplicate || len(tries) != 3 || tries[0].Outcome != "unknown" {
		t.Fatalf("调用记录 %+v %+v", rec, tries)
	}
	// 第 1 次 try 的迟到响应：结算按 reservation 状态幂等，只留诊断。
	late := Settlement{Try: Try{TaskID: "t1", CallID: "c1", TryNo: 1, ReservationID: tries[0].ReservationID},
		Outcome: "ok", ActualMicro: 7, ResultSHA256: strings.Repeat("a", 64)}
	if _, err := h.store.SettleTry(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if b2 := h.budget(t, "t1"); b2 != b {
		t.Fatalf("迟到结算改变了账本：%+v → %+v", b, b2)
	}
}

// 期限：剩余期限不足以完成退避（遵从 Retry-After）时不再新建 try，504 call_deadline_exceeded，调用 failed。
func TestDeadlineStopsNewTries(t *testing.T) {
	lim := testLimits()
	lim.ModelCallDeadline = 300 * time.Millisecond
	ad := newAdapter("p", step{err: rateLimited(), retryAfter: time.Second})
	h := newHarness(t, lim, ad)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 504 || r.Code != persistence.CodeCallDeadlineExceeded {
		t.Fatalf("得到 %+v", r)
	}
	if calls, _, _ := ad.stats(); calls != 1 {
		t.Fatalf("上游被调用 %d 次", calls)
	}
	rec, _ := h.call(t, "t1", "c1")
	if rec.State != StateFailed || rec.FailReason != persistence.CodeCallDeadlineExceeded {
		t.Fatalf("调用记录 %+v", rec)
	}
}

// 期限到达时在途 try 被中止：已发出的请求按 unknown 结算（possible_external_duplicate），不再新建 try。
func TestDeadlineAbortsInFlightTry(t *testing.T) {
	lim := testLimits()
	lim.ModelCallDeadline = 200 * time.Millisecond
	ad := newAdapter("p", step{hang: true, sent: true})
	h := newHarness(t, lim, ad)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 504 || r.Code != persistence.CodeCallDeadlineExceeded {
		t.Fatalf("得到 %+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateUnknown || !rec.PossibleExternalDuplicate || len(tries) != 1 {
		t.Fatalf("调用记录 %+v，try %d", rec, len(tries))
	}
	if b := h.budget(t, "t1"); b.UnknownMicro != 100 || b.ReservedMicro != 0 {
		t.Fatalf("账本 %+v", b)
	}
}

// failed 调用：可重试类别（Tx2 预算不足）只有带 Retry 时才新建 try；不带 Retry 返回持久化失败。
func TestFailedRetryOnlyWithHeader(t *testing.T) {
	fatal := &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 401, Code: upstream.CodeUpstreamRejected}
	ad := newAdapter("p", step{body: `{"answer":1}`}, step{err: fatal})
	h := newHarness(t, testLimits(), ad)
	h.store.setLimit("t1", 50) // 可用 50 < 估算 100
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 402 || r.Code != persistence.CodeBudgetInsufficient {
		t.Fatalf("得到 %+v", r)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.State != StateFailed || rec.TriesUsed != 0 {
		t.Fatalf("调用记录 %+v", rec)
	}
	h.store.setLimit("t1", 1_000)
	if r := h.invoke(t, inv("c1", chatBody)); r.Code != persistence.CodeBudgetInsufficient {
		t.Fatalf("不带 Retry：%+v", r)
	}
	in := inv("c1", chatBody)
	in.Retry = true
	if r := h.invoke(t, in); r.Status != 200 {
		t.Fatalf("带 Retry：%+v", r)
	}
	// fatal（上游明确拒绝）不属于可重试类别：带 Retry 也返回持久化失败。
	if r := h.invoke(t, inv("c2", chatBody)); r.Status != 401 || r.Code != upstream.CodeUpstreamRejected {
		t.Fatalf("fatal：%+v", r)
	}
	in = inv("c2", chatBody)
	in.Retry = true
	if r := h.invoke(t, in); r.Code != upstream.CodeUpstreamRejected {
		t.Fatalf("fatal 带 Retry：%+v", r)
	}
	if calls, _, _ := ad.stats(); calls != 2 {
		t.Fatalf("上游被调用 %d 次", calls)
	}
}

// 访问已撤销的 attempt 在 Tx1 被拒（403），不登记调用。
func TestRevokedAccessRejected(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.store.mu.Lock()
	h.store.revoked["a1"] = true
	h.store.mu.Unlock()
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 403 || r.Code != persistence.CodeAccessRevoked {
		t.Fatalf("得到 %+v", r)
	}
}

// ---- 离开原因 ----

// Worker 取消请求后 try 继续（上下文未被取消）并完成结算；随后同 ID 重放命中。
func TestWorkerCancelDoesNotCancelTry(t *testing.T) {
	gate := make(chan struct{})
	ad := newAdapter("p", step{gate: gate, body: `{"answer":"late"}`})
	h := newHarness(t, testLimits(), ad)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := h.c.Invoke(ctx, inv("c1", chatBody)); errc <- err }()
	waitEntered(t, ad)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Invoke 应随 Worker 上下文返回：%v", err)
	}
	h.c.CancelAttempt("a1", "attempt_ended") // 非取消原因的撤销也不取消 try
	close(gate)
	select { // 等待后台执行完成结算（结算发生在 try 返回之后）
	case <-h.store.settled:
	case <-time.After(5 * time.Second):
		t.Fatal("try 没有结算")
	}
	if _, _, errs := ad.stats(); len(errs) != 1 || errs[0] != nil {
		t.Fatalf("try 的上下文被取消：%v", errs)
	}
	r, err := h.newCoordinator(t, testLimits()).Invoke(context.Background(), inv("c1", chatBody))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Replayed || string(r.Body) != `{"answer":"late"}` {
		t.Fatalf("重放 %+v", r)
	}
}

// cancel 原因取消在途 try：已发出 → unknown（调用保持 unknown）；未发出 → 释放预留，调用 failed（cancel_requested）。
func TestCancelAttemptCancelsTry(t *testing.T) {
	for _, sent := range []bool{true, false} {
		t.Run(fmt.Sprintf("sent=%v", sent), func(t *testing.T) {
			ad := newAdapter("p", step{hang: true, sent: sent})
			h := newHarness(t, testLimits(), ad)
			done := make(chan Result, 1)
			go func() { r, _ := h.c.Invoke(context.Background(), inv("c1", chatBody)); done <- r }()
			waitEntered(t, ad)
			h.c.CancelAttempt("other", ReasonCancel) // 其他 attempt 的取消不影响
			select {
			case r := <-done:
				t.Fatalf("其他 attempt 的取消中止了 try：%+v", r)
			case <-time.After(20 * time.Millisecond):
			}
			h.c.CancelAttempt("a1", ReasonCancel)
			r := <-done
			if r.Status != 409 || r.Code != persistence.CodeCancelRequested {
				t.Fatalf("得到 %+v", r)
			}
			rec, tries := h.call(t, "t1", "c1") // 结算先于结果返回，无需等待
			b := h.budget(t, "t1")
			if len(tries) != 1 || b.ReservedMicro != 0 {
				t.Fatalf("try %d，账本 %+v", len(tries), b)
			}
			if sent && (rec.State != StateUnknown || b.UnknownMicro != 100) {
				t.Fatalf("已发出：调用 %+v，账本 %+v", rec, b)
			}
			if !sent && (rec.State != StateFailed || rec.FailReason != persistence.CodeCancelRequested || b.UnknownMicro != 0) {
				t.Fatalf("未发出：调用 %+v，账本 %+v", rec, b)
			}
		})
	}
}

// ---- 并发限额 ----

// 每任务在途上限：第二个调用阻塞在槽位上，直到第一个释放；并发峰值不超过上限。
func TestPerTaskInflightBlocksUntilRelease(t *testing.T) {
	lim := testLimits()
	lim.PerTaskInflight = 1
	gate := make(chan struct{})
	ad := newAdapter("p", step{gate: gate, hold: true})
	h := newHarness(t, lim, ad)
	first := make(chan Result, 1)
	go func() { r, _ := h.c.Invoke(context.Background(), inv("c1", chatBody)); first <- r }()
	waitEntered(t, ad)
	second := make(chan Result, 1)
	go func() { r, _ := h.c.Invoke(context.Background(), inv("c2", chatBody)); second <- r }()
	waitBegun(t, h.store, "c2")
	// 第一个 try 在测试释放前一直占用唯一槽位，第二个调用不可能预留或进入上游（下面的窗口只是反向检查）。
	select {
	case <-ad.entered:
		t.Fatal("第二个 try 没有等待槽位")
	case <-time.After(50 * time.Millisecond):
	}
	if rec, _ := h.call(t, "t1", "c2"); rec.TriesUsed != 0 {
		t.Fatalf("等待槽位时不应预留：%+v", rec)
	}
	close(gate)
	if r := <-first; r.Status != 200 {
		t.Fatalf("first %+v", r)
	}
	if r := <-second; r.Status != 200 {
		t.Fatalf("second %+v", r)
	}
	if _, maxIn, _ := ad.stats(); maxIn != 1 {
		t.Fatalf("并发峰值 %d", maxIn)
	}
}

// 每 provider 上限跨任务生效；等待槽位计入期限：超期 → 504，调用没有 try、置为 failed。
func TestPerProviderInflightWaitCountsTowardDeadline(t *testing.T) {
	lim := testLimits()
	lim.PerProviderInflight = 1
	lim.ModelCallDeadline = 200 * time.Millisecond
	gate := make(chan struct{})
	// 占用 provider 槽位的 try 忽略自己的期限，只在第二个调用观察到期限、返回之后由测试释放。
	ad := newAdapter("p", step{gate: gate, hold: true})
	h := newHarness(t, lim, ad)
	first := make(chan Result, 1)
	go func() { r, _ := h.c.Invoke(context.Background(), inv("c1", chatBody)); first <- r }()
	waitEntered(t, ad)
	other := inv("c9", chatBody)
	other.TaskID, other.AttemptID = "t2", "a2"
	r := h.invoke(t, other)
	if r.Status != 504 || r.Code != persistence.CodeCallDeadlineExceeded {
		t.Fatalf("得到 %+v", r)
	}
	if rec, tries := h.call(t, "t2", "c9"); rec.State != StateFailed || len(tries) != 0 {
		t.Fatalf("调用记录 %+v，try %d", rec, len(tries))
	}
	close(gate)
	<-first
}

// ---- 第二个原子提交点 ----

// 结果 blob 写入失败：不结算 ok（没有 completed、不记 spent），try 按 unknown 结算，预留不悬空。
func TestBlobWriteFailureDoesNotComplete(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.blobs.failPut = true
	if _, err := h.c.Invoke(context.Background(), inv("c1", chatBody)); err == nil {
		t.Fatal("blob 写入失败应返回错误")
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State == StateCompleted || rec.ResultRef != "" || len(tries) != 1 || tries[0].Error != CodeBlobWriteFailed {
		t.Fatalf("调用记录 %+v %+v", rec, tries)
	}
	if b := h.budget(t, "t1"); b.SpentMicro != 0 || b.ReservedMicro != 0 || b.UnknownMicro != 100 {
		t.Fatalf("账本 %+v", b)
	}
}

// Tx2 存储故障：返回错误，调用置为 failed（store_unavailable，可重试类别），带 Retry 可继续。
func TestReserveStoreFailure(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.store.reserveErr = persistence.ErrUnavailable
	if _, err := h.c.Invoke(context.Background(), inv("c1", chatBody)); !errors.Is(err, persistence.ErrUnavailable) {
		t.Fatalf("得到 %v", err)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.State != StateFailed || rec.FailReason != CodeStoreUnavailable {
		t.Fatalf("调用记录 %+v", rec)
	}
	in := inv("c1", chatBody)
	in.Retry = true
	if r := h.invoke(t, in); r.Status != 200 {
		t.Fatalf("带 Retry：%+v", r)
	}
}

// ---- blob 授权与日志 ----

// OpenBlob 按 scope_blobs(task) 授权：本任务 completed 调用的结果与以其他来源登记到本任务 scope 的 blob
// 可读；其他任务或未登记的 blob 一律 ErrNotFound。
func TestOpenBlobScope(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	r := h.invoke(t, inv("c1", chatBody))
	rc, err := h.c.OpenBlob(context.Background(), "t1", r.BlobSHA256)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, r.Body) {
		t.Fatalf("blob 内容 %s", got)
	}
	if _, err := h.c.OpenBlob(context.Background(), "t2", r.BlobSHA256); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("其他任务：%v", err)
	}
	stray, _ := h.blobs.Put(context.Background(), strings.NewReader("stray"))
	if _, err := h.c.OpenBlob(context.Background(), "t1", stray.SHA256); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("未授权的 blob：%v", err)
	}
	h.store.scope("t1", stray.SHA256) // 例如 checkpoint 引用的产物
	rc, err = h.c.OpenBlob(context.Background(), "t1", stray.SHA256)
	if err != nil {
		t.Fatalf("scope 内的非调用 blob：%v", err)
	}
	rc.Close()
}

// 日志含规定字段，不含提示词。
func TestLogFieldsWithoutPrompt(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.invoke(t, inv("c1", chatBody))
	logs := h.logs.String()
	for _, f := range []string{`"task_id":"t1"`, `"attempt_id":"a1"`, `"call_id":"c1"`, `"try_no":1`,
		`"endpoint":"/v1/chat/completions"`, `"provider":"p"`, `"status":200`, `"latency_ms":`} {
		if !strings.Contains(logs, f) {
			t.Errorf("日志缺少 %s：%s", f, logs)
		}
	}
	if strings.Contains(logs, "secret prompt") {
		t.Fatalf("日志含提示词：%s", logs)
	}
}

// 退避：基数翻倍、不超过上限、在 [d/2, d] 内抖动，且至少为 Retry-After。
func TestBackoff(t *testing.T) {
	c := &Coordinator{limits: Limits{BackoffBase: 2 * time.Second, BackoffMax: 60 * time.Second}}
	for n, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 10: 60 * time.Second} {
		for range 20 {
			if d := c.backoff(n, 0); d < want/2 || d > want {
				t.Fatalf("第 %d 次：%v 不在 [%v, %v]", n, d, want/2, want)
			}
		}
	}
	if d := c.backoff(1, 30*time.Second); d != 30*time.Second {
		t.Fatalf("Retry-After：%v", d)
	}
}

// ---- 缓存分支（§11.2、§11.4、§11.5） ----

type fakeEntry struct {
	sha  string
	size int64
}

type fakeStored struct {
	kind upstream.Kind
	url  string
	resp cache.Response
	sha  string
}

// fakeCache 是按 (kind, provider, ver, params) 精确匹配的内存缓存；记录查找、跳过与写入。
type fakeCache struct {
	mu       sync.Mutex
	entries  map[string]fakeEntry
	lookups  int
	bypasses int
	stores   []fakeStored
	onLookup func()        // 查找时（Tx1 之后、Tx2 之前）执行，用于制造竞争
	joined   chan struct{} // 非 nil 时每个 follower 加入共享请求发送一次（测试据此同步，不用 sleep）
	nJoined  int
}

func fakeCacheKey(kind upstream.Kind, provider, ver string, params []byte) string {
	return string(kind) + "|" + provider + "|" + ver + "|" + string(params)
}

func (f *fakeCache) Lookup(_ context.Context, kind upstream.Kind, provider, ver string, params []byte, _ string) (string, int64, bool) {
	f.mu.Lock()
	f.lookups++
	e, ok := f.entries[fakeCacheKey(kind, provider, ver, params)]
	hook := f.onLookup
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return e.sha, e.size, ok
}

func (f *fakeCache) Store(kind upstream.Kind, _, _ string, _ []byte, rawURL string, resp cache.Response, sha string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stores = append(f.stores, fakeStored{kind: kind, url: rawURL, resp: resp, sha: sha})
}

func (f *fakeCache) Bypass() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bypasses++
}

func (f *fakeCache) CoalesceKey(kind upstream.Kind, provider, ver string, params []byte, _ string) (string, bool) {
	return fakeCacheKey(kind, provider, ver, params), true
}

func (f *fakeCache) Coalesced() {
	f.mu.Lock()
	f.nJoined++
	ch := f.joined
	f.mu.Unlock()
	if ch != nil {
		ch <- struct{}{}
	}
}

func (f *fakeCache) coalesced() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nJoined
}

func (f *fakeCache) counts() (lookups, bypasses, stores int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookups, f.bypasses, len(f.stores)
}

const searchBody = `{"query":"go"}`

func searchInv(callID string) Invoke {
	in := inv(callID, searchBody)
	in.Kind = upstream.KindSearch
	return in
}

// newCacheHarness 装配一个使用 cs 的 Coordinator；adapter 为搜索类别。
func newCacheHarness(t *testing.T, cs CacheSource, script ...step) *harness {
	t.Helper()
	ad := newAdapter("p", script...)
	ad.kind = upstream.KindSearch
	h := &harness{store: newFakeStore(t), ad: ad, blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{},
		logs: &syncBuffer{}, cache: cs}
	h.c = h.newCoordinator(t, testLimits())
	return h
}

// seed 把 body 存为 blob，并在 fc 中登记 searchBody 的条目指向它。
func (h *harness) seed(t *testing.T, fc *fakeCache, body string) fakeEntry {
	t.Helper()
	ref, err := h.blobs.Put(context.Background(), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resolved, _, err := h.ad.Resolve([]byte(searchBody))
	if err != nil {
		t.Fatal(err)
	}
	e := fakeEntry{sha: ref.SHA256, size: ref.Size}
	fc.mu.Lock()
	fc.entries[fakeCacheKey(upstream.KindSearch, "p", "fake/1", resolved)] = e
	fc.mu.Unlock()
	return e
}

// 命中：不预留、不访问上游、不改动账本；journal completed（source = cache，无 try），结果授权到任务 scope；
// 之后同 ID 的请求按 journal 重放，不再查缓存。
func TestCacheHitNoReservation(t *testing.T) {
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newCacheHarness(t, fc)
	e := h.seed(t, fc, `{"results":["cached"]}`)
	r := h.invoke(t, searchInv("c1"))
	if r.Status != 200 || r.Replayed || r.BlobSHA256 != e.sha || string(r.Body) != `{"results":["cached"]}` {
		t.Fatalf("命中结果 %+v", r)
	}
	if calls, _, _ := h.ad.stats(); calls != 0 {
		t.Fatalf("命中不应访问上游，得到 %d 次", calls)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.Source != "cache" || rec.ResultRef != e.sha || rec.TriesUsed != 0 || len(tries) != 0 {
		t.Fatalf("调用记录 %+v，try %+v", rec, tries)
	}
	if b := h.budget(t, "t1"); b.ReservedMicro != 0 || b.SpentMicro != 0 || b.UnknownMicro != 0 || h.store.nRes != 0 {
		t.Fatalf("命中不应预留或计费：%+v，预留 %d 次", b, h.store.nRes)
	}
	rc, err := h.c.OpenBlob(context.Background(), "t1", e.sha)
	if err != nil {
		t.Fatalf("命中结果应授权到任务 scope：%v", err)
	}
	_ = rc.Close() // 只读的内存 blob，关闭错误无关紧要
	again := h.invoke(t, searchInv("c1"))
	if !again.Replayed || again.BlobSHA256 != e.sha {
		t.Fatalf("重放结果 %+v", again)
	}
	if lookups, _, stores := fc.counts(); lookups != 1 || stores != 0 {
		t.Fatalf("重放不应查缓存、命中不应写缓存：lookups=%d stores=%d", lookups, stores)
	}
}

// journal 优先：已完成的调用以 journal 结果重放，缓存里出现同参数的其他结果也不覆盖它；未命中走上游并在
// 结算之后把结果（大小为结果 blob 的大小）交给缓存。
func TestCacheJournalFirst(t *testing.T) {
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newCacheHarness(t, fc, step{body: `{"results":["upstream"]}`})
	first := h.invoke(t, searchInv("c1"))
	if first.Status != 200 || string(first.Body) != `{"results":["upstream"]}` {
		t.Fatalf("未命中结果 %+v", first)
	}
	fc.mu.Lock()
	stored := append([]fakeStored(nil), fc.stores...)
	fc.mu.Unlock()
	if len(stored) != 1 || stored[0].sha != first.BlobSHA256 || stored[0].kind != upstream.KindSearch ||
		stored[0].resp.Status != 200 || stored[0].resp.Size != int64(len(first.Body)) || stored[0].resp.ResponseTime.IsZero() {
		t.Fatalf("结算后的缓存写入 %+v", stored)
	}
	h.seed(t, fc, `{"results":["cached-later"]}`)
	again := h.invoke(t, searchInv("c1"))
	if !again.Replayed || again.BlobSHA256 != first.BlobSHA256 || string(again.Body) != `{"results":["upstream"]}` {
		t.Fatalf("journal 应先于缓存：%+v", again)
	}
	if lookups, _, _ := fc.counts(); lookups != 1 {
		t.Fatalf("重放不应查缓存，lookups=%d", lookups)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.Source != "upstream" || rec.ResultRef != first.BlobSHA256 {
		t.Fatalf("已记录的结果被改变：%+v", rec)
	}
}

// no-cache：不读缓存（计 bypass），走上游，结果仍写入缓存；指令计入指纹（同 ID 去掉指令为指纹分歧）。
func TestNoCacheSkipsReadButWrites(t *testing.T) {
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newCacheHarness(t, fc, step{body: `{"results":["fresh"]}`})
	h.seed(t, fc, `{"results":["stale"]}`)
	in := searchInv("c1")
	in.NoCache = true
	r := h.invoke(t, in)
	if r.Status != 200 || string(r.Body) != `{"results":["fresh"]}` {
		t.Fatalf("no-cache 结果 %+v", r)
	}
	if lookups, bypasses, stores := fc.counts(); lookups != 0 || bypasses != 1 || stores != 1 {
		t.Fatalf("no-cache：lookups=%d bypasses=%d stores=%d，期望 0/1/1", lookups, bypasses, stores)
	}
	if calls, _, _ := h.ad.stats(); calls != 1 {
		t.Fatalf("上游调用 %d 次", calls)
	}
	if r := h.invoke(t, searchInv("c1")); r.Code != persistence.CodeFingerprintMismatch {
		t.Fatalf("去掉 no-cache 的同 ID 请求应为指纹分歧，得到 %+v", r)
	}
}

// 命中条目引用的 blob 缺失或内容不符 → 按未命中走上游，不提交缓存结果。
func TestCacheHitBlobMismatchFallsBack(t *testing.T) {
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newCacheHarness(t, fc, step{body: `{"results":["upstream"]}`})
	e := h.seed(t, fc, `{"results":["cached"]}`)
	h.blobs.mu.Lock()
	h.blobs.m[e.sha] = []byte(`{"results":["tampered"]}`)
	h.blobs.mu.Unlock()
	r := h.invoke(t, searchInv("c1"))
	if r.Status != 200 || string(r.Body) != `{"results":["upstream"]}` {
		t.Fatalf("复核失败应走上游，得到 %+v", r)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.Source != "upstream" || rec.State != StateCompleted {
		t.Fatalf("调用记录 %+v", rec)
	}
}

// E23：取消在命中之后、Tx2 之前提交 → CompleteFromCache 拒绝，结果不被授权，调用为 failed（cancel_requested）。
func TestCacheHitAfterCancelNotAuthorized(t *testing.T) {
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newCacheHarness(t, fc)
	e := h.seed(t, fc, `{"results":["cached"]}`)
	fc.mu.Lock()
	fc.onLookup = func() {
		h.store.mu.Lock()
		h.store.desired["t1"] = "cancel"
		h.store.mu.Unlock()
	}
	fc.mu.Unlock()
	r := h.invoke(t, searchInv("c1"))
	if r.Status != 409 || r.Code != persistence.CodeCancelRequested || r.Body != nil {
		t.Fatalf("取消先提交时得到 %+v", r)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.State != StateFailed || rec.FailReason != persistence.CodeCancelRequested || rec.ResultRef != "" {
		t.Fatalf("调用记录 %+v", rec)
	}
	if ok, err := h.store.BlobAuthorized(context.Background(), "t1", e.sha); err != nil || ok {
		t.Fatalf("命中结果不应被授权：%v / %v", ok, err)
	}
	if calls, _, _ := h.ad.stats(); calls != 0 {
		t.Fatalf("上游调用 %d 次", calls)
	}
}

// 模型调用从不查缓存、也不写缓存（§11.1）。
func TestChatNeverCached(t *testing.T) {
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.cache = fc
	h.c = h.newCoordinator(t, testLimits())
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 200 {
		t.Fatalf("chat 结果 %+v", r)
	}
	in := inv("c2", chatBody)
	in.NoCache = true
	if r := h.invoke(t, in); r.Status != 200 {
		t.Fatalf("chat no-cache 结果 %+v", r)
	}
	if lookups, bypasses, stores := fc.counts(); lookups != 0 || bypasses != 0 || stores != 0 {
		t.Fatalf("chat 不应触及缓存：lookups=%d bypasses=%d stores=%d", lookups, bypasses, stores)
	}
}

// 真实的 cache.Source：Redis 不可达时全部未命中（计 error），任务结果正确；真实 Redis（AGENTBOX_TEST_REDIS_ADDR）
// 上第二个同参数调用命中，不访问上游、不预留。
func TestCacheSourceEndToEnd(t *testing.T) {
	newSource := func(t *testing.T, h *harness, addr string) (*cache.Source, *cache.Redis) {
		t.Helper()
		signer, err := cache.LoadKeys(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		r := cache.NewRedis(cache.RedisConfig{Addr: addr})
		src, err := cache.NewSource(cache.SourceConfig{KV: r, Signer: signer, Blobs: h.blobs})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			src.Close()
			_ = r.Close() // 测试结束，关闭错误无关紧要
		})
		return src, r
	}
	t.Run("Redis 不可达", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		dead := ln.Addr().String()
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		h := newCacheHarness(t, nil)
		src, _ := newSource(t, h, dead)
		h.cache = src
		h.c = h.newCoordinator(t, testLimits())
		for _, id := range []string{"c1", "c2"} {
			if r := h.invoke(t, searchInv(id)); r.Status != 200 || string(r.Body) != `{"answer":"ok"}` {
				t.Fatalf("%s：%+v", id, r)
			}
		}
		if calls, _, _ := h.ad.stats(); calls != 2 {
			t.Fatalf("Redis 不可达时应全部走上游，上游调用 %d 次", calls)
		}
		m := src.Metrics()
		if m["hit"] != 0 || m["error"] < 2 {
			t.Fatalf("指标 %v", m)
		}
	})
	t.Run("真实 Redis", func(t *testing.T) {
		addr := os.Getenv("AGENTBOX_TEST_REDIS_ADDR")
		if addr == "" {
			if os.Getenv("CI") == "true" {
				t.Fatal("CI 中必须设置 AGENTBOX_TEST_REDIS_ADDR")
			}
			t.Skip("未设置 AGENTBOX_TEST_REDIS_ADDR，跳过")
		}
		h := newCacheHarness(t, nil)
		h.ad.provider = fmt.Sprintf("p%d", time.Now().UnixNano()) // 每次运行使用新键，不受旧条目影响
		src, r := newSource(t, h, addr)
		h.cache = src
		h.c = h.newCoordinator(t, testLimits())
		first := h.invoke(t, searchInv("c1"))
		resolved, _, err := h.ad.Resolve([]byte(searchBody))
		if err != nil {
			t.Fatal(err)
		}
		key, err := cache.Key(upstream.KindSearch, h.ad.provider, "fake/1", resolved)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			_, ok, err := r.Get(context.Background(), key)
			if err == nil && ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("异步写入没有到达 Redis：%v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		second := h.invoke(t, searchInv("c2"))
		if second.BlobSHA256 != first.BlobSHA256 || !bytes.Equal(second.Body, first.Body) || second.Replayed {
			t.Fatalf("命中结果 %+v，首次 %+v", second, first)
		}
		if calls, _, _ := h.ad.stats(); calls != 1 {
			t.Fatalf("命中不应访问上游，上游调用 %d 次", calls)
		}
		if rec, _ := h.call(t, "t1", "c2"); rec.Source != "cache" || rec.TriesUsed != 0 {
			t.Fatalf("c2 记录 %+v", rec)
		}
		if m := src.Metrics(); m["hit"] != 1 || m["miss"] != 1 {
			t.Fatalf("指标 %v", m)
		}
	})
}

// ---- 请求合并（§11.4） ----

const fetchBody = `{"url":"https://example.com/a"}`

func fetchInv(callID string) Invoke {
	in := inv(callID, fetchBody)
	in.Kind = upstream.KindFetch
	return in
}

// newCoalesceHarness 装配一个使用 fakeCache（记录 follower 加入）的 Coordinator；adapter 为 kind 类别。
func newCoalesceHarness(t *testing.T, kind upstream.Kind, script ...step) (*harness, *fakeCache) {
	t.Helper()
	fc := &fakeCache{entries: map[string]fakeEntry{}, joined: make(chan struct{}, 64)}
	ad := newAdapter("p", script...)
	ad.kind = kind
	h := &harness{store: newFakeStore(t), ad: ad, blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{},
		logs: &syncBuffer{}, cache: fc}
	h.c = h.newCoordinator(t, testLimits())
	return h, fc
}

type invokeOutcome struct {
	r   Result
	err error
}

// start 在后台发起一次 Invoke，结果从返回的通道取得。
func (h *harness) start(ctx context.Context, in Invoke) <-chan invokeOutcome {
	ch := make(chan invokeOutcome, 1)
	go func() {
		r, err := h.c.Invoke(ctx, in)
		ch <- invokeOutcome{r, err}
	}()
	return ch
}

func await(t *testing.T, ch <-chan invokeOutcome) Result {
	t.Helper()
	select {
	case o := <-ch:
		if o.err != nil {
			t.Fatalf("Invoke：%v", o.err)
		}
		return o.r
	case <-time.After(5 * time.Second):
		t.Fatal("Invoke 没有返回")
	}
	return Result{}
}

// waitJoined 等待 n 个 follower 加入共享请求。
func waitJoined(t *testing.T, fc *fakeCache, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-fc.joined:
		case <-time.After(5 * time.Second):
			t.Fatalf("只有 %d 个 follower 加入，期望 %d", i, n)
		}
	}
}

// 同一 attempt 的 10 个相同抓取：1 次上游 try（leader 付费），9 个 follower 各自 completed（source = coalesced，
// 无 try、无费用），结果为同一 blob 且授权到任务 scope；coalesced 计 9 次。
func TestCoalesceIdenticalFetches(t *testing.T) {
	gate := make(chan struct{})
	h, fc := newCoalesceHarness(t, upstream.KindFetch, step{gate: gate, body: `{"page":"a"}`})
	ctx := context.Background()
	outs := []<-chan invokeOutcome{h.start(ctx, fetchInv("c0"))}
	waitEntered(t, h.ad) // c0 是 leader：它的 try 已发出并阻塞在 gate 上
	for i := 1; i < 10; i++ {
		outs = append(outs, h.start(ctx, fetchInv(fmt.Sprintf("c%d", i))))
	}
	waitJoined(t, fc, 9)
	close(gate)
	var sha string
	for i, ch := range outs {
		r := await(t, ch)
		if r.Status != 200 || r.Replayed || string(r.Body) != `{"page":"a"}` || (sha != "" && r.BlobSHA256 != sha) {
			t.Fatalf("c%d：%+v", i, r)
		}
		sha = r.BlobSHA256
	}
	if calls, _, _ := h.ad.stats(); calls != 1 {
		t.Fatalf("上游调用 %d 次，期望 1", calls)
	}
	for i := 0; i < 10; i++ {
		rec, tries := h.call(t, "t1", fmt.Sprintf("c%d", i))
		wantSrc, wantTries := SourceCoalesced, 0
		if i == 0 {
			wantSrc, wantTries = "upstream", 1
		}
		if rec.State != StateCompleted || rec.Source != wantSrc || rec.ResultRef != sha || rec.TriesUsed != wantTries ||
			len(tries) != wantTries {
			t.Fatalf("c%d 记录 %+v，try %+v", i, rec, tries)
		}
	}
	if b := h.budget(t, "t1"); b.SpentMicro != 100 || b.ReservedMicro != 0 || b.UnknownMicro != 0 || h.store.nRes != 1 {
		t.Fatalf("只有 leader 付费：账本 %+v，预留 %d 次", b, h.store.nRes)
	}
	if n := fc.coalesced(); n != 9 {
		t.Fatalf("coalesced = %d，期望 9", n)
	}
	if lookups, _, _ := fc.counts(); lookups != 1 {
		t.Fatalf("follower 加入进行中的共享请求时不查缓存：lookups = %d", lookups)
	}
	if r := h.invoke(t, fetchInv("c5")); !r.Replayed || r.BlobSHA256 != sha {
		t.Fatalf("follower 的调用应按 journal 重放：%+v", r)
	}
}

// 合并键含 sub-run 与 attempt：不同 sub-run（X-Agentbox-Subrun）或不同 attempt 的同一抓取各自访问上游。
// 每个 try 都阻塞在 gate 上，三个 try 都开始即说明没有任何一个加入别人的共享请求。
func TestCoalesceNotAcrossSubrunOrAttempt(t *testing.T) {
	gate := make(chan struct{})
	h, fc := newCoalesceHarness(t, upstream.KindFetch,
		step{gate: gate, body: `{"page":"1"}`}, step{gate: gate, body: `{"page":"2"}`}, step{gate: gate, body: `{"page":"3"}`})
	ctx := context.Background()
	sr1, sr2, other := fetchInv("sr1/s/fetch/1"), fetchInv("sr2/s/fetch/1"), fetchInv("root/s/fetch/1")
	sr1.SubrunID, sr2.SubrunID, other.AttemptID = "sr1", "sr2", "a2"
	outs := []<-chan invokeOutcome{h.start(ctx, sr1)}
	waitEntered(t, h.ad)
	outs = append(outs, h.start(ctx, sr2))
	waitEntered(t, h.ad)
	outs = append(outs, h.start(ctx, other))
	waitEntered(t, h.ad)
	close(gate)
	for _, ch := range outs {
		if r := await(t, ch); r.Status != 200 {
			t.Fatalf("结果 %+v", r)
		}
	}
	if calls, _, _ := h.ad.stats(); calls != 3 || fc.coalesced() != 0 {
		t.Fatalf("上游调用 %d 次、coalesced %d，期望 3 与 0", calls, fc.coalesced())
	}
}

// no-cache 不加入进行中的共享请求；模型调用从不合并。
func TestCoalesceSkipsNoCacheAndChat(t *testing.T) {
	t.Run("no-cache", func(t *testing.T) {
		gate := make(chan struct{})
		h, fc := newCoalesceHarness(t, upstream.KindSearch, step{gate: gate}, step{gate: gate})
		ctx := context.Background()
		first := h.start(ctx, searchInv("c1"))
		waitEntered(t, h.ad)
		in := searchInv("c2")
		in.NoCache = true
		second := h.start(ctx, in)
		waitEntered(t, h.ad)
		close(gate)
		for _, ch := range []<-chan invokeOutcome{first, second} {
			if r := await(t, ch); r.Status != 200 {
				t.Fatalf("结果 %+v", r)
			}
		}
		if rec, _ := h.call(t, "t1", "c2"); rec.Source != "upstream" || fc.coalesced() != 0 {
			t.Fatalf("no-cache 记录 %+v，coalesced %d", rec, fc.coalesced())
		}
	})
	t.Run("chat", func(t *testing.T) {
		gate := make(chan struct{})
		h, fc := newCoalesceHarness(t, upstream.KindChat, step{gate: gate}, step{gate: gate})
		ctx := context.Background()
		first := h.start(ctx, inv("c1", chatBody))
		waitEntered(t, h.ad)
		second := h.start(ctx, inv("c2", chatBody))
		waitEntered(t, h.ad)
		close(gate)
		for _, ch := range []<-chan invokeOutcome{first, second} {
			if r := await(t, ch); r.Status != 200 {
				t.Fatalf("结果 %+v", r)
			}
		}
		if lookups, _, _ := fc.counts(); lookups != 0 || fc.coalesced() != 0 {
			t.Fatalf("chat 不应触及缓存或合并：lookups %d，coalesced %d", lookups, fc.coalesced())
		}
	})
}

// leader 的 Worker 断开：共享请求不被取消，继续完成并结算；follower 拿到结果；leader 的调用之后可重放。
func TestCoalesceLeaderWorkerLeaves(t *testing.T) {
	gate := make(chan struct{})
	h, fc := newCoalesceHarness(t, upstream.KindFetch, step{gate: gate, body: `{"page":"late"}`})
	lctx, leave := context.WithCancel(context.Background())
	leader := h.start(lctx, fetchInv("c0"))
	waitEntered(t, h.ad)
	follower := h.start(context.Background(), fetchInv("c1"))
	waitJoined(t, fc, 1)
	leave()
	if o := <-leader; !errors.Is(o.err, context.Canceled) {
		t.Fatalf("leader 的 Invoke 应随 Worker 上下文返回：%+v", o)
	}
	close(gate)
	r := await(t, follower)
	if r.Status != 200 || string(r.Body) != `{"page":"late"}` {
		t.Fatalf("follower 结果 %+v", r)
	}
	if _, _, errs := h.ad.stats(); len(errs) != 1 || errs[0] != nil {
		t.Fatalf("共享请求的上下文被取消：%v", errs)
	}
	if rec, _ := h.call(t, "t1", "c0"); rec.State != StateCompleted || rec.Source != "upstream" || rec.ResultRef != r.BlobSHA256 {
		t.Fatalf("leader 记录 %+v", rec)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.State != StateCompleted || rec.Source != SourceCoalesced {
		t.Fatalf("follower 记录 %+v", rec)
	}
	again, err := h.newCoordinator(t, testLimits()).Invoke(context.Background(), fetchInv("c0"))
	if err != nil || !again.Replayed || again.BlobSHA256 != r.BlobSHA256 {
		t.Fatalf("leader 的调用应可重放：%+v / %v", again, err)
	}
}

// 任务取消：共享请求被取消（已发出 → unknown 计费一次；未发出 → 释放），leader 与每个 follower 都结算为
// cancel_requested；follower 没有 try、不计费。
func TestCoalesceTaskCancel(t *testing.T) {
	for _, sent := range []bool{true, false} {
		t.Run(fmt.Sprintf("sent=%v", sent), func(t *testing.T) {
			h, fc := newCoalesceHarness(t, upstream.KindFetch, step{hang: true, sent: sent})
			ctx := context.Background()
			outs := []<-chan invokeOutcome{h.start(ctx, fetchInv("c0"))}
			waitEntered(t, h.ad)
			outs = append(outs, h.start(ctx, fetchInv("c1")), h.start(ctx, fetchInv("c2")))
			waitJoined(t, fc, 2)
			h.store.mu.Lock()
			h.store.desired["t1"] = "cancel"
			h.store.mu.Unlock()
			h.c.CancelAttempt("a1", ReasonCancel)
			for i, ch := range outs {
				if r := await(t, ch); r.Status != 409 || r.Code != persistence.CodeCancelRequested {
					t.Fatalf("c%d：%+v", i, r)
				}
			}
			for _, id := range []string{"c1", "c2"} {
				if rec, tries := h.call(t, "t1", id); rec.State != StateFailed || rec.FailReason != persistence.CodeCancelRequested ||
					len(tries) != 0 {
					t.Fatalf("follower %s 记录 %+v，try %+v", id, rec, tries)
				}
			}
			rec, _ := h.call(t, "t1", "c0")
			b := h.budget(t, "t1")
			if calls, _, _ := h.ad.stats(); calls != 1 || b.ReservedMicro != 0 || b.SpentMicro != 0 {
				t.Fatalf("上游 %d 次，账本 %+v", calls, b)
			}
			if sent && (rec.State != StateUnknown || b.UnknownMicro != 100) {
				t.Fatalf("已发出：leader %+v，账本 %+v", rec, b)
			}
			if !sent && (rec.State != StateFailed || rec.FailReason != persistence.CodeCancelRequested || b.UnknownMicro != 0) {
				t.Fatalf("未发出：leader %+v，账本 %+v", rec, b)
			}
		})
	}
}

// E19 共享请求变体：attempt 被替换（非取消原因）时共享请求继续至完成并写入 journal；新 attempt 以 leader 的
// call ID 请求得到 call_in_progress，完成后重放；follower 在访问复查时被拒（access_revoked，可重试），新 attempt
// 带 X-Agentbox-Retry 重发后正常完成。
func TestCoalesceAttemptReplacedContinues(t *testing.T) {
	gate := make(chan struct{})
	h, fc := newCoalesceHarness(t, upstream.KindFetch, step{gate: gate, body: `{"page":"shared"}`})
	ctx := context.Background()
	leader := h.start(ctx, fetchInv("c0"))
	waitEntered(t, h.ad)
	follower := h.start(ctx, fetchInv("c1"))
	waitJoined(t, fc, 1)
	h.store.mu.Lock()
	h.store.revoked["a1"] = true
	h.store.mu.Unlock()
	h.c.CancelAttempt("a1", "attempt_replaced")
	next := fetchInv("c0")
	next.AttemptID = "a2"
	if r := h.invoke(t, next); r.Code != persistence.CodeCallInProgress {
		t.Fatalf("新 attempt 在共享请求进行中应得到 call_in_progress：%+v", r)
	}
	close(gate)
	if r := await(t, leader); r.Status != 200 || string(r.Body) != `{"page":"shared"}` {
		t.Fatalf("leader 结果 %+v", r)
	}
	if r := await(t, follower); r.Status != 403 || r.Code != persistence.CodeAccessRevoked {
		t.Fatalf("follower 结果 %+v", r)
	}
	if _, _, errs := h.ad.stats(); len(errs) != 1 || errs[0] != nil {
		t.Fatalf("共享请求的上下文被取消：%v", errs)
	}
	if rec, _ := h.call(t, "t1", "c0"); rec.State != StateCompleted || rec.Source != "upstream" {
		t.Fatalf("leader 记录 %+v", rec)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.State != StateFailed || rec.FailReason != persistence.CodeAccessRevoked {
		t.Fatalf("follower 记录 %+v", rec)
	}
	c2 := h.newCoordinator(t, testLimits()) // 新进程视角：leader 的执行已结束
	if r, err := c2.Invoke(ctx, next); err != nil || !r.Replayed || string(r.Body) != `{"page":"shared"}` {
		t.Fatalf("新 attempt 应重放 leader 的结果：%+v / %v", r, err)
	}
	retry := fetchInv("c1")
	retry.AttemptID, retry.Retry = "a2", true
	if r, err := c2.Invoke(ctx, retry); err != nil || r.Status != 200 || r.Replayed {
		t.Fatalf("follower 的调用带 Retry 重发：%+v / %v", r, err)
	}
}

// ---- 按端点类别的调用期限 ----

// 期限在 Tx1 按端点类别选定：模型调用 deadline_at = created_at + ModelCallDeadline，搜索与抓取用 CallDeadline。
// 模型调用的上游耗时超过 CallDeadline（但在 ModelCallDeadline 之内）仍正常完成；同样挂起的搜索在 CallDeadline 到期。
func TestDeadlinePerEndpointKind(t *testing.T) {
	lim := testLimits()
	lim.CallDeadline, lim.ModelCallDeadline = 150*time.Millisecond, 5*time.Second

	gate := make(chan struct{})
	chat := newHarness(t, lim, newAdapter("p", step{body: `{"answer":"slow"}`, gate: gate}))
	done := chat.start(context.Background(), inv("c1", chatBody))
	waitEntered(t, chat.ad)
	time.Sleep(3 * lim.CallDeadline) // 超过搜索与抓取的期限
	close(gate)
	if r := await(t, done); r.Status != 200 || string(r.Body) != `{"answer":"slow"}` {
		t.Fatalf("模型调用超过 CallDeadline 后应在 ModelCallDeadline 内完成：%+v", r)
	}
	if rec, _ := chat.call(t, "t1", "c1"); rec.State != StateCompleted || rec.DeadlineAt.Sub(rec.CreatedAt) != lim.ModelCallDeadline {
		t.Fatalf("模型调用记录 %+v；期望 completed、deadline_at = created_at + %s", rec, lim.ModelCallDeadline)
	}

	for _, in := range []Invoke{searchInv("s1"), fetchInv("f1")} {
		ad := newAdapter("p", step{hang: true, sent: true})
		ad.kind = in.Kind
		h := newHarness(t, lim, ad)
		if r := h.invoke(t, in); r.Status != 504 || r.Code != persistence.CodeCallDeadlineExceeded {
			t.Fatalf("%s：挂起的调用应在 CallDeadline 到期：%+v", endpointOf(in.Kind), r)
		}
		if rec, _ := h.call(t, "t1", in.CallID); rec.DeadlineAt.Sub(rec.CreatedAt) != lim.CallDeadline {
			t.Fatalf("%s：deadline_at - created_at = %s；期望 %s", endpointOf(in.Kind), rec.DeadlineAt.Sub(rec.CreatedAt), lim.CallDeadline)
		}
	}
}

// 零值 Limits 取默认值：搜索与抓取 120 s，模型调用 300 s。
func TestDeadlineDefaults(t *testing.T) {
	l := Limits{}.withDefaults()
	if l.deadlineFor(upstream.KindSearch) != 120*time.Second || l.deadlineFor(upstream.KindFetch) != 120*time.Second ||
		l.deadlineFor(upstream.KindChat) != 300*time.Second {
		t.Fatalf("默认期限 %+v", l)
	}
}

// ---- 每 turn 的工具调用额度（M4 Plan 12 Task 4；契约 E） ----

// TestToolBudgetExhaustedIs429：Tx1 的 tool_budget_exhausted 映射为 429 且不可重试；Tx1 给出的额度随成功、重放与 429
// 透传到 Result.ToolBudget；不限的任务不带额度。
func TestToolBudgetExhaustedIs429(t *testing.T) {
	ad := newAdapter("p", step{body: `{"results":[]}`}, step{body: `{"results":[]}`})
	ad.kind = upstream.KindSearch
	h := newHarness(t, testLimits(), ad)
	h.store.setToolLimit("t1", 1)

	first := h.invoke(t, searchInv("s1"))
	if first.Status != 200 || first.ToolBudget == nil || *first.ToolBudget != (ToolBudget{Used: 1, Limit: 1}) {
		t.Fatalf("首次搜索 %+v", first)
	}
	over := h.invoke(t, searchInv("s2"))
	if over.Status != 429 || over.Code != persistence.CodeToolBudgetExhausted || over.ToolBudget == nil ||
		*over.ToolBudget != (ToolBudget{Used: 1, Limit: 1}) {
		t.Fatalf("超出额度 %+v", over)
	}
	if retryableReason(persistence.CodeToolBudgetExhausted) {
		t.Fatal("tool_budget_exhausted 不可重试")
	}
	if calls, _, _ := ad.stats(); calls != 1 {
		t.Fatalf("被拒绝的调用不应到达上游，上游被调用 %d 次", calls)
	}
	again := h.invoke(t, searchInv("s1"))
	if !again.Replayed || again.ToolBudget == nil || again.ToolBudget.Used != 1 {
		t.Fatalf("重放 %+v", again)
	}
	if b := h.budget(t, "t1"); b.ToolCallsUsed != 1 {
		t.Fatalf("重放与拒绝不计数：%+v", b)
	}

	unlimited := h.invoke(t, Invoke{TaskID: "t2", AttemptID: "a2", EnvID: "e2", CallID: "u1", Kind: upstream.KindSearch,
		Body: []byte(searchBody)})
	if unlimited.Status != 200 || unlimited.ToolBudget != nil {
		t.Fatalf("不限的任务不带额度：%+v", unlimited)
	}
}

// ==== M4 Plan 14 Task 5：sub-run 的在途上限、合并与取消（规格 §9.7、§11.4、§13.4） ====

// subInv 是 sub-run sr 中的一次调用（call id 以 <sr>/ 开头，与 edge 的校验一致）。
func subInv(base Invoke, sr, id string) Invoke {
	base.SubrunID, base.CallID = sr, sr+"/"+id
	return base
}

// holdSteps 返回 n 个忽略 ctx、只在 gate 关闭时返回的 try。
func holdSteps(n int, gate chan struct{}) []step {
	s := make([]step, n)
	for i := range s {
		s[i] = step{gate: gate, hold: true}
	}
	return s
}

// noMoreEntered 断言在一个短窗口内没有新的上游 try 开始（反向检查：槽位阻塞）。
func noMoreEntered(t *testing.T, a *fakeAdapter, why string) {
	t.Helper()
	select {
	case <-a.entered:
		t.Fatal(why)
	case <-time.After(50 * time.Millisecond):
	}
}

// 每 sub-run 在途 ≤ 2：同一 sub-run 的 5 个不同调用并发，上游同时在途峰值 = 2；等待 sub-run 槽位的调用不预留。
func TestPerSubrunInflight(t *testing.T) {
	gate := make(chan struct{})
	ad := newAdapter("p", holdSteps(5, gate)...)
	h := newHarness(t, testLimits(), ad)
	var outs []<-chan invokeOutcome
	for i := 0; i < 5; i++ {
		outs = append(outs, h.start(context.Background(), subInv(inv("", chatBody), "st1", fmt.Sprintf("c%d", i))))
	}
	waitEntered(t, ad)
	waitEntered(t, ad)
	noMoreEntered(t, ad, "同一 sub-run 第三个 try 没有等待 sub-run 槽位")
	if b := h.budget(t, "t1"); b.ReservedMicro != 200 {
		t.Fatalf("等待槽位的调用不应预留：账本 %+v", b)
	}
	close(gate)
	for i, ch := range outs {
		if r := await(t, ch); r.Status != 200 {
			t.Fatalf("c%d：%+v", i, r)
		}
	}
	if calls, maxIn, _ := ad.stats(); calls != 5 || maxIn != 2 {
		t.Fatalf("上游 %d 次、峰值 %d，期望 5 与 2", calls, maxIn)
	}
	if rec, _ := h.call(t, "t1", "st1/c0"); rec.SubrunID != "st1" {
		t.Fatalf("调用应登记归属 st1：%+v", rec)
	}
	if _, ok := h.c.subSem[subrunSemKey("t1", "st1")]; ok {
		t.Fatal("全部调用结束后 sub-run 槽位应被回收")
	}
}

// 两个 sub-run 各 3 个调用：峰值 = 4（task 上限），sub-run 槽位不跨 sub-run 共享；root 调用不受 sub-run 槽位限制。
func TestPerSubrunInflightTwoSubruns(t *testing.T) {
	gate := make(chan struct{})
	ad := newAdapter("p", holdSteps(6, gate)...)
	h := newHarness(t, testLimits(), ad)
	var outs []<-chan invokeOutcome
	for _, sr := range []string{"st1", "st2"} {
		for i := 0; i < 3; i++ {
			outs = append(outs, h.start(context.Background(), subInv(inv("", chatBody), sr, fmt.Sprintf("c%d", i))))
		}
	}
	for i := 0; i < 4; i++ {
		waitEntered(t, ad)
	}
	noMoreEntered(t, ad, "第五个 try 越过了 task 上限")
	close(gate)
	for i, ch := range outs {
		if r := await(t, ch); r.Status != 200 {
			t.Fatalf("#%d：%+v", i, r)
		}
	}
	if calls, maxIn, _ := ad.stats(); calls != 6 || maxIn != 4 {
		t.Fatalf("上游 %d 次、峰值 %d，期望 6 与 4", calls, maxIn)
	}

	gate2 := make(chan struct{})
	root := newHarness(t, testLimits(), newAdapter("p", holdSteps(3, gate2)...))
	var routs []<-chan invokeOutcome
	for i := 0; i < 3; i++ {
		routs = append(routs, root.start(context.Background(), inv(fmt.Sprintf("root/c%d", i), chatBody)))
	}
	for i := 0; i < 3; i++ {
		waitEntered(t, root.ad)
	}
	close(gate2)
	for _, ch := range routs {
		if r := await(t, ch); r.Status != 200 {
			t.Fatalf("root：%+v", r)
		}
	}
}

// E24：同一 sub-run 内 10 个相同抓取（不同 call_id、同 cache_key）→ 上游 1 次、9 个 coalesced（follower 不占槽位）；
// 另一 sub-run 的同一抓取不合并 → 上游共 2 次。
func TestSubrunCoalesceE24(t *testing.T) {
	gate := make(chan struct{})
	h, fc := newCoalesceHarness(t, upstream.KindFetch, step{gate: gate, body: `{"page":"st1"}`}, step{gate: gate, body: `{"page":"st2"}`})
	ctx := context.Background()
	outs := []<-chan invokeOutcome{h.start(ctx, subInv(fetchInv(""), "st1", "f0"))}
	waitEntered(t, h.ad)
	for i := 1; i < 10; i++ {
		outs = append(outs, h.start(ctx, subInv(fetchInv(""), "st1", fmt.Sprintf("f%d", i))))
	}
	waitJoined(t, fc, 9)
	other := h.start(ctx, subInv(fetchInv(""), "st2", "f0"))
	waitEntered(t, h.ad)
	close(gate)
	for i, ch := range outs {
		if r := await(t, ch); r.Status != 200 || string(r.Body) != `{"page":"st1"}` {
			t.Fatalf("st1/f%d：%+v", i, r)
		}
	}
	if r := await(t, other); r.Status != 200 || string(r.Body) != `{"page":"st2"}` {
		t.Fatalf("st2：%+v", r)
	}
	if calls, _, _ := h.ad.stats(); calls != 2 || fc.coalesced() != 9 {
		t.Fatalf("上游 %d 次、coalesced %d，期望 2 与 9", calls, fc.coalesced())
	}
	for i := 1; i < 10; i++ {
		if rec, _ := h.call(t, "t1", fmt.Sprintf("st1/f%d", i)); rec.Source != SourceCoalesced || rec.SubrunID != "st1" {
			t.Fatalf("follower %d：%+v", i, rec)
		}
	}
}

// CancelSubrun(st1)：取消 st1 的在途 try（已发出 → unknown；未发出 → 释放）、st1 的共享请求（follower 以同因失败）
// 与等待 sub-run 槽位的调用，Worker 均得到 409 subrun_closed；st2 的同时在途调用继续完成；幂等。
func TestCancelSubrun(t *testing.T) {
	gate := make(chan struct{})
	h, fc := newCoalesceHarness(t, upstream.KindFetch,
		step{hang: true, sent: true},             // st1 leader（url a）
		step{hang: true, sent: false},            // st1 第二个调用（url b）
		step{gate: gate, body: `{"page":"st2"}`}) // st2（url a，不与 st1 合并）
	ctx := context.Background()
	urlInv := func(sr, id, u string) Invoke {
		in := subInv(fetchInv(""), sr, id)
		in.Body = []byte(`{"url":"https://example.com/` + u + `"}`)
		return in
	}
	leader := h.start(ctx, urlInv("st1", "a0", "a"))
	waitEntered(t, h.ad)
	follower := h.start(ctx, urlInv("st1", "a1", "a"))
	waitJoined(t, fc, 1)
	second := h.start(ctx, urlInv("st1", "b0", "b"))
	waitEntered(t, h.ad)
	waiting := h.start(ctx, urlInv("st1", "c0", "c"))
	waitBegun(t, h.store, "st1/c0")
	st2 := h.start(ctx, urlInv("st2", "a0", "a"))
	waitEntered(t, h.ad)

	h.c.CancelSubrun("t1", "a1", "st2x") // 不存在的 sub-run：无影响
	h.c.CancelSubrun("t1", "a2", "st1")  // 其他 attempt：无影响
	h.store.closeSubrun("t1", "st1")
	h.c.CancelSubrun("t1", "a1", "st1")
	h.c.CancelSubrun("t1", "a1", "st1") // 幂等
	for name, ch := range map[string]<-chan invokeOutcome{"leader": leader, "follower": follower, "second": second, "waiting": waiting} {
		if r := await(t, ch); r.Status != 409 || r.Code != persistence.CodeSubrunClosed {
			t.Fatalf("%s：%+v", name, r)
		}
	}
	select {
	case o := <-st2:
		t.Fatalf("st2 的调用被取消：%+v", o)
	default:
	}
	close(gate)
	if r := await(t, st2); r.Status != 200 || string(r.Body) != `{"page":"st2"}` {
		t.Fatalf("st2：%+v", r)
	}
	if rec, _ := h.call(t, "t1", "st1/a0"); rec.State != StateUnknown {
		t.Fatalf("已发出的 leader 应为 unknown：%+v", rec)
	}
	for _, id := range []string{"st1/a1", "st1/b0", "st1/c0"} {
		if rec, _ := h.call(t, "t1", id); rec.State != StateFailed || rec.FailReason != persistence.CodeSubrunClosed {
			t.Fatalf("%s：%+v", id, rec)
		}
	}
	if _, tries := h.call(t, "t1", "st1/c0"); len(tries) != 0 {
		t.Fatalf("等待槽位的调用不应有 try：%+v", tries)
	}
	if b := h.budget(t, "t1"); b.ReservedMicro != 0 || b.UnknownMicro != 100 || b.SpentMicro != 100 {
		t.Fatalf("账本 %+v：期望 unknown 100（st1 leader）、spent 100（st2）", b)
	}
	if retryableReason(persistence.CodeSubrunClosed) {
		t.Fatal("subrun_closed 不可重试")
	}
	if _, ok := h.c.bySubrun[subrunKey{"t1", "a1", "st1"}]; ok {
		t.Fatal("结束的 job 应从 bySubrun 移除")
	}
}

// sub-run 的错误码映射：subrun_budget_exhausted → 402（Invoke 经 ReserveTry 被拒），subrun_closed → 409
// （CheckAccess 带 sub-run）；SubrunBudget 透传 sub-run 层账本。
func TestSubrunCodesAndAccess(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.store.mu.Lock()
	h.store.subExhausted[callKey{"t1", "st1"}] = true
	h.store.mu.Unlock()
	if r := h.invoke(t, subInv(inv("", chatBody), "st1", "c1")); r.Status != 402 || r.Code != persistence.CodeSubrunBudgetExhausted {
		t.Fatalf("sub-run 预算耗尽：%+v", r)
	}
	if r := h.invoke(t, subInv(inv("", chatBody), "st2", "c1")); r.Status != 200 {
		t.Fatalf("其他 sub-run 不受影响：%+v", r)
	}
	if calls, _, _ := h.ad.stats(); calls != 1 {
		t.Fatalf("被拒的调用不应到达上游：%d", calls)
	}
	ctx := context.Background()
	if r, err := h.c.CheckAccess(ctx, "t1", "a1", "st1"); err != nil || r.Code != "" {
		t.Fatalf("open 的 sub-run：%+v / %v", r, err)
	}
	h.store.closeSubrun("t1", "st1")
	if r, err := h.c.CheckAccess(ctx, "t1", "a1", "st1"); err != nil || r.Status != 409 || r.Code != persistence.CodeSubrunClosed {
		t.Fatalf("closed 的 sub-run：%+v / %v", r, err)
	}
	if r, err := h.c.CheckAccess(ctx, "t1", "a1", ""); err != nil || r.Code != "" {
		t.Fatalf("root：%+v / %v", r, err)
	}
	if r := h.invoke(t, subInv(inv("", chatBody), "st1", "c2")); r.Status != 409 || r.Code != persistence.CodeSubrunClosed {
		t.Fatalf("closed 的 sub-run 的新调用：%+v", r)
	}
	capMicro := int64(500)
	h.store.mu.Lock()
	h.store.subBudgets[callKey{"t1", "st2"}] = SubrunBudget{CapMicro: &capMicro, SpentMicro: 100}
	h.store.mu.Unlock()
	if b, err := h.c.SubrunBudget(ctx, "t1", "st2"); err != nil || b.CapMicro == nil || *b.CapMicro != 500 || b.Available() != 400 {
		t.Fatalf("SubrunBudget：%+v / %v", b, err)
	}
	if _, err := h.c.SubrunBudget(ctx, "t1", "nope"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的 sub-run 账本：%v", err)
	}
	if Limits.withDefaults(Limits{}).PerSubrunInflight != 2 {
		t.Fatal("PerSubrunInflight 默认应为 2")
	}
}

// ==== M4 Plan 15 Task 8：exec 调度（§10；D6、D7、D8、D12、D16） ====

// opLog 记录 exec 流程中各 fake 的操作序列（slot、ExecStore、ExecEnvs），用于断言顺序。
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) add(op string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ops = append(l.ops, op)
}

func (l *opLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ops...)
}

func (l *opLog) count(op string) int {
	n := 0
	for _, o := range l.list() {
		if o == op {
			n++
		}
	}
	return n
}

// ---- 内存 ExecStore：语义同 Task 7 的 postgres 契约（共用 fakeStore 的调用记录与锁） ----

type fakeExecTry struct {
	try     ExecTry
	amount  int64
	rstate  string // held | settled | charged_unknown
	started bool
	settled ExecSettlement
}

type fakeExecStore struct {
	*fakeStore
	log      *opLog
	quotas   map[string]*ExecQuota
	etries   map[callKey][]*fakeExecTry
	byRes    map[string]*fakeExecTry
	stopped  map[string]bool // env_id → stopped_at 已记录
	reserves []ReserveExecRequest
}

func newFakeExecStore(s *fakeStore, log *opLog) *fakeExecStore {
	return &fakeExecStore{fakeStore: s, log: log, quotas: map[string]*ExecQuota{}, etries: map[callKey][]*fakeExecTry{},
		byRes: map[string]*fakeExecTry{}, stopped: map[string]bool{}}
}

func (s *fakeExecStore) markStopped(envID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped[envID] = true
}

func (s *fakeExecStore) ReserveExec(_ context.Context, r ReserveExecRequest) (ExecTry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.add("reserve")
	if ft := s.byRes[r.ReservationID]; ft != nil {
		return ft.try, nil
	}
	if code := admit(s.facts(r.TaskID, r.AttemptID, r.SubrunID)); code != "" {
		return ExecTry{}, rejected(code)
	}
	q := s.quotas[r.TaskID]
	if q == nil {
		q = &ExecQuota{CountLimit: r.Policy.CountLimit, CPULimitUsec: r.Policy.CPULimitUsec, WallLimitMs: r.Policy.WallLimitMs}
		s.quotas[r.TaskID] = q
	}
	k := callKey{r.TaskID, r.CallID}
	rec := s.calls[k]
	switch {
	case rec == nil:
		return ExecTry{}, persistence.ErrNotFound
	case rec.Endpoint != ExecEndpoint:
		return ExecTry{}, persistence.ErrConflict
	case rec.SubrunID != r.SubrunID:
		return ExecTry{}, rejected(persistence.CodeFingerprintMismatch)
	}
	for _, ft := range s.etries[k] {
		if ft.rstate == "held" || !s.stopped[ft.try.EnvID] {
			return ExecTry{}, rejected(persistence.CodeCallInProgress)
		}
	}
	switch {
	case rec.State == StateCompleted:
		return ExecTry{}, persistence.ErrConflict
	case !time.Now().Before(rec.DeadlineAt):
		return ExecTry{}, rejected(persistence.CodeCallDeadlineExceeded)
	case rec.TriesUsed >= r.MaxTries:
		return ExecTry{}, rejected(persistence.CodeTriesExhausted)
	}
	var inflight int64
	for ck, ts := range s.etries {
		for _, ft := range ts {
			if ck.taskID == r.TaskID && ft.rstate == "held" {
				inflight++
			}
		}
	}
	switch {
	case q.Blocked:
		return ExecTry{}, rejected(CodeExecBlocked)
	case q.CountUsed+inflight >= q.CountLimit:
		return ExecTry{}, rejected(CodeExecQuotaExhausted)
	case q.CPUAvailable() < r.CPUEstimateUsec:
		return ExecTry{}, rejected(CodeExecCPUExhausted)
	case q.WallSpentMs+r.WallMs > q.WallLimitMs:
		return ExecTry{}, rejected(CodeExecWallExhausted)
	}
	tryNo := rec.TriesUsed + 1
	ft := &fakeExecTry{try: ExecTry{Try: Try{TaskID: r.TaskID, CallID: r.CallID, TryNo: tryNo, ReservationID: r.ReservationID,
		AttemptID: r.AttemptID}, EnvID: ExecEnvID(r.TaskID, r.CallID, tryNo)}, amount: r.CPUEstimateUsec, rstate: "held"}
	s.etries[k] = append(s.etries[k], ft)
	s.byRes[r.ReservationID] = ft
	s.reserves = append(s.reserves, r)
	q.CPUReservedUsec += r.CPUEstimateUsec
	rec.State, rec.TriesUsed, rec.ResolvingSince = StateInFlight, tryNo, nil
	return ft.try, nil
}

func (s *fakeExecStore) MarkExecStarting(_ context.Context, t ExecTry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.add("mark")
	ft := s.byRes[t.ReservationID]
	if ft == nil {
		return persistence.ErrNotFound
	}
	rec := s.calls[callKey{t.TaskID, t.CallID}]
	if code := admit(s.facts(t.TaskID, t.AttemptID, rec.SubrunID)); code != "" {
		return rejected(code)
	}
	if ft.rstate != "held" {
		return persistence.ErrConflict
	}
	ft.started = true
	return nil
}

func (s *fakeExecStore) SettleExec(_ context.Context, st ExecSettlement) (CallRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.add("settle")
	ft := s.byRes[st.Try.ReservationID]
	if ft == nil {
		return CallRecord{}, persistence.ErrNotFound
	}
	rec := s.calls[callKey{st.Try.TaskID, st.Try.CallID}]
	switch st.Outcome {
	case ExecCompleted, ExecTimedOut:
		if len(st.ResultSHA256) != 64 {
			return CallRecord{}, persistence.ErrInvalid
		}
	case ExecCancelled, ExecStartFailed, ExecUnknown:
		if st.ResultSHA256 != "" || len(st.Outputs) != 0 || (st.Outcome == ExecStartFailed && st.Started) {
			return CallRecord{}, persistence.ErrInvalid
		}
	default:
		return CallRecord{}, persistence.ErrInvalid
	}
	if ft.rstate != "held" {
		return *rec, nil
	}
	q := s.quotas[st.Try.TaskID]
	q.CPUReservedUsec -= ft.amount
	switch {
	case st.Outcome == ExecUnknown:
		q.CPUUnknownUsec += ft.amount
		ft.rstate = "charged_unknown"
	case st.CPUKnown:
		q.CPUSpentUsec += st.CPUUsec
		ft.rstate = "settled"
	default:
		q.CPUSpentUsec += ft.amount
		ft.rstate = "settled"
	}
	if st.Started {
		q.CountUsed++
	}
	q.WallSpentMs += st.WallMs
	q.Blocked = q.Blocked || q.CPUSpentUsec > q.CPULimitUsec
	ft.settled = st
	switch st.Outcome {
	case ExecCompleted, ExecTimedOut:
		rec.State, rec.ResultRef = StateCompleted, st.ResultSHA256
		for _, o := range st.Outputs {
			s.scoped[callKey{st.Try.TaskID, o.SHA256}] = true
		}
	case ExecCancelled:
		rec.State, rec.FailReason = StateFailed, CodeExecCancelled
	case ExecStartFailed:
		rec.State, rec.FailReason = StateFailed, CodeExecStartFailed
	case ExecUnknown:
		rec.State = StateUnknown
	}
	return *rec, nil
}

func (s *fakeExecStore) LoadExecQuota(_ context.Context, taskID string) (ExecQuota, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.quotas[taskID]
	if q == nil {
		return ExecQuota{}, persistence.ErrNotFound
	}
	return *q, nil
}

// tries 返回调用的 exec try（按 try_no）。
func (s *fakeExecStore) tries(taskID, callID string) []fakeExecTry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []fakeExecTry
	for _, ft := range s.etries[callKey{taskID, callID}] {
		out = append(out, *ft)
	}
	return out
}

func (s *fakeExecStore) nReserves() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.reserves)
}

// ---- 可脚本化的 exec 环境 ----

// execScript 描述一个 exec 环境（按 Create 的先后取用）中 workload 的行为。
type execScript struct {
	exitCode       int
	stdout, stderr string
	bigOut         int64         // stdout 与 stderr 各交替写入这么多字节（无缓冲管道：只顺序读一个会挂起）
	hang           bool          // 写完输出后挂起，直至环境被 Stop
	startErr       error         // Start 的返回
	waitErr        error         // Wait 的返回
	stopBlocked    bool          // Stop 期限内未确认停止
	createErr      error         // Create 的返回
	createGate     chan struct{} // 非 nil：Create 阻塞至关闭
	outputs        map[string]string
	skipped        []provider.SkippedOutput
}

type fakeExecHandle struct {
	log            *opLog
	stdout, stderr *io.PipeReader
	done           chan struct{}
	status         provider.ExitStatus
	waitErr        error
}

func (h *fakeExecHandle) Stdin() io.WriteCloser { return fakeStdin{h.log} }
func (h *fakeExecHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *fakeExecHandle) Stderr() io.ReadCloser { return h.stderr }
func (h *fakeExecHandle) Wait() (provider.ExitStatus, error) {
	<-h.done
	return h.status, h.waitErr
}
func (h *fakeExecHandle) Terminate(time.Duration) error { return nil }

type fakeStdin struct{ log *opLog }

func (fakeStdin) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (s fakeStdin) Close() error {
	s.log.add("stdin_closed")
	return nil
}

type fakeExecEnv struct {
	id, inDir string
	sc        execScript
	killed    chan struct{}
	killOnce  sync.Once
	h         *fakeExecHandle
	staged    map[string]string      // Start 时 /in 的内容（相对路径 → 内容）
	modes     map[string]os.FileMode // Start 时 /in 中文件与目录的权限
}

func (e *fakeExecEnv) kill() { e.killOnce.Do(func() { close(e.killed) }) }

type fakeEnvs struct {
	t       *testing.T
	log     *opLog
	store   *fakeExecStore
	mu      sync.Mutex
	scripts []execScript
	n       int
	envs    map[string]*fakeExecEnv
	reqs    []ExecEnvRequest
	specs   []provider.ExecSpec
	files   []*os.File
	created chan string // 每次 Create 开始时发送 env_id
	started chan string // 每次 Start 成功时发送 env_id
	holds   map[string]int
}

// Hold 记录持有；Create、Stop、Diag、OpenOutputs 与 Cleanup 都必须在持有期间调用（requireHeld）。
func (f *fakeEnvs) Hold(envID string) func() {
	f.mu.Lock()
	if f.holds == nil {
		f.holds = map[string]int{}
	}
	f.holds[envID]++
	f.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.holds[envID]--; f.holds[envID] == 0 {
				delete(f.holds, envID)
			}
		})
	}
}

// requireHeld：cleanup loop 只在环境不被持有时销毁它，因此停止之后的诊断与收集必须仍在持有期间（E37 CI 偶发）。
func (f *fakeEnvs) requireHeld(op, envID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holds[envID] == 0 {
		f.t.Errorf("%s(%s) 不在 Hold 期间：cleanup loop 可能已销毁环境", op, envID)
	}
}

// heldCount 返回仍未释放的持有数。
func (f *fakeEnvs) heldCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, v := range f.holds {
		n += v
	}
	return n
}

func (f *fakeEnvs) env(id string) *fakeExecEnv {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.envs[id]
}

func (f *fakeEnvs) Create(_ context.Context, r ExecEnvRequest) (string, error) {
	f.log.add("create")
	f.requireHeld("Create", r.EnvID)
	f.mu.Lock()
	sc := execScript{stdout: "ok\n"}
	if f.n < len(f.scripts) {
		sc = f.scripts[f.n]
	}
	f.n++
	f.reqs = append(f.reqs, r)
	f.mu.Unlock()
	f.created <- r.EnvID
	if sc.createGate != nil {
		<-sc.createGate
	}
	if sc.createErr != nil {
		return "", sc.createErr
	}
	in := filepath.Join(f.t.TempDir(), "in")
	if err := os.Mkdir(in, 0o755); err != nil {
		return "", err
	}
	f.mu.Lock()
	f.envs[r.EnvID] = &fakeExecEnv{id: r.EnvID, inDir: in, sc: sc, killed: make(chan struct{})}
	f.mu.Unlock()
	return in, nil
}

func (f *fakeEnvs) Start(_ context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	f.log.add("start")
	e := f.env(envID)
	staged, modes := map[string]string{}, map[string]os.FileMode{}
	err := filepath.WalkDir(e.inDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(e.inDir, p)
		if err != nil || rel == "." {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		modes[filepath.ToSlash(rel)] = fi.Mode().Perm()
		if !d.IsDir() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			staged[filepath.ToSlash(rel)] = string(b)
		}
		return nil
	})
	if err != nil {
		f.t.Errorf("读取 /in 暂存：%v", err)
	}
	f.mu.Lock()
	f.specs = append(f.specs, spec)
	e.staged, e.modes = staged, modes
	f.mu.Unlock()
	if e.sc.startErr != nil {
		return nil, e.sc.startErr
	}
	or, ow := io.Pipe()
	er, ew := io.Pipe()
	h := &fakeExecHandle{log: f.log, stdout: or, stderr: er, done: make(chan struct{}), waitErr: e.sc.waitErr}
	e.h = h
	sc := e.sc
	go func() {
		defer close(h.done)
		write := func(w *io.PipeWriter, s string) {
			if s != "" {
				if _, err := w.Write([]byte(s)); err != nil {
					return // 读取端已关闭：与真实管道相同，丢弃
				}
			}
		}
		write(ow, sc.stdout)
		write(ew, sc.stderr)
		if sc.bigOut > 0 {
			chunk := bytes.Repeat([]byte("x"), 64<<10)
			for n := int64(0); n < sc.bigOut; n += int64(len(chunk)) {
				if _, err := ow.Write(chunk); err != nil {
					break
				}
				if _, err := ew.Write(chunk); err != nil {
					break
				}
			}
		}
		if sc.hang {
			<-e.killed
			h.status = provider.ExitStatus{Signal: syscall.Signal(9)}
		} else {
			h.status = provider.ExitStatus{Code: sc.exitCode}
		}
		if err := ow.Close(); err != nil {
			f.t.Errorf("关闭 stdout：%v", err)
		}
		if err := ew.Close(); err != nil {
			f.t.Errorf("关闭 stderr：%v", err)
		}
	}()
	f.started <- envID
	return h, nil
}

func (f *fakeEnvs) Stop(_ context.Context, envID string) (ExecStop, error) {
	f.log.add("stop")
	f.requireHeld("Stop", envID)
	e := f.env(envID)
	if e == nil { // Create 失败：没有残留
		f.store.markStopped(envID)
		return ExecStop{Stopped: true, Recorded: true}, nil
	}
	if e.sc.stopBlocked {
		return ExecStop{Blocked: true}, nil
	}
	e.kill()
	if e.h != nil {
		<-e.h.done
	}
	f.store.markStopped(envID)
	return ExecStop{Stopped: true, Recorded: true}, nil
}

func (f *fakeEnvs) Diag(_ context.Context, envID string) (provider.ResourceDiag, error) {
	f.log.add("diag")
	f.requireHeld("Diag", envID)
	return provider.ResourceDiag{CPUUsageUsec: 81234}, nil
}

func (f *fakeEnvs) OpenOutputs(_ context.Context, envID string, max int) ([]provider.OutputFile, []provider.SkippedOutput, error) {
	f.log.add("outputs")
	f.requireHeld("OpenOutputs", envID)
	if max != provider.MaxOutputFiles {
		f.t.Errorf("OpenOutputs max = %d，期望 %d", max, provider.MaxOutputFiles)
	}
	e := f.env(envID)
	var paths []string
	for p := range e.sc.outputs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	dir := f.t.TempDir()
	var out []provider.OutputFile
	for i, p := range paths {
		name := filepath.Join(dir, fmt.Sprint(i))
		if err := os.WriteFile(name, []byte(e.sc.outputs[p]), 0o600); err != nil {
			return nil, nil, err
		}
		fh, err := os.Open(name)
		if err != nil {
			return nil, nil, err
		}
		f.mu.Lock()
		f.files = append(f.files, fh)
		f.mu.Unlock()
		out = append(out, provider.OutputFile{Path: p, Size: int64(len(e.sc.outputs[p])), File: fh})
	}
	return out, e.sc.skipped, nil
}

// Cleanup 与 provider 的 Destroy 相同：仍有打开的输出文件时 EBUSY（记为测试失败）。
func (f *fakeEnvs) Cleanup(_ context.Context, envID string) error {
	f.log.add("cleanup")
	f.requireHeld("Cleanup", envID)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, fh := range f.files {
		if _, err := fh.Stat(); err == nil {
			f.t.Errorf("Cleanup(%s) 时输出文件 %s 仍未关闭（Destroy 会 EBUSY）", envID, fh.Name())
			return syscall.EBUSY
		}
	}
	return nil
}

// killAll 让挂起的 workload 结束（测试清理；Stop Blocked 的环境不会被 Coordinator 停止）。
func (f *fakeEnvs) killAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.envs {
		e.kill()
	}
}

func (f *fakeEnvs) lastSpec(t *testing.T) provider.ExecSpec {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.specs) == 0 {
		t.Fatal("Start 没有被调用")
	}
	return f.specs[len(f.specs)-1]
}

// fakeSlots 是每任务 per 个的 exec slot（admission.ExecGate 的简化）。
type fakeSlots struct {
	log  *opLog
	per  int
	mu   sync.Mutex
	sems map[string]chan struct{}
}

func (s *fakeSlots) Acquire(ctx context.Context, taskID string) (func(), error) {
	s.mu.Lock()
	ch := s.sems[taskID]
	if ch == nil {
		ch = make(chan struct{}, s.per)
		s.sems[taskID] = ch
	}
	s.mu.Unlock()
	select {
	case ch <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.log.add("acquire")
	var once sync.Once
	return func() {
		once.Do(func() {
			<-ch
			s.log.add("release")
		})
	}, nil
}

// ---- 装配 ----

type execHarness struct {
	*harness
	es    *fakeExecStore
	envs  *fakeEnvs
	slots *fakeSlots
	ops   *opLog
	cfg   ExecConfig
}

func newExecHarness(t *testing.T, mod func(*ExecConfig), scripts ...execScript) *execHarness {
	t.Helper()
	ops := &opLog{}
	base := &harness{store: newFakeStore(t), ad: newAdapter("p1"), blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{},
		logs: &syncBuffer{}}
	es := newFakeExecStore(base.store, ops)
	envs := &fakeEnvs{t: t, log: ops, store: es, scripts: scripts, envs: map[string]*fakeExecEnv{},
		created: make(chan string, 64), started: make(chan string, 64)}
	slots := &fakeSlots{log: ops, per: 2, sems: map[string]chan struct{}{}}
	cfg := ExecConfig{Store: es, Envs: envs, Slots: slots, Policy: ExecPolicy{CountLimit: 50, CPULimitUsec: 600_000_000, WallLimitMs: 1_800_000},
		Default: ExecLimits{WallMs: 60_000, MemoryBytes: 512 << 20}, Max: ExecLimits{WallMs: 300_000, MemoryBytes: 1 << 30},
		QueueTimeout: 2 * time.Second, ImageDigest: "img-1", StopTimeout: 2 * time.Second}
	if mod != nil {
		mod(&cfg)
	}
	h := &execHarness{harness: base, es: es, envs: envs, slots: slots, ops: ops, cfg: cfg}
	h.c = h.execCoordinator(t, cfg)
	return h
}

func (h *execHarness) execCoordinator(t *testing.T, cfg ExecConfig) *Coordinator {
	t.Helper()
	c, err := New(Config{Store: h.store, Adapters: []upstream.Adapter{h.ad}, Blobs: h.blobs, Events: h.events, Limits: testLimits(),
		Logger: slog.New(slog.NewJSONHandler(h.logs, nil)), Exec: &cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	t.Cleanup(h.envs.killAll) // 先于 Close 运行（LIFO）
	return c
}

// execBody 构造 exec 请求体：code 按 JSON 编码，extra 是附加的成员（以逗号开头）。
func execBody(code, extra string) string {
	b, err := json.Marshal(code)
	if err != nil {
		panic(err)
	}
	return `{"language":"python3","code":` + string(b) + extra + `}`
}

func xinv(callID, body string) ExecInvoke {
	return ExecInvoke{TaskID: "t1", AttemptID: "a1", CallID: callID, Body: []byte(body)}
}

func (h *execHarness) exec(t *testing.T, in ExecInvoke) Result {
	t.Helper()
	r, err := h.c.Exec(context.Background(), in)
	if err != nil {
		t.Fatalf("Exec(%s)：%v", in.CallID, err)
	}
	return r
}

func (h *execHarness) startExec(in ExecInvoke) <-chan invokeOutcome {
	ch := make(chan invokeOutcome, 1)
	go func() {
		r, err := h.c.Exec(context.Background(), in)
		ch <- invokeOutcome{r, err}
	}()
	return ch
}

func waitChan(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("等待 %s 超时", what)
	}
	return ""
}

// seedInput 保存一个输入 blob 并授权到任务 t1 的 scope。
func (h *execHarness) seedInput(t *testing.T, content string) string {
	t.Helper()
	ref, err := h.blobs.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	h.store.scope("t1", ref.SHA256)
	return ref.SHA256
}

type execResultView struct {
	Status string `json:"status"`
	Exit   *struct {
		Code   int `json:"code"`
		Signal int `json:"signal"`
	} `json:"exit"`
	Diag *struct {
		OOMKillDelta uint64 `json:"oom_kill_delta"`
		OOMObserved  bool   `json:"oom_observed"`
		CPUUsageUsec uint64 `json:"cpu_usage_usec"`
	} `json:"diag"`
	Stdout            string `json:"stdout"`
	StdoutTruncated   bool   `json:"stdout_truncated"`
	StdoutInvalidUTF8 bool   `json:"stdout_invalid_utf8"`
	Stderr            string `json:"stderr"`
	StderrTruncated   bool   `json:"stderr_truncated"`
	StderrInvalidUTF8 bool   `json:"stderr_invalid_utf8"`
	Outputs           []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	} `json:"outputs"`
	SkippedOutputs []struct {
		Path   string `json:"path"`
		Reason string `json:"reason"`
	} `json:"skipped_outputs"`
	QueueMs int64 `json:"queue_ms"`
	WallMs  int64 `json:"wall_ms"`
	Limits  struct {
		WallMs      int64 `json:"wall_ms"`
		MemoryBytes int64 `json:"memory_bytes"`
	} `json:"limits"`
	ImageDigest string `json:"image_digest"`
}

func parseExecResult(t *testing.T, r Result) execResultView {
	t.Helper()
	if r.Status != 200 {
		t.Fatalf("exec 结果 %d %s，期望 200", r.Status, r.Code)
	}
	var v execResultView
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("结果不是 JSON：%v\n%s", err, r.Body)
	}
	return v
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- 1. 请求校验与 limits 截断 ----

func TestExecRequestValidation(t *testing.T) {
	h := newExecHarness(t, nil)
	sha := strings.Repeat("a", 64)
	in := func(path string) string { return `{"sha256":"` + sha + `","path":"` + path + `"}` }
	cases := map[string]struct{ body, code string }{
		"language":     {`{"language":"bash","code":"echo 1"}`, upstream.CodeInvalidRequest},
		"缺少 code":      {`{"language":"python3"}`, upstream.CodeInvalidRequest},
		"code 超长":      {execBody(strings.Repeat("x", 256<<10+1), ""), upstream.CodeInvalidRequest},
		"..":           {execBody("x", `,"inputs":[`+in("../x")+`]`), upstream.CodeInvalidRequest},
		"绝对路径":         {execBody("x", `,"inputs":[`+in("/abs")+`]`), upstream.CodeInvalidRequest},
		"不规范":          {execBody("x", `,"inputs":[`+in("a//b")+`]`), upstream.CodeInvalidRequest},
		"重复路径":         {execBody("x", `,"inputs":[`+in("a")+`,`+in("a")+`]`), upstream.CodeInvalidRequest},
		"文件与目录冲突":      {execBody("x", `,"inputs":[`+in("a")+`,`+in("a/b")+`]`), upstream.CodeInvalidRequest},
		".agentbox":    {execBody("x", `,"inputs":[`+in(".agentbox/x")+`]`), upstream.CodeInvalidRequest},
		"sha 非法":       {execBody("x", `,"inputs":[{"sha256":"AB","path":"a"}]`), upstream.CodeInvalidRequest},
		"未知字段":         {execBody("x", `,"argv":["sh"]`), upstream.CodeUnsupportedField},
		"inputs 内未知字段": {execBody("x", `,"inputs":[{"sha256":"`+sha+`","path":"a","mode":1}]`), upstream.CodeUnsupportedField},
		"limits 内未知字段": {execBody("x", `,"limits":{"cpu":2}`), upstream.CodeUnsupportedField},
		"wall_ms 非正":   {execBody("x", `,"limits":{"wall_ms":0}`), upstream.CodeInvalidRequest},
		"重复属性名":        {`{"language":"python3","code":"x","code":"y"}`, CodeDuplicateJSONKey},
		"非对象":          {`[1]`, upstream.CodeInvalidRequest},
	}
	for name, tc := range cases {
		r := h.exec(t, xinv("c-"+name, tc.body))
		if r.Code != tc.code || r.Status != 400 {
			t.Errorf("%s：得到 %d %s，期望 400 %s", name, r.Status, r.Code, tc.code)
		}
	}
	if n := len(h.store.calls); n != 0 {
		t.Fatalf("被拒绝的请求不应登记调用，得到 %d 个", n)
	}
	if ops := h.ops.list(); len(ops) != 0 {
		t.Fatalf("被拒绝的请求不应有任何 exec 操作：%v", ops)
	}

	// 截断：超过上限的 limits 按上限生效，截断值进入指纹与响应（截断前后的两个请求指纹相同）。
	r := h.exec(t, xinv("trunc", execBody("print(1)", `,"limits":{"wall_ms":999999,"memory_bytes":4294967296}`)))
	v := parseExecResult(t, r)
	if v.Limits.WallMs != 300_000 || v.Limits.MemoryBytes != 1<<30 {
		t.Fatalf("limits = %+v，期望按上限截断", v.Limits)
	}
	if got := h.envs.reqs[0].MemoryBytes; got != 1<<30 {
		t.Fatalf("ExecEnvRequest.MemoryBytes = %d，期望生效值 1 GiB", got)
	}
	r2 := h.exec(t, xinv("trunc", execBody("print(1)", `,"limits":{"wall_ms":300000,"memory_bytes":1073741824}`)))
	if !r2.Replayed || r2.BlobSHA256 != r.BlobSHA256 {
		t.Fatalf("截断后相同的请求应重放：%+v", r2)
	}
	// 默认值。
	v3 := parseExecResult(t, h.exec(t, xinv("dflt", execBody("print(1)", ""))))
	if v3.Limits.WallMs != 60_000 || v3.Limits.MemoryBytes != 512<<20 || v3.ImageDigest != "img-1" {
		t.Fatalf("默认 limits / image_digest：%+v %s", v3.Limits, v3.ImageDigest)
	}
}

// ---- 2. 指纹：输入顺序无关；image_digest 变化 → 409 fingerprint_mismatch ----

func TestExecFingerprint(t *testing.T) {
	h := newExecHarness(t, nil)
	a, b := h.seedInput(t, "A"), h.seedInput(t, "B")
	ab := execBody("x", `,"inputs":[{"sha256":"`+a+`","path":"a"},{"sha256":"`+b+`","path":"b"}]`)
	ba := execBody("x", `,"inputs":[{"path":"b","sha256":"`+b+`"},{"sha256":"`+a+`","path":"a"}]`)
	r1 := h.exec(t, xinv("c1", ab))
	parseExecResult(t, r1)
	r2 := h.exec(t, xinv("c1", ba))
	if !r2.Replayed || r2.BlobSHA256 != r1.BlobSHA256 {
		t.Fatalf("输入顺序不同应得到相同指纹（重放）：%+v", r2)
	}
	cfg := h.cfg
	cfg.ImageDigest = "img-2"
	c2 := h.execCoordinator(t, cfg)
	r3, err := c2.Exec(context.Background(), xinv("c1", ab))
	if err != nil {
		t.Fatal(err)
	}
	if r3.Status != 409 || r3.Code != persistence.CodeFingerprintMismatch {
		t.Fatalf("image_digest 变化：%d %s，期望 409 fingerprint_mismatch", r3.Status, r3.Code)
	}
	if ev := h.events.list(); len(ev) != 1 || !strings.Contains(ev[0], "endpoint=/v1/exec") {
		t.Fatalf("replay_divergence 事件：%v", ev)
	}
}

// ---- 3. 正常路径：操作顺序、argv/Dir/Env、/in 暂存、结果 JSON、结算与授权 ----

func TestExecHappyPath(t *testing.T) {
	h := newExecHarness(t, nil, execScript{stdout: "hi\n", stderr: "bad\xff!", exitCode: 3,
		outputs: map[string]string{"r/a.txt": "hello world!", "z": ""},
		skipped: []provider.SkippedOutput{{Path: "l", Reason: provider.SkipSymlink}}})
	in := h.seedInput(t, "x,y\n1,2\n")
	code := "print('hi')\n"
	r := h.exec(t, xinv("c1", execBody(code, `,"inputs":[{"sha256":"`+in+`","path":"data/x.csv"}]`)))
	v := parseExecResult(t, r)

	want := []string{"acquire", "reserve", "create", "mark", "start", "stdin_closed", "stop", "diag", "outputs", "cleanup", "settle", "release"}
	if got := h.ops.list(); !equalStrings(got, want) {
		t.Fatalf("操作序列\n得到 %v\n期望 %v", got, want)
	}
	if n := h.envs.heldCount(); n != 0 {
		t.Fatalf("try 结束后仍有 %d 个环境持有未释放（cleanup loop 将永远跳过它）", n)
	}
	spec := h.envs.lastSpec(t)
	if !equalStrings(spec.Argv, []string{"python3", "-I", "-B", "/in/.agentbox/main.py"}) || spec.Dir != "/out" ||
		!equalStrings(spec.Env, []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=/tmp", "LANG=C.UTF-8",
			"PYTHONDONTWRITEBYTECODE=1", "PYTHONUNBUFFERED=1"}) {
		t.Fatalf("ExecSpec = %+v", spec)
	}
	env := h.envs.env(h.envs.reqs[0].EnvID)
	if env.staged[".agentbox/main.py"] != code || env.staged["data/x.csv"] != "x,y\n1,2\n" || len(env.staged) != 2 {
		t.Fatalf("/in 暂存内容：%v", env.staged)
	}
	for p, m := range env.modes {
		wantMode := os.FileMode(0o444)
		if p == ".agentbox" || p == "data" {
			wantMode = 0o755
		}
		if m != wantMode {
			t.Errorf("/in/%s 的权限 %v，期望 %v", p, m, wantMode)
		}
	}
	req := h.envs.reqs[0]
	if req.AttemptID != "a1" || req.EnvID != ExecEnvID("t1", "c1", 1) || req.MemoryBytes != 512<<20 {
		t.Fatalf("ExecEnvRequest = %+v", req)
	}

	if v.Status != "completed" || v.Exit == nil || v.Exit.Code != 3 || v.Exit.Signal != 0 {
		t.Fatalf("status/exit：%+v %+v", v.Status, v.Exit)
	}
	if v.Diag == nil || v.Diag.CPUUsageUsec != 81234 || v.Stdout != "hi\n" || v.StdoutTruncated || v.StdoutInvalidUTF8 {
		t.Fatalf("diag/stdout：%+v %q", v.Diag, v.Stdout)
	}
	if v.Stderr != "bad�!" || !v.StderrInvalidUTF8 || v.StderrTruncated {
		t.Fatalf("stderr 非法 UTF-8 应替换为 U+FFFD 并置标志：%q %v", v.Stderr, v.StderrInvalidUTF8)
	}
	sum := sha256.Sum256([]byte("hello world!"))
	if len(v.Outputs) != 2 || v.Outputs[0].Path != "r/a.txt" || v.Outputs[0].SHA256 != hex.EncodeToString(sum[:]) ||
		v.Outputs[0].Size != 12 || v.Outputs[1].Path != "z" || v.Outputs[1].Size != 0 {
		t.Fatalf("outputs = %+v", v.Outputs)
	}
	if len(v.SkippedOutputs) != 1 || v.SkippedOutputs[0].Path != "l" || v.SkippedOutputs[0].Reason != "symlink" {
		t.Fatalf("skipped_outputs = %+v", v.SkippedOutputs)
	}
	if v.QueueMs < 0 || v.WallMs < 0 || v.ImageDigest != "img-1" {
		t.Fatalf("queue/wall/image：%+v", v)
	}
	// 输出 blob 内容。
	rc, err := h.blobs.Open(v.Outputs[0].SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(rc); string(b) != "hello world!" {
		t.Fatalf("输出 blob 内容 %q", b)
	}

	tries := h.es.tries("t1", "c1")
	if len(tries) != 1 {
		t.Fatalf("try 数 %d", len(tries))
	}
	st := tries[0].settled
	if st.Outcome != ExecCompleted || !st.Started || !st.CPUKnown || st.CPUUsec != 81234 || st.ResultSHA256 != r.BlobSHA256 ||
		st.ResultSize != int64(len(r.Body)) || len(st.Outputs) != 2 || st.Outputs[0].Size != 12 {
		t.Fatalf("结算 %+v", st)
	}
	// CPU 预留 = 1 核 × 60 s × 1.1（usec）。
	if res := h.es.reserves[0]; res.CPUEstimateUsec != 66_000_000 || res.WallMs != 60_000 || res.MaxTries != 3 || res.Policy.CountLimit != 50 {
		t.Fatalf("ReserveExec = %+v", res)
	}
	rec, _ := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.Endpoint != ExecEndpoint || rec.ResultRef != r.BlobSHA256 {
		t.Fatalf("journal %+v", rec)
	}
	if !rec.DeadlineAt.After(rec.CreatedAt.Add(2*time.Second+60*time.Second+29*time.Second)) ||
		rec.DeadlineAt.After(rec.CreatedAt.Add(2*time.Second+60*time.Second+31*time.Second)) {
		t.Fatalf("调用期限应为排队上限 + wall + 30 s：%v", rec.DeadlineAt.Sub(rec.CreatedAt))
	}
	for _, sha := range []string{r.BlobSHA256, v.Outputs[0].SHA256} {
		if ok, _ := h.store.BlobAuthorized(context.Background(), "t1", sha); !ok {
			t.Fatalf("blob %s 未授权到任务 scope", sha)
		}
	}

	// 4. 重放：fake 环境无任何操作，exec_count 不变。
	before := len(h.ops.list())
	r2 := h.exec(t, xinv("c1", execBody(code, `,"inputs":[{"sha256":"`+in+`","path":"data/x.csv"}]`)))
	if !r2.Replayed || !bytes.Equal(r2.Body, r.Body) || r2.BlobSHA256 != r.BlobSHA256 {
		t.Fatalf("重放：%+v", r2)
	}
	if after := len(h.ops.list()); after != before {
		t.Fatalf("重放不应有 exec 操作：%v", h.ops.list()[before:])
	}
	if q, _ := h.es.LoadExecQuota(context.Background(), "t1"); q.CountUsed != 1 || q.CPUSpentUsec != 81234 || q.CPUReservedUsec != 0 {
		t.Fatalf("配额 %+v", q)
	}
}

// ---- 5. 超时：wall 到期 → Stop → timed_out，journal completed ----

func TestExecWallTimeout(t *testing.T) {
	h := newExecHarness(t, nil, execScript{stdout: "partial", hang: true})
	r := h.exec(t, xinv("c1", execBody("while True: pass", `,"limits":{"wall_ms":50}`)))
	v := parseExecResult(t, r)
	if v.Status != "timed_out" || v.Stdout != "partial" || v.WallMs < 50 || v.Exit == nil || v.Exit.Signal != 9 {
		t.Fatalf("超时结果 %+v exit %+v", v, v.Exit)
	}
	rec, _ := h.call(t, "t1", "c1")
	if rec.State != StateCompleted {
		t.Fatalf("timed_out 的 journal 应为 completed：%+v", rec)
	}
	if st := h.es.tries("t1", "c1")[0].settled; st.Outcome != ExecTimedOut || !st.Started || st.WallMs < 50 {
		t.Fatalf("结算 %+v", st)
	}
	if h.ops.count("release") != 1 {
		t.Fatalf("slot 未归还：%v", h.ops.list())
	}
}

// ---- 6. E36（单元）：stdout/stderr 各 100 MB → 不挂起，各保留恰 1 MiB 并置截断 ----

func TestExecOutputCapE36(t *testing.T) {
	h := newExecHarness(t, nil, execScript{bigOut: 100 << 20})
	done := make(chan Result, 1)
	go func() {
		r, err := h.c.Exec(context.Background(), xinv("c1", execBody("x", "")))
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	var r Result
	select {
	case r = <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("大量输出时 exec 挂起（stdout 与 stderr 未被并发读取）")
	}
	v := parseExecResult(t, r)
	if len(v.Stdout) != 1<<20 || !v.StdoutTruncated || len(v.Stderr) != 1<<20 || !v.StderrTruncated || v.Status != "completed" {
		t.Fatalf("stdout %d %v / stderr %d %v / %s", len(v.Stdout), v.StdoutTruncated, len(v.Stderr), v.StderrTruncated, v.Status)
	}
}

// ---- 7. 取消：任何原因（D7）→ Stop → failed{exec_cancelled}；新 attempt 带 Retry 重跑；I12 [A] ----

func TestExecCancelAnyReason(t *testing.T) {
	for _, reason := range []string{"replaced", ReasonCancel} {
		t.Run(reason, func(t *testing.T) {
			h := newExecHarness(t, nil, execScript{hang: true}, execScript{stdout: "again"})
			body := execBody("x", "")
			ch := h.startExec(xinv("c1", body))
			waitChan(t, h.envs.started, "exec 启动")
			h.c.CancelAttempt("a1", reason)
			r := await(t, ch)
			if r.Status != 409 || r.Code != CodeExecCancelled {
				t.Fatalf("取消：%d %s", r.Status, r.Code)
			}
			rec, _ := h.call(t, "t1", "c1")
			if rec.State != StateFailed || rec.FailReason != CodeExecCancelled {
				t.Fatalf("journal %+v", rec)
			}
			st := h.es.tries("t1", "c1")[0].settled
			if st.Outcome != ExecCancelled || !st.Started || !st.CPUKnown || st.CPUUsec != 81234 {
				t.Fatalf("结算 %+v", st)
			}
			want := []string{"acquire", "reserve", "create", "mark", "start", "stdin_closed", "stop", "diag", "cleanup", "settle", "release"}
			if got := h.ops.list(); !equalStrings(got, want) {
				t.Fatalf("操作序列 %v", got)
			}
			// 不带 Retry：返回持久化失败；新 attempt 带 Retry：新 try 运行完成。
			if r := h.exec(t, ExecInvoke{TaskID: "t1", AttemptID: "a2", CallID: "c1", Body: []byte(body)}); r.Code != CodeExecCancelled {
				t.Fatalf("不带 Retry 应返回持久化失败：%+v", r)
			}
			r2 := h.exec(t, ExecInvoke{TaskID: "t1", AttemptID: "a2", CallID: "c1", Body: []byte(body), Retry: true})
			if v := parseExecResult(t, r2); v.Stdout != "again" {
				t.Fatalf("重跑结果 %+v", v)
			}
			tries := h.es.tries("t1", "c1")
			if len(tries) != 2 || tries[1].try.AttemptID != "a2" || tries[1].try.EnvID == tries[0].try.EnvID {
				t.Fatalf("重跑 try：%+v", tries)
			}
		})
	}
}

func TestExecCancelBeforeStartI12(t *testing.T) {
	gate := make(chan struct{})
	h := newExecHarness(t, nil, execScript{createGate: gate})
	ch := h.startExec(xinv("c1", execBody("x", "")))
	waitChan(t, h.envs.created, "Create")
	h.c.CancelAttempt("a1", "replaced")
	close(gate)
	r := await(t, ch)
	if r.Code != CodeExecCancelled {
		t.Fatalf("取消：%+v", r)
	}
	want := []string{"acquire", "reserve", "create", "stop", "diag", "cleanup", "settle", "release"}
	if got := h.ops.list(); !equalStrings(got, want) {
		t.Fatalf("MarkExecStarting 之前取消不应启动：%v", got)
	}
	tr := h.es.tries("t1", "c1")[0]
	if tr.started || tr.settled.Started || tr.settled.Outcome != ExecCancelled {
		t.Fatalf("try %+v", tr)
	}
	if q, _ := h.es.LoadExecQuota(context.Background(), "t1"); q.CountUsed != 0 {
		t.Fatalf("未启动的 exec 不计数：%+v", q)
	}
}

// 访问在 MarkExecStarting 时已失效（撤销先提交）→ 不启动，按 cancelled 结算。
func TestExecMarkStartingRejected(t *testing.T) {
	gate := make(chan struct{})
	h := newExecHarness(t, nil, execScript{createGate: gate})
	ch := h.startExec(xinv("c1", execBody("x", "")))
	waitChan(t, h.envs.created, "Create")
	h.store.mu.Lock()
	h.store.revoked["a1"] = true
	h.store.mu.Unlock()
	close(gate)
	if r := await(t, ch); r.Code != CodeExecCancelled {
		t.Fatalf("结果 %+v", r)
	}
	if h.ops.count("start") != 0 || h.ops.count("mark") != 1 || h.ops.count("release") != 1 {
		t.Fatalf("操作序列 %v", h.ops.list())
	}
}

// D16：sub-run 内的 exec 记入 calls.subrun_id，CancelSubrun 终止它。
func TestExecCancelSubrun(t *testing.T) {
	h := newExecHarness(t, nil, execScript{hang: true})
	in := xinv("st1/s1/exec/1", execBody("x", ""))
	in.SubrunID = "st1"
	ch := h.startExec(in)
	waitChan(t, h.envs.started, "exec 启动")
	h.c.CancelSubrun("t1", "a1", "st2") // 其他 sub-run：无影响
	h.c.CancelSubrun("t1", "a1", "st1")
	if r := await(t, ch); r.Code != CodeExecCancelled {
		t.Fatalf("结果 %+v", r)
	}
	rec, _ := h.call(t, "t1", "st1/s1/exec/1")
	if rec.SubrunID != "st1" || h.es.reserves[0].SubrunID != "st1" {
		t.Fatalf("sub-run 归属：%+v %+v", rec, h.es.reserves[0])
	}
}

// ---- 8. Stop Blocked → unknown，slot 不归还；重发 → call_in_progress ----

func TestExecStopBlocked(t *testing.T) {
	h := newExecHarness(t, nil, execScript{hang: true, stopBlocked: true})
	body := execBody("x", `,"limits":{"wall_ms":30}`)
	r := h.exec(t, xinv("c1", body))
	if r.Status != 502 || r.Code != CodeExecUnknown {
		t.Fatalf("Stop Blocked：%d %s", r.Status, r.Code)
	}
	want := []string{"acquire", "reserve", "create", "mark", "start", "stdin_closed", "stop", "settle"}
	if got := h.ops.list(); !equalStrings(got, want) {
		t.Fatalf("Stop Blocked 不收集、不清理、不归还 slot：%v", got)
	}
	st := h.es.tries("t1", "c1")[0].settled
	if st.Outcome != ExecUnknown || !st.Started {
		t.Fatalf("结算 %+v", st)
	}
	rec, _ := h.call(t, "t1", "c1")
	if rec.State != StateUnknown {
		t.Fatalf("journal %+v", rec)
	}
	r2 := h.exec(t, xinv("c1", body))
	if r2.Status != 409 || r2.Code != persistence.CodeCallInProgress {
		t.Fatalf("前一 try 环境未停止时重发：%d %s", r2.Status, r2.Code)
	}
	if n := len(h.es.tries("t1", "c1")); n != 1 {
		t.Fatalf("不应新建 try：%d", n)
	}
}

// ---- 9. start_err → start_failed（不计数）；ErrControlLost → Stop 确认后 unknown ----

func TestExecStartErrors(t *testing.T) {
	h := newExecHarness(t, nil, execScript{startErr: &provider.StartError{Reason: "exec_failed"}},
		execScript{startErr: fmt.Errorf("start: %w", provider.ErrControlLost)})
	r := h.exec(t, xinv("c1", execBody("x", "")))
	if r.Status != 502 || r.Code != CodeExecStartFailed {
		t.Fatalf("start_err：%d %s", r.Status, r.Code)
	}
	st := h.es.tries("t1", "c1")[0].settled
	if st.Outcome != ExecStartFailed || st.Started {
		t.Fatalf("结算 %+v", st)
	}
	want := []string{"acquire", "reserve", "create", "mark", "start", "stop", "diag", "cleanup", "settle", "release"}
	if got := h.ops.list(); !equalStrings(got, want) {
		t.Fatalf("操作序列 %v", got)
	}
	if q, _ := h.es.LoadExecQuota(context.Background(), "t1"); q.CountUsed != 0 {
		t.Fatalf("start_failed 不计 exec_count：%+v", q)
	}

	r2 := h.exec(t, xinv("c2", execBody("y", "")))
	if r2.Status != 502 || r2.Code != CodeExecUnknown {
		t.Fatalf("ErrControlLost：%d %s", r2.Status, r2.Code)
	}
	st2 := h.es.tries("t1", "c2")[0].settled
	if st2.Outcome != ExecUnknown || !st2.Started {
		t.Fatalf("结算 %+v", st2)
	}
	ops := h.ops.list()[len(want):]
	want2 := []string{"acquire", "reserve", "create", "mark", "start", "stop", "diag", "cleanup", "settle", "release"}
	if !equalStrings(ops, want2) {
		t.Fatalf("ErrControlLost 应先确认停止再按 unknown 结算：%v", ops)
	}
}

// Create 失败：Stop 确认无残留后按 start_failed 结算，Worker 得到可重试的 503 exec_env_unavailable；带 Retry 重发可新建 try。
func TestExecCreateFailure(t *testing.T) {
	h := newExecHarness(t, nil, execScript{createErr: errors.New("uid 范围耗尽")})
	body := execBody("x", "")
	r := h.exec(t, xinv("c1", body))
	if r.Status != 503 || r.Code != CodeExecEnvUnavailable {
		t.Fatalf("Create 失败：%d %s", r.Status, r.Code)
	}
	want := []string{"acquire", "reserve", "create", "stop", "cleanup", "settle", "release"}
	if got := h.ops.list(); !equalStrings(got, want) {
		t.Fatalf("操作序列 %v", got)
	}
	st := h.es.tries("t1", "c1")[0].settled
	if st.Outcome != ExecStartFailed || st.Started || !st.CPUKnown || st.CPUUsec != 0 || !strings.Contains(st.Error, CodeExecEnvUnavailable) {
		t.Fatalf("结算 %+v", st)
	}
	if q, _ := h.es.LoadExecQuota(context.Background(), "t1"); q.CPUSpentUsec != 0 {
		t.Fatalf("未运行的 exec 不应计 CPU：%+v", q)
	}
	r2 := h.exec(t, ExecInvoke{TaskID: "t1", AttemptID: "a1", CallID: "c1", Body: []byte(body), Retry: true})
	parseExecResult(t, r2)
}

// 输入累计超过上限（256 MiB；测试把上限调小）→ 中止，按 start_failed 结算，400 inputs_too_large（不启动）。
func TestExecInputsTooLarge(t *testing.T) {
	h := newExecHarness(t, nil)
	if h.c.execInputMax != 256<<20 {
		t.Fatalf("输入上限 %d，期望 256 MiB", h.c.execInputMax)
	}
	h.c.execInputMax = 10
	a := h.seedInput(t, "123456")
	b := h.seedInput(t, "7890ab")
	r := h.exec(t, xinv("c1", execBody("x", `,"inputs":[{"sha256":"`+a+`","path":"a"},{"sha256":"`+b+`","path":"b"}]`)))
	if r.Status != 400 || r.Code != CodeInputsTooLarge {
		t.Fatalf("输入超限：%d %s", r.Status, r.Code)
	}
	if h.ops.count("start") != 0 || h.ops.count("release") != 1 {
		t.Fatalf("操作序列 %v", h.ops.list())
	}
	if st := h.es.tries("t1", "c1")[0].settled; st.Outcome != ExecStartFailed || st.Started {
		t.Fatalf("结算 %+v", st)
	}
}

// ---- 10. 输入未授权 → 403，无预留、无环境 ----

func TestExecInputNotAuthorized(t *testing.T) {
	h := newExecHarness(t, nil)
	ref, err := h.blobs.Put(context.Background(), strings.NewReader("secret of t2"))
	if err != nil {
		t.Fatal(err)
	}
	r := h.exec(t, xinv("c1", execBody("x", `,"inputs":[{"sha256":"`+ref.SHA256+`","path":"a"}]`)))
	if r.Status != 403 || r.Code != CodeInputNotAuthorized {
		t.Fatalf("未授权输入：%d %s", r.Status, r.Code)
	}
	if ops := h.ops.list(); len(ops) != 0 {
		t.Fatalf("不应排队、预留或建环境：%v", ops)
	}
	rec, _ := h.call(t, "t1", "c1")
	if rec.State != StateFailed || rec.FailReason != CodeInputNotAuthorized {
		t.Fatalf("journal %+v", rec)
	}
}

// ---- 11. 每任务第 3 个并发 exec 排队；排队超时 → 504 exec_queue_timeout，无预留 ----

func TestExecQueueTimeout(t *testing.T) {
	h := newExecHarness(t, func(c *ExecConfig) { c.QueueTimeout = 100 * time.Millisecond },
		execScript{hang: true}, execScript{hang: true})
	ch1 := h.startExec(xinv("c1", execBody("1", "")))
	ch2 := h.startExec(xinv("c2", execBody("2", "")))
	waitChan(t, h.envs.started, "exec 1 启动")
	waitChan(t, h.envs.started, "exec 2 启动")
	start := time.Now()
	r := h.exec(t, xinv("c3", execBody("3", "")))
	if r.Status != 504 || r.Code != CodeExecQueueTimeout {
		t.Fatalf("第 3 个 exec：%d %s", r.Status, r.Code)
	}
	if el := time.Since(start); el < 100*time.Millisecond {
		t.Fatalf("排队超时过早：%v", el)
	}
	if n := h.es.nReserves(); n != 2 {
		t.Fatalf("排队超时的 exec 不应预留：%d 次 ReserveExec", n)
	}
	rec, _ := h.call(t, "t1", "c3")
	if rec.State != StateFailed || rec.FailReason != CodeExecQueueTimeout {
		t.Fatalf("journal %+v", rec)
	}
	h.c.CancelAttempt("a1", "replaced")
	for _, ch := range []<-chan invokeOutcome{ch1, ch2} {
		if r := await(t, ch); r.Code != CodeExecCancelled {
			t.Fatalf("结果 %+v", r)
		}
	}
	if h.ops.count("acquire") != 2 || h.ops.count("release") != 2 {
		t.Fatalf("slot：%v", h.ops.list())
	}
}

// 未配置 exec → 404 endpoint_not_configured。
func TestExecNotConfigured(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p1"))
	r, err := h.c.Exec(context.Background(), xinv("c1", execBody("x", "")))
	if err != nil || r.Status != 404 || r.Code != CodeEndpointNotConfigured {
		t.Fatalf("%+v %v", r, err)
	}
}

// ==== M4 Plan 15 Task 9：/v1/budget 的 exec 配额与 exec 码的 HTTP 状态 ====

// ExecQuota：尚无配额行时为策略值（用量 0，false）；有行时原样返回（true）；未配置 exec 为 ErrExecNotConfigured；
// 存储错误原样返回。
func TestExecQuotaPassThrough(t *testing.T) {
	h := newExecHarness(t, func(c *ExecConfig) {
		c.Policy = ExecPolicy{CountLimit: 7, CPULimitUsec: 9_000_000, WallLimitMs: 120_000}
	})
	ctx := context.Background()
	q, ok, err := h.c.ExecQuota(ctx, "t1")
	if err != nil || ok || q != (ExecQuota{CountLimit: 7, CPULimitUsec: 9_000_000, WallLimitMs: 120_000}) {
		t.Fatalf("无配额行: %+v %v %v", q, ok, err)
	}
	row := ExecQuota{CountLimit: 50, CountUsed: 2, CPULimitUsec: 100, CPUReservedUsec: 10, CPUSpentUsec: 20, CPUUnknownUsec: 5,
		WallLimitMs: 1000, WallSpentMs: 300, Blocked: true}
	h.es.mu.Lock()
	h.es.quotas["t1"] = &row
	h.es.mu.Unlock()
	if q, ok, err := h.c.ExecQuota(ctx, "t1"); err != nil || !ok || q != row {
		t.Fatalf("有配额行: %+v %v %v", q, ok, err)
	}
	plain := newHarness(t, testLimits(), newAdapter("p1"))
	if _, _, err := plain.c.ExecQuota(ctx, "t1"); !errors.Is(err, ErrExecNotConfigured) {
		t.Fatalf("未配置: %v", err)
	}
	failing := newExecHarness(t, func(c *ExecConfig) { c.Store = quotaErrStore{c.Store} })
	if _, _, err := failing.c.ExecQuota(ctx, "t1"); err == nil || errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("存储错误应原样返回: %v", err)
	}
}

type quotaErrStore struct{ ExecStore }

func (quotaErrStore) LoadExecQuota(context.Context, string) (ExecQuota, error) {
	return ExecQuota{}, errors.New("db down")
}

// edge 原样采用 Result.Status：exec 的拒绝与失败码映射（§10；Task 9 状态表；exec_unknown 为 502）。
func TestExecStatusCodes(t *testing.T) {
	want := map[string]int{
		CodeExecQuotaExhausted: 402, CodeExecCPUExhausted: 402, CodeExecWallExhausted: 402, CodeExecBlocked: 402,
		CodeInputNotAuthorized:         403,
		persistence.CodeCallInProgress: 409, CodeExecCancelled: 409, persistence.CodeFingerprintMismatch: 409,
		CodeExecStartFailed: 502, CodeExecUnknown: 502,
		CodeExecEnvUnavailable:               503,
		persistence.CodeCallDeadlineExceeded: 504, CodeExecQueueTimeout: 504,
		CodeInputsTooLarge: 400,
	}
	for code, status := range want {
		if got := statusFor(code); got != status {
			t.Errorf("statusFor(%s) = %d，期望 %d", code, got, status)
		}
	}
}

// ---- M4 真实验收修复：暂停时不再为在途调用安排新的 try ----

// TestPauseStopsAutoRetry：暂停（desired = pause）不撤销访问，在途 try 照常结束；但该 try 以可重试失败结束后，
// Gateway 不再自动新建 try——调用以该 try 的原因（可重试类别）置为 failed 并立即返回，Worker 可以尽快写 checkpoint
// （验收中对不可达站点的抓取按 3 × 10 s 重试，停止要 20–39 s）。继续后（desired = run）带 X-Agentbox-Retry 在上限内
// 新建 try。未暂停时同样的失败照常自动重试（TestRetryableExhaustsTries）。
func TestPauseStopsAutoRetry(t *testing.T) {
	timeout := &upstream.Error{Outcome: upstream.OutcomeRetryable, Status: 502, Code: upstream.CodeUpstreamUnreachable}
	gate := make(chan struct{})
	ad := newAdapter("p", step{gate: gate, hold: true, err: timeout}, step{body: `{"answer":"later"}`})
	h := newHarness(t, testLimits(), ad)
	done := make(chan Result, 1)
	go func() {
		r, err := h.c.Invoke(context.Background(), inv("c1", chatBody))
		if err != nil {
			t.Errorf("Invoke：%v", err)
		}
		done <- r
	}()
	waitEntered(t, ad)
	h.store.mu.Lock()
	h.store.desired["t1"] = "pause"
	h.store.mu.Unlock()
	close(gate)
	var r Result
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("暂停后调用没有返回")
	}
	if r.Status != 502 || r.Code != upstream.CodeUpstreamUnreachable {
		t.Fatalf("得到 %+v", r)
	}
	if calls, _, _ := ad.stats(); calls != 1 {
		t.Fatalf("暂停后不应再新建 try，上游被调用 %d 次", calls)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateFailed || rec.FailReason != upstream.CodeUpstreamUnreachable || rec.TriesUsed != 1 || len(tries) != 1 ||
		!retryableReason(rec.FailReason) {
		t.Fatalf("调用记录 %+v，try %d", rec, len(tries))
	}
	if b := h.budget(t, "t1"); b.ReservedMicro != 0 {
		t.Fatalf("账本 %+v", b)
	}
	h.store.mu.Lock()
	h.store.desired["t1"] = "run"
	h.store.mu.Unlock()
	in := inv("c1", chatBody)
	in.Retry = true
	if r := h.invoke(t, in); r.Status != 200 {
		t.Fatalf("继续后带 Retry：%+v", r)
	}
	if rec, _ := h.call(t, "t1", "c1"); rec.State != StateCompleted || rec.TriesUsed != 2 {
		t.Fatalf("继续后调用记录 %+v", rec)
	}
}
