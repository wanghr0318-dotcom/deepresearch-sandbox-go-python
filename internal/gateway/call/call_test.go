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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
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
}

func newFakeStore(t *testing.T) *fakeStore {
	return &fakeStore{t: t, revoked: map[string]bool{}, desired: map[string]string{}, budgets: map[string]*Budget{},
		calls: map[callKey]*CallRecord{}, tries: map[callKey][]*fakeTry{}, begun: make(chan string, 64), settled: make(chan string, 64),
		scoped: map[callKey]bool{}}
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

func (s *fakeStore) facts(taskID, attemptID string) AccessFacts {
	d := s.desired[taskID]
	if d == "" {
		d = "run"
	}
	return AccessFacts{TaskID: taskID, AttemptID: attemptID, Active: !s.revoked[attemptID], Current: true, Desired: d}
}

func (s *fakeStore) CheckAccess(_ context.Context, taskID, attemptID string) (AccessFacts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.facts(taskID, attemptID), nil
}

func (s *fakeStore) BeginCall(_ context.Context, r BeginCallRequest) (BeginCallResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if code := admit(s.facts(r.TaskID, r.AttemptID)); code != "" {
		return BeginCallResult{}, rejected(code)
	}
	k := callKey{r.TaskID, r.CallID}
	rec := s.calls[k]
	now := time.Now()
	if rec == nil {
		rec = &CallRecord{TaskID: r.TaskID, CallID: r.CallID, Fingerprint: r.Fingerprint, Endpoint: r.Endpoint,
			State: StateResolving, Source: "upstream", CreatedAt: now, DeadlineAt: now.Add(r.Deadline),
			FirstAttemptID: r.AttemptID, SupersedesCallID: r.SupersedesCallID, SupersedeReason: r.SupersedeReason,
			ResolvingSince: &now}
		s.calls[k] = rec
		s.budget(r.TaskID)
		select {
		case s.begun <- r.CallID:
		default:
		}
		return BeginCallResult{Record: *rec}, nil
	}
	if rec.State == StateResolving && rec.ResolvingSince == nil && rec.TriesUsed == 0 && rec.Fingerprint == r.Fingerprint {
		rec.ResolvingSince = &now
		return BeginCallResult{Record: *rec}, nil
	}
	return BeginCallResult{Record: *rec, Existing: true}, nil
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
	if code := admit(s.facts(r.TaskID, r.AttemptID)); code != "" {
		return Try{}, rejected(code)
	}
	b := s.budget(r.TaskID)
	switch {
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
	return Limits{CallDeadline: 5 * time.Second, BackoffBase: time.Millisecond, BackoffMax: 4 * time.Millisecond}
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
		s, err := Fingerprint("/v1/chat/completions", "fake/1", "p", model, defaults, []byte(body))
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
	if _, err := Fingerprint("/v1/chat/completions", "fake/1", "p", "m1", nil, []byte(`{"a":1,"a":2}`)); !errors.Is(err, jcs.ErrDuplicateKey) {
		t.Fatalf("重复属性名：%v", err)
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
	lim.CallDeadline = 300 * time.Millisecond
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
	lim.CallDeadline = 200 * time.Millisecond
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
	lim.CallDeadline = 200 * time.Millisecond
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
