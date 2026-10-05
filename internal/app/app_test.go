package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/fake"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/recovery"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// 装配测试使用真实 PostgreSQL（每个测试一个临时数据库）与 provider/fake；Worker 是以 Go 函数实现的
// 协议对端。本地未设置 AGENTBOX_TEST_DATABASE_URL 时跳过；CI 中未设置则失败。等待只按条件轮询，
// 期限只作为失败的上限，不作为通过的依据。

const dsnEnv = "AGENTBOX_TEST_DATABASE_URL"

func newDatabase(t *testing.T) string {
	t.Helper()
	admin := os.Getenv(dsnEnv)
	if admin == "" {
		if os.Getenv("CI") == "true" {
			t.Fatalf("CI 中必须设置 %s", dsnEnv)
		}
		t.Skipf("未设置 %s，跳过", dsnEnv)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_app_" + hex.EncodeToString(b)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("连接测试数据库: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("创建测试数据库: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// ---- 假时钟 ----

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
	ws  []*clockWaiter
}

type clockWaiter struct {
	at time.Time
	c  chan time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Now()} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) At(t time.Time) (<-chan time.Time, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := make(chan time.Time, 1)
	if !t.After(f.now) {
		c <- f.now
		return c, func() {}
	}
	w := &clockWaiter{at: t, c: c}
	f.ws = append(f.ws, w)
	return c, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.ws = slices.DeleteFunc(f.ws, func(x *clockWaiter) bool { return x == w })
	}
}

// Advance 推进时间并触发全部到期的等待者。
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	f.ws = slices.DeleteFunc(f.ws, func(w *clockWaiter) bool {
		if w.at.After(f.now) {
			return false
		}
		w.c <- f.now
		return true
	})
}

// ---- Worker：协议对端 ----

// workerProgram 读取 init，发出 ready；spec 为 {"block": true} 时等待被终止，否则发出 result 并以 0 退出。
func workerProgram(ctx context.Context, _ provider.ExecSpec, stdin io.Reader, stdout, _ io.Writer) provider.ExitStatus {
	lines := make(chan []byte, 1)
	go func() {
		b, _ := bufio.NewReader(stdin).ReadBytes('\n')
		lines <- b
	}()
	var line []byte
	select {
	case line = <-lines:
	case <-ctx.Done():
		return provider.ExitStatus{Signal: syscall.SIGKILL}
	}
	var in protocol.Init
	if err := json.Unmarshal(line, &in); err != nil {
		return provider.ExitStatus{Code: 2}
	}
	var cfg struct {
		Block bool `json:"block"`
	}
	_ = json.Unmarshal(in.Config, &cfg)
	emit := func(m map[string]any) {
		b, _ := json.Marshal(m)
		_, _ = stdout.Write(append(b, '\n'))
	}
	emit(map[string]any{"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task",
		"worker": map[string]any{"name": "app-test", "version": "0"}, "capabilities": []string{}})
	if cfg.Block {
		<-ctx.Done()
		return provider.ExitStatus{Signal: syscall.SIGKILL}
	}
	emit(map[string]any{"type": "result", "v": 1, "seq": 2, "summary": "done", "outputs": []string{}})
	return provider.ExitStatus{Code: 0}
}

// ---- 装置 ----

type harness struct {
	t    *testing.T
	dsn  string
	dir  string
	prov *fake.Provider

	mu         sync.Mutex
	log        []string
	modes      []api.Mode
	logs       bytes.Buffer
	addr       chan string
	recovered  chan recovery.Report
	scanGate   chan struct{}  // 非 nil 时 Scan 等待它关闭
	stopFails  map[string]int // 环境 → 接下来这么多次 Stop 返回 ErrStopUnconfirmed（mu 保护）
	stopCalls  map[string]int // 环境 → Stop 调用次数（mu 保护）
	panicTask  atomic.Value   // string：该任务（"*" 为任何任务）的 CreateAttempt panic
	failMarks  atomic.Int32   // 接下来这么多次 MarkQuarantineAlerted 返回 ErrUnavailable
	cancel     context.CancelFunc
	result     chan error
	resultSeen bool
}

func newHarness(t *testing.T) *harness {
	h := &harness{t: t, dsn: newDatabase(t), dir: t.TempDir(), prov: fake.New(workerProgram),
		addr: make(chan string, 1), recovered: make(chan recovery.Report, 1)}
	h.panicTask.Store("")
	t.Cleanup(func() {
		if h.cancel != nil {
			h.cancel()
			if !h.resultSeen {
				select {
				case <-h.result:
				case <-time.After(60 * time.Second):
					t.Errorf("Run 未在期限内返回")
				}
			}
		}
		if t.Failed() {
			h.mu.Lock()
			t.Logf("事件：%q\n日志：\n%s", h.log, h.logs.String())
			h.mu.Unlock()
		}
	})
	return h
}

func (h *harness) record(s string) {
	h.mu.Lock()
	h.log = append(h.log, s)
	h.mu.Unlock()
}

func (h *harness) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.log)
}

func (h *harness) seenModes() []api.Mode {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.modes)
}

type lockedWriter struct{ h *harness }

func (w lockedWriter) Write(b []byte) (int, error) {
	w.h.mu.Lock()
	defer w.h.mu.Unlock()
	return w.h.logs.Write(b)
}

func testConfig() Config {
	return Config{Listen: "127.0.0.1:0", ShutdownTimeout: 10 * time.Second, DefaultRunTime: 1000 * time.Hour, RunTimeCap: 1000 * time.Hour,
		Capacity: admission.Capacity{RunSlots: 4, MemoryBytes: 8 << 30}}
}

func (h *harness) deps(clock task.Clock) Deps {
	return Deps{
		DataDir: h.dir,
		AcquireOwnership: func(ctx context.Context) (Ownership, error) {
			h.record("acquire_ownership")
			o, err := postgres.AcquireOwnership(ctx, h.dsn, postgres.OwnershipOptions{CheckInterval: 50 * time.Millisecond, CheckTimeout: time.Second})
			if err != nil {
				return nil, err
			}
			return o, nil
		},
		OpenStore: func(ctx context.Context, own Ownership) (Store, error) {
			s, err := postgres.Open(ctx, postgres.Options{DSN: h.dsn, Ownership: own.(*postgres.Ownership)})
			if err != nil {
				return nil, err
			}
			return recStore{Store: s, h: h}, nil
		},
		NewProvider: func(string) (provider.Provider, error) { return recProvider{Provider: h.prov, h: h}, nil },
		Clock:       clock,
		Logger:      slog.New(slog.NewJSONHandler(lockedWriter{h}, nil)),
		Hooks: Hooks{
			Step: func(name string) { h.record("step:" + name) },
			Mode: func(m api.Mode) {
				h.mu.Lock()
				h.modes = append(h.modes, m)
				h.mu.Unlock()
			},
			Recovered: func(r recovery.Report) { h.recovered <- r },
			Listening: func(addr string) { h.addr <- addr },
		},
	}
}

func (h *harness) start(cfg Config, clock task.Clock) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.result = make(chan error, 1)
	d := h.deps(clock)
	go func() { h.result <- Run(ctx, cfg, d) }()
}

// wait 等待 Run 返回。
func (h *harness) wait() error {
	h.t.Helper()
	select {
	case err := <-h.result:
		h.resultSeen = true
		return err
	case <-time.After(60 * time.Second):
		h.t.Fatal("Run 未在期限内返回")
		return nil
	}
}

func (h *harness) waitAddr() string {
	h.t.Helper()
	select {
	case a := <-h.addr:
		return "http://" + a
	case err := <-h.result:
		h.resultSeen = true
		h.t.Fatalf("Run 提前返回: %v", err)
	case <-time.After(60 * time.Second):
		h.t.Fatal("API 未开始监听")
	}
	return ""
}

func (h *harness) waitRecovered() recovery.Report {
	h.t.Helper()
	select {
	case r := <-h.recovered:
		return r
	case <-time.After(60 * time.Second):
		h.t.Fatal("恢复未完成")
	}
	return recovery.Report{}
}

// eventually 轮询 cond 直到成立；step 在每轮之前调用（例如推进假时钟）。
func eventually(t *testing.T, what string, step func(), cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时：%s", what)
		}
		if step != nil {
			step()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *harness) queryRow(sql string, args []any, dest ...any) {
	h.t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, h.dsn)
	if err != nil {
		h.t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if err := c.QueryRow(ctx, sql, args...).Scan(dest...); err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
}

func (h *harness) exec(sql string, args ...any) {
	h.t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, h.dsn)
	if err != nil {
		h.t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if _, err := c.Exec(ctx, sql, args...); err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
}

func (h *harness) taskStatus(id string) (status string, attempts int64) {
	h.queryRow("SELECT status, attempts_total FROM tasks WHERE task_id = $1", []any{id}, &status, &attempts)
	return status, attempts
}

// recStore 记录启动顺序中的 Store 调用，并按测试设置注入故障。
type recStore struct {
	Store
	h *harness
}

func (s recStore) InspectInstallation(ctx context.Context) (ownership.DBState, error) {
	s.h.record("store.InspectInstallation")
	return s.Store.InspectInstallation(ctx)
}

func (s recStore) Migrate(ctx context.Context) error {
	s.h.record("store.Migrate")
	return s.Store.Migrate(ctx)
}

func (s recStore) RevokeAllActive(ctx context.Context, reason string) (int, error) {
	s.h.record("store.RevokeAllActive")
	return s.Store.RevokeAllActive(ctx, reason)
}

func (s recStore) LoadRecoveryFacts(ctx context.Context) (recovery.Facts, error) {
	s.h.record("store.LoadRecoveryFacts")
	return s.Store.LoadRecoveryFacts(ctx)
}

func (s recStore) ListActiveTasks(ctx context.Context) ([]string, error) {
	s.h.record("store.ListActiveTasks")
	return s.Store.ListActiveTasks(ctx)
}

func (s recStore) AccountUnrecordedRunTime(ctx context.Context, taskID, attemptID string, until time.Time) error {
	s.h.record("store.AccountUnrecordedRunTime:" + attemptID)
	return s.Store.AccountUnrecordedRunTime(ctx, taskID, attemptID, until)
}

func (s recStore) MarkQuarantineAlerted(ctx context.Context, path string) error {
	if s.h.failMarks.Add(-1) >= 0 {
		s.h.record("store.MarkQuarantineAlerted.fail:" + path)
		return fmt.Errorf("%w: 注入", persistence.ErrUnavailable)
	}
	s.h.record("store.MarkQuarantineAlerted:" + path)
	return s.Store.MarkQuarantineAlerted(ctx, path)
}

func (s recStore) CreateAttempt(ctx context.Context, a task.NewAttempt) (task.Attempt, error) {
	if p := s.h.panicTask.Load().(string); p == "*" || p == a.TaskID {
		panic("注入的 actor panic")
	}
	return s.Store.CreateAttempt(ctx, a)
}

// recProvider 记录 List 与 Scan；scanGate 非 nil 时 Scan 等待它关闭（使恢复超过期限）。
type recProvider struct {
	*fake.Provider
	h *harness
}

func (p recProvider) List(ctx context.Context) ([]provider.EnvInfo, error) {
	p.h.record("provider.List")
	return p.Provider.List(ctx)
}

func (p recProvider) Scan(ctx context.Context) (provider.ScanReport, error) {
	p.h.record("provider.Scan")
	if g := p.h.scanGate; g != nil {
		select {
		case <-g:
		case <-ctx.Done():
			return provider.ScanReport{}, ctx.Err()
		}
	}
	return p.Provider.Scan(ctx)
}

// Stop 记录每个环境的调用次数；stopFails 中的环境在剩余次数内返回 ErrStopUnconfirmed。
func (p recProvider) Stop(ctx context.Context, envID string) error {
	p.h.mu.Lock()
	if p.h.stopCalls == nil {
		p.h.stopCalls = map[string]int{}
	}
	p.h.stopCalls[envID]++
	fail := p.h.stopFails[envID] > 0
	if fail {
		p.h.stopFails[envID]--
	}
	p.h.mu.Unlock()
	if fail {
		return provider.ErrStopUnconfirmed
	}
	return p.Provider.Stop(ctx, envID)
}

func (h *harness) stops(envID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stopCalls[envID]
}

// ---- HTTP ----

func httpDo(t *testing.T, method, u, body string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func submit(t *testing.T, base, requestID, spec string) string {
	t.Helper()
	st, b := httpDo(t, "POST", base+"/tasks", `{"request_id":"`+requestID+`","spec":`+spec+`}`)
	if st != http.StatusCreated {
		t.Fatalf("POST /tasks = %d %s", st, b)
	}
	var r struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(b, &r); err != nil || r.TaskID == "" {
		t.Fatalf("POST /tasks 返回 %s", b)
	}
	return r.TaskID
}

func statusMode(t *testing.T, base string) string {
	t.Helper()
	st, b := httpDo(t, "GET", base+"/status", "")
	var r struct {
		Mode string `json:"mode"`
	}
	if st != http.StatusOK || json.Unmarshal(b, &r) != nil {
		t.Fatalf("GET /status = %d %s", st, b)
	}
	return r.Mode
}

func errorCode(t *testing.T, b []byte) string {
	t.Helper()
	var e struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(b, &e)
	return e.Code
}

// runningEnvs 返回 fake provider 中仍在运行或闸门仍开的环境。
func (h *harness) liveEnvs() []string {
	envs, _ := h.prov.List(context.Background())
	var out []string
	for _, e := range envs {
		if e.Running || e.Complete {
			out = append(out, e.EnvID)
		}
	}
	return out
}

func (h *harness) hasRunningEnv() bool {
	envs, _ := h.prov.List(context.Background())
	for _, e := range envs {
		if e.Running {
			return true
		}
	}
	return false
}

// ---- 测试 ----

// TestStartupStepOrder：§14.1 的启动顺序——数据目录、flock、advisory lock、安装身份、迁移、撤销访问、
// List 与扫描、恢复事实与计划、执行、admission 重建，然后依次 cleanup loop、actor、API。
func TestStartupStepOrder(t *testing.T) {
	h := newHarness(t)
	h.start(testConfig(), task.SystemClock())
	h.waitAddr()
	got := h.events()
	want := []string{
		"step:data_dir", "step:flock", "acquire_ownership", "step:advisory_lock",
		"store.InspectInstallation", "step:install_identity", "store.Migrate", "step:migrate",
		"store.RevokeAllActive", "step:revoke_access", "provider.List", "provider.Scan", "step:scan",
		"store.LoadRecoveryFacts", "step:recovery_plan", "step:recovery_execute", "step:admission_rebuild",
		"step:cleanup_loop", "store.ListActiveTasks", "step:scheduler", "step:api",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("启动顺序\n得到 %q\n期望 %q", got, want)
	}
	if m := h.seenModes(); !slices.Equal(m, []api.Mode{api.ModeNormal}) {
		t.Fatalf("模式变化 %q，期望只有 normal", m)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("ctx 结束后 Run 返回 %v，期望 nil", err)
	}
}

// TestEndToEndTaskThroughHTTP：app.Run（fake provider + 真实 PostgreSQL）经 HTTP API 提交任务，任务到达
// 终态 succeeded，环境停止；永远无法满足容量的 limits 被拒绝（EffectiveLimits）。
func TestEndToEndTaskThroughHTTP(t *testing.T) {
	h := newHarness(t)
	h.start(testConfig(), task.SystemClock())
	base := h.waitAddr()

	st, b := httpDo(t, "POST", base+"/tasks", `{"request_id":"too-big","spec":{},"limits":{"memory_max":17179869184}}`)
	if st != http.StatusBadRequest || errorCode(t, b) != "invalid_limits" {
		t.Fatalf("超过容量的 limits 得到 %d %s，期望 400 invalid_limits", st, b)
	}

	id := submit(t, base, "r1", `{"steps":[]}`)
	var view struct {
		Status        string `json:"status"`
		AttemptsTotal int64  `json:"attempts_total"`
	}
	eventually(t, "任务到达终态", nil, func() bool {
		st, b := httpDo(t, "GET", base+"/tasks/"+id, "")
		if st != http.StatusOK || json.Unmarshal(b, &view) != nil {
			t.Fatalf("GET /tasks/%s = %d %s", id, st, b)
		}
		return task.IsTerminal(view.Status)
	})
	if view.Status != "succeeded" || view.AttemptsTotal != 1 {
		t.Fatalf("任务结束为 %+v，期望 succeeded 且 1 个 attempt", view)
	}
	st, b = httpDo(t, "GET", base+"/tasks/"+id+"/inspect", "")
	var in struct {
		Attempts []struct {
			OutcomeClass string `json:"outcome_class"`
		} `json:"attempts"`
	}
	if st != http.StatusOK || json.Unmarshal(b, &in) != nil || len(in.Attempts) != 1 || in.Attempts[0].OutcomeClass != "succeeded" {
		t.Fatalf("inspect = %d %s", st, b)
	}
	eventually(t, "环境停止", nil, func() bool { return len(h.liveEnvs()) == 0 })
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// TestRecoveryDeadlineDiagnosticMode：恢复超过期限仍未就绪 → API 以诊断模式启动（只读 status/inspect），
// 不启动 actor；恢复完成后再启动 actor 并切换到 normal。
func TestRecoveryDeadlineDiagnosticMode(t *testing.T) {
	h := newHarness(t)
	h.scanGate = make(chan struct{})
	clock := newFakeClock()
	cfg := testConfig()
	cfg.RecoveryDeadline = time.Minute
	h.start(cfg, clock)
	eventually(t, "恢复进入扫描", nil, func() bool { return slices.Contains(h.events(), "provider.Scan") })
	clock.Advance(time.Minute)
	base := h.waitAddr()

	if m := statusMode(t, base); m != "diagnostic" {
		t.Fatalf("恢复超过期限后模式为 %q，期望 diagnostic", m)
	}
	st, b := httpDo(t, "POST", base+"/tasks", `{"request_id":"r1","spec":{}}`)
	if st != http.StatusServiceUnavailable || errorCode(t, b) != "diagnostic_mode" {
		t.Fatalf("诊断模式下 POST /tasks = %d %s，期望 503 diagnostic_mode", st, b)
	}
	if st, _ := httpDo(t, "GET", base+"/tasks", ""); st != http.StatusServiceUnavailable {
		t.Fatalf("诊断模式下 GET /tasks = %d，期望 503", st)
	}
	for _, e := range h.events() {
		if e == "store.ListActiveTasks" || e == "step:scheduler" || e == "step:cleanup_loop" {
			t.Fatalf("诊断模式下启动了执行：%q", h.events())
		}
	}

	close(h.scanGate)
	eventually(t, "恢复完成后切换到 normal", nil, func() bool { return statusMode(t, base) == "normal" })
	ev := h.events()
	if i, j := slices.Index(ev, "step:recovery_execute"), slices.Index(ev, "step:scheduler"); i < 0 || j < i ||
		slices.Index(ev, "step:api") > i {
		t.Fatalf("诊断模式路径的顺序不对：%q", ev)
	}
	if m := h.seenModes(); !slices.Equal(m, []api.Mode{api.ModeDiagnostic, api.ModeNormal}) {
		t.Fatalf("模式变化 %q", m)
	}
	id := submit(t, base, "r2", `{}`)
	eventually(t, "恢复后任务执行", nil, func() bool { s, _ := h.taskStatus(id); return s == "succeeded" })
}

// TestOwnershipLostStopsAllEnvironments：advisory lock 丢失 → ownership_lost：拒绝写操作，物理停止全部
// 执行环境，然后 Run 返回 ErrOwnershipLost。
func TestOwnershipLostStopsAllEnvironments(t *testing.T) {
	h := newHarness(t)
	h.start(testConfig(), task.SystemClock())
	base := h.waitAddr()
	submit(t, base, "r1", `{"block":true}`)
	submit(t, base, "r2", `{"block":true}`)
	eventually(t, "两个环境在运行", nil, func() bool {
		envs, _ := h.prov.List(context.Background())
		n := 0
		for _, e := range envs {
			if e.Running {
				n++
			}
		}
		return n == 2
	})

	h.exec(`SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory'
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) AND pid <> pg_backend_pid()`)
	err := h.wait()
	if !errors.Is(err, persistence.ErrOwnershipLost) || !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("失锁后 Run 返回 %v，期望 ErrOwnershipLost", err)
	}
	if live := h.liveEnvs(); len(live) != 0 {
		t.Fatalf("Run 返回时仍有环境未停止：%q", live)
	}
	if m := h.seenModes(); len(m) == 0 || m[len(m)-1] != api.ModeOwnershipLost {
		t.Fatalf("模式变化 %q，期望最终为 ownership_lost", m)
	}
	ev := h.events()
	if i := slices.Index(ev, "step:api"); i < 0 || !slices.Contains(ev[i+1:], "provider.List") {
		t.Fatalf("停止时没有经 provider.List 停止全部环境：%q", ev)
	}
}

// TestActorPanicFatalStop：actor panic → 致命停止：停止全部执行环境后 Run 返回该错误。
func TestActorPanicFatalStop(t *testing.T) {
	h := newHarness(t)
	h.start(testConfig(), task.SystemClock())
	base := h.waitAddr()
	submit(t, base, "r1", `{"block":true}`)
	eventually(t, "环境在运行", nil, h.hasRunningEnv)

	h.panicTask.Store("*") // 此后任何任务的 CreateAttempt（在 actor 的 Store goroutine 中执行）panic
	submit(t, base, "r2", `{}`)
	err := h.wait()
	if err == nil || !strings.Contains(err.Error(), "panic") || errors.Is(err, persistence.ErrOwnershipLost) {
		t.Fatalf("actor panic 后 Run 返回 %v，期望含 panic 的致命错误", err)
	}
	if live := h.liveEnvs(); len(live) != 0 {
		t.Fatalf("Run 返回时仍有环境未停止：%q", live)
	}
}

// TestRecoveryHandoff：恢复报告交给装配——Excluded 的任务不建立 actor；stop_blocked 的任务以 stop_blocked
// 模式（lost_on_restart，持有恢复重建的槽位）启动 actor，确认停止后经 OnStopRecorded 补记运行时间再按
// 故障恢复重跑；每条报警先记日志再 MarkQuarantineAlerted，失败后在后台重试。
func TestRecoveryHandoff(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	st, err := postgres.Open(ctx, postgres.Options{DSN: h.dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	installID, err := ownership.Bootstrap(ctx, st, datadir.NewIDFile(h.dir), datadir.NewTokenFile(h.dir),
		func() string { return "inst-handoff" }, func() ([]byte, error) { return make([]byte, datadir.TokenSize), nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"task-x", "task-y"} {
		if _, err := st.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-" + id, BodyHash: []byte{1}, TaskID: id,
			Spec: json.RawMessage(`{}`), MaxFaultRetries: 3}); err != nil {
			t.Fatal(err)
		}
	}
	// task-y：上次运行中的 attempt，其环境无法确认停止（stop_blocked）。
	if _, err := st.CreateAttempt(ctx, task.NewAttempt{TaskID: "task-y", AttemptID: "att-y1", AttemptNo: 1, EnvID: "env-y1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.prov.Create(ctx, provider.EnvSpec{EnvID: "env-y1", InstallID: installID, Kind: provider.KindTask,
		Template: "default", UIDSize: 4096}); err != nil {
		t.Fatal(err)
	}
	h.prov.FailStop("env-y1", provider.ErrStopUnconfirmed)
	// task-x：事实无法自洽（执行中却没有 attempt）→ 隔离、排除。外来环境 → 隔离、报警、计入占用。
	h.exec("UPDATE tasks SET status = 'running' WHERE task_id = 'task-x'")
	h.prov.InjectForeign("env-foreign")
	h.failMarks.Store(1)

	clock := newFakeClock()
	cfg := testConfig()
	cfg.Capacity = admission.Capacity{RunSlots: 2, MemoryBytes: 8 << 30}
	cfg.RetryBackoff = func(int) time.Duration { return 0 }
	h.start(cfg, clock)
	rep := h.waitRecovered()
	base := h.waitAddr()

	if !slices.Equal(rep.Excluded, []string{"task-x"}) || !slices.Contains(rep.StopBlocked, "env-y1") {
		t.Fatalf("恢复报告 Excluded=%q StopBlocked=%q", rep.Excluded, rep.StopBlocked)
	}
	ho := stopBlockedHandoff(rep)
	if g, ok := ho.opts.StopBlocked["task-y"]; len(ho.opts.StopBlocked) != 1 || !ok || g.TaskID != "task-y" || g.ID == 0 || len(ho.orphans) != 0 {
		t.Fatalf("stop_blocked 交接 %+v，无 actor 的环境 %+v", ho.opts.StopBlocked, ho.orphans)
	}
	// 报警：两个隔离项都已标记（其中第一次标记失败、在后台重试）。
	eventually(t, "隔离项都已标记报警", nil, func() bool {
		var total, alerted int
		h.queryRow("SELECT count(*), count(*) FILTER (WHERE alerted) FROM quarantined_resources", nil, &total, &alerted)
		return total == 2 && alerted == 2
	})
	marks := 0
	for _, e := range h.events() {
		if strings.HasPrefix(e, "store.MarkQuarantineAlerted") {
			marks++
		}
	}
	if marks != 3 {
		t.Fatalf("MarkQuarantineAlerted 调用 %d 次，期望 3（一次失败后重试）：%q", marks, h.events())
	}

	// 两个槽位分别被 env-y1（stop_blocked 的任务持有）与外来隔离项占用：新任务只能排队。
	z := submit(t, base, "rz", `{}`)
	for i := 0; i < 5; i++ {
		clock.Advance(2 * time.Minute) // 推动 stop_blocked 的退避重试；停止仍无法确认
		time.Sleep(20 * time.Millisecond)
	}
	if _, n := h.taskStatus(z); n != 0 {
		t.Fatalf("槽位被 stop_blocked 占用时新任务创建了 %d 个 attempt", n)
	}
	if _, n := h.taskStatus("task-y"); n != 1 {
		t.Fatalf("停止确认之前 task-y 有 %d 个 attempt，期望只有原来的 1 个", n)
	}

	h.prov.FailStop("env-y1", nil)
	eventually(t, "task-y 与新任务完成", func() { clock.Advance(30 * time.Second) }, func() bool {
		y, _ := h.taskStatus("task-y")
		zs, _ := h.taskStatus(z)
		return y == "succeeded" && zs == "succeeded"
	})
	var class string
	var faults, total int64
	h.queryRow("SELECT outcome_class FROM attempts WHERE attempt_id = 'att-y1'", nil, &class)
	h.queryRow("SELECT fault_retries_used, attempts_total FROM tasks WHERE task_id = 'task-y'", nil, &faults, &total)
	if class != "lost_on_restart" || faults != 1 || total != 2 {
		t.Fatalf("att-y1 outcome=%q，task-y fault_retries_used=%d attempts_total=%d", class, faults, total)
	}
	if n := countOf(h.events(), "store.AccountUnrecordedRunTime:att-y1"); n != 1 {
		t.Fatalf("att-y1 的未记账运行时间补记了 %d 次，期望 1", n)
	}
	if s, n := h.taskStatus("task-x"); s != "running" || n != 0 {
		t.Fatalf("被排除的 task-x 为 %s/%d：不应有 actor", s, n)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// TestOrphanStopBlockedRetried：任务已终态、环境未确认停止（没有 actor 负责）→ 装配的后台重试按退避
// 调用 StopEnv，直到 Recorded 才归还其占用（唯一的 run slot），之后 cleanup loop 清理该环境。
func TestOrphanStopBlockedRetried(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	st, err := postgres.Open(ctx, postgres.Options{DSN: h.dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	installID, err := ownership.Bootstrap(ctx, st, datadir.NewIDFile(h.dir), datadir.NewTokenFile(h.dir),
		func() string { return "inst-orphan" }, func() ([]byte, error) { return make([]byte, datadir.TokenSize), nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-t", BodyHash: []byte{1}, TaskID: "task-t",
		Spec: json.RawMessage(`{}`), MaxFaultRetries: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateAttempt(ctx, task.NewAttempt{TaskID: "task-t", AttemptID: "att-t1", AttemptNo: 1, EnvID: "env-t1"}); err != nil {
		t.Fatal(err)
	}
	// 判决已提交（任务终态），但环境的 stopped_at 尚未记录：重启后没有 actor 负责它。
	if _, err := st.FinalizeAttempt(ctx, task.Verdict{AttemptID: "att-t1", TaskID: "task-t", ControlVersion: 1,
		FromStatus: "starting", AttemptStatus: "ended", OutcomeClass: "succeeded", TaskStatus: "succeeded",
		TaskStatusReason: "succeeded", Result: json.RawMessage(`{"summary":"x","outputs":[]}`), EventType: "task_terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.prov.Create(ctx, provider.EnvSpec{EnvID: "env-t1", InstallID: installID, Kind: provider.KindTask,
		Template: "default", UIDSize: 4096}); err != nil {
		t.Fatal(err)
	}
	const fails = 3 // 第 1 次由恢复执行，其余由后台重试
	h.stopFails = map[string]int{"env-t1": fails}

	clock := newFakeClock()
	cfg := testConfig()
	cfg.Capacity = admission.Capacity{RunSlots: 1, MemoryBytes: 8 << 30}
	h.start(cfg, clock)
	rep := h.waitRecovered()
	base := h.waitAddr()
	ho := stopBlockedHandoff(rep)
	if !slices.Contains(rep.StopBlocked, "env-t1") || len(ho.orphans) != 1 || ho.orphans[0].EnvID != "env-t1" ||
		!ho.orphans[0].HasGrant || len(ho.opts.StopBlocked) != 0 {
		t.Fatalf("StopBlocked=%q 交接 %+v 无 actor 的环境 %+v", rep.StopBlocked, ho.opts.StopBlocked, ho.orphans)
	}
	z := submit(t, base, "rz", `{}`)

	// 每次推进 60 s（不小于任何一次退避）只触发一次重试；停止仍未确认时槽位一直被占用。
	advance := func() { clock.Advance(time.Minute) }
	for n := 2; n <= fails; n++ {
		eventually(t, fmt.Sprintf("第 %d 次停止", n), advance, func() bool { return h.stops("env-t1") >= n })
		if _, a := h.taskStatus(z); a != 0 {
			t.Fatalf("第 %d 次停止仍未确认时新任务已创建 %d 个 attempt：占用被提前归还", n, a)
		}
	}
	eventually(t, "确认停止后新任务完成", advance, func() bool { s, _ := h.taskStatus(z); return s == "succeeded" })
	if n := h.stops("env-t1"); n != fails+1 {
		t.Fatalf("env-t1 的 Stop 调用 %d 次，期望 %d", n, fails+1)
	}
	var before bool
	h.queryRow(`SELECT e.stopped_at <= a.created_at FROM environments e, attempts a
		WHERE e.env_id = 'env-t1' AND a.task_id = $1`, []any{z}, &before)
	if !before {
		t.Fatal("新任务的 attempt 早于 env-t1 的 stopped_at：占用在 Recorded 之前被归还")
	}
	eventually(t, "cleanup loop 清理 env-t1", nil, func() bool {
		var state string
		h.queryRow("SELECT cleanup_state FROM environments WHERE env_id = 'env-t1'", nil, &state)
		return state == "done"
	})
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

func countOf(xs []string, x string) int {
	n := 0
	for _, v := range xs {
		if v == x {
			n++
		}
	}
	return n
}

// TestEffectiveLimits：累计运行时限在创建时确定（规格 §14.4）——省略时补入默认值；显式值须为正且不超过
// 服务端上限；其余键原样保留；永远无法满足容量的 memory_max 被拒绝。
func TestEffectiveLimits(t *testing.T) {
	c := Config{DefaultRunTime: time.Hour, RunTimeCap: 24 * time.Hour, DefaultMemoryBytes: 1 << 30,
		Capacity: admission.Capacity{RunSlots: 4, MemoryBytes: 8 << 30}}
	cases := []struct {
		in, want, err string
	}{
		{in: ``, want: `{"max_run_time_ms":3600000}`},
		{in: `{}`, want: `{"max_run_time_ms":3600000}`},
		{in: `{"memory_max":1024,"x":"keep"}`, want: `{"max_run_time_ms":3600000,"memory_max":1024,"x":"keep"}`},
		{in: `{"max_run_time_ms":60000}`, want: `{"max_run_time_ms":60000}`},
		{in: `{"max_run_time_ms":86400000}`, want: `{"max_run_time_ms":86400000}`},
		{in: `{"max_run_time_ms":0}`, err: "max_run_time_ms"},
		{in: `{"max_run_time_ms":-5}`, err: "负数"},
		{in: `{"max_run_time_ms":86400001}`, err: "max_run_time_ms"},
		{in: `{"max_run_time_ms":1.5}`, err: "不合法"},
		{in: `{"memory_max":17179869184}`, err: "永远无法被授予"},
	}
	for _, tc := range cases {
		got, err := c.effectiveLimits(json.RawMessage(tc.in))
		switch {
		case tc.err != "":
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%s：应拒绝（含 %q），得到 %s, %v", tc.in, tc.err, got, err)
			}
		case err != nil || string(got) != tc.want:
			t.Errorf("%s → %s, %v；期望 %s", tc.in, got, err, tc.want)
		}
	}
	// 存储后的时限即任务的时限：之后修改默认值不影响它。
	stored, _ := c.effectiveLimits(nil)
	c.DefaultRunTime = 5 * time.Minute
	if got := c.runTimeLimit(task.TaskState{Limits: stored}); got != time.Hour {
		t.Fatalf("已存储的时限应为 1h，得到 %s", got)
	}
	bad := c
	bad.DefaultRunTime, bad.RunTimeCap = 48*time.Hour, 24*time.Hour
	if err := bad.validate(); err == nil || !strings.Contains(err.Error(), "运行时限") {
		t.Fatal("默认时限超过上限的配置应被拒绝")
	}
}
