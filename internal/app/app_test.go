package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/admission"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/datadir"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ownership"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/fake"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/recovery"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/session"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
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

	// exec（M4 Plan 15 Task 10）。
	execDigest      func() (string, error) // Deps.ExecImageDigest
	crashed         atomic.Bool            // 模拟崩溃：provider.Stop 不停止（ErrStopUnconfirmed），SettleExec 失败
	failExecDestroy atomic.Int32           // 接下来这么多次 exec 环境的 Destroy 失败
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
		NewProvider:     func(string) (provider.Provider, error) { return recProvider{Provider: h.prov, h: h}, nil },
		ExecImageDigest: h.execDigest,
		Clock:           clock,
		Logger:          slog.New(slog.NewJSONHandler(lockedWriter{h}, nil)),
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

func (s recStore) ConvertLedger(ctx context.Context) (recovery.LedgerConversion, error) {
	s.h.record("store.ConvertLedger")
	return s.Store.ConvertLedger(ctx)
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

// SettleExec 在模拟崩溃时失败：exec 的预留保持 held、调用保持 in_flight（与 SIGKILL 留下的状态相同）。
func (s recStore) SettleExec(ctx context.Context, x call.ExecSettlement) (call.CallRecord, error) {
	if s.h.crashed.Load() {
		return call.CallRecord{}, fmt.Errorf("%w: 模拟崩溃", persistence.ErrUnavailable)
	}
	return s.Store.SettleExec(ctx, x)
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
	if fail || p.h.crashed.Load() {
		return provider.ErrStopUnconfirmed
	}
	if isExecEnv(envID) {
		p.h.record("provider.Stop:" + envID)
	}
	return p.Provider.Stop(ctx, envID)
}

// Destroy：failExecDestroy 次数内 exec 环境的 Destroy 失败（模拟 /out 仍被占用）。
func (p recProvider) Destroy(ctx context.Context, envID string) error {
	if isExecEnv(envID) && p.h.failExecDestroy.Add(-1) >= 0 {
		return errors.New("注入：EBUSY")
	}
	return p.Provider.Destroy(ctx, envID)
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
		"store.LoadRecoveryFacts", "step:recovery_plan", "store.ConvertLedger", "step:recovery_execute", "step:admission_rebuild",
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
		Capacity: admission.Capacity{RunSlots: 4, MemoryBytes: 8 << 30}, DefaultBudgetMicro: 2_000_000, BudgetCapMicro: 50_000_000}
	const b = `"budget_micro":2000000,`
	cases := []struct {
		in, want, err string
	}{
		{in: ``, want: `{` + b + `"max_run_time_ms":3600000}`},
		{in: `{}`, want: `{` + b + `"max_run_time_ms":3600000}`},
		{in: `{"memory_max":1024,"x":"keep"}`, want: `{` + b + `"max_run_time_ms":3600000,"memory_max":1024,"x":"keep"}`},
		{in: `{"max_run_time_ms":60000}`, want: `{` + b + `"max_run_time_ms":60000}`},
		{in: `{"max_run_time_ms":86400000}`, want: `{` + b + `"max_run_time_ms":86400000}`},
		{in: `{"max_run_time_ms":0}`, err: "max_run_time_ms"},
		{in: `{"max_run_time_ms":-5}`, err: "负数"},
		{in: `{"max_run_time_ms":86400001}`, err: "max_run_time_ms"},
		{in: `{"max_run_time_ms":1.5}`, err: "不合法"},
		{in: `{"memory_max":17179869184}`, err: "永远无法被授予"},
		// budget_micro（§9.6）：显式值须为正整数且不超过上限，随任务存储。
		{in: `{"budget_micro":5000}`, want: `{"budget_micro":5000,"max_run_time_ms":3600000}`},
		{in: `{"budget_micro":50000000}`, want: `{"budget_micro":50000000,"max_run_time_ms":3600000}`},
		{in: `{"budget_micro":0}`, err: "budget_micro"},
		{in: `{"budget_micro":50000001}`, err: "budget_micro"},
		{in: `{"budget_micro":-1}`, err: "负数"},
		{in: `{"budget_micro":1.5}`, err: "不合法"},
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
	// init.budget_limits 取存储的 budget_micro。
	l, err := parseLimits(stored)
	if err != nil || string(budgetLimits(l)) != `{"budget_micro":2000000}` {
		t.Fatalf("budget_limits = %s, %v", budgetLimits(l), err)
	}
	bad = c.withDefaults()
	bad.DefaultBudgetMicro, bad.BudgetCapMicro = 60_000_000, 50_000_000
	if err := bad.validate(); err == nil || !strings.Contains(err.Error(), "默认预算") {
		t.Fatalf("默认预算超过上限的配置应被拒绝，得到 %v", err)
	}
}

// TestGatewayConfigValidation：搜索供应商只能是 fake | tavily | ddg_lite | serper（fake 须同时设置 upstream_allow_private，
// tavily 与 serper 须有 Key）；Worker 环境变量只允许白名单中的键（凭据不得经环境变量进入沙箱）。
func TestGatewayConfigValidation(t *testing.T) {
	base := testConfig().withDefaults()
	cases := []struct {
		name string
		mod  func(c *Config)
		err  string
	}{
		{"默认 ddg_lite", func(*Config) {}, ""},
		{"未知供应商", func(c *Config) { c.SearchProvider = "google" }, "fake、tavily、serper 或 ddg_lite"},
		{"serper 无 Key", func(c *Config) { c.SearchProvider = "serper" }, "AGENTBOX_SEARCH_API_KEY"},
		{"serper 有 Key", func(c *Config) { c.SearchProvider, c.SearchAPIKey = "serper", "k" }, ""},
		{"fake 无 allow-private", func(c *Config) { c.SearchProvider = "fake" }, "upstream_allow_private"},
		{"fake 有 allow-private", func(c *Config) { c.SearchProvider, c.UpstreamAllowPrivate = "fake", []string{"127.0.0.1"} }, ""},
		{"tavily 无 Key", func(c *Config) { c.SearchProvider = "tavily" }, "AGENTBOX_SEARCH_API_KEY"},
		{"tavily 有 Key", func(c *Config) { c.SearchProvider, c.SearchAPIKey = "tavily", "k" }, ""},
		{"fake 搜索上游地址", func(c *Config) {
			c.SearchProvider, c.UpstreamAllowPrivate, c.SearchBaseURL = "fake", []string{"127.0.0.1:9"}, "http://127.0.0.1:9"
		}, ""},
		{"搜索上游地址不是 URL", func(c *Config) { c.SearchBaseURL = "127.0.0.1:9/search" }, "绝对 URL"},
		{"模型无名", func(c *Config) { c.Model.BaseURL = "https://m.example/v1" }, "模型名"},
		{"白名单含默认模型", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a", Models: []string{"a", "b"},
				PricingByModel: map[string]upstream.Pricing{"b": {InputMicroPerMTok: 1}}}
		}, ""},
		{"白名单不含默认模型", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a", Models: []string{"b"}}
		}, "不含默认模型"},
		{"未声明模型的单价", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a",
				PricingByModel: map[string]upstream.Pricing{"b": {InputMicroPerMTok: 1}}}
		}, "未声明"},
		{"模型单价为负", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a", Models: []string{"a", "b"},
				PricingByModel: map[string]upstream.Pricing{"b": {OutputMicroPerMTok: -1}}}
		}, "不能为负数"},
		{"Worker 环境变量含 Key", func(c *Config) { c.WorkerEnv = []string{"PYTHONPATH=/opt", "OPENAI_API_KEY=x"} }, "OPENAI_API_KEY"},
		{"Worker 环境变量白名单", func(c *Config) {
			c.WorkerEnv = []string{"PYTHONPATH=/opt", "PYTHONUNBUFFERED=1", "AGENTBOX_GATEWAY_TIMEOUT_S=630"}
		}, ""},
		{"Redis 地址", func(c *Config) { c.RedisAddr = "127.0.0.1:6379" }, ""},
		{"Redis 地址缺端口", func(c *Config) { c.RedisAddr = "127.0.0.1" }, "host:port"},
		{"账号无模型上游", func(c *Config) {
			c.Accounts, c.UserOrchestratorModel, c.UserWorkerModel = true, "a", "a"
		}, "模型上游"},
		{"账号的用户模型已声明", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a", Models: []string{"a", "b"}}
			c.Accounts, c.UserOrchestratorModel, c.UserWorkerModel = true, "b", "a"
		}, ""},
		{"账号的编排模型未声明", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a", Models: []string{"a", "b"}}
			c.Accounts, c.UserOrchestratorModel, c.UserWorkerModel = true, "kimi-k3", "a"
		}, "编排模型"},
		{"账号的 worker 模型未声明", func(c *Config) {
			c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a"}
			c.Accounts, c.UserOrchestratorModel, c.UserWorkerModel = true, "a", ""
		}, "worker模型"},
	}
	for _, tc := range cases {
		c := base
		tc.mod(&c)
		err := c.validate()
		switch {
		case tc.err == "" && err != nil:
			t.Errorf("%s：应通过，得到 %v", tc.name, err)
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s：应拒绝（含 %q），得到 %v", tc.name, tc.err, err)
		}
	}
	if task.RevokeReasonCancel != call.ReasonCancel {
		t.Fatalf("task.RevokeReasonCancel = %q 与 call.ReasonCancel = %q 不一致", task.RevokeReasonCancel, call.ReasonCancel)
	}
}

// ---- Gateway 装配 ----

// gatewayAdapters 把模型白名单与按模型的价格表交给 chat adapter，并把同一份价格表（补上 Version）交给 call
// 结算：白名单内的模型按其价格估算，未给价格的模型用默认价格，白名单外的模型被拒绝。
func TestGatewayAdaptersPerModelPricing(t *testing.T) {
	cfg := testConfig()
	cfg.Model = ModelConfig{BaseURL: "http://127.0.0.1:1/v1", Name: "kimi-k2.6", Models: []string{"kimi-k2.6", "kimi-k3"},
		Pricing:        upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 1_000_000},
		PricingByModel: map[string]upstream.Pricing{"kimi-k3": {InputMicroPerMTok: 2_000_000, OutputMicroPerMTok: 8_000_000}}}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	adapters, pricing, chatPricing := cfg.gatewayAdapters(upstream.NewDialer(upstream.DialerConfig{}))
	if p := chatPricing["kimi-k3"]; p.OutputMicroPerMTok != 8_000_000 || p.Version != "config/in=2000000,out=8000000" {
		t.Fatalf("chat 按模型价格表 %+v", chatPricing)
	}
	if pricing[upstream.KindChat].InputMicroPerMTok != 1_000_000 {
		t.Fatalf("chat 默认价格表 %+v", pricing[upstream.KindChat])
	}
	var chat upstream.Adapter
	for _, a := range adapters {
		if a.Kind() == upstream.KindChat {
			chat = a
		}
	}
	if chat == nil {
		t.Fatal("没有 chat adapter")
	}
	estimate := func(model string) (int64, error) {
		body := `{"messages":[{"role":"user","content":"x"}],"max_tokens":1000000}`
		if model != "" {
			body = `{"model":"` + model + `",` + body[1:]
		}
		out, _, err := chat.Resolve([]byte(body))
		if err != nil {
			return 0, err
		}
		return chat.Estimate(out)
	}
	// max_tokens 截断到 4096：输出项 4096 × 单价；输入项很小，这里只比较量级。
	k3, err1 := estimate("kimi-k3")
	def, err2 := estimate("")
	if err1 != nil || err2 != nil || k3 < 4096*8 || def >= 4096*2 {
		t.Fatalf("估算 kimi-k3=%d（%v）默认=%d（%v）", k3, err1, def, err2)
	}
	if _, err := estimate("kimi-k2.7-code"); err == nil {
		t.Fatal("白名单外的模型应被拒绝")
	}
}

// gatewayWorker 是经 Gateway 发起一次模型调用的 Worker：读取 init（交给 inits）、发出 ready，经本 attempt 的
// Unix socket（<data>/gateway/<attempt_id>.sock，fake provider 的 Worker 在宿主进程内运行）POST
// /v1/chat/completions，把响应状态交给 replies，然后发出 result。envs 收到 Worker 的环境变量。
func gatewayWorker(dataDir string, inits, replies chan<- string, envs chan<- []string) fake.Program {
	return func(ctx context.Context, spec provider.ExecSpec, stdin io.Reader, stdout, _ io.Writer) provider.ExitStatus {
		envs <- spec.Env
		line, err := bufio.NewReader(stdin).ReadBytes('\n')
		if err != nil {
			return provider.ExitStatus{Code: 2}
		}
		inits <- string(line)
		var in protocol.Init
		if err := json.Unmarshal(line, &in); err != nil {
			return provider.ExitStatus{Code: 2}
		}
		emit := func(m map[string]any) {
			b, _ := json.Marshal(m)
			_, _ = stdout.Write(append(b, '\n'))
		}
		emit(map[string]any{"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task",
			"worker": map[string]any{"name": "app-gateway-test", "version": "0"}, "capabilities": []string{}})
		sock := filepath.Join(gatewayDir(dataDir), in.AttemptID+".sock")
		client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}}}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/v1/chat/completions",
			strings.NewReader(`{"messages":[{"role":"user","content":"ping"}]}`))
		if err != nil {
			return provider.ExitStatus{Code: 2}
		}
		req.Header.Set("X-Agentbox-Call-Id", "root/s1/chat/1")
		resp, err := client.Do(req)
		if err != nil {
			replies <- "error: " + err.Error()
		} else {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			replies <- fmt.Sprintf("%d %s blob=%s", resp.StatusCode, b, resp.Header.Get("X-Agentbox-Blob"))
		}
		emit(map[string]any{"type": "result", "v": 1, "seq": 2, "summary": "done", "outputs": []string{}})
		return provider.ExitStatus{Code: 0}
	}
}

// TestGatewayChatCallThroughSocket（Plan 7 Task 5 装配）：真实的 edge、call 协调器与 chat adapter 装配在 app 中，
// Worker 经本 attempt 的 Unix socket 调用 /v1/chat/completions，请求经验证 dialer 到达进程内 fake 模型上游
// （upstream_allow_private 放行），结算后 inspect 汇总该调用与 try（G11：task_id → attempt_id → call_id → try_no）。
// 供应商 Key 只出现在上游请求的 Authorization 头中：不在 init、Worker 环境变量、服务日志、事件与 inspect 中。
func TestGatewayChatCallThroughSocket(t *testing.T) {
	key := "sk-agentbox-test-" + randomHex()
	var upstreamCalls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, "unexpected request", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"id":"chatcmpl-test-1","object":"chat.completion","choices":[{"index":0,`+
			`"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":7}}`); err != nil {
			t.Errorf("fake 上游写响应: %v", err)
		}
	}))
	defer up.Close()
	upURL, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t)
	inits, replies, envs := make(chan string, 4), make(chan string, 4), make(chan []string, 4)
	h.prov = fake.New(gatewayWorker(h.dir, inits, replies, envs))
	cfg := testConfig()
	cfg.Model = ModelConfig{BaseURL: up.URL + "/v1", Name: "m-test", APIKey: key,
		Pricing: upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}}
	cfg.SearchProvider, cfg.UpstreamAllowPrivate = upstream.SearchFake, []string{upURL.Host}
	cfg.WorkerEnv = []string{"PYTHONPATH=/opt/agentbox"}
	h.start(cfg, task.SystemClock())
	base := h.waitAddr()

	id := submit(t, base, "gw-1", `{}`)
	var reply string
	select {
	case reply = <-replies:
	case <-time.After(60 * time.Second):
		t.Fatal("Worker 未完成 Gateway 调用")
	}
	if !strings.HasPrefix(reply, "200 ") || !strings.Contains(reply, "pong") {
		t.Fatalf("Gateway 调用 = %s", reply)
	}
	eventually(t, "任务到达终态", nil, func() bool {
		st, _ := h.taskStatus(id)
		return task.IsTerminal(st)
	})
	if st, _ := h.taskStatus(id); st != "succeeded" {
		t.Fatalf("任务结束为 %s", st)
	}
	if n := upstreamCalls.Load(); n != 1 {
		t.Fatalf("上游收到 %d 次请求，期望 1", n)
	}

	st, body := httpDo(t, "GET", base+"/tasks/"+id+"/inspect", "")
	var in struct {
		Attempts []struct {
			AttemptID string `json:"attempt_id"`
			EnvID     string `json:"env_id"`
		} `json:"attempts"`
		Calls []struct {
			CallID            string `json:"call_id"`
			Endpoint          string `json:"endpoint"`
			State             string `json:"state"`
			FirstAttemptID    string `json:"first_attempt_id"`
			TriesUsed         int64  `json:"tries_used"`
			CostChargedMicro  int64  `json:"cost_charged_micro"`
			UpstreamRequestID string `json:"upstream_request_id"`
			ResultRef         string `json:"result_ref"`
			Tries             []struct {
				TryNo     int64  `json:"try_no"`
				AttemptID string `json:"attempt_id"`
				EnvID     string `json:"env_id"`
				Outcome   string `json:"outcome"`
				CostMicro int64  `json:"cost_micro"`
			} `json:"tries"`
		} `json:"calls"`
	}
	if st != http.StatusOK || json.Unmarshal(body, &in) != nil || len(in.Attempts) != 1 || len(in.Calls) != 1 {
		t.Fatalf("inspect = %d %s", st, body)
	}
	c, att := in.Calls[0], in.Attempts[0]
	// 5 个输入 token × 1 µ$ + 7 个输出 token × 2 µ$ = 19 µ$（按 usage 结算）。
	if c.CallID != "root/s1/chat/1" || c.Endpoint != "/v1/chat/completions" || c.State != "completed" ||
		c.FirstAttemptID != att.AttemptID || c.TriesUsed != 1 || c.CostChargedMicro != 19 ||
		c.UpstreamRequestID != "chatcmpl-test-1" || len(c.ResultRef) != 64 || !strings.Contains(reply, "blob="+c.ResultRef) {
		t.Fatalf("inspect 中的调用 = %+v（attempt %s，回复 %s）", c, att.AttemptID, reply)
	}
	if len(c.Tries) != 1 || c.Tries[0].TryNo != 1 || c.Tries[0].AttemptID != att.AttemptID || c.Tries[0].EnvID != att.EnvID ||
		c.Tries[0].Outcome != "ok" || c.Tries[0].CostMicro != 19 {
		t.Fatalf("inspect 中的 try = %+v", c.Tries)
	}
	if strings.Contains(string(body), "ping") || strings.Contains(string(body), "pong") {
		t.Errorf("inspect 不应包含请求或响应正文：%s", body)
	}

	// init 带 budget_limits（默认预算）；凭据不在 init 与 Worker 环境变量中。
	initLine := <-inits
	if !strings.Contains(initLine, `"budget_limits":{"budget_micro":2000000}`) {
		t.Errorf("init 缺少 budget_limits：%s", initLine)
	}
	env := <-envs
	var events string
	h.queryRow("SELECT COALESCE(string_agg(type || ' ' || payload::text, E'\n'), '') FROM events WHERE task_id = $1",
		[]any{id}, &events)
	h.mu.Lock()
	logs := h.logs.String()
	h.mu.Unlock()
	for name, s := range map[string]string{"init": initLine, "Worker 环境变量": strings.Join(env, "\n"), "服务日志": logs,
		"事件": events, "inspect": string(body)} {
		if strings.Contains(s, key) {
			t.Errorf("供应商 Key 出现在%s中", name)
		}
	}
	if !strings.Contains(logs, `"call_id":"root/s1/chat/1"`) {
		t.Errorf("服务日志应记录调用元数据（检查本身有效）")
	}

	// 任务结束后入口已撤销：socket 文件已删除。
	if _, err := os.Stat(filepath.Join(gatewayDir(h.dir), att.AttemptID+".sock")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("attempt 结束后 socket 仍存在：%v", err)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// writeTestCert 在 dir 中生成 127.0.0.1 的自签名证书与私钥（PEM），返回路径与可信根池。
func writeTestCert(t *testing.T, dir string) (certFile, keyFile string, roots *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "agentbox-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots = x509.NewCertPool()
	roots.AddCert(cert)
	return certFile, keyFile, roots
}

// TestTLSListenerAndWebDir：配置 TLS 证书后 API 以 HTTPS 监听（明文请求不被应答为 API），并同源提供工作台
// 静态文件；证书与私钥只设置其一时拒绝启动。
func TestTLSListenerAndWebDir(t *testing.T) {
	cfg := testConfig()
	cfg.TLSCertFile = "cert.pem"
	if err := Run(context.Background(), cfg, Deps{}); err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("只设置证书时 Run = %v，期望 TLS 配置错误", err)
	}

	h := newHarness(t)
	web := t.TempDir()
	if err := os.WriteFile(filepath.Join(web, "index.html"), []byte("<!doctype html>workbench"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg = testConfig()
	var roots *x509.CertPool
	cfg.TLSCertFile, cfg.TLSKeyFile, roots = writeTestCert(t, t.TempDir())
	cfg.WebDir = web
	h.start(cfg, task.SystemClock())
	addr := strings.TrimPrefix(h.waitAddr(), "http://")
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	get := func(path string, hdr map[string]string) (int, []byte, http.Header) {
		t.Helper()
		req, err := http.NewRequest("GET", "https://"+addr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("HTTPS GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, b, resp.Header
	}
	if st, b, _ := get("/status", map[string]string{"Origin": "https://" + addr}); st != http.StatusOK || !strings.Contains(string(b), `"mode"`) {
		t.Fatalf("HTTPS /status（同源 https Origin）= %d %s", st, b)
	}
	if st, b, _ := get("/status", map[string]string{"Origin": "http://" + addr}); st != http.StatusForbidden {
		t.Fatalf("TLS 下 http Origin 得到 %d %s，期望 403", st, b)
	}
	st, b, hdr := get("/some/view", nil)
	if st != http.StatusOK || string(b) != "<!doctype html>workbench" || hdr.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("HTTPS SPA 回退 = %d %s %v", st, b, hdr)
	}
	if st, b := httpDo(t, "GET", "http://"+addr+"/status", ""); st == http.StatusOK {
		t.Fatalf("明文 HTTP 请求被应答为 API: %d %s", st, b)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}

}

// ---- 共享缓存装配（Plan 9 Task 3） ----

// TestCacheStartup：启动时总会加载缓存签名密钥（不存在则生成 <data>/cache.key，0600），缓存关闭时也一样；
// 没有 Redis 地址时缓存不启用，/status 不含 cache；配置了 Redis（AGENTBOX_TEST_REDIS_ADDR）时 /status 给出
// §11.5 的指标。
func TestCacheStartup(t *testing.T) {
	status := func(t *testing.T, base string) map[string]json.RawMessage {
		t.Helper()
		st, b := httpDo(t, "GET", base+"/status", "")
		var v map[string]json.RawMessage
		if st != http.StatusOK || json.Unmarshal(b, &v) != nil {
			t.Fatalf("GET /status = %d %s", st, b)
		}
		return v
	}
	checkKey := func(t *testing.T, dir string) {
		t.Helper()
		fi, err := os.Stat(filepath.Join(dir, cache.KeyFileName))
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("应生成 0600 的 %s：%v / %v", cache.KeyFileName, fi, err)
		}
	}
	t.Run("无 Redis 地址", func(t *testing.T) {
		h := newHarness(t)
		h.start(testConfig(), task.SystemClock())
		base := h.waitAddr()
		checkKey(t, h.dir)
		if v := status(t, base); v["cache"] != nil {
			t.Fatalf("缓存未启用时 /status 不应含 cache：%s", v["cache"])
		}
		h.cancel()
		if err := h.wait(); err != nil {
			t.Fatalf("Run 返回 %v", err)
		}
	})
	t.Run("Redis", func(t *testing.T) {
		addr := os.Getenv("AGENTBOX_TEST_REDIS_ADDR")
		if addr == "" {
			if os.Getenv("CI") == "true" {
				t.Fatal("CI 中必须设置 AGENTBOX_TEST_REDIS_ADDR")
			}
			t.Skip("未设置 AGENTBOX_TEST_REDIS_ADDR，跳过")
		}
		h := newHarness(t)
		cfg := testConfig()
		cfg.RedisAddr = addr
		h.start(cfg, task.SystemClock())
		base := h.waitAddr()
		checkKey(t, h.dir)
		var m map[string]int64
		if err := json.Unmarshal(status(t, base)["cache"], &m); err != nil {
			t.Fatalf("/status 的 cache：%v", err)
		}
		for _, k := range []string{"hit", "miss", "bypass", "coalesced", "error", "breaker_open", "integrity_failure"} {
			if _, ok := m[k]; !ok {
				t.Fatalf("/status 的 cache 缺少 %s：%v", k, m)
			}
		}
		h.cancel()
		if err := h.wait(); err != nil {
			t.Fatalf("Run 返回 %v", err)
		}
	})
}

// ---- 用户账号装配（Plan 11 Task 4） ----

// TestAccountsWiring：启用账号时 /auth/register 可用（非 TLS 监听的会话 cookie 不带 Secure），POST /research
// 生成的 spec 只含主题与配置的用户模型，任务归属该用户并经 Scheduler 运行到终态；运维 Bearer 能看到它。
// 用户模型不在白名单中时 Run 在取得任何依赖之前返回错误。
func TestAccountsWiring(t *testing.T) {
	model := ModelConfig{BaseURL: "https://m.example/v1", Name: "kimi-k2.6", Models: []string{"kimi-k2.6", "kimi-k3"}}
	bad := testConfig()
	bad.Model, bad.Accounts, bad.UserOrchestratorModel, bad.UserWorkerModel = model, true, "gpt-x", "kimi-k2.6"
	if err := Run(context.Background(), bad, Deps{}); err == nil || !strings.Contains(err.Error(), "gpt-x") {
		t.Fatalf("用户模型未声明时 Run = %v，期望配置错误", err)
	}

	h := newHarness(t)
	cfg := testConfig()
	cfg.Model, cfg.Accounts, cfg.UserOrchestratorModel, cfg.UserWorkerModel = model, true, "kimi-k3", "kimi-k2.6"
	cfg.APIToken = "operator-token-0123456789"
	h.start(cfg, task.SystemClock())
	base := h.waitAddr()
	do := func(method, path, body string, hdr map[string]string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp, b
	}

	resp, b := do("POST", "/auth/register", `{"username":"Alice","password":"Passw0rdX"}`, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /auth/register = %d %s", resp.StatusCode, b)
	}
	var session *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "agentbox_session" {
			session = c
		}
	}
	if session == nil || session.Value == "" || session.Secure || !session.HttpOnly {
		t.Fatalf("会话 cookie = %+v（非 TLS 监听不应带 Secure）", session)
	}
	cookie := map[string]string{"Cookie": session.Name + "=" + session.Value}
	resp, b = do("POST", "/research", `{"request_id":"res-1","topic":"  量子计算的现状  "}`, cookie)
	var created struct {
		TaskID string `json:"task_id"`
	}
	if resp.StatusCode != http.StatusAccepted || json.Unmarshal(b, &created) != nil || created.TaskID == "" {
		t.Fatalf("POST /research = %d %s", resp.StatusCode, b)
	}
	var spec []byte
	var owner *int64
	h.queryRow("SELECT spec_json::text, owner_user_id FROM tasks WHERE task_id = $1", []any{created.TaskID}, &spec, &owner)
	var got map[string]string
	if err := json.Unmarshal(spec, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"topic": "量子计算的现状", "orchestrator_model": "kimi-k3", "worker_model": "kimi-k2.6"}
	if len(got) != len(want) || got["topic"] != want["topic"] || got["orchestrator_model"] != want["orchestrator_model"] ||
		got["worker_model"] != want["worker_model"] {
		t.Fatalf("研究 spec = %s，期望 %v", spec, want)
	}
	if owner == nil || *owner <= 0 {
		t.Fatalf("研究任务应归属用户，owner_user_id = %v", owner)
	}
	eventually(t, "研究任务到达终态", nil, func() bool {
		st, _ := h.taskStatus(created.TaskID)
		return task.IsTerminal(st)
	})
	if resp, b := do("GET", "/tasks", "", map[string]string{"Authorization": "Bearer " + cfg.APIToken}); resp.StatusCode != http.StatusOK ||
		!strings.Contains(string(b), created.TaskID) {
		t.Fatalf("运维 GET /tasks = %d %s", resp.StatusCode, b)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// ---- M4 Plan 12 Task 9：会话装配 ----

// userSession 注册用户并返回带会话 cookie 的请求函数。
func userSession(t *testing.T, base, name string) func(method, path, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(base+"/auth/register", "application/json", strings.NewReader(`{"username":"`+name+`","password":"Passw0rdX"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	var cookie string
	for _, c := range resp.Cookies() {
		if c.Name == "agentbox_session" {
			cookie = c.Name + "=" + c.Value
		}
	}
	if resp.StatusCode != http.StatusCreated || cookie == "" {
		t.Fatalf("注册 %s = %d", name, resp.StatusCode)
	}
	return func(method, path, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Cookie", cookie)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, b
	}
}

// 会话标志与装配：额度 1–1000、evict 须大于 idle-freeze、会话需要账号（Run 拒绝启动）；未配置会话 Worker 时会话端点
// 503 sessions_unavailable；配置后 turn 的 spec 由 server 固定模型，limits 为 server 默认值加 max_tool_calls = 30，
// budgets.tool_call_limit = 30。
func TestSessionsWiring(t *testing.T) {
	model := ModelConfig{BaseURL: "https://m.example/v1", Name: "kimi-k2.6", Models: []string{"kimi-k2.6", "kimi-k3"},
		PricingByModel: map[string]upstream.Pricing{"kimi-k3": {InputMicroPerMTok: 1, OutputMicroPerMTok: 7_000_000}}}
	model.Pricing.OutputMicroPerMTok = 2_000_000
	accounts := func(c Config) Config {
		c.Model, c.Accounts, c.UserOrchestratorModel, c.UserWorkerModel = model, true, "kimi-k3", "kimi-k2.6"
		return c
	}
	for _, tc := range []struct {
		mut  func(*Config)
		want string
	}{
		{func(c *Config) { c.TurnToolBudget = -1 }, "--turn-tool-budget"},
		{func(c *Config) { c.TurnToolBudget = 1001 }, "--turn-tool-budget"},
		{func(c *Config) { c.SessionIdleFreeze, c.SessionEvictAfter = time.Hour, time.Hour }, "--session-evict-after"},
		{func(c *Config) { c.SessionIdleFreeze = -time.Second }, "--session-idle-freeze"},
		{func(c *Config) { c.Accounts, c.Model = false, ModelConfig{}; c.SessionWorkerArgv = []string{"w"} }, "需要用户账号"},
	} {
		c := accounts(testConfig())
		tc.mut(&c)
		if err := Run(context.Background(), c, Deps{}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Run = %v，期望配置错误（%s）", err, tc.want)
		}
	}
	c := accounts(testConfig()).withDefaults()
	spec, limits, err := c.turnSpec("你好", true)
	if err != nil || string(spec) != `{"kind":"turn","text":"你好","deep_research":true,"orchestrator_model":"kimi-k3","worker_model":"kimi-k2.6",`+
		`"research":{"orchestrator_output_micro_per_mtok":7000000,"worker_output_micro_per_mtok":2000000}}` {
		t.Fatalf("TurnSpec spec = %s, %v", spec, err)
	}
	var l map[string]int64
	if err := json.Unmarshal(limits, &l); err != nil || l["max_tool_calls"] != 30 || l["budget_micro"] != c.DefaultBudgetMicro || l["max_run_time_ms"] <= 0 {
		t.Fatalf("TurnSpec limits = %s, %v", limits, err)
	}
	for in, ok := range map[string]bool{`{"max_tool_calls":1}`: true, `{"max_tool_calls":1000}`: true, `{"max_tool_calls":0}`: false,
		`{"max_tool_calls":1001}`: false, `{"max_tool_calls":-3}`: false} {
		if _, err := c.effectiveLimits(json.RawMessage(in)); (err == nil) != ok {
			t.Errorf("effectiveLimits(%s) = %v，期望接受 = %v", in, err, ok)
		}
	}

	// 未配置会话 Worker：会话端点 503 sessions_unavailable。
	h := newHarness(t)
	h.start(accounts(testConfig()), task.SystemClock())
	do := userSession(t, h.waitAddr(), "alice")
	if code, b := do("POST", "/sessions", `{"request_id":"s-1"}`); code != http.StatusServiceUnavailable || errorCode(t, b) != "sessions_unavailable" {
		t.Fatalf("未启用会话时 POST /sessions = %d %s", code, b)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}

	// 配置会话 Worker：turn 的 spec 与 limits 由 server 生成，工具额度写入 budgets。
	h = newHarness(t)
	cfg := accounts(testConfig())
	cfg.SessionWorkerArgv = []string{"session-worker"}
	h.start(cfg, task.SystemClock())
	do = userSession(t, h.waitAddr(), "bob")
	code, b := do("POST", "/sessions", `{"request_id":"s-1"}`)
	var sess struct {
		SessionID string `json:"session_id"`
	}
	if code != http.StatusCreated || json.Unmarshal(b, &sess) != nil || sess.SessionID == "" {
		t.Fatalf("POST /sessions = %d %s", code, b)
	}
	code, b = do("POST", "/sessions/"+sess.SessionID+"/messages", `{"request_id":"m-1","text":"第一条消息","deep_research":false}`)
	var turn struct {
		TurnID string `json:"turn_id"`
	}
	if code != http.StatusAccepted || json.Unmarshal(b, &turn) != nil || turn.TurnID == "" {
		t.Fatalf("POST messages = %d %s", code, b)
	}
	var gotSpec, gotLimits []byte
	var toolLimit int64
	h.queryRow(`SELECT t.spec_json::text, t.limits_json::text, b.tool_call_limit FROM tasks t JOIN budgets b ON b.task_id = t.task_id
		WHERE t.task_id = $1`, []any{turn.TurnID}, &gotSpec, &gotLimits, &toolLimit)
	if err := json.Unmarshal(gotLimits, &l); err != nil || l["max_tool_calls"] != 30 || toolLimit != 30 ||
		!strings.Contains(string(gotSpec), `"worker_model": "kimi-k2.6"`) {
		t.Fatalf("turn spec = %s，limits = %s，tool_call_limit = %d", gotSpec, gotLimits, toolLimit)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// ==== M4 Plan 15 Task 10：exec 装配、启动恢复（E37）、清理与 inspect ====

const execCallID = "root/s1/exec/1"

// execWorld 驱动 exec 装配测试：task Worker 经本 attempt 的 socket POST /v1/exec（同一调用 ID；attempt 2 起带
// X-Agentbox-Retry），把响应交给 replies，然后等待 release（nil 表示不等待）再发出 result；收到 attempt 1 的响应后
// 若 holdFirst 则一直等到被终止（模拟崩溃前未完成的 attempt）。exec 程序（Dir = /out）第 blockRuns 次之前的执行
// 一直运行到被终止，并在开始时关闭 running；其余执行写 /out/result.txt、输出 hello 并以 0 退出。
type execWorld struct {
	h         *harness
	replies   chan string
	release   chan struct{}
	running   chan struct{}
	holdFirst bool
	blockRuns int32
	runs      atomic.Int32
	once      sync.Once
}

func newExecWorld(h *harness) *execWorld {
	w := &execWorld{h: h, replies: make(chan string, 8), running: make(chan struct{})}
	h.prov = fake.New(w.program)
	h.execDigest = func() (string, error) { return "digest-exec-test", nil }
	return w
}

func (w *execWorld) program(ctx context.Context, spec provider.ExecSpec, stdin io.Reader, stdout, _ io.Writer) provider.ExitStatus {
	if spec.Dir == "/out" {
		if w.runs.Add(1) <= w.blockRuns {
			w.once.Do(func() { close(w.running) })
			<-ctx.Done()
			return provider.ExitStatus{Signal: syscall.SIGKILL}
		}
		if err := os.WriteFile(filepath.Join(w.h.prov.OutDir(spec.ExecID), "result.txt"), []byte("42\n"), 0o644); err != nil {
			return provider.ExitStatus{Code: 3}
		}
		_, _ = io.WriteString(stdout, "hello\n")
		return provider.ExitStatus{Code: 0}
	}
	line, err := bufio.NewReader(stdin).ReadBytes('\n')
	if err != nil {
		return provider.ExitStatus{Code: 2}
	}
	var in protocol.Init
	if err := json.Unmarshal(line, &in); err != nil {
		return provider.ExitStatus{Code: 2}
	}
	emit := func(m map[string]any) {
		b, _ := json.Marshal(m)
		_, _ = stdout.Write(append(b, '\n'))
	}
	emit(map[string]any{"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task",
		"worker": map[string]any{"name": "app-exec-test", "version": "0"}, "capabilities": []string{}})
	sock := filepath.Join(gatewayDir(w.h.dir), in.AttemptID+".sock")
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/v1/exec",
		strings.NewReader(`{"language":"python3","code":"print('hello')"}`))
	if err != nil {
		return provider.ExitStatus{Code: 2}
	}
	req.Header.Set("X-Agentbox-Call-Id", execCallID)
	if in.AttemptNo > 1 {
		req.Header.Set("X-Agentbox-Retry", "true")
	}
	resp, err := client.Do(req)
	if err != nil {
		w.replies <- fmt.Sprintf("attempt %d error: %v", in.AttemptNo, err)
	} else {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		w.replies <- fmt.Sprintf("attempt %d %d %s", in.AttemptNo, resp.StatusCode, b)
	}
	if w.holdFirst && in.AttemptNo == 1 {
		<-ctx.Done()
		return provider.ExitStatus{Signal: syscall.SIGKILL}
	}
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return provider.ExitStatus{Signal: syscall.SIGKILL}
		}
	}
	emit(map[string]any{"type": "result", "v": 1, "seq": 2, "summary": "done", "outputs": []string{}})
	return provider.ExitStatus{Code: 0}
}

func (w *execWorld) reply(t *testing.T) string {
	t.Helper()
	select {
	case r := <-w.replies:
		return r
	case <-time.After(60 * time.Second):
		t.Fatal("Worker 未完成 /v1/exec 调用")
	}
	return ""
}

func execTestConfig() Config {
	cfg := testConfig()
	cfg.Exec = ExecConfig{Slots: 2}
	cfg.RetryBackoff = func(int) time.Duration { return 0 }
	return cfg
}

// TestExecWiring：启用 exec 后经 edge 完成一次 exec（fake provider 的 exec 程序）；同步清理失败时 cleanup loop 在 attempt
// 尚无判决时即回收 exec 环境（D8）；inspect 的 exec try 带 env_id、queue_ms、wall_ms、cpu_usec、exec_started_at 与结局。
func TestExecWiring(t *testing.T) {
	h := newHarness(t)
	w := newExecWorld(h)
	w.release = make(chan struct{})
	h.failExecDestroy.Store(1)
	h.start(execTestConfig(), task.SystemClock())
	base := h.waitAddr()
	id := submit(t, base, "ex-1", `{}`)

	r := w.reply(t)
	if !strings.HasPrefix(r, "attempt 1 200 ") || !strings.Contains(r, `"stdout":"hello\n"`) || !strings.Contains(r, "result.txt") {
		t.Fatalf("/v1/exec = %s", r)
	}
	var envID string
	h.queryRow("SELECT env_id FROM call_tries WHERE task_id = $1 AND call_id = $2", []any{id, execCallID}, &envID)
	if !isExecEnv(envID) {
		t.Fatalf("exec try 的环境 %q", envID)
	}
	// 同步清理的 Destroy 失败 → cleanup loop 接手；此时 attempt 仍在运行（Worker 等待 release），没有判决。
	eventually(t, "cleanup loop 回收 exec 环境", nil, func() bool {
		var state string
		var tries int64
		h.queryRow("SELECT cleanup_state, cleanup_tries FROM environments WHERE env_id = $1", []any{envID}, &state, &tries)
		return state == "done" && tries >= 1
	})
	var verdict bool
	h.queryRow(`SELECT a.verdict_hash IS NOT NULL FROM attempts a JOIN environments e ON e.attempt_id = a.attempt_id
		WHERE e.env_id = $1`, []any{envID}, &verdict)
	if verdict {
		t.Fatal("exec 环境回收时所属 attempt 已有判决：本测试应在判决之前完成回收")
	}
	if l := h.liveEnvs(); slices.Contains(l, envID) {
		t.Fatalf("exec 环境仍在 provider 中：%v", l)
	}
	close(w.release)
	eventually(t, "任务完成", nil, func() bool { s, _ := h.taskStatus(id); return s == "succeeded" })

	st, body := httpDo(t, "GET", base+"/tasks/"+id+"/inspect", "")
	var in struct {
		Calls []struct {
			CallID   string `json:"call_id"`
			Endpoint string `json:"endpoint"`
			State    string `json:"state"`
			Tries    []struct {
				EnvID         string     `json:"env_id"`
				Outcome       string     `json:"outcome"`
				QueueMs       *int64     `json:"queue_ms"`
				WallMs        *int64     `json:"wall_ms"`
				CPUUsec       *int64     `json:"cpu_usec"`
				ExecStartedAt *time.Time `json:"exec_started_at"`
			} `json:"tries"`
		} `json:"calls"`
	}
	if st != http.StatusOK || json.Unmarshal(body, &in) != nil || len(in.Calls) != 1 || len(in.Calls[0].Tries) != 1 {
		t.Fatalf("inspect = %d %s", st, body)
	}
	c, tr := in.Calls[0], in.Calls[0].Tries[0]
	if c.CallID != execCallID || c.Endpoint != "/v1/exec" || c.State != "completed" || tr.EnvID != envID || tr.Outcome != "ok" ||
		tr.QueueMs == nil || tr.WallMs == nil || tr.CPUUsec == nil || tr.ExecStartedAt == nil {
		t.Fatalf("inspect 的 exec 调用 = %s", body)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// TestExecDisabledAndTemplate：--exec-slots 0（Config.Exec 零值）→ /v1/exec 为 404 endpoint_not_configured；启用 exec
// 而 exec 模板不可用（缺 python3）→ Run 在取得任何锁之前返回错误。
func TestExecDisabledAndTemplate(t *testing.T) {
	h := newHarness(t)
	w := newExecWorld(h)
	h.start(testConfig(), task.SystemClock())
	base := h.waitAddr()
	id := submit(t, base, "ex-off", `{}`)
	if r := w.reply(t); !strings.HasPrefix(r, "attempt 1 404 ") || !strings.Contains(r, "endpoint_not_configured") {
		t.Fatalf("exec 关闭时 /v1/exec = %s", r)
	}
	eventually(t, "任务完成", nil, func() bool { s, _ := h.taskStatus(id); return task.IsTerminal(s) })
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}

	h2 := newHarness(t)
	h2.execDigest = func() (string, error) {
		return "", errors.New("rootfs: exec 模板中 /usr/bin/python3 不存在")
	}
	h2.start(execTestConfig(), task.SystemClock())
	err := h2.wait()
	if !errors.Is(err, errExecImage) || !strings.Contains(err.Error(), "python3") {
		t.Fatalf("exec 模板缺 python3 时 Run = %v", err)
	}
	if ev := h2.events(); slices.Contains(ev, "step:flock") || slices.Contains(ev, "acquire_ownership") {
		t.Fatalf("exec 模板检查失败后仍取得了锁：%q", ev)
	}
}

// crashDuringExec 启动 app，让 attempt 1 的 exec 运行起来，然后模拟 SIGKILL：provider 不再停止任何环境、exec 结算
// 失败（预留保持 held、调用保持 in_flight、环境未记录 stopped_at，exec 程序仍在运行），再让 Run 返回。返回任务与
// exec 环境。
func crashDuringExec(t *testing.T, h *harness, w *execWorld) (taskID, envID string) {
	t.Helper()
	w.blockRuns, w.holdFirst = 1, true
	h.start(execTestConfig(), task.SystemClock())
	h.waitRecovered()
	base := h.waitAddr()
	taskID = submit(t, base, "e37", `{}`)
	select {
	case <-w.running:
	case <-time.After(60 * time.Second):
		t.Fatal("exec 未开始运行")
	}
	h.queryRow("SELECT env_id FROM call_tries WHERE task_id = $1 AND call_id = $2", []any{taskID, execCallID}, &envID)
	h.crashed.Store(true)
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
	w.reply(t) // attempt 1 收到的响应（Gateway 关闭）不影响结论
	var callState, rsv string
	var stopped bool
	h.queryRow(`SELECT c.state, r.state, e.stopped_at IS NOT NULL FROM calls c
		JOIN call_tries ct ON ct.task_id = c.task_id AND ct.call_id = c.call_id
		JOIN reservations r ON r.reservation_id = ct.reservation_id
		JOIN environments e ON e.env_id = ct.env_id
		WHERE c.task_id = $1 AND c.call_id = $2`, []any{taskID, execCallID}, &callState, &rsv, &stopped)
	if callState != "in_flight" || rsv != "held" || stopped {
		t.Fatalf("崩溃后的状态：调用 %s、预留 %s、stopped_at 已记录 %v；期望 in_flight、held、未记录", callState, rsv, stopped)
	}
	if !slices.Contains(h.liveEnvs(), envID) {
		t.Fatalf("崩溃后 exec 环境 %s 应仍存活：%v", envID, h.liveEnvs())
	}
	h.crashed.Store(false)
	h.mu.Lock()
	h.log = nil
	h.mu.Unlock()
	return taskID, envID
}

// TestE37ExecCrashRecovery（确定性版本）：exec 运行中 server 崩溃 → 重启的恢复阶段先停止旧 exec 环境并记录 stopped_at，
// 之后 API 才开始监听（Gateway 入口只在 actor 启动后建立）；旧 try 为 unknown，cpu_unknown 增加；新 attempt 以同一调用
// 重发（Retry）→ 新 try 在新环境中运行完成。
func TestE37ExecCrashRecovery(t *testing.T) {
	h := newHarness(t)
	w := newExecWorld(h)
	taskID, oldEnv := crashDuringExec(t, h, w)

	h.start(execTestConfig(), task.SystemClock())
	h.waitRecovered()
	h.waitAddr()
	ev := h.events()
	stopAt, apiAt := slices.Index(ev, "provider.Stop:"+oldEnv), slices.Index(ev, "step:api")
	if stopAt < 0 || apiAt < 0 || stopAt > apiAt || slices.Index(ev, "step:recovery_execute") < stopAt {
		t.Fatalf("恢复应先停止旧 exec 环境再启动 API：%q", ev)
	}
	var stopped bool
	var oldOutcome string
	var cpuUnknown int64
	h.queryRow(`SELECT e.stopped_at IS NOT NULL, ct.outcome, q.cpu_unknown_usec FROM call_tries ct
		JOIN environments e ON e.env_id = ct.env_id JOIN exec_quotas q ON q.task_id = ct.task_id
		WHERE ct.task_id = $1 AND ct.call_id = $2 AND ct.try_no = 1`, []any{taskID, execCallID}, &stopped, &oldOutcome, &cpuUnknown)
	if !stopped || oldOutcome != "unknown" || cpuUnknown <= 0 {
		t.Fatalf("旧 try：stopped_at 已记录 %v，结局 %q，cpu_unknown_usec %d", stopped, oldOutcome, cpuUnknown)
	}
	if r := w.reply(t); !strings.HasPrefix(r, "attempt 2 200 ") || !strings.Contains(r, `"stdout":"hello\n"`) {
		t.Fatalf("重跑的 /v1/exec = %s", r)
	}
	eventually(t, "任务完成", nil, func() bool { s, _ := h.taskStatus(taskID); return s == "succeeded" })
	var tries int64
	var newEnv, newOutcome, callState string
	h.queryRow(`SELECT c.tries_used, c.state, ct.env_id, ct.outcome FROM calls c JOIN call_tries ct
		ON ct.task_id = c.task_id AND ct.call_id = c.call_id AND ct.try_no = 2 WHERE c.task_id = $1 AND c.call_id = $2`,
		[]any{taskID, execCallID}, &tries, &callState, &newEnv, &newOutcome)
	if tries != 2 || callState != "completed" || newEnv == oldEnv || newOutcome != "ok" {
		t.Fatalf("重跑：tries_used %d、调用 %s、新环境 %s（旧 %s）、结局 %s", tries, callState, newEnv, oldEnv, newOutcome)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// TestE37CancelledTaskNotRerun：崩溃前用户的取消已提交 → 重启后旧 exec 环境被停止，任务 cancelled，该调用不再出现新 try。
func TestE37CancelledTaskNotRerun(t *testing.T) {
	h := newHarness(t)
	w := newExecWorld(h)
	taskID, oldEnv := crashDuringExec(t, h, w)
	ctx := context.Background()
	st, err := postgres.Open(ctx, postgres.Options{DSN: h.dsn})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AcceptControl(ctx, api.ControlRequest{RequestID: "cancel-e37", BodyHash: []byte{1}, TaskID: taskID,
		Desired: "cancel", Reason: "user"}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	h.start(execTestConfig(), task.SystemClock())
	h.waitRecovered()
	h.waitAddr()
	if ev := h.events(); !slices.Contains(ev, "provider.Stop:"+oldEnv) {
		t.Fatalf("恢复未停止旧 exec 环境：%q", ev)
	}
	eventually(t, "任务取消", nil, func() bool { s, _ := h.taskStatus(taskID); return s == "cancelled" })
	time.Sleep(200 * time.Millisecond) // 给任何（错误的）重跑留出时间
	var tries int64
	var stopped bool
	h.queryRow(`SELECT count(*), bool_and(e.stopped_at IS NOT NULL) FROM call_tries ct JOIN environments e ON e.env_id = ct.env_id
		WHERE ct.task_id = $1`, []any{taskID}, &tries, &stopped)
	if tries != 1 || !stopped {
		t.Fatalf("取消的任务：try 数 %d、环境已停止 %v；期望只有原来的 1 个且已停止", tries, stopped)
	}
	select {
	case r := <-w.replies:
		t.Fatalf("取消的任务不应再有 Worker 调用 exec：%s", r)
	default:
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// ==== M4 Plan 14 Task 7：sub-run 扩展协商、恢复时重新绑定与裁决收尾 ====

// subrunPeer 是脚本化 Worker 的协议对端：逐行读取宿主消息，按类型等待答复。
type subrunPeer struct {
	ctx   context.Context
	lines chan []byte
	out   io.Writer
	seq   int64
}

func newSubrunPeer(ctx context.Context, stdin io.Reader, stdout io.Writer) *subrunPeer {
	p := &subrunPeer{ctx: ctx, lines: make(chan []byte, 16), out: stdout}
	go func() {
		defer close(p.lines)
		r := bufio.NewReader(stdin)
		for {
			b, err := r.ReadBytes('\n')
			if len(bytes.TrimSpace(b)) > 0 {
				select {
				case p.lines <- b:
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return p
}

// emit 发出一条 Worker 事件（补上 v 与递增的 seq）。
func (p *subrunPeer) emit(m map[string]any) {
	p.seq++
	m["v"], m["seq"] = 1, p.seq
	b, _ := json.Marshal(m)
	_, _ = p.out.Write(append(b, '\n'))
}

// await 读到类型为 typ 的宿主消息为止（其他消息跳过）；stdin 结束或 ctx 结束时 ok = false。
func (p *subrunPeer) await(typ string) (raw []byte, msg map[string]any, ok bool) {
	for {
		select {
		case b, open := <-p.lines:
			if !open {
				return nil, nil, false
			}
			var m map[string]any
			if json.Unmarshal(b, &m) == nil && m["type"] == typ {
				return b, m, true
			}
		case <-p.ctx.Done():
			return nil, nil, false
		}
	}
}

// subrunRec 记录每个 attempt 的原始 init（task → attempt_no → 行）、Worker 等待控制的信号与脚本内的意外。
type subrunRec struct {
	mu      sync.Mutex
	inits   map[string]map[int64]string
	waiting chan string // Worker 已到达等待 pause/cancel 的位置：task_id
	fail    chan string
}

func newSubrunRec() *subrunRec {
	return &subrunRec{inits: map[string]map[int64]string{}, waiting: make(chan string, 8), fail: make(chan string, 8)}
}

func (r *subrunRec) init(taskID string, no int64) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.inits[taskID][no]
	return s, ok
}

// subrunScenario 是 protocol/fixtures/v1/scenarios 中的一个场景（只取行）。
type subrunScenario struct {
	Lines []struct {
		From    string          `json:"from"`
		Message json.RawMessage `json:"message"`
	} `json:"lines"`
}

// subrunWorker 按 config.script 与 attempt_no 执行脚本；ready 回 subruns: 1 当且仅当 init 请求了扩展。
func subrunWorker(dataDir string, rec *subrunRec, fixture subrunScenario) fake.Program {
	return func(ctx context.Context, _ provider.ExecSpec, stdin io.Reader, stdout, _ io.Writer) provider.ExitStatus {
		killed := provider.ExitStatus{Signal: syscall.SIGKILL}
		p := newSubrunPeer(ctx, stdin, stdout)
		raw, _, ok := p.await("init")
		if !ok {
			return killed
		}
		var in protocol.Init
		if err := json.Unmarshal(raw, &in); err != nil {
			return provider.ExitStatus{Code: 2}
		}
		rec.mu.Lock()
		if rec.inits[in.TaskID] == nil {
			rec.inits[in.TaskID] = map[int64]string{}
		}
		rec.inits[in.TaskID][in.AttemptNo] = string(raw)
		rec.mu.Unlock()
		var cfg struct {
			Script string `json:"script"`
		}
		_ = json.Unmarshal(in.Config, &cfg) // 没有 script 时按 plain 处理
		failf := func(format string, args ...any) provider.ExitStatus {
			rec.fail <- fmt.Sprintf("%s/%d: ", cfg.Script, in.AttemptNo) + fmt.Sprintf(format, args...)
			return provider.ExitStatus{Code: 3}
		}
		if cfg.Script == "fixture" { // 原样重放 fixture 的 Worker 行；宿主行只等待同类型的答复
			for _, l := range fixture.Lines[1:] {
				var head struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal(l.Message, &head); err != nil {
					return failf("fixture: %v", err)
				}
				if l.From == "host" {
					if _, _, ok := p.await(head.Type); !ok {
						return killed
					}
					continue
				}
				_, _ = stdout.Write(append(bytes.Clone(l.Message), '\n'))
			}
			return provider.ExitStatus{Code: 0}
		}
		ready := map[string]any{"type": "ready", "protocol_version": 1, "mode": "task",
			"worker": map[string]any{"name": "app-subrun-test", "version": "0"}, "capabilities": []string{}}
		if slices.Contains(in.Extensions, protocol.ExtensionSubruns) {
			ready["subruns"] = 1
		}
		p.emit(ready)
		start := func(id string) bool {
			p.emit(map[string]any{"type": "subrun_start", "subrun_id": id, "parent_step_id": "research", "deadline_ms": 600000})
			_, m, ok := p.await("subrun_started")
			return ok && m["subrun_id"] == id && m["status"] == "started"
		}
		checkpoint := func(id string, subs []map[string]any) bool {
			m := map[string]any{"type": "checkpoint", "checkpoint_id": id, "scope": "task", "step_id": "research",
				"state": map[string]any{"cp": id}}
			if subs != nil {
				m["subruns"] = subs
			}
			p.emit(m)
			_, r, ok := p.await("checkpoint_result")
			return ok && r["status"] == "committed"
		}
		waitControl := func(typ string) provider.ExitStatus {
			rec.waiting <- in.TaskID
			if _, _, ok := p.await(typ); !ok {
				return killed
			}
			return provider.ExitStatus{Code: 0} // 收到控制即退出：裁决按 desired（paused / cancelled）
		}
		if in.AttemptNo == 1 {
			switch cfg.Script {
			case "rebind":
				for _, id := range []string{"st1", "st2", "st3"} {
					if !start(id) {
						return failf("subrun_start %s 未被接受", id)
					}
				}
				p.emit(map[string]any{"type": "subrun_end", "subrun_id": "st1", "status": "succeeded", "summary": "s1"})
				body := []byte(`{"summary":"s1","sources":[],"partial":false}`)
				dir := filepath.Join(workspaceDir(dataDir, in.TaskID), "out", in.AttemptID, "subruns")
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return failf("%v", err)
				}
				if err := os.WriteFile(filepath.Join(dir, "st1.json"), body, 0o644); err != nil {
					return failf("%v", err)
				}
				sum := sha256.Sum256(body)
				sha := hex.EncodeToString(sum[:])
				p.emit(map[string]any{"type": "artifact", "artifact_id": "subrun-st1", "path": "subruns/st1.json", "declared_sha256": sha,
					"declared_size": len(body), "media_type": "application/json", "visibility": "internal"})
				if _, m, ok := p.await("artifact_result"); !ok || m["status"] != "saved" {
					return failf("artifact_result = %v", m)
				}
				if !checkpoint("cp-1", []map[string]any{{"subrun_id": "st1", "status": "completed", "result_ref": sha}}) {
					return failf("cp-1 未提交")
				}
				// E42：st3 的 end{succeeded} 之后没有 checkpoint 列出它；E40：st4 取消后崩溃。
				p.emit(map[string]any{"type": "subrun_end", "subrun_id": "st3", "status": "succeeded", "summary": "s3"})
				if !start("st4") {
					return failf("subrun_start st4 未被接受")
				}
				p.emit(map[string]any{"type": "subrun_cancel", "subrun_id": "st4", "reason": "abandon"})
				if !checkpoint("cp-2", nil) { // 同步：之前的事件都已处理
					return failf("cp-2 未提交")
				}
				return killed // 被杀死（crashed_signal）：故障重试
			case "pause":
				if !start("st1") || !checkpoint("cp-1", nil) {
					return failf("st1 或 cp-1 失败")
				}
				return waitControl("pause")
			case "cancel":
				if !start("st1") || !start("st2") {
					return failf("subrun_start 未被接受")
				}
				p.emit(map[string]any{"type": "subrun_end", "subrun_id": "st2", "status": "succeeded", "summary": "s2"})
				if !checkpoint("cp-1", nil) {
					return failf("cp-1 未提交")
				}
				return waitControl("cancel")
			}
		}
		// 恢复后的 attempt（与 plain）直接给出结果：成功裁决收尾仍未终态的 sub-run。
		p.emit(map[string]any{"type": "result", "summary": "done", "outputs": []string{}})
		return provider.ExitStatus{Code: 0}
	}
}

// TestSubrunAttemptLifecycle（M4 Plan 14 Task 7）：真实库 + 脚本化 Worker。
//   - 协商：init.extensions = ["subruns"]，ready 回 subruns: 1；--worker-subruns=false 时 init 无 extensions。
//   - 重新绑定：第 1 个 attempt 启动 st1–st3，checkpoint 列出 st1 completed，st3 end{succeeded} 之后未入 checkpoint（E42），
//     st4 取消后崩溃（E40）；新 attempt 的 init.resume.subruns = [st1 completed+result_ref, st2 started, st3 started,
//     st4 cancelled]，st2、st3 绑定到新 attempt，st4 不重新绑定；成功裁决把仍未终态的 st2、st3 置为
//     failed{not_completed_at_result}。
//   - E45（§13.5 执行中修订）：deadline 在暂停之前已过 → 暂停裁决记录剩余 0，恢复 → timed_out，并在
//     init.resume.subruns 中告知。
//   - task 取消裁决：全部非终态（started、end_proposed）→ cancelled{task_cancel}。
//   - fixture subrun_end_without_checkpoint：st1 由成功裁决事务置为 failed。
func TestSubrunAttemptLifecycle(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "protocol", "fixtures", "v1", "scenarios", "subrun_end_without_checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture subrunScenario
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t)
	rec := newSubrunRec()
	h.prov = fake.New(subrunWorker(h.dir, rec, fixture))
	cfg := testConfig()
	cfg.WorkerSubruns = true
	h.start(cfg, task.SystemClock())
	base := h.waitAddr()

	ids := map[string]string{}
	for _, s := range []string{"rebind", "pause", "cancel", "fixture"} {
		ids[s] = submit(t, base, "sr-"+s, `{"script":"`+s+`"}`)
	}
	checkFail := func() {
		select {
		case f := <-rec.fail:
			t.Fatalf("Worker 脚本：%s", f)
		default:
		}
	}
	waitStatus := func(h *harness, id, want string) {
		t.Helper()
		eventually(t, "任务 "+id+" 到达 "+want, checkFail, func() bool {
			st, _ := h.taskStatus(id)
			return st == want || task.IsTerminal(st)
		})
		if st, _ := h.taskStatus(id); st != want {
			var events string
			h.queryRow("SELECT COALESCE(string_agg(type || ' ' || payload::text, E'\n' ORDER BY task_seq), '') FROM events WHERE task_id = $1",
				[]any{id}, &events)
			t.Fatalf("任务 %s 为 %s，期望 %s；事件：\n%s", id, st, want, events)
		}
	}
	control := func(id, action string) {
		t.Helper()
		st, b := httpDo(t, "POST", base+"/tasks/"+id+"/"+action, `{"request_id":"`+action+"-"+id+`","reason":"test"}`)
		if st != http.StatusAccepted && st != http.StatusOK {
			t.Fatalf("POST %s = %d %s", action, st, b)
		}
	}
	// row 返回 status|绑定 attempt 的序号|result_ref|failure_reason|cancel_reason。
	row := func(id, subrunID string) string {
		var out string
		h.queryRow(`SELECT concat_ws('|', s.status, COALESCE(a.attempt_no::text, '?'), COALESCE(s.result_ref, '-'),
				COALESCE(s.failure_reason, ''), COALESCE(s.cancel_reason, ''))
			FROM subruns s LEFT JOIN attempts a ON a.attempt_id = s.bound_attempt_id WHERE s.task_id = $1 AND s.subrun_id = $2`,
			[]any{id, subrunID}, &out)
		return out
	}
	initOf := func(rec *subrunRec, id string, no int64) protocol.Init {
		t.Helper()
		raw, ok := rec.init(id, no)
		var in protocol.Init
		if !ok || json.Unmarshal([]byte(raw), &in) != nil {
			t.Fatalf("任务 %s 没有第 %d 个 attempt 的 init", id, no)
		}
		return in
	}

	// 暂停与取消：等 Worker 到达等待控制的位置。
	for range 2 {
		var id string
		select {
		case id = <-rec.waiting:
		case f := <-rec.fail:
			t.Fatalf("Worker 脚本：%s", f)
		case <-time.After(60 * time.Second):
			t.Fatal("Worker 未到达等待控制的位置")
		}
		switch id {
		case ids["pause"]:
			// deadline 在暂停之前已过（宿主计时器按原期限，尚未触发）：暂停裁决记录剩余 0，继续时 timed_out。
			// 暂停期间才过期的情形不再超时（暂停期间不计时，见 postgres 的 TestSubrunDeadlineSuspendedWhilePaused 与 e2e E45）。
			h.exec("UPDATE subruns SET deadline_at = now() - interval '1 second' WHERE task_id = $1", id)
			control(id, "pause")
			waitStatus(h, id, "paused")
			control(id, "resume")
		case ids["cancel"]:
			control(id, "cancel")
		}
	}

	// 重新绑定（E40、E42）与成功裁决收尾。
	id := ids["rebind"]
	waitStatus(h, id, "succeeded")
	first := initOf(rec, id, 1)
	if !slices.Equal(first.Extensions, []string{protocol.ExtensionSubruns}) || first.Resume != nil {
		t.Fatalf("第 1 个 attempt 的 init：extensions = %v，resume = %+v", first.Extensions, first.Resume)
	}
	st1 := row(id, "st1")
	sha := strings.Split(st1, "|")[2]
	want := fmt.Sprint([]protocol.ResumeSubrun{{SubrunID: "st1", Status: "completed", ResultRef: sha},
		{SubrunID: "st2", Status: "started"}, {SubrunID: "st3", Status: "started"}, {SubrunID: "st4", Status: "cancelled"}})
	if r := initOf(rec, id, 2).Resume; r == nil || r.CheckpointID != "cp-2" || fmt.Sprint(r.Subruns) != want || len(sha) != 64 {
		t.Fatalf("第 2 个 attempt 的 init.resume = %+v，期望 cp-2 与 %s", r, want)
	}
	for subrunID, w := range map[string]string{
		"st1": "completed|1|" + sha + "||",
		"st2": "failed|2|-|not_completed_at_result|",
		"st3": "failed|2|-|not_completed_at_result|",
		"st4": "cancelled|1|-||orchestrator",
	} {
		if g := row(id, subrunID); g != w {
			t.Errorf("rebind %s = %s，期望 %s", subrunID, g, w)
		}
	}

	// E45：暂停期间过期 → 恢复时 timed_out，并告知 Worker。
	id = ids["pause"]
	waitStatus(h, id, "succeeded")
	if r := initOf(rec, id, 2).Resume; r == nil ||
		fmt.Sprint(r.Subruns) != fmt.Sprint([]protocol.ResumeSubrun{{SubrunID: "st1", Status: "timed_out"}}) {
		t.Fatalf("E45 的 init.resume = %+v", r)
	}
	if g := row(id, "st1"); g != "timed_out|1|-||deadline" {
		t.Errorf("E45 st1 = %s", g)
	}

	// task 取消裁决：started 与 end_proposed 都收尾为 cancelled。
	id = ids["cancel"]
	waitStatus(h, id, "cancelled")
	for _, subrunID := range []string{"st1", "st2"} {
		if g := row(id, subrunID); g != "cancelled|1|-|task_cancel|task_cancel" {
			t.Errorf("取消裁决 %s = %s", subrunID, g)
		}
	}

	// fixture：subrun_end{succeeded} 未入 checkpoint 即 result → 成功裁决把 st1 置为 failed。
	id = ids["fixture"]
	waitStatus(h, id, "succeeded")
	if g := row(id, "st1"); g != "failed|1|-|not_completed_at_result|" {
		t.Errorf("fixture st1 = %s", g)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}

	// --worker-subruns=false：init 不请求扩展（无 extensions 字段）。
	off := newHarness(t)
	offRec := newSubrunRec()
	off.prov = fake.New(subrunWorker(off.dir, offRec, fixture))
	off.start(testConfig(), task.SystemClock())
	id = submit(t, off.waitAddr(), "sr-off", `{"script":"plain"}`)
	waitStatus(off, id, "succeeded")
	if raw, ok := offRec.init(id, 1); !ok || strings.Contains(raw, `"extensions"`) {
		t.Fatalf("--worker-subruns=false 的 init = %s", raw)
	}
	off.cancel()
	if err := off.wait(); err != nil {
		t.Fatalf("Run 返回 %v", err)
	}
}

// startCapture 记录 StartIncarnation 收到的 spec，并以错误返回（不启动 Worker）。
type startCapture struct{ spec runner.IncarnationSpec }

func (s *startCapture) StartIncarnation(_ context.Context, spec runner.IncarnationSpec) (*runner.Incarnation, error) {
	s.spec = spec
	return nil, errors.New("test: 不启动")
}

// TestSessionSubrunWiring（M4 Plan 14 Task 7）：会话 init 按 --worker-subruns 请求扩展；task_start.resume 与 task 模式的
// init.resume 由同一函数组装（含 resume.subruns），没有 checkpoint 时不下发 resume。
func TestSessionSubrunWiring(t *testing.T) {
	for _, on := range []bool{true, false} {
		c := &startCapture{}
		cfg := testConfig()
		cfg.WorkerSubruns = on
		w := sessionWorkers{r: c, cfg: cfg, dataDir: t.TempDir()}
		if _, err := w.Start(context.Background(), session.WorkerStart{SessionID: "s1", IncarnationID: "i1", EnvID: "e1"}); err == nil {
			t.Fatal("StartIncarnation 的错误应原样返回")
		}
		if got := c.spec.Extensions; on != slices.Equal(got, []string{protocol.ExtensionSubruns}) || (!on && got != nil) {
			t.Errorf("--worker-subruns=%v：会话 init.extensions = %v", on, got)
		}
	}
	ts := task.TaskState{Subruns: []task.SubrunState{{SubrunID: "st1", Status: "completed", ResultRef: strings.Repeat("a", 64)},
		{SubrunID: "st2", Status: "timed_out"}}}
	if r := taskResume(ts); r != nil {
		t.Fatalf("没有 checkpoint 时 resume = %+v", r)
	}
	ts.Latest = &task.LatestCheckpoint{CheckpointID: "cp-1", StepID: "research", State: json.RawMessage(`{}`), Refs: []string{}}
	r := taskResume(ts)
	want := fmt.Sprint([]protocol.ResumeSubrun{{SubrunID: "st1", Status: "completed", ResultRef: strings.Repeat("a", 64)},
		{SubrunID: "st2", Status: "timed_out"}})
	if r == nil || r.CheckpointID != "cp-1" || fmt.Sprint(r.Subruns) != want {
		t.Fatalf("resume = %+v", r)
	}
	in := protocol.Init{Type: protocol.TypeInit, Bootstrap: protocol.BootstrapVersion, ProtocolVersions: []int64{protocol.Version},
		Mode: protocol.ModeTask, TaskID: "t", AttemptID: "a", AttemptNo: 2, OutDir: "/workspace/out/a", Resume: r,
		Extensions: Config{WorkerSubruns: true}.workerExtensions()}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.DecodeLine(protocol.HostToWorker, b); err != nil {
		t.Fatalf("组装的 init 未通过协议校验：%v（%s）", err, b)
	}
}
