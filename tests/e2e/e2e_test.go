//go:build linux

// Package e2e 是控制面验收（规格 §16.4 的首个切片与 E1、E4、E7、E8、E10；代码组织 §8）：真实 PostgreSQL、
// 单一装配 app.Run、provider/fake 的进程型 Program（在宿主上直接运行真实的 `python3 -m sim_worker`，
// 不隔离，只供测试）。
//
// Program 扮演"环境"：把 init.out_dir（/workspace/out/<attempt>）改写为宿主 workspace 下的同一目录，
// 并在 stdin/stdout 的逐行转发中提供测试注入点——丢弃宿主回复、在某条 Worker 消息前暂停、在提交后杀死
// Worker 进程组、在产物登记前后改写 out_dir 中的文件。注入点由任务 spec 的 "e2e" 字段声明，只作用于
// 第 1 个 attempt；sim_worker 忽略该字段。Store 阻塞（E8）由包装 Deps.OpenStore 返回的 Store 实现。
//
// 需要 python3 与 AGENTBOX_TEST_DATABASE_URL：本地缺失时跳过，CI（CI=true）中缺失则失败。等待只按条件
// 轮询；期限只决定何时判为失败。
//
// TestReal* 用例（real_isolation_test.go 及各领域文件）以 root 在真实隔离环境中重跑这些验收（provider/local + 生产
// 启动器）。其余领域的用例按文件拆分：system（真实 server 子进程）、gateway、cache、session、subrun、exec。
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/admission"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/invariants"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/fake"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/local"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/resource"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/rootfs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

const (
	dsnEnv = "AGENTBOX_TEST_DATABASE_URL"
	// waitLimit 是条件等待的失败上限，不是通过的依据。
	waitLimit = 90 * time.Second
)

// ---- 前置条件 ----

func required(t *testing.T, what string) {
	t.Helper()
	if os.Getenv("CI") == "true" {
		t.Fatalf("CI 中必须提供 %s", what)
	}
	t.Skipf("缺少 %s，跳过控制面验收", what)
}

// workerPath 返回 sim_worker 所在目录（PYTHONPATH）；没有 python3 时跳过（CI 中失败）。
func workerPath(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		required(t, "python3（运行 sim_worker）")
	}
	dir, err := filepath.Abs(filepath.Join("..", "..", "worker"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sim_worker", "__main__.py")); err != nil {
		t.Fatalf("找不到 sim_worker: %v", err)
	}
	return dir
}

func newDatabase(t *testing.T) string {
	t.Helper()
	admin := os.Getenv(dsnEnv)
	if admin == "" {
		required(t, dsnEnv)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_e2e_" + hex.EncodeToString(b)
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

// ---- 任务 spec 中的注入声明 ----

// directives 是 spec.e2e：只作用于第 1 个 attempt。
type directives struct {
	// KillAfterCommitted：第 N 个 committed 的 checkpoint_result 写给 Worker 之前以 SIGKILL 杀死 Worker
	// 进程组（该回复不送达）。此时 checkpoint 已提交。
	KillAfterCommitted int `json:"kill_after_committed,omitempty"`
	// DropCheckpointResult：丢弃第 N 个 checkpoint_result（宿主已写出，Worker 收不到）。
	DropCheckpointResult int `json:"drop_checkpoint_result,omitempty"`
	// Hold：第 Nth 条该类型的 Worker 消息交给 runner 之前暂停，直到测试放行。
	Hold *holdSpec `json:"hold,omitempty"`
	// Tamper：改写 out_dir 中某个产物的文件（见 tamper*）。
	Tamper *tamperSpec `json:"tamper,omitempty"`
}

type holdSpec struct {
	Type string `json:"type"`
	Nth  int    `json:"nth"`
}

// 产物改写方式（E10）。
const (
	tamperSymlink    = "symlink"     // 登记前把文件换成指向 out_dir 之外同内容文件的符号链接
	tamperDirSymlink = "dir_symlink" // 登记前把路径中的目录换成指向 ../../in 的符号链接
	tamperDotDot     = "dotdot"      // 把 artifact 消息的 path 改为 ../in/<name>（恶意 Worker）
	tamperFIFO       = "fifo"        // 登记前把文件换成 FIFO
	tamperAfterSave  = "after_save"  // artifact_result(saved) 送达 Worker 之前改写文件内容
)

type tamperSpec struct {
	ArtifactID string `json:"artifact_id"`
	Mode       string `json:"mode"`
}

type step map[string]any

func spec(d *directives, summary string, outputs []string, steps ...step) string {
	m := map[string]any{"steps": steps, "summary": summary, "outputs": outputs}
	if steps == nil {
		m["steps"] = []step{}
	}
	if outputs == nil {
		m["outputs"] = []string{}
	}
	if d != nil {
		m["e2e"] = d
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func progressStep(msg string) step  { return step{"op": "progress", "message": msg} }
func checkpointStep(id string) step { return step{"op": "checkpoint", "step_id": id} }
func sleepStep(ms int) step         { return step{"op": "sleep", "ms": ms} }
func artifactStep(id, path, content string) step {
	return step{"op": "artifact", "artifact_id": id, "path": path, "content": content, "media_type": "text/plain"}
}

// ---- 每个任务的观察记录 ----

// hostLine 是一条宿主发给 Worker 的回复。
type hostLine struct {
	AttemptNo    int64
	Type         string `json:"type"`
	CheckpointID string `json:"checkpoint_id"`
	ArtifactID   string `json:"artifact_id"`
	Status       string `json:"status"`
	Code         string `json:"code"`
	Dropped      bool
}

type taskRec struct {
	mu        sync.Mutex
	inits     map[int64]protocol.Init // attempt_no → 发给 Worker 的 init（改写 out_dir 之前）
	host      []hostLine
	worker    map[int64][]string // attempt_no → Worker 消息类型序列
	exits     map[int64]time.Time
	kills     map[int64]func() // attempt_no → 以 SIGKILL 杀死该 attempt 的 Worker（Worker 启动后登记）
	held      chan struct{}    // Hold 生效时关闭
	release   chan struct{}    // 测试关闭以放行
	heldOnce  sync.Once
	relOnce   sync.Once
	tamperErr error
}

func (r *taskRec) hostLines() []hostLine {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.host)
}

func (r *taskRec) workerTypes(n int64) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.worker[n])
}

func (r *taskRec) init(n int64) (protocol.Init, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	in, ok := r.inits[n]
	return in, ok
}

func (r *taskRec) exitAt(n int64) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	at, ok := r.exits[n]
	return at, ok
}

func (r *taskRec) releaseHold() { r.relOnce.Do(func() { close(r.release) }) }

// ---- Store 阻塞（E8）----

// storeGate 记录执行路径上的 Store 调用；阻塞期间每个调用等待 failAfter（慢查询到期）后返回
// persistence.ErrUnavailable，解除阻塞则继续执行真实调用。
type storeGate struct {
	mu        sync.Mutex
	blocked   chan struct{}
	failAfter time.Duration
	calls     []string
	failed    map[string]int
	cacheHook func(call.CacheCompletion) // 可选：CompleteFromCache 执行前调用（E23）
}

func (g *storeGate) pass(ctx context.Context, op string) error {
	g.mu.Lock()
	g.calls = append(g.calls, op)
	ch := g.blocked
	g.mu.Unlock()
	if ch == nil {
		return nil
	}
	t := time.NewTimer(g.failAfter)
	defer t.Stop()
	select {
	case <-ch:
		return nil
	case <-t.C:
	case <-ctx.Done():
	}
	g.mu.Lock()
	g.failed[op]++
	g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: e2e 注入的 Store 阻塞（%s）", persistence.ErrUnavailable, op)
}

func (g *storeGate) block() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blocked == nil {
		g.blocked = make(chan struct{})
	}
}

func (g *storeGate) unblock() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blocked != nil {
		close(g.blocked)
		g.blocked = nil
	}
}

func (g *storeGate) called(op string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Contains(g.calls, op)
}

func (g *storeGate) failures(op string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failed[op]
}

// gatedStore 包装装配使用的 Store：task actor、runner 与 coordinator 的全部用例先经过 storeGate。
type gatedStore struct {
	app.Store
	g *storeGate
}

func (s gatedStore) CreateAttempt(ctx context.Context, a task.NewAttempt) (task.Attempt, error) {
	if err := s.g.pass(ctx, "CreateAttempt:"+a.TaskID); err != nil {
		return task.Attempt{}, err
	}
	return s.Store.CreateAttempt(ctx, a)
}

func (s gatedStore) GetAttempt(ctx context.Context, id string) (task.Attempt, error) {
	if err := s.g.pass(ctx, "GetAttempt:"+id); err != nil {
		return task.Attempt{}, err
	}
	return s.Store.GetAttempt(ctx, id)
}

func (s gatedStore) ApplyControl(ctx context.Context, c task.ApplyControl) (task.ControlState, error) {
	if err := s.g.pass(ctx, "ApplyControl:"+c.TaskID); err != nil {
		return task.ControlState{}, err
	}
	return s.Store.ApplyControl(ctx, c)
}

func (s gatedStore) GetControlState(ctx context.Context, id string) (task.ControlState, error) {
	if err := s.g.pass(ctx, "GetControlState:"+id); err != nil {
		return task.ControlState{}, err
	}
	return s.Store.GetControlState(ctx, id)
}

func (s gatedStore) FinalizeAttempt(ctx context.Context, v task.Verdict) (task.Attempt, error) {
	if err := s.g.pass(ctx, "FinalizeAttempt:"+v.AttemptID); err != nil {
		return task.Attempt{}, err
	}
	return s.Store.FinalizeAttempt(ctx, v)
}

func (s gatedStore) LoadTask(ctx context.Context, id string) (task.TaskState, error) {
	if err := s.g.pass(ctx, "LoadTask:"+id); err != nil {
		return task.TaskState{}, err
	}
	return s.Store.LoadTask(ctx, id)
}

func (s gatedStore) ListActiveTasks(ctx context.Context) ([]string, error) {
	if err := s.g.pass(ctx, "ListActiveTasks"); err != nil {
		return nil, err
	}
	return s.Store.ListActiveTasks(ctx)
}

func (s gatedStore) PersistRunTime(ctx context.Context, taskID, attemptID string, ms int64) (int64, error) {
	if err := s.g.pass(ctx, "PersistRunTime:"+attemptID); err != nil {
		return 0, err
	}
	return s.Store.PersistRunTime(ctx, taskID, attemptID, ms)
}

func (s gatedStore) RevokeAttemptAccess(ctx context.Context, attemptID, reason string) error {
	if err := s.g.pass(ctx, "RevokeAttemptAccess:"+attemptID); err != nil {
		return err
	}
	return s.Store.RevokeAttemptAccess(ctx, attemptID, reason)
}

func (s gatedStore) AppendWorkerEvents(ctx context.Context, attemptID string, ev []runner.WorkerEvent) (runner.Watermark, error) {
	if err := s.g.pass(ctx, "AppendWorkerEvents:"+attemptID); err != nil {
		return runner.Watermark{}, err
	}
	return s.Store.AppendWorkerEvents(ctx, attemptID, ev)
}

func (s gatedStore) WorkerEventWatermark(ctx context.Context, attemptID string) (runner.Watermark, error) {
	if err := s.g.pass(ctx, "WorkerEventWatermark:"+attemptID); err != nil {
		return runner.Watermark{}, err
	}
	return s.Store.WorkerEventWatermark(ctx, attemptID)
}

func (s gatedStore) CommitCheckpoint(ctx context.Context, c runner.Checkpoint) (runner.CommittedCheckpoint, error) {
	if err := s.g.pass(ctx, "CommitCheckpoint:"+c.AttemptID); err != nil {
		return runner.CommittedCheckpoint{}, err
	}
	return s.Store.CommitCheckpoint(ctx, c)
}

func (s gatedStore) QueryCheckpoint(ctx context.Context, sc runner.Scope, id string) (runner.CommittedCheckpoint, error) {
	if err := s.g.pass(ctx, "QueryCheckpoint:"+sc.ID); err != nil {
		return runner.CommittedCheckpoint{}, err
	}
	return s.Store.QueryCheckpoint(ctx, sc, id)
}

func (s gatedStore) RegisterArtifact(ctx context.Context, a runner.Artifact) (runner.ArtifactVersion, error) {
	if err := s.g.pass(ctx, "RegisterArtifact:"+a.AttemptID); err != nil {
		return runner.ArtifactVersion{}, err
	}
	return s.Store.RegisterArtifact(ctx, a)
}

func (s gatedStore) GetArtifact(ctx context.Context, taskID, artifactID, sum string) (runner.ArtifactVersion, error) {
	if err := s.g.pass(ctx, "GetArtifact:"+taskID); err != nil {
		return runner.ArtifactVersion{}, err
	}
	return s.Store.GetArtifact(ctx, taskID, artifactID, sum)
}

func (s gatedStore) LatestArtifact(ctx context.Context, taskID, artifactID string) (runner.ArtifactVersion, error) {
	if err := s.g.pass(ctx, "LatestArtifact:"+taskID); err != nil {
		return runner.ArtifactVersion{}, err
	}
	return s.Store.LatestArtifact(ctx, taskID, artifactID)
}

func (s gatedStore) RecordTerminalProposal(ctx context.Context, p runner.TerminalProposal) (runner.TerminalProposal, error) {
	if err := s.g.pass(ctx, "RecordTerminalProposal:"+p.AttemptID); err != nil {
		return runner.TerminalProposal{}, err
	}
	return s.Store.RecordTerminalProposal(ctx, p)
}

func (s gatedStore) GetTerminalProposal(ctx context.Context, attemptID string) (runner.TerminalProposal, error) {
	if err := s.g.pass(ctx, "GetTerminalProposal:"+attemptID); err != nil {
		return runner.TerminalProposal{}, err
	}
	return s.Store.GetTerminalProposal(ctx, attemptID)
}

func (s gatedStore) RecordIntent(ctx context.Context, i resource.Intent) (resource.Intent, error) {
	if err := s.g.pass(ctx, "RecordIntent:"+i.EnvID); err != nil {
		return resource.Intent{}, err
	}
	return s.Store.RecordIntent(ctx, i)
}

func (s gatedStore) ResolveIntent(ctx context.Context, intentID, state string) (resource.Intent, error) {
	if err := s.g.pass(ctx, "ResolveIntent:"+intentID); err != nil {
		return resource.Intent{}, err
	}
	return s.Store.ResolveIntent(ctx, intentID, state)
}

func (s gatedStore) GetIntent(ctx context.Context, intentID string) (resource.Intent, error) {
	if err := s.g.pass(ctx, "GetIntent:"+intentID); err != nil {
		return resource.Intent{}, err
	}
	return s.Store.GetIntent(ctx, intentID)
}

func (s gatedStore) SeedUIDRanges(ctx context.Context, base, size int64, count int) error {
	if err := s.g.pass(ctx, "SeedUIDRanges"); err != nil {
		return err
	}
	return s.Store.SeedUIDRanges(ctx, base, size, count)
}

func (s gatedStore) AssignUIDRange(ctx context.Context, envID, allocationID string) (resource.UIDRange, error) {
	if err := s.g.pass(ctx, "AssignUIDRange:"+envID); err != nil {
		return resource.UIDRange{}, err
	}
	return s.Store.AssignUIDRange(ctx, envID, allocationID)
}

func (s gatedStore) ReleaseUIDRange(ctx context.Context, rangeID, allocationID string) (resource.UIDRange, error) {
	if err := s.g.pass(ctx, "ReleaseUIDRange:"+rangeID); err != nil {
		return resource.UIDRange{}, err
	}
	return s.Store.ReleaseUIDRange(ctx, rangeID, allocationID)
}

func (s gatedStore) GetUIDRange(ctx context.Context, envID string) (resource.UIDRange, error) {
	if err := s.g.pass(ctx, "GetUIDRange:"+envID); err != nil {
		return resource.UIDRange{}, err
	}
	return s.Store.GetUIDRange(ctx, envID)
}

func (s gatedStore) MarkStopped(ctx context.Context, envID string, at time.Time) (resource.Environment, error) {
	if err := s.g.pass(ctx, "MarkStopped:"+envID); err != nil {
		return resource.Environment{}, err
	}
	return s.Store.MarkStopped(ctx, envID, at)
}

func (s gatedStore) UpdateCleanup(ctx context.Context, u resource.CleanupUpdate) (resource.Environment, error) {
	if err := s.g.pass(ctx, "UpdateCleanup:"+u.EnvID); err != nil {
		return resource.Environment{}, err
	}
	return s.Store.UpdateCleanup(ctx, u)
}

func (s gatedStore) GetEnvironment(ctx context.Context, envID string) (resource.Environment, error) {
	if err := s.g.pass(ctx, "GetEnvironment:"+envID); err != nil {
		return resource.Environment{}, err
	}
	return s.Store.GetEnvironment(ctx, envID)
}

func (s gatedStore) ListCleanupCandidates(ctx context.Context, now time.Time, limit int) ([]resource.Environment, error) {
	if err := s.g.pass(ctx, "ListCleanupCandidates"); err != nil {
		return nil, err
	}
	return s.Store.ListCleanupCandidates(ctx, now, limit)
}

func (s gatedStore) RecordQuarantine(ctx context.Context, q resource.Quarantine) error {
	if err := s.g.pass(ctx, "RecordQuarantine:"+q.Path); err != nil {
		return err
	}
	return s.Store.RecordQuarantine(ctx, q)
}

// CompleteFromCache 是缓存命中与合并的 Tx2（E23）：设置了 cacheHook 时先调用它（测试在其中暂停，制造"查缓存
// 命中之后、Tx2 之前"的取消竞争），再执行真实事务。不经 storeGate 的阻塞。
func (s gatedStore) CompleteFromCache(ctx context.Context, r call.CacheCompletion) (call.CallRecord, error) {
	s.g.mu.Lock()
	hook := s.g.cacheHook
	s.g.mu.Unlock()
	if hook != nil {
		hook(r)
	}
	return s.Store.CompleteFromCache(ctx, r)
}

// ---- 装置 ----

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) tail(n int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.b.String()
	if len(s) > n {
		s = s[len(s)-n:]
	}
	return s
}

type harness struct {
	t      *testing.T
	dsn    string
	dir    string
	pyPath string
	prov   provider.Provider // fake.Provider（进程型 Program），或真实隔离时包装 provider/local 的 interceptProv
	gate   *storeGate
	logs   lockedBuffer

	// 真实隔离（newRealHarness）：provider/local + 生产启动器。real 为 false 时以下字段不用。
	real        bool
	newProvider func(installID string) (provider.Provider, error)
	local       *local.Provider
	cgRoot      string
	installID   string

	store *postgres.Store // 测试自己的读取连接（不经 gate、不绑定所有权）
	blobs *blob.Local
	base  string

	mu             sync.Mutex
	tasks          map[string]*taskRec
	controlRetries map[string]int // 控制请求得到 503 后以同一 request_id 重试的次数（按错误码）

	cancel     context.CancelFunc
	result     chan error
	resultSeen bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, pyPath: workerPath(t), dsn: newDatabase(t), dir: t.TempDir(),
		gate: &storeGate{failAfter: 500 * time.Millisecond, failed: map[string]int{}}, tasks: map[string]*taskRec{},
		controlRetries: map[string]int{}}
	h.prov = fake.New(h.program)
	h.cleanup()
	return h
}

// cleanup 登记测试结束时的收尾：放行所有暂停、停止服务、关闭读取连接；失败时输出服务日志。
func (h *harness) cleanup() {
	t := h.t
	t.Cleanup(func() {
		if h.cancel != nil {
			h.gate.unblock()
			h.mu.Lock()
			for _, r := range h.tasks {
				r.releaseHold()
			}
			h.mu.Unlock()
			h.cancel()
			if !h.resultSeen {
				select {
				case <-h.result:
				case <-time.After(waitLimit):
					t.Errorf("Run 未在期限内返回")
				}
			}
		}
		if h.store != nil {
			h.store.Close()
		}
		if t.Failed() {
			t.Logf("服务日志（末尾）：\n%s", h.logs.tail(32<<10))
		}
	})
}

func baseConfig() app.Config {
	return app.Config{Listen: "127.0.0.1:0", ShutdownTimeout: 20 * time.Second, DefaultRunTime: 1000 * time.Hour, RunTimeCap: 1000 * time.Hour,
		Capacity: admission.Capacity{RunSlots: 4, MemoryBytes: 8 << 30}}
}

// start 以单一装配 app.Run 启动服务（真实 PostgreSQL、fake provider 的进程型 Program）。
func (h *harness) start(cfg app.Config) {
	h.t.Helper()
	addr := make(chan string, 1)
	d := app.Deps{
		DataDir: h.dir,
		AcquireOwnership: func(ctx context.Context) (app.Ownership, error) {
			o, err := postgres.AcquireOwnership(ctx, h.dsn, postgres.OwnershipOptions{CheckInterval: 200 * time.Millisecond, CheckTimeout: 2 * time.Second})
			if err != nil {
				return nil, err
			}
			return o, nil
		},
		OpenStore: func(ctx context.Context, own app.Ownership) (app.Store, error) {
			s, err := postgres.Open(ctx, postgres.Options{DSN: h.dsn, Ownership: own.(*postgres.Ownership)})
			if err != nil {
				return nil, err
			}
			return gatedStore{Store: s, g: h.gate}, nil
		},
		NewProvider: func(string) (provider.Provider, error) { return h.prov, nil },
		Logger:      slog.New(slog.NewJSONHandler(&h.logs, nil)),
		Hooks:       app.Hooks{Listening: func(a string) { addr <- a }},
	}
	if h.real {
		d.NewProvider = h.newProvider
		if len(cfg.WorkerEnv) == 0 {
			cfg.WorkerEnv = []string{"PYTHONPATH=" + rootfs.WorkerDir}
		}
		if cfg.Exec.Enabled() {
			d.ExecImageDigest = execImageDigest // 与 cmd/agentbox 的 prepareIsolation 相同
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.result = make(chan error, 1)
	go func() { h.result <- app.Run(ctx, cfg, d) }()
	select {
	case a := <-addr:
		h.base = "http://" + a
	case err := <-h.result:
		h.resultSeen = true
		h.t.Fatalf("Run 提前返回: %v", err)
	case <-time.After(waitLimit):
		h.t.Fatal("API 未开始监听")
	}
	st, err := postgres.Open(context.Background(), postgres.Options{DSN: h.dsn})
	if err != nil {
		h.t.Fatal(err)
	}
	h.store = st
	if h.blobs, err = blob.NewLocal(filepath.Join(h.dir, "blobs")); err != nil {
		h.t.Fatal(err)
	}
}

// finish 是每个测试结束时的公共检查：无泄漏（全部环境停止、销毁且 cleanup_state = done），静止时
// 不变量成立（verify-invariants 的同一检查），然后正常停止服务。
func (h *harness) finish() {
	h.t.Helper()
	h.assertNoLeak()
	vs, err := invariants.Verify(context.Background(), h.store, h.prov, h.blobs, true)
	if err != nil {
		h.t.Fatalf("不变量检查: %v", err)
	}
	if len(vs) != 0 {
		h.t.Fatalf("不变量违反：%+v", vs)
	}
	h.cancel()
	select {
	case err := <-h.result:
		h.resultSeen = true
		if err != nil {
			h.t.Fatalf("Run 返回 %v", err)
		}
	case <-time.After(waitLimit):
		h.t.Fatal("Run 未在期限内返回")
	}
}

// cleanupRate 是 cleanup loop 的吞吐上限：每 2 s 一轮、每轮至多 16 个环境（resource.Options 的默认值，
// 装配未暴露）。高吞吐的测试结束时有待清理的积压，等待期限按积压放宽。
const cleanupRate = 8 // 个环境 / s

func (h *harness) assertNoLeak() {
	h.t.Helper()
	var backlog int
	h.queryRow("SELECT count(*) FROM environments WHERE cleanup_state <> 'done'", nil, &backlog)
	limit := waitLimit + time.Duration(backlog/cleanupRate)*time.Second
	start := time.Now()
	eventuallyWithin(h.t, "全部环境停止、销毁并清理完成，UID 范围全部归还", limit, 100*time.Millisecond, func() bool {
		envs, _ := h.prov.List(context.Background())
		if len(envs) != 0 {
			return false
		}
		var pending, held int
		h.queryRow("SELECT count(*) FROM environments WHERE cleanup_state <> 'done' OR stopped_at IS NULL", nil, &pending)
		h.queryRow("SELECT count(*) FROM uid_ranges WHERE state <> 'free'", nil, &held)
		return pending == 0 && held == 0
	})
	if backlog > 100 {
		h.t.Logf("清理积压 %d 个环境，排空用时 %s", backlog, time.Since(start).Round(time.Millisecond))
	}
}

// assertSlotsFree：n 个阻塞任务能同时运行（槽位全部归还），随后取消它们。
func (h *harness) assertSlotsFree(n int) {
	h.t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = h.submit(fmt.Sprintf("slots-%d", i), spec(nil, "", nil, sleepStep(600000)))
	}
	eventually(h.t, fmt.Sprintf("%d 个环境同时运行", n), func() bool {
		envs, _ := h.prov.List(context.Background())
		running := 0
		for _, e := range envs {
			if e.Running {
				running++
			}
		}
		return running == n
	})
	for i, id := range ids {
		if st, b := h.cancelTask(id, fmt.Sprintf("slots-cancel-%d", i)); st != http.StatusOK {
			h.t.Fatalf("取消 %s = %d %s", id, st, b)
		}
	}
	for _, id := range ids {
		if v := h.waitTerminal(id); v.Status != "cancelled" {
			h.t.Fatalf("阻塞任务 %s 结束为 %s", id, v.Status)
		}
	}
}

func (h *harness) rec(taskID string) *taskRec {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.tasks[taskID]
	if r == nil {
		r = &taskRec{inits: map[int64]protocol.Init{}, worker: map[int64][]string{}, exits: map[int64]time.Time{},
			kills: map[int64]func(){}, held: make(chan struct{}), release: make(chan struct{})}
		h.tasks[taskID] = r
	}
	return r
}

func (h *harness) waitHeld(taskID string) {
	h.t.Helper()
	select {
	case <-h.rec(taskID).held:
	case <-time.After(waitLimit):
		h.t.Fatalf("任务 %s 的 Worker 消息未到达暂停点", taskID)
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

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	eventuallyWithin(t, what, waitLimit, 10*time.Millisecond, cond)
}

func eventuallyWithin(t *testing.T, what string, limit, poll time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时（%s）：%s", limit, what)
		}
		time.Sleep(poll)
	}
}

// ---- HTTP 与读取 ----

func httpDo(method, u, body string) (int, []byte, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func (h *harness) submitE(requestID, spec string) (string, error) {
	st, b, err := httpDo("POST", h.base+"/tasks", `{"request_id":"`+requestID+`","spec":`+spec+`}`)
	if err != nil {
		return "", err
	}
	var r struct {
		TaskID string `json:"task_id"`
	}
	if st != http.StatusCreated || json.Unmarshal(b, &r) != nil || r.TaskID == "" {
		return "", fmt.Errorf("POST /tasks = %d %s", st, b)
	}
	return r.TaskID, nil
}

func (h *harness) submit(requestID, spec string) string {
	h.t.Helper()
	id, err := h.submitE(requestID, spec)
	if err != nil {
		h.t.Fatal(err)
	}
	return id
}

// cancelTaskE 发出取消；503（contention、store_unavailable、commit_unknown）按 API 契约以同一 request_id
// 重试（幂等），与正确的客户端行为相同，重试次数计入 h.controlRetries。
func (h *harness) cancelTaskE(taskID, requestID string) (int, string, error) {
	deadline := time.Now().Add(waitLimit)
	for {
		st, b, err := httpDo("POST", h.base+"/tasks/"+taskID+"/cancel", `{"request_id":"`+requestID+`","reason":"e2e"}`)
		var e struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(b, &e)
		if err != nil || st != http.StatusServiceUnavailable || time.Now().After(deadline) ||
			(e.Code != "contention" && e.Code != "store_unavailable" && e.Code != "commit_unknown") {
			return st, e.Code, err
		}
		h.mu.Lock()
		h.controlRetries[e.Code]++
		h.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (h *harness) cancelTask(taskID, requestID string) (int, string) {
	h.t.Helper()
	st, code, err := h.cancelTaskE(taskID, requestID)
	if err != nil {
		h.t.Fatal(err)
	}
	return st, code
}

// httpTask 经 API 读取任务视图。
func (h *harness) httpTask(id string) (status, reason string, attempts int64) {
	h.t.Helper()
	st, b, err := httpDo("GET", h.base+"/tasks/"+id, "")
	var v struct {
		Status        string `json:"status"`
		StatusReason  string `json:"status_reason"`
		AttemptsTotal int64  `json:"attempts_total"`
	}
	if err != nil || st != http.StatusOK || json.Unmarshal(b, &v) != nil {
		h.t.Fatalf("GET /tasks/%s = %d %s %v", id, st, b, err)
	}
	return v.Status, v.StatusReason, v.AttemptsTotal
}

func (h *harness) waitTerminalE(id string) (api.TaskView, error) {
	deadline := time.Now().Add(waitLimit)
	for {
		v, err := h.store.GetTask(context.Background(), id)
		if err == nil && task.IsTerminal(v.Status) {
			return v, nil
		}
		if time.Now().After(deadline) {
			return v, fmt.Errorf("任务 %s 未到达终态（%s，%v）", id, v.Status, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *harness) waitTerminal(id string) api.TaskView {
	h.t.Helper()
	v, err := h.waitTerminalE(id)
	if err != nil {
		h.t.Fatal(err)
	}
	return v
}

func (h *harness) inspect(id string) api.Inspection {
	h.t.Helper()
	in, err := h.store.Inspect(context.Background(), id)
	if err != nil {
		h.t.Fatalf("Inspect %s: %v", id, err)
	}
	return in
}

func (h *harness) loadTask(id string) task.TaskState {
	h.t.Helper()
	ts, err := h.store.LoadTask(context.Background(), id)
	if err != nil {
		h.t.Fatalf("LoadTask %s: %v", id, err)
	}
	return ts
}

func countType(evs []api.Event, typ string) int {
	n := 0
	for _, e := range evs {
		if e.Type == typ {
			n++
		}
	}
	return n
}

func (h *harness) eventsE(id string) ([]api.Event, error) {
	return h.store.ListEvents(context.Background(), id, 0, 10000)
}

func (h *harness) events(id string) []api.Event {
	h.t.Helper()
	evs, err := h.eventsE(id)
	if err != nil {
		h.t.Fatal(err)
	}
	return evs
}

// readBlob 从 BlobStore 读取内容并校验其 sha256。
func (h *harness) readBlob(sum string) []byte {
	h.t.Helper()
	rc, err := h.blobs.Open(sum)
	if err != nil {
		h.t.Fatalf("读取 blob %s: %v", sum, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		h.t.Fatal(err)
	}
	if got := sha256Hex(b); got != sum {
		h.t.Fatalf("blob %s 的内容哈希为 %s", sum, got)
	}
	return b
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type pinnedRef struct {
	ArtifactID string `json:"artifact_id"`
	Version    int64  `json:"version"`
	SHA256     string `json:"sha256"`
}

// pinnedResult 读取 attempt 已保存的 result 提议（终态提议内容在 BlobStore 中，Ref 为其 sha256），并按
// 固定版本读回每个输出：Store 中登记的 (artifact_id, version, sha256) 与之一致，BlobStore 内容的哈希
// 与 sha256 一致。返回 artifact_id → 内容。
func (h *harness) pinnedResult(taskID, attemptID string) (summary string, outputs []pinnedRef, content map[string]string) {
	h.t.Helper()
	ctx := context.Background()
	p, err := h.store.GetTerminalProposal(ctx, attemptID)
	if err != nil || p.Kind != "result" {
		h.t.Fatalf("attempt %s 的终态提议 %+v %v", attemptID, p, err)
	}
	var r struct {
		Summary string      `json:"summary"`
		Outputs []pinnedRef `json:"outputs"`
	}
	if err := json.Unmarshal(h.readBlob(p.Ref), &r); err != nil {
		h.t.Fatal(err)
	}
	content = map[string]string{}
	for _, o := range r.Outputs {
		v, err := h.store.GetArtifact(ctx, taskID, o.ArtifactID, o.SHA256)
		if err != nil || v.Version != o.Version || o.Version == 0 {
			h.t.Fatalf("固定输出 %+v 在 Store 中为 %+v %v", o, v, err)
		}
		content[o.ArtifactID] = string(h.readBlob(o.SHA256))
	}
	return r.Summary, r.Outputs, content
}

// assertPinnedDownloads 经 API 下载结果与每个固定版本的产物（规格 §15.1）：ETag 为内容的带引号 sha256，
// 结果中的固定输出与终态提议一致，产物内容与 BlobStore 中的一致。
func (h *harness) assertPinnedDownloads(taskID string, outs []pinnedRef, content map[string]string) {
	h.t.Helper()
	get := func(path string) ([]byte, string) {
		resp, err := http.Get(h.base + path)
		if err != nil {
			h.t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			h.t.Fatalf("GET %s = %d %s %v", path, resp.StatusCode, b, err)
		}
		s := sha256.Sum256(b)
		sum := hex.EncodeToString(s[:])
		if etag := resp.Header.Get("ETag"); etag != `"`+sum+`"` {
			h.t.Fatalf("GET %s：ETag %s 与内容的 sha256 %s 不符", path, etag, sum)
		}
		return b, sum
	}
	b, _ := get("/tasks/" + taskID + "/result")
	var r struct {
		Outputs []pinnedRef `json:"outputs"`
	}
	if err := json.Unmarshal(b, &r); err != nil || !slices.Equal(r.Outputs, outs) {
		h.t.Fatalf("GET /result 的固定输出 %s，期望 %+v", b, outs)
	}
	for _, o := range outs {
		b, sha := get(fmt.Sprintf("/tasks/%s/artifacts/%s?version=%d", taskID, url.PathEscape(o.ArtifactID), o.Version))
		if sha != o.SHA256 || string(b) != content[o.ArtifactID] {
			h.t.Fatalf("产物 %+v 下载为 %q（sha256 %s）", o, b, sha)
		}
	}
}

func attemptByNo(in api.Inspection, n int64) api.AttemptView {
	for _, a := range in.Attempts {
		if a.AttemptNo == n {
			return a
		}
	}
	return api.AttemptView{}
}

func checkpointByStep(in api.Inspection, stepID string) api.CheckpointView {
	for _, c := range in.Checkpoints {
		if c.StepID == stepID {
			return c
		}
	}
	return api.CheckpointView{}
}

// ---- 进程型 Program：在宿主上运行真实的 sim_worker ----

type workerRun struct {
	h      *harness
	rec    *taskRec
	in     protocol.Init
	d      directives
	ws     string
	outDir string
	pid    int
	killFn func()

	ckResults, committed int
	typeCount            map[string]int
	paths                map[string]string // artifact_id → path（after_save 用；rec.mu 保护，两个转发方向都访问）
}

// program 是 fake provider 的 Program：读取 init，把 out_dir 改写为宿主 workspace 下的同一目录，启动
// spec.Argv（python3 -m sim_worker）为独立进程组，逐行转发 stdin/stdout 并执行注入；ctx 结束（Stop、
// Terminate）时 SIGKILL 进程组。
func (h *harness) program(ctx context.Context, spec provider.ExecSpec, stdin io.Reader, stdout, stderr io.Writer) provider.ExitStatus {
	br := bufio.NewReaderSize(stdin, 1<<20)
	first := make(chan []byte, 1)
	go func() {
		b, _ := br.ReadBytes('\n')
		first <- b
	}()
	var line []byte
	select {
	case line = <-first:
	case <-ctx.Done():
		return provider.ExitStatus{Signal: syscall.SIGKILL}
	}
	var in protocol.Init
	if err := json.Unmarshal(line, &in); err != nil {
		return provider.ExitStatus{Code: 2}
	}
	run := h.newRun(in)
	hostInit := in
	hostInit.OutDir = run.outDir
	initLine, err := protocol.EncodeLine(protocol.HostToWorker, &hostInit)
	if err != nil {
		return provider.ExitStatus{Code: 2}
	}

	cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	cmd.Dir = run.ws
	cmd.Env = append(os.Environ(), "PYTHONPATH="+h.pyPath)
	cmd.Env = append(cmd.Env, spec.Env...)
	// 进程型 Program 没有挂载：Worker 经 SDK 的路径覆盖直接连接该 attempt 的 Gateway socket（<data>/gateway）。
	cmd.Env = append(cmd.Env, gatewaySocketEnv+"="+h.gwSocket(in.AttemptID))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stderr = stderr
	pin, err := cmd.StdinPipe()
	if err != nil {
		return provider.ExitStatus{Code: 127}
	}
	pout, err := cmd.StdoutPipe()
	if err != nil {
		return provider.ExitStatus{Code: 127}
	}
	if err := cmd.Start(); err != nil {
		return provider.ExitStatus{Code: 127}
	}
	run.pid = cmd.Process.Pid
	run.rec.mu.Lock()
	run.rec.kills[in.AttemptNo] = run.kill
	run.rec.mu.Unlock()
	stopKill := context.AfterFunc(ctx, run.kill)
	defer stopKill()
	_, _ = pin.Write(append(initLine, '\n'))
	go run.pumpHost(br, pin)
	run.pumpWorker(ctx, pout, stdout)
	_ = cmd.Wait()
	run.rec.mu.Lock()
	run.rec.exits[in.AttemptNo] = time.Now()
	run.rec.mu.Unlock()
	if ps := cmd.ProcessState; ps != nil {
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return provider.ExitStatus{Signal: ws.Signal()}
		}
		return provider.ExitStatus{Code: ps.ExitCode()}
	}
	return provider.ExitStatus{Code: 1}
}

// newRun 记录 attempt 的 init（改写 out_dir 之前），解析第 1 个 attempt 的注入声明；out_dir 为宿主
// workspace 下的 out/<attempt_id>。
func (h *harness) newRun(in protocol.Init) *workerRun {
	run := &workerRun{h: h, rec: h.rec(in.TaskID), in: in, ws: filepath.Join(h.dir, "workspaces", in.TaskID),
		typeCount: map[string]int{}, paths: map[string]string{}}
	run.outDir = filepath.Join(run.ws, "out", in.AttemptID)
	if in.AttemptNo == 1 {
		var cfg struct {
			E2E directives `json:"e2e"`
		}
		_ = json.Unmarshal(in.Config, &cfg)
		run.d = cfg.E2E
	}
	run.rec.mu.Lock()
	run.rec.inits[in.AttemptNo] = in
	run.rec.mu.Unlock()
	return run
}

// kill 以 SIGKILL 杀死 Worker：进程型 Program 杀死其进程组；真实隔离时杀死环境 cgroup 中的 sim_worker 进程。
func (w *workerRun) kill() {
	if w.killFn != nil {
		w.killFn()
		return
	}
	_ = syscall.Kill(-w.pid, syscall.SIGKILL)
}

// pumpHost 把宿主的消息逐行转发给 Worker；stdin 结束时关闭 Worker 的 stdin。
func (w *workerRun) pumpHost(br *bufio.Reader, pin io.WriteCloser) {
	defer pin.Close()
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 && w.onHost(line) {
			_, _ = pin.Write(line)
		}
		if err != nil {
			return
		}
	}
}

// onHost 记录宿主回复并执行注入；返回是否送达 Worker。
func (w *workerRun) onHost(line []byte) bool {
	var m hostLine
	_ = json.Unmarshal(line, &m)
	m.AttemptNo = w.in.AttemptNo
	forward := true
	switch m.Type {
	case protocol.TypeCheckpointResult:
		w.ckResults++
		if w.d.DropCheckpointResult == w.ckResults {
			forward = false
		}
		if m.Status == protocol.CheckpointCommitted {
			w.committed++
			if w.d.KillAfterCommitted == w.committed {
				w.kill() // checkpoint 已提交，Worker 尚未得知
				forward = false
			}
		}
	case protocol.TypeArtifactResult:
		if t := w.d.Tamper; t != nil && t.Mode == tamperAfterSave && t.ArtifactID == m.ArtifactID && m.Status == protocol.ArtifactSaved {
			// 登记之后改写 out_dir 中的文件：已保存的副本必须保持权威。
			w.rec.mu.Lock()
			path := w.paths[m.ArtifactID]
			w.rec.mu.Unlock()
			w.tampered(os.WriteFile(filepath.Join(w.outDir, path), []byte("tampered after save"), 0o644))
		}
	}
	m.Dropped = !forward
	w.rec.mu.Lock()
	w.rec.host = append(w.rec.host, m)
	w.rec.mu.Unlock()
	return forward
}

func (w *workerRun) tampered(err error) {
	if err != nil {
		w.rec.mu.Lock()
		w.rec.tamperErr = errors.Join(w.rec.tamperErr, err)
		w.rec.mu.Unlock()
	}
}

// pumpWorker 把 Worker 的消息逐行交给 runner，直到 Worker 的 stdout 结束；runner 关闭读端后只读出丢弃。
func (w *workerRun) pumpWorker(ctx context.Context, pout io.Reader, stdout io.Writer) {
	br := bufio.NewReaderSize(pout, 1<<20)
	broken := false
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			line = w.onWorker(ctx, line)
			if !broken {
				if _, werr := stdout.Write(line); werr != nil {
					broken = true
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// onWorker 记录 Worker 消息并执行注入，返回交给 runner 的行。
func (w *workerRun) onWorker(ctx context.Context, line []byte) []byte {
	var m struct {
		Type       string `json:"type"`
		ArtifactID string `json:"artifact_id"`
		Path       string `json:"path"`
	}
	_ = json.Unmarshal(line, &m)
	w.typeCount[m.Type]++
	w.rec.mu.Lock()
	w.rec.worker[w.in.AttemptNo] = append(w.rec.worker[w.in.AttemptNo], m.Type)
	w.rec.mu.Unlock()
	if hd := w.d.Hold; hd != nil && hd.Type == m.Type && hd.Nth == w.typeCount[m.Type] {
		w.rec.heldOnce.Do(func() { close(w.rec.held) })
		// 只等显式释放（或测试清理时的 releaseHold）。不再以 ctx.Done() 放行：拦截上下文在 Worker 进程退出
		// （Wait）时取消，而 sim_worker 发出 result 后立即退出，会把"暂停 result"变成"暂停到进程退出"的竞争，
		// 在慢机器上让 result 抢在取消请求之前到达 runner（CI 上 E4 屏障 1 曾因此得到 409 task_ended）。
		<-w.rec.release
	}
	t := w.d.Tamper
	if m.Type != protocol.TypeArtifact || t == nil || t.ArtifactID != m.ArtifactID {
		return line
	}
	file := filepath.Join(w.outDir, m.Path)
	switch t.Mode {
	case tamperSymlink:
		// 同内容的文件放在 out_dir 之外，声明的哈希仍然匹配：只有"不跟随符号链接"能拒绝它。
		b, err := os.ReadFile(file)
		outside := filepath.Join(w.ws, "outside-"+m.ArtifactID)
		w.tampered(errors.Join(err, os.WriteFile(outside, b, 0o644), os.Remove(file), os.Symlink(outside, file)))
	case tamperDirSymlink:
		// <out_dir>/<dir>/<name> 中的目录换成指向 ../../in（workspace/in）的符号链接，in 中放同内容文件。
		b, err := os.ReadFile(file)
		dir, name := filepath.Split(m.Path)
		in := filepath.Join(w.ws, "in")
		w.tampered(errors.Join(err, os.MkdirAll(in, 0o755), os.WriteFile(filepath.Join(in, name), b, 0o644),
			os.RemoveAll(filepath.Join(w.outDir, filepath.Clean(dir))),
			os.Symlink(filepath.Join("..", "..", "in"), filepath.Join(w.outDir, filepath.Clean(dir)))))
	case tamperFIFO:
		w.tampered(errors.Join(os.Remove(file), syscall.Mkfifo(file, 0o644)))
	case tamperDotDot:
		var obj map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&obj); err != nil {
			w.tampered(err)
			return line
		}
		obj["path"] = "../in/" + filepath.Base(m.Path)
		b, err := json.Marshal(obj)
		w.tampered(err)
		return append(b, '\n')
	case tamperAfterSave:
		w.rec.mu.Lock()
		w.paths[m.ArtifactID] = m.Path
		w.rec.mu.Unlock()
	}
	return line
}

// ---- 测试 ----

// TestFirstSlice：§8 首个切片（控制面层）——经 HTTP 提交 → 握手 → progress → checkpoint → 杀死 Worker
// 进程 → 新 attempt 以该 checkpoint 恢复 → result → 裁决 succeeded → 环境停止并清理 → 产物按固定版本可读。
func TestFirstSlice(t *testing.T) { testFirstSlice(t, newHarness(t)) }

func testFirstSlice(t *testing.T, h *harness) {
	h.start(baseConfig())
	id := h.submit("first-slice", spec(&directives{KillAfterCommitted: 1}, "report ready", []string{"notes", "report"},
		progressStep("collecting"),
		artifactStep("notes", "notes.txt", "draft notes"),
		checkpointStep("collect"),
		progressStep("writing"),
		artifactStep("report", "report/final.md", "# final report"),
	))
	h.waitTerminal(id)
	status, reason, attempts := h.httpTask(id)
	if status != "succeeded" || attempts != 2 {
		t.Fatalf("经 API 读取任务为 %s/%s，attempts_total=%d；期望 succeeded 与 2 个 attempt", status, reason, attempts)
	}
	in := h.inspect(id)
	a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2)
	if a1.OutcomeClass != runner.ClassCrashedSignal || a1.ExitSignal == nil || *a1.ExitSignal != int64(syscall.SIGKILL) || a1.PlatformKilled {
		t.Fatalf("被杀死的 attempt 1：%+v，期望 crashed_signal（SIGKILL、非平台终止）", a1)
	}
	if a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempt 2：%+v", a2)
	}
	// 握手与 progress：attempt 1 的 Worker 事件按序持久化。
	evs := h.events(id)
	var a1Types []string
	for _, e := range evs {
		if e.AttemptID == a1.AttemptID && e.Source == "worker" {
			a1Types = append(a1Types, e.Type)
		}
	}
	if len(a1Types) < 4 || a1Types[0] != "ready" || a1Types[1] != "progress" || !slices.Contains(a1Types, "checkpoint") {
		t.Fatalf("attempt 1 的 Worker 事件 %q", a1Types)
	}
	if countType(evs, "task_terminal") != 1 {
		t.Fatalf("task_terminal 事件 %d 个", countType(evs, "task_terminal"))
	}
	// 恢复：attempt 2 的 init.resume 指向 attempt 1 提交的 checkpoint。
	cp := checkpointByStep(in, "collect")
	init2, ok := h.rec(id).init(2)
	if cp.CheckpointID == "" || cp.AttemptID != a1.AttemptID || !ok || init2.Resume == nil || init2.Resume.CheckpointID != cp.CheckpointID {
		t.Fatalf("checkpoint %+v；attempt 2 的 resume %+v", cp, init2.Resume)
	}
	if init1, _ := h.rec(id).init(1); init1.Resume != nil {
		t.Fatalf("attempt 1 不应带 resume：%+v", init1.Resume)
	}
	if got := h.rec(id).workerTypes(2); slices.Contains(got, "checkpoint") || countType2(got, "artifact") != 1 {
		t.Fatalf("attempt 2 重做了 checkpoint 之前的步骤：%q", got)
	}
	// 产物按固定版本可读：经 Store 与 BlobStore 读取并校验哈希，再经 API 下载结果与固定版本。
	summary, outs, content := h.pinnedResult(id, a2.AttemptID)
	if summary != "report ready" || len(outs) != 2 || content["notes"] != "draft notes" || content["report"] != "# final report" {
		t.Fatalf("result %q %+v %q", summary, outs, content)
	}
	h.assertPinnedDownloads(id, outs, content)
	// 环境停止并清理。
	h.assertNoLeak()
	in = h.inspect(id)
	for _, a := range in.Attempts {
		if a.StoppedAt == nil || a.CleanupState != "done" {
			t.Fatalf("attempt %d 的环境 %+v 未停止或未清理", a.AttemptNo, a)
		}
	}
	h.finish()
}

func countType2(xs []string, x string) int {
	n := 0
	for _, v := range xs {
		if v == x {
			n++
		}
	}
	return n
}

// TestE1KillAfterSecondCheckpoint：E1——第 2 个 checkpoint 提交后杀死 Worker → 新 attempt 从
// checkpoint 2 恢复；fault_retries_used = 1；产物完整（两个 attempt 保存的产物都固定在 result 中）。
func TestE1KillAfterSecondCheckpoint(t *testing.T) { testE1(t, newHarness(t)) }

func testE1(t *testing.T, h *harness) {
	h.start(baseConfig())
	id := h.submit("e1", spec(&directives{KillAfterCommitted: 2}, "e1", []string{"a1", "a2", "a3"},
		artifactStep("a1", "a1.txt", "one"),
		checkpointStep("s1"),
		artifactStep("a2", "a2.txt", "two"),
		checkpointStep("s2"),
		artifactStep("a3", "a3.txt", "three"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	ts := h.loadTask(id)
	if ts.FaultRetriesUsed != 1 || ts.AttemptsTotal != 2 {
		t.Fatalf("fault_retries_used=%d attempts_total=%d，期望 1 与 2", ts.FaultRetriesUsed, ts.AttemptsTotal)
	}
	in := h.inspect(id)
	s2 := checkpointByStep(in, "s2")
	if len(in.Checkpoints) != 2 || s2.CommitSeq != 2 || ts.Latest == nil || ts.Latest.CheckpointID != s2.CheckpointID {
		t.Fatalf("checkpoints %+v，最新 %+v", in.Checkpoints, ts.Latest)
	}
	init2, _ := h.rec(id).init(2)
	if init2.Resume == nil || init2.Resume.CheckpointID != s2.CheckpointID || init2.Resume.StepID != "s2" {
		t.Fatalf("attempt 2 的 resume %+v，期望 checkpoint 2（%s）", init2.Resume, s2.CheckpointID)
	}
	// 被杀死时 checkpoint 2 的确认没有送达 Worker。
	var killed bool
	for _, l := range h.rec(id).hostLines() {
		killed = killed || (l.AttemptNo == 1 && l.CheckpointID == s2.CheckpointID && l.Dropped)
	}
	if !killed {
		t.Fatalf("checkpoint 2 的回复 %+v", h.rec(id).hostLines())
	}
	a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2)
	if a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v / %+v", a1, a2)
	}
	_, outs, content := h.pinnedResult(id, a2.AttemptID)
	want := map[string]string{"a1": "one", "a2": "two", "a3": "three"}
	if len(outs) != 3 || len(content) != 3 {
		t.Fatalf("固定输出 %+v", outs)
	}
	for _, o := range outs {
		if o.Version != 1 || content[o.ArtifactID] != want[o.ArtifactID] {
			t.Fatalf("产物 %+v 内容 %q", o, content[o.ArtifactID])
		}
	}
	h.finish()
}

// TestE4CancelResultRace：E4——取消与 result 竞争：确定性屏障各一例（取消先被接受 / result 先裁决），
// 加随机交错（seed 记录在日志中，失败时输出；AGENTBOX_E2E_SEED 可复现注入的延迟）：恰一个裁决；
// 取消先被接受则永不 succeeded；无泄漏（环境全部清理、槽位全部归还）。
func TestE4CancelResultRace(t *testing.T) { testE4(t, newHarness(t), 1000) }

// testE4 的随机交错默认 runs 次（-short 时 50 次；AGENTBOX_E2E_E4_RUNS 覆盖）。
func testE4(t *testing.T, h *harness, runs int) {
	const lanes = 8
	if testing.Short() {
		runs = min(runs, 50)
	}
	if s := os.Getenv("AGENTBOX_E2E_E4_RUNS"); s != "" {
		runs, _ = strconv.Atoi(s)
	}
	cfg := baseConfig()
	cfg.Capacity = admission.Capacity{RunSlots: lanes, MemoryBytes: 8 << 30}
	// 使用默认的 UID 范围池（64 段）：池耗尽时 CreateEnv 等待清理归还，不使任务失败；停止记录后立即唤醒清理。
	h.start(cfg)

	// 屏障 1：result 已由 Worker 发出但尚未交给 runner 时接受取消 → 裁决必为 cancelled。
	a := h.submit("e4-cancel-first", spec(&directives{Hold: &holdSpec{Type: "result", Nth: 1}}, "x", nil))
	h.waitHeld(a)
	if st, code := h.cancelTask(a, "e4-cancel-first-c"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	h.rec(a).releaseHold()
	if v := h.waitTerminal(a); v.Status != "cancelled" {
		t.Fatalf("取消先被接受后任务结束为 %+v", v)
	}
	if n := countType(h.events(a), "task_terminal"); n != 1 {
		t.Fatalf("task_terminal 事件 %d 个", n)
	}
	t.Logf("屏障 1：%s", h.inspect(a).Task.StatusReason)

	// 屏障 2：裁决 succeeded 已提交后取消 → task_ended，终态不变。
	b := h.submit("e4-result-first", spec(nil, "x", nil))
	if v := h.waitTerminal(b); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	if st, code := h.cancelTask(b, "e4-result-first-c"); st != http.StatusConflict || code != persistence.CodeTaskEnded {
		t.Fatalf("裁决后取消 = %d %s，期望 409 task_ended", st, code)
	}
	if v, _ := h.store.GetTask(context.Background(), b); v.Status != "succeeded" || countType(h.events(b), "task_terminal") != 1 {
		t.Fatalf("裁决后取消改变了终态：%+v", v)
	}

	// 随机交错：每条通道串行地提交任务（Worker 睡眠随机时长后发出 result），在随机延迟后取消。
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("AGENTBOX_E2E_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	t.Logf("E4 随机交错：%d 次，seed=%d（AGENTBOX_E2E_SEED=%d 复现注入的延迟）", runs, seed, seed)
	start := time.Now()
	var (
		mu       sync.Mutex
		outcomes = map[string]int{}
		errs     []error
		next     int
		wg       sync.WaitGroup
	)
	for lane := 0; lane < lanes; lane++ {
		wg.Add(1)
		go func(lane int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewPCG(seed, uint64(lane)))
			for {
				mu.Lock()
				i := next
				next++
				stop := i >= runs || len(errs) > 0
				mu.Unlock()
				if stop {
					return
				}
				out, err := h.e4Run(i, rng.IntN(60), time.Duration(rng.IntN(400))*time.Millisecond)
				mu.Lock()
				if err != nil {
					errs = append(errs, fmt.Errorf("第 %d 次（seed=%d）：%w", i, seed, err))
				}
				outcomes[out]++
				mu.Unlock()
			}
		}(lane)
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("E4 失败，seed=%d：%v", seed, errors.Join(errs...))
	}
	h.mu.Lock()
	retries := maps.Clone(h.controlRetries)
	h.mu.Unlock()
	t.Logf("E4 随机交错 %d 次用时 %s，结果分布：%v；取消得到 503 后同 request_id 重试：%v",
		runs, time.Since(start).Round(time.Millisecond), outcomes, retries)
	if outcomes["succeeded/"] == 0 || outcomes["cancelled/attempted"]+outcomes["cancelled/completed_during_cancel"] == 0 {
		t.Logf("注意：随机交错没有同时覆盖 succeeded 与执行中取消（seed=%d）", seed)
	}
	h.assertNoLeak()
	h.assertSlotsFree(lanes)
	h.finish()
}

// e4Run 执行一次随机交错并检查：取消被接受（200）→ cancelled；取消被拒（409 task_ended）→ 此前已
// succeeded；至多一个 attempt，有 attempt 时恰一个 task_terminal 裁决事件，没有 attempt（排队中取消）时没有。
func (h *harness) e4Run(i, workMs int, delay time.Duration) (string, error) {
	id, err := h.submitE(fmt.Sprintf("e4-%d", i), spec(nil, "x", nil, sleepStep(workMs)))
	if err != nil {
		return "", err
	}
	time.Sleep(delay)
	st, code, err := h.cancelTaskE(id, fmt.Sprintf("e4-%d-c", i))
	if err != nil {
		return "", err
	}
	accepted := st == http.StatusOK
	if !accepted && (st != http.StatusConflict || code != persistence.CodeTaskEnded) {
		return "", fmt.Errorf("任务 %s：取消 = %d %s", id, st, code)
	}
	v, err := h.waitTerminalE(id)
	if err != nil {
		return "", err
	}
	in, err := h.store.Inspect(context.Background(), id)
	if err != nil {
		return "", err
	}
	evs, err := h.eventsE(id)
	if err != nil {
		return "", err
	}
	terminals := countType(evs, "task_terminal")
	switch {
	case accepted && v.Status != "cancelled":
		return "", fmt.Errorf("任务 %s：取消已被接受，终态却是 %s/%s", id, v.Status, v.StatusReason)
	case !accepted && v.Status != "succeeded":
		return "", fmt.Errorf("任务 %s：取消被拒（task_ended），终态却是 %s/%s", id, v.Status, v.StatusReason)
	case len(in.Attempts) > 1:
		return "", fmt.Errorf("任务 %s：%d 个 attempt（取消或成功都不应重试）", id, len(in.Attempts))
	case len(in.Attempts) == 1 && (terminals != 1 || in.Attempts[0].Status != "ended"):
		return "", fmt.Errorf("任务 %s：attempt %+v，task_terminal 事件 %d 个", id, in.Attempts[0], terminals)
	case len(in.Attempts) == 0 && terminals != 0:
		return "", fmt.Errorf("任务 %s：没有 attempt 却有 %d 个 task_terminal 事件", id, terminals)
	}
	kind := v.StatusReason
	switch {
	case v.Status == "succeeded":
		kind = ""
	case len(in.Attempts) == 0:
		kind = "queued"
	case kind != "completed_during_cancel":
		kind = "attempted"
	}
	return v.Status + "/" + kind, nil
}

// TestE7DropCheckpointResult：E7——丢弃第一个 checkpoint_result → SDK 超时后以同一 ID 查询，得到相同的
// committed 结果；没有重复提交，指针不回退（查询之后、下一个 checkpoint 之前仍指向 checkpoint 1）。
func TestE7DropCheckpointResult(t *testing.T) { testE7(t, newHarness(t)) }

func testE7(t *testing.T, h *harness) {
	h.start(baseConfig())
	id := h.submit("e7", spec(&directives{DropCheckpointResult: 1, Hold: &holdSpec{Type: "checkpoint", Nth: 2}}, "e7", nil,
		checkpointStep("c1"),
		checkpointStep("c2"),
	))
	h.waitHeld(id) // Worker 已得到 c1 的结果（经查询）并发出 c2
	in := h.inspect(id)
	c1 := checkpointByStep(in, "c1")
	ts := h.loadTask(id)
	if len(in.Checkpoints) != 1 || c1.CommitSeq != 1 || ts.Latest == nil || ts.Latest.CheckpointID != c1.CheckpointID {
		t.Fatalf("查询之后：checkpoints %+v，最新 %+v", in.Checkpoints, ts.Latest)
	}
	var results []hostLine
	for _, l := range h.rec(id).hostLines() {
		if l.Type == protocol.TypeCheckpointResult && l.CheckpointID == c1.CheckpointID {
			results = append(results, l)
		}
	}
	if len(results) != 2 || !results[0].Dropped || results[1].Dropped ||
		results[0].Status != protocol.CheckpointCommitted || results[1].Status != protocol.CheckpointCommitted {
		t.Fatalf("c1 的回复 %+v：期望被丢弃的 committed 与查询得到的 committed", results)
	}
	if got := h.rec(id).workerTypes(1); countType2(got, protocol.TypeCheckpointQuery) != 1 || countType2(got, protocol.TypeCheckpoint) != 2 {
		t.Fatalf("Worker 消息 %q：期望 1 次 checkpoint_query、c1 不重发", got)
	}
	h.rec(id).releaseHold()
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in = h.inspect(id)
	ts = h.loadTask(id)
	c2 := checkpointByStep(in, "c2")
	if len(in.Checkpoints) != 2 || checkpointByStep(in, "c1").CommitSeq != 1 || c2.CommitSeq != 2 ||
		ts.Latest == nil || ts.Latest.CheckpointID != c2.CheckpointID || ts.AttemptsTotal != 1 {
		t.Fatalf("结束时 checkpoints %+v，最新 %+v，attempts %d", in.Checkpoints, ts.Latest, ts.AttemptsTotal)
	}
	h.finish()
}

// TestE8StoreBlocked：E8——Store 阻塞（持有 tasks 行锁，并使执行路径上的 Store 调用成为超过期限的慢
// 查询），server 不重启。阻塞期间：runner 达到 Store 故障阈值后在规定时间（§19 的 30 s）内停止执行树，
// 物理停止（不依赖数据库写入）完成，未确认记录停止的环境保守占用容量（排队任务得不到槽位）；解除后由
// 现有 actor 与 coordinator 补提交停止与裁决，任务按故障重试从已提交 checkpoint 恢复并成功，不变量成立。
func TestE8StoreBlocked(t *testing.T) { testE8(t, newHarness(t)) }

func testE8(t *testing.T, h *harness) {
	cfg := baseConfig()
	cfg.Capacity = admission.Capacity{RunSlots: 1, MemoryBytes: 8 << 30}
	cfg.RetryBackoff = func(int) time.Duration { return 200 * time.Millisecond } // coordinator 的持久化重试
	h.start(cfg)
	ctx := context.Background()

	t1 := h.submit("e8-1", spec(&directives{Hold: &holdSpec{Type: "checkpoint", Nth: 2}}, "e8", []string{"out"},
		checkpointStep("s1"),
		checkpointStep("s2"),
		artifactStep("out", "out.txt", "after block"),
	))
	h.waitHeld(t1) // s1 已提交，s2 尚未交给 runner
	t2 := h.submit("e8-2", spec(nil, "second", nil))
	eventually(t, "第二个任务的 actor 已读取任务并等待槽位", func() bool { return h.gate.called("LoadTask:" + t2) })
	att1 := h.loadTask(t1).CurrentAttemptID
	env1 := attemptByNo(h.inspect(t1), 1).EnvID

	// 阻塞：另一连接持有 t1 的 tasks 行锁，执行路径上的 Store 调用全部成为慢查询。
	lockConn, err := pgx.Connect(ctx, h.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockConn.Close(ctx) }()
	tx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1 FROM tasks WHERE task_id = $1 FOR UPDATE", t1); err != nil {
		t.Fatal(err)
	}
	h.gate.block()
	blockedAt := time.Now()
	h.rec(t1).releaseHold()

	// 执行树在规定时间内停止：Worker 进程退出，环境闸门关闭且没有在途执行。
	eventually(t, "阻塞期间 Worker 进程退出", func() bool { _, ok := h.rec(t1).exitAt(1); return ok })
	exitAt, _ := h.rec(t1).exitAt(1)
	if d := exitAt.Sub(blockedAt); d > 30*time.Second {
		t.Fatalf("Store 阻塞 %s 后执行树才停止，超过故障阈值 30 s", d)
	}
	eventually(t, "环境被物理停止（不依赖数据库写入）", func() bool {
		envs, _ := h.prov.List(ctx)
		for _, e := range envs {
			if e.EnvID == env1 {
				return !e.Running && !e.Complete
			}
		}
		return false
	})
	// 停止已确认但无法记录：coordinator 保留事实并重试，容量保守保留。
	eventually(t, "coordinator 重试记录停止", func() bool { return h.gate.failures("MarkStopped:"+env1) >= 2 })
	if h.gate.called("CreateAttempt:" + t2) {
		t.Fatal("阻塞期间第二个任务得到了槽位：未确认记录停止的环境的容量被提前归还")
	}
	var stoppedNull bool
	var attStatus string
	h.queryRow("SELECT stopped_at IS NULL FROM environments WHERE env_id = $1", []any{env1}, &stoppedNull)
	h.queryRow("SELECT status FROM attempts WHERE attempt_id = $1", []any{att1}, &attStatus)
	if !stoppedNull || attStatus == "ended" {
		t.Fatalf("阻塞期间数据库被写入：stopped_at 为空=%v，attempt 状态 %s", stoppedNull, attStatus)
	}
	t.Logf("Store 阻塞 %s 后执行树停止", exitAt.Sub(blockedAt).Round(time.Millisecond))

	// 解除：现有 actor 与 coordinator 补提交（服务未重启，启动恢复不重跑）。
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	h.gate.unblock()
	if v := h.waitTerminal(t1); v.Status != "succeeded" {
		t.Fatalf("任务 1 结束为 %+v", v)
	}
	if v := h.waitTerminal(t2); v.Status != "succeeded" {
		t.Fatalf("任务 2 结束为 %+v", v)
	}
	in := h.inspect(t1)
	a1 := attemptByNo(in, 1)
	ts := h.loadTask(t1)
	if a1.OutcomeClass != runner.ClassStoreUnavailable || a1.StoppedAt == nil || ts.FaultRetriesUsed != 1 || ts.AttemptsTotal != 2 {
		t.Fatalf("attempt 1 %+v；fault_retries_used=%d attempts_total=%d", a1, ts.FaultRetriesUsed, ts.AttemptsTotal)
	}
	if init2, _ := h.rec(t1).init(2); init2.Resume == nil || init2.Resume.StepID != "s1" {
		t.Fatalf("attempt 2 的 resume %+v，期望阻塞前提交的 s1", init2.Resume)
	}
	if n := countType(h.events(t1), "attempt_ended"); n != 1 {
		t.Fatalf("attempt_ended 事件 %d 个", n)
	}
	var ordered bool
	h.queryRow(`SELECT e.stopped_at <= a.created_at FROM environments e, attempts a WHERE e.env_id = $1 AND a.task_id = $2`,
		[]any{env1, t2}, &ordered)
	if !ordered {
		t.Fatal("第二个任务的 attempt 早于 env1 记录停止：容量在 Recorded 之前被归还")
	}
	_, _, content := h.pinnedResult(t1, attemptByNo(in, 2).AttemptID)
	if content["out"] != "after block" {
		t.Fatalf("产物 %q", content)
	}
	h.assertSlotsFree(1)
	h.finish()
}

// TestE10ArtifactTampering：E10——产物经符号链接（文件或目录分量）、`../in`、FIFO 提交 → 拒绝且不登记；
// 登记后改写 out_dir 中的文件 → 已保存副本保持权威（固定的 sha256 与内容都是登记时的）。
func TestE10ArtifactTampering(t *testing.T) { testE10(t, newHarness(t)) }

func testE10(t *testing.T, h *harness) {
	cfg := baseConfig()
	cfg.Runner.ExitGrace = 2 * time.Second // 协议违规后 Worker 等不到回复，由 exit_grace 终止
	h.start(cfg)
	type tc struct {
		mode, path    string
		status, class string // 期望的任务终态与 attempt 分类
		code          string // 期望的 artifact_result 拒绝码；"" 表示没有回复（协议违规）或已保存
	}
	cases := []tc{
		{tamperSymlink, "data.txt", "failed", runner.ClassWorkerError, "path_invalid"},
		{tamperDirSymlink, "sub/data.txt", "failed", runner.ClassWorkerError, "path_invalid"},
		{tamperDotDot, "sub/data.txt", "failed", runner.ClassProtocolViolation, ""},
		{tamperFIFO, "data.txt", "failed", runner.ClassWorkerError, "not_regular_file"},
		{tamperAfterSave, "data.txt", "succeeded", runner.ClassSucceeded, ""},
	}
	const original = "original content"
	ids := map[string]string{}
	for _, c := range cases {
		ids[c.mode] = h.submit("e10-"+c.mode, spec(&directives{Tamper: &tamperSpec{ArtifactID: "a", Mode: c.mode}}, "e10", []string{"a"},
			artifactStep("a", c.path, original)))
	}
	ctx := context.Background()
	for _, c := range cases {
		id := ids[c.mode]
		v := h.waitTerminal(id)
		rec := h.rec(id)
		rec.mu.Lock()
		terr := rec.tamperErr
		rec.mu.Unlock()
		if terr != nil {
			t.Fatalf("%s：注入失败：%v", c.mode, terr)
		}
		in := h.inspect(id)
		if v.Status != c.status || len(in.Attempts) != 1 || in.Attempts[0].OutcomeClass != c.class {
			t.Fatalf("%s：任务 %s/%s，attempts %+v；期望 %s 与 %s", c.mode, v.Status, v.StatusReason, in.Attempts, c.status, c.class)
		}
		var replies []hostLine
		for _, l := range rec.hostLines() {
			if l.Type == protocol.TypeArtifactResult {
				replies = append(replies, l)
			}
		}
		latest, err := h.store.LatestArtifact(ctx, id, "a")
		if c.mode == tamperAfterSave {
			_, outs, content := h.pinnedResult(id, in.Attempts[0].AttemptID)
			if len(replies) != 1 || replies[0].Status != protocol.ArtifactSaved || err != nil || latest.Version != 1 ||
				len(outs) != 1 || outs[0].SHA256 != sha256Hex([]byte(original)) || content["a"] != original {
				t.Fatalf("%s：回复 %+v，最新版本 %+v %v，固定输出 %+v，内容 %q", c.mode, replies, latest, err, outs, content)
			}
			b, _ := os.ReadFile(filepath.Join(h.dir, "workspaces", id, "out", in.Attempts[0].AttemptID, c.path))
			if string(b) == original {
				t.Fatalf("%s：out_dir 中的文件没有被改写，用例无效", c.mode)
			}
			continue
		}
		if !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("%s：被拒绝的产物仍被登记：%+v %v", c.mode, latest, err)
		}
		switch {
		case c.code == "" && len(replies) != 0:
			t.Fatalf("%s：协议违规后仍有回复 %+v", c.mode, replies)
		case c.code != "" && (len(replies) != 1 || replies[0].Status != protocol.ArtifactRejected || replies[0].Code != c.code):
			t.Fatalf("%s：回复 %+v，期望 rejected/%s", c.mode, replies, c.code)
		}
	}
	h.finish()
}
