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
// 文件末尾的 TestReal* 用例以 root 在真实隔离环境中重跑这些验收（provider/local + 生产启动器，见该节说明）。
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/app"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/cli"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/faultinject"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/invariants"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/fake"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/local"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
	"github.com/wanghr0318-dotcom/go-agentbox/tests/e2e/fakeupstream"
	"github.com/wanghr0318-dotcom/go-agentbox/tests/e2e/procprov"
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

// ==== 系统级验收：真实 server 子进程（E5、E6、E13–E16）====
//
// 以下用例把只供测试的 tests/e2e/agentbox-e2e（与 cmd/agentbox server 相同的 app.Run，provider 换成进程型
// 的 tests/e2e/procprov，并开启 internal/faultinject）构建一次，作为真实子进程运行：SIGKILL、重启、失锁与
// 数据库断连都作用于真实进程。每个用例结束时：环境全部停止并清理、UID 范围归还、没有存活的执行进程、没有
// 隔离记录，正常停止 server 后运行 `agentbox-e2e verify-invariants --quiescent`。

var (
	serverBinOnce sync.Once
	serverBinDir  string
	serverBinPath string
	serverBinErr  error
)

func TestMain(m *testing.M) {
	// 真实隔离用例（provider/local + 生产启动器）以 /proc/self/exe 再执行本测试二进制作为专用启动进程、
	// 沙箱 init 与 stage-2 helper：与 cmd/agentbox 的 main 相同，分流必须早于其他一切。
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case sandbox.HelperArg:
			sandbox.RunHelper() // 成功时 execve workload，不返回
			os.Exit(126)
		case sandbox.InitArg:
			if err := sandbox.RunInit(); err != nil {
				fmt.Fprintln(os.Stderr, "sandbox init:", err)
				os.Exit(1)
			}
			os.Exit(0)
		case sandbox.LaunchArg:
			if err := sandbox.RunLaunch(); err != nil {
				fmt.Fprintln(os.Stderr, "sandbox-launch:", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	code := m.Run()
	if serverBinDir != "" {
		_ = os.RemoveAll(serverBinDir)
	}
	if agentboxBinDir != "" {
		_ = os.RemoveAll(agentboxBinDir)
	}
	if swDir != "" {
		_ = os.RemoveAll(swDir)
	}
	if realCgRoot != "" {
		killRemove(realCgRoot)
	}
	os.Exit(code)
}

// goBinary 返回用于构建测试二进制的 go 命令：优先 PATH 中的 go；找不到时（例如 sudo 重置了 PATH）用 go 命令
// 传给测试进程的 GOROOT 环境变量（runtime.GOROOT 自 Go 1.24 起已弃用）。
func goBinary() string {
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}
	if root := os.Getenv("GOROOT"); root != "" {
		return filepath.Join(root, "bin", "go")
	}
	return "go"
}

// serverBinary 构建 agentbox-e2e（每次 go test 一次）。
func serverBinary(t *testing.T) string {
	t.Helper()
	serverBinOnce.Do(func() {
		goBin := goBinary()
		if serverBinDir, serverBinErr = os.MkdirTemp("", "agentbox-e2e-bin-"); serverBinErr != nil {
			return
		}
		serverBinPath = filepath.Join(serverBinDir, "agentbox-e2e")
		if out, err := exec.Command(goBin, "build", "-o", serverBinPath, "./agentbox-e2e").CombinedOutput(); err != nil {
			serverBinErr = fmt.Errorf("构建 agentbox-e2e: %v\n%s", err, out)
		}
	})
	if serverBinErr != nil {
		t.Fatal(serverBinErr)
	}
	return serverBinPath
}

// serverProc 是一个 server 子进程。
type serverProc struct {
	cmd  *exec.Cmd
	logs *lockedBuffer
	done chan struct{}
	base string
}

func (p *serverProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// sysHarness 是一个数据目录与一个数据库上的 server 子进程序列（启动、被杀死、重启）。
type sysHarness struct {
	t         *testing.T
	bin       string
	dir       string
	dsn       string // 测试侧连接（application_name = testAppName）
	serverDSN string // server 的 advisory lock 连接
	storeDSN  string // server 业务连接（E14 经 TCP 代理）
	pyPath    string
	procs     []*serverProc
	srv       *serverProc
	store     *postgres.Store
	blobs     *blob.Local
}

// testAppName 是测试侧数据库连接的 application_name：据此区分 server 的连接（E5 等待被杀死的 server 的
// 连接全部结束）。
const testAppName = "agentbox-e2e-test"

func withAppName(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("application_name", testAppName)
	u.RawQuery = q.Encode()
	return u.String()
}

func newSys(t *testing.T) *sysHarness {
	t.Helper()
	s := &sysHarness{t: t, pyPath: workerPath(t), serverDSN: newDatabase(t), dir: t.TempDir()}
	s.dsn = withAppName(t, s.serverDSN)
	s.bin = serverBinary(t)
	s.storeDSN = s.serverDSN
	t.Cleanup(func() {
		for _, p := range s.procs {
			if !p.exited() {
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		}
		if pids, _ := procprov.LiveUnder(s.dir); len(pids) > 0 {
			for _, pid := range pids {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
			t.Errorf("测试结束时仍有 %d 个执行进程（已杀死）", len(pids))
		}
		if s.store != nil {
			s.store.Close()
		}
		if t.Failed() {
			for i, p := range s.procs {
				t.Logf("server #%d 日志（末尾）：\n%s", i+1, p.logs.tail(16<<10))
			}
		}
	})
	return s
}

// start 启动 server 子进程（fault 非空时设置 AGENTBOX_FAULT），等待它开始监听。
func (s *sysHarness) start(fault string, extra ...string) *serverProc {
	s.t.Helper()
	addrFile := filepath.Join(s.dir, fmt.Sprintf("addr-%d", len(s.procs)+1))
	args := append([]string{"server", "--data-dir", s.dir, "--database-url", s.storeDSN, "--lock-database-url", s.serverDSN,
		"--addr-file", addrFile, "--pythonpath", s.pyPath}, extra...)
	cmd := exec.Command(s.bin, args...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, faultinject.Env+"=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if fault != "" {
		cmd.Env = append(cmd.Env, faultinject.Env+"="+fault)
	}
	p := &serverProc{cmd: cmd, logs: &lockedBuffer{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.logs, p.logs
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.procs = append(s.procs, p)
	s.srv = p
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	eventually(s.t, "server 开始监听", func() bool {
		if b, err := os.ReadFile(addrFile); err == nil && len(b) > 0 {
			p.base = "http://" + string(b)
			return true
		}
		if p.exited() {
			s.t.Fatalf("server 提前退出（%v）", cmd.ProcessState)
		}
		return false
	})
	if s.store == nil {
		st, err := postgres.Open(context.Background(), postgres.Options{DSN: s.dsn})
		if err != nil {
			s.t.Fatal(err)
		}
		s.store = st
		if s.blobs, err = blob.NewLocal(filepath.Join(s.dir, "blobs")); err != nil {
			s.t.Fatal(err)
		}
	}
	return p
}

func (s *sysHarness) waitExit(p *serverProc) *os.ProcessState {
	s.t.Helper()
	select {
	case <-p.done:
	case <-time.After(waitLimit):
		s.t.Fatal("server 未在期限内退出")
	}
	return p.cmd.ProcessState
}

func killedBySIGKILL(ps *os.ProcessState) bool {
	ws, ok := ps.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}

// stop 以 SIGTERM 正常停止当前 server，要求退出码 0。
func (s *sysHarness) stop() {
	s.t.Helper()
	if err := s.srv.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		s.t.Fatal(err)
	}
	if ps := s.waitExit(s.srv); !ps.Success() {
		s.t.Fatalf("server 正常停止的退出状态为 %v", ps)
	}
}

func (s *sysHarness) submitE(base, requestID, spec string) (string, error) {
	st, b, err := httpDo("POST", base+"/tasks", `{"request_id":"`+requestID+`","spec":`+spec+`}`)
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

func (s *sysHarness) submit(requestID, spec string) string {
	s.t.Helper()
	id, err := s.submitE(s.srv.base, requestID, spec)
	if err != nil {
		s.t.Fatal(err)
	}
	return id
}

func (s *sysHarness) waitTerminal(id string) api.TaskView {
	s.t.Helper()
	var v api.TaskView
	eventually(s.t, "任务 "+id+" 到达终态", func() bool {
		var err error
		v, err = s.store.GetTask(context.Background(), id)
		return err == nil && task.IsTerminal(v.Status)
	})
	return v
}

func (s *sysHarness) inspect(id string) api.Inspection {
	s.t.Helper()
	in, err := s.store.Inspect(context.Background(), id)
	if err != nil {
		s.t.Fatalf("Inspect %s: %v", id, err)
	}
	return in
}

func (s *sysHarness) loadTask(id string) task.TaskState {
	s.t.Helper()
	ts, err := s.store.LoadTask(context.Background(), id)
	if err != nil {
		s.t.Fatalf("LoadTask %s: %v", id, err)
	}
	return ts
}

func (s *sysHarness) events(id string) []api.Event {
	s.t.Helper()
	evs, err := s.store.ListEvents(context.Background(), id, 0, 100000)
	if err != nil {
		s.t.Fatal(err)
	}
	return evs
}

func (s *sysHarness) envDir(envID string) string { return filepath.Join(s.dir, "envs", envID) }

// scanner 是测试侧的只读 procprov（独立扫描数据目录与 /proc）。
func (s *sysHarness) scanner() *procprov.Provider {
	s.t.Helper()
	id, ok, err := datadir.NewIDFile(s.dir).Read()
	if err != nil || !ok {
		s.t.Fatalf("读取 install_id: %v %v", ok, err)
	}
	p, err := procprov.New(procprov.Options{DataDir: s.dir, InstallID: id, ReadOnly: true})
	if err != nil {
		s.t.Fatal(err)
	}
	return p
}

func pgQueryRow(t *testing.T, dsn, sql string, args []any, dest ...any) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if err := c.QueryRow(ctx, sql, args...).Scan(dest...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// finish 是每个系统级用例结束时的检查：无泄漏、无隔离记录；正常停止 server 后 verify-invariants --quiescent
// 通过。
func (s *sysHarness) finish() {
	s.t.Helper()
	scan := s.scanner()
	eventuallyWithin(s.t, "环境全部停止并清理、UID 范围归还、没有执行进程与环境目录", waitLimit, 50*time.Millisecond, func() bool {
		var pending, held int
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM environments WHERE cleanup_state <> 'done' OR stopped_at IS NULL", nil, &pending)
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM uid_ranges WHERE state <> 'free'", nil, &held)
		envs, err := scan.List(context.Background())
		pids, perr := procprov.LiveUnder(s.dir)
		return pending == 0 && held == 0 && err == nil && len(envs) == 0 && perr == nil && len(pids) == 0
	})
	var quarantined int
	pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM quarantined_resources", nil, &quarantined)
	if quarantined != 0 {
		s.t.Fatalf("误隔离：%d 条隔离记录", quarantined)
	}
	s.stop()
	out, err := exec.Command(s.bin, "verify-invariants", "--quiescent", "--data-dir", s.dir, "--database-url", s.dsn).CombinedOutput()
	if err != nil {
		s.t.Fatalf("verify-invariants --quiescent：%v\n%s", err, out)
	}
	s.t.Logf("verify-invariants --quiescent：%s", strings.TrimSpace(string(out)))
}

// ---- 执行计数（sim_worker 的 exec_log）与执行日志（procprov.journal）----

type execRec struct {
	AttemptID string `json:"attempt_id"`
	AttemptNo int64  `json:"attempt_no"`
	Index     int    `json:"index"`
	Op        string `json:"op"`
}

func readExecLog(t *testing.T, path string) []execRec {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		t.Fatal(err)
	}
	var out []execRec
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var r execRec
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("exec_log 行 %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// sysSpec 是带执行计数的任务 spec。
func sysSpec(execLog, summary string, outputs []string, steps ...step) string {
	m := map[string]any{"steps": steps, "summary": summary, "outputs": outputs, "exec_log": execLog}
	if outputs == nil {
		m["outputs"] = []string{}
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// checkpointLoop 返回 n 轮"progress → sleep → checkpoint c<i>"，以及 step_id → 步骤下标。
func checkpointLoop(n, sleepMs int) ([]step, map[string]int) {
	var steps []step
	idx := map[string]int{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("c%d", i)
		steps = append(steps, progressStep(id), sleepStep(sleepMs), checkpointStep(id))
		idx[id] = len(steps) - 1
	}
	return steps, idx
}

// assertNoDuplicateExecution 由 sim_worker 的执行计数判断没有重复执行（§16.4 E5、E6）：
//   - 不同 attempt 的记录不交错（旧 attempt 在新 attempt 开始执行后不再执行任何步骤）；
//   - 每个 attempt 从"之前的 attempt 提交的最新 checkpoint 的下一步"开始，按序逐步执行，没有重做已提交
//     checkpoint 之前的步骤；
//   - 只有数据库中存在的 attempt 执行过；成功的最后一个 attempt 恰产生一次 result。
//
// 另以 procprov 的执行日志检查 I2 的物理侧：每个 attempt 的执行进程开始之前，上一个 attempt 的执行树已退出
// 或已确认停止。
func (s *sysHarness) assertNoDuplicateExecution(in api.Inspection, recs []execRec, stepIndex map[string]int, nSteps int) {
	s.t.Helper()
	byID := map[string]api.AttemptView{}
	for _, a := range in.Attempts {
		byID[a.AttemptID] = a
	}
	// 之前的 attempt 提交的最新 checkpoint → 起始步骤。
	startOf := func(no int64) int {
		var seq int64
		start := 0
		for _, c := range in.Checkpoints {
			if a, ok := byID[c.AttemptID]; ok && a.AttemptNo < no && c.CommitSeq > seq {
				seq, start = c.CommitSeq, stepIndex[c.StepID]+1
			}
		}
		return start
	}
	var lastNo int64
	next := map[int64]int{}
	results := map[int64]int{}
	for i, r := range recs {
		a, ok := byID[r.AttemptID]
		switch {
		case !ok || a.AttemptNo != r.AttemptNo:
			s.t.Fatalf("执行计数第 %d 行来自数据库中不存在的 attempt：%+v", i, r)
		case r.AttemptNo < lastNo:
			s.t.Fatalf("执行计数第 %d 行：attempt %d 在 attempt %d 开始执行之后仍在执行（%+v）", i, r.AttemptNo, lastNo, r)
		}
		lastNo = r.AttemptNo
		want, seen := next[r.AttemptNo]
		if !seen {
			want = startOf(r.AttemptNo)
		}
		if r.Index != want {
			s.t.Fatalf("attempt %d 执行了第 %d 步，期望第 %d 步（重复执行或跳过）：%+v", r.AttemptNo, r.Index, want, recs)
		}
		next[r.AttemptNo] = r.Index + 1
		if r.Op == "result" {
			results[r.AttemptNo]++
		}
	}
	last := in.Attempts[len(in.Attempts)-1]
	if last.OutcomeClass == runner.ClassSucceeded && (results[last.AttemptNo] != 1 || next[last.AttemptNo] != nSteps+1) {
		s.t.Fatalf("成功的 attempt %d 的执行计数不完整：%+v", last.AttemptNo, recs)
	}

	journal, err := procprov.ReadJournal(s.dir)
	if err != nil {
		s.t.Fatal(err)
	}
	started, dead := map[string]int64{}, map[string]int64{}
	for _, e := range journal {
		switch e.Event {
		case "start":
			if _, ok := started[e.EnvID]; !ok {
				started[e.EnvID] = e.TimeNs
			}
		case "exit", "stopped":
			if _, ok := dead[e.EnvID]; !ok && started[e.EnvID] != 0 {
				dead[e.EnvID] = e.TimeNs
			}
		}
	}
	atts := slices.Clone(in.Attempts)
	slices.SortFunc(atts, func(a, b api.AttemptView) int { return int(a.AttemptNo - b.AttemptNo) })
	for i := 1; i < len(atts); i++ {
		prev, cur := atts[i-1].EnvID, atts[i].EnvID
		if started[prev] == 0 || started[cur] == 0 {
			continue
		}
		if dead[prev] == 0 || started[cur] <= dead[prev] {
			s.t.Fatalf("attempt %d 的执行开始时 attempt %d 的执行树尚未确认结束（I2）：%+v", atts[i].AttemptNo, atts[i-1].AttemptNo, journal)
		}
	}
}

// readPinnedOutputs 读取 attempt 的 result 提议并按固定版本读回输出（哈希经 BlobStore 校验）。
func (s *sysHarness) readPinnedOutputs(taskID, attemptID string) map[string]string {
	s.t.Helper()
	ctx := context.Background()
	p, err := s.store.GetTerminalProposal(ctx, attemptID)
	if err != nil || p.Kind != "result" {
		s.t.Fatalf("attempt %s 的终态提议 %+v %v", attemptID, p, err)
	}
	read := func(sum string) []byte {
		rc, err := s.blobs.Open(sum)
		if err != nil {
			s.t.Fatalf("读取 blob %s: %v", sum, err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil || sha256Hex(b) != sum {
			s.t.Fatalf("blob %s 内容哈希不符（%v）", sum, err)
		}
		return b
	}
	var r struct {
		Outputs []pinnedRef `json:"outputs"`
	}
	if err := json.Unmarshal(read(p.Ref), &r); err != nil {
		s.t.Fatal(err)
	}
	out := map[string]string{}
	for _, o := range r.Outputs {
		v, err := s.store.GetArtifact(ctx, taskID, o.ArtifactID, o.SHA256)
		if err != nil || v.Version != o.Version {
			s.t.Fatalf("固定输出 %+v 在 Store 中为 %+v %v", o, v, err)
		}
		out[o.ArtifactID] = string(read(o.SHA256))
	}
	return out
}

// ---- E5 ----

// e5Phase 是崩溃时任务所处的阶段（由重启前的持久化事实判定），决定 §14.2 的恢复行为。
type e5Phase int

const (
	e5NoAttempt   e5Phase = iota // attempt 尚未创建：保持排队，重启后正常执行，不计故障重试
	e5OpenAttempt                // 有本次重启丢失的 attempt：确认停止后 lost_on_restart，按故障重试排队
	e5Terminal                   // 裁决已提交：保留终态，只处理残留资源
)

func (p e5Phase) String() string {
	return [...]string{"尚无 attempt", "attempt 未结束", "裁决已提交"}[p]
}

// e5Phases 是每个钩子点允许的阶段。停止环境与提交裁决由 actor 并发发出（task.Decide 的 finish 同时发出
// StopEnvironment 与 Finalize），因此停止前后崩溃时裁决可能已提交，也可能尚未提交；其余钩子点的阶段确定。
var e5Phases = map[string][]e5Phase{
	faultinject.AttemptCreateBefore:    {e5NoAttempt},
	faultinject.AttemptCreateAfter:     {e5OpenAttempt},
	faultinject.EnvCreateBefore:        {e5OpenAttempt},
	faultinject.EnvCreateAfter:         {e5OpenAttempt},
	faultinject.WorkerStarted:          {e5OpenAttempt},
	faultinject.CheckpointCommitBefore: {e5OpenAttempt},
	faultinject.CheckpointCommitAfter:  {e5OpenAttempt},
	faultinject.EnvStopBefore:          {e5OpenAttempt, e5Terminal},
	faultinject.EnvStopAfter:           {e5OpenAttempt, e5Terminal},
	faultinject.VerdictBefore:          {e5OpenAttempt},
	faultinject.VerdictAfter:           {e5Terminal},
	faultinject.CleanupDestroyed:       {e5Terminal}, // cleanup 只处理已有裁决的 attempt 的环境
}

// TestE5ServerKilledAtHookPoints：E5——在每个钩子点 SIGKILL server 并重启 → 按 §14.2 恢复；无重复执行
// （执行计数与执行日志）；孤儿回收（上一进程留下的存活执行树与环境目录全部停止并清理）；无误隔离。
// -short 只运行其中 4 个点。
func TestE5ServerKilledAtHookPoints(t *testing.T) {
	var points []string
	for _, p := range faultinject.Points {
		_, e5 := e5Phases[p]
		switch {
		case gatewayPoints[p] != "":
			continue // Gateway 的钩子点由各自的实验覆盖（任务不经 Gateway 时不会到达）
		case !e5:
			t.Fatalf("钩子点 %s 没有期望的恢复行为", p)
		}
		points = append(points, p)
	}
	if testing.Short() {
		points = []string{faultinject.WorkerStarted, faultinject.CheckpointCommitAfter, faultinject.VerdictBefore, faultinject.VerdictAfter}
	}
	start := time.Now()
	t.Cleanup(func() {
		t.Logf("E5：%d 个钩子点 × 重启，用时 %s", len(points), time.Since(start).Round(time.Millisecond))
	})
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			e5Case(t, point, e5Phases[point])
		})
	}
}

func e5Case(t *testing.T, point string, allowed []e5Phase) {
	s := newSys(t)
	execLog := filepath.Join(s.dir, "exec.log")
	steps := []step{
		progressStep("begin"),
		artifactStep("a1", "a1.txt", "one"),
		checkpointStep("s1"),
		sleepStep(50),
		artifactStep("a2", "a2.txt", "two"),
		checkpointStep("s2"),
		progressStep("end"),
	}
	stepIndex := map[string]int{"s1": 2, "s2": 5}
	first := s.start(point + ":1")
	id := s.submit("e5", sysSpec(execLog, "e5", []string{"a1", "a2"}, steps...))

	// server 在钩子点被 SIGKILL。
	ps := s.waitExit(first)
	if !killedBySIGKILL(ps) || !strings.Contains(first.logs.tail(1<<20), "faultinject: SIGKILL at "+point+":1") {
		t.Fatalf("server 应在 %s 被 SIGKILL，退出状态 %v", point, ps)
	}
	// 被杀死的进程已发出的 COMMIT 仍可能在数据库中完成：等它的连接全部结束，重启前的事实才是稳定的。
	eventuallyWithin(t, "上一 server 的数据库连接全部结束", waitLimit, 20*time.Millisecond, func() bool {
		var n int
		pgQueryRow(t, s.dsn, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND pid <> pg_backend_pid() AND application_name <> $1`, []any{testAppName}, &n)
		return n == 0
	})
	pre := s.inspect(id)
	preTask := s.loadTask(id)
	var phase e5Phase
	switch {
	case len(pre.Attempts) == 0 && !task.IsTerminal(pre.Task.Status):
		phase = e5NoAttempt
	case len(pre.Attempts) == 1 && pre.Attempts[0].Status != "ended" && !task.IsTerminal(pre.Task.Status):
		phase = e5OpenAttempt
	case len(pre.Attempts) == 1 && pre.Attempts[0].Status == "ended" && pre.Task.Status == "succeeded":
		phase = e5Terminal
	default:
		t.Fatalf("重启前的事实不属于任何阶段：%+v", pre)
	}
	if !slices.Contains(allowed, phase) {
		t.Fatalf("在 %s 崩溃后阶段为「%s」，期望 %v：%+v", point, phase, allowed, pre)
	}
	survivors, _ := procprov.LiveUnder(s.dir)
	preEnvs, _ := s.scanner().List(context.Background())

	s.start("")
	v := s.waitTerminal(id)
	in := s.inspect(id)
	ts := s.loadTask(id)
	if v.Status != "succeeded" || countType(s.events(id), "task_terminal") != 1 {
		t.Fatalf("恢复后任务 %+v，task_terminal 事件 %d 个", v, countType(s.events(id), "task_terminal"))
	}
	switch phase {
	case e5NoAttempt:
		if len(in.Attempts) != 1 || ts.FaultRetriesUsed != 0 {
			t.Fatalf("排队中崩溃：attempts %+v，fault_retries_used=%d；期望 1 个 attempt、不计重试", in.Attempts, ts.FaultRetriesUsed)
		}
	case e5OpenAttempt:
		a1 := attemptByNo(in, 1)
		if len(in.Attempts) != 2 || a1.OutcomeClass != "lost_on_restart" || ts.FaultRetriesUsed != preTask.FaultRetriesUsed+1 ||
			ts.AttemptsTotal != 2 {
			t.Fatalf("丢失的 attempt：attempts %+v，fault_retries_used=%d attempts_total=%d；期望 lost_on_restart 与 1 次故障重试",
				in.Attempts, ts.FaultRetriesUsed, ts.AttemptsTotal)
		}
	case e5Terminal:
		if len(in.Attempts) != 1 || ts.FaultRetriesUsed != preTask.FaultRetriesUsed || ts.AttemptsTotal != preTask.AttemptsTotal {
			t.Fatalf("裁决已提交：attempts %+v；终态之后不应有新 attempt 或计数变化", in.Attempts)
		}
	}
	last := in.Attempts[len(in.Attempts)-1]
	if last.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("最后一个 attempt %+v", last)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), stepIndex, len(steps))
	if got := s.readPinnedOutputs(id, last.AttemptID); got["a1"] != "one" || got["a2"] != "two" || len(got) != 2 {
		t.Fatalf("固定输出 %q", got)
	}
	t.Logf("%s：重启前 %d 个环境目录、%d 个存活执行进程；恢复后 %d 个 attempt（%s），fault_retries_used=%d",
		point, len(preEnvs), len(survivors), len(in.Attempts), attemptClasses(in), ts.FaultRetriesUsed)
	s.finish()
}

func attemptClasses(in api.Inspection) string {
	var cs []string
	for _, a := range in.Attempts {
		cs = append(cs, a.OutcomeClass)
	}
	return strings.Join(cs, ", ")
}

// ---- E13 ----

// TestE13AdvisoryLockConnectionKilled：E13——终止 advisory lock 的连接（pg_locks 中 locktype = advisory 的
// 后端），业务连接仍存活，且有一个业务事务正在等待行锁（在途）。server 检测到失去所有权后进入
// ownership_lost、停止本机全部执行树并以非零状态退出；记录检测延迟与在途事务的最终结果；重启后完整恢复。
func TestE13AdvisoryLockConnectionKilled(t *testing.T) {
	s := newSys(t)
	ctx := context.Background()
	execLog := filepath.Join(s.dir, "exec.log")
	steps, stepIndex := checkpointLoop(30, 20)
	srv := s.start("", "--check-interval", "100ms")
	id := s.submit("e13", sysSpec(execLog, "e13", nil, steps...))
	eventually(t, "第一个 checkpoint 已提交", func() bool { return len(s.inspect(id).Checkpoints) >= 1 })

	// 在途业务事务：另一连接持有该任务 task_progress 行的锁，下一次 checkpoint 提交等待它。
	holder, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close(ctx) }()
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1 FROM task_progress WHERE task_id = $1 FOR UPDATE", id); err != nil {
		t.Fatal(err)
	}
	before := len(s.inspect(id).Checkpoints)
	admin, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	var inflight string
	eventually(t, "server 的业务事务在等待行锁", func() bool {
		err := admin.QueryRow(ctx, `SELECT query FROM pg_stat_activity WHERE datname = current_database()
			AND wait_event_type = 'Lock' AND pid <> pg_backend_pid() LIMIT 1`).Scan(&inflight)
		return err == nil
	})
	var lockPID, business int32
	if err := admin.QueryRow(ctx, `SELECT a.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted AND a.datname = current_database()`).Scan(&lockPID); err != nil {
		t.Fatalf("找不到 advisory lock 的后端：%v", err)
	}
	var holderPID int32
	if err := holder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
		AND pid NOT IN (pg_backend_pid(), $1, $2)`, lockPID, holderPID).Scan(&business); err != nil {
		t.Fatal(err)
	}
	killedAt := time.Now()
	var terminated bool
	if err := admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", lockPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("pg_terminate_backend(%d) = %v %v", lockPID, terminated, err)
	}

	ps := s.waitExit(srv)
	exitAfter := time.Since(killedAt)
	logs := srv.logs.tail(1 << 20)
	if ps.Success() || killedBySIGKILL(ps) || !strings.Contains(logs, "ownership_lost") {
		t.Fatalf("失锁后 server 应以 ownership_lost 非零退出，退出状态 %v", ps)
	}
	detected, ok := logTime(logs, func(m map[string]any) bool { return m["msg"] == "API 模式" && m["mode"] == "ownership_lost" })
	if !ok {
		t.Fatal("日志中没有进入 ownership_lost 的记录")
	}
	// 本地执行树在退出前全部停止。
	if pids, _ := procprov.LiveUnder(s.dir); len(pids) != 0 {
		t.Fatalf("server 退出后仍有执行进程 %v", pids)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	after := len(s.inspect(id).Checkpoints)
	result := "未提交（随 ownership_lost 取消，连接随进程退出关闭）"
	if after != before {
		result = fmt.Sprintf("已提交（checkpoint %d → %d）", before, after)
	}
	t.Logf("E13：终止锁连接（pid %d）时另有 %d 个业务连接存活；检测延迟 %s，进程退出 %s；在途事务 %q 的最终结果：%s",
		lockPID, business, detected.Sub(killedAt).Round(time.Millisecond), exitAfter.Round(time.Millisecond),
		strings.Join(strings.Fields(inflight), " "), result)
	if business == 0 {
		t.Fatal("终止锁连接时没有存活的业务连接，用例无效")
	}

	// 重启：完整恢复。丢失的 attempt 以 lost_on_restart 结束，新 attempt 从最新 checkpoint 继续。
	s.start("")
	if v := s.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("重启后任务结束为 %+v", v)
	}
	in := s.inspect(id)
	ts := s.loadTask(id)
	if len(in.Attempts) != 2 || attemptByNo(in, 1).OutcomeClass != "lost_on_restart" || ts.FaultRetriesUsed != 1 {
		t.Fatalf("attempts %+v，fault_retries_used=%d", in.Attempts, ts.FaultRetriesUsed)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), stepIndex, len(steps))
	s.finish()
}

// logTime 返回第一条满足 match 的 JSON 日志的时间。
func logTime(logs string, match func(map[string]any) bool) (time.Time, bool) {
	for _, line := range strings.Split(logs, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || !match(m) {
			continue
		}
		s, _ := m["time"].(string)
		at, err := time.Parse(time.RFC3339Nano, s)
		return at, err == nil
	}
	return time.Time{}, false
}

// ---- E14 ----

// tcpProxy 是测试控制的 TCP 代理：Cut 断开全部连接并拒绝新连接（接受后立即关闭），Restore 恢复转发。
type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	cut    bool
	conns  map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() {
		_ = ln.Close()
		p.Cut()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *tcpProxy) serve(c net.Conn) {
	p.mu.Lock()
	if p.cut {
		p.mu.Unlock()
		_ = c.Close()
		return
	}
	p.mu.Unlock()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	if p.cut {
		p.mu.Unlock()
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns[c], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
	_ = c.Close()
	_ = up.Close()
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, up)
	p.mu.Unlock()
}

// Cut 断开全部现有连接，之后的新连接被立即关闭。
func (p *tcpProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.conns {
		_ = c.Close()
	}
}

// Restore 恢复转发。
func (p *tcpProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = false
}

// TestE14PostgresUnavailable：E14——任务中途 PostgreSQL 不可用（server 的业务连接经测试控制的 TCP 代理，
// 代理切断全部连接；advisory lock 的专用连接直连，所有权保持），server 不重启：runner 达到 Store 故障阈值后
// 以 store_unavailable 终止，执行树被物理停止（不依赖数据库写入）；恢复连接后由现有 actor 与 coordinator
// 补提交停止、裁决与状态，任务按故障重试从已提交的 checkpoint 继续并成功。
func TestE14PostgresUnavailable(t *testing.T) {
	s := newSys(t)
	u, err := url.Parse(s.serverDSN)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newTCPProxy(t, u.Host)
	u.Host = proxy.ln.Addr().String()
	s.storeDSN = u.String()
	execLog := filepath.Join(s.dir, "exec.log")
	// 每轮提交一个 checkpoint：切断时还有大量剩余步骤，恢复后的 attempt 只执行剩余部分。
	steps, stepIndex := checkpointLoop(150, 40)
	srv := s.start("")
	id := s.submit("e14", sysSpec(execLog, "e14", nil, steps...))
	eventually(t, "已提交 3 个 checkpoint", func() bool { return len(s.inspect(id).Checkpoints) >= 3 })
	env1 := attemptByNo(s.inspect(id), 1).EnvID
	if env1 == "" {
		t.Fatal("attempt 1 没有环境")
	}

	cutAt := time.Now()
	proxy.Cut()
	eventually(t, "Store 不可用期间执行树被物理停止", func() bool {
		pids, err := procprov.Marked(s.envDir(env1))
		return err == nil && len(pids) == 0
	})
	stoppedAfter := time.Since(cutAt)
	if srv.exited() {
		t.Fatalf("Store 不可用时 server 不应退出：%v", srv.cmd.ProcessState)
	}
	var stoppedNull bool
	var attStatus string
	pgQueryRow(t, s.dsn, "SELECT stopped_at IS NULL FROM environments WHERE env_id = $1", []any{env1}, &stoppedNull)
	pgQueryRow(t, s.dsn, "SELECT status FROM attempts WHERE env_id = $1", []any{env1}, &attStatus)
	if !stoppedNull || attStatus == "ended" {
		t.Fatalf("断连期间数据库被写入：stopped_at 为空=%v，attempt 状态 %s", stoppedNull, attStatus)
	}
	t.Logf("E14：切断数据库连接 %s 后执行树被物理停止", stoppedAfter.Round(time.Millisecond))

	proxy.Restore()
	if v := s.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("恢复连接后任务结束为 %+v", v)
	}
	in := s.inspect(id)
	ts := s.loadTask(id)
	a1 := attemptByNo(in, 1)
	if a1.OutcomeClass != runner.ClassStoreUnavailable || a1.StoppedAt == nil || ts.FaultRetriesUsed != 1 || ts.AttemptsTotal != 2 {
		t.Fatalf("attempt 1 %+v；fault_retries_used=%d attempts_total=%d", a1, ts.FaultRetriesUsed, ts.AttemptsTotal)
	}
	if len(s.procs) != 1 || srv.exited() {
		t.Fatal("server 被重启或已退出：补齐应由现有所有者完成")
	}
	if n := countType(s.events(id), "attempt_ended"); n != 1 { // 与 E8 相同：故障结束的 attempt 一个
		t.Fatalf("attempt_ended 事件 %d 个", n)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), stepIndex, len(steps))
	s.finish()
}

// ---- E15 ----

// dropFirstPost 是 API 前的反向代理：第一个 POST /tasks 照常交给 server 处理（读完响应，服务端已提交），
// 但不把响应交给客户端，直接断开连接。
type dropFirstPost struct {
	backend *url.URL
	proxy   *httputil.ReverseProxy
	mu      sync.Mutex
	posts   int
	dropped int
}

func (d *dropFirstPost) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/tasks" {
		d.proxy.ServeHTTP(w, r)
		return
	}
	d.mu.Lock()
	d.posts++
	drop := d.dropped == 0
	if drop {
		d.dropped++
	}
	d.mu.Unlock()
	if !drop {
		d.proxy.ServeHTTP(w, r)
		return
	}
	body, _ := io.ReadAll(r.Body)
	req, _ := http.NewRequest(r.Method, d.backend.String()+r.URL.Path, bytes.NewReader(body))
	req.Header = r.Header.Clone()
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close() // 响应丢失
	}
}

// reverseProxy 转发到 backend，并把 Host 改为 backend（API 只接受允许列表中的 Host，§15.3）。
func reverseProxy(backend *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(backend) }, FlushInterval: -1}
}

func runCLI(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	env := cli.Env{Sleep: func(time.Duration) {}, Getenv: func(string) string { return "" }}
	code := cli.RunEnv(env, args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// TestE15LostCreateResponse：E15——`POST /tasks` 的响应丢失（代理丢弃），CLI 以同一 request_id 重试 → 只创建
// 一个任务；再以同一 request_id、不同内容提交 → 409 request_conflict，仍只有一个任务。
func TestE15LostCreateResponse(t *testing.T) {
	s := newSys(t)
	s.start("")
	backend, err := url.Parse(s.srv.base)
	if err != nil {
		t.Fatal(err)
	}
	d := &dropFirstPost{backend: backend, proxy: reverseProxy(backend)}
	px := httptest.NewServer(d)
	defer px.Close()
	execLog := filepath.Join(s.dir, "exec.log")
	spec1 := sysSpec(execLog, "e15", nil, progressStep("only"))
	code, out, errOut := runCLI("task", "submit", "--spec", spec1, "--request-id", "e15-req", "--addr", px.URL)
	if code != 0 {
		t.Fatalf("CLI submit 退出码 %d：%s", code, errOut)
	}
	var created struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(out), &created); err != nil || created.TaskID == "" {
		t.Fatalf("CLI 输出 %q: %v", out, err)
	}
	d.mu.Lock()
	posts, dropped := d.posts, d.dropped
	d.mu.Unlock()
	if dropped != 1 || posts != 2 {
		t.Fatalf("代理看到 %d 次 POST、丢弃 %d 个响应；期望首个响应丢失后重试一次", posts, dropped)
	}
	var tasks, requests int
	var resource string
	pgQueryRow(t, s.dsn, "SELECT count(*) FROM tasks", nil, &tasks)
	pgQueryRow(t, s.dsn, "SELECT count(*), max(resource_id) FROM api_requests WHERE request_id = 'e15-req'", nil, &requests, &resource)
	if tasks != 1 || requests != 1 || resource != created.TaskID {
		t.Fatalf("任务 %d 个、请求记录 %d 条（资源 %s）；期望只创建一个任务 %s", tasks, requests, resource, created.TaskID)
	}

	// 同一 request_id、不同内容 → 409 request_conflict（CLI 与直接请求）。
	spec2 := sysSpec(execLog, "e15-other", nil, progressStep("other"))
	code, _, errOut = runCLI("task", "submit", "--spec", spec2, "--request-id", "e15-req", "--addr", px.URL)
	if code != 1 || !strings.Contains(errOut, "409 request_conflict") {
		t.Fatalf("不同内容重试：退出码 %d，%s", code, errOut)
	}
	st, b, err := httpDo("POST", s.srv.base+"/tasks", `{"request_id":"e15-req","spec":`+spec2+`}`)
	if err != nil || st != http.StatusConflict || !strings.Contains(string(b), `"request_conflict"`) {
		t.Fatalf("不同内容重试 = %d %s %v", st, b, err)
	}
	pgQueryRow(t, s.dsn, "SELECT count(*) FROM tasks", nil, &tasks)
	if tasks != 1 {
		t.Fatalf("冲突请求创建了任务：%d 个", tasks)
	}
	if v := s.waitTerminal(created.TaskID); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	s.finish()
}

// ---- E6、E16：SSE 断开与 CLI 重连 ----

// sseCutter 是 API 前的反向代理：事件流响应在随机字节数处被中断（前 forceCuts 个请求必中断；randomCuts
// 时其余以概率 3/4 中断；seed 记录在日志中）。记录每个事件流请求是否送达了 task_terminal。
type sseCutter struct {
	backend    *url.URL
	proxy      *httputil.ReverseProxy
	forceCuts  int
	randomCuts bool
	mu         sync.Mutex
	rng        *mrand.Rand
	requests   []sseRequest
}

type sseRequest struct {
	LastEventID string
	Cut         bool
	Terminal    bool
}

func newSSECutter(t *testing.T, base string, forceCuts int, randomCuts bool, seed uint64) (*sseCutter, *httptest.Server) {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	c := &sseCutter{backend: u, proxy: reverseProxy(u), forceCuts: forceCuts, randomCuts: randomCuts,
		rng: mrand.New(mrand.NewPCG(seed, 6))}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	return c, srv
}

func (c *sseCutter) snapshot() []sseRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.requests)
}

func (c *sseCutter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/events") {
		c.proxy.ServeHTTP(w, r)
		return
	}
	c.mu.Lock()
	n := len(c.requests)
	limit := -1
	if n < c.forceCuts || (c.randomCuts && c.rng.IntN(4) != 0) {
		limit = 1 + c.rng.IntN(800)
	}
	c.requests = append(c.requests, sseRequest{LastEventID: r.Header.Get("Last-Event-ID")})
	c.mu.Unlock()
	record := func(f func(*sseRequest)) {
		c.mu.Lock()
		f(&c.requests[n])
		c.mu.Unlock()
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, c.backend.String()+r.URL.Path, nil)
	req.Header = r.Header.Clone()
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	flusher := w.(http.Flusher)
	var seen bytes.Buffer
	buf := make([]byte, 256)
	written := 0
	for {
		k, rerr := resp.Body.Read(buf)
		if k > 0 {
			chunk := buf[:k]
			if limit >= 0 && written+k >= limit {
				chunk = buf[:limit-written]
			}
			_, _ = w.Write(chunk)
			flusher.Flush()
			seen.Write(chunk)
			written += len(chunk)
			// task_terminal 的帧完整送达（帧以空行结束）。
			if i := bytes.Index(seen.Bytes(), []byte("event: "+api.EventTaskTerminal+"\n")); i >= 0 &&
				bytes.Contains(seen.Bytes()[i:], []byte("\n\n")) {
				record(func(q *sseRequest) { q.Terminal = true })
			}
			if limit >= 0 && written >= limit {
				record(func(q *sseRequest) { q.Cut = true })
				panic(http.ErrAbortHandler) // 中断连接：客户端读到不完整的流
			}
		}
		if rerr != nil {
			return
		}
	}
}

// watchCLI 以 CLI 的 `task watch` 跟随事件（经 base），返回退出码与输出的事件。
func watchCLI(t *testing.T, base, id string) (int, []api.Event, string) {
	t.Helper()
	type res struct {
		code     int
		out, err string
	}
	ch := make(chan res, 1)
	go func() {
		var out, errOut bytes.Buffer
		env := cli.Env{Sleep: func(time.Duration) { time.Sleep(10 * time.Millisecond) }, Getenv: func(string) string { return "" }}
		code := cli.RunEnv(env, []string{"task", "watch", id, "--addr", base}, &out, &errOut)
		ch <- res{code, out.String(), errOut.String()}
	}()
	var r res
	select {
	case r = <-ch:
	case <-time.After(waitLimit):
		t.Fatal("CLI watch 未在期限内结束")
	}
	var evs []api.Event
	for _, line := range strings.Split(strings.TrimSpace(r.out), "\n") {
		if line == "" {
			continue
		}
		var e struct {
			TaskSeq int64  `json:"task_seq"`
			Type    string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("CLI 输出行 %q: %v", line, err)
		}
		evs = append(evs, api.Event{TaskSeq: e.TaskSeq, Type: e.Type})
	}
	return r.code, evs, r.err
}

// assertSameEvents：CLI 收到的事件与数据库中直到 task_terminal 的事件完全一致——task_seq 从 1 连续、无缺失、
// 无重复，类型相同，最后一个是 task_terminal（服务端在它之后关闭流）。
func assertSameEvents(t *testing.T, got, want []api.Event) {
	t.Helper()
	for i, e := range want {
		if e.Type == api.EventTaskTerminal {
			want = want[:i+1]
			break
		}
	}
	if len(got) != len(want) {
		t.Fatalf("CLI 收到 %d 个事件，数据库中有 %d 个", len(got), len(want))
	}
	for i := range want {
		if got[i].TaskSeq != int64(i+1) || got[i].TaskSeq != want[i].TaskSeq || got[i].Type != want[i].Type {
			t.Fatalf("第 %d 个事件：CLI %+v，数据库 %+v", i, got[i], want[i])
		}
	}
	if len(got) == 0 || got[len(got)-1].Type != api.EventTaskTerminal {
		t.Fatal("最后一个事件不是 task_terminal")
	}
}

// TestE6SSERandomDisconnect：E6——事件流在随机位置被中断，CLI 以 Last-Event-ID 带退避重连 → 事件无缺失、
// 无重复；任务不重新执行（只有一个 attempt，执行计数中每步恰一次）。
func TestE6SSERandomDisconnect(t *testing.T) {
	s := newSys(t)
	s.start("")
	seed := uint64(time.Now().UnixNano())
	if v := os.Getenv("AGENTBOX_E2E_SEED"); v != "" {
		seed, _ = strconv.ParseUint(v, 10, 64)
	}
	cutter, px := newSSECutter(t, s.srv.base, 2, true, seed)
	execLog := filepath.Join(s.dir, "exec.log")
	var steps []step
	for i := 0; i < 60; i++ {
		steps = append(steps, progressStep(fmt.Sprintf("p%d", i)), sleepStep(15))
	}
	id := s.submit("e6", sysSpec(execLog, "e6", nil, steps...))
	code, got, errOut := watchCLI(t, px.URL, id)
	if code != 0 {
		t.Fatalf("CLI watch 退出码 %d：%s", code, errOut)
	}
	assertSameEvents(t, got, s.events(id))
	reqs := cutter.snapshot()
	cuts := 0
	for _, r := range reqs {
		if r.Cut {
			cuts++
		}
	}
	if cuts < 2 {
		t.Fatalf("事件流只被中断 %d 次，用例无效", cuts)
	}
	in := s.inspect(id)
	if len(in.Attempts) != 1 || in.Task.Status != "succeeded" {
		t.Fatalf("任务被重新执行：%+v", in)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), nil, len(steps))
	t.Logf("E6：seed=%d（AGENTBOX_E2E_SEED 复现），%d 个事件，%d 次事件流请求，其中 %d 次被中断", seed, len(got), len(reqs), cuts)
	s.finish()
}

// TestE16WatchStopsAfterTerminal：E16——CLI 收到 task_terminal 后停止重连：已结束的任务只发出一个请求；
// 运行中的任务经中断的流跟随，送达 task_terminal 的请求是最后一个请求。非法游标与超出最新事件的游标
// → 400 invalid_cursor。
func TestE16WatchStopsAfterTerminal(t *testing.T) {
	s := newSys(t)
	s.start("")
	execLog := filepath.Join(s.dir, "exec.log")

	done := s.submit("e16-done", sysSpec(execLog, "e16", nil, progressStep("x")))
	s.waitTerminal(done)
	plain, px := newSSECutter(t, s.srv.base, 0, false, 0) // 只计数，不中断
	code, got, errOut := watchCLI(t, px.URL, done)
	if code != 0 {
		t.Fatalf("CLI watch 退出码 %d：%s", code, errOut)
	}
	assertSameEvents(t, got, s.events(done))
	if reqs := plain.snapshot(); len(reqs) != 1 || !reqs[0].Terminal {
		t.Fatalf("已结束任务的 watch 发出 %d 个请求 %+v；期望收到 task_terminal 后不再请求", len(reqs), reqs)
	}

	running := s.submit("e16-running", sysSpec(execLog, "e16", nil, progressStep("a"), sleepStep(300), progressStep("b")))
	cutter, px2 := newSSECutter(t, s.srv.base, 2, false, uint64(time.Now().UnixNano())) // 前两个请求中断
	code, got, errOut = watchCLI(t, px2.URL, running)
	if code != 0 {
		t.Fatalf("CLI watch 退出码 %d：%s", code, errOut)
	}
	assertSameEvents(t, got, s.events(running))
	reqs := cutter.snapshot()
	terminals := 0
	for _, r := range reqs {
		if r.Terminal {
			terminals++
		}
	}
	if terminals != 1 || !reqs[len(reqs)-1].Terminal {
		t.Fatalf("事件流请求 %+v：送达 task_terminal 的请求应是最后一个", reqs)
	}

	for _, cursor := range []string{"abc", "-1", "01", "1.5", "999999"} {
		req, _ := http.NewRequest(http.MethodGet, s.srv.base+"/tasks/"+done+"/events", nil)
		req.Header.Set("Last-Event-ID", cursor)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), `"invalid_cursor"`) {
			t.Fatalf("Last-Event-ID %q = %d %s，期望 400 invalid_cursor", cursor, resp.StatusCode, b)
		}
	}
	t.Logf("E16：已结束任务 1 个请求；运行中任务 %d 个请求（%d 次中断），task_terminal 之后没有请求", len(reqs), len(reqs)-1)
	s.finish()
}

// ==== 真实隔离验收（Plan 2 Task 13）：provider/local + 生产启动器 ====
//
// 以下用例需要 root 与 cgroup v2（非 root 时跳过；CI 的 linux-integration 作业以 root 运行，必须实际执行）。
// Worker 在真实沙箱中运行：user/pid/mount/net 等命名空间、映射 UID、只读 rootfs 模板（包含 /opt/agentbox）、
// seccomp、降权后的 stage-2 helper。worker 包由测试复制到 rootfs.WorkerDir（与 README 快速开始的安装一致）。
//
// 两种装置：
//   - 进程内（newRealHarness）：与上面的控制面验收共用 harness 与断言，只把 provider 换成 provider/local
//     （local.NewProcessStarter）。专用启动进程、沙箱 init 与 stage-2 helper 是本测试二进制（TestMain 分流到
//     与 cmd/agentbox 相同的 sandbox.RunLaunch/RunInit/RunHelper）。注入点（丢弃回复、暂停、杀死 Worker、
//     改写产物）由包装 StartExec 的 interceptProv 在 Worker 的 stdin/stdout 上实现；"杀死 Worker"从宿主向
//     环境 cgroup 中的 sim_worker 进程发 SIGKILL。
//   - 真实子进程（newRealSys）：构建并运行 cmd/agentbox（CGO_ENABLED 取调用方环境；CI 为 0），用于
//     server 被 SIGKILL 后重启（E5 物理回收）与累计运行时限跨重启；结束时运行 `agentbox verify-invariants
//     --quiescent`。
//
// 全部用例使用本次运行独占的 cgroup 根 /sys/fs/cgroup/agentbox-e2e-<随机>，TestMain 结束时删除。

// requireRealIsolation：非 root 时跳过；root 但不是 cgroup v2 时，CI 中失败、本地跳过。
func requireRealIsolation(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("需要 root：真实隔离验收（provider/local + 生产启动器）")
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &st); err != nil || st.Type != 0x63677270 { // CGROUP2_SUPER_MAGIC
		required(t, "cgroup v2（/sys/fs/cgroup）")
	}
}

var (
	workerOnce sync.Once
	workerErr  error
)

// installWorker 把仓库中的 worker 包（agentbox_worker、sim_worker、deepresearch）复制到 rootfs.WorkerDir
// （每次 go test 一次；不复制 __pycache__），返回该目录。等价于 README 快速开始中的
// `sudo install -d /opt/agentbox && sudo cp -r worker/agentbox_worker worker/sim_worker worker/deepresearch /opt/agentbox/`。
func installWorker(t *testing.T) string {
	t.Helper()
	src := workerPath(t)
	workerOnce.Do(func() { workerErr = copyWorker(src, rootfs.WorkerDir) })
	if workerErr != nil {
		t.Fatalf("安装 worker 包到 %s: %v", rootfs.WorkerDir, workerErr)
	}
	return rootfs.WorkerDir
}

func copyWorker(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, pkg := range []string{"agentbox_worker", "sim_worker", "deepresearch"} {
		err := filepath.WalkDir(filepath.Join(src, pkg), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			rel, err := filepath.Rel(src, p)
			if err != nil {
				return err
			}
			out := filepath.Join(dst, rel)
			if d.IsDir() {
				if err := os.MkdirAll(out, 0o755); err != nil {
					return err
				}
				return os.Chmod(out, 0o755)
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if err := os.WriteFile(out, b, 0o644); err != nil {
				return err
			}
			return os.Chmod(out, 0o644)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	realCgOnce sync.Once
	realCgRoot string // 本次运行独占的 cgroup 根；TestMain 结束时删除
	realCgErr  error
)

func realCgroupRoot(t *testing.T) string {
	t.Helper()
	realCgOnce.Do(func() {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		dir := "/sys/fs/cgroup/agentbox-e2e-" + hex.EncodeToString(b)
		if realCgErr = os.Mkdir(dir, 0o755); realCgErr == nil {
			realCgRoot = dir
		}
	})
	if realCgErr != nil {
		t.Fatalf("创建测试 cgroup 根: %v", realCgErr)
	}
	return realCgRoot
}

// killRemove 杀死 dir 下全部 cgroup（深者先）中的进程并删除它们，最后删除 dir 本身。尽力而为。
func killRemove(dir string) {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if e.IsDir() {
			killRemove(filepath.Join(dir, e.Name()))
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "cgroup.kill"), []byte("1"), 0o644)
	for i := 0; i < 500; i++ {
		if err := os.Remove(dir); err == nil || errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// cgroupPids 返回 dir 及其子 cgroup 中的全部进程。
func cgroupPids(dir string) []int {
	var pids []int
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		b, _ := os.ReadFile(filepath.Join(p, "cgroup.procs"))
		for _, f := range strings.Fields(string(b)) {
			if n, err := strconv.Atoi(f); err == nil {
				pids = append(pids, n)
			}
		}
		return nil
	})
	return pids
}

var (
	starterOnce sync.Once
	realStarter *local.ProcessStarter
	starterErr  error
)

// processStarter 返回生产启动器（本测试进程随之成为 child subreaper）。
func processStarter(t *testing.T) *local.ProcessStarter {
	t.Helper()
	starterOnce.Do(func() { realStarter, starterErr = local.NewProcessStarter() })
	if starterErr != nil {
		t.Fatal(starterErr)
	}
	return realStarter
}

// realDataDir 返回 0711 的数据目录（沙箱 init 须能经过它到达 workspace；t.TempDir 的上级目录是 0700）。
func realDataDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "agentbox-e2e-data-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o711); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// newRealHarness 是进程内装置的真实隔离版本：provider/local + 生产启动器。
func newRealHarness(t *testing.T) *harness {
	t.Helper()
	requireRealIsolation(t)
	h := &harness{t: t, pyPath: installWorker(t), dsn: newDatabase(t), dir: realDataDir(t), real: true,
		cgRoot: realCgroupRoot(t), gate: &storeGate{failAfter: 500 * time.Millisecond, failed: map[string]int{}},
		tasks: map[string]*taskRec{}, controlRetries: map[string]int{}}
	starter := processStarter(t)
	h.newProvider = func(installID string) (provider.Provider, error) {
		p, err := local.New(local.Options{DataDir: h.dir, CgroupRoot: h.cgRoot, InstallID: installID, Starter: starter})
		if err != nil {
			return nil, err
		}
		h.local, h.installID = p, installID
		h.prov = &interceptProv{Provider: p, h: h}
		return h.prov, nil
	}
	h.cleanup()
	return h
}

// envCgroup 是环境的 cgroup 目录（provider/local：<root>/agentbox-<install_id>/env-<env_id>）。
func (h *harness) envCgroup(envID string) string {
	return filepath.Join(h.cgRoot, "agentbox-"+h.installID, "env-"+envID)
}

// killWorker 从宿主以 SIGKILL 杀死环境 cgroup 中的 Worker 进程（sim_worker 或 python3 -m deepresearch；
// 不经平台：分类为 crashed_signal）。
func (h *harness) killWorker(envID string) {
	for _, pid := range cgroupPids(h.envCgroup(envID)) {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil &&
			(bytes.Contains(b, []byte("sim_worker")) || bytes.Contains(b, []byte("-m\x00deepresearch"))) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// interceptProv 包装 provider/local：StartExec 返回的句柄在 Worker 的 stdin/stdout 上逐行转发并执行注入
// （与进程型 Program 的注入点相同）。其余操作原样委托。
type interceptProv struct {
	provider.Provider
	h *harness
}

func (p *interceptProv) StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	hd, err := p.Provider.StartExec(ctx, envID, spec)
	if err != nil {
		return nil, err
	}
	return p.h.intercept(envID, hd), nil
}

type interceptHandle struct {
	provider.ExecHandle
	stdin  *io.PipeWriter
	stdout *io.PipeReader
	exited func()
}

func (x *interceptHandle) Stdin() io.WriteCloser { return x.stdin }
func (x *interceptHandle) Stdout() io.ReadCloser { return x.stdout }

func (x *interceptHandle) Wait() (provider.ExitStatus, error) {
	st, err := x.ExecHandle.Wait()
	x.exited()
	return st, err
}

// intercept：runner 写给 Worker 的第一行是 init（记录并解析注入声明），之后两个方向逐行经 onHost/onWorker。
// Worker 退出（Wait 返回）时记录退出时刻并结束暂停。
func (h *harness) intercept(envID string, hd provider.ExecHandle) provider.ExecHandle {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	var (
		mu   sync.Mutex
		run  *workerRun
		once sync.Once
	)
	x := &interceptHandle{ExecHandle: hd, stdin: inW, stdout: outR}
	x.exited = func() {
		once.Do(func() {
			mu.Lock()
			r := run
			mu.Unlock()
			if r != nil {
				r.rec.mu.Lock()
				r.rec.exits[r.in.AttemptNo] = time.Now()
				r.rec.mu.Unlock()
			}
			cancel()
		})
	}
	go func() {
		defer outW.Close()
		br := bufio.NewReaderSize(inR, 1<<20)
		line, err := br.ReadBytes('\n')
		var in protocol.Init
		if jerr := json.Unmarshal(line, &in); err != nil || jerr != nil {
			// 不是 init：不注入，原样转发。
			_, _ = hd.Stdin().Write(line)
			go func() {
				_, _ = io.Copy(hd.Stdin(), br)
				_ = hd.Stdin().Close()
			}()
			_, _ = io.Copy(outW, hd.Stdout())
			return
		}
		r := h.newRun(in)
		r.killFn = func() { h.killWorker(envID) }
		mu.Lock()
		run = r
		mu.Unlock()
		_, _ = hd.Stdin().Write(line)
		go r.pumpHost(br, hd.Stdin())
		r.pumpWorker(ctx, hd.Stdout(), outW)
	}()
	return x
}

// postTask 经 HTTP 提交任务；limits 非空时随请求提交。
func postTask(base, requestID, spec, limits string) (string, error) {
	body := `{"request_id":"` + requestID + `","spec":` + spec
	if limits != "" {
		body += `,"limits":` + limits
	}
	st, b, err := httpDo("POST", base+"/tasks", body+"}")
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

// ---- 进程内装置上的真实隔离用例：与控制面验收相同的断言 ----

// TestRealFirstSlice：首个切片在真实沙箱中——提交 → checkpoint → 杀死 Worker → 恢复 → result。
func TestRealFirstSlice(t *testing.T) { testFirstSlice(t, newRealHarness(t)) }

// TestRealE1KillAfterSecondCheckpoint：E1 在真实沙箱中。
func TestRealE1KillAfterSecondCheckpoint(t *testing.T) { testE1(t, newRealHarness(t)) }

// TestRealE4CancelResultRace：E4 在真实沙箱中；随机交错降为 100 次（seed 记录在日志中）。
func TestRealE4CancelResultRace(t *testing.T) { testE4(t, newRealHarness(t), 100) }

// TestRealE7DropCheckpointResult：E7 在真实沙箱中。
func TestRealE7DropCheckpointResult(t *testing.T) { testE7(t, newRealHarness(t)) }

// TestRealE8StoreBlocked：E8 在真实沙箱中（执行树的物理停止经 cgroup）。
func TestRealE8StoreBlocked(t *testing.T) { testE8(t, newRealHarness(t)) }

// TestRealE10ArtifactTampering：E10 在真实沙箱中（产物文件属于映射 UID）。
func TestRealE10ArtifactTampering(t *testing.T) { testE10(t, newRealHarness(t)) }

// e2Limits 是 E2、E3 的内存限额：128 MiB（memory.swap.max 固定为 0）。
const e2Limits = `{"memory_max":134217728}`

// TestRealE2WorkerOOM：E2——sim_worker 主进程超出 memory.max（主进程是预期受害者）→ worker_oom_likely →
// 重试一次 → failed；两个 attempt 都被 SIGKILL 且 OOMKillDelta > 0；oom_retries_used = 1，不计故障重试；
// 无残留 cgroup。
func TestRealE2WorkerOOM(t *testing.T) {
	h := newRealHarness(t)
	h.start(baseConfig())
	id, err := postTask(h.base, "e2", spec(nil, "never", nil, progressStep("allocating"), step{"op": "allocate_mb", "mb": 512}), e2Limits)
	if err != nil {
		t.Fatal(err)
	}
	v := h.waitTerminal(id)
	in := h.inspect(id)
	ts := h.loadTask(id)
	if v.Status != "failed" || v.StatusReason != runner.ClassWorkerOOMLikely || len(in.Attempts) != 2 ||
		ts.OOMRetriesUsed != 1 || ts.FaultRetriesUsed != 0 {
		t.Fatalf("任务 %s/%s，attempts %+v，oom_retries_used=%d fault_retries_used=%d；期望 failed/worker_oom_likely、2 个 attempt、OOM 重试 1 次",
			v.Status, v.StatusReason, in.Attempts, ts.OOMRetriesUsed, ts.FaultRetriesUsed)
	}
	for _, a := range in.Attempts {
		if a.OutcomeClass != runner.ClassWorkerOOMLikely || a.ExitSignal == nil || *a.ExitSignal != int64(syscall.SIGKILL) ||
			a.OOMKillDelta <= 0 || a.PlatformKilled {
			t.Fatalf("attempt %d：%+v，期望 worker_oom_likely（SIGKILL、OOMKillDelta > 0、非平台终止）", a.AttemptNo, a)
		}
	}
	h.assertNoLeak()
	for _, a := range in.Attempts {
		if _, err := os.Stat(h.envCgroup(a.EnvID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("attempt %d 的环境 cgroup 仍存在（%v）", a.AttemptNo, err)
		}
	}
	t.Logf("E2：%d 个 attempt（%s），oom_kill_delta = %d / %d", len(in.Attempts), attemptClasses(in),
		in.Attempts[0].OOMKillDelta, in.Attempts[1].OOMKillDelta)
	h.finish()
}

// TestRealE3SideProcessOOM：E3——同一环境 cgroup 中的另一个进程超出 memory.max 被 OOM 杀死，Worker 主进程
// 正常结束 → 按 Worker 结果裁决（succeeded），记 oom_observed_in_attempt，不归因为 Worker OOM、不重试。
// sim_worker 没有派生子进程的步骤，OOM 进程由测试在 Worker 暂停期间经 provider 在同一环境中启动（与 Worker
// 的子进程同处环境 cgroup，平台的归因只依据该 cgroup 的 OOM 计数与 Worker 主进程的退出状态）。
func TestRealE3SideProcessOOM(t *testing.T) {
	h := newRealHarness(t)
	h.start(baseConfig())
	// 暂停在 checkpoint 上：Worker 等待 checkpoint_result，因而在 OOM 进程运行期间不会结束。
	id, err := postTask(h.base, "e3", spec(&directives{Hold: &holdSpec{Type: "checkpoint", Nth: 1}}, "e3 done", nil,
		progressStep("before hog"), checkpointStep("c1"), progressStep("after hog")), e2Limits)
	if err != nil {
		t.Fatal(err)
	}
	h.waitHeld(id)
	envID := attemptByNo(h.inspect(id), 1).EnvID
	hog, err := h.local.StartExec(context.Background(), envID, provider.ExecSpec{ExecID: "e3-hog", Dir: "/tmp",
		Argv: []string{"python3", "-c", "b = bytearray(512 << 20)\nfor i in range(0, len(b), 4096): b[i] = 1\n"}})
	if err != nil {
		t.Fatalf("在环境中启动 OOM 进程: %v", err)
	}
	_ = hog.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, hog.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, hog.Stderr()) }()
	st, err := hog.Wait()
	if err != nil || st.Signal != syscall.SIGKILL {
		t.Fatalf("OOM 进程退出 %+v %v，期望被 SIGKILL", st, err)
	}
	diag, err := h.local.ResourceDiag(context.Background(), envID)
	if err != nil || diag.OOMKillDelta == 0 {
		t.Fatalf("OOM 之后的诊断 %+v %v", diag, err)
	}
	h.rec(id).releaseHold()
	v := h.waitTerminal(id)
	in := h.inspect(id)
	ts := h.loadTask(id)
	if v.Status != "succeeded" || len(in.Attempts) != 1 || ts.OOMRetriesUsed != 0 || ts.FaultRetriesUsed != 0 {
		t.Fatalf("任务 %s/%s，attempts %+v；期望按 Worker 结果 succeeded、不重试", v.Status, v.StatusReason, in.Attempts)
	}
	a := in.Attempts[0]
	if a.OutcomeClass != runner.ClassOOMObserved || a.OOMKillDelta <= 0 || a.ExitCode == nil || *a.ExitCode != 0 {
		t.Fatalf("attempt %+v，期望 oom_observed_in_attempt（退出码 0、OOMKillDelta > 0）", a)
	}
	if summary, outs, _ := h.pinnedResult(id, a.AttemptID); summary != "e3 done" || len(outs) != 0 {
		t.Fatalf("result %q %+v", summary, outs)
	}
	t.Logf("E3：OOM 进程被杀死（oom_kill_delta = %d），attempt 分类 %s，任务 %s", a.OOMKillDelta, a.OutcomeClass, v.Status)
	h.finish()
}

// ---- 真实子进程：cmd/agentbox ----

var (
	agentboxOnce   sync.Once
	agentboxBinDir string
	agentboxBin    string
	agentboxErr    error
)

// agentboxBinary 构建 cmd/agentbox（每次 go test 一次）。
func agentboxBinary(t *testing.T) string {
	t.Helper()
	agentboxOnce.Do(func() {
		goBin := goBinary()
		if agentboxBinDir, agentboxErr = os.MkdirTemp("", "agentbox-bin-"); agentboxErr != nil {
			return
		}
		agentboxBin = filepath.Join(agentboxBinDir, "agentbox")
		if out, err := exec.Command(goBin, "build", "-o", agentboxBin, "../../cmd/agentbox").CombinedOutput(); err != nil {
			agentboxErr = fmt.Errorf("构建 cmd/agentbox: %v\n%s", err, out)
		}
	})
	if agentboxErr != nil {
		t.Fatal(agentboxErr)
	}
	return agentboxBin
}

// newRealSys 返回运行 cmd/agentbox 的系统级装置（复用 sysHarness 的读取与停止；启动与结束检查见
// startReal、finishReal）。
func newRealSys(t *testing.T) *sysHarness {
	t.Helper()
	requireRealIsolation(t)
	installWorker(t)
	realCgroupRoot(t)
	s := &sysHarness{t: t, serverDSN: newDatabase(t), dir: realDataDir(t), bin: agentboxBinary(t)}
	s.dsn = withAppName(t, s.serverDSN)
	s.storeDSN = s.serverDSN
	t.Cleanup(func() {
		for _, p := range s.procs {
			if !p.exited() {
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		}
		if s.store != nil {
			s.store.Close()
		}
		if t.Failed() {
			for i, p := range s.procs {
				t.Logf("server #%d 日志（末尾）：\n%s", i+1, p.logs.tail(16<<10))
			}
		}
	})
	return s
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// startReal 以 `agentbox server` 启动 server 子进程（生产启动器、默认 rootfs 模板、本次运行的 cgroup 根），
// 等待 API 进入 normal 模式。
func (s *sysHarness) startReal(extra ...string) *serverProc {
	s.t.Helper()
	base := "http://127.0.0.1:" + freePort(s.t)
	cmd := exec.Command(s.bin, append([]string{"server", "--data-dir", s.dir, "--database-url", s.serverDSN,
		"--listen", strings.TrimPrefix(base, "http://"), "--cgroup-root", realCgRoot}, extra...)...)
	p := &serverProc{cmd: cmd, logs: &lockedBuffer{}, done: make(chan struct{}), base: base}
	cmd.Stdout, cmd.Stderr = p.logs, p.logs
	// server 被 SIGKILL 时，冻结的会话环境（E33）中的进程无法退出，仍持有继承的输出管道：Wait 在进程退出后至多
	// 再等 WaitDelay 读取输出，然后关闭管道返回。
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.procs = append(s.procs, p)
	s.srv = p
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	eventually(s.t, "server 就绪（API normal 模式）", func() bool {
		if p.exited() {
			s.t.Fatalf("server 提前退出（%v）：\n%s", cmd.ProcessState, p.logs.tail(4<<10))
		}
		st, b, err := httpDo("GET", base+"/status", "")
		return err == nil && st == http.StatusOK && bytes.Contains(b, []byte(`"normal"`))
	})
	if s.store == nil {
		st, err := postgres.Open(context.Background(), postgres.Options{DSN: s.dsn})
		if err != nil {
			s.t.Fatal(err)
		}
		s.store = st
		if s.blobs, err = blob.NewLocal(filepath.Join(s.dir, "blobs")); err != nil {
			s.t.Fatal(err)
		}
	}
	return p
}

// installCgroup 是该数据目录的安装 cgroup。
func (s *sysHarness) installCgroup() string {
	s.t.Helper()
	id, ok, err := datadir.NewIDFile(s.dir).Read()
	if err != nil || !ok {
		s.t.Fatalf("读取 install_id: %v %v", ok, err)
	}
	return filepath.Join(realCgRoot, "agentbox-"+id)
}

// waitConnsGone 等待被杀死的 server 的数据库连接全部结束（它已发出的 COMMIT 仍可能完成）。
func (s *sysHarness) waitConnsGone() {
	s.t.Helper()
	eventuallyWithin(s.t, "上一 server 的数据库连接全部结束", waitLimit, 20*time.Millisecond, func() bool {
		var n int
		pgQueryRow(s.t, s.dsn, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND pid <> pg_backend_pid() AND application_name <> $1`, []any{testAppName}, &n)
		return n == 0
	})
}

// finishReal：环境全部停止并清理、UID 范围归还、没有环境目录与环境 cgroup、没有隔离记录；正常停止 server
// 后 `agentbox verify-invariants --quiescent` 通过。
func (s *sysHarness) finishReal() {
	s.t.Helper()
	installCg := s.installCgroup()
	eventuallyWithin(s.t, "环境全部停止并清理、UID 范围归还、没有环境目录与环境 cgroup", waitLimit, 50*time.Millisecond, func() bool {
		var pending, held int
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM environments WHERE cleanup_state <> 'done' OR stopped_at IS NULL", nil, &pending)
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM uid_ranges WHERE state <> 'free'", nil, &held)
		envDirs, _ := os.ReadDir(filepath.Join(s.dir, "envs"))
		cgs, _ := filepath.Glob(filepath.Join(installCg, "env-*"))
		return pending == 0 && held == 0 && len(envDirs) == 0 && len(cgs) == 0 && len(cgroupPids(installCg)) == 0
	})
	var quarantined int
	pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM quarantined_resources", nil, &quarantined)
	if quarantined != 0 {
		s.t.Fatalf("误隔离：%d 条隔离记录", quarantined)
	}
	s.stop()
	out, err := exec.Command(s.bin, "verify-invariants", "--quiescent", "--data-dir", s.dir, "--database-url", s.dsn,
		"--cgroup-root", realCgRoot).CombinedOutput()
	if err != nil {
		s.t.Fatalf("agentbox verify-invariants --quiescent：%v\n%s", err, out)
	}
	s.t.Logf("agentbox verify-invariants --quiescent：%s", strings.TrimSpace(string(out)))
}

// TestRealE5ServerKilledPhysicalRecovery：E5 的物理回收部分——cmd/agentbox server 在 Worker 运行中被
// SIGKILL（checkpoint s1 已提交）→ 重启：残留执行树经 cgroup 停止、孤儿环境（目录、cgroup）回收、无误隔离；
// attempt 1 为 lost_on_restart，attempt 2 从 s1 恢复且两个 attempt 的执行不交错（执行计数）；产物完整。
// 实测：server 退出后 init 读到控制连接 EOF 而退出，PID namespace 随之销毁其中全部进程，因此重启时通常
// 只剩环境目录与 cgroup（存活进程数记录在日志中；若有存活进程，断言它们在恢复后不再存活）。
func TestRealE5ServerKilledPhysicalRecovery(t *testing.T) {
	s := newRealSys(t)
	first := s.startReal()
	steps := []step{
		progressStep("begin"),
		artifactStep("a1", "a1.txt", "one"),
		checkpointStep("s1"),
		sleepStep(5000),
		artifactStep("a2", "a2.txt", "two"),
		progressStep("end"),
	}
	const s1Index = 2
	id := s.submit("e5-real", sysSpec("/workspace/exec.log", "e5", []string{"a1", "a2"}, steps...))
	eventually(t, "checkpoint s1 已提交", func() bool {
		in, err := s.store.Inspect(context.Background(), id)
		return err == nil && len(in.Checkpoints) == 1
	})
	env1 := attemptByNo(s.inspect(id), 1).EnvID
	envCg := filepath.Join(s.installCgroup(), "env-"+env1)
	if n := len(cgroupPids(envCg)); n == 0 {
		t.Fatalf("server 被杀死前环境 cgroup %s 中没有进程", envCg)
	}
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if ps := s.waitExit(first); !killedBySIGKILL(ps) {
		t.Fatalf("server 退出状态 %v", ps)
	}
	s.waitConnsGone()
	survivors := cgroupPids(envCg)
	_, dirErr := os.Stat(filepath.Join(s.dir, "envs", env1))
	t.Logf("server 被 SIGKILL 后：环境 cgroup 中 %d 个存活进程，环境目录存在=%v", len(survivors), dirErr == nil)
	if _, cgErr := os.Stat(envCg); dirErr != nil || cgErr != nil {
		t.Fatalf("server 被杀死后环境 %s 的目录与 cgroup 应作为孤儿残留：%v %v", env1, dirErr, cgErr)
	}

	s.startReal()
	v := s.waitTerminal(id)
	in := s.inspect(id)
	ts := s.loadTask(id)
	a1 := attemptByNo(in, 1)
	if v.Status != "succeeded" || len(in.Attempts) != 2 || a1.OutcomeClass != runner.ClassLostOnRestart ||
		ts.FaultRetriesUsed != 1 || countType(s.events(id), "task_terminal") != 1 {
		t.Fatalf("恢复后任务 %+v，attempts %+v，fault_retries_used=%d", v, in.Attempts, ts.FaultRetriesUsed)
	}
	if _, err := os.Stat(envCg); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("残留环境 cgroup %s 未被回收（%v）", envCg, err)
	}
	for _, pid := range survivors {
		if err := syscall.Kill(pid, 0); err == nil {
			if b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); !bytes.Contains(b, []byte(") Z ")) {
				t.Fatalf("残留执行进程 %d 在恢复后仍存活", pid)
			}
		}
	}
	// 执行计数：attempt 1 执行到 sleep（第 3 步）为止；attempt 2 从 s1 的下一步开始、不重做之前的步骤；
	// attempt 2 开始执行之后 attempt 1 没有再执行任何步骤（残留执行树已停止）。
	recs := readExecLog(t, filepath.Join(s.dir, "workspaces", id, "exec.log"))
	next := map[int64]int{1: 0, 2: s1Index + 1}
	var started2 bool
	for i, r := range recs {
		if r.AttemptNo == 2 {
			started2 = true
		} else if started2 {
			t.Fatalf("执行计数第 %d 行：attempt 2 开始执行后 attempt %d 仍在执行：%+v", i, r.AttemptNo, recs)
		}
		if r.Index != next[r.AttemptNo] {
			t.Fatalf("attempt %d 执行了第 %d 步，期望第 %d 步：%+v", r.AttemptNo, r.Index, next[r.AttemptNo], recs)
		}
		next[r.AttemptNo]++
	}
	if next[1] != s1Index+2 || next[2] != len(steps)+1 {
		t.Fatalf("执行计数不完整：%+v", recs)
	}
	last := in.Attempts[len(in.Attempts)-1]
	if got := s.readPinnedOutputs(id, last.AttemptID); got["a1"] != "one" || got["a2"] != "two" || len(got) != 2 {
		t.Fatalf("固定输出 %q", got)
	}
	t.Logf("E5（物理回收）：重启前 %d 个存活进程 + 环境目录；恢复后 %d 个 attempt（%s）", len(survivors), len(in.Attempts), attemptClasses(in))
	s.finishReal()
}

// TestRealRunTimeLimitAcrossRestart：累计运行时限（规格 §14.4）——时限 6 s 的任务运行约 3 s 后 server 被
// SIGKILL 并重启：未记账区间按墙钟计入、额度不重置，到期以 task_deadline_exceeded 终止；重启后的 attempt
// 运行时间明显短于完整时限。
func TestRealRunTimeLimitAcrossRestart(t *testing.T) {
	const limit = 6 * time.Second
	s := newRealSys(t)
	first := s.startReal()
	id, err := postTask(first.base, "deadline", spec(nil, "never", nil, checkpointStep("c1"), sleepStep(600000)),
		fmt.Sprintf(`{"max_run_time_ms":%d}`, limit.Milliseconds()))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "checkpoint c1 已提交", func() bool {
		in, err := s.store.Inspect(context.Background(), id)
		return err == nil && len(in.Checkpoints) == 1
	})
	time.Sleep(limit / 2)
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	s.waitExit(first)
	s.waitConnsGone()
	pre := s.loadTask(id)

	s.startReal()
	var start2 time.Time
	eventually(t, "重启后任务到达终态或创建 attempt 2", func() bool {
		v, err := s.store.GetTask(context.Background(), id)
		if err == nil && task.IsTerminal(v.Status) {
			return true
		}
		in, err := s.store.Inspect(context.Background(), id)
		if err == nil && len(in.Attempts) >= 2 && start2.IsZero() {
			start2 = time.Now()
		}
		return false
	})
	v := s.waitTerminal(id)
	in := s.inspect(id)
	ts := s.loadTask(id)
	if v.Status != "failed" || v.StatusReason != runner.ClassDeadlineExceeded || attemptByNo(in, 1).OutcomeClass != runner.ClassLostOnRestart {
		t.Fatalf("任务 %s/%s，attempts %+v；期望 task_deadline_exceeded 且 attempt 1 为 lost_on_restart", v.Status, v.StatusReason, in.Attempts)
	}
	// 重启前运行了至少 limit/2（持久化周期 10 s，尚未记账）：恢复按墙钟差全额计入。
	if ts.RunTimeMs < (limit / 2).Milliseconds() {
		t.Fatalf("恢复后 run_time_ms = %d，未计入重启前约 %s 的运行时间（重启前持久化 %d）", ts.RunTimeMs, limit/2, pre.RunTimeMs)
	}
	var ran2 time.Duration
	if !start2.IsZero() {
		ran2 = time.Since(start2)
		if ran2 > limit-limit/4 {
			t.Fatalf("重启后的 attempt 运行了 %s 才到期：额度被重置（时限 %s）", ran2.Round(time.Millisecond), limit)
		}
	}
	t.Logf("时限 %s：重启前持久化 run_time_ms=%d；重启后 %d 个 attempt（%s），attempt 2 存续约 %s；最终 run_time_ms=%d",
		limit, pre.RunTimeMs, len(in.Attempts), attemptClasses(in), ran2.Round(time.Millisecond), ts.RunTimeMs)
	s.finishReal()
}

// ---- M2：Gateway 故障实验（fake upstream；规格 §16.4 E17、E19、E20、E48、E11b）----
//
// 上游是进程内的 tests/e2e/fakeupstream（不访问外网）；Worker 是真实的 sim_worker（chat 操作经 Gateway 的
// Unix socket 调用模型）。E17、E19、E20 用进程内装置（provider/fake 的进程型 Program），E48、E11b 需要
// faultinject，用 agentbox-e2e 子进程（procprov）。测试也直接连接 attempt 的 socket（与 Worker 同属主）发出
// 同 ID 的请求，观察 Worker 看不到的响应（409、504、重放头）。每条用例以静止时的不变量检查结束（含 I3、I14）。

const (
	gatewaySocketEnv = "AGENTBOX_GATEWAY_SOCKET" // Worker SDK 的 socket 路径覆盖
	modelKeyEnv      = "AGENTBOX_MODEL_API_KEY"  // agentbox-e2e 读取的模型 Key
	e2eCallID        = "root/s1/chat/1"          // sim_worker 的 chat 操作（step_id s1）的第一个调用 ID
	probeCallID      = "e2e/probe/chat/1"        // 测试直接发出的另一调用
)

// gatewayPoints 是 Gateway 的钩子点及覆盖它们的实验（E5 不遍历它们：任务不经 Gateway 时不会到达）。
var gatewayPoints = map[string]string{faultinject.CallInFlight: "E48", faultinject.ReservationCommit: "E11b"}

var (
	gwMessages = []map[string]string{
		{"role": "system", "content": fakeupstream.StagePlan + " e2e 规划"},
		{"role": "user", "content": "研究主题：e2e"},
	}
	probeMessages = []map[string]string{{"role": "user", "content": "e2e 探针调用"}}
)

func chatStep(stepID string) step {
	return step{"op": "chat", "step_id": stepID, "messages": gwMessages}
}

// chatBody 是 sim_worker 的 chat 操作发出的请求体（{"messages": ...}，未设置 max_tokens）：指纹按 JCS 规范化，
// 与 Worker 的编码细节无关。
func chatBody(msgs []map[string]string) string {
	b, err := json.Marshal(map[string]any{"messages": msgs})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func gwSecret() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b) // crypto/rand 不会失败（Go 1.20 起失败时直接终止进程）
	return "sk-e2e-" + hex.EncodeToString(b)
}

// gatewayCfg 是进程内装置的 Gateway 配置：模型上游指向 fake upstream（单价每 token 1 微美元），搜索供应商 fake
// 且经 SearchBaseURL 访问 fake upstream 的 /search，fake upstream 的 host:port 显式放行（与 cmd/agentbox 的
// --search-provider fake 校验相同）。
func gatewayCfg(fu *fakeupstream.Server, key string, lim call.Limits) app.Config {
	cfg := baseConfig()
	cfg.Model = app.ModelConfig{BaseURL: fu.ModelBaseURL(), Name: fakeupstream.Model, APIKey: key,
		Pricing: upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 1_000_000}}
	cfg.SearchProvider, cfg.SearchBaseURL = upstream.SearchFake, fu.URL()
	cfg.UpstreamAllowPrivate = []string{fu.HostPort()}
	cfg.Gateway = lim
	return cfg
}

func (h *harness) gwSocket(attemptID string) string {
	return filepath.Join(h.dir, "gateway", attemptID+".sock")
}

func (s *sysHarness) gwSocket(attemptID string) string {
	return filepath.Join(s.dir, "gateway", attemptID+".sock")
}

// gwResp 是测试直接发给 Gateway 的一次请求的结果。
type gwResp struct {
	Status   int
	Code     string
	Replayed bool
	Blob     string
}

// gwDo 在 attempt 的 socket 上以一条新连接发出一个请求（不复用连接）。
func gwDo(sock, method, path, callID, body string) (gwResp, error) {
	return gwDoSubrun(sock, "", method, path, callID, body)
}

// gwDoSubrun 同 gwDo；subrunID 非空时带 X-Agentbox-Subrun（归属该 sub-run，call id 须以 <subrunID>/ 开头）。
func gwDoSubrun(sock, subrunID, method, path, callID, body string) (gwResp, error) {
	tr := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequest(method, "http://gateway"+path, strings.NewReader(body))
	if err != nil {
		return gwResp{}, err
	}
	if callID != "" {
		req.Header.Set("X-Agentbox-Call-Id", callID)
	}
	if subrunID != "" {
		req.Header.Set("X-Agentbox-Subrun", subrunID)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return gwResp{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return gwResp{}, err
	}
	r := gwResp{Status: resp.StatusCode, Replayed: resp.Header.Get("X-Agentbox-Replayed") == "true",
		Blob: resp.Header.Get("X-Agentbox-Blob")}
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(b, &e); err != nil {
			return r, fmt.Errorf("错误响应体 %q: %w", b, err)
		}
		r.Code = e.Error.Code
	}
	return r, nil
}

// gwCall 是 calls 表中的一行（测试关心的列）。
type gwCall struct {
	State          string
	Tries          int
	PED            bool // possible_external_duplicate
	FirstAttempt   string
	ResultRef      string
	FailReason     string
	Deadline       time.Time
	ResolvingSince *time.Time
}

func gwCallRow(t *testing.T, dsn, taskID, callID string) gwCall {
	t.Helper()
	var c gwCall
	pgQueryRow(t, dsn, `SELECT state, tries_used, possible_external_duplicate, first_attempt_id, COALESCE(result_ref, ''),
			fail_reason, deadline_at, resolving_since FROM calls WHERE task_id = $1 AND call_id = $2`, []any{taskID, callID},
		&c.State, &c.Tries, &c.PED, &c.FirstAttempt, &c.ResultRef, &c.FailReason, &c.Deadline, &c.ResolvingSince)
	return c
}

// gwTry 是一次 try 与它的 reservation。
type gwTry struct {
	TryNo       int
	AttemptID   string
	Outcome     string
	Error       string
	Reservation string // reservation 的状态（held、settled、released、charged_unknown）
	Amount      int64
	Cost        int64
}

func gwTries(t *testing.T, dsn, taskID, callID string) []gwTry {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT t.try_no, t.attempt_id, t.outcome, t.error, r.state, r.amount, t.cost_micro
		FROM call_tries t JOIN reservations r USING (reservation_id) WHERE t.task_id = $1 AND t.call_id = $2 ORDER BY t.try_no`,
		taskID, callID)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (gwTry, error) {
		var x gwTry
		err := r.Scan(&x.TryNo, &x.AttemptID, &x.Outcome, &x.Error, &x.Reservation, &x.Amount, &x.Cost)
		return x, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// gwBudget 读取任务账本（reserved、spent、unknown）。
func gwBudget(t *testing.T, dsn, taskID string) (reserved, spent, unknown int64) {
	t.Helper()
	pgQueryRow(t, dsn, "SELECT reserved_micro, spent_micro, unknown_micro FROM budgets WHERE task_id = $1", []any{taskID},
		&reserved, &spent, &unknown)
	return reserved, spent, unknown
}

// assertKeyOnlyUpstream：Key 只出现在发往上游的 Authorization 头中，不出现在服务日志与事件中（§9.9）。
func assertKeyOnlyUpstream(t *testing.T, key, logs string, evs []api.Event, reqs []fakeupstream.Request) {
	t.Helper()
	b, err := json.Marshal(evs)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs, key) || strings.Contains(string(b), key) {
		t.Fatal("模型 Key 出现在服务日志或事件中")
	}
	for _, r := range reqs {
		if r.Authorization != "Bearer "+key {
			t.Fatalf("上游请求 #%d 的 Authorization 头不是配置的 Key", r.Seq)
		}
	}
}

func outcomes(reqs []fakeupstream.Request) []string {
	var out []string
	for _, r := range reqs {
		out = append(out, fmt.Sprintf("#%d %s → %s", r.N, r.Action, r.Outcome))
	}
	return out
}

// TestE17UpstreamFaults：E17——同一逻辑调用的上游依次返回 429（Retry-After: 1）、响应中途断开、挂起至调用期限
// → 共 3 次 try（累计上限）：429 按 Retry-After 退避后重试、预留释放；中途断开与超时的挂起都已发出、按 unknown
// 各计一次（估算转入 unknown，不重复计入）；不再新建第 4 次 try；I3 成立。
//
// 挂起排在最后：chat adapter 没有独立于调用期限的超时，挂起的 try 只能由 deadline_at 结束，期限之后不再新建 try。
func TestE17UpstreamFaults(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Status: http.StatusTooManyRequests, RetryAfter: "1"})
	fu.Inject(fakeupstream.Chat, 2, fakeupstream.Action{Disconnect: true})
	fu.Inject(fakeupstream.Chat, 3, fakeupstream.Action{Hang: true})
	key := gwSecret()
	const deadline = 8 * time.Second
	h.start(gatewayCfg(fu, key, call.Limits{ModelCallDeadline: deadline, BackoffBase: 50 * time.Millisecond}))
	id := h.submit("e17", spec(nil, "e17", nil, chatStep("s1")))
	v := h.waitTerminal(id)

	eventually(t, "挂起的第 3 个请求在调用期限时被 Gateway 放弃", func() bool {
		reqs := fu.Requests(fakeupstream.Chat)
		return len(reqs) == 3 && reqs[2].Outcome == fakeupstream.OutcomeAborted
	})
	reqs := fu.Requests(fakeupstream.Chat)
	if reqs[0].Outcome != fakeupstream.OutcomeStatus || reqs[1].Outcome != fakeupstream.OutcomeDisconnected || fu.Distinct(fakeupstream.Chat) != 1 {
		t.Fatalf("上游请求 %q，不同请求体 %d 个", outcomes(reqs), fu.Distinct(fakeupstream.Chat))
	}
	if gap := reqs[1].At.Sub(reqs[0].At); gap < time.Second {
		t.Fatalf("429 之后 %s 就重试，未遵从 Retry-After: 1", gap)
	}
	tries := gwTries(t, h.dsn, id, e2eCallID)
	if len(tries) != 3 ||
		tries[0].Outcome != "retryable" || tries[0].Error != upstream.CodeUpstreamRateLimited || tries[0].Reservation != "released" ||
		tries[1].Outcome != "unknown" || tries[1].Reservation != "charged_unknown" ||
		tries[2].Outcome != "unknown" || tries[2].Reservation != "charged_unknown" {
		t.Fatalf("tries %+v；期望 retryable/released、unknown/charged_unknown × 2", tries)
	}
	c := gwCallRow(t, h.dsn, id, e2eCallID)
	if c.State != "unknown" || c.Tries != 3 || !c.PED {
		t.Fatalf("调用 %+v；期望 unknown、tries_used 3、possible_external_duplicate", c)
	}
	reserved, spent, unknown := gwBudget(t, h.dsn, id)
	if reserved != 0 || spent != 0 || unknown != tries[1].Amount+tries[2].Amount {
		t.Fatalf("账本 reserved=%d spent=%d unknown=%d；期望 0、0、%d（每个 unknown try 的估算各计一次）",
			reserved, spent, unknown, tries[1].Amount+tries[2].Amount)
	}
	// Worker 得到 504 call_deadline_exceeded（不可重试的 Worker 失败）：任务结束，没有新的 attempt 与 try。
	if v.Status != "failed" || h.loadTask(id).AttemptsTotal != 1 || fu.Count(fakeupstream.Chat) != 3 {
		t.Fatalf("任务 %+v，attempts_total=%d，上游请求 %d 个", v, h.loadTask(id).AttemptsTotal, fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), reqs)
	t.Logf("E17：上游 %q；tries %+v；账本 reserved=%d spent=%d unknown=%d；任务 %s/%s",
		outcomes(reqs), tries, reserved, spent, unknown, v.Status, v.StatusReason)
	h.finish()
}

// TestE19AttemptReplacedDuringCall：E19——上游调用在途时杀死 Worker（新 attempt 取代旧 attempt）→ 旧 try 不被
// 取消，继续至完成并写入 journal；新 attempt 以同一调用 ID 得到 409 call_in_progress；完成后新 attempt 的 Worker
// 命中重放（tries_used 仍为 1、上游计数不增、它的 checkpoint refs 含同一结果 blob），任务成功。
func TestE19AttemptReplacedDuringCall(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	key := gwSecret()
	h.start(gatewayCfg(fu, key, call.Limits{}))
	id := h.submit("e19", spec(nil, "e19", nil, chatStep("s1"), checkpointStep("c1")))
	eventually(t, "调用到达上游并挂起", func() bool { return fu.Hanging() == 1 })
	r := h.rec(id)
	in1, _ := r.init(1)
	r.mu.Lock()
	kill := r.kills[1]
	r.mu.Unlock()
	kill() // 上游 try 在途时 Worker 崩溃：离开原因不是取消，try 继续（§9.1）

	var in2 protocol.Init
	eventually(t, "attempt 2 的 Worker 启动、Gateway socket 就绪", func() bool {
		var ok bool
		if in2, ok = r.init(2); !ok {
			return false
		}
		_, err := os.Stat(h.gwSocket(in2.AttemptID))
		return err == nil
	})
	// 旧 try 仍挂起在上游：attempt 2 的 Worker 在它完成之前无法结束（其同 ID 请求得到 call_in_progress 并重试），
	// 因此此时从测试侧发出的同 ID 请求所在的入口一定存活。
	got, err := gwDo(h.gwSocket(in2.AttemptID), http.MethodPost, "/v1/chat/completions", e2eCallID, chatBody(gwMessages))
	if err != nil || got.Status != http.StatusConflict || got.Code != "call_in_progress" {
		t.Fatalf("旧 try 在途时新 attempt 的同 ID 请求得到 %+v %v，期望 409 call_in_progress", got, err)
	}
	if _, err := os.Stat(h.gwSocket(in1.AttemptID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("旧 attempt 的 socket 应已撤销：%v", err)
	}

	// 放行旧 try：它结算并写入 journal；attempt 2 的 Worker（SDK 对 call_in_progress 以同一 ID 有界重试）随后
	// 命中重放，提交 checkpoint c1（refs 含结果 blob）并结束。此后不再从测试侧发请求：attempt 2 结束时入口被撤销。
	fu.Release()
	v := h.waitTerminal(id)
	c := gwCallRow(t, h.dsn, id, e2eCallID)
	tries := gwTries(t, h.dsn, id, e2eCallID)
	if v.Status != "succeeded" || len(tries) != 1 || tries[0].AttemptID != in1.AttemptID || tries[0].Outcome != "ok" ||
		tries[0].Reservation != "settled" || c.State != "completed" || c.FirstAttempt != in1.AttemptID || c.Tries != 1 {
		t.Fatalf("任务 %s；调用 %+v；tries %+v；期望旧 attempt 的唯一 try 以 ok 结算、调用 completed", v.Status, c, tries)
	}
	if fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("上游收到 %d 个请求，重放不应访问上游", fu.Count(fakeupstream.Chat))
	}
	in := h.inspect(id)
	a2 := attemptByNo(in, 2)
	if a1 := attemptByNo(in, 1); a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v", in.Attempts)
	}
	// inspect 的调用审计：completed、source upstream、1 个 try（旧 attempt 的），结果为同一 blob。
	var cv api.CallView
	for _, x := range in.Calls {
		if x.CallID == e2eCallID {
			cv = x
		}
	}
	if cv.State != "completed" || cv.Source != "upstream" || cv.TriesUsed != 1 || len(cv.Tries) != 1 ||
		cv.Tries[0].AttemptID != in1.AttemptID || cv.ResultRef != c.ResultRef {
		t.Fatalf("inspect 的调用 %+v", cv)
	}
	// 重放：attempt 2 的 Worker 得到同一结果 blob（它在 checkpoint c1 的 refs 中），而没有新 try。
	var refs []string
	var refsJSON string
	pgQueryRow(t, h.dsn, "SELECT refs_json::text FROM checkpoints WHERE attempt_id = $1 AND step_id = 'c1'", []any{a2.AttemptID}, &refsJSON)
	if err := json.Unmarshal([]byte(refsJSON), &refs); err != nil || !slices.Contains(refs, c.ResultRef) {
		t.Fatalf("attempt 2 的 checkpoint refs %s 应含结果 blob %s（%v）", refsJSON, c.ResultRef, err)
	}
	reserved, spent, unknown := gwBudget(t, h.dsn, id)
	if reserved != 0 || unknown != 0 || spent != tries[0].Cost {
		t.Fatalf("账本 reserved=%d spent=%d unknown=%d，try 费用 %d", reserved, spent, unknown, tries[0].Cost)
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E19：tries %+v（attempt 1 = %s）；新 attempt 409 后重放 blob %s；上游请求 %d 个；账本 spent=%d",
		tries, in1.AttemptID, c.ResultRef, fu.Count(fakeupstream.Chat), spent)
	h.finish()
}

// TestE20CancelDuringCall：E20——上游调用在途（请求已到达上游）时用户取消 → 取消立即撤销 Gateway 入口：
// 在途 try 被取消，已发出因此按 unknown 结算（估算转入 unknown）；旧 Worker 的现有连接被关闭，socket 删除，
// 之后任何端点（同 ID 重放、/v1/budget）的新连接都被拒绝。
func TestE20CancelDuringCall(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	key := gwSecret()
	h.start(gatewayCfg(fu, key, call.Limits{}))
	id := h.submit("e20", spec(nil, "e20", nil, chatStep("s1")))
	eventually(t, "调用到达上游并挂起", func() bool { return fu.Hanging() == 1 })
	in1, _ := h.rec(id).init(1)
	sock := h.gwSocket(in1.AttemptID)

	// 一条已建立的空闲连接：取消前 /v1/budget 可用，并看到在途 try 的预留。
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	req, err := http.NewRequest(http.MethodGet, "http://gateway/v1/budget", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close() // 正文已读完，关闭错误无关紧要
	var bud struct {
		ReservedMicro int64 `json:"reserved_micro"`
	}
	if err != nil || resp.StatusCode != http.StatusOK || json.Unmarshal(body, &bud) != nil || bud.ReservedMicro <= 0 {
		t.Fatalf("取消前 /v1/budget = %d %s %v", resp.StatusCode, body, err)
	}

	if st, code := h.cancelTask(id, "e20-cancel"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	// 现有连接被关闭：读到 EOF 或连接错误（期限只是失败判定）。
	if err := conn.SetReadDeadline(time.Now().Add(waitLimit)); err != nil {
		t.Fatal(err)
	}
	_, rerr := br.ReadByte()
	var ne net.Error
	if rerr == nil || (errors.As(rerr, &ne) && ne.Timeout()) {
		t.Fatalf("取消后旧连接仍然打开：%v", rerr)
	}
	eventually(t, "旧 attempt 的 socket 被删除", func() bool {
		_, err := os.Stat(sock)
		return errors.Is(err, fs.ErrNotExist)
	})
	for _, probe := range []struct{ method, path, callID, body string }{
		{http.MethodPost, "/v1/chat/completions", e2eCallID, chatBody(gwMessages)},
		{http.MethodGet, "/v1/budget", "", ""},
	} {
		if got, err := gwDo(sock, probe.method, probe.path, probe.callID, probe.body); err == nil {
			t.Fatalf("取消后 %s %s 的新连接应被拒绝，得到 %+v", probe.method, probe.path, got)
		}
	}

	eventually(t, "在途的上游请求被取消", func() bool {
		reqs := fu.Requests(fakeupstream.Chat)
		return len(reqs) == 1 && reqs[0].Outcome == fakeupstream.OutcomeAborted
	})
	v := h.waitTerminal(id)
	eventually(t, "在途 try 结算", func() bool {
		tr := gwTries(t, h.dsn, id, e2eCallID)
		return len(tr) == 1 && tr[0].Outcome != ""
	})
	tries := gwTries(t, h.dsn, id, e2eCallID)
	c := gwCallRow(t, h.dsn, id, e2eCallID)
	reserved, spent, unknown := gwBudget(t, h.dsn, id)
	if v.Status != "cancelled" || tries[0].Outcome != "unknown" || tries[0].Reservation != "charged_unknown" ||
		c.State != "unknown" || !c.PED || reserved != 0 || spent != 0 || unknown != tries[0].Amount {
		t.Fatalf("任务 %s；调用 %+v；tries %+v；账本 reserved=%d spent=%d unknown=%d；期望已发出的 try 按 unknown 结算",
			v.Status, c, tries, reserved, spent, unknown)
	}
	if fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("上游收到 %d 个请求", fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E20：取消前预留 %d；try %+v；账本 unknown=%d；旧连接 %v；上游 %q",
		bud.ReservedMicro, tries[0], unknown, rerr, outcomes(fu.Requests(fakeupstream.Chat)))
	h.finish()
}

// TestSearchViaFakeUpstream：搜索供应商 fake 配置了 SearchBaseURL 时，Worker 的搜索经 Gateway（验证 dialer，主机
// 显式放行）到达 fake upstream 的 /search：上游恰收到一次请求（不带 Key），Worker 得到上游给出的结果（URL 指向
// fake upstream 的页面），调用以 ok 结算、结果授权到任务 scope。
func TestSearchViaFakeUpstream(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	h.start(gatewayCfg(fu, gwSecret(), call.Limits{}))
	id := h.submit("search", spec(nil, "search", nil,
		step{"op": "search", "step_id": "q1", "query": "固态电池", "max_results": 3}))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	const callID = "root/q1/search/1"
	c := gwCallRow(t, h.dsn, id, callID)
	tries := gwTries(t, h.dsn, id, callID)
	reqs := fu.Requests(fakeupstream.Search)
	if c.State != "completed" || len(tries) != 1 || tries[0].Outcome != "ok" || len(reqs) != 1 || reqs[0].Authorization != "" {
		t.Fatalf("调用 %+v；tries %+v；上游搜索请求 %+v", c, tries, reqs)
	}
	var res struct {
		Results []struct {
			URL     string `json:"url"`
			Snippet string `json:"snippet"`
		} `json:"results"`
	}
	if err := json.Unmarshal(h.readBlob(c.ResultRef), &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 3 || !strings.HasPrefix(res.Results[0].URL, fu.URL()+"/pages/") || res.Results[0].Snippet == "" {
		t.Fatalf("搜索结果 %+v，期望来自 fake upstream 的 3 条", res.Results)
	}
	t.Logf("搜索经 fake upstream：上游请求 %d 个；结果 %+v", fu.Count(fakeupstream.Search), res.Results)
	h.finish()
}

// currentAttempt 返回任务的当前 attempt（没有时为空）。
func (s *sysHarness) currentAttempt(taskID string) string {
	s.t.Helper()
	var a *string
	pgQueryRow(s.t, s.dsn, "SELECT current_attempt_id FROM tasks WHERE task_id = $1", []any{taskID}, &a)
	if a == nil {
		return ""
	}
	return *a
}

// waitServerConnsGone 等待被杀死的 server 的数据库连接全部结束：它已发出的 COMMIT 仍可能完成，之后的事实才稳定。
func (s *sysHarness) waitServerConnsGone() {
	s.t.Helper()
	eventuallyWithin(s.t, "上一 server 的数据库连接全部结束", waitLimit, 20*time.Millisecond, func() bool {
		var n int
		pgQueryRow(s.t, s.dsn, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND pid <> pg_backend_pid() AND application_name <> $1`, []any{testAppName}, &n)
		return n == 0
	})
}

// killAtCallInFlight 以 faultinject 点 call.in_flight 启动 server、提交一个 chat 任务，等待 server 在调用 A
// （Worker 的 root/s1/chat/1）的上游 try 返回之后、结算之前被 SIGKILL，并等待其数据库连接全部结束。
// before 非空时调用方须让 A 在上游挂起：本函数等 A 到达上游后执行 before，再放行 A；为空时 A 直接返回。
func (s *sysHarness) killAtCallInFlight(fu *fakeupstream.Server, flags []string, before func(id string)) (string, *serverProc) {
	s.t.Helper()
	first := s.start(faultinject.CallInFlight+":1", flags...)
	id := s.submit("e48", spec(nil, "e48", nil, chatStep("s1")))
	if before != nil {
		eventually(s.t, "调用 A 到达上游并挂起", func() bool { return fu.Hanging() == 1 })
		before(id)
		fu.Release() // A 的上游请求返回 → 结算之前到达 call.in_flight → SIGKILL
	}
	ps := s.waitExit(first)
	if !killedBySIGKILL(ps) || !strings.Contains(first.logs.tail(1<<20), "faultinject: SIGKILL at "+faultinject.CallInFlight+":1") {
		s.t.Fatalf("server 应在 %s 被 SIGKILL，退出状态 %v", faultinject.CallInFlight, ps)
	}
	s.waitServerConnsGone()
	return id, first
}

// assertRestartConverted：重启后的账本转换把被杀死时在途的 try 按 unknown 结算（server_restart）、预留转入 unknown，
// 调用 A 不再是 in_flight；deadline_at 与重启前相同。
func assertRestartConverted(t *testing.T, s *sysHarness, id string, a0 gwCall) gwTry {
	t.Helper()
	tries := gwTries(t, s.dsn, id, e2eCallID)
	if len(tries) == 0 || tries[0].Outcome != "unknown" || tries[0].Error != "server_restart" || tries[0].Reservation != "charged_unknown" {
		t.Fatalf("重启后 try 1 应按 unknown（server_restart）结算：%+v", tries)
	}
	if a := gwCallRow(t, s.dsn, id, e2eCallID); a.State == "in_flight" || !a.PED || !a.Deadline.Equal(a0.Deadline) {
		t.Fatalf("重启后调用 A %+v；期望不再 in_flight、possible_external_duplicate、deadline_at 不变（%s）", a, a0.Deadline)
	}
	if !strings.Contains(s.srv.logs.tail(1<<30), `"msg":"账本转换","reservations":1,"calls":1`) {
		t.Fatal("重启后的 server 日志缺少账本转换的计数（1 笔预留、1 个调用）")
	}
	return tries[0]
}

// TestE48ServerKilledDuringCall：E48——调用进行中 SIGKILL server（faultinject 点 call.in_flight：Tx2 已提交、
// 上游请求已发出并返回、尚未结算；fake upstream 确认收到请求），等两个调用都超过 deadline_at 后重启：
//   - 启动账本转换（§14.1 第 4 步）把在途调用 A 的 try 按 unknown 结算、held 预留转入 unknown；deadline_at 不变；
//   - 遗留的 resolving 调用 B（同一 attempt 的另一调用，因每任务在途上限 1 等待槽位）被复位为可重新解析，期限不变；
//   - 新 attempt 的 Worker 以同 ID 请求 A 得到 504 call_deadline_exceeded（不再访问上游），任务失败。
//
// 复位调用在期限之后被接管时得到 call_deadline_exceeded 由 TestResetResolving（存储层）覆盖：新 attempt 的 Worker
// 收到 504 后立即失败，测试无法确定地在它结束之前另行连接该 attempt 的 socket。
func TestE48ServerKilledDuringCall(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	s := newSys(t)
	key := gwSecret()
	t.Setenv(modelKeyEnv, key)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	const deadline = 15 * time.Second
	flags := []string{"--fake-upstream", fu.URL(), "--model-call-deadline", deadline.String(), "--gateway-per-task-inflight", "1"}
	bDone := make(chan error, 1)
	id, first := s.killAtCallInFlight(fu, flags, func(id string) {
		// 调用 B：同一 attempt 的另一调用，等待 A 占用的每任务在途槽位，停在 resolving（Tx1 已提交、没有 try）。
		sock := s.gwSocket(s.currentAttempt(id))
		go func() {
			_, err := gwDo(sock, http.MethodPost, "/v1/chat/completions", probeCallID, chatBody(probeMessages))
			bDone <- err
		}()
		eventually(t, "调用 B 登记为 resolving 并等待槽位", func() bool {
			var n int
			pgQueryRow(t, s.dsn, `SELECT count(*) FROM calls WHERE task_id = $1 AND call_id = $2 AND state = 'resolving'
				AND resolving_since IS NOT NULL`, []any{id, probeCallID}, &n)
			return n == 1
		})
	})
	select {
	case err := <-bDone:
		if err == nil {
			t.Fatal("调用 B 不应在 server 被杀死之前得到响应")
		}
	case <-time.After(waitLimit):
		t.Fatal("server 被杀死后调用 B 的连接未结束")
	}
	a0, b0 := gwCallRow(t, s.dsn, id, e2eCallID), gwCallRow(t, s.dsn, id, probeCallID)
	tries0 := gwTries(t, s.dsn, id, e2eCallID)
	reqs := fu.Requests(fakeupstream.Chat)
	if a0.State != "in_flight" || a0.Tries != 1 || len(tries0) != 1 || tries0[0].Reservation != "held" ||
		b0.State != "resolving" || b0.ResolvingSince == nil || b0.Tries != 0 ||
		len(reqs) != 1 || reqs[0].Outcome != fakeupstream.OutcomeOK {
		t.Fatalf("被杀死时：A %+v tries %+v；B %+v；上游 %q；期望 A in_flight（已发出并返回、未结算）、B resolving",
			a0, tries0, b0, outcomes(reqs))
	}

	// 两个调用都超过 deadline_at（按数据库时间）之后再重启：新 attempt 的同 ID 请求必然在期限之后到达。
	eventuallyWithin(t, "两个调用都超过 deadline_at", deadline+waitLimit, 100*time.Millisecond, func() bool {
		var n int
		pgQueryRow(t, s.dsn, "SELECT count(*) FROM calls WHERE task_id = $1 AND now() >= deadline_at", []any{id}, &n)
		return n == 2
	})
	s.start("", flags...)
	try1 := assertRestartConverted(t, s, id, a0)
	if b1 := gwCallRow(t, s.dsn, id, probeCallID); b1.State != "resolving" || b1.ResolvingSince != nil || !b1.Deadline.Equal(b0.Deadline) {
		t.Fatalf("遗留的 resolving 调用应被复位（resolving_since 清空、期限不变）：%+v，原为 %+v", b1, b0)
	}

	v := s.waitTerminal(id)
	in := s.inspect(id)
	a2 := attemptByNo(in, 2)
	var got504 bool
	for _, e := range s.events(id) {
		got504 = got504 || (e.AttemptID == a2.AttemptID && e.Source == "worker" && strings.Contains(string(e.Payload), "call_deadline_exceeded"))
	}
	a := gwCallRow(t, s.dsn, id, e2eCallID)
	reserved, spent, unknown := gwBudget(t, s.dsn, id)
	if v.Status != "failed" || !got504 || a.State != "unknown" || a.Tries != 1 || !a.Deadline.Equal(a0.Deadline) ||
		reserved != 0 || spent != 0 || unknown != try1.Amount || fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("任务 %s（attempt 2 的 Worker 收到 504：%v）；调用 A %+v；账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个",
			v.Status, got504, a, reserved, spent, unknown, fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, first.logs.tail(1<<30)+s.srv.logs.tail(1<<30), s.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E48（超期后重启）：deadline_at A=%s B=%s（重启前后相同）；try 1 %+v；新 attempt 的同 ID 请求 → 504；任务 %s/%s；"+
		"账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个",
		a0.Deadline.Format(time.RFC3339Nano), b0.Deadline.Format(time.RFC3339Nano), try1, v.Status, v.StatusReason,
		reserved, spent, unknown, fu.Count(fakeupstream.Chat))
	s.finish()
}

// TestE48RestartBeforeDeadline：E48 的另一半——期限之内重启：账本转换把在途 try 按 unknown 结算后，新 attempt
// 的同 ID 请求在累计上限内新建 try 2（上游第二次收到同一请求体），调用完成，任务成功；deadline_at 不变；
// 账本 unknown = try 1 的估算、spent = try 2 的实际费用。
func TestE48RestartBeforeDeadline(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	s := newSys(t)
	key := gwSecret()
	t.Setenv(modelKeyEnv, key)
	flags := []string{"--fake-upstream", fu.URL()}
	id, first := s.killAtCallInFlight(fu, flags, nil)
	a0 := gwCallRow(t, s.dsn, id, e2eCallID)
	if a0.State != "in_flight" || fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("被杀死时调用 A %+v，上游请求 %d 个", a0, fu.Count(fakeupstream.Chat))
	}
	s.start("", flags...)
	try1 := assertRestartConverted(t, s, id, a0)
	v := s.waitTerminal(id)
	in := s.inspect(id)
	tries := gwTries(t, s.dsn, id, e2eCallID)
	a := gwCallRow(t, s.dsn, id, e2eCallID)
	reserved, spent, unknown := gwBudget(t, s.dsn, id)
	if v.Status != "succeeded" || len(tries) != 2 || tries[1].Outcome != "ok" || tries[1].Reservation != "settled" ||
		tries[0].AttemptID != attemptByNo(in, 1).AttemptID || tries[1].AttemptID != attemptByNo(in, 2).AttemptID ||
		a.State != "completed" || a.Tries != 2 || !a.PED || !a.Deadline.Equal(a0.Deadline) ||
		reserved != 0 || unknown != try1.Amount || spent != tries[1].Cost ||
		fu.Count(fakeupstream.Chat) != 2 || fu.Distinct(fakeupstream.Chat) != 1 {
		t.Fatalf("任务 %s；调用 %+v；tries %+v；账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个（不同请求体 %d）",
			v.Status, a, tries, reserved, spent, unknown, fu.Count(fakeupstream.Chat), fu.Distinct(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, first.logs.tail(1<<30)+s.srv.logs.tail(1<<30), s.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E48（期限内重启）：deadline_at %s 不变；tries %+v；账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个",
		a0.Deadline.Format(time.RFC3339Nano), tries, reserved, spent, unknown, fu.Count(fakeupstream.Chat))
	s.finish()
}

// TestE11bReservationCommitLost：E11b——ReserveTry 的 COMMIT 已执行而回复丢失（faultinject 点 reservation.commit，
// 经同一 reservation_id 重跑事务体，与提交结果未知后的重跑相同）→ 解析为原 try：恰一笔预留、一个 try，
// 上游只收到一次请求，调用正常完成；账本与 I3 一致。
func TestE11bReservationCommitLost(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	s := newSys(t)
	key := gwSecret()
	t.Setenv(modelKeyEnv, key)
	p := s.start(faultinject.ReservationCommit+":1", "--fake-upstream", fu.URL())
	id := s.submit("e11b", spec(nil, "e11b", nil, chatStep("s1")))
	v := s.waitTerminal(id)
	if v.Status != "succeeded" || !strings.Contains(p.logs.tail(1<<30), "faultinject: reply lost at "+faultinject.ReservationCommit+":1") {
		t.Fatalf("任务 %+v；server 日志应记录 %s 的回复丢失", v, faultinject.ReservationCommit)
	}
	tries := gwTries(t, s.dsn, id, e2eCallID)
	c := gwCallRow(t, s.dsn, id, e2eCallID)
	var reservations int
	pgQueryRow(t, s.dsn, "SELECT count(*) FROM reservations WHERE task_id = $1", []any{id}, &reservations)
	reserved, spent, unknown := gwBudget(t, s.dsn, id)
	if reservations != 1 || len(tries) != 1 || tries[0].Outcome != "ok" || tries[0].Reservation != "settled" ||
		c.State != "completed" || c.Tries != 1 || reserved != 0 || unknown != 0 || spent != tries[0].Cost {
		t.Fatalf("预留 %d 笔；tries %+v；调用 %+v；账本 reserved=%d spent=%d unknown=%d；期望恰一笔预留与一个 ok try",
			reservations, tries, c, reserved, spent, unknown)
	}
	if n := fu.Count(fakeupstream.Chat); n != 1 {
		t.Fatalf("上游收到 %d 个请求，期望 1", n)
	}
	assertKeyOnlyUpstream(t, key, p.logs.tail(1<<30), s.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E11b：预留 %d 笔；try %+v；账本 spent=%d；上游请求 %d 个", reservations, tries[0], spent, fu.Count(fakeupstream.Chat))
	s.finish()
}

// ---- M2：Gateway 真实沙箱冒烟与 G3（Plan 8 Task 4）----
//
// 真实沙箱（provider/local + 生产启动器，newRealHarness）中运行 sim_worker：chat 经沙箱内的
// /run/agentbox/gateway.sock 到达进程内 Gateway，再到 fake upstream。Worker 暂停在 checkpoint 上（Hold）时，
// 测试从宿主读取环境 cgroup 中各进程，并在同一环境中另起一个 workload 进程（与 E3 相同的 provider StartExec，
// uid 1000、同一 namespace）从沙箱内部观察。sim_worker 的 print 操作只能输出固定文本，无法输出环境，因此
// 沙箱内的 env 与 /proc/<pid>/environ 由该进程输出（G3 见 TestRealNoCredentialsInSandbox）。

const searchKeyEnv = "AGENTBOX_SEARCH_API_KEY" // 宿主上搜索供应商 Key 的环境变量（cmd/agentbox 只从这里读取）

// sandboxProcs 返回环境 cgroup 中的全部进程：宿主 pid → cmdline（NUL 换成空格）。
func (h *harness) sandboxProcs(envID string) map[int]string {
	out := map[int]string{}
	for _, pid := range cgroupPids(h.envCgroup(envID)) {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
			out[pid] = strings.TrimSpace(string(bytes.ReplaceAll(b, []byte{0}, []byte{' '})))
		}
	}
	return out
}

// workerPid 返回环境中 Worker 主进程（python3 -m <module>）的宿主 pid。
func (h *harness) workerPid(envID, module string) int {
	h.t.Helper()
	for pid, cmd := range h.sandboxProcs(envID) {
		if strings.Contains(cmd, "-m "+module) {
			return pid
		}
	}
	h.t.Fatalf("环境 %s 中没有 %s 进程：%v", envID, module, h.sandboxProcs(envID))
	return 0
}

// procUID 返回宿主进程 pid 的真实 uid（/proc/<pid>/status 的 Uid 行）。
func procUID(t *testing.T, pid int) uint32 {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "Uid:" {
			n, err := strconv.ParseUint(f[1], 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			return uint32(n)
		}
	}
	t.Fatalf("/proc/%d/status 没有 Uid 行", pid)
	return 0
}

// sandboxExec 在环境中以 workload 身份（与 Worker 相同的沙箱）运行 python3 -c script，返回 stdout；
// 退出状态非 0 时失败。
func (h *harness) sandboxExec(envID, execID string, env []string, script string) string {
	h.t.Helper()
	hd, err := h.local.StartExec(context.Background(), envID, provider.ExecSpec{ExecID: execID, Dir: "/tmp", Env: env,
		Argv: []string{"python3", "-c", script}})
	if err != nil {
		h.t.Fatalf("在环境中启动 %s: %v", execID, err)
	}
	if err := hd.Stdin().Close(); err != nil {
		h.t.Fatal(err)
	}
	var stdout, stderr lockedBuffer
	done := make(chan struct{}, 2)
	for _, p := range []struct {
		dst *lockedBuffer
		src io.Reader
	}{{&stdout, hd.Stdout()}, {&stderr, hd.Stderr()}} {
		go func() {
			_, _ = io.Copy(p.dst, p.src) // 读到 EOF 或管道关闭为止；内容由下方断言检查
			done <- struct{}{}
		}()
	}
	st, err := hd.Wait()
	for range 2 {
		select {
		case <-done:
		case <-time.After(waitLimit):
			h.t.Fatalf("%s 的输出未结束", execID)
		}
	}
	if err != nil || st.Code != 0 || st.Signal != 0 {
		h.t.Fatalf("%s 退出 %+v %v；stderr：\n%s", execID, st, err, stderr.tail(4<<10))
	}
	return stdout.tail(1 << 30)
}

// checkpointRefs 返回任务 scope 中 step_id 的 checkpoint 的 refs。
func (h *harness) checkpointRefs(taskID, stepID string) []string {
	h.t.Helper()
	var raw []byte
	h.queryRow("SELECT refs_json FROM checkpoints WHERE scope_kind = 'task' AND scope_id = $1 AND step_id = $2",
		[]any{taskID, stepID}, &raw)
	var refs []string
	if err := json.Unmarshal(raw, &refs); err != nil {
		h.t.Fatalf("checkpoint %s 的 refs_json %s: %v", stepID, raw, err)
	}
	return refs
}

// assertNoSecret：text 中不含任何 secret（只报告位置，不在失败信息中回显 Key）。
func assertNoSecret(t *testing.T, where, text string, secrets ...string) {
	t.Helper()
	for i, s := range secrets {
		if s == "" {
			t.Fatalf("第 %d 个 Key 为空：G3 须在配置了非空 Key 时验证", i+1)
		}
		if strings.Contains(text, s) {
			t.Fatalf("第 %d 个 Key 出现在%s中", i+1, where)
		}
	}
}

// TestRealGatewayChat：Gateway 真实沙箱冒烟——sim_worker 的 chat 经沙箱内 /run/agentbox/gateway.sock → Gateway →
// fake upstream 返回脚本化回复 → checkpoint 的 refs 带该结果 blob → result。socket 在宿主上属主为 Worker 的映射
// uid、0600，沙箱内为 uid 1000、0600 的 socket；inspect 显示 1 个 completed 调用、1 次 ok try。
func TestRealGatewayChat(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	key := gwSecret()
	h.start(gatewayCfg(fu, key, call.Limits{}))
	id := h.submit("gw-chat", spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, "gw chat", nil,
		chatStep("s1"), checkpointStep("c1"), progressStep("after chat")))
	h.waitHeld(id)
	in1, _ := h.rec(id).init(1)
	envID := attemptByNo(h.inspect(id), 1).EnvID
	wuid := procUID(t, h.workerPid(envID, "sim_worker"))
	fi, err := os.Lstat(h.gwSocket(in1.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || fi.Mode().Type() != fs.ModeSocket || fi.Mode().Perm() != 0o600 || st.Uid != wuid || wuid == 0 {
		t.Fatalf("宿主上的 Gateway socket mode %v uid %v；期望 Worker 的映射 uid %d、0600 的 socket", fi.Mode(), fi.Sys(), wuid)
	}
	inside := strings.TrimSpace(h.sandboxExec(envID, "gw-stat", nil,
		"import os, stat\ns = os.stat('/run/agentbox/gateway.sock')\n"+
			"print(os.getuid(), s.st_uid, oct(stat.S_IMODE(s.st_mode)), stat.S_ISSOCK(s.st_mode))"))
	if inside != "1000 1000 0o600 True" {
		t.Fatalf("沙箱内 /run/agentbox/gateway.sock：%q；期望 uid 1000 的进程看到属主 1000、0600 的 socket", inside)
	}
	h.rec(id).releaseHold()

	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	if len(in.Calls) != 1 {
		t.Fatalf("inspect 中有 %d 个调用：%+v", len(in.Calls), in.Calls)
	}
	c := in.Calls[0]
	if c.CallID != e2eCallID || c.Endpoint != "/v1/chat/completions" || c.State != "completed" || c.TriesUsed != 1 ||
		len(c.Tries) != 1 || c.Tries[0].Outcome != "ok" || c.Tries[0].AttemptID != in1.AttemptID || c.ResultRef == "" {
		t.Fatalf("调用 %+v；期望 1 个 completed 调用、1 次 ok try", c)
	}
	if refs := h.checkpointRefs(id, "c1"); !slices.Contains(refs, c.ResultRef) {
		t.Fatalf("checkpoint c1 的 refs %v 不含结果 blob %s", refs, c.ResultRef)
	}
	var reply fakeupstream.ChatReply
	if err := json.Unmarshal(h.readBlob(c.ResultRef), &reply); err != nil {
		t.Fatal(err)
	}
	if len(reply.Choices) != 1 || !strings.Contains(reply.Choices[0].Message.Content, "fake 任务") {
		t.Fatalf("结果 blob 中的回复 %+v，期望 fake upstream 的计划阶段脚本回复", reply)
	}
	if fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("上游收到 %d 个 chat 请求", fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("Gateway 冒烟：宿主 socket uid %d 0600，沙箱内 %q；调用 %s → %s（try %+v），blob %s 在 checkpoint c1 的 refs 中",
		wuid, inside, c.CallID, c.State, c.Tries[0], c.ResultRef)
	h.finish()
}

// g3Script 在沙箱内输出自身的环境（os.environ 即 env）、/proc/self/environ，以及沙箱内可见的每个进程的
// cmdline 与 /proc/<pid>/environ（Worker 与该进程同为 uid 1000，可读；不可读的记录错误）。
const g3Script = `import os
print("ENV", sorted(os.environ.items()))
print("SELF", open("/proc/self/environ", "rb").read().split(b"\0"))
for p in sorted(os.listdir("/proc")):
    if not p.isdigit():
        continue
    try:
        cmd = open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
        env = open(f"/proc/{p}/environ", "rb").read().split(b"\0")
        print("PROC", p, "[" + cmd + "]", len(env), env)
    except OSError as e:
        print("PROC", p, "unreadable", e.errno)
`

// TestRealNoCredentialsInSandbox：G3（§9.9）——配置了非空的模型与搜索 Key（宿主环境变量 AGENTBOX_MODEL_API_KEY、
// AGENTBOX_SEARCH_API_KEY 也设为同一值，与 cmd/agentbox 的来源相同；Worker 启动器继承本进程环境时会泄漏）时，
// Worker 经 Gateway 完成 chat 与搜索，而：
//   - 环境 cgroup 中每个进程（init、helper、Worker）的 /proc/<pid>/environ 与 cmdline（宿主读取）不含 Key 的值，
//     也没有这两个变量名；
//   - 沙箱内 uid 1000 进程输出的 env、/proc/self/environ 与可见进程（含 Worker）的 /proc/<pid>/environ 不含 Key；
//   - server 日志与整个事件表（payload）不含 Key；Key 只出现在发往模型上游的 Authorization 头中。
func TestRealNoCredentialsInSandbox(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	modelKey, searchKey := gwSecret(), gwSecret()
	t.Setenv(modelKeyEnv, modelKey)
	t.Setenv(searchKeyEnv, searchKey)
	cfg := gatewayCfg(fu, modelKey, call.Limits{})
	cfg.SearchAPIKey = searchKey
	h.start(cfg)
	id := h.submit("g3", spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, "g3", nil,
		chatStep("s1"),
		step{"op": "search", "step_id": "q1", "query": "固态电池", "max_results": 3},
		step{"op": "print", "message": "g3: 已完成 chat 与搜索"},
		checkpointStep("c1")))
	h.waitHeld(id)
	envID := attemptByNo(h.inspect(id), 1).EnvID
	wpid := h.workerPid(envID, "sim_worker")
	procs := h.sandboxProcs(envID)
	var workerEnv []string
	for pid, cmd := range procs {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if err != nil {
			t.Fatalf("读取沙箱进程 %d（%s）的 environ: %v", pid, cmd, err)
		}
		where := fmt.Sprintf("沙箱进程 %d（%s）的 environ 或 cmdline", pid, cmd)
		assertNoSecret(t, where, string(b)+"\n"+cmd, modelKey, searchKey)
		if bytes.Contains(b, []byte(modelKeyEnv)) || bytes.Contains(b, []byte(searchKeyEnv)) {
			t.Fatalf("%s 含 Key 的变量名", where)
		}
		if pid == wpid {
			workerEnv = strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
		}
	}
	// 沙箱内部：以 Worker 的环境运行观察进程（它看到的就是沙箱给 workload 的全部环境）。
	out := h.sandboxExec(envID, "g3-env", workerEnv, g3Script)
	assertNoSecret(t, "沙箱内的 env、/proc/self/environ 或进程 environ", out, modelKey, searchKey)
	var sawWorker bool
	for _, line := range strings.Split(out, "\n") {
		sawWorker = sawWorker || (strings.HasPrefix(line, "PROC ") && strings.Contains(line, "-m sim_worker]") &&
			strings.Contains(line, "PYTHONPATH="))
	}
	if !strings.HasPrefix(out, "ENV ") || !strings.Contains(out, "\nSELF ") || !sawWorker {
		t.Fatalf("沙箱内的观察输出缺少 env、/proc/self/environ 或 Worker 的 environ：\n%s", out)
	}
	h.rec(id).releaseHold()

	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	if len(in.Calls) != 2 || in.Calls[0].State != "completed" || in.Calls[1].State != "completed" ||
		fu.Count(fakeupstream.Chat) != 1 || fu.Count(fakeupstream.Search) != 1 {
		t.Fatalf("调用 %+v；上游 chat %d、search %d 个请求", in.Calls, fu.Count(fakeupstream.Chat), fu.Count(fakeupstream.Search))
	}
	assertNoSecret(t, "server 日志", h.logs.tail(1<<30), modelKey, searchKey)
	var leaked, total int
	for _, k := range []string{modelKey, searchKey} {
		var n int
		h.queryRow("SELECT count(*) FROM events WHERE strpos(payload::text, $1) > 0", []any{k}, &n)
		leaked += n
	}
	h.queryRow("SELECT count(*) FROM events", nil, &total)
	if leaked != 0 || total == 0 {
		t.Fatalf("事件表 %d 行中有 %d 行含 Key", total, leaked)
	}
	assertKeyOnlyUpstream(t, modelKey, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("G3：沙箱进程 %d 个（%v）的 environ 与 cmdline、沙箱内观察输出 %d 字节、server 日志 %d 字节、事件表 %d 行均不含 Key；"+
		"Worker 环境 %v", len(procs), slices.Collect(maps.Values(procs)), len(out), len(h.logs.tail(1<<30)), total, workerEnv)
	h.finish()
}

// ---- M2：DeepResearch 业务验收（fake upstream，真实沙箱；Plan 8 Task 5，§16.4 自动化业务验收）----
//
// 真实沙箱中运行 python3 -m deepresearch，fake upstream 按阶段标记回复固定研究题目（2 个任务、每次搜索 3 条
// 结果、固定页面）。断言研究步骤（checkpoint plan、task-1、task-2、report）、report 产物、报告引用 ↔ 已保存的
// 证据 blob，以及杀死 Worker 后从已提交的 checkpoint 恢复且已完成的调用不再到达上游。每条用例以 finish（静止时
// 的不变量检查，即 verify-invariants --quiescent 的同一检查，含 I3、I14）结束。

const researchTopic = "固态电池的产业化进展"

var researchTasks = []fakeupstream.ResearchTask{
	{Title: "电解质路线", Intent: "比较硫化物、氧化物与聚合物电解质", Query: "固态电解质 技术路线"},
	{Title: "量产与成本", Intent: "梳理量产时间表与成本结构", Query: "固态电池 量产 成本"},
}

// researchMaxResults 是研究配置的 max_results（搜索请求体的一部分；直接重放探针须发送同一请求体）。
const researchMaxResults = 3

// researchCfg 是 deepresearch Worker 的 Gateway 配置（WorkerArgv 为 python3 -m deepresearch）。
func researchCfg(fu *fakeupstream.Server) app.Config {
	fu.SetResearch(researchTasks)
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.WorkerArgv = []string{"python3", "-m", "deepresearch"}
	return cfg
}

func researchSpec(d *directives) string {
	m := map[string]any{"topic": researchTopic, "max_tasks": 4, "max_results": researchMaxResults, "max_fetch": 3}
	if d != nil {
		m["e2e"] = d
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// upstreamCounts 是 fake upstream 的计数快照：阶段（plan、summarize、report）与类别（search、fetch）。
type upstreamCounts struct{ Plan, Summarize, Report, Search, Fetch int }

func countsOf(fu *fakeupstream.Server) upstreamCounts {
	return upstreamCounts{Plan: fu.StageCount(fakeupstream.StagePlan), Summarize: fu.StageCount(fakeupstream.StageSummarize),
		Report: fu.StageCount(fakeupstream.StageReport), Search: fu.Count(fakeupstream.Search), Fetch: fu.Count(fakeupstream.Fetch)}
}

// 完整研究的上游请求：1 次计划、每个任务 1 次搜索 + 3 次抓取 + 1 次摘要、1 次报告。
var researchFull = upstreamCounts{Plan: 1, Summarize: 2, Report: 1, Search: 2, Fetch: 6}

func stepOrder(in api.Inspection) (steps, attempts []string) {
	cps := slices.Clone(in.Checkpoints)
	slices.SortFunc(cps, func(a, b api.CheckpointView) int { return int(a.CommitSeq - b.CommitSeq) })
	for _, c := range cps {
		steps = append(steps, c.StepID)
		attempts = append(attempts, c.AttemptID)
	}
	return steps, attempts
}

// citation 是报告"证据"列表中的一项：[N] → 证据 blob 的 sha256 与来源 URL。
type citation struct {
	N        int
	URL, SHA string
}

var (
	evidenceLine = regexp.MustCompile(`^- \[(\d+)\] .+ — (\S+) — sha256:([0-9a-f]{64})$`)
	citeRef      = regexp.MustCompile(`\[(\d+)\]`)
)

// assertResearchReport：report 产物已保存（事件 artifact_saved(report) 与最新版本一致）；报告非空，正文中的每个 [n]
// 都在末尾"证据"列表中；列表恰为全部 6 条证据（编号 1..6、URL 是固定题目的搜索结果），每个 sha 的 blob 存在
// （BlobStore 内容哈希一致、blobs 表有记录）、授权到任务 scope（scope_blobs）、是抓取该 URL 的结果，并在 report
// checkpoint 的 refs 中。返回列表与正文中引用的编号。
func (h *harness) assertResearchReport(fu *fakeupstream.Server, taskID string) ([]citation, []int) {
	t := h.t
	t.Helper()
	av, err := h.store.LatestArtifact(context.Background(), taskID, "report")
	if err != nil {
		t.Fatalf("report 产物: %v", err)
	}
	var saved bool
	for _, e := range h.events(taskID) {
		var p struct {
			ArtifactID string `json:"artifact_id"`
			SHA256     string `json:"sha256"`
		}
		if e.Type == "artifact_saved" && json.Unmarshal(e.Payload, &p) == nil && p.ArtifactID == "report" && p.SHA256 == av.SHA256 {
			saved = true
		}
	}
	if !saved {
		t.Fatalf("事件中没有 artifact_saved(report, %s)", av.SHA256)
	}
	report := string(h.readBlob(av.SHA256))
	body, list, ok := strings.Cut(report, "\n## 证据\n")
	if !ok || !strings.HasPrefix(report, "# "+researchTopic+"\n") || strings.Contains(body, "参考来源") {
		t.Fatalf("报告结构不符（标题、证据节、模型自写的来源节应被替换）：\n%s", report)
	}
	urls := map[string]bool{}
	for _, task := range researchTasks {
		for i := 1; i <= 3; i++ {
			urls[fu.ResultURL(task.Query, i)] = true
		}
	}
	refs := h.checkpointRefs(taskID, "report")
	var cites []citation
	listed := map[int]bool{}
	for _, line := range strings.Split(strings.TrimSpace(list), "\n") {
		m := evidenceLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("证据列表行 %q 不符合 `- [n] 标题 — URL — sha256:<sha>`", line)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatal(err)
		}
		c := citation{N: n, URL: m[2], SHA: m[3]}
		if c.N != len(cites)+1 || !urls[c.URL] {
			t.Fatalf("证据 %+v：编号应连续、URL 应是固定题目的搜索结果", c)
		}
		var fetched struct {
			URL     string `json:"url"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(h.readBlob(c.SHA), &fetched); err != nil {
			t.Fatalf("证据 [%d] 的 blob 不是抓取结果: %v", c.N, err)
		}
		var inBlobs, inScope int
		h.queryRow("SELECT count(*) FROM blobs WHERE sha256 = $1", []any{c.SHA}, &inBlobs)
		h.queryRow("SELECT count(*) FROM scope_blobs WHERE scope_kind = 'task' AND scope_id = $1 AND sha256 = $2",
			[]any{taskID, c.SHA}, &inScope)
		if fetched.URL != c.URL || !strings.Contains(fetched.Content, c.URL) || inBlobs != 1 || inScope != 1 || !slices.Contains(refs, c.SHA) {
			t.Fatalf("证据 [%d] %s：抓取 URL %q、blobs %d、scope_blobs %d、在 report refs 中 %v",
				c.N, c.SHA, fetched.URL, inBlobs, inScope, slices.Contains(refs, c.SHA))
		}
		listed[c.N] = true
		cites = append(cites, c)
	}
	if len(cites) != 3*len(researchTasks) {
		t.Fatalf("证据列表 %d 条，期望 %d", len(cites), 3*len(researchTasks))
	}
	var cited []int
	for _, m := range citeRef.FindAllStringSubmatch(body, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatal(err)
		}
		if !listed[n] {
			t.Fatalf("正文引用 [%d] 不在证据列表中：\n%s", n, report)
		}
		cited = append(cited, n)
	}
	if len(cited) == 0 {
		t.Fatalf("报告正文没有引用：\n%s", report)
	}
	return cites, cited
}

// assertCallsSettled：全部调用 completed，每个恰 1 次 ok try；返回 call_id → 调用。
func assertCallsSettled(t *testing.T, in api.Inspection, want int) map[string]api.CallView {
	t.Helper()
	calls := map[string]api.CallView{}
	for _, c := range in.Calls {
		if c.State != "completed" || c.TriesUsed != 1 || len(c.Tries) != 1 || c.Tries[0].Outcome != "ok" || c.ResultRef == "" {
			t.Fatalf("调用 %+v；期望 completed、恰 1 次 ok try", c)
		}
		calls[c.CallID] = c
	}
	if len(calls) != want {
		t.Fatalf("inspect 中 %d 个调用，期望 %d：%+v", len(calls), want, in.Calls)
	}
	return calls
}

// researchCalls 是完整研究的调用 ID：计划 1 个，每个任务 1 次搜索、3 次抓取、1 次摘要，报告 1 个。
func researchCalls() []string {
	ids := []string{"root/plan/chat/1"}
	for i := range researchTasks {
		step := fmt.Sprintf("root/task-%d", i+1)
		ids = append(ids, step+"/search/1", step+"/fetch/1", step+"/fetch/2", step+"/fetch/3", step+"/chat/1")
	}
	return append(ids, "root/report/chat/1")
}

// TestRealDeepResearchFixedTopic：固定题目的完整研究——checkpoint 依次为 plan、task-1、task-2、report；
// 事件含 artifact_saved(report)；报告引用均对应已保存、已授权到任务的证据 blob；每个调用恰到达上游一次。
func TestRealDeepResearchFixedTopic(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(researchCfg(fu))
	id := h.submit("dr-fixed", researchSpec(nil))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	steps, _ := stepOrder(in)
	if !slices.Equal(steps, []string{"plan", "task-1", "task-2", "report"}) || len(in.Attempts) != 1 {
		t.Fatalf("checkpoints %v，attempts %+v；期望 plan、task-1、task-2、report 与 1 个 attempt", steps, in.Attempts)
	}
	calls := assertCallsSettled(t, in, len(researchCalls()))
	for _, cid := range researchCalls() {
		if _, ok := calls[cid]; !ok {
			t.Fatalf("缺少调用 %s：%v", cid, slices.Collect(maps.Keys(calls)))
		}
	}
	cites, cited := h.assertResearchReport(fu, id)
	got := countsOf(fu)
	for _, k := range []fakeupstream.Kind{fakeupstream.Chat, fakeupstream.Search, fakeupstream.Fetch} {
		if fu.Count(k) != fu.Distinct(k) {
			t.Fatalf("%s：上游 %d 个请求、%d 个不同请求（不应重发）", k, fu.Count(k), fu.Distinct(k))
		}
	}
	if got != researchFull {
		t.Fatalf("上游计数 %+v，期望 %+v", got, researchFull)
	}
	if _, outs, _ := h.pinnedResult(id, in.Attempts[0].AttemptID); len(outs) != 1 || outs[0].ArtifactID != "report" {
		t.Fatalf("result 的固定输出 %+v", outs)
	}
	t.Logf("固定题目：checkpoints %v；上游计数 %+v；报告引用 %v；证据 %+v", steps, got, cited, cites)
	h.finish()
}

// TestRealDeepResearchKillAndResume：task-1 的 checkpoint 提交后、结果送达 Worker 之前杀死 Worker →
// 新 attempt 的 init.resume.checkpoint_id 为 task-1 的 checkpoint；计划调用的上游计数仍为 1；恢复后任务 1 的
// search、fetch、summarize 不再到达上游：任务 1 的上游计数在恢复时与结束时相同，inspect 中这些调用只有 attempt 1
// 的那一次 try；attempt 2 提交的 checkpoint 的 refs 含任务 1 的 blob；报告引用对应已保存的证据 blob。
//
// 观察点：上游第 2 次搜索（attempt 2 的 task-2 搜索）挂起，此时 attempt 1 已结束、attempt 2 尚未发出其他请求，
// 恢复时的计数快照是确定的。
func TestRealDeepResearchKillAndResume(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	fu.Inject(fakeupstream.Search, 2, fakeupstream.Action{Hang: true})
	h.start(researchCfg(fu))
	// committed 的 checkpoint_result 依次为 plan（第 1 个）、task-1（第 2 个）。
	id := h.submit("dr-kill", researchSpec(&directives{KillAfterCommitted: 2}))
	r := h.rec(id)
	eventually(t, "attempt 2 的 task-2 搜索到达上游并挂起", func() bool { return fu.Hanging() == 1 })
	in2, ok := r.init(2)
	cp1 := checkpointByStep(h.inspect(id), "task-1")
	if !ok || in2.Resume == nil || in2.Resume.StepID != "task-1" || in2.Resume.CheckpointID != cp1.CheckpointID || cp1.CheckpointID == "" {
		t.Fatalf("attempt 2 的 init.resume %+v；期望 task-1 的 checkpoint %+v", in2.Resume, cp1)
	}
	before := countsOf(fu)
	if want := (upstreamCounts{Plan: 1, Summarize: 1, Search: 2, Fetch: 3}); before != want {
		t.Fatalf("恢复后、task-2 搜索挂起时上游计数 %+v，期望 %+v（任务 1 未重发）", before, want)
	}
	t1Before := task1Counts(fu)
	if t1Before != (upstreamCounts{Summarize: 1, Search: 1, Fetch: 3}) {
		t.Fatalf("恢复时任务 1 的上游计数 %+v", t1Before)
	}
	fu.Release()

	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2)
	steps, cpAttempts := stepOrder(in)
	if len(in.Attempts) != 2 || a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded ||
		!slices.Equal(steps, []string{"plan", "task-1", "task-2", "report"}) ||
		!slices.Equal(cpAttempts, []string{a1.AttemptID, a1.AttemptID, a2.AttemptID, a2.AttemptID}) {
		t.Fatalf("attempts %s；checkpoints %v（attempt %v）；期望 attempt 1 被杀死于 task-1 之后、attempt 2 提交 task-2 与 report",
			attemptClasses(in), steps, cpAttempts)
	}
	// 计划与任务 1 的调用只有 attempt 1 的那一次 try；任务 2 与报告的调用由 attempt 2 首次发起。
	calls := assertCallsSettled(t, in, len(researchCalls()))
	for cid, c := range calls {
		want := a2.AttemptID
		if cid == "root/plan/chat/1" || strings.HasPrefix(cid, "root/task-1/") {
			want = a1.AttemptID
		}
		if c.FirstAttemptID != want || c.Tries[0].AttemptID != want {
			t.Fatalf("调用 %s 的首个 attempt %s、try 的 attempt %s，期望 %s", cid, c.FirstAttemptID, c.Tries[0].AttemptID, want)
		}
	}
	final := countsOf(fu)
	for _, k := range []fakeupstream.Kind{fakeupstream.Chat, fakeupstream.Search, fakeupstream.Fetch} {
		if fu.Count(k) != fu.Distinct(k) {
			t.Fatalf("%s：上游 %d 个请求、%d 个不同请求（不应重发）", k, fu.Count(k), fu.Distinct(k))
		}
	}
	if final != researchFull {
		t.Fatalf("上游计数 %+v，期望 %+v（计划 1 次、任务 1 不重发）", final, researchFull)
	}
	if t1After := task1Counts(fu); t1After != t1Before {
		t.Fatalf("恢复后任务 1 的上游计数 %+v，恢复时 %+v：任务 1 的调用不应再到达上游", t1After, t1Before)
	}
	// attempt 2 提交的 checkpoint（task-2、report）的累积 refs 含任务 1 的证据与摘要 blob（来自 attempt 1）。
	var t1Blobs []string
	for _, cid := range []string{"root/task-1/fetch/1", "root/task-1/fetch/2", "root/task-1/fetch/3", "root/task-1/chat/1"} {
		t1Blobs = append(t1Blobs, calls[cid].ResultRef)
	}
	for _, stepID := range []string{"task-2", "report"} {
		refs := h.checkpointRefs(id, stepID)
		for _, b := range t1Blobs {
			if !slices.Contains(refs, b) {
				t.Fatalf("attempt 2 的 checkpoint %s 的 refs %v 不含任务 1 的 blob %s", stepID, refs, b)
			}
		}
	}
	cites, cited := h.assertResearchReport(fu, id)
	t.Logf("杀死并恢复：attempts %s；init.resume = %s（%s）；checkpoints %v；上游计数 恢复时 %+v → 结束 %+v；"+
		"任务 1 上游计数 恢复时 %+v = 结束 %+v；attempt 2 refs 含任务 1 blob %v；报告引用 %v；证据 %+v",
		attemptClasses(in), in2.Resume.CheckpointID, in2.Resume.StepID, steps, before, final, t1Before, task1Counts(fu),
		t1Blobs, cited, cites)
	h.finish()
}

// task1Counts 是任务 1 的调用到达上游的请求数：查询为任务 1 的搜索、任务 1 搜索结果页面的抓取、标题为任务 1
// 的摘要（Plan 与 Report 恒为 0）。
func task1Counts(fu *fakeupstream.Server) upstreamCounts {
	task := researchTasks[0]
	pages := map[string]bool{}
	for i := 1; i <= 3; i++ {
		u, err := url.Parse(fu.ResultURL(task.Query, i))
		if err != nil {
			panic(err) // ResultURL 总是合法
		}
		pages[u.Path] = true
	}
	var c upstreamCounts
	for _, r := range fu.Requests(fakeupstream.Search) {
		if strings.Contains(r.Body, task.Query) {
			c.Search++
		}
	}
	for _, r := range fu.Requests(fakeupstream.Fetch) {
		if pages[r.Path] {
			c.Fetch++
		}
	}
	for _, r := range fu.Requests(fakeupstream.Chat) {
		if r.Stage == fakeupstream.StageSummarize && strings.Contains(r.Body, task.Title) {
			c.Summarize++
		}
	}
	return c
}

// ---- M3：缓存故障实验（规格 §16.4 E21–E25）与缓存收益测量（§16.6）----
//
// 共享缓存是真实 Redis（AGENTBOX_TEST_REDIS_ADDR；本地为 compose 的 Redis，CI 为 Redis service）；上游是 fake
// upstream，抓取页面的缓存头由 SetPageHeader 声明。Redis 运行在测试无法停止的容器中：宕机、变慢与黑洞由测试控制的
// TCP 代理（faultProxy）在 Gateway 与 Redis 之间模拟。测试直接读写同一 Redis（cache 包的客户端；超过 4 KiB 的值
// 用原始 RESP）来篡改条目，签名密钥取自 server 数据目录中的 cache.key（cache.LoadKeys）。页面路径与搜索查询带随机
// 后缀：Redis 跨用例、跨运行共享，旧运行留下的条目（以别的密钥签名）不会落在本用例的键上。
//
// Worker 是 sim_worker（fetch 操作的调用 ID 为 root/<step>/fetch/1）；E24 与收益测量直接连接 attempt 的 socket
// 发请求。每条用例以静止时的不变量检查结束（含 I14 的缓存部分与 I15）。
//
// TestE24CoalesceWithinAttempt 只覆盖 root 调用的合并。sub-run 的部分（同一 sub-run 内合并、两个 sub-run 的同一
// 抓取不合并）见 M4 Plan 14 Task 11 段的 TestE24SubrunCoalesce：edge 自 M4（Plan 14 Task 5）接受 X-Agentbox-Subrun，
// 合并键含 subrun_id（§11.4）。

const redisEnv = "AGENTBOX_TEST_REDIS_ADDR"

func testRedisAddr(t *testing.T) string {
	t.Helper()
	a := os.Getenv(redisEnv)
	if a == "" {
		required(t, redisEnv)
	}
	return a
}

func randSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b) // crypto/rand 不会失败（Go 1.20 起失败时直接终止进程）
	return hex.EncodeToString(b)
}

// cacheCfg 是开启共享缓存的进程内装置配置（gatewayCfg 加 Redis 地址）。
func cacheCfg(fu *fakeupstream.Server, redisAddr string) app.Config {
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.RedisAddr = redisAddr
	return cfg
}

// cachePage 在 fake upstream 上声明带响应头 hdr 的抓取页面，返回其 URL。
func cachePage(fu *fakeupstream.Server, path string, hdr http.Header) string {
	fu.SetPageHeader(path, "text/plain; charset=utf-8", "页面 "+path+" 的正文", hdr)
	return fu.URL() + path
}

func ccHeader(v string) http.Header { return http.Header{"Cache-Control": {v}} }

// fetchKey 是抓取 u 的缓存键，与 Gateway 的计算相同（fetch adapter 的 provider、版本与 Resolve 后的参数）。
func fetchKey(t *testing.T, u string) string {
	t.Helper()
	params, err := json.Marshal(map[string]string{"url": u})
	if err != nil {
		t.Fatal(err)
	}
	ad := upstream.NewFetch(upstream.FetchConfig{})
	k, err := cache.Key(upstream.KindFetch, ad.Provider(), ad.Version(), params)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// testRedis 是测试自己的 Redis 客户端（直连，不经代理；超时宽松）。
func testRedis(t *testing.T, addr string) *cache.Redis {
	t.Helper()
	r := cache.NewRedis(cache.RedisConfig{Addr: addr, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second})
	t.Cleanup(func() { _ = r.Close() }) // 测试结束，关闭错误无关紧要
	return r
}

func cacheEntry(t *testing.T, r *cache.Redis, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := r.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("读取缓存条目 %s: %v", key, err)
	}
	return v, ok
}

// waitSealed 等待键 key 上出现 Gateway 写入的有效条目（以 server 的签名密钥校验通过、未过期），返回原始字节与解码值。
// 写入是异步的：上游结果返回给 Worker 之后才落到 Redis。
func waitSealed(t *testing.T, r *cache.Redis, signer *cache.Signer, key string) ([]byte, cache.Value) {
	t.Helper()
	var raw []byte
	var val cache.Value
	eventually(t, "缓存条目 "+key+" 由 Gateway 写入", func() bool {
		b, ok, err := r.Get(context.Background(), key)
		if err != nil || !ok {
			return false
		}
		v, err := signer.Open(key, b, time.Now())
		if err != nil {
			return false
		}
		raw, val = b, v
		return true
	})
	return raw, val
}

// rawSet 以原始 RESP 写入任意大小的值（cache 客户端拒绝超过 4 KiB 的值）。
func rawSet(t *testing.T, addr, key string, val []byte, ttl time.Duration) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	args := [][]byte{[]byte("SET"), []byte(key), val, []byte("PX"), []byte(strconv.FormatInt(ttl.Milliseconds(), 10))}
	cmd := fmt.Appendf(nil, "*%d\r\n", len(args))
	for _, a := range args {
		cmd = fmt.Appendf(cmd, "$%d\r\n", len(a))
		cmd = append(append(cmd, a...), '\r', '\n')
	}
	if _, err := c.Write(cmd); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil || line != "+OK\r\n" {
		t.Fatalf("原始 SET 的回复 %q %v", line, err)
	}
}

// cacheCall 是 calls 表中缓存实验关心的列。
type cacheCall struct {
	State, Source, ResultRef, FailReason string
	Tries                                int
}

func cacheCallRow(t *testing.T, dsn, taskID, callID string) cacheCall {
	t.Helper()
	var c cacheCall
	pgQueryRow(t, dsn, `SELECT state, source, COALESCE(result_ref, ''), fail_reason, tries_used FROM calls
		WHERE task_id = $1 AND call_id = $2`, []any{taskID, callID}, &c.State, &c.Source, &c.ResultRef, &c.FailReason, &c.Tries)
	return c
}

// cacheMetrics 经 GET /status 读取共享缓存的指标（hit、miss、bypass、coalesced、error、breaker_open、integrity_failure）。
func (h *harness) cacheMetrics() map[string]int64 {
	h.t.Helper()
	st, b, err := httpDo("GET", h.base+"/status", "")
	var v struct {
		Cache map[string]int64 `json:"cache"`
	}
	if err != nil || st != http.StatusOK || json.Unmarshal(b, &v) != nil || v.Cache == nil {
		h.t.Fatalf("GET /status = %d %s %v（期望含 cache 指标）", st, b, err)
	}
	return v.Cache
}

func metricsDelta(after, before map[string]int64) map[string]int64 {
	d := map[string]int64{}
	for k, v := range after {
		d[k] = v - before[k]
	}
	return d
}

func fetchCallID(i int) string { return fmt.Sprintf("root/f%d/fetch/1", i) }

// runFetches 提交一个依次抓取 urls 的任务（步骤 f1..fn）并等待它成功结束。
func (h *harness) runFetches(requestID string, urls ...string) string {
	h.t.Helper()
	steps := make([]step, len(urls))
	for i, u := range urls {
		steps[i] = step{"op": "fetch", "step_id": fmt.Sprintf("f%d", i+1), "url": u}
	}
	id := h.submit(requestID, spec(nil, requestID, nil, steps...))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		h.t.Fatalf("任务 %s（%s）结束为 %s/%s", id, requestID, v.Status, v.StatusReason)
	}
	return id
}

// pathCount 是 fake upstream 收到的、路径与 u 相同的抓取请求数。
func pathCount(t *testing.T, fu *fakeupstream.Server, u string) int {
	t.Helper()
	p, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range fu.Requests(fakeupstream.Fetch) {
		if r.Path == p.Path {
			n++
		}
	}
	return n
}

// waitAttemptSocket 等待任务第 1 个 attempt 的 Worker 启动、Gateway socket 就绪，返回 socket 路径。
func (h *harness) waitAttemptSocket(taskID string) string {
	h.t.Helper()
	var sock string
	eventually(h.t, "attempt 1 的 Gateway socket 就绪", func() bool {
		in, ok := h.rec(taskID).init(1)
		if !ok {
			return false
		}
		sock = h.gwSocket(in.AttemptID)
		_, err := os.Stat(sock)
		return err == nil
	})
	return sock
}

// proxyMode 是 faultProxy 对 Gateway → Redis 方向的处理方式。
type proxyMode int

const (
	proxyPass      proxyMode = iota // 原样转发
	proxyDelay                      // 每段请求延迟 delay 后转发（变慢）
	proxyBlackhole                  // 吞掉请求，连接保持（客户端等到超时）
	proxyDown                       // 新连接接受后立即关闭（宕机）
)

// faultProxy 是 Gateway 与 Redis 之间由测试控制的 TCP 代理（E21）。切换模式时断开现有连接，之后的操作都经新连接、
// 按新模式处理。
type faultProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	mode  proxyMode
	delay time.Duration
	conns map[net.Conn]struct{}
}

func newFaultProxy(t *testing.T, target string) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() {
		_ = ln.Close() // 测试结束，关闭错误无关紧要
		p.set(proxyDown, 0)
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *faultProxy) Addr() string { return p.ln.Addr().String() }

func (p *faultProxy) set(m proxyMode, delay time.Duration) {
	p.mu.Lock()
	p.mode, p.delay = m, delay
	conns := maps.Clone(p.conns)
	p.mu.Unlock()
	for c := range conns {
		_ = c.Close() // 断开现有连接；关闭错误无关紧要
	}
}

func (p *faultProxy) current() (proxyMode, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode, p.delay
}

func (p *faultProxy) serve(c net.Conn) {
	if m, _ := p.current(); m == proxyDown {
		_ = c.Close() // 宕机：拒绝连接
		return
	}
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.conns[c], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { p.forward(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }() // 连接被断开时的错误无关紧要
	<-done
	_, _ = c.Close(), up.Close()
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, up)
	p.mu.Unlock()
}

// forward 把 Gateway 发出的请求按当前模式转发给 Redis。
func (p *faultProxy) forward(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			m, d := p.current()
			switch m {
			case proxyDown:
				return
			case proxyDelay:
				time.Sleep(d) // 注入的延迟本身
			}
			if m != proxyBlackhole {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// TestE21CacheRedisDegraded：E21——Redis 正常、变慢（每个请求延迟 200 ms，超过 50 ms 读超时）、黑洞（请求无回复）、
// 宕机（连接被拒）→ 每个任务的结果都正确（与正常时同一结果 blob），Redis 故障只表现为未命中：/status 可见 hit、
// miss、bypass（含凭据参数的 URL）、error，连续 5 次失败后熔断打开（breaker_open），之后不再访问 Redis。
func TestE21CacheRedisDegraded(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	px := newFaultProxy(t, addr)
	sfx := randSuffix()
	page := func(name string) string { return cachePage(fu, "/e21-"+sfx+"/"+name, ccHeader("max-age=600")) }
	u := page("a")
	cred := fu.URL() + "/e21-" + sfx + "/private?token=e2e"
	h.start(cacheCfg(fu, px.Addr()))
	rc := testRedis(t, addr)
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}

	// 正常：第一次未命中、走上游并写入；第二个任务（新的任务与调用 ID）命中同一结果 blob；凭据 URL 不读缓存（bypass）。
	id1 := h.runFetches("e21-1", u)
	c1 := cacheCallRow(t, h.dsn, id1, fetchCallID(1))
	waitSealed(t, rc, signer, fetchKey(t, u))
	id2 := h.runFetches("e21-2", u, cred)
	c2 := cacheCallRow(t, h.dsn, id2, fetchCallID(1))
	c2cred := cacheCallRow(t, h.dsn, id2, fetchCallID(2))
	if c1.Source != "upstream" || c1.Tries != 1 || c2.State != "completed" || c2.Source != "cache" || c2.Tries != 0 ||
		c2.ResultRef != c1.ResultRef || c2cred.Source != "upstream" {
		t.Fatalf("正常阶段：首次 %+v，命中 %+v，凭据 URL %+v", c1, c2, c2cred)
	}
	m0 := h.cacheMetrics()
	if m0["hit"] != 1 || m0["miss"] != 1 || m0["bypass"] != 1 || m0["error"] != 0 || m0["breaker_open"] != 0 {
		t.Fatalf("正常阶段的指标 %v", m0)
	}

	// 变慢与黑洞：查找超时（error）→ 按未命中走上游，写入同样超时；结果与正常时相同。
	for i, ph := range []struct {
		name string
		mode proxyMode
	}{{"变慢", proxyDelay}, {"黑洞", proxyBlackhole}} {
		px.set(ph.mode, 200*time.Millisecond)
		id := h.runFetches(fmt.Sprintf("e21-%d", i+3), u)
		if c := cacheCallRow(t, h.dsn, id, fetchCallID(1)); c.State != "completed" || c.Source != "upstream" || c.ResultRef != c1.ResultRef {
			t.Fatalf("%s：调用 %+v，期望走上游且结果为 %s", ph.name, c, c1.ResultRef)
		}
	}

	// 宕机：连续失败达到 5 次后熔断打开；之后的查找与写入不访问 Redis（breaker_open），任务仍然正确完成。
	px.set(proxyDown, 0)
	urls := []string{u, page("b"), page("c"), page("d")}
	id5 := h.runFetches("e21-5", urls...)
	for i := range urls {
		c := cacheCallRow(t, h.dsn, id5, fetchCallID(i+1))
		if c.State != "completed" || c.Source != "upstream" || (i == 0 && c.ResultRef != c1.ResultRef) {
			t.Fatalf("宕机：调用 %d %+v", i+1, c)
		}
	}
	var m map[string]int64
	eventually(t, "/status 显示熔断打开（breaker_open > 0）", func() bool {
		m = h.cacheMetrics()
		return m["breaker_open"] > 0
	})
	if m["error"] < cache.DefaultFailureThreshold || m["hit"] != 1 || m["integrity_failure"] != 0 {
		t.Fatalf("故障阶段的指标 %v：期望 error ≥ %d、hit 仍为 1", m, cache.DefaultFailureThreshold)
	}
	if n := pathCount(t, fu, u); n != 4 {
		t.Fatalf("页面 a 到达上游 %d 次，期望 4（命中的一次不访问上游）", n)
	}
	t.Logf("E21：正常阶段指标 %v；变慢/黑洞/宕机之后 %v；页面 a 的结果 blob %s", m0, m, c1.ResultRef)
	h.finish()
}

// TestE22CacheIntegrity：E22——直接写入 Redis 的篡改条目：错误的 HMAC、替换 blob sha（未重新签名）、持有密钥者签名
// 但引用不存在的 blob、过 expires_at 后重放原本有效的签名条目、超过 4 KiB 的值 → 每一种都是未命中：走上游、结果与
// 正常时相同，integrity_failure 恰加 1、hit 不变、error 不变（超大值不是 Redis 故障，不计入熔断）；条目被删除，
// 上游结果随后重新写入有效条目。
func TestE22CacheIntegrity(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	sfx := randSuffix()
	u := cachePage(fu, "/e22-"+sfx+"/a", ccHeader("max-age=600"))
	short := cachePage(fu, "/e22-"+sfx+"/short", ccHeader("max-age=3"))
	h.start(cacheCfg(fu, addr))
	rc := testRedis(t, addr)
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}
	key, shortKey := fetchKey(t, u), fetchKey(t, short)

	id0 := h.runFetches("e22-seed", u, short)
	ref := cacheCallRow(t, h.dsn, id0, fetchCallID(1)).ResultRef
	shortRef := cacheCallRow(t, h.dsn, id0, fetchCallID(2)).ResultRef
	good, _ := waitSealed(t, rc, signer, key)
	shortRaw, shortVal := waitSealed(t, rc, signer, shortKey)
	// 对照：未篡改的条目命中。
	if c := cacheCallRow(t, h.dsn, h.runFetches("e22-hit", u), fetchCallID(1)); c.Source != "cache" || c.ResultRef != ref {
		t.Fatalf("未篡改的条目应命中：%+v", c)
	}

	ctx := context.Background()
	set := func(k string, v []byte) {
		t.Helper()
		if err := rc.Set(ctx, k, v, 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	mutate := func(raw []byte, f func(m map[string]any)) []byte {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		f(m)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name, url, key, ref string
		tamper              func()
	}{
		{"错误的 HMAC", u, key, ref, func() {
			set(key, mutate(good, func(m map[string]any) {
				mac, err := base64.StdEncoding.DecodeString(m["mac"].(string))
				if err != nil || len(mac) == 0 {
					t.Fatalf("条目的 mac %v %v", m["mac"], err)
				}
				mac[0] ^= 0xff
				m["mac"] = base64.StdEncoding.EncodeToString(mac)
			}))
		}},
		{"替换 blob sha（未重新签名）", u, key, ref, func() {
			set(key, mutate(good, func(m map[string]any) { m["blob_sha256"] = shortRef }))
		}},
		{"持有密钥者签名、引用的 blob 不存在", u, key, ref, func() {
			now := time.Now()
			v, err := signer.Seal(key, cache.Value{BlobSHA256: sha256Hex([]byte("e22 不存在的 blob " + sfx)), Status: 200,
				ContentType: "text/plain", Size: 16, FetchedAt: now, ExpiresAt: now.Add(10 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			set(key, v)
		}},
		{"过 expires_at 后重放签名条目", short, shortKey, shortRef, func() {
			eventually(t, "短寿命条目过了 expires_at", func() bool { return time.Now().After(shortVal.ExpiresAt) })
			set(shortKey, shortRaw)
		}},
		{"超过 4 KiB 的值", u, key, ref, func() {
			rawSet(t, addr, key, bytes.Repeat([]byte("x"), cache.MaxValueSize+1000), 10*time.Minute)
		}},
	}
	for i, c := range cases {
		before := h.cacheMetrics()
		n0 := pathCount(t, fu, c.url)
		c.tamper()
		id := h.runFetches(fmt.Sprintf("e22-%d", i), c.url)
		got := cacheCallRow(t, h.dsn, id, fetchCallID(1))
		d := metricsDelta(h.cacheMetrics(), before)
		if got.State != "completed" || got.Source != "upstream" || got.ResultRef != c.ref || got.Tries != 1 {
			t.Fatalf("%s：调用 %+v，期望未命中、走上游且结果为 %s", c.name, got, c.ref)
		}
		if d["integrity_failure"] != 1 || d["hit"] != 0 || d["error"] != 0 || d["breaker_open"] != 0 || pathCount(t, fu, c.url) != n0+1 {
			t.Fatalf("%s：指标变化 %v，上游 %d → %d", c.name, d, n0, pathCount(t, fu, c.url))
		}
		// 篡改的条目已被删除，上游结果重新写入有效条目（下一个用例在它之上篡改）。
		waitSealed(t, rc, signer, c.key)
		t.Logf("E22 %s：未命中，指标变化 %v", c.name, d)
	}
	h.finish()
}

// TestE23CacheHitCancelRace：E23——缓存命中之后、Tx2（CompleteFromCache）之前取消先提交 → Tx2 复查访问时读到
// desired = cancel（或取消随即提交的访问撤销）而拒绝：调用置为 failed（cancel_requested 或 access_revoked）、没有
// try，命中的 blob 没有授权到该任务的 scope，也没有来源记录；任务以 cancelled 结束，上游不被访问。
func TestE23CacheHitCancelRace(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	u := cachePage(fu, "/e23-"+randSuffix()+"/a", ccHeader("max-age=600"))
	h.start(cacheCfg(fu, addr))
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}
	id0 := h.runFetches("e23-seed", u)
	ref := cacheCallRow(t, h.dsn, id0, fetchCallID(1)).ResultRef
	waitSealed(t, testRedis(t, addr), signer, fetchKey(t, u))

	entered := make(chan call.CacheCompletion, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) }) // 先于装置的收尾执行（LIFO），不让服务停在钩子里
	h.gate.mu.Lock()
	h.gate.cacheHook = func(r call.CacheCompletion) {
		select {
		case entered <- r:
		default:
		}
		<-release
	}
	h.gate.mu.Unlock()

	id := h.submit("e23", spec(nil, "e23", nil, step{"op": "fetch", "step_id": "f1", "url": u}))
	var cc call.CacheCompletion
	select {
	case cc = <-entered:
	case <-time.After(waitLimit):
		t.Fatal("缓存命中未到达 Tx2")
	}
	if cc.TaskID != id || cc.Source != call.SourceCache || cc.ResultSHA256 != ref {
		t.Fatalf("Tx2 的请求 %+v，期望任务 %s 以 cache 来源提交 %s", cc, id, ref)
	}
	// Tx2 停在命中之后：取消在它之前提交（取消请求本身在另一个 goroutine 中，不依赖 Tx2 何时继续）。
	cancelErr := make(chan error, 1)
	go func() {
		st, code, err := h.cancelTaskE(id, "e23-cancel")
		if err == nil && st != http.StatusOK {
			err = fmt.Errorf("取消 = %d %s", st, code)
		}
		cancelErr <- err
	}()
	eventually(t, "取消已提交（task_control.desired = cancel）", func() bool {
		var desired string
		h.queryRow("SELECT desired FROM task_control WHERE task_id = $1", []any{id}, &desired)
		return desired == "cancel"
	})
	releaseOnce.Do(func() { close(release) })
	if err := <-cancelErr; err != nil {
		t.Fatal(err)
	}
	if v := h.waitTerminal(id); v.Status != "cancelled" {
		t.Fatalf("任务结束为 %s/%s", v.Status, v.StatusReason)
	}
	// 任务终态不等待 Gateway 中的请求：放行后 Tx2 才被拒、调用才离开 resolving。
	var c cacheCall
	eventually(t, "被暂停的 Tx2 结束、调用离开 resolving", func() bool {
		c = cacheCallRow(t, h.dsn, id, fetchCallID(1))
		return c.State != "resolving"
	})
	var scoped, prov int
	h.queryRow("SELECT count(*) FROM scope_blobs WHERE scope_kind = 'task' AND scope_id = $1 AND sha256 = $2", []any{id, ref}, &scoped)
	h.queryRow("SELECT count(*) FROM blob_provenance WHERE scope_kind = 'task' AND scope_id = $1", []any{id}, &prov)
	// 拒绝原因取决于 Tx2 读到的是哪一个已提交的取消效果：desired = cancel（cancel_requested），或取消随即撤销的
	// attempt 访问（access_revoked）。两者都在 Tx2 之前提交，结果都不被授权。
	if c.State != "failed" || (c.FailReason != "cancel_requested" && c.FailReason != "access_revoked") ||
		c.Source != "upstream" || c.ResultRef != "" || c.Tries != 0 ||
		scoped != 0 || prov != 0 {
		t.Fatalf("调用 %+v；scope_blobs %d 行、blob_provenance %d 行；期望取消先提交时结果不被授权", c, scoped, prov)
	}
	if n := pathCount(t, fu, u); n != 1 {
		t.Fatalf("上游收到 %d 个抓取请求，期望 1（只有播种任务）", n)
	}
	t.Logf("E23：命中 %s 后取消先提交 → 调用 %+v，结果未授权", ref, c)
	h.finish()
}

// TestE24CoalesceWithinAttempt：E24（同一 sub-run 的一半）——同一 attempt（sub-run 为 root）内 10 个相同的抓取
// 并发到达（各自的调用 ID）→ 上游只收到一次请求：leader 的调用 source = upstream、1 个 try；其余 9 个以 coalesced
// 完成、没有 try，全部得到同一结果 blob；/status 的 coalesced 加 9。
func TestE24CoalesceWithinAttempt(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	u := cachePage(fu, "/e24-"+randSuffix()+"/a", nil) // 没有新鲜度头：不写缓存，只看合并
	fu.Inject(fakeupstream.Fetch, 1, fakeupstream.Action{Hang: true})
	h.start(cacheCfg(fu, addr))
	id := h.submit("e24", spec(nil, "e24", nil, sleepStep(600000)))
	sock := h.waitAttemptSocket(id)
	before := h.cacheMetrics()

	const n = 10
	body := `{"url":"` + u + `"}`
	res := make([]gwResp, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i], errs[i] = gwDo(sock, http.MethodPost, "/v1/fetch", fmt.Sprintf("e2e/e24/fetch/%d", i+1), body)
		}()
	}
	eventually(t, "leader 的请求挂起在上游、9 个请求作为 follower 加入", func() bool {
		return fu.Hanging() == 1 && metricsDelta(h.cacheMetrics(), before)["coalesced"] == n-1
	})
	fu.Release()
	wg.Wait()
	for i := range n {
		if errs[i] != nil || res[i].Status != http.StatusOK || res[i].Replayed || res[i].Blob == "" || res[i].Blob != res[0].Blob {
			t.Fatalf("请求 %d：%+v %v（请求 1：%+v）", i+1, res[i], errs[i], res[0])
		}
	}
	var upstreamCalls, coalesced, tries, distinct int
	h.queryRow(`SELECT count(*) FILTER (WHERE source = 'upstream'), count(*) FILTER (WHERE source = 'coalesced'),
			COALESCE(sum(tries_used), 0)::bigint, count(DISTINCT result_ref)
		FROM calls WHERE task_id = $1 AND call_id LIKE 'e2e/e24/%' AND state = 'completed'`, []any{id},
		&upstreamCalls, &coalesced, &tries, &distinct)
	if upstreamCalls != 1 || coalesced != n-1 || tries != 1 || distinct != 1 || fu.Count(fakeupstream.Fetch) != 1 {
		t.Fatalf("calls：upstream %d、coalesced %d、tries 合计 %d、不同结果 %d；上游抓取 %d 次", upstreamCalls, coalesced, tries,
			distinct, fu.Count(fakeupstream.Fetch))
	}
	d := metricsDelta(h.cacheMetrics(), before)
	if d["coalesced"] != n-1 || d["hit"] != 0 {
		t.Fatalf("指标变化 %v", d)
	}
	if st, code := h.cancelTask(id, "e24-cancel"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	if v := h.waitTerminal(id); v.Status != "cancelled" {
		t.Fatalf("任务结束为 %s", v.Status)
	}
	t.Logf("E24：10 个相同抓取 → 上游 %d 次；calls upstream=%d coalesced=%d；指标变化 %v；结果 %s",
		fu.Count(fakeupstream.Fetch), upstreamCalls, coalesced, d, res[0].Blob)
	h.finish()
}

// TestE25FreshnessViaFakeUpstream：E25——fake upstream 的响应头复现新鲜度表：
//   - 短 max-age（3 s）可缓存，expires_at 不晚于响应时刻 + 3 s；过期后同一抓取回到上游（不返回过期条目）；
//   - max-age=60 且 Age: 50 → expires_at 约为响应时刻 + 10 s（按年龄扣减，寿命不被延长）；命中不改写条目；
//   - Vary: Accept-Encoding 可缓存；Vary 含其他维度或 *、重复的 Cache-Control 指令、重复的 Expires、无效的 Expires、
//     no-store、Set-Cookie、不认识的指令、没有显式新鲜度 → 不缓存（第二次抓取仍走上游，Redis 中没有条目）。
func TestE25FreshnessViaFakeUpstream(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	sfx := randSuffix()
	page := func(name string, hdr http.Header) string { return cachePage(fu, "/e25-"+sfx+"/"+name, hdr) }
	in1h := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	in2h := time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat)
	short := page("short", ccHeader("max-age=3"))
	aged := page("aged", http.Header{"Cache-Control": {"max-age=60"}, "Age": {"50"}})
	varyAE := page("vary-ae", http.Header{"Cache-Control": {"max-age=600"}, "Vary": {"Accept-Encoding"}})
	uncacheable := []string{
		page("vary-cookie", http.Header{"Cache-Control": {"max-age=600"}, "Vary": {"Cookie"}}),
		page("vary-star", http.Header{"Cache-Control": {"max-age=600"}, "Vary": {"*"}}),
		page("conflicting-cc", http.Header{"Cache-Control": {"max-age=60", "max-age=120"}}),
		page("duplicate-expires", http.Header{"Expires": {in1h, in2h}}),
		page("invalid-expires", http.Header{"Cache-Control": {"max-age=600"}, "Expires": {"not-a-date"}}),
		page("no-store", ccHeader("no-store, max-age=600")),
		page("set-cookie", http.Header{"Cache-Control": {"max-age=600"}, "Set-Cookie": {"sid=e2e"}}),
		page("unknown-directive", ccHeader("max-age=600, x-agentbox-unknown")),
		page("no-freshness", nil),
	}
	h.start(cacheCfg(fu, addr))
	rc := testRedis(t, addr)
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}

	h.runFetches("e25-1", append([]string{short, aged, varyAE}, uncacheable...)...)
	_, shortVal := waitSealed(t, rc, signer, fetchKey(t, short))
	agedRaw, agedVal := waitSealed(t, rc, signer, fetchKey(t, aged))
	_, varyVal := waitSealed(t, rc, signer, fetchKey(t, varyAE))
	life := func(v cache.Value) time.Duration { return v.ExpiresAt.Sub(v.FetchedAt) }
	if l := life(shortVal); l <= 0 || l > 3*time.Second {
		t.Fatalf("max-age=3 的条目寿命 %s，期望 (0, 3s]", l)
	}
	if l := life(agedVal); l <= 5*time.Second || l > 10*time.Second {
		t.Fatalf("max-age=60、Age: 50 的条目寿命 %s，期望约 10 s（不超过 10 s）", l)
	}
	if l := life(varyVal); l <= 590*time.Second || l > 600*time.Second {
		t.Fatalf("Vary: Accept-Encoding 的条目寿命 %s，期望约 600 s", l)
	}
	for _, u := range uncacheable {
		if _, ok := cacheEntry(t, rc, fetchKey(t, u)); ok {
			t.Fatalf("%s 不应被缓存", u)
		}
	}

	// 第二个任务：可缓存的命中，其余仍走上游；命中不改写（不延长）条目。
	m1 := h.cacheMetrics()
	id2 := h.runFetches("e25-2", append([]string{aged, varyAE}, uncacheable...)...)
	for i := range 2 + len(uncacheable) {
		want := "upstream"
		if i < 2 {
			want = "cache"
		}
		if c := cacheCallRow(t, h.dsn, id2, fetchCallID(i+1)); c.State != "completed" || c.Source != want {
			t.Fatalf("第二个任务的调用 %d %+v，期望 %s", i+1, c, want)
		}
	}
	if raw, ok := cacheEntry(t, rc, fetchKey(t, aged)); !ok || !bytes.Equal(raw, agedRaw) {
		t.Fatal("命中之后 Age 页面的条目被改写或消失（命中不得延长寿命）")
	}
	if d := metricsDelta(h.cacheMetrics(), m1); d["hit"] != 2 {
		t.Fatalf("第二个任务的指标变化 %v，期望 hit = 2", d)
	}

	// 短 max-age 过期后：同一抓取回到上游（不返回过期条目），不计命中。
	eventually(t, "max-age=3 的条目过了 expires_at", func() bool { return time.Now().After(shortVal.ExpiresAt) })
	m2 := h.cacheMetrics()
	id3 := h.runFetches("e25-3", short)
	if c := cacheCallRow(t, h.dsn, id3, fetchCallID(1)); c.Source != "upstream" {
		t.Fatalf("过期后的抓取 %+v，期望走上游", c)
	}
	if d := metricsDelta(h.cacheMetrics(), m2); d["hit"] != 0 {
		t.Fatalf("过期后的指标变化 %v", d)
	}
	for _, c := range []struct {
		u    string
		want int
	}{{short, 2}, {aged, 1}, {varyAE, 1}} {
		if n := pathCount(t, fu, c.u); n != c.want {
			t.Fatalf("%s 到达上游 %d 次，期望 %d", c.u, n, c.want)
		}
	}
	for _, u := range uncacheable {
		if n := pathCount(t, fu, u); n != 2 {
			t.Fatalf("不可缓存的 %s 到达上游 %d 次，期望 2", u, n)
		}
	}
	t.Logf("E25：寿命 short=%s aged=%s vary-ae=%s；%d 种不支持的响应均未缓存", life(shortVal), life(agedVal), life(varyVal), len(uncacheable))
	h.finish()
}

// ---- 缓存收益测量（§16.6；bench/cache/run.sh）----

// benchSample 是一次 Gateway 调用的测量。
type benchSample struct {
	Group     string
	Run       int
	CallID    string
	Kind      string
	Source    string
	LatencyMs float64
}

// benchGroup 是一组（缓存开或关）的汇总。
type benchGroup struct {
	Name          string
	Calls         int
	Hits          int
	UpstreamCalls int
	Metrics       map[string]int64
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// TestCacheBenefit 测量共享缓存的收益（只由 bench/cache/run.sh 运行：设置 AGENTBOX_CACHE_BENCH_OUT 时执行）。
// 两组（缓存开、缓存关）各 N 次（默认 10）重复同一组查询（1 个搜索 + 3 个抓取）；每次重复是一个新的任务、使用新的
// 调用 ID，测到的是跨任务共享缓存，而不是同一调用 ID 的 journal 重放。测试直接在 attempt 的 socket 上逐个发出调用，
// 记录调用方看到的延迟与调用的来源（calls.source）；上游是 fake upstream，正常回复前固定延迟（默认 200 ms）。
// 原始数据与汇总以 Markdown 写入 AGENTBOX_CACHE_BENCH_OUT。
func TestCacheBenefit(t *testing.T) {
	out := os.Getenv("AGENTBOX_CACHE_BENCH_OUT")
	if out == "" {
		t.Skip("缓存收益测量只由 bench/cache/run.sh 运行（设置 AGENTBOX_CACHE_BENCH_OUT）")
	}
	addr := testRedisAddr(t)
	runs := envInt("AGENTBOX_CACHE_BENCH_RUNS", 10)
	latency := time.Duration(envInt("AGENTBOX_CACHE_BENCH_LATENCY_MS", 200)) * time.Millisecond
	sfx := randSuffix()
	query := "缓存收益 " + sfx
	started := time.Now().UTC()
	var samples []benchSample
	var groups []benchGroup
	for _, g := range []struct {
		name  string
		cache bool
	}{{"cache_on", true}, {"cache_off", false}} {
		t.Run(g.name, func(t *testing.T) {
			fu := fakeupstream.New()
			t.Cleanup(fu.Close)
			fu.SetLatency(fakeupstream.Search, latency)
			fu.SetLatency(fakeupstream.Fetch, latency)
			pages := make([]string, 3)
			for i := range pages {
				pages[i] = cachePage(fu, fmt.Sprintf("/bench-%s/%s/%d", sfx, g.name, i+1), ccHeader("max-age=3600"))
			}
			h := newHarness(t)
			cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
			if g.cache {
				cfg.RedisAddr = addr
			}
			h.start(cfg)
			for run := 1; run <= runs; run++ {
				id := h.submit(fmt.Sprintf("bench-%s-%d", g.name, run), spec(nil, "bench", nil, sleepStep(600000)))
				sock := h.waitAttemptSocket(id)
				type req struct{ kind, path, body string }
				reqs := []req{{"search", "/v1/search", `{"query":"` + query + `","max_results":3}`}}
				for _, p := range pages {
					reqs = append(reqs, req{"fetch", "/v1/fetch", `{"url":"` + p + `"}`})
				}
				for i, r := range reqs {
					callID := fmt.Sprintf("bench/r%d/%s/%d", run, r.kind, i+1)
					t0 := time.Now()
					resp, err := gwDo(sock, http.MethodPost, r.path, callID, r.body)
					dur := time.Since(t0)
					if err != nil || resp.Status != http.StatusOK {
						t.Fatalf("%s：%+v %v", callID, resp, err)
					}
					c := cacheCallRow(t, h.dsn, id, callID)
					samples = append(samples, benchSample{Group: g.name, Run: run, CallID: callID, Kind: r.kind, Source: c.Source,
						LatencyMs: float64(dur.Microseconds()) / 1000})
				}
				if st, code := h.cancelTask(id, fmt.Sprintf("bench-%s-%d-cancel", g.name, run)); st != http.StatusOK {
					t.Fatalf("取消 = %d %s", st, code)
				}
				h.waitTerminal(id)
			}
			bg := benchGroup{Name: g.name, UpstreamCalls: fu.Count(fakeupstream.Search) + fu.Count(fakeupstream.Fetch)}
			for _, s := range samples {
				if s.Group == g.name {
					bg.Calls++
					if s.Source == "cache" {
						bg.Hits++
					}
				}
			}
			if g.cache {
				bg.Metrics = h.cacheMetrics()
			}
			groups = append(groups, bg)
			h.finish()
		})
	}
	if t.Failed() {
		return
	}
	report := benchReport(started, runs, latency, addr, groups, samples)
	if err := os.WriteFile(out, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("缓存收益测量写入 %s：\n%s", out, report)
}

// latencyStats 返回样本延迟的 n、最小、p50、p90、最大与平均（毫秒；分位数取最近秩）。
func latencyStats(xs []float64) string {
	if len(xs) == 0 {
		return "| 0 | – | – | – | – | – |"
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	rank := func(p float64) float64 {
		i := int(p*float64(len(s))+0.999999) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	sum := 0.0
	for _, x := range s {
		sum += x
	}
	return fmt.Sprintf("| %d | %.1f | %.1f | %.1f | %.1f | %.1f |", len(s), s[0], rank(0.5), rank(0.9), s[len(s)-1], sum/float64(len(s)))
}

func benchReport(started time.Time, runs int, latency time.Duration, redisAddr string, groups []benchGroup, samples []benchSample) string {
	var b strings.Builder
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		kernel = []byte("未知")
	}
	host, err := os.Hostname()
	if err != nil {
		host = "未知"
	}
	fmt.Fprintf(&b, "### 环境\n\n- 开始时间（UTC）：%s\n- 主机：%s；内核 %s；%s/%s，%d 个逻辑 CPU；%s\n",
		started.Format(time.RFC3339), host, strings.TrimSpace(string(kernel)), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(&b, "- Redis：%s；PostgreSQL：AGENTBOX_TEST_DATABASE_URL（本机）\n", redisAddr)
	fmt.Fprintf(&b, "- 上游：进程内 fake upstream（127.0.0.1），搜索与抓取的每个正常回复前固定延迟 %s\n", latency)
	fmt.Fprintf(&b, "- 每组 %d 次重复；每次重复 = 新任务（新的逻辑任务与调用 ID）依次发出 1 个搜索 + 3 个抓取（抓取页面 Cache-Control: max-age=3600）；延迟为测试在 attempt 的 Gateway socket 上看到的端到端时间\n\n", runs)
	b.WriteString("### 汇总\n\n| 组 | 调用数 | 命中（source = cache） | 命中率 | 上游请求数 | /status 缓存指标 |\n|---|---|---|---|---|---|\n")
	for _, g := range groups {
		m := "（缓存关闭）"
		if g.Metrics != nil {
			keys := slices.Sorted(maps.Keys(g.Metrics))
			var parts []string
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%d", k, g.Metrics[k]))
			}
			m = strings.Join(parts, " ")
		}
		rate := 0.0
		if g.Calls > 0 {
			rate = float64(g.Hits) / float64(g.Calls)
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %.0f%% | %d | %s |\n", g.Name, g.Calls, g.Hits, rate*100, g.UpstreamCalls, m)
	}
	if len(groups) == 2 && groups[1].UpstreamCalls > 0 {
		fmt.Fprintf(&b, "\n上游请求减少：%d → %d（%.0f%%）。\n", groups[1].UpstreamCalls, groups[0].UpstreamCalls,
			100*(1-float64(groups[0].UpstreamCalls)/float64(groups[1].UpstreamCalls)))
	}
	b.WriteString("\n### 延迟分布（毫秒）\n\n| 样本 | n | 最小 | p50 | p90 | 最大 | 平均 |\n|---|---|---|---|---|---|---|\n")
	sel := func(f func(s benchSample) bool) []float64 {
		var xs []float64
		for _, s := range samples {
			if f(s) {
				xs = append(xs, s.LatencyMs)
			}
		}
		return xs
	}
	for _, row := range []struct {
		name string
		f    func(s benchSample) bool
	}{
		{"缓存开：命中（cache）", func(s benchSample) bool { return s.Group == "cache_on" && s.Source == "cache" }},
		{"缓存开：未命中（upstream）", func(s benchSample) bool { return s.Group == "cache_on" && s.Source == "upstream" }},
		{"缓存关：全部（upstream）", func(s benchSample) bool { return s.Group == "cache_off" }},
		{"命中：搜索", func(s benchSample) bool { return s.Source == "cache" && s.Kind == "search" }},
		{"命中：抓取", func(s benchSample) bool { return s.Source == "cache" && s.Kind == "fetch" }},
		{"上游：搜索", func(s benchSample) bool { return s.Source == "upstream" && s.Kind == "search" }},
		{"上游：抓取", func(s benchSample) bool { return s.Source == "upstream" && s.Kind == "fetch" }},
	} {
		fmt.Fprintf(&b, "| %s %s\n", row.name, latencyStats(sel(row.f)))
	}
	b.WriteString("\n### 原始数据\n\n| 组 | 重复 | 调用 ID | 类别 | 来源 | 延迟（ms） |\n|---|---|---|---|---|---|\n")
	for _, s := range samples {
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %.1f |\n", s.Group, s.Run, s.CallID, s.Kind, s.Source, s.LatencyMs)
	}
	return b.String()
}

// ---- M4 Plan 12 Task 9：会话（基本流、工具额度、D5 与恢复、awaiting_input、用户隔离；E28–E33） ----
//
// 会话 Worker 是脚本化的 Go 程序 tests/e2e/sessionworker（CGO_ENABLED=0），行为由每条消息正文中的 key=value 选项选择
// （script=…、searches=N、sleep_ms=N、hold_ms=N、state=ref）。非 root 用例经 agentbox-e2e（procprov，不隔离）运行，
// SIGKILL 与重启都是真实的；Worker 的观察记录写在会话 workspace 的 .sw/log.jsonl。TestRealE2x/E3x 以 root 在真实隔离
// 环境中运行 cmd/agentbox（会话 Worker 复制到 rootfs.WorkerDir）。

var (
	swOnce sync.Once
	swDir  string
	swPath string
	swErr  error
)

// sessionWorkerBinary 以 CGO_ENABLED=0 构建 tests/e2e/sessionworker（每次 go test 一次）。
func sessionWorkerBinary(t *testing.T) string {
	t.Helper()
	swOnce.Do(func() {
		if swDir, swErr = os.MkdirTemp("", "agentbox-sw-"); swErr != nil {
			return
		}
		if swErr = os.Chmod(swDir, 0o755); swErr != nil {
			return
		}
		swPath = filepath.Join(swDir, "sessionworker")
		cmd := exec.Command(goBinary(), "build", "-o", swPath, "./sessionworker")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			swErr = fmt.Errorf("构建 sessionworker: %v\n%s", err, out)
		}
	})
	if swErr != nil {
		t.Fatal(swErr)
	}
	return swPath
}

// sessSys 是启用会话的系统级装置：agentbox-e2e（或 root 时的 cmd/agentbox）+ fake upstream + 会话 Worker。
type sessSys struct {
	*sysHarness
	flags []string
	real  bool
}

// newSessionSys：非 root、procprov。extra 是额外的 server 标志（每次启动都使用）。
func newSessionSys(t *testing.T, extra ...string) *sessSys {
	t.Helper()
	s := newSys(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	ss := &sessSys{sysHarness: s,
		flags: append([]string{"--fake-upstream", fu.URL(), "--session-worker", sessionWorkerBinary(t)}, extra...)}
	t.Cleanup(ss.dumpOnFailure)
	return ss
}

// dumpOnFailure 在失败时输出会话、incarnation、环境与 UID 范围的存储状态，以及各 server 除访问日志之外的日志
// （轮询请求会挤掉 tail 中有用的部分）。
func (s *sessSys) dumpOnFailure() {
	if !s.t.Failed() {
		return
	}
	ctx := context.Background()
	if c, err := pgx.Connect(ctx, s.dsn); err == nil {
		for _, q := range []string{
			`SELECT session_id, status, COALESCE(last_error, ''), COALESCE(current_incarnation_id, ''), COALESCE(uid_range_id, ''),
				COALESCE(current_task_id, ''), COALESCE(blocked_by_task_id, '') FROM sessions`,
			`SELECT incarnation_id, session_id, env_id, status, COALESCE(end_reason, '') FROM incarnations ORDER BY started_at`,
			`SELECT env_id, kind, COALESCE(attempt_id, ''), status, stopped_at IS NOT NULL, cleanup_state, COALESCE(uid_range_id, ''),
				COALESCE(cleanup_error, '') FROM environments ORDER BY created_at`,
			`SELECT uid_range_id, state, COALESCE(owner_id, ''), COALESCE(allocation_id, '') FROM uid_ranges WHERE state <> 'free'`,
			`SELECT task_id, status, status_reason, COALESCE(session_id, '') FROM tasks ORDER BY created_at`,
		} {
			rows, err := c.Query(ctx, q)
			if err != nil {
				s.t.Logf("%s: %v", q, err)
				continue
			}
			var lines []string
			for rows.Next() {
				vals, _ := rows.Values()
				lines = append(lines, fmt.Sprint(vals...))
			}
			rows.Close()
			s.t.Logf("%s\n  %s", strings.Fields(q)[1], strings.Join(lines, "\n  "))
		}
		_ = c.Close(ctx)
	}
	for i, p := range s.procs {
		var keep []string
		for _, line := range strings.Split(p.logs.tail(8<<20), "\n") {
			if !strings.Contains(line, `"msg":"api request"`) {
				keep = append(keep, line)
			}
		}
		if len(keep) > 300 {
			keep = keep[len(keep)-300:]
		}
		s.t.Logf("server #%d 日志（不含访问日志，末尾）：\n%s", i+1, strings.Join(keep, "\n"))
	}
}

func (s *sessSys) startS() *serverProc {
	s.t.Helper()
	if s.real {
		return s.startReal(s.flags...)
	}
	return s.start("", s.flags...)
}

// kill 以 SIGKILL 杀死当前 server，等待其数据库连接全部结束。
func (s *sessSys) kill() {
	s.t.Helper()
	if err := s.srv.cmd.Process.Kill(); err != nil {
		s.t.Fatal(err)
	}
	if ps := s.waitExit(s.srv); !killedBySIGKILL(ps) {
		s.t.Fatalf("server 退出状态 %v，期望被 SIGKILL", ps)
	}
	s.waitConnsGone()
}

func (s *sessSys) end() {
	s.t.Helper()
	if s.real {
		s.finishReal()
		return
	}
	s.finish()
}

func (s *sessSys) row(sql string, args []any, dest ...any) {
	s.t.Helper()
	pgQueryRow(s.t, s.dsn, sql, args, dest...)
}

// swRec 是会话 Worker 的一条观察记录。
type swRec map[string]any

func (r swRec) str(k string) string { v, _ := r[k].(string); return v }

func (r swRec) num(k string) int64 { v, _ := r[k].(float64); return int64(v) }

// swLog 读取会话 workspace 中 Worker 的观察记录（.sw/log.jsonl）。
func (s *sessSys) swLog(sessionID string) []swRec {
	s.t.Helper()
	b, err := os.ReadFile(filepath.Join(s.dir, "sessions", sessionID, "workspace", ".sw", "log.jsonl"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		s.t.Fatal(err)
	}
	var out []swRec
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var r swRec
		if err := json.Unmarshal(line, &r); err != nil {
			s.t.Fatalf("Worker 记录 %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// swFind 返回满足条件的记录。
func swFind(recs []swRec, f func(swRec) bool) []swRec {
	var out []swRec
	for _, r := range recs {
		if f(r) {
			out = append(out, r)
		}
	}
	return out
}

func swEvent(event string, kv ...string) func(swRec) bool {
	return func(r swRec) bool {
		if r.str("event") != event {
			return false
		}
		for i := 0; i+1 < len(kv); i += 2 {
			if r.str(kv[i]) != kv[i+1] {
				return false
			}
		}
		return true
	}
}

// sessUser 是一个已登录的用户（cookie 跨 server 重启有效）。
type sessUser struct {
	s      *sessSys
	cookie string
}

func (s *sessSys) user(name string) *sessUser {
	s.t.Helper()
	resp, err := http.Post(s.srv.base+"/auth/register", "application/json",
		strings.NewReader(`{"username":"`+name+`","password":"correct horse battery"}`))
	if err != nil {
		s.t.Fatal(err)
	}
	_ = resp.Body.Close()
	u := &sessUser{s: s}
	for _, c := range resp.Cookies() {
		if c.Name == "agentbox_session" {
			u.cookie = c.Name + "=" + c.Value
		}
	}
	if resp.StatusCode != http.StatusCreated || u.cookie == "" {
		s.t.Fatalf("注册 %s = %d", name, resp.StatusCode)
	}
	return u
}

func (u *sessUser) do(method, path, body string) (int, []byte) {
	u.s.t.Helper()
	req, err := http.NewRequest(method, u.s.srv.base+path, strings.NewReader(body))
	if err != nil {
		u.s.t.Fatal(err)
	}
	req.Header.Set("Cookie", u.cookie)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		u.s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		u.s.t.Fatal(err)
	}
	return resp.StatusCode, b
}

func (u *sessUser) createSession(requestID string) string {
	u.s.t.Helper()
	code, b := u.do("POST", "/sessions", `{"request_id":"`+requestID+`"}`)
	var v api.SessionView
	if code != http.StatusCreated || json.Unmarshal(b, &v) != nil || v.SessionID == "" {
		u.s.t.Fatalf("POST /sessions = %d %s", code, b)
	}
	return v.SessionID
}

func (u *sessUser) message(sessionID, requestID, text string) api.CreateTurnResult {
	u.s.t.Helper()
	body, _ := json.Marshal(map[string]any{"request_id": requestID, "text": text, "deep_research": false})
	code, b := u.do("POST", "/sessions/"+sessionID+"/messages", string(body))
	var r api.CreateTurnResult
	if code != http.StatusAccepted || json.Unmarshal(b, &r) != nil || r.TurnID == "" {
		u.s.t.Fatalf("POST messages %q = %d %s", text, code, b)
	}
	return r
}

// control 发送 turn 控制（stop | continue | finish）。
func (u *sessUser) control(turnID, action, requestID string) {
	u.s.t.Helper()
	if code, b := u.do("POST", "/turns/"+turnID+"/"+action, `{"request_id":"`+requestID+`"}`); code != http.StatusAccepted {
		u.s.t.Fatalf("POST /turns/%s/%s = %d %s", turnID, action, code, b)
	}
}

func (u *sessUser) turn(sessionID, turnID string) (api.TurnView, bool) {
	u.s.t.Helper()
	code, b := u.do("GET", "/sessions/"+sessionID+"/turns", "")
	var r struct {
		Turns []api.TurnView `json:"turns"`
	}
	if code != http.StatusOK || json.Unmarshal(b, &r) != nil {
		u.s.t.Fatalf("GET turns = %d %s", code, b)
	}
	for _, v := range r.Turns {
		if v.TurnID == turnID {
			return v, true
		}
	}
	return api.TurnView{}, false
}

func (u *sessUser) waitTurn(sessionID, turnID, what string, cond func(api.TurnView) bool) api.TurnView {
	u.s.t.Helper()
	var v api.TurnView
	eventuallyWithin(u.s.t, "turn "+turnID+" "+what, waitLimit, 100*time.Millisecond, func() bool {
		var ok bool
		v, ok = u.turn(sessionID, turnID)
		return ok && cond(v)
	})
	return v
}

func turnIs(status string) func(api.TurnView) bool {
	return func(v api.TurnView) bool { return v.Status == status }
}

// closeSession 删除会话并等待它 closed（workspace 删除、UID 范围归还）。
func (u *sessUser) closeSession(sessionID string) {
	u.s.t.Helper()
	if code, b := u.do("DELETE", "/sessions/"+sessionID, ""); code != http.StatusAccepted {
		u.s.t.Fatalf("DELETE /sessions/%s = %d %s", sessionID, code, b)
	}
	u.s.waitSession(sessionID, "closed")
}

// waitSession 等待会话的存储状态（sessions.status）。
func (s *sessSys) waitSession(sessionID, status string) {
	s.t.Helper()
	eventually(s.t, "会话 "+sessionID+" 进入 "+status, func() bool {
		var st string
		s.row("SELECT status FROM sessions WHERE session_id = $1", []any{sessionID}, &st)
		return st == status
	})
}

// sse 以该用户读取整个会话的事件流（Last-Event-ID = 0），直到 until 成立或期限到；返回每条事件的原始 JSON。
func (u *sessUser) sse(sessionID string, until func([]json.RawMessage) bool) []json.RawMessage {
	u.s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", u.s.srv.base+"/sessions/"+sessionID+"/events", nil)
	if err != nil {
		u.s.t.Fatal(err)
	}
	req.Header.Set("Cookie", u.cookie)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		u.s.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		u.s.t.Fatalf("GET events = %d", resp.StatusCode)
	}
	var out []json.RawMessage
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if data, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "data: "); ok {
			out = append(out, json.RawMessage(data))
			if until(out) {
				return out
			}
		}
		if err != nil {
			u.s.t.Fatalf("事件流在条件成立之前结束（%v），已读 %d 条", err, len(out))
		}
	}
}

func hasEventType(typ string) func([]json.RawMessage) bool {
	return func(evs []json.RawMessage) bool {
		for _, e := range evs {
			var v struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(e, &v) == nil && v.Type == typ {
				return true
			}
		}
		return false
	}
}

// internalKeys 返回 JSON 中任意深度出现的内部字段名（用户视图不得出现）。
func internalKeys(v any) []string {
	var out []string
	switch x := v.(type) {
	case map[string]any:
		for k, sub := range x {
			switch k {
			case "internal", "attempt_id", "call_id", "worker_seq", "usage", "model", "task_seq":
				out = append(out, k)
			}
			if strings.HasPrefix(k, "cost") || strings.Contains(k, "price") {
				out = append(out, k)
			}
			out = append(out, internalKeys(sub)...)
		}
	case []any:
		for _, sub := range x {
			out = append(out, internalKeys(sub)...)
		}
	}
	return out
}

// assertSessionSeqContiguous：会话的 session_seq（task 事件与会话事件合计）从 1 起连续。
func (s *sessSys) assertSessionSeqContiguous(sessionID string) {
	s.t.Helper()
	var n, lo, hi int64
	s.row(`SELECT count(*), COALESCE(min(q), 0), COALESCE(max(q), 0) FROM (SELECT session_seq AS q FROM events WHERE session_id = $1
		UNION ALL SELECT session_seq FROM session_events WHERE session_id = $1) x`, []any{sessionID}, &n, &lo, &hi)
	if n == 0 || lo != 1 || hi != n {
		s.t.Fatalf("会话 %s 的 session_seq 不连续：%d 条，范围 %d..%d", sessionID, n, lo, hi)
	}
}

// TestSessionBasicFlow：创建会话 → 两条消息（第二条读到第一条提交的会话状态：base checkpoint、计数与 workspace 文件）
// → 事件流 session_seq 连续、用户视图没有内部字段；另一用户读写该会话一律 404；删除会话后环境停止、workspace 删除、
// UID 范围归还，不变量（含 I9、I10）成立。
func TestSessionBasicFlow(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	alice, bob := s.user("alice"), s.user("bob")
	sid := alice.createSession("cs-1")
	r1 := alice.message(sid, "m-1", "searches=2")
	v1 := alice.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	r2 := alice.message(sid, "m-2", "searches=1")
	v2 := alice.waitTurn(sid, r2.TurnID, "成功", turnIs("succeeded"))
	if v1.Summary != "count=1 task="+r1.TurnID || v2.Summary != "count=2 task="+r2.TurnID {
		t.Fatalf("第二条消息应读到第一条提交的会话状态：%q / %q", v1.Summary, v2.Summary)
	}
	if v1.ToolCallsUsed != 2 || v1.ToolCallLimit != 30 || v2.ToolCallsUsed != 1 {
		t.Errorf("工具额度 = %d/%d、%d", v1.ToolCallsUsed, v1.ToolCallLimit, v2.ToolCallsUsed)
	}
	logs := s.swLog(sid)
	st1 := swFind(logs, swEvent("task_start", "task_id", r1.TurnID))
	st2 := swFind(logs, swEvent("task_start", "task_id", r2.TurnID))
	if len(st1) != 1 || len(st2) != 1 || st1[0].str("base_session_checkpoint_id") != "" ||
		st2[0].str("base_session_checkpoint_id") != "sc-"+st1[0].str("attempt_id") || st1[0].num("pid") != st2[0].num("pid") {
		t.Fatalf("task_start 记录：%v / %v（两轮应在同一 incarnation 中，第二轮以第一轮的会话 checkpoint 为 base）", st1, st2)
	}
	if note, err := os.ReadFile(filepath.Join(s.dir, "sessions", sid, "workspace", "session", "note.txt")); err != nil ||
		string(note) != "count=2 task="+r2.TurnID {
		t.Errorf("workspace 会话文件 = %q, %v", note, err)
	}
	// 用户隔离：另一用户读写该会话与其 turn 一律 404。
	for _, req := range []struct{ method, path, body string }{
		{"GET", "/sessions/" + sid, ""}, {"GET", "/sessions/" + sid + "/turns", ""}, {"GET", "/sessions/" + sid + "/events", ""},
		{"POST", "/sessions/" + sid + "/messages", `{"request_id":"x","text":"hi","deep_research":false}`},
		{"POST", "/turns/" + r1.TurnID + "/stop", `{"request_id":"y"}`}, {"DELETE", "/sessions/" + sid, ""},
	} {
		if code, b := bob.do(req.method, req.path, req.body); code != http.StatusNotFound {
			t.Errorf("用户 B %s %s = %d %s，期望 404", req.method, req.path, code, b)
		}
	}
	// 事件流：用户视图没有内部字段，seq 递增；存储的 session_seq 连续。
	evs := alice.sse(sid, func(evs []json.RawMessage) bool {
		n := 0
		for _, e := range evs {
			var v struct {
				Type   string `json:"type"`
				TurnID string `json:"turn_id"`
			}
			if json.Unmarshal(e, &v) == nil && v.Type == "turn_result" {
				n++
			}
		}
		return n == 2
	})
	last := int64(0)
	for _, e := range evs {
		var v map[string]any
		if err := json.Unmarshal(e, &v); err != nil {
			t.Fatal(err)
		}
		if keys := internalKeys(v); len(keys) > 0 {
			t.Errorf("用户事件含内部字段 %v：%s", keys, e)
		}
		seq := int64(v["seq"].(float64))
		if seq <= last {
			t.Errorf("事件 seq 不递增：%d 之后 %d", last, seq)
		}
		last = seq
	}
	s.assertSessionSeqContiguous(sid)
	alice.closeSession(sid)
	if _, err := os.Stat(filepath.Join(s.dir, "sessions", sid)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("关闭后会话目录仍存在：%v", err)
	}
	s.end()
}

// TestToolBudgetAcrossRestart：Worker 调用 35 次搜索——第 31 次起 429 tool_budget_exhausted，turn 仍 succeeded，
// tool_calls_used = 30；第 20 次调用之后 SIGKILL server 并重启：计数延续（调用以同一 call id 重放不计数），恢复后的
// attempt 至多再新建 10 个调用。
func TestToolBudgetAcrossRestart(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r := u.message(sid, "m-1", "searches=35 sleep_ms=100")
	var before int
	eventually(t, "至少 20 次搜索调用已登记", func() bool {
		s.row("SELECT count(*) FROM calls WHERE task_id = $1", []any{r.TurnID}, &before)
		return before >= 20
	})
	s.kill()
	s.row("SELECT count(*) FROM calls WHERE task_id = $1", []any{r.TurnID}, &before)
	if before >= 30 {
		t.Fatalf("SIGKILL 之前已登记 %d 个调用，测试前提不成立", before)
	}
	s.startS()
	v := u.waitTurn(sid, r.TurnID, "成功", turnIs("succeeded"))
	var calls, used int
	s.row("SELECT count(*) FROM calls WHERE task_id = $1", []any{r.TurnID}, &calls)
	s.row("SELECT tool_calls_used FROM budgets WHERE task_id = $1", []any{r.TurnID}, &used)
	if v.ToolCallsUsed != 30 || used != 30 || calls != 30 || calls-before > 10 {
		t.Fatalf("工具额度：Turn.tool_calls_used = %d，budgets = %d，调用 %d 个（重启前 %d 个）", v.ToolCallsUsed, used, calls, before)
	}
	searches := swFind(s.swLog(sid), swEvent("search", "task_id", r.TurnID))
	attempts := map[string]bool{}
	var exhausted int
	for _, rec := range searches {
		attempts[rec.str("attempt_id")] = true
		if rec.str("error") != "" { // SIGKILL 时在途的请求：连接中断，没有状态码
			continue
		}
		switch n := rec.num("n"); {
		case n > 30 && rec.num("status") != http.StatusTooManyRequests:
			t.Errorf("第 %d 次搜索 = %v，期望 429", n, rec["status"])
		case n > 30:
			exhausted++
		case rec.num("status") != http.StatusOK:
			t.Errorf("第 %d 次搜索 = %v，期望 200（新建或重放）", n, rec["status"])
		}
	}
	if len(attempts) != 2 || exhausted != 5 {
		t.Errorf("搜索记录：%d 个 attempt，%d 次 429（期望 2 个 attempt、最后一个 attempt 的第 31–35 次 429）", len(attempts), exhausted)
	}
	u.closeSession(sid)
	s.end()
}

// TestSessionStopSupersedeRestore（D5 与恢复）：turn 1 运行中 stop → paused（turn_stopped 及其之前的事件可见）→ 新消息
// → turn 1 cancelled/superseded、turn 2 运行并带 carryover；restore turn 1 → turn 3 的首个 task_start.resume 为种子
// seed-…，额度重新为 30。
func TestSessionStopSupersedeRestore(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=2 hold_ms=120000")
	eventually(t, "turn 1 完成两次搜索", func() bool {
		return len(swFind(s.swLog(sid), swEvent("search", "task_id", r1.TurnID))) == 2
	})
	u.control(r1.TurnID, "stop", "st-1")
	v1 := u.waitTurn(sid, r1.TurnID, "暂停", turnIs("paused"))
	if v1.ToolCallsUsed != 2 {
		t.Errorf("暂停的 turn 1 tool_calls_used = %d", v1.ToolCallsUsed)
	}
	evs := u.sse(sid, hasEventType("turn_stopped"))
	if !hasEventType("turn_created")(evs) {
		t.Errorf("turn_stopped 之前的事件不可见：%s", evs)
	}
	r2 := u.message(sid, "m-2", "searches=1")
	if r2.SupersededTurnID != r1.TurnID {
		t.Fatalf("新消息应取代暂停的 turn 1：%+v", r2)
	}
	v1 = u.waitTurn(sid, r1.TurnID, "被取代", turnIs("cancelled"))
	if v1.StatusReason != "superseded" || !v1.Restorable {
		t.Errorf("turn 1 = %s/%s，restorable %v", v1.Status, v1.StatusReason, v1.Restorable)
	}
	u.waitTurn(sid, r2.TurnID, "成功", turnIs("succeeded"))
	st2 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r2.TurnID))
	if len(st2) != 1 {
		t.Fatalf("turn 2 的 task_start：%v", st2)
	}
	carry, _ := st2[0]["carryover"].(map[string]any)
	if carry["task_id"] != r1.TurnID || len(fmt.Sprint(carry["checkpoint_ref"])) != 64 {
		t.Errorf("turn 2 的 carryover = %v，期望指向 turn 1 的最新 checkpoint", st2[0]["carryover"])
	}
	code, b := u.do("POST", "/turns/"+r1.TurnID+"/restore", `{"request_id":"rs-1"}`)
	var r3 api.CreateTurnResult
	if code != http.StatusAccepted || json.Unmarshal(b, &r3) != nil || r3.TurnID == "" {
		t.Fatalf("POST restore = %d %s", code, b)
	}
	v3 := u.waitTurn(sid, r3.TurnID, "成功", turnIs("succeeded"))
	st3 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r3.TurnID))
	att1 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r1.TurnID))
	if len(st3) == 0 || len(att1) == 0 || st3[0].str("resume_checkpoint_id") != "seed-tc-"+att1[0].str("attempt_id") ||
		st3[0].str("restored_from_task_id") != r1.TurnID {
		t.Fatalf("turn 3 的首个 task_start = %v，期望从种子 seed-tc-%s 继续", st3, att1)
	}
	if v3.ToolCallLimit != 30 || v3.ToolCallsUsed != 2 || v3.RestoredFromTurnID != r1.TurnID { // 恢复的 turn 沿用源正文（2 次搜索），额度自 0 计
		t.Errorf("turn 3 = %+v，期望新的 30 次额度", v3)
	}
	u.closeSession(sid)
	s.end()
}

// TestSessionAwaitingInput：ask_user → turn awaiting_input，run slot 归还（--run-slots 1 下另一用户的 turn 照常完成）
// → answer → 同一 turn 从原位置继续并成功，Worker 收到 directive.answer（带待答提问的 question_id）。
func TestSessionAwaitingInput(t *testing.T) {
	s := newSessionSys(t, "--run-slots", "1")
	s.startS()
	alice, bob := s.user("alice"), s.user("bob")
	sa, sb := alice.createSession("cs-a"), bob.createSession("cs-b")
	r := alice.message(sa, "m-1", "script=ask_user")
	alice.waitTurn(sa, r.TurnID, "等待回答", turnIs("awaiting_input"))
	rb := bob.message(sb, "m-b", "searches=1")
	bob.waitTurn(sb, rb.TurnID, "成功（run slot 已归还）", turnIs("succeeded"))
	if code, b := alice.do("POST", "/turns/"+r.TurnID+"/answer",
		`{"request_id":"an-1","answers":[{"question_id":"1","choice":"A"}]}`); code != http.StatusAccepted {
		t.Fatalf("POST answer = %d %s", code, b)
	}
	alice.waitTurn(sa, r.TurnID, "回答后成功", turnIs("succeeded"))
	starts := swFind(s.swLog(sa), swEvent("task_start", "task_id", r.TurnID))
	if len(starts) != 2 {
		t.Fatalf("task_start 记录：%v", starts)
	}
	dir, _ := starts[1]["directive"].(map[string]any)
	answers, _ := dir["answers"].([]any)
	if dir["kind"] != "answer" || dir["question_id"] != "q-"+r.TurnID || len(answers) != 1 || starts[1].str("resume_checkpoint_id") != "tc-"+starts[0].str("attempt_id") {
		t.Fatalf("回答后的 task_start = %v，期望 directive.answer{question_id = q-%s} 并从提问前的 checkpoint 继续", starts[1], r.TurnID)
	}
	alice.closeSession(sa)
	bob.closeSession(sb)
	s.end()
}

// ---- 会话：root、真实隔离（cmd/agentbox + provider/local；E28–E33） ----

var (
	swInstallOnce sync.Once
	swInstallErr  error
)

// installSessionWorker 把 sessionworker 复制到 rootfs.WorkerDir（默认模板包含该目录），返回环境内的路径。
func installSessionWorker(t *testing.T) string {
	t.Helper()
	src := sessionWorkerBinary(t)
	dst := filepath.Join(rootfs.WorkerDir, "sessionworker")
	swInstallOnce.Do(func() { // 写临时文件后改名：旧二进制仍在运行时（ETXTBSY）也能替换
		b, err := os.ReadFile(src)
		tmp := dst + ".tmp"
		if err == nil {
			err = os.WriteFile(tmp, b, 0o755)
		}
		if err == nil {
			err = os.Chmod(tmp, 0o755)
		}
		if err == nil {
			err = os.Rename(tmp, dst)
		}
		swInstallErr = err
	})
	if swInstallErr != nil {
		t.Fatalf("安装 sessionworker 到 %s: %v", dst, swInstallErr)
	}
	return dst
}

// newRealSessionSys：root，cmd/agentbox 以生产启动器运行；模型与搜索指向 fake upstream（用户账号需要模型上游），
// 会话 Worker 为复制进 rootfs.WorkerDir 的 sessionworker。
func newRealSessionSys(t *testing.T, extra ...string) *sessSys {
	t.Helper()
	s := newRealSys(t)
	worker := installSessionWorker(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u, err := url.Parse(fu.URL())
	if err != nil {
		t.Fatal(err)
	}
	ss := &sessSys{sysHarness: s, real: true, flags: append([]string{
		"--model-base-url", fu.URL() + "/v1", "--model-name", fakeupstream.Model,
		"--user-orchestrator-model", fakeupstream.Model, "--user-worker-model", fakeupstream.Model,
		"--search-provider", upstream.SearchFake, "--search-base-url", fu.URL(), "--upstream-allow-private", u.Host,
		"--session-worker-argv", worker,
	}, extra...)}
	t.Cleanup(ss.dumpOnFailure)
	return ss
}

// incarnationOf 返回会话最近建立的 incarnation 与其环境。
func (s *sessSys) incarnationOf(sessionID string) (incID, envID, status, endReason string) {
	s.t.Helper()
	s.row(`SELECT incarnation_id, env_id, status, COALESCE(end_reason, '') FROM incarnations WHERE session_id = $1
		ORDER BY started_at DESC LIMIT 1`, []any{sessionID}, &incID, &envID, &status, &endReason)
	return
}

func (s *sessSys) envCgroupDir(envID string) string {
	return filepath.Join(s.installCgroup(), "env-"+envID)
}

// cgroupFrozen 读取环境 cgroup 的 cgroup.events 中的 frozen 值。
func cgroupFrozen(dir string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, "cgroup.events"))
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "frozen "); ok {
			return strings.TrimSpace(v) == "1", nil
		}
	}
	return false, errors.New("cgroup.events 没有 frozen 行")
}

// sessionStateEvents 返回会话的 session_state 事件中的状态序列（按 session_seq）。
func (s *sessSys) sessionStateEvents(sessionID string) []string {
	s.t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT payload->>'state' FROM session_events WHERE session_id = $1 AND type = 'session_state'
		ORDER BY session_seq`, sessionID)
	if err != nil {
		s.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		s.t.Fatal(err)
	}
	return out
}

// TestRealE28SessionColdRestore（E28）：会话 idle 时 Worker 死亡（exit_when_idle）→ evicted → 新消息 → restoring →
// 冷恢复：同一 workspace（Worker 读到上次写入 /workspace/session 的文件）、新的 incarnation 与入口 socket、同一 UID
// 范围。分别以 inline state 与 state_ref 的会话 checkpoint 各运行一次；state_ref 时 Worker 从
// /run/agentbox/restore/<sha> 读取，ready 之后暂存目录被删除；恢复期间（ready 之前）连接 Gateway 被拒。
func TestRealE28SessionColdRestore(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	for _, mode := range []string{"inline", "ref"} {
		sid := u.createSession("cs-" + mode)
		r1 := u.message(sid, "m1-"+mode, "searches=1 state="+mode+" script=exit_when_idle")
		u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
		s.waitSession(sid, "evicted")
		inc1, env1, st1, _ := s.incarnationOf(sid)
		if st1 != "ended" {
			t.Fatalf("%s：Worker 退出后 incarnation %s = %s", mode, inc1, st1)
		}
		r2 := u.message(sid, "m2-"+mode, "searches=1")
		v2 := u.waitTurn(sid, r2.TurnID, "冷恢复后成功", turnIs("succeeded"))
		if v2.Summary != "count=2 task="+r2.TurnID {
			t.Errorf("%s：冷恢复后的会话状态 = %q", mode, v2.Summary)
		}
		inc2, env2, _, _ := s.incarnationOf(sid)
		var range1, range2 string
		s.row("SELECT uid_range_id FROM environments WHERE env_id = $1", []any{env1}, &range1)
		s.row("SELECT uid_range_id FROM environments WHERE env_id = $1", []any{env2}, &range2)
		if inc2 == inc1 || env2 == env1 || range1 != range2 || range1 == "" {
			t.Errorf("%s：恢复应建立新的 incarnation（入口 inc-<id>.sock）并沿用同一 UID 范围：%s/%s → %s/%s，范围 %s → %s",
				mode, inc1, env1, inc2, env2, range1, range2)
		}
		states := s.sessionStateEvents(sid)
		if i := slices.Index(states, "evicted"); i < 0 || !slices.Contains(states[i:], "restoring") {
			t.Errorf("%s：会话状态事件 = %v，期望 evicted 之后 restoring", mode, states)
		}
		starts := swFind(s.swLog(sid), func(r swRec) bool { return r.str("event") == "start" && r["resumed"] == true })
		att1 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r1.TurnID))
		if len(starts) != 1 || len(att1) != 1 {
			t.Fatalf("%s：恢复记录 %v，turn 1 记录 %v", mode, starts, att1)
		}
		rs := starts[0]
		if rs.str("checkpoint_id") != "sc-"+att1[0].str("attempt_id") || rs.str("note") != "count=1 task="+r1.TurnID ||
			rs["gateway_refused"] != true {
			t.Errorf("%s：恢复记录 = %v（期望 checkpoint sc-%s、读到上次写入的会话文件、恢复期间 Gateway 被拒）",
				mode, rs, att1[0].str("attempt_id"))
		}
		staged := rs.str("staged_state_path")
		if (mode == "ref") != strings.HasPrefix(staged, protocol.StagedStateDir) {
			t.Errorf("%s：staged_state_path = %q", mode, staged)
		}
		if _, err := os.Stat(filepath.Join(s.dir, "restore", env2)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s：ready 之后恢复暂存目录仍存在（%v）", mode, err)
		}
		u.closeSession(sid)
	}
	s.end()
}

// TestRealE29OutcomeLossAndReleaseTimeout（E29）：task_outcome 丢失一次 → Worker 以 task_outcome_query 查询后释放，
// turn 终态不变、incarnation 继续服务下一轮；Worker 永不发 task_released → T_release 后 incarnation 销毁
// （end_reason = release_timeout），turn 终态、会话指针与 fault_retries_used 不变，不重跑。
func TestRealE29OutcomeLossAndReleaseTimeout(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1 script=drop_outcome_once")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	eventually(t, "Worker 查询裁决后释放", func() bool {
		return len(swFind(s.swLog(sid), swEvent("task_outcome"))) == 1
	})
	logs := s.swLog(sid)
	if len(swFind(logs, swEvent("outcome_dropped"))) != 1 {
		t.Fatalf("Worker 应丢弃第一次 task_outcome：%v", logs)
	}
	inc1, _, _, _ := s.incarnationOf(sid)
	r2 := u.message(sid, "m-2", "searches=1 script=never_release")
	u.waitTurn(sid, r2.TurnID, "成功", turnIs("succeeded"))
	if inc, _, _, _ := s.incarnationOf(sid); inc != inc1 {
		t.Fatalf("查询恢复之后 incarnation 应继续服务：%s → %s", inc1, inc)
	}
	eventuallyWithin(t, "T_release 后 incarnation 销毁", 2*waitLimit, 100*time.Millisecond, func() bool {
		_, _, st, reason := s.incarnationOf(sid)
		return st == "ended" && reason == "release_timeout"
	})
	starts := swFind(s.swLog(sid), swEvent("task_start", "task_id", r2.TurnID))
	var status, pointer string
	var faults int64
	s.row("SELECT status, fault_retries_used FROM tasks WHERE task_id = $1", []any{r2.TurnID}, &status, &faults)
	s.row("SELECT COALESCE(latest_checkpoint_id, '') FROM session_progress WHERE session_id = $1", []any{sid}, &pointer)
	if len(starts) != 1 || status != "succeeded" || faults != 0 || pointer != "sc-"+starts[0].str("attempt_id") {
		t.Fatalf("释放超时不改变裁决：task_start %d 次，状态 %s，fault_retries_used %d，会话指针 %s", len(starts), status, faults, pointer)
	}
	u.closeSession(sid)
	s.end()
}

// TestRealE30LeakedChildFailsRelease（E30）：T1 释放前留下子进程 → 释放核验（进程表回到基线）失败 → incarnation
// 销毁；T2 在新的 incarnation 中运行；calls 中没有 T1 的 attempt 在 T2 期间登记的调用。
func TestRealE30LeakedChildFailsRelease(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1 script=leak_child")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	inc1, env1, _, _ := s.incarnationOf(sid)
	eventuallyWithin(t, "释放核验失败后 incarnation 销毁", 2*waitLimit, 100*time.Millisecond, func() bool {
		var st, reason string
		s.row("SELECT status, COALESCE(end_reason, '') FROM incarnations WHERE incarnation_id = $1", []any{inc1}, &st, &reason)
		return st == "ended" && reason == "release_timeout"
	})
	if len(swFind(s.swLog(sid), swEvent("leaked_child"))) != 1 {
		t.Fatal("Worker 没有留下子进程")
	}
	if pids := cgroupPids(s.envCgroupDir(env1)); len(pids) != 0 {
		t.Errorf("销毁之后环境 %s 仍有进程 %v", env1, pids)
	}
	r2 := u.message(sid, "m-2", "searches=1")
	u.waitTurn(sid, r2.TurnID, "在新 incarnation 中成功", turnIs("succeeded"))
	if inc2, _, _, _ := s.incarnationOf(sid); inc2 == inc1 {
		t.Fatalf("T2 应在新的 incarnation 中运行")
	}
	var late int
	s.row(`SELECT count(*) FROM calls c JOIN tasks t2 ON t2.task_id = $2 WHERE c.task_id = $1 AND c.created_at >= t2.created_at`,
		[]any{r1.TurnID, r2.TurnID}, &late)
	if late != 0 {
		t.Errorf("T1 在 T2 期间登记了 %d 个调用", late)
	}
	u.closeSession(sid)
	s.end()
}

// TestRealE31FreezeAndFreezeFailure（E31）：--session-idle-freeze 2s → frozen（会话事件出现时 cgroup.events 已是
// frozen 1）→ 新消息 thaw 并在同一 incarnation 中运行；冻结无法确认（cgroup.events 被替换为 frozen 0 的文件）→
// 驱逐（evicted），不记录 frozen。
func TestRealE31FreezeAndFreezeFailure(t *testing.T) {
	s := newRealSessionSys(t, "--session-idle-freeze", "2s")
	s.startS()
	u := s.user("alice")

	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	inc1, env1, _, _ := s.incarnationOf(sid)
	cg := s.envCgroupDir(env1)
	eventually(t, "会话冻结（事件出现时 cgroup 已冻结）", func() bool {
		if !slices.Contains(s.sessionStateEvents(sid), "frozen") {
			return false
		}
		frozen, err := cgroupFrozen(cg)
		if err != nil || !frozen {
			t.Fatalf("session_state{frozen} 已出现，但 cgroup.events frozen = %v（%v）", frozen, err)
		}
		return true
	})
	r2 := u.message(sid, "m-2", "searches=1")
	u.waitTurn(sid, r2.TurnID, "thaw 后成功", turnIs("succeeded"))
	if inc, _, _, _ := s.incarnationOf(sid); inc != inc1 {
		t.Fatalf("thaw 之后应在同一 incarnation 中运行：%s → %s", inc1, inc)
	}
	u.closeSession(sid)

	sid2 := u.createSession("cs-2")
	r3 := u.message(sid2, "m-3", "searches=1")
	u.waitTurn(sid2, r3.TurnID, "成功", turnIs("succeeded"))
	_, env2, _, _ := s.incarnationOf(sid2)
	events := filepath.Join(s.envCgroupDir(env2), "cgroup.events")
	fakeEvents := filepath.Join(t.TempDir(), "cgroup.events")
	if err := os.WriteFile(fakeEvents, []byte("populated 1\nfrozen 0\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mount(fakeEvents, events, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("以 bind 替换 %s: %v", events, err)
	}
	mounted := true
	unmount := func() {
		if mounted {
			_ = syscall.Unmount(events, syscall.MNT_DETACH)
			mounted = false
		}
	}
	defer unmount()
	eventuallyWithin(t, "冻结无法确认 → 驱逐", 2*waitLimit, 50*time.Millisecond, func() bool {
		var st string
		s.row("SELECT status FROM sessions WHERE session_id = $1", []any{sid2}, &st)
		if st == "evicting" || st == "evicted" {
			unmount()
		}
		return st == "evicted"
	})
	if states := s.sessionStateEvents(sid2); slices.Contains(states, "frozen") {
		t.Errorf("冻结未确认却记录了 frozen：%v", states)
	}
	var reason string
	s.row("SELECT COALESCE(end_reason, '') FROM incarnations WHERE env_id = $1", []any{env2}, &reason)
	if reason != "freeze_failed" {
		t.Errorf("incarnation end_reason = %q，期望 freeze_failed", reason)
	}
	u.closeSession(sid2)
	s.end()
}

// TestRealE32CloseStopsEnvBeforeDeletingWorkspace（E32）：turn 暂停后 DELETE /sessions/{id} → 暂停的 turn
// cancelled → 环境的 stopped_at 早于 workspace 目录删除 → 会话 closed、UID 范围归还。
func TestRealE32CloseStopsEnvBeforeDeletingWorkspace(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1 hold_ms=120000")
	eventually(t, "turn 1 完成搜索", func() bool {
		return len(swFind(s.swLog(sid), swEvent("search", "task_id", r1.TurnID))) == 1
	})
	u.control(r1.TurnID, "stop", "st-1")
	u.waitTurn(sid, r1.TurnID, "暂停", turnIs("paused"))
	_, env, _, _ := s.incarnationOf(sid)
	ws := filepath.Join(s.dir, "sessions", sid, "workspace")
	if code, b := u.do("DELETE", "/sessions/"+sid, ""); code != http.StatusAccepted {
		t.Fatalf("DELETE = %d %s", code, b)
	}
	var deletedAt time.Time
	eventuallyWithin(t, "workspace 目录删除", waitLimit, 5*time.Millisecond, func() bool {
		if _, err := os.Stat(ws); errors.Is(err, os.ErrNotExist) {
			deletedAt = time.Now()
			return true
		}
		return false
	})
	s.waitSession(sid, "closed")
	var stoppedAt time.Time
	var status, rangeState string
	s.row("SELECT stopped_at FROM environments WHERE env_id = $1", []any{env}, &stoppedAt)
	s.row("SELECT status FROM tasks WHERE task_id = $1", []any{r1.TurnID}, &status)
	s.row(`SELECT COALESCE((SELECT state FROM uid_ranges WHERE owner_id = $1 AND state <> 'free'), 'free')`,
		[]any{"session:" + sid}, &rangeState)
	if status != "cancelled" || !stoppedAt.Before(deletedAt) || rangeState != "free" {
		t.Fatalf("关闭：turn %s，环境停止于 %s、workspace 删除于 %s，UID 范围 %s", status, stoppedAt, deletedAt, rangeState)
	}
	s.end()
}

// TestRealE33RestartWithFrozenSession（E33）：存在 frozen 会话时 SIGKILL server 并重启 → 会话 evicted（incarnation
// ended{lost_on_restart}）、冻结的旧环境被停止并回收 → 新消息冷恢复成功（读到冻结前提交的会话状态）。
func TestRealE33RestartWithFrozenSession(t *testing.T) {
	s := newRealSessionSys(t, "--session-idle-freeze", "2s")
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	s.waitSession(sid, "frozen")
	inc1, env1, _, _ := s.incarnationOf(sid)
	if frozen, err := cgroupFrozen(s.envCgroupDir(env1)); err != nil || !frozen {
		t.Fatalf("frozen 会话的 cgroup.events frozen = %v（%v）", frozen, err)
	}
	s.kill()
	s.startS()
	s.waitSession(sid, "evicted")
	var st, reason string
	s.row("SELECT status, COALESCE(end_reason, '') FROM incarnations WHERE incarnation_id = $1", []any{inc1}, &st, &reason)
	if st != "ended" || reason != "lost_on_restart" {
		t.Errorf("重启后旧 incarnation = %s/%s", st, reason)
	}
	eventually(t, "冻结的旧环境被停止并回收", func() bool {
		var stopped bool
		var cleanup string
		s.row("SELECT stopped_at IS NOT NULL, cleanup_state FROM environments WHERE env_id = $1", []any{env1}, &stopped, &cleanup)
		_, err := os.Stat(s.envCgroupDir(env1))
		return stopped && cleanup == "done" && errors.Is(err, os.ErrNotExist)
	})
	r2 := u.message(sid, "m-2", "searches=1")
	v2 := u.waitTurn(sid, r2.TurnID, "冷恢复后成功", turnIs("succeeded"))
	if v2.Summary != "count=2 task="+r2.TurnID {
		t.Errorf("冷恢复后的会话状态 = %q", v2.Summary)
	}
	u.closeSession(sid)
	s.end()
}

// ==== M4 Plan 14 Task 11：sub-run 故障实验（规格 §13、§16.4 E24、E40–E45；本段到此结束前不含其他任务的用例） ====
//
// Worker 是 sim_worker 的 sub-run 操作（subrun_start、带 subrun 的 Gateway 步骤、subrun_end、subrun_cancel、
// subrun_ignore_cancel、checkpoint 的 forge_completed；见 worker/sim_worker/app.py）。每个实验一对用例：进程内装置
// （fake provider 的进程型 Program，非 root）与真实隔离（provider/local + 生产启动器，root；TestRealE…）。二者共用
// testE…(t, h)，结束时都以 h.finish() 运行静止时的不变量检查（verify-invariants 的同一检查，含 I3 两层与 I13）。

// subrunCfg 是开启 sub-run 扩展的 Gateway 配置（app.Config.WorkerSubruns 的零值为关闭；cmd/agentbox 默认开启）。
func subrunCfg(fu *fakeupstream.Server) app.Config {
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.WorkerSubruns = true
	return cfg
}

func srStartStep(id string, deadlineMs int) step {
	return step{"op": "subrun_start", "subrun_id": id, "deadline_ms": deadlineMs}
}

func srFetchStep(sub, u string) step {
	return step{"op": "fetch", "step_id": "f", "subrun": sub, "url": u}
}

func srEndStep(id string) step {
	return step{"op": "subrun_end", "subrun_id": id, "summary": id + " 的摘要"}
}

// crashStep 只在第 1 个 attempt 以 SIGKILL 杀死 Worker 自身。
func crashStep() step { return step{"op": "exit", "code": 137, "signal": 9, "attempt": 1} }

// subrunRow 返回 sub-run 的 status|bound attempt_no|failure_reason|cancel_reason。
func (h *harness) subrunRow(taskID, subrunID string) string {
	h.t.Helper()
	var out string
	h.queryRow(`SELECT concat_ws('|', s.status, a.attempt_no, s.failure_reason, s.cancel_reason) FROM subruns s
		JOIN attempts a ON a.attempt_id = s.bound_attempt_id WHERE s.task_id = $1 AND s.subrun_id = $2`,
		[]any{taskID, subrunID}, &out)
	return out
}

// resumeSubruns 是第 n 个 attempt 的 init.resume.subruns（subrun_id → status）。
func (h *harness) resumeSubruns(taskID string, n int64) map[string]string {
	h.t.Helper()
	in, ok := h.rec(taskID).init(n)
	if !ok || in.Resume == nil {
		h.t.Fatalf("attempt %d 没有 init.resume：%+v", n, in)
	}
	out := map[string]string{}
	for _, s := range in.Resume.Subruns {
		out[s.SubrunID] = s.Status
	}
	return out
}

// subrunCalls 返回 call_id 以 prefix 开头的调用：call_id → state/source/首个 attempt_no。
func (h *harness) subrunCalls(taskID, prefix string) map[string]string {
	h.t.Helper()
	var agg string
	h.queryRow(`SELECT COALESCE(string_agg(c.call_id || '=' || concat_ws('/', c.state, c.source, a.attempt_no), ' ' ORDER BY c.call_id), '')
		FROM calls c JOIN attempts a ON a.attempt_id = c.first_attempt_id WHERE c.task_id = $1 AND starts_with(c.call_id, $2)`,
		[]any{taskID, prefix}, &agg)
	out := map[string]string{}
	for _, kv := range strings.Fields(agg) {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}
	return out
}

func (h *harness) controlTask(taskID, action, requestID string) {
	h.t.Helper()
	st, b, err := httpDo("POST", h.base+"/tasks/"+taskID+"/"+action, `{"request_id":"`+requestID+`","reason":"e2e"}`)
	if err != nil || st != http.StatusOK {
		h.t.Fatalf("%s %s = %d %s %v", action, taskID, st, b, err)
	}
}

// TestE24SubrunCoalesce：E24（sub-run 部分）——同一 sub-run 内 10 个相同抓取 → fake upstream 计数 1（1 个 upstream、
// 9 个 coalesced）；另一个 sub-run 的同一抓取不合并（合并键含 subrun_id，§11.4）→ 计数 2。
func TestE24SubrunCoalesce(t *testing.T) { testE24Subrun(t, newHarness(t)) }

func TestRealE24SubrunCoalesce(t *testing.T) { testE24Subrun(t, newRealHarness(t)) }

func testE24Subrun(t *testing.T, h *harness) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u := cachePage(fu, "/e24s-"+randSuffix()+"/a", nil) // 没有新鲜度头：不写缓存，只看合并
	fu.Inject(fakeupstream.Fetch, 1, fakeupstream.Action{Hang: true})
	cfg := cacheCfg(fu, addr)
	cfg.WorkerSubruns = true
	h.start(cfg)
	id := h.submit("e24s", spec(nil, "e24s", nil, srStartStep("st1", 600000), srStartStep("st2", 600000), sleepStep(600000)))
	sock := h.waitAttemptSocket(id)
	eventually(t, "两个 sub-run 已启动", func() bool {
		var n int
		h.queryRow("SELECT count(*) FROM subruns WHERE task_id = $1 AND status = 'started'", []any{id}, &n)
		return n == 2
	})
	before := h.cacheMetrics()

	const n = 10
	body := `{"url":"` + u + `"}`
	res := make([]gwResp, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i], errs[i] = gwDoSubrun(sock, "st1", http.MethodPost, "/v1/fetch", fmt.Sprintf("st1/e24/fetch/%d", i+1), body)
		}()
	}
	eventually(t, "st1 的 leader 挂起在上游、9 个请求作为 follower 加入", func() bool {
		return fu.Hanging() == 1 && metricsDelta(h.cacheMetrics(), before)["coalesced"] == n-1
	})
	// st2 的同一抓取不加入 st1 的共享请求：自行访问上游（第 2 次抓取不挂起）。
	r2, err := gwDoSubrun(sock, "st2", http.MethodPost, "/v1/fetch", "st2/e24/fetch/1", body)
	if err != nil || r2.Status != http.StatusOK || r2.Replayed || r2.Blob == "" {
		t.Fatalf("st2 的抓取：%+v %v", r2, err)
	}
	fu.Release()
	wg.Wait()
	for i := range n {
		if errs[i] != nil || res[i].Status != http.StatusOK || res[i].Blob == "" || res[i].Blob != res[0].Blob {
			t.Fatalf("st1 请求 %d：%+v %v（请求 1：%+v）", i+1, res[i], errs[i], res[0])
		}
	}
	st1 := h.subrunCalls(id, "st1/e24/")
	var up, co int
	for _, v := range st1 {
		up += strings.Count(v, "/upstream/")
		co += strings.Count(v, "/coalesced/")
	}
	if len(st1) != n || up != 1 || co != n-1 || fu.Count(fakeupstream.Fetch) != 2 {
		t.Fatalf("st1 的调用 %v；上游抓取 %d 次", st1, fu.Count(fakeupstream.Fetch))
	}
	if st2 := h.subrunCalls(id, "st2/"); st2["st2/e24/fetch/1"] != "completed/upstream/1" {
		t.Fatalf("st2 的调用 %v", st2)
	}
	if st, code := h.cancelTask(id, "e24s-cancel"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	if v := h.waitTerminal(id); v.Status != "cancelled" {
		t.Fatalf("任务结束为 %s", v.Status)
	}
	for _, sub := range []string{"st1", "st2"} {
		if g := h.subrunRow(id, sub); g != "cancelled|1|task_cancel|task_cancel" {
			t.Fatalf("取消裁决后 %s = %s", sub, g)
		}
	}
	t.Logf("E24（sub-run）：st1 内 10 个相同抓取 + st2 同一抓取 → 上游 %d 次", fu.Count(fakeupstream.Fetch))
	h.finish()
}

// TestE40CancelThenCrash：E40——编排层取消 st2 后 SIGKILL Worker → 新 attempt 的 init.resume.subruns 中 st2 为
// cancelled（不恢复执行），st2 没有新调用；st1 重新绑定并完成。
func TestE40CancelThenCrash(t *testing.T) { testE40(t, newHarness(t)) }

func TestRealE40CancelThenCrash(t *testing.T) { testE40(t, newRealHarness(t)) }

func testE40(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	p := "/e40-" + randSuffix()
	a, b, c, d := cachePage(fu, p+"/a", nil), cachePage(fu, p+"/b", nil), cachePage(fu, p+"/c", nil), cachePage(fu, p+"/d", nil)
	h.start(subrunCfg(fu))
	id := h.submit("e40", spec(nil, "e40", nil,
		srStartStep("st1", 600000), srStartStep("st2", 600000),
		srFetchStep("st1", a), srFetchStep("st2", b),
		checkpointStep("c1"),
		step{"op": "subrun_cancel", "subrun_id": "st2"},
		crashStep(),
		srFetchStep("st2", c), // 恢复后不得执行：st2 已 cancelled
		srFetchStep("st1", d),
		srEndStep("st1"),
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in := h.inspect(id)
	if a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2); a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v / %+v", a1, a2)
	}
	if got := h.resumeSubruns(id, 2); got["st2"] != "cancelled" || got["st1"] != "started" {
		t.Fatalf("attempt 2 的 resume.subruns = %v", got)
	}
	if g := h.subrunRow(id, "st2"); g != "cancelled|1||orchestrator" {
		t.Fatalf("st2 = %s", g)
	}
	if g := h.subrunRow(id, "st1"); g != "completed|2||" {
		t.Fatalf("st1 = %s", g)
	}
	st2 := h.subrunCalls(id, "st2/")
	if len(st2) != 1 || st2["st2/f/fetch/1"] != "completed/upstream/1" || pathCount(t, fu, c) != 0 {
		t.Fatalf("st2 的调用 %v；/c 上游抓取 %d 次", st2, pathCount(t, fu, c))
	}
	if pathCount(t, fu, a) != 1 || pathCount(t, fu, d) != 1 {
		t.Fatalf("st1 的抓取：/a %d 次、/d %d 次", pathCount(t, fu, a), pathCount(t, fu, d))
	}
	h.finish()
}

// TestE41LateCheckpointCompleted：E41——已取消的 sub-run 被迟到的 checkpoint 列为 completed → checkpoint_result
// rejected/invalid_transition，该 checkpoint 不写入、指针不变（之后的 checkpoint 的 commit_seq 连续）；st2 保持 cancelled。
func TestE41LateCheckpointCompleted(t *testing.T) { testE41(t, newHarness(t)) }

func TestRealE41LateCheckpointCompleted(t *testing.T) { testE41(t, newRealHarness(t)) }

func testE41(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h.start(subrunCfg(fu))
	id := h.submit("e41", spec(nil, "e41", nil,
		srStartStep("st1", 600000), srStartStep("st2", 600000),
		checkpointStep("c1"),
		step{"op": "subrun_cancel", "subrun_id": "st2"},
		step{"op": "checkpoint", "step_id": "forged", "forge_completed": []string{"st2"}, "expect_rejected": "invalid_transition"},
		srEndStep("st1"),
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	var rejected int
	for _, l := range h.rec(id).hostLines() {
		if l.Type == protocol.TypeCheckpointResult && l.Status == protocol.CheckpointRejected && l.Code == "invalid_transition" {
			rejected++
		}
	}
	in := h.inspect(id)
	if rejected != 1 || len(in.Checkpoints) != 2 {
		t.Fatalf("invalid_transition 拒绝 %d 次；已提交 checkpoint %+v", rejected, in.Checkpoints)
	}
	if c1, c2 := checkpointByStep(in, "c1"), checkpointByStep(in, "c2"); c1.CommitSeq != 1 || c2.CommitSeq != 2 {
		t.Fatalf("commit_seq：c1 %d、c2 %d（被拒的 checkpoint 不应占用序号）", c1.CommitSeq, c2.CommitSeq)
	}
	if g := h.subrunRow(id, "st2"); g != "cancelled|1||orchestrator" {
		t.Fatalf("st2 = %s", g)
	}
	if g := h.subrunRow(id, "st1"); g != "completed|1||" {
		t.Fatalf("st1 = %s", g)
	}
	h.finish()
}

// TestE42EndThenCrash：E42——subrun_end{succeeded} 之后、checkpoint 之前崩溃 → end_proposed 不视为完成：恢复时 st1
// 重新绑定（resume.subruns 中为 started）并重新执行。checkpoint 之后完成的调用以同一 call id 重放（上游不重复），新的
// 调用以续号 call id 出现（st1/f/fetch/3，首个 attempt 为 2）；checkpoint 之前的调用不重做。
func TestE42EndThenCrash(t *testing.T) { testE42(t, newHarness(t)) }

func TestRealE42EndThenCrash(t *testing.T) { testE42(t, newRealHarness(t)) }

func testE42(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	p := "/e42-" + randSuffix()
	a, b, c := cachePage(fu, p+"/a", nil), cachePage(fu, p+"/b", nil), cachePage(fu, p+"/c", nil)
	fetchC := srFetchStep("st1", c)
	fetchC["attempt"] = 2
	h.start(subrunCfg(fu))
	id := h.submit("e42", spec(nil, "e42", nil,
		srStartStep("st1", 600000),
		srFetchStep("st1", a),
		checkpointStep("c1"),
		srFetchStep("st1", b),
		fetchC,
		srEndStep("st1"),
		crashStep(),
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in := h.inspect(id)
	if a1 := attemptByNo(in, 1); a1.OutcomeClass != runner.ClassCrashedSignal {
		t.Fatalf("attempt 1：%+v", a1)
	}
	if got := h.resumeSubruns(id, 2); got["st1"] != "started" {
		t.Fatalf("attempt 2 的 resume.subruns = %v（end_proposed 未入 checkpoint 不视为完成）", got)
	}
	var subrunEnds int
	for _, typ := range h.rec(id).workerTypes(1) {
		if typ == "subrun_end" {
			subrunEnds++
		}
	}
	if subrunEnds != 1 {
		t.Fatalf("attempt 1 应在崩溃前发出 subrun_end：%q", h.rec(id).workerTypes(1))
	}
	if g := h.subrunRow(id, "st1"); g != "completed|2||" {
		t.Fatalf("st1 = %s", g)
	}
	want := map[string]string{"st1/f/fetch/1": "completed/upstream/1", "st1/f/fetch/2": "completed/upstream/1",
		"st1/f/fetch/3": "completed/upstream/2"}
	if got := h.subrunCalls(id, "st1/"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("st1 的调用 %v，期望 %v", got, want)
	}
	if pathCount(t, fu, a) != 1 || pathCount(t, fu, b) != 1 || pathCount(t, fu, c) != 1 {
		t.Fatalf("上游抓取：/a %d、/b %d、/c %d（重放不应再访问上游）", pathCount(t, fu, a), pathCount(t, fu, b), pathCount(t, fu, c))
	}
	h.finish()
}

// TestE43SubrunBudget：E43——sub-run 上限 0：付费调用（chat）被拒 402 subrun_budget_exhausted，不访问上游、两层账本不动；
// 同 URL 的缓存命中（root 先抓取并写入共享缓存）仍返回；sub-run 以抽取式总结完成（预算耗尽不自动取消 sub-run）。
func TestE43SubrunBudget(t *testing.T) { testE43(t, newHarness(t)) }

func TestRealE43SubrunBudget(t *testing.T) { testE43(t, newRealHarness(t)) }

func testE43(t *testing.T, h *harness) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u := cachePage(fu, "/e43-"+randSuffix()+"/p", ccHeader("max-age=600"))
	cfg := cacheCfg(fu, addr)
	cfg.WorkerSubruns = true
	h.start(cfg)
	// 先由另一个任务的 root 抓取写入共享缓存（写入在响应之后完成：等到 Redis 中出现该键）。
	h.runFetches("e43-warm", u)
	r, key := testRedis(t, addr), fetchKey(t, u)
	eventually(t, "抓取结果写入共享缓存", func() bool { _, ok := cacheEntry(t, r, key); return ok })
	chat := chatStep("s")
	chat["subrun"], chat["expect_error"] = "st1", "subrun_budget_exhausted"
	id := h.submit("e43", spec(nil, "e43", nil,
		step{"op": "subrun_start", "subrun_id": "st1", "deadline_ms": 600000, "budget_cap_micro": 0},
		chat,
		srFetchStep("st1", u),
		srEndStep("st1"),
		checkpointStep("c1"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	if g := h.subrunRow(id, "st1"); g != "completed|1||" {
		t.Fatalf("st1 = %s", g)
	}
	calls := h.subrunCalls(id, "st1/")
	if calls["st1/f/fetch/1"] != "completed/cache/1" || fu.Count(fakeupstream.Chat) != 0 || pathCount(t, fu, u) != 1 {
		t.Fatalf("st1 的调用 %v；上游 chat %d 次、抓取 %d 次", calls, fu.Count(fakeupstream.Chat), pathCount(t, fu, u))
	}
	var tries int
	var spent, reserved, unknown int64
	h.queryRow(`SELECT (SELECT count(*) FROM call_tries WHERE task_id = $1 AND call_id LIKE 'st1/%'),
			sb.spent_micro, sb.reserved_micro, sb.unknown_micro FROM subrun_budgets sb WHERE sb.task_id = $1 AND sb.subrun_id = 'st1'`,
		[]any{id}, &tries, &spent, &reserved, &unknown)
	if tries != 0 || spent != 0 || reserved != 0 || unknown != 0 {
		t.Fatalf("sub-run 层：try %d 个，spent/reserved/unknown = %d/%d/%d", tries, spent, reserved, unknown)
	}
	h.finish()
}

// TestE44IgnoreCancel：E44——SDK 忽略 subrun_cancel_requested（deadline 触发）→ T_subrun_cancel 到期后 attempt 被终止
// （outcome_class = subrun_cancel_timeout，故障重试）→ 新 attempt 从 checkpoint 恢复，st1 为 timed_out 且不再执行。
func TestE44IgnoreCancel(t *testing.T) { testE44(t, newHarness(t)) }

func TestRealE44IgnoreCancel(t *testing.T) { testE44(t, newRealHarness(t)) }

func testE44(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	cfg := subrunCfg(fu)
	cfg.Runner.SubrunCancelTimeout = time.Second
	h.start(cfg)
	id := h.submit("e44", spec(nil, "e44", nil,
		step{"op": "subrun_ignore_cancel"},
		srStartStep("st1", 1500),
		checkpointStep("c1"),
		step{"op": "sleep", "ms": 600000, "subrun": "st1"},
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in := h.inspect(id)
	a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2)
	if a1.OutcomeClass != runner.ClassSubrunCancelTimeout || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v / %+v", a1, a2)
	}
	init2, _ := h.rec(id).init(2)
	if c1 := checkpointByStep(in, "c1"); init2.Resume == nil || init2.Resume.CheckpointID != c1.CheckpointID {
		t.Fatalf("attempt 2 的 resume %+v，期望 c1（%s）", init2.Resume, c1.CheckpointID)
	}
	if got := h.resumeSubruns(id, 2); got["st1"] != "timed_out" {
		t.Fatalf("attempt 2 的 resume.subruns = %v", got)
	}
	if g := h.subrunRow(id, "st1"); !strings.HasPrefix(g, "timed_out|1|") || !strings.HasSuffix(g, "|deadline") {
		t.Fatalf("st1 = %s", g)
	}
	if got := h.rec(id).workerTypes(2); slices.Contains(got, "subrun_start") {
		t.Fatalf("attempt 2 不应重发已终态 sub-run 的 subrun_start：%q", got)
	}
	h.finish()
}

// TestE45PausePastDeadline：E45——暂停期间 deadline 过期（deadline 为绝对时间，暂停期间继续计时）→ 继续时新 attempt
// 的 init.resume.subruns 告知 st1 为 timed_out，st1 的其余步骤不再执行；任务成功。
func TestE45PausePastDeadline(t *testing.T) { testE45(t, newHarness(t)) }

func TestRealE45PausePastDeadline(t *testing.T) { testE45(t, newRealHarness(t)) }

func testE45(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u := cachePage(fu, "/e45-"+randSuffix()+"/a", nil)
	steps := []step{srStartStep("st1", 4000)}
	for i := range 40 { // 每 100 ms 一个 checkpoint：暂停请求在下一个提交边界生效
		steps = append(steps, step{"op": "sleep", "ms": 100, "subrun": "st1"}, checkpointStep(fmt.Sprintf("k%d", i)))
	}
	steps = append(steps, srFetchStep("st1", u), srEndStep("st1"), checkpointStep("done"))
	h.start(subrunCfg(fu))
	id := h.submit("e45", spec(nil, "e45", nil, steps...))
	eventually(t, "st1 已启动", func() bool {
		var n int
		h.queryRow("SELECT count(*) FROM subruns WHERE task_id = $1", []any{id}, &n)
		return n == 1
	})
	h.controlTask(id, "pause", "e45-pause")
	eventually(t, "任务暂停", func() bool {
		st, _, _ := h.httpTask(id)
		return st == "paused"
	})
	if g := h.subrunRow(id, "st1"); g != "started|1||" {
		t.Fatalf("暂停时 st1 = %s（应在 deadline 之前暂停）", g)
	}
	eventuallyWithin(t, "st1 的 deadline 在暂停期间过期", 10*time.Second, 100*time.Millisecond, func() bool {
		var past bool
		h.queryRow("SELECT now() > deadline_at FROM subruns WHERE task_id = $1 AND subrun_id = 'st1'", []any{id}, &past)
		return past
	})
	h.controlTask(id, "resume", "e45-resume")
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	if got := h.resumeSubruns(id, 2); got["st1"] != "timed_out" {
		t.Fatalf("attempt 2 的 resume.subruns = %v", got)
	}
	if g := h.subrunRow(id, "st1"); !strings.HasPrefix(g, "timed_out|") {
		t.Fatalf("st1 = %s", g)
	}
	if pathCount(t, fu, u) != 0 || len(h.subrunCalls(id, "st1/")) != 0 {
		t.Fatalf("timed_out 的 st1 不应再发出调用：/a 抓取 %d 次", pathCount(t, fu, u))
	}
	h.finish()
}

// ==== M4 Plan 14 Task 11 段结束 ====

// ==== M4 Plan 15 Task 12：exec 真实链路与故障实验（规格 §10、§16 E35–E38；E34、E39 见段末说明） ====
//
// 全部为 root、真实隔离：Worker（sim_worker）在任务沙箱中经 /run/agentbox/gateway.sock 调 POST /v1/exec，
// Gateway 为每次 exec 建立独立的 exec 环境（provider/local，exec 模板，无 Gateway socket、无 workspace、netns 仅 lo），
// 在其中以 python3 -I -B /in/.agentbox/main.py 运行代码。进程内装置（newRealHarness）启用 exec 时取与
// cmd/agentbox 相同的 exec 模板摘要；E37 用真实子进程 cmd/agentbox（exec 默认开启，--exec-slots 4）。
//
// E34（权限边界）按项目负责人的决定不另写提权/攻击探针：由已有的 seccomp 拒绝探针（mount/unshare/setns/pivot_root/
// CLONE_NEW*、ptrace/process_vm_*、bpf/io_uring/keyctl/perf_event_open/userfaultfd、非 AF_UNIX socket）与能力集
// {KILL}、无 ptrace 的回归锁覆盖——不是穷尽证明。E39（UID 范围复用）由 internal/provider/local 的
// TestE39UIDRangeReuse（root）与 resource 的回收/隔离用例覆盖（Plan 15 Task 5 已合入）。

// execImageDigest 与 cmd/agentbox 的 prepareIsolation 相同：确认 exec 模板可用（python3 可解析）并返回其摘要。
func execImageDigest() (string, error) {
	et := rootfs.ExecTemplate()
	if err := et.EnsureExec(); err != nil {
		return "", err
	}
	return rootfs.TemplateDigest(et)
}

// execCfg 是启用 exec 的进程内 Gateway 配置（其余 exec 策略取默认值：slots 4、每任务 2、wall 60 s、内存 512 MiB）。
func execCfg(fu *fakeupstream.Server) app.Config {
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.Exec = app.ExecConfig{Slots: app.DefaultExecSlots}
	return cfg
}

// execStep 是 sim_worker 的 exec 操作（期望结果状态 completed；inputs 为 [sha256, path]）。
func execStep(stepID, code string, inputs ...[2]string) step {
	s := step{"op": "exec", "step_id": stepID, "code": code, "expect_status": "completed", "wall_ms": 60000}
	if len(inputs) > 0 {
		s["inputs"] = inputs
	}
	return s
}

// execResult 是 /v1/exec 的结果 JSON（结果 blob 与 200 响应体）中测试关心的字段。
type execResult struct {
	Status string `json:"status"`
	Exit   *struct {
		Code   int `json:"code"`
		Signal int `json:"signal"`
	} `json:"exit"`
	Diag *struct {
		OOMKillDelta uint64 `json:"oom_kill_delta"`
		OOMObserved  bool   `json:"oom_observed"`
	} `json:"diag"`
	Stdout          string `json:"stdout"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	Stderr          string `json:"stderr"`
	StderrTruncated bool   `json:"stderr_truncated"`
	Outputs         []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	} `json:"outputs"`
	SkippedOutputs []struct {
		Reason string `json:"reason"`
	} `json:"skipped_outputs"`
	QueueMs int64 `json:"queue_ms"`
	WallMs  int64 `json:"wall_ms"`
	Limits  struct {
		MemoryBytes int64 `json:"memory_bytes"`
	} `json:"limits"`
}

func (r execResult) exitCode() (code, signal int) {
	if r.Exit == nil {
		return -1, -1
	}
	return r.Exit.Code, r.Exit.Signal
}

func (r execResult) output(path string) (sha string, size int64, ok bool) {
	for _, o := range r.Outputs {
		if o.Path == path {
			return o.SHA256, o.Size, true
		}
	}
	return "", 0, false
}

// execCall 返回任务中 callID 的调用（inspect）与其结果 blob 解析出的 exec 结果。
func (h *harness) execCall(taskID, callID string) (api.CallView, execResult) {
	h.t.Helper()
	for _, c := range h.inspect(taskID).Calls {
		if c.CallID != callID {
			continue
		}
		if c.Endpoint != "/v1/exec" || c.ResultRef == "" {
			h.t.Fatalf("调用 %s：%+v，期望有结果的 /v1/exec 调用", callID, c)
		}
		var r execResult
		if err := json.Unmarshal(h.readBlob(c.ResultRef), &r); err != nil {
			h.t.Fatalf("调用 %s 的结果 blob: %v", callID, err)
		}
		return c, r
	}
	h.t.Fatalf("任务 %s 没有调用 %s", taskID, callID)
	return api.CallView{}, execResult{}
}

// gwRaw 在 attempt 的 socket 上以一条新连接发出一个请求，返回状态码与响应体（exec 结果、/blobs 内容）。
func gwRaw(sock, method, path, callID, body string) (int, []byte, error) {
	tr := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequest(method, "http://gateway"+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if callID != "" {
		req.Header.Set("X-Agentbox-Call-Id", callID)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// execBody 是 /v1/exec 的请求体（memoryBytes 为 0 时不写内存限制）。
func execBody(code string, wallMs, memoryBytes int64) string {
	lim := map[string]int64{"wall_ms": wallMs}
	if memoryBytes > 0 {
		lim["memory_bytes"] = memoryBytes
	}
	b, err := json.Marshal(map[string]any{"language": "python3", "code": code, "limits": lim})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// heldTask 提交一个在第 1 个 checkpoint 处暂停的任务，等它到达暂停点，返回任务 ID 与其 attempt 的 Gateway socket。
func (h *harness) heldTask(requestID string) (string, string) {
	h.t.Helper()
	id := h.submit(requestID, spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, requestID, nil,
		checkpointStep("c1"), progressStep("after hold")))
	h.waitHeld(id)
	in, ok := h.rec(id).init(1)
	if !ok {
		h.t.Fatalf("任务 %s 没有 attempt 1 的 init", id)
	}
	return id, h.gwSocket(in.AttemptID)
}

// waitEnvGone 等待环境的 cgroup 与环境目录都不存在（执行树已清空、/out 已卸载删除）。
func (h *harness) waitEnvGone(envID string) {
	h.t.Helper()
	eventually(h.t, "环境 "+envID+" 的 cgroup 与目录已删除", func() bool {
		_, cgErr := os.Stat(h.envCgroup(envID))
		_, dirErr := os.Stat(filepath.Join(h.dir, "envs", envID))
		return errors.Is(cgErr, os.ErrNotExist) && errors.Is(dirErr, os.ErrNotExist)
	})
}

const (
	execPage    = "x,y\n1,2\n3,4\n10,20\n"
	execSumCode = `import csv, io, json
page = json.load(open("/in/page.json"))
rows = list(csv.reader(io.StringIO(page["content"])))
with open("/out/result.csv", "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["x", "y", "sum"])
    for x, y in rows[1:]:
        w.writerow([x, y, int(x) + int(y)])
print("rows", len(rows) - 1)
`
	execSumCSV    = "x,y,sum\n1,2,3\n3,4,7\n10,20,30\n"
	execTotalCode = `import csv
rows = list(csv.DictReader(open("/in/data/result.csv")))
total = sum(int(r["sum"]) for r in rows)
open("total.txt", "w").write(f"{total}\n")
print("total", total)
`
)

// TestRealExecViaGateway：真实链路——沙箱中的 sim_worker 先 fetch（fake upstream）得到结果 blob，再 exec 以该 blob 为
// /in/page.json、写 /out/result.csv；第二个 exec 以前者的输出为 /in/data/result.csv、写 total.txt（cwd 为 /out）。
// 两次 exec 都 completed（退出码 0），输出经 attempt 的 Gateway GET /blobs/{sha} 读取到预期内容，exec_count_used = 2，
// 每次 exec 在独立的 exec 环境中运行且已停止、清理；任务成功，静止时 I1–I16 成立。
func TestRealExecViaGateway(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	const pagePath = "/data/points.csv"
	fu.SetPage(pagePath, "text/csv", execPage)
	pageURL := fu.URL() + pagePath
	// fetch 的结果 blob 是 upstream.FetchResult 的 JSON（与 fetch adapter 的编码相同），sha 可预先算出。
	fr, err := json.Marshal(upstream.FetchResult{URL: pageURL, FinalURL: pageURL, Status: 200, ContentType: "text/csv",
		Encoding: "utf-8", Content: execPage})
	if err != nil {
		t.Fatal(err)
	}
	fetchSHA, csvSHA, totalSHA := sha256Hex(fr), sha256Hex([]byte(execSumCSV)), sha256Hex([]byte("40\n"))
	id := h.submit("exec-gw", spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, "exec", nil,
		step{"op": "fetch", "step_id": "f1", "url": pageURL},
		execStep("x1", execSumCode, [2]string{fetchSHA, "page.json"}),
		execStep("x2", execTotalCode, [2]string{csvSHA, "data/result.csv"}),
		checkpointStep("c1"), progressStep("done")))
	h.waitHeld(id)
	in1, _ := h.rec(id).init(1)
	sock := h.gwSocket(in1.AttemptID)
	for sha, want := range map[string]string{csvSHA: execSumCSV, totalSHA: "40\n"} {
		st, b, err := gwRaw(sock, http.MethodGet, "/blobs/"+sha, "", "")
		if err != nil || st != http.StatusOK || string(b) != want {
			t.Fatalf("GET /blobs/%s = %d %q %v；期望 %q", sha, st, b, err, want)
		}
	}
	h.rec(id).releaseHold()
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	refs := h.checkpointRefs(id, "c1")
	for _, sha := range []string{fetchSHA, csvSHA, totalSHA} {
		if !slices.Contains(refs, sha) {
			t.Fatalf("checkpoint c1 的 refs %v 不含 %s", refs, sha)
		}
	}
	attemptEnv := attemptByNo(h.inspect(id), 1).EnvID
	envs := map[string]bool{}
	for _, c := range []struct{ callID, stdout, out, sha string }{
		{"root/x1/exec/1", "rows 3\n", "result.csv", csvSHA},
		{"root/x2/exec/1", "total 40\n", "total.txt", totalSHA},
	} {
		cv, r := h.execCall(id, c.callID)
		code, sig := r.exitCode()
		sha, size, ok := r.output(c.out)
		if cv.State != "completed" || cv.TriesUsed != 1 || len(cv.Tries) != 1 || cv.Tries[0].Outcome != "ok" ||
			r.Status != "completed" || code != 0 || sig != 0 || r.Stdout != c.stdout || !ok || sha != c.sha ||
			len(r.Outputs) != 1 || size != int64(len(h.readBlob(sha))) {
			t.Fatalf("调用 %s：%+v；结果 %+v", c.callID, cv, r)
		}
		envID := cv.Tries[0].EnvID
		if envID == "" || envID == attemptEnv || envs[envID] || cv.Tries[0].ExecStartedAt == nil || cv.Tries[0].CPUUsec == nil {
			t.Fatalf("调用 %s 的 try %+v：期望独立的、已启动并结算 CPU 的 exec 环境（attempt 环境 %s）", c.callID, cv.Tries[0], attemptEnv)
		}
		envs[envID] = true
		var kind, cleanup string
		var stopped bool
		h.queryRow("SELECT kind, stopped_at IS NOT NULL, cleanup_state FROM environments WHERE env_id = $1", []any{envID},
			&kind, &stopped, &cleanup)
		if kind != "exec" || !stopped {
			t.Fatalf("exec 环境 %s：kind %s，已停止 %v", envID, kind, stopped)
		}
		h.waitEnvGone(envID)
	}
	var used int64
	h.queryRow("SELECT exec_count_used FROM exec_quotas WHERE task_id = $1", []any{id}, &used)
	if used != 2 {
		t.Fatalf("exec_count_used = %d，期望 2", used)
	}
	t.Logf("exec 真实链路：fetch %s → exec x1 → %s（result.csv）→ exec x2 → %s（total.txt）；exec 环境 %v", fetchSHA[:12],
		csvSHA[:12], totalSHA[:12], slices.Collect(maps.Keys(envs)))
	h.finish()
}

// e35Code：主进程 fork 一个后台写入者后立即退出。写入者持续向 /out/log 追加整行 "<序号> <是否已成孤儿>"（O_APPEND 单次
// 写入，不间断；写失败时退出），并继承了 stdout/stderr 管道（主进程退出时管道不会关闭）。主进程等写入者写满 50 行
// 后才退出，此后写入者的 getppid 改变，行尾记为 1。
const e35Code = `import os
r, w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    parent = os.getppid()
    fd = os.open("/out/log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    i = 0
    while True:
        try:
            os.write(fd, f"{i} {int(os.getppid() != parent)}\n".encode())
        except OSError:
            os._exit(1)
        i += 1
        if i == 50:
            os.write(w, b"x")
            os.close(w)
os.close(w)
os.read(r, 1)
print("main exits; writer", pid, flush=True)
os._exit(0)
`

// TestRealE35BackgroundWriter：E35——主进程退出而后台写入者继续追加 /out/log。收集发生在执行树清空（populated 0）
// 之后：结果中 log 的 size 与 sha 与 blob 一致（两次读取相同）、内容是从 0 开始连续的完整行，且包含主进程退出之后
// 写入的行（写入者在主进程退出后仍在写，收集等它被停止）；调用不因写入者持有 stdout 管道而挂起；exec 环境的
// cgroup 与目录随后删除（文件不再可能增长）。
func TestRealE35BackgroundWriter(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	id := h.submit("e35", spec(nil, "e35", nil, execStep("bg", e35Code)))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	cv, r := h.execCall(id, "root/bg/exec/1")
	code, sig := r.exitCode()
	sha, size, ok := r.output("log")
	if r.Status != "completed" || code != 0 || sig != 0 || !ok || !strings.HasPrefix(r.Stdout, "main exits; writer ") {
		t.Fatalf("E35 结果 %+v", r)
	}
	first, second := h.readBlob(sha), h.readBlob(sha)
	if !bytes.Equal(first, second) || int64(len(first)) != size || sha256Hex(first) != sha {
		t.Fatalf("log 的两次读取不一致或与结果不符：size %d/%d/%d", size, len(first), len(second))
	}
	if !bytes.HasSuffix(first, []byte("\n")) {
		t.Fatalf("log 以不完整的行结束：%q", first[max(0, len(first)-64):])
	}
	lines := strings.Split(strings.TrimSuffix(string(first), "\n"), "\n")
	var orphan int
	for i, l := range lines {
		f := strings.Fields(l)
		if len(f) != 2 || f[0] != strconv.Itoa(i) {
			t.Fatalf("log 第 %d 行 %q 不是连续的完整行", i, l)
		}
		if f[1] == "1" {
			orphan++
		}
	}
	if len(lines) < 50 || orphan == 0 {
		t.Fatalf("log 共 %d 行、主进程退出后写入 %d 行；期望写入者在主进程退出后仍在写", len(lines), orphan)
	}
	envID := cv.Tries[0].EnvID
	h.waitEnvGone(envID)
	t.Logf("E35：log %d 行（主进程退出后 %d 行），%d 字节，sha %s；exec 环境 %s 已删除；wall %d ms", len(lines), orphan,
		size, sha[:12], envID, r.WallMs)
	h.finish()
}

// TestRealE36StdoutFlood：E36——exec 向 stdout 写 100 MB：调用不挂起，stdout 只保留前 1 MiB 并置 stdout_truncated，
// stderr 完整，任务按时完成。
func TestRealE36StdoutFlood(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	const code = `import sys
b = b"x" * (1 << 20)
for _ in range(100):
    sys.stdout.buffer.write(b)
sys.stdout.buffer.flush()
print("done", file=sys.stderr)
`
	start := time.Now()
	id := h.submit("e36", spec(nil, "e36", nil, execStep("flood", code)))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	took := time.Since(start)
	_, r := h.execCall(id, "root/flood/exec/1")
	c, sig := r.exitCode()
	if r.Status != "completed" || c != 0 || sig != 0 || !r.StdoutTruncated || len(r.Stdout) != 1<<20 ||
		strings.Trim(r.Stdout, "x") != "" || r.Stderr != "done\n" || r.StderrTruncated {
		t.Fatalf("E36 结果：status %s exit %d/%d stdout %d 字节 truncated=%v stderr %q", r.Status, c, sig, len(r.Stdout),
			r.StdoutTruncated, r.Stderr)
	}
	if took > 60*time.Second {
		t.Fatalf("100 MB stdout 的任务用时 %s", took)
	}
	t.Logf("E36：100 MB stdout → 保留 %d 字节、stdout_truncated；exec wall %d ms，任务用时 %s", len(r.Stdout), r.WallMs,
		took.Round(time.Millisecond))
	h.finish()
}

// e38FillCode：/out 写满（tmpfs 64 MiB；页面计入环境 cgroup 的内存），然后分配内存直到超过 memory.max。
const e38FillCode = `import errno
written = 0
try:
    with open("/out/fill", "wb", buffering=0) as f:
        while True:
            f.write(b"\0" * (1 << 20))
            written += 1
except OSError as e:
    print("fill", errno.errorcode.get(e.errno), written, flush=True)
hog = []
for i in range(512):
    hog.append(bytearray(1 << 20))
print("survived", len(hog), flush=True)
`

// e38FilesCode：在 /out 中创建 2000 个文件（nr_inodes = 1024）。
const e38FilesCode = `import errno
n = 0
try:
    for i in range(2000):
        open(f"/out/f{i:04d}", "w").close()
        n += 1
except OSError as e:
    print("files", errno.errorcode.get(e.errno), n, flush=True)
else:
    print("files ok", n, flush=True)
`

// TestRealE38ExecPressure：E38——4 个暂停中的任务经各自 attempt 的 Gateway socket 同时发出 8 个 exec（每任务 2 个，
// 全局 slots 4）：
//   - ① /out 写满后分配内存超过 memory.max（96 MiB）：写满得到 ENOSPC/EFBIG，随后环境内 OOM，结果如实报告
//     （signal 9、diag.oom_observed）；
//   - ② 创建 2000 个文件：nr_inodes 用尽得到 ENOSPC，收集 256 个、其余报告 too_many；
//   - ③ 其余 6 个各 sleep 3 s：同时存活的 exec 环境从不超过 4 个（第 5 个起排队，queue_ms 记录等待）；
//   - 压力期间 GET /tasks/{id} 与 GET /status 每次都在 2 s 内响应；
//   - 全部 exec 环境回收，任务成功，静止时 I1、I11、I12 等不变量成立。
func TestRealE38ExecPressure(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	ids, socks := make([]string, 4), make([]string, 4)
	for i := range ids {
		ids[i], socks[i] = h.heldTask(fmt.Sprintf("e38-%d", i))
	}

	// 控制面响应：压力期间持续轮询，记录最大延迟与失败。
	stopPoll := make(chan struct{})
	var (
		pollMu     sync.Mutex
		maxLatency time.Duration
		polls      int
		pollErrs   []string
	)
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stopPoll:
				return
			case <-time.After(100 * time.Millisecond):
			}
			for _, p := range []string{"/status", "/tasks/" + ids[0]} {
				t0 := time.Now()
				st, _, err := httpDo("GET", h.base+p, "")
				d := time.Since(t0)
				pollMu.Lock()
				polls++
				maxLatency = max(maxLatency, d)
				if err != nil || st != http.StatusOK || d > 2*time.Second {
					pollErrs = append(pollErrs, fmt.Sprintf("GET %s = %d %v（%s）", p, st, err, d))
				}
				pollMu.Unlock()
			}
		}
	}()
	// 同时存活的 exec 环境数（stopped_at 未记录；slot 在 stopped_at 持久化之后才归还）。
	var maxLive int
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ctx := context.Background()
		c, err := pgx.Connect(ctx, h.dsn)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(ctx) }()
		for {
			select {
			case <-stopPoll:
				return
			case <-time.After(20 * time.Millisecond):
			}
			var n int
			if c.QueryRow(ctx, "SELECT count(*) FROM environments WHERE kind = 'exec' AND stopped_at IS NULL").Scan(&n) == nil {
				pollMu.Lock()
				maxLive = max(maxLive, n)
				pollMu.Unlock()
			}
		}
	}()

	type job struct {
		task   int
		callID string
		body   string
	}
	const sleepCode = "import time\ntime.sleep(3)\nprint('slept')\n"
	jobs := []job{
		{0, "e2e/fill/exec/1", execBody(e38FillCode, 60000, 96<<20)},
		{0, "e2e/files/exec/1", execBody(e38FilesCode, 60000, 0)},
	}
	for i := 1; i < 4; i++ {
		for k := 1; k <= 2; k++ {
			jobs = append(jobs, job{i, fmt.Sprintf("e2e/sleep%d/exec/1", k), execBody(sleepCode, 60000, 0)})
		}
	}
	type answer struct {
		status int
		res    execResult
		raw    []byte
		err    error
	}
	answers := make([]answer, len(jobs))
	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, b, err := gwRaw(socks[j.task], http.MethodPost, "/v1/exec", j.callID, j.body)
			a := answer{status: st, raw: b, err: err}
			if err == nil && st == http.StatusOK {
				a.err = json.Unmarshal(b, &a.res)
			}
			answers[i] = a
		}()
	}
	wg.Wait()
	close(stopPoll)
	<-pollDone
	<-sampleDone

	for i, a := range answers {
		if a.err != nil || a.status != http.StatusOK || a.res.Status != "completed" {
			t.Fatalf("exec %s（任务 %d）= %d %v：%s", jobs[i].callID, jobs[i].task, a.status, a.err, a.raw)
		}
	}
	fill, files := answers[0].res, answers[1].res
	if _, sig := fill.exitCode(); !strings.HasPrefix(fill.Stdout, "fill ") || sig != 9 || fill.Diag == nil || !fill.Diag.OOMObserved ||
		strings.Contains(fill.Stdout, "survived") || fill.Limits.MemoryBytes != 96<<20 {
		t.Fatalf("① /out 写满 + 内存压力：stdout %q，exit %+v，diag %+v，limits %+v；期望写满报错后环境内 OOM（signal 9、oom_observed）",
			fill.Stdout, fill.Exit, fill.Diag, fill.Limits)
	}
	if f := strings.Fields(fill.Stdout); len(f) < 2 || (f[1] != "ENOSPC" && f[1] != "EFBIG") {
		t.Fatalf("① 写满 /out 的错误 %q，期望 ENOSPC（tmpfs 写满）或 EFBIG（RLIMIT_FSIZE）", fill.Stdout)
	}
	ff := strings.Fields(files.Stdout)
	n := -1
	if len(ff) == 3 {
		n, _ = strconv.Atoi(ff[2]) // 非数字时为 0，由下方断言报告
	}
	var tooMany int
	for _, s := range files.SkippedOutputs {
		if s.Reason == "too_many" {
			tooMany++
		}
	}
	if len(ff) != 3 || ff[1] != "ENOSPC" || n >= 1024 || n < 256 || len(files.Outputs) != 256 || tooMany != n-256 {
		t.Fatalf("② 2000 个文件：stdout %q，收集 %d 个，too_many %d；期望 nr_inodes 用尽得到 ENOSPC、收集 256 个",
			files.Stdout, len(files.Outputs), tooMany)
	}
	var queued int
	for _, a := range answers {
		if a.res.QueueMs >= 100 {
			queued++
		}
	}
	pollMu.Lock()
	ml, lat, np, perrs := maxLive, maxLatency, polls, pollErrs
	pollMu.Unlock()
	if ml > app.DefaultExecSlots || ml == 0 || queued < len(jobs)-app.DefaultExecSlots {
		t.Fatalf("③ 同时存活的 exec 环境最多 %d 个（slots %d），排队 ≥ 100 ms 的 exec %d 个", ml, app.DefaultExecSlots, queued)
	}
	if len(perrs) != 0 || np == 0 {
		t.Fatalf("压力期间控制面轮询 %d 次，最大延迟 %s，异常：%v", np, lat, perrs)
	}
	for _, id := range ids {
		h.rec(id).releaseHold()
	}
	for _, id := range ids {
		if v := h.waitTerminal(id); v.Status != "succeeded" {
			t.Fatalf("任务 %+v", v)
		}
	}
	var envs, cleaned int
	h.queryRow("SELECT count(*), count(*) FILTER (WHERE cleanup_state = 'done') FROM environments WHERE kind = 'exec'", nil, &envs, &cleaned)
	if envs != len(jobs) {
		t.Fatalf("exec 环境 %d 个，期望 %d", envs, len(jobs))
	}
	t.Logf("E38：① %q exit %+v oom_kill_delta %d；② %q 收集 %d、too_many %d；③ 同时存活的 exec 环境最多 %d 个，排队 ≥ 100 ms 的 %d 个；"+
		"控制面轮询 %d 次、最大延迟 %s", strings.TrimSpace(fill.Stdout), fill.Exit, fill.Diag.OOMKillDelta, strings.TrimSpace(files.Stdout),
		len(files.Outputs), tooMany, ml, queued, np, lat.Round(time.Millisecond))
	h.finish()
}

// ---- E37：exec 进行中 SIGKILL server（cmd/agentbox 真实子进程） ----

const (
	e37CallID = "root/e1/exec/1"
	e37Code   = "import time\ntime.sleep(8)\nprint('done')\n"
)

// e37Try 是一次 exec try 与其环境。
type e37Try struct {
	TryNo     int
	EnvID     string
	Outcome   string
	Error     string
	StoppedAt *time.Time
	CreatedAt time.Time
}

func (s *sysHarness) e37Tries(taskID string) []e37Try {
	s.t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT ct.try_no, ct.env_id, ct.outcome, ct.error, e.stopped_at, e.created_at FROM call_tries ct
		JOIN environments e ON e.env_id = ct.env_id WHERE ct.task_id = $1 AND ct.call_id = $2 ORDER BY ct.try_no`, taskID, e37CallID)
	if err != nil {
		s.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (e37Try, error) {
		var x e37Try
		err := r.Scan(&x.TryNo, &x.EnvID, &x.Outcome, &x.Error, &x.StoppedAt, &x.CreatedAt)
		return x, err
	})
	if err != nil {
		s.t.Fatal(err)
	}
	return out
}

// e37Started 提交 exec sleep 的任务，等 exec 主进程在 exec 环境中运行，返回任务 ID 与该 exec 环境。
func (s *sysHarness) e37Started(requestID string) (string, string) {
	s.t.Helper()
	id := s.submit(requestID, spec(nil, requestID, nil, execStep("e1", e37Code)))
	var envID string
	eventually(s.t, "exec 主进程在 exec 环境中运行", func() bool {
		var started int
		pgQueryRow(s.t, s.dsn, `SELECT count(*) FROM call_tries WHERE task_id = $1 AND call_id = $2 AND exec_started_at IS NOT NULL`,
			[]any{id, e37CallID}, &started)
		if started == 0 {
			return false
		}
		pgQueryRow(s.t, s.dsn, `SELECT env_id FROM call_tries WHERE task_id = $1 AND call_id = $2 AND try_no = 1`,
			[]any{id, e37CallID}, &envID)
		for _, pid := range cgroupPids(filepath.Join(s.installCgroup(), "env-"+envID)) {
			if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil && bytes.Contains(b, []byte("/in/.agentbox/main.py")) {
				return true
			}
		}
		return false
	})
	return id, envID
}

// killServer 以 SIGKILL 杀死当前 server，等待其数据库连接全部结束。
func (s *sysHarness) killServer() {
	s.t.Helper()
	p := s.srv
	if err := p.cmd.Process.Kill(); err != nil {
		s.t.Fatal(err)
	}
	if ps := s.waitExit(p); !killedBySIGKILL(ps) {
		s.t.Fatalf("server 退出状态 %v", ps)
	}
	s.waitConnsGone()
}

// TestRealE37ServerKilledDuringExec：E37——exec（sleep 8 s）运行中 SIGKILL cmd/agentbox server 并重启：
//   - 旧 exec 环境的 stopped_at 在新 try 的环境建立之前记录（重跑前确认此前的 exec 环境已停止），旧 try 为 unknown；
//   - attempt 1 为 lost_on_restart，attempt 2 的 Worker 以同一 call id 重发，新 try 在新环境中完成，任务成功；
//   - 另一例：先取消任务（202 已返回）再立即杀死 server → 恢复后旧环境已停止，任务 cancelled，没有新的 try。
func TestRealE37ServerKilledDuringExec(t *testing.T) {
	t.Run("rerun", func(t *testing.T) {
		s := newRealSys(t)
		s.startReal()
		id, env1 := s.e37Started("e37")
		s.killServer()
		pre := s.e37Tries(id)
		if len(pre) != 1 || pre[0].StoppedAt != nil {
			t.Fatalf("server 被杀死时的 try：%+v；期望 1 个、环境未记录停止", pre)
		}
		s.startReal()
		v := s.waitTerminal(id)
		in := s.inspect(id)
		tries := s.e37Tries(id)
		if v.Status != "succeeded" || attemptByNo(in, 1).OutcomeClass != runner.ClassLostOnRestart || len(tries) != 2 {
			t.Fatalf("恢复后任务 %s，attempts %s，tries %+v", v.Status, attemptClasses(in), tries)
		}
		old, cur := tries[0], tries[1]
		if old.EnvID != env1 || old.Outcome != "unknown" || old.StoppedAt == nil || cur.EnvID == env1 || cur.Outcome != "ok" ||
			!old.StoppedAt.Before(cur.CreatedAt) {
			t.Fatalf("旧 try %+v，新 try %+v；期望旧环境先确认停止（stopped_at）再建立新 try 的环境", old, cur)
		}
		var res, state string
		pgQueryRow(t, s.dsn, `SELECT state, COALESCE(result_ref, '') FROM calls WHERE task_id = $1 AND call_id = $2`,
			[]any{id, e37CallID}, &state, &res)
		rc, err := s.blobs.Open(res)
		if err != nil {
			t.Fatalf("调用 %s 的结果 blob %q: %v", state, res, err)
		}
		b, err := io.ReadAll(rc)
		_ = rc.Close() // 只读句柄，关闭错误无关紧要
		var r execResult
		if err != nil || json.Unmarshal(b, &r) != nil || state != "completed" || r.Status != "completed" || r.Stdout != "done\n" {
			t.Fatalf("调用 %s；结果 %s %v", state, b, err)
		}
		t.Logf("E37：旧 exec 环境 %s stopped_at %s（unknown）→ 新 try 环境 %s 建立于 %s（ok）；attempts %s", old.EnvID,
			old.StoppedAt.Format(time.RFC3339Nano), cur.EnvID, cur.CreatedAt.Format(time.RFC3339Nano), attemptClasses(in))
		s.finishReal()
	})
	t.Run("cancelled_not_rerun", func(t *testing.T) {
		s := newRealSys(t)
		s.startReal()
		id, env1 := s.e37Started("e37-cancel")
		st, b, err := httpDo("POST", s.srv.base+"/tasks/"+id+"/cancel", `{"request_id":"e37-cancel-req","reason":"e2e"}`)
		if err != nil || (st != http.StatusAccepted && st != http.StatusOK) {
			t.Fatalf("取消 = %d %s %v", st, b, err)
		}
		s.killServer()
		s.startReal()
		v := s.waitTerminal(id)
		time.Sleep(time.Second) // 给任何（错误的）重跑留出时间
		tries := s.e37Tries(id)
		if v.Status != "cancelled" || len(tries) != 1 || tries[0].EnvID != env1 || tries[0].StoppedAt == nil {
			t.Fatalf("取消后杀死 server：任务 %s，tries %+v；期望 cancelled、只有原来的 1 个 try 且其环境已停止", v.Status, tries)
		}
		t.Logf("E37（取消）：任务 %s，唯一 try %+v", v.Status, tries[0])
		s.finishReal()
	})
}

// ==== M4 Plan 15 Task 12 段结束 ====
