package runner_test

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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// ---- 测试装置：真实 PostgreSQL（Plan 4 Store）、临时 BlobStore、回放脚本的 fake Starter ----

const dsnEnv = "AGENTBOX_TEST_DATABASE_URL"

// env 是一个独立的临时数据库、BlobStore 与宿主 workspace。
type env struct {
	store *postgres.Store
	blobs *blob.Local
	ws    string // 宿主侧 workspace 根；产物目录为 ws/out/<attempt_id>
}

func newEnv(t *testing.T) *env {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("产物保存依赖 Linux openat2")
	}
	admin := os.Getenv(dsnEnv)
	if admin == "" {
		if os.Getenv("CI") == "true" {
			t.Fatalf("CI 中必须设置 %s", dsnEnv)
		}
		t.Skipf("未设置 %s，跳过 PostgreSQL 测试", dsnEnv)
	}
	ctx := context.Background()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_test_" + hex.EncodeToString(b)
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("连接测试数据库: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("创建测试数据库: %v", err)
	}
	_ = conn.Close(ctx)
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
	s, err := postgres.Open(ctx, postgres.Options{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.InitializeInstallation(ctx, "install-test", make([]byte, ownership.TokenSize)); err != nil {
		t.Fatalf("初始化: %v", err)
	}
	if err := s.CompleteInstallation(ctx, "install-test"); err != nil {
		t.Fatalf("完成安装: %v", err)
	}
	bl, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &env{store: s, blobs: bl, ws: t.TempDir()}
}

// newTask 建立任务 taskID 及其当前 attempt att-<taskID>。
func (e *env) newTask(t *testing.T, taskID string) string {
	t.Helper()
	ctx := context.Background()
	if _, err := e.store.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-" + taskID, BodyHash: []byte("h"), TaskID: taskID,
		Spec: json.RawMessage(`{"worker":"sim"}`), MaxFaultRetries: 3}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	att := "att-" + taskID
	if _, err := e.store.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: att, AttemptNo: 1, EnvID: "env-" + taskID}); err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
	return att
}

// faultStore 在真实 Store 之前注入 CommitCheckpoint 的故障：commitFault 返回非 nil 错误时，
// commitFirst 为 true 则先真实提交再返回该错误（模拟"已提交但回复丢失"）。
type faultStore struct {
	runner.Store
	commits     int
	commitFault func(n int) (commitFirst bool, err error)

	mu            sync.Mutex
	appends       int
	appendFault   func(n int) error // 非 nil 错误：AppendWorkerEvents 不写入并返回该错误
	registerFault error             // RegisterArtifact 不写入并返回该错误
	recordDelay   time.Duration     // RecordTerminalProposal 先等待（受 ctx 约束）
}

func (f *faultStore) AppendWorkerEvents(ctx context.Context, attemptID string, events []runner.WorkerEvent) (runner.Watermark, error) {
	f.mu.Lock()
	f.appends++
	n := f.appends
	f.mu.Unlock()
	if f.appendFault != nil {
		if err := f.appendFault(n); err != nil {
			return runner.Watermark{}, err
		}
	}
	return f.Store.AppendWorkerEvents(ctx, attemptID, events)
}

func (f *faultStore) RegisterArtifact(ctx context.Context, a runner.Artifact) (runner.ArtifactVersion, error) {
	if f.registerFault != nil {
		return runner.ArtifactVersion{}, f.registerFault
	}
	return f.Store.RegisterArtifact(ctx, a)
}

func (f *faultStore) RecordTerminalProposal(ctx context.Context, p runner.TerminalProposal) (runner.TerminalProposal, error) {
	if f.recordDelay > 0 {
		select {
		case <-time.After(f.recordDelay):
		case <-ctx.Done():
			return runner.TerminalProposal{}, ctx.Err()
		}
	}
	return f.Store.RecordTerminalProposal(ctx, p)
}

func (f *faultStore) CommitCheckpoint(ctx context.Context, c runner.Checkpoint) (runner.CommittedCheckpoint, error) {
	f.commits++
	if f.commitFault != nil {
		if first, err := f.commitFault(f.commits); err != nil {
			if first {
				if _, cerr := f.Store.CommitCheckpoint(ctx, c); cerr != nil {
					return runner.CommittedCheckpoint{}, cerr
				}
			}
			return runner.CommittedCheckpoint{}, err
		}
	}
	return f.Store.CommitCheckpoint(ctx, c)
}

// step 是脚本化 Worker 的一步。
type step func(p *fakeProc) error

// w 写一条 Worker 消息（map 编码为一行 JSON，string 原样写出）。
func w(msg any) step {
	return func(p *fakeProc) error {
		line, ok := msg.(string)
		if !ok {
			b, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			line = string(b)
		}
		_, err := io.WriteString(p.outW, line+"\n")
		return err
	}
}

// h 等待宿主送达的下一条消息（init 之后、未被丢弃的），记入 p.got。
func h() step {
	return func(p *fakeProc) error {
		select {
		case m := <-p.host:
			p.mu.Lock()
			p.got = append(p.got, m)
			p.mu.Unlock()
			return nil
		case <-p.kill:
			return errors.New("已被终止")
		case <-time.After(10 * time.Second):
			return errors.New("等待宿主消息超时")
		}
	}
}

// do 在脚本中执行一个动作（例如改写文件）。
func do(f func() error) step { return func(*fakeProc) error { return f() } }

// raw 原样写出 s（不追加换行），用于构造末尾半行。
func raw(s string) step {
	return func(p *fakeProc) error {
		_, err := io.WriteString(p.outW, s)
		return err
	}
}

// ctl 以宿主 actor 的身份把控制请求交给 runner（Run 的 controls 通道）。与 actor 一样，只在
// OnReady 之后发送。
func ctl(kind string, graceMs int64) step {
	return func(p *fakeProc) error {
		select {
		case <-p.readied:
		case <-time.After(10 * time.Second):
			return errors.New("Worker 未就绪")
		}
		return ctlNow(kind, graceMs)(p)
	}
}

// ctlNow 立即发送控制请求（不等待就绪）。
func ctlNow(kind string, graceMs int64) step {
	return func(p *fakeProc) error {
		select {
		case p.controls <- runner.Control{Kind: kind, GraceMs: graceMs}:
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("runner 未接收控制请求")
		}
	}
}

// untilKilled 一直挂起，直到 runner 终止本进程（模拟不理会期限的 Worker）。
func untilKilled() step {
	return func(p *fakeProc) error {
		select {
		case <-p.kill:
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("未被终止")
		}
	}
}

// pause 暂停脚本 d（给处理器时间处理已写出的行）。
func pause(d time.Duration) step {
	return func(*fakeProc) error { time.Sleep(d); return nil }
}

// fakeProc 是一个回放脚本的执行句柄：stdout 由脚本写出，stdin 全部读出并记录。
type fakeProc struct {
	script   []step
	exitCode int
	drop     func(m map[string]any) bool // 返回 true 的宿主消息视为在传输中丢失
	// holdStdout：退出时不关闭 stdout 写端（模拟仍持有管道的子进程），用于屏障 A。
	holdStdout bool
	controls   chan runner.Control
	readied    chan struct{} // runner 调用 OnReady 后关闭

	stdinR, outR, errR *io.PipeReader
	stdinW, outW, errW *io.PipeWriter
	host               chan map[string]any
	kill               chan struct{}
	killOnce           sync.Once
	done               chan struct{}
	stdinDone          chan struct{} // stdin 读到 EOF（runner 关闭 stdin）后关闭
	status             provider.ExitStatus

	mu        sync.Mutex
	init      map[string]any
	all       []map[string]any // init 之后宿主写出的全部消息（含丢弃的）
	got       []map[string]any // 脚本经 h() 收到的消息
	scriptErr error
}

func (p *fakeProc) Stdin() io.WriteCloser { return p.stdinW }
func (p *fakeProc) Stdout() io.ReadCloser { return p.outR }
func (p *fakeProc) Stderr() io.ReadCloser { return p.errR }
func (p *fakeProc) Wait() (provider.ExitStatus, error) {
	<-p.done
	return p.status, nil
}
func (p *fakeProc) Terminate(time.Duration) error {
	p.killOnce.Do(func() { close(p.kill) })
	return nil
}

func (p *fakeProc) readStdin() {
	defer close(p.stdinDone)
	br := bufio.NewReader(p.stdinR)
	first := true
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			var m map[string]any
			if jerr := json.Unmarshal(line, &m); jerr != nil {
				m = map[string]any{"undecodable": string(line)}
			}
			p.mu.Lock()
			if first {
				p.init, first = m, false
				p.mu.Unlock()
				continue
			}
			p.all = append(p.all, m)
			p.mu.Unlock()
			if p.drop == nil || !p.drop(m) {
				p.host <- m
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *fakeProc) run() {
	_, _ = io.WriteString(p.errW, "fake worker stderr\n")
	var err error
	for _, s := range p.script {
		if err = s(p); err != nil {
			break
		}
	}
	p.mu.Lock()
	p.scriptErr = err
	p.mu.Unlock()
	if !p.holdStdout {
		_ = p.outW.Close()
	}
	_ = p.errW.Close()
	select {
	case <-p.kill:
		p.status = provider.ExitStatus{Code: -1, Signal: 9}
	default:
		p.status = provider.ExitStatus{Code: p.exitCode}
	}
	close(p.done)
}

type fakeStarter struct {
	proc *fakeProc
}

func (s *fakeStarter) StartExec(context.Context, string, provider.ExecSpec) (provider.ExecHandle, error) {
	p := s.proc
	p.stdinR, p.stdinW = io.Pipe()
	p.outR, p.outW = io.Pipe()
	p.errR, p.errW = io.Pipe()
	p.host = make(chan map[string]any, 1024)
	p.kill = make(chan struct{})
	p.done = make(chan struct{})
	p.stdinDone = make(chan struct{})
	go p.readStdin()
	go p.run()
	return p, nil
}

// tcase 是一次 attempt：任务、attempt、产物目录与运行参数。
type tcase struct {
	e       *env
	taskID  string
	att     string
	outDir  string
	fs      *faultStore
	opt     runner.Options
	drop    func(map[string]any) bool
	init    *protocol.Init
	onReady func()
	hold    bool
	cancel  context.CancelCauseFunc // 本次 Run 的 ctx 的取消函数（run 开始时设置）
}

func (e *env) newCase(t *testing.T, taskID string) *tcase {
	t.Helper()
	att := e.newTask(t, taskID)
	out := filepath.Join(e.ws, "out", att)
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	return &tcase{e: e, taskID: taskID, att: att, outDir: out, fs: &faultStore{Store: e.store}}
}

func (c *tcase) run(t *testing.T, exitCode int, script ...step) (runner.Outcome, *fakeProc) {
	t.Helper()
	init := c.init
	if init == nil {
		init = &protocol.Init{Type: protocol.TypeInit, Bootstrap: 1, ProtocolVersions: []int64{1}, Mode: protocol.ModeTask,
			TaskID: c.taskID, AttemptID: c.att, AttemptNo: 1, OutDir: "/workspace/out/" + c.att}
	}
	controls := make(chan runner.Control)
	p := &fakeProc{script: script, exitCode: exitCode, drop: c.drop, holdStdout: c.hold, controls: controls,
		readied: make(chan struct{})}
	onReady := func() {
		if c.onReady != nil {
			c.onReady()
		}
		close(p.readied)
	}
	r := runner.New(c.fs, c.e.blobs, &fakeStarter{proc: p}, nil, c.opt)
	base, cancelBase := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBase()
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	c.cancel = cancel
	out := r.Run(ctx, runner.Attempt{TaskID: c.taskID, AttemptID: c.att, AttemptNo: init.AttemptNo, EnvID: "env-" + c.taskID,
		Init: *init, OutDir: c.outDir, OnReady: onReady}, controls)
	<-p.stdinDone // Run 返回前已关闭 stdin；等记录完它写出的全部消息
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.scriptErr != nil {
		t.Fatalf("脚本: %v（宿主消息 %v）", p.scriptErr, p.all)
	}
	return out, p
}

func (c *tcase) watermark(t *testing.T) int64 {
	t.Helper()
	wm, err := c.e.store.WorkerEventWatermark(context.Background(), c.att)
	if err != nil {
		t.Fatal(err)
	}
	return wm.WorkerSeq
}

func (c *tcase) latest(t *testing.T) string {
	t.Helper()
	st, err := c.e.store.LoadTask(context.Background(), c.taskID)
	if err != nil {
		t.Fatal(err)
	}
	if st.Latest == nil {
		return ""
	}
	return st.Latest.CheckpointID
}

func (c *tcase) write(t *testing.T, name, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(c.outDir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return sum(content)
}

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// ev 构造一条 Worker 事件。
func ev(typ string, seq int, kv ...any) map[string]any {
	m := map[string]any{"type": typ, "v": 1, "seq": seq}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

func ready(seq int) map[string]any {
	return ev("ready", seq, "protocol_version", 1, "mode", "task", "worker", map[string]any{"name": "fake", "version": "0"}, "capabilities", []string{})
}

func checkpoint(seq int, id string, kv ...any) map[string]any {
	m := ev("checkpoint", seq, "checkpoint_id", id, "scope", "task", "step_id", "s-"+id)
	if len(kv) == 0 {
		m["state"] = map[string]any{"cp": id}
	}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

func artifact(seq int, id, path, sha string, size int) map[string]any {
	return ev("artifact", seq, "artifact_id", id, "path", path, "declared_sha256", sha, "declared_size", size,
		"media_type", "text/plain", "visibility", "output")
}

func result(seq int, outputs ...string) map[string]any {
	if outputs == nil {
		outputs = []string{}
	}
	return ev("result", seq, "summary", "done", "outputs", outputs)
}

// reply 断言宿主回复的类型、状态与错误码。
func reply(t *testing.T, m map[string]any, typ, status, code string) {
	t.Helper()
	gotCode, _ := m["code"].(string)
	if m["type"] != typ || m["status"] != status || gotCode != code {
		t.Fatalf("回复 %v，期望 %s/%s/%q", m, typ, status, code)
	}
}

func noViolation(t *testing.T, out runner.Outcome) {
	t.Helper()
	if out.Violation != "" || out.ExitErr != nil {
		t.Fatalf("意外的违规或错误: %q %v", out.Violation, out.ExitErr)
	}
}

// pinned 解析 result 提议内容中的 outputs。
func pinned(t *testing.T, out runner.Outcome) []map[string]any {
	t.Helper()
	if out.Proposal == nil || out.Proposal.Kind != "result" {
		t.Fatalf("没有 result 提议: %+v", out.Proposal)
	}
	var c struct {
		Outputs []map[string]any `json:"outputs"`
	}
	if err := json.Unmarshal(out.ResultPayload, &c); err != nil {
		t.Fatal(err)
	}
	return c.Outputs
}

func blobContent(t *testing.T, e *env, sha string) string {
	t.Helper()
	rc, err := e.blobs.Open(sha)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ---- 规格 §5.11 的 protocol 场景转录回放 ----

type scenario struct {
	Name  string `json:"name"`
	Lines []struct {
		From    string          `json:"from"`
		Message json.RawMessage `json:"message"`
	} `json:"lines"`
	Expect struct {
		Stream    string `json:"stream"`
		Violation string `json:"violation"`
		At        int    `json:"at"`
	} `json:"expect"`
	SDK *struct {
		ExitCode int `json:"exit_code"`
	} `json:"sdk"`
}

func decodeMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// TestScenarioFixtures 以 protocol/fixtures 的 task 模式场景驱动 AttemptRunner：Worker 行按转录写出，
// 宿主行处等待 runner 的回复并逐字段比较；再核对违规、已持久化的事件水位、终态提议与退出码。
// 转录中的宿主 cancel/pause 由测试以 actor 身份经 controls 交给 runner，再核对 runner 发出的协议消息。
func TestScenarioFixtures(t *testing.T) {
	e := newEnv(t)
	dir := filepath.Join("..", "..", "protocol", "fixtures", "v1", "scenarios")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, ent := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, ent.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var sc scenario
		if err := json.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("%s: %v", ent.Name(), err)
		}
		ran++
		t.Run(sc.Name, func(t *testing.T) { replay(t, e, sc) })
	}
	if ran < 22 {
		t.Fatalf("只回放了 %d 个场景", ran)
	}
}

func replay(t *testing.T, e *env, sc scenario) {
	c := e.newCase(t, "fx-"+sc.Name)
	ctx := context.Background()
	switch sc.Name {
	case "checkpoint_conflict": // 同一 checkpoint_id 已以不同内容提交
		if _, err := e.store.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: c.taskID},
			CheckpointID: "cp-1", AttemptID: c.att, StepID: "s1", State: json.RawMessage(`{"next_index":99}`)}); err != nil {
			t.Fatal(err)
		}
	case "retryable_error_then_committed": // 第一次提交时 Store 不可用（确认未提交）
		c.fs.commitFault = func(n int) (bool, error) {
			if n == 1 {
				return false, persistence.ErrUnavailable
			}
			return false, nil
		}
	case "ack_lost_retry_same_result": // 第一个 checkpoint_result 在传输中丢失
		dropped := false
		c.drop = func(m map[string]any) bool {
			if !dropped && m["type"] == "checkpoint_result" {
				dropped = true
				return true
			}
			return false
		}
	case "artifact_registered":
		c.write(t, "report.md", "# 报告\n")
	case "event_after_paused": // runner 另检查 paused 指向最新已提交的 checkpoint（§5.9）；本场景只测流规则
		if _, err := e.store.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: c.taskID},
			CheckpointID: "cp-1", AttemptID: c.att, StepID: "s1", State: json.RawMessage(`{"n":1}`)}); err != nil {
			t.Fatal(err)
		}
	}

	var init protocol.Init
	if err := json.Unmarshal(sc.Lines[0].Message, &init); err != nil {
		t.Fatal(err)
	}
	init.TaskID, init.AttemptID = c.taskID, c.att
	c.init = &init

	var script []step
	var wantHost []map[string]any
	var accepted, terminal, wantControl string
	acceptedSeq := 0
	for i, l := range sc.Lines[1:] {
		idx := i + 1
		var compact bytes.Buffer
		if err := json.Compact(&compact, l.Message); err != nil {
			t.Fatal(err)
		}
		if l.From == "host" {
			m := decodeMap(t, l.Message)
			if typ := m["type"]; typ == "cancel" || typ == "pause" {
				// 宿主控制：actor 在此处把请求交给 runner，runner 发出协议消息
				grace, _ := m["grace_ms"].(float64)
				script = append(script, ctl(typ.(string), int64(grace)))
				m["attempt_id"] = c.att
				wantControl = typ.(string)
			}
			script = append(script, h())
			wantHost = append(wantHost, m)
			continue
		}
		script = append(script, w(compact.String()))
		if sc.Expect.Stream == "violation" && idx >= sc.Expect.At {
			continue
		}
		m := decodeMap(t, l.Message)
		accepted, _ = m["type"].(string)
		if _, ok := m["seq"]; ok {
			acceptedSeq++
		}
		if terminal == "" && (accepted == "result" || accepted == "error" || accepted == "paused") {
			terminal = accepted
		}
	}
	exit := 0
	if sc.SDK != nil {
		exit = sc.SDK.ExitCode
	}
	out, p := c.run(t, exit, script...)

	wantInit := decodeMap(t, mustEncode(t, &init))
	if !reflect.DeepEqual(p.init, wantInit) {
		t.Fatalf("init = %v，期望 %v", p.init, wantInit)
	}
	// 控制消息由主循环发出，与处理器的回复之间没有确定的先后；分别比较两者各自的顺序
	split := func(ms []map[string]any) (ctl, rest []map[string]any) {
		for _, m := range ms {
			if m["type"] == "cancel" || m["type"] == "pause" {
				ctl = append(ctl, m)
			} else {
				rest = append(rest, m)
			}
		}
		return ctl, rest
	}
	gotCtl, gotRest := split(p.got)
	wantCtl, wantRest := split(wantHost)
	if !reflect.DeepEqual(gotCtl, wantCtl) || !reflect.DeepEqual(gotRest, wantRest) {
		t.Fatalf("宿主回复 = %v，期望 %v", p.got, wantHost)
	}
	if wantControl != "" {
		// 宿主意图生效后在 grace 内正常结束：按意图分类，不是平台终止，不重试（§5.8、§5.9）
		want := map[string]string{"cancel": runner.ClassCancelled, "pause": runner.ClassPaused}[wantControl]
		if out.Class != want || out.Retry != "" || out.PlatformKilled || out.Control != wantControl {
			t.Fatalf("分类 = %s/%q killed=%v control=%q，期望 %s", out.Class, out.Retry, out.PlatformKilled, out.Control, want)
		}
	}
	wantViolation := ""
	switch {
	case sc.Expect.Stream == "violation":
		wantViolation = sc.Expect.Violation
	case accepted == "handshake_error":
		wantViolation = runner.ViolationHandshakeError
	}
	if out.Violation != wantViolation {
		t.Fatalf("violation = %q，期望 %q", out.Violation, wantViolation)
	}
	if wantViolation != "" && (out.Retry != "" || out.Class == runner.ClassSucceeded) { // §5.8：协议违规不重试
		t.Fatalf("违规的分类 = %s/%q", out.Class, out.Retry)
	}
	if got := c.watermark(t); got != int64(acceptedSeq) {
		t.Fatalf("已持久化事件水位 = %d，期望 %d", got, acceptedSeq)
	}
	if terminal == "" {
		if out.Proposal != nil {
			t.Fatalf("意外的终态提议 %+v", out.Proposal)
		}
	} else {
		if out.Proposal == nil || out.Proposal.Kind != terminal {
			t.Fatalf("终态提议 = %+v，期望 %s", out.Proposal, terminal)
		}
		stored, err := e.store.GetTerminalProposal(ctx, c.att)
		if err != nil || stored != *out.Proposal {
			t.Fatalf("已记录的提议 = %+v (%v)，期望 %+v", stored, err, *out.Proposal)
		}
		if got := blobContent(t, e, out.Proposal.Ref); got != string(out.ResultPayload) {
			t.Fatalf("提议内容 blob = %s，Outcome 为 %s", got, out.ResultPayload)
		}
	}
	if out.Exit.Code != exit || out.ExitErr != nil {
		t.Fatalf("退出 = %+v %v，期望 %d", out.Exit, out.ExitErr, exit)
	}
}

func mustEncode(t *testing.T, m protocol.Message) []byte {
	t.Helper()
	b, err := protocol.EncodeLine(protocol.HostToWorker, m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ---- 规格 §5.11 中没有转录文件的 runner/Store 层场景，以及 E10 的单元层 ----

func TestRunnerCases(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	t.Run("on_ready_called_once", func(t *testing.T) {
		c := e.newCase(t, "ready-once")
		calls := 0
		c.onReady = func() { calls++ }
		out, _ := c.run(t, 0, w(ready(1)), w(ev("progress", 2, "kind", "step_started", "message", "x")), w(result(3)))
		noViolation(t, out)
		if calls != 1 {
			t.Fatalf("OnReady 调用 %d 次，期望 1", calls)
		}
		if !strings.Contains(string(out.StderrTail), "fake worker stderr") {
			t.Fatalf("stderr 末尾 = %q", out.StderrTail)
		}

		c = e.newCase(t, "ready-never")
		calls = 0
		c.onReady = func() { calls++ }
		out, _ = c.run(t, 1)
		if calls != 0 || out.Proposal != nil || out.Exit.Code != 1 {
			t.Fatalf("未 ready 即退出：OnReady %d 次，提议 %+v，退出 %+v", calls, out.Proposal, out.Exit)
		}
	})

	t.Run("missing_ref", func(t *testing.T) {
		c := e.newCase(t, "missing-ref")
		absent := sum("never stored")
		out, p := c.run(t, 0, w(ready(1)),
			w(checkpoint(2, "cp-1", "state", map[string]any{"n": 1}, "refs", []string{absent})), h(),
			w(checkpoint(3, "cp-2", "state_ref", absent)), h(),
			w(result(4)))
		noViolation(t, out)
		reply(t, p.got[0], "checkpoint_result", "rejected", "missing_ref")
		reply(t, p.got[1], "checkpoint_result", "rejected", "missing_ref")
		if l := c.latest(t); l != "" {
			t.Fatalf("被拒绝的 checkpoint 推进了指针到 %s", l)
		}
		if wm := c.watermark(t); wm != 4 {
			t.Fatalf("水位 %d", wm)
		}
	})

	t.Run("old_checkpoint_no_rewind", func(t *testing.T) {
		c := e.newCase(t, "no-rewind")
		out, p := c.run(t, 0, w(ready(1)),
			w(checkpoint(2, "cp-1")), h(),
			w(checkpoint(3, "cp-2")), h(),
			w(checkpoint(4, "cp-1")), h(), // 重试旧 checkpoint：只读取，返回原结果
			w(result(5)))
		noViolation(t, out)
		for _, m := range p.got {
			reply(t, m, "checkpoint_result", "committed", "")
		}
		if l := c.latest(t); l != "cp-2" {
			t.Fatalf("指针 = %q，期望 cp-2", l)
		}
		cp1, err := e.store.QueryCheckpoint(ctx, runner.Scope{Kind: "task", ID: c.taskID}, "cp-1")
		if err != nil || cp1.CommitSeq != 1 {
			t.Fatalf("cp-1 = %+v %v", cp1, err)
		}
	})

	// foreignBlob 让另一个任务保存并登记一个 blob：它存在于 BlobStore，但只授权给那个任务。
	foreignBlob := func(t *testing.T, taskID string) string {
		other := e.newTask(t, taskID)
		ref, err := e.blobs.Put(ctx, strings.NewReader(`{"secret":"`+taskID+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.store.RegisterArtifact(ctx, runner.Artifact{TaskID: taskID, AttemptID: other, ArtifactID: "x",
			SHA256: ref.SHA256, Size: ref.Size, MediaType: "application/json", Visibility: "output"}); err != nil {
			t.Fatal(err)
		}
		return ref.SHA256
	}

	t.Run("foreign_blob_rejected", func(t *testing.T) {
		foreign := foreignBlob(t, "foreign-a")
		c := e.newCase(t, "foreign-refs")
		out, p := c.run(t, 0, w(ready(1)),
			w(checkpoint(2, "cp-1", "state", map[string]any{"n": 1}, "refs", []string{foreign})), h(),
			w(result(3)))
		noViolation(t, out)
		reply(t, p.got[0], "checkpoint_result", "rejected", persistence.CodeRefNotAuthorized)
		if l := c.latest(t); l != "" {
			t.Fatalf("指针 = %q", l)
		}
	})

	t.Run("state_ref_foreign_blob_rejected", func(t *testing.T) {
		foreign := foreignBlob(t, "foreign-b")
		c := e.newCase(t, "foreign-state-ref")
		own := c.write(t, "state.json", `{"next_index":3}`)
		out, p := c.run(t, 0, w(ready(1)),
			w(checkpoint(2, "cp-1", "state_ref", foreign)), h(),
			w(artifact(3, "state", "state.json", own, len(`{"next_index":3}`))), h(),
			w(checkpoint(4, "cp-2", "state_ref", own)), h(), // 本任务保存的 blob 可以作为 state_ref
			w(result(5)))
		noViolation(t, out)
		reply(t, p.got[0], "checkpoint_result", "rejected", persistence.CodeRefNotAuthorized)
		reply(t, p.got[1], "artifact_result", "saved", "")
		reply(t, p.got[2], "checkpoint_result", "committed", "")
		if l := c.latest(t); l != "cp-2" {
			t.Fatalf("指针 = %q", l)
		}
	})

	t.Run("artifact_fail_blocks_checkpoint", func(t *testing.T) {
		c := e.newCase(t, "artifact-fail")
		real := c.write(t, "a.txt", "real")
		declared := sum("fake")
		good := c.write(t, "b.txt", "good content")
		out, p := c.run(t, 0, w(ready(1)),
			w(artifact(2, "a", "a.txt", declared, 4)), // 声明的哈希与文件内容不符
			w(checkpoint(3, "cp-1", "state", map[string]any{"n": 1}, "refs", []string{declared})), h(), h(),
			w(checkpoint(4, "cp-2", "state", map[string]any{"n": 2}, "refs", []string{real})), h(),
			w(artifact(5, "b", "b.txt", good, len("good content"))),
			w(checkpoint(6, "cp-3", "state", map[string]any{"n": 3}, "refs", []string{good})), h(), h(),
			w(result(7)))
		noViolation(t, out)
		// 回复顺序即处理顺序：产物保存完成（或失败）后才处理其后的 checkpoint
		reply(t, p.got[0], "artifact_result", "rejected", "hash_mismatch")
		reply(t, p.got[1], "checkpoint_result", "rejected", "missing_ref")
		// 实际内容已写入 BlobStore，但未登记，因此未授权到任务
		reply(t, p.got[2], "checkpoint_result", "rejected", persistence.CodeRefNotAuthorized)
		reply(t, p.got[3], "artifact_result", "saved", "")
		reply(t, p.got[4], "checkpoint_result", "committed", "")
		if _, err := e.store.GetArtifact(ctx, c.taskID, "a", real); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("哈希不符的产物被登记: %v", err)
		}
		if l := c.latest(t); l != "cp-3" {
			t.Fatalf("指针 = %q", l)
		}
	})

	inDir := filepath.Join(e.ws, "in")
	if err := os.MkdirAll(inDir, 0o755); err != nil {
		t.Fatal(err)
	}
	secret := "input secret"
	if err := os.WriteFile(filepath.Join(inDir, "secret"), []byte(secret), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("artifact_path_escape_out_to_in", func(t *testing.T) {
		c := e.newCase(t, "escape-dotdot")
		out, p := c.run(t, 0, w(ready(1)), w(artifact(2, "x", "../../in/secret", sum(secret), len(secret))))
		if out.Violation != protocol.CodePathInvalid {
			t.Fatalf("violation = %q，期望 path_invalid", out.Violation)
		}
		if len(p.all) != 0 {
			t.Fatalf("越界路径得到了回复 %v", p.all)
		}
		if _, err := e.store.GetArtifact(ctx, c.taskID, "x", sum(secret)); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("越界产物被登记: %v", err)
		}
		if wm := c.watermark(t); wm != 1 {
			t.Fatalf("水位 %d", wm)
		}
	})

	t.Run("artifact_symlink_rejected", func(t *testing.T) {
		c := e.newCase(t, "escape-symlink")
		for name, target := range map[string]string{
			"link":    filepath.Join("..", "..", "in", "secret"), // 相对链接指向 /in
			"abslink": filepath.Join(inDir, "secret"),            // 绝对链接
			"dirlink": filepath.Join("..", "..", "in"),           // 中间分量是目录链接
			"inlink":  "real.txt",                                // 指向 out_dir 内的链接同样拒绝
		} {
			if err := os.Symlink(target, filepath.Join(c.outDir, name)); err != nil {
				t.Fatal(err)
			}
		}
		c.write(t, "real.txt", secret)
		out, p := c.run(t, 0, w(ready(1)),
			w(artifact(2, "a", "link", sum(secret), len(secret))), h(),
			w(artifact(3, "b", "abslink", sum(secret), len(secret))), h(),
			w(artifact(4, "c", "dirlink/secret", sum(secret), len(secret))), h(),
			w(artifact(5, "d", "inlink", sum(secret), len(secret))), h(),
			w(result(6)))
		noViolation(t, out)
		for _, m := range p.got {
			reply(t, m, "artifact_result", "rejected", "path_invalid")
		}
	})

	t.Run("artifact_fifo_rejected", func(t *testing.T) {
		c := e.newCase(t, "fifo")
		if err := exec.Command("mkfifo", filepath.Join(c.outDir, "pipe")).Run(); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		if err := os.Mkdir(filepath.Join(c.outDir, "subdir"), 0o755); err != nil {
			t.Fatal(err)
		}
		out, p := c.run(t, 0, w(ready(1)),
			w(artifact(2, "p", "pipe", sum(""), 0)), h(), // 没有写端：打开不得阻塞
			w(artifact(3, "d", "subdir", sum(""), 0)), h(),
			w(artifact(4, "m", "missing.txt", sum(""), 0)), h(),
			w(result(5)))
		noViolation(t, out)
		reply(t, p.got[0], "artifact_result", "rejected", "not_regular_file")
		reply(t, p.got[1], "artifact_result", "rejected", "not_regular_file")
		reply(t, p.got[2], "artifact_result", "rejected", "path_invalid")
	})

	t.Run("artifact_too_large", func(t *testing.T) {
		c := e.newCase(t, "too-large")
		c.opt.MaxArtifactBytes = 8
		big := c.write(t, "big.bin", "0123456789abcdef")
		out, p := c.run(t, 0, w(ready(1)),
			w(artifact(2, "big", "big.bin", big, 16)), h(),
			w(artifact(3, "liar", "big.bin", big, 4)), h(), // 声明值不可信：以实际大小判定
			w(result(4)))
		noViolation(t, out)
		reply(t, p.got[0], "artifact_result", "rejected", "artifact_too_large")
		reply(t, p.got[1], "artifact_result", "rejected", "artifact_too_large")
	})

	t.Run("artifact_rewrite_after_register", func(t *testing.T) {
		c := e.newCase(t, "rewrite")
		orig := "original content"
		sha := c.write(t, "doc.txt", orig)
		out, p := c.run(t, 0, w(ready(1)),
			w(artifact(2, "doc", "doc.txt", sha, len(orig))), h(),
			do(func() error {
				return os.WriteFile(filepath.Join(c.outDir, "doc.txt"), []byte("tampered after save"), 0o644)
			}),
			w(result(3, "doc")))
		noViolation(t, out)
		reply(t, p.got[0], "artifact_result", "saved", "")
		if got := blobContent(t, e, sha); got != orig {
			t.Fatalf("已保存副本 = %q，期望原内容", got)
		}
		outs := pinned(t, out)
		if len(outs) != 1 || outs[0]["sha256"] != sha || outs[0]["version"] != float64(1) {
			t.Fatalf("固定的输出 = %v", outs)
		}
	})

	t.Run("output_saved_by_earlier_attempt_pinned", func(t *testing.T) {
		c := e.newCase(t, "earlier")
		old, err := e.blobs.Put(ctx, strings.NewReader("from attempt 1"))
		if err != nil {
			t.Fatal(err)
		}
		// 之前的 attempt 保存的产物（同一任务）：本 attempt 不重新登记
		if _, err := e.store.RegisterArtifact(ctx, runner.Artifact{TaskID: c.taskID, AttemptID: c.att, ArtifactID: "notes",
			SHA256: old.SHA256, Size: old.Size, MediaType: "text/plain", Visibility: "output"}); err != nil {
			t.Fatal(err)
		}
		out, _ := c.run(t, 0, w(ready(1)), w(result(2, "notes", "absent")))
		noViolation(t, out)
		outs := pinned(t, out)
		if len(outs) != 2 || outs[0]["version"] != float64(1) || outs[0]["sha256"] != old.SHA256 {
			t.Fatalf("固定的输出 = %v，期望 notes@1", outs)
		}
		if _, ok := outs[1]["version"]; ok {
			t.Fatalf("任务中不存在的产物不应固定版本：%v", outs[1])
		}
	})

	t.Run("historical_output_version_pinned", func(t *testing.T) {
		c := e.newCase(t, "pinned")
		v1 := c.write(t, "report.md", "v1")
		out, p := c.run(t, 0, w(ready(1)),
			w(artifact(2, "report", "report.md", v1, 2)), h(),
			do(func() error { return os.WriteFile(filepath.Join(c.outDir, "report.md"), []byte("v2!"), 0o644) }),
			w(artifact(3, "report", "report.md", sum("v2!"), 3)), h(),
			do(func() error { return os.WriteFile(filepath.Join(c.outDir, "report.md"), []byte("v1"), 0o644) }),
			w(artifact(4, "report", "report.md", v1, 2)), h(), // 相同内容重复登记返回原版本
			w(result(5, "report")))
		noViolation(t, out)
		if p.got[0]["version"] != float64(1) || p.got[1]["version"] != float64(2) || p.got[2]["version"] != float64(1) {
			t.Fatalf("版本 = %v", p.got)
		}
		outs := pinned(t, out)
		if len(outs) != 1 || outs[0]["version"] != float64(1) || outs[0]["sha256"] != v1 {
			t.Fatalf("固定的输出 = %v，期望 report@1", outs)
		}
		// 之后登记的新版本不改变已固定的输出；历史版本仍可按 (sha256, version) 读取
		v3, err := e.blobs.Put(ctx, strings.NewReader("v3"))
		if err != nil {
			t.Fatal(err)
		}
		av, err := e.store.RegisterArtifact(ctx, runner.Artifact{TaskID: c.taskID, AttemptID: c.att, ArtifactID: "report",
			SHA256: v3.SHA256, Size: v3.Size, MediaType: "text/plain", Visibility: "output"})
		if err != nil || av.Version != 3 {
			t.Fatalf("新版本 = %+v %v", av, err)
		}
		stored, err := e.store.GetTerminalProposal(ctx, c.att)
		if err != nil || blobContent(t, e, stored.Ref) != string(out.ResultPayload) {
			t.Fatalf("已记录的提议内容改变: %+v %v", stored, err)
		}
		if old, err := e.store.GetArtifact(ctx, c.taskID, "report", v1); err != nil || old.Version != 1 || blobContent(t, e, v1) != "v1" {
			t.Fatalf("历史版本 = %+v %v", old, err)
		}
	})

	t.Run("commit_unknown_then_in_flight", func(t *testing.T) {
		c := e.newCase(t, "in-flight")
		c.fs.commitFault = func(n int) (bool, error) {
			if n == 1 { // 已提交但结果丢失
				return true, &persistence.CommitUnknownError{Op: "CommitCheckpoint", Identity: "cp-1", Err: errors.New("连接断开")}
			}
			return false, nil
		}
		out, p := c.run(t, 0, w(ready(1)),
			w(checkpoint(2, "cp-1")), h(),
			w(checkpoint(3, "cp-2")), h(), // cp-1 结果未知时提交另一个 checkpoint
			w(ev("checkpoint_query", 4, "checkpoint_id", "cp-1", "scope", "task")), h(),
			w(checkpoint(5, "cp-2")), h(),
			w(result(6)))
		noViolation(t, out)
		reply(t, p.got[0], "checkpoint_result", "retryable_error", "")
		reply(t, p.got[1], "checkpoint_result", "rejected", "commit_in_flight")
		reply(t, p.got[2], "checkpoint_result", "committed", "")
		reply(t, p.got[3], "checkpoint_result", "committed", "")
		if l := c.latest(t); l != "cp-2" {
			t.Fatalf("指针 = %q", l)
		}
	})

	t.Run("output_limit_exceeded", func(t *testing.T) {
		c := e.newCase(t, "too-big-line")
		huge := ev("progress", 2, "kind", "x", "message", strings.Repeat("a", protocol.MaxEventBytes))
		out, _ := c.run(t, 0, w(ready(1)), w(huge), w(result(3)))
		if out.Violation != runner.ViolationOutputLimit || out.Proposal != nil {
			t.Fatalf("violation = %q 提议 %+v", out.Violation, out.Proposal)
		}

		c = e.newCase(t, "attempt-total")
		c.opt.MaxAttemptEvents = 400
		var script []step
		script = append(script, w(ready(1)))
		for i := 2; i < 10; i++ {
			script = append(script, w(ev("progress", i, "kind", "x", "message", strings.Repeat("b", 40))))
		}
		out, _ = c.run(t, 0, script...)
		if out.Violation != runner.ViolationOutputLimit {
			t.Fatalf("每 attempt 合计超限: violation = %q", out.Violation)
		}
	})

	t.Run("events_persisted_in_batches", func(t *testing.T) {
		c := e.newCase(t, "batches")
		c.opt.EventBatch = 3
		script := []step{w(ready(1))}
		for i := 2; i <= 20; i++ {
			script = append(script, w(ev("progress", i, "kind", "x", "message", fmt.Sprint(i))))
		}
		script = append(script, w(result(21)))
		out, _ := c.run(t, 0, script...)
		noViolation(t, out)
		if wm := c.watermark(t); wm != 21 {
			t.Fatalf("水位 %d，期望 21", wm)
		}
	})
}

// ---- 规格 §5.8 与 §14.3：Classify 是裁决分类的唯一实现 ----

func TestClassify(t *testing.T) {
	result := &runner.TerminalProposal{Kind: "result"}
	errProp := &runner.TerminalProposal{Kind: "error"}
	paused := &runner.TerminalProposal{Kind: "paused"}
	werr := func(code string, retryable bool) json.RawMessage {
		b, _ := json.Marshal(map[string]any{"code": code, "message": "m", "retryable": retryable})
		return b
	}
	sig := func(s syscall.Signal) provider.ExitStatus { return provider.ExitStatus{Code: -1, Signal: s} }
	code := func(c int) provider.ExitStatus { return provider.ExitStatus{Code: c} }
	oom := provider.ResourceDiag{OOMKillDelta: 1, OOMObserved: true}

	cases := []struct {
		name         string
		in           runner.ClassifyInput
		class, retry string
	}{
		// §5.8
		{"result_exit0_succeeded", runner.ClassifyInput{Proposal: result}, "succeeded", ""},
		{"result_nonzero_exit_after_result", runner.ClassifyInput{Proposal: result, Exit: code(3)}, "exit_after_result", ""},
		{"business_event_after_result", runner.ClassifyInput{Proposal: result, Violation: "after_terminal"}, "protocol_violation", ""},
		{"exit_grace_after_result", runner.ClassifyInput{Proposal: result, Exit: sig(syscall.SIGKILL), PlatformKill: runner.KillExitGrace}, "exit_after_result", ""},
		{"exit_grace_after_error", runner.ClassifyInput{Proposal: errProp, Payload: werr("tool_failed", true), Exit: sig(syscall.SIGKILL), PlatformKill: runner.KillExitGrace}, "worker_error", "fault"},
		{"paused_with_request", runner.ClassifyInput{Proposal: paused, Control: "pause"}, "paused", ""},
		{"paused_without_request", runner.ClassifyInput{Proposal: paused}, "protocol_violation", ""},
		{"paused_not_latest", runner.ClassifyInput{Proposal: paused, Control: "pause", Violation: runner.ViolationPausedNotLatest}, "protocol_violation", ""},
		{"cancel_effective_exit", runner.ClassifyInput{Control: "cancel"}, "cancelled", ""},
		{"cancel_effective_crash_not_crashed", runner.ClassifyInput{Control: "cancel", Exit: sig(syscall.SIGSEGV)}, "cancelled", ""},
		{"cancel_grace_expired", runner.ClassifyInput{Control: "cancel", PlatformKill: runner.KillCancel, Exit: sig(syscall.SIGKILL), Diag: oom}, "cancelled", ""},
		{"pause_grace_expired", runner.ClassifyInput{Control: "pause", PlatformKill: runner.KillPause, Exit: sig(syscall.SIGKILL)}, "paused", ""},
		{"error_during_cancel", runner.ClassifyInput{Control: "cancel", Proposal: errProp, Payload: werr("x", true), Exit: code(1)}, "cancelled", ""},
		{"valid_result_during_cancel", runner.ClassifyInput{Proposal: result, Control: "cancel"}, "succeeded", ""},
		{"valid_result_during_pause_oom", runner.ClassifyInput{Proposal: result, Control: "pause", Diag: oom}, "oom_observed_in_attempt", ""},
		{"no_proposal_exit0", runner.ClassifyInput{}, "exited_without_proposal", ""},
		{"no_proposal_exit1", runner.ClassifyInput{Exit: code(1)}, "exited_without_proposal", ""},
		{"stdout_closed_no_exit", runner.ClassifyInput{PlatformKill: runner.KillExitGrace, Exit: sig(syscall.SIGKILL)}, "exited_without_proposal", ""},
		{"result_output_incomplete", runner.ClassifyInput{Proposal: result, OutputIncomplete: true}, "output_incomplete", ""},
		{"stdin_partial_write", runner.ClassifyInput{Proposal: result, PlatformKill: runner.KillStdinBroken}, "protocol_violation", ""},
		// §14.3
		{"crashed_signal", runner.ClassifyInput{Exit: sig(syscall.SIGSEGV)}, "crashed_signal", "fault"},
		{"sigkill_without_oom", runner.ClassifyInput{Exit: sig(syscall.SIGKILL)}, "crashed_signal", "fault"},
		{"control_lost", runner.ClassifyInput{ControlLost: true, ExitErr: provider.ErrControlLost}, "control_lost", "fault"},
		{"control_lost_after_result", runner.ClassifyInput{Proposal: result, ControlLost: true, ExitErr: provider.ErrControlLost}, "control_lost", "fault"},
		{"control_lost_at_start", runner.ClassifyInput{StartErr: provider.ErrControlLost}, "control_lost", "fault"},
		{"ready_timeout", runner.ClassifyInput{PlatformKill: runner.KillReadyTimeout, Exit: sig(syscall.SIGKILL)}, "ready_timeout", "fault"},
		{"lost_on_restart", runner.ClassifyInput{PlatformKill: runner.KillShutdown, Exit: sig(syscall.SIGKILL)}, "lost_on_restart", "fault"},
		{"start_err_env", runner.ClassifyInput{StartErr: &provider.StartError{Reason: "exec"}}, "create_failed_env", ""},
		{"start_cancelled_by_deadline", runner.ClassifyInput{StartErr: context.DeadlineExceeded, PlatformKill: runner.KillTimeout}, "task_deadline_exceeded", ""},
		{"store_unavailable", runner.ClassifyInput{PlatformKill: runner.KillStoreUnavailable, Exit: sig(syscall.SIGKILL)}, "store_unavailable", "fault"},
		{"worker_oom_likely", runner.ClassifyInput{Exit: sig(syscall.SIGKILL), Diag: oom}, "worker_oom_likely", "oom"},
		{"oom_observed_normal_end", runner.ClassifyInput{Proposal: result, Diag: oom}, "oom_observed_in_attempt", ""},
		{"oom_observed_error_by_worker", runner.ClassifyInput{Proposal: errProp, Payload: werr("x", false), Exit: code(1), Diag: oom}, "worker_error", ""},
		{"worker_error_retryable", runner.ClassifyInput{Proposal: errProp, Payload: werr("tool_failed", true), Exit: code(1)}, "worker_error", "fault"},
		{"worker_error_budget_exhausted", runner.ClassifyInput{Proposal: errProp, Payload: werr("budget_exhausted", true), Exit: code(1)}, "worker_error", ""},
		{"worker_error_protocol_prefix", runner.ClassifyInput{Proposal: errProp, Payload: werr("protocol_error", true), Exit: code(1)}, "worker_error", ""},
		{"worker_error_not_retryable", runner.ClassifyInput{Proposal: errProp, Payload: werr("bad_input", false), Exit: code(1)}, "worker_error", ""},
		{"protocol_mismatch_handshake", runner.ClassifyInput{Violation: runner.ViolationHandshakeError, Exit: code(1)}, "protocol_mismatch", ""},
		{"protocol_mismatch_mode", runner.ClassifyInput{Violation: runner.ViolationModeMismatch}, "protocol_mismatch", ""},
		{"protocol_violation", runner.ClassifyInput{Violation: "seq_invalid", Exit: sig(syscall.SIGKILL), Diag: oom}, "protocol_violation", ""},
		{"output_limit_exceeded", runner.ClassifyInput{Violation: runner.ViolationOutputLimit}, "output_limit_exceeded", ""},
		{"task_deadline_exceeded", runner.ClassifyInput{Proposal: result, PlatformKill: runner.KillTimeout, Exit: sig(syscall.SIGKILL)}, "task_deadline_exceeded", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class, retry := runner.Classify(tc.in)
			if class != tc.class || retry != tc.retry {
				t.Fatalf("Classify = %s/%q，期望 %s/%q", class, retry, tc.class, tc.retry)
			}
		})
	}

	// §14.3 中由其他组件给出、只由类别决定重试资格的行
	for class, want := range map[string]string{
		"crashed_signal": "fault", "control_lost": "fault", "ready_timeout": "fault", "lost_on_restart": "fault",
		"subrun_cancel_timeout": "fault", "create_failed_transient": "fault", "store_unavailable": "fault",
		"worker_oom_likely": "oom", "release_timeout": "", "create_failed_env": "", "create_outcome_unknown": "",
		"protocol_mismatch": "", "protocol_violation": "", "output_limit_exceeded": "", "exit_after_result": "",
		"task_deadline_exceeded": "", "cancelled": "", "paused": "",
	} {
		if got := runner.RetryOf(class); got != want {
			t.Errorf("RetryOf(%s) = %q，期望 %q", class, got, want)
		}
	}
}

// ---- 完成屏障、期限、控制与 Store 故障阈值（规格 §5.7–§5.9、§14.5） ----

func TestRunnerDeadlinesAndControls(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	fast := runner.Options{ReadyTimeout: 5 * time.Second, ExitGrace: 5 * time.Second}

	expect := func(t *testing.T, out runner.Outcome, class, retry, kill string, killed bool) {
		t.Helper()
		if out.Class != class || out.Retry != retry || out.PlatformKill != kill || out.PlatformKilled != killed {
			t.Fatalf("结果 = %s/%q kill=%q killed=%v（violation %q），期望 %s/%q kill=%q killed=%v",
				out.Class, out.Retry, out.PlatformKill, out.PlatformKilled, out.Violation, class, retry, kill, killed)
		}
	}

	t.Run("ready_timeout", func(t *testing.T) {
		c := e.newCase(t, "ready-timeout")
		c.opt = runner.Options{ReadyTimeout: 50 * time.Millisecond}
		out, _ := c.run(t, 0, untilKilled())
		expect(t, out, "ready_timeout", "fault", runner.KillReadyTimeout, true)
	})

	t.Run("exit_grace_after_result", func(t *testing.T) {
		c := e.newCase(t, "exit-grace")
		c.opt = runner.Options{ExitGrace: 50 * time.Millisecond}
		out, _ := c.run(t, 0, w(ready(1)), w(result(2)), untilKilled())
		expect(t, out, "exit_after_result", "", runner.KillExitGrace, true)
		if out.Proposal == nil || out.Proposal.Kind != "result" {
			t.Fatalf("提议 %+v", out.Proposal)
		}
	})

	t.Run("barrier_a_timeout_output_incomplete", func(t *testing.T) {
		c := e.newCase(t, "barrier-a")
		c.opt = runner.Options{DrainTimeout: 50 * time.Millisecond}
		c.hold = true // 进程退出，但执行树中仍有进程持有 stdout
		out, _ := c.run(t, 0, w(ready(1)), w(result(2)), raw(`{"type":"progress","v":1,"seq":3`))
		if !out.OutputIncomplete || out.Violation != "" {
			t.Fatalf("output_incomplete = %v violation = %q", out.OutputIncomplete, out.Violation)
		}
		expect(t, out, "output_incomplete", "", "", false) // 退出之后终止执行树不是对 Worker 的终止
		if wm := c.watermark(t); wm != 2 {                 // 末尾半行被丢弃
			t.Fatalf("水位 %d", wm)
		}
	})

	t.Run("barrier_b_timeout_output_incomplete", func(t *testing.T) {
		c := e.newCase(t, "barrier-b")
		c.opt = runner.Options{FinalizeTimeout: 50 * time.Millisecond}
		c.fs.recordDelay = time.Minute // 记录终态提议的 Store 调用挂住
		out, _ := c.run(t, 0, w(ready(1)), w(result(2)))
		if !out.OutputIncomplete {
			t.Fatal("屏障 B 超时未标记 output_incomplete")
		}
		expect(t, out, "output_incomplete", "", "", false)
	})

	t.Run("exit_before_result_processed", func(t *testing.T) {
		c := e.newCase(t, "exit-before-processed")
		c.opt = fast
		c.fs.recordDelay = 200 * time.Millisecond // 进程退出时 result 尚在处理
		out, _ := c.run(t, 0, w(ready(1)), w(result(2)))
		expect(t, out, "succeeded", "", "", false)
		if stored, err := e.store.GetTerminalProposal(ctx, c.att); err != nil || out.Proposal == nil || stored != *out.Proposal {
			t.Fatalf("屏障 B 之后提议应已记录: %+v %v", stored, err)
		}
	})

	t.Run("result_then_nonzero_exit", func(t *testing.T) {
		c := e.newCase(t, "result-nonzero")
		out, _ := c.run(t, 3, w(ready(1)), w(result(2)))
		expect(t, out, "exit_after_result", "", "", false)
	})

	t.Run("cancel_grace_expired", func(t *testing.T) {
		c := e.newCase(t, "cancel-expired")
		out, p := c.run(t, 0, w(ready(1)), ctl("cancel", 50), h(), untilKilled())
		if p.got[0]["type"] != "cancel" {
			t.Fatalf("宿主消息 %v", p.got)
		}
		if p.got[0]["grace_ms"] != float64(50) || p.got[0]["attempt_id"] != c.att {
			t.Fatalf("cancel = %v", p.got[0])
		}
		expect(t, out, "cancelled", "", runner.KillCancel, true)
	})

	t.Run("exit_after_cancel_no_restart", func(t *testing.T) {
		c := e.newCase(t, "exit-after-cancel")
		out, _ := c.run(t, 1, w(ready(1)), w(ev("progress", 2, "kind", "step_started", "message", "x")),
			ctl("cancel", 5000), h())
		// 宿主取消后以非零码退出：按宿主意图，不判崩溃、不重试
		expect(t, out, "cancelled", "", "", false)
	})

	t.Run("pause_deadline_expired", func(t *testing.T) {
		c := e.newCase(t, "pause-expired")
		out, p := c.run(t, 0, w(ready(1)), w(checkpoint(2, "cp-1")), h(), ctl("pause", 50), h(), untilKilled())
		if p.got[1]["type"] != "pause" {
			t.Fatalf("宿主消息 %v", p.got)
		}
		expect(t, out, "paused", "", runner.KillPause, true)
		if l := c.latest(t); l != "cp-1" { // 停在已提交的 checkpoint
			t.Fatalf("指针 = %q", l)
		}
	})

	t.Run("paused_not_latest_checkpoint", func(t *testing.T) {
		c := e.newCase(t, "paused-stale")
		out, _ := c.run(t, 0, w(ready(1)), w(checkpoint(2, "cp-1")), h(), w(checkpoint(3, "cp-2")), h(),
			ctl("pause", 5000), h(), w(ev("paused", 4, "checkpoint_id", "cp-1")))
		if out.Violation != runner.ViolationPausedNotLatest {
			t.Fatalf("violation = %q", out.Violation)
		}
		expect(t, out, "protocol_violation", "", "", false)
	})

	t.Run("paused_at_resume_checkpoint", func(t *testing.T) {
		c := e.newCase(t, "paused-resume")
		if _, err := e.store.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: c.taskID},
			CheckpointID: "cp-0", AttemptID: c.att, StepID: "s0", State: json.RawMessage(`{"n":0}`)}); err != nil {
			t.Fatal(err)
		}
		c.init = &protocol.Init{Type: protocol.TypeInit, Bootstrap: 1, ProtocolVersions: []int64{1}, Mode: protocol.ModeTask,
			TaskID: c.taskID, AttemptID: c.att, AttemptNo: 1, OutDir: "/workspace/out/" + c.att,
			Resume: &protocol.Resume{CheckpointID: "cp-0", StepID: "s0", State: json.RawMessage(`{"n":0}`)}}
		out, _ := c.run(t, 0, w(ready(1)), ctl("pause", 5000), h(), w(ev("paused", 2, "checkpoint_id", "cp-0")))
		noViolation(t, out)
		expect(t, out, "paused", "", "", false)
	})

	t.Run("control_before_ready_kills", func(t *testing.T) {
		c := e.newCase(t, "ctl-before-ready")
		out, p := c.run(t, 0, ctlNow("cancel", 5000), untilKilled())
		if len(p.all) != 0 {
			t.Fatalf("就绪前发送了控制消息 %v", p.all)
		}
		expect(t, out, "cancelled", "", runner.KillCancel, true)
	})

	t.Run("run_time_exceeded", func(t *testing.T) {
		c := e.newCase(t, "run-time")
		out, _ := c.run(t, 0, w(ready(1)), do(func() error { c.cancel(runner.ErrRunTimeExceeded); return nil }), untilKilled())
		expect(t, out, "task_deadline_exceeded", "", runner.KillTimeout, true)
	})

	t.Run("store_unavailable_after_5_failures", func(t *testing.T) {
		c := e.newCase(t, "store-down")
		c.fs.appendFault = func(int) error { return persistence.ErrUnavailable }
		script := []step{w(ready(1))}
		for i := 2; i <= 12; i++ { // 每条事件单独一次持久化尝试
			script = append(script, w(ev("progress", i, "kind", "x", "message", "m")), pause(10*time.Millisecond))
		}
		script = append(script, untilKilled())
		out, _ := c.run(t, 0, script...)
		expect(t, out, "store_unavailable", "fault", runner.KillStoreUnavailable, true)
		c.fs.mu.Lock()
		n := c.fs.appends
		c.fs.mu.Unlock()
		if n < 5 {
			t.Fatalf("终止前持久化尝试 %d 次，期望阈值 5 附近", n)
		}
	})

	t.Run("store_unavailable_after_window", func(t *testing.T) {
		c := e.newCase(t, "store-window")
		c.opt = runner.Options{StoreFailureCount: 1000, StoreFailureWindow: 20 * time.Millisecond}
		c.fs.appendFault = func(int) error { return persistence.ErrUnavailable }
		out, _ := c.run(t, 0, w(ready(1)), pause(40*time.Millisecond), w(ev("progress", 2, "kind", "x", "message", "m")), untilKilled())
		expect(t, out, "store_unavailable", "fault", runner.KillStoreUnavailable, true)
	})

	t.Run("store_failures_reset_by_success", func(t *testing.T) {
		c := e.newCase(t, "store-flaky")
		c.fs.appendFault = func(n int) error { // 每 4 次失败后一次成功：从不连续 5 次
			if n%5 == 0 {
				return nil
			}
			return persistence.ErrUnavailable
		}
		script := []step{w(ready(1))}
		for i := 2; i <= 12; i++ {
			script = append(script, w(ev("progress", i, "kind", "x", "message", "m")), pause(5*time.Millisecond))
		}
		script = append(script, w(result(13)))
		out, _ := c.run(t, 0, script...)
		if out.PlatformKill != "" {
			t.Fatalf("未达阈值却终止: %q", out.PlatformKill)
		}
	})

	t.Run("artifact_transient_failure_save_timeout", func(t *testing.T) {
		c := e.newCase(t, "artifact-transient")
		sha := c.write(t, "a.txt", "content")
		c.fs.registerFault = persistence.ErrUnavailable
		out, p := c.run(t, 0, w(ready(1)), w(artifact(2, "a", "a.txt", sha, len("content"))), h(), w(result(3)))
		noViolation(t, out)
		reply(t, p.got[0], "artifact_result", "rejected", "save_timeout") // §5.4 的稳定码，Worker 可重试
	})
}
