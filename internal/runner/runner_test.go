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

// fakeProc 是一个回放脚本的执行句柄：stdout 由脚本写出，stdin 全部读出并记录。
type fakeProc struct {
	script   []step
	exitCode int
	drop     func(m map[string]any) bool // 返回 true 的宿主消息视为在传输中丢失

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
	_ = p.outW.Close()
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
	p := &fakeProc{script: script, exitCode: exitCode, drop: c.drop}
	r := runner.New(c.fs, c.e.blobs, &fakeStarter{proc: p}, nil, c.opt)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out := r.Run(ctx, runner.Attempt{TaskID: c.taskID, AttemptID: c.att, AttemptNo: init.AttemptNo, EnvID: "env-" + c.taskID,
		Init: *init, OutDir: c.outDir, OnReady: c.onReady}, nil)
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
// 含控制消息（cancel、pause）的场景属于 Task 6。
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
		control := false
		for _, l := range sc.Lines {
			if typ := decodeMap(t, l.Message)["type"]; l.From == "host" && (typ == "cancel" || typ == "pause") {
				control = true
			}
		}
		if control {
			t.Logf("%s 含控制消息，由 Task 6 覆盖", sc.Name)
			continue
		}
		ran++
		t.Run(sc.Name, func(t *testing.T) { replay(t, e, sc) })
	}
	if ran < 15 {
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
	}

	var init protocol.Init
	if err := json.Unmarshal(sc.Lines[0].Message, &init); err != nil {
		t.Fatal(err)
	}
	init.TaskID, init.AttemptID = c.taskID, c.att
	c.init = &init

	var script []step
	var wantHost []map[string]any
	var accepted, terminal string
	acceptedSeq := 0
	for i, l := range sc.Lines[1:] {
		idx := i + 1
		var compact bytes.Buffer
		if err := json.Compact(&compact, l.Message); err != nil {
			t.Fatal(err)
		}
		if l.From == "host" {
			script = append(script, h())
			wantHost = append(wantHost, decodeMap(t, l.Message))
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
	if !reflect.DeepEqual(p.got, wantHost) {
		t.Fatalf("宿主回复 = %v，期望 %v", p.got, wantHost)
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
