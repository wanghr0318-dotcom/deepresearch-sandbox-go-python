// Command sessionworker 是端到端验收（M4 Plan 12 Task 9）用的脚本化会话 Worker：以 internal/protocol 编解码 session 模式
// 的协议（init(session) → ready{session_ext} → task_start … task_outcome → task_released，quiesce、session_close），
// 行为由环境变量 AGENTBOX_SW_SCRIPT 与每条消息（task_start.config.text）中的 key=value 选项选择。
//
// 脚本（AGENTBOX_SW_SCRIPT，或消息中的 script=<名>，后者只作用于该 turn）：
//   - normal：提交 1 个 task checkpoint，调用 searches 次 /v1/search（默认 1），result.session_state 内联
//     （state=inline，默认）或以产物登记的 blob 作 state_ref（state=ref）；
//   - leak_child：释放（task_released）之前留下一个长睡眠的子进程（E30：释放核验失败）；
//   - drop_outcome_once：第一次收到 task_outcome 时不答复，稍后以 task_outcome_query 查询（E29）；
//   - never_release：从不发送 task_released（E29：release_timeout）；
//   - ask_user：没有 answer 指令时提交 checkpoint 后发 awaiting_input，收到 directive.answer 后完成；
//   - exit_when_idle：释放之后退出（E28：会话 idle 时 Worker 死亡）。
//
// 其余选项：sleep_ms=N（每次搜索之后等待）、hold_ms=N（结果之前等待，只在没有 resume 的 attempt 中；期间响应 pause/cancel）。
//
// 每个 turn 把计数写入 /workspace/session/note.txt，并在 /workspace/.sw/log.jsonl 追加观察记录（恢复时读到的状态与
// 文件、恢复期间连接 Gateway 是否被拒、收到的 task_start 字段、每次搜索的状态码与额度头），供测试读取。
// 进程型 Program（非 root）用 AGENTBOX_SW_WORKSPACE 与 AGENTBOX_SW_RESTORE 把 /workspace 与 /run/agentbox/restore
// 映射到宿主目录，AGENTBOX_GATEWAY_SOCKET 给出入口 socket；真实隔离时三者取环境内的固定路径。
// CGO_ENABLED=0 构建。
package main

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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
)

const (
	envScript    = "AGENTBOX_SW_SCRIPT"
	envWorkspace = "AGENTBOX_SW_WORKSPACE"
	envRestore   = "AGENTBOX_SW_RESTORE"
	envSocket    = "AGENTBOX_GATEWAY_SOCKET"
	envChild     = "AGENTBOX_SW_CHILD"

	defaultSocket = "/run/agentbox/gateway.sock"
)

func main() {
	if os.Getenv(envChild) == "1" { // leak_child 留下的子进程：一直睡眠，直到环境被停止
		time.Sleep(365 * 24 * time.Hour)
		return
	}
	w := newWorker()
	if err := w.run(); err != nil {
		w.log(map[string]any{"event": "fatal", "error": err.Error()})
		fmt.Fprintln(os.Stderr, "sessionworker:", err)
		os.Exit(1)
	}
}

type hostMsg struct {
	Type string
	Raw  []byte
	Msg  protocol.Message
}

type worker struct {
	script  string
	ws      string // /workspace 的实际路径
	restore string // /run/agentbox/restore 的实际路径
	gw      *http.Client

	out    *bufio.Writer
	outMu  sync.Mutex
	seq    int64
	host   chan hostMsg
	queue  []hostMsg
	ctrl   string // 当前 attempt 收到的 pause / cancel
	logMu  sync.Mutex
	exited bool

	sessionCP string          // 已提交的会话指针
	state     json.RawMessage // 已提交的会话状态
	dropped   bool            // drop_outcome_once 已丢弃过一次
}

func newWorker() *worker {
	w := &worker{script: envOr(envScript, "normal"), ws: envOr(envWorkspace, "/workspace"),
		restore: envOr(envRestore, strings.TrimSuffix(protocol.StagedStateDir, "/")),
		out:     bufio.NewWriter(os.Stdout), host: make(chan hostMsg, 64)}
	sock := envOr(envSocket, defaultSocket)
	w.gw = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}, DisableKeepAlives: true}}
	return w
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ---- 输入与输出 ----

func (w *worker) readHost() {
	defer close(w.host)
	r := bufio.NewReaderSize(os.Stdin, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var h struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(line, &h)
			m, derr := protocol.DecodeSessionLine(protocol.HostToWorker, line)
			if derr != nil && h.Type != protocol.TypeInit {
				w.log(map[string]any{"event": "bad_host_line", "error": derr.Error(), "line": string(line)})
				continue
			}
			w.host <- hostMsg{Type: h.Type, Raw: append([]byte(nil), line...), Msg: m}
		}
		if err != nil {
			return
		}
	}
}

func (w *worker) send(m protocol.Message) error {
	w.outMu.Lock()
	defer w.outMu.Unlock()
	line, err := protocol.EncodeSessionLine(protocol.WorkerToHost, m)
	if err != nil {
		return fmt.Errorf("编码 %s: %w", m.MessageType(), err)
	}
	if _, err := w.out.Write(append(line, '\n')); err != nil {
		return err
	}
	return w.out.Flush()
}

func (w *worker) hdr(typ, attemptID string) protocol.EventHeader {
	w.seq++
	return protocol.EventHeader{Type: typ, V: protocol.Version, Seq: w.seq, AttemptID: attemptID}
}

// recv 返回下一条宿主消息（先取队列）；stdin 结束时 ok 为假。
func (w *worker) recv() (hostMsg, bool) {
	if len(w.queue) > 0 {
		m := w.queue[0]
		w.queue = w.queue[1:]
		return m, true
	}
	m, ok := <-w.host
	return m, ok
}

// noteControl 记录当前 attempt 的 pause/cancel；返回是否为控制消息。
func (w *worker) noteControl(m hostMsg) bool {
	switch m.Type {
	case protocol.TypePause:
		if w.ctrl == "" {
			w.ctrl = protocol.TypePause
		}
		return true
	case protocol.TypeCancel:
		w.ctrl = protocol.TypeCancel
		return true
	}
	return false
}

// poll 非阻塞地取出已到达的宿主消息：控制记入 ctrl，其余入队。
func (w *worker) poll() {
	for {
		select {
		case m, ok := <-w.host:
			if !ok {
				w.exited = true
				return
			}
			if !w.noteControl(m) {
				w.queue = append(w.queue, m)
			}
		default:
			return
		}
	}
}

// await 等待指定类型的宿主消息；途中的控制记入 ctrl，其余消息保留在队列中。
func (w *worker) await(typ string) (hostMsg, error) {
	var keep []hostMsg
	defer func() { w.queue = append(keep, w.queue...) }()
	for {
		m, ok := w.recv()
		if !ok {
			return hostMsg{}, io.EOF
		}
		if w.noteControl(m) {
			continue
		}
		if m.Type == typ {
			return m, nil
		}
		keep = append(keep, m)
	}
}

// sleep 等待 d，期间每 20 ms 检查控制；收到控制时提前返回 true。
func (w *worker) sleep(d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		w.poll()
		if w.ctrl != "" {
			return true
		}
		time.Sleep(min(20*time.Millisecond, time.Until(end)))
	}
	w.poll()
	return w.ctrl != ""
}

// log 在 <workspace>/.sw/log.jsonl 追加一行观察记录。
func (w *worker) log(rec map[string]any) {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	dir := filepath.Join(w.ws, ".sw")
	_ = os.MkdirAll(dir, 0o755)
	f, err := os.OpenFile(filepath.Join(dir, "log.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	rec["pid"], rec["ts"] = os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano)
	b, _ := json.Marshal(rec)
	_, _ = f.Write(append(b, '\n'))
}

// mapPath 把环境内路径映射到实际路径（进程型 Program 没有挂载）。
func (w *worker) mapPath(p string) string {
	switch {
	case p == "/workspace" || strings.HasPrefix(p, "/workspace/"):
		return w.ws + strings.TrimPrefix(p, "/workspace")
	case strings.HasPrefix(p, protocol.StagedStateDir):
		return filepath.Join(w.restore, strings.TrimPrefix(p, protocol.StagedStateDir))
	}
	return p
}

// ---- 会话 ----

func (w *worker) run() error {
	go w.readHost()
	m, ok := w.recv()
	if !ok || m.Type != protocol.TypeInit {
		return errors.New("第一条消息不是 init")
	}
	var in protocol.Init
	if err := json.Unmarshal(m.Raw, &in); err != nil {
		return fmt.Errorf("init: %w", err)
	}
	if in.Mode != protocol.ModeSession {
		return fmt.Errorf("init.mode = %q，期望 session", in.Mode)
	}
	if err := w.resume(in); err != nil {
		return err
	}
	ready := &protocol.Ready{EventHeader: w.hdr(protocol.TypeReady, ""), ProtocolVersion: protocol.Version,
		Mode: protocol.ModeSession, Worker: protocol.WorkerInfo{Name: "sessionworker", Version: "e2e"},
		Capabilities: []string{}, SessionExt: protocol.SessionExtVersion}
	if slices.Contains(in.Extensions, protocol.ExtensionSubruns) { // 扩展确认：当且仅当宿主请求（规格 §5.2；不使用 sub-run）
		ready.Subruns = protocol.SubrunsExtVersion
	}
	if err := w.send(ready); err != nil {
		return err
	}
	for {
		m, ok := w.recv()
		if !ok {
			return nil // stdin 结束：宿主已关闭
		}
		switch m.Type {
		case protocol.TypeTaskStart:
			if err := w.task(m.Msg.(*protocol.TaskStart)); err != nil {
				return err
			}
			if w.exited {
				return nil
			}
		case protocol.TypeQuiesce:
			w.log(map[string]any{"event": "quiesce", "session_checkpoint_id": w.sessionCP})
			if err := w.send(&protocol.Quiesced{EventHeader: w.hdr(protocol.TypeQuiesced, ""), SessionCheckpointID: w.sessionCP}); err != nil {
				return err
			}
		case protocol.TypeSessionClose:
			w.log(map[string]any{"event": "session_close"})
			return w.send(&protocol.Closed{EventHeader: w.hdr(protocol.TypeClosed, "")})
		default:
			w.log(map[string]any{"event": "unexpected", "type": m.Type})
		}
	}
}

// resume 读取冷恢复的会话状态（inline 或 /run/agentbox/restore/<sha>），记录 workspace 中上次写入的文件，并确认恢复
// 期间（ready 之前）连接 Gateway 被拒绝（入口没有附着任何 attempt）。
func (w *worker) resume(in protocol.Init) error {
	r := in.SessionResume
	rec := map[string]any{"event": "start", "incarnation_id": in.IncarnationID, "session_id": in.SessionID, "resumed": r != nil}
	if r != nil {
		state := r.State
		if r.StagedStatePath != "" {
			b, err := os.ReadFile(w.mapPath(r.StagedStatePath))
			if err != nil {
				return fmt.Errorf("读取暂存的会话状态: %w", err)
			}
			state = b
			rec["staged_state_path"] = r.StagedStatePath
		}
		w.sessionCP, w.state = r.CheckpointID, state
		rec["checkpoint_id"], rec["state"] = r.CheckpointID, json.RawMessage(state)
		if b, err := os.ReadFile(filepath.Join(w.ws, "session", "note.txt")); err == nil {
			rec["note"] = string(b)
		}
		_, err := w.gateway(context.Background(), http.MethodGet, "/v1/budget", "", nil)
		rec["gateway_refused"] = err != nil
	}
	w.log(rec)
	return nil
}

// ---- turn ----

type opts struct {
	script, stateMode string
	searches          int
	sleepMs, holdMs   int
}

func (w *worker) turnOpts(cfg json.RawMessage) opts {
	o := opts{script: w.script, stateMode: "inline", searches: 1}
	var c struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(cfg, &c)
	for _, f := range strings.Fields(c.Text) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		n, _ := strconv.Atoi(v)
		switch k {
		case "script":
			o.script = v
		case "state":
			o.stateMode = v
		case "searches":
			o.searches = n
		case "sleep_ms":
			o.sleepMs = n
		case "hold_ms":
			o.holdMs = n
		}
	}
	return o
}

func (w *worker) task(ts *protocol.TaskStart) error {
	att := ts.AttemptID
	w.ctrl = ""
	o := w.turnOpts(ts.Config)
	rec := map[string]any{"event": "task_start", "task_id": ts.TaskID, "attempt_id": att, "attempt_no": ts.AttemptNo,
		"base_session_checkpoint_id": ts.BaseSessionCheckpointID, "restored_from_task_id": ts.RestoredFromTaskID}
	if ts.Resume != nil {
		rec["resume_checkpoint_id"] = ts.Resume.CheckpointID
	}
	if ts.Directive != nil {
		rec["directive"] = ts.Directive
	}
	if ts.Carryover != nil {
		rec["carryover"] = ts.Carryover
	}
	w.log(rec)
	if err := w.send(&protocol.TaskAccepted{EventHeader: w.hdr(protocol.TypeTaskAccepted, att)}); err != nil {
		return err
	}
	// task checkpoint：恢复时沿用 resume 的 checkpoint，否则提交一个新的。
	taskCP := ""
	if ts.Resume != nil {
		taskCP = ts.Resume.CheckpointID
	} else {
		cp := "tc-" + att
		if err := w.send(&protocol.Checkpoint{EventHeader: w.hdr(protocol.TypeCheckpoint, att), CheckpointID: cp,
			Scope: protocol.ScopeTask, StepID: "s1", State: json.RawMessage(`{"step":1}`)}); err != nil {
			return err
		}
		m, err := w.await(protocol.TypeCheckpointResult)
		if err != nil {
			return err
		}
		if r := m.Msg.(*protocol.CheckpointResult); r.Status == protocol.CheckpointCommitted {
			taskCP = cp
		}
	}
	answered := ts.Directive != nil && ts.Directive.Kind == protocol.DirectiveAnswer
	if o.script == "ask_user" && !answered && w.ctrl == "" {
		if err := w.send(&protocol.AwaitingInput{EventHeader: w.hdr(protocol.TypeAwaitingInput, att), CheckpointID: taskCP,
			QuestionID: "q-" + ts.TaskID}); err != nil {
			return err
		}
		return w.outcome(att, o, nil)
	}
	used := 0
	for i := 1; i <= o.searches && w.ctrl == ""; i++ {
		code, hdr, err := w.search(fmt.Sprintf("root/s1/search/%d", i), fmt.Sprintf("query %d of %s", i, ts.TaskID))
		rec := map[string]any{"event": "search", "task_id": ts.TaskID, "attempt_id": att, "n": i, "status": code, "budget": hdr}
		if err != nil {
			rec["error"] = err.Error()
		}
		w.log(rec)
		if code == http.StatusOK {
			used++
		}
		if o.sleepMs > 0 && w.sleep(time.Duration(o.sleepMs)*time.Millisecond) {
			break
		}
		w.poll()
	}
	if w.ctrl == "" && o.holdMs > 0 && ts.Resume == nil {
		w.sleep(time.Duration(o.holdMs) * time.Millisecond)
	}
	switch w.ctrl {
	case protocol.TypeCancel: // 取消：不发提议，等裁决（Python SDK 的行为）
		w.log(map[string]any{"event": "cancelled", "task_id": ts.TaskID, "attempt_id": att})
		return w.outcome(att, o, nil)
	case protocol.TypePause:
		card, _ := json.Marshal(map[string]any{"card": map[string]any{"subtopics_done": 0, "subtopics_total": 0, "sources": used,
			"tool_calls_used": used, "tool_call_limit": 30}})
		if err := w.send(&protocol.Progress{EventHeader: w.hdr(protocol.TypeProgress, att), StepID: "s1", Kind: "turn_stopped",
			Message: "stopped", Data: card}); err != nil {
			return err
		}
		if err := w.send(&protocol.Paused{EventHeader: w.hdr(protocol.TypePaused, att), CheckpointID: taskCP}); err != nil {
			return err
		}
		return w.outcome(att, o, nil)
	}
	// 结果：会话状态计数加一，并写入 workspace 的会话文件。
	var st struct {
		Count int    `json:"count"`
		Last  string `json:"last"`
	}
	_ = json.Unmarshal(w.state, &st)
	st.Count++
	st.Last = ts.TaskID
	newState, _ := json.Marshal(st)
	note := fmt.Sprintf("count=%d task=%s", st.Count, ts.TaskID)
	if err := os.MkdirAll(filepath.Join(w.ws, "session"), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(w.ws, "session", "note.txt"), []byte(note), 0o644); err != nil {
		return err
	}
	ss := &protocol.SessionState{CheckpointID: "sc-" + att}
	if o.stateMode == "ref" {
		sha, err := w.stateArtifact(ts, newState)
		if err != nil {
			return err
		}
		ss.StateRef = sha
	} else {
		ss.State = newState
	}
	if err := w.send(&protocol.Result{EventHeader: w.hdr(protocol.TypeResult, att), Summary: note, Outputs: []string{},
		SessionState: ss}); err != nil {
		return err
	}
	return w.outcome(att, o, newState)
}

// stateArtifact 把会话状态写到 out_dir 并登记为产物（内部可见），返回其 sha256（作为 state_ref）。
func (w *worker) stateArtifact(ts *protocol.TaskStart, state []byte) (string, error) {
	dir := w.mapPath(ts.OutDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "session-state.json"), state, 0o644); err != nil {
		return "", err
	}
	sum := sha256.Sum256(state)
	sha := hex.EncodeToString(sum[:])
	if err := w.send(&protocol.Artifact{EventHeader: w.hdr(protocol.TypeArtifact, ts.AttemptID), ArtifactID: "session-state",
		Path: "session-state.json", DeclaredSHA256: sha, DeclaredSize: int64(len(state)), MediaType: "application/json",
		Visibility: protocol.VisibilityInternal}); err != nil {
		return "", err
	}
	m, err := w.await(protocol.TypeArtifactResult)
	if err != nil {
		return "", err
	}
	if r := m.Msg.(*protocol.ArtifactResult); r.Status != protocol.ArtifactSaved {
		return "", fmt.Errorf("会话状态产物未保存：%s", r.Status)
	}
	return sha, nil
}

// outcome 等待裁决（task_outcome）并按脚本释放。newState 是本 attempt 提议的会话状态（成功裁决时采用）。
func (w *worker) outcome(att string, o opts, newState json.RawMessage) error {
	for {
		m, err := w.await(protocol.TypeTaskOutcome)
		if err != nil {
			return err
		}
		out := m.Msg.(*protocol.TaskOutcome)
		if out.AttemptID != att {
			continue
		}
		if o.script == "drop_outcome_once" && !w.dropped {
			w.dropped = true
			w.log(map[string]any{"event": "outcome_dropped", "attempt_id": att, "verdict": out.Verdict})
			time.Sleep(300 * time.Millisecond)
			if err := w.send(&protocol.TaskOutcomeQuery{EventHeader: w.hdr(protocol.TypeTaskOutcomeQuery, att)}); err != nil {
				return err
			}
			continue
		}
		w.log(map[string]any{"event": "task_outcome", "attempt_id": att, "verdict": out.Verdict,
			"committed_session_checkpoint_id": out.CommittedSessionCheckpointID})
		if out.Verdict == protocol.VerdictSucceeded && newState != nil {
			w.state = newState
		}
		w.sessionCP = out.CommittedSessionCheckpointID
		switch o.script {
		case "never_release":
			w.log(map[string]any{"event": "not_releasing", "attempt_id": att})
			return nil
		case "leak_child":
			child := exec.Command(os.Args[0]) // 以绝对路径启动（--session-worker-argv 给出的路径）
			child.Env = append(os.Environ(), envChild+"=1")
			if err := child.Start(); err != nil {
				return fmt.Errorf("leak_child: %w", err)
			}
			w.log(map[string]any{"event": "leaked_child", "attempt_id": att, "child_pid": child.Process.Pid})
		}
		if err := w.send(&protocol.TaskReleased{EventHeader: w.hdr(protocol.TypeTaskReleased, att)}); err != nil {
			return err
		}
		if o.script == "exit_when_idle" {
			w.log(map[string]any{"event": "exit_when_idle", "attempt_id": att})
			w.exited = true
		}
		return nil
	}
}

// ---- Gateway ----

func (w *worker) gateway(ctx context.Context, method, path, callID string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://gateway"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if callID != "" {
		req.Header.Set("X-Agentbox-Call-Id", callID)
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.gw.Do(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp, nil
}

// search 调用 /v1/search，返回状态码与额度头 X-Agentbox-Tool-Budget。
func (w *worker) search(callID, query string) (int, string, error) {
	body, _ := json.Marshal(map[string]any{"query": query, "max_results": 3})
	resp, err := w.gateway(context.Background(), http.MethodPost, "/v1/search", callID, body)
	if err != nil {
		return 0, "", err
	}
	return resp.StatusCode, resp.Header.Get("X-Agentbox-Tool-Budget"), nil
}
