package protocol

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// fixtureDir 是跨语言 fixtures 的位置；Python 侧使用同一份文件。
const fixtureDir = "../../protocol/fixtures/v1"

type messageCase struct {
	Name      string          `json:"name"`
	Direction string          `json:"direction"`
	Message   json.RawMessage `json:"message"`
	Raw       string          `json:"raw"`
	Code      string          `json:"code"`
}

func (c messageCase) line() []byte {
	if c.Raw != "" {
		return []byte(c.Raw)
	}
	return c.Message
}

func loadJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("解析 %s: %v", path, err)
	}
}

func parseDirection(t *testing.T, s string) Direction {
	t.Helper()
	switch s {
	case "host":
		return HostToWorker
	case "worker":
		return WorkerToHost
	}
	t.Fatalf("未知方向 %q", s)
	return 0
}

func TestMessageFixtures(t *testing.T) {
	var f struct {
		Valid   []messageCase `json:"valid"`
		Invalid []messageCase `json:"invalid"`
	}
	loadJSON(t, filepath.Join(fixtureDir, "messages.json"), &f)
	if len(f.Valid) == 0 || len(f.Invalid) == 0 {
		t.Fatal("消息 fixtures 为空")
	}

	for _, c := range f.Valid {
		t.Run("valid/"+c.Name, func(t *testing.T) {
			dir := parseDirection(t, c.Direction)
			m, err := DecodeLine(dir, c.line())
			if err != nil {
				t.Fatalf("应当合法：%v", err)
			}
			b, err := EncodeLine(dir, m)
			if err != nil {
				t.Fatalf("重新编码：%v", err)
			}
			if _, err := DecodeLine(dir, b); err != nil {
				t.Fatalf("往返后解码：%v", err)
			}
		})
	}
	for _, c := range f.Invalid {
		t.Run("invalid/"+c.Name, func(t *testing.T) {
			_, err := DecodeLine(parseDirection(t, c.Direction), c.line())
			if got := CodeOf(err); got != c.Code {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", got, c.Code, err)
			}
		})
	}
}

func checkpointLine(state string, refs []string) []byte {
	r, _ := json.Marshal(refs)
	return []byte(`{"type":"checkpoint","v":1,"seq":1,"checkpoint_id":"cp-1","scope":"task","step_id":"s1","state":` + state + `,"refs":` + string(r) + `}`)
}

// TestDecodeLimits 覆盖 §5.10 的大小上限：fixtures 不适合存放这些按常量生成的大消息。
func TestDecodeLimits(t *testing.T) {
	ref := strings.Repeat("a", 64)
	refs := make([]string, MaxRefsPerCheckpoint)
	for i := range refs {
		refs[i] = ref
	}
	cases := []struct {
		name string
		dir  Direction
		line string
		want string // 空表示应当合法
	}{
		{"超长事件", WorkerToHost, `{"type":"progress","v":1,"seq":1,"kind":"x","message":"` + strings.Repeat("a", MaxEventBytes) + `"}`, CodeMessageTooLarge},
		{"非 init 控制消息按 16 KiB 计", HostToWorker, `{"type":"cancel","v":1,"attempt_id":"a-1","grace_ms":0,"reason":"` + strings.Repeat("a", MaxControlBytes) + `"}`, CodeMessageTooLarge},
		{"init 上限是 1 MiB", HostToWorker, `{"type":"init","bootstrap":1,"protocol_versions":[1],"mode":"task","task_id":"t","attempt_id":"a","attempt_no":1,"out_dir":"/o","config":"` + strings.Repeat("a", 100<<10) + `"}`, ""},
		{"state 恰好到上限", WorkerToHost, string(checkpointLine(`"`+strings.Repeat("a", MaxInlineStateBytes-2)+`"`, []string{})), ""},
		{"state 超过上限", WorkerToHost, string(checkpointLine(`"`+strings.Repeat("a", MaxInlineStateBytes-1)+`"`, []string{})), CodeStateTooLarge},
		{"state 的空白不计入", WorkerToHost, string(checkpointLine(`{ "k" :  "aaaaaaaaaa" }`, []string{})), ""},
		{"refs 恰好到上限", WorkerToHost, string(checkpointLine(`{}`, refs)), ""},
		{"refs 超过上限", WorkerToHost, string(checkpointLine(`{}`, append(refs, ref))), CodeTooManyRefs},
		{"超过所有上限的行不解析", HostToWorker, strings.Repeat("x", MaxInitBytes+1), CodeMessageTooLarge},
		{"非法 UTF-8", WorkerToHost, "{\"type\":\"progress\",\"v\":1,\"seq\":1,\"kind\":\"x\",\"message\":\"\xff\"}", CodeMalformedJSON},
		{"孤立代理项转义可以解码", WorkerToHost, string(checkpointLine(`"\ud800"`, []string{})), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeLine(c.dir, []byte(c.line)); CodeOf(err) != c.want {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", CodeOf(err), c.want, err)
			}
		})
	}
}

func TestEncodeLine(t *testing.T) {
	cp := &Checkpoint{
		EventHeader:  EventHeader{Type: TypeCheckpoint, V: Version, Seq: 3},
		CheckpointID: "cp-1",
		Scope:        ScopeTask,
		StepID:       "s1",
		State:        json.RawMessage(`{"next_index":1}`),
		Refs:         []string{},
	}
	b, err := EncodeLine(WorkerToHost, cp)
	if err != nil {
		t.Fatalf("EncodeLine: %v", err)
	}
	got, err := DecodeLine(WorkerToHost, b)
	if back, ok := got.(*Checkpoint); err != nil || !ok || back.Seq != 3 || string(back.State) != `{"next_index":1}` {
		t.Fatalf("往返结果不一致：%#v（err = %v）", got, err)
	}

	rejects := []struct {
		name string
		dir  Direction
		m    Message
		want string
	}{
		{"Worker 事件按宿主方向编码", HostToWorker, &Paused{EventHeader: EventHeader{Type: TypePaused, V: Version, Seq: 1}, CheckpointID: "cp-1"}, CodeUnknownType},
		{"type 字段与消息类型不一致", WorkerToHost, &Paused{EventHeader: EventHeader{Type: TypeResult, V: Version, Seq: 1}, CheckpointID: "cp-1"}, CodeInvalidField},
		{"缺少 state 与 state_ref", WorkerToHost, &Checkpoint{EventHeader: EventHeader{Type: TypeCheckpoint, V: Version, Seq: 1}, CheckpointID: "cp-1", Scope: ScopeTask, StepID: "s1"}, CodeInvalidField},
		{"编码结果违反 JSON 结构限制", WorkerToHost, &Progress{EventHeader: EventHeader{Type: TypeProgress, V: Version, Seq: 1}, Kind: "x", Message: "y", Data: json.RawMessage(`1e999`)}, CodeInvalidField},
	}
	for _, c := range rejects {
		t.Run(c.name, func(t *testing.T) {
			if _, err := EncodeLine(c.dir, c.m); CodeOf(err) != c.want {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", CodeOf(err), c.want, err)
			}
		})
	}
}

// TestImportsOnlyStandardLibrary 保证本包只依赖标准库（代码组织设计 §3.1 规则 3）：
// 标准库导入路径的第一段不含 "."。
func TestImportsOnlyStandardLibrary(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("读取目录: %v", err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("解析 %s: %v", e.Name(), err)
		}
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: %v", e.Name(), err)
			}
			if first := strings.SplitN(path, "/", 2)[0]; strings.Contains(first, ".") {
				t.Errorf("%s 导入了非标准库包 %s", e.Name(), path)
			}
		}
	}
}

type scenarioLine struct {
	From    string          `json:"from"`
	Message json.RawMessage `json:"message"`
	Raw     string          `json:"raw"`
}

type scenario struct {
	Name   string         `json:"name"`
	Lines  []scenarioLine `json:"lines"`
	Expect struct {
		Stream    string `json:"stream"`
		Violation string `json:"violation"`
		At        int    `json:"at"`
	} `json:"expect"`
}

// replayScenario 依次解码每一行，并把 Worker 消息送入 WorkerStream；
// 返回首个违规所在行号与错误码，无违规时返回 (-1, "")。
func replayScenario(t *testing.T, sc scenario) (int, string) {
	t.Helper()
	var s WorkerStream
	for i, l := range sc.Lines {
		dir := parseDirection(t, l.From)
		line := []byte(l.Message)
		if l.Raw != "" {
			line = []byte(l.Raw)
		}
		m, err := DecodeLine(dir, line)
		if err == nil && dir == WorkerToHost {
			err = s.Observe(m)
		}
		if err != nil {
			if code := CodeOf(err); code != "" {
				return i, code
			}
			t.Fatalf("第 %d 行返回了非协议错误：%v", i, err)
		}
	}
	return -1, ""
}

func TestScenarioFixtures(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureDir, "scenarios", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("没有场景 fixtures（err=%v）", err)
	}
	for _, path := range files {
		var sc scenario
		loadJSON(t, path, &sc)
		t.Run(sc.Name, func(t *testing.T) {
			at, code := replayScenario(t, sc)
			switch sc.Expect.Stream {
			case "ok":
				if code != "" {
					t.Fatalf("第 %d 行违规 %s，期望无违规", at, code)
				}
			case "violation":
				if at != sc.Expect.At || code != sc.Expect.Violation {
					t.Fatalf("得到 (%d, %q)，期望 (%d, %q)", at, code, sc.Expect.At, sc.Expect.Violation)
				}
			default:
				t.Fatalf("未知的 expect.stream %q", sc.Expect.Stream)
			}
		})
	}
}

// ---- session 扩展（M4 Plan 12）----

// sameJSON 比较两段 JSON 的语义是否一致（键序与空白不计）。
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("解析 %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("解析 %s: %v", b, err)
	}
	return reflect.DeepEqual(x, y)
}

func TestSessionMessageFixtures(t *testing.T) {
	var f struct {
		Valid   []messageCase `json:"valid"`
		Invalid []messageCase `json:"invalid"`
	}
	loadJSON(t, filepath.Join(fixtureDir, "session_messages.json"), &f)
	if len(f.Valid) == 0 || len(f.Invalid) == 0 {
		t.Fatal("session 消息 fixtures 为空")
	}
	for _, c := range f.Valid {
		t.Run("valid/"+c.Name, func(t *testing.T) {
			dir := parseDirection(t, c.Direction)
			m, err := DecodeSessionLine(dir, c.line())
			if err != nil {
				t.Fatalf("应当合法：%v", err)
			}
			b, err := EncodeSessionLine(dir, m)
			if err != nil {
				t.Fatalf("重新编码：%v", err)
			}
			if !sameJSON(t, b, c.line()) {
				t.Fatalf("往返编码不一致：\n得到 %s\n期望 %s", b, c.line())
			}
		})
	}
	for _, c := range f.Invalid {
		t.Run("invalid/"+c.Name, func(t *testing.T) {
			_, err := DecodeSessionLine(parseDirection(t, c.Direction), c.line())
			if got := CodeOf(err); got != c.Code {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", got, c.Code, err)
			}
		})
	}
}

// TestSessionCodecBoundaries 覆盖不适合放进跨语言 fixtures 的编解码边界：
// task 模式不认识 session 专属类型；task_start 携带 resume.state，按 init 的 1 MiB 计。
func TestSessionCodecBoundaries(t *testing.T) {
	cases := []struct {
		name   string
		decode func(Direction, []byte) (Message, error)
		dir    Direction
		line   string
		want   string
	}{
		{"task 模式不认识 task_accepted", DecodeLine, WorkerToHost, `{"type":"task_accepted","v":1,"seq":2,"attempt_id":"a-1"}`, CodeUnknownType},
		{"task 模式不认识 task_start", DecodeLine, HostToWorker, `{"type":"task_start","v":1,"task_id":"t","attempt_id":"a","attempt_no":1,"out_dir":"/o"}`, CodeUnknownType},
		{"task_start 上限是 1 MiB", DecodeSessionLine, HostToWorker, `{"type":"task_start","v":1,"task_id":"t","attempt_id":"a","attempt_no":1,"out_dir":"/o","config":"` + strings.Repeat("a", 100<<10) + `"}`, ""},
		{"task_start 超过 1 MiB", DecodeSessionLine, HostToWorker, `{"type":"task_start","v":1,"task_id":"t","attempt_id":"a","attempt_no":1,"out_dir":"/o","config":"` + strings.Repeat("a", MaxInitBytes) + `"}`, CodeMessageTooLarge},
		{"task_outcome 按 16 KiB 计", DecodeSessionLine, HostToWorker, `{"type":"task_outcome","v":1,"attempt_id":"a","verdict":"failed","pad":"` + strings.Repeat("a", MaxControlBytes) + `"}`, CodeMessageTooLarge},
		{"session_resume 的键名区分大小写", DecodeSessionLine, HostToWorker, `{"type":"init","bootstrap":1,"protocol_versions":[1],"mode":"session","session_id":"s","incarnation_id":"i","session_resume":{"checkpoint_id":"sc","state":{},"Staged_State_Path":"x"}}`, CodeInvalidField},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.decode(c.dir, []byte(c.line)); CodeOf(err) != c.want {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", CodeOf(err), c.want, err)
			}
		})
	}

	// session 模式的编码同样执行 session 规则：缺 attempt_id 的 checkpoint_result 不得发出。
	cr := &CheckpointResult{HostHeader: HostHeader{Type: TypeCheckpointResult, V: Version}, CheckpointID: "cp-1", Scope: ScopeTask, Status: CheckpointCommitted}
	if _, err := EncodeSessionLine(HostToWorker, cr); CodeOf(err) != CodeMissingField {
		t.Fatalf("EncodeSessionLine 错误码 = %q，期望 %q", CodeOf(err), CodeMissingField)
	}
	if _, err := EncodeLine(HostToWorker, cr); err != nil {
		t.Fatalf("task 模式下 attempt_id 可省略：%v", err)
	}
}

type sessionScenario struct {
	Name   string
	Lines  []scenarioLine
	Expect struct {
		Stream    string `json:"stream"`
		Phase     string `json:"phase"`
		Violation string `json:"violation"`
		At        int    `json:"at"`
	}
}

// loadSessionScenarios 读取一个 session 场景文件（JSONL）：带 "scenario" 键的行开始一个新场景
// （含 description 与 expect），其后带 "from" 的行是该场景按时间顺序的消息。
func loadSessionScenarios(t *testing.T, path string) []sessionScenario {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	var out []sessionScenario
	for i, raw := range strings.Split(string(b), "\n") {
		raw = strings.TrimRight(raw, "\r")
		if strings.TrimSpace(raw) == "" {
			continue
		}
		var rec struct {
			Scenario string          `json:"scenario"`
			Expect   json.RawMessage `json:"expect"`
			scenarioLine
		}
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("%s 第 %d 行: %v", path, i+1, err)
		}
		if rec.Scenario != "" {
			sc := sessionScenario{Name: rec.Scenario}
			if err := json.Unmarshal(rec.Expect, &sc.Expect); err != nil {
				t.Fatalf("%s 第 %d 行 expect: %v", path, i+1, err)
			}
			out = append(out, sc)
			continue
		}
		if len(out) == 0 || rec.From == "" {
			t.Fatalf("%s 第 %d 行既不是场景头也不属于任何场景", path, i+1)
		}
		out[len(out)-1].Lines = append(out[len(out)-1].Lines, rec.scenarioLine)
	}
	return out
}

// replaySession 依次解码每一行：宿主消息送入 HostSent，Worker 事件送入 Accept；
// 返回首个违规所在行号与错误码（无违规时为 -1, ""）以及最终阶段。
func replaySession(t *testing.T, sc sessionScenario) (int, string, *SessionStream) {
	t.Helper()
	s := NewSessionStream()
	for i, l := range sc.Lines {
		dir := parseDirection(t, l.From)
		line := []byte(l.Message)
		if l.Raw != "" {
			line = []byte(l.Raw)
		}
		m, err := DecodeSessionLine(dir, line)
		if err == nil {
			if dir == HostToWorker {
				err = s.HostSent(m)
			} else {
				err = s.Accept(m)
			}
		}
		if err != nil {
			if code := CodeOf(err); code != "" {
				return i, code, s
			}
			t.Fatalf("第 %d 行返回了非协议错误：%v", i, err)
		}
	}
	return -1, "", s
}

func TestSessionScenarioFixtures(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureDir, "scenarios", "session_*.jsonl"))
	if err != nil || len(files) == 0 {
		t.Fatalf("没有 session 场景 fixtures（err=%v）", err)
	}
	// 计划要求覆盖的场景：名称 → 期望（"ok" 或违规码）。
	required := map[string]string{
		"happy_path":                  "ok",
		"task_outcome_lost":           "ok",
		"base_mismatch":               "ok",
		"awaiting_input":              "ok",
		"quiesce":                     "ok",
		"progress_after_awaiting":     CodeAfterTerminal,
		"checkpoint_wrong_attempt":    CodeWrongAttempt,
		"task_released_after_quiesce": CodeWrongAttempt,
		"ready_without_session_ext":   CodeSessionExtMissing,
	}
	seen := map[string]bool{}
	for _, path := range files {
		for _, sc := range loadSessionScenarios(t, path) {
			if seen[sc.Name] {
				t.Fatalf("场景名 %q 重复", sc.Name)
			}
			seen[sc.Name] = true
			t.Run(sc.Name, func(t *testing.T) {
				at, code, s := replaySession(t, sc)
				switch sc.Expect.Stream {
				case "ok":
					if code != "" {
						t.Fatalf("第 %d 行违规 %s，期望无违规", at, code)
					}
					if s.Phase() != sc.Expect.Phase {
						t.Fatalf("最终阶段 %q，期望 %q", s.Phase(), sc.Expect.Phase)
					}
				case "violation":
					if at != sc.Expect.At || code != sc.Expect.Violation {
						t.Fatalf("得到 (%d, %q)，期望 (%d, %q)", at, code, sc.Expect.At, sc.Expect.Violation)
					}
				default:
					t.Fatalf("未知的 expect.stream %q", sc.Expect.Stream)
				}
				if want, ok := required[sc.Name]; ok {
					got := sc.Expect.Stream
					if got == "violation" {
						got = sc.Expect.Violation
					}
					if got != want {
						t.Fatalf("场景 %q 的期望是 %q，计划要求 %q", sc.Name, got, want)
					}
				}
			})
		}
	}
	for name := range required {
		if !seen[name] {
			t.Errorf("缺少场景 %q", name)
		}
	}
}

// TestSessionStreamCurrent 检查 Current 随 task_start、task_released 变化（宿主据此路由事件）。
func TestSessionStreamCurrent(t *testing.T) {
	s := NewSessionStream()
	steps := []struct {
		host  bool
		line  string
		phase string
		cur   string
	}{
		{true, `{"type":"init","bootstrap":1,"protocol_versions":[1],"mode":"session","session_id":"s-1","incarnation_id":"i-1"}`, PhaseReady, ""},
		{false, `{"type":"ready","v":1,"seq":1,"protocol_version":1,"mode":"session","session_ext":1,"worker":{"name":"w"},"capabilities":[]}`, PhaseIdle, ""},
		{true, `{"type":"task_start","v":1,"task_id":"t-1","attempt_id":"a-1","attempt_no":1,"out_dir":"/o"}`, PhaseStarting, "a-1"},
		{false, `{"type":"task_accepted","v":1,"seq":2,"attempt_id":"a-1"}`, PhaseActive, "a-1"},
		{true, `{"type":"cancel","v":1,"attempt_id":"a-1","reason":"user","grace_ms":0}`, PhaseActive, "a-1"},
		{true, `{"type":"task_outcome","v":1,"attempt_id":"a-1","verdict":"cancelled"}`, PhaseProposed, "a-1"},
		{false, `{"type":"task_released","v":1,"seq":3,"attempt_id":"a-1"}`, PhaseIdle, ""},
		{true, `{"type":"session_close","v":1,"grace_ms":0}`, PhaseClosing, ""},
		{false, `{"type":"closed","v":1,"seq":4}`, PhaseClosed, ""},
	}
	for i, st := range steps {
		dir, apply := WorkerToHost, s.Accept
		if st.host {
			dir, apply = HostToWorker, s.HostSent
		}
		m, err := DecodeSessionLine(dir, []byte(st.line))
		if err == nil {
			err = apply(m)
		}
		if err != nil {
			t.Fatalf("第 %d 步：%v", i, err)
		}
		if s.Phase() != st.phase || s.Current() != st.cur {
			t.Fatalf("第 %d 步后 (%q, %q)，期望 (%q, %q)", i, s.Phase(), s.Current(), st.phase, st.cur)
		}
	}
}
