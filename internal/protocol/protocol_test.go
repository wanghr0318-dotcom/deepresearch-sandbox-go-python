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
		if in, ok := m.(*Init); ok && err == nil {
			s.NegotiateExtensions(in.Extensions) // sub-run 扩展协商（M4 Plan 14）
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

// ---- sub-run 扩展（M4 Plan 14）----

func hasCode(err error, code string) bool { return CodeOf(err) == code }

func TestSubrunMessages(t *testing.T) {
	ok := []string{
		`{"type":"subrun_start","v":1,"seq":3,"subrun_id":"st1","parent_step_id":"research","deadline_ms":600000}`,
		`{"type":"subrun_start","v":1,"seq":3,"subrun_id":"st2","parent_step_id":"research","deadline_ms":1,"budget_cap_micro":0}`,
		`{"type":"subrun_end","v":1,"seq":9,"subrun_id":"st1","status":"succeeded","summary":"ok"}`,
		`{"type":"subrun_cancel","v":1,"seq":9,"subrun_id":"st1","reason":"enough evidence"}`,
		`{"type":"checkpoint","v":1,"seq":10,"checkpoint_id":"c2","scope":"task","step_id":"research","state":{},"subruns":[{"subrun_id":"st1","status":"completed","result_ref":"` + strings.Repeat("a", 64) + `"},{"subrun_id":"st2","status":"started"}]}`,
	}
	for _, l := range ok {
		if _, err := DecodeLine(WorkerToHost, []byte(l)); err != nil {
			t.Fatalf("%s: %v", l, err)
		}
	}
	bad := map[string]string{
		`{"type":"subrun_start","v":1,"seq":3,"subrun_id":"root","parent_step_id":"p","deadline_ms":1}`:                                                                                             CodeInvalidField,
		`{"type":"subrun_start","v":1,"seq":3,"subrun_id":"St1","parent_step_id":"p","deadline_ms":1}`:                                                                                              CodeInvalidField,
		`{"type":"subrun_start","v":1,"seq":3,"subrun_id":"st1","parent_step_id":"p","deadline_ms":0}`:                                                                                              CodeInvalidField,
		`{"type":"subrun_start","v":1,"seq":3,"subrun_id":"st1","parent_step_id":"p","deadline_ms":3600001}`:                                                                                        CodeInvalidField,
		`{"type":"subrun_end","v":1,"seq":9,"subrun_id":"st1","status":"completed","summary":""}`:                                                                                                   CodeInvalidField,
		`{"type":"checkpoint","v":1,"seq":10,"checkpoint_id":"c","scope":"task","step_id":"s","state":{},"subruns":[{"subrun_id":"st1","status":"completed"}]}`:                                     CodeInvalidField,
		`{"type":"checkpoint","v":1,"seq":10,"checkpoint_id":"c","scope":"task","step_id":"s","state":{},"subruns":[{"subrun_id":"st1","status":"started"},{"subrun_id":"st1","status":"failed"}]}`: CodeInvalidField,
	}
	for l, code := range bad {
		if _, err := DecodeLine(WorkerToHost, []byte(l)); !hasCode(err, code) {
			t.Fatalf("%s: err = %v，期望 %s", l, err, code)
		}
	}
	// 宿主方向：rejected 必须带 code；started 不得带 code
	if _, err := DecodeLine(HostToWorker, []byte(`{"type":"subrun_started","v":1,"subrun_id":"st1","status":"rejected"}`)); !hasCode(err, CodeInvalidField) {
		t.Fatal("rejected 缺 code 应拒绝")
	}
	if _, err := DecodeLine(HostToWorker, []byte(`{"type":"subrun_started","v":1,"subrun_id":"st1","status":"started","code":"conflict"}`)); !hasCode(err, CodeInvalidField) {
		t.Fatal("started 带 code 应拒绝")
	}
}

// TestSubrunLimits 覆盖按常量生成、不适合放进 fixtures 的上限：summary 与 parent_step_id 按 UTF-8 字节计；
// checkpoint 的 subruns[].result_ref 与 refs 合计受每 checkpoint 1024 个引用的上限约束。
func TestSubrunLimits(t *testing.T) {
	ref := strings.Repeat("a", 64)
	refs := make([]string, MaxRefsPerCheckpoint)
	for i := range refs {
		refs[i] = ref
	}
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	oneLess, err := json.Marshal(refs[1:])
	if err != nil {
		t.Fatal(err)
	}
	cp := func(refs []byte, subruns string) string {
		return `{"type":"checkpoint","v":1,"seq":1,"checkpoint_id":"c","scope":"task","step_id":"s","state":{},"refs":` + string(refs) + `,"subruns":` + subruns + `}`
	}
	end := func(summary string) string {
		return `{"type":"subrun_end","v":1,"seq":1,"subrun_id":"st1","status":"failed","summary":"` + summary + `"}`
	}
	start := func(parent string) string {
		return `{"type":"subrun_start","v":1,"seq":1,"subrun_id":"st1","parent_step_id":"` + parent + `","deadline_ms":1}`
	}
	result := func(summary string) string {
		return `{"type":"result","v":1,"seq":1,"summary":"s","outputs":[],"subruns":[{"id":"st1","status":"failed","summary":"` + summary + `"}]}`
	}
	cases := []struct {
		name, line, want string
	}{
		{"refs 加 result_ref 恰好到上限", cp(oneLess, `[{"subrun_id":"st1","status":"completed","result_ref":"`+ref+`"}]`), ""},
		{"refs 加 result_ref 超过上限", cp(refsJSON, `[{"subrun_id":"st1","status":"completed","result_ref":"`+ref+`"}]`), CodeTooManyRefs},
		{"非 completed 的项不计入 refs", cp(refsJSON, `[{"subrun_id":"st1","status":"started"}]`), ""},
		{"subrun_end.summary 恰好 4096 字节", end(strings.Repeat("a", MaxSubrunSummary)), ""},
		{"subrun_end.summary 按字节计超限", end(strings.Repeat("é", MaxSubrunSummary/2) + "a"), CodeInvalidField},
		{"result.subruns[].summary 超限", result(strings.Repeat("a", MaxSubrunSummary+1)), CodeInvalidField},
		{"parent_step_id 恰好 256 字节", start(strings.Repeat("p", 256)), ""},
		{"parent_step_id 超过 256 字节", start(strings.Repeat("é", 128) + "p"), CodeInvalidField},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeLine(WorkerToHost, []byte(c.line)); CodeOf(err) != c.want {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", CodeOf(err), c.want, err)
			}
		})
	}
}

func TestSubrunEncodeDirection(t *testing.T) {
	started := &SubrunStarted{HostHeader: HostHeader{Type: TypeSubrunStarted, V: Version}, SubrunID: "st1", Status: SubrunStatusStarted}
	if _, err := EncodeLine(WorkerToHost, started); !hasCode(err, CodeUnknownType) {
		t.Fatalf("按 Worker 方向编码 subrun_started：err = %v，期望 %s", err, CodeUnknownType)
	}
	if _, err := EncodeLine(HostToWorker, started); err != nil {
		t.Fatalf("按宿主方向编码 subrun_started：%v", err)
	}
	start := &SubrunStart{EventHeader: EventHeader{Type: TypeSubrunStart, V: Version, Seq: 1}, SubrunID: "st1", ParentStepID: "p", DeadlineMS: 1}
	if _, err := EncodeLine(HostToWorker, start); !hasCode(err, CodeUnknownType) {
		t.Fatalf("按宿主方向编码 subrun_start：err = %v，期望 %s", err, CodeUnknownType)
	}
	// session 模式：task 相关的 sub-run 事件必须带 attempt_id；宿主答复不带
	if _, err := EncodeSessionLine(WorkerToHost, start); !hasCode(err, CodeMissingField) {
		t.Fatalf("session 模式缺 attempt_id 的 subrun_start：err = %v，期望 %s", err, CodeMissingField)
	}
	start.AttemptID = "a-1"
	if _, err := EncodeSessionLine(WorkerToHost, start); err != nil {
		t.Fatalf("session 模式编码 subrun_start：%v", err)
	}
	if _, err := EncodeSessionLine(HostToWorker, started); err != nil {
		t.Fatalf("session 模式编码 subrun_started：%v", err)
	}
}

// TestSubrunWorkerStream 覆盖 task 模式事件流中 sub-run 扩展的规则：协商、未知 ID、ready 之前与终态之后。
func TestSubrunWorkerStream(t *testing.T) {
	const (
		ready    = `{"type":"ready","v":1,"seq":1,"protocol_version":1,"mode":"task","worker":{"name":"w"},"capabilities":[]}`
		readyExt = `{"type":"ready","v":1,"seq":1,"protocol_version":1,"mode":"task","worker":{"name":"w"},"capabilities":[],"subruns":1}`
	)
	start := func(seq int, id string) string {
		return `{"type":"subrun_start","v":1,"seq":` + strconv.Itoa(seq) + `,"subrun_id":"` + id + `","parent_step_id":"p","deadline_ms":1000}`
	}
	end := func(seq int, id string) string {
		return `{"type":"subrun_end","v":1,"seq":` + strconv.Itoa(seq) + `,"subrun_id":"` + id + `","status":"succeeded","summary":"ok"}`
	}
	cancel := func(seq int, id string) string {
		return `{"type":"subrun_cancel","v":1,"seq":` + strconv.Itoa(seq) + `,"subrun_id":"` + id + `","reason":"enough"}`
	}
	progress := func(seq int, id string) string {
		return `{"type":"progress","v":1,"seq":` + strconv.Itoa(seq) + `,"kind":"tool_call","message":"m","subrun_id":"` + id + `"}`
	}
	cp := func(seq int) string {
		return `{"type":"checkpoint","v":1,"seq":` + strconv.Itoa(seq) + `,"checkpoint_id":"c","scope":"task","step_id":"s","state":{},"subruns":[{"subrun_id":"st9","status":"started"}]}`
	}
	result := func(seq int) string {
		return `{"type":"result","v":1,"seq":` + strconv.Itoa(seq) + `,"summary":"s","outputs":[]}`
	}
	cases := []struct {
		name       string
		negotiated bool
		lines      []string
		at         int    // 期望违规的行；-1 表示无违规
		want       string // 期望错误码
	}{
		{"协商后的完整流程", true, []string{readyExt, start(2, "st1"), progress(3, "st1"), cancel(4, "st1"), end(5, "st1"), result(6)}, -1, ""},
		{"checkpoint 可列出本 attempt 未启动的 sub-run（恢复前已完成）", true, []string{readyExt, cp(2)}, -1, ""},
		{"ready 之前的 subrun_start", true, []string{start(1, "st1")}, 0, CodeBeforeReady},
		{"终态提议之后的 subrun_end", true, []string{readyExt, start(2, "st1"), result(3), end(4, "st1")}, 3, CodeAfterTerminal},
		{"请求了扩展而 ready 未确认", true, []string{ready}, 0, CodeExtensionMismatch},
		{"未请求扩展而 ready 确认", false, []string{readyExt}, 0, CodeExtensionMismatch},
		{"未协商时的 subrun_start", false, []string{ready, start(2, "st1")}, 1, CodeExtensionNotNegotiated},
		{"未协商时带 subrun_id 的 progress", false, []string{ready, progress(2, "st1")}, 1, CodeExtensionNotNegotiated},
		{"未协商时带 subruns 的 checkpoint", false, []string{ready, cp(2)}, 1, CodeExtensionNotNegotiated},
		{"未启动 ID 的 subrun_end", true, []string{readyExt, start(2, "st1"), end(3, "st2")}, 2, CodeSubrunUnknown},
		{"未启动 ID 的 subrun_cancel", true, []string{readyExt, cancel(2, "st1")}, 1, CodeSubrunUnknown},
		{"未启动 ID 的 progress", true, []string{readyExt, progress(2, "st1")}, 1, CodeSubrunUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var s WorkerStream
			if c.negotiated {
				s.NegotiateExtensions([]string{"future_ext", ExtensionSubruns})
			} else {
				s.NegotiateExtensions([]string{"future_ext"})
			}
			at, code := -1, ""
			for i, l := range c.lines {
				m, err := DecodeLine(WorkerToHost, []byte(l))
				if err != nil {
					t.Fatalf("第 %d 行解码：%v", i, err)
				}
				if err := s.Observe(m); err != nil {
					at, code = i, CodeOf(err)
					break
				}
			}
			if at != c.at || code != c.want {
				t.Fatalf("得到 (%d, %q)，期望 (%d, %q)", at, code, c.at, c.want)
			}
		})
	}
}

// TestSubrunScenariosPresent 保证规格 §5.11 要求的三个 sub-run 场景存在且期望符合计划。
func TestSubrunScenariosPresent(t *testing.T) {
	want := map[string]struct {
		stream, violation string
	}{
		"subrun_start_before_ack":           {"violation", CodeSubrunUnknown},
		"subrun_late_complete_after_cancel": {"ok", ""},
		"subrun_end_without_checkpoint":     {"ok", ""},
	}
	for name, w := range want {
		var sc scenario
		loadJSON(t, filepath.Join(fixtureDir, "scenarios", name+".json"), &sc)
		if sc.Name != name || sc.Expect.Stream != w.stream || sc.Expect.Violation != w.violation {
			t.Errorf("%s：名称 %q，期望 (%q, %q)，计划要求 (%q, %q)", name, sc.Name, sc.Expect.Stream, sc.Expect.Violation, w.stream, w.violation)
		}
	}
}

// TestSubrunSessionStream 覆盖 session 模式下宿主侧 sub-run 消息的检查（Worker 侧由 session_subruns.jsonl 覆盖）。
func TestSubrunSessionStream(t *testing.T) {
	steps := []struct {
		host bool
		line string
	}{
		{true, `{"type":"init","bootstrap":1,"protocol_versions":[1],"mode":"session","session_id":"s-1","incarnation_id":"i-1","extensions":["subruns"]}`},
		{false, `{"type":"ready","v":1,"seq":1,"protocol_version":1,"mode":"session","session_ext":1,"subruns":1,"worker":{"name":"w"},"capabilities":[]}`},
		{true, `{"type":"task_start","v":1,"task_id":"t-1","attempt_id":"a-1","attempt_no":1,"out_dir":"/o"}`},
		{false, `{"type":"task_accepted","v":1,"seq":2,"attempt_id":"a-1"}`},
		{false, `{"type":"subrun_start","v":1,"seq":3,"attempt_id":"a-1","subrun_id":"st1","parent_step_id":"p","deadline_ms":1000}`},
		{true, `{"type":"subrun_started","v":1,"subrun_id":"st1","status":"started"}`},
		{true, `{"type":"subrun_cancel_requested","v":1,"subrun_id":"st1","reason":"deadline"}`},
	}
	s := NewSessionStream()
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
	}
	unknown := &SubrunStarted{HostHeader: HostHeader{Type: TypeSubrunStarted, V: Version}, SubrunID: "st2", Status: SubrunStatusStarted}
	if err := s.HostSent(unknown); !hasCode(err, CodeSubrunUnknown) {
		t.Fatalf("对未启动的 sub-run 发送 subrun_started：err = %v，期望 %s", err, CodeSubrunUnknown)
	}

	idle := NewSessionStream()
	if err := idle.HostSent(&Init{Type: TypeInit, Bootstrap: BootstrapVersion, ProtocolVersions: []int64{Version}, Mode: ModeSession, SessionID: "s-1", IncarnationID: "i-1", Extensions: []string{ExtensionSubruns}}); err != nil {
		t.Fatal(err)
	}
	req := &SubrunCancelRequested{HostHeader: HostHeader{Type: TypeSubrunCancelRequested, V: Version}, SubrunID: "st1", Reason: SubrunCancelDeadline}
	if err := idle.HostSent(req); !hasCode(err, CodeWrongAttempt) {
		t.Fatalf("没有当前 attempt 时发送 subrun_cancel_requested：err = %v，期望 %s", err, CodeWrongAttempt)
	}
}
