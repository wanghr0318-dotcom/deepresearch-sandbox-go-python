package api

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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// ---- 内存 Store ----

type fakeStore struct {
	mu       sync.Mutex
	order    []string // 创建顺序
	tasks    map[string]*TaskView
	events   map[string][]Event
	requests map[string]RequestRecord
	inspect  map[string]Inspection
	limits   map[string]string // request_id → 存储的 limits
	// failWith 非空时，下一次 CreateTask/AcceptControl/GetTask/ListTasks 返回该错误；
	// commitThenUnknown 为真时先提交再返回 ErrCommitUnknown。
	failWith          error
	commitThenUnknown bool
	results           map[string]json.RawMessage // task_id → result_json
	artifacts         map[string][]ArtifactView  // task_id/artifact_id → 按版本升序
	internal          map[string]bool            // task_id/artifact_id → visibility = internal（对下载不可见）
}

func newFakeStore() *fakeStore {
	return &fakeStore{tasks: map[string]*TaskView{}, events: map[string][]Event{},
		requests: map[string]RequestRecord{}, inspect: map[string]Inspection{}, limits: map[string]string{},
		results: map[string]json.RawMessage{}, artifacts: map[string][]ArtifactView{}, internal: map[string]bool{}}
}

func (f *fakeStore) TaskResult(_ context.Context, taskID string) (ResultView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[taskID]
	if !ok {
		return ResultView{}, persistence.ErrNotFound
	}
	return ResultView{Status: t.Status, Result: f.results[taskID]}, nil
}

func (f *fakeStore) PinnedArtifact(_ context.Context, taskID, artifactID string, version int64) (ArtifactView, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.tasks[taskID]; !ok {
		return ArtifactView{}, false, persistence.ErrNotFound
	}
	vs := f.artifacts[taskID+"/"+artifactID]
	if len(vs) == 0 || f.internal[taskID+"/"+artifactID] {
		return ArtifactView{}, false, nil
	}
	if version == 0 {
		return vs[len(vs)-1], true, nil
	}
	for _, v := range vs {
		if v.Version == version {
			return v, true, nil
		}
	}
	return ArtifactView{}, false, nil
}

// addArtifact 登记产物的下一个版本，并把内容写入 blobs；返回登记的 sha256。
func (f *fakeStore) addArtifact(blobs *fakeBlobs, taskID, artifactID, mediaType string, content []byte) string {
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	blobs.put(sha, content)
	f.mu.Lock()
	defer f.mu.Unlock()
	k := taskID + "/" + artifactID
	f.artifacts[k] = append(f.artifacts[k], ArtifactView{ArtifactID: artifactID, Version: int64(len(f.artifacts[k]) + 1),
		SHA256: sha, Size: int64(len(content)), MediaType: mediaType})
	return sha
}

// fakeBlobs 是内存 BlobStore；put 可以写入与 sha256 不符的内容以模拟篡改。
type fakeBlobs struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (b *fakeBlobs) put(sha string, content []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m[sha] = content
}

func (b *fakeBlobs) Open(sha string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.m[sha]
	if !ok {
		return nil, errors.New("blob 不存在")
	}
	return io.NopCloser(bytes.NewReader(c)), nil
}

func (f *fakeStore) limitsOf(t *testing.T, requestID string) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.limits[requestID]
	if !ok {
		t.Fatalf("请求 %s 没有创建任务", requestID)
	}
	return l
}

func (f *fakeStore) takeFailure() error {
	err := f.failWith
	f.failWith = nil
	return err
}

func (f *fakeStore) appendEvent(taskID, typ string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	evs := f.events[taskID]
	f.events[taskID] = append(evs, Event{TaskSeq: int64(len(evs) + 1), Source: "host", Type: typ,
		Payload: json.RawMessage(`{"n":` + strconv.Itoa(len(evs)+1) + `}`), TS: time.Unix(1700000000, 0).UTC()})
}

func (f *fakeStore) addTask(id, status, desired string) {
	f.mu.Lock()
	f.tasks[id] = &TaskView{TaskID: id, Status: status, Desired: desired, ControlVersion: 1}
	f.order = append(f.order, id)
	f.mu.Unlock()
}

func (f *fakeStore) claim(requestID, kind string, hash []byte) (RequestRecord, bool, error) {
	rec, ok := f.requests[requestID]
	if !ok {
		return RequestRecord{}, false, nil
	}
	if rec.Kind != kind || !bytes.Equal(rec.BodyHash, hash) {
		return RequestRecord{}, false, fmt.Errorf("%w: request_conflict", persistence.ErrConflict)
	}
	return rec, true, nil
}

func (f *fakeStore) CreateTask(_ context.Context, req CreateTaskRequest) (CreateTaskResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.takeFailure(); err != nil {
		return CreateTaskResult{}, err
	}
	if rec, replayed, err := f.claim(req.RequestID, "create_task", req.BodyHash); err != nil {
		return CreateTaskResult{}, err
	} else if replayed {
		var res CreateTaskResult
		_ = json.Unmarshal(rec.Response, &res)
		res.Replayed = true
		return res, nil
	}
	if req.TaskID == "" || len(req.Spec) == 0 || req.MaxFaultRetries != 3 {
		return CreateTaskResult{}, errors.New("fake: 缺少字段")
	}
	f.tasks[req.TaskID] = &TaskView{TaskID: req.TaskID, Status: "queued", Desired: "run", ControlVersion: 1}
	f.limits[req.RequestID] = string(req.Limits)
	f.order = append(f.order, req.TaskID)
	f.events[req.TaskID] = []Event{{TaskSeq: 1, Source: "host", Type: "task_created"}}
	res := CreateTaskResult{TaskID: req.TaskID}
	b, _ := json.Marshal(res)
	f.requests[req.RequestID] = RequestRecord{RequestID: req.RequestID, Kind: "create_task", BodyHash: req.BodyHash, ResourceID: req.TaskID, Response: b}
	if f.commitThenUnknown {
		f.commitThenUnknown = false
		return CreateTaskResult{}, &persistence.CommitUnknownError{Op: "CreateTask", Identity: req.RequestID, Err: errors.New("超时")}
	}
	return res, nil
}

func (f *fakeStore) AcceptControl(_ context.Context, req ControlRequest) (ControlResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.takeFailure(); err != nil {
		return ControlResult{}, err
	}
	if rec, replayed, err := f.claim(req.RequestID, "control", req.BodyHash); err != nil {
		return ControlResult{}, err
	} else if replayed {
		var res ControlResult
		_ = json.Unmarshal(rec.Response, &res)
		if res.TaskID != req.TaskID {
			return ControlResult{}, persistence.ErrConflict
		}
		res.Replayed = true
		return res, nil
	}
	t, ok := f.tasks[req.TaskID]
	if !ok {
		return ControlResult{}, fmt.Errorf("%w: 任务", persistence.ErrNotFound)
	}
	switch {
	case t.Status == "succeeded" || t.Status == "failed" || t.Status == "cancelled":
		return ControlResult{}, &persistence.RejectedError{Code: persistence.CodeTaskEnded}
	case t.Desired == "cancel" && req.Desired != "cancel":
		return ControlResult{}, &persistence.RejectedError{Code: persistence.CodeCancelPending}
	case req.Desired == "run" && t.Status != "paused":
		return ControlResult{}, &persistence.RejectedError{Code: persistence.CodeNotPaused}
	}
	t.ControlVersion++
	t.Desired = req.Desired
	res := ControlResult{TaskID: req.TaskID, ControlVersion: t.ControlVersion}
	b, _ := json.Marshal(res)
	f.requests[req.RequestID] = RequestRecord{RequestID: req.RequestID, Kind: "control", BodyHash: req.BodyHash, ResourceID: req.TaskID, Response: b}
	return res, nil
}

func (f *fakeStore) GetRequest(_ context.Context, requestID string) (RequestRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.requests[requestID]
	if !ok {
		return RequestRecord{}, persistence.ErrNotFound
	}
	return rec, nil
}

func (f *fakeStore) GetTask(_ context.Context, taskID string) (TaskView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.takeFailure(); err != nil {
		return TaskView{}, err
	}
	t, ok := f.tasks[taskID]
	if !ok {
		return TaskView{}, persistence.ErrNotFound
	}
	return *t, nil
}

func (f *fakeStore) ListEvents(_ context.Context, taskID string, afterSeq int64, limit int) ([]Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Event
	for _, e := range f.events[taskID] {
		if e.TaskSeq > afterSeq && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeStore) ListTasks(_ context.Context, after string, limit int) ([]TaskView, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.takeFailure(); err != nil {
		return nil, "", err
	}
	var out []TaskView
	started := after == ""
	for i := len(f.order) - 1; i >= 0; i-- { // 新的在前
		id := f.order[i]
		if !started {
			started = id == after
			continue
		}
		if len(out) == limit {
			return out, out[len(out)-1].TaskID, nil
		}
		out = append(out, *f.tasks[id])
	}
	return out, "", nil
}

func (f *fakeStore) Inspect(_ context.Context, taskID string) (Inspection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	in, ok := f.inspect[taskID]
	if !ok {
		return Inspection{}, persistence.ErrNotFound
	}
	return in, nil
}

// ---- 测试服务器 ----

type testServer struct {
	t     *testing.T
	h     *Handler
	srv   *httptest.Server
	store *fakeStore
	blobs *fakeBlobs
	mode  Mode
	mu    sync.Mutex
	logs  bytes.Buffer
}

// newTestServer 启动绑定 127.0.0.1 的服务；mutate 可修改配置（ListenAddr 已填为实际地址）。
func newTestServer(t *testing.T, mutate func(*Config)) *testServer {
	t.Helper()
	ts := &testServer{t: t, store: newFakeStore(), blobs: &fakeBlobs{m: map[string][]byte{}}, mode: ModeNormal}
	srv := httptest.NewUnstartedServer(nil)
	cfg := Config{
		Store:        ts.store,
		Blobs:        ts.blobs,
		Mode:         func() Mode { ts.mu.Lock(); defer ts.mu.Unlock(); return ts.mode },
		ListenAddr:   srv.Listener.Addr().String(),
		Logger:       slog.New(slog.NewJSONHandler(&lockedWriter{mu: &ts.mu, w: &ts.logs}, nil)),
		PollInterval: 5 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts.h = h
	srv.Config.Handler = h
	srv.Start()
	ts.srv = srv
	t.Cleanup(srv.Close)
	return ts
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

func (ts *testServer) setMode(m Mode) { ts.mu.Lock(); ts.mode = m; ts.mu.Unlock() }

// do 发送请求，并断言返回的状态码属于该操作在 routes（即 openapi.yaml）中声明的集合。
func (ts *testServer) do(method, path, body string, hdr map[string]string) (int, []byte, http.Header) {
	ts.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.srv.URL+path, rd)
	if err != nil {
		ts.t.Fatal(err)
	}
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := ts.srv.Client().Do(req)
	if err != nil {
		ts.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	ts.checkDeclared(req, resp.StatusCode)
	return resp.StatusCode, b, resp.Header
}

func (ts *testServer) checkDeclared(req *http.Request, status int) {
	ts.t.Helper()
	_, pattern := ts.h.mux.Handler(req)
	for _, rt := range routes {
		if rt.method+" "+rt.path == pattern {
			if !slices.Contains(rt.statuses, status) {
				ts.t.Errorf("%s 返回了未在契约中声明的状态码 %d", pattern, status)
			}
			return
		}
	}
}

func errCode(t *testing.T, b []byte) string {
	t.Helper()
	var e errorBody
	if err := json.Unmarshal(b, &e); err != nil || e.Code == "" || e.Message == "" {
		t.Fatalf("错误体不是 {code, message}: %s", b)
	}
	return e.Code
}

func expect(t *testing.T, gotStatus int, body []byte, wantStatus int, wantCode string) {
	t.Helper()
	if gotStatus != wantStatus {
		t.Fatalf("状态码 = %d，期望 %d；body = %s", gotStatus, wantStatus, body)
	}
	if wantCode != "" {
		if c := errCode(t, body); c != wantCode {
			t.Fatalf("错误码 = %q，期望 %q", c, wantCode)
		}
	}
}

// ---- 契约 ----

// openAPIOperations 以行为单位读取 openapi.yaml 的 paths：路径（缩进 2）、方法（缩进 4）、
// responses 下的状态码（缩进 8）。
func openAPIOperations(t *testing.T) map[string][]int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	ops := map[string][]int{}
	statusRe := regexp.MustCompile(`^'(\d{3})':`)
	var inPaths, inResponses bool
	var path, op string
	for _, line := range strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n") {
		trimmed := strings.TrimLeft(line, " ")
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := len(line) - len(trimmed)
		switch {
		case indent == 0:
			inPaths = trimmed == "paths:"
			op = ""
		case !inPaths:
		case indent == 2:
			path, op = strings.TrimSuffix(trimmed, ":"), ""
		case indent == 4:
			op = ""
			if m := strings.TrimSuffix(trimmed, ":"); slices.Contains([]string{"get", "post", "put", "patch", "delete"}, m) {
				op = strings.ToUpper(m) + " " + path
				ops[op] = []int{}
			}
		case indent == 6 && op != "":
			inResponses = trimmed == "responses:"
		case indent == 8 && op != "" && inResponses:
			if m := statusRe.FindStringSubmatch(trimmed); m != nil {
				n, _ := strconv.Atoi(m[1])
				ops[op] = append(ops[op], n)
			}
		}
	}
	return ops
}

func TestHandlersMatchOpenAPI(t *testing.T) {
	spec := openAPIOperations(t)
	impl := map[string][]int{}
	for _, rt := range routes {
		impl[rt.method+" "+rt.path] = slices.Sorted(slices.Values(rt.statuses))
	}
	for k, v := range spec {
		sort.Ints(v)
		got, ok := impl[k]
		if !ok {
			t.Errorf("openapi.yaml 声明了 %s，处理器未实现", k)
			continue
		}
		if !slices.Equal(got, v) {
			t.Errorf("%s 状态码：处理器 %v，openapi.yaml %v", k, got, v)
		}
	}
	for k := range impl {
		if _, ok := spec[k]; !ok {
			t.Errorf("处理器实现了 %s，openapi.yaml 未声明", k)
		}
	}
	if len(spec) != 12 {
		t.Errorf("openapi.yaml 解析出 %d 个操作，期望 12（M1 范围加产物最新版本下载）", len(spec))
	}
	for k := range spec {
		if strings.Contains(k, "/sessions") {
			t.Errorf("sessions 属于 M4，不应出现在本版契约: %s", k)
		}
	}
}

// ---- 任务 ----

func TestCreateTaskIdempotency(t *testing.T) {
	ts := newTestServer(t, nil)
	st, b, _ := ts.do("POST", "/tasks", `{"request_id":"r1","spec":{"a":1,"b":[1,2]},"limits":{"cpu":1}}`, nil)
	expect(t, st, b, 201, "")
	var first CreateTaskResult
	_ = json.Unmarshal(b, &first)
	if first.TaskID == "" {
		t.Fatalf("没有 task_id: %s", b)
	}
	// 同内容（键顺序与空白不同）返回首次结果。
	st, b, _ = ts.do("POST", "/tasks", "{ \"limits\": {\"cpu\": 1},\n \"spec\": {\"b\": [1, 2], \"a\": 1}, \"request_id\": \"r1\" }", nil)
	expect(t, st, b, 201, "")
	var again CreateTaskResult
	_ = json.Unmarshal(b, &again)
	if again.TaskID != first.TaskID {
		t.Fatalf("重放返回 %q，期望首次结果 %q", again.TaskID, first.TaskID)
	}
	if n := len(ts.store.order); n != 1 {
		t.Fatalf("重放创建了新任务：共 %d 个", n)
	}
	// 同 request_id 不同内容 → 409 request_conflict。
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"r1","spec":{"a":2}}`, nil)
	expect(t, st, b, 409, "request_conflict")

	for _, bad := range []string{
		`{"spec":{"a":1}}`,                    // 缺 request_id
		`{"request_id":"r2"}`,                 // 缺 spec
		`{"request_id":"r2","spec":[1]}`,      // spec 不是对象
		`{"request_id":"r2","spec":{},"x":1}`, // 未知字段
		`{"request_id":"r2","spec":{}} {}`,    // 多余内容
		`not json`,
	} {
		st, b, _ = ts.do("POST", "/tasks", bad, nil)
		expect(t, st, b, 400, "invalid_request")
	}
}

// TestCreateTaskEffectiveLimits：装配提供的 EffectiveLimits 拒绝 → 400 invalid_limits 且不创建任务；
// 接受时存储的是它返回的有效 limits（省略 limits 时也调用，收到 nil）。
func TestCreateTaskEffectiveLimits(t *testing.T) {
	var seen []string
	ts := newTestServer(t, func(c *Config) {
		c.EffectiveLimits = func(l json.RawMessage) (json.RawMessage, error) {
			seen = append(seen, string(l))
			var v struct {
				MemoryMax int64 `json:"memory_max"`
			}
			if len(l) > 0 {
				if err := json.Unmarshal(l, &v); err != nil || v.MemoryMax > 1024 {
					return nil, errors.New("memory_max 超过总内存")
				}
			}
			return json.RawMessage(fmt.Sprintf(`{"max_run_time_ms":3600000,"memory_max":%d}`, v.MemoryMax)), nil
		}
	})
	st, b, _ := ts.do("POST", "/tasks", `{"request_id":"r1","spec":{},"limits":{"memory_max":4096}}`, nil)
	expect(t, st, b, 400, "invalid_limits")
	if n := len(ts.store.order); n != 0 {
		t.Fatalf("被拒绝的请求创建了任务：共 %d 个", n)
	}
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"r2","spec":{},"limits":{"memory_max":512}}`, nil)
	expect(t, st, b, 201, "")
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"r3","spec":{}}`, nil)
	expect(t, st, b, 201, "")
	if want := []string{`{"memory_max":4096}`, `{"memory_max":512}`, ``}; !slices.Equal(seen, want) {
		t.Fatalf("EffectiveLimits 收到 %q，期望 %q", seen, want)
	}
	if got := ts.store.limitsOf(t, "r3"); got != `{"max_run_time_ms":3600000,"memory_max":0}` {
		t.Fatalf("存储的 limits = %s，应为有效 limits", got)
	}
}

func TestCreateTaskCommitUnknownResolvedByRequestRecord(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.commitThenUnknown = true
	st, b, _ := ts.do("POST", "/tasks", `{"request_id":"r1","spec":{}}`, nil)
	expect(t, st, b, 201, "")
	// 未提交时提交结果未知 → 503，客户端以同一 request_id 重试。
	ts.store.failWith = &persistence.CommitUnknownError{Op: "CreateTask", Identity: "r2", Err: errors.New("超时")}
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"r2","spec":{}}`, nil)
	expect(t, st, b, 503, "commit_unknown")
}

func TestStoreErrorMapping(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.failWith = fmt.Errorf("%w: 连接失败", persistence.ErrUnavailable)
	st, b, _ := ts.do("GET", "/tasks", "", nil)
	expect(t, st, b, 503, "store_unavailable")
	ts.store.failWith = fmt.Errorf("%w: limit 必须为正", persistence.ErrInvalid)
	st, b, _ = ts.do("GET", "/tasks", "", nil)
	expect(t, st, b, 400, "invalid_request")
	ts.store.failWith = errors.New("意外")
	st, b, _ = ts.do("GET", "/tasks", "", nil)
	expect(t, st, b, 500, "internal")
	if strings.Contains(string(b), "意外") {
		t.Fatal("内部错误细节泄露到响应")
	}
}

func TestGetAndListTasks(t *testing.T) {
	ts := newTestServer(t, nil)
	for i := 1; i <= 3; i++ {
		ts.store.addTask(fmt.Sprintf("t%d", i), "queued", "run")
	}
	st, b, _ := ts.do("GET", "/tasks/t2", "", nil)
	expect(t, st, b, 200, "")
	var tv map[string]any
	_ = json.Unmarshal(b, &tv)
	if tv["task_id"] != "t2" || tv["status"] != "queued" || tv["desired"] != "run" {
		t.Fatalf("任务视图 = %s", b)
	}
	st, b, _ = ts.do("GET", "/tasks/nope", "", nil)
	expect(t, st, b, 404, "task_not_found")

	var seen []string
	after := ""
	for page := 0; page < 5; page++ {
		st, b, _ = ts.do("GET", "/tasks?limit=2&after="+after, "", nil)
		expect(t, st, b, 200, "")
		var l struct {
			Tasks []struct {
				TaskID string `json:"task_id"`
			} `json:"tasks"`
			Next string `json:"next"`
		}
		_ = json.Unmarshal(b, &l)
		for _, x := range l.Tasks {
			seen = append(seen, x.TaskID)
		}
		if l.Next == "" {
			break
		}
		after = l.Next
	}
	if !slices.Equal(seen, []string{"t3", "t2", "t1"}) {
		t.Fatalf("分页结果 = %v", seen)
	}
	for _, q := range []string{"?limit=0", "?limit=201", "?limit=x"} {
		st, b, _ = ts.do("GET", "/tasks"+q, "", nil)
		expect(t, st, b, 400, "invalid_request")
	}
}

func TestControl(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.addTask("run1", "running", "run")
	ts.store.addTask("done", "succeeded", "run")

	st, b, _ := ts.do("POST", "/tasks/run1/pause", `{"request_id":"c1","reason":"x"}`, nil)
	expect(t, st, b, 200, "")
	var r1 ControlResult
	_ = json.Unmarshal(b, &r1)
	// 同 request_id 同内容 → 首次结果（control_version 不再递增）。
	st, b, _ = ts.do("POST", "/tasks/run1/pause", `{"reason":"x","request_id":"c1"}`, nil)
	expect(t, st, b, 200, "")
	var r2 ControlResult
	_ = json.Unmarshal(b, &r2)
	if r1.ControlVersion != 2 || r2 != r1 {
		t.Fatalf("重放结果 %+v，首次 %+v", r2, r1)
	}
	// 同 request_id 用于另一种操作或另一个任务 → request_conflict。
	st, b, _ = ts.do("POST", "/tasks/run1/cancel", `{"request_id":"c1","reason":"x"}`, nil)
	expect(t, st, b, 409, "request_conflict")
	st, b, _ = ts.do("POST", "/tasks/done/pause", `{"request_id":"c1","reason":"x"}`, nil)
	expect(t, st, b, 409, "request_conflict")

	// resume 要求 paused（fake 中任务仍是 running）。
	st, b, _ = ts.do("POST", "/tasks/run1/resume", `{"request_id":"c2"}`, nil)
	expect(t, st, b, 409, "not_paused")
	st, b, _ = ts.do("POST", "/tasks/run1/cancel", `{"request_id":"c3"}`, nil)
	expect(t, st, b, 200, "")
	st, b, _ = ts.do("POST", "/tasks/run1/pause", `{"request_id":"c4"}`, nil)
	expect(t, st, b, 409, "cancel_pending")
	st, b, _ = ts.do("POST", "/tasks/done/cancel", `{"request_id":"c5"}`, nil)
	expect(t, st, b, 409, "task_ended")
	st, b, _ = ts.do("POST", "/tasks/nope/cancel", `{"request_id":"c6"}`, nil)
	expect(t, st, b, 404, "task_not_found")
	st, b, _ = ts.do("POST", "/tasks/run1/cancel", `{}`, nil)
	expect(t, st, b, 400, "invalid_request")
}

func TestInspect(t *testing.T) {
	ts := newTestServer(t, nil)
	code := int64(137)
	ts.store.inspect["t1"] = Inspection{
		Task:        TaskView{TaskID: "t1", Status: "failed"},
		Attempts:    []AttemptView{{AttemptID: "a1", AttemptNo: 1, Status: "ended", OutcomeClass: "oom", ExitSignal: &code, OOMKillDelta: 1, EnvID: "e1", CleanupState: "done"}},
		Checkpoints: []CheckpointView{{CheckpointID: "c1", StepID: "s1", AttemptID: "a1", CommitSeq: 4}},
	}
	st, b, _ := ts.do("GET", "/tasks/t1/inspect", "", nil)
	expect(t, st, b, 200, "")
	var in struct {
		Task struct {
			Status string `json:"status"`
		} `json:"task"`
		Attempts []struct {
			OutcomeClass string `json:"outcome_class"`
			ExitSignal   *int64 `json:"exit_signal"`
			OOMKillDelta int64  `json:"oom_kill_delta"`
		} `json:"attempts"`
		Checkpoints []struct {
			CommitSeq int64 `json:"commit_seq"`
		} `json:"checkpoints"`
	}
	if err := json.Unmarshal(b, &in); err != nil {
		t.Fatal(err)
	}
	if in.Task.Status != "failed" || len(in.Attempts) != 1 || in.Attempts[0].OutcomeClass != "oom" ||
		in.Attempts[0].ExitSignal == nil || *in.Attempts[0].ExitSignal != 137 || in.Attempts[0].OOMKillDelta != 1 ||
		len(in.Checkpoints) != 1 || in.Checkpoints[0].CommitSeq != 4 {
		t.Fatalf("inspect = %s", b)
	}
	st, b, _ = ts.do("GET", "/tasks/nope/inspect", "", nil)
	expect(t, st, b, 404, "task_not_found")
}

// ---- 固定输出下载 ----

// TestTaskResult：终态且有结果时返回规范 JSON，ETag 为正文的 sha256；未终态 409、无结果 404、不存在 404。
func TestTaskResult(t *testing.T) {
	ts := newTestServer(t, nil)
	st, b, _ := ts.do("GET", "/tasks/nope/result", "", nil)
	expect(t, st, b, 404, "task_not_found")

	ts.store.addTask("run", "running", "run")
	st, b, _ = ts.do("GET", "/tasks/run/result", "", nil)
	expect(t, st, b, 409, "task_not_terminal")

	ts.store.addTask("bad", "failed", "run")
	st, b, _ = ts.do("GET", "/tasks/bad/result", "", nil)
	expect(t, st, b, 404, "not_ready")

	ts.store.addTask("ok", "succeeded", "run")
	sha := strings.Repeat("ab", 32)
	ts.store.results["ok"] = json.RawMessage(`{"summary": "done", "outputs": [{"version": 2, "sha256": "` + sha + `", "artifact_id": "report"}]}`)
	st, b, hdr := ts.do("GET", "/tasks/ok/result", "", nil)
	expect(t, st, b, 200, "")
	want := `{"outputs":[{"artifact_id":"report","sha256":"` + sha + `","version":2}],"summary":"done"}`
	if string(b) != want {
		t.Fatalf("结果应为规范 JSON：%s", b)
	}
	sum := sha256.Sum256(b)
	if hdr.Get("ETag") != `"`+hex.EncodeToString(sum[:])+`"` || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("ETag 应为正文的带引号 sha256：%v", hdr)
	}
}

// TestArtifactDownload：默认最新版本、?version=N 与 /versions/{v} 选版本；ETag 与内容 sha256 一致；
// 主动内容（HTML、SVG、无法解析的类型）一律 attachment，全部带 nosniff。
func TestArtifactDownload(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.addTask("t1", "running", "run")
	v1 := ts.store.addArtifact(ts.blobs, "t1", "report", "text/markdown", []byte("# v1"))
	v2 := ts.store.addArtifact(ts.blobs, "t1", "report", "text/markdown", []byte("# v2"))
	for _, c := range []struct{ path, body, sha string }{
		{"/tasks/t1/artifacts/report", "# v2", v2},
		{"/tasks/t1/artifacts/report?version=1", "# v1", v1},
		{"/tasks/t1/artifacts/report?version=2", "# v2", v2},
		{"/tasks/t1/artifacts/report/versions/1", "# v1", v1},
	} {
		st, b, hdr := ts.do("GET", c.path, "", nil)
		expect(t, st, b, 200, "")
		sum := sha256.Sum256(b)
		if string(b) != c.body || hdr.Get("ETag") != `"`+c.sha+`"` || hex.EncodeToString(sum[:]) != c.sha {
			t.Fatalf("%s：内容 %q ETag %s", c.path, b, hdr.Get("ETag"))
		}
		if hdr.Get("Content-Type") != "text/markdown" || hdr.Get("X-Content-Type-Options") != "nosniff" ||
			hdr.Get("Content-Disposition") != "" {
			t.Fatalf("%s：安全类型应内联并带 nosniff：%v", c.path, hdr)
		}
	}
	for _, c := range []struct {
		path   string
		status int
		code   string
	}{
		{"/tasks/t1/artifacts/report?version=3", 404, "artifact_not_found"},
		{"/tasks/t1/artifacts/report/versions/9", 404, "artifact_not_found"},
		{"/tasks/t1/artifacts/missing", 404, "artifact_not_found"},
		{"/tasks/nope/artifacts/report", 404, "task_not_found"},
		{"/tasks/t1/artifacts/report?version=0", 400, "invalid_request"},
		{"/tasks/t1/artifacts/report?version=x", 400, "invalid_request"},
		{"/tasks/t1/artifacts/report/versions/-1", 400, "invalid_request"},
	} {
		st, b, _ := ts.do("GET", c.path, "", nil)
		expect(t, st, b, c.status, c.code)
	}

	for _, c := range []struct{ id, media, wantType string }{
		{"page.html", "text/html; charset=utf-8", "text/html; charset=utf-8"},
		{"pic.svg", "image/svg+xml", "image/svg+xml"},
		{"odd", "not a media type", "application/octet-stream"},
	} {
		ts.store.addArtifact(ts.blobs, "t1", c.id, c.media, []byte("<script>alert(1)</script>"))
		st, b, hdr := ts.do("GET", "/tasks/t1/artifacts/"+c.id, "", nil)
		expect(t, st, b, 200, "")
		if disp := hdr.Get("Content-Disposition"); hdr.Get("Content-Type") != c.wantType || hdr.Get("X-Content-Type-Options") != "nosniff" ||
			(disp != "attachment; filename="+c.id && disp != `attachment; filename="`+c.id+`"`) {
			t.Fatalf("%s（%s）应以 attachment 下载：%v", c.id, c.media, hdr)
		}
	}

	sha := ts.store.addArtifact(ts.blobs, "t1", "lost", "text/plain", []byte("x"))
	ts.blobs.mu.Lock()
	delete(ts.blobs.m, sha)
	ts.blobs.mu.Unlock()
	st, b, _ := ts.do("GET", "/tasks/t1/artifacts/lost", "", nil)
	expect(t, st, b, 500, "blob_unavailable")

	// internal 产物对下载不可见：响应与不存在的产物逐字节相同（不泄露存在性）。
	ts.store.addArtifact(ts.blobs, "t1", "scratch", "text/plain", []byte("internal"))
	ts.store.mu.Lock()
	ts.store.internal["t1/scratch"] = true
	ts.store.mu.Unlock()
	_, missing, _ := ts.do("GET", "/tasks/t1/artifacts/nothing", "", nil)
	for _, p := range []string{"/tasks/t1/artifacts/scratch", "/tasks/t1/artifacts/scratch?version=1", "/tasks/t1/artifacts/scratch/versions/1"} {
		st, b, _ := ts.do("GET", p, "", nil)
		expect(t, st, b, 404, "artifact_not_found")
		if !bytes.Equal(b, missing) {
			t.Fatalf("%s：internal 产物的响应 %s 与不存在的 %s 不同", p, b, missing)
		}
	}
}

// TestArtifactTamperedAborts：BlobStore 中的内容与登记的 sha256 不符（篡改或损坏）时中止连接，客户端
// 读不到完整响应——小内容（仍在缓冲内）与大内容（已分块发送一部分）都一样。
func TestArtifactTamperedAborts(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.addTask("t1", "running", "run")
	for _, size := range []int{16, 256 << 10} {
		id := fmt.Sprintf("a%d", size)
		sha := ts.store.addArtifact(ts.blobs, "t1", id, "text/plain", bytes.Repeat([]byte("a"), size))
		ts.blobs.put(sha, bytes.Repeat([]byte("b"), size)) // 同样大小、不同内容
		resp, err := ts.srv.Client().Get(ts.srv.URL + "/tasks/t1/artifacts/" + id)
		if err != nil {
			continue // 响应头发出之前就中止
		}
		b, err := io.ReadAll(resp.Body)
		if cerr := resp.Body.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err == nil {
			t.Fatalf("%d 字节：篡改的内容被完整发送（%d 字节，状态 %d）", size, len(b), resp.StatusCode)
		}
	}
}

func TestStatusAndModes(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.addTask("t1", "running", "run")
	ts.store.inspect["t1"] = Inspection{Task: TaskView{TaskID: "t1"}}
	for _, m := range []Mode{ModeNormal, ModeDiagnostic, ModeOwnershipLost} {
		ts.setMode(m)
		st, b, _ := ts.do("GET", "/status", "", nil)
		expect(t, st, b, 200, "")
		var s statusResponse
		_ = json.Unmarshal(b, &s)
		if s.Mode != m {
			t.Fatalf("status mode = %q，期望 %q", s.Mode, m)
		}
	}

	ts.setMode(ModeDiagnostic)
	st, b, _ := ts.do("GET", "/tasks/t1/inspect", "", nil)
	expect(t, st, b, 200, "")
	for _, c := range [][2]string{{"GET", "/tasks"}, {"GET", "/tasks/t1"}, {"GET", "/tasks/t1/events"}, {"POST", "/tasks/t1/cancel"}, {"GET", "/tasks/t1/result"}} {
		st, b, _ = ts.do(c[0], c[1], `{"request_id":"d"}`, nil)
		expect(t, st, b, 503, "diagnostic_mode")
	}

	ts.setMode(ModeOwnershipLost)
	st, b, _ = ts.do("GET", "/tasks/t1", "", nil)
	expect(t, st, b, 200, "")
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"o1","spec":{}}`, nil)
	expect(t, st, b, 503, "ownership_lost")
	st, b, _ = ts.do("POST", "/tasks/t1/cancel", `{"request_id":"o2"}`, nil)
	expect(t, st, b, 503, "ownership_lost")
	if len(ts.store.requests) != 0 {
		t.Fatal("ownership_lost 模式下写入了请求")
	}
}

// ---- SSE ----

type sseFrame struct {
	id, event, data string
	comment         bool
}

func readFrame(r *bufio.Reader) (sseFrame, error) {
	var f sseFrame
	got := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return f, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if got {
				return f, nil
			}
			continue
		}
		got = true
		switch {
		case strings.HasPrefix(line, ":"):
			f.comment = true
		case strings.HasPrefix(line, "id: "):
			f.id = line[4:]
		case strings.HasPrefix(line, "event: "):
			f.event = line[7:]
		case strings.HasPrefix(line, "data: "):
			f.data = line[6:]
		}
	}
}

func (ts *testServer) openStream(taskID, lastID string) (*http.Response, *bufio.Reader) {
	ts.t.Helper()
	req, _ := http.NewRequest("GET", ts.srv.URL+"/tasks/"+taskID+"/events", nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := ts.srv.Client().Do(req)
	if err != nil {
		ts.t.Fatal(err)
	}
	ts.checkDeclared(req, resp.StatusCode)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		b, _ := io.ReadAll(resp.Body)
		ts.t.Fatalf("打开事件流: %d %s", resp.StatusCode, b)
	}
	return resp, bufio.NewReader(resp.Body)
}

func TestSSEResumeNoGapNoDuplicateAndCloseAfterTerminal(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.addTask("t1", "running", "run")
	for i := 0; i < 3; i++ {
		ts.store.appendEvent("t1", "progress")
	}
	var ids []int64
	resp, r := ts.openStream("t1", "")
	for len(ids) < 2 { // 读两条后断开
		f, err := readFrame(r)
		if err != nil {
			t.Fatal(err)
		}
		if f.comment {
			continue
		}
		n, _ := strconv.ParseInt(f.id, 10, 64)
		var ev eventJSON
		if err := json.Unmarshal([]byte(f.data), &ev); err != nil || ev.TaskSeq != n || ev.Type != f.event {
			t.Fatalf("帧与数据不一致: %+v", f)
		}
		ids = append(ids, n)
	}
	resp.Body.Close()

	for i := 0; i < 3; i++ {
		ts.store.appendEvent("t1", "progress")
	}
	resp, r = ts.openStream("t1", strconv.FormatInt(ids[len(ids)-1], 10))
	defer resp.Body.Close()
	go func() {
		time.Sleep(30 * time.Millisecond)
		ts.store.appendEvent("t1", "progress")
		ts.store.appendEvent("t1", EventTaskTerminal)
		ts.store.appendEvent("t1", "after_terminal") // 不应发送
	}()
	for {
		f, err := readFrame(r)
		if err == io.EOF {
			break // task_terminal 之后服务端关闭
		}
		if err != nil {
			t.Fatal(err)
		}
		if f.comment {
			continue
		}
		n, _ := strconv.ParseInt(f.id, 10, 64)
		ids = append(ids, n)
	}
	want := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	if !slices.Equal(ids, want) {
		t.Fatalf("收到的事件序号 %v，期望 %v（无缺失无重复，终态后关闭）", ids, want)
	}
}

func TestSSEInvalidCursorAndUnknownTask(t *testing.T) {
	ts := newTestServer(t, nil)
	ts.store.addTask("t1", "running", "run")
	ts.store.appendEvent("t1", "progress")
	ts.store.appendEvent("t1", "progress")
	for _, c := range []string{"abc", "-1", "1.5", "+1", "3", "9999999999999999999"} {
		st, b, _ := ts.do("GET", "/tasks/t1/events", "", map[string]string{"Last-Event-ID": c})
		expect(t, st, b, 400, "invalid_cursor")
	}
	st, b, _ := ts.do("GET", "/tasks/nope/events", "", nil)
	expect(t, st, b, 404, "task_not_found")

	// 游标等于最新事件是合法的：之后的事件照常送达。
	ts.store.appendEvent("t1", EventTaskTerminal)
	resp, r := ts.openStream("t1", "2")
	defer resp.Body.Close()
	f, err := readFrame(r)
	if err != nil || f.id != "3" || f.event != EventTaskTerminal {
		t.Fatalf("frame = %+v, err = %v", f, err)
	}
}

func TestSSEHeartbeat(t *testing.T) {
	ts := newTestServer(t, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	ts.store.addTask("t1", "running", "run")
	resp, r := ts.openStream("t1", "")
	defer resp.Body.Close()
	f, err := readFrame(r)
	if err != nil || !f.comment || f.id != "" {
		t.Fatalf("期望注释行心跳，得到 %+v, %v", f, err)
	}
}

// ---- 访问控制 ----

func TestNonLoopbackRequiresTokenAndHostAllowlist(t *testing.T) {
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "0.0.0.0:8080", AllowedHosts: []string{"box:8080"}}); err == nil {
		t.Fatal("非 loopback 无 token 应拒绝启动")
	}
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: ":8080", Token: "x", AllowedHosts: []string{"box:8080"}}); err != nil {
		t.Fatalf("配置 token 后应可启动: %v", err)
	}
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "10.0.0.1:8080", Token: "x"}); err == nil {
		t.Fatal("非 loopback 无 Host 允许列表应拒绝启动")
	}
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "127.0.0.1:8080", AllowedOrigins: []string{"*"}}); err == nil {
		t.Fatal("通配 Origin 应拒绝")
	}
}

func TestAccessControlAndTokenSecrecy(t *testing.T) {
	const token = "s3cret-token-0123456789abcdef"
	var addr string
	ts := newTestServer(t, func(c *Config) {
		addr = c.ListenAddr
		c.ListenAddr = "0.0.0.0:" + addr[strings.LastIndex(addr, ":")+1:]
		c.Token = token
		c.AllowedHosts = []string{addr}
		c.AllowedOrigins = []string{"http://localhost:5173"}
	})
	ts.store.addTask("t1", "running", "run")
	auth := map[string]string{"Authorization": "Bearer " + token}
	var bodies [][]byte
	call := func(method, path, body string, hdr map[string]string, wantStatus int, wantCode string) http.Header {
		t.Helper()
		st, b, h := ts.do(method, path, body, hdr)
		expect(t, st, b, wantStatus, wantCode)
		bodies = append(bodies, b)
		return h
	}

	call("GET", "/status", "", nil, 401, "unauthorized")
	call("GET", "/tasks/t1", "", map[string]string{"Authorization": "Bearer wrong"}, 401, "unauthorized")
	call("GET", "/tasks/t1", "", map[string]string{"Authorization": token}, 401, "unauthorized")
	call("GET", "/tasks/t1", "", auth, 200, "")
	call("POST", "/tasks", `{"request_id":"r1","spec":{}}`, auth, 201, "")
	call("GET", "/tasks/t1", "", map[string]string{"Authorization": "Bearer " + token, "Host": "evil.example"}, 403, "forbidden_host")
	call("GET", "/tasks/t1", "", map[string]string{"Authorization": "Bearer " + token, "Origin": "http://evil.example"}, 403, "forbidden_origin")
	h := call("GET", "/tasks/t1", "", map[string]string{"Authorization": "Bearer " + token, "Origin": "http://localhost:5173"}, 200, "")
	if got := h.Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("Access-Control-Allow-Origin = %q，期望精确回显允许的 Origin", got)
	}
	// token 放进 URL 也不会被记录（日志只记路径）；错误响应不回显 token。
	call("GET", "/tasks/t1?token="+token, "", map[string]string{"Authorization": "Bearer " + token + "x"}, 401, "unauthorized")

	for _, b := range bodies {
		if bytes.Contains(b, []byte(token)) {
			t.Fatalf("响应中出现 token: %s", b)
		}
	}
	ts.mu.Lock()
	logs := ts.logs.String()
	ts.mu.Unlock()
	if !strings.Contains(logs, `"path":"/tasks/t1"`) {
		t.Fatalf("日志未记录请求: %s", logs)
	}
	if strings.Contains(logs, token) {
		t.Fatalf("日志中出现 token: %s", logs)
	}
}

func TestDefaultLoopbackHosts(t *testing.T) {
	ts := newTestServer(t, nil)
	port := ts.srv.Listener.Addr().String()[strings.LastIndex(ts.srv.Listener.Addr().String(), ":")+1:]
	for host, want := range map[string]int{"localhost:" + port: 200, "127.0.0.1:" + port: 200, "attacker.example:" + port: 403, "localhost:1": 403} {
		st, _, _ := ts.do("GET", "/status", "", map[string]string{"Host": host})
		if st != want {
			t.Errorf("Host %s → %d，期望 %d", host, st, want)
		}
	}
}

func TestLoadToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TokenFile)
	if err := os.WriteFile(path, []byte("abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if tok, err := LoadToken(dir); err != nil || tok != "abc" {
		t.Fatalf("LoadToken = %q, %v", tok, err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadToken(dir); err == nil {
		t.Fatal("0644 的 token 文件应拒绝")
	}
}

// ---- 浏览器安全与静态提供（E26） ----

const wantCSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; " +
	"connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"

func checkSecurityHeaders(t *testing.T, what string, h http.Header) {
	t.Helper()
	for k, want := range map[string]string{"Content-Security-Policy": wantCSP, "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"} {
		if got := h.Get(k); got != want {
			t.Errorf("%s: %s = %q，期望 %q", what, k, got, want)
		}
	}
}

// TestBrowserOriginAndHost：默认 Origin 允许列表等于监听地址（同源）；外部 Origin 的 POST 在到达 Store 之前被拒，
// 错误 Host 被拒；不带 Origin 的请求（CLI）照常。
func TestBrowserOriginAndHost(t *testing.T) {
	ts := newTestServer(t, nil)
	st, b, _ := ts.do("POST", "/tasks", `{"request_id":"evil","spec":{}}`, map[string]string{"Origin": "http://evil.example"})
	expect(t, st, b, 403, "forbidden_origin")
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"null","spec":{}}`, map[string]string{"Origin": "null"})
	expect(t, st, b, 403, "forbidden_origin")
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"host","spec":{}}`, map[string]string{"Host": "evil.example"})
	expect(t, st, b, 403, "forbidden_host")
	ts.store.mu.Lock()
	n := len(ts.store.order)
	ts.store.mu.Unlock()
	if n != 0 {
		t.Fatalf("被拒的请求创建了 %d 个任务", n)
	}
	st, b, h := ts.do("POST", "/tasks", `{"request_id":"same","spec":{}}`, map[string]string{"Origin": ts.srv.URL})
	expect(t, st, b, 201, "")
	checkSecurityHeaders(t, "API 响应", h)
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"cli","spec":{}}`, nil)
	expect(t, st, b, 201, "")

	// TLS 时默认 Origin 的 scheme 为 https。
	ts2 := newTestServer(t, func(c *Config) { c.TLS = true })
	st, b, _ = ts2.do("GET", "/status", "", map[string]string{"Origin": ts2.srv.URL})
	expect(t, st, b, 403, "forbidden_origin")
	st, b, _ = ts2.do("GET", "/status", "", map[string]string{"Origin": "https://" + ts2.srv.Listener.Addr().String()})
	expect(t, st, b, 200, "")
}

// TestNonLoopbackWithoutTLSWarns：非 loopback 监听未启用内置 TLS 时启动警告但不拒绝；启用后不警告。
func TestNonLoopbackWithoutTLSWarns(t *testing.T) {
	for _, tc := range []struct {
		listen string
		tls    bool
		warn   bool
	}{{"0.0.0.0:8080", false, true}, {"0.0.0.0:8080", true, false}, {"127.0.0.1:8080", false, false}} {
		var buf bytes.Buffer
		_, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: tc.listen, Token: "x", TLS: tc.tls,
			AllowedHosts: []string{"box:8080"}, Logger: slog.New(slog.NewJSONHandler(&buf, nil))})
		if err != nil {
			t.Fatalf("%+v: New: %v", tc, err)
		}
		if got := strings.Contains(buf.String(), `"level":"WARN"`); got != tc.warn {
			t.Errorf("%+v: 警告 = %v，期望 %v；日志 %s", tc, got, tc.warn, buf.String())
		}
	}
}

// TestWebDirStaticAndSPAFallback：静态文件与 SPA 回退带安全头且不需要 token；API 路径（/status、/tasks…、
// /events…）从不由静态文件应答，未知 API 端点保持 JSON 404；访问日志不含 token 与 Authorization。
func TestWebDirStaticAndSPAFallback(t *testing.T) {
	const token = "web-s3cret-token-0123456789"
	dir := t.TempDir()
	const index = "<!doctype html><title>agentbox</title>"
	if err := os.MkdirAll(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets", "app.js"), []byte("console.log(1)"), 0o644); err != nil {
		t.Fatal(err)
	}
	ts := newTestServer(t, func(c *Config) { c.WebDir = dir; c.Token = token })
	ts.store.addTask("t1", "running", "run")
	auth := map[string]string{"Authorization": "Bearer " + token}

	for _, tc := range []struct{ path, body, ctype string }{
		{"/", index, "text/html"},
		{"/index.html", index, "text/html"},
		{"/assets/app.js", "console.log(1)", "javascript"},
		{"/tasks-view/t1", index, "text/html"}, // 首段不是 API 前缀
		{"/runs/t1/timeline", index, "text/html"},
		{"/assets", index, "text/html"}, // 目录无 index.html → SPA 回退，不列目录
	} {
		st, b, h := ts.do("GET", tc.path, "", nil)
		if st != 200 || string(b) != tc.body || !strings.Contains(h.Get("Content-Type"), tc.ctype) {
			t.Errorf("GET %s = %d %q (%s)，期望 200 %q", tc.path, st, b, h.Get("Content-Type"), tc.body)
		}
		checkSecurityHeaders(t, "GET "+tc.path, h)
	}
	st, b, _ := ts.do("POST", "/somewhere", "", nil)
	expect(t, st, b, 405, "method_not_allowed")
	// 带扩展名的缺失资源是纯文本 404，不回退到 index.html。
	for _, p := range []string{"/assets/missing.js", "/favicon.ico", "/deep/route/style.css"} {
		st, b, h := ts.do("GET", p, "", nil)
		if st != 404 || strings.Contains(string(b), index) || !strings.HasPrefix(h.Get("Content-Type"), "text/plain") {
			t.Errorf("GET %s = %d %q (%s)，期望纯文本 404", p, st, b, h.Get("Content-Type"))
		}
		checkSecurityHeaders(t, "GET "+p, h)
	}

	// API 路径不被 SPA 吞掉：需要 token，未知端点为 JSON 404。
	for _, tc := range []struct {
		path string
		hdr  map[string]string
		st   int
		code string
	}{
		{"/status", nil, 401, "unauthorized"},
		{"/tasks", nil, 401, "unauthorized"},
		{"/tasks/t1/nope", auth, 404, "not_found"},
		{"/tasks/t1/events/extra", auth, 404, "not_found"},
		{"/events", auth, 404, "not_found"},
		{"/events/t1", auth, 404, "not_found"},
		{"/status/x", auth, 404, "not_found"},
	} {
		st, b, h := ts.do("GET", tc.path, "", tc.hdr)
		expect(t, st, b, tc.st, tc.code)
		if !strings.HasPrefix(h.Get("Content-Type"), "application/json") {
			t.Errorf("GET %s 的 Content-Type = %q，期望 JSON", tc.path, h.Get("Content-Type"))
		}
	}
	st, b, _ = ts.do("GET", "/tasks/t1", "", auth)
	expect(t, st, b, 200, "")

	// 访问日志：记录了请求，但从不出现 token 或 Authorization 头。
	ts.do("GET", "/assets/app.js", "", auth)
	ts.do("GET", "/tasks/t1?access_token="+token, "", map[string]string{"Authorization": "Bearer " + token + "x"})
	ts.mu.Lock()
	logs := ts.logs.String()
	ts.mu.Unlock()
	if !strings.Contains(logs, `"path":"/assets/app.js"`) || !strings.Contains(logs, `"path":"/tasks/t1"`) {
		t.Fatalf("访问日志未记录请求: %s", logs)
	}
	for _, secret := range []string{token, "Bearer", "Authorization", "authorization"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("访问日志中出现 %q: %s", secret, logs)
		}
	}
}
