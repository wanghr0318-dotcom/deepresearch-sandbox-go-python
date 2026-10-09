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
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
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
	var spec struct {
		Topic string `json:"topic"`
	}
	// 错误无关紧要：spec 已校验为 JSON 对象；topic 缺失时视图中为空（与 postgres 的 COALESCE 一致）。
	_ = json.Unmarshal(req.Spec, &spec)
	f.tasks[req.TaskID] = &TaskView{TaskID: req.TaskID, Status: "queued", Desired: "run", ControlVersion: 1,
		Topic: spec.Topic, CreatedAt: time.Date(2026, 10, 6, 8, 30, 0, 0, time.UTC)}
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
	for _, rt := range allRoutes() {
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
	for _, rt := range allRoutes() {
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
	if n := len(spec) - len(sessionOperations); n != 17 {
		t.Errorf("openapi.yaml 解析出 %d 个非会话操作，期望 17（M1 范围加产物最新版本下载，加账号的 5 个操作）", n)
	}
	for _, op := range sessionOperations {
		if _, ok := impl[op]; !ok {
			t.Errorf("会话操作 %s 未路由", op)
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
		// 调用表的 model 来自 journal：chat 调用给出解析后的模型，其他端点（空值）省略该字段。
		Calls: []CallView{{CallID: "k1", Endpoint: "/v1/chat/completions", Model: "kimi-k2.6", State: "completed",
			// 模型降级链：try 带 provider、skipped 与 hedge；单供应商的 try（k2）省略这三个字段。
			Tries: []TryView{{TryNo: 1, State: "settled", Outcome: "retryable", Provider: "primary"},
				{TryNo: 2, State: "settled", Outcome: "ok", Provider: "backup", Skipped: "primary:tried", Hedge: true}}},
			{CallID: "k2", Endpoint: "/v1/search", State: "completed", Tries: []TryView{{TryNo: 1, State: "settled", Outcome: "ok"}}}},
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
	var raw struct {
		Calls []map[string]any `json:"calls"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw.Calls) != 2 || raw.Calls[0]["model"] != "kimi-k2.6" {
		t.Fatalf("chat 调用应带 model：%s", b)
	}
	if _, has := raw.Calls[1]["model"]; has {
		t.Fatalf("非 chat 调用不应有 model 字段：%s", b)
	}
	tries, _ := raw.Calls[0]["tries"].([]any)
	if len(tries) != 2 {
		t.Fatalf("tries：%s", b)
	}
	if t2, _ := tries[1].(map[string]any); t2["provider"] != "backup" || t2["skipped"] != "primary:tried" || t2["hedge"] != true {
		t.Fatalf("try 应带 provider、skipped 与 hedge：%s", b)
	}
	single, _ := raw.Calls[1]["tries"].([]any)
	for _, k := range []string{"provider", "skipped", "hedge"} {
		if _, has := single[0].(map[string]any)[k]; has {
			t.Fatalf("单供应商的 try 不应有 %s：%s", k, b)
		}
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
		if strings.Contains(string(b), `"cache"`) {
			t.Fatalf("未配置缓存指标时不应有 cache 字段：%s", b)
		}
	}
	// 缓存指标（§11.5）经 /status 暴露；诊断模式下同样可读。
	cs := newTestServer(t, func(c *Config) {
		c.CacheMetrics = func() map[string]int64 { return map[string]int64{"hit": 3, "miss": 1} }
	})
	cs.setMode(ModeDiagnostic)
	cst, cb, _ := cs.do("GET", "/status", "", nil)
	expect(t, cst, cb, 200, "")
	var withCache statusResponse
	if err := json.Unmarshal(cb, &withCache); err != nil || withCache.Cache["hit"] != 3 || withCache.Cache["miss"] != 1 {
		t.Fatalf("status 的 cache 字段：%s / %v", cb, err)
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

// ---- 用户账号（M3 Plan 11） ----

// fakeAccounts 是内存中的 Accounts；任务本身仍存于 fakeStore，owner 记在 owners。
type fakeAccounts struct {
	mu       sync.Mutex
	store    *fakeStore
	nextID   int64
	users    map[string]*fakeUser // key（小写用户名）→ 用户
	sessions map[string]int64     // string(idHash) → user id
	owners   map[string]int64     // task_id → owner
	lastReq  CreateTaskRequest    // 最近一次 CreateResearch 的请求
}

type fakeUser struct {
	u    User
	hash string
}

func newFakeAccounts(s *fakeStore) *fakeAccounts {
	return &fakeAccounts{store: s, users: map[string]*fakeUser{}, sessions: map[string]int64{}, owners: map[string]int64{}}
}

func (a *fakeAccounts) CreateUser(_ context.Context, display, key, hash string) (User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.users[key]; ok {
		return User{}, fmt.Errorf("%w: username", persistence.ErrConflict)
	}
	a.nextID++
	u := User{ID: a.nextID, Username: display, Role: "user", CreatedAt: time.Unix(1700000000, 0).UTC()}
	a.users[key] = &fakeUser{u: u, hash: hash}
	return u, nil
}

func (a *fakeAccounts) UserForLogin(_ context.Context, key string) (User, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fu, ok := a.users[key]
	if !ok {
		return User{}, "", persistence.ErrNotFound
	}
	return fu.u, fu.hash, nil
}

func (a *fakeAccounts) CreateSession(_ context.Context, idHash []byte, userID int64, _ time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessions[string(idHash)] = userID
	return nil
}

func (a *fakeAccounts) SessionUser(_ context.Context, idHash []byte) (User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	id, ok := a.sessions[string(idHash)]
	if !ok {
		return User{}, persistence.ErrNotFound
	}
	for _, fu := range a.users {
		if fu.u.ID == id && !fu.u.Disabled {
			return fu.u, nil
		}
	}
	return User{}, persistence.ErrNotFound
}

func (a *fakeAccounts) DeleteSession(_ context.Context, idHash []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, string(idHash))
	return nil
}

func (a *fakeAccounts) SetDisabled(_ context.Context, key string, disabled bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	fu, ok := a.users[key]
	if !ok {
		return persistence.ErrNotFound
	}
	fu.u.Disabled = disabled
	if disabled {
		for k, id := range a.sessions {
			if id == fu.u.ID {
				delete(a.sessions, k)
			}
		}
	}
	return nil
}

func (a *fakeAccounts) ListUsers(context.Context) ([]User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []User
	for _, fu := range a.users {
		out = append(out, fu.u)
	}
	return out, nil
}

func (a *fakeAccounts) CreateResearch(ctx context.Context, userID int64, req CreateTaskRequest) (CreateTaskResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.mu.Lock()
	_, replay := a.store.requests[req.RequestID]
	running := false
	for id, owner := range a.owners {
		if t := a.store.tasks[id]; owner == userID && t != nil && !isTerminal(t.Status) {
			running = true
		}
	}
	a.store.mu.Unlock()
	if !replay && running { // 与 postgres 实现相同：重放先于运行中检查，且错误带包装
		return CreateTaskResult{}, fmt.Errorf("CreateResearch(%s): %w", req.RequestID, ErrUserTaskRunning)
	}
	res, err := a.store.CreateTask(ctx, req)
	if err != nil {
		return CreateTaskResult{}, err
	}
	if replay && a.owners[res.TaskID] != userID {
		return CreateTaskResult{}, fmt.Errorf("%w: request_conflict", persistence.ErrConflict)
	}
	a.owners[res.TaskID] = userID
	a.lastReq = req
	return res, nil
}

func (a *fakeAccounts) TaskOwner(ctx context.Context, taskID string) (int64, error) {
	if _, err := a.store.GetTask(ctx, taskID); err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.owners[taskID], nil
}

func (a *fakeAccounts) ListTasksByOwner(ctx context.Context, ownerID int64, _ string, limit int) ([]TaskView, string, error) {
	all, _, err := a.store.ListTasks(ctx, "", 1000)
	if err != nil {
		return nil, "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []TaskView
	for _, v := range all {
		if a.owners[v.TaskID] == ownerID && len(out) < limit {
			out = append(out, v)
		}
	}
	return out, "", nil
}

// seedUser 直接建立用户与会话（不经 PBKDF2），返回会话 cookie 的明文值。
func (a *fakeAccounts) seedUser(t *testing.T, name string) (User, string) {
	t.Helper()
	u, err := a.CreateUser(context.Background(), name, strings.ToLower(name), "unused")
	if err != nil {
		t.Fatal(err)
	}
	id, idHash, err := account.NewSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CreateSession(context.Background(), idHash, u.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	return u, id
}

func (a *fakeAccounts) own(taskID string, userID int64) {
	a.mu.Lock()
	a.owners[taskID] = userID
	a.mu.Unlock()
}

const adminToken = "admin-s3cret-token-0123456789"

var adminAuth = map[string]string{"Authorization": "Bearer " + adminToken}

func testResearchSpec(topic string) (json.RawMessage, error) {
	return json.Marshal(map[string]string{"topic": topic, "orchestrator_model": "kimi-k3", "worker_model": "kimi-k2.6"})
}

// newAccountServer 启动启用账号的服务（同时配置运维 token）。
func newAccountServer(t *testing.T, mutate func(*Config)) (*testServer, *fakeAccounts) {
	t.Helper()
	var acc *fakeAccounts
	ts := newTestServer(t, func(c *Config) {
		acc = newFakeAccounts(c.Store.(*fakeStore))
		c.Accounts = acc
		c.Token = adminToken
		c.ResearchSpec = testResearchSpec
		if mutate != nil {
			mutate(c)
		}
	})
	return ts, acc
}

func session(id string) map[string]string {
	return map[string]string{"Cookie": SessionCookie + "=" + id}
}

// sessionCookie 返回响应中下发的会话 cookie 及其原始 Set-Cookie 行。
func sessionCookie(t *testing.T, h http.Header) (*http.Cookie, string) {
	t.Helper()
	for _, c := range (&http.Response{Header: h}).Cookies() {
		if c.Name == SessionCookie {
			return c, h.Get("Set-Cookie")
		}
	}
	t.Fatalf("响应没有下发 %s cookie: %v", SessionCookie, h)
	return nil, ""
}

// TestAuthRegisterLoginLogout：注册即登录并下发 HttpOnly/SameSite=Strict cookie（TLS 时带 Secure）；重名 409、
// 弱密码与非法用户名 400；登录失败（密码错、用户不存在、已停用）的响应正文逐字节相同；登出删除会话；
// 访问日志与响应不含会话 ID 与密码。
func TestAuthRegisterLoginLogout(t *testing.T) {
	ts, acc := newAccountServer(t, nil)
	const pw = "Passw0rdX"
	st, b, h := ts.do("POST", "/auth/register", `{"username":"Alice","password":"`+pw+`"}`, nil)
	expect(t, st, b, 201, "")
	var me userJSON
	if err := json.Unmarshal(b, &me); err != nil || me != (userJSON{Username: "Alice", Role: "user"}) {
		t.Fatalf("注册响应 = %s (%v)", b, err)
	}
	c, raw := sessionCookie(t, h)
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.Secure || c.MaxAge != 7*24*3600 {
		t.Fatalf("cookie 属性不符: %s", raw)
	}
	secrets := []string{c.Value, pw}
	st, b, _ = ts.do("GET", "/auth/me", "", session(c.Value))
	expect(t, st, b, 200, "")
	if !bytes.Contains(b, []byte(`"username":"Alice"`)) {
		t.Fatalf("/auth/me = %s", b)
	}

	st, b, _ = ts.do("POST", "/auth/register", `{"username":"alice","password":"An0therPass"}`, nil)
	expect(t, st, b, 409, "username_taken")
	st, b, _ = ts.do("POST", "/auth/register", `{"username":"bob","password":"short"}`, nil)
	expect(t, st, b, 400, "invalid_password")
	// 长度够但只有小写一类：同样 400，正文给出新规则文案。
	st, b, _ = ts.do("POST", "/auth/register", `{"username":"bob","password":"abcdefgh"}`, nil)
	expect(t, st, b, 400, "invalid_password")
	if !strings.Contains(string(b), "密码须为 8–16 位，且至少包含数字、大写字母、小写字母中的两种") {
		t.Fatalf("invalid_password 文案 = %s", b)
	}
	st, b, _ = ts.do("POST", "/auth/register", `{"username":"b b","password":"Passw0rdX"}`, nil)
	expect(t, st, b, 400, "invalid_username")

	// 登录失败：三种原因的正文逐字节相同。
	var fails [][]byte
	for _, body := range []string{
		`{"username":"alice","password":"wrong-password"}`,
		`{"username":"nobody","password":"wrong-password"}`,
	} {
		st, b, h = ts.do("POST", "/auth/login", body, nil)
		expect(t, st, b, 401, "invalid_credentials")
		if h.Get("Set-Cookie") != "" {
			t.Fatalf("登录失败下发了 cookie: %s", h.Get("Set-Cookie"))
		}
		fails = append(fails, b)
	}
	st, b, h = ts.do("POST", "/auth/login", `{"username":"ALICE","password":"`+pw+`"}`, nil)
	expect(t, st, b, 200, "")
	c2, _ := sessionCookie(t, h)
	secrets = append(secrets, c2.Value)
	if err := acc.SetDisabled(context.Background(), "alice", true); err != nil {
		t.Fatal(err)
	}
	st, b, _ = ts.do("POST", "/auth/login", `{"username":"alice","password":"`+pw+`"}`, nil)
	expect(t, st, b, 401, "invalid_credentials")
	fails = append(fails, b)
	for _, f := range fails[1:] {
		if !bytes.Equal(f, fails[0]) {
			t.Fatalf("登录失败的正文不同：%s 与 %s", f, fails[0])
		}
	}
	st, b, _ = ts.do("GET", "/auth/me", "", session(c2.Value))
	expect(t, st, b, 401, "unauthorized") // 停用即吊销会话
	st, b, _ = ts.do("GET", "/auth/me", "", session(c.Value))
	expect(t, st, b, 401, "unauthorized")
	if err := acc.SetDisabled(context.Background(), "alice", false); err != nil {
		t.Fatal(err)
	}
	st, b, h = ts.do("POST", "/auth/login", `{"username":"alice","password":"`+pw+`"}`, nil)
	expect(t, st, b, 200, "")
	c, _ = sessionCookie(t, h)
	secrets = append(secrets, c.Value)

	// 登出：删除会话并下发 Max-Age=0；旧 cookie 失效。
	st, b, h = ts.do("POST", "/auth/logout", "", session(c.Value))
	expect(t, st, b, 204, "")
	if _, raw := sessionCookie(t, h); !strings.Contains(raw, "Max-Age=0") || !strings.Contains(raw, "HttpOnly") {
		t.Fatalf("登出的 Set-Cookie = %s", raw)
	}
	st, b, _ = ts.do("GET", "/auth/me", "", session(c.Value))
	expect(t, st, b, 401, "unauthorized")
	st, b, _ = ts.do("POST", "/auth/logout", "", session(c.Value))
	expect(t, st, b, 401, "unauthorized")

	ts.mu.Lock()
	logs := ts.logs.String()
	ts.mu.Unlock()
	if !strings.Contains(logs, `"path":"/auth/me"`) {
		t.Fatalf("访问日志未记录请求: %s", logs)
	}
	for _, s := range append(secrets, SessionCookie) {
		if strings.Contains(logs, s) {
			t.Fatalf("访问日志中出现会话 ID、cookie 或密码 %q: %s", s, logs)
		}
	}
	for _, f := range fails {
		for _, s := range secrets {
			if bytes.Contains(f, []byte(s)) {
				t.Fatalf("错误正文中出现秘密: %s", f)
			}
		}
	}

	// TLS 监听：cookie 带 Secure。
	tls, _ := newAccountServer(t, func(c *Config) { c.SecureCookies = true })
	st, b, h = tls.do("POST", "/auth/register", `{"username":"carol","password":"`+pw+`"}`, nil)
	expect(t, st, b, 201, "")
	if c, raw := sessionCookie(t, h); !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode {
		t.Fatalf("SecureCookies 时 cookie 属性不符: %s", raw)
	}
}

// TestAuthRateLimit：每 IP 注册 5 次/分钟、登录 10 次/分钟，超出 429 rate_limited（在解析请求体之前判定）；
// 令牌随时间补充。
func TestAuthRateLimit(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(1700000000, 0)
	ts, _ := newAccountServer(t, func(c *Config) {
		c.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	})
	for i := 0; i < 5; i++ {
		st, b, _ := ts.do("POST", "/auth/register", `{"username":"u`+strconv.Itoa(i)+`x","password":"short"}`, nil)
		expect(t, st, b, 400, "invalid_password")
	}
	st, b, _ := ts.do("POST", "/auth/register", `{"username":"u9x","password":"short"}`, nil)
	expect(t, st, b, 429, "rate_limited")
	for i := 0; i < 10; i++ {
		st, b, _ := ts.do("POST", "/auth/login", `{`, nil)
		expect(t, st, b, 400, "invalid_request")
	}
	st, b, _ = ts.do("POST", "/auth/login", `{`, nil)
	expect(t, st, b, 429, "rate_limited")
	// X-Forwarded-For 不改变客户端 IP。
	st, b, _ = ts.do("POST", "/auth/login", `{`, map[string]string{"X-Forwarded-For": "203.0.113.9"})
	expect(t, st, b, 429, "rate_limited")
	mu.Lock()
	now = now.Add(time.Minute)
	mu.Unlock()
	st, b, _ = ts.do("POST", "/auth/register", `{"username":"u9x","password":"short"}`, nil)
	expect(t, st, b, 400, "invalid_password")
}

// TestAuthPrincipalResolution：匿名只能访问 register、login、status；Bearer 优先且无效即 401（不回退到
// cookie）；运维权限不变；只限会话的操作对运维 403；外部 Origin 的登录 403；未启用账号时没有 /auth 与 /research。
func TestAuthPrincipalResolution(t *testing.T) {
	ts, acc := newAccountServer(t, func(c *Config) { c.AllowedOrigins = []string{"https://box.example"} })
	ts.store.addTask("t1", "running", "run")
	_, cookie := acc.seedUser(t, "dave")

	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/tasks", ""}, {"GET", "/tasks/t1", ""}, {"GET", "/tasks/t1/inspect", ""},
		{"POST", "/tasks", `{"request_id":"a","spec":{}}`}, {"GET", "/auth/me", ""}, {"POST", "/auth/logout", ""},
		{"POST", "/research", `{"request_id":"a","topic":"x"}`},
	} {
		st, b, _ := ts.do(tc.method, tc.path, tc.body, nil)
		expect(t, st, b, 401, "unauthorized")
	}
	st, b, _ := ts.do("GET", "/status", "", nil)
	expect(t, st, b, 200, "")

	// Bearer 无效时即便 cookie 有效也 401。
	hdr := session(cookie)
	hdr["Authorization"] = "Bearer wrong"
	st, b, _ = ts.do("GET", "/tasks", "", hdr)
	expect(t, st, b, 401, "unauthorized")
	st, b, _ = ts.do("GET", "/auth/me", "", hdr)
	expect(t, st, b, 401, "unauthorized")
	st, b, _ = ts.do("GET", "/auth/me", "", session(cookie))
	expect(t, st, b, 200, "")

	// 运维 Bearer：全部端点照旧；只限会话的操作 403。
	st, b, _ = ts.do("GET", "/tasks", "", adminAuth)
	expect(t, st, b, 200, "")
	if !bytes.Contains(b, []byte(`"t1"`)) {
		t.Fatalf("运维列表应含无主任务: %s", b)
	}
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"adm","spec":{}}`, adminAuth)
	expect(t, st, b, 201, "")
	st, b, _ = ts.do("POST", "/tasks/t1/pause", `{"request_id":"p"}`, adminAuth)
	expect(t, st, b, 200, "")
	st, b, _ = ts.do("GET", "/tasks/t1/inspect", "", adminAuth)
	expect(t, st, b, 404, "task_not_found") // fake 中没有诊断数据：到达了处理器
	st, b, _ = ts.do("POST", "/research", `{"request_id":"r","topic":"x"}`, adminAuth)
	expect(t, st, b, 403, "forbidden")
	st, b, _ = ts.do("GET", "/auth/me", "", adminAuth)
	expect(t, st, b, 403, "forbidden")

	st, b, _ = ts.do("POST", "/auth/login", `{"username":"dave","password":"whatever-1"}`, map[string]string{"Origin": "https://evil.example"})
	expect(t, st, b, 403, "forbidden_origin")
	st, b, _ = ts.do("POST", "/auth/register", `{"username":"eve","password":"whatever-1"}`, map[string]string{"Origin": "https://evil.example"})
	expect(t, st, b, 403, "forbidden_origin")

	plain := newTestServer(t, nil)
	for _, p := range []string{"/auth/login", "/auth/register", "/research"} {
		st, b, _ = plain.do("POST", p, `{}`, nil)
		expect(t, st, b, 404, "not_found")
	}
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "127.0.0.1:1", Accounts: acc}); err == nil {
		t.Fatal("启用账号而没有 ResearchSpec 应拒绝")
	}
}

// TestUserTaskIsolation：用户只看到自己的任务；他人与无主任务在列表、详情、事件、结果、产物、取消上一律
// 与不存在相同的 404；inspect、pause、resume、POST /tasks 对用户 403。
func TestUserTaskIsolation(t *testing.T) {
	ts, acc := newAccountServer(t, nil)
	a, ca := acc.seedUser(t, "anna")
	b, cb := acc.seedUser(t, "ben")
	for _, id := range []string{"ta", "tb", "t0"} {
		ts.store.addTask(id, "succeeded", "run")
		ts.store.appendEvent(id, EventTaskTerminal)
		ts.store.results[id] = json.RawMessage(`{"summary":"s","outputs":[]}`)
		ts.store.addArtifact(ts.blobs, id, "report.md", "text/markdown", []byte("# r"))
	}
	ts.store.addTask("ta2", "running", "run")
	acc.own("ta", a.ID)
	acc.own("ta2", a.ID)
	acc.own("tb", b.ID)

	st, body, _ := ts.do("GET", "/tasks", "", session(ca))
	expect(t, st, body, 200, "")
	var l taskListJSON
	if err := json.Unmarshal(body, &l); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, x := range l.Tasks {
		ids = append(ids, x.TaskID)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"ta", "ta2"}) {
		t.Fatalf("用户 A 的列表 = %v", ids)
	}

	_, missing, _ := ts.do("GET", "/tasks/nope", "", session(ca))
	for _, id := range []string{"tb", "t0", "nope"} {
		for _, tc := range []struct{ method, path, body string }{
			{"GET", "/tasks/" + id, ""},
			{"GET", "/tasks/" + id + "/events", ""},
			{"GET", "/tasks/" + id + "/result", ""},
			{"GET", "/tasks/" + id + "/artifacts/report.md", ""},
			{"GET", "/tasks/" + id + "/artifacts/report.md/versions/1", ""},
			{"POST", "/tasks/" + id + "/cancel", `{"request_id":"c-` + id + `"}`},
		} {
			st, got, _ := ts.do(tc.method, tc.path, tc.body, session(ca))
			expect(t, st, got, 404, "task_not_found")
			if !bytes.Equal(got, missing) {
				t.Fatalf("%s %s 的 404 正文与不存在的任务不同: %s / %s", tc.method, tc.path, got, missing)
			}
		}
	}
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/tasks/ta", ""}, {"GET", "/tasks/ta/events", ""}, {"GET", "/tasks/ta/result", ""},
		{"GET", "/tasks/ta/artifacts/report.md", ""}, {"GET", "/tasks/ta/artifacts/report.md/versions/1", ""},
		{"POST", "/tasks/ta2/cancel", `{"request_id":"c-own"}`},
	} {
		st, got, _ := ts.do(tc.method, tc.path, tc.body, session(ca))
		expect(t, st, got, 200, "")
	}
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/tasks/ta/inspect", ""}, {"POST", "/tasks/ta2/pause", `{"request_id":"p"}`},
		{"POST", "/tasks/ta2/resume", `{"request_id":"r"}`}, {"POST", "/tasks", `{"request_id":"n","spec":{}}`},
	} {
		st, got, _ := ts.do(tc.method, tc.path, tc.body, session(ca))
		expect(t, st, got, 403, "forbidden")
	}
	// 用户 B 同样看不到 A 的任务。
	st, body, _ = ts.do("GET", "/tasks/ta", "", session(cb))
	expect(t, st, body, 404, "task_not_found")

	ts.mu.Lock()
	logs := ts.logs.String()
	ts.mu.Unlock()
	if strings.Contains(logs, ca) || strings.Contains(logs, cb) {
		t.Fatalf("访问日志中出现会话 ID: %s", logs)
	}
}

// TestResearch：主题去首尾空白后由 server 生成 spec 并以默认 limits 创建、归属用户；同 request_id 重放返回
// 首次结果；已有非终态任务时 409 user_task_running；主题非法 400；他人重用 request_id 为 request_conflict。
func TestResearch(t *testing.T) {
	ts, acc := newAccountServer(t, func(c *Config) {
		c.EffectiveLimits = func(l json.RawMessage) (json.RawMessage, error) {
			if l != nil {
				return nil, errors.New("研究不接受请求中的 limits")
			}
			return json.RawMessage(`{"max_run_time_ms":600000}`), nil
		}
	})
	a, ca := acc.seedUser(t, "anna")
	_, cb := acc.seedUser(t, "ben")

	for _, topic := range []string{`""`, `"   "`, strconv.Quote(strings.Repeat("研", 501))} {
		st, b, _ := ts.do("POST", "/research", `{"request_id":"bad","topic":`+topic+`}`, session(ca))
		expect(t, st, b, 400, "invalid_topic")
	}
	st, b, _ := ts.do("POST", "/research", `{"request_id":"r1","topic":"x","spec":{"model":"gpt"}}`, session(ca))
	expect(t, st, b, 400, "invalid_request") // 用户不能指定 spec
	// 非 UTF-8 请求体（例如以 GBK 发送的"量子"）被拒绝，而不是被 encoding/json 静默替换为 U+FFFD 后存下乱码。
	st, b, _ = ts.do("POST", "/research", "{\"request_id\":\"gbk\",\"topic\":\"\xc1\xbf\xd7\xd3\"}", session(ca))
	expect(t, st, b, 400, "invalid_request")

	st, b, _ = ts.do("POST", "/research", `{"request_id":"r1","topic":"  量子计算  "}`, session(ca))
	expect(t, st, b, 202, "")
	var first CreateTaskResult
	if err := json.Unmarshal(b, &first); err != nil || first.TaskID == "" {
		t.Fatalf("研究响应 = %s", b)
	}
	acc.mu.Lock()
	req, owner := acc.lastReq, acc.owners[first.TaskID]
	acc.mu.Unlock()
	if owner != a.ID {
		t.Fatalf("任务 owner = %d，期望 %d", owner, a.ID)
	}
	if string(req.Spec) != `{"orchestrator_model":"kimi-k3","topic":"量子计算","worker_model":"kimi-k2.6"}` {
		t.Fatalf("研究 spec = %s", req.Spec)
	}
	if string(req.Limits) != `{"max_run_time_ms":600000}` {
		t.Fatalf("研究 limits = %s", req.Limits)
	}
	st, b, _ = ts.do("POST", "/research", `{"request_id":"r1","topic":"  量子计算  "}`, session(ca))
	expect(t, st, b, 202, "")
	if !bytes.Contains(b, []byte(first.TaskID)) {
		t.Fatalf("重放应返回首次结果: %s", b)
	}
	st, b, _ = ts.do("POST", "/research", `{"request_id":"r2","topic":"另一个"}`, session(ca))
	expect(t, st, b, 409, "user_task_running")
	st, b, _ = ts.do("POST", "/research", `{"request_id":"r1","topic":"  量子计算  "}`, session(cb))
	expect(t, st, b, 409, "request_conflict")

	ts.store.mu.Lock()
	ts.store.tasks[first.TaskID].Status = "succeeded"
	ts.store.mu.Unlock()
	st, b, _ = ts.do("POST", "/research", `{"request_id":"r2","topic":"另一个"}`, session(ca))
	expect(t, st, b, 202, "")
	st, b, _ = ts.do("POST", "/research", `{"request_id":"r3","topic":"并行"}`, session(cb))
	expect(t, st, b, 202, "") // 每用户独立
}

// TestAccountsWithoutTokenHaveNoAdmin：启用账号而未配置 token 时，没有凭据的请求是匿名（不按运维处理），
// 任何 Authorization 头都是 401；用户会话照常。未启用账号时行为不变（loopback 无 token 全部开放）。
func TestAccountsWithoutTokenHaveNoAdmin(t *testing.T) {
	ts, acc := newAccountServer(t, func(c *Config) { c.Token = "" })
	ts.store.addTask("t1", "running", "run")
	_, cookie := acc.seedUser(t, "fay")
	for _, tc := range []struct {
		method, path, body string
		hdr                map[string]string
	}{
		{"GET", "/tasks", "", nil},
		{"GET", "/tasks/t1", "", nil},
		{"POST", "/tasks", `{"request_id":"x","spec":{}}`, nil},
		{"GET", "/tasks/t1/inspect", "", nil},
		{"GET", "/tasks", "", map[string]string{"Authorization": "Bearer anything"}},
		{"GET", "/status", "", map[string]string{"Authorization": "Bearer anything"}},
	} {
		st, b, _ := ts.do(tc.method, tc.path, tc.body, tc.hdr)
		expect(t, st, b, 401, "unauthorized")
	}
	st, b, _ := ts.do("GET", "/status", "", nil)
	expect(t, st, b, 200, "")
	st, b, _ = ts.do("GET", "/tasks", "", session(cookie))
	expect(t, st, b, 200, "")
	st, b, _ = ts.do("GET", "/tasks/t1/inspect", "", session(cookie))
	expect(t, st, b, 403, "forbidden")

	plain := newTestServer(t, nil)
	plain.store.addTask("t1", "running", "run")
	st, b, _ = plain.do("GET", "/tasks/t1", "", nil)
	expect(t, st, b, 200, "")
}

// readAllFrames 解析已结束的事件流，返回事件帧（不含心跳）。
func readAllFrames(body []byte) []sseFrame {
	r := bufio.NewReader(bytes.NewReader(body))
	var out []sseFrame
	for {
		f, err := readFrame(r)
		if err != nil {
			return out
		}
		if !f.comment {
			out = append(out, f)
		}
	}
}

// TestUserEventRedaction：用户的事件流只含允许列表中的事件类型，payload 只含允许的字段（无费用、模型、
// 调用、用量等）；被丢弃事件的 task_seq 不出现但游标照常前进，Last-Event-ID 续传（含指向被丢弃事件的
// ID）有效；用户的任务视图不含 current_attempt_id；运维看到的事件流与任务视图不变。
func TestUserEventRedaction(t *testing.T) {
	ts, acc := newAccountServer(t, nil)
	a, ca := acc.seedUser(t, "gil")
	ts.store.addTask("ta", "succeeded", "run")
	ts.store.mu.Lock()
	ts.store.tasks["ta"].CurrentAttemptID = "att_1"
	ts.store.mu.Unlock()
	acc.own("ta", a.ID)
	raw := []struct{ source, typ, payload string }{
		{"host", "task_created", `{}`},
		{"host", "attempt_created", `{"attempt_no":1,"env_id":"env_secret"}`},
		{"worker", "ready", `{"type":"ready","seq":1,"mode":"task"}`},
		{"host", "replay_divergence", `{"call_id":"call_1","cost_micro":5}`},
		{"worker", "checkpoint", `{"type":"checkpoint","seq":2,"state":{"model":"kimi-k3","budget":1}}`},
		{"host", "checkpoint_committed", `{"checkpoint_id":"cp1","commit_seq":1,"step_id":"plan"}`},
		{"worker", "progress", `{"type":"progress","seq":3,"step_id":"search","kind":"step","message":"searching","data":{"cost_micro":9,"model":"kimi-k2.6","call_id":"c"}}`},
		{"host", "artifact_saved", `{"artifact_id":"report.md","version":1,"sha256":"ab"}`},
		{"worker", "error", `{"type":"error","seq":4,"code":"budget_exhausted","message":"tokens"}`},
		{"worker", "result", `{"type":"result","seq":5,"summary":"done","outputs":[],"usage":{"tokens":5}}`},
		{"host", EventTaskTerminal, `{"attempt_id":"att_1","outcome_class":"ok","task_status":"succeeded","status_reason":"ok","platform_killed":false,"output_incomplete":false}`},
	}
	ts.store.mu.Lock()
	ts.store.events["ta"] = nil
	for i, e := range raw {
		ts.store.events["ta"] = append(ts.store.events["ta"], Event{TaskSeq: int64(i + 1), AttemptID: "att_1", Source: e.source, Type: e.typ,
			WorkerSeq: int64(i), Payload: json.RawMessage(e.payload), TS: time.Unix(1700000000, 0).UTC()})
	}
	ts.store.mu.Unlock()

	ids := func(fs []sseFrame) []string {
		var out []string
		for _, f := range fs {
			out = append(out, f.id)
		}
		return out
	}
	st, body, _ := ts.do("GET", "/tasks/ta/events", "", session(ca))
	expect(t, st, body, 200, "")
	frames := readAllFrames(body)
	if got := ids(frames); !slices.Equal(got, []string{"1", "2", "6", "7", "8", "10", "11"}) {
		t.Fatalf("用户事件流的 ID = %v", got)
	}
	for _, bad := range []string{"cost", "model", "call", "usage", "tokens", "budget", "env_", "outcome_class", "worker_seq", "sha256", "data", "replay_divergence"} {
		for _, f := range frames {
			if strings.Contains(f.data, bad) || strings.Contains(f.event, bad) {
				t.Fatalf("用户事件含 %q: %+v", bad, f)
			}
		}
	}
	if !strings.Contains(frames[3].data, `"payload":{"kind":"step","message":"searching","step_id":"search"}`) {
		t.Fatalf("progress 的用户视图 = %s", frames[3].data)
	}
	for lastID, want := range map[string][]string{"6": {"7", "8", "10", "11"}, "4": {"6", "7", "8", "10", "11"}} {
		hdr := session(ca)
		hdr["Last-Event-ID"] = lastID
		st, body, _ = ts.do("GET", "/tasks/ta/events", "", hdr)
		expect(t, st, body, 200, "")
		if got := ids(readAllFrames(body)); !slices.Equal(got, want) {
			t.Fatalf("Last-Event-ID %s 续传 = %v，期望 %v", lastID, got, want)
		}
	}
	st, body, _ = ts.do("GET", "/tasks/ta", "", session(ca))
	expect(t, st, body, 200, "")
	if strings.Contains(string(body), "att_1") {
		t.Fatalf("用户的任务视图含 current_attempt_id: %s", body)
	}
	st, body, _ = ts.do("GET", "/tasks", "", session(ca))
	expect(t, st, body, 200, "")
	if strings.Contains(string(body), "att_1") {
		t.Fatalf("用户的任务列表含 current_attempt_id: %s", body)
	}

	// 运维：全部事件，payload 原样。
	st, body, _ = ts.do("GET", "/tasks/ta/events", "", adminAuth)
	expect(t, st, body, 200, "")
	all := readAllFrames(body)
	if len(all) != len(raw) || !strings.Contains(all[3].data, `"cost_micro":5`) || !strings.Contains(all[6].data, `"worker_seq":6`) {
		t.Fatalf("运维事件流被改动: %+v", all)
	}
	st, body, _ = ts.do("GET", "/tasks/ta", "", adminAuth)
	expect(t, st, body, 200, "")
	if !strings.Contains(string(body), `"current_attempt_id":"att_1"`) {
		t.Fatalf("运维的任务视图 = %s", body)
	}
}

// TestTaskViewTopicAndCreatedAt：任务视图带 spec 中的 topic 与 created_at（RFC 3339）；用户与运维都能看到，
// 用户视图仍不含 attempt、费用或模型；spec 没有 topic 时省略该字段。
func TestTaskViewTopicAndCreatedAt(t *testing.T) {
	ts, acc := newAccountServer(t, nil)
	_, ca := acc.seedUser(t, "hana")
	st, b, _ := ts.do("POST", "/research", `{"request_id":"rt1","topic":"  钠离子电池  "}`, session(ca))
	expect(t, st, b, 202, "")
	var res CreateTaskResult
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("研究响应 = %s: %v", b, err)
	}
	ts.store.mu.Lock()
	ts.store.tasks[res.TaskID].CurrentAttemptID = "att_9"
	ts.store.mu.Unlock()

	const topic, created = `"topic":"钠离子电池"`, `"created_at":"2026-10-06T08:30:00Z"`
	for _, path := range []string{"/tasks/" + res.TaskID, "/tasks"} {
		st, body, _ := ts.do("GET", path, "", session(ca))
		expect(t, st, body, 200, "")
		s := string(body)
		if !strings.Contains(s, topic) || !strings.Contains(s, created) {
			t.Fatalf("用户 %s 缺 topic/created_at: %s", path, s)
		}
		for _, bad := range []string{"att_9", "current_attempt_id", "cost", "model", "kimi", "budget"} {
			if strings.Contains(s, bad) {
				t.Fatalf("用户 %s 含 %q: %s", path, bad, s)
			}
		}
		st, body, _ = ts.do("GET", path, "", adminAuth)
		expect(t, st, body, 200, "")
		if s := string(body); !strings.Contains(s, topic) || !strings.Contains(s, created) || !strings.Contains(s, `"current_attempt_id":"att_9"`) {
			t.Fatalf("运维 %s = %s", path, s)
		}
	}

	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"rt2","spec":{"app":"other"}}`, adminAuth)
	expect(t, st, b, 201, "")
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("创建响应 = %s: %v", b, err)
	}
	st, b, _ = ts.do("GET", "/tasks/"+res.TaskID, "", adminAuth)
	expect(t, st, b, 200, "")
	if strings.Contains(string(b), `"topic"`) || !strings.Contains(string(b), created) {
		t.Fatalf("spec 无 topic 的任务视图 = %s", b)
	}
}

// ---- 会话契约（M4 Plan 12 Task 1）----

// sessionOperations 是 openapi.yaml 中的会话操作全集（Task 8 全部路由，状态码由 TestHandlersMatchOpenAPI 比较）。
var sessionOperations = []string{
	"POST /sessions", "GET /sessions",
	"GET /sessions/{id}", "PATCH /sessions/{id}", "DELETE /sessions/{id}",
	"POST /sessions/{id}/wake", "POST /sessions/{id}/messages",
	"GET /sessions/{id}/turns", "GET /sessions/{id}/events",
	"POST /turns/{id}/stop", "POST /turns/{id}/continue", "POST /turns/{id}/finish",
	"POST /turns/{id}/answer", "POST /turns/{id}/restore",
	"GET /turns/{id}/raw/{sha256}",
}

func sessionRec(source, typ, payload string) SessionEventRecord {
	r := SessionEventRecord{SessionSeq: 7, Source: source, Type: typ, Payload: json.RawMessage(payload),
		TS: time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)}
	if source != "session" {
		r.TaskID, r.TaskSeq, r.AttemptID = "t1", 3, "att_1"
	}
	return r
}

func eventData(t *testing.T, ev SessionEvent) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(ev.Data, &m); err != nil {
		t.Fatalf("data 不是 JSON 对象: %s: %v", ev.Data, err)
	}
	return m
}

// hasKey 递归报告 JSON 值中是否出现名为 key 的对象键。
func hasKey(v any, key string) bool {
	switch x := v.(type) {
	case map[string]any:
		for k, c := range x {
			if k == key || hasKey(c, key) {
				return true
			}
		}
	case []any:
		for _, c := range x {
			if hasKey(c, key) {
				return true
			}
		}
	}
	return false
}

func TestSessionEventMapping(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	t.Run("工具结果去掉费用、模型与内部 ID，保留预览与响应引用", func(t *testing.T) {
		payload := `{"step_id":"orch","kind":"tool_result","message":"搜索完成","data":{` +
			`"tool_call_id":"tc_1","call_id":"root/orch/search/1","cost_micro":12,"tool":"web_search","ok":true,` +
			`"preview":{"results":[{"title":"A","url":"https://a.example/x","site":"a.example","snippet":"s","cost_micro":1}]},` +
			`"raw":{"request":{"model":"kimi-k3","query":"固态电池","usage":{"total_tokens":3},"messages":[{"role":"user","content":"q"}]},` +
			`"response_ref":"` + sha + `","call_id":"root/orch/search/1","inline":{"x":1}}}}`
		r := sessionRec("worker", "progress", payload)
		ev, ok := ToSessionEvent(r, false)
		if !ok || ev.Type != SEvToolResult || ev.Seq != 7 || ev.TurnID != "t1" || ev.Internal != nil || !ev.TS.Equal(r.TS) {
			t.Fatalf("用户视图 = %+v, %v", ev, ok)
		}
		d := eventData(t, ev)
		for _, k := range []string{"call_id", "cost_micro", "model", "usage", "attempt_id", "inline"} {
			if hasKey(d, k) {
				t.Errorf("用户 data 含 %q: %s", k, ev.Data)
			}
		}
		if d["step_id"] != "orch" || d["tool_call_id"] != "tc_1" || d["tool"] != "web_search" || d["ok"] != true {
			t.Errorf("用户 data = %s", ev.Data)
		}
		raw, _ := d["raw"].(map[string]any)
		req, _ := raw["request"].(map[string]any)
		if raw["response_ref"] != sha || req["query"] != "固态电池" || req["messages"] == nil {
			t.Errorf("raw = %s", ev.Data)
		}
		res, _ := d["preview"].(map[string]any)["results"].([]any)
		if len(res) != 1 || res[0].(map[string]any)["url"] != "https://a.example/x" {
			t.Errorf("preview = %s", ev.Data)
		}

		adm, ok := ToSessionEvent(r, true)
		if !ok || adm.Type != SEvToolResult || adm.Internal == nil || !bytes.Equal(adm.Internal.Payload, r.Payload) ||
			adm.Internal.Source != "worker" || adm.Internal.Type != "progress" || adm.Internal.AttemptID != "att_1" || adm.Internal.TaskSeq != 3 {
			t.Fatalf("运维视图 = %+v, %v", adm, ok)
		}
		if !bytes.Equal(adm.Data, ev.Data) {
			t.Errorf("运维 data = %s，用户 data = %s", adm.Data, ev.Data)
		}
	})

	t.Run("未知进度种类与内部事件对用户不可见，运维视为 internal", func(t *testing.T) {
		for _, r := range []SessionEventRecord{
			sessionRec("worker", "progress", `{"step_id":"s","kind":"debug","message":"x","data":{}}`),
			sessionRec("worker", "checkpoint", `{"checkpoint_id":"c1","step_id":"s"}`),
			sessionRec("host", "replay_divergence", `{"call_id":"c"}`),
			sessionRec("host", "control_accepted", `{"desired":"pause","control_version":2}`),
		} {
			if ev, ok := ToSessionEvent(r, false); ok {
				t.Errorf("%s/%s 对用户可见: %+v", r.Source, r.Type, ev)
			}
			ev, ok := ToSessionEvent(r, true)
			if !ok || ev.Type != SEvInternal || ev.Internal == nil || !bytes.Equal(ev.Internal.Payload, r.Payload) || ev.Seq != 7 {
				t.Errorf("%s/%s 运维视图 = %+v, %v", r.Source, r.Type, ev, ok)
			}
		}
	})

	t.Run("turn 状态映射与面向用户的失败文案", func(t *testing.T) {
		cases := []struct {
			typ, payload, status, reason, msg string
		}{
			{"task_terminal", `{"attempt_id":"att_1","task_status":"paused","status_reason":"awaiting_input","outcome_class":"x"}`, "awaiting_input", "", ""},
			{"task_terminal", `{"attempt_id":"att_1","task_status":"failed","status_reason":"model_unavailable"}`, "failed", "model_unavailable", "模型服务暂时不可用，请重试"},
			{"control_applied", `{"control_version":2,"status":"pausing"}`, "stopping", "", ""},
			{"control_applied", `{"control_version":3,"status":"cancelling"}`, "stopping", "", ""},
			{"attempt_ended", `{"attempt_id":"att_1","task_status":"queued","status_reason":"worker_crash"}`, "queued", "worker_crash", ""},
			{"task_terminal", `{"attempt_id":"att_1","task_status":"succeeded","status_reason":"ok"}`, "succeeded", "ok", ""},
		}
		for _, c := range cases {
			ev, ok := ToSessionEvent(sessionRec("host", c.typ, c.payload), false)
			if !ok || ev.Type != SEvTurnStatus {
				t.Fatalf("%s %s → %+v, %v", c.typ, c.payload, ev, ok)
			}
			d := eventData(t, ev)
			want := map[string]any{"status": c.status}
			if c.reason != "" {
				want["reason"] = c.reason
			}
			if c.msg != "" {
				want["user_message"] = c.msg
			}
			if !mapsEqualJSON(d, want) {
				t.Errorf("%s %s → data %s，期望 %v", c.typ, c.payload, ev.Data, want)
			}
		}
		if s := UserTurnStatus("paused", "paused"); s != "paused" {
			t.Errorf("paused/paused → %q", s)
		}
	})

	t.Run("会话生命周期：过渡态不可见，驱逐可见", func(t *testing.T) {
		if ev, ok := ToSessionEvent(sessionRec("session", SEvSessionState, `{"state":"quiescing"}`), false); ok {
			t.Errorf("quiescing 对用户可见: %+v", ev)
		}
		ev, ok := ToSessionEvent(sessionRec("session", SEvSessionState, `{"state":"evicted","user_message":"会话暂时无法恢复","last_error":"boom"}`), false)
		if !ok || ev.Type != SEvSessionState || ev.TurnID != "" {
			t.Fatalf("evicted → %+v, %v", ev, ok)
		}
		if d := eventData(t, ev); !mapsEqualJSON(d, map[string]any{"state": "evicted", "user_message": "会话暂时无法恢复"}) {
			t.Errorf("evicted data = %s", ev.Data)
		}
		for in, want := range map[string]string{"creating": "idle", "quiescing": "idle", "evicting": "frozen", "frozen": "frozen", "closing": "closing", "running": "running"} {
			if got := UserSessionState(in); got != want {
				t.Errorf("UserSessionState(%q) = %q，期望 %q", in, got, want)
			}
		}
	})

	t.Run("turn 创建与结果只保留契约字段", func(t *testing.T) {
		ev, ok := ToSessionEvent(sessionRec("host", "task_created",
			`{"turn_index":2,"text":"你好","deep_research":true,"restored_from_turn_id":"t0","spec":{"model":"kimi-k3"},"owner_user_id":5}`), false)
		if !ok || ev.Type != SEvTurnCreated ||
			!mapsEqualJSON(eventData(t, ev), map[string]any{"turn_index": 2.0, "text": "你好", "deep_research": true, "restored_from_turn_id": "t0"}) {
			t.Errorf("task_created → %+v %s, %v", ev, ev.Data, ok)
		}
		ev, ok = ToSessionEvent(sessionRec("worker", "result",
			`{"attempt_id":"att_1","summary":"答复","outputs":["report"],"session_state":{"state":{"k":1}}}`), false)
		if !ok || ev.Type != SEvTurnResult ||
			!mapsEqualJSON(eventData(t, ev), map[string]any{"summary": "答复", "outputs": []any{"report"}}) {
			t.Errorf("result → %+v %s, %v", ev, ev.Data, ok)
		}
	})
}

func mapsEqualJSON(a, b map[string]any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

func TestRedactRaw(t *testing.T) {
	in := `{"id":"chatcmpl-1","object":"chat.completion","model":"kimi-k3","system_fingerprint":"fp",` +
		`"choices":[{"index":0,"message":{"role":"assistant","content":"<b>答</b> & 1.0","id":"keep-nested"}}],` +
		`"usage":{"prompt_tokens":1},"cost_micro":9,"meta":{"call_id":"c","upstream_request_id":"u","n":12345678901234567890}}`
	var got map[string]any
	if err := json.Unmarshal(RedactRaw([]byte(in)), &got); err != nil {
		t.Fatalf("RedactRaw 输出不是 JSON: %v", err)
	}
	for _, k := range []string{"model", "system_fingerprint", "usage", "cost_micro", "call_id", "upstream_request_id"} {
		if hasKey(got, k) {
			t.Errorf("脱敏后仍含 %q: %v", k, got)
		}
	}
	if _, ok := got["id"]; ok {
		t.Errorf("顶层 id 未去掉: %v", got)
	}
	msg := got["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "<b>答</b> & 1.0" || msg["id"] != "keep-nested" || got["object"] != "chat.completion" {
		t.Errorf("正文被改动: %v", got)
	}
	if out := string(RedactRaw([]byte(in))); !strings.Contains(out, "12345678901234567890") || !strings.Contains(out, "<b>") {
		t.Errorf("数字精度或字符被改写: %s", out)
	}
	for _, b := range [][]byte{[]byte("not json {"), {0xff, 0x00, 0x01}, nil} {
		if got := RedactRaw(b); !bytes.Equal(got, b) {
			t.Errorf("非 JSON 被改动: %q → %q", b, got)
		}
	}
}

// openAPISchemaBlock 返回 components.schemas 下名为 name 的模式的各行（不含首行）。
func openAPISchemaBlock(t *testing.T, lines []string, name string) []string {
	t.Helper()
	inSchemas := false
	for i, line := range lines {
		if line == "  schemas:" {
			inSchemas = true
			continue
		}
		if !inSchemas || line != "    "+name+":" {
			continue
		}
		var out []string
		for _, l := range lines[i+1:] {
			if strings.TrimSpace(l) != "" && len(l)-len(strings.TrimLeft(l, " ")) <= 4 {
				break
			}
			out = append(out, l)
		}
		return out
	}
	t.Fatalf("components.schemas 缺少 %s", name)
	return nil
}

func flowEnum(t *testing.T, block []string) []string {
	t.Helper()
	for _, l := range block {
		if _, rest, ok := strings.Cut(l, "enum: ["); ok {
			items, _, _ := strings.Cut(rest, "]")
			var out []string
			for _, it := range strings.Split(items, ",") {
				out = append(out, strings.TrimSpace(it))
			}
			sort.Strings(out)
			return out
		}
	}
	t.Fatal("模式中没有 enum")
	return nil
}

// keysAt 返回 block 中 header 行（缩进 indent）之下、缩进 indent+2 的键名。
func keysAt(block []string, header string, indent int) []string {
	var out []string
	in := false
	for _, l := range block {
		n := len(l) - len(strings.TrimLeft(l, " "))
		switch {
		case strings.TrimSpace(l) == "":
		case n == indent && strings.TrimSpace(l) == header:
			in = true
		case in && n <= indent:
			in = false
		case in && n == indent+2:
			k, _, _ := strings.Cut(strings.TrimSpace(l), ":")
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func TestOpenAPISessionContract(t *testing.T) {
	spec := openAPIOperations(t)
	for _, op := range sessionOperations {
		st, ok := spec[op]
		if !ok {
			t.Errorf("openapi.yaml 缺少 %s", op)
			continue
		}
		need := []int{401, 403, 500, 503}
		if op != "POST /sessions" && op != "GET /sessions" {
			need = append(need, 404) // 他人、无主与不存在的会话或 turn 回答相同的 404
		}
		for _, s := range need {
			if !slices.Contains(st, s) {
				t.Errorf("%s 未声明 %d", op, s)
			}
		}
	}

	b, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")

	types := slices.Sorted(slices.Values(SessionEventTypes))
	if got := flowEnum(t, openAPISchemaBlock(t, lines, "SessionEventType")); !slices.Equal(got, types) {
		t.Errorf("SessionEventType 枚举 %v，Go 常量 %v", got, types)
	}
	if got := keysAt(openAPISchemaBlock(t, lines, "SessionEvent"), "mapping:", 8); !slices.Equal(got, types) {
		t.Errorf("SessionEvent discriminator mapping %v，期望 %v", got, types)
	}
	// 每种对外事件的 data 模式（<CamelType>Data）的字段与 Go 的允许列表一致。
	for _, typ := range types {
		if typ == SEvInternal {
			continue
		}
		var camel strings.Builder
		for _, part := range strings.Split(typ, "_") {
			camel.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
		got := keysAt(openAPISchemaBlock(t, lines, camel.String()+"Data"), "properties:", 6)
		if want := slices.Sorted(slices.Values(sessionEventDataFields[typ])); !slices.Equal(got, want) {
			t.Errorf("%sData 字段 %v，允许列表 %v", camel.String(), got, want)
		}
	}
	for _, st := range []string{"Session", "Turn"} {
		block := openAPISchemaBlock(t, lines, st)
		var enum []string
		for i, l := range block {
			if strings.TrimSpace(l) == "state:" || strings.TrimSpace(l) == "status:" || strings.HasPrefix(strings.TrimSpace(l), "state: {") || strings.HasPrefix(strings.TrimSpace(l), "status: {") {
				enum = flowEnum(t, block[i:i+1])
				break
			}
		}
		want := slices.Sorted(slices.Values(userSessionStates))
		if st == "Turn" {
			want = slices.Sorted(slices.Values(userTurnStatuses))
		}
		if !slices.Equal(enum, want) {
			t.Errorf("%s 状态枚举 %v，期望 %v", st, enum, want)
		}
	}
	codes := flowEnum(t, openAPISchemaBlock(t, lines, "SessionErrorCode"))
	for _, c := range []string{"session_not_found", "turn_not_found", "not_found", "session_closed", "turn_in_progress",
		"invalid_turn_state", "not_restorable", "invalid_text", "invalid_title", "invalid_answers", "sessions_unavailable",
		"user_task_running", "request_conflict", "invalid_cursor", "invalid_request", "forbidden"} {
		if !slices.Contains(codes, c) {
			t.Errorf("SessionErrorCode 缺少 %s", c)
		}
	}
}

// ---- 会话端点（M4 Plan 12 Task 8）----

// fakeSessions 是内存中的 Sessions，按 postgres 实现的规则模拟 D5、turn_in_progress、关闭与 turn 控制的状态要求。
type fakeSessions struct {
	mu         sync.Mutex
	order      []string // 创建顺序
	sessions   map[string]*SessionView
	usernames  map[int64]string
	turns      map[string]*fakeTurn
	events     map[string][]SessionEventRecord // session_id → 按 session_seq 升序
	authorized map[string]bool                 // task_id/sha
	requests   map[string]fakeSessionRequest
	// failControl、failTurn 非空时，下一次 TurnControl、CreateTurn 返回该错误。
	failControl, failTurn error
	lastTurn              CreateTurnRequest
	lastControl           TurnControlRequest
}

type fakeTurn struct {
	session string
	view    TurnView
	version int64
}

type fakeSessionRequest struct {
	kind string
	hash []byte
	resp any
}

func newFakeSessions() *fakeSessions {
	return &fakeSessions{sessions: map[string]*SessionView{}, usernames: map[int64]string{}, turns: map[string]*fakeTurn{},
		events: map[string][]SessionEventRecord{}, authorized: map[string]bool{}, requests: map[string]fakeSessionRequest{}}
}

// replay 按 request_id 查找已提交的请求：同 kind 同 hash 返回原结果，否则 ErrConflict。
func (f *fakeSessions) replay(requestID, kind string, hash []byte) (any, bool, error) {
	rec, ok := f.requests[requestID]
	if !ok {
		return nil, false, nil
	}
	if rec.kind != kind || !bytes.Equal(rec.hash, hash) {
		return nil, false, fmt.Errorf("%w: request_conflict", persistence.ErrConflict)
	}
	return rec.resp, true, nil
}

func (f *fakeSessions) addSession(id string, owner int64, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ts := time.Date(2026, 10, 6, 9, 0, len(f.order), 0, time.UTC)
	f.sessions[id] = &SessionView{SessionID: id, State: state, OwnerUserID: owner, CreatedAt: ts, LastActiveAt: ts}
	f.order = append(f.order, id)
}

func (f *fakeSessions) addTurn(sessionID, turnID, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := int64(0)
	for _, t := range f.turns {
		if t.session == sessionID {
			n++
		}
	}
	f.turns[turnID] = &fakeTurn{session: sessionID, view: TurnView{TurnID: turnID, TurnIndex: n, Text: "问题 " + turnID, Status: status,
		ToolCallLimit: 30, CreatedAt: time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)}}
}

func (f *fakeSessions) setTurnStatus(turnID, status string) {
	f.mu.Lock()
	f.turns[turnID].view.Status = status
	f.mu.Unlock()
}

func (f *fakeSessions) setState(sessionID, state string) {
	f.mu.Lock()
	f.sessions[sessionID].State = state
	f.mu.Unlock()
}

// appendRecord 追加一条会话记录，session_seq 取下一个序号。
func (f *fakeSessions) appendRecord(sessionID string, r SessionEventRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r.SessionSeq = int64(len(f.events[sessionID]) + 1)
	r.TS = time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	f.events[sessionID] = append(f.events[sessionID], r)
}

func (f *fakeSessions) CreateSession(_ context.Context, req CreateSessionRequest) (SessionView, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if resp, ok, err := f.replay(req.RequestID, "create_session", req.BodyHash); err != nil || ok {
		if err != nil {
			return SessionView{}, false, err
		}
		v := *f.sessions[resp.(string)]
		v.Owner, v.InternalState = "", ""
		return v, true, nil
	}
	ts := time.Date(2026, 10, 6, 9, 0, len(f.order), 0, time.UTC)
	f.sessions[req.SessionID] = &SessionView{SessionID: req.SessionID, Title: req.Title, State: "idle", OwnerUserID: req.OwnerUserID,
		CreatedAt: ts, LastActiveAt: ts}
	f.order = append(f.order, req.SessionID)
	f.requests[req.RequestID] = fakeSessionRequest{kind: "create_session", hash: req.BodyHash, resp: req.SessionID}
	return *f.sessions[req.SessionID], false, nil
}

func (f *fakeSessions) ListSessions(_ context.Context, ownerID int64, after string, limit int) ([]SessionView, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []SessionView
	started := after == ""
	for i := len(f.order) - 1; i >= 0; i-- {
		v := *f.sessions[f.order[i]]
		if !started {
			started = v.SessionID == after
			continue
		}
		if ownerID > 0 && (v.OwnerUserID != ownerID || v.State == "closed") {
			continue
		}
		if ownerID == 0 {
			v.Owner, v.InternalState = f.usernames[v.OwnerUserID], "internal_"+v.State
		}
		out = append(out, v)
		if len(out) == limit {
			return out, v.SessionID, nil
		}
	}
	return out, "", nil
}

func (f *fakeSessions) GetSession(_ context.Context, sessionID string) (SessionView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.sessions[sessionID]
	if !ok {
		return SessionView{}, fmt.Errorf("%w: 会话 %s", persistence.ErrNotFound, sessionID)
	}
	return *v, nil
}

func (f *fakeSessions) openSession(sessionID string) (*SessionView, error) {
	v, ok := f.sessions[sessionID]
	switch {
	case !ok:
		return nil, persistence.ErrNotFound
	case v.State == "closing" || v.State == "closed":
		return v, fmt.Errorf("%w: %s", ErrSessionClosed, sessionID)
	}
	return v, nil
}

func (f *fakeSessions) RenameSession(_ context.Context, sessionID, title string) (SessionView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, err := f.openSession(sessionID)
	if err != nil {
		return SessionView{}, err
	}
	v.Title = title
	return *v, nil
}

func (f *fakeSessions) CloseSession(_ context.Context, sessionID string) (SessionView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.sessions[sessionID]
	if !ok {
		return SessionView{}, persistence.ErrNotFound
	}
	if v.State != "closed" {
		v.State = "closing"
	}
	return *v, nil
}

func (f *fakeSessions) WakeSession(_ context.Context, requestID string, bodyHash []byte, sessionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok, err := f.replay(requestID, "wake", bodyHash); err != nil || ok {
		return err
	}
	if _, err := f.openSession(sessionID); err != nil {
		return err
	}
	f.requests[requestID] = fakeSessionRequest{kind: "wake", hash: bodyHash, resp: sessionID}
	return nil
}

func (f *fakeSessions) CreateTurn(_ context.Context, req CreateTurnRequest) (CreateTurnResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if resp, ok, err := f.replay(req.RequestID, "create_turn", req.BodyHash); err != nil || ok {
		if err != nil {
			return CreateTurnResult{}, err
		}
		res := resp.(CreateTurnResult)
		res.Replayed = true
		return res, nil
	}
	if err := f.failTurn; err != nil {
		f.failTurn = nil
		return CreateTurnResult{}, err
	}
	s, err := f.openSession(req.SessionID)
	if err != nil {
		return CreateTurnResult{}, err
	}
	if !req.Operator && s.OwnerUserID != req.OwnerUserID {
		return CreateTurnResult{}, persistence.ErrNotFound
	}
	var res CreateTurnResult
	var n int64
	for _, t := range f.turns {
		if t.session != req.SessionID {
			continue
		}
		n++
		switch t.view.Status {
		case "queued", "running", "stopping":
			return CreateTurnResult{}, fmt.Errorf("%w: %s", ErrTurnInProgress, t.view.TurnID)
		case "paused", "awaiting_input":
			res.SupersededTurnID = t.view.TurnID
		}
	}
	if req.RestoredFromTaskID != "" {
		if src := f.turns[req.RestoredFromTaskID]; src == nil || src.view.Status != "cancelled" {
			return CreateTurnResult{}, fmt.Errorf("%w: %s", ErrNotRestorable, req.RestoredFromTaskID)
		}
	}
	if res.SupersededTurnID != "" {
		t := f.turns[res.SupersededTurnID]
		t.view.Status, t.view.StatusReason, t.view.Restorable = "cancelled", "superseded", true
	}
	f.turns[req.TaskID] = &fakeTurn{session: req.SessionID, view: TurnView{TurnID: req.TaskID, TurnIndex: n, Text: req.Text,
		DeepResearch: req.DeepResearch, Status: "queued", RestoredFromTurnID: req.RestoredFromTaskID, ToolCallLimit: 30}}
	res.TurnID, res.TurnIndex = req.TaskID, n
	f.lastTurn = req
	f.requests[req.RequestID] = fakeSessionRequest{kind: "create_turn", hash: req.BodyHash, resp: res}
	return res, nil
}

func (f *fakeSessions) ListTurns(_ context.Context, sessionID string, afterIndex int64, limit int) ([]TurnView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sessions[sessionID]; !ok {
		return nil, persistence.ErrNotFound
	}
	var out []TurnView
	for _, t := range f.turns {
		if t.session == sessionID && t.view.TurnIndex > afterIndex {
			out = append(out, t.view)
		}
	}
	slices.SortFunc(out, func(a, b TurnView) int { return int(a.TurnIndex - b.TurnIndex) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeSessions) TurnSession(_ context.Context, taskID string) (string, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.turns[taskID]
	if !ok {
		return "", 0, fmt.Errorf("%w: turn %s", persistence.ErrNotFound, taskID)
	}
	return t.session, f.sessions[t.session].OwnerUserID, nil
}

func (f *fakeSessions) TurnControl(_ context.Context, req TurnControlRequest) (ControlResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if resp, ok, err := f.replay(req.RequestID, "turn_control", req.BodyHash); err != nil || ok {
		if err != nil {
			return ControlResult{}, err
		}
		res := resp.(ControlResult)
		res.Replayed = true
		return res, nil
	}
	if err := f.failControl; err != nil {
		f.failControl = nil
		return ControlResult{}, err
	}
	t, ok := f.turns[req.TaskID]
	if !ok {
		return ControlResult{}, persistence.ErrNotFound
	}
	need := map[string][]string{"stop": {"queued", "running"}, "continue": {"paused"}, "finish": {"paused"}, "answer": {"awaiting_input"}}[req.Action]
	if !slices.Contains(need, t.view.Status) {
		return ControlResult{}, fmt.Errorf("%w: %s", ErrInvalidTurnState, t.view.Status)
	}
	t.version++
	res := ControlResult{TaskID: req.TaskID, ControlVersion: t.version}
	f.lastControl = req
	f.requests[req.RequestID] = fakeSessionRequest{kind: "turn_control", hash: req.BodyHash, resp: res}
	return res, nil
}

func (f *fakeSessions) ListSessionEvents(_ context.Context, sessionID string, afterSeq int64, limit int) ([]SessionEventRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []SessionEventRecord
	for _, r := range f.events[sessionID] {
		if r.SessionSeq > afterSeq && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeSessions) TurnBlobAuthorized(_ context.Context, taskID, sha string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.authorized[taskID+"/"+sha], nil
}

// testTurnSpec 是测试用的 TurnSpec：spec 带文本与 deep_research，limits 为 max_tool_calls = 30。
func testTurnSpec(text string, deep bool) (json.RawMessage, json.RawMessage, error) {
	spec, err := json.Marshal(map[string]any{"kind": "turn", "text": text, "deep_research": deep, "orchestrator_model": "kimi-k3"})
	return spec, json.RawMessage(`{ "max_tool_calls": 30 }`), err
}

// newSessionServer 启动启用账号与会话的服务；会话 ID 依次为 s1、s2…。
func newSessionServer(t *testing.T, mutate func(*Config)) (*testServer, *fakeAccounts, *fakeSessions) {
	t.Helper()
	fs := newFakeSessions()
	var mu sync.Mutex
	n := 0
	ts, acc := newAccountServer(t, func(c *Config) {
		c.Sessions = fs
		c.TurnSpec = testTurnSpec
		c.NewSessionID = func() string { mu.Lock(); defer mu.Unlock(); n++; return "s" + strconv.Itoa(n) }
		if mutate != nil {
			mutate(c)
		}
	})
	return ts, acc, fs
}

// decodeJSONBody 解码响应正文。
func decodeJSONBody(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("响应不是预期的 JSON: %s: %v", b, err)
	}
}

// TestSessionIsolation：用户 A 建会话、发消息得 202；用户 B 对 A 的会话与 turn 的每个端点、无主会话与不存在的会话
// 一律 404 且正文相同（turn 端点 turn_not_found，原文 not_found）；A 看到自己已 closed 的会话也是 404；运维可读 A 的
// 会话（含 owner、internal_state）、turn 与事件（含 internal），一切写操作 403；日志不含消息正文与 answers。
func TestSessionIsolation(t *testing.T) {
	ts, acc, fs := newSessionServer(t, nil)
	a, ca := acc.seedUser(t, "anna")
	_, cb := acc.seedUser(t, "ben")
	fs.usernames[a.ID] = "anna"

	st, b, _ := ts.do("POST", "/sessions", `{"request_id":"c1"}`, session(ca))
	expect(t, st, b, 201, "")
	var sv SessionView
	decodeJSONBody(t, b, &sv)
	if sv.SessionID != "s1" || sv.State != "idle" || strings.Contains(string(b), "owner") || strings.Contains(string(b), "internal_state") {
		t.Fatalf("创建会话的响应 = %s", b)
	}
	const secretText = "机密问题-不应出现在日志里"
	st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m1","text":"  `+secretText+`  ","deep_research":false}`, session(ca))
	expect(t, st, b, 202, "")
	var mr CreateTurnResult
	decodeJSONBody(t, b, &mr)
	if mr.TurnID == "" || mr.TurnIndex != 0 {
		t.Fatalf("消息响应 = %s", b)
	}
	turn := mr.TurnID
	sum := sha256.Sum256([]byte(`{}`))
	sha := hex.EncodeToString(sum[:])
	fs.mu.Lock()
	fs.authorized[turn+"/"+sha] = true
	fs.mu.Unlock()
	ts.blobs.put(sha, []byte(`{}`))
	fs.appendRecord("s1", SessionEventRecord{Source: "session", Type: SEvSessionState, Payload: json.RawMessage(`{"state":"idle"}`)})
	fs.addSession("s0", 0, "idle") // 无主会话
	fs.addTurn("s0", "t0", "paused")

	type op struct{ method, path, body string }
	sessionOps := func(id string) []op {
		return []op{
			{"GET", "/sessions/" + id, ""},
			{"PATCH", "/sessions/" + id, `{"title":"新标题"}`},
			{"DELETE", "/sessions/" + id, ""},
			{"POST", "/sessions/" + id + "/wake", `{"request_id":"w-` + id + `"}`},
			{"POST", "/sessions/" + id + "/messages", `{"request_id":"m-` + id + `","text":"x","deep_research":true}`},
			{"GET", "/sessions/" + id + "/turns", ""},
			{"GET", "/sessions/" + id + "/events", ""},
		}
	}
	turnOps := func(id string) []op {
		return []op{
			{"POST", "/turns/" + id + "/stop", `{"request_id":"x-` + id + `"}`},
			{"POST", "/turns/" + id + "/continue", `{"request_id":"x-` + id + `"}`},
			{"POST", "/turns/" + id + "/finish", `{"request_id":"x-` + id + `"}`},
			{"POST", "/turns/" + id + "/answer", `{"request_id":"x-` + id + `","answers":[{"question_id":"q1","choice":"A"}]}`},
			{"POST", "/turns/" + id + "/restore", `{"request_id":"x-` + id + `"}`},
		}
	}
	check := func(who map[string]string, ops []op, missingOps []op, code string) {
		t.Helper()
		for i, o := range ops {
			_, missing, _ := ts.do(missingOps[i].method, missingOps[i].path, missingOps[i].body, who)
			st, got, _ := ts.do(o.method, o.path, o.body, who)
			expect(t, st, got, 404, code)
			if !bytes.Equal(got, missing) {
				t.Fatalf("%s %s 的 404 正文与不存在的不同: %s / %s", o.method, o.path, got, missing)
			}
		}
	}
	for _, id := range []string{"s1", "s0"} {
		check(session(cb), sessionOps(id), sessionOps("nope"), "session_not_found")
	}
	for _, id := range []string{turn, "t0"} {
		check(session(cb), turnOps(id), turnOps("nope"), "turn_not_found")
		check(session(cb), []op{{"GET", "/turns/" + id + "/raw/" + sha, ""}}, []op{{"GET", "/turns/nope/raw/" + sha, ""}}, "not_found")
	}
	st, b, _ = ts.do("GET", "/sessions", "", session(cb))
	expect(t, st, b, 200, "")
	if strings.Contains(string(b), "s1") || strings.Contains(string(b), "s0") {
		t.Fatalf("用户 B 的会话列表含他人会话: %s", b)
	}

	// 运维：读全部，写 403。
	st, b, _ = ts.do("GET", "/sessions/s1", "", adminAuth)
	expect(t, st, b, 200, "")
	if !strings.Contains(string(b), `"owner":"anna"`) || !strings.Contains(string(b), `"internal_state":"internal_idle"`) {
		t.Fatalf("运维的会话视图 = %s", b)
	}
	st, b, _ = ts.do("GET", "/sessions", "", adminAuth)
	expect(t, st, b, 200, "")
	if !strings.Contains(string(b), `"session_id":"s0"`) || !strings.Contains(string(b), `"owner":"anna"`) {
		t.Fatalf("运维的会话列表 = %s", b)
	}
	st, b, _ = ts.do("GET", "/sessions/s1/turns", "", adminAuth)
	expect(t, st, b, 200, "")
	st, b, _ = ts.do("GET", "/turns/"+turn+"/raw/"+sha, "", adminAuth)
	expect(t, st, b, 200, "")
	fs.appendRecord("s1", SessionEventRecord{Source: "session", Type: SEvSessionState, Payload: json.RawMessage(`{"state":"closed"}`)})
	st, b, _ = ts.do("GET", "/sessions/s1/events", "", adminAuth)
	expect(t, st, b, 200, "")
	if frames := readAllFrames(b); len(frames) != 2 || !strings.Contains(frames[0].data, `"internal":{"source":"session"`) {
		t.Fatalf("运维的事件流 = %+v", frames)
	}
	adminWrites := append([]op{{"POST", "/sessions", `{"request_id":"adm"}`}}, sessionOps("s1")...)
	adminWrites = append(adminWrites, turnOps(turn)...)
	for _, o := range adminWrites {
		if o.method == "GET" {
			continue
		}
		st, got, _ := ts.do(o.method, o.path, o.body, adminAuth)
		expect(t, st, got, 403, "forbidden")
	}

	// A 自己的会话进入 closed 后对 A 也是 404（运维仍可读）。
	fs.setState("s1", "closed")
	st, b, _ = ts.do("GET", "/sessions/s1", "", session(ca))
	expect(t, st, b, 404, "session_not_found")
	st, b, _ = ts.do("POST", "/turns/"+turn+"/stop", `{"request_id":"after-close"}`, session(ca))
	expect(t, st, b, 404, "turn_not_found")
	st, b, _ = ts.do("GET", "/sessions/s1", "", adminAuth)
	expect(t, st, b, 200, "")

	ts.mu.Lock()
	logs := ts.logs.String()
	ts.mu.Unlock()
	if strings.Contains(logs, secretText) || strings.Contains(logs, ca) || strings.Contains(logs, cb) {
		t.Fatalf("日志含消息正文或会话 cookie: %s", logs)
	}
}

// TestSessionCRUD：标题校验（创建 ≤ 80，重命名 1–80）、重命名、删除为 202 closing（重复删除仍 202）、关闭中的会话
// 重命名与发消息 409 session_closed；wake 202 无正文；列表的 limit 与游标校验；创建以 request_id 幂等，他人重用为冲突。
func TestSessionCRUD(t *testing.T) {
	ts, acc, _ := newSessionServer(t, nil)
	_, ca := acc.seedUser(t, "anna")
	_, cb := acc.seedUser(t, "ben")

	st, b, _ := ts.do("POST", "/sessions", `{"request_id":"c1","title":"`+strings.Repeat("长", 81)+`"}`, session(ca))
	expect(t, st, b, 400, "invalid_title")
	st, b, _ = ts.do("POST", "/sessions", `{"request_id":"c1","title":"  电池研究  "}`, session(ca))
	expect(t, st, b, 201, "")
	if !strings.Contains(string(b), `"title":"电池研究"`) {
		t.Fatalf("创建 = %s", b)
	}
	st, b, _ = ts.do("POST", "/sessions", `{"request_id":"c1","title":"  电池研究  "}`, session(ca))
	expect(t, st, b, 201, "")
	if !strings.Contains(string(b), `"session_id":"s1"`) {
		t.Fatalf("重放应返回首次创建的会话: %s", b)
	}
	st, b, _ = ts.do("POST", "/sessions", `{"request_id":"c1","title":"  电池研究  "}`, session(cb))
	expect(t, st, b, 409, "request_conflict")

	for _, title := range []string{`"   "`, strconv.Quote(strings.Repeat("x", 81))} {
		st, b, _ = ts.do("PATCH", "/sessions/s1", `{"title":`+title+`}`, session(ca))
		expect(t, st, b, 400, "invalid_title")
	}
	st, b, _ = ts.do("PATCH", "/sessions/s1", `{"title":" 新名字 "}`, session(ca))
	expect(t, st, b, 200, "")
	if !strings.Contains(string(b), `"title":"新名字"`) {
		t.Fatalf("重命名 = %s", b)
	}

	st, b, h := ts.do("POST", "/sessions/s1/wake", `{"request_id":"w1"}`, session(ca))
	expect(t, st, b, 202, "")
	if len(b) != 0 || h.Get("Content-Type") != "" {
		t.Fatalf("wake 应为无正文的 202: %q %v", b, h)
	}
	st, b, _ = ts.do("POST", "/sessions/s1/wake", `{}`, session(ca))
	expect(t, st, b, 400, "invalid_request")

	for _, q := range []string{"?limit=0", "?limit=201", "?limit=x"} {
		st, b, _ = ts.do("GET", "/sessions"+q, "", session(ca))
		expect(t, st, b, 400, "invalid_request")
	}
	st, b, _ = ts.do("GET", "/sessions?after=%27%3B", "", session(ca))
	expect(t, st, b, 400, "invalid_cursor")
	st, b, _ = ts.do("POST", "/sessions", `{"request_id":"c2"}`, session(ca))
	expect(t, st, b, 201, "")
	var newer SessionView
	decodeJSONBody(t, b, &newer)
	st, b, _ = ts.do("GET", "/sessions?limit=1", "", session(ca))
	expect(t, st, b, 200, "")
	var page sessionListJSON
	decodeJSONBody(t, b, &page)
	if len(page.Sessions) != 1 || page.Sessions[0].SessionID != newer.SessionID || page.Next != newer.SessionID {
		t.Fatalf("第一页 = %s", b)
	}
	st, b, _ = ts.do("GET", "/sessions?limit=1&after="+newer.SessionID, "", session(ca))
	expect(t, st, b, 200, "")
	decodeJSONBody(t, b, &page)
	if len(page.Sessions) != 1 || page.Sessions[0].SessionID != "s1" {
		t.Fatalf("第二页 = %s", b)
	}

	st, b, _ = ts.do("DELETE", "/sessions/s1", "", session(ca))
	expect(t, st, b, 202, "")
	if !strings.Contains(string(b), `"state":"closing"`) {
		t.Fatalf("删除 = %s", b)
	}
	st, b, _ = ts.do("DELETE", "/sessions/s1", "", session(ca))
	expect(t, st, b, 202, "")
	st, b, _ = ts.do("PATCH", "/sessions/s1", `{"title":"x"}`, session(ca))
	expect(t, st, b, 409, "session_closed")
	st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m","text":"x","deep_research":false}`, session(ca))
	expect(t, st, b, 409, "session_closed")
	st, b, _ = ts.do("POST", "/sessions/s1/wake", `{"request_id":"w2"}`, session(ca))
	expect(t, st, b, 409, "session_closed")
}

// TestSessionMessages：正文校验（去首尾空白 1–4000，deep_research 必填）；TurnSpec 得到去空白的文本，limits 规范化后
// 随 turn 存储；运行中再发消息 409 turn_in_progress；paused 时 202 并带 superseded_turn_id；同 request_id 重放返回
// 首次结果；他人会话不能重用 request_id；每用户 1 个运行中为 409 user_task_running；turn 列表按 turn_index 分页。
func TestSessionMessages(t *testing.T) {
	ts, acc, fs := newSessionServer(t, nil)
	a, ca := acc.seedUser(t, "anna")
	st, b, _ := ts.do("POST", "/sessions", `{"request_id":"c1"}`, session(ca))
	expect(t, st, b, 201, "")

	for _, body := range []string{
		`{"request_id":"m","text":"   ","deep_research":false}`,
		`{"request_id":"m","text":"` + strings.Repeat("字", 4001) + `","deep_research":false}`,
	} {
		st, b, _ = ts.do("POST", "/sessions/s1/messages", body, session(ca))
		expect(t, st, b, 400, "invalid_text")
	}
	for _, body := range []string{`{"request_id":"m","text":"x"}`, `{"request_id":"m","text":"x","deep_research":false,"model":"gpt"}`, `{"text":"x","deep_research":false}`} {
		st, b, _ = ts.do("POST", "/sessions/s1/messages", body, session(ca))
		expect(t, st, b, 400, "invalid_request")
	}

	st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m1","text":"  什么是固态电池？  ","deep_research":true}`, session(ca))
	expect(t, st, b, 202, "")
	var first CreateTurnResult
	decodeJSONBody(t, b, &first)
	fs.mu.Lock()
	req := fs.lastTurn
	fs.mu.Unlock()
	if req.Text != "什么是固态电池？" || !req.DeepResearch || req.OwnerUserID != a.ID || req.SessionID != "s1" || req.RestoredFromTaskID != "" ||
		string(req.Limits) != `{"max_tool_calls":30}` || !strings.Contains(string(req.Spec), `"text":"什么是固态电池？"`) || req.MaxFaultRetries != 3 {
		t.Fatalf("CreateTurn 请求 = %+v (spec %s, limits %s)", req, req.Spec, req.Limits)
	}
	st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m1","text":"  什么是固态电池？  ","deep_research":true}`, session(ca))
	expect(t, st, b, 202, "")
	var again CreateTurnResult
	decodeJSONBody(t, b, &again)
	if again.TurnID != first.TurnID {
		t.Fatalf("重放 = %s，首次 %+v", b, first)
	}

	for _, status := range []string{"queued", "running", "stopping"} {
		fs.setTurnStatus(first.TurnID, status)
		st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m2","text":"再问","deep_research":false}`, session(ca))
		expect(t, st, b, 409, "turn_in_progress")
	}
	fs.setTurnStatus(first.TurnID, "paused")
	st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m2","text":"再问","deep_research":false}`, session(ca))
	expect(t, st, b, 202, "")
	var second CreateTurnResult
	decodeJSONBody(t, b, &second)
	if second.SupersededTurnID != first.TurnID || second.TurnIndex != 1 || !strings.Contains(string(b), `"superseded_turn_id":"`+first.TurnID+`"`) {
		t.Fatalf("取代暂停的 turn = %s", b)
	}

	fs.mu.Lock()
	fs.failTurn = fmt.Errorf("CreateTurn: %w", ErrUserTaskRunning)
	fs.mu.Unlock()
	st, b, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m3","text":"三","deep_research":false}`, session(ca))
	expect(t, st, b, 409, "user_task_running")

	_, cb := acc.seedUser(t, "ben")
	st, b, _ = ts.do("POST", "/sessions", `{"request_id":"cb"}`, session(cb))
	expect(t, st, b, 201, "")
	st, b, _ = ts.do("POST", "/sessions/s2/messages", `{"request_id":"m1","text":"  什么是固态电池？  ","deep_research":true}`, session(cb))
	expect(t, st, b, 409, "request_conflict")

	st, b, _ = ts.do("GET", "/sessions/s1/turns", "", session(ca))
	expect(t, st, b, 200, "")
	var tl turnListJSON
	decodeJSONBody(t, b, &tl)
	if len(tl.Turns) != 2 || tl.Turns[0].TurnIndex != 0 || tl.Turns[0].Status != "cancelled" || tl.Turns[0].StatusReason != "superseded" ||
		!tl.Turns[0].Restorable || strings.Contains(string(b), `"next"`) {
		t.Fatalf("turn 列表 = %s", b)
	}
	st, b, _ = ts.do("GET", "/sessions/s1/turns?after_index=0&limit=5", "", session(ca))
	expect(t, st, b, 200, "")
	decodeJSONBody(t, b, &tl)
	if len(tl.Turns) != 1 || tl.Turns[0].TurnID != second.TurnID {
		t.Fatalf("after_index=0 = %s", b)
	}
	for _, q := range []string{"?after_index=-1", "?after_index=x", "?limit=0"} {
		st, b, _ = ts.do("GET", "/sessions/s1/turns"+q, "", session(ca))
		expect(t, st, b, 400, "invalid_request")
	}
}

// TestTurnControls：stop/continue/finish/answer 写入控制并回答 202 {turn_id, control_version}；状态不符 409
// invalid_turn_state；answers 不是 1–3 项、question_id 为空、choice 与 other 不是恰一个时 400 invalid_answers，合法的
// answers 规范化后交给存储；restore 以源 turn 的文本、deep_research = true 开新 turn，不可恢复时 409 not_restorable。
func TestTurnControls(t *testing.T) {
	ts, acc, fs := newSessionServer(t, nil)
	a, ca := acc.seedUser(t, "anna")
	fs.addSession("s1", a.ID, "idle")
	fs.addTurn("s1", "t1", "running")

	st, b, _ := ts.do("POST", "/turns/t1/stop", `{"request_id":"r1"}`, session(ca))
	expect(t, st, b, 202, "")
	if string(bytes.TrimSpace(b)) != `{"turn_id":"t1","control_version":1}` {
		t.Fatalf("stop = %s", b)
	}
	st, b, _ = ts.do("POST", "/turns/t1/stop", `{"request_id":"r1"}`, session(ca))
	expect(t, st, b, 202, "") // 重放
	st, b, _ = ts.do("POST", "/turns/t1/continue", `{"request_id":"r1"}`, session(ca))
	expect(t, st, b, 409, "request_conflict") // 同 request_id 用于另一种操作
	st, b, _ = ts.do("POST", "/turns/t1/continue", `{"request_id":"r2"}`, session(ca))
	expect(t, st, b, 409, "invalid_turn_state")
	fs.mu.Lock()
	fs.failControl = fmt.Errorf("TurnControl: %w", ErrInvalidTurnState)
	fs.mu.Unlock()
	fs.setTurnStatus("t1", "paused")
	st, b, _ = ts.do("POST", "/turns/t1/continue", `{"request_id":"r3"}`, session(ca))
	expect(t, st, b, 409, "invalid_turn_state")
	st, b, _ = ts.do("POST", "/turns/t1/finish", `{"request_id":"r4"}`, session(ca))
	expect(t, st, b, 202, "")

	fs.setTurnStatus("t1", "awaiting_input")
	for _, answers := range []string{
		`[]`,
		`[{"question_id":"q1","choice":"A"},{"question_id":"q2","choice":"A"},{"question_id":"q3","choice":"A"},{"question_id":"q4","choice":"A"}]`,
		`[{"question_id":"","choice":"A"}]`,
		`[{"question_id":"q1"}]`,
		`[{"question_id":"q1","choice":"A","other":"B"}]`,
		`[{"question_id":"q1","other":"  "}]`,
	} {
		st, b, _ = ts.do("POST", "/turns/t1/answer", `{"request_id":"a1","answers":`+answers+`}`, session(ca))
		expect(t, st, b, 400, "invalid_answers")
	}
	st, b, _ = ts.do("POST", "/turns/t1/answer", `{"request_id":"a1"}`, session(ca))
	expect(t, st, b, 400, "invalid_answers")
	st, b, _ = ts.do("POST", "/turns/t1/answer", `{"request_id":"a1","answers":[{"question_id":"q1","choice":"A","extra":1}]}`, session(ca))
	expect(t, st, b, 400, "invalid_request")
	st, b, _ = ts.do("POST", "/turns/t1/answer", `{"request_id":"a1","answers":[{"other":"自己的说法","question_id":"q-2-1"},{"question_id":"q-2-2","choice":"B"}]}`, session(ca))
	expect(t, st, b, 202, "")
	fs.mu.Lock()
	ctl := fs.lastControl
	fs.mu.Unlock()
	if ctl.Action != "answer" || ctl.TaskID != "t1" || string(ctl.Answers) != `[{"other":"自己的说法","question_id":"q-2-1"},{"choice":"B","question_id":"q-2-2"}]` {
		t.Fatalf("answer 控制 = %+v (%s)", ctl, ctl.Answers)
	}
	st, b, _ = ts.do("POST", "/turns/t1/stop", `{"request_id":"r5"}`, session(ca))
	expect(t, st, b, 409, "invalid_turn_state")

	// restore：源 turn 未取消 → not_restorable；取消后以源文本开新 turn。
	st, b, _ = ts.do("POST", "/turns/t1/restore", `{"request_id":"re1"}`, session(ca))
	expect(t, st, b, 409, "not_restorable")
	fs.setTurnStatus("t1", "cancelled")
	st, b, _ = ts.do("POST", "/turns/t1/restore", `{"request_id":"re2"}`, session(ca))
	expect(t, st, b, 202, "")
	var res CreateTurnResult
	decodeJSONBody(t, b, &res)
	fs.mu.Lock()
	req := fs.lastTurn
	fs.mu.Unlock()
	if res.TurnIndex != 1 || req.RestoredFromTaskID != "t1" || req.Text != "问题 t1" || !req.DeepResearch || req.SessionID != "s1" ||
		!strings.Contains(string(req.Spec), `"deep_research":true`) {
		t.Fatalf("restore = %s，CreateTurn 请求 %+v", b, req)
	}
	st, b, _ = ts.do("POST", "/turns/t1/restore", `{"request_id":"re3"}`, session(ca))
	expect(t, st, b, 409, "turn_in_progress") // 新 turn 仍在排队
}

// openSessionStream 打开整会话事件流（断言 200 与 text/event-stream）。
func (ts *testServer) openSessionStream(sessionID string, hdr map[string]string) (*http.Response, *bufio.Reader) {
	ts.t.Helper()
	req, err := http.NewRequest("GET", ts.srv.URL+"/sessions/"+sessionID+"/events", nil)
	if err != nil {
		ts.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := ts.srv.Client().Do(req)
	if err != nil {
		ts.t.Fatal(err)
	}
	ts.checkDeclared(req, resp.StatusCode)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		b, _ := io.ReadAll(resp.Body)
		ts.t.Fatalf("打开会话事件流: %d %s", resp.StatusCode, b)
	}
	return resp, bufio.NewReader(resp.Body)
}

// readEventFrames 读取 n 个事件帧（跳过心跳）。
func readEventFrames(t *testing.T, r *bufio.Reader, n int) []sseFrame {
	t.Helper()
	var out []sseFrame
	for len(out) < n {
		f, err := readFrame(r)
		if err != nil {
			t.Fatalf("读取事件帧: %v（已读 %+v）", err, out)
		}
		if !f.comment {
			out = append(out, f)
		}
	}
	return out
}

// TestSessionSSE：5 条记录中 1 条对用户不可见，用户收到 4 条、id 为各自的 session_seq，data 为对外事件（无
// internal）；turn 的 task_terminal 不关闭流；Last-Event-ID 3（不可见记录的 seq）续传不重复；超出最新 400；
// session_state{closed} 之后服务端关闭；心跳为注释行。
func TestSessionSSE(t *testing.T) {
	ts, acc, fs := newSessionServer(t, func(c *Config) { c.Heartbeat = 20 * time.Millisecond })
	a, ca := acc.seedUser(t, "anna")
	fs.addSession("s1", a.ID, "idle")
	fs.addSession("s2", a.ID, "idle")
	for _, r := range []SessionEventRecord{
		{Source: "session", Type: SEvSessionState, Payload: json.RawMessage(`{"state":"idle"}`)},
		{TaskID: "t1", TaskSeq: 1, Source: "host", Type: "task_created", Payload: json.RawMessage(`{"turn_index":0,"text":"你好","deep_research":false,"spec":{"model":"kimi-k3"}}`)},
		{TaskID: "t1", TaskSeq: 2, Source: "host", Type: "control_accepted", Payload: json.RawMessage(`{"desired":"pause","control_version":2}`)},
		{TaskID: "t1", TaskSeq: 3, AttemptID: "att_1", Source: "worker", Type: "progress", Payload: json.RawMessage(`{"step_id":"orch","kind":"thinking","data":{"text":"想","usage":{"total_tokens":3}}}`)},
		{TaskID: "t1", TaskSeq: 4, AttemptID: "att_1", Source: "host", Type: EventTaskTerminal, Payload: json.RawMessage(`{"attempt_id":"att_1","task_status":"succeeded","status_reason":"ok"}`)},
	} {
		fs.appendRecord("s1", r)
	}

	resp, r := ts.openSessionStream("s1", session(ca))
	frames := readEventFrames(t, r, 4)
	resp.Body.Close()
	var ids, types []string
	for _, f := range frames {
		ids, types = append(ids, f.id), append(types, f.event)
		var ev SessionEvent
		decodeJSONBody(t, []byte(f.data), &ev)
		if strconv.FormatInt(ev.Seq, 10) != f.id || ev.Type != f.event || ev.Internal != nil ||
			strings.Contains(f.data, "usage") || strings.Contains(f.data, "att_1") || strings.Contains(f.data, "kimi") {
			t.Fatalf("事件帧 = %+v", f)
		}
	}
	if !slices.Equal(ids, []string{"1", "2", "4", "5"}) || !slices.Equal(types, []string{SEvSessionState, SEvTurnCreated, SEvThinking, SEvTurnStatus}) {
		t.Fatalf("用户收到 ids %v types %v", ids, types)
	}

	// 续传：Last-Event-ID 3 指向不可见记录，之后的 4、5 不重复；task_terminal 后流不关闭，closed 后关闭。
	hdr := session(ca)
	hdr["Last-Event-ID"] = "3"
	resp, r = ts.openSessionStream("s1", hdr)
	defer resp.Body.Close()
	frames = readEventFrames(t, r, 2)
	if frames[0].id != "4" || frames[1].id != "5" {
		t.Fatalf("续传 = %+v", frames)
	}
	for _, c := range []string{"6", "abc", "-1"} {
		h := session(ca)
		h["Last-Event-ID"] = c
		st, b, _ := ts.do("GET", "/sessions/s1/events", "", h)
		expect(t, st, b, 400, "invalid_cursor")
	}
	fs.appendRecord("s1", SessionEventRecord{Source: "session", Type: SEvSessionState, Payload: json.RawMessage(`{"state":"closed"}`)})
	fs.appendRecord("s1", SessionEventRecord{Source: "session", Type: SEvSessionState, Payload: json.RawMessage(`{"state":"idle"}`)}) // 不应发送
	var rest []string
	for {
		f, err := readFrame(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !f.comment {
			rest = append(rest, f.id+"/"+f.data)
		}
	}
	if len(rest) != 1 || !strings.HasPrefix(rest[0], `6/`) || !strings.Contains(rest[0], `"state":"closed"`) {
		t.Fatalf("closed 之后应关闭流: %v", rest)
	}

	// 运维看到全部记录：不可见的记录以 internal 类型出现，每条带原始记录。
	st, b, _ := ts.do("GET", "/sessions/s1/events", "", adminAuth)
	expect(t, st, b, 200, "")
	all := readAllFrames(b)
	if len(all) != 6 || all[2].event != SEvInternal || !strings.Contains(all[2].data, `"internal":{"source":"host","type":"control_accepted"`) ||
		!strings.Contains(all[3].data, `"usage"`) {
		t.Fatalf("运维事件流 = %+v", all)
	}

	// 心跳：没有事件时只有注释行。
	resp2, r2 := ts.openSessionStream("s2", session(ca))
	defer resp2.Body.Close()
	if f, err := readFrame(r2); err != nil || !f.comment || f.id != "" {
		t.Fatalf("期望注释行心跳，得到 %+v, %v", f, err)
	}
}

// TestTurnRaw：用户得到脱敏的原文（无 usage、顶层 id），运维原样；Content-Disposition attachment、nosniff；非 JSON
// 为 octet-stream；未授权 sha 与非法 sha 404 not_found；内容与 sha256 不符 500 blob_unavailable。
func TestTurnRaw(t *testing.T) {
	ts, acc, fs := newSessionServer(t, nil)
	a, ca := acc.seedUser(t, "anna")
	fs.addSession("s1", a.ID, "idle")
	fs.addTurn("s1", "t1", "running")
	put := func(content string) string {
		sum := sha256.Sum256([]byte(content))
		sha := hex.EncodeToString(sum[:])
		ts.blobs.put(sha, []byte(content))
		fs.mu.Lock()
		fs.authorized["t1/"+sha] = true
		fs.mu.Unlock()
		return sha
	}
	raw := `{"id":"chatcmpl-1","model":"kimi-k3","choices":[{"message":{"content":"<答>"}}],"usage":{"prompt_tokens":5}}`
	sha := put(raw)

	st, b, h := ts.do("GET", "/turns/t1/raw/"+sha, "", session(ca))
	expect(t, st, b, 200, "")
	if strings.Contains(string(b), "usage") || strings.Contains(string(b), "chatcmpl-1") || strings.Contains(string(b), "kimi") || !strings.Contains(string(b), "<答>") {
		t.Fatalf("用户原文未脱敏: %s", b)
	}
	if h.Get("Content-Type") != "application/json" || !strings.HasPrefix(h.Get("Content-Disposition"), "attachment") || h.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("原文响应头 = %v", h)
	}
	st, b, _ = ts.do("GET", "/turns/t1/raw/"+sha, "", adminAuth)
	expect(t, st, b, 200, "")
	if string(b) != raw {
		t.Fatalf("运维原文被改动: %s", b)
	}

	text := put("plain text, not json")
	st, b, h = ts.do("GET", "/turns/t1/raw/"+text, "", session(ca))
	expect(t, st, b, 200, "")
	if string(b) != "plain text, not json" || h.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("非 JSON 原文 = %q %v", b, h)
	}

	_, missing, _ := ts.do("GET", "/turns/nope/raw/"+sha, "", session(ca))
	for _, s := range []string{strings.Repeat("cd", 32), strings.ToUpper(sha), "abc", sha + "0"} {
		st, b, _ = ts.do("GET", "/turns/t1/raw/"+s, "", session(ca))
		expect(t, st, b, 404, "not_found")
		if !bytes.Equal(b, missing) {
			t.Fatalf("原文 404 正文不同: %s / %s", b, missing)
		}
	}

	tampered := strings.Repeat("ef", 32)
	ts.blobs.put(tampered, []byte(`{"x":1}`))
	fs.mu.Lock()
	fs.authorized["t1/"+tampered] = true
	fs.mu.Unlock()
	st, b, _ = ts.do("GET", "/turns/t1/raw/"+tampered, "", session(ca))
	expect(t, st, b, 500, "blob_unavailable")
}

// TestSessionsUnavailable：启用账号而未配置会话（Sessions 为 nil）时每个会话操作 503 sessions_unavailable（认证之后）；
// 未启用账号时会话端点不存在；配置 Sessions 而缺少账号或 TurnSpec 时拒绝启动。
func TestSessionsUnavailable(t *testing.T) {
	ts, acc := newAccountServer(t, nil)
	_, ca := acc.seedUser(t, "anna")
	for _, op := range sessionOperations {
		method, path, _ := strings.Cut(op, " ")
		path = strings.NewReplacer("{id}", "x1", "{sha256}", strings.Repeat("ab", 32)).Replace(path)
		who := session(ca)
		if method == "GET" {
			who = adminAuth
		}
		st, b, _ := ts.do(method, path, `{"request_id":"r"}`, who)
		expect(t, st, b, 503, "sessions_unavailable")
		st, b, _ = ts.do(method, path, `{"request_id":"r"}`, nil)
		expect(t, st, b, 401, "unauthorized")
	}

	plain := newTestServer(t, nil)
	st, b, _ := plain.do("GET", "/sessions", "", nil)
	expect(t, st, b, 404, "not_found")

	fs := newFakeSessions()
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "127.0.0.1:1", Sessions: fs, TurnSpec: testTurnSpec}); err == nil {
		t.Fatal("会话需要启用账号")
	}
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "127.0.0.1:1", Sessions: fs,
		Accounts: newFakeAccounts(newFakeStore()), ResearchSpec: testResearchSpec}); err == nil {
		t.Fatal("会话需要 TurnSpec")
	}
}

// ==== M4 Plan 14 Task 10：inspect 的 sub-run 时间线与两层费用（规格 §15.4） ====

// TestInspectSubruns：inspect 返回 task 层账本（budget）与 sub-run 列表（状态、时间、原因、sub-run 层账本、调用数）；
// sub-run 的调用带 subrun_id，root 调用省略；没有 sub-run 时 subruns 为 []；用户主体访问 inspect 仍 403。
func TestInspectSubruns(t *testing.T) {
	ts := newTestServer(t, nil)
	t0 := time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)
	ended := t0.Add(90 * time.Second)
	capMicro := int64(400)
	toolLimit := int64(30)
	ts.store.inspect["t1"] = Inspection{
		Task:   TaskView{TaskID: "t1", Status: "running"},
		Budget: &BudgetView{LimitMicro: 2000, ReservedMicro: 100, SpentMicro: 170, UnknownMicro: 5, ToolCallLimit: &toolLimit, ToolCallsUsed: 7},
		Subruns: []SubrunView{
			{SubrunID: "st1", ParentStepID: "research", Status: "started", StartedAt: t0, DeadlineAt: t0.Add(10 * time.Minute),
				CapMicro: &capMicro, ReservedMicro: 100, SpentMicro: 120, UnknownMicro: 5, Calls: 2},
			{SubrunID: "st2", ParentStepID: "research", Status: "timed_out", StartedAt: t0, EndedAt: &ended,
				DeadlineAt: t0.Add(time.Minute), CancelReason: "deadline"},
			{SubrunID: "st3", ParentStepID: "research", Status: "failed", StartedAt: t0, EndedAt: &ended,
				DeadlineAt: t0.Add(time.Minute), FailureReason: "model_unavailable"},
		},
		Calls: []CallView{{CallID: "orch/1", Endpoint: "/v1/chat/completions", State: "completed"},
			{CallID: "st1/1", Endpoint: "/v1/search", State: "completed", SubrunID: "st1"}},
	}
	ts.store.inspect["t2"] = Inspection{Task: TaskView{TaskID: "t2", Status: "queued"}}

	st, b, _ := ts.do("GET", "/tasks/t1/inspect", "", nil)
	expect(t, st, b, 200, "")
	var out struct {
		Budget  map[string]any   `json:"budget"`
		Subruns []map[string]any `json:"subruns"`
		Calls   []map[string]any `json:"calls"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Budget["limit_micro"] != 2000.0 || out.Budget["spent_micro"] != 170.0 || out.Budget["unknown_micro"] != 5.0 ||
		out.Budget["tool_calls_used"] != 7.0 || out.Budget["tool_call_limit"] != 30.0 {
		t.Fatalf("budget = %s", b)
	}
	if len(out.Subruns) != 3 {
		t.Fatalf("subruns = %s", b)
	}
	s1, s2, s3 := out.Subruns[0], out.Subruns[1], out.Subruns[2]
	if s1["subrun_id"] != "st1" || s1["status"] != "started" || s1["cap_micro"] != 400.0 || s1["spent_micro"] != 120.0 ||
		s1["reserved_micro"] != 100.0 || s1["unknown_micro"] != 5.0 || s1["calls"] != 2.0 || s1["started_at"] != "2026-10-06T08:00:00Z" {
		t.Fatalf("st1 = %v", s1)
	}
	for _, k := range []string{"ended_at", "cancel_reason", "failure_reason"} {
		if _, has := s1[k]; has {
			t.Errorf("进行中的 st1 不应有 %s：%v", k, s1)
		}
	}
	if s2["status"] != "timed_out" || s2["cancel_reason"] != "deadline" || s2["ended_at"] != "2026-10-06T08:01:30Z" {
		t.Fatalf("st2 = %v", s2)
	}
	if _, has := s2["cap_micro"]; has {
		t.Errorf("没有上限的 sub-run 不应有 cap_micro：%v", s2)
	}
	if s3["failure_reason"] != "model_unavailable" || s3["spent_micro"] != 0.0 || s3["calls"] != 0.0 {
		t.Fatalf("st3 = %v", s3)
	}
	if _, has := out.Calls[0]["subrun_id"]; has || out.Calls[1]["subrun_id"] != "st1" {
		t.Fatalf("calls 的 subrun_id：%s", b)
	}

	st, b, _ = ts.do("GET", "/tasks/t2/inspect", "", nil)
	expect(t, st, b, 200, "")
	if !bytes.Contains(b, []byte(`"subruns":[]`)) || bytes.Contains(b, []byte(`"budget"`)) {
		t.Fatalf("没有 sub-run 与账本时 = %s", b)
	}

	// 用户主体（含任务所有者）访问 inspect 仍 403：sub-run ID、费用与模型只对运维可见。
	acc, a := newAccountServer(t, nil)
	u, cookie := a.seedUser(t, "gus")
	acc.store.addTask("t1", "running", "run")
	a.own("t1", u.ID)
	acc.store.inspect["t1"] = ts.store.inspect["t1"]
	st, b, _ = acc.do("GET", "/tasks/t1/inspect", "", session(cookie))
	expect(t, st, b, 403, "forbidden")
	st, b, _ = acc.do("GET", "/tasks/t1/inspect", "", adminAuth)
	expect(t, st, b, 200, "")
}

// TestOpenAPIInspectSubruns：openapi.yaml 的 Subrun、TaskBudget 字段与 Go 的 JSON 标签一致；Inspection 声明
// subruns（必填）与 budget，Call 声明 subrun_id。
func TestOpenAPIInspectSubruns(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
	for name, v := range map[string]any{"Subrun": SubrunView{}, "TaskBudget": BudgetView{}} {
		if got, want := keysAt(openAPISchemaBlock(t, lines, name), "properties:", 6), jsonTagNames(v); !slices.Equal(got, want) {
			t.Errorf("%s 字段 %v，Go JSON 标签 %v", name, got, want)
		}
	}
	insp := openAPISchemaBlock(t, lines, "Inspection")
	if got := keysAt(insp, "properties:", 6); !slices.Contains(got, "subruns") || !slices.Contains(got, "budget") {
		t.Errorf("Inspection 字段 %v 缺少 subruns 或 budget", got)
	}
	if !slices.Contains(flowList(insp, "required: ["), "subruns") {
		t.Error("Inspection.subruns 应为必填")
	}
	if got := keysAt(openAPISchemaBlock(t, lines, "Call"), "properties:", 6); !slices.Contains(got, "subrun_id") {
		t.Errorf("Call 字段 %v 缺少 subrun_id", got)
	}
}

// TestSessionEventHidesSubrunID：用户的会话事件只带子主题（subtopic_id），不带 sub-run ID（信封或 data 中任意深度）与费用。
func TestSessionEventHidesSubrunID(t *testing.T) {
	payload := `{"step_id":"st1","kind":"tool_call","subrun_id":"st1","data":{"tool_call_id":"st1:3","tool":"web_search",` +
		`"subtopic_id":"st1","subrun_id":"st1","input":{"query":"q","subrun_id":"st1"},"cost_micro":9}}`
	ev, ok := ToSessionEvent(sessionRec("worker", "progress", payload), false)
	if !ok || ev.Type != SEvToolCall {
		t.Fatalf("用户视图 = %+v, %v", ev, ok)
	}
	d := eventData(t, ev)
	for _, k := range []string{"subrun_id", "cost_micro"} {
		if hasKey(d, k) {
			t.Errorf("用户 data 含 %q：%s", k, ev.Data)
		}
	}
	if d["subtopic_id"] != "st1" || d["tool_call_id"] != "st1:3" {
		t.Errorf("用户 data = %s", ev.Data)
	}
}

// jsonTagNames 返回结构体字段的 JSON 名（排序）。
func jsonTagNames(v any) []string {
	var out []string
	rt := reflect.TypeOf(v)
	for i := 0; i < rt.NumField(); i++ {
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// flowList 返回块中第一处 "<prefix>a, b]" 的项。
func flowList(block []string, prefix string) []string {
	for _, l := range block {
		if _, rest, ok := strings.Cut(l, prefix); ok {
			items, _, _ := strings.Cut(rest, "]")
			var out []string
			for _, it := range strings.Split(items, ",") {
				out = append(out, strings.TrimSpace(it))
			}
			return out
		}
	}
	return nil
}

// ==== M4 Plan 14 Task 10 段结束 ====

// ==== M4 Plan 14 Task 12 段：运维在会话中创建 turn（POST /tasks 带 session_id，C12-7） ====

// TestOperatorCreateTurnInSession：运维 POST /tasks 带 session_id → 201 {task_id}，turn 归会话所有者（不比较调用者），
// spec 由服务端生成（模型、limits 不变），research 的键覆盖到 spec.research；重放返回同一 task_id；spec 只接受
// text、deep_research、research，limits 不可指定；不存在的会话 404；用户调用 POST /tasks 仍 403；未启用会话 503。
func TestOperatorCreateTurnInSession(t *testing.T) {
	ts, acc, fs := newSessionServer(t, nil)
	_, ca := acc.seedUser(t, "anna")
	st, b, _ := ts.do("POST", "/sessions", `{"request_id":"c1"}`, session(ca))
	expect(t, st, b, 201, "")

	body := `{"request_id":"op1","session_id":"s1","spec":{"text":" 固态电池 ","deep_research":true,` +
		`"research":{"scheduling":"serial","fixed_plan":[{"id":"1","title":"t","budget":5}]}}}`
	st, b, _ = ts.do("POST", "/tasks", body, adminAuth)
	expect(t, st, b, 201, "")
	var first CreateTaskResult
	decodeJSONBody(t, b, &first)
	fs.mu.Lock()
	req := fs.lastTurn
	fs.mu.Unlock()
	var spec map[string]any
	if err := json.Unmarshal(req.Spec, &spec); err != nil {
		t.Fatal(err)
	}
	research, _ := spec["research"].(map[string]any)
	if first.TaskID == "" || req.TaskID != first.TaskID || !req.Operator || req.SessionID != "s1" || req.Text != "固态电池" ||
		!req.DeepResearch || spec["orchestrator_model"] != "kimi-k3" || spec["text"] != "固态电池" ||
		research["scheduling"] != "serial" || research["fixed_plan"] == nil || string(req.Limits) != `{"max_tool_calls":30}` {
		t.Fatalf("CreateTurn 请求 = %+v (spec %s, limits %s)", req, req.Spec, req.Limits)
	}
	st, b, _ = ts.do("POST", "/tasks", body, adminAuth)
	expect(t, st, b, 201, "")
	var again CreateTaskResult
	decodeJSONBody(t, b, &again)
	if again.TaskID != first.TaskID {
		t.Fatalf("重放 = %s，首次 %+v", b, first)
	}

	for _, bad := range []string{
		`{"request_id":"op2","session_id":"s1","spec":{"text":"x","deep_research":true,"orchestrator_model":"gpt"}}`,
		`{"request_id":"op2","session_id":"s1","spec":{"text":"x"}}`,
		`{"request_id":"op2","session_id":"s1","spec":{"text":"x","deep_research":true},"limits":{"max_tool_calls":99}}`,
		`{"request_id":"op2","session_id":"s1","spec":{"text":"x","deep_research":true,"research":[1]}}`,
	} {
		st, b, _ = ts.do("POST", "/tasks", bad, adminAuth)
		expect(t, st, b, 400, "invalid_request")
	}
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"op2","session_id":"s1","spec":{"text":" ","deep_research":true}}`, adminAuth)
	expect(t, st, b, 400, "invalid_text")
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"op3","session_id":"nope","spec":{"text":"x","deep_research":false}}`, adminAuth)
	expect(t, st, b, 404, "session_not_found")
	st, b, _ = ts.do("POST", "/tasks", `{"request_id":"op4","session_id":"s1","spec":{"text":"x","deep_research":false}}`, session(ca))
	expect(t, st, b, 403, "forbidden")

	plain, _ := newAccountServer(t, nil)
	st, b, _ = plain.do("POST", "/tasks", `{"request_id":"op5","session_id":"s1","spec":{"text":"x","deep_research":false}}`, adminAuth)
	expect(t, st, b, 503, "sessions_unavailable")
}

// TestOverlayResearch：运维的 research 键覆盖服务端的同名键，其余服务端键保留；spec 无 research 时新建。
func TestOverlayResearch(t *testing.T) {
	got, err := overlayResearch(json.RawMessage(`{"kind":"turn","research":{"worker_output_micro_per_mtok":8,"scheduling":"parallel"}}`),
		json.RawMessage(`{"scheduling":"serial"}`))
	if err != nil || string(got) != `{"kind":"turn","research":{"scheduling":"serial","worker_output_micro_per_mtok":8}}` {
		t.Fatalf("overlay = %s, %v", got, err)
	}
	got, err = overlayResearch(json.RawMessage(`{"kind":"turn"}`), json.RawMessage(`{"scheduling":"serial"}`))
	if err != nil || string(got) != `{"kind":"turn","research":{"scheduling":"serial"}}` {
		t.Fatalf("overlay（无 research）= %s, %v", got, err)
	}
	if got, err = overlayResearch(json.RawMessage(`{"a":1}`), nil); err != nil || string(got) != `{"a":1}` {
		t.Fatalf("overlay（nil）= %s, %v", got, err)
	}
}

// ==== M4 Plan 14 Task 12 段结束 ====

// ==== 停止修复 F2：宿主兜底停止卡 ====

// TestFallbackTurnStopped：宿主由该 turn 已有的 Worker 事件构造停止卡——最新计划、子主题完成数（各子主题的最后状态）与
// 总数、登记为来源的抓取（同一网址只计一次）、工具额度取宿主的计数；findings 为固定文案；至少一个子主题完成才可写报告。
func TestFallbackTurnStopped(t *testing.T) {
	limit := int64(30)
	progress := []json.RawMessage{
		json.RawMessage(`{"kind":"todo_updated","data":{"items":[{"id":"1","title":"旧","status":"pending","budget_share":5}]}}`),
		json.RawMessage(`{"kind":"todo_updated","data":{"items":[{"id":"1","title":"甲","status":"done","budget_share":10,"cost_micro":3},` +
			`{"id":"2","title":"乙","status":"in_progress","budget_share":10},{"id":"3","title":"写报告","status":"pending","budget_share":0}]}}`),
		json.RawMessage(`{"kind":"subtopic","data":{"id":"1","title":"甲","status":"running"}}`),
		json.RawMessage(`{"kind":"subtopic","data":{"id":"1","title":"甲","status":"done","summary":"s"}}`),
		json.RawMessage(`{"kind":"subtopic","data":{"id":"2","title":"乙","status":"running"}}`),
		json.RawMessage(`{"kind":"tool_result","subrun_id":"st1","data":{"tool":"web_fetch","ok":true,"preview":{"kind":"fetch","n":1,"url":"https://a.example/1"}}}`),
		json.RawMessage(`{"kind":"tool_result","data":{"tool":"web_fetch","ok":true,"preview":{"kind":"fetch","n":1,"url":"https://a.example/1"}}}`),
		json.RawMessage(`{"kind":"tool_result","data":{"tool":"web_fetch","ok":true,"preview":{"kind":"fetch","n":2,"url":"https://b.example/2"}}}`),
		json.RawMessage(`{"kind":"tool_result","data":{"tool":"web_fetch","ok":true,"preview":{"kind":"fetch","url":"https://c.example/pdf","excerpt":"PDF"}}}`),
		json.RawMessage(`{"kind":"tool_result","data":{"tool":"web_fetch","ok":false,"preview":{"kind":"text","text":"抓取失败"}}}`),
		json.RawMessage(`{"kind":"tool_result","data":{"tool":"web_search","ok":true,"preview":{"results":[]}}}`),
		json.RawMessage(`{"kind":"budget","data":{"used":4,"limit":30}}`),
		json.RawMessage(`not json`),
	}
	b, err := FallbackTurnStopped(progress, 7, &limit)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("data 不是 JSON 对象：%s", b)
	}
	want := map[string]any{
		"card": map[string]any{"subtopics_done": 1.0, "subtopics_total": 2.0, "sources": 2.0, "tool_calls_used": 7.0, "tool_call_limit": 30.0,
			"todo": []any{
				map[string]any{"id": "1", "title": "甲", "status": "done", "budget_share": 10.0},
				map[string]any{"id": "2", "title": "乙", "status": "in_progress", "budget_share": 10.0},
				map[string]any{"id": "3", "title": "写报告", "status": "pending", "budget_share": 0.0},
			}},
		"findings":   FallbackStopFindings,
		"can_finish": true,
	}
	if !mapsEqualJSON(got, want) {
		t.Errorf("停止卡 = %s", b)
	}
	if FallbackStopFindings != "停止时模型正在处理，未能生成摘要。可以继续研究，或用已完成的部分立即写报告。" {
		t.Errorf("固定文案被改动：%q", FallbackStopFindings)
	}

	// 规划阶段即被强制结束：没有任何事件；额度没有宿主计数时取最后一条 budget 事件。
	b, err = FallbackTurnStopped([]json.RawMessage{json.RawMessage(`{"kind":"budget","data":{"used":2,"limit":20}}`)}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	want = map[string]any{"card": map[string]any{"subtopics_done": 0.0, "subtopics_total": 0.0, "sources": 0.0, "tool_calls_used": 2.0,
		"tool_call_limit": 20.0, "todo": []any{}}, "findings": FallbackStopFindings, "can_finish": false}
	if !mapsEqualJSON(got, want) {
		t.Errorf("空 turn 的停止卡 = %s", b)
	}
}

// TestHostTurnStoppedLooksLikeWorkerCard：宿主写的 turn_stopped 记录（host/turn_stopped）映射为与 Worker 停止卡相同的会话
// 事件（同一 type、同一字段允许列表，内部键去掉）；任务事件的用户视图同样可见。
func TestHostTurnStoppedLooksLikeWorkerCard(t *testing.T) {
	data := `{"card":{"subtopics_done":1,"subtopics_total":2,"sources":3,"tool_calls_used":4,"tool_call_limit":30,` +
		`"todo":[{"id":"1","title":"甲","status":"done","budget_share":10}]},"findings":"f","can_finish":true}`
	worker, ok := ToSessionEvent(sessionRec("worker", "progress", `{"step_id":"s1","kind":"turn_stopped","message":"研究已停止","data":`+data+`}`), false)
	if !ok {
		t.Fatal("Worker 停止卡不可见")
	}
	host, ok := ToSessionEvent(sessionRec("host", SEvTurnStopped, data), false)
	if !ok || host.Type != SEvTurnStopped || !bytes.Equal(host.Data, worker.Data) || host.TurnID != "t1" || host.Internal != nil {
		t.Fatalf("宿主停止卡 = %+v %s, %v；Worker 停止卡 %s", host, host.Data, ok, worker.Data)
	}
	leaky, ok := ToSessionEvent(sessionRec("host", SEvTurnStopped, `{"card":{"sources":1,"model":"kimi-k3","cost_micro":9},"findings":"f",`+
		`"can_finish":false,"attempt_id":"att_1"}`), false)
	if d := eventData(t, leaky); !ok || hasKey(d, "model") || hasKey(d, "cost_micro") || hasKey(d, "attempt_id") {
		t.Errorf("宿主停止卡泄露内部键：%s", leaky.Data)
	}
	adm, ok := ToSessionEvent(sessionRec("host", SEvTurnStopped, data), true)
	if !ok || adm.Type != SEvTurnStopped || adm.Internal == nil || adm.Internal.Source != "host" {
		t.Errorf("运维视图 = %+v, %v", adm, ok)
	}
	if ev, ok := userEvent(Event{Source: "host", Type: SEvTurnStopped, Payload: json.RawMessage(data)}); !ok || string(ev.Payload) != `{}` {
		t.Errorf("任务事件的用户视图 = %+v, %v", ev, ok)
	}
}

// TestStopCardData：复用已存储停止卡的 data（Worker 卡取 progress.data，宿主卡取 payload），只保留契约字段、去掉内部键。
func TestStopCardData(t *testing.T) {
	want := `{"can_finish":true,"card":{"sources":4},"findings":"真实发现"}`
	for source, payload := range map[string]string{
		"worker": `{"kind":"turn_stopped","step_id":"s","data":{"card":{"sources":4,"cost_micro":1},"findings":"真实发现","can_finish":true,"x":1}}`,
		"host":   `{"card":{"sources":4,"model":"m"},"findings":"真实发现","can_finish":true}`,
	} {
		b, err := StopCardData(source, []byte(payload))
		if err != nil || string(b) != want {
			t.Errorf("%s：%s, %v", source, b, err)
		}
	}
}

// ==== 停止修复 F2 段结束 ====
