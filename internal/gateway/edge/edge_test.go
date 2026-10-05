package edge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
)

const testSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// fakeCalls 记录 edge 交来的调用。invoke/open 为空时取默认行为。
type fakeCalls struct {
	mu      sync.Mutex
	invokes []call.Invoke
	checks  [][2]string // (taskID, attemptID)
	opens   [][2]string // (taskID, sha)
	cancels [][2]string // (attemptID, reason)
	access  call.Result
	budget  call.Budget

	invoke func(ctx context.Context, in call.Invoke) (call.Result, error)
	open   func(taskID, sha string) (io.ReadCloser, error)
}

func (f *fakeCalls) Invoke(ctx context.Context, in call.Invoke) (call.Result, error) {
	f.mu.Lock()
	f.invokes = append(f.invokes, in)
	fn := f.invoke
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return call.Result{Body: []byte(`{"ok":true}`), BlobSHA256: testSHA, Status: 200}, nil
}

func (f *fakeCalls) CheckAccess(_ context.Context, taskID, attemptID string) (call.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks = append(f.checks, [2]string{taskID, attemptID})
	return f.access, nil
}

func (f *fakeCalls) Budget(_ context.Context, _ string) (call.Budget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.budget, nil
}

func (f *fakeCalls) OpenBlob(_ context.Context, taskID, sha string) (io.ReadCloser, error) {
	f.mu.Lock()
	f.opens = append(f.opens, [2]string{taskID, sha})
	fn := f.open
	f.mu.Unlock()
	if fn != nil {
		return fn(taskID, sha)
	}
	return nil, blob.ErrNotFound
}

func (f *fakeCalls) CancelAttempt(attemptID, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, [2]string{attemptID, reason})
}

func (f *fakeCalls) snapshot() (invokes []call.Invoke, checks, opens, cancels [][2]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call.Invoke(nil), f.invokes...), append([][2]string(nil), f.checks...),
		append([][2]string(nil), f.opens...), append([][2]string(nil), f.cancels...)
}

type fakeAttempts map[string][2]string // attemptID → (taskID, envID)

func (f fakeAttempts) Lookup(_ context.Context, attemptID string) (string, string, error) {
	v, ok := f[attemptID]
	if !ok {
		return "", "", fmt.Errorf("attempt %s 不存在", attemptID)
	}
	return v[0], v[1], nil
}

var defaultAttempts = fakeAttempts{"a1": {"t1", "e1"}, "a2": {"t2", "e2"}}

func newEdge(t *testing.T, cfg Config, calls *fakeCalls) *Edge {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("edge 测试需要 Unix 平台的 AF_UNIX")
	}
	if cfg.SocketDir == "" {
		cfg.SocketDir = filepath.Join(t.TempDir(), "gateway")
	}
	e := New(cfg, calls, defaultAttempts)
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

func bind(t *testing.T, e *Edge, attemptID, envID string) string {
	t.Helper()
	path, err := e.Bind(context.Background(), attemptID, envID)
	if err != nil {
		t.Fatalf("Bind(%s): %v", attemptID, err)
	}
	return path
}

func unixClient(path string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		},
		DisableKeepAlives: true,
	}}
}

type reply struct {
	status int
	header http.Header
	body   []byte
}

func do(t *testing.T, path, method, url string, body io.Reader, hdr map[string]string) reply {
	t.Helper()
	req, err := http.NewRequest(method, "http://gw"+url, body)
	if err != nil {
		t.Errorf("NewRequest: %v", err)
		return reply{}
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := unixClient(path).Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, url, err)
		return reply{}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("读取 %s 响应: %v", url, err)
	}
	return reply{resp.StatusCode, resp.Header, data}
}

func errCode(t *testing.T, r reply) string {
	t.Helper()
	var eb errorBody
	if err := json.Unmarshal(r.body, &eb); err != nil {
		t.Errorf("错误体不是 JSON：%q: %v", r.body, err)
	}
	return eb.Error.Code
}

// watchClosed 设置 connStateHook（须在 Bind 之前），返回 StateClosed 事件（attempt_id）的通道。
func watchClosed(e *Edge) <-chan string {
	ch := make(chan string, 64)
	e.connStateHook = func(attemptID string, s http.ConnState) {
		if s == http.StateClosed {
			ch <- attemptID
		}
	}
	return ch
}

func waitClosed(t *testing.T, ch <-chan string, attemptID string) {
	t.Helper()
	select {
	case id := <-ch:
		if id != attemptID {
			t.Fatalf("StateClosed 来自 %s，期望 %s", id, attemptID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待连接关闭超时")
	}
}

func callHdr(id string) map[string]string { return map[string]string{HeaderCallID: id} }

// 原始 keep-alive 连接：发一个 GET /v1/budget 并读完响应，连接保持打开。
func rawBudget(c net.Conn, br *bufio.Reader) error {
	if _, err := io.WriteString(c, "GET /v1/budget HTTP/1.1\r\nHost: gw\r\n\r\n"); err != nil {
		return err
	}
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// 两个 attempt 各一个 socket：每个连接只以自己 socket 的 attempt/task/env 调用 Calls。
func TestBindingIsolatesAttempts(t *testing.T) {
	calls := &fakeCalls{}
	e := newEdge(t, Config{}, calls)
	p1, p2 := bind(t, e, "a1", "e1"), bind(t, e, "a2", "")
	if p1 != filepath.Join(e.cfg.SocketDir, "a1.sock") {
		t.Fatalf("socket 路径 = %s", p1)
	}
	for _, p := range []string{p1, p2} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode()&os.ModeSocket == 0 || st.Mode().Perm() != 0o600 {
			t.Errorf("%s mode = %v，期望 0600 socket", p, st.Mode())
		}
	}
	entries, err := os.ReadDir(e.cfg.SocketDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("SocketDir 残留临时文件：%v", entries)
	}

	r := do(t, p1, "POST", "/v1/search", strings.NewReader(`{"query":"q"}`), map[string]string{
		HeaderCallID: "root/s1/search/1", HeaderRetry: "true",
		HeaderSupersedes: "root/s0/search/1", HeaderSupersedeReason: "divergence",
	})
	if r.status != 200 || r.header.Get(HeaderBlob) != testSHA || r.header.Get(HeaderReplayed) != "" {
		t.Errorf("a1 search: %d %v %s", r.status, r.header, r.body)
	}
	r = do(t, p2, "POST", "/v1/chat/completions", strings.NewReader(`{"messages":[]}`), callHdr("root/s1/chat/1"))
	if r.status != 200 {
		t.Errorf("a2 chat: %d %s", r.status, r.body)
	}
	r = do(t, p2, "GET", "/v1/budget", nil, nil)
	if r.status != 200 {
		t.Errorf("a2 budget: %d %s", r.status, r.body)
	}

	invokes, checks, _, _ := calls.snapshot()
	want := []call.Invoke{
		{TaskID: "t1", AttemptID: "a1", EnvID: "e1", CallID: "root/s1/search/1", Kind: upstream.KindSearch,
			Body: []byte(`{"query":"q"}`), Retry: true, Supersedes: "root/s0/search/1", SupersedeReason: "divergence"},
		{TaskID: "t2", AttemptID: "a2", EnvID: "e2", CallID: "root/s1/chat/1", Kind: upstream.KindChat,
			Body: []byte(`{"messages":[]}`)},
	}
	if len(invokes) != len(want) {
		t.Fatalf("invokes = %+v", invokes)
	}
	for i := range want {
		got := invokes[i]
		if got.TaskID != want[i].TaskID || got.AttemptID != want[i].AttemptID || got.EnvID != want[i].EnvID ||
			got.CallID != want[i].CallID || got.Kind != want[i].Kind || !bytes.Equal(got.Body, want[i].Body) ||
			got.Retry != want[i].Retry || got.Supersedes != want[i].Supersedes || got.SupersedeReason != want[i].SupersedeReason {
			t.Errorf("invoke[%d] = %+v，期望 %+v", i, got, want[i])
		}
	}
	if len(checks) != 1 || checks[0] != [2]string{"t2", "a2"} {
		t.Errorf("CheckAccess = %v", checks)
	}
}

// Bind 的错误路径与 Close 的清理。
func TestBindLifecycle(t *testing.T) {
	calls := &fakeCalls{}
	dir := filepath.Join(t.TempDir(), "gateway")
	e := New(Config{SocketDir: dir}, calls, defaultAttempts)
	if runtime.GOOS == "windows" {
		t.Skip("edge 测试需要 Unix 平台的 AF_UNIX")
	}
	p := bind(t, e, "a1", "e1")
	if _, err := e.Bind(context.Background(), "a1", "e1"); !errors.Is(err, ErrAlreadyBound) {
		t.Errorf("重复 Bind: %v", err)
	}
	if _, err := e.Bind(context.Background(), "a2", "e1"); err == nil {
		t.Error("环境不一致的 Bind 应失败")
	}
	if _, err := e.Bind(context.Background(), "../x", ""); err == nil {
		t.Error("含路径分隔符的 attempt_id 应被拒绝")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Close 后 socket 仍存在：%v", err)
	}
	if _, err := e.Bind(context.Background(), "a2", "e2"); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 后 Bind: %v", err)
	}
	if _, _, _, cancels := calls.snapshot(); len(cancels) != 0 {
		t.Errorf("Close 不应调用 CancelAttempt：%v", cancels)
	}
}

// 第 17 个连接在接受后立即关闭；已接受的 16 个连接继续可用；释放一个连接后新连接可用。
func TestConnectionLimit(t *testing.T) {
	e := newEdge(t, Config{}, &fakeCalls{})
	closed := watchClosed(e)
	p := bind(t, e, "a1", "e1")
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			_ = c.Close() // 测试清理
		}
	}()
	for i := 0; i < DefaultMaxConns; i++ {
		c, err := net.Dial("unix", p)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		if err := rawBudget(c, bufio.NewReader(c)); err != nil {
			t.Fatalf("连接 %d: %v", i+1, err)
		}
	}
	c17, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err) // 内核完成握手；edge 在 accept 后关闭
	}
	defer c17.Close()
	if err := c17.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := rawBudget(c17, bufio.NewReader(c17)); err == nil {
		t.Fatal("第 17 个连接应被关闭")
	}
	// 已接受的 16 个连接不受影响。
	for i, c := range conns {
		if err := rawBudget(c, bufio.NewReader(c)); err != nil {
			t.Errorf("连接 %d 在拒绝第 17 个后失效：%v", i+1, err)
		}
	}
	// 关闭一个已接受的连接；edge 将其移出连接集合（StateClosed）后，新连接被接受并可服务。
	if err := conns[0].Close(); err != nil {
		t.Fatal(err)
	}
	conns = conns[1:]
	waitClosed(t, closed, "a1")
	c, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	conns = append(conns, c)
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := rawBudget(c, bufio.NewReader(c)); err != nil {
		t.Fatalf("释放连接后新连接未被接受：%v", err)
	}
}

// 第 33 个活跃请求得到 429；已在处理中的请求不受影响。
func TestActiveRequestLimit(t *testing.T) {
	entered, release := make(chan struct{}, 64), make(chan struct{})
	calls := &fakeCalls{invoke: func(ctx context.Context, _ call.Invoke) (call.Result, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return call.Result{}, ctx.Err()
		}
		return call.Result{Body: []byte(`{}`), Status: 200}, nil
	}}
	e := newEdge(t, Config{MaxConns: 64}, calls) // 连接上限放宽，使活跃请求上限可达
	p := bind(t, e, "a1", "e1")

	var wg sync.WaitGroup
	statuses := make(chan int, DefaultMaxActive)
	for i := 0; i < DefaultMaxActive; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := do(t, p, "POST", "/v1/fetch", strings.NewReader(`{}`), callHdr(fmt.Sprintf("root/s/fetch/%d", i)))
			statuses <- r.status
		}(i)
	}
	for i := 0; i < DefaultMaxActive; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("只有 %d 个请求进入 Invoke", i)
		}
	}
	r := do(t, p, "GET", "/v1/budget", nil, nil)
	if r.status != http.StatusTooManyRequests || errCode(t, r) != CodeTooManyRequests {
		t.Errorf("第 33 个请求: %d %s", r.status, r.body)
	}
	close(release)
	wg.Wait()
	close(statuses)
	for s := range statuses {
		if s != 200 {
			t.Errorf("活跃请求得到 %d", s)
		}
	}
	if r := do(t, p, "GET", "/v1/budget", nil, nil); r.status != 200 {
		t.Errorf("释放后请求: %d %s", r.status, r.body)
	}
}

// 请求体恰为 4 MiB 通过；多 1 字节得到 413 且不调用 Invoke（含声明长度与分块两种）。
func TestBodyLimit(t *testing.T) {
	calls := &fakeCalls{}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	exact := bytes.Repeat([]byte("a"), DefaultMaxBody)
	over := bytes.Repeat([]byte("a"), DefaultMaxBody+1)

	if r := do(t, p, "POST", "/v1/chat/completions", bytes.NewReader(exact), callHdr("c/1")); r.status != 200 {
		t.Errorf("4 MiB: %d %s", r.status, r.body)
	}
	if r := do(t, p, "POST", "/v1/chat/completions", bytes.NewReader(over), callHdr("c/2")); r.status != 413 ||
		errCode(t, r) != CodeRequestTooLarge {
		t.Errorf("4 MiB+1（Content-Length）: %d %s", r.status, r.body)
	}
	// io.MultiReader 不是已知长度的类型：客户端以分块编码发送，由 MaxBytesReader 截断。
	if r := do(t, p, "POST", "/v1/chat/completions", io.MultiReader(bytes.NewReader(over)), callHdr("c/3")); r.status != 413 ||
		errCode(t, r) != CodeRequestTooLarge {
		t.Errorf("4 MiB+1（chunked）: %d %s", r.status, r.body)
	}
	invokes, _, _, _ := calls.snapshot()
	if len(invokes) != 1 || len(invokes[0].Body) != DefaultMaxBody {
		t.Errorf("Invoke 次数 = %d", len(invokes))
	}
}

// 请求头与路由校验：拒绝发生在 Invoke 之前。
func TestRequestValidation(t *testing.T) {
	calls := &fakeCalls{}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	cases := []struct {
		name, method, url string
		hdr               map[string]string
		status            int
		code              string
	}{
		{"缺 call id", "POST", "/v1/search", nil, 400, CodeMissingCallID},
		{"call id 过长", "POST", "/v1/fetch", callHdr(strings.Repeat("x", MaxCallIDBytes+1)), 400, CodeMissingCallID},
		{"sub-run 头", "POST", "/v1/search", map[string]string{HeaderCallID: "c/1", HeaderSubrun: "sr1"}, 400, CodeSubrunUnsupported},
		{"exec 未实现", "POST", "/v1/exec", callHdr("c/1"), 501, CodeNotImplemented},
		{"方法不符", "GET", "/v1/chat/completions", callHdr("c/1"), 405, CodeMethodNotAllowed},
		{"未知端点", "POST", "/v1/other", callHdr("c/1"), 404, CodeNotFound},
	}
	for _, c := range cases {
		r := do(t, p, c.method, c.url, strings.NewReader(`{}`), c.hdr)
		if r.status != c.status || errCode(t, r) != c.code {
			t.Errorf("%s: %d %s，期望 %d %s", c.name, r.status, r.body, c.status, c.code)
		}
	}
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr(strings.Repeat("x", MaxCallIDBytes))); r.status != 200 {
		t.Errorf("256 字节 call id: %d %s", r.status, r.body)
	}
	if invokes, _, _, _ := calls.snapshot(); len(invokes) != 1 {
		t.Errorf("Invoke 次数 = %d，期望 1", len(invokes))
	}
}

// call.Result 的拒绝原样映射为状态与错误体；成功时带 blob 与重放头；内部错误不泄露细节。
func TestInvokeResultMapping(t *testing.T) {
	var mu sync.Mutex
	var next call.Result
	var nextErr error
	calls := &fakeCalls{invoke: func(context.Context, call.Invoke) (call.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		return next, nextErr
	}}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	set := func(r call.Result, err error) {
		mu.Lock()
		next, nextErr = r, err
		mu.Unlock()
	}

	set(call.Result{Status: 402, Code: "budget_exhausted"}, nil)
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c/1")); r.status != 402 || errCode(t, r) != "budget_exhausted" {
		t.Errorf("拒绝: %d %s", r.status, r.body)
	}
	set(call.Result{Body: []byte(`{"results":[]}`), Replayed: true, BlobSHA256: testSHA, Status: 200}, nil)
	r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c/1"))
	if r.status != 200 || string(r.body) != `{"results":[]}` || r.header.Get(HeaderBlob) != testSHA ||
		r.header.Get(HeaderReplayed) != "true" || r.header.Get("Content-Type") != "application/json" {
		t.Errorf("重放: %d %v %s", r.status, r.header, r.body)
	}
	set(call.Result{}, call.ErrClosed)
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c/2")); r.status != 503 || errCode(t, r) != CodeGatewayUnavailable {
		t.Errorf("协调器关闭: %d %s", r.status, r.body)
	}
	set(call.Result{}, errors.New("db: secret detail"))
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c/3")); r.status != 500 ||
		errCode(t, r) != CodeInternal || bytes.Contains(r.body, []byte("secret")) {
		t.Errorf("内部错误: %d %s", r.status, r.body)
	}
}

// Revoke：现有连接（空闲与处理中）被关闭、listener 不再接受、socket 删除、reason 透传给 CancelAttempt；
// 面向 Worker 的 Invoke 上下文随撤销结束。
func TestRevoke(t *testing.T) {
	entered := make(chan struct{}, 1)
	invokeErr := make(chan error, 1)
	calls := &fakeCalls{invoke: func(ctx context.Context, _ call.Invoke) (call.Result, error) {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			invokeErr <- ctx.Err()
			return call.Result{}, ctx.Err()
		case <-time.After(10 * time.Second):
			invokeErr <- nil
			return call.Result{Status: 200}, nil
		}
	}}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	p2 := bind(t, e, "a2", "e2")

	idle, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	idleBR := bufio.NewReader(idle)
	if err := rawBudget(idle, idleBR); err != nil {
		t.Fatal(err)
	}
	busy, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	if _, err := io.WriteString(busy, "POST /v1/search HTTP/1.1\r\nHost: gw\r\nX-Agentbox-Call-Id: c/1\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("请求未进入 Invoke")
	}

	if err := e.Revoke(context.Background(), "a1", call.ReasonCancel); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]net.Conn{"空闲连接": idle, "处理中连接": busy} {
		if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		n, err := c.Read(make([]byte, 1))
		var ne net.Error
		if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
			t.Errorf("%s 未被关闭：n=%d err=%v", name, n, err)
		}
	}
	select {
	case err := <-invokeErr:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("撤销后 Invoke 上下文 = %v，期望已取消", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("撤销后 Invoke 未返回")
	}
	if c, err := net.Dial("unix", p); err == nil {
		_ = c.Close() // 不应到达此处
		t.Error("撤销后仍能连接")
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("撤销后 socket 仍存在：%v", err)
	}
	// 另一个 attempt 不受影响。
	if r := do(t, p2, "GET", "/v1/budget", nil, nil); r.status != 200 {
		t.Errorf("a2 budget: %d %s", r.status, r.body)
	}
	// 非取消原因原样透传（是否取消 try 由协调器按原因决定）；未绑定的 attempt 幂等。
	if err := e.Revoke(context.Background(), "a2", "attempt_ended"); err != nil {
		t.Fatal(err)
	}
	if err := e.Revoke(context.Background(), "a1", call.ReasonCancel); err != nil {
		t.Fatal(err)
	}
	_, _, _, cancels := calls.snapshot()
	want := [][2]string{{"a1", "cancel"}, {"a2", "attempt_ended"}, {"a1", "cancel"}}
	if fmt.Sprint(cancels) != fmt.Sprint(want) {
		t.Errorf("CancelAttempt = %v，期望 %v", cancels, want)
	}
}

// Worker 在响应前断开：交给 Invoke 的上下文不被取消，Invoke 得以完成。
func TestClientDisconnectDoesNotCancelInvoke(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	done := make(chan error, 1) // Invoke 被释放时其 ctx 的状态
	calls := &fakeCalls{invoke: func(ctx context.Context, _ call.Invoke) (call.Result, error) {
		started <- struct{}{}
		<-release
		done <- ctx.Err()
		return call.Result{Body: []byte(`{}`), Status: 200}, nil
	}}
	e := newEdge(t, Config{}, calls)
	closed := watchClosed(e)
	p := bind(t, e, "a1", "e1")
	c, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, "POST /v1/chat/completions HTTP/1.1\r\nHost: gw\r\nX-Agentbox-Call-Id: c/1\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("请求未进入 Invoke")
	}
	// 顺序：Invoke 已开始 → Worker 断开 → edge 观察到断开并释放该连接（StateClosed；此时 net/http 已
	// 取消 r.Context()，若误把它交给 Invoke 则下方断言失败）→ 释放 Invoke → 断言其 ctx 未被取消。
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, closed, "a1")
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Invoke 完成时上下文 = %v，期望未取消", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Invoke 未完成")
	}
}

// /blobs/{sha}：先经访问检查，再流式返回，ETag 为 sha；拒绝与不存在不调用或不泄露。
func TestBlobEndpoint(t *testing.T) {
	first := bytes.Repeat([]byte("x"), 64<<10)
	rest := []byte("tail")
	gotFirst := make(chan struct{})
	calls := &fakeCalls{open: func(_, sha string) (io.ReadCloser, error) {
		if sha != testSHA {
			return nil, blob.ErrNotFound
		}
		pr, pw := io.Pipe()
		go func() {
			if _, err := pw.Write(first); err != nil {
				_ = pw.CloseWithError(err) // PipeWriter.CloseWithError 总是返回 nil
				return
			}
			// 客户端在 blob 写完之前就收到了前半部分，证明响应是流式的。
			select {
			case <-gotFirst:
			case <-time.After(5 * time.Second):
				_ = pw.CloseWithError(errors.New("客户端未在 blob 写完前收到数据")) // PipeWriter.CloseWithError 总是返回 nil
				return
			}
			if _, err := pw.Write(rest); err != nil {
				_ = pw.CloseWithError(err) // PipeWriter.CloseWithError 总是返回 nil
				return
			}
			if err := pw.Close(); err != nil {
				t.Errorf("关闭 pipe: %v", err)
			}
		}()
		return pr, nil
	}}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")

	resp, err := unixClient(p).Get("http://gw/blobs/" + testSHA)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("ETag") != `"`+testSHA+`"` {
		t.Fatalf("blob: %d ETag=%q", resp.StatusCode, resp.Header.Get("ETag"))
	}
	head := make([]byte, len(first))
	if _, err := io.ReadFull(resp.Body, head); err != nil {
		t.Fatal(err)
	}
	close(gotFirst)
	tail, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(head, first) || !bytes.Equal(tail, rest) {
		t.Errorf("blob 内容不符：tail=%q", tail)
	}

	other := strings.Repeat("f", 64)
	if r := do(t, p, "GET", "/blobs/"+other, nil, nil); r.status != 404 || errCode(t, r) != CodeNotFound {
		t.Errorf("不存在的 blob: %d %s", r.status, r.body)
	}
	if r := do(t, p, "GET", "/blobs/not-a-sha", nil, nil); r.status != 404 {
		t.Errorf("非法 sha: %d %s", r.status, r.body)
	}
	calls.mu.Lock()
	calls.access = call.Result{Status: 403, Code: "access_revoked"}
	calls.mu.Unlock()
	if r := do(t, p, "GET", "/blobs/"+testSHA, nil, nil); r.status != 403 || errCode(t, r) != "access_revoked" {
		t.Errorf("访问被拒: %d %s", r.status, r.body)
	}
	if r := do(t, p, "GET", "/v1/budget", nil, nil); r.status != 403 || errCode(t, r) != "access_revoked" {
		t.Errorf("budget 访问被拒: %d %s", r.status, r.body)
	}
	_, checks, opens, _ := calls.snapshot()
	if len(opens) != 2 || opens[0] != [2]string{"t1", testSHA} || opens[1] != [2]string{"t1", other} {
		t.Errorf("OpenBlob = %v（访问被拒与非法 sha 不应打开）", opens)
	}
	if len(checks) != 4 {
		t.Errorf("CheckAccess 次数 = %d，期望 4", len(checks))
	}
}

// /v1/budget 返回账本各项与可用额度。
func TestBudget(t *testing.T) {
	calls := &fakeCalls{budget: call.Budget{LimitMicro: 1000, ReservedMicro: 100, SpentMicro: 200, UnknownMicro: 50}}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	r := do(t, p, "GET", "/v1/budget", nil, nil)
	var got budgetBody
	if err := json.Unmarshal(r.body, &got); err != nil {
		t.Fatalf("%d %s: %v", r.status, r.body, err)
	}
	want := budgetBody{LimitMicro: 1000, ReservedMicro: 100, SpentMicro: 200, UnknownMicro: 50, AvailableMicro: 650}
	if r.status != 200 || got != want {
		t.Errorf("budget = %d %+v", r.status, got)
	}
}
