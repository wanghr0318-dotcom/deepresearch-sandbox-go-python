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
	"slices"
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
	// sub-run（Plan 14 Task 5）：subChecks 是每次 CheckAccess 的 subrunID；subBudgets 记录 SubrunBudget 的 subrunID。
	subChecks  []string
	subBudgets []string
	subBudget  call.SubrunBudget

	invoke func(ctx context.Context, in call.Invoke) (call.Result, error)
	open   func(taskID, sha string) (io.ReadCloser, error)

	// exec（Plan 15 Task 9）：execs 记录 Exec 的输入；exec 为空时返回 200 {"status":"completed"}。
	// execQuota 为 nil 表示 exec 未配置（ExecQuota 返回 call.ErrExecNotConfigured）。
	execs      []call.ExecInvoke
	exec       func(ctx context.Context, in call.ExecInvoke) (call.Result, error)
	execQuota  *call.ExecQuota
	execHasRow bool
}

func (f *fakeCalls) Exec(ctx context.Context, in call.ExecInvoke) (call.Result, error) {
	f.mu.Lock()
	f.execs = append(f.execs, in)
	fn := f.exec
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return call.Result{Body: []byte(`{"status":"completed"}`), BlobSHA256: testSHA, Status: 200}, nil
}

func (f *fakeCalls) ExecQuota(_ context.Context, _ string) (call.ExecQuota, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.execQuota == nil {
		return call.ExecQuota{}, false, call.ErrExecNotConfigured
	}
	return *f.execQuota, f.execHasRow, nil
}

func (f *fakeCalls) execSnapshot() []call.ExecInvoke {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call.ExecInvoke(nil), f.execs...)
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

func (f *fakeCalls) CheckAccess(_ context.Context, taskID, attemptID, subrunID string) (call.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks = append(f.checks, [2]string{taskID, attemptID})
	f.subChecks = append(f.subChecks, subrunID)
	return f.access, nil
}

func (f *fakeCalls) Budget(_ context.Context, _ string) (call.Budget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.budget, nil
}

func (f *fakeCalls) SubrunBudget(_ context.Context, _, subrunID string) (call.SubrunBudget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subBudgets = append(f.subBudgets, subrunID)
	return f.subBudget, nil
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

var defaultAttempts = fakeAttempts{"a1": {"t1", "e1"}, "a2": {"t2", "e2"}, "a3": {"t3", "e1"}}

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
		HeaderSupersedes: "root/s0/search/1", HeaderSupersedeReason: "divergence", HeaderCache: " No-Cache ",
	})
	if r.status != 200 || r.header.Get(HeaderBlob) != testSHA || r.header.Get(HeaderReplayed) != "" {
		t.Errorf("a1 search: %d %v %s", r.status, r.header, r.body)
	}
	// 缓存指令只接受 no-cache；其他取值在调用 call 之前以 400 拒绝。
	r = do(t, p1, "POST", "/v1/search", strings.NewReader(`{"query":"q"}`), map[string]string{
		HeaderCallID: "root/s1/search/2", HeaderCache: "only-if-cached",
	})
	if r.status != 400 || !strings.Contains(string(r.body), CodeInvalidRequest) {
		t.Errorf("未知缓存指令：%d %s", r.status, r.body)
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
			Body: []byte(`{"query":"q"}`), Retry: true, Supersedes: "root/s0/search/1", SupersedeReason: "divergence", NoCache: true},
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
			got.Retry != want[i].Retry || got.Supersedes != want[i].Supersedes || got.SupersedeReason != want[i].SupersedeReason ||
			got.NoCache != want[i].NoCache {
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

// TestEnvHeldUntilSocketRemoved：EnvHeld 覆盖入口 socket 的整个生命周期——Bind 之后为真；Revoke 把入口移出
// 映射之后、socket 删除之前仍为真（resource 据此推迟 UID 范围归还，见 resource.Options.EntryHeld）；删除之后为
// 假。incarnation 入口相同。同一环境的另一入口仍在时保持为真。
func TestEnvHeldUntilSocketRemoved(t *testing.T) {
	e := newEdge(t, Config{}, &fakeCalls{})
	var during []bool
	env := "e1" // 正在撤销的入口所属环境
	e.removeHook = func(path string) {
		_, err := os.Stat(path)
		during = append(during, err == nil && e.EnvHeld(env))
	}
	if e.EnvHeld("e1") {
		t.Fatal("未绑定时 EnvHeld(e1) 为真")
	}
	p1 := bind(t, e, "a1", "e1")
	bind(t, e, "a3", "e1")
	if !e.EnvHeld("e1") || e.EnvHeld("e2") {
		t.Fatalf("绑定后 EnvHeld(e1)=%v EnvHeld(e2)=%v", e.EnvHeld("e1"), e.EnvHeld("e2"))
	}
	if err := e.Revoke(context.Background(), "a1", "attempt_stopping"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Revoke 后 socket 仍存在：%v", err)
	}
	if !e.EnvHeld("e1") {
		t.Fatal("同一环境的 a3 仍绑定时 EnvHeld(e1) 为假")
	}
	if err := e.Revoke(context.Background(), "a3", "attempt_stopping"); err != nil {
		t.Fatal(err)
	}
	if e.EnvHeld("e1") {
		t.Fatal("全部入口撤销后 EnvHeld(e1) 仍为真")
	}

	env = "e2"
	if _, err := e.BindIncarnation(context.Background(), "i1", "e2"); err != nil {
		t.Fatal(err)
	}
	if !e.EnvHeld("e2") {
		t.Fatal("incarnation 入口绑定后 EnvHeld(e2) 为假")
	}
	if err := e.RevokeIncarnation(context.Background(), "i1"); err != nil {
		t.Fatal(err)
	}
	if e.EnvHeld("e2") {
		t.Fatal("incarnation 入口撤销后 EnvHeld(e2) 仍为真")
	}
	if !slices.Equal(during, []bool{true, true, true}) {
		t.Fatalf("删除 socket 之前（socket 存在且 EnvHeld 为真）= %v，期望三次均为真", during)
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
		{"sub-run ID 非法", "POST", "/v1/search", map[string]string{HeaderCallID: "Sr1/1", HeaderSubrun: "Sr1"}, 400, CodeInvalidRequest},
		{"call id 不以 sub-run 开头", "POST", "/v1/search", map[string]string{HeaderCallID: "c/1", HeaderSubrun: "sr1"}, 400, CodeInvalidRequest},
		{"exec 缺 call id", "POST", "/v1/exec", nil, 400, CodeMissingCallID},
		{"exec 方法不符", "GET", "/v1/exec", callHdr("c/1"), 405, CodeMethodNotAllowed},
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

// 工具调用额度（M4 Plan 12 Task 4；契约 E）：搜索与抓取的响应（含 429 与重放）带 X-Agentbox-Tool-Budget；不限时不写；
// /v1/budget 带 tool_calls_used 与 tool_call_limit（不限为 null）。
func TestToolBudgetHeaderAndBudgetFields(t *testing.T) {
	var mu sync.Mutex
	var next call.Result
	limit := int64(30)
	calls := &fakeCalls{budget: call.Budget{LimitMicro: 1000, ToolCallLimit: &limit, ToolCallsUsed: 3},
		invoke: func(context.Context, call.Invoke) (call.Result, error) {
			mu.Lock()
			defer mu.Unlock()
			return next, nil
		}}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	set := func(r call.Result) {
		mu.Lock()
		next = r
		mu.Unlock()
	}

	set(call.Result{Body: []byte(`{"results":[]}`), BlobSHA256: testSHA, Status: 200, ToolBudget: &call.ToolBudget{Used: 3, Limit: 30}})
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c1")); r.status != 200 || r.header.Get(HeaderToolBudget) != "3/30" {
		t.Errorf("搜索: %d %v", r.status, r.header)
	}
	set(call.Result{Status: 429, Code: "tool_budget_exhausted", ToolBudget: &call.ToolBudget{Used: 30, Limit: 30}})
	if r := do(t, p, "POST", "/v1/fetch", strings.NewReader(`{}`), callHdr("c2")); r.status != 429 ||
		errCode(t, r) != "tool_budget_exhausted" || r.header.Get(HeaderToolBudget) != "30/30" {
		t.Errorf("429: %d %v %s", r.status, r.header, r.body)
	}
	set(call.Result{Body: []byte(`{}`), Replayed: true, BlobSHA256: testSHA, Status: 200, ToolBudget: &call.ToolBudget{Used: 30, Limit: 30}})
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c1")); r.header.Get(HeaderToolBudget) != "30/30" ||
		r.header.Get(HeaderReplayed) != "true" {
		t.Errorf("重放: %d %v", r.status, r.header)
	}
	set(call.Result{Body: []byte(`{}`), BlobSHA256: testSHA, Status: 200})
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("c3")); r.header.Values(HeaderToolBudget) != nil {
		t.Errorf("不限时不应写额度头: %v", r.header)
	}

	r := do(t, p, "GET", "/v1/budget", nil, nil)
	var got map[string]any
	if err := json.Unmarshal(r.body, &got); err != nil {
		t.Fatalf("%d %s: %v", r.status, r.body, err)
	}
	if got["tool_calls_used"] != float64(3) || got["tool_call_limit"] != float64(30) {
		t.Errorf("/v1/budget = %s", r.body)
	}
	calls.mu.Lock()
	calls.budget = call.Budget{LimitMicro: 1000}
	calls.mu.Unlock()
	r = do(t, p, "GET", "/v1/budget", nil, nil)
	got = nil
	if err := json.Unmarshal(r.body, &got); err != nil {
		t.Fatalf("%d %s: %v", r.status, r.body, err)
	}
	if v, ok := got["tool_call_limit"]; !ok || v != nil || got["tool_calls_used"] != float64(0) {
		t.Errorf("不限时 tool_call_limit 应为 null：%s", r.body)
	}
}

// ==== M4 Plan 12 Task 5：session incarnation 入口（§9.1）====

// expectRefused 断言新连接被接受后立即关闭（得不到任何 HTTP 响应）。
func expectRefused(t *testing.T, path, why string) {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Errorf("%s：dial %v（期望可连接、随即被关闭）", why, err)
		return
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// 写入可能因对端已关闭而失败，以读取结果为准。
	_, _ = io.WriteString(c, "GET /v1/budget HTTP/1.1\r\nHost: gw\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	var ne net.Error
	switch {
	case err == nil:
		resp.Body.Close()
		t.Errorf("%s：连接未被拒绝，得到 %d", why, resp.StatusCode)
	case errors.As(err, &ne) && ne.Timeout():
		t.Errorf("%s：连接未被关闭（读超时）", why)
	}
}

// incarnation 入口：未 Attach 时拒绝连接；Attach(A) 后连接与请求归 A；Detach(A, cancel) 关闭 A 的连接、结束其
// 在途请求后转交 CancelAttempt(A, cancel)，之后拒绝新连接；Attach(B) 后请求归 B（E30：无 T1 调用计到 T2）；
// RevokeIncarnation 删除 socket（幂等）。
func TestIncarnationListener(t *testing.T) {
	entered := make(chan string, 4)
	var mu sync.Mutex
	var events []string // Invoke 返回与 CancelAttempt 的顺序
	calls := &fakeCalls{invoke: func(ctx context.Context, in call.Invoke) (call.Result, error) {
		if in.CallID != "block" {
			return call.Result{Body: []byte(`{}`), Status: 200}, nil
		}
		entered <- in.AttemptID
		<-ctx.Done()
		mu.Lock()
		events = append(events, "invoke_returned:"+in.AttemptID)
		mu.Unlock()
		return call.Result{}, ctx.Err()
	}}
	e := newEdge(t, Config{}, calls)
	ctx := context.Background()

	p, err := e.BindIncarnation(ctx, "i1", "e1")
	if err != nil {
		t.Fatal(err)
	}
	if p != filepath.Join(e.cfg.SocketDir, "inc-i1.sock") {
		t.Fatalf("socket 路径 = %s", p)
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode()&os.ModeSocket == 0 || st.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v，期望 0600 socket", st.Mode())
	}
	if _, err := e.BindIncarnation(ctx, "i1", "e1"); !errors.Is(err, ErrAlreadyBound) {
		t.Errorf("重复 BindIncarnation = %v，期望 ErrAlreadyBound", err)
	}
	if _, err := e.BindIncarnation(ctx, "../x", "e1"); err == nil {
		t.Error("非法 incarnation_id 应被拒绝")
	}

	// 空闲（未 Attach）：拒绝连接。
	expectRefused(t, p, "未 Attach")
	if err := e.Attach(ctx, "nope", "a1"); err == nil {
		t.Error("未绑定的 incarnation 上 Attach 应失败")
	}
	if err := e.Attach(ctx, "i1", "a2"); err == nil || errors.Is(err, ErrAlreadyBound) {
		t.Errorf("环境不符的 Attach = %v，期望环境不符错误", err)
	}
	if err := e.Attach(ctx, "i1", "a1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Attach(ctx, "i1", "a3"); !errors.Is(err, ErrAlreadyBound) {
		t.Errorf("已有 Attach 时 Attach = %v，期望 ErrAlreadyBound", err)
	}

	// A 的空闲 keep-alive 连接与一个在途请求。
	idle, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	if err := rawBudget(idle, bufio.NewReader(idle)); err != nil {
		t.Fatal(err)
	}
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("s1")); r.status != 200 {
		t.Errorf("A search: %d %s", r.status, r.body)
	}
	busy, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	if _, err := io.WriteString(busy, "POST /v1/search HTTP/1.1\r\nHost: gw\r\nX-Agentbox-Call-Id: block\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-entered:
		if id != "a1" {
			t.Fatalf("在途请求归 %s", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("请求未进入 Invoke")
	}

	if err := e.Detach(ctx, "i1", "a1", call.ReasonCancel); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	_, _, _, cancels := calls.snapshot()
	events = append(events, fmt.Sprint(cancels))
	got := fmt.Sprint(events)
	mu.Unlock()
	if got != "[invoke_returned:a1 [[a1 cancel]]]" {
		t.Errorf("Detach 返回时：%s（期望在途请求先结束，再转交 CancelAttempt）", got)
	}
	for name, c := range map[string]net.Conn{"空闲连接": idle, "处理中连接": busy} {
		if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		n, err := c.Read(make([]byte, 1))
		var ne net.Error
		if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
			t.Errorf("Detach 后%s未被关闭：n=%d err=%v", name, n, err)
		}
	}
	expectRefused(t, p, "Detach 之后")
	// 幂等：重复 Detach 不报错（原因仍转交）。
	if err := e.Detach(ctx, "i1", "a1", call.ReasonCancel); err != nil {
		t.Errorf("重复 Detach: %v", err)
	}

	// Attach(B)：之后的请求只归 B。
	if err := e.Attach(ctx, "i1", "a3"); err != nil {
		t.Fatal(err)
	}
	if r := do(t, p, "POST", "/v1/fetch", strings.NewReader(`{}`), callHdr("f1")); r.status != 200 {
		t.Errorf("B fetch: %d %s", r.status, r.body)
	}
	if r := do(t, p, "GET", "/v1/budget", nil, nil); r.status != 200 {
		t.Errorf("B budget: %d %s", r.status, r.body)
	}
	// 迟到的、针对旧 attempt 的 Detach 不影响 B。
	if err := e.Detach(ctx, "i1", "a1", "attempt_ended"); err != nil {
		t.Fatal(err)
	}
	if r := do(t, p, "GET", "/v1/budget", nil, nil); r.status != 200 {
		t.Errorf("旧 attempt 的 Detach 影响了 B：%d %s", r.status, r.body)
	}
	invokes, checks, _, _ := calls.snapshot()
	var seen []string
	for _, in := range invokes {
		seen = append(seen, in.CallID+"="+in.TaskID+"/"+in.AttemptID+"/"+in.EnvID)
	}
	if fmt.Sprint(seen) != "[s1=t1/a1/e1 block=t1/a1/e1 f1=t3/a3/e1]" {
		t.Errorf("Invoke 归属 = %v", seen)
	}
	if fmt.Sprint(checks) != "[[t1 a1] [t3 a3] [t3 a3]]" {
		t.Errorf("CheckAccess = %v", checks)
	}

	// RevokeIncarnation：关闭 B 的连接与 listener、删除 socket；幂等；之后 Attach 失败。
	bconn, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer bconn.Close()
	if err := rawBudget(bconn, bufio.NewReader(bconn)); err != nil {
		t.Fatal(err)
	}
	if err := e.RevokeIncarnation(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	if err := bconn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := bconn.Read(make([]byte, 1)); err == nil {
		t.Error("RevokeIncarnation 后连接未关闭")
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("RevokeIncarnation 后 socket 仍存在：%v", err)
	}
	if c, err := net.Dial("unix", p); err == nil {
		_ = c.Close() // 不应到达此处
		t.Error("RevokeIncarnation 后仍能连接")
	}
	if err := e.RevokeIncarnation(ctx, "i1"); err != nil {
		t.Errorf("重复 RevokeIncarnation: %v", err)
	}
	if err := e.Attach(ctx, "i1", "a3"); err == nil {
		t.Error("撤销后 Attach 应失败")
	}
}

// Detach 等待在途请求以 ctx 为期限：超时返回 ctx 错误，但 CancelAttempt 仍转交、新连接仍被拒绝；
// Close 删除 incarnation socket，之后 BindIncarnation 为 ErrClosed。
func TestIncarnationDetachDeadline(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	calls := &fakeCalls{invoke: func(context.Context, call.Invoke) (call.Result, error) {
		entered <- struct{}{}
		<-release // 不理会 ctx 的 Invoke
		return call.Result{Body: []byte(`{}`), Status: 200}, nil
	}}
	if runtime.GOOS == "windows" {
		t.Skip("edge 测试需要 Unix 平台的 AF_UNIX")
	}
	e := New(Config{SocketDir: filepath.Join(t.TempDir(), "gateway")}, calls, defaultAttempts)
	ctx := context.Background()
	p, err := e.BindIncarnation(ctx, "i1", "e1")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Attach(ctx, "i1", "a1"); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := io.WriteString(c, "POST /v1/chat/completions HTTP/1.1\r\nHost: gw\r\nX-Agentbox-Call-Id: c/1\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("请求未进入 Invoke")
	}
	dctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := e.Detach(dctx, "i1", "a1", "attempt_ended"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Detach = %v，期望 DeadlineExceeded", err)
	}
	if _, _, _, cancels := calls.snapshot(); fmt.Sprint(cancels) != "[[a1 attempt_ended]]" {
		t.Errorf("CancelAttempt = %v", cancels)
	}
	expectRefused(t, p, "Detach 超时之后")
	close(release)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Close 后 socket 仍存在：%v", err)
	}
	if _, err := e.BindIncarnation(ctx, "i2", "e1"); !errors.Is(err, ErrClosed) {
		t.Errorf("Close 后 BindIncarnation = %v，期望 ErrClosed", err)
	}
}

// ==== M4 Plan 14 Task 5：X-Agentbox-Subrun（规格 §9.2、§9.3） ====

// 计费调用：sub-run ID 须合规、call id 须以 <subrun_id>/ 开头（否则 400 invalid_request，不调用 Invoke）；合法时
// SubrunID 进入 Invoke；不带头为 root。sub-run 的拒绝（409 subrun_closed、402 subrun_budget_exhausted）原样映射。
func TestSubrunHeaderOnBillable(t *testing.T) {
	var mu sync.Mutex
	var next call.Result
	calls := &fakeCalls{invoke: func(context.Context, call.Invoke) (call.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		if next.Status == 0 {
			return call.Result{Body: []byte(`{}`), BlobSHA256: testSHA, Status: 200}, nil
		}
		return next, nil
	}}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	hdr := func(sr, id string) map[string]string { return map[string]string{HeaderSubrun: sr, HeaderCallID: id} }
	bad := []map[string]string{
		hdr("root", "root/s/1"),                 // 保留名
		hdr("ST1", "ST1/s/1"),                   // 大写
		hdr("-st1", "-st1/s/1"),                 // 首字符
		hdr(strings.Repeat("a", 33), "a/s/1"),   // 超过 32 字节
		hdr("st1", "root/s/1"),                  // 前缀不符
		hdr("st1", "st1x/s/1"),                  // 只是字符串前缀
		hdr("st1", "st1/"),                      // 前缀之后为空
		hdr("st1", "st1"),                       // 没有分隔符
		{HeaderSubrun: "st1", HeaderCallID: ""}, // 缺 call id 仍为 missing_call_id（下面单独断言）
	}
	for i, h := range bad[:len(bad)-1] {
		if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), h); r.status != 400 || errCode(t, r) != CodeInvalidRequest {
			t.Errorf("#%d %v: %d %s", i, h, r.status, r.body)
		}
	}
	if r := do(t, p, "POST", "/v1/fetch", strings.NewReader(`{}`), bad[len(bad)-1]); r.status != 400 || errCode(t, r) != CodeMissingCallID {
		t.Errorf("缺 call id: %d %s", r.status, r.body)
	}
	if invokes, _, _, _ := calls.snapshot(); len(invokes) != 0 {
		t.Fatalf("被拒的请求不应调用 Invoke：%+v", invokes)
	}
	if r := do(t, p, "POST", "/v1/chat/completions", strings.NewReader(`{}`), hdr("st1", "st1/s/chat/1")); r.status != 200 {
		t.Fatalf("合法 sub-run: %d %s", r.status, r.body)
	}
	if r := do(t, p, "POST", "/v1/search", strings.NewReader(`{}`), callHdr("root/s/1")); r.status != 200 {
		t.Fatalf("root: %d %s", r.status, r.body)
	}
	invokes, _, _, _ := calls.snapshot()
	if len(invokes) != 2 || invokes[0].SubrunID != "st1" || invokes[0].CallID != "st1/s/chat/1" || invokes[1].SubrunID != "" {
		t.Fatalf("Invoke = %+v", invokes)
	}
	for _, c := range []struct {
		status int
		code   string
	}{{409, "subrun_closed"}, {402, "subrun_budget_exhausted"}} {
		mu.Lock()
		next = call.Result{Status: c.status, Code: c.code}
		mu.Unlock()
		if r := do(t, p, "POST", "/v1/fetch", strings.NewReader(`{}`), hdr("st1", "st1/f/1")); r.status != c.status || errCode(t, r) != c.code {
			t.Errorf("%s: %d %s", c.code, r.status, r.body)
		}
	}
}

// 只读端点：/v1/budget 与 /blobs/{sha} 带 X-Agentbox-Subrun 时不要求 call id（SDK 的 sub-run 视图在这两个端点
// 也带该头），sub-run 进入 CheckAccess；/v1/budget 附加 sub-run 层（无上限时 cap 与 available 为 null）；不带头时
// 响应与旧版逐字节一致；非法 ID → 400；访问被拒（409 subrun_closed）原样返回。
func TestSubrunHeaderOnReadOnly(t *testing.T) {
	capMicro := int64(400)
	calls := &fakeCalls{
		budget:    call.Budget{LimitMicro: 1000, ReservedMicro: 100, SpentMicro: 200, UnknownMicro: 50},
		subBudget: call.SubrunBudget{CapMicro: &capMicro, ReservedMicro: 100, SpentMicro: 50, UnknownMicro: 25},
		open: func(_, sha string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("blob")), nil
		},
	}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	sub := map[string]string{HeaderSubrun: "st1"}

	r := do(t, p, "GET", "/v1/budget", nil, nil)
	const old = `{"limit_micro":1000,"reserved_micro":100,"spent_micro":200,"unknown_micro":50,"available_micro":650,"tool_calls_used":0,"tool_call_limit":null}`
	if r.status != 200 || string(r.body) != old {
		t.Errorf("不带头的 /v1/budget 应与旧响应一致：%d %s", r.status, r.body)
	}
	r = do(t, p, "GET", "/v1/budget", nil, sub)
	want := `{"limit_micro":1000,"reserved_micro":100,"spent_micro":200,"unknown_micro":50,"available_micro":650,"tool_calls_used":0,"tool_call_limit":null,` +
		`"subrun":{"cap_micro":400,"reserved_micro":100,"spent_micro":50,"unknown_micro":25,"available_micro":225}}`
	if r.status != 200 || string(r.body) != want {
		t.Errorf("带头的 /v1/budget：%d %s", r.status, r.body)
	}
	calls.mu.Lock()
	calls.subBudget = call.SubrunBudget{SpentMicro: 10}
	calls.mu.Unlock()
	r = do(t, p, "GET", "/v1/budget", nil, sub)
	if !strings.HasSuffix(string(r.body), `"subrun":{"cap_micro":null,"reserved_micro":0,"spent_micro":10,"unknown_micro":0,"available_micro":null}}`) {
		t.Errorf("无上限的 sub-run 层：%s", r.body)
	}
	if r := do(t, p, "GET", "/blobs/"+testSHA, nil, sub); r.status != 200 || string(r.body) != "blob" {
		t.Errorf("带头的 blob：%d %s", r.status, r.body)
	}
	for _, path := range []string{"/v1/budget", "/blobs/" + testSHA} {
		if r := do(t, p, "GET", path, nil, map[string]string{HeaderSubrun: "root"}); r.status != 400 || errCode(t, r) != CodeInvalidRequest {
			t.Errorf("%s 非法 sub-run：%d %s", path, r.status, r.body)
		}
	}
	calls.mu.Lock()
	calls.access = call.Result{Status: 409, Code: "subrun_closed"}
	calls.mu.Unlock()
	for _, path := range []string{"/v1/budget", "/blobs/" + testSHA} {
		if r := do(t, p, "GET", path, nil, sub); r.status != 409 || errCode(t, r) != "subrun_closed" {
			t.Errorf("%s 已关闭的 sub-run：%d %s", path, r.status, r.body)
		}
	}
	calls.mu.Lock()
	defer calls.mu.Unlock()
	if fmt.Sprint(calls.subChecks) != "[ st1 st1 st1 st1 st1]" {
		t.Errorf("CheckAccess 的 sub-run = %q", calls.subChecks)
	}
	if fmt.Sprint(calls.subBudgets) != "[st1 st1]" {
		t.Errorf("SubrunBudget 调用 = %q（访问被拒与不带头时不读 sub-run 层）", calls.subBudgets)
	}
}

// ==== M4 Plan 15 Task 9：POST /v1/exec 与 /v1/budget 的 exec 配额（规格 §10、§9.3） ====

// /v1/exec 与计费端点同样要求 call id、接受 Retry 与 Supersedes、校验 sub-run 前缀，并把它们交给 Calls.Exec
// （不经 Invoke）；exec 没有缓存，带 X-Agentbox-Cache 为 400；成功响应带结果 blob 与重放头。
func TestExecEndpoint(t *testing.T) {
	var mu sync.Mutex
	replayed := false
	calls := &fakeCalls{exec: func(context.Context, call.ExecInvoke) (call.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		return call.Result{Body: []byte(`{"status":"completed","exit_code":0}`), BlobSHA256: testSHA, Replayed: replayed, Status: 200}, nil
	}}
	e := newEdge(t, Config{MaxBody: 1024}, calls)
	p := bind(t, e, "a1", "e1")
	body := `{"language":"python3","code":"print(1)"}`

	r := do(t, p, "POST", "/v1/exec", strings.NewReader(body), callHdr("root/s1/exec/1"))
	if r.status != 200 || string(r.body) != `{"status":"completed","exit_code":0}` || r.header.Get(HeaderBlob) != testSHA ||
		r.header.Get(HeaderReplayed) != "" || r.header.Get("Content-Type") != "application/json" {
		t.Fatalf("exec: %d %v %s", r.status, r.header, r.body)
	}
	mu.Lock()
	replayed = true
	mu.Unlock()
	r = do(t, p, "POST", "/v1/exec", strings.NewReader(body), map[string]string{
		HeaderCallID: "st1/s1/exec/1", HeaderSubrun: "st1", HeaderRetry: "true",
		HeaderSupersedes: "st1/s1/exec/0", HeaderSupersedeReason: "divergence",
	})
	if r.status != 200 || r.header.Get(HeaderReplayed) != "true" {
		t.Fatalf("重放: %d %v %s", r.status, r.header, r.body)
	}
	for name, h := range map[string]map[string]string{
		"缓存指令":         {HeaderCallID: "root/s1/exec/2", HeaderCache: "no-cache"},
		"sub-run 前缀不符": {HeaderCallID: "root/s1/exec/2", HeaderSubrun: "st1"},
	} {
		if r := do(t, p, "POST", "/v1/exec", strings.NewReader(body), h); r.status != 400 || errCode(t, r) != CodeInvalidRequest {
			t.Errorf("%s: %d %s", name, r.status, r.body)
		}
	}
	if r := do(t, p, "POST", "/v1/exec", strings.NewReader(strings.Repeat("x", 2048)), callHdr("root/s1/exec/3")); r.status != 413 ||
		errCode(t, r) != CodeRequestTooLarge {
		t.Errorf("请求体超限: %d %s", r.status, r.body)
	}

	execs := calls.execSnapshot()
	if len(execs) != 2 {
		t.Fatalf("Exec 次数 = %d，期望 2：%+v", len(execs), execs)
	}
	want0 := call.ExecInvoke{TaskID: "t1", AttemptID: "a1", CallID: "root/s1/exec/1", Body: []byte(body)}
	want1 := call.ExecInvoke{TaskID: "t1", AttemptID: "a1", CallID: "st1/s1/exec/1", SubrunID: "st1", Body: []byte(body),
		Retry: true, Supersedes: "st1/s1/exec/0", SupersedeReason: "divergence"}
	for i, want := range []call.ExecInvoke{want0, want1} {
		got := execs[i]
		if got.TaskID != want.TaskID || got.AttemptID != want.AttemptID || got.CallID != want.CallID || got.SubrunID != want.SubrunID ||
			string(got.Body) != string(want.Body) || got.Retry != want.Retry || got.Supersedes != want.Supersedes ||
			got.SupersedeReason != want.SupersedeReason {
			t.Errorf("Exec #%d = %+v，期望 %+v", i, got, want)
		}
	}
	if invokes, _, _, _ := calls.snapshot(); len(invokes) != 0 {
		t.Errorf("exec 不应经 Invoke：%+v", invokes)
	}
}

// exec 的拒绝与失败（Task 8 的 Result.Status/Code）原样映射为状态与错误体，不带结果 blob 头；内部错误为 500，
// 协调器关闭为 503。
func TestExecResultMapping(t *testing.T) {
	var mu sync.Mutex
	var next call.Result
	var nextErr error
	calls := &fakeCalls{exec: func(context.Context, call.ExecInvoke) (call.Result, error) {
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
	cases := []struct {
		status int
		code   string
	}{
		{402, call.CodeExecQuotaExhausted}, {402, call.CodeExecCPUExhausted}, {402, call.CodeExecWallExhausted}, {402, call.CodeExecBlocked},
		{403, call.CodeInputNotAuthorized},
		{409, "call_in_progress"}, {409, call.CodeExecCancelled}, {409, "fingerprint_mismatch"},
		{502, call.CodeExecStartFailed}, {502, call.CodeExecUnknown},
		{503, call.CodeExecEnvUnavailable},
		{504, "call_deadline_exceeded"}, {504, call.CodeExecQueueTimeout},
		{400, call.CodeInputsTooLarge}, {404, call.CodeEndpointNotConfigured},
	}
	for i, c := range cases {
		set(call.Result{Status: c.status, Code: c.code}, nil)
		r := do(t, p, "POST", "/v1/exec", strings.NewReader(`{}`), callHdr(fmt.Sprintf("root/s/exec/%d", i+1)))
		if r.status != c.status || errCode(t, r) != c.code || r.header.Get(HeaderBlob) != "" {
			t.Errorf("%s: %d %v %s，期望 %d", c.code, r.status, r.header, r.body, c.status)
		}
	}
	set(call.Result{}, call.ErrClosed)
	if r := do(t, p, "POST", "/v1/exec", strings.NewReader(`{}`), callHdr("root/s/exec/99")); r.status != 503 || errCode(t, r) != CodeGatewayUnavailable {
		t.Errorf("协调器关闭: %d %s", r.status, r.body)
	}
	set(call.Result{}, errors.New("db: secret detail"))
	if r := do(t, p, "POST", "/v1/exec", strings.NewReader(`{}`), callHdr("root/s/exec/100")); r.status != 500 ||
		errCode(t, r) != CodeInternal || bytes.Contains(r.body, []byte("secret")) {
		t.Errorf("内部错误: %d %s", r.status, r.body)
	}
}

// Worker 在 exec 响应前断开：交给 Exec 的上下文不被取消（exec 继续并结算，供同 ID 重放）。
func TestClientDisconnectDoesNotCancelExec(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	done := make(chan error, 1)
	calls := &fakeCalls{exec: func(ctx context.Context, _ call.ExecInvoke) (call.Result, error) {
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
	if _, err := io.WriteString(c, "POST /v1/exec HTTP/1.1\r\nHost: gw\r\nX-Agentbox-Call-Id: root/s/exec/1\r\nContent-Length: 2\r\n\r\n{}"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("请求未进入 Exec")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, closed, "a1")
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Exec 完成时上下文 = %v，期望未取消", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Exec 未完成")
	}
}

// /v1/budget 的 exec 配额：已配置 exec 时附加 "exec"（尚无配额行时 Calls 返回策略值、用量 0，edge 同样输出）；
// cpu_available_usec = limit − reserved − spent − unknown（可为负）；未配置 exec 时不出现（见 TestSubrunHeaderOnReadOnly
// 的逐字节断言）；读取失败为 500。
func TestBudgetExecQuota(t *testing.T) {
	calls := &fakeCalls{
		budget: call.Budget{LimitMicro: 1000},
		execQuota: &call.ExecQuota{CountLimit: 50, CountUsed: 3, CPULimitUsec: 600_000_000, CPUReservedUsec: 66_000_000,
			CPUSpentUsec: 1_000_000, CPUUnknownUsec: 500_000, WallLimitMs: 1_800_000, WallSpentMs: 4200},
		execHasRow: true,
	}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	r := do(t, p, "GET", "/v1/budget", nil, nil)
	want := `,"exec":{"count_limit":50,"count_used":3,"cpu_limit_usec":600000000,"cpu_available_usec":532500000,` +
		`"wall_limit_ms":1800000,"wall_spent_ms":4200,"blocked":false}}`
	if r.status != 200 || !strings.HasSuffix(string(r.body), want) || !strings.HasPrefix(string(r.body), `{"limit_micro":1000,`) {
		t.Errorf("/v1/budget: %d %s", r.status, r.body)
	}
	calls.mu.Lock()
	calls.execQuota = &call.ExecQuota{CountLimit: 50, CountUsed: 50, CPULimitUsec: 100, CPUSpentUsec: 150, WallLimitMs: 10, Blocked: true}
	calls.mu.Unlock()
	r = do(t, p, "GET", "/v1/budget", nil, map[string]string{HeaderSubrun: "st1"})
	if r.status != 200 || !strings.Contains(string(r.body), `"exec":{"count_limit":50,"count_used":50,"cpu_limit_usec":100,"cpu_available_usec":-50,`+
		`"wall_limit_ms":10,"wall_spent_ms":0,"blocked":true}`) || !strings.Contains(string(r.body), `"subrun":`) {
		t.Errorf("blocked、超额与 sub-run 头: %d %s", r.status, r.body)
	}
	e2 := newEdge(t, Config{}, &fakeCalls{execQuota: &call.ExecQuota{}})
	e2.calls = failingQuota{e2.calls}
	p2 := bind(t, e2, "a1", "e1")
	if r := do(t, p2, "GET", "/v1/budget", nil, nil); r.status != 500 || errCode(t, r) != CodeInternal {
		t.Errorf("读取失败: %d %s", r.status, r.body)
	}
}

// failingQuota 让 ExecQuota 以存储错误失败。
type failingQuota struct{ Calls }

func (failingQuota) ExecQuota(context.Context, string) (call.ExecQuota, bool, error) {
	return call.ExecQuota{}, false, errors.New("db down")
}
