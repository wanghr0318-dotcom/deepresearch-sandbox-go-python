package upstream

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件只使用进程内 httptest 服务器；主机名解析与拨号经 DialerConfig 的钩子导向测试服务器，不访问外网。

const testKey = "sk-agentbox-test-SECRET-7f3a9c"

// hooks 记录解析与拨号，并把所有通过检查的拨号导向 srv。
type hooks struct {
	mu      sync.Mutex
	dns     map[string][][]netip.Addr // 每次解析依次取下一组（取完后重复最后一组）
	lookups map[string]int
	dialed  []string
	target  string
}

func newHooks(target string, dns map[string][]string) *hooks {
	h := &hooks{dns: map[string][][]netip.Addr{}, lookups: map[string]int{}, target: target}
	for host, ips := range dns {
		var seq []netip.Addr
		for _, ip := range ips {
			seq = append(seq, netip.MustParseAddr(ip))
		}
		h.dns[host] = [][]netip.Addr{seq}
	}
	return h
}

func (h *hooks) resolve(_ context.Context, host string) ([]netip.Addr, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	seq, ok := h.dns[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	i := h.lookups[host]
	h.lookups[host]++
	if i >= len(seq) {
		i = len(seq) - 1
	}
	return seq[i], nil
}

func (h *hooks) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	h.mu.Lock()
	h.dialed = append(h.dialed, addr)
	h.mu.Unlock()
	if h.target == "" {
		return nil, errors.New("测试未配置拨号目标")
	}
	var d net.Dialer
	return d.DialContext(ctx, network, h.target)
}

func (h *hooks) dialer(allow ...string) *Dialer {
	return NewDialer(DialerConfig{AllowPrivate: allow, Resolver: h.resolve, Dial: h.dial})
}

func (h *hooks) dials() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.dialed...)
}

func mustResolve(t *testing.T, a Adapter, body string) []byte {
	t.Helper()
	out, _, err := a.Resolve([]byte(body))
	if err != nil {
		t.Fatalf("Resolve(%s): %v", body, err)
	}
	return out
}

func wantErr(t *testing.T, e *Error, o Outcome, code string) {
	t.Helper()
	if e == nil {
		t.Fatalf("期望 %s/%s，得到成功", o, code)
	}
	if e.Outcome != o || e.Code != code {
		t.Fatalf("期望 %s/%s，得到 %s/%s（%v）", o, code, e.Outcome, e.Code, e)
	}
}

func asErr(t *testing.T, err error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("期望 *Error，得到 %T %v", err, err)
	}
	return e
}

// E18：直连与经 DNS 解析的禁止地址都被拒，且从不拨号。
func TestDialerRejectsForbiddenAddresses(t *testing.T) {
	forbidden := []string{"169.254.169.254", "127.0.0.1", "10.0.0.1", "::1", "::ffff:10.0.0.1", "fc00::1",
		"100.64.0.1", "0.0.0.0", "::", "224.0.0.1", "fe80::1", "192.168.1.1", "172.16.0.1", "::ffff:127.0.0.1"}
	for _, ip := range forbidden {
		h := newHooks("", map[string][]string{"evil.test": {ip}})
		d := h.dialer()
		if _, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort(ip, "80")); !errors.Is(err, ErrBlocked) {
			t.Errorf("直连 %s：期望 ErrBlocked，得到 %v", ip, err)
		}
		if _, err := d.DialContext(context.Background(), "tcp", "evil.test:443"); !errors.Is(err, ErrBlocked) {
			t.Errorf("evil.test → %s：期望 ErrBlocked，得到 %v", ip, err)
		}
		u, _ := url.Parse("http://" + net.JoinHostPort(ip, "80") + "/")
		if err := d.CheckURL(u); !errors.Is(err, ErrBlocked) {
			t.Errorf("CheckURL %s：期望 ErrBlocked，得到 %v", u, err)
		}
		if got := h.dials(); len(got) != 0 {
			t.Errorf("%s：不应拨号，实际拨号 %v", ip, got)
		}
	}
	// 非默认端口被拒；显式放行的主机（含端口）跳过类别与端口检查。
	h := newHooks("", map[string][]string{"pub.test": {"93.184.216.34"}})
	if _, err := h.dialer().DialContext(context.Background(), "tcp", "pub.test:8080"); !errors.Is(err, ErrBlocked) {
		t.Errorf("端口 8080：期望 ErrBlocked，得到 %v", err)
	}
	d := h.dialer("127.0.0.1")
	if _, err := d.DialContext(context.Background(), "tcp", "127.0.0.1:9"); errors.Is(err, ErrBlocked) {
		t.Errorf("放行主机不应被拒：%v", err)
	}
	if got := h.dials(); len(got) != 1 || got[0] != "127.0.0.1:9" {
		t.Errorf("放行主机应拨号 127.0.0.1:9，实际 %v", got)
	}
}

// E18：DNS rebinding——第一次解析为公网、第二次为私网；dialer 只解析一次并连接第一次检查过的 IP。
func TestDialerDNSRebindingConnectsCheckedIP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "rebind.test" {
			t.Errorf("Host 头应保留原始主机名，得到 %q", r.Host)
		}
		io.WriteString(w, "hello")
	}))
	defer srv.Close()
	h := newHooks(srv.Listener.Addr().String(), nil)
	h.dns["rebind.test"] = [][]netip.Addr{{netip.MustParseAddr("93.184.216.34")}, {netip.MustParseAddr("10.0.0.1")}}
	f := NewFetch(FetchConfig{Dialer: h.dialer()})
	resp, e := f.Do(context.Background(), mustResolve(t, f, `{"url":"http://rebind.test/"}`))
	if e != nil {
		t.Fatalf("Do: %v", e)
	}
	var res FetchResult
	json.Unmarshal(resp.Body, &res)
	if res.Content != "hello" {
		t.Fatalf("内容 %q", res.Content)
	}
	if got := h.dials(); len(got) != 1 || got[0] != "93.184.216.34:80" {
		t.Fatalf("应连接已检查的 93.184.216.34:80，实际 %v", got)
	}
	if n := h.lookups["rebind.test"]; n != 1 {
		t.Fatalf("应只解析一次，实际 %d 次", n)
	}
}

// E18：重定向每跳重新检查——跳到私有地址被拒；5 跳通过，6 跳被拒。
func TestFetchRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/private":
			http.Redirect(w, r, "http://10.0.0.1/secret", http.StatusFound)
		case r.URL.Path == "/private-name":
			http.Redirect(w, r, "http://internal.test/secret", http.StatusFound)
		case strings.HasPrefix(r.URL.Path, "/r/"):
			var n int
			fmt.Sscanf(r.URL.Path, "/r/%d", &n)
			if n == 0 {
				io.WriteString(w, "end")
				return
			}
			http.Redirect(w, r, fmt.Sprintf("http://pub.test/r/%d", n-1), http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	h := newHooks(srv.Listener.Addr().String(), map[string][]string{"pub.test": {"93.184.216.34"}, "internal.test": {"192.168.0.10"}})
	f := NewFetch(FetchConfig{Dialer: h.dialer()})

	resp, e := f.Do(context.Background(), mustResolve(t, f, `{"url":"http://pub.test/r/5"}`))
	if e != nil {
		t.Fatalf("5 跳应通过：%v", e)
	}
	var res FetchResult
	json.Unmarshal(resp.Body, &res)
	if res.Content != "end" || res.FinalURL != "http://pub.test/r/0" {
		t.Fatalf("5 跳结果 %+v", res)
	}
	_, e = f.Do(context.Background(), mustResolve(t, f, `{"url":"http://pub.test/r/6"}`))
	wantErr(t, e, OutcomeFatal, CodeTooManyRedirects)

	for _, p := range []string{"/private", "/private-name"} {
		_, e = f.Do(context.Background(), mustResolve(t, f, `{"url":"http://pub.test`+p+`"}`))
		wantErr(t, e, OutcomeFatal, CodeEgressBlocked)
	}
	for _, a := range h.dials() {
		if a != "93.184.216.34:80" {
			t.Fatalf("不应拨号私有地址：%v", h.dials())
		}
	}
}

// 规则 6：上限作用于解压后正文——恰 5 MiB 不截断；gzip 压缩的 5 MiB+1 字节截断为 5 MiB 并置 truncated。
// 同时检查抓取请求只带固定请求头（GET、Accept-Encoding: gzip、无请求体）。
func TestFetchTruncatesAfterDecompression(t *testing.T) {
	const limit = DefaultFetchMaxBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.ContentLength > 0 || r.Header.Get("Accept-Encoding") != "gzip" || r.Header.Get("Authorization") != "" {
			t.Errorf("请求不合规：%s len=%d AE=%q", r.Method, r.ContentLength, r.Header.Get("Accept-Encoding"))
		}
		switch r.URL.Path {
		case "/exact":
			w.Write(bytes.Repeat([]byte("a"), limit))
		case "/gz":
			w.Header().Set("Content-Encoding", "gzip")
			zw := gzip.NewWriter(w)
			zw.Write(bytes.Repeat([]byte("a"), limit+1))
			zw.Close()
		}
	}))
	defer srv.Close()
	h := newHooks(srv.Listener.Addr().String(), map[string][]string{"pub.test": {"93.184.216.34"}})
	f := NewFetch(FetchConfig{Dialer: h.dialer()})
	for _, c := range []struct {
		path      string
		truncated bool
	}{{"/exact", false}, {"/gz", true}} {
		resp, e := f.Do(context.Background(), mustResolve(t, f, `{"url":"http://pub.test`+c.path+`"}`))
		if e != nil {
			t.Fatalf("%s: %v", c.path, e)
		}
		var res FetchResult
		json.Unmarshal(resp.Body, &res)
		if res.Truncated != c.truncated || len(res.Content) != limit || resp.Usage.ResponseBytes != limit {
			t.Fatalf("%s：truncated=%v len=%d bytes=%d", c.path, res.Truncated, len(res.Content), resp.Usage.ResponseBytes)
		}
	}
}

// 规则 1：抓取只接受 {url}，拒绝 userinfo、非 http(s)、非默认端口、禁止的 IP 字面量。
func TestFetchResolveRejects(t *testing.T) {
	f := NewFetch(FetchConfig{Dialer: newHooks("", nil).dialer()})
	for body, code := range map[string]string{
		`{"url":"http://user:pw@example.com/"}`:             CodeInvalidURL,
		`{"url":"ftp://example.com/"}`:                      CodeInvalidURL,
		`{"url":"file:///etc/passwd"}`:                      CodeInvalidURL,
		`{"url":"/relative"}`:                               CodeInvalidURL,
		`{"url":"http://example.com:8080/"}`:                CodeEgressBlocked,
		`{"url":"http://169.254.169.254/latest/"}`:          CodeEgressBlocked,
		`{"url":"http://[::ffff:10.0.0.1]/"}`:               CodeEgressBlocked,
		`{"url":"http://example.com/","headers":{"a":"b"}}`: CodeUnsupportedField,
		`{"url":"http://example.com/","method":"POST"}`:     CodeUnsupportedField,
	} {
		_, _, err := f.Resolve([]byte(body))
		e := asErr(t, err)
		if e.Outcome != OutcomeFatal || e.Code != code {
			t.Errorf("%s：期望 fatal/%s，得到 %s/%s", body, code, e.Outcome, e.Code)
		}
	}
	out := mustResolve(t, f, `{"url":"https://example.com/a?b=1#frag"}`)
	if string(out) != `{"url":"https://example.com/a?b=1"}` {
		t.Fatalf("规范化结果 %s", out)
	}
}

// 规则 7：不继承代理环境变量——代理指向必然失败的地址，请求仍直连测试服务器。
func TestNoProxyFromEnvironment(t *testing.T) {
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"} {
		t.Setenv(k, "http://127.0.0.1:1")
	}
	t.Setenv("NO_PROXY", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "direct") }))
	defer srv.Close()
	h := newHooks(srv.Listener.Addr().String(), map[string][]string{"pub.test": {"93.184.216.34"}})
	d := h.dialer()
	c := d.HTTPClient(1<<20, 5*time.Second)
	if tr := c.Transport.(*guardTransport).inner.(*http.Transport); tr.Proxy != nil {
		t.Fatal("Transport.Proxy 应为 nil")
	}
	resp, err := c.Get("http://pub.test/")
	if err != nil {
		t.Fatalf("应直连：%v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "direct" || len(h.dials()) != 1 || h.dials()[0] != "93.184.216.34:80" {
		t.Fatalf("body=%q dials=%v", b, h.dials())
	}
}

func newChat(srvURL string, pricing Pricing) Adapter {
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	return NewChat(ChatConfig{BaseURL: srvURL + "/v1", Model: "m-1", APIKey: testKey, Pricing: pricing,
		MaxTokensDefault: 256, HTTP: d.HTTPClient(DefaultModelMaxBody, 10*time.Second)})
}

// chat Resolve：补默认并进入 applied_defaults；拒绝 stream:true、非文本消息、未声明模型与未知字段；估算公式。
func TestChatResolveAndEstimate(t *testing.T) {
	a := newChat("http://127.0.0.1:1", Pricing{Version: "p1", InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 15_000_000})
	msgs := `[{"role":"user","content":"hello world, this is a test"}]`
	out, defaults, err := a.Resolve([]byte(`{"messages":` + msgs + `,"stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	if defaults["max_tokens"] != 256 || defaults["model"] != "m-1" {
		t.Fatalf("applied_defaults %v", defaults)
	}
	var obj map[string]json.RawMessage
	json.Unmarshal(out, &obj)
	if string(obj["max_tokens"]) != "256" || string(obj["model"]) != `"m-1"` || obj["stream"] != nil {
		t.Fatalf("resolved %s", out)
	}
	// input_estimate = ceil(len(messages)/4)；估算 = ceil(in×3e6/1e6) + ceil(256×15e6/1e6)。
	in := (int64(len(obj["messages"])) + 3) / 4
	got, err := a.Estimate(out)
	if want := in*3 + 256*15; err != nil || got != want {
		t.Fatalf("Estimate = %d, %v；期望 %d", got, err, want)
	}
	// 显式 max_tokens 不进 applied_defaults。
	_, defaults, _ = a.Resolve([]byte(`{"model":"m-1","max_tokens":10,"messages":` + msgs + `}`))
	if len(defaults) != 0 {
		t.Fatalf("显式值不应进入 applied_defaults：%v", defaults)
	}
	for body, code := range map[string]string{
		`{"messages":` + msgs + `,"stream":true}`:                               CodeUnsupportedField,
		`{"messages":[{"role":"user","content":[{"type":"text","text":"x"}]}]}`: CodeUnsupportedField,
		`{"messages":[{"role":"user","content":null}]}`:                         CodeUnsupportedField,
		`{"messages":` + msgs + `,"n":2}`:                                       CodeUnsupportedField,
		`{"messages":` + msgs + `,"model":"other"}`:                             CodeUnsupportedModel,
		`{"messages":[]}`:                          CodeInvalidRequest,
		`{"messages":` + msgs + `,"max_tokens":0}`: CodeInvalidRequest,
	} {
		_, _, err := a.Resolve([]byte(body))
		e := asErr(t, err)
		if e.Outcome != OutcomeFatal || e.Status != http.StatusBadRequest || e.Code != code {
			t.Errorf("%s：期望 fatal/400/%s，得到 %s/%d/%s", body, code, e.Outcome, e.Status, e.Code)
		}
	}
}

// chat Do 的结果归类：ok（解析 usage 与请求 ID）、429 + Retry-After、5xx、4xx、中途断开、发送前失败；
// 并断言任何错误文本与日志都不含 Key。
func TestChatOutcomesAndNoKeyLeak(t *testing.T) {
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	var mode atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer "+testKey {
			t.Errorf("请求 %s auth=%v", r.URL.Path, r.Header.Get("Authorization") != "")
		}
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte(testKey)) {
			t.Error("请求体不应含 Key")
		}
		switch mode.Load().(string) {
		case "ok":
			w.Header().Set("X-Request-Id", "req-42")
			io.WriteString(w, `{"id":"cmpl-1","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":11,"completion_tokens":7}}`)
		case "nousage":
			io.WriteString(w, `{"id":"cmpl-2","choices":[]}`)
		case "badjson":
			io.WriteString(w, `not json`)
		case "429":
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		case "503":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "401":
			// 恶意或调试型上游回显请求头：错误文本不得带出响应体。
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":"bad key `+r.Header.Get("Authorization")+`"}`)
		case "cut":
			conn, buf, _ := w.(http.Hijacker).Hijack()
			buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"id\":\"cmpl-")
			buf.Flush()
			conn.Close()
		}
	}))
	defer srv.Close()

	a := newChat(srv.URL, Pricing{InputMicroPerMTok: 1, OutputMicroPerMTok: 1})
	resolved := mustResolve(t, a, `{"messages":[{"role":"user","content":"hello"}]}`)
	var errs []string
	run := func(m string) (Response, *Error) {
		mode.Store(m)
		resp, e := a.Do(context.Background(), resolved)
		if e != nil {
			errs = append(errs, e.Error(), fmt.Sprintf("%+v %#v", e, e.Err))
		}
		return resp, e
	}

	resp, e := run("ok")
	if e != nil || resp.Usage != (Usage{InputTokens: 11, OutputTokens: 7, Requests: 1, ResponseBytes: int64(len(resp.Body))}) || resp.UpstreamRequestID != "req-42" {
		t.Fatalf("ok：%+v %v", resp.Usage, e)
	}
	resp, e = run("nousage")
	in, maxTok, _ := chatInputs(resolved)
	if e != nil || resp.Usage.InputTokens != in || resp.Usage.OutputTokens != maxTok || resp.UpstreamRequestID != "cmpl-2" {
		t.Fatalf("缺 usage 应按保守估算计量：%+v %v", resp.Usage, e)
	}
	_, e = run("badjson")
	wantErr(t, e, OutcomeUnknown, CodeUpstreamBadResponse)
	resp, e = run("429")
	wantErr(t, e, OutcomeRetryable, CodeUpstreamRateLimited)
	if resp.RetryAfter != 7*time.Second || e.Status != 429 {
		t.Fatalf("Retry-After 解析为 %v，状态 %d", resp.RetryAfter, e.Status)
	}
	_, e = run("503")
	wantErr(t, e, OutcomeRetryable, CodeUpstreamUnavailable)
	_, e = run("401")
	wantErr(t, e, OutcomeFatal, CodeUpstreamRejected)
	_, e = run("cut")
	wantErr(t, e, OutcomeUnknown, CodeUpstreamUnconfirmed)

	// 发送前失败（连接被拒）→ retryable。
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	closed := "http://" + l.Addr().String()
	l.Close()
	b := newChat(closed, Pricing{})
	_, e = b.Do(context.Background(), resolved)
	wantErr(t, e, OutcomeRetryable, CodeUpstreamUnreachable)
	errs = append(errs, e.Error())
	// 未放行的私有上游 → fatal egress_blocked。
	c := NewChat(ChatConfig{BaseURL: srv.URL + "/v1", Model: "m-1", APIKey: testKey,
		HTTP: NewDialer(DialerConfig{}).HTTPClient(DefaultModelMaxBody, 5*time.Second)})
	_, e = c.Do(context.Background(), resolved)
	wantErr(t, e, OutcomeFatal, CodeEgressBlocked)
	errs = append(errs, e.Error())

	for _, s := range append(errs, logBuf.String()) {
		if strings.Contains(s, testKey) {
			t.Fatalf("错误或日志含 Key：%q", s)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	for v, want := range map[string]time.Duration{
		"7": 7 * time.Second, "0": 0, "-3": 0, "abc": 0, "": 0, "999999999": maxRetryAfter,
		now.Add(30 * time.Second).Format(http.TimeFormat): 30 * time.Second,
		now.Add(-time.Minute).Format(http.TimeFormat):     0,
	} {
		if got := parseRetryAfter(v, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v，期望 %v", v, got, want)
		}
	}
}

// 搜索：fake 注入与占位结果、tavily 协议映射（Key 只在请求头）、ddg_lite 解析保存的样本、解析失败为 fatal、未知供应商。
func TestSearchProviders(t *testing.T) {
	ctx := context.Background()
	decode := func(resp Response) searchReply {
		var r searchReply
		if err := json.Unmarshal(resp.Body, &r); err != nil {
			t.Fatalf("响应 %s: %v", resp.Body, err)
		}
		return r
	}

	fake := NewSearch(SearchConfig{Provider: SearchFake, Pricing: Pricing{SearchMicroPerRequest: 500},
		Fake: func(_ context.Context, q string, n int) ([]SearchResult, error) {
			if q == "fail" {
				return nil, &Error{Outcome: OutcomeRetryable, Status: 503, Code: CodeUpstreamUnavailable}
			}
			return []SearchResult{{Title: q, URL: "https://a.example/", Snippet: "s"}, {Title: "2"}, {Title: "3"}}, nil
		}})
	resolved, defaults, err := fake.Resolve([]byte(`{"query":"go"}`))
	if err != nil || defaults["max_results"] != defaultSearchResults {
		t.Fatalf("Resolve: %v %v", defaults, err)
	}
	if m, _ := fake.Estimate(resolved); m != 500 {
		t.Fatalf("Estimate %d", m)
	}
	resp, e := fake.Do(ctx, mustResolve(t, fake, `{"query":"go","max_results":2}`))
	if e != nil || len(decode(resp).Results) != 2 || decode(resp).Results[0].Title != "go" {
		t.Fatalf("fake：%s %v", resp.Body, e)
	}
	_, e = fake.Do(ctx, mustResolve(t, fake, `{"query":"fail"}`))
	wantErr(t, e, OutcomeRetryable, CodeUpstreamUnavailable)
	for _, body := range []string{`{"query":""}`, `{"query":"x","max_results":21}`, `{"query":"x","lang":"en"}`} {
		if _, _, err := fake.Resolve([]byte(body)); err == nil {
			t.Errorf("%s 应被拒", body)
		}
	}
	placeholder := NewSearch(SearchConfig{Provider: SearchFake})
	resp, e = placeholder.Do(ctx, mustResolve(t, placeholder, `{"query":"q","max_results":3}`))
	if e != nil || len(decode(resp).Results) != 3 {
		t.Fatalf("占位结果：%s %v", resp.Body, e)
	}

	sample, err := os.ReadFile("testdata/ddg_lite_sample.html")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search":
			var in map[string]any
			json.NewDecoder(r.Body).Decode(&in)
			if r.Header.Get("Authorization") != "Bearer "+testKey || in["query"] != "golang context" || in["api_key"] != nil {
				t.Errorf("tavily 请求不合规：%v", in)
			}
			io.WriteString(w, `{"request_id":"tv-1","results":[{"title":"T","url":"https://t.example/","content":"C","score":0.9}]}`)
		case "/lite/":
			if r.URL.Query().Get("q") != "golang context" {
				t.Errorf("ddg 查询参数 %q", r.URL.RawQuery)
			}
			w.Write(sample)
		case "/broken/":
			io.WriteString(w, "<html><body>captcha</body></html>")
		}
	}))
	defer srv.Close()
	client := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}}).HTTPClient(DefaultModelMaxBody, 5*time.Second)

	tv := NewSearch(SearchConfig{Provider: SearchTavily, APIKey: testKey, BaseURL: srv.URL, HTTP: client})
	resp, e = tv.Do(ctx, mustResolve(t, tv, `{"query":"golang context"}`))
	if e != nil || resp.UpstreamRequestID != "tv-1" || decode(resp).Results[0] != (SearchResult{Title: "T", URL: "https://t.example/", Snippet: "C"}) {
		t.Fatalf("tavily：%s %v", resp.Body, e)
	}

	ddg := NewSearch(SearchConfig{Provider: SearchDDGLite, BaseURL: srv.URL + "/lite/", HTTP: client})
	resp, e = ddg.Do(ctx, mustResolve(t, ddg, `{"query":"golang context","max_results":10}`))
	if e != nil {
		t.Fatalf("ddg_lite: %v", e)
	}
	want := []SearchResult{
		{Title: "context package - context - Go Packages", URL: "https://pkg.go.dev/context",
			Snippet: "Package context defines the Context type, which carries deadlines, cancellation signals, & other request-scoped values."},
		{Title: "Go Concurrency Patterns: Context - The Go Programming Language", URL: "https://go.dev/blog/context",
			Snippet: "In Go servers, each incoming request is handled in its own goroutine."},
		{Title: `Example "quoted" title`, URL: "https://example.org/a?x=1&y=2", Snippet: "Third snippet."},
	}
	got := decode(resp).Results
	if len(got) != len(want) {
		t.Fatalf("ddg_lite 结果 %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ddg_lite[%d] = %+v，期望 %+v", i, got[i], want[i])
		}
	}
	broken := NewSearch(SearchConfig{Provider: SearchDDGLite, BaseURL: srv.URL + "/broken/", HTTP: client})
	_, e = broken.Do(ctx, mustResolve(t, broken, `{"query":"golang context"}`))
	wantErr(t, e, OutcomeFatal, CodeUpstreamBadResponse)
	if res, err := parseDDGLite([]byte("<html>No results.</html>")); err != nil || len(res) != 0 {
		t.Fatalf("无结果页：%v %v", res, err)
	}

	_, _, err = NewSearch(SearchConfig{Provider: "bing"}).Resolve([]byte(`{"query":"x"}`))
	if e := asErr(t, err); e.Code != CodeUnsupportedProvider {
		t.Fatalf("未知供应商：%v", e)
	}
}
