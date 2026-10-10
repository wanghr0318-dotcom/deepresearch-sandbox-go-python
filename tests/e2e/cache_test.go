//go:build linux

// M3：缓存故障实验（E21–E25）与缓存收益测量。装置与共用辅助函数见 e2e_test.go。

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// ---- M3：缓存故障实验（规格 §16.4 E21–E25）与缓存收益测量（§16.6）----
//
// 共享缓存是真实 Redis（AGENTBOX_TEST_REDIS_ADDR；本地为 compose 的 Redis，CI 为 Redis service）；上游是 fake
// upstream，抓取页面的缓存头由 SetPageHeader 声明。Redis 运行在测试无法停止的容器中：宕机、变慢与黑洞由测试控制的
// TCP 代理（faultProxy）在 Gateway 与 Redis 之间模拟。测试直接读写同一 Redis（cache 包的客户端；超过 4 KiB 的值
// 用原始 RESP）来篡改条目，签名密钥取自 server 数据目录中的 cache.key（cache.LoadKeys）。页面路径与搜索查询带随机
// 后缀：Redis 跨用例、跨运行共享，旧运行留下的条目（以别的密钥签名）不会落在本用例的键上。
//
// Worker 是 sim_worker（fetch 操作的调用 ID 为 root/<step>/fetch/1）；E24 与收益测量直接连接 attempt 的 socket
// 发请求。每条用例以静止时的不变量检查结束（含 I14 的缓存部分与 I15）。
//
// TestE24CoalesceWithinAttempt 只覆盖 root 调用的合并。sub-run 的部分（同一 sub-run 内合并、两个 sub-run 的同一
// 抓取不合并）见 M4 Plan 14 Task 11 段的 TestE24SubrunCoalesce：edge 自 M4（Plan 14 Task 5）接受 X-Agentbox-Subrun，
// 合并键含 subrun_id（§11.4）。

const redisEnv = "AGENTBOX_TEST_REDIS_ADDR"

func testRedisAddr(t *testing.T) string {
	t.Helper()
	a := os.Getenv(redisEnv)
	if a == "" {
		required(t, redisEnv)
	}
	return a
}

func randSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b) // crypto/rand 不会失败（Go 1.20 起失败时直接终止进程）
	return hex.EncodeToString(b)
}

// cacheCfg 是开启共享缓存的进程内装置配置（gatewayCfg 加 Redis 地址）。
func cacheCfg(fu *fakeupstream.Server, redisAddr string) app.Config {
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.RedisAddr = redisAddr
	return cfg
}

// cachePage 在 fake upstream 上声明带响应头 hdr 的抓取页面，返回其 URL。
func cachePage(fu *fakeupstream.Server, path string, hdr http.Header) string {
	fu.SetPageHeader(path, "text/plain; charset=utf-8", "页面 "+path+" 的正文", hdr)
	return fu.URL() + path
}

func ccHeader(v string) http.Header { return http.Header{"Cache-Control": {v}} }

// fetchKey 是抓取 u 的缓存键，与 Gateway 的计算相同（fetch adapter 的 provider、版本与 Resolve 后的参数）。
func fetchKey(t *testing.T, u string) string {
	t.Helper()
	params, err := json.Marshal(map[string]string{"url": u})
	if err != nil {
		t.Fatal(err)
	}
	ad := upstream.NewFetch(upstream.FetchConfig{})
	k, err := cache.Key(upstream.KindFetch, ad.Provider(), ad.Version(), params)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// testRedis 是测试自己的 Redis 客户端（直连，不经代理；超时宽松）。
func testRedis(t *testing.T, addr string) *cache.Redis {
	t.Helper()
	r := cache.NewRedis(cache.RedisConfig{Addr: addr, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second})
	t.Cleanup(func() { _ = r.Close() }) // 测试结束，关闭错误无关紧要
	return r
}

func cacheEntry(t *testing.T, r *cache.Redis, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := r.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("读取缓存条目 %s: %v", key, err)
	}
	return v, ok
}

// waitSealed 等待键 key 上出现 Gateway 写入的有效条目（以 server 的签名密钥校验通过、未过期），返回原始字节与解码值。
// 写入是异步的：上游结果返回给 Worker 之后才落到 Redis。
func waitSealed(t *testing.T, r *cache.Redis, signer *cache.Signer, key string) ([]byte, cache.Value) {
	t.Helper()
	var raw []byte
	var val cache.Value
	eventually(t, "缓存条目 "+key+" 由 Gateway 写入", func() bool {
		b, ok, err := r.Get(context.Background(), key)
		if err != nil || !ok {
			return false
		}
		v, err := signer.Open(key, b, time.Now())
		if err != nil {
			return false
		}
		raw, val = b, v
		return true
	})
	return raw, val
}

// rawSet 以原始 RESP 写入任意大小的值（cache 客户端拒绝超过 4 KiB 的值）。
func rawSet(t *testing.T, addr, key string, val []byte, ttl time.Duration) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	args := [][]byte{[]byte("SET"), []byte(key), val, []byte("PX"), []byte(strconv.FormatInt(ttl.Milliseconds(), 10))}
	cmd := fmt.Appendf(nil, "*%d\r\n", len(args))
	for _, a := range args {
		cmd = fmt.Appendf(cmd, "$%d\r\n", len(a))
		cmd = append(append(cmd, a...), '\r', '\n')
	}
	if _, err := c.Write(cmd); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil || line != "+OK\r\n" {
		t.Fatalf("原始 SET 的回复 %q %v", line, err)
	}
}

// cacheCall 是 calls 表中缓存实验关心的列。
type cacheCall struct {
	State, Source, ResultRef, FailReason string
	Tries                                int
}

func cacheCallRow(t *testing.T, dsn, taskID, callID string) cacheCall {
	t.Helper()
	var c cacheCall
	pgQueryRow(t, dsn, `SELECT state, source, COALESCE(result_ref, ''), fail_reason, tries_used FROM calls
		WHERE task_id = $1 AND call_id = $2`, []any{taskID, callID}, &c.State, &c.Source, &c.ResultRef, &c.FailReason, &c.Tries)
	return c
}

// cacheMetrics 经 GET /status 读取共享缓存的指标（hit、miss、bypass、coalesced、error、breaker_open、integrity_failure）。
func (h *harness) cacheMetrics() map[string]int64 {
	h.t.Helper()
	st, b, err := httpDo("GET", h.base+"/status", "")
	var v struct {
		Cache map[string]int64 `json:"cache"`
	}
	if err != nil || st != http.StatusOK || json.Unmarshal(b, &v) != nil || v.Cache == nil {
		h.t.Fatalf("GET /status = %d %s %v（期望含 cache 指标）", st, b, err)
	}
	return v.Cache
}

func metricsDelta(after, before map[string]int64) map[string]int64 {
	d := map[string]int64{}
	for k, v := range after {
		d[k] = v - before[k]
	}
	return d
}

func fetchCallID(i int) string { return fmt.Sprintf("root/f%d/fetch/1", i) }

// runFetches 提交一个依次抓取 urls 的任务（步骤 f1..fn）并等待它成功结束。
func (h *harness) runFetches(requestID string, urls ...string) string {
	h.t.Helper()
	steps := make([]step, len(urls))
	for i, u := range urls {
		steps[i] = step{"op": "fetch", "step_id": fmt.Sprintf("f%d", i+1), "url": u}
	}
	id := h.submit(requestID, spec(nil, requestID, nil, steps...))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		h.t.Fatalf("任务 %s（%s）结束为 %s/%s", id, requestID, v.Status, v.StatusReason)
	}
	return id
}

// pathCount 是 fake upstream 收到的、路径与 u 相同的抓取请求数。
func pathCount(t *testing.T, fu *fakeupstream.Server, u string) int {
	t.Helper()
	p, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range fu.Requests(fakeupstream.Fetch) {
		if r.Path == p.Path {
			n++
		}
	}
	return n
}

// waitAttemptSocket 等待任务第 1 个 attempt 的 Worker 启动、Gateway socket 就绪，返回 socket 路径。
func (h *harness) waitAttemptSocket(taskID string) string {
	h.t.Helper()
	var sock string
	eventually(h.t, "attempt 1 的 Gateway socket 就绪", func() bool {
		in, ok := h.rec(taskID).init(1)
		if !ok {
			return false
		}
		sock = h.gwSocket(in.AttemptID)
		_, err := os.Stat(sock)
		return err == nil
	})
	return sock
}

// proxyMode 是 faultProxy 对 Gateway → Redis 方向的处理方式。
type proxyMode int

const (
	proxyPass      proxyMode = iota // 原样转发
	proxyDelay                      // 每段请求延迟 delay 后转发（变慢）
	proxyBlackhole                  // 吞掉请求，连接保持（客户端等到超时）
	proxyDown                       // 新连接接受后立即关闭（宕机）
)

// faultProxy 是 Gateway 与 Redis 之间由测试控制的 TCP 代理（E21）。切换模式时断开现有连接，之后的操作都经新连接、
// 按新模式处理。
type faultProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	mode  proxyMode
	delay time.Duration
	conns map[net.Conn]struct{}
}

func newFaultProxy(t *testing.T, target string) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() {
		_ = ln.Close() // 测试结束，关闭错误无关紧要
		p.set(proxyDown, 0)
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *faultProxy) Addr() string { return p.ln.Addr().String() }

func (p *faultProxy) set(m proxyMode, delay time.Duration) {
	p.mu.Lock()
	p.mode, p.delay = m, delay
	conns := maps.Clone(p.conns)
	p.mu.Unlock()
	for c := range conns {
		_ = c.Close() // 断开现有连接；关闭错误无关紧要
	}
}

func (p *faultProxy) current() (proxyMode, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode, p.delay
}

func (p *faultProxy) serve(c net.Conn) {
	if m, _ := p.current(); m == proxyDown {
		_ = c.Close() // 宕机：拒绝连接
		return
	}
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.conns[c], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { p.forward(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }() // 连接被断开时的错误无关紧要
	<-done
	_, _ = c.Close(), up.Close()
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, up)
	p.mu.Unlock()
}

// forward 把 Gateway 发出的请求按当前模式转发给 Redis。
func (p *faultProxy) forward(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			m, d := p.current()
			switch m {
			case proxyDown:
				return
			case proxyDelay:
				time.Sleep(d) // 注入的延迟本身
			}
			if m != proxyBlackhole {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// TestE21CacheRedisDegraded：E21——Redis 正常、变慢（每个请求延迟 200 ms，超过 50 ms 读超时）、黑洞（请求无回复）、
// 宕机（连接被拒）→ 每个任务的结果都正确（与正常时同一结果 blob），Redis 故障只表现为未命中：/status 可见 hit、
// miss、bypass（含凭据参数的 URL）、error，连续 5 次失败后熔断打开（breaker_open），之后不再访问 Redis。
func TestE21CacheRedisDegraded(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	px := newFaultProxy(t, addr)
	sfx := randSuffix()
	page := func(name string) string { return cachePage(fu, "/e21-"+sfx+"/"+name, ccHeader("max-age=600")) }
	u := page("a")
	cred := fu.URL() + "/e21-" + sfx + "/private?token=e2e"
	h.start(cacheCfg(fu, px.Addr()))
	rc := testRedis(t, addr)
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}

	// 正常：第一次未命中、走上游并写入；第二个任务（新的任务与调用 ID）命中同一结果 blob；凭据 URL 不读缓存（bypass）。
	id1 := h.runFetches("e21-1", u)
	c1 := cacheCallRow(t, h.dsn, id1, fetchCallID(1))
	waitSealed(t, rc, signer, fetchKey(t, u))
	id2 := h.runFetches("e21-2", u, cred)
	c2 := cacheCallRow(t, h.dsn, id2, fetchCallID(1))
	c2cred := cacheCallRow(t, h.dsn, id2, fetchCallID(2))
	if c1.Source != "upstream" || c1.Tries != 1 || c2.State != "completed" || c2.Source != "cache" || c2.Tries != 0 ||
		c2.ResultRef != c1.ResultRef || c2cred.Source != "upstream" {
		t.Fatalf("正常阶段：首次 %+v，命中 %+v，凭据 URL %+v", c1, c2, c2cred)
	}
	m0 := h.cacheMetrics()
	if m0["hit"] != 1 || m0["miss"] != 1 || m0["bypass"] != 1 || m0["error"] != 0 || m0["breaker_open"] != 0 {
		t.Fatalf("正常阶段的指标 %v", m0)
	}

	// 变慢与黑洞：查找超时（error）→ 按未命中走上游，写入同样超时；结果与正常时相同。
	for i, ph := range []struct {
		name string
		mode proxyMode
	}{{"变慢", proxyDelay}, {"黑洞", proxyBlackhole}} {
		px.set(ph.mode, 200*time.Millisecond)
		id := h.runFetches(fmt.Sprintf("e21-%d", i+3), u)
		if c := cacheCallRow(t, h.dsn, id, fetchCallID(1)); c.State != "completed" || c.Source != "upstream" || c.ResultRef != c1.ResultRef {
			t.Fatalf("%s：调用 %+v，期望走上游且结果为 %s", ph.name, c, c1.ResultRef)
		}
	}

	// 宕机：连续失败达到 5 次后熔断打开；之后的查找与写入不访问 Redis（breaker_open），任务仍然正确完成。
	px.set(proxyDown, 0)
	urls := []string{u, page("b"), page("c"), page("d")}
	id5 := h.runFetches("e21-5", urls...)
	for i := range urls {
		c := cacheCallRow(t, h.dsn, id5, fetchCallID(i+1))
		if c.State != "completed" || c.Source != "upstream" || (i == 0 && c.ResultRef != c1.ResultRef) {
			t.Fatalf("宕机：调用 %d %+v", i+1, c)
		}
	}
	var m map[string]int64
	eventually(t, "/status 显示熔断打开（breaker_open > 0）", func() bool {
		m = h.cacheMetrics()
		return m["breaker_open"] > 0
	})
	if m["error"] < cache.DefaultFailureThreshold || m["hit"] != 1 || m["integrity_failure"] != 0 {
		t.Fatalf("故障阶段的指标 %v：期望 error ≥ %d、hit 仍为 1", m, cache.DefaultFailureThreshold)
	}
	if n := pathCount(t, fu, u); n != 4 {
		t.Fatalf("页面 a 到达上游 %d 次，期望 4（命中的一次不访问上游）", n)
	}
	t.Logf("E21：正常阶段指标 %v；变慢/黑洞/宕机之后 %v；页面 a 的结果 blob %s", m0, m, c1.ResultRef)
	h.finish()
}

// TestE22CacheIntegrity：E22——直接写入 Redis 的篡改条目：错误的 HMAC、替换 blob sha（未重新签名）、持有密钥者签名
// 但引用不存在的 blob、过 expires_at 后重放原本有效的签名条目、超过 4 KiB 的值 → 每一种都是未命中：走上游、结果与
// 正常时相同，integrity_failure 恰加 1、hit 不变、error 不变（超大值不是 Redis 故障，不计入熔断）；条目被删除，
// 上游结果随后重新写入有效条目。
func TestE22CacheIntegrity(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	sfx := randSuffix()
	u := cachePage(fu, "/e22-"+sfx+"/a", ccHeader("max-age=600"))
	short := cachePage(fu, "/e22-"+sfx+"/short", ccHeader("max-age=3"))
	h.start(cacheCfg(fu, addr))
	rc := testRedis(t, addr)
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}
	key, shortKey := fetchKey(t, u), fetchKey(t, short)

	id0 := h.runFetches("e22-seed", u, short)
	ref := cacheCallRow(t, h.dsn, id0, fetchCallID(1)).ResultRef
	shortRef := cacheCallRow(t, h.dsn, id0, fetchCallID(2)).ResultRef
	good, _ := waitSealed(t, rc, signer, key)
	shortRaw, shortVal := waitSealed(t, rc, signer, shortKey)
	// 对照：未篡改的条目命中。
	if c := cacheCallRow(t, h.dsn, h.runFetches("e22-hit", u), fetchCallID(1)); c.Source != "cache" || c.ResultRef != ref {
		t.Fatalf("未篡改的条目应命中：%+v", c)
	}

	ctx := context.Background()
	set := func(k string, v []byte) {
		t.Helper()
		if err := rc.Set(ctx, k, v, 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	mutate := func(raw []byte, f func(m map[string]any)) []byte {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		f(m)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name, url, key, ref string
		tamper              func()
	}{
		{"错误的 HMAC", u, key, ref, func() {
			set(key, mutate(good, func(m map[string]any) {
				mac, err := base64.StdEncoding.DecodeString(m["mac"].(string))
				if err != nil || len(mac) == 0 {
					t.Fatalf("条目的 mac %v %v", m["mac"], err)
				}
				mac[0] ^= 0xff
				m["mac"] = base64.StdEncoding.EncodeToString(mac)
			}))
		}},
		{"替换 blob sha（未重新签名）", u, key, ref, func() {
			set(key, mutate(good, func(m map[string]any) { m["blob_sha256"] = shortRef }))
		}},
		{"持有密钥者签名、引用的 blob 不存在", u, key, ref, func() {
			now := time.Now()
			v, err := signer.Seal(key, cache.Value{BlobSHA256: sha256Hex([]byte("e22 不存在的 blob " + sfx)), Status: 200,
				ContentType: "text/plain", Size: 16, FetchedAt: now, ExpiresAt: now.Add(10 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			set(key, v)
		}},
		{"过 expires_at 后重放签名条目", short, shortKey, shortRef, func() {
			eventually(t, "短寿命条目过了 expires_at", func() bool { return time.Now().After(shortVal.ExpiresAt) })
			set(shortKey, shortRaw)
		}},
		{"超过 4 KiB 的值", u, key, ref, func() {
			rawSet(t, addr, key, bytes.Repeat([]byte("x"), cache.MaxValueSize+1000), 10*time.Minute)
		}},
	}
	for i, c := range cases {
		before := h.cacheMetrics()
		n0 := pathCount(t, fu, c.url)
		c.tamper()
		id := h.runFetches(fmt.Sprintf("e22-%d", i), c.url)
		got := cacheCallRow(t, h.dsn, id, fetchCallID(1))
		d := metricsDelta(h.cacheMetrics(), before)
		if got.State != "completed" || got.Source != "upstream" || got.ResultRef != c.ref || got.Tries != 1 {
			t.Fatalf("%s：调用 %+v，期望未命中、走上游且结果为 %s", c.name, got, c.ref)
		}
		if d["integrity_failure"] != 1 || d["hit"] != 0 || d["error"] != 0 || d["breaker_open"] != 0 || pathCount(t, fu, c.url) != n0+1 {
			t.Fatalf("%s：指标变化 %v，上游 %d → %d", c.name, d, n0, pathCount(t, fu, c.url))
		}
		// 篡改的条目已被删除，上游结果重新写入有效条目（下一个用例在它之上篡改）。
		waitSealed(t, rc, signer, c.key)
		t.Logf("E22 %s：未命中，指标变化 %v", c.name, d)
	}
	h.finish()
}

// TestE23CacheHitCancelRace：E23——缓存命中之后、Tx2（CompleteFromCache）之前取消先提交 → Tx2 复查访问时读到
// desired = cancel（或取消随即提交的访问撤销）而拒绝：调用置为 failed（cancel_requested 或 access_revoked）、没有
// try，命中的 blob 没有授权到该任务的 scope，也没有来源记录；任务以 cancelled 结束，上游不被访问。
func TestE23CacheHitCancelRace(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	u := cachePage(fu, "/e23-"+randSuffix()+"/a", ccHeader("max-age=600"))
	h.start(cacheCfg(fu, addr))
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}
	id0 := h.runFetches("e23-seed", u)
	ref := cacheCallRow(t, h.dsn, id0, fetchCallID(1)).ResultRef
	waitSealed(t, testRedis(t, addr), signer, fetchKey(t, u))

	entered := make(chan call.CacheCompletion, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) }) // 先于装置的收尾执行（LIFO），不让服务停在钩子里
	h.gate.mu.Lock()
	h.gate.cacheHook = func(r call.CacheCompletion) {
		select {
		case entered <- r:
		default:
		}
		<-release
	}
	h.gate.mu.Unlock()

	id := h.submit("e23", spec(nil, "e23", nil, step{"op": "fetch", "step_id": "f1", "url": u}))
	var cc call.CacheCompletion
	select {
	case cc = <-entered:
	case <-time.After(waitLimit):
		t.Fatal("缓存命中未到达 Tx2")
	}
	if cc.TaskID != id || cc.Source != call.SourceCache || cc.ResultSHA256 != ref {
		t.Fatalf("Tx2 的请求 %+v，期望任务 %s 以 cache 来源提交 %s", cc, id, ref)
	}
	// Tx2 停在命中之后：取消在它之前提交（取消请求本身在另一个 goroutine 中，不依赖 Tx2 何时继续）。
	cancelErr := make(chan error, 1)
	go func() {
		st, code, err := h.cancelTaskE(id, "e23-cancel")
		if err == nil && st != http.StatusOK {
			err = fmt.Errorf("取消 = %d %s", st, code)
		}
		cancelErr <- err
	}()
	eventually(t, "取消已提交（task_control.desired = cancel）", func() bool {
		var desired string
		h.queryRow("SELECT desired FROM task_control WHERE task_id = $1", []any{id}, &desired)
		return desired == "cancel"
	})
	releaseOnce.Do(func() { close(release) })
	if err := <-cancelErr; err != nil {
		t.Fatal(err)
	}
	if v := h.waitTerminal(id); v.Status != "cancelled" {
		t.Fatalf("任务结束为 %s/%s", v.Status, v.StatusReason)
	}
	// 任务终态不等待 Gateway 中的请求：放行后 Tx2 才被拒、调用才离开 resolving。
	var c cacheCall
	eventually(t, "被暂停的 Tx2 结束、调用离开 resolving", func() bool {
		c = cacheCallRow(t, h.dsn, id, fetchCallID(1))
		return c.State != "resolving"
	})
	var scoped, prov int
	h.queryRow("SELECT count(*) FROM scope_blobs WHERE scope_kind = 'task' AND scope_id = $1 AND sha256 = $2", []any{id, ref}, &scoped)
	h.queryRow("SELECT count(*) FROM blob_provenance WHERE scope_kind = 'task' AND scope_id = $1", []any{id}, &prov)
	// 拒绝原因取决于 Tx2 读到的是哪一个已提交的取消效果：desired = cancel（cancel_requested），或取消随即撤销的
	// attempt 访问（access_revoked）。两者都在 Tx2 之前提交，结果都不被授权。
	if c.State != "failed" || (c.FailReason != "cancel_requested" && c.FailReason != "access_revoked") ||
		c.Source != "upstream" || c.ResultRef != "" || c.Tries != 0 ||
		scoped != 0 || prov != 0 {
		t.Fatalf("调用 %+v；scope_blobs %d 行、blob_provenance %d 行；期望取消先提交时结果不被授权", c, scoped, prov)
	}
	if n := pathCount(t, fu, u); n != 1 {
		t.Fatalf("上游收到 %d 个抓取请求，期望 1（只有播种任务）", n)
	}
	t.Logf("E23：命中 %s 后取消先提交 → 调用 %+v，结果未授权", ref, c)
	h.finish()
}

// TestE24CoalesceWithinAttempt：E24（同一 sub-run 的一半）——同一 attempt（sub-run 为 root）内 10 个相同的抓取
// 并发到达（各自的调用 ID）→ 上游只收到一次请求：leader 的调用 source = upstream、1 个 try；其余 9 个以 coalesced
// 完成、没有 try，全部得到同一结果 blob；/status 的 coalesced 加 9。
func TestE24CoalesceWithinAttempt(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	u := cachePage(fu, "/e24-"+randSuffix()+"/a", nil) // 没有新鲜度头：不写缓存，只看合并
	fu.Inject(fakeupstream.Fetch, 1, fakeupstream.Action{Hang: true})
	h.start(cacheCfg(fu, addr))
	id := h.submit("e24", spec(nil, "e24", nil, sleepStep(600000)))
	sock := h.waitAttemptSocket(id)
	before := h.cacheMetrics()

	const n = 10
	body := `{"url":"` + u + `"}`
	res := make([]gwResp, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i], errs[i] = gwDo(sock, http.MethodPost, "/v1/fetch", fmt.Sprintf("e2e/e24/fetch/%d", i+1), body)
		}()
	}
	eventually(t, "leader 的请求挂起在上游、9 个请求作为 follower 加入", func() bool {
		return fu.Hanging() == 1 && metricsDelta(h.cacheMetrics(), before)["coalesced"] == n-1
	})
	fu.Release()
	wg.Wait()
	for i := range n {
		if errs[i] != nil || res[i].Status != http.StatusOK || res[i].Replayed || res[i].Blob == "" || res[i].Blob != res[0].Blob {
			t.Fatalf("请求 %d：%+v %v（请求 1：%+v）", i+1, res[i], errs[i], res[0])
		}
	}
	var upstreamCalls, coalesced, tries, distinct int
	h.queryRow(`SELECT count(*) FILTER (WHERE source = 'upstream'), count(*) FILTER (WHERE source = 'coalesced'),
			COALESCE(sum(tries_used), 0)::bigint, count(DISTINCT result_ref)
		FROM calls WHERE task_id = $1 AND call_id LIKE 'e2e/e24/%' AND state = 'completed'`, []any{id},
		&upstreamCalls, &coalesced, &tries, &distinct)
	if upstreamCalls != 1 || coalesced != n-1 || tries != 1 || distinct != 1 || fu.Count(fakeupstream.Fetch) != 1 {
		t.Fatalf("calls：upstream %d、coalesced %d、tries 合计 %d、不同结果 %d；上游抓取 %d 次", upstreamCalls, coalesced, tries,
			distinct, fu.Count(fakeupstream.Fetch))
	}
	d := metricsDelta(h.cacheMetrics(), before)
	if d["coalesced"] != n-1 || d["hit"] != 0 {
		t.Fatalf("指标变化 %v", d)
	}
	if st, code := h.cancelTask(id, "e24-cancel"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	if v := h.waitTerminal(id); v.Status != "cancelled" {
		t.Fatalf("任务结束为 %s", v.Status)
	}
	t.Logf("E24：10 个相同抓取 → 上游 %d 次；calls upstream=%d coalesced=%d；指标变化 %v；结果 %s",
		fu.Count(fakeupstream.Fetch), upstreamCalls, coalesced, d, res[0].Blob)
	h.finish()
}

// TestE25FreshnessViaFakeUpstream：E25——fake upstream 的响应头复现新鲜度表：
//   - 短 max-age（3 s）可缓存，expires_at 不晚于响应时刻 + 3 s；过期后同一抓取回到上游（不返回过期条目）；
//   - max-age=60 且 Age: 50 → expires_at 约为响应时刻 + 10 s（按年龄扣减，寿命不被延长）；命中不改写条目；
//   - Vary: Accept-Encoding 可缓存；Vary 含其他维度或 *、重复的 Cache-Control 指令、重复的 Expires、无效的 Expires、
//     no-store、Set-Cookie、不认识的指令、没有显式新鲜度 → 不缓存（第二次抓取仍走上游，Redis 中没有条目）。
func TestE25FreshnessViaFakeUpstream(t *testing.T) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	sfx := randSuffix()
	page := func(name string, hdr http.Header) string { return cachePage(fu, "/e25-"+sfx+"/"+name, hdr) }
	in1h := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	in2h := time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat)
	short := page("short", ccHeader("max-age=3"))
	aged := page("aged", http.Header{"Cache-Control": {"max-age=60"}, "Age": {"50"}})
	varyAE := page("vary-ae", http.Header{"Cache-Control": {"max-age=600"}, "Vary": {"Accept-Encoding"}})
	uncacheable := []string{
		page("vary-cookie", http.Header{"Cache-Control": {"max-age=600"}, "Vary": {"Cookie"}}),
		page("vary-star", http.Header{"Cache-Control": {"max-age=600"}, "Vary": {"*"}}),
		page("conflicting-cc", http.Header{"Cache-Control": {"max-age=60", "max-age=120"}}),
		page("duplicate-expires", http.Header{"Expires": {in1h, in2h}}),
		page("invalid-expires", http.Header{"Cache-Control": {"max-age=600"}, "Expires": {"not-a-date"}}),
		page("no-store", ccHeader("no-store, max-age=600")),
		page("set-cookie", http.Header{"Cache-Control": {"max-age=600"}, "Set-Cookie": {"sid=e2e"}}),
		page("unknown-directive", ccHeader("max-age=600, x-agentbox-unknown")),
		page("no-freshness", nil),
	}
	h.start(cacheCfg(fu, addr))
	rc := testRedis(t, addr)
	signer, err := cache.LoadKeys(h.dir)
	if err != nil {
		t.Fatal(err)
	}

	h.runFetches("e25-1", append([]string{short, aged, varyAE}, uncacheable...)...)
	_, shortVal := waitSealed(t, rc, signer, fetchKey(t, short))
	agedRaw, agedVal := waitSealed(t, rc, signer, fetchKey(t, aged))
	_, varyVal := waitSealed(t, rc, signer, fetchKey(t, varyAE))
	life := func(v cache.Value) time.Duration { return v.ExpiresAt.Sub(v.FetchedAt) }
	if l := life(shortVal); l <= 0 || l > 3*time.Second {
		t.Fatalf("max-age=3 的条目寿命 %s，期望 (0, 3s]", l)
	}
	if l := life(agedVal); l <= 5*time.Second || l > 10*time.Second {
		t.Fatalf("max-age=60、Age: 50 的条目寿命 %s，期望约 10 s（不超过 10 s）", l)
	}
	if l := life(varyVal); l <= 590*time.Second || l > 600*time.Second {
		t.Fatalf("Vary: Accept-Encoding 的条目寿命 %s，期望约 600 s", l)
	}
	for _, u := range uncacheable {
		if _, ok := cacheEntry(t, rc, fetchKey(t, u)); ok {
			t.Fatalf("%s 不应被缓存", u)
		}
	}

	// 第二个任务：可缓存的命中，其余仍走上游；命中不改写（不延长）条目。
	m1 := h.cacheMetrics()
	id2 := h.runFetches("e25-2", append([]string{aged, varyAE}, uncacheable...)...)
	for i := range 2 + len(uncacheable) {
		want := "upstream"
		if i < 2 {
			want = "cache"
		}
		if c := cacheCallRow(t, h.dsn, id2, fetchCallID(i+1)); c.State != "completed" || c.Source != want {
			t.Fatalf("第二个任务的调用 %d %+v，期望 %s", i+1, c, want)
		}
	}
	if raw, ok := cacheEntry(t, rc, fetchKey(t, aged)); !ok || !bytes.Equal(raw, agedRaw) {
		t.Fatal("命中之后 Age 页面的条目被改写或消失（命中不得延长寿命）")
	}
	if d := metricsDelta(h.cacheMetrics(), m1); d["hit"] != 2 {
		t.Fatalf("第二个任务的指标变化 %v，期望 hit = 2", d)
	}

	// 短 max-age 过期后：同一抓取回到上游（不返回过期条目），不计命中。
	eventually(t, "max-age=3 的条目过了 expires_at", func() bool { return time.Now().After(shortVal.ExpiresAt) })
	m2 := h.cacheMetrics()
	id3 := h.runFetches("e25-3", short)
	if c := cacheCallRow(t, h.dsn, id3, fetchCallID(1)); c.Source != "upstream" {
		t.Fatalf("过期后的抓取 %+v，期望走上游", c)
	}
	if d := metricsDelta(h.cacheMetrics(), m2); d["hit"] != 0 {
		t.Fatalf("过期后的指标变化 %v", d)
	}
	for _, c := range []struct {
		u    string
		want int
	}{{short, 2}, {aged, 1}, {varyAE, 1}} {
		if n := pathCount(t, fu, c.u); n != c.want {
			t.Fatalf("%s 到达上游 %d 次，期望 %d", c.u, n, c.want)
		}
	}
	for _, u := range uncacheable {
		if n := pathCount(t, fu, u); n != 2 {
			t.Fatalf("不可缓存的 %s 到达上游 %d 次，期望 2", u, n)
		}
	}
	t.Logf("E25：寿命 short=%s aged=%s vary-ae=%s；%d 种不支持的响应均未缓存", life(shortVal), life(agedVal), life(varyVal), len(uncacheable))
	h.finish()
}

// ---- 缓存收益测量（§16.6；bench/cache/run.sh）----

// benchSample 是一次 Gateway 调用的测量。
type benchSample struct {
	Group     string
	Run       int
	CallID    string
	Kind      string
	Source    string
	LatencyMs float64
}

// benchGroup 是一组（缓存开或关）的汇总。
type benchGroup struct {
	Name          string
	Calls         int
	Hits          int
	UpstreamCalls int
	Metrics       map[string]int64
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// TestCacheBenefit 测量共享缓存的收益（只由 bench/cache/run.sh 运行：设置 AGENTBOX_CACHE_BENCH_OUT 时执行）。
// 两组（缓存开、缓存关）各 N 次（默认 10）重复同一组查询（1 个搜索 + 3 个抓取）；每次重复是一个新的任务、使用新的
// 调用 ID，测到的是跨任务共享缓存，而不是同一调用 ID 的 journal 重放。测试直接在 attempt 的 socket 上逐个发出调用，
// 记录调用方看到的延迟与调用的来源（calls.source）；上游是 fake upstream，正常回复前固定延迟（默认 200 ms）。
// 原始数据与汇总以 Markdown 写入 AGENTBOX_CACHE_BENCH_OUT。
func TestCacheBenefit(t *testing.T) {
	out := os.Getenv("AGENTBOX_CACHE_BENCH_OUT")
	if out == "" {
		t.Skip("缓存收益测量只由 bench/cache/run.sh 运行（设置 AGENTBOX_CACHE_BENCH_OUT）")
	}
	addr := testRedisAddr(t)
	runs := envInt("AGENTBOX_CACHE_BENCH_RUNS", 10)
	latency := time.Duration(envInt("AGENTBOX_CACHE_BENCH_LATENCY_MS", 200)) * time.Millisecond
	sfx := randSuffix()
	query := "缓存收益 " + sfx
	started := time.Now().UTC()
	var samples []benchSample
	var groups []benchGroup
	for _, g := range []struct {
		name  string
		cache bool
	}{{"cache_on", true}, {"cache_off", false}} {
		t.Run(g.name, func(t *testing.T) {
			fu := fakeupstream.New()
			t.Cleanup(fu.Close)
			fu.SetLatency(fakeupstream.Search, latency)
			fu.SetLatency(fakeupstream.Fetch, latency)
			pages := make([]string, 3)
			for i := range pages {
				pages[i] = cachePage(fu, fmt.Sprintf("/bench-%s/%s/%d", sfx, g.name, i+1), ccHeader("max-age=3600"))
			}
			h := newHarness(t)
			cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
			if g.cache {
				cfg.RedisAddr = addr
			}
			h.start(cfg)
			for run := 1; run <= runs; run++ {
				id := h.submit(fmt.Sprintf("bench-%s-%d", g.name, run), spec(nil, "bench", nil, sleepStep(600000)))
				sock := h.waitAttemptSocket(id)
				type req struct{ kind, path, body string }
				reqs := []req{{"search", "/v1/search", `{"query":"` + query + `","max_results":3}`}}
				for _, p := range pages {
					reqs = append(reqs, req{"fetch", "/v1/fetch", `{"url":"` + p + `"}`})
				}
				for i, r := range reqs {
					callID := fmt.Sprintf("bench/r%d/%s/%d", run, r.kind, i+1)
					t0 := time.Now()
					resp, err := gwDo(sock, http.MethodPost, r.path, callID, r.body)
					dur := time.Since(t0)
					if err != nil || resp.Status != http.StatusOK {
						t.Fatalf("%s：%+v %v", callID, resp, err)
					}
					c := cacheCallRow(t, h.dsn, id, callID)
					samples = append(samples, benchSample{Group: g.name, Run: run, CallID: callID, Kind: r.kind, Source: c.Source,
						LatencyMs: float64(dur.Microseconds()) / 1000})
				}
				if st, code := h.cancelTask(id, fmt.Sprintf("bench-%s-%d-cancel", g.name, run)); st != http.StatusOK {
					t.Fatalf("取消 = %d %s", st, code)
				}
				h.waitTerminal(id)
			}
			bg := benchGroup{Name: g.name, UpstreamCalls: fu.Count(fakeupstream.Search) + fu.Count(fakeupstream.Fetch)}
			for _, s := range samples {
				if s.Group == g.name {
					bg.Calls++
					if s.Source == "cache" {
						bg.Hits++
					}
				}
			}
			if g.cache {
				bg.Metrics = h.cacheMetrics()
			}
			groups = append(groups, bg)
			h.finish()
		})
	}
	if t.Failed() {
		return
	}
	report := benchReport(started, runs, latency, addr, groups, samples)
	if err := os.WriteFile(out, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("缓存收益测量写入 %s：\n%s", out, report)
}

// latencyStats 返回样本延迟的 n、最小、p50、p90、最大与平均（毫秒；分位数取最近秩）。
func latencyStats(xs []float64) string {
	if len(xs) == 0 {
		return "| 0 | – | – | – | – | – |"
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	rank := func(p float64) float64 {
		i := int(p*float64(len(s))+0.999999) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	sum := 0.0
	for _, x := range s {
		sum += x
	}
	return fmt.Sprintf("| %d | %.1f | %.1f | %.1f | %.1f | %.1f |", len(s), s[0], rank(0.5), rank(0.9), s[len(s)-1], sum/float64(len(s)))
}

func benchReport(started time.Time, runs int, latency time.Duration, redisAddr string, groups []benchGroup, samples []benchSample) string {
	var b strings.Builder
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		kernel = []byte("未知")
	}
	host, err := os.Hostname()
	if err != nil {
		host = "未知"
	}
	fmt.Fprintf(&b, "### 环境\n\n- 开始时间（UTC）：%s\n- 主机：%s；内核 %s；%s/%s，%d 个逻辑 CPU；%s\n",
		started.Format(time.RFC3339), host, strings.TrimSpace(string(kernel)), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(&b, "- Redis：%s；PostgreSQL：AGENTBOX_TEST_DATABASE_URL（本机）\n", redisAddr)
	fmt.Fprintf(&b, "- 上游：进程内 fake upstream（127.0.0.1），搜索与抓取的每个正常回复前固定延迟 %s\n", latency)
	fmt.Fprintf(&b, "- 每组 %d 次重复；每次重复 = 新任务（新的逻辑任务与调用 ID）依次发出 1 个搜索 + 3 个抓取（抓取页面 Cache-Control: max-age=3600）；延迟为测试在 attempt 的 Gateway socket 上看到的端到端时间\n\n", runs)
	b.WriteString("### 汇总\n\n| 组 | 调用数 | 命中（source = cache） | 命中率 | 上游请求数 | /status 缓存指标 |\n|---|---|---|---|---|---|\n")
	for _, g := range groups {
		m := "（缓存关闭）"
		if g.Metrics != nil {
			keys := slices.Sorted(maps.Keys(g.Metrics))
			var parts []string
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%d", k, g.Metrics[k]))
			}
			m = strings.Join(parts, " ")
		}
		rate := 0.0
		if g.Calls > 0 {
			rate = float64(g.Hits) / float64(g.Calls)
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %.0f%% | %d | %s |\n", g.Name, g.Calls, g.Hits, rate*100, g.UpstreamCalls, m)
	}
	if len(groups) == 2 && groups[1].UpstreamCalls > 0 {
		fmt.Fprintf(&b, "\n上游请求减少：%d → %d（%.0f%%）。\n", groups[1].UpstreamCalls, groups[0].UpstreamCalls,
			100*(1-float64(groups[0].UpstreamCalls)/float64(groups[1].UpstreamCalls)))
	}
	b.WriteString("\n### 延迟分布（毫秒）\n\n| 样本 | n | 最小 | p50 | p90 | 最大 | 平均 |\n|---|---|---|---|---|---|---|\n")
	sel := func(f func(s benchSample) bool) []float64 {
		var xs []float64
		for _, s := range samples {
			if f(s) {
				xs = append(xs, s.LatencyMs)
			}
		}
		return xs
	}
	for _, row := range []struct {
		name string
		f    func(s benchSample) bool
	}{
		{"缓存开：命中（cache）", func(s benchSample) bool { return s.Group == "cache_on" && s.Source == "cache" }},
		{"缓存开：未命中（upstream）", func(s benchSample) bool { return s.Group == "cache_on" && s.Source == "upstream" }},
		{"缓存关：全部（upstream）", func(s benchSample) bool { return s.Group == "cache_off" }},
		{"命中：搜索", func(s benchSample) bool { return s.Source == "cache" && s.Kind == "search" }},
		{"命中：抓取", func(s benchSample) bool { return s.Source == "cache" && s.Kind == "fetch" }},
		{"上游：搜索", func(s benchSample) bool { return s.Source == "upstream" && s.Kind == "search" }},
		{"上游：抓取", func(s benchSample) bool { return s.Source == "upstream" && s.Kind == "fetch" }},
	} {
		fmt.Fprintf(&b, "| %s %s\n", row.name, latencyStats(sel(row.f)))
	}
	b.WriteString("\n### 原始数据\n\n| 组 | 重复 | 调用 ID | 类别 | 来源 | 延迟（ms） |\n|---|---|---|---|---|---|\n")
	for _, s := range samples {
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %.1f |\n", s.Group, s.Run, s.CallID, s.Kind, s.Source, s.LatencyMs)
	}
	return b.String()
}
