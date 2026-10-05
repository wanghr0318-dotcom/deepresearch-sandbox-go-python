package cache

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
)

// Redis 往返用例使用真实 Redis：本地由 deploy/docker-compose.yml 提供，CI 由 Redis service 容器提供。
// 本地未设置 AGENTBOX_TEST_REDIS_ADDR 时跳过；CI 中未设置则失败。超时与熔断用例使用不回复的 TCP 服务
// 与注入时钟，不以 sleep 同步；耗时只作为失败的上限。

const redisAddrEnv = "AGENTBOX_TEST_REDIS_ADDR"

func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		if os.Getenv("CI") == "true" {
			t.Fatalf("CI 中必须设置 %s", redisAddrEnv)
		}
		t.Skipf("未设置 %s，跳过", redisAddrEnv)
	}
	return addr
}

func testKey(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "agentbox-test:" + t.Name() + ":" + hex.EncodeToString(b)
}

// TestRedisRoundTrip：对真实 Redis 的 PING、SET PX、GET、DEL 往返；空值是命中；4 KiB 上限在发出前检查。
func TestRedisRoundTrip(t *testing.T) {
	r := NewRedis(RedisConfig{Addr: redisAddr(t), ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := context.Background()
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	key := testKey(t)
	t.Cleanup(func() {
		if err := r.Del(context.Background(), key); err != nil {
			t.Errorf("清理 Del: %v", err)
		}
	})
	if _, ok, err := r.Get(ctx, key); err != nil || ok {
		t.Fatalf("Get 不存在的键: ok=%v err=%v", ok, err)
	}
	val := []byte("a\r\nb$-1\r\n\x00") // 值中的 CRLF 与 RESP 标记不影响二进制安全
	if err := r.Set(ctx, key, val, 90*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok, err := r.Get(ctx, key)
	if err != nil || !ok || string(got) != string(val) {
		t.Fatalf("Get = %q, %v, %v；期望 %q", got, ok, err, val)
	}
	// PX 以毫秒设置有效期：用 PTTL 确认剩余寿命在 (0, 90 s] 内。
	rep, err := r.do(ctx, time.Second, "PTTL", key)
	if err != nil || rep.kind != ':' || rep.n <= 0 || rep.n > 90_000 {
		t.Fatalf("PTTL = %+v, %v", rep, err)
	}
	if err := r.Del(ctx, key); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, ok, err := r.Get(ctx, key); err != nil || ok {
		t.Fatalf("Del 后 Get: ok=%v err=%v", ok, err)
	}
	if err := r.Del(ctx, key); err != nil {
		t.Fatalf("Del 不存在的键: %v", err)
	}

	// 空值是命中，且与不存在区分。
	if err := r.Set(ctx, key, []byte{}, time.Minute); err != nil {
		t.Fatalf("Set 空值: %v", err)
	}
	if got, ok, err := r.Get(ctx, key); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("Get 空值 = %q, %v, %v", got, ok, err)
	}

	// 恰好 4 KiB 可写可读；4 KiB + 1 在发出前拒绝。
	full := []byte(strings.Repeat("x", MaxValueSize))
	if err := r.Set(ctx, key, full, time.Minute); err != nil {
		t.Fatalf("Set 4 KiB: %v", err)
	}
	if got, ok, err := r.Get(ctx, key); err != nil || !ok || len(got) != MaxValueSize {
		t.Fatalf("Get 4 KiB: len=%d ok=%v err=%v", len(got), ok, err)
	}
	if err := r.Set(ctx, key, append(full, 'y'), time.Minute); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Set 4 KiB+1 = %v，期望 ErrValueTooLarge", err)
	}
	if err := r.Set(ctx, key, val, 0); err == nil {
		t.Fatal("ttl 为 0 的 Set 应被拒绝")
	}

	// Redis 错误回复（对字符串键执行列表命令）返回 *RedisError，连接仍可用。
	if _, err := r.do(ctx, time.Second, "LPUSH", key, "v"); !isRedisError(err) {
		t.Fatalf("LPUSH 字符串键 = %v，期望 RedisError", err)
	}
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("错误回复后 Ping: %v", err)
	}
}

func isRedisError(err error) bool {
	var re *RedisError
	return errors.As(err, &re)
}

// silentServer 接受连接、读取并丢弃请求，从不回复。
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var conns []net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = io.Copy(io.Discard, c) // 连接被任一端关闭时结束；读取错误无关紧要
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close() // 只为结束 Accept 循环
		mu.Lock()
		for _, c := range conns {
			_ = c.Close() // 只为结束读取协程
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

// TestReadTimeoutCountsAsError：服务器不回复时，GET 在读期限内返回超时错误；经 Guarded 视为未命中并计为错误。
// SET 与 DEL 同样受写期限约束。
func TestReadTimeoutCountsAsError(t *testing.T) {
	r := NewRedis(RedisConfig{Addr: silentServer(t)})
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := context.Background()
	const limit = 2 * time.Second // 只作为失败上限；实际期限由 50 ms 的连接期限保证

	start := time.Now()
	_, _, err := r.Get(ctx, "k")
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Get = %v，期望超时", err)
	}
	if d := time.Since(start); d > limit {
		t.Fatalf("Get 用时 %v", d)
	}
	if err := r.Set(ctx, "k", []byte("v"), time.Minute); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Set = %v，期望超时", err)
	}
	if err := r.Del(ctx, "k"); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Del = %v，期望超时", err)
	}

	var m Metrics
	g := Guarded(r, NewBreaker(nil), &m)
	start = time.Now()
	v, ok, err := g.Get(ctx, "k")
	if v != nil || ok || err != nil {
		t.Fatalf("Guarded Get = %q, %v, %v；期望未命中", v, ok, err)
	}
	if d := time.Since(start); d > limit {
		t.Fatalf("Guarded Get 用时 %v", d)
	}
	if m.Error.Load() != 1 || m.Hit.Load() != 0 || m.Miss.Load() != 0 {
		t.Fatalf("指标 error=%d hit=%d miss=%d", m.Error.Load(), m.Hit.Load(), m.Miss.Load())
	}

	// 调用方 ctx 先于操作期限结束：返回 ctx 错误，不计入熔断。
	b := NewBreaker(nil)
	g = Guarded(r, b, &m)
	for i := 0; i < DefaultFailureThreshold; i++ {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, ok, err := g.Get(cctx, "k"); ok || err != nil {
			t.Fatalf("取消的 Get = %v, %v", ok, err)
		}
	}
	if b.State() != StateClosed {
		t.Fatalf("调用方取消不应打开熔断，state=%v", b.State())
	}

	// 连续 5 次超时 → 熔断打开，之后的操作不发出。
	for i := 0; i < DefaultFailureThreshold; i++ {
		if _, ok, err := g.Get(ctx, "k"); ok || err != nil {
			t.Fatalf("Get = %v, %v", ok, err)
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("5 次超时后 state=%v", b.State())
	}
	if err := g.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("熔断时 Set = %v", err)
	}
	if m.BreakerOpen.Load() != 1 {
		t.Fatalf("breaker_open=%d", m.BreakerOpen.Load())
	}
}

// fakeClock 是可手动推进的时钟。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// scriptedKV 依次返回预设的错误，并记录被调用的次数。
type scriptedKV struct {
	calls int
	fail  bool
}

var errBoom = errors.New("boom")

func (k *scriptedKV) result() error {
	k.calls++
	if k.fail {
		return errBoom
	}
	return nil
}

func (k *scriptedKV) Get(context.Context, string) ([]byte, bool, error) {
	if err := k.result(); err != nil {
		return nil, false, err
	}
	return []byte("v"), true, nil
}

func (k *scriptedKV) Set(context.Context, string, []byte, time.Duration) error { return k.result() }
func (k *scriptedKV) Del(context.Context, string) error                        { return k.result() }

// TestBreakerOpensAndHalfOpens：连续 5 次失败打开；30 s 内不发出；届满后半开只放行一次试探；
// 试探失败重新打开，试探成功关闭；中间的成功清零连续失败计数。
func TestBreakerOpensAndHalfOpens(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	b := NewBreaker(clk.now)
	var m Metrics
	kv := &scriptedKV{fail: true}
	g := Guarded(kv, b, &m)
	ctx := context.Background()

	// 4 次失败 + 1 次成功 + 4 次失败：仍关闭（成功清零计数）。
	for i := 0; i < 4; i++ {
		if err := g.Del(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}
	kv.fail = false
	if v, ok, err := g.Get(ctx, "k"); !ok || err != nil || string(v) != "v" {
		t.Fatalf("成功的 Get = %q, %v, %v", v, ok, err)
	}
	kv.fail = true
	for i := 0; i < 4; i++ {
		if _, ok, err := g.Get(ctx, "k"); ok || err != nil {
			t.Fatalf("失败的 Get = %v, %v", ok, err)
		}
	}
	if b.State() != StateClosed {
		t.Fatalf("非连续失败不应打开，state=%v", b.State())
	}

	// 第 5 次连续失败 → 打开。
	if err := g.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if b.State() != StateOpen || m.Error.Load() != 9 {
		t.Fatalf("state=%v error=%d", b.State(), m.Error.Load())
	}

	// 打开期间不发出，视为未命中。
	calls := kv.calls
	clk.advance(DefaultOpenDuration - time.Nanosecond)
	if _, ok, err := g.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("打开时 Get = %v, %v", ok, err)
	}
	if kv.calls != calls || m.BreakerOpen.Load() != 1 {
		t.Fatalf("打开时发出了操作：calls %d→%d breaker_open=%d", calls, kv.calls, m.BreakerOpen.Load())
	}

	// 30 s 届满 → 半开：只放行一个试探。
	clk.advance(time.Nanosecond)
	if !b.Allow() {
		t.Fatal("届满后应放行试探")
	}
	if b.State() != StateHalfOpen || b.Allow() {
		t.Fatalf("半开时只放行一个试探，state=%v", b.State())
	}
	b.Abort() // 试探被调用方放弃：仍半开，名额释放
	if b.State() != StateHalfOpen {
		t.Fatalf("Abort 后 state=%v", b.State())
	}

	// 试探失败 → 重新打开，计时从此刻开始。
	if _, ok, err := g.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("试探 Get = %v, %v", ok, err)
	}
	if b.State() != StateOpen || kv.calls != calls+1 {
		t.Fatalf("试探失败后 state=%v calls=%d", b.State(), kv.calls)
	}
	clk.advance(DefaultOpenDuration - time.Nanosecond)
	if b.Allow() {
		t.Fatal("重新打开后 30 s 内不应放行")
	}

	// 再次届满，试探成功 → 关闭。
	clk.advance(time.Nanosecond)
	kv.fail = false
	if _, ok, err := g.Get(ctx, "k"); !ok || err != nil {
		t.Fatalf("试探成功的 Get = %v, %v", ok, err)
	}
	if b.State() != StateClosed {
		t.Fatalf("试探成功后 state=%v", b.State())
	}

	// 打开期间迟到的成功与失败不改变状态。
	b2 := NewBreaker(clk.now)
	for i := 0; i < DefaultFailureThreshold; i++ {
		b2.Failure()
	}
	b2.Success()
	b2.Failure()
	if b2.State() != StateOpen {
		t.Fatalf("迟到的报告改变了状态：%v", b2.State())
	}

	// 超过 4 KiB 的值不发出、计错误、不计入熔断。
	errs, calls := m.Error.Load(), kv.calls
	if err := g.Set(ctx, "k", make([]byte, MaxValueSize+1), time.Minute); err != nil {
		t.Fatal(err)
	}
	if kv.calls != calls || m.Error.Load() != errs+1 {
		t.Fatalf("超大值：calls %d→%d error %d→%d", calls, kv.calls, errs, m.Error.Load())
	}
}

// TestReadReply：RESP2 解析的边界。
func TestReadReply(t *testing.T) {
	big := strings.Repeat("x", MaxValueSize)
	cases := []struct {
		name, in string
		want     reply
		wantErr  error // nil 表示成功；*RedisError 用 errRedisReply 标记
	}{
		{"简单字符串", "+OK\r\n", reply{kind: '+', str: "OK"}, nil},
		{"整数", ":3\r\n", reply{kind: ':', n: 3}, nil},
		{"null 批量", "$-1\r\n", reply{kind: '$', null: true}, nil},
		{"空批量", "$0\r\n\r\n", reply{kind: '$', bulk: []byte{}}, nil},
		{"含 CRLF 的批量", "$4\r\na\r\nb\r\n", reply{kind: '$', bulk: []byte("a\r\nb")}, nil},
		{"恰好 4 KiB", "$4096\r\n" + big + "\r\n", reply{kind: '$', bulk: []byte(big)}, nil},
		{"错误回复", "-ERR boom\r\n", reply{}, errRedisReply},
		{"超过 4 KiB", "$4097\r\n" + big + "y\r\n", reply{}, ErrValueTooLarge},
		{"批量缺少 CRLF", "$3\r\nabcd\r\n", reply{}, ErrProtocol},
		{"批量被截断", "$3\r\nab", reply{}, io.ErrUnexpectedEOF},
		{"非法长度", "$-2\r\n", reply{}, ErrProtocol},
		{"非数字长度", "$abc\r\n", reply{}, ErrProtocol},
		{"非数字整数", ":x\r\n", reply{}, ErrProtocol},
		{"数组不支持", "*1\r\n$1\r\na\r\n", reply{}, ErrProtocol},
		{"未知类型", "OK\r\n", reply{}, ErrProtocol},
		{"缺少 CR", "+OK\n", reply{}, ErrProtocol},
		{"空行", "\r\n", reply{}, ErrProtocol},
		{"行被截断", "+OK", reply{}, io.ErrUnexpectedEOF},
		{"无输入", "", reply{}, io.EOF},
		{"行过长", "+" + strings.Repeat("a", 8192) + "\r\n", reply{}, ErrProtocol},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readReply(bufio.NewReader(strings.NewReader(c.in)))
			switch {
			case c.wantErr == errRedisReply:
				var re *RedisError
				if !errors.As(err, &re) || re.Msg != "ERR boom" {
					t.Fatalf("err = %v，期望 RedisError(ERR boom)", err)
				}
			case c.wantErr != nil:
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("err = %v，期望 %v", err, c.wantErr)
				}
			default:
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				if got.kind != c.want.kind || got.str != c.want.str || got.n != c.want.n || got.null != c.want.null ||
					string(got.bulk) != string(c.want.bulk) || (got.kind == '$' && !got.null && got.bulk == nil) {
					t.Fatalf("got %+v，期望 %+v", got, c.want)
				}
			}
		})
	}
}

var errRedisReply = errors.New("redis error reply")

// TestOversizedReplyDiscardsConnection：服务器回复超过 4 KiB 的值 → Get 报错且该连接被丢弃（回复流位置不再可信），
// 下一次操作使用新连接；错误回复则保留连接。
func TestOversizedReplyDiscardsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// 每个连接只服务一个请求序列：第 1 个连接先回复错误、再回复超大值；第 2 个连接回复 null。
	scripts := [][]string{{"-ERR first\r\n", "$5000\r\n"}, {"$-1\r\n"}}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, script := range scripts {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			br := bufio.NewReader(c)
			for _, resp := range script {
				if err := skipCommand(br); err != nil {
					t.Errorf("读取命令: %v", err)
					break
				}
				if _, err := io.WriteString(c, resp); err != nil {
					t.Errorf("写回复: %v", err)
					break
				}
			}
			defer func() { _ = c.Close() }() // 测试结束时关闭；错误无关紧要
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close() // 只为结束 Accept
		wg.Wait()
	})

	r := NewRedis(RedisConfig{Addr: ln.Addr().String(), ReadTimeout: 2 * time.Second})
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := context.Background()
	if _, _, err := r.Get(ctx, "k"); !isRedisError(err) {
		t.Fatalf("第 1 次 Get = %v，期望 RedisError", err)
	}
	if _, _, err := r.Get(ctx, "k"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("第 2 次 Get = %v，期望 ErrValueTooLarge", err)
	}
	if _, ok, err := r.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("第 3 次 Get（新连接）= %v, %v", ok, err)
	}
}

// skipCommand 读取并丢弃一条 RESP 数组命令。
func skipCommand(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if len(line) < 2 || line[0] != '*' {
		return ErrProtocol
	}
	n := 0
	for _, ch := range line[1:] {
		n = n*10 + int(ch-'0')
	}
	for i := 0; i < n; i++ {
		rep, err := readReply(br)
		if err != nil {
			return err
		}
		if rep.kind != '$' {
			return ErrProtocol
		}
	}
	return nil
}

// ---- 键与 URL 规范化（§11.3） ----

func TestKeyCanonicalization(t *testing.T) {
	mustKey := func(kind upstream.Kind, provider, ver, params string) string {
		t.Helper()
		k, err := Key(kind, provider, ver, []byte(params))
		if err != nil {
			t.Fatalf("Key(%s, %s, %s, %s): %v", kind, provider, ver, params, err)
		}
		return k
	}
	a := mustKey(upstream.KindSearch, "brave", "search/1", `{"q":"go","count":5,"opts":{"b":1,"a":2}}`)
	b := mustKey(upstream.KindSearch, "brave", "search/1", ` { "opts":{"a":2,"b":1}, "count":5.0, "q":"go" }`)
	if a != b {
		t.Errorf("键对参数顺序、空白与数字写法敏感：%s ≠ %s", a, b)
	}
	if !strings.HasPrefix(a, "v1:search:brave:search/1:") || len(a) != len("v1:search:brave:search/1:")+64 {
		t.Errorf("键格式 %q 不是 v1:{kind}:{provider}:{ver}:sha256", a)
	}
	for name, other := range map[string]string{
		"provider": mustKey(upstream.KindSearch, "tavily", "search/1", `{"q":"go","count":5,"opts":{"b":1,"a":2}}`),
		"version":  mustKey(upstream.KindSearch, "brave", "search/2", `{"q":"go","count":5,"opts":{"b":1,"a":2}}`),
		"params":   mustKey(upstream.KindSearch, "brave", "search/1", `{"q":"go","count":6,"opts":{"b":1,"a":2}}`),
	} {
		if other == a {
			t.Errorf("键对 %s 不敏感", name)
		}
	}

	f1 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"HTTP://Example.COM:80/a/B?x=1&y=2#frag"}`)
	f2 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"http://example.com/a/B?x=1&y=2"}`)
	if f1 != f2 {
		t.Errorf("抓取键未按 URL 规范化：%s ≠ %s", f1, f2)
	}
	if f3 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"http://example.com/a/B?y=2&x=1"}`); f3 == f1 {
		t.Error("查询串应原样保留（参数顺序不同的 URL 不应得到相同的键）")
	}
	if f4 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"http://example.com/a/b?x=1&y=2"}`); f4 == f1 {
		t.Error("路径大小写应保留")
	}

	for name, c := range map[string]struct {
		kind              upstream.Kind
		provider, ver, ps string
	}{
		"chat 不可缓存":        {upstream.KindChat, "p", "v", `{}`},
		"provider 含冒号":     {upstream.KindSearch, "a:b", "v", `{}`},
		"version 为空":       {upstream.KindSearch, "p", "", `{}`},
		"provider 含空白":     {upstream.KindSearch, "a b", "v", `{}`},
		"重复属性名":            {upstream.KindSearch, "p", "v", `{"q":1,"q":2}`},
		"参数不是对象":           {upstream.KindSearch, "p", "v", `[1]`},
		"参数不是 JSON":        {upstream.KindSearch, "p", "v", `{`},
		"抓取缺 url":          {upstream.KindFetch, "p", "v", `{"u":"http://a/"}`},
		"抓取 url 不是字符串":     {upstream.KindFetch, "p", "v", `{"url":1}`},
		"抓取 url 非 http":    {upstream.KindFetch, "p", "v", `{"url":"ftp://a/"}`},
		"抓取占用保留字段":         {upstream.KindFetch, "p", "v", `{"url":"http://a/","accept_encoding":"br"}`},
		"参数为 null":         {upstream.KindSearch, "p", "v", `null`},
		"version 含控制字符":    {upstream.KindSearch, "p", "v\x01", `{}`},
		"provider 含 DEL":   {upstream.KindSearch, "p\x7f", "v", `{}`},
		"抓取 url 为相对路径":     {upstream.KindFetch, "p", "v", `{"url":"/a"}`},
		"抓取 url 无 host":    {upstream.KindFetch, "p", "v", `{"url":"http:///a"}`},
		"抓取 url opaque 形式": {upstream.KindFetch, "p", "v", `{"url":"http:a"}`},
	} {
		if k, err := Key(c.kind, c.provider, c.ver, []byte(c.ps)); err == nil {
			t.Errorf("%s：应失败，得到键 %s", name, k)
		}
	}
}

func TestNormalizeURL(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"HTTP://WWW.Example.COM/Path?Q=A#Frag", "http://www.example.com/Path?Q=A"},
		{"http://example.com:80/", "http://example.com/"},
		{"https://example.com:443/x", "https://example.com/x"},
		{"http://example.com:443/x", "http://example.com:443/x"},
		{"https://example.com:80/x", "https://example.com:80/x"},
		{"https://example.com:8443/x", "https://example.com:8443/x"},
		{"http://example.com:/x", "http://example.com/x"},
		{"http://[2001:DB8::1]:80/x", "http://[2001:db8::1]/x"},
		{"http://[2001:db8::1]:8080/x", "http://[2001:db8::1]:8080/x"},
		{"http://example.com", "http://example.com"},
		{"http://example.com/a%2Fb?x=%41&y=1+2&&z", "http://example.com/a%2Fb?x=%41&y=1+2&&z"},
		{"http://example.com/?", "http://example.com/?"},
		{"http://example.com/#", "http://example.com/"},
	} {
		got, err := NormalizeURL(c.in)
		if err != nil {
			t.Errorf("NormalizeURL(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("NormalizeURL(%q) = %q，期望 %q", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "example.com/x", "mailto:a@b", "file:///etc/passwd", "http://%zz/", "http:a"} {
		if got, err := NormalizeURL(bad); err == nil {
			t.Errorf("NormalizeURL(%q) 应失败，得到 %q", bad, got)
		}
	}
}

// ---- 准入（§11.3） ----

func TestAdmit(t *testing.T) {
	ok200 := func(h map[string][]string) Response { return Response{Status: 200, Header: h, Size: 1024} }
	const page = "https://example.com/doc?q=go"
	for _, c := range []struct {
		name   string
		kind   upstream.Kind
		url    string
		r      Response
		reason string // "" 表示可缓存
	}{
		{"可缓存抓取", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {"public, max-age=60"}}), ""},
		{"Vary 仅 Accept-Encoding", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"accept-encoding"}}), ""},
		{"恰好 2 MiB", upstream.KindFetch, page, Response{Status: 200, Size: 2 << 20}, ""},
		{"搜索成功结果", upstream.KindSearch, "", Response{Status: 200, Size: 10}, ""},
		{"非凭据参数 keyword/author", upstream.KindFetch, "https://example.com/?keyword=go&author=x&monkey=1", ok200(nil), ""},

		{"模型调用", upstream.KindChat, page, ok200(nil), ReasonKind},
		{"未知类别", upstream.Kind("exec"), page, ok200(nil), ReasonKind},
		{"错误响应 404", upstream.KindFetch, page, Response{Status: 404, Size: 10}, ReasonStatus},
		{"非 200 的 2xx", upstream.KindFetch, page, Response{Status: 203, Size: 10}, ReasonStatus},
		{"搜索失败结果", upstream.KindSearch, "", Response{Status: 500, Size: 10}, ReasonStatus},
		{"超过 2 MiB", upstream.KindFetch, page, Response{Status: 200, Size: 2<<20 + 1}, ReasonSize},
		{"大小未知", upstream.KindFetch, page, Response{Status: 200, Size: -1}, ReasonSize},
		{"Set-Cookie", upstream.KindFetch, page, ok200(map[string][]string{"Set-Cookie": {"a=b"}}), ReasonSetCookie},
		{"set-cookie 非规范大小写", upstream.KindFetch, page, ok200(map[string][]string{"set-cookie": {"a=b"}}), ReasonSetCookie},
		{"no-store", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {"public", "max-age=60, No-Store"}}), ReasonNoStore},
		{"private（带字段名）", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {`private="X-Foo", max-age=60`}}), ReasonPrivate},
		{"no-cache", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {"no-cache"}}), ReasonNoCache},
		{"Vary: *", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"Accept-Encoding, *"}}), ReasonVaryStar},
		{"Vary 其他维度", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"Accept-Encoding", "User-Agent"}}), ReasonVaryDimension},
		{"Vary Cookie", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"Cookie"}}), ReasonVaryDimension},
		{"抓取 URL 为空", upstream.KindFetch, "", ok200(nil), ReasonURLInvalid},
		{"查询参数名无法解码", upstream.KindFetch, "https://example.com/?%zz=1", ok200(nil), ReasonURLInvalid},
		{"userinfo", upstream.KindFetch, "https://user:pw@example.com/", ok200(nil), ReasonURLCredentials},
		{"token", upstream.KindFetch, "https://example.com/?q=1&token=abc", ok200(nil), ReasonURLCredentials},
		{"sig（大写）", upstream.KindFetch, "https://example.com/?SIG=abc", ok200(nil), ReasonURLCredentials},
		{"signature", upstream.KindFetch, "https://example.com/?signature=abc", ok200(nil), ReasonURLCredentials},
		{"key", upstream.KindFetch, "https://example.com/?key=abc", ok200(nil), ReasonURLCredentials},
		{"auth", upstream.KindFetch, "https://example.com/?auth=abc", ok200(nil), ReasonURLCredentials},
		{"X-Amz-*", upstream.KindFetch, "https://b.s3.amazonaws.com/o?X-Amz-Signature=abc&X-Amz-Date=1", ok200(nil), ReasonURLCredentials},
		{"access_token", upstream.KindFetch, "https://example.com/?access_token=abc", ok200(nil), ReasonURLCredentials},
		{"api-key", upstream.KindFetch, "https://example.com/?api-key=abc", ok200(nil), ReasonURLCredentials},
		{"百分号编码的参数名", upstream.KindFetch, "https://example.com/?%74oken=abc", ok200(nil), ReasonURLCredentials},
		{"分号分隔", upstream.KindFetch, "https://example.com/?a=1;token=abc", ok200(nil), ReasonURLCredentials},
	} {
		ok, reason := Admit(c.kind, c.url, c.r)
		if ok != (c.reason == "") || reason != c.reason {
			t.Errorf("%s：Admit = (%v, %q)，期望原因 %q", c.name, ok, reason, c.reason)
		}
	}
}

// ---- 新鲜度（§11.3、E25） ----

func TestLifetimeE25(t *testing.T) {
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	date := func(d time.Duration) string { return base.Add(d).Format(time.RFC1123) }
	// resp 构造一个 request_time = base−1s、response_time = base 的 200 响应。
	resp := func(h map[string][]string) Response {
		return Response{Status: 200, Header: h, Size: 100, RequestTime: base.Add(-time.Second), ResponseTime: base}
	}
	const never = time.Duration(-1)
	for _, c := range []struct {
		name string
		r    Response
		now  time.Time
		want time.Duration // 相对 base 的 expires_at；never 表示不缓存
	}{
		{"max-age 减去响应延迟", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}}), base, 59 * time.Second},
		{"短 max-age：到手即过期", resp(map[string][]string{"Cache-Control": {"max-age=1"}, "Date": {date(0)}}), base, never},
		{"max-age=0", resp(map[string][]string{"Cache-Control": {"max-age=0"}}), base, never},
		{"Age 计入年龄", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"50"}, "Date": {date(0)}}), base, 9 * time.Second},
		{"Age 超过寿命", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"120"}}), base, never},
		{"Date 落后：apparent_age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(-30 * time.Second)}}), base, 30 * time.Second},
		{"Date 超前：apparent_age 取 0", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(time.Hour)}}), base, 59 * time.Second},
		{"无 Date 的 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60"}}), base, 59 * time.Second},
		{"s-maxage 优先于 max-age", resp(map[string][]string{"Cache-Control": {"max-age=1000, s-maxage=10"}}), base, 9 * time.Second},
		{"max-age 优先于 Expires", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}, "Expires": {date(time.Hour)}}), base, 59 * time.Second},
		{"Expires − Date", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(2 * time.Minute)}}), base, 119 * time.Second},
		{"Expires − Date（RFC 850 与 asctime）", resp(map[string][]string{
			"Date": {base.Format(time.RFC850)}, "Expires": {base.Add(2 * time.Minute).Format(time.ANSIC)}}), base, 119 * time.Second},
		{"Date 由调用方给出", Response{Status: 200, Header: map[string][]string{"Expires": {date(2 * time.Minute)}}, Date: base,
			RequestTime: base.Add(-time.Second), ResponseTime: base}, base, 119 * time.Second},
		{"Expires 无 Date", resp(map[string][]string{"Expires": {date(time.Hour)}}), base, never},
		{"Expires 已过", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(-time.Minute)}}), base, never},
		{"Expires 无效（0）", resp(map[string][]string{"Date": {date(0)}, "Expires": {"0"}}), base, never},
		{"Expires 无效且有 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Expires": {"-1"}}), base, never},
		{"引号形式的 max-age", resp(map[string][]string{"Cache-Control": {`max-age="60"`}}), base, 59 * time.Second},
		{"大小写与多行指令", resp(map[string][]string{"Cache-Control": {"Public", "Max-Age=60, must-revalidate"}}), base, 59 * time.Second},
		{"无显式新鲜度", resp(map[string][]string{"Cache-Control": {"public"}, "Date": {date(0)}, "Last-Modified": {date(-24 * time.Hour)}}), base, never},
		{"没有任何头", resp(nil), base, never},
		{"冲突：两个 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60", "max-age=120"}}), base, never},
		{"冲突：两个相同的 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60, max-age=60"}}), base, never},
		{"冲突：两个 Expires", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(time.Hour), date(2 * time.Hour)}}), base, never},
		{"冲突：两个 Date", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0), date(-time.Minute)}}), base, never},
		{"冲突：两种大小写的 Date", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}, "date": {date(0)}}), base, never},
		{"冲突：Date 与调用方给出的不一致", Response{Status: 200, Header: map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}},
			Date: base.Add(-time.Minute), RequestTime: base.Add(-time.Second), ResponseTime: base}, base, never},
		{"冲突：两个 Age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"1", "2"}}), base, never},
		{"无效 Age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"-1"}}), base, never},
		{"无效 max-age", resp(map[string][]string{"Cache-Control": {"max-age=abc"}}), base, never},
		{"max-age 缺参数", resp(map[string][]string{"Cache-Control": {"max-age"}}), base, never},
		{"max-age 空参数", resp(map[string][]string{"Cache-Control": {"max-age="}}), base, never},
		{"Cache-Control 语法错误", resp(map[string][]string{"Cache-Control": {`max-age=60, "x"`}}), base, never},
		{"引号未闭合", resp(map[string][]string{"Cache-Control": {`max-age="60`}}), base, never},
		{"不支持的指令", resp(map[string][]string{"Cache-Control": {"max-age=60, x-custom"}}), base, never},
		{"no-store", resp(map[string][]string{"Cache-Control": {"max-age=60, no-store"}}), base, never},
		{"no-cache", resp(map[string][]string{"Cache-Control": {"max-age=60, no-cache"}}), base, never},
		{"private", resp(map[string][]string{"Cache-Control": {"max-age=60, private"}}), base, never},
		{"24 h 上限", resp(map[string][]string{"Cache-Control": {"max-age=172800"}}), base, 24 * time.Hour},
		{"24 h 上限（Expires）", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(72 * time.Hour)}}), base, 24 * time.Hour},
		{"溢出的 delta-seconds 封顶后仍受 24 h 上限", resp(map[string][]string{"Cache-Control": {"max-age=99999999999999999999"}}), base, 24 * time.Hour},
		{"不延长：稍后重算截止点不变", resp(map[string][]string{"Cache-Control": {"max-age=60"}}), base.Add(30 * time.Second), 59 * time.Second},
		{"不延长：上限锚定在响应时刻", resp(map[string][]string{"Cache-Control": {"max-age=172800"}}), base.Add(time.Hour), 24 * time.Hour},
		{"不延长：截止点之后不缓存", resp(map[string][]string{"Cache-Control": {"max-age=60"}}), base.Add(59 * time.Second), never},
		{"缺 RequestTime", Response{Status: 200, Header: map[string][]string{"Cache-Control": {"max-age=60"}}, ResponseTime: base}, base, never},
		{"响应早于请求", Response{Status: 200, Header: map[string][]string{"Cache-Control": {"max-age=60"}},
			RequestTime: base, ResponseTime: base.Add(-time.Second)}, base, never},
	} {
		got, ok := Lifetime(c.r, c.now)
		switch {
		case c.want == never && ok:
			t.Errorf("%s：应不缓存，得到 expires_at = base%+v", c.name, got.Sub(base))
		case c.want != never && !ok:
			t.Errorf("%s：应可缓存至 base%+v，得到不缓存", c.name, c.want)
		case c.want != never && !got.Equal(base.Add(c.want)):
			t.Errorf("%s：expires_at = base%+v，期望 base%+v", c.name, got.Sub(base), c.want)
		}
	}

	// E25 的 Vary 行：新鲜度本身成立，准入决定是否缓存（只允许 Accept-Encoding 维度）。
	for _, c := range []struct {
		vary      string
		cacheable bool
	}{{"Accept-Encoding", true}, {"Accept-Language", false}, {"*", false}} {
		r := resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Vary": {c.vary}})
		admitted, _ := Admit(upstream.KindFetch, "https://example.com/", r)
		_, fresh := Lifetime(r, base)
		if got := admitted && fresh; got != c.cacheable {
			t.Errorf("Vary: %s：可缓存 = %v，期望 %v", c.vary, got, c.cacheable)
		}
	}
}

// ---- 签名值与密钥（§11.3、§11.5） ----

func policySampleValue(base time.Time) Value {
	return Value{
		BlobSHA256:  strings.Repeat("ab", 32),
		Status:      200,
		ContentType: "text/html; charset=utf-8",
		FinalURL:    "https://example.com/doc",
		Size:        1234,
		FetchedAt:   base,
		ExpiresAt:   base.Add(time.Hour),
	}
}

// policyMutate 解码缓存值、修改后重新编码（保持合法 JSON，用于篡改用例）。
func policyMutate(t *testing.T, raw []byte, f func(m map[string]any)) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	f(m)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSealOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 5, 12, 0, 0, 123456789, time.FixedZone("CST", 8*3600))
	const key = "v1:fetch:http_get:fetch/1:" + "00"
	v := policySampleValue(base)
	raw, err := s.Seal(key, v)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Open(key, raw, base.Add(time.Minute))
	if err != nil {
		t.Fatalf("往返: %v", err)
	}
	if got.BlobSHA256 != v.BlobSHA256 || got.Status != v.Status || got.ContentType != v.ContentType ||
		got.FinalURL != v.FinalURL || got.Size != v.Size || !got.FetchedAt.Equal(v.FetchedAt) ||
		!got.ExpiresAt.Equal(v.ExpiresAt) || got.KID != s.cur.kid || len(got.MAC) != 32 {
		t.Errorf("往返不一致：%+v", got)
	}

	var other string
	{
		s2, err := LoadKeys(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		other = s2.cur.kid
	}
	now := base.Add(time.Minute)
	for _, c := range []struct {
		name string
		key  string
		raw  []byte
		now  time.Time
		want error
	}{
		{"篡改 MAC", key, policyMutate(t, raw, func(m map[string]any) {
			mac := []byte(m["mac"].(string))
			if mac[0] == 'A' {
				mac[0] = 'B'
			} else {
				mac[0] = 'A'
			}
			m["mac"] = string(mac)
		}), now, ErrEntryBadMAC},
		{"换键", key + "1", raw, now, ErrEntryBadMAC},
		{"篡改 expires_at（延长）", key, policyMutate(t, raw, func(m map[string]any) {
			m["expires_at"] = base.Add(48 * time.Hour).UTC().Format(time.RFC3339Nano)
		}), now, ErrEntryBadMAC},
		{"篡改 blob_sha256", key, policyMutate(t, raw, func(m map[string]any) { m["blob_sha256"] = strings.Repeat("cd", 32) }), now, ErrEntryBadMAC},
		{"篡改 size", key, policyMutate(t, raw, func(m map[string]any) { m["size"] = 1 }), now, ErrEntryBadMAC},
		{"篡改 status", key, policyMutate(t, raw, func(m map[string]any) { m["status"] = 203 }), now, ErrEntryBadMAC},
		{"篡改 final_url", key, policyMutate(t, raw, func(m map[string]any) { m["final_url"] = "https://evil.example/" }), now, ErrEntryBadMAC},
		{"篡改 kid 为未知", key, policyMutate(t, raw, func(m map[string]any) { m["kid"] = other }), now, ErrEntryUnknownKID},
		{"kid 为空", key, policyMutate(t, raw, func(m map[string]any) { m["kid"] = "" }), now, ErrEntryUnknownKID},
		{"已过期（恰在 expires_at）", key, raw, v.ExpiresAt, ErrEntryExpired},
		{"已过期（之后）", key, raw, v.ExpiresAt.Add(time.Second), ErrEntryExpired},
		{"未知字段", key, policyMutate(t, raw, func(m map[string]any) { m["extra"] = 1 }), now, ErrEntryMalformed},
		{"缺 MAC", key, policyMutate(t, raw, func(m map[string]any) { delete(m, "mac") }), now, ErrEntryMalformed},
		{"MAC 长度错误", key, policyMutate(t, raw, func(m map[string]any) { m["mac"] = "AAAA" }), now, ErrEntryMalformed},
		{"尾随内容", key, append(append([]byte{}, raw...), []byte(` {}`)...), now, ErrEntryMalformed},
		{"重复属性名", key, append([]byte(`{"kid":"x",`), raw[1:]...), now, ErrEntryMalformed},
		{"不是 JSON", key, []byte("not json"), now, ErrEntryMalformed},
		{"超过 4 KiB 不解析", key, []byte(`{"pad":"` + strings.Repeat("x", MaxSealedBytes) + `"}`), now, ErrEntryTooLarge},
	} {
		if _, err := s.Open(c.key, c.raw, c.now); !errors.Is(err, c.want) {
			t.Errorf("%s：Open 错误 = %v，期望 %v", c.name, err, c.want)
		}
	}

	big := policySampleValue(base)
	big.FinalURL = "https://example.com/" + strings.Repeat("a", MaxSealedBytes)
	if _, err := s.Seal(key, big); !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("超过 4 KiB 的值：Seal 错误 = %v，期望 ErrEntryTooLarge", err)
	}
	for name, f := range map[string]func(v *Value){
		"blob_sha256 大写": func(v *Value) { v.BlobSHA256 = strings.Repeat("AB", 32) },
		"blob_sha256 过短": func(v *Value) { v.BlobSHA256 = "ab" },
		"状态码无效":          func(v *Value) { v.Status = 0 },
		"大小为负":           func(v *Value) { v.Size = -1 },
		"expires_at 为零":  func(v *Value) { v.ExpiresAt = time.Time{} },
	} {
		bad := policySampleValue(base)
		f(&bad)
		if _, err := s.Seal(key, bad); err == nil {
			t.Errorf("%s：Seal 应失败", name)
		}
	}
	if _, err := s.Seal("", v); err == nil {
		t.Error("空缓存键：Seal 应失败")
	}
}

func TestKeyFileAndRotation(t *testing.T) {
	dir := t.TempDir()
	if err := RotateKey(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("密钥文件不存在时 RotateKey 错误 = %v，期望 ErrNotExist", err)
	}
	s0, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, KeyFileName))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Errorf("cache.key 权限 %v，期望 0600", info.Mode().Perm())
	}
	again, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again.cur.kid != s0.cur.kid || again.prev != nil {
		t.Fatalf("重复加载生成了新密钥：%s → %s", s0.cur.kid, again.cur.kid)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("数据目录残留临时文件：%v", entries)
	}

	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	const key = "v1:search:brave:search/1:00"
	raw0, err := s0.Seal(key, policySampleValue(base))
	if err != nil {
		t.Fatal(err)
	}

	if err := RotateKey(dir); err != nil {
		t.Fatal(err)
	}
	s1, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s1.cur.kid == s0.cur.kid || s1.prev == nil || s1.prev.kid != s0.cur.kid {
		t.Fatalf("轮换后当前/上一 kid 不正确")
	}
	if _, err := s1.Open(key, raw0, base); err != nil {
		t.Errorf("一次轮换后上一 kid 应仍可验证：%v", err)
	}
	raw1, err := s1.Seal(key, policySampleValue(base))
	if err != nil {
		t.Fatal(err)
	}
	if v, err := s1.Open(key, raw1, base); err != nil || v.KID != s1.cur.kid {
		t.Errorf("新值应以当前 kid 签名：kid=%s err=%v", v.KID, err)
	}
	if _, err := s0.Open(key, raw1, base); !errors.Is(err, ErrEntryUnknownKID) {
		t.Errorf("旧 Signer 不认识新 kid：错误 = %v", err)
	}

	if err := RotateKey(dir); err != nil {
		t.Fatal(err)
	}
	s2, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Open(key, raw0, base); !errors.Is(err, ErrEntryUnknownKID) {
		t.Errorf("两次轮换后最初的 kid 应失效：错误 = %v", err)
	}
	if _, err := s2.Open(key, raw1, base); err != nil {
		t.Errorf("两次轮换后上一 kid 应可验证：%v", err)
	}

	// 损坏的密钥文件：报错，不重新生成（否则掩盖问题并使全部条目失效）。
	path := filepath.Join(dir, KeyFileName)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"截断":        string(good[:len(good)/2]),
		"kid 与密钥不符": strings.Replace(string(good), s2.cur.kid, strings.Repeat("0", 16), 1),
		"版本未知":      strings.Replace(string(good), `"version": 1`, `"version": 2`, 1),
		"未知字段":      strings.Replace(string(good), `"version": 1`, `"version": 1, "x": 1`, 1),
		"空文件":       "",
	} {
		if content == string(good) {
			t.Fatalf("%s：替换没有生效", name)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadKeys(dir); err == nil {
			t.Errorf("%s：LoadKeys 应失败", name)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != content {
			t.Errorf("%s：损坏的密钥文件被覆盖", name)
		}
	}
	if _, err := LoadKeys(filepath.Join(dir, "missing")); err == nil {
		t.Error("数据目录不存在时 LoadKeys 应失败")
	}
}

// ---- 接入层 Source（§11.2、§11.5；Plan 9 Task 3） ----

// memKV 是内存 KV：记录 Get 次数与每个键的 TTL。
type memKV struct {
	mu   sync.Mutex
	m    map[string][]byte
	ttl  map[string]time.Duration
	gets int
}

func newMemKV() *memKV { return &memKV{m: map[string][]byte{}, ttl: map[string]time.Duration{}} }

func (k *memKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.gets++
	v, ok := k.m[key]
	return v, ok, nil
}

func (k *memKV) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key], k.ttl[key] = append([]byte(nil), val...), ttl
	return nil
}

func (k *memKV) Del(_ context.Context, key string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, key)
	return nil
}

func (k *memKV) entry(key string) ([]byte, time.Duration, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[key]
	return v, k.ttl[key], ok
}

func (k *memKV) put(key string, val []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key] = val
}

// memBlobs 是内存 BlobReader。
type memBlobs struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (b *memBlobs) Open(sha string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.m[sha]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(string(v))), nil
}

func (b *memBlobs) add(body string) string {
	sum := sha256.Sum256([]byte(body))
	sha := hex.EncodeToString(sum[:])
	b.mu.Lock()
	defer b.mu.Unlock()
	b.m[sha] = []byte(body)
	return sha
}

func (b *memBlobs) set(sha, body string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if body == "" {
		delete(b.m, sha)
		return
	}
	b.m[sha] = []byte(body)
}

type sourceFixture struct {
	src   *Source
	kv    *memKV
	blobs *memBlobs
	clk   *fakeClock
	m     *Metrics
}

func newSourceFixture(t *testing.T) *sourceFixture {
	t.Helper()
	signer, err := LoadKeys(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &sourceFixture{kv: newMemKV(), blobs: &memBlobs{m: map[string][]byte{}},
		clk: &fakeClock{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}, m: &Metrics{}}
	f.src, err = NewSource(SourceConfig{KV: f.kv, Metrics: f.m, Signer: signer, Blobs: f.blobs, Now: f.clk.now})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// store 同步提交一次写入（等待异步写入结束）。
func (f *sourceFixture) store(kind upstream.Kind, provider string, params, rawURL string, r Response, sha string) {
	f.src.Store(kind, provider, "v/1", []byte(params), rawURL, r, sha)
	f.src.wg.Wait()
}

func (f *sourceFixture) lookup(kind upstream.Kind, provider, params, rawURL string) (string, int64, bool) {
	return f.src.Lookup(context.Background(), kind, provider, "v/1", []byte(params), rawURL)
}

func (f *sourceFixture) metrics() string {
	return fmt.Sprint(f.src.Metrics())
}

// 搜索：写入的寿命是 adapter TTL（6 h）；命中、未命中各计一次；完整性失败（blob 内容不符、blob 缺失、值被篡改、
// 过期）→ 未命中、删除条目、计 integrity_failure。
func TestSourceSearchHitAndIntegrity(t *testing.T) {
	f := newSourceFixture(t)
	const params = `{"query":"go","max_results":5}`
	sha := f.blobs.add(`{"results":[]}`)
	put := func() string {
		t.Helper()
		f.store(upstream.KindSearch, "fake", params, "", Response{Status: 200, Size: int64(len(`{"results":[]}`)),
			ResponseTime: f.clk.now()}, sha)
		key, err := Key(upstream.KindSearch, "fake", "v/1", []byte(params))
		if err != nil {
			t.Fatal(err)
		}
		if _, ttl, ok := f.kv.entry(key); !ok || ttl != DefaultSearchTTL {
			t.Fatalf("搜索条目应以 6 h 写入，得到 ok=%v ttl=%v", ok, ttl)
		}
		return key
	}
	key := put()
	if got, size, hit := f.lookup(upstream.KindSearch, "fake", params, ""); !hit || got != sha || size != int64(len(`{"results":[]}`)) {
		t.Fatalf("应命中 %s，得到 %s %d %v", sha, got, size, hit)
	}
	if _, _, hit := f.lookup(upstream.KindSearch, "fake", `{"query":"rust","max_results":5}`, ""); hit {
		t.Fatal("不同参数不应命中")
	}
	if f.m.Hit.Load() != 1 || f.m.Miss.Load() != 1 || f.m.IntegrityFailure.Load() != 0 {
		t.Fatalf("指标 %s", f.metrics())
	}

	for _, c := range []struct {
		name    string
		breakIt func()
		repair  func()
	}{
		{"blob 内容不符", func() { f.blobs.set(sha, `{"results":["x"]}`) }, func() { f.blobs.set(sha, `{"results":[]}`) }},
		{"blob 缺失", func() { f.blobs.set(sha, "") }, func() { f.blobs.set(sha, `{"results":[]}`) }},
		{"值被篡改", func() {
			raw, _, _ := f.kv.entry(key)
			f.kv.put(key, []byte(strings.Replace(string(raw), `"status":200`, `"status":203`, 1)))
		}, func() {}},
		{"已过期", func() { f.clk.advance(DefaultSearchTTL) }, func() {}},
	} {
		before := f.m.IntegrityFailure.Load()
		c.breakIt()
		if _, _, hit := f.lookup(upstream.KindSearch, "fake", params, ""); hit {
			t.Fatalf("%s：不应命中", c.name)
		}
		if _, _, ok := f.kv.entry(key); ok {
			t.Fatalf("%s：条目应被删除", c.name)
		}
		if f.m.IntegrityFailure.Load() != before+1 {
			t.Fatalf("%s：integrity_failure 应加 1，指标 %s", c.name, f.metrics())
		}
		c.repair()
		key = put()
	}
	if f.m.Hit.Load() != 1 || f.m.Miss.Load() != 1 {
		t.Fatalf("完整性失败不应再计 hit 或 miss：%s", f.metrics())
	}
}

// 抓取：寿命取响应的显式新鲜度；不可缓存（Set-Cookie、无显式新鲜度、非 200、正文超过 2 MiB）不写入；URL 按
// 规范化后的键命中；含凭据参数的 URL 查找时直接跳过（计 bypass，不访问 Redis）。
func TestSourceFetchAdmission(t *testing.T) {
	f := newSourceFixture(t)
	now := f.clk.now()
	date := now.Format("Mon, 02 Jan 2006 15:04:05 GMT")
	sha := f.blobs.add("<html>ok</html>")
	resp := func(h map[string][]string) Response {
		return Response{Status: 200, Header: h, Size: int64(len("<html>ok</html>")), RequestTime: now, ResponseTime: now}
	}
	fetch := func(u string) string { return `{"url":"` + u + `"}` }

	cases := []struct {
		name   string
		url    string
		r      Response
		stored bool
	}{
		{"max-age", "http://example.com/a", resp(map[string][]string{"Date": {date}, "Cache-Control": {"max-age=60"}}), true},
		{"Set-Cookie", "http://example.com/b", resp(map[string][]string{"Date": {date}, "Cache-Control": {"max-age=60"}, "Set-Cookie": {"s=1"}}), false},
		{"无显式新鲜度", "http://example.com/c", resp(map[string][]string{"Date": {date}}), false},
		{"非 200", "http://example.com/d", Response{Status: 404, Header: map[string][]string{"Date": {date}, "Cache-Control": {"max-age=60"}},
			Size: 10, RequestTime: now, ResponseTime: now}, false},
		{"超过 2 MiB", "http://example.com/e", Response{Status: 200, Header: map[string][]string{"Date": {date}, "Cache-Control": {"max-age=60"}},
			Size: MaxCacheableBytes + 1, RequestTime: now, ResponseTime: now}, false},
	}
	for _, c := range cases {
		f.store(upstream.KindFetch, "http_get", fetch(c.url), c.url, c.r, sha)
		key, err := Key(upstream.KindFetch, "http_get", "v/1", []byte(fetch(c.url)))
		if err != nil {
			t.Fatal(err)
		}
		_, ttl, ok := f.kv.entry(key)
		if ok != c.stored || (ok && ttl != time.Minute) {
			t.Fatalf("%s：写入=%v ttl=%v，期望写入=%v", c.name, ok, ttl, c.stored)
		}
	}
	if _, _, hit := f.lookup(upstream.KindFetch, "http_get", fetch("HTTP://Example.COM:80/a#frag"), "HTTP://Example.COM:80/a#frag"); !hit {
		t.Fatalf("规范化后相同的 URL 应命中：%s", f.metrics())
	}
	gets := f.kv.gets
	if _, _, hit := f.lookup(upstream.KindFetch, "http_get", fetch("http://example.com/a?token=x"), "http://example.com/a?token=x"); hit {
		t.Fatal("含凭据参数的 URL 不应命中")
	}
	f.src.Bypass()
	if f.kv.gets != gets || f.m.Bypass.Load() != 2 {
		t.Fatalf("凭据 URL 应跳过 Redis 并计 bypass：gets %d→%d，指标 %s", gets, f.kv.gets, f.metrics())
	}
	// 合并键（§11.4）：可缓存的抓取给出与 Key 相同的 cache_key（规范化后相同的 URL 同键）；凭据 URL 与模型调用
	// 不参与合并；CoalesceKey 不访问 Redis、不计数，Coalesced 每次计一。
	want, err := Key(upstream.KindFetch, "http_get", "v/1", []byte(fetch("http://example.com/a")))
	if err != nil {
		t.Fatal(err)
	}
	if k, ok := f.src.CoalesceKey(upstream.KindFetch, "http_get", "v/1", []byte(fetch("HTTP://Example.COM:80/a")), "HTTP://Example.COM:80/a"); !ok || k != want {
		t.Fatalf("CoalesceKey = %q, %v，期望 %q", k, ok, want)
	}
	if _, ok := f.src.CoalesceKey(upstream.KindFetch, "http_get", "v/1", []byte(fetch("http://example.com/a?token=x")), "http://example.com/a?token=x"); ok {
		t.Fatal("凭据 URL 不应参与合并")
	}
	if _, ok := f.src.CoalesceKey(upstream.KindChat, "p", "v/1", []byte(`{}`), ""); ok {
		t.Fatal("模型调用不应参与合并")
	}
	f.src.Coalesced()
	if f.kv.gets != gets || f.m.Bypass.Load() != 2 || f.src.Metrics()["coalesced"] != 1 {
		t.Fatalf("CoalesceKey 不应访问 Redis 或计数：gets %d→%d，指标 %s", gets, f.kv.gets, f.metrics())
	}
}

// Redis 不可达：查找全部未命中，只计 error（不再计 miss）；连续 5 次失败后熔断打开，之后计 breaker_open。
// 写入同样只计数、不报错。
func TestSourceRedisDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	signer, err := LoadKeys(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRedis(RedisConfig{Addr: dead})
	defer func() { _ = r.Close() }() // 测试结束，关闭错误无关紧要
	m := &Metrics{}
	src, err := NewSource(SourceConfig{KV: r, Metrics: m, Signer: signer, Blobs: &memBlobs{m: map[string][]byte{}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, _, hit := src.Lookup(context.Background(), upstream.KindSearch, "fake", "v/1", []byte(`{"query":"q"}`), ""); hit {
			t.Fatal("Redis 不可达时不应命中")
		}
	}
	if m.Error.Load() != 5 || m.BreakerOpen.Load() != 2 || m.Miss.Load() != 0 || m.Hit.Load() != 0 {
		t.Fatalf("指标 %v", src.Metrics())
	}
	src.Store(upstream.KindSearch, "fake", "v/1", []byte(`{"query":"q"}`), "", Response{Status: 200, Size: 1, ResponseTime: time.Now()},
		strings.Repeat("ab", 32))
	src.Close()
	if m.BreakerOpen.Load() != 3 {
		t.Fatalf("熔断打开时写入应计 breaker_open：%v", src.Metrics())
	}
}

// oversizedKV 像 Redis 客户端一样拒绝超过 4 KiB 的 GET 回复（ErrValueTooLarge），其余委托 memKV。
type oversizedKV struct{ *memKV }

func (k oversizedKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, ok, err := k.memKV.Get(ctx, key)
	if ok && len(v) > MaxValueSize {
		return nil, false, ErrValueTooLarge
	}
	return v, ok, err
}

// 超大条目（E22）：Redis 回复超过 4 KiB 的值是完整性失败——未命中、删除条目、计 integrity_failure；不计 error、
// 不计入熔断（反复出现也不会打开熔断）。
func TestSourceOversizedEntryIsIntegrityFailure(t *testing.T) {
	signer, err := LoadKeys(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kv := oversizedKV{newMemKV()}
	m := &Metrics{}
	b := NewBreaker(nil)
	src, err := NewSource(SourceConfig{KV: kv, Breaker: b, Metrics: m, Signer: signer, Blobs: &memBlobs{m: map[string][]byte{}}})
	if err != nil {
		t.Fatal(err)
	}
	params := []byte(`{"query":"big"}`)
	key, err := Key(upstream.KindSearch, "fake", "v/1", params)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*DefaultFailureThreshold; i++ {
		kv.put(key, []byte(strings.Repeat("x", MaxValueSize+1)))
		if _, _, hit := src.Lookup(context.Background(), upstream.KindSearch, "fake", "v/1", params, ""); hit {
			t.Fatal("超大条目不应命中")
		}
		if _, _, ok := kv.entry(key); ok {
			t.Fatal("超大条目应被删除")
		}
	}
	if got := src.Metrics(); got["integrity_failure"] != int64(2*DefaultFailureThreshold) || got["error"] != 0 || got["miss"] != 0 ||
		got["breaker_open"] != 0 || b.State() != StateClosed {
		t.Fatalf("指标 %v，熔断 %v", got, b.State())
	}
}

// 真实 Redis：写入后命中（经 Seal/Open 与 blob 复核）。
func TestSourceRealRedis(t *testing.T) {
	addr := redisAddr(t)
	signer, err := LoadKeys(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRedis(RedisConfig{Addr: addr})
	defer func() { _ = r.Close() }() // 测试结束，关闭错误无关紧要
	blobs := &memBlobs{m: map[string][]byte{}}
	sha := blobs.add(`{"results":["r"]}`)
	src, err := NewSource(SourceConfig{KV: r, Signer: signer, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	params := []byte(`{"query":"` + testKey(t) + `"}`)
	if _, _, hit := src.Lookup(context.Background(), upstream.KindSearch, "fake", "v/1", params, ""); hit {
		t.Fatal("新键不应命中")
	}
	src.Store(upstream.KindSearch, "fake", "v/1", params, "", Response{Status: 200, Size: int64(len(`{"results":["r"]}`)), ResponseTime: time.Now()}, sha)
	src.wg.Wait()
	got, _, hit := src.Lookup(context.Background(), upstream.KindSearch, "fake", "v/1", params, "")
	if !hit || got != sha {
		t.Fatalf("写入后应命中：%v %s，指标 %v", hit, got, src.Metrics())
	}
	key, err := Key(upstream.KindSearch, "fake", "v/1", params)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Del(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	src.Close()
	if m := src.Metrics(); m["hit"] != 1 || m["miss"] != 1 || m["error"] != 0 {
		t.Fatalf("指标 %v", m)
	}
}
