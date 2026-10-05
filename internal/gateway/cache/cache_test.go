package cache

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
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
