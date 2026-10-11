package cache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestRedisEval：EVAL 返回批量字符串与整数；EVALSHA 对未缓存脚本是 NOSCRIPT 错误回复（连接仍可用），EVAL 之后命中。
func TestRedisEval(t *testing.T) {
	r := NewRedis(RedisConfig{Addr: redisAddr(t), ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = r.Close() })
	ctx := context.Background()
	key := testKey(t)

	script := "redis.call('SET', KEYS[1], ARGV[1], 'PX', 60000) return redis.call('GET', KEYS[1]) .. ':' .. ARGV[2] -- " + key
	sum := sha1.Sum([]byte(script))
	sha := hex.EncodeToString(sum[:])
	_, err := r.EvalSHA(ctx, sha, []string{key}, "a", "b")
	var re *RedisError
	if !errors.As(err, &re) || !strings.HasPrefix(re.Msg, "NOSCRIPT") {
		t.Fatalf("未缓存脚本应为 NOSCRIPT，得到 %v", err)
	}
	got, err := r.Eval(ctx, script, []string{key}, "a", "b")
	if err != nil || string(got) != "a:b" {
		t.Fatalf("Eval = %q, %v", got, err)
	}
	got, err = r.EvalSHA(ctx, sha, []string{key}, "c", "d")
	if err != nil || string(got) != "c:d" {
		t.Fatalf("EvalSHA = %q, %v", got, err)
	}
	got, err = r.Eval(ctx, "return 42", nil)
	if err != nil || string(got) != "42" {
		t.Fatalf("整数回复 = %q, %v", got, err)
	}
	if _, err := r.Eval(ctx, "return {1,2}", nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("数组回复应为协议错误，得到 %v", err)
	}
	if _, err := r.Eval(ctx, "return nil", nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("空回复应为协议错误，得到 %v", err)
	}
	if err := r.Del(ctx, key); err != nil {
		t.Fatal(err)
	}
}

// TestPoolWaitIsPoolBusy（复审 F9）：取不到连接许可的超时是 ErrPoolBusy（同时仍是期限错误）；线路上的
// 超时不是。
func TestPoolWaitIsPoolBusy(t *testing.T) {
	r := NewRedis(RedisConfig{Addr: silentServer(t), ReadTimeout: 200 * time.Millisecond, PoolSize: 1})
	t.Cleanup(func() { _ = r.Close() })
	first := make(chan error, 1)
	go func() { _, _, err := r.Get(context.Background(), "k"); first <- err }()
	time.Sleep(50 * time.Millisecond) // 第一个操作占住唯一的许可，阻塞在不回复的服务上
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := r.Get(ctx, "k")
	if !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("等待许可超时应为 ErrPoolBusy，得到 %v", err)
	}
	if err := <-first; err == nil || errors.Is(err, ErrPoolBusy) {
		t.Fatalf("线路上的超时不是 ErrPoolBusy：%v", err)
	}
}
