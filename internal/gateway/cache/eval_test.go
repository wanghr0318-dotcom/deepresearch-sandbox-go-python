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
