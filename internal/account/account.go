// Package account 是用户账号的纯逻辑部分：密码哈希（PBKDF2-HMAC-SHA256）、用户名与密码规则、
// 会话 ID 的生成与哈希、按 IP 的令牌桶限速。不做任何 I/O，不依赖 persistence、net/http 与 api。
package account

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrUsername = errors.New("account: 用户名须为 3–32 位字母、数字、下划线、点或连字符")
	ErrPassword = errors.New("account: 密码须为 8–16 位，且至少包含数字、大写字母、小写字母中的两种")
)

// Iterations 是新哈希使用的 PBKDF2 迭代次数（OWASP 当前建议）。
const Iterations = 600_000

const (
	scheme        = "pbkdf2-sha256"
	saltLen       = 16
	keyLen        = 32
	maxIterations = 10_000_000
	sessionIDLen  = 32
)

// NormalizeUsername 去首尾空白并校验；返回展示用原样与唯一键（小写）。
func NormalizeUsername(s string) (display, key string, err error) {
	display = strings.TrimSpace(s)
	if len(display) < 3 || len(display) > 32 {
		return "", "", ErrUsername
	}
	for i := 0; i < len(display); i++ {
		c := display[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-'
		if !ok {
			return "", "", ErrUsername
		}
	}
	return display, strings.ToLower(display), nil
}

// ValidatePassword 校验注册密码：8–16 个字符（按 rune），且数字、大写字母、小写字母三类中至少有两类。
// 只用于注册；登录只比较哈希，规则变化不影响已有账号。
func ValidatePassword(pw string) error {
	if n := utf8.RuneCountInString(pw); n < 8 || n > 16 {
		return ErrPassword
	}
	var digit, upper, lower bool
	for _, r := range pw {
		switch {
		case r >= '0' && r <= '9':
			digit = true
		case r >= 'A' && r <= 'Z':
			upper = true
		case r >= 'a' && r <= 'z':
			lower = true
		}
	}
	classes := 0
	for _, b := range []bool{digit, upper, lower} {
		if b {
			classes++
		}
	}
	if classes < 2 {
		return ErrPassword
	}
	return nil
}

// HashPassword 返回 "pbkdf2-sha256$600000$<salt>$<hash>"（base64url，无填充）。
func HashPassword(pw string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	return encode(pw, salt), nil
}

func encode(pw string, salt []byte) string {
	key := pbkdf2SHA256(pw, salt, Iterations, keyLen)
	enc := base64.RawURLEncoding
	return fmt.Sprintf("%s$%d$%s$%s", scheme, Iterations, enc.EncodeToString(salt), enc.EncodeToString(key))
}

// VerifyPassword 解析存储串并常量时间比较；格式错误返回 false。
func VerifyPassword(stored, pw string) bool {
	parts := strings.Split(stored, "$")
	if len(parts) != 4 || parts[0] != scheme {
		return false
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter < 1 || iter > maxIterations {
		return false
	}
	enc := base64.RawURLEncoding
	salt, err1 := enc.DecodeString(parts[2])
	want, err2 := enc.DecodeString(parts[3])
	if err1 != nil || err2 != nil || len(want) == 0 {
		return false
	}
	got := pbkdf2SHA256(pw, salt, iter, len(want))
	return subtle.ConstantTimeCompare(got, want) == 1
}

// pbkdf2SHA256 是 PBKDF2-HMAC-SHA256（RFC 8018），使用标准库 crypto/pbkdf2，不自行实现密码学原语。
// 参数在此之前已校验（iter 在 1..maxIterations，keyLen 为正），标准库只会在参数越界时返回错误。
func pbkdf2SHA256(pw string, salt []byte, iter, keyLen int) []byte {
	key, err := pbkdf2.Key(sha256.New, pw, salt, iter, keyLen)
	if err != nil {
		panic(fmt.Sprintf("account: pbkdf2 参数无效：%v", err))
	}
	return key
}

// dummyHash 是 DummyVerify 使用的固定哈希，首次使用时生成一次（固定盐，与真实哈希同参数）。
var dummyHash = sync.OnceValue(func() string {
	return encode("agentbox-dummy-password", make([]byte, saltLen))
})

// DummyVerify 对固定的哈希做一次完整校验，用于用户不存在时消除时序差异。
func DummyVerify(pw string) {
	// 结果无意义：只为付出与真实校验相同的计算代价。
	_ = VerifyPassword(dummyHash(), pw)
}

// NewSessionID 返回 cookie 中的明文 ID（32 字节随机数的 base64url）与库中保存的 sha256。
func NewSessionID() (id string, idHash []byte, err error) {
	b := make([]byte, sessionIDLen)
	if _, err := rand.Read(b); err != nil {
		return "", nil, err
	}
	id = base64.RawURLEncoding.EncodeToString(b)
	return id, HashSessionID(id), nil
}

// HashSessionID 计算明文 ID 的 sha256（解析 cookie 时用）。
func HashSessionID(id string) []byte {
	sum := sha256.Sum256([]byte(id))
	return sum[:]
}

// maxKeys 是 Limiter 触发清理的键数上限。
const maxKeys = 10_000

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter 是按键（IP）的令牌桶；now 注入以便测试。容量为 perMinute，每秒补充 perMinute/60 个令牌。
type Limiter struct {
	burst   float64 // 容量，也是每分钟补充的令牌数
	now     func() time.Time
	mu      sync.Mutex
	buckets map[string]*bucket
}

// NewLimiter 返回每键每分钟 perMinute 次的限速器；now 为 nil 时用 time.Now。
func NewLimiter(perMinute int, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{
		burst:   float64(perMinute),
		now:     now,
		buckets: make(map[string]*bucket),
	}
}

// Allow 消耗 key 的一个令牌；没有令牌时返回 false。
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= maxKeys {
			l.evict(now)
		}
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	} else if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens = min(l.burst, b.tokens+el*l.burst/60)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// evict 删除 1 分钟内未活动的键（这些桶已补满，删除与保留等价）。调用方持有 l.mu。
func (l *Limiter) evict(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.last) >= time.Minute {
			delete(l.buckets, k)
		}
	}
}
