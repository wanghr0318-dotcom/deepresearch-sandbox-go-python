package account

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPasswordHashRoundTrip(t *testing.T) {
	h, err := HashPassword("correct horse")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(h, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2-sha256" || parts[1] != "600000" {
		t.Fatalf("格式 = %q", h)
	}
	if !VerifyPassword(h, "correct horse") || VerifyPassword(h, "correct horsE") {
		t.Fatal("校验结果错误")
	}
	h2, err := HashPassword("correct horse")
	if err != nil {
		t.Fatal(err)
	}
	if h == h2 {
		t.Fatal("同一密码两次哈希相同：盐未随机")
	}
	for _, bad := range []string{"", "pbkdf2-sha256$x$y$z", "md5$1$a$b", h + "$extra"} {
		if VerifyPassword(bad, "correct horse") {
			t.Fatalf("畸形存储串 %q 被接受", bad)
		}
	}
}

// TestPBKDF2Vectors：自实现的 PBKDF2-HMAC-SHA256 与参考值一致（RFC 7914 §11 向量与 Python hashlib 计算值），
// 覆盖单块、多块与截断输出。
func TestPBKDF2Vectors(t *testing.T) {
	for _, c := range []struct {
		pw, salt     string
		iter, keyLen int
		want         string
	}{
		{"passwd", "salt", 1, 64, "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc49ca9cccf179b645991664b39d77ef317c71b845b1e30bd509112041d3a19783"},
		{"password", "salt", 4096, 40, "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134af7ad98c1b458ce3f"},
	} {
		if got := hex.EncodeToString(pbkdf2SHA256(c.pw, []byte(c.salt), c.iter, c.keyLen)); got != c.want {
			t.Errorf("pbkdf2(%q, %q, %d, %d) = %s, want %s", c.pw, c.salt, c.iter, c.keyLen, got, c.want)
		}
	}
}

func TestUsernameAndPasswordRules(t *testing.T) {
	d, k, err := NormalizeUsername("  Alice_01 ")
	if err != nil || d != "Alice_01" || k != "alice_01" {
		t.Fatalf("NormalizeUsername = %q %q %v", d, k, err)
	}
	for _, bad := range []string{"ab", strings.Repeat("a", 33), "has space", "中文名", "a/b"} {
		if _, _, err := NormalizeUsername(bad); !errors.Is(err, ErrUsername) {
			t.Fatalf("%q 应被拒绝", bad)
		}
	}
}

func TestValidatePassword(t *testing.T) {
	ok := []string{"abcdefG1", "ABCDEFG1", "abcdefgH", "Abcdefghijklmnop", "12345678a", "密码abcD1234"}
	bad := []string{
		"abcdeG1",           // 7 位
		"abcdefghijklmnoP1", // 17 位
		"abcdefgh",          // 只有小写
		"ABCDEFGH",          // 只有大写
		"12345678",          // 只有数字
		"密码密码密码密码",          // 无任何一类
		"!!!!!!!!a",         // 只有一类
	}
	for _, pw := range ok {
		if err := ValidatePassword(pw); err != nil {
			t.Errorf("%q 应当通过：%v", pw, err)
		}
	}
	for _, pw := range bad {
		if ValidatePassword(pw) == nil {
			t.Errorf("%q 应当被拒绝", pw)
		}
	}
}

func TestSessionID(t *testing.T) {
	id, h, err := NewSessionID()
	if err != nil || len(id) != 43 || !bytes.Equal(h, HashSessionID(id)) {
		t.Fatalf("NewSessionID = %q %x %v", id, h, err)
	}
	id2, _, err := NewSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if id == id2 {
		t.Fatal("两个会话 ID 相同")
	}
}

func TestLimiter(t *testing.T) {
	now := time.Unix(0, 0)
	l := NewLimiter(5, func() time.Time { return now })
	for i := 0; i < 5; i++ {
		if !l.Allow("1.2.3.4") {
			t.Fatalf("第 %d 次应放行", i+1)
		}
	}
	if l.Allow("1.2.3.4") {
		t.Fatal("第 6 次应拒绝")
	}
	if !l.Allow("5.6.7.8") {
		t.Fatal("不同 IP 互不影响")
	}
	now = now.Add(12 * time.Second) // 每 12 s 补 1 个令牌
	if !l.Allow("1.2.3.4") || l.Allow("1.2.3.4") {
		t.Fatal("补充速率错误")
	}
}
