package cache

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
)

// MaxSealedBytes 是编码后缓存值的大小上限（§11.3、§19：4 KiB）。
const MaxSealedBytes = 4 << 10

// maxExactSize 是 Size 的上限：JCS 把数字按双精度输出，超过 2^53 的整数不能逐一区分。
const maxExactSize = 1 << 53

// macDomain 区分缓存值 MAC 与同一密钥可能的其他用途。
const macDomain = "agentbox-cache-value-v1\x00"

// Open 的失败类别。任一失败都应视为未命中、删除条目并记录 cache_integrity_failure（§11.5）。
var (
	ErrEntryTooLarge   = errors.New("cache: 缓存值超过 4 KiB")
	ErrEntryMalformed  = errors.New("cache: 缓存值格式错误")
	ErrEntryUnknownKID = errors.New("cache: 缓存值的 kid 既不是当前也不是上一密钥")
	ErrEntryBadMAC     = errors.New("cache: 缓存值的 HMAC 校验失败")
	ErrEntryExpired    = errors.New("cache: 缓存值已过 expires_at")
)

// Value 是缓存条目（正文在 BlobStore，以 BlobSHA256 引用）。KID 与 MAC 由 Seal 填写、由 Open 返回。
type Value struct {
	BlobSHA256  string
	Status      int
	ContentType string
	FinalURL    string
	Size        int64
	FetchedAt   time.Time
	ExpiresAt   time.Time
	KID         string
	MAC         []byte
}

// wireValue 是缓存值的 JSON 编码；时间为 UTC 的 RFC 3339（纳秒）字符串。
type wireValue struct {
	BlobSHA256  string `json:"blob_sha256"`
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	FinalURL    string `json:"final_url"`
	Size        int64  `json:"size"`
	FetchedAt   string `json:"fetched_at"`
	ExpiresAt   string `json:"expires_at"`
	KID         string `json:"kid"`
	MAC         string `json:"mac,omitempty"`
}

// macInput 是 MAC 覆盖的内容：缓存键与 MAC 之外的全部字段（含 expires_at 与 kid）。
type macInput struct {
	Key string `json:"key"`
	wireValue
}

// Seal 以当前密钥签名并编码缓存值：HMAC-SHA256 覆盖缓存键、元数据、expires_at 与 kid（以 RFC 8785
// 规范化后计算）。v.KID 与 v.MAC 被忽略并以当前密钥重新填写。编码后超过 4 KiB 返回 ErrEntryTooLarge。
func (s *Signer) Seal(key string, v Value) ([]byte, error) {
	if key == "" {
		return nil, errors.New("cache: 缓存键为空")
	}
	if !isLowerHex(v.BlobSHA256, sha256.Size) {
		return nil, errors.New("cache: blob_sha256 须为 64 位小写十六进制")
	}
	if v.Status < 100 || v.Status > 599 {
		return nil, fmt.Errorf("cache: 状态码 %d 无效", v.Status)
	}
	if v.Size < 0 || v.Size >= maxExactSize {
		return nil, fmt.Errorf("cache: 大小 %d 无效", v.Size)
	}
	if v.FetchedAt.IsZero() || v.ExpiresAt.IsZero() {
		return nil, errors.New("cache: fetched_at 与 expires_at 不能为零值")
	}
	w := wireValue{
		BlobSHA256:  v.BlobSHA256,
		Status:      v.Status,
		ContentType: v.ContentType,
		FinalURL:    v.FinalURL,
		Size:        v.Size,
		FetchedAt:   v.FetchedAt.UTC().Format(time.RFC3339Nano),
		ExpiresAt:   v.ExpiresAt.UTC().Format(time.RFC3339Nano),
		KID:         s.cur.kid,
	}
	mac, err := valueMAC(s.cur.secret, key, w)
	if err != nil {
		return nil, err
	}
	w.MAC = base64.StdEncoding.EncodeToString(mac)
	out, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("cache: 编码缓存值: %w", err)
	}
	if len(out) > MaxSealedBytes {
		return nil, ErrEntryTooLarge
	}
	return out, nil
}

// Open 校验并解码缓存值：先限长（> 4 KiB 不解析），严格解析（拒绝未知字段、重复属性名与尾随内容），
// kid 须为当前或上一密钥，MAC 须与缓存键及全部字段一致，且 now（Gateway 时钟）早于 expires_at。
func (s *Signer) Open(key string, raw []byte, now time.Time) (Value, error) {
	if len(raw) > MaxSealedBytes {
		return Value{}, ErrEntryTooLarge
	}
	if _, err := jcs.Canonical(json.RawMessage(raw)); err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrEntryMalformed, err)
	}
	var w wireValue
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrEntryMalformed, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Value{}, fmt.Errorf("%w: 值之后有多余内容", ErrEntryMalformed)
	}
	k, ok := s.lookup(w.KID)
	if !ok {
		return Value{}, ErrEntryUnknownKID
	}
	got, err := base64.StdEncoding.Strict().DecodeString(w.MAC)
	if err != nil || len(got) != sha256.Size {
		return Value{}, fmt.Errorf("%w: mac 须为 32 字节的 base64", ErrEntryMalformed)
	}
	mac := w.MAC
	w.MAC = ""
	want, err := valueMAC(k.secret, key, w)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrEntryMalformed, err)
	}
	if !hmac.Equal(got, want) {
		return Value{}, ErrEntryBadMAC
	}
	w.MAC = mac
	fetched, err1 := time.Parse(time.RFC3339Nano, w.FetchedAt)
	expires, err2 := time.Parse(time.RFC3339Nano, w.ExpiresAt)
	if err1 != nil || err2 != nil || w.Size < 0 || w.Size >= maxExactSize {
		return Value{}, fmt.Errorf("%w: 时间或大小无效", ErrEntryMalformed)
	}
	if !now.Before(expires) {
		return Value{}, ErrEntryExpired
	}
	return Value{
		BlobSHA256:  w.BlobSHA256,
		Status:      w.Status,
		ContentType: w.ContentType,
		FinalURL:    w.FinalURL,
		Size:        w.Size,
		FetchedAt:   fetched,
		ExpiresAt:   expires,
		KID:         w.KID,
		MAC:         got,
	}, nil
}

// valueMAC 计算 HMAC-SHA256(secret, macDomain ‖ JCS({key, 字段…}))；w.MAC 须为空。
func valueMAC(secret []byte, key string, w wireValue) ([]byte, error) {
	msg, err := jcs.Canonical(macInput{Key: key, wireValue: w})
	if err != nil {
		return nil, fmt.Errorf("cache: 规范化 MAC 输入: %w", err)
	}
	m := hmac.New(sha256.New, secret)
	_, _ = m.Write([]byte(macDomain)) // hash.Hash 的 Write 从不返回错误
	_, _ = m.Write(msg)               // 同上
	return m.Sum(nil), nil
}

func isLowerHex(s string, n int) bool {
	if len(s) != 2*n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
