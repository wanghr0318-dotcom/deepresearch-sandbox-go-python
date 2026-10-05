package cache

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// KeyFileName 是数据目录下的缓存签名密钥文件（§11.5：`<data>/cache.key`，0600）。
const KeyFileName = "cache.key"

const (
	keySecretBytes  = 32
	keyFileMaxBytes = 4 << 10
	keyFileVersion  = 1
)

// signingKey 是一把 HMAC 密钥；kid 由密钥派生（见 deriveKID），因此不会与密钥不一致。
type signingKey struct {
	kid    string
	secret []byte
}

// Signer 持有当前与上一缓存签名密钥：Seal 只用当前密钥，Open 接受二者（§11.5）。
type Signer struct {
	cur  signingKey
	prev *signingKey
}

func (s *Signer) lookup(kid string) (signingKey, bool) {
	switch {
	case kid == "":
		return signingKey{}, false
	case kid == s.cur.kid:
		return s.cur, true
	case s.prev != nil && kid == s.prev.kid:
		return *s.prev, true
	}
	return signingKey{}, false
}

// keyFile 是 cache.key 的 JSON 编码；secret 为小写十六进制。
type keyFile struct {
	Version  int           `json:"version"`
	Current  keyFileEntry  `json:"current"`
	Previous *keyFileEntry `json:"previous,omitempty"`
}

type keyFileEntry struct {
	KID    string `json:"kid"`
	Secret string `json:"secret"`
}

// LoadKeys 读取 `<data>/cache.key`；文件不存在时生成新的当前密钥并以 0600 持久化写入（安装时生成）。
// 并发的首次创建只有一个生效（以硬链接独占创建），其余读取已生效的文件。文件损坏返回错误，不重新生成
// （否则一次读错误会使全部已签名条目失效且掩盖问题）。
func LoadKeys(dataDir string) (*Signer, error) {
	s, err := readKeyFile(dataDir)
	if err == nil {
		return s, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	cur, err := newSigningKey()
	if err != nil {
		return nil, err
	}
	if err := writeKeyFile(dataDir, &Signer{cur: cur}, true); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	return readKeyFile(dataDir)
}

// RotateKey 轮换缓存签名密钥：当前密钥成为上一密钥，生成新的当前密钥，原有的上一密钥被丢弃
// （以它签名的条目此后验证失败、视为未命中）。密钥文件须已存在（避免误指数据目录时静默新建）。
// 服务在启动时加载密钥，轮换在服务停止时进行（调用方持有数据目录锁），重启后生效。
func RotateKey(dataDir string) error {
	old, err := readKeyFile(dataDir)
	if err != nil {
		return err
	}
	cur, err := newSigningKey()
	if err != nil {
		return err
	}
	prev := old.cur
	return writeKeyFile(dataDir, &Signer{cur: cur, prev: &prev}, false)
}

func newSigningKey() (signingKey, error) {
	secret := make([]byte, keySecretBytes)
	if _, err := rand.Read(secret); err != nil {
		return signingKey{}, fmt.Errorf("cache: 生成密钥: %w", err)
	}
	return signingKey{kid: deriveKID(secret), secret: secret}, nil
}

// deriveKID 返回密钥的标识：SHA-256("agentbox-cache-kid\0" ‖ secret) 的前 8 字节（小写十六进制）。
func deriveKID(secret []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte("agentbox-cache-kid\x00")) // hash.Hash 的 Write 从不返回错误
	_, _ = h.Write(secret)                           // 同上
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// readKeyFile 读取并严格校验密钥文件；不存在时返回包装 fs.ErrNotExist 的错误。
func readKeyFile(dataDir string) (*Signer, error) {
	path := filepath.Join(dataDir, KeyFileName)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cache: 打开 %s: %w", path, err)
	}
	defer func() { _ = f.Close() }() // 只读文件，关闭错误无影响
	raw, err := io.ReadAll(io.LimitReader(f, keyFileMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("cache: 读取 %s: %w", path, err)
	}
	if len(raw) > keyFileMaxBytes {
		return nil, fmt.Errorf("cache: %s 超过 %d 字节", path, keyFileMaxBytes)
	}
	var kf keyFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&kf); err != nil {
		return nil, fmt.Errorf("cache: %s 损坏: %w", path, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("cache: %s 损坏：值之后有多余内容", path)
	}
	if kf.Version != keyFileVersion {
		return nil, fmt.Errorf("cache: %s 版本 %d 不受支持", path, kf.Version)
	}
	cur, err := kf.Current.decode()
	if err != nil {
		return nil, fmt.Errorf("cache: %s 的当前密钥: %w", path, err)
	}
	s := &Signer{cur: cur}
	if kf.Previous != nil {
		prev, err := kf.Previous.decode()
		if err != nil {
			return nil, fmt.Errorf("cache: %s 的上一密钥: %w", path, err)
		}
		if prev.kid == cur.kid {
			return nil, fmt.Errorf("cache: %s 的当前与上一密钥相同", path)
		}
		s.prev = &prev
	}
	return s, nil
}

func (e keyFileEntry) decode() (signingKey, error) {
	if !isLowerHex(e.Secret, keySecretBytes) {
		return signingKey{}, fmt.Errorf("secret 须为 %d 字节的小写十六进制", keySecretBytes)
	}
	secret, err := hex.DecodeString(e.Secret)
	if err != nil {
		return signingKey{}, err
	}
	if kid := deriveKID(secret); e.KID != kid {
		return signingKey{}, fmt.Errorf("kid %q 与密钥不符", e.KID)
	}
	return signingKey{kid: e.KID, secret: secret}, nil
}

func encodeEntry(k signingKey) keyFileEntry {
	return keyFileEntry{KID: k.kid, Secret: hex.EncodeToString(k.secret)}
}

// writeKeyFile 持久化写入密钥文件（0600）：临时文件 → 写入并 fsync →（exclusive 时）硬链接到目标名，
// 目标已存在返回包装 fs.ErrExist 的错误；否则 rename 原子替换 → fsync 目录。
func writeKeyFile(dataDir string, s *Signer, exclusive bool) error {
	kf := keyFile{Version: keyFileVersion, Current: encodeEntry(s.cur)}
	if s.prev != nil {
		p := encodeEntry(*s.prev)
		kf.Previous = &p
	}
	content, err := json.MarshalIndent(kf, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: 编码密钥文件: %w", err)
	}
	content = append(content, '\n')

	tmp, err := os.CreateTemp(dataDir, KeyFileName+".tmp-*") // CreateTemp 以 0600 创建
	if err != nil {
		return fmt.Errorf("cache: 创建临时密钥文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // rename 成功后此处无文件可删；硬链接后删除的只是临时名
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close() // 已有写入错误
		return fmt.Errorf("cache: 写入临时密钥文件: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close() // 已有 fsync 错误
		return fmt.Errorf("cache: fsync 临时密钥文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cache: 关闭临时密钥文件: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return fmt.Errorf("cache: 设置密钥文件权限: %w", err)
	}
	final := filepath.Join(dataDir, KeyFileName)
	if exclusive {
		if err := os.Link(tmpPath, final); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("cache: %s 已存在: %w", final, fs.ErrExist)
			}
			return fmt.Errorf("cache: 创建 %s: %w", final, err)
		}
	} else if err := os.Rename(tmpPath, final); err != nil {
		return fmt.Errorf("cache: 替换 %s: %w", final, err)
	}
	return syncKeyDir(dataDir)
}

// syncKeyDir fsync 目录，使新目录项在断电后仍在；Windows 不支持对目录 fsync（只为开发机编译）。
func syncKeyDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cache: 打开目录: %w", err)
	}
	defer func() { _ = d.Close() }() // 只读目录句柄
	if err := d.Sync(); err != nil {
		return fmt.Errorf("cache: fsync 目录: %w", err)
	}
	return nil
}
