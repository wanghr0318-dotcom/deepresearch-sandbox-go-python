// Package apikey implements platform API keys (design 2026-10-11 §5): the key format abx_live_<id>_<secret>, the
// HMAC-SHA256 of the secret under a server pepper, verification with a constant-time comparison, scopes, expiry
// and revocation, a 30 s in-memory cache invalidated by PostgreSQL NOTIFY apikey_changed, and last_used_at
// updates at most once a minute. Key secrets are never stored or logged; only their HMAC is.
package apikey

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Prefix starts every platform key.
const Prefix = "abx_live_"

const (
	idRandBytes     = 10 // 16 base32 characters
	secretBytes     = 32 // 52 base32 characters
	idLen           = 2 + 16
	secretLen       = 52
	pepperBytes     = 32
	PepperFile      = "apikey.pepper"
	maxKeyNameRunes = 64
)

// enc is lower-case RFC 4648 base32 without padding (a–z, 2–7): no '_' so the parts split unambiguously.
var enc = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// ErrInvalidKey is returned for anything that is not a currently valid key: malformed, unknown, wrong secret,
// revoked, expired, or belonging to a disabled user or org. Callers answer 401 invalid_api_key.
var ErrInvalidKey = errors.New("apikey: invalid api key")

// LooksLikeKey reports whether s claims to be a platform key (has the abx_ prefix); such values are never checked
// as the operator token.
func LooksLikeKey(s string) bool { return strings.HasPrefix(s, "abx_") }

// Generated is a freshly minted key. Full is shown to the user once and never stored.
type Generated struct {
	ID         string
	Full       string
	SecretHMAC []byte
}

// Generate mints a key and its HMAC under pepper.
func Generate(pepper []byte) (Generated, error) {
	var idb [idRandBytes]byte
	var sec [secretBytes]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return Generated{}, err
	}
	if _, err := rand.Read(sec[:]); err != nil {
		return Generated{}, err
	}
	id := "k_" + enc.EncodeToString(idb[:])
	return Generated{ID: id, Full: Prefix + id + "_" + enc.EncodeToString(sec[:]), SecretHMAC: HMAC(pepper, sec[:])}, nil
}

// Parse splits a full key into its ID and decoded secret.
func Parse(s string) (id string, secret []byte, err error) {
	rest, ok := strings.CutPrefix(s, Prefix)
	if !ok || len(rest) != idLen+1+secretLen || rest[idLen] != '_' {
		return "", nil, ErrInvalidKey
	}
	id, sec := rest[:idLen], rest[idLen+1:]
	if !ValidID(id) {
		return "", nil, ErrInvalidKey
	}
	secret, err = enc.DecodeString(sec)
	if err != nil || len(secret) != secretBytes || enc.EncodeToString(secret) != sec {
		return "", nil, ErrInvalidKey
	}
	return id, secret, nil
}

// ValidID reports whether id has the form k_ + 16 base32 characters.
func ValidID(id string) bool {
	body, ok := strings.CutPrefix(id, "k_")
	if !ok || len(body) != idLen-2 {
		return false
	}
	b, err := enc.DecodeString(body)
	return err == nil && len(b) == idRandBytes && enc.EncodeToString(b) == body
}

// HMAC returns HMAC-SHA256(pepper, secret).
func HMAC(pepper, secret []byte) []byte {
	m := hmac.New(sha256.New, pepper)
	m.Write(secret)
	return m.Sum(nil)
}

// CheckSecret compares the secret's HMAC with the stored one in constant time.
func CheckSecret(pepper, secret, stored []byte) bool {
	return hmac.Equal(HMAC(pepper, secret), stored)
}

// ValidName checks a key's display name: 1–64 characters, no control characters.
func ValidName(name string) bool {
	n := 0
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return false
		}
		n++
	}
	return n >= 1 && n <= maxKeyNameRunes
}

// LoadOrCreatePepper reads <dataDir>/apikey.pepper, creating it (0600, 32 random bytes in hex) when it does not
// exist. Every server sharing a database must use the same pepper file.
func LoadOrCreatePepper(dataDir string) ([]byte, error) {
	path := filepath.Join(dataDir, PepperFile)
	p, err := LoadPepper(dataDir)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return p, err
	}
	var b [pepperBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) { // created concurrently: use that one
		return LoadPepper(dataDir)
	}
	if err != nil {
		return nil, fmt.Errorf("apikey: create %s: %w", PepperFile, err)
	}
	_, werr := f.WriteString(hex.EncodeToString(b[:]) + "\n")
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("apikey: write %s: %w", PepperFile, err)
	}
	return b[:], nil
}

// LoadPepper reads an existing <dataDir>/apikey.pepper; it must be 0600 (not checked on Windows).
func LoadPepper(dataDir string) ([]byte, error) {
	path := filepath.Join(dataDir, PepperFile)
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("apikey: read %s: %w", PepperFile, err)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("apikey: %s has mode %v, want 0600", PepperFile, fi.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("apikey: read %s: %w", PepperFile, err)
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(b) != pepperBytes {
		return nil, fmt.Errorf("apikey: %s must hold %d bytes in hex", PepperFile, pepperBytes)
	}
	return b, nil
}
