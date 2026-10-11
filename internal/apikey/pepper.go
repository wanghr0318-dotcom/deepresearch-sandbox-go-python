package apikey

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// ErrPepper means the pepper file and the database disagree: the server must not start (exit 2), because a new
// pepper would silently invalidate every issued key.
var ErrPepper = errors.New("apikey: pepper check failed")

const fingerprintLabel = "agentbox-apikey-pepper-check"

// Fingerprint identifies a pepper without revealing it: HMAC-SHA256(pepper, fixed label).
func Fingerprint(pepper []byte) []byte { return HMAC(pepper, []byte(fingerprintLabel)) }

// PepperStore records the pepper fingerprint in the database.
type PepperStore interface {
	// PepperState returns the recorded fingerprint (nil when none) and whether any API key exists.
	PepperState(ctx context.Context) (fingerprint []byte, keysExist bool, err error)
	// RecordPepper records fp unless a fingerprint is already recorded, and returns the recorded one.
	RecordPepper(ctx context.Context, fp []byte) ([]byte, error)
}

// EnsurePepper loads <dataDir>/apikey.pepper and checks it against the fingerprint recorded in the database.
//   - File present: its fingerprint is recorded if none is, and must match the recorded one.
//   - File missing: created (atomically, 0600) only when the database has neither a fingerprint nor any key.
//
// Every mismatch is ErrPepper with an explanation; storage failures are returned as they are.
func EnsurePepper(ctx context.Context, dataDir string, st PepperStore) ([]byte, error) {
	removeStaleTemps(dataDir)
	recorded, keys, err := st.PepperState(ctx)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, PepperFile)
	p, err := LoadPepper(dataDir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if recorded != nil || keys {
			return nil, fmt.Errorf("%w: %s is missing but the database already has API keys (or a recorded pepper); restore the file "+
				"from where it is kept, or copy it from the other servers sharing this database (a new pepper would invalidate every issued key)",
				ErrPepper, path)
		}
		if p, err = createPepper(dataDir); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, fmt.Errorf("%w: %w", ErrPepper, err)
	}
	fp := Fingerprint(p)
	stored, err := st.RecordPepper(ctx, fp)
	if err != nil {
		return nil, err
	}
	if !hmac.Equal(stored, fp) {
		return nil, fmt.Errorf("%w: %s does not match the pepper recorded in the database (another server's or an older file); "+
			"use the same file on every server sharing this database", ErrPepper, path)
	}
	return p, nil
}

// LoadOrCreatePepper reads <dataDir>/apikey.pepper, creating it when missing, without any database check (tests
// and tools; the server uses EnsurePepper).
func LoadOrCreatePepper(dataDir string) ([]byte, error) {
	p, err := LoadPepper(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return createPepper(dataDir)
	}
	return p, err
}

// createPepper writes a new pepper atomically: a 0600 temp file is written and fsynced, then hard-linked into
// place (never replacing an existing file), so a crash never leaves an empty or partial pepper and a concurrent
// starter either wins or reads the winner's complete file.
func createPepper(dataDir string) ([]byte, error) {
	path := filepath.Join(dataDir, PepperFile)
	var b [pepperBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dataDir, PepperFile+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("apikey: create %s: %w", PepperFile, err)
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }() // the linked name keeps the content
	werr := f.Chmod(0o600)
	if runtime.GOOS == "windows" {
		werr = nil // permission bits are not enforced there (LoadPepper does not check them either)
	}
	_, err = f.WriteString(hex.EncodeToString(b[:]) + "\n")
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, err, serr, cerr); err != nil {
		return nil, fmt.Errorf("apikey: write %s: %w", PepperFile, err)
	}
	if err := os.Link(tmp, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return LoadPepper(dataDir) // another starter won
		}
		return nil, fmt.Errorf("apikey: install %s: %w", PepperFile, err)
	}
	syncDir(dataDir)
	return b[:], nil
}

// staleTempAge keeps a concurrent starter's temp file (being written right now) from being removed under it.
const staleTempAge = time.Minute

// removeStaleTemps removes apikey.pepper.tmp-* files left by a crash during createPepper (best effort).
func removeStaleTemps(dataDir string) {
	matches, _ := filepath.Glob(filepath.Join(dataDir, PepperFile+".tmp-*"))
	for _, m := range matches {
		if fi, err := os.Lstat(m); err == nil && fi.Mode().IsRegular() && time.Since(fi.ModTime()) > staleTempAge {
			_ = os.Remove(m)
		}
	}
}

// syncDir makes the new directory entry durable (best effort; not supported on Windows).
func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
