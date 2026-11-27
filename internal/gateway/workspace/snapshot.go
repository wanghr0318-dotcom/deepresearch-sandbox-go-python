package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
)

// SnapshotArchive reads and writes immutable objects by SHA-256. Missing Get returns blob.ErrNotFound.
type SnapshotArchive interface {
	Get(context.Context, string) (io.ReadCloser, error)
	Put(context.Context, string, io.Reader, int64) error
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *state) snapshotObjects() map[string]int64 {
	objects := make(map[string]int64)
	for _, op := range s.Applied {
		// Length is not stored on the replay records; their digest covers the complete body.
		objects[op.ResponseSHA256] = -1
		objects[op.ResultSHA256] = -1
	}
	for _, file := range s.Files {
		objects[file.SHA256] = file.Size
	}
	return objects
}

// verifyRemote checks the stored bytes themselves; multipart ETags are not content hashes.
func verifyRemote(ctx context.Context, a SnapshotArchive, sha string, size int64) error {
	r, err := a.Get(ctx, sha)
	if err != nil {
		return err
	}
	defer r.Close()
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != sha || (size >= 0 && n != size) {
		return fmt.Errorf("workspace: archive content mismatch for %s", sha)
	}
	return nil
}

// saveObject deduplicates by digest. Check local bytes before assigning a key, then verify the actual upload.
func (m *Manager) saveObject(ctx context.Context, sha string, size int64, open func() (io.ReadCloser, error)) error {
	a := m.cfg.Archive
	if err := verifyRemote(ctx, a, sha, size); !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	r, err := open()
	if err != nil {
		return err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if digestBytes(data) != sha || (size >= 0 && int64(len(data)) != size) {
		return fmt.Errorf("workspace: local content mismatch for %s", sha)
	}
	n := int64(len(data))
	if err := a.Put(ctx, sha, bytes.NewReader(data), n); err != nil {
		return err
	}
	return verifyRemote(ctx, a, sha, n)
}

// Snapshot captures one version under the task lock. Its immutable blobs remain valid after release.
// Only the caller's committed database checkpoint may make the returned digest a recovery target.
func (m *Manager) Snapshot(ctx context.Context, taskID string) (string, error) {
	if m.cfg.Archive == nil {
		return "", errors.New("workspace: snapshot archive not configured")
	}
	w := m.get(taskID)
	w.mu.Lock()
	if w.gone {
		w.mu.Unlock()
		return "", errors.New("workspace: cannot snapshot destroyed workspace")
	}
	st, err := m.load(taskID, w)
	if err != nil {
		w.mu.Unlock()
		return "", err
	}
	snapshot := st.clone()
	w.mu.Unlock()
	if snapshot.State != StateActive {
		return "", fmt.Errorf("workspace: cannot snapshot state %q", snapshot.State)
	}
	for sha, size := range snapshot.snapshotObjects() {
		if err := m.saveObject(ctx, sha, size, func() (io.ReadCloser, error) { return m.cfg.Blobs.Open(sha) }); err != nil {
			return "", err
		}
	}
	// Reads update UsedAt solely for local expiry. Exclude it from the portable identity.
	snapshot.UsedAt = time.Time{}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	sha := digestBytes(data)
	err = m.saveObject(ctx, sha, int64(len(data)), func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	if err != nil {
		return "", err
	}
	return sha, nil
}

// Restore installs a checkpoint even if the local head is newer. Call before binding the new
// attempt; the caller chooses the digest from its fenced PG record.
func (m *Manager) Restore(ctx context.Context, taskID, digest string) error {
	if m.cfg.Archive == nil {
		return errors.New("workspace: snapshot archive not configured")
	}
	w := m.get(taskID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gone {
		return errors.New("workspace: cannot restore destroyed workspace")
	}
	r, err := m.cfg.Archive.Get(ctx, digest)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	if digestBytes(data) != digest {
		return fmt.Errorf("workspace: manifest digest mismatch for %s", digest)
	}
	st, err := decodeState(data, taskID)
	if err != nil {
		return err
	}
	if st.State != StateActive {
		return fmt.Errorf("workspace: inactive snapshot state %q", st.State)
	}
	for sha, size := range st.snapshotObjects() {
		body, err := m.cfg.Archive.Get(ctx, sha)
		if err != nil {
			return err
		}
		ref, err := m.cfg.Blobs.Put(ctx, body)
		body.Close()
		if err != nil {
			return err
		}
		if ref.SHA256 != sha || (size >= 0 && ref.Size != size) {
			return fmt.Errorf("workspace: restored object mismatch for %s", sha)
		}
	}
	return m.installRecovered(w, st)
}

// ResetUncommitted starts a replacement with empty files when the task has never committed
// a checkpoint. Previously executed external effects are handled by the durable call journal.
func (m *Manager) ResetUncommitted(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w := m.get(taskID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gone {
		return errors.New("workspace: cannot reset destroyed workspace")
	}
	return m.installRecovered(w, &state{TaskID: taskID, State: StateActive, Files: map[string]entry{},
		Version: version(nil), CreatedAt: m.cfg.Now()})
}

func (m *Manager) installRecovered(w *ws, st *state) error {
	// Write quotas count all task writes, including work discarded by a rollback.
	local := w.st
	if local == nil {
		persisted, err := readState(m.path(st.TaskID), st.TaskID)
		if err != nil && !errors.Is(err, errCorrupt) {
			return err
		}
		local = persisted
	}
	if local != nil {
		st.WriteOps = max(st.WriteOps, local.WriteOps)
		st.WrittenBytes = max(st.WrittenBytes, local.WrittenBytes)
	}
	st.UsedAt = m.cfg.Now()
	if err := writeState(m.cfg.Dir, st); err != nil {
		return err
	}
	w.st, w.savedUse = st, st.UsedAt
	return nil
}
