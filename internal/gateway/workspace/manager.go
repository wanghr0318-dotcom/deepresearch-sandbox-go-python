// Package workspace implements per-turn workspaces for the agent's shell and file tools (design
// docs/design/2026-10-10-shell-file-mcp-design.md): a manifest of files (content in the BlobStore) held by the
// Gateway, advanced by exec_shell (one fresh exec environment per command, call.Coordinator.ExecShell) and by
// write_file, read by read_file and list_dir. File operations never touch a host path: they are manifest lookups
// and content-addressed blob reads.
//
// Lifecycle: created on first use; destroyed when the task is terminal (Sweep); expired after the idle timeout;
// recovered from its state file after a restart, or marked lost when that file cannot be trusted.
//
// Blobs written through a workspace (write_file content, exec outputs) are ordinary BlobStore content and are never
// garbage-collected; write_file is therefore metered per task (MaxWriteOps, MaxWrittenBytes).
package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// Error codes of workspace operations (other codes pass through from the exec pipeline).
const (
	CodeInvalidRequest      = "invalid_request"
	CodeUnsupportedField    = "unsupported_field"
	CodeInvalidPath         = "invalid_path"
	CodeNotFound            = "not_found"
	CodeIsDirectory         = "is_directory"
	CodeNotDirectory        = "not_a_directory"
	CodePathConflict        = "path_conflict"
	CodeWorkspaceFull       = "workspace_full"
	CodeWriteQuota          = "workspace_write_quota" // 429: per-task write_file ops or bytes exhausted
	CodeContentTooLarge     = "content_too_large"
	CodeWorkspaceExpired    = "workspace_expired"
	CodeWorkspaceLost       = "workspace_lost"
	CodeWorkspaceDestroyed  = "workspace_destroyed" // 410: the task ended and the sweeper destroyed the workspace
	CodeStagingFailed       = "workspace_staging_failed"
	CodeFingerprintMismatch = "fingerprint_mismatch"
	CodeStoreUnavailable    = "store_unavailable"
)

// Limits (design §4.3).
const (
	DefaultMaxFiles        = 256
	DefaultMaxBytes        = 64<<20 - StagingHeadroom
	DefaultIdleTimeout     = 2 * time.Hour
	DefaultMaxWriteOps     = 2000
	DefaultMaxWrittenBytes = 256 << 20
	MaxWriteBytes          = 1 << 20
	MaxReadBytes           = 256 << 10
	MaxReadLines           = 2000
	// StagingHeadroom is kept free in the /out tmpfs beyond the workspace bytes: one 4 KiB block per file for
	// rounding (DefaultMaxFiles × 4 KiB), so staging a full workspace always fits. The command gets the remaining
	// free space (at least --exec-out-bytes − workspace bytes − headroom).
	StagingHeadroom = DefaultMaxFiles * 4 << 10
)

// Execer runs workspace commands (*call.Coordinator).
type Execer interface {
	ExecShell(ctx context.Context, in call.ShellInvoke) (call.Result, error)
}

// Access is the §9.2 access check (*call.Coordinator).
type Access interface {
	CheckAccess(ctx context.Context, taskID, attemptID, subrunID string) (call.Result, error)
}

// Blobs is the BlobStore subset used here.
type Blobs interface {
	Put(ctx context.Context, r io.Reader) (blob.Ref, error)
	Open(sha string) (io.ReadCloser, error)
}

// Tasks reports whether a task is terminal (or no longer exists).
type Tasks interface {
	TaskTerminal(ctx context.Context, taskID string) (bool, error)
}

// Config assembles a Manager. Dir, Exec, Access, Blobs and Tasks are required.
type Config struct {
	Dir             string // <data>/tool-workspaces
	Exec            Execer
	Access          Access
	Blobs           Blobs
	Tasks           Tasks
	IdleTimeout     time.Duration // default 2 h
	MaxFiles        int           // default 256 (= exec /out collection cap)
	MaxBytes        int64         // default 64 MiB − StagingHeadroom (= --exec-out-bytes − headroom)
	MaxWriteOps     int64         // per task, default 2000
	MaxWrittenBytes int64         // per task, default 256 MiB
	Now             func() time.Time
	Logger          *slog.Logger
}

// Request is one workspace operation from the edge.
type Request struct {
	TaskID, AttemptID, SubrunID string
	CallID                      string // exec only
	Body                        []byte
	Retry                       bool
}

// Manager owns all workspaces of this Gateway.
type Manager struct {
	cfg  Config
	log  *slog.Logger
	root context.Context
	stop context.CancelFunc

	mu  sync.Mutex
	wss map[string]*ws // by task id; loaded lazily
}

type ws struct {
	mu   sync.Mutex
	st   *state // nil until loaded
	gone bool   // destroyed by Sweep: requests that waited on mu must not recreate it
}

// New validates the config, creates Dir (0700) and returns a Manager.
func New(cfg Config) (*Manager, error) {
	if cfg.Dir == "" || cfg.Exec == nil || cfg.Access == nil || cfg.Blobs == nil || cfg.Tasks == nil {
		return nil, errors.New("workspace: Dir, Exec, Access, Blobs and Tasks are required")
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.MaxFiles <= 0 {
		cfg.MaxFiles = DefaultMaxFiles
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	if cfg.MaxWriteOps <= 0 {
		cfg.MaxWriteOps = DefaultMaxWriteOps
	}
	if cfg.MaxWrittenBytes <= 0 {
		cfg.MaxWrittenBytes = DefaultMaxWrittenBytes
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("workspace: create %s: %w", cfg.Dir, err)
	}
	m := &Manager{cfg: cfg, log: log, wss: map[string]*ws{}}
	m.root, m.stop = context.WithCancel(context.Background())
	return m, nil
}

// Close cancels waits on in-flight commands (they continue and settle in the coordinator; a retry replays them).
func (m *Manager) Close() { m.stop() }

func (m *Manager) get(taskID string) *ws {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.wss[taskID]
	if w == nil {
		w = &ws{}
		m.wss[taskID] = w
	}
	return w
}

func (m *Manager) path(taskID string) string { return filepath.Join(m.cfg.Dir, fileName(taskID)) }

// load returns the workspace state (caller holds w.mu): creates it on first use; reads and verifies the state file
// after a restart (every file blob must exist), marking the workspace lost when it cannot be trusted. I/O errors
// (state file or blob store unreadable) are returned and leave the workspace as it is.
func (m *Manager) load(taskID string, w *ws) (*state, error) {
	if w.st != nil {
		return w.st, nil
	}
	now := m.cfg.Now()
	st, err := readState(m.path(taskID), taskID)
	if err == nil && st != nil && st.State == StateActive {
		err = m.checkBlobs(st)
		if err == nil {
			m.log.Info("gateway: workspace recovered", "task_id", taskID, "files", len(st.Files), "version", st.Version)
		}
	}
	switch {
	case errors.Is(err, errCorrupt):
		m.log.Warn("gateway: workspace lost", "task_id", taskID, "err", err)
		st = &state{TaskID: taskID, State: StateLost, Reason: "state_unreadable", Files: map[string]entry{},
			Version: version(nil), CreatedAt: now, UsedAt: now}
		if werr := writeState(m.cfg.Dir, st); werr != nil {
			return nil, werr
		}
	case err != nil:
		return nil, err
	case st == nil:
		st = &state{TaskID: taskID, State: StateActive, Files: map[string]entry{}, Version: version(nil),
			CreatedAt: now, UsedAt: now}
		if werr := writeState(m.cfg.Dir, st); werr != nil {
			return nil, werr
		}
		m.log.Info("gateway: workspace created", "task_id", taskID)
	}
	w.st = st
	return st, nil
}

// checkBlobs: a missing blob makes the state corrupt; other errors are transient.
func (m *Manager) checkBlobs(st *state) error {
	for p, e := range st.Files {
		rc, err := m.cfg.Blobs.Open(e.SHA256)
		if errors.Is(err, blob.ErrNotFound) {
			return fmt.Errorf("%w: blob of %q missing", errCorrupt, p)
		}
		if err != nil {
			return fmt.Errorf("workspace: open blob of %q: %w", p, err)
		}
		_ = rc.Close() // existence check only
	}
	return nil
}

// commit persists next and makes it current (caller holds w.mu). On failure the current state is unchanged.
func (m *Manager) commit(w *ws, next *state) error {
	next.Version = version(next.Files)
	next.UsedAt = m.cfg.Now()
	next.Ops++
	if err := writeState(m.cfg.Dir, next); err != nil {
		return err
	}
	w.st = next
	return nil
}

func fail(status int, code string) call.Result { return call.Result{Status: status, Code: code} }

func okJSON(v any) (call.Result, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return call.Result{}, err
	}
	return call.Result{Status: http.StatusOK, Body: bytes.TrimSuffix(buf.Bytes(), []byte("\n"))}, nil
}

// begin does the access check, locks and loads the workspace, and rejects expired/lost/destroyed ones. Every
// accepted operation (reads included) refreshes the idle clock. On success the caller must call the returned unlock.
func (m *Manager) begin(ctx context.Context, r Request) (*ws, *state, func(), call.Result, error) {
	res, err := m.cfg.Access.CheckAccess(ctx, r.TaskID, r.AttemptID, r.SubrunID)
	if err != nil || res.Code != "" {
		return nil, nil, nil, res, err
	}
	w := m.get(r.TaskID)
	w.mu.Lock()
	if w.gone {
		w.mu.Unlock()
		return nil, nil, nil, fail(http.StatusGone, CodeWorkspaceDestroyed), nil
	}
	st, err := m.load(r.TaskID, w)
	if err != nil {
		w.mu.Unlock()
		return nil, nil, nil, call.Result{}, err
	}
	switch st.State {
	case StateExpired:
		w.mu.Unlock()
		return nil, nil, nil, fail(http.StatusGone, CodeWorkspaceExpired), nil
	case StateLost:
		w.mu.Unlock()
		return nil, nil, nil, fail(http.StatusGone, CodeWorkspaceLost), nil
	}
	st.UsedAt = m.cfg.Now() // in memory; persisted with the next change (Sweep reads the in-memory state)
	return w, st, w.mu.Unlock, call.Result{}, nil
}

// decode parses a JSON object body, rejecting unknown members.
func decode(body []byte, dst any) (string, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return CodeUnsupportedField, false
		}
		return CodeInvalidRequest, false
	}
	if dec.More() {
		return CodeInvalidRequest, false
	}
	return "", true
}

// summary is the "workspace" member of responses.
type summary struct {
	Version string `json:"version"`
	Files   int    `json:"files"`
	Bytes   int64  `json:"bytes"`
}

func summarize(st *state) summary { return summary{st.Version, len(st.Files), st.bytes()} }

func (m *Manager) logOp(op string, r Request, start time.Time, res call.Result, attrs ...any) {
	args := append([]any{"op", op, "task_id", r.TaskID, "status", res.Status, "code", res.Code,
		"latency_ms", m.cfg.Now().Sub(start).Milliseconds()}, attrs...)
	m.log.Info("gateway: workspace", args...)
}

// hasDir reports whether any file lies under dir.
func hasDir(st *state, dir string) bool {
	for p := range st.Files {
		if under(p, dir) {
			return true
		}
	}
	return false
}

func parent(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}
