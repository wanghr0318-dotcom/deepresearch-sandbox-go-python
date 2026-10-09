// Package workspace implements per-turn workspaces for the agent's shell and file tools (design
// docs/design/2026-10-10-shell-file-mcp-design.md): a manifest of files (content in the BlobStore) held by the
// Gateway, advanced by exec_shell (one fresh exec environment per command, call.Coordinator.ExecShell) and by
// write_file, read by read_file and list_dir. File operations never touch a host path: they are manifest lookups
// and content-addressed blob reads.
//
// Lifecycle: created on first use; destroyed when the task is terminal (Sweep); expired after the idle timeout;
// recovered from its state file after a restart, or marked lost when that file cannot be trusted.
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
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// Error codes of workspace operations (other codes pass through from the exec pipeline).
const (
	CodeInvalidRequest   = "invalid_request"
	CodeUnsupportedField = "unsupported_field"
	CodeInvalidPath      = "invalid_path"
	CodeNotFound         = "not_found"
	CodeIsDirectory      = "is_directory"
	CodeNotDirectory     = "not_a_directory"
	CodePathConflict     = "path_conflict"
	CodeWorkspaceFull    = "workspace_full"
	CodeContentTooLarge  = "content_too_large"
	CodeWorkspaceExpired = "workspace_expired"
	CodeWorkspaceLost    = "workspace_lost"
	CodeStoreUnavailable = "store_unavailable"
)

// Limits (design §4.3).
const (
	DefaultMaxFiles    = 256
	DefaultMaxBytes    = 64 << 20
	DefaultIdleTimeout = 2 * time.Hour
	MaxWriteBytes      = 1 << 20
	MaxReadBytes       = 256 << 10
	MaxReadLines       = 2000
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
	Dir         string // <data>/tool-workspaces
	Exec        Execer
	Access      Access
	Blobs       Blobs
	Tasks       Tasks
	IdleTimeout time.Duration // default 2 h
	MaxFiles    int           // default 256 (= exec /out collection cap)
	MaxBytes    int64         // default 64 MiB (= --exec-out-bytes)
	Now         func() time.Time
	Logger      *slog.Logger
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
	mu sync.Mutex
	st *state // nil until loaded; destroyed → removed from Manager.wss
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
// after a restart (every file blob must exist), marking the workspace lost when it cannot be trusted.
func (m *Manager) load(taskID string, w *ws) (*state, error) {
	if w.st != nil {
		return w.st, nil
	}
	now := m.cfg.Now()
	st, err := readState(m.path(taskID), taskID)
	if err == nil && st != nil && st.State == StateActive {
		for p, e := range st.Files {
			rc, oerr := m.cfg.Blobs.Open(e.SHA256)
			if oerr != nil {
				err = fmt.Errorf("%w: blob of %q: %v", errCorrupt, p, oerr)
				break
			}
			_ = rc.Close() // existence check only
		}
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

// begin does the access check, locks and loads the workspace, and rejects expired/lost ones. On success the caller
// must call the returned unlock.
func (m *Manager) begin(ctx context.Context, r Request) (*ws, *state, func(), call.Result, error) {
	res, err := m.cfg.Access.CheckAccess(ctx, r.TaskID, r.AttemptID, r.SubrunID)
	if err != nil || res.Code != "" {
		return nil, nil, nil, res, err
	}
	w := m.get(r.TaskID)
	w.mu.Lock()
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

// ---- read_file ----

type readReq struct {
	Path      string `json:"path"`
	StartLine *int   `json:"start_line"`
	MaxLines  *int   `json:"max_lines"`
}

type readResp struct {
	Path       string `json:"path"`
	Content    string `json:"content"`
	StartLine  int    `json:"start_line"`
	EndLine    int    `json:"end_line"`
	TotalLines int    `json:"total_lines"`
	Truncated  bool   `json:"truncated"`
	Binary     bool   `json:"binary"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
}

// Read handles POST /v1/workspace/read.
func (m *Manager) Read(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	var q readReq
	defer func() { m.logOp("read", r, start, res, "path", q.Path) }()
	if code, ok := decode(r.Body, &q); !ok {
		return fail(http.StatusBadRequest, code), nil
	}
	first, max := 1, MaxReadLines
	if q.StartLine != nil {
		first = *q.StartLine
	}
	if q.MaxLines != nil {
		max = *q.MaxLines
	}
	if first < 1 || max < 1 || max > MaxReadLines {
		return fail(http.StatusBadRequest, CodeInvalidRequest), nil
	}
	if !ValidPath(q.Path) {
		return fail(http.StatusBadRequest, CodeInvalidPath), nil
	}
	_, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	e, ok := st.Files[q.Path]
	isDir := !ok && hasDir(st, q.Path)
	unlock()
	if isDir {
		return fail(http.StatusBadRequest, CodeIsDirectory), nil
	}
	if !ok {
		return fail(http.StatusNotFound, CodeNotFound), nil
	}
	data, err := m.readBlob(e.SHA256)
	if err != nil {
		return call.Result{}, err
	}
	out := readResp{Path: q.Path, StartLine: first, Size: e.Size, SHA256: e.SHA256}
	if !utf8.Valid(data) {
		out.Binary = true
		out.EndLine = first - 1
		return okJSON(out)
	}
	lines := strings.SplitAfter(string(data), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	out.TotalLines = len(lines)
	var b strings.Builder
	end := first - 1
	for i := first - 1; i < len(lines) && i < first-1+max; i++ {
		if b.Len()+len(lines[i]) > MaxReadBytes {
			out.Truncated = true
			break
		}
		b.WriteString(lines[i])
		end = i + 1
	}
	if end < len(lines) {
		out.Truncated = true
	}
	out.Content, out.EndLine = b.String(), end
	return okJSON(out)
}

func (m *Manager) readBlob(sha string) ([]byte, error) {
	rc, err := m.cfg.Blobs.Open(sha)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
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

// ---- write_file ----

type writeReq struct {
	Path       string  `json:"path"`
	Content    *string `json:"content"`
	Executable *bool   `json:"executable"`
}

type writeResp struct {
	Path      string  `json:"path"`
	Size      int64   `json:"size"`
	SHA256    string  `json:"sha256"`
	Created   bool    `json:"created"`
	Workspace summary `json:"workspace"`
}

// Write handles POST /v1/workspace/write.
func (m *Manager) Write(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	var q writeReq
	defer func() { m.logOp("write", r, start, res, "path", q.Path) }()
	if code, ok := decode(r.Body, &q); !ok {
		return fail(http.StatusBadRequest, code), nil
	}
	if q.Content == nil {
		return fail(http.StatusBadRequest, CodeInvalidRequest), nil
	}
	if !ValidPath(q.Path) {
		return fail(http.StatusBadRequest, CodeInvalidPath), nil
	}
	if len(*q.Content) > MaxWriteBytes {
		return fail(http.StatusRequestEntityTooLarge, CodeContentTooLarge), nil
	}
	w, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	defer unlock()
	old, exists := st.Files[q.Path]
	if !exists {
		if hasDir(st, q.Path) {
			return fail(http.StatusConflict, CodePathConflict), nil
		}
		for d := parent(q.Path); d != ""; d = parent(d) {
			if _, isFile := st.Files[d]; isFile {
				return fail(http.StatusConflict, CodePathConflict), nil
			}
		}
		if len(st.Files) >= m.cfg.MaxFiles {
			return fail(http.StatusConflict, CodeWorkspaceFull), nil
		}
	}
	size := int64(len(*q.Content))
	if st.bytes()-old.Size+size > m.cfg.MaxBytes {
		return fail(http.StatusConflict, CodeWorkspaceFull), nil
	}
	ref, err := m.cfg.Blobs.Put(ctx, strings.NewReader(*q.Content))
	if err != nil {
		return call.Result{}, err
	}
	exec := old.Exec
	if q.Executable != nil {
		exec = *q.Executable
	}
	next := st.clone()
	next.Files[q.Path] = entry{SHA256: ref.SHA256, Size: ref.Size, Exec: exec}
	if err := m.commit(w, next); err != nil {
		m.log.Error("gateway: workspace state write failed", "task_id", r.TaskID, "err", err)
		return fail(http.StatusServiceUnavailable, CodeStoreUnavailable), nil
	}
	return okJSON(writeResp{Path: q.Path, Size: ref.Size, SHA256: ref.SHA256, Created: !exists, Workspace: summarize(next)})
}

func parent(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}

// ---- list_dir ----

type listReq struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

type listEntry struct {
	Name       string `json:"name"`
	Type       string `json:"type"` // file | dir
	Size       int64  `json:"size,omitempty"`
	Executable bool   `json:"executable,omitempty"`
}

type listResp struct {
	Path      string      `json:"path"`
	Entries   []listEntry `json:"entries"`
	Workspace summary     `json:"workspace"`
}

// List handles POST /v1/workspace/list.
func (m *Manager) List(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	var q listReq
	defer func() { m.logOp("list", r, start, res, "path", q.Path) }()
	if len(bytes.TrimSpace(r.Body)) > 0 {
		if code, ok := decode(r.Body, &q); !ok {
			return fail(http.StatusBadRequest, code), nil
		}
	}
	if q.Path == "." {
		q.Path = ""
	}
	if !validDir(q.Path) {
		return fail(http.StatusBadRequest, CodeInvalidPath), nil
	}
	_, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	defer unlock()
	if _, isFile := st.Files[q.Path]; isFile {
		return fail(http.StatusBadRequest, CodeNotDirectory), nil
	}
	if q.Path != "" && !hasDir(st, q.Path) {
		return fail(http.StatusNotFound, CodeNotFound), nil
	}
	prefix := ""
	if q.Path != "" {
		prefix = q.Path + "/"
	}
	seen := map[string]bool{}
	entries := []listEntry{}
	for p, e := range st.Files {
		if !under(p, q.Path) {
			continue
		}
		rel := strings.TrimPrefix(p, prefix)
		if !q.Recursive {
			if i := strings.IndexByte(rel, '/'); i >= 0 {
				if d := rel[:i]; !seen[d] {
					seen[d] = true
					entries = append(entries, listEntry{Name: d, Type: "dir"})
				}
				continue
			}
		}
		entries = append(entries, listEntry{Name: rel, Type: "file", Size: e.Size, Executable: e.Exec})
	}
	sort.Slice(entries, func(a, b int) bool { return entries[a].Name < entries[b].Name })
	return okJSON(listResp{Path: q.Path, Entries: entries, Workspace: summarize(st)})
}

// ---- exec_shell ----

type execReq struct {
	Command   string `json:"command"`
	TimeoutMs *int64 `json:"timeout_ms"`
}

// execResult is the part of the exec result JSON the Manager needs.
type execResult struct {
	Status  string `json:"status"`
	Outputs []struct {
		Path       string `json:"path"`
		SHA256     string `json:"sha256"`
		Size       int64  `json:"size"`
		Executable bool   `json:"executable"`
	} `json:"outputs"`
	Skipped []struct {
		Path   string `json:"path"`
		Reason string `json:"reason"`
	} `json:"skipped_outputs"`
}

type changes struct {
	Added    []string `json:"added"`
	Modified []string `json:"modified"`
	Deleted  []string `json:"deleted"`
}

// Exec handles POST /v1/workspace/exec: runs the command against the current head and, when it completed or timed
// out, makes the /out snapshot the new head. The Manager waits for the command to settle even if the client went
// away (ctx is used only for the access check), so a settled result is always applied; a retry with the same call
// id returns the stored response.
func (m *Manager) Exec(ctx context.Context, r Request) (res call.Result, err error) {
	start := m.cfg.Now()
	defer func() { m.logOp("exec", r, start, res, "call_id", r.CallID) }()
	var q execReq
	if code, ok := decode(r.Body, &q); !ok {
		return fail(http.StatusBadRequest, code), nil
	}
	if q.Command == "" || len(q.Command) > call.MaxShellCommandBytes || (q.TimeoutMs != nil && *q.TimeoutMs <= 0) {
		return fail(http.StatusBadRequest, CodeInvalidRequest), nil
	}
	w, st, unlock, res, err := m.begin(ctx, r)
	if err != nil || res.Code != "" {
		return res, err
	}
	defer unlock()
	if a, ok := st.findApplied(r.CallID); ok {
		body, err := m.readBlob(a.ResponseSHA256)
		if err != nil {
			return call.Result{}, err
		}
		return call.Result{Status: http.StatusOK, Body: body, BlobSHA256: a.ResultSHA256, Replayed: true}, nil
	}
	in := call.ShellInvoke{TaskID: r.TaskID, AttemptID: r.AttemptID, CallID: r.CallID, SubrunID: r.SubrunID,
		Command: q.Command, Retry: r.Retry}
	if q.TimeoutMs != nil {
		in.WallMs = *q.TimeoutMs
	}
	paths := make([]string, 0, len(st.Files))
	for p := range st.Files {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	for _, p := range paths {
		e := st.Files[p]
		in.Files = append(in.Files, call.ShellFile{Path: p, SHA256: e.SHA256, Size: e.Size, Executable: e.Exec})
	}
	er, err := m.cfg.Exec.ExecShell(m.root, in)
	if err != nil || er.Code != "" || er.Status != http.StatusOK {
		return er, err // rejected, failed, cancelled or unknown: head unchanged
	}
	var x execResult
	if err := json.Unmarshal(er.Body, &x); err != nil {
		return call.Result{}, fmt.Errorf("workspace: exec result: %w", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(er.Body, &top); err != nil {
		return call.Result{}, fmt.Errorf("workspace: exec result: %w", err)
	}
	next := st.clone()
	ch := changes{Added: []string{}, Modified: []string{}, Deleted: []string{}}
	if x.Status == "completed" || x.Status == "timed_out" {
		next.Files = map[string]entry{}
		for _, o := range x.Outputs {
			if !ValidPath(o.Path) { // cannot happen for /out collection; never let it into the manifest
				x.Skipped = append(x.Skipped, struct {
					Path   string `json:"path"`
					Reason string `json:"reason"`
				}{o.Path, "invalid_path"})
				continue
			}
			next.Files[o.Path] = entry{SHA256: o.SHA256, Size: o.Size, Exec: o.Executable}
		}
		for p, e := range next.Files {
			old, ok := st.Files[p]
			switch {
			case !ok:
				ch.Added = append(ch.Added, p)
			case old != e:
				ch.Modified = append(ch.Modified, p)
			}
		}
		for p := range st.Files {
			if _, ok := next.Files[p]; !ok {
				ch.Deleted = append(ch.Deleted, p)
			}
		}
		slices.Sort(ch.Added)
		slices.Sort(ch.Modified)
		slices.Sort(ch.Deleted)
	}
	next.Version = version(next.Files)
	for k, v := range map[string]any{"changes": ch, "workspace": summarize(next)} {
		b, err := json.Marshal(v)
		if err != nil {
			return call.Result{}, err
		}
		top[k] = b
	}
	if len(x.Skipped) > 0 {
		b, err := json.Marshal(x.Skipped)
		if err != nil {
			return call.Result{}, err
		}
		top["skipped_outputs"] = b
	}
	body, err := json.Marshal(top)
	if err != nil {
		return call.Result{}, err
	}
	ref, err := m.cfg.Blobs.Put(m.root, bytes.NewReader(body))
	if err != nil {
		return call.Result{}, err
	}
	next.addApplied(applied{CallID: r.CallID, ResponseSHA256: ref.SHA256, ResultSHA256: er.BlobSHA256})
	if err := m.commit(w, next); err != nil {
		m.log.Error("gateway: workspace state write failed", "task_id", r.TaskID, "err", err)
		return fail(http.StatusServiceUnavailable, CodeStoreUnavailable), nil
	}
	return call.Result{Status: http.StatusOK, Body: body, BlobSHA256: er.BlobSHA256, Replayed: er.Replayed}, nil
}

// ---- lifecycle ----

// Sweep destroys the workspaces of terminal tasks and expires idle ones. It scans the state files (so workspaces
// left by a previous process are covered) and skips workspaces that are busy.
func (m *Manager) Sweep(ctx context.Context) error {
	ents, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return err
	}
	now := m.cfg.Now()
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			// A crashed writer's leftover; a fresh one may belong to a write in progress (rename pending).
			if fi, err := e.Info(); err == nil && strings.HasPrefix(name, ".tmp-") && time.Since(fi.ModTime()) > time.Minute {
				_ = os.Remove(filepath.Join(m.cfg.Dir, name))
			}
			continue
		}
		full := filepath.Join(m.cfg.Dir, name)
		st, rerr := readState(full, "")
		if rerr != nil || st == nil {
			// Unreadable without a task id: nothing can address it any more except its task, and the next access
			// marks it lost. Remove it only when it is also older than the idle timeout.
			if fi, serr := os.Stat(full); serr == nil && time.Since(fi.ModTime()) > m.cfg.IdleTimeout {
				_ = os.Remove(full)
			}
			continue
		}
		terminal, terr := m.cfg.Tasks.TaskTerminal(ctx, st.TaskID)
		if terr != nil {
			m.log.Warn("gateway: workspace sweep: task state", "task_id", st.TaskID, "err", terr)
			continue
		}
		w := m.get(st.TaskID)
		if !w.mu.TryLock() {
			continue // in use: next sweep
		}
		if terminal {
			if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
				m.log.Warn("gateway: workspace destroy", "task_id", st.TaskID, "err", err)
			} else {
				m.log.Info("gateway: workspace destroyed", "task_id", st.TaskID, "files", len(st.Files), "ops", st.Ops)
			}
			w.st = nil
			m.mu.Lock()
			delete(m.wss, st.TaskID)
			m.mu.Unlock()
			w.mu.Unlock()
			continue
		}
		cur := st
		if w.st != nil {
			cur = w.st
		}
		if cur.State == StateActive && now.Sub(cur.UsedAt) > m.cfg.IdleTimeout {
			next := cur.clone()
			next.State, next.Reason, next.Files = StateExpired, "idle_timeout", map[string]entry{}
			next.Version = version(nil)
			if err := writeState(m.cfg.Dir, next); err != nil {
				m.log.Warn("gateway: workspace expire", "task_id", st.TaskID, "err", err)
			} else {
				w.st = next
				m.log.Info("gateway: workspace expired", "task_id", st.TaskID, "idle", now.Sub(cur.UsedAt).String())
			}
		}
		w.mu.Unlock()
	}
	return nil
}

// Run sweeps every interval until ctx ends (one sweep immediately).
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := m.Sweep(ctx); err != nil && ctx.Err() == nil {
			m.log.Warn("gateway: workspace sweep", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
