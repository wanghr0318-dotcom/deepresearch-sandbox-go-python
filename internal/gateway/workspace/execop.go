package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// ---- exec_shell ----

type execReq struct {
	Command   string `json:"command"`
	TimeoutMs *int64 `json:"timeout_ms"`
}

// fingerprint identifies the request an applied call answered (replay must be the same request).
func (q execReq) fingerprint() string {
	b, _ := json.Marshal(q) // a struct of a string and an optional int always encodes
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type skippedOutput struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
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
	Skipped []skippedOutput `json:"skipped_outputs"`
	// Staged is the wrapper's positive signal that the workspace was copied into /out completely (call/shell.go).
	Staged bool `json:"workspace_staged"`
}

type changes struct {
	Added    []string `json:"added"`
	Modified []string `json:"modified"`
	Deleted  []string `json:"deleted"`
}

// Exec handles POST /v1/workspace/exec: runs the command against the current head and, when the wrapper confirmed
// staging and the command completed or timed out, makes the /out snapshot the new head. The Manager waits for the
// command to settle even if the client went away (the wait keeps the request's values — trace — but not its
// cancellation; only Close stops waiting), so a settled result is always applied; a retry with the same call id and
// the same request returns the stored response, a different request under that id is a fingerprint mismatch.
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
	fp := q.fingerprint()
	if a, ok := st.findApplied(r.CallID); ok {
		return m.replayApplied(a, fp)
	}
	in := shellInvoke(r, q, st)
	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stopWait := context.AfterFunc(m.root, cancel)
	defer stopWait()
	er, err := m.cfg.Exec.ExecShell(wctx, in)
	if err != nil || er.Code != "" || er.Status != http.StatusOK {
		return er, err // rejected, failed, cancelled or unknown: head unchanged
	}
	var x execResult
	if err := json.Unmarshal(er.Body, &x); err != nil {
		return call.Result{}, fmt.Errorf("workspace: exec result: %w", err)
	}
	if !x.Staged {
		// The command never ran on a complete copy of the workspace (staging failed or was cut short): /out is not
		// a workspace version. The exec call itself is journaled (quota used); the head stays as it was.
		m.log.Warn("gateway: workspace staging failed", "task_id", r.TaskID, "call_id", r.CallID, "status", x.Status)
		return fail(http.StatusServiceUnavailable, CodeStagingFailed), nil
	}
	next, ch := m.applySnapshot(st, &x)
	body, err := buildResponse(er.Body, ch, next, x.Skipped)
	if err != nil {
		return call.Result{}, err
	}
	ref, err := m.cfg.Blobs.Put(wctx, bytes.NewReader(body))
	if err != nil {
		return call.Result{}, err
	}
	next.addApplied(applied{CallID: r.CallID, RequestSHA256: fp, ResponseSHA256: ref.SHA256, ResultSHA256: er.BlobSHA256})
	if err := m.commit(w, next); err != nil {
		m.log.Error("gateway: workspace state write failed", "task_id", r.TaskID, "err", err)
		return fail(http.StatusServiceUnavailable, CodeStoreUnavailable), nil
	}
	return call.Result{Status: http.StatusOK, Body: body, BlobSHA256: er.BlobSHA256, Replayed: er.Replayed}, nil
}

func (m *Manager) replayApplied(a applied, fp string) (call.Result, error) {
	if a.RequestSHA256 != fp {
		return fail(http.StatusConflict, CodeFingerprintMismatch), nil
	}
	rc, err := m.cfg.Blobs.Open(a.ResponseSHA256)
	if err != nil {
		return call.Result{}, err
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return call.Result{}, err
	}
	return call.Result{Status: http.StatusOK, Body: buf.Bytes(), BlobSHA256: a.ResultSHA256, Replayed: true}, nil
}

func shellInvoke(r Request, q execReq, st *state) call.ShellInvoke {
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
	return in
}

// applySnapshot builds the next state from the collected /out (completed or timed_out; other statuses keep the files)
// and the changes against the current head. Outputs that are not valid workspace paths (".agentbox/…", control
// characters, …) are moved to x.Skipped with reason invalid_path.
//
// MaxBytes is not enforced here: the snapshot is what the command left in /out, which the exec environment already
// bounds (the /out tmpfs is --exec-out-bytes, at least MaxBytes + StagingHeadroom; at most MaxFiles outputs are
// collected). A command can therefore leave a workspace above MaxBytes (by at most --exec-out-bytes − MaxBytes);
// it is kept as it is — the response's "workspace.bytes" shows the size — and write_file answers workspace_full
// while its result would exceed MaxBytes. Dropping outputs instead would silently lose the command's results.
func (m *Manager) applySnapshot(st *state, x *execResult) (*state, changes) {
	next := st.clone()
	ch := changes{Added: []string{}, Modified: []string{}, Deleted: []string{}}
	if x.Status != "completed" && x.Status != "timed_out" {
		return next, ch
	}
	next.Files = map[string]entry{}
	for _, o := range x.Outputs {
		if !ValidPath(o.Path) {
			x.Skipped = append(x.Skipped, skippedOutput{o.Path, "invalid_path"})
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
	next.Version = version(next.Files)
	return next, ch
}

// buildResponse is the exec result JSON plus "changes", "workspace" and the final "skipped_outputs".
func buildResponse(result []byte, ch changes, next *state, skipped []skippedOutput) ([]byte, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(result, &top); err != nil {
		return nil, fmt.Errorf("workspace: exec result: %w", err)
	}
	if skipped == nil {
		skipped = []skippedOutput{}
	}
	for k, v := range map[string]any{"changes": ch, "workspace": summarize(next), "skipped_outputs": skipped} {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		top[k] = b
	}
	return json.Marshal(top)
}
