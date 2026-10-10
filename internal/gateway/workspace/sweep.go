package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Sweep destroys the workspaces of terminal tasks and expires idle ones. It scans the state files (so workspaces
// left by a previous process are covered) and skips workspaces that are busy.
func (m *Manager) Sweep(ctx context.Context) error {
	ents, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := e.Name()
		switch {
		case strings.HasPrefix(name, ".tmp-"):
			// A crashed writer's leftover; a fresh one may belong to a write in progress (rename pending).
			if fi, err := e.Info(); err == nil && m.cfg.Now().Sub(fi.ModTime()) > time.Minute {
				_ = os.Remove(filepath.Join(m.cfg.Dir, name))
			}
		case strings.HasSuffix(name, ".json"):
			m.sweepOne(ctx, filepath.Join(m.cfg.Dir, name))
		}
	}
	return nil
}

// sweepOne handles one state file: destroy when the task is terminal, expire when idle.
func (m *Manager) sweepOne(ctx context.Context, full string) {
	st, err := readState(full, "")
	if err != nil || st == nil {
		// Unreadable without a task id: nothing can address it any more except its task, and the next access marks
		// it lost (or reports the I/O error). Remove it only when it is also older than the idle timeout.
		if errors.Is(err, errCorrupt) {
			if fi, serr := os.Stat(full); serr == nil && m.cfg.Now().Sub(fi.ModTime()) > m.cfg.IdleTimeout {
				_ = os.Remove(full)
			}
		}
		return
	}
	terminal, err := m.cfg.Tasks.TaskTerminal(ctx, st.TaskID)
	if err != nil {
		m.log.Warn("gateway: workspace sweep: task state", "task_id", st.TaskID, "err", err)
		return
	}
	w := m.get(st.TaskID)
	if !w.mu.TryLock() {
		return // in use: next sweep
	}
	defer w.mu.Unlock()
	if terminal {
		m.destroy(w, st, full)
		return
	}
	cur := st
	if w.st != nil {
		cur = w.st
	}
	if cur.State != StateActive || m.cfg.Now().Sub(cur.UsedAt) <= m.cfg.IdleTimeout {
		return
	}
	next := cur.clone()
	next.State, next.Reason, next.Files = StateExpired, "idle_timeout", map[string]entry{}
	next.Version = version(nil)
	if err := writeState(m.cfg.Dir, next); err != nil {
		m.log.Warn("gateway: workspace expire", "task_id", st.TaskID, "err", err)
		return
	}
	w.st = next
	m.log.Info("gateway: workspace expired", "task_id", st.TaskID, "idle", m.cfg.Now().Sub(cur.UsedAt).String())
}

// destroy removes the state file (caller holds w.mu) and leaves a tombstone on w: requests already waiting for this
// workspace get 410 workspace_destroyed instead of recreating it.
func (m *Manager) destroy(w *ws, st *state, full string) {
	if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.log.Warn("gateway: workspace destroy", "task_id", st.TaskID, "err", err)
		return
	}
	m.log.Info("gateway: workspace destroyed", "task_id", st.TaskID, "files", len(st.Files), "ops", st.Ops)
	w.st, w.gone = nil, true
	m.mu.Lock()
	if m.wss[st.TaskID] == w {
		delete(m.wss, st.TaskID)
	}
	m.mu.Unlock()
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
