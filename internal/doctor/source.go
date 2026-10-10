package doctor

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

// LoadRunDir reads an eval run directory.
func LoadRunDir(dir string) (Source, []*eval.Trajectory, error) {
	lr, err := eval.LoadRun(dir)
	if err != nil {
		return Source{}, nil, err
	}
	m := lr.Manifest
	return Source{Kind: "eval_run", Dir: dir, RunID: m.RunID, Suite: m.Suite.Name, Agent: m.Agent, Server: m.Server.Addr},
		lr.Trajectories, nil
}

// Window selects the tasks of a server source.
type Window struct {
	Since, Until time.Time // created_at in [Since, Until)
	MaxTasks     int       // default 500
	// EventsTimeout bounds reading the event stream of a task that is not terminal (default 3 s).
	EventsTimeout time.Duration
}

// LoadServer reads the tasks created in the window from a running server (operator API): GET /tasks pages
// (newest first) until a page is older than Since, then inspect and the event stream of each task. Tasks that are
// still running are included with the events so far. It returns notes about anything skipped.
func LoadServer(ctx context.Context, c *eval.Client, w Window) (Source, []*eval.Trajectory, []string, error) {
	if w.MaxTasks <= 0 {
		w.MaxTasks = 500
	}
	if w.EventsTimeout <= 0 {
		w.EventsTimeout = 3 * time.Second
	}
	src := Source{Kind: "server", Server: redact(c.Base), Since: w.Since.UTC().Format(time.RFC3339)}
	if !w.Until.IsZero() {
		src.Until = w.Until.UTC().Format(time.RFC3339)
	}
	var views []eval.TaskView
	var notes []string
	after := ""
pages:
	for {
		page, err := c.ListTasks(ctx, after, 200)
		if err != nil {
			return src, nil, nil, fmt.Errorf("list tasks: %w", err)
		}
		for _, v := range page.Tasks {
			if v.CreatedAt.Before(w.Since) {
				break pages // newest first: everything after this is older
			}
			if !w.Until.IsZero() && !v.CreatedAt.Before(w.Until) {
				continue
			}
			if len(views) == w.MaxTasks {
				notes = append(notes, fmt.Sprintf("more than %d tasks in the window: only the newest %d are analysed", w.MaxTasks, w.MaxTasks))
				break pages
			}
			views = append(views, v)
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	var trs []*eval.Trajectory
	for _, v := range views {
		tr, err := loadTask(ctx, c, v, w.EventsTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return src, nil, nil, ctx.Err()
			}
			notes = append(notes, fmt.Sprintf("task %s skipped: %v", v.TaskID, err))
			continue
		}
		trs = append(trs, tr)
	}
	return src, trs, notes, nil
}

// terminalStatuses are the task statuses that end the event stream.
var terminalStatuses = map[string]bool{"succeeded": true, "failed": true, "cancelled": true}

func loadTask(ctx context.Context, c *eval.Client, v eval.TaskView, eventsTimeout time.Duration) (*eval.Trajectory, error) {
	in, _, err := c.Inspect(ctx, v.TaskID)
	if err != nil {
		return nil, fmt.Errorf("inspect: %w", err)
	}
	tr := &eval.Trajectory{Schema: eval.SchemaTrajectory, TaskID: "", ServerTaskID: v.TaskID, Kind: "task",
		Status: in.Task.Status, StatusReason: in.Task.StatusReason, Calls: in.Calls, Attempts: in.Attempts,
		Checkpoints: in.Checkpoints, Subruns: in.Subruns, Budget: in.Budget, SubmittedAt: v.CreatedAt}
	if tr.Calls == nil {
		tr.Calls = []eval.Call{}
	}
	ectx := ctx
	if !terminalStatuses[in.Task.Status] {
		var cancel context.CancelFunc
		ectx, cancel = context.WithTimeout(ctx, eventsTimeout)
		defer cancel()
	}
	err = c.Events(ectx, v.TaskID, func(ev eval.Event) { tr.Events = append(tr.Events, ev) })
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("events: %w", err)
	}
	end := time.Now()
	for _, ev := range tr.Events {
		if ev.Source == "host" && ev.Type == "task_terminal" {
			end = ev.TS
		}
	}
	tr.TerminalAt = end
	tr.LatencyMs = max(0, end.Sub(v.CreatedAt).Milliseconds())
	if in.Task.Status == "succeeded" {
		tr.Outcome = eval.Outcome{Verdict: eval.VerdictPass}
	} else if terminalStatuses[in.Task.Status] {
		tr.Outcome = eval.Outcome{Verdict: eval.VerdictFail, Category: "task_" + in.Task.Status + ":" + in.Task.StatusReason}
	} else {
		// Running or paused: not a failure; it may still be slow.
		tr.Outcome = eval.Outcome{Verdict: eval.VerdictPass, Category: "not_terminal:" + in.Task.Status}
	}
	return tr, nil
}

// redact strips userinfo from an address.
func redact(addr string) string {
	u, err := url.Parse(addr)
	if err != nil {
		return "(invalid address)"
	}
	u.User = nil
	return u.String()
}
