package session

import (
	"context"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// opSpan starts a span for a session actor operation that touches the sandbox or the worker (start, quiesce,
// freeze, thaw, release, close, destroy, finish_close). Pure store transitions are not traced. Session operations
// are session-scoped roots: they are not part of any single turn's trace; session.id ties them together.
// Returns a nil span for untraced effects.
func opSpan(ctx context.Context, sessionID string, eff Effect) (context.Context, obs.Span) {
	var name string
	attrs := []obs.Attr{obs.Str("session.id", sessionID)}
	switch f := eff.(type) {
	case StartIncarnation:
		name = "session.start_incarnation"
		attrs = append(attrs, obs.Bool("resume", f.Resume != nil))
	case QuiesceIncarnation:
		name = "session.quiesce"
		attrs = append(attrs, obs.Str("incarnation.id", f.IncarnationID))
	case FreezeIncarnation:
		name = "session.freeze"
		attrs = append(attrs, obs.Str("incarnation.id", f.IncarnationID), obs.Str("env.id", f.EnvID))
	case ThawIncarnation:
		name = "session.thaw"
		attrs = append(attrs, obs.Str("incarnation.id", f.IncarnationID), obs.Str("env.id", f.EnvID))
	case ReleaseIncarnation:
		name = "session.release"
		attrs = append(attrs, obs.Str("task.id", f.Handoff.TaskID), obs.Str("attempt.id", f.Handoff.AttemptID),
			obs.Str("verdict", f.Handoff.Verdict))
	case CloseIncarnation:
		name = "session.close_incarnation"
		attrs = append(attrs, obs.Str("incarnation.id", f.IncarnationID))
	case DestroyIncarnation:
		name = "session.destroy_incarnation"
		attrs = append(attrs, obs.Str("incarnation.id", f.IncarnationID), obs.Str("env.id", f.EnvID), obs.Str("reason", f.Reason))
	case FinishCloseOp:
		name = "session.finish_close"
	default:
		return ctx, nil
	}
	return obs.Start(ctx, name, attrs...)
}

// endOpSpan ends an operation span with its result.
func endOpSpan(span obs.Span, done OpDone) {
	if span == nil {
		return
	}
	if done.IncarnationID != "" {
		span.SetAttrs(obs.Str("incarnation.id", done.IncarnationID))
	}
	switch {
	case done.Err != nil:
		span.Fail("store_error")
	case done.Failed != nil:
		span.Fail("op_failed")
	case done.Conflict:
		span.Fail("conflict")
	}
	span.End()
}
