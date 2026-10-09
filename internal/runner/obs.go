package runner

import (
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// runAttrs are the allowlisted attributes of a worker.run span (IDs and the mode only).
func runAttrs(mode string, a Attempt) []obs.Attr {
	return []obs.Attr{obs.Str("worker.mode", mode), obs.Str("task.id", a.TaskID), obs.Str("attempt.id", a.AttemptID),
		obs.Int("attempt.no", a.AttemptNo), obs.Str("env.id", a.EnvID)}
}

// endRun ends the worker.run span with the classified outcome (closed enumerations only).
func endRun(span obs.Span, out Outcome) {
	span.SetAttrs(obs.Str("outcome.class", out.Class))
	if out.PlatformKill != "" {
		span.SetAttrs(obs.Str("kill.reason", out.PlatformKill))
	}
	if out.Control != "" {
		span.SetAttrs(obs.Str("control", out.Control))
	}
	if out.OutputIncomplete {
		span.SetAttrs(obs.Bool("output_incomplete", true))
	}
	if out.Class != ClassSucceeded && out.Class != ClassPaused && out.Class != ClassCancelled {
		span.Fail(out.Class)
	}
	span.End()
}

// endHandshake ends the session worker.handshake span once (code "" = accepted).
func (att *sessionRun) endHandshake(code string) {
	if att.handshake == nil || att.handshakeDone {
		return
	}
	att.handshakeDone = true
	if code != "" {
		att.handshake.Fail(code)
	}
	att.handshake.End()
}
