package obs

import (
	"context"
	"log/slog"
)

// LogHandler wraps h so that records logged with a context carrying an active span get trace_id and span_id
// attributes (Grafana links them to Tempo). Without an active span — in particular whenever tracing is
// off — records pass through unchanged.
func LogHandler(h slog.Handler) slog.Handler { return logHandler{h} }

type logHandler struct{ slog.Handler }

func (l logHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if tid, sid := IDs(ctx); tid != "" {
			r = r.Clone()
			r.AddAttrs(slog.String("trace_id", tid), slog.String("span_id", sid))
		}
	}
	return l.Handler.Handle(ctx, r)
}

func (l logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return logHandler{l.Handler.WithAttrs(attrs)}
}

func (l logHandler) WithGroup(name string) slog.Handler { return logHandler{l.Handler.WithGroup(name)} }
