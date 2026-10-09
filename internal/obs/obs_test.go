package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

type vectors struct {
	Valid []struct {
		Value, TraceID, ParentID string
		Sampled                  bool
	}
	Invalid []string
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "protocol", "fixtures", "v1", "traceparent.json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Valid []struct {
			Value    string `json:"value"`
			TraceID  string `json:"trace_id"`
			ParentID string `json:"parent_id"`
			Sampled  bool   `json:"sampled"`
		} `json:"valid"`
		Invalid []string `json:"invalid"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	var v vectors
	for _, x := range raw.Valid {
		v.Valid = append(v.Valid, struct {
			Value, TraceID, ParentID string
			Sampled                  bool
		}{x.Value, x.TraceID, x.ParentID, x.Sampled})
	}
	v.Invalid = raw.Invalid
	return v
}

func TestParseTraceparentVectors(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Valid {
		tc, ok := obs.ParseTraceparent(c.Value)
		if !ok || tc.TraceID != c.TraceID || tc.ParentID != c.ParentID || tc.Sampled() != c.Sampled {
			t.Errorf("ParseTraceparent(%q) = %+v, %v", c.Value, tc, ok)
		}
		if got := obs.FormatTraceparent(tc); got != c.Value {
			t.Errorf("FormatTraceparent round trip = %q, want %q", got, c.Value)
		}
	}
	for _, s := range v.Invalid {
		if tc, ok := obs.ParseTraceparent(s); ok {
			t.Errorf("ParseTraceparent(%q) accepted: %+v", s, tc)
		}
	}
}

// Off by default: no tracer, no recorder, spans are no-ops and contexts are untouched.
func TestOffIsNoop(t *testing.T) {
	if obs.Tracing() || obs.Metrics() || obs.Enabled() {
		t.Fatal("observability must be off by default")
	}
	ctx := context.Background()
	got, sp := obs.Start(ctx, "x", obs.Str("k", "v"))
	if got != ctx {
		t.Error("Start must return ctx unchanged when off")
	}
	sp.SetAttrs(obs.Int("n", 1))
	sp.Event("e")
	sp.Fail("code")
	sp.End()
	if tp := obs.Traceparent(got); tp != "" {
		t.Errorf("Traceparent = %q", tp)
	}
	if obs.WithRemoteParent(ctx, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") != ctx {
		t.Error("WithRemoteParent must be a no-op when off")
	}
	obs.NoteSubmit(ctx, "task-off")
	if _, _, ok := obs.TakeSubmit("task-off"); ok {
		t.Error("NoteSubmit must not record when off")
	}
	obs.M().GatewayCall("chat", "completed") // must not panic
}

func TestSubmitRegistry(t *testing.T) {
	tr, _ := obstest.Install(t)
	ctx, sp := obs.Start(context.Background(), "HTTP POST /tasks")
	obs.NoteSubmit(ctx, "task-1")
	sp.End()
	tp, at, ok := obs.TakeSubmit("task-1")
	if !ok || at.IsZero() {
		t.Fatal("submission not recorded")
	}
	tc, ok := obs.ParseTraceparent(tp)
	if !ok || tc.TraceID != tr.Spans()[0].TraceID || tc.ParentID != tr.Spans()[0].SpanID {
		t.Errorf("traceparent %q does not identify the API span", tp)
	}
	if _, _, ok := obs.TakeSubmit("task-1"); ok {
		t.Error("TakeSubmit must remove the entry")
	}
	// A task span started from the remote parent joins the API trace.
	actx := obs.WithRemoteParent(context.Background(), tp)
	_, ts := obs.Start(actx, "task")
	ts.End()
	spans := tr.Spans()
	if spans[1].TraceID != spans[0].TraceID || spans[1].ParentID != spans[0].SpanID {
		t.Errorf("task span not a child of the API span: %+v", spans)
	}
}

func TestLogHandlerAddsIDsOnlyWithSpan(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(obs.LogHandler(slog.NewJSONHandler(&buf, nil)))
	log.InfoContext(context.Background(), "no span")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatalf("trace_id without a span: %s", buf.String())
	}
	buf.Reset()
	obstest.Install(t)
	ctx, sp := obs.Start(context.Background(), "s")
	defer sp.End()
	log.With("a", 1).InfoContext(ctx, "with span")
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatal(err)
	}
	tid, sid := obs.IDs(ctx)
	if rec["trace_id"] != tid || rec["span_id"] != sid || tid == "" {
		t.Errorf("record %v lacks trace_id %s / span_id %s", rec, tid, sid)
	}
}

func TestCarry(t *testing.T) {
	tr, _ := obstest.Install(t)
	src, sp := obs.Start(context.Background(), "parent")
	defer sp.End()
	dst := obs.Carry(context.Background(), src)
	_, child := obs.Start(dst, "child")
	child.End()
	s := tr.Spans()
	if s[1].ParentID != s[0].SpanID {
		t.Errorf("child parent = %q, want %q", s[1].ParentID, s[0].SpanID)
	}
}

// BenchmarkOffPath is the per-call cost of instrumentation when observability is off (the default): a span with
// attributes, a metric event and a traceparent lookup.
func BenchmarkOffPath(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c, sp := obs.Start(ctx, "gateway.call", obs.Str("task.id", "t"), obs.Int("try.no", 1))
		sp.SetAttrs(obs.Str("call.result", "completed"))
		sp.End()
		obs.M().GatewayCall("chat", "completed")
		_ = obs.Traceparent(c)
	}
}
