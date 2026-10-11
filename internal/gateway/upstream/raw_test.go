package upstream

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// RawStream (model API pass-through, design §7.3): a same-format stream hands out the provider's SSE events
// verbatim, in order, including events that produce no neutral event (ping, [DONE]); the raw events re-read as
// the same reply. A converted stream has no raw view.
func TestDoStreamRawEvents(t *testing.T) {
	p, q := fakePair(t)
	a := mixedAdapter(t, p, q)
	openaiBody := []byte(`{"model":"m-1","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`)
	anthropicBody := []byte(`{"model":"m-1","max_tokens":50,"messages":[{"role":"user","content":"hi"}]}`)
	for _, c := range []struct {
		name  string
		route int
		body  []byte
		proto llmproto.Protocol
		last  string // data of the last raw event
	}{
		{"openai", 0, openaiBody, llmproto.ProtocolOpenAI, "[DONE]"},
		{"anthropic", 1, anthropicBody, llmproto.ProtocolAnthropic, `{"type":"message_stop"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, e := a.DoStream(context.Background(), c.route, c.body, c.proto)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			rs := s.(RawStream)
			var raw []llmproto.SSEEvent
			for {
				_, err := s.Next()
				evs, ok := rs.TakeRaw()
				if !ok {
					t.Fatal("same-format stream has no raw view")
				}
				raw = append(raw, evs...)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(raw) < 3 || strings.TrimSpace(raw[len(raw)-1].Data) != c.last {
				t.Fatalf("raw events %d, last %q", len(raw), raw[len(raw)-1].Data)
			}
			var b strings.Builder
			for _, ev := range raw {
				b.Write(llmproto.FormatSSE(ev.Event, []byte(ev.Data)))
			}
			rd := llmproto.NewEventReader(strings.NewReader(b.String()), c.proto)
			var col llmproto.Collector
			for {
				ev, err := rd.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				col.Add(ev)
			}
			if !col.Complete() || !strings.HasPrefix(col.Response().Blocks[0].Text, "fake 回复") {
				t.Fatalf("re-read raw events: %+v", col.Response())
			}
		})
	}
	s, e := a.DoStream(context.Background(), 1, openaiBody, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, ok := s.(RawStream).TakeRaw(); ok {
		t.Fatal("converted stream has a raw view")
	}
}
