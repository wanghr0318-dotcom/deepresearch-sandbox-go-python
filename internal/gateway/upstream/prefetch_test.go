package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// anthropicStreamAdapter routes logical model m-1 to an Anthropic provider at base (route 1).
func anthropicStreamAdapter(t *testing.T, base string) ChatAdapter {
	t.Helper()
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	return NewChat(ChatConfig{BaseURL: "http://127.0.0.1:1/v1", Model: "m-1", HTTP: d.HTTPClient(DefaultModelMaxBody, 0),
		StreamHTTP: d.HTTPClient(0, 0),
		Fallbacks: []ChatRoute{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: base,
			HTTP: d.HTTPClient(DefaultModelMaxBody, 0), StreamHTTP: d.HTTPClient(0, 0)}}}).(ChatAdapter)
}

func sse(event, data string) string { return "event: " + event + "\ndata: " + data + "\n\n" }

const (
	msgStart = `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":1}}}`
	msgTail  = `{"type":"content_block_stop","index":0}`
)

func anthropicTail() string {
	return sse("content_block_stop", msgTail) +
		sse("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}`) +
		sse("message_stop", `{"type":"message_stop"}`)
}

// Close drops the buffered events: Next after Close fails at once with ErrStreamClosed instead of replaying
// message_start and the first content.
func TestNextAfterCloseFails(t *testing.T) {
	base := sseServer(t, func(w http.ResponseWriter, flush func()) {
		_, _ = io.WriteString(w, chunk(`{"role":"assistant","content":""}`)+chunk(`{"content":"hello"}`)+streamTail)
		flush()
	})
	s, e := streamAdapter(t, base, 0).DoStream(context.Background(), 0, openaiBody, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if ev, err := s.Next(); !errors.Is(err, ErrStreamClosed) {
			t.Fatalf("Next after Close: %+v %v", ev, err)
		}
	}
}

// Events before the first content are capped: more than maxPrefetchEvents empty blocks, or more than
// maxPrefetchBytes in them, is a retryable upstream_bad_response before the first byte (failover possible).
func TestPrefetchCap(t *testing.T) {
	emptyBlocks := func(n int) string {
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteString(sse("content_block_start", fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":{"type":"text","text":""}}`, i)))
			b.WriteString(sse("content_block_stop", fmt.Sprintf(`{"type":"content_block_stop","index":%d}`, i)))
		}
		return b.String()
	}
	body := []byte(`{"model":"m-1","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`)
	for name, stream := range map[string]string{
		"events": sse("message_start", msgStart) + emptyBlocks(maxPrefetchEvents),
		"bytes": sse("message_start", strings.Replace(msgStart, `"msg_1"`, `"msg_`+strings.Repeat("a", maxPrefetchBytes)+`"`, 1)) +
			emptyBlocks(1),
	} {
		t.Run(name, func(t *testing.T) {
			base := sseServer(t, func(w http.ResponseWriter, flush func()) {
				_, _ = io.WriteString(w, stream)
				flush()
			})
			s, e := anthropicStreamAdapter(t, base).DoStream(context.Background(), 1, body, llmproto.ProtocolAnthropic)
			if s != nil || e == nil || e.Outcome != OutcomeRetryable || e.Code != CodeUpstreamBadResponse {
				t.Fatalf("got stream=%v err=%v, want retryable upstream_bad_response", s != nil, e)
			}
		})
	}
	// Just under the event cap is fine.
	base := sseServer(t, func(w http.ResponseWriter, flush func()) {
		_, _ = io.WriteString(w, sse("message_start", msgStart)+emptyBlocks((maxPrefetchEvents-3)/2)+
			sse("content_block_start", `{"type":"content_block_start","index":999,"content_block":{"type":"text","text":""}}`)+
			sse("content_block_delta", `{"type":"content_block_delta","index":999,"delta":{"type":"text_delta","text":"ok"}}`)+
			strings.Replace(anthropicTail(), `"index":0`, `"index":999`, 1))
		flush()
	})
	s, e := anthropicStreamAdapter(t, base).DoStream(context.Background(), 1, body, llmproto.ProtocolAnthropic)
	if e != nil {
		t.Fatalf("under the cap: %v", e)
	}
	_ = s.Close()
}

// Empty text deltas before the first content are dropped: they neither count against the cap nor are
// replayed.
func TestPrefetchSkipsEmptyDeltas(t *testing.T) {
	var b strings.Builder
	b.WriteString(sse("message_start", msgStart))
	b.WriteString(sse("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
	for i := 0; i < 3*maxPrefetchEvents; i++ {
		b.WriteString(sse("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":""}}`))
	}
	b.WriteString(sse("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`))
	b.WriteString(anthropicTail())
	base := sseServer(t, func(w http.ResponseWriter, flush func()) {
		_, _ = io.WriteString(w, b.String())
		flush()
	})
	body := []byte(`{"model":"m-1","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`)
	s, e := anthropicStreamAdapter(t, base).DoStream(context.Background(), 1, body, llmproto.ProtocolAnthropic)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var types []string
	for {
		ev, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, string(ev.Type))
	}
	if got := strings.Join(types, ","); got != "message_start,block_start,block_delta,block_stop,message_delta,message_stop" {
		t.Fatalf("events %s", got)
	}
}
