package modelapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

type slowStore struct {
	Store
	d time.Duration
}

func (s slowStore) RecordModelRequest(ctx context.Context, r Record) error {
	time.Sleep(s.d)
	return s.Store.RecordModelRequest(ctx, r)
}

// A healthy reply that completes shortly before the maximum duration, followed by a slow finish (PostgreSQL slow):
// the response bytes written after the handler returns hit the static write deadline.
func TestRereviewHealthyReplyNearMaxDuration(t *testing.T) {
	for _, stream := range []bool{false, true} {
		e := newEnv(t, modeOpenAI, func(c *Config) {
			c.MaxDuration = 1500 * time.Millisecond
			c.Store = slowStore{Store: c.Store, d: 3 * time.Second}
		})
		e.p.SetChunkDelay(fakeupstream.Chat, 150*time.Millisecond)
		e.p.SetChatHook(func([]byte) json.RawMessage {
			return json.RawMessage(`{"role":"assistant","content":"a short reply in a few chunks"}`)
		})
		t0 := time.Now()
		var b []byte
		st := 0
		resp, err := http.Post(e.srv.URL+PathChatCompletions, "application/json", strings.NewReader(chatBody(llmproto.ProtocolOpenAI, "m-1", stream, "")))
		if err == nil {
			st = resp.StatusCode
			b, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		r := e.record()
		t.Logf("record status %d code %q latency %v ttfb %v; client done after %v", r.Status, r.ErrorCode, r.Latency, r.TTFB, time.Since(t0))
		t.Logf("stream=%v status %d complete=%v code=%q read err=%v body tail=%q", stream, st, r.StreamComplete, r.ErrorCode, err, tail(string(b)))
		if err != nil || (!stream && !strings.Contains(string(b), "chat.completion")) {
			t.Errorf("BUG stream=%v: a completed reply did not reach the client cleanly: err=%v", stream, err)
		}
	}
}

func tail(s string) string {
	if len(s) > 80 {
		return s[len(s)-80:]
	}
	return s
}

var _ = http.MethodPost
