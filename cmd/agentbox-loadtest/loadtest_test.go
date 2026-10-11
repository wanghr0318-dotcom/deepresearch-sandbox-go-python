package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, c := range []struct{ p, want float64 }{{50, 5}, {95, 10}, {99, 10}, {10, 1}, {11, 2}} {
		if got := percentile(v, c.p); got != c.want {
			t.Errorf("p%v = %v, want %v", c.p, got, c.want)
		}
	}
	if percentile(nil, 50) != 0 {
		t.Error("empty input")
	}
	d := distOf([]time.Duration{3 * time.Millisecond, time.Millisecond, 2 * time.Millisecond})
	if d.N != 3 || d.P50 != 2 || d.Max != 3 || d.Mean != 2 {
		t.Errorf("distOf = %+v", d)
	}
}

func TestSummarizeCountsOutcomes(t *testing.T) {
	rs := []result{
		{Status: 200, Total: time.Millisecond},
		{Status: 200, Stream: true, Complete: true, TTFT: time.Millisecond, Total: 2 * time.Millisecond},
		{Status: 200, Stream: true, Canceled: true, TTFT: time.Millisecond},
		{Status: 200, Stream: true, ErrEvent: true},
		{Status: 429, Code: "insufficient_quota"},
		{Code: "transport"},
	}
	s := summarize(rs, time.Second)
	if s.Requests != 6 || s.OK != 3 || s.RPS != 6 || s.Total.N != 2 || s.TTFT.N != 2 {
		t.Fatalf("summary = %+v", s)
	}
	want := "200×2 200 client_cancelled×1 200 stream_error_event×1 429 insufficient_quota×1 transport×1"
	if got := s.outcomesString(); got != want {
		t.Errorf("outcomes = %q, want %q", got, want)
	}
	if r := s.errorRate(); r != 0.5 {
		t.Errorf("error rate = %v", r)
	}
}

func TestErrorInfo(t *testing.T) {
	for _, c := range []struct{ body, code string }{
		{`{"error":{"message":"m","type":"rate_limit_exceeded","code":"rate_limit_exceeded"}}`, "rate_limit_exceeded"},
		{`{"error":{"message":"insufficient quota: the monthly budget at the org level is exhausted","type":"insufficient_quota","code":"insufficient_quota"}}`, "insufficient_quota"},
		{`{"type":"error","error":{"type":"rate_limit_error","message":"insufficient quota: the monthly budget at the key level is exhausted"}}`, "insufficient_quota"},
		{`{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, "rate_limit_error"},
		{`{"code":"forbidden","message":"x"}`, "forbidden"},
		{`not json`, ""},
	} {
		if got, _ := errorInfo([]byte(c.body)); got != c.code {
			t.Errorf("errorInfo(%s) = %q, want %q", c.body, got, c.code)
		}
	}
}

// fakeSSE serves a fixed SSE body.
func fakeSSE(t *testing.T, body string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestSendStreams(t *testing.T) {
	ctx := context.Background()
	hc := newHTTPClient(1)
	oa := fakeSSE(t, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	r := send(ctx, hc, target{OpenAIURL: oa}, reqSpec{Proto: OpenAI, Model: "m", Stream: true, MaxTokens: 1, Prompt: "p", Key: "k"})
	if !r.ok() || !r.Complete || r.TTFT <= 0 || r.ErrEvent {
		t.Errorf("openai stream: %+v", r)
	}
	oaErr := fakeSSE(t, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"error\":{\"message\":\"x\"}}\n\n")
	r = send(ctx, hc, target{OpenAIURL: oaErr}, reqSpec{Proto: OpenAI, Stream: true})
	if r.ok() || !r.ErrEvent || r.outcome() != "200 stream_error_event" {
		t.Errorf("openai error event: %+v %s", r, r.outcome())
	}
	an := fakeSSE(t, "event: message_start\ndata: {}\n\nevent: content_block_delta\ndata: {}\n\nevent: message_stop\ndata: {}\n\n")
	r = send(ctx, hc, target{AnthropicURL: an}, reqSpec{Proto: Anthropic, Stream: true})
	if !r.ok() || !r.Complete || r.TTFT <= 0 {
		t.Errorf("anthropic stream: %+v", r)
	}
	cut := fakeSSE(t, "event: content_block_delta\ndata: {}\n\n")
	r = send(ctx, hc, target{AnthropicURL: cut}, reqSpec{Proto: Anthropic, Stream: true})
	if r.ok() || r.outcome() != "200 stream_incomplete" {
		t.Errorf("cut anthropic stream: %+v %s", r, r.outcome())
	}
	r = send(ctx, hc, target{AnthropicURL: an}, reqSpec{Proto: Anthropic, Stream: true, CancelAfter: true})
	if !r.ok() || !r.Canceled || r.Complete {
		t.Errorf("abandoned stream: %+v", r)
	}
}

func TestRequestBodies(t *testing.T) {
	s := reqSpec{Proto: OpenAI, Model: "m", Stream: true, MaxTokens: 7, Prompt: "p"}
	if b := string(s.body()); !strings.Contains(b, `"include_usage":true`) || !strings.Contains(b, `"stream":true`) {
		t.Errorf("openai stream body %s", b)
	}
	s.Proto = Anthropic
	if b := string(s.body()); strings.Contains(b, "stream_options") || !strings.Contains(b, `"max_tokens":7`) {
		t.Errorf("anthropic body %s", b)
	}
	c := config{promptBytes: 64}
	if n := len(c.prompt(3, 4)); n != 64 {
		t.Errorf("prompt length %d", n)
	}
	ps, err := parsePaths("oa>an,an>oa")
	if err != nil || len(ps) != 2 || ps[0].String() != "oa>an" || ps[1].provider != OpenAI {
		t.Errorf("parsePaths = %v, %v", ps, err)
	}
	if _, err := parsePaths("oa-an"); err == nil {
		t.Error("parsePaths accepted oa-an")
	}
	if h, err := parseHook("15s=docker stop x"); err != nil || h.At != 15*time.Second || h.Cmd != "docker stop x" {
		t.Errorf("parseHook = %+v, %v", h, err)
	}
}

func TestOpenLoopRate(t *testing.T) {
	rs, skipped, el := openLoop(context.Background(), 200, 250*time.Millisecond, 8, func(context.Context, reqSpec) result {
		return result{Status: 200}
	}, func(int, int) reqSpec { return reqSpec{} })
	if len(rs) < 45 || len(rs) > 51 || skipped != 0 || el < 240*time.Millisecond {
		t.Errorf("open loop: %d results, %d skipped, %s", len(rs), skipped, el)
	}
}

func TestRunUsage(t *testing.T) {
	var out, errb strings.Builder
	if code := run(nil, &out, &errb); code != 2 || !strings.Contains(errb.String(), "usage") {
		t.Errorf("no args: %d %q", code, errb.String())
	}
	errb.Reset()
	if code := run([]string{"latency"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "--token-file") {
		t.Errorf("missing flags: %d %q", code, errb.String())
	}
}
