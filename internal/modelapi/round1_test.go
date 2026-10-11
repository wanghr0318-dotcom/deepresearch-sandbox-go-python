package modelapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// Review m1: a panic while serving still settles the hold, records the request and releases the stream slot.
func TestPanicStillFinishes(t *testing.T) {
	var calls atomic.Int32
	one := int64(1)
	e := newEnv(t, modeOpenAI, func(c *Config) {
		c.Now = func() time.Time {
			if calls.Add(1) == 2 { // the first try's start in run
				panic("injected")
			}
			return time.Now()
		}
	})
	e.limits.ls = map[quota.Level]quota.Limits{quota.LevelKey: {MaxStreams: &one}}
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+PathChatCompletions,
		strings.NewReader(chatBody(llmproto.ProtocolOpenAI, "m-1", true, "")))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
	}
	r := e.record()
	if r.Status != http.StatusInternalServerError || e.holder.get(r.HoldID).state == quota.StateHeld {
		t.Fatalf("record %+v hold %+v", r, e.holder.get(r.HoldID))
	}
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))
	if readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("the stream slot was not released: %d", resp.StatusCode)
	}
	e.record()
}

// Review m2: with both max_tokens and max_completion_tokens only the effective (smaller) one is forwarded; a request
// without either gets the default; the input estimate is bytes ÷ 2 + 1024.
func TestMaxTokensForwarding(t *testing.T) {
	for _, c := range []struct {
		body    string
		want    int64
		mt, mct bool
	}{
		{`{"model":"m","max_tokens":100,"max_completion_tokens":50,"messages":[{"role":"user","content":"hi"}]}`, 50, false, true},
		{`{"model":"m","max_tokens":40,"max_completion_tokens":50,"messages":[{"role":"user","content":"hi"}]}`, 40, true, false},
		{`{"model":"m","max_completion_tokens":70,"messages":[{"role":"user","content":"hi"}]}`, 70, false, true},
		{`{"model":"m","messages":[{"role":"user","content":"hi"}]}`, 4096, true, false},
	} {
		req, err := llmproto.DecodeRequest(llmproto.ProtocolOpenAI, []byte(c.body))
		if err != nil {
			t.Fatal(err)
		}
		out, eff, err := withMaxTokens([]byte(c.body), req, 4096)
		if err != nil {
			t.Fatal(err)
		}
		var obj map[string]json.RawMessage
		_ = json.Unmarshal(out, &obj)
		_, mt := obj["max_tokens"]
		_, mct := obj["max_completion_tokens"]
		if eff != c.want || mt != c.mt || mct != c.mct {
			t.Fatalf("%s → %s (eff %d)", c.body, out, eff)
		}
	}
	if inputEstimate(make([]byte, 1000)) != 1524 {
		t.Fatal("input estimate")
	}
}

// Review m2: when the known actual cost exceeds the hold, the actual is settled and the overshoot is counted.
func TestOvershootSettledAtActual(t *testing.T) {
	_, rec := obstest.Install(t)
	e := newEnv(t, modeOpenAI)
	e.p.SetChatHook(func([]byte) json.RawMessage {
		return json.RawMessage(`{"role":"assistant","content":"` + strings.Repeat("long ", 4000) + `"}`) // ≫ max_tokens 1
	})
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, `"max_tokens":1,`))
	if readAll(t, resp); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	r := e.record()
	h := e.holder.get(r.HoldID)
	if h.state != quota.StateSettled || h.charged <= h.amount || r.CostMicro != h.charged {
		t.Fatalf("hold %+v record %d", h, r.CostMicro)
	}
	if !rec.Has("ModelHoldOvershoot protocol=openai") {
		t.Fatalf("metrics %v", rec.Events())
	}
}

// flakyStore fails the first n writes.
type flakyStore struct {
	*fakeStore
	fail atomic.Int32
}

func (f *flakyStore) RecordModelRequest(ctx context.Context, r Record) error {
	if f.fail.Add(-1) >= 0 {
		return errors.New("db blip")
	}
	return f.fakeStore.RecordModelRequest(ctx, r)
}

// Review m3: recording is retried; a row that still cannot be written is counted.
func TestRecordRetried(t *testing.T) {
	_, rec := obstest.Install(t)
	fs := &flakyStore{fakeStore: &fakeStore{ch: make(chan Record, 4)}}
	fs.fail.Store(2)
	e := newEnv(t, modeOpenAI, func(c *Config) { c.Store = fs })
	e.store = fs.fakeStore
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	readAll(t, resp)
	e.record() // written on the third attempt
	fs.fail.Store(5)
	resp = e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	readAll(t, resp)
	deadline := time.Now().Add(5 * time.Second)
	for !rec.Has("ModelRecordFailure") {
		if time.Now().After(deadline) {
			t.Fatalf("no record failure metric: %v", rec.Events())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Review m5: the rpm limit is taken before the body is read: a caller over its limit gets 429 even for a body that
// would be invalid.
func TestRPMBeforeBody(t *testing.T) {
	zero := int64(0)
	e := newEnv(t, modeOpenAI)
	e.limits.ls = map[quota.Level]quota.Limits{quota.LevelKey: {RPM: &zero}}
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, `not json at all`)
	if code, _ := errorOf(t, llmproto.ProtocolOpenAI, readAll(t, resp)); resp.StatusCode != 429 || code != "rate_limit_exceeded" {
		t.Fatalf("%d %s", resp.StatusCode, code)
	}
	_ = fakeupstream.Chat
}
