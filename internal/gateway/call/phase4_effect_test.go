package call

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// The fixture sends the counter request once the coordinator has staged that shell
// invocation. Other calls use the ordinary test provider and the same journal.
type counterExecEnv struct {
	*fakeEnvs
	counterURL string
}

func (e *counterExecEnv) Start(ctx context.Context, id string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	command, err := os.ReadFile(filepath.Join(e.env(id).inDir, ".agentbox", "cmd.sh"))
	if err != nil {
		return nil, err
	}
	if string(command) != "increment-test-counter" {
		return e.fakeEnvs.Start(ctx, id, spec)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.counterURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err == nil {
		resp.Body.Close()
		return nil, fmt.Errorf("counter unexpectedly acknowledged the request")
	}
	return nil, provider.ErrControlLost
}

func TestConcurrentShellReplayAfterCounterReplyLoss(t *testing.T) {
	var effects atomic.Int32
	applied := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}
		effects.Add(1)
		select {
		case applied <- struct{}{}:
		default:
		}
		<-release
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("disconnect: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer srv.Close()
	defer unblock()
	h := newExecHarness(t, func(cfg *ExecConfig) {
		cfg.HoldUnknown = true
		cfg.Envs = &counterExecEnv{fakeEnvs: cfg.Envs.(*fakeEnvs), counterURL: srv.URL}
	}, execScript{}, execScript{stdout: "7\n"})
	count := sinv("effect", "increment-test-counter")
	type outcome struct {
		res Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := h.c.ExecShell(context.Background(), count)
		done <- outcome{res, err}
	}()
	select {
	case <-applied:
	case <-time.After(5 * time.Second):
		t.Fatal("counter was never applied")
	}
	safe := sinv("sum", "echo 7")
	saved := h.shell(t, safe)
	if got := parseExecResult(t, saved); got.Exit == nil || got.Exit.Code != 0 || got.Stdout != "7\n" {
		t.Fatalf("independent call did not finish: %+v", got)
	}
	unblock()
	select {
	case ended := <-done:
		if ended.err != nil || ended.res.Code != CodeExecUnknown {
			t.Fatalf("lost response produced %v, %s", ended.err, ended.res.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("counter call failed to settle")
	}
	count.Retry = true
	if retry := h.shell(t, count); retry.Code != CodeExecUnknown {
		t.Fatalf("unresolved side effect retry: %+v", retry)
	}
	safe.Retry = true
	replay := h.shell(t, safe)
	if replay.Status != saved.Status || string(replay.Body) != string(saved.Body) || len(h.es.tries("t1", "sum")) != 1 || len(h.es.tries("t1", "effect")) != 1 || effects.Load() != 1 {
		t.Fatal("replay changed a result, allocated another try or duplicated the counter")
	}
	t.Log("gateway-layer: 2 distinct call IDs, sum completed during effect; sum execution tries=1, identical replay; counter effects=1, tries=1, retry=exec_unknown")
}
