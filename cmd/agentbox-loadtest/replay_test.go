package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplayFixedCorpus(t *testing.T) {
	captured := make(chan []byte, 8)
	var requests [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		captured <- body
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		time.Sleep(20 * time.Millisecond)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":2}}\n\n"+
			"data: [DONE]\n\n")
	}))
	defer server.Close()
	t.Setenv("AGENTBOX_REPLAY_TOKEN", "")
	corpus := filepath.Join(t.TempDir(), "prompts.json")
	if err := os.WriteFile(corpus, []byte(`[{"id":"one","group":"short","prompt":"p1"},{"id":"two","group":"long","prompt":"p2"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"direct", "gateway"} {
		var out, stderr bytes.Buffer
		if code := run([]string{"replay", "--base", server.URL + "/v1", "--corpus", corpus, "--model", "model",
			"--role", role, "--repeat", "2"}, &out, &stderr); code != 0 {
			t.Fatalf("replay %s: exit %d: %s", role, code, &stderr)
		}
		dec := json.NewDecoder(&out)
		for i, wantID := range []string{"one", "two", "one", "two"} {
			var row replaySample
			if err := dec.Decode(&row); err != nil {
				t.Fatal(err)
			}
			if row.Kind != "request" || row.Index != i || row.ID != wantID || row.Role != role ||
				row.TTFTMS == nil || row.Outcome != "200" || row.Usage == nil || row.Usage.Completion != 2 {
				t.Fatalf("unexpected sample: %+v", row)
			}
			requests = append(requests, <-captured)
			if row.SHA256 != fmt.Sprintf("%x", sha256.Sum256(requests[len(requests)-1])) {
				t.Fatalf("digest %s does not match replayed request %d", row.SHA256, i)
			}
		}
		var summary struct {
			Kind                  string  `json:"kind"`
			UsageSamples          int     `json:"usage_samples"`
			OutputTokensPerSecond float64 `json:"output_tokens_per_wall_second"`
		}
		if err := dec.Decode(&summary); err != nil || summary.Kind != "summary" || summary.UsageSamples != 4 ||
			summary.OutputTokensPerSecond <= 0 {
			t.Fatalf("summary %+v, error %v", summary, err)
		}
	}
	if len(requests) != 8 {
		t.Fatalf("requests: %d, want 8", len(requests))
	}
	for i := range 4 {
		if !bytes.Equal(requests[i], requests[i+4]) {
			t.Fatalf("request %d differs between endpoints", i)
		}
	}
}
