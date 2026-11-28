package postgres

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
)

// TestSingleAttemptCloudAdapter exposes only fixed fixture operations on loopback.
// A local operator starts and finishes the observation with files independent
// of the writer's transport. The writer has neither host nor database authority.
func TestSingleAttemptCloudAdapter(t *testing.T) {
	dir := os.Getenv("AGENTBOX_SINGLE_ATTEMPT_DIR")
	if dir == "" {
		t.Skip("opt-in two-node experiment")
	}
	ctx := t.Context()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	secret, err := os.ReadFile(os.Getenv("AGENTBOX_SINGLE_ATTEMPT_TOKEN_FILE"))
	must(err)
	token := strings.TrimSpace(string(secret))
	if len(token) < 32 {
		t.Fatal("test token too short")
	}
	ledger, err := os.OpenFile(filepath.Join(dir, "ledger.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	must(err)
	defer ledger.Close()
	record := func(event string, fields map[string]any) {
		row := map[string]any{"event": event, "at": time.Now().UTC()}
		for k, v := range fields {
			row[k] = v
		}
		must(json.NewEncoder(ledger).Encode(row))
		must(ledger.Sync())
	}
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	const id, old, next = "fence-one", "att-fence-one", "att2-fence-one"
	var db string
	must(s.pool.QueryRow(ctx, "select current_database()").Scan(&db))
	must(os.WriteFile(filepath.Join(dir, "dbname"), []byte(db+"\n"), 0600))
	fixture(t, s, id)
	mkblob := func() *blob.Local { b, e := blob.NewLocal(t.TempDir()); must(e); return b }
	b, err := blob.NewLocal(t.TempDir())
	must(err)
	services := chainServices{s}
	coord, err := call.New(call.Config{Store: s, Blobs: b, Events: services})
	must(err)
	t.Cleanup(coord.Close)
	m, err := workspace.New(workspace.Config{Dir: t.TempDir(), Blobs: b, Access: coord, Exec: coord, Tasks: services, Archive: &chainArchive{objects: mkblob()}})
	must(err)
	t.Cleanup(m.Close)
	pgstate := func() map[string]any {
		var owner string
		var expiry, now time.Time
		must(s.pool.QueryRow(ctx, "SELECT current_attempt_id,lease_expires_at,clock_timestamp() FROM tasks WHERE task_id=$1", id).Scan(&owner, &expiry, &now))
		return map[string]any{"owner": owner, "lease_expiry": expiry.UTC(), "pg_clock": now.UTC(), "task": id, "adapter_pid": os.Getpid()}
	}
	stamp := func() map[string]any {
		fields := pgstate()
		state, e := s.LoadTask(ctx, id)
		must(e)
		fields["checkpoint"] = state.Latest.CheckpointID
		head, e := m.Snapshot(ctx, id)
		must(e)
		fields["head"] = head
		r, e := m.Read(ctx, workspace.Request{TaskID: id, AttemptID: next, Body: []byte(`{"path":"cursor.txt"}`)})
		must(e)
		var file struct{ Content string }
		must(json.Unmarshal(r.Body, &file))
		if r.Status != 200 || file.Content != "successor" {
			t.Fatalf("successor file invalid status=%d", r.Status)
		}
		h := sha256.Sum256([]byte(file.Content))
		fields["content_sha256"] = hex.EncodeToString(h[:])
		p, e := s.GetTerminalProposal(ctx, next)
		must(e)
		fields["result_ref"] = p.Ref
		return fields
	}
	var mu sync.Mutex
	phase := "old"
	started := false
	initial := pgstate()
	record("old_owner", initial)
	current := initial
	counts := map[string]int{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"phase": phase, "started": started, "old_attempt": old, "new_attempt": next, "pg": current})
	})
	mux.HandleFunc("POST /write/{op}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		op := r.PathValue("op")
		opid := r.Header.Get("X-Operation-ID")
		if len(opid) > 100 {
			http.Error(w, "invalid operation id", 400)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 || r.Header.Get("X-Attempt-ID") != old {
			http.Error(w, "invalid fixture identity", 403)
			return
		}
		status, layer, code := 409, "adapter", "phase_not_new"
		if phase == "new" {
			var failure error
			layer = "pg"
			switch op {
			case "renew":
				failure = s.RenewAttemptLease(ctx, id, old)
			case "checkpoint":
				_, failure = s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: id}, AttemptID: old, CheckpointID: "late", StepID: "step", State: []byte(`{"owner":"old"}`)})
			case "final_result":
				_, failure = s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: old, Kind: "result", Ref: "late-result"})
			case "file_head":
				layer = "gateway_workspace_access"
				var response call.Result
				response, failure = m.Write(ctx, workspace.Request{TaskID: id, AttemptID: old, Body: []byte(`{"path":"cursor.txt","content":"late-writer"}`)})
				status, code = response.Status, response.Code
			default:
				http.NotFound(w, r)
				return
			}
			if failure != nil {
				var rejected *persistence.RejectedError
				if errors.As(failure, &rejected) {
					code = rejected.Code
					status = 409
				} else {
					code = "store_error"
					status = 500
				}
			} else if layer == "pg" {
				status, code = 200, "accepted"
			}
		}
		outcome := "rejected"
		if status >= 500 {
			outcome = "error"
		} else if status < 300 {
			outcome = "accepted"
		}
		fields := map[string]any{"operation": op, "operation_id": opid, "attempt": old, "phase": phase, "layer": layer, "code": code, "outcome": outcome, "accepted": outcome == "accepted", "status": status}
		if phase == "new" {
			counts[op+"/"+outcome]++
		}
		record("write", fields)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(fields)
	})
	port := 19489
	if value := os.Getenv("AGENTBOX_SINGLE_ATTEMPT_PORT"); value != "" {
		port, err = strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			t.Fatal("AGENTBOX_SINGLE_ATTEMPT_PORT must be in the range 1..65535")
		}
	}
	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	must(err)
	httpServer := &http.Server{Handler: mux, ReadTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	go func() { _ = httpServer.Serve(l) }()
	defer httpServer.Close()
	exists := func(name string) bool { _, e := os.Stat(filepath.Join(dir, name)); return e == nil }
	until := time.Now().Add(12 * time.Minute)
	last := time.Now()
	for !exists("start") {
		if time.Now().After(until) {
			t.Fatal("start timed out")
		}
		if time.Since(last) > 5*time.Second {
			must(s.RenewAttemptLease(ctx, id, old))
			last = time.Now()
		}
		time.Sleep(200 * time.Millisecond)
	}
	must(s.RenewAttemptLease(ctx, id, old))
	mu.Lock()
	started = true
	current = pgstate()
	record("last_old_renew", current)
	mu.Unlock()
	for {
		expired, e := s.ExpiredAttemptLease(ctx, id, old)
		must(e)
		if expired {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	mu.Lock()
	record("expired", pgstate())
	mu.Unlock()
	retryWithNewAttempt(t, s, id)
	r, err := m.Write(ctx, workspace.Request{TaskID: id, AttemptID: next, Body: []byte(`{"path":"cursor.txt","content":"successor"}`)})
	must(err)
	if r.Status != 200 {
		t.Fatalf("new owner write %d", r.Status)
	}
	head, err := m.Snapshot(ctx, id)
	must(err)
	_, err = s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: id}, AttemptID: next, CheckpointID: "current", StepID: "step", State: []byte(`{"owner":"new"}`), WorkspaceManifestSHA256: head})
	must(err)
	_, err = s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: next, Kind: "result", Ref: "successor-result"})
	must(err)
	mu.Lock()
	current = stamp()
	before := current
	record("new_owner", before)
	phase = "new"
	mu.Unlock()
	deadline := time.Now().Add(100 * time.Second)
	for !exists("finish") && time.Now().Before(deadline) {
		time.Sleep(150 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	phase = "done"
	after := stamp()
	record("final", after)
	for _, key := range []string{"owner", "checkpoint", "head", "content_sha256", "result_ref", "lease_expiry"} {
		if before[key] != after[key] {
			t.Errorf("changed: %s", key)
		}
	}
	for _, op := range []string{"renew", "checkpoint", "file_head", "final_result"} {
		if counts[op+"/rejected"] == 0 || counts[op+"/accepted"]+counts[op+"/error"] != 0 {
			t.Errorf("invalid evidence for %s: %v", op, counts)
		}
	}
	t.Log(fmt.Sprintf("writer receipts %v", counts))
}
