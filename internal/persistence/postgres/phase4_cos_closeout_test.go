//go:build cos_integration

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
)

type closeoutCOS struct {
	*blob.COSArchive
	shortKey            string
	putBytes, shortRead int64
	puts                int
}

func (a *closeoutCOS) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if key == a.shortKey {
		source := &io.LimitedReader{R: r, N: 1}
		err := a.COSArchive.Put(ctx, key, source, size)
		a.shortRead = 1 - source.N
		return err
	}
	err := a.COSArchive.Put(ctx, key, r, size)
	if err == nil {
		a.putBytes += size
		a.puts++
	}
	return err
}

// persistThenInterrupt injects loss of control between durable blob storage and workspace publication.
type persistThenInterrupt struct {
	*blob.Local
	interrupt bool
	saved     string
}

func (b *persistThenInterrupt) Put(ctx context.Context, r io.Reader) (blob.Ref, error) {
	ref, err := b.Local.Put(ctx, r)
	if b.interrupt && err == nil {
		b.saved = ref.SHA256
		return blob.Ref{}, io.ErrUnexpectedEOF
	}
	return ref, err
}

func cosDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// Uses one disposable PG database per case, and a new subprefix for each invocation.
// The interruption points are deterministic application/SDK boundaries on a live COS path.
func TestCOSCheckpointCloseout(t *testing.T) {
	const ttl = 120 * time.Second
	ctx := t.Context()
	prefix := os.Getenv("AGENTBOX_COS_PREFIX")
	if prefix == "" || os.Getenv("AGENTBOX_TEST_DATABASE_URL") == "" {
		t.Fatal("dedicated COS prefix and PG admin DSN required")
	}
	prefix = fmt.Sprintf("%s/closeout-%d", prefix, time.Now().UnixNano())
	for _, tc := range []struct {
		name        string
		files, size int
	}{{"partial-upload", 1, 4096}, {"before-pg", 1, 4096}, {"small", 1, 4096}, {"large", 64, 512 << 10}} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := blob.NewCOSArchive(os.Getenv("AGENTBOX_COS_BUCKET_URL"), prefix+"/"+tc.name, blob.COSCredentials{CVMRole: os.Getenv("AGENTBOX_COS_CVM_ROLE")})
			if err != nil {
				t.Fatal(err)
			}
			backend := &closeoutCOS{COSArchive: base}
			s := newStore(t, Options{AttemptLeaseTTL: ttl})
			const taskID = "t1"
			gwFixture(t, s, taskID, 1000)
			active := "att-t1"
			fresh := func() (*workspace.Manager, *persistThenInterrupt) {
				t.Helper()
				files, err := blob.NewLocal(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				b := &persistThenInterrupt{Local: files}
				svc := chainServices{s}
				coord, err := call.New(call.Config{Store: s, Blobs: files, Events: svc})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(coord.Close)
				m, err := workspace.New(workspace.Config{Dir: t.TempDir(), Blobs: b, Access: coord, Exec: coord, Tasks: svc, Archive: backend})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(m.Close)
				return m, b
			}
			m, local := fresh()
			write := func(index int, val string) error {
				r, err := json.Marshal(map[string]string{"path": fmt.Sprintf("f%d", index), "content": val})
				if err != nil {
					return err
				}
				res, err := m.Write(ctx, workspace.Request{TaskID: taskID, AttemptID: active, Body: r})
				if err == nil && res.Status != 200 {
					return fmt.Errorf("write %d: %s", res.Status, res.Code)
				}
				return err
			}
			content := func(index int, rev string) string {
				mark := fmt.Sprintf("file-%02d-%s-", index, rev)
				return mark + strings.Repeat("x", tc.size-len(mark))
			}
			for i := 0; i < tc.files; i++ {
				if err := write(i, content(i, "initial")); err != nil {
					t.Fatal(err)
				}
			}
			cursor := json.RawMessage(`{"last_tool":"call-1","budget_micro":900}`)
			beginCall(t, s, taskID, "call-1")
			tr := mustReserve(t, s, taskID, "call-1", 100)
			original := settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 100, ResultSHA256: cosDigest("recorded"), ResultSize: 8})
			commit := func(id, sha string) {
				t.Helper()
				_, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: taskID}, AttemptID: active, CheckpointID: id, StepID: "step", State: cursor, WorkspaceManifestSHA256: sha})
				if err != nil {
					t.Fatal(err)
				}
			}
			snapshot := func() (string, time.Duration, error) {
				// Read PG time on both sides and renew inside the actual snapshot while it is running.
				until := func() time.Time {
					var v time.Time
					if err := s.pool.QueryRow(ctx, "SELECT lease_expires_at FROM tasks WHERE task_id=$1", taskID).Scan(&v); err != nil {
						t.Fatal(err)
					}
					return v
				}
				before := until()
				done := make(chan struct{})
				type renewals struct {
					count int
					err   error
				}
				joined := make(chan renewals, 1)
				go func() {
					ticker := time.NewTicker(2 * time.Second)
					defer ticker.Stop()
					var result renewals
					for {
						select {
						case <-done:
							joined <- result
							return
						case <-ticker.C:
							result.err = s.RenewAttemptLease(ctx, taskID, active)
							if result.err != nil {
								joined <- result
								return
							}
							result.count++
						}
					}
				}()
				start := time.Now()
				sha, err := m.Snapshot(ctx, taskID)
				elapsed := time.Since(start)
				close(done)
				result := <-joined
				if result.err != nil {
					t.Fatal(result.err)
				}
				after := until()
				if result.count > 0 && !after.After(before) {
					t.Fatal("renewal failed to extend lease")
				}
				t.Logf("snapshot manifest=%s elapsed=%s successful_renewals=%d PG_before=%s PG_after=%s", sha, elapsed, result.count, before.UTC().Format(time.RFC3339Nano), after.UTC().Format(time.RFC3339Nano))
				return sha, elapsed, err
			}
			c1, full, err := snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if backend.puts != tc.files+1 {
				t.Fatal("unexpected full upload count", backend.puts)
			}
			fullBytes := backend.putBytes
			fullPuts := backend.puts
			commit("C1", c1)
			next := content(0, "changed")
			if tc.name == "partial-upload" {
				local.interrupt = true
				if err := write(0, next); !errors.Is(err, io.ErrUnexpectedEOF) || local.saved != cosDigest(next) {
					t.Fatalf("candidate persistence fault: %v %s", err, local.saved)
				}
				local.interrupt = false
				same, err := m.Snapshot(ctx, taskID)
				if err != nil || same != c1 {
					t.Fatalf("candidate leaked into workspace: %s %v", same, err)
				}
				t.Logf("candidate=%s locally durable; hot manifest unchanged=%s", local.saved, same)
			}
			if err := write(0, next); err != nil {
				t.Fatal(err)
			}
			wantHead, wantManifest := "C1", c1
			if tc.name == "partial-upload" {
				backend.shortKey = cosDigest(next)
			}
			c2, delta, deltaErr := snapshot()
			if tc.name == "partial-upload" {
				if deltaErr == nil || c2 != "" || backend.shortRead != 1 {
					t.Fatalf("short PUT did not interrupt: %q %d %v", c2, backend.shortRead, deltaErr)
				}
				body, err := backend.Get(ctx, cosDigest(next))
				if body != nil {
					body.Close()
				}
				if !errors.Is(err, blob.ErrNotFound) {
					t.Fatalf("incomplete object visible: %v", err)
				}
				t.Logf("interrupted real COS PUT: expected_size=%d reader_bytes=1 GET=not_found", tc.size)
			} else {
				if deltaErr != nil || c2 == c1 || backend.puts-fullPuts != 2 {
					t.Fatalf("invalid delta: %v %s puts=%d", deltaErr, c2, backend.puts-fullPuts)
				}
				if tc.name != "before-pg" {
					commit("C2", c2)
					wantHead, wantManifest = "C2", c2
				}
			}
			retryWithNewAttempt(t, s, taskID)
			active = "att2-t1"
			st, err := s.LoadTask(ctx, taskID)
			if err != nil || st.Latest == nil || st.Latest.CheckpointID != wantHead || st.Latest.WorkspaceManifestSHA256 != wantManifest {
				t.Fatalf("wrong PG head: %+v %v", st.Latest, err)
			}
			var remembered map[string]any
			if err := json.Unmarshal(st.Latest.State, &remembered); err != nil || remembered["last_tool"] != "call-1" || remembered["budget_micro"] != float64(900) {
				t.Fatalf("cursor: %v %v", remembered, err)
			}
			m, local = fresh()
			start := time.Now()
			if err := m.Restore(ctx, taskID, wantManifest); err != nil {
				t.Fatal(err)
			}
			restored := time.Since(start)
			for i := 0; i < tc.files; i++ {
				rev := "initial"
				if i == 0 && wantHead == "C2" {
					rev = "changed"
				}
				want := content(i, rev)
				file, err := local.Open(cosDigest(want))
				if err != nil {
					t.Fatal(err)
				}
				h := sha256.New()
				n, err := io.Copy(h, file)
				file.Close()
				if err != nil || n != int64(tc.size) || hex.EncodeToString(h.Sum(nil)) != cosDigest(want) {
					t.Fatal("wrong restored body", i, n, err)
				}
				body := []byte(fmt.Sprintf(`{"path":"f%d","max_lines":1}`, i))
				r, err := m.Read(ctx, workspace.Request{TaskID: taskID, AttemptID: active, Body: body})
				var w struct{ Content, SHA256 string }
				if err != nil || r.Status != 200 || json.Unmarshal(r.Body, &w) != nil || w.SHA256 != cosDigest(want) || len(w.Content) == 0 || !strings.HasPrefix(want, w.Content) {
					t.Fatalf("bad file mapping %d: %d %v", i, r.Status, err)
				}
			}
			replay, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: taskID, CallID: "call-1", AttemptID: active, Fingerprint: "fp-call-1", Endpoint: "chat", Deadline: 120 * time.Second})
			if err != nil || !replay.Existing || replay.Record.ResultRef != original.ResultRef || original.ResultRef != cosDigest("recorded") || count(t, s, "select count(*) from call_tries where task_id=$1 and call_id='call-1'", taskID) != 1 {
				t.Fatalf("journal replay: %v %v", replay.Existing, err)
			}
			t.Logf("case=%s files=%d file_bytes=%d full_put_bytes=%d full_puts=%d delta_put_bytes=%d delta_puts=%d full=%s delta=%s restore=%s PG_head=%s restored_manifest=%s verified_files=%d original_call_tries=1", tc.name, tc.files, tc.size, fullBytes, fullPuts, backend.putBytes-fullBytes, backend.puts-fullPuts, full, delta, restored, wantHead, wantManifest, tc.files)
		})
	}
}
