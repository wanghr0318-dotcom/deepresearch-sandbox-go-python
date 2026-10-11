package postgres

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/modelapi"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
)

// undo0014 把库还原为只应用了 0013 的形状（undo0013 先执行它）。
const undo0014 = `DROP TABLE model_request_tries, model_requests; DROP FUNCTION agentbox_model_request_partitions(timestamptz);
	DROP INDEX quota_holds_model_request; DELETE FROM schema_migrations WHERE version >= 14;
`

// g3Violations 返回不变量 G3 的违反。
func g3Violations(t *testing.T, s *Store) []string {
	t.Helper()
	vs, err := s.DBViolations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, v := range vs {
		if v.ID == "G3" {
			out = append(out, v.Detail)
		}
	}
	return out
}

func modelRecord(w quotaWorld, id string, hold quota.HoldID, at time.Time) modelapi.Record {
	ttfb := 120 * time.Millisecond
	complete := true
	return modelapi.Record{ID: id, OrgID: w.org, UserID: w.user, KeyID: w.key, ClientProtocol: llmproto.ProtocolOpenAI,
		Model: "smart", Stream: true, Provider: "claude", ProviderProtocol: llmproto.ProtocolAnthropic, Converted: true,
		Status: 200, Usage: llmproto.Usage{InputTokens: 10, OutputTokens: 5, CacheReadTokens: 2, CacheWriteTokens: 1},
		UsageKnown: true, CostMicro: 77, TTFB: &ttfb, Latency: time.Second, StreamComplete: &complete,
		Dropped: []string{"messages[0].content[0].cache_control"}, HoldID: hold, CreatedAt: at,
		Tries: []modelapi.Try{
			{No: 1, Provider: "primary", Outcome: "retryable", Status: 503, Latency: 10 * time.Millisecond},
			{No: 2, Provider: "claude", Outcome: "ok", Status: 200, Latency: 900 * time.Millisecond, CostMicro: 77, Skipped: "x:circuit_open"},
		}}
}

// RecordModelRequest writes the row and its tries (idempotently), creating month partitions on demand; the operator
// (OrgID 0) is recorded under the reserved org; invariant G3 catches a row whose hold is still held, belongs to
// something else, or is shared.
func TestModelRequestStore(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	st := w.s.ModelRequests()
	deadline := time.Now().Add(time.Hour)
	hold, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "mr_1", 100, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.q.Settle(ctx, hold, 77, false); err != nil {
		t.Fatal(err)
	}
	r := modelRecord(w, "mr_1", hold, time.Now())
	for range 2 {
		if err := st.RecordModelRequest(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	var line string
	if err := w.s.pool.QueryRow(ctx, `SELECT format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', org_id, user_id, key_id, prompt_tokens,
			completion_tokens, cache_read_tokens, cost_micro, ttfb_ms, stream_complete, dropped_fields, converted)
		FROM model_requests WHERE id = 'mr_1'`).Scan(&line); err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{itoa(w.org), itoa(w.user), w.key, "13", "5", "2", "77", "120", "t", "{messages[0].content[0].cache_control}", "t"}, "|")
	if line != want {
		t.Fatalf("row %s, want %s", line, want)
	}
	if n := count(t, w.s, `SELECT count(*) FROM model_request_tries WHERE request_id = 'mr_1'`); n != 2 {
		t.Fatalf("tries %d", n)
	}
	if vs := g3Violations(t, w.s); len(vs) != 0 {
		t.Fatalf("G3: %v", vs)
	}

	// Another month (partition created on demand) and the operator.
	next := time.Now().AddDate(0, 3, 0)
	hold2, err := w.q.Reserve(ctx, quota.Scope{}, quota.SourceModelRequest, "mr_2", 10, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.q.Release(ctx, hold2); err != nil {
		t.Fatal(err)
	}
	r2 := modelRecord(w, "mr_2", hold2, next)
	r2.OrgID, r2.UserID, r2.KeyID, r2.Tries = 0, 0, "", nil
	if err := st.RecordModelRequest(ctx, r2); err != nil {
		t.Fatal(err)
	}
	if n := count(t, w.s, `SELECT count(*) FROM model_requests r JOIN orgs o ON o.id = r.org_id
		WHERE r.id = 'mr_2' AND o.name = 'operator' AND r.user_id IS NULL AND r.key_id IS NULL`); n != 1 {
		t.Fatal("operator row")
	}
	if n := count(t, w.s, `SELECT count(*) FROM pg_class WHERE relname = 'model_requests_`+next.UTC().Format("200601")+`'`); n != 1 {
		t.Fatal("partition for a later month not created")
	}
	if vs := g3Violations(t, w.s); len(vs) != 0 {
		t.Fatalf("G3: %v", vs)
	}

	// Violations: a held hold, a hold of another request, a shared hold.
	hold3, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "mr_3", 10, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RecordModelRequest(ctx, modelRecord(w, "mr_3", hold3, time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordModelRequest(ctx, modelRecord(w, "mr_4", hold, time.Now())); err != nil {
		t.Fatal(err)
	}
	vs := strings.Join(g3Violations(t, w.s), "\n")
	if !strings.Contains(vs, "mr_3：已完成的请求的 hold") || !strings.Contains(vs, "mr_4：hold "+string(hold)+" 属于 model_request mr_1") {
		t.Fatalf("G3 should report mr_3 and mr_4:\n%s", vs)
	}
	if err := w.q.Settle(ctx, hold3, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.pool.Exec(ctx, `DELETE FROM model_requests WHERE id = 'mr_4'`); err != nil {
		t.Fatal(err)
	}
	if vs := g3Violations(t, w.s); len(vs) != 0 {
		t.Fatalf("G3 after fix: %v", vs)
	}
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
