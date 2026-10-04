package invariants_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/invariants"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// env 是一个完成安装的测试数据库、Store、BlobStore 与一条直连（用于构造违反的状态）。
type env struct {
	t     *testing.T
	store *postgres.Store
	db    *pgx.Conn
	blobs *blob.Local
	root  string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	admin := os.Getenv("AGENTBOX_TEST_DATABASE_URL")
	if admin == "" {
		if os.Getenv("CI") == "true" {
			t.Fatal("CI 中必须设置 AGENTBOX_TEST_DATABASE_URL")
		}
		t.Skip("未设置 AGENTBOX_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_inv_" + hex.EncodeToString(b)
	ac, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ac.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	_ = ac.Close(ctx)
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			_ = c.Close(context.Background())
		}
	})
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	s, err := postgres.Open(ctx, postgres.Options{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.InitializeInstallation(ctx, "install-inv", make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteInstallation(ctx, "install-inv"); err != nil {
		t.Fatal(err)
	}
	db, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	root := t.TempDir()
	blobs, err := blob.NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, store: s, db: db, blobs: blobs, root: root}
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

// fixture 建立任务 t1 与其 attempt，保存一个产物（内容在 BlobStore 中）并提交引用它的 checkpoint。
func (e *env) fixture() blob.Ref {
	e.t.Helper()
	ctx := context.Background()
	if _, err := e.store.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1",
		Spec: json.RawMessage(`{}`), MaxFaultRetries: 3}); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.store.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"}); err != nil {
		e.t.Fatal(err)
	}
	ref, err := e.blobs.Put(ctx, bytes.NewReader([]byte("report body")))
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.store.RegisterArtifact(ctx, runner.Artifact{TaskID: "t1", AttemptID: "a1", ArtifactID: "report",
		SHA256: ref.SHA256, Size: ref.Size, MediaType: "text/plain", Visibility: "output"}); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.store.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1",
		AttemptID: "a1", StepID: "s1", State: json.RawMessage(`{}`), Refs: []string{ref.SHA256}}); err != nil {
		e.t.Fatal(err)
	}
	return ref
}

type scanner provider.ScanReport

func (s scanner) Scan(context.Context) (provider.ScanReport, error) {
	return provider.ScanReport(s), nil
}

func (e *env) verify(scan scanner, quiescent bool) []invariants.Violation {
	e.t.Helper()
	vs, err := invariants.Verify(context.Background(), e.store, scan, e.blobs, quiescent)
	if err != nil {
		e.t.Fatal(err)
	}
	return vs
}

func ids(vs []invariants.Violation) string {
	var out []string
	for _, v := range vs {
		out = append(out, v.ID+"/"+v.Class)
	}
	return strings.Join(out, ",")
}

// TestCleanStatePasses：正常的库与资源状态没有任何违反（含 Q 类）。
func TestCleanStatePasses(t *testing.T) {
	e := newEnv(t)
	e.fixture()
	if vs := e.verify(scanner{}, true); len(vs) != 0 {
		t.Fatalf("正常状态不应有违反：%+v", vs)
	}
}

// TestEachInvariantIsReported：每条不变量构造一个违反的状态，断言被报告且类别正确。
func TestEachInvariantIsReported(t *testing.T) {
	cases := []struct {
		name, want string
		breakIt    func(e *env, ref blob.Ref)
		scan       scanner
		quiescent  bool
	}{
		{"I1 已清理环境仍有资源", "I1/Q", func(e *env, _ blob.Ref) {
			e.exec("UPDATE environments SET stopped_at = now(), cleanup_state = 'done' WHERE env_id = 'e1'")
		}, scanner{Items: []provider.ScanItem{{Layer: "cgroup", Path: "agentbox-x/env-e1", EnvID: "e1", Owner: provider.OwnedPartial}}}, true},
		{"I2 两个可能拥有执行树的 attempt", "I2/A", func(e *env, _ blob.Ref) {
			e.exec("INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ('a2', 't1', 2, 'e2', 'starting')")
			e.exec("INSERT INTO environments (env_id, kind, attempt_id, status) VALUES ('e2', 'task', 'a2', 'creating')")
		}, scanner{}, false},
		{"I4 task_seq 有空洞", "I4/A", func(e *env, _ blob.Ref) {
			e.exec("DELETE FROM events WHERE task_id = 't1' AND task_seq = 2")
		}, scanner{}, false},
		{"I5 产物内容被改写（checkpoint 也引用它，故同时违反 I6）", "I5/A,I6/A", func(e *env, ref blob.Ref) {
			path := filepath.Join(e.root, "sha256", ref.SHA256[:2], ref.SHA256[2:])
			if err := os.WriteFile(path, []byte("tampered!!!"), 0o600); err != nil {
				e.t.Fatal(err)
			}
		}, scanner{}, false},
		{"I6 指针回退", "I6/A", func(e *env, _ blob.Ref) {
			e.exec("UPDATE task_progress SET latest_commit_seq = 0, latest_checkpoint_id = NULL WHERE task_id = 't1'")
		}, scanner{}, false},
		{"I6 引用未授权且不存在的 blob（授权与内容各报一次）", "I6/A,I6/A", func(e *env, _ blob.Ref) {
			e.exec("UPDATE checkpoints SET refs_json = '[\"" + strings.Repeat("f", 64) + "\"]' WHERE checkpoint_id = 'cp1'")
		}, scanner{}, false},
		{"I7 两个 active 访问", "I7/A", func(e *env, _ blob.Ref) {
			e.exec("INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ('a2', 't1', 2, 'e2', 'ended')")
			e.exec("INSERT INTO attempt_access (attempt_id, task_id, state) VALUES ('a2', 't1', 'active')")
		}, scanner{}, false},
		{"I8 隔离资源超期未报警", "I8/B", func(e *env, _ blob.Ref) {
			e.exec("INSERT INTO quarantined_resources (resource_path, kind, reason, detected_at) VALUES ('envs/x', 'env_dir', '无 owner.json', now() - interval '2 minutes')")
		}, scanner{}, false},
		{"I16 请求对应的资源不存在", "I16/A", func(e *env, _ blob.Ref) {
			e.exec("INSERT INTO api_requests (request_id, kind, body_hash, resource_id, response) VALUES ('r9', 'create_task', 'x', 'missing', 'null')")
		}, scanner{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			ref := e.fixture()
			c.breakIt(e, ref)
			if got := ids(e.verify(c.scan, c.quiescent)); got != c.want {
				t.Fatalf("应只报告 %s，得到 %q", c.want, got)
			}
		})
	}
}

// TestQuiescentOnlyAndAlertClears：Q 类只在静止时检查；隔离资源报警后不再违反 I8。
func TestQuiescentOnlyAndAlertClears(t *testing.T) {
	e := newEnv(t)
	e.fixture()
	e.exec("UPDATE environments SET stopped_at = now(), cleanup_state = 'done' WHERE env_id = 'e1'")
	scan := scanner{Items: []provider.ScanItem{{Layer: "env_dir", Path: "envs/e1", EnvID: "e1", Owner: provider.OwnedPartial}}}
	if vs := e.verify(scan, false); len(vs) != 0 {
		t.Fatalf("非静止时不应检查 Q 类：%+v", vs)
	}
	ctx := context.Background()
	if err := e.store.RecordQuarantine(ctx, resource.Quarantine{Layer: "env_dir", Path: "envs/x", Reason: "无 owner.json"}); err != nil {
		t.Fatal(err)
	}
	e.exec("UPDATE quarantined_resources SET detected_at = now() - interval '2 minutes'")
	if got := ids(e.verify(scanner{}, false)); got != "I8/B" {
		t.Fatalf("超期未报警应违反 I8，得到 %q", got)
	}
	if err := e.store.MarkQuarantineAlerted(ctx, "envs/x"); err != nil {
		t.Fatal(err)
	}
	if vs := e.verify(scanner{}, false); len(vs) != 0 {
		t.Fatalf("报警后不应再违反：%+v", vs)
	}
}
