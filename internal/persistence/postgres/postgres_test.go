package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// ---- 基础：测试数据库、安装引导（E46）、事务辅助（E12、争用、未知提交）、失锁（E13） ----

// 事务语义只在真实 PostgreSQL 上测试（代码组织设计 §4.2）。本地未设置
// AGENTBOX_TEST_DATABASE_URL 时跳过；CI 中未设置则失败，不允许静默跳过。
const dsnEnv = "AGENTBOX_TEST_DATABASE_URL"

func adminDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		if os.Getenv("CI") == "true" {
			t.Fatalf("CI 中必须设置 %s", dsnEnv)
		}
		t.Skipf("未设置 %s，跳过 PostgreSQL 测试", dsnEnv)
	}
	return dsn
}

// newDatabase 建立一个独立的临时数据库并在测试结束时删除，返回其 DSN。
func newDatabase(t *testing.T) string {
	t.Helper()
	admin := adminDSN(t)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_test_" + hex.EncodeToString(b)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("连接测试数据库: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("创建测试数据库: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// newStore 返回已完成安装引导的 Store。
func newStore(t *testing.T, opt Options) *Store {
	t.Helper()
	if opt.DSN == "" {
		opt.DSN = newDatabase(t)
	}
	s, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.InitializeInstallation(context.Background(), "install-test"); err != nil {
		t.Fatalf("初始化: %v", err)
	}
	if err := s.CompleteInstallation(context.Background(), "install-test"); err != nil {
		t.Fatalf("完成安装: %v", err)
	}
	return s
}

func count(t *testing.T, s *Store, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// TestInstallationBootstrapE46 覆盖规格 E46：空库首启、三个中断点、以及三种拒绝启动的情况。
func TestInstallationBootstrapE46(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T) (*Store, datadir.IDFile) {
		s, err := Open(ctx, Options{DSN: newDatabase(t)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s, datadir.NewIDFile(t.TempDir())
	}
	var seq atomic.Int64
	newID := func() string { return fmt.Sprintf("install-%d", seq.Add(1)) }
	finish := func(t *testing.T, s *Store, f datadir.IDFile, want string) {
		t.Helper()
		id, err := ownership.Bootstrap(ctx, s, f, newID)
		if err != nil || (want != "" && id != want) {
			t.Fatalf("重新引导得到 (%q, %v)，期望 %q", id, err, want)
		}
		st, _ := s.InspectInstallation(ctx)
		fid, _, _ := f.Read()
		if st.Installation == nil || !st.Installation.Complete || st.Installation.InstallID != id || fid != id {
			t.Fatalf("引导后状态不一致：%+v 文件 %q", st.Installation, fid)
		}
	}
	refused := func(t *testing.T, s *Store, f datadir.IDFile) {
		t.Helper()
		if _, err := ownership.Bootstrap(ctx, s, f, newID); !errors.Is(err, ownership.ErrRefused) {
			t.Fatalf("应拒绝启动，得到 %v", err)
		}
	}

	t.Run("空库首启", func(t *testing.T) {
		s, f := open(t)
		finish(t, s, f, "")
	})
	t.Run("初始迁移提交前中断", func(t *testing.T) {
		s, f := open(t)
		s.hooks.beforeCommit = func(string) error { return errors.New("模拟中断") }
		if _, err := ownership.Bootstrap(ctx, s, f, newID); err == nil {
			t.Fatal("中断应使引导失败")
		}
		s.hooks.beforeCommit = nil
		if st, _ := s.InspectInstallation(ctx); st.HasMigrations || st.HasAgentboxTables {
			t.Fatalf("事务未提交时库应仍为空：%+v", st)
		}
		finish(t, s, f, "")
	})
	t.Run("提交后、写身份文件前中断", func(t *testing.T) {
		s, f := open(t)
		if err := s.InitializeInstallation(ctx, "install-a"); err != nil {
			t.Fatal(err)
		}
		finish(t, s, f, "install-a")
	})
	t.Run("写身份文件后、置 complete 前中断", func(t *testing.T) {
		s, f := open(t)
		if err := s.InitializeInstallation(ctx, "install-b"); err != nil {
			t.Fatal(err)
		}
		if err := f.Write("install-b"); err != nil {
			t.Fatal(err)
		}
		finish(t, s, f, "install-b")
	})
	t.Run("有 schema 无 installation 记录", func(t *testing.T) {
		s, f := open(t)
		if err := s.InitializeInstallation(ctx, "install-c"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, "DELETE FROM installation"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, f)
	})
	t.Run("身份不一致", func(t *testing.T) {
		s, f := open(t)
		finish(t, s, f, "")
		if err := f.Write("install-other"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, f)
	})
	t.Run("未知 schema", func(t *testing.T) {
		s, f := open(t)
		if _, err := s.pool.Exec(ctx, "CREATE TABLE tasks (task_id text)"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, f)
	})
}

// probeTable 建立一张只供事务辅助测试使用的表，含 id 为 1、2 的两行。
func probeTable(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		"CREATE TABLE probe (id int PRIMARY KEY, v text NOT NULL DEFAULT ''); INSERT INTO probe (id) VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
}

func lockProbe(ctx context.Context, tx pgx.Tx, id int) error {
	_, err := tx.Exec(ctx, "SELECT 1 FROM probe WHERE id = $1 FOR UPDATE", id)
	return err
}

// TestDeadlockIsRetried 覆盖 E12 的存储部分：两事务经屏障违反锁顺序形成环，
// 被中止的一方重跑，两者最终都提交。
func TestDeadlockIsRetried(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{OpDeadline: 10 * time.Second, LockTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second})
	probeTable(t, s)
	var barrier sync.WaitGroup
	barrier.Add(2)
	var attempts atomic.Int32
	lockBoth := func(first, second int) error {
		var round atomic.Int32
		return s.run(ctx, "deadlock", fmt.Sprint(first), func(ctx context.Context, tx pgx.Tx) error {
			attempts.Add(1)
			if err := lockProbe(ctx, tx, first); err != nil {
				return err
			}
			if round.Add(1) == 1 {
				barrier.Done()
				barrier.Wait() // 两边都持有第一把锁后再去拿第二把
			}
			return lockProbe(ctx, tx, second)
		})
	}
	errs := make(chan error, 2)
	go func() { errs <- lockBoth(1, 2) }()
	go func() { errs <- lockBoth(2, 1) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("死锁应被检测并重跑，得到 %v", err)
		}
	}
	if attempts.Load() < 3 {
		t.Fatalf("应有一方被中止后重跑（尝试次数 %d）", attempts.Load())
	}
}

// TestContentionAndUnknownDeadline：持有行锁使操作在 deadline 内得到 ErrContention（不计入
// 故障阈值）；提交结果始终未知时，deadline 用尽返回携带身份的 CommitUnknownError。
func TestContentionAndUnknownDeadline(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{OpDeadline: 800 * time.Millisecond, LockTimeout: 200 * time.Millisecond, StatementTimeout: 500 * time.Millisecond})
	probeTable(t, s)

	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, "SELECT 1 FROM probe WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	err = s.run(ctx, "contended", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
	_ = holder.Rollback(ctx)
	if !errors.Is(err, persistence.ErrContention) || persistence.CountsTowardFailureThreshold(err) {
		t.Fatalf("锁被持有时应为 ErrContention 且不计入故障阈值，得到 %v", err)
	}

	s.hooks.afterCommit = func(string) error { return errors.New("模拟：回复始终丢失") }
	err = s.run(ctx, "insert-probe", "probe-3", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO probe (id) VALUES (3) ON CONFLICT DO NOTHING")
		return err
	})
	var unknown *persistence.CommitUnknownError
	if !errors.As(err, &unknown) || unknown.Identity != "probe-3" || !persistence.CountsTowardFailureThreshold(err) {
		t.Fatalf("应返回携带身份的 CommitUnknownError，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM probe WHERE id = 3"); n != 1 {
		t.Fatalf("幂等的重跑只应留下一行，得到 %d", n)
	}
}

// TestOwnershipLossCancelsOperations 覆盖 E13 的存储部分：终止锁连接后检测到失锁，
// 在途数据库操作被取消，新操作被拒绝；记录检测延迟。
func TestOwnershipLossCancelsOperations(t *testing.T) {
	ctx := context.Background()
	dsn := newDatabase(t)
	own, err := AcquireOwnership(ctx, dsn, OwnershipOptions{CheckInterval: 200 * time.Millisecond, CheckTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = own.Close() }()
	if _, err := AcquireOwnership(ctx, dsn, OwnershipOptions{}); !errors.Is(err, ErrAlreadyOwned) {
		t.Fatalf("第二个实例应取不到 advisory lock，得到 %v", err)
	}
	s := newStore(t, Options{DSN: dsn, Ownership: own, OpDeadline: 10 * time.Second, LockTimeout: 9 * time.Second, StatementTimeout: 9 * time.Second})
	probeTable(t, s)

	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, "SELECT 1 FROM probe WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	inflight := make(chan error, 1)
	go func() {
		inflight <- s.run(ctx, "inflight", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
	}()

	var pid int
	if err := s.pool.QueryRow(ctx, "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-own.Lost():
		t.Logf("失锁检测延迟 %v", time.Since(start))
	case <-time.After(5 * time.Second):
		t.Fatal("5 s 内没有检测到失锁")
	}
	select {
	case err := <-inflight:
		if !errors.Is(err, persistence.ErrOwnershipLost) {
			t.Fatalf("在途操作应以 ErrOwnershipLost 结束，得到 %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("在途操作没有被取消")
	}
	if _, err := s.InspectInstallation(ctx); !errors.Is(err, persistence.ErrOwnershipLost) {
		t.Fatalf("失锁后新操作应被拒绝，得到 %v", err)
	}
}
