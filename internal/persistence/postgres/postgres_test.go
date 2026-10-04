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
		fired := false
		s.hooks.beforeCommit = func(string) error { fired = true; return errors.New("模拟中断") }
		if _, err := ownership.Bootstrap(ctx, s, f, newID); err == nil || !fired {
			t.Fatalf("中断应使引导失败（钩子触发 %v），得到 %v", fired, err)
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

// holdProbe 在独立事务中持有 probe 行锁，测试结束时回滚。
func holdProbe(t *testing.T, s *Store, id int) {
	t.Helper()
	ctx := context.Background()
	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(ctx) })
	if err := lockProbe(ctx, holder, id); err != nil {
		t.Fatal(err)
	}
}

// waitLockWait 等到当前数据库中有会话阻塞在行锁上，确认在途操作确实停在锁等待中。
func waitLockWait(t *testing.T, s *Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for count(t, s, `SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND wait_event_type = 'Lock'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("5 s 内在途操作没有阻塞在行锁上")
		}
		time.Sleep(10 * time.Millisecond)
	}
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

	holdProbe(t, s, 1)
	inflight := make(chan error, 1)
	go func() {
		inflight <- s.run(ctx, "inflight", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
	}()
	waitLockWait(t, s)

	// 只取本测试数据库中的锁连接：其他并行测试的库里也可能有同键的 advisory lock。
	var pid int
	if err := s.pool.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-own.Lost():
		latency, bound := time.Since(start), 2*(200*time.Millisecond+200*time.Millisecond)
		t.Logf("失锁检测延迟 %v", latency)
		if latency >= bound {
			t.Fatalf("失锁检测延迟 %v 超过 2*(CheckInterval+CheckTimeout) = %v", latency, bound)
		}
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
	if err := s.Migrate(ctx); !errors.Is(err, persistence.ErrOwnershipLost) {
		t.Fatalf("失锁后迁移应被拒绝，得到 %v", err)
	}
	if err := own.Close(); err != nil {
		t.Fatalf("关闭锁连接: %v", err)
	}
	if err := own.Close(); err != nil { // 幂等；随后 defer 中的第三次调用同样无害
		t.Fatalf("再次关闭应返回 nil，得到 %v", err)
	}
}

// TestErrorClassification 在真实 PostgreSQL 上核对事务辅助的错误归类与是否计入故障阈值。
func TestErrorClassification(t *testing.T) {
	ctx := context.Background()
	long := Options{OpDeadline: 10 * time.Second, LockTimeout: 9 * time.Second, StatementTimeout: 9 * time.Second}

	t.Run("事务中后端被终止", func(t *testing.T) {
		s := newStore(t, long)
		probeTable(t, s)
		pids, killed := make(chan int, 1), make(chan struct{})
		go func() {
			defer close(killed)
			pid := <-pids
			_, _ = s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
			// 等后端真正退出，事务体的下一条语句必然落在已终止的连接上。
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				var alive bool
				if err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&alive); err != nil || !alive {
					return
				}
			}
		}()
		var attempts atomic.Int32
		var firstErr error
		err := s.run(ctx, "terminated", "probe-3", func(ctx context.Context, tx pgx.Tx) error {
			n := attempts.Add(1)
			if n == 1 {
				var pid int
				if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
					return err
				}
				pids <- pid
				<-killed
			}
			_, err := tx.Exec(ctx, "INSERT INTO probe (id) VALUES (3) ON CONFLICT DO NOTHING")
			if n == 1 {
				firstErr = err
			}
			return err
		})
		if err != nil || attempts.Load() != 2 {
			t.Fatalf("被终止的尝试应重跑并成功：err %v，尝试 %d 次", err, attempts.Load())
		}
		// 通常是 FATAL 57P01；若客户端先看到套接字关闭，则是网络错误——两者都应按连接错误重跑。
		if !isConnection(firstErr) {
			t.Fatalf("第一次尝试应以连接错误（57P01 或套接字关闭）结束，得到 %v", firstErr)
		}
		if n := count(t, s, "SELECT count(*) FROM probe WHERE id = 3"); n != 1 {
			t.Fatalf("应只写入一行，得到 %d", n)
		}
	})

	t.Run("语句超时", func(t *testing.T) {
		s := newStore(t, Options{OpDeadline: 500 * time.Millisecond, LockTimeout: time.Second, StatementTimeout: 50 * time.Millisecond})
		var timeouts, attempts int
		var last error
		err := s.run(ctx, "slow", "1", func(ctx context.Context, tx pgx.Tx) error {
			attempts++
			_, last = tx.Exec(ctx, "SELECT pg_sleep(0.2)")
			if sqlState(last) == "57014" {
				timeouts++
			}
			return last
		})
		if !errors.Is(err, persistence.ErrUnavailable) || !persistence.CountsTowardFailureThreshold(err) {
			t.Fatalf("语句超时应为 ErrUnavailable 且计入故障阈值，得到 %v", err)
		}
		// 57014 在 deadline 内重跑；最后一次尝试可能被操作 deadline 截断，故只要求出现过 57014。
		if timeouts == 0 || attempts < 2 {
			t.Fatalf("语句超时应在 deadline 内重跑：尝试 %d 次，其中 57014 %d 次", attempts, timeouts)
		}
		// 底层错误以 %w 保留：最后一次尝试的错误，或它在事务体之前被 deadline 截断时的 context 错误。
		if !errors.Is(err, last) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("结果应以 %%w 保留最后一次的底层错误 %v，得到 %v", last, err)
		}
	})

	t.Run("事务体吞掉语句错误", func(t *testing.T) {
		s := newStore(t, long)
		var attempts atomic.Int32
		err := s.run(ctx, "swallow", "1", func(ctx context.Context, tx pgx.Tx) error {
			attempts.Add(1)
			_, _ = tx.Exec(ctx, "SELECT 1/0")
			return nil
		})
		if !errors.Is(err, errInvalid) || persistence.CountsTowardFailureThreshold(err) || attempts.Load() != 1 {
			t.Fatalf("应为 errInvalid、不计入阈值且只尝试一次：%v（尝试 %d 次）", err, attempts.Load())
		}
	})

	t.Run("COMMIT 时连接被终止", func(t *testing.T) {
		// 每次尝试在 COMMIT 前终止自己的后端：COMMIT 得到 FATAL 57P01，可能已到达服务端，
		// 只能视为结果未知；deadline 用尽时返回 CommitUnknownError。
		s := newStore(t, Options{OpDeadline: time.Second, LockTimeout: time.Second, StatementTimeout: time.Second})
		var pid int
		s.hooks.beforeCommit = func(string) error {
			_, _ = s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
				if count(t, s, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid) == 0 {
					break
				}
			}
			return nil
		}
		err := s.run(ctx, "commit-fatal", "1", func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
		})
		var unknown *persistence.CommitUnknownError
		if !errors.As(err, &unknown) || !persistence.CountsTowardFailureThreshold(err) {
			t.Fatalf("COMMIT 得到 FATAL 时应为 CommitUnknownError，得到 %v", err)
		}
		if !isConnection(unknown.Err) {
			t.Fatalf("未知提交应携带连接错误（57P01 或套接字关闭），得到 %v", unknown.Err)
		}
	})

	t.Run("COMMIT 前调用方取消", func(t *testing.T) {
		// context 已结束时 COMMIT 不会发出（SafeToRetry）：确定未提交，归为调用方取消而非未知。
		s := newStore(t, long)
		probeTable(t, s)
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		s.hooks.beforeCommit = func(string) error { cancel(); return nil }
		err := s.run(cctx, "cancel-before-commit", "probe-3", func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "INSERT INTO probe (id) VALUES (3) ON CONFLICT DO NOTHING")
			return err
		})
		if !errors.Is(err, context.Canceled) || errors.Is(err, persistence.ErrCommitUnknown) || persistence.CountsTowardFailureThreshold(err) {
			t.Fatalf("COMMIT 未发出时应为调用方取消、不是未知提交，得到 %v", err)
		}
		if n := count(t, s, "SELECT count(*) FROM probe WHERE id = 3"); n != 0 {
			t.Fatalf("未提交的事务不应留下行，得到 %d", n)
		}
	})

	t.Run("调用方取消", func(t *testing.T) {
		s := newStore(t, long)
		probeTable(t, s)
		holdProbe(t, s, 1)
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- s.run(cctx, "canceled", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
		}()
		waitLockWait(t, s)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) || errors.Is(err, persistence.ErrUnavailable) || persistence.CountsTowardFailureThreshold(err) {
				t.Fatalf("调用方取消应包装 context.Canceled、不是 ErrUnavailable、不计入阈值，得到 %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("取消后操作没有结束")
		}
	})
}

// TestCompositeForeignKeys：任务内引用使用复合外键，事件不能指向另一个任务的 attempt（规格 §6）。
func TestCompositeForeignKeys(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (task_id, spec_json, status, max_fault_retries) VALUES ('A', '{}', 'queued', 0), ('B', '{}', 'queued', 0);
		INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ('b-1', 'B', 1, 'env-b', 'running');
		INSERT INTO blobs (sha256, size) VALUES ('h', 1);
		INSERT INTO artifact_heads (task_id, artifact_id) VALUES ('A', 'out'), ('B', 'out')`); err != nil {
		t.Fatal(err)
	}
	event := func(taskID string) error {
		_, err := s.pool.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, source, type, payload, content_hash)
			VALUES ($1, 1, 'k', 'b-1', 'host', 'x', '{}', '\x00')`, taskID)
		return err
	}
	artifact := func(taskID, artifactID string) error {
		_, err := s.pool.Exec(ctx, `INSERT INTO artifacts (task_id, artifact_id, version, sha256, size, media_type, visibility, attempt_id)
			VALUES ($1, $2, 1, 'h', 1, 'text/plain', 'output', 'b-1')`, taskID, artifactID)
		return err
	}
	if err := event("A"); sqlState(err) != "23503" {
		t.Fatalf("任务 A 的事件引用任务 B 的 attempt 应违反外键，得到 %v", err)
	}
	if err := artifact("A", "out"); sqlState(err) != "23503" {
		t.Fatalf("任务 A 的产物引用任务 B 的 attempt 应违反外键，得到 %v", err)
	}
	if err := artifact("B", "missing"); sqlState(err) != "23503" {
		t.Fatalf("没有 artifact_heads 的产物应违反外键，得到 %v", err)
	}
	if err := event("B"); err != nil {
		t.Fatalf("同一任务内的引用应被接受，得到 %v", err)
	}
	if err := artifact("B", "out"); err != nil {
		t.Fatalf("同一任务内的引用应被接受，得到 %v", err)
	}
}
