package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
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
	if err := s.InitializeInstallation(context.Background(), "install-test", make([]byte, ownership.TokenSize)); err != nil {
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

// TestInstallationBootstrapE46 覆盖规格 E46（含引导令牌修订）：空库首启、每个崩溃窗口、提交结果未知时
// 本次启动失败而下次启动恢复、另一个数据目录不能接管 pending、令牌丢失与损坏、旧 pending 记录，以及
// 三种拒绝启动的情况。
func TestInstallationBootstrapE46(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T) (*Store, string) {
		s, err := Open(ctx, Options{DSN: newDatabase(t)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s, t.TempDir()
	}
	var seq atomic.Int64
	newID := func() string { return fmt.Sprintf("install-%d", seq.Add(1)) }
	newToken := func() ([]byte, error) {
		b := make([]byte, ownership.TokenSize)
		_, err := rand.Read(b)
		return b, err
	}
	boot := func(s *Store, dir string) (string, error) {
		return ownership.Bootstrap(ctx, s, datadir.NewIDFile(dir), datadir.NewTokenFile(dir), newID, newToken)
	}
	finish := func(t *testing.T, s *Store, dir, want string) {
		t.Helper()
		id, err := boot(s, dir)
		if err != nil || (want != "" && id != want) {
			t.Fatalf("重新引导得到 (%q, %v)，期望 %q", id, err, want)
		}
		st, _ := s.InspectInstallation(ctx)
		fid, _, _ := datadir.NewIDFile(dir).Read()
		if st.Installation == nil || !st.Installation.Complete || st.Installation.InstallID != id || fid != id {
			t.Fatalf("引导后状态不一致：%+v 文件 %q", st.Installation, fid)
		}
	}
	refused := func(t *testing.T, s *Store, dir string) {
		t.Helper()
		if _, err := boot(s, dir); !errors.Is(err, ownership.ErrRefused) {
			t.Fatalf("应拒绝启动，得到 %v", err)
		}
	}
	token := func(t *testing.T, dir string) string {
		t.Helper()
		b, exists, err := datadir.NewTokenFile(dir).Read()
		if err != nil || !exists {
			t.Fatalf("令牌应已持久化：exists=%v err=%v", exists, err)
		}
		return string(b)
	}
	// pendingUnknown 让初始化事务真正提交而回复丢失：本次引导必须失败（结束本次启动），库中留下 pending。
	pendingUnknown := func(t *testing.T, s *Store, dir string) string {
		t.Helper()
		s.hooks.afterCommit = func(op string) error {
			if op == "InitializeInstallation" {
				return errors.New("模拟：COMMIT 已执行但回复丢失")
			}
			return nil
		}
		defer func() { s.hooks.afterCommit = nil }()
		if id, err := boot(s, dir); !errors.Is(err, persistence.ErrCommitUnknown) || id != "" {
			t.Fatalf("初始化提交结果未知时本次启动应失败且不返回 install_id，得到 (%q, %v)", id, err)
		}
		st, _ := s.InspectInstallation(ctx)
		if st.Installation == nil || st.Installation.Complete {
			t.Fatalf("库中应留下 pending 记录：%+v", st.Installation)
		}
		return st.Installation.InstallID
	}

	t.Run("空库首启", func(t *testing.T) {
		s, dir := open(t)
		finish(t, s, dir, "")
	})
	t.Run("令牌已持久、初始化事务提交前中断：重启复用同一令牌", func(t *testing.T) {
		s, dir := open(t)
		fired := false
		s.hooks.beforeCommit = func(string) error { fired = true; return errors.New("模拟中断") }
		if _, err := boot(s, dir); err == nil || !fired {
			t.Fatalf("中断应使引导失败（钩子触发 %v），得到 %v", fired, err)
		}
		s.hooks.beforeCommit = nil
		if st, _ := s.InspectInstallation(ctx); st.HasMigrations || st.HasAgentboxTables {
			t.Fatalf("事务未提交时库应仍为空：%+v", st)
		}
		before := token(t, dir)
		finish(t, s, dir, "")
		if token(t, dir) != before {
			t.Fatal("重启应复用已持久化的令牌，而不是生成新令牌")
		}
	})
	t.Run("初始化提交结果未知：本次启动失败，下次启动恢复", func(t *testing.T) {
		s, dir := open(t)
		id := pendingUnknown(t, s, dir)
		finish(t, s, dir, id)
	})
	t.Run("另一个数据目录不能接管 pending 安装", func(t *testing.T) {
		s, dirA := open(t)
		id := pendingUnknown(t, s, dirA)
		dirB := t.TempDir()
		refused(t, s, dirB)
		if st, _ := s.InspectInstallation(ctx); st.Installation == nil || st.Installation.Complete || st.Installation.InstallID != id {
			t.Fatalf("被拒绝后库不应改变：%+v", st.Installation)
		}
		if _, exists, _ := datadir.NewIDFile(dirB).Read(); exists {
			t.Fatal("被拒绝的数据目录不应写入 install_id")
		}
		if _, exists, _ := datadir.NewTokenFile(dirB).Read(); exists {
			t.Fatal("被拒绝的数据目录不应生成令牌")
		}
		finish(t, s, dirA, id)
	})
	t.Run("本数据目录的令牌丢失：拒绝", func(t *testing.T) {
		s, dir := open(t)
		pendingUnknown(t, s, dir)
		if err := os.Remove(dir + "/bootstrap_token"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, dir)
	})
	t.Run("令牌损坏：启动失败而不是当作首次安装", func(t *testing.T) {
		s, dir := open(t)
		if err := os.WriteFile(dir+"/bootstrap_token", []byte("not-a-token\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if id, err := boot(s, dir); !errors.Is(err, datadir.ErrTokenCorrupt) || id != "" {
			t.Fatalf("应因令牌损坏失败，得到 (%q, %v)", id, err)
		}
		if st, _ := s.InspectInstallation(ctx); st.HasMigrations || st.HasAgentboxTables {
			t.Fatalf("令牌损坏时不应初始化数据库：%+v", st)
		}
	})
	t.Run("写身份文件后、置 complete 前中断", func(t *testing.T) {
		s, dir := open(t)
		id := pendingUnknown(t, s, dir)
		if err := datadir.NewIDFile(dir).Write(id); err != nil {
			t.Fatal(err)
		}
		finish(t, s, dir, id)
	})
	t.Run("旧 pending 记录（修订前，无令牌哈希列）", func(t *testing.T) {
		s, dir := open(t)
		if err := s.InitializeInstallation(ctx, "install-legacy", make([]byte, ownership.TokenSize)); err != nil {
			t.Fatal(err)
		}
		// 还原为只应用了 0001 的旧库
		if _, err := s.pool.Exec(ctx, `DROP TABLE call_tries, reservations, calls, budgets;
			ALTER TABLE installation DROP COLUMN bootstrap_token_hash; DELETE FROM schema_migrations WHERE version >= 2`); err != nil {
			t.Fatal(err)
		}
		if st, err := s.InspectInstallation(ctx); err != nil || st.Installation == nil || st.Installation.TokenHash != nil {
			t.Fatalf("旧库应读作无令牌哈希：%+v %v", st.Installation, err)
		}
		refused(t, s, dir) // 无身份文件：无法证明由本目录发起
		if err := datadir.NewIDFile(dir).Write("install-legacy"); err != nil {
			t.Fatal(err)
		}
		finish(t, s, dir, "install-legacy") // 身份文件一致：置 complete
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if n := count(t, s, "SELECT count(*) FROM schema_migrations WHERE version IN (2, 3)"); n != 2 {
			t.Fatalf("引导完成后应应用 0002 与 0003，得到 %d", n)
		}
	})
	t.Run("库中的令牌哈希必须为 32 字节", func(t *testing.T) {
		s, dir := open(t)
		finish(t, s, dir, "")
		if _, err := s.pool.Exec(ctx, `UPDATE installation SET bootstrap_token_hash = '\x0102'`); sqlState(err) != "23514" {
			t.Fatalf("长度不对的令牌哈希应违反检查约束，得到 %v", err)
		}
	})
	t.Run("有 schema 无 installation 记录", func(t *testing.T) {
		s, dir := open(t)
		if err := s.InitializeInstallation(ctx, "install-c", make([]byte, ownership.TokenSize)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, "DELETE FROM installation"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, dir)
	})
	t.Run("身份不一致", func(t *testing.T) {
		s, dir := open(t)
		finish(t, s, dir, "")
		if err := datadir.NewIDFile(dir).Write("install-other"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, dir)
	})
	t.Run("未知 schema", func(t *testing.T) {
		s, dir := open(t)
		if _, err := s.pool.Exec(ctx, "CREATE TABLE tasks (task_id text)"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, dir)
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

// ---- api 与 task 用例：未知提交（E11a）、首次提交未完成、同身份不同内容、重放与读取 ----

// useCase 是一次幂等事务用例调用及其"只有一份结果"的检查。
type useCase struct {
	op     string
	setup  func(t *testing.T, s *Store)
	call   func(s *Store) (any, error)
	unique string // 应恰有 1 行的计数查询
}

// checkCommitLost 覆盖 E11a：COMMIT 真正执行而客户端收到连接错误。用例必须在同一 deadline
// 内以同一身份重跑并得到原结果；重复调用返回同一结果；只有一份结果。
func checkCommitLost(t *testing.T, cases []useCase) {
	for _, uc := range cases {
		t.Run(uc.op, func(t *testing.T) {
			s := newStore(t, Options{})
			uc.setup(t, s)
			var lost atomic.Bool
			s.hooks.afterCommit = func(op string) error {
				if op == uc.op && lost.CompareAndSwap(false, true) {
					return errors.New("模拟：COMMIT 已执行但回复丢失")
				}
				return nil
			}
			got, err := uc.call(s)
			if err != nil {
				t.Fatalf("提交回复丢失后应解析为成功，得到 %v", err)
			}
			if !lost.Load() {
				t.Fatal("故障钩子没有触发")
			}
			s.hooks.afterCommit = nil
			again, err := uc.call(s)
			if err != nil || fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", got) {
				t.Fatalf("重复调用应返回原结果：%+v / %v，原为 %+v", again, err, got)
			}
			if n := count(t, s, uc.unique); n != 1 {
				t.Fatalf("%s 应恰有 1 行，得到 %d", uc.unique, n)
			}
		})
	}
}

// expectConflicts 断言每个调用都返回 persistence.ErrConflict。
func expectConflicts(t *testing.T, cases map[string]func() error) {
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, persistence.ErrConflict) {
				t.Fatalf("应为冲突，得到 %v", err)
			}
		})
	}
}

// fixture 建立一个任务及其第一次 attempt（att-<taskID>，环境 env-<taskID>）。
func fixture(t *testing.T, s *Store, taskID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-" + taskID, BodyHash: []byte("h"), TaskID: taskID,
		Spec: json.RawMessage(`{"worker":"sim"}`), MaxFaultRetries: 3}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: "att-" + taskID, AttemptNo: 1, EnvID: "env-" + taskID}); err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
}

// verdict 是 att-<taskID> 在 control_version 1（desired = run）下的失败判决。
func verdict(taskID string, exit int64) task.Verdict {
	return task.Verdict{AttemptID: "att-" + taskID, TaskID: taskID, ControlVersion: 1, FromStatus: "starting", AttemptStatus: "ended",
		OutcomeClass: "worker_error", ExitCode: &exit, TaskStatus: "failed", TaskStatusReason: "worker_error",
		EventType: "attempt_ended", EventPayload: json.RawMessage(fmt.Sprintf(`{"exit":%d}`, exit))}
}

func withFixture(t *testing.T, s *Store) { fixture(t, s, "t1") }

// stopEnv 模拟 Reconciler 确认环境已停止（stopped_at 由 resource 用例写入，Task 7 才实现）。
func stopEnv(t *testing.T, s *Store, envID string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), "UPDATE environments SET stopped_at = now() WHERE env_id = $1", envID); err != nil {
		t.Fatal(err)
	}
}

// retryWithNewAttempt 走一次故障重试：att-<taskID> 裁决回到 queued，旧环境停止，创建第 2 个 attempt。
func retryWithNewAttempt(t *testing.T, s *Store, taskID string) {
	t.Helper()
	ctx := context.Background()
	v := verdict(taskID, 1)
	v.TaskStatus = "queued"
	if _, err := s.FinalizeAttempt(ctx, v); err != nil {
		t.Fatalf("故障重试的判决: %v", err)
	}
	stopEnv(t, s, "env-"+taskID)
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: "att2-" + taskID, AttemptNo: 2, EnvID: "env2-" + taskID}); err != nil {
		t.Fatalf("第 2 个 attempt: %v", err)
	}
}

// expectRejected 断言 err 是原因码为 code 的 *persistence.RejectedError。
func expectRejected(t *testing.T, err error, code string) {
	t.Helper()
	var rej *persistence.RejectedError
	if !errors.As(err, &rej) || rej.Code != code || !errors.Is(err, persistence.ErrRejected) || persistence.CountsTowardFailureThreshold(err) {
		t.Fatalf("应以 %s 拒绝，得到 %v", code, err)
	}
}

// snapshot 记录任务的可观察状态，用于确认被拒绝的写入没有留下任何改变。
func snapshot(t *testing.T, s *Store, taskID string) string {
	t.Helper()
	var out string
	if err := s.pool.QueryRow(context.Background(), `SELECT concat_ws('|', t.status, t.current_attempt_id, t.row_version,
			t.applied_control_version, t.status_reason, c.desired, c.control_version, c.reason,
			p.latest_checkpoint_id, p.latest_commit_seq,
			(SELECT count(*) FROM events e WHERE e.task_id = t.task_id),
			(SELECT count(*) FROM attempts a WHERE a.task_id = t.task_id),
			(SELECT count(*) FROM attempts a WHERE a.task_id = t.task_id AND a.verdict_hash IS NOT NULL),
			(SELECT string_agg(concat_ws(':', a.attempt_id, a.status, a.outcome_class), ',' ORDER BY a.attempt_no)
				FROM attempts a WHERE a.task_id = t.task_id),
			(SELECT string_agg(x.state, ',' ORDER BY x.attempt_id) FROM attempt_access x WHERE x.task_id = t.task_id),
			(SELECT count(*) FROM environments v JOIN attempts a USING (attempt_id) WHERE a.task_id = t.task_id),
			(SELECT count(*) FROM api_requests q WHERE q.resource_id = t.task_id),
			(SELECT count(*) FROM artifacts r WHERE r.task_id = t.task_id))
		FROM tasks t JOIN task_control c USING (task_id) JOIN task_progress p USING (task_id) WHERE t.task_id = $1`, taskID).Scan(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestCreateAttemptAdmission 覆盖规格 §8.1 创建 attempt 的准入：前置条件在同一事务内检查，
// 被拒绝时任务不变；已创建的 attempt 重复请求时返回原结果（即使前置条件此时已不成立）。
func TestCreateAttemptAdmission(t *testing.T) {
	ctx := context.Background()
	t.Run("取消后创建", func(t *testing.T) {
		s := newStore(t, Options{})
		if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"}); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"})
		expectRejected(t, err, persistence.CodeNotRunnable)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
	t.Run("旧环境未停止时创建，停止后创建，重复请求", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		v := verdict("t1", 1)
		v.TaskStatus = "queued"
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		req := task.NewAttempt{TaskID: "t1", AttemptID: "att2-t1", AttemptNo: 2, EnvID: "env2-t1"}
		before := snapshot(t, s, "t1")
		_, err := s.CreateAttempt(ctx, req)
		expectRejected(t, err, persistence.CodePreviousNotStopped)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		stopEnv(t, s, "env-t1")
		first, err := s.CreateAttempt(ctx, req)
		if err != nil {
			t.Fatalf("旧环境停止后应能创建：%v", err)
		}
		again, err := s.CreateAttempt(ctx, req) // 任务已是 running，准入不再成立，但这是同一请求
		if err != nil || fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", first) {
			t.Fatalf("重复请求应返回原结果：%+v %v，原为 %+v", again, err, first)
		}
		if v, _ := s.GetTask(ctx, "t1"); v.Status != "running" || v.CurrentAttemptID != "att2-t1" || v.AttemptsTotal != 2 {
			t.Fatalf("任务应由第 2 个 attempt 运行：%+v", v)
		}
	})
}

// TestFinalizeArbitratesControl 覆盖规格 §8.1 最终裁决：判决在持有任务锁后核对最新控制与当前 attempt。
func TestFinalizeArbitratesControl(t *testing.T) {
	ctx := context.Background()
	succeeded := func(cv int64) task.Verdict {
		zero := int64(0)
		return task.Verdict{AttemptID: "att-t1", TaskID: "t1", ControlVersion: cv, FromStatus: "starting", AttemptStatus: "ended",
			OutcomeClass: "succeeded", ExitCode: &zero, TaskStatus: "succeeded", Result: json.RawMessage(`{"ok":true}`),
			EventType: "attempt_ended", EventPayload: json.RawMessage(`{"exit":0}`)}
	}
	t.Run("取消先提交，过期的成功判决被拒绝，按最新控制重算", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		c, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"})
		if err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err = s.FinalizeAttempt(ctx, succeeded(1))
		expectRejected(t, err, persistence.CodeControlChanged)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		if _, err := s.FinalizeAttempt(ctx, succeeded(c.ControlVersion)); !errors.Is(err, errInvalid) {
			t.Fatalf("desired = cancel 时不能裁决为 succeeded，得到 %v", err)
		}
		v := succeeded(c.ControlVersion)
		v.TaskStatus, v.TaskStatusReason = "cancelled", "completed_during_cancel"
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatalf("按最新控制重算的判决应提交：%v", err)
		}
		if got, _ := s.GetTask(ctx, "t1"); got.Status != "cancelled" {
			t.Fatalf("任务应为 cancelled：%+v", got)
		}
	})
	t.Run("判决先提交，随后的取消被拒绝", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		if _, err := s.FinalizeAttempt(ctx, succeeded(1)); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"})
		expectRejected(t, err, persistence.CodeTaskEnded)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		if _, err := s.GetRequest(ctx, "c1"); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("被拒绝的请求不应留下记录，得到 %v", err)
		}
	})
	t.Run("旧 attempt 的迟到判决被拒绝", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		// 模拟恢复（Plan 6）把执行交还队列而旧 attempt 尚无判决：任务回到 queued，旧环境已停止。
		if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'queued' WHERE task_id = 't1'"); err != nil {
			t.Fatal(err)
		}
		stopEnv(t, s, "env-t1")
		if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att2-t1", AttemptNo: 2, EnvID: "env2-t1"}); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.FinalizeAttempt(ctx, succeeded(1))
		expectRejected(t, err, persistence.CodeStaleAttempt)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
	t.Run("任务已回到 queued 而尚无新 attempt，判决被拒绝", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		// 恢复已把任务交还队列，current_attempt_id 仍是 att-t1：判决不能把 queued 的任务裁决为终态。
		if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'queued' WHERE task_id = 't1'"); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.FinalizeAttempt(ctx, succeeded(1))
		expectRejected(t, err, persistence.CodeStaleAttempt)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
}

// TestControlWriteRules 覆盖规格 §8.1 控制写入：已接受的 cancel 不可被覆盖；resume 要求 paused；
// 被拒绝的请求不留下任何改变与请求记录；request_id 不能被另一个任务重用。
func TestControlWriteRules(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	control := func(id, taskID, desired string) error {
		_, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: id, BodyHash: []byte(desired), TaskID: taskID, Desired: desired})
		return err
	}
	rejected := func(id, taskID, desired, code string) {
		t.Helper()
		before := snapshot(t, s, taskID)
		expectRejected(t, control(id, taskID, desired), code)
		if after := snapshot(t, s, taskID); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		if _, err := s.GetRequest(ctx, id); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("被拒绝的请求 %s 不应留下记录，得到 %v", id, err)
		}
	}
	rejected("c1", "t1", "run", persistence.CodeNotPaused)
	if err := control("c2", "t1", "cancel"); err != nil {
		t.Fatal(err)
	}
	rejected("c3", "t1", "pause", persistence.CodeCancelPending)
	if err := control("c4", "t1", "cancel"); err != nil {
		t.Fatalf("重复 cancel 应被接受：%v", err)
	}

	// resume：经由用例到达 paused（接受 pause → pausing → 以当前控制版本裁决为 paused），再接受 run 并应用。
	if err := control("p1", "t2", "pause"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "t2", ControlVersion: 2, Status: "pausing"}); err != nil {
		t.Fatalf("应用 pause：%v", err)
	}
	v := verdict("t2", 0)
	v.ControlVersion, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = 2, "paused", "paused", ""
	if _, err := s.FinalizeAttempt(ctx, v); err != nil {
		t.Fatalf("暂停判决：%v", err)
	}
	if err := control("p2", "t2", "run"); err != nil {
		t.Fatalf("paused 的任务应能 resume：%v", err)
	}
	st, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "t2", ControlVersion: 3, Status: "queued"})
	if err != nil || st.Status != "queued" || st.AppliedControlVersion != 3 {
		t.Fatalf("应用 resume：%+v %v", st, err)
	}
	if got, _ := s.GetTask(ctx, "t2"); got.Status != "queued" || got.Desired != "run" {
		t.Fatalf("resume 后任务应为 queued：%+v", got)
	}

	// 同一 request_id 与请求体用于另一个任务：冲突，而不是返回 t1 的结果。
	before := snapshot(t, s, "t2")
	if err := control("c2", "t2", "cancel"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("跨任务重用 request_id 应为冲突，得到 %v", err)
	}
	if after := snapshot(t, s, "t2"); after != before {
		t.Fatalf("冲突后任务不应改变：%s → %s", before, after)
	}
}

// TestApplyControlTransitions 覆盖规格 §8.1 控制应用的来源状态：判决后迟到的应用是重放，
// 被取代的版本为 control_changed，状态机不允许的转换为无效输入；被拒绝时任务不变。
func TestApplyControlTransitions(t *testing.T) {
	ctx := context.Background()
	accept := func(t *testing.T, s *Store, id, desired string) int64 {
		t.Helper()
		r, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: id, BodyHash: []byte(desired), TaskID: "t1", Desired: desired})
		if err != nil {
			t.Fatal(err)
		}
		return r.ControlVersion
	}
	apply := func(s *Store, cv int64, status string) (task.ControlState, error) {
		return s.ApplyControl(ctx, task.ApplyControl{TaskID: "t1", ControlVersion: cv, Status: status})
	}
	t.Run("判决后迟到的应用是重放，终态保持", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		cv := accept(t, s, "c1", "cancel")
		v := verdict("t1", 1)
		v.ControlVersion, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = cv, "cancelled", "cancelled", ""
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.GetTask(ctx, "t1"); got.AppliedControlVersion != 2 {
			t.Fatalf("判决应推进 applied_control_version 到 2：%+v", got)
		}
		before := snapshot(t, s, "t1")
		st, err := apply(s, cv, "cancelling")
		if err != nil || st.Status != "cancelled" || st.AppliedControlVersion != 2 {
			t.Fatalf("迟到的应用应返回当前状态：%+v %v", st, err)
		}
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("重放不应改变任务：%s → %s", before, after)
		}
	})
	t.Run("被取代的版本被拒绝，最新版本可应用", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		pause := accept(t, s, "c1", "pause")
		cancel := accept(t, s, "c2", "cancel")
		before := snapshot(t, s, "t1")
		_, err := apply(s, pause, "pausing")
		expectRejected(t, err, persistence.CodeControlChanged)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		st, err := apply(s, cancel, "cancelling")
		if err != nil || st.Status != "cancelling" || st.AppliedControlVersion != cancel {
			t.Fatalf("最新版本应能应用：%+v %v", st, err)
		}
	})
	t.Run("状态机不允许的转换是无效输入", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		cv := accept(t, s, "c1", "pause")
		before := snapshot(t, s, "t1")
		for _, status := range []string{"paused", "cancelling", "queued"} { // running + pause 只能到 pausing
			if _, err := apply(s, cv, status); !errors.Is(err, errInvalid) {
				t.Fatalf("running 在 desired = pause 时不能转换为 %s，得到 %v", status, err)
			}
		}
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
	t.Run("成功判决后按判决版本迟到的应用是重放", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		zero := int64(0)
		v := verdict("t1", 0)
		v.ExitCode, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = &zero, "succeeded", "succeeded", ""
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		st, err := apply(s, 1, "running")
		if err != nil || st.Status != "succeeded" {
			t.Fatalf("迟到的应用应返回 succeeded：%+v %v", st, err)
		}
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("重放不应改变任务：%s → %s", before, after)
		}
	})
	t.Run("终态而控制未应用时被拒绝为 task_ended", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		cv := accept(t, s, "c1", "cancel")
		// 用例不会产生这种组合（判决会推进 applied_control_version）；直接写入以检查终态防线。
		if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'cancelled' WHERE task_id = 't1'"); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := apply(s, cv, "cancelling")
		expectRejected(t, err, persistence.CodeTaskEnded)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
}

// TestHostEventIdempotency 直接驱动 appendHostEvent：同一 event_key 同内容返回原 task_seq，
// 内容不同为冲突；task_seq 从 1 起连续无空洞。
func TestHostEventIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	appendEvent := func(payload string) (int64, error) {
		var seq int64
		err := s.run(ctx, "AppendHostEvent", "t1/k1", func(ctx context.Context, tx pgx.Tx) error {
			if err := lockEventSeq(ctx, tx, "t1"); err != nil {
				return err
			}
			n, err := appendHostEvent(ctx, tx, hostEvent{taskID: "t1", key: "k1", typ: "probe", payload: []byte(payload)})
			seq = n
			return err
		})
		return seq, err
	}
	first, err := appendEvent(`{"n":1}`)
	if err != nil {
		t.Fatal(err)
	}
	again, err := appendEvent(`{"n":1}`)
	if err != nil || again != first {
		t.Fatalf("同一 event_key 同内容应返回原 task_seq %d，得到 %d %v", first, again, err)
	}
	if _, err := appendEvent(`{"n":2}`); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("同一 event_key 不同内容应为冲突，得到 %v", err)
	}
	if n, m := count(t, s, "SELECT count(*) FROM events WHERE task_id = 't1'"),
		count(t, s, "SELECT COALESCE(max(task_seq), 0)::int FROM events WHERE task_id = 't1'"); n != m || n != 3 {
		t.Fatalf("task_seq 应从 1 起连续：count = %d，max = %d", n, m)
	}
}

func TestCommitLostResolvesAPIAndTaskUseCases(t *testing.T) {
	ctx := context.Background()
	createTask := func(t *testing.T, s *Store) {
		if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	checkCommitLost(t, []useCase{
		{"CreateTask", func(*testing.T, *Store) {}, func(s *Store) (any, error) {
			r, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)})
			r.Replayed = false
			return r, err
		}, "SELECT count(*) FROM tasks"},
		{"AcceptControl", withFixture, func(s *Store) (any, error) {
			r, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"})
			r.Replayed = false
			return r, err
		}, "SELECT count(*) FROM events WHERE type = 'control_accepted'"},
		{"CreateAttempt", createTask, func(s *Store) (any, error) {
			return s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"})
		}, "SELECT count(*) FROM attempts"},
		{"ApplyControl", func(t *testing.T, s *Store) {
			withFixture(t, s)
			if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"}); err != nil {
				t.Fatal(err)
			}
		}, func(s *Store) (any, error) {
			return s.ApplyControl(ctx, task.ApplyControl{TaskID: "t1", ControlVersion: 2, Status: "cancelling"})
		}, "SELECT count(*) FROM events WHERE type = 'control_applied'"},
		{"FinalizeAttempt", withFixture, func(s *Store) (any, error) {
			return s.FinalizeAttempt(ctx, verdict("t1", 1))
		}, "SELECT count(*) FROM events WHERE type = 'attempt_ended'"},
	})
}

// TestPendingFirstCommitIsArbitratedByRetry：第一次提交尚未完成 → 新连接查询为空 →
// 以同一身份重跑（等待锁）→ 第一次随后提交成功 → 重跑得到原结果，表中只有一份。
func TestPendingFirstCommitIsArbitratedByRetry(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{OpDeadline: 5 * time.Second, LockTimeout: 4 * time.Second, StatementTimeout: 4 * time.Second})
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	s.hooks.beforeCommit = func(op string) error {
		if op == "CreateAttempt" && first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
		return nil
	}
	req := task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"}
	type result struct {
		a   task.Attempt
		err error
	}
	results := make(chan result, 2)
	go func() { a, err := s.CreateAttempt(ctx, req); results <- result{a, err} }()
	<-held
	if _, err := s.GetAttempt(ctx, "a1"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("第一次提交完成前查询应为空（仍是未知），得到 %v", err)
	}
	go func() { a, err := s.CreateAttempt(ctx, req); results <- result{a, err} }()
	time.Sleep(200 * time.Millisecond) // 让重跑进入锁等待；结论不依赖这段时间
	close(release)
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil || r.a.AttemptID != "a1" {
			t.Fatalf("两次调用都应得到原结果，得到 %+v %v", r.a, r.err)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM attempts"); n != 1 {
		t.Fatalf("attempts 应只有 1 行，得到 %d", n)
	}
}

func TestAPIAndTaskConflicts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	if _, err := s.FinalizeAttempt(ctx, verdict("t1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-t3", BodyHash: []byte("h"), TaskID: "t3", Spec: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	expectConflicts(t, map[string]func() error{
		"CreateTask 同 request_id 不同内容": func() error {
			_, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-t1", BodyHash: []byte("other"), TaskID: "t9", Spec: json.RawMessage(`{}`)})
			return err
		},
		"CreateAttempt 同 attempt_id 不同环境": func() error {
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att-t1", AttemptNo: 1, EnvID: "env-other"})
			return err
		},
		"CreateAttempt 同 attempt_no 不同 attempt_id": func() error {
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att-x", AttemptNo: 1, EnvID: "env-x"})
			return err
		},
		"FinalizeAttempt 同为 failed 但退出码不同": func() error {
			_, err := s.FinalizeAttempt(ctx, verdict("t1", 2))
			return err
		},
		"ApplyControl 版本尚未被接受": func() error {
			_, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "t1", ControlVersion: 9, Status: "x"})
			return err
		},
		"CreateTask 已存在的 task_id（新 request_id）": func() error {
			start := time.Now()
			_, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-new", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)})
			if d := time.Since(start); d > 500*time.Millisecond { // 不应重试到 OpDeadline
				return fmt.Errorf("冲突应立即返回，耗时 %v（%v）", d, err)
			}
			return err
		},
		"CreateAttempt 已存在的 env_id": func() error {
			start := time.Now()
			// t3 处于 queued，准入成立；只有 env_id 已被 t1 的环境占用
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t3", AttemptID: "att-t3", AttemptNo: 1, EnvID: "env-t1"})
			if d := time.Since(start); d > 500*time.Millisecond {
				return fmt.Errorf("冲突应立即返回，耗时 %v（%v）", d, err)
			}
			return err
		},
	})
}

// TestRequestReplayAndReads：同一请求重放返回 Replayed，读取接口与事件顺序正确。
func TestRequestReplayAndReads(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	r, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "pause"})
	if err != nil || r.ControlVersion != 2 || r.Replayed {
		t.Fatalf("首次控制：%+v %v", r, err)
	}
	r2, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "pause"})
	if err != nil || !r2.Replayed || r2.ControlVersion != 2 {
		t.Fatalf("重放应返回原结果并标记 Replayed：%+v %v", r2, err)
	}
	rec, err := s.GetRequest(ctx, "c1")
	if err != nil || rec.Kind != "control" || rec.ResourceID != "t1" {
		t.Fatalf("GetRequest：%+v %v", rec, err)
	}
	v, err := s.GetTask(ctx, "t1")
	if err != nil || v.Desired != "pause" || v.ControlVersion != 2 || v.CurrentAttemptID != "att-t1" || v.AttemptsTotal != 1 {
		t.Fatalf("GetTask：%+v %v", v, err)
	}
	events, err := s.ListEvents(ctx, "t1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for i, e := range events {
		if e.TaskSeq != int64(i+1) {
			t.Fatalf("事件序号应从 1 起连续：%+v", events)
		}
		types = append(types, e.Type)
	}
	if strings.Join(types, ",") != "task_created,attempt_created,control_accepted" {
		t.Fatalf("事件顺序：%v", types)
	}
	if _, err := s.GetTask(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
}

// ---- runner 用例：未知提交、同身份不同内容、Worker 事件批次 ----

func TestCommitLostResolvesRunnerUseCases(t *testing.T) {
	ctx := context.Background()
	checkCommitLost(t, []useCase{
		{"CommitCheckpoint", withFixture, func(s *Store) (any, error) {
			return s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1",
				AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{"n":1}`)})
		}, "SELECT count(*) FROM checkpoints"},
		{"RegisterArtifact", withFixture, func(s *Store) (any, error) {
			return s.RegisterArtifact(ctx, runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report",
				SHA256: strings.Repeat("a", 64), Size: 9, MediaType: "text/markdown", Visibility: "output"})
		}, "SELECT count(*) FROM artifacts"},
		{"AppendWorkerEvents", withFixture, func(s *Store) (any, error) {
			return s.AppendWorkerEvents(ctx, "att-t1", []runner.WorkerEvent{{Seq: 1, Type: "ready", Payload: json.RawMessage(`{}`)}})
		}, "SELECT count(*) FROM events WHERE source = 'worker'"},
		{"RecordTerminalProposal", withFixture, func(s *Store) (any, error) {
			return s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "result", Ref: "seq:9"})
		}, "SELECT count(*) FROM attempts WHERE terminal_proposal = 'result'"},
	})
}

func TestRunnerConflicts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	cp := runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1", AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{"n":1}`)}
	if _, err := s.CommitCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "error", Ref: "seq:3"}); err != nil {
		t.Fatal(err)
	}
	art := runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report", SHA256: strings.Repeat("b", 64), Size: 9,
		MediaType: "text/markdown", Visibility: "output"}
	if _, err := s.RegisterArtifact(ctx, art); err != nil {
		t.Fatal(err)
	}
	cp2 := cp
	cp2.State = json.RawMessage(`{"n":2}`)
	art2 := art
	art2.ArtifactID, art2.Size = "other", 10
	expectConflicts(t, map[string]func() error{
		"CommitCheckpoint 同 ID 不同 state": func() error { _, err := s.CommitCheckpoint(ctx, cp2); return err },
		"RecordTerminalProposal 不同内容": func() error {
			_, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "result", Ref: "seq:3"})
			return err
		},
		"RegisterArtifact 同 sha256 不同大小": func() error { _, err := s.RegisterArtifact(ctx, art2); return err },
	})
}

// TestWorkerEventBatches：重放、部分重叠、同序号不同内容、缺口与非连续批次。
func TestWorkerEventBatches(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	ev := func(seq int64, body string) runner.WorkerEvent {
		return runner.WorkerEvent{Seq: seq, Type: "progress", Payload: json.RawMessage(`{"m":"` + body + `"}`)}
	}
	batch := func(from, to int64, body string) []runner.WorkerEvent {
		var out []runner.WorkerEvent
		for i := from; i <= to; i++ {
			out = append(out, ev(i, fmt.Sprintf("%s%d", body, i)))
		}
		return out
	}
	steps := []struct {
		name  string
		batch []runner.WorkerEvent
		want  int64
		err   error
	}{
		{"首批 1–3", batch(1, 3, "x"), 3, nil},
		{"整批重放", batch(1, 3, "x"), 3, nil},
		{"部分重叠 2–5", batch(2, 5, "x"), 5, nil},
		{"同序号不同内容", batch(5, 6, "y"), 0, persistence.ErrConflict},
		{"缺口 8–9", batch(8, 9, "x"), 0, persistence.ErrConflict},
		{"非连续批次", []runner.WorkerEvent{ev(6, "a"), ev(8, "b")}, 0, errInvalid},
	}
	for _, st := range steps {
		w, err := s.AppendWorkerEvents(ctx, "att-t1", st.batch)
		if st.err != nil {
			if !errors.Is(err, st.err) {
				t.Fatalf("%s：应为 %v，得到 %v", st.name, st.err, err)
			}
			continue
		}
		if err != nil || w.WorkerSeq != st.want {
			t.Fatalf("%s：得到 (%d, %v)，期望水位 %d", st.name, w.WorkerSeq, err, st.want)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM events WHERE source = 'worker'"); n != 5 {
		t.Fatalf("应只有 5 条 Worker 事件，得到 %d", n)
	}
	if w, err := s.WorkerEventWatermark(ctx, "att-t1"); err != nil || w.WorkerSeq != 5 {
		t.Fatalf("水位：%+v %v", w, err)
	}
	if n, m := count(t, s, "SELECT count(*) FROM events WHERE task_id = 't1'"),
		count(t, s, "SELECT max(task_seq)::int FROM events WHERE task_id = 't1'"); n != m {
		t.Fatalf("task_seq 应从 1 起连续无空洞：%d 条，最大 %d", n, m)
	}
}

// TestCheckpointFencingAndRefs 覆盖规格 §5.5 第 2、3 条：新 checkpoint 的 fencing 与引用授权在提交事务内
// 检查，被拒绝时指针与事件都不改变；旧 attempt 也不能登记产物。
func TestCheckpointFencingAndRefs(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	own, foreign, missing := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	register := func(taskID, attemptID, sha string) error {
		_, err := s.RegisterArtifact(ctx, runner.Artifact{TaskID: taskID, AttemptID: attemptID, ArtifactID: "state-" + sha[:1],
			SHA256: sha, Size: 3, MediaType: "application/json", Visibility: "internal"})
		return err
	}
	if err := register("t2", "att-t2", foreign); err != nil {
		t.Fatal(err)
	}
	retryWithNewAttempt(t, s, "t1")
	if err := register("t1", "att2-t1", own); err != nil {
		t.Fatal(err)
	}
	cp := func(id, attemptID, stateRef string, refs ...string) runner.Checkpoint {
		return runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: id, AttemptID: attemptID, StepID: "s1",
			StateRef: stateRef, Refs: refs}
	}
	before := snapshot(t, s, "t1")
	for name, c := range map[string]struct {
		cp   runner.Checkpoint
		code string
	}{
		"旧 attempt":   {cp("cp-old", "att-t1", own), persistence.CodeStaleAttempt},
		"其他任务的 blob":  {cp("cp-foreign", "att2-t1", own, foreign), persistence.CodeRefNotAuthorized},
		"不存在的 blob":   {cp("cp-missing", "att2-t1", missing), persistence.CodeRefNotAuthorized},
		"其他任务的 scope": {runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t2"}, CheckpointID: "cp-x", AttemptID: "att2-t1", StepID: "s1", StateRef: foreign}, persistence.CodeStaleAttempt},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.CommitCheckpoint(ctx, c.cp)
			expectRejected(t, err, c.code)
		})
	}
	expectRejected(t, register("t1", "att-t1", strings.Repeat("d", 64)), persistence.CodeStaleAttempt)
	if after := snapshot(t, s, "t1"); after != before {
		t.Fatalf("被拒绝的写入不应改变指针、事件或产物：%s → %s", before, after)
	}
	got, err := s.CommitCheckpoint(ctx, cp("cp-ok", "att2-t1", own, own))
	if err != nil || got.CommitSeq != 1 {
		t.Fatalf("当前 attempt 引用本任务已授权的 blob 应提交：%+v %v", got, err)
	}
}

// TestNoWritesAfterVerdict 覆盖规格 §5.8：判决提交后，同一 attempt 迟到的 checkpoint、产物与终态提议都被拒绝，
// 任务的指针、事件与产物不变；判决前已完成的写入重放时仍返回原结果。
func TestNoWritesAfterVerdict(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	art := runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report", SHA256: strings.Repeat("a", 64), Size: 9,
		MediaType: "text/markdown", Visibility: "output"}
	first, err := s.RegisterArtifact(ctx, art)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinalizeAttempt(ctx, verdict("t1", 1)); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s, "t1")
	_, err = s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp-late",
		AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{"n":1}`)})
	expectRejected(t, err, persistence.CodeStaleAttempt)
	late := art
	late.SHA256 = strings.Repeat("b", 64)
	_, err = s.RegisterArtifact(ctx, late)
	expectRejected(t, err, persistence.CodeStaleAttempt)
	_, err = s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "result", Ref: "seq:9"})
	expectRejected(t, err, persistence.CodeStaleAttempt)
	if after := snapshot(t, s, "t1"); after != before {
		t.Fatalf("判决后迟到的写入不应改变任务：%s → %s", before, after)
	}
	if again, err := s.RegisterArtifact(ctx, art); err != nil || again != first {
		t.Fatalf("判决前已登记的产物重放应返回原结果：%+v %v", again, err)
	}
}

// ---- resource 用例：未知提交、单调性与分配代次 ----

func withRanges(t *testing.T, s *Store) {
	withFixture(t, s)
	if err := s.SeedUIDRanges(context.Background(), 100000, 4096, 4); err != nil {
		t.Fatal(err)
	}
}

// stopAndClean 记录环境已停止并完成清理——归还其 UID 范围的前置条件（规格 §4.5）。
func stopAndClean(t *testing.T, s *Store, envID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.MarkStopped(ctx, envID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: envID, State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
}

func TestCommitLostResolvesResourceUseCases(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	checkCommitLost(t, []useCase{
		{"RecordIntent", withFixture, func(s *Store) (any, error) {
			return s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"})
		}, "SELECT count(*) FROM resource_intents"},
		{"ResolveIntent", func(t *testing.T, s *Store) {
			withFixture(t, s)
			if _, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"}); err != nil {
				t.Fatal(err)
			}
		}, func(s *Store) (any, error) {
			return s.ResolveIntent(ctx, "i1", resource.IntentAcquired)
		}, "SELECT count(*) FROM resource_intents WHERE state = 'acquired'"},
		{"AssignUIDRange", withRanges, func(s *Store) (any, error) {
			return s.AssignUIDRange(ctx, "env-t1", "alloc-1")
		}, "SELECT count(*) FROM uid_ranges WHERE state = 'assigned'"},
		{"ReleaseUIDRange", func(t *testing.T, s *Store) {
			withRanges(t, s)
			if _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1"); err != nil {
				t.Fatal(err)
			}
			stopAndClean(t, s, "env-t1")
		}, func(s *Store) (any, error) {
			return s.ReleaseUIDRange(ctx, "uid-100000", "alloc-1")
		}, "SELECT count(*) FROM uid_ranges WHERE uid_range_id = 'uid-100000' AND state = 'free'"},
		{"MarkStopped", withFixture, func(s *Store) (any, error) {
			return s.MarkStopped(ctx, "env-t1", at)
		}, "SELECT count(*) FROM environments WHERE stopped_at IS NOT NULL"},
		{"UpdateCleanup", withFixture, func(s *Store) (any, error) {
			return s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", ExpectedTries: 0, State: resource.CleanupPending, Error: "busy"})
		}, "SELECT count(*) FROM environments WHERE cleanup_tries = 1"},
	})
}

func TestResourceConflicts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	withRanges(t, s)
	if _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
	expectConflicts(t, map[string]func() error{
		"RecordIntent 占用同一资源": func() error {
			_, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i2", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"})
			return err
		},
		"ResolveIntent 跳跃":       func() error { _, err := s.ResolveIntent(ctx, "i1", resource.IntentReleased); return err },
		"AssignUIDRange 同环境不同分配": func() error { _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-2"); return err },
		"ReleaseUIDRange 分配代次不符": func() error { _, err := s.ReleaseUIDRange(ctx, "uid-100000", "alloc-old"); return err },
		"UpdateCleanup 状态倒退": func() error {
			_, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", ExpectedTries: 1, State: resource.CleanupPending})
			return err
		},
		"UpdateCleanup 过期的尝试次数": func() error {
			_, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", ExpectedTries: 0, State: resource.CleanupDone, Error: "x"})
			return err
		},
	})
}

// TestReleaseDoesNotFreeLaterAllocation：环境未停止并完成清理时不能归还范围（规格 §4.5）；
// 释放旧分配不得释放后来复用的同一段范围。
func TestReleaseDoesNotFreeLaterAllocation(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	if err := s.SeedUIDRanges(ctx, 100000, 4096, 1); err != nil {
		t.Fatal(err)
	}
	r, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseUIDRange(ctx, r.UIDRangeID, "alloc-1"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("环境仍在运行时归还范围应为冲突，得到 %v", err)
	}
	if _, err := s.MarkStopped(ctx, "env-t1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseUIDRange(ctx, r.UIDRangeID, "alloc-1"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("已停止但未完成清理时归还范围应为冲突，得到 %v", err)
	}
	if got, err := s.GetUIDRange(ctx, "env-t1"); err != nil || got.State != "assigned" {
		t.Fatalf("被拒绝的归还不应改变分配：%+v %v", got, err)
	}
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseUIDRange(ctx, r.UIDRangeID, "alloc-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AssignUIDRange(ctx, "env-t2", "alloc-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseUIDRange(ctx, r.UIDRangeID, "alloc-1"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("重放旧的释放应为冲突，得到 %v", err)
	}
	if got, err := s.GetUIDRange(ctx, "env-t2"); err != nil || got.AllocationID != "alloc-2" {
		t.Fatalf("后来的分配应保持不变：%+v %v", got, err)
	}
}

// ---- 控制面补齐（Plan 5 Task 1）：重试计数、not_before、LoadTask、运行时间、访问撤销、清理候选、隔离 ----

// TestRetryCountersAndNotBefore：重试计数只在新建 attempt 的事务中递增一次（含提交回复丢失后的重跑）；
// not_before 只用于回到 queued 的判决。
func TestRetryCountersAndNotBefore(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	notBefore := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	v := verdict("t1", 1)
	v.TaskStatus, v.NotBefore = "queued", &notBefore
	if _, err := s.FinalizeAttempt(ctx, v); err != nil {
		t.Fatal(err)
	}
	if st, err := s.LoadTask(ctx, "t1"); err != nil || st.NotBefore == nil || !st.NotBefore.Equal(notBefore) {
		t.Fatalf("not_before 应被写入：%+v %v", st.NotBefore, err)
	}
	stopEnv(t, s, "env-t1")
	var lost atomic.Bool
	s.hooks.afterCommit = func(op string) error {
		if op == "CreateAttempt" && lost.CompareAndSwap(false, true) {
			return errors.New("模拟：COMMIT 已执行但回复丢失")
		}
		return nil
	}
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att2-t1", AttemptNo: 2, EnvID: "env2-t1", Retry: task.RetryFault}); err != nil || !lost.Load() {
		t.Fatalf("故障重试的 attempt：%v（钩子触发 %v）", err, lost.Load())
	}
	s.hooks.afterCommit = nil
	v2 := verdict("t1", 137)
	v2.AttemptID, v2.TaskStatus = "att2-t1", "queued"
	if _, err := s.FinalizeAttempt(ctx, v2); err != nil {
		t.Fatal(err)
	}
	stopEnv(t, s, "env2-t1")
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att3-t1", AttemptNo: 3, EnvID: "env3-t1", Retry: task.RetryOOM}); err != nil {
		t.Fatal(err)
	}
	st, err := s.LoadTask(ctx, "t1")
	if err != nil || st.FaultRetriesUsed != 1 || st.OOMRetriesUsed != 1 || st.AttemptsTotal != 3 {
		t.Fatalf("计数应为故障 1、OOM 1、共 3 次：%+v %v", st, err)
	}
	fixture(t, s, "t2")
	bad := verdict("t2", 1)
	bad.NotBefore = &notBefore // TaskStatus = failed
	if _, err := s.FinalizeAttempt(ctx, bad); !errors.Is(err, errInvalid) {
		t.Fatalf("非 queued 的判决带 not_before 应为 errInvalid，得到 %v", err)
	}
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t2", AttemptID: "x", AttemptNo: 2, EnvID: "x", Retry: "maybe"}); !errors.Is(err, errInvalid) {
		t.Fatalf("未定义的重试类别应为 errInvalid，得到 %v", err)
	}
}

// TestLoadTaskReturnsLatestCheckpoint：LoadTask 返回控制与计数事实，以及最新已提交 checkpoint 的完整内容。
func TestLoadTaskReturnsLatestCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	for i, id := range []string{"cp1", "cp2"} {
		if _, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: id,
			AttemptID: "att-t1", StepID: "s" + id, State: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	st, err := s.LoadTask(ctx, "t1")
	if err != nil || st.Status != "running" || st.CurrentAttemptID != "att-t1" || st.Desired != "run" || st.ControlVersion != 1 ||
		st.AttemptsTotal != 1 || st.MaxFaultRetries != 3 {
		t.Fatalf("任务事实：%+v %v", st, err)
	}
	if st.Latest == nil || st.Latest.CheckpointID != "cp2" || st.Latest.StepID != "scp2" || string(st.Latest.State) != `{"n": 2}` || len(st.Latest.Refs) != 0 {
		t.Fatalf("应返回最新 checkpoint 的完整内容：%+v", st.Latest)
	}
	if st2, err := s.LoadTask(ctx, "t2"); err != nil || st2.Latest != nil {
		t.Fatalf("无 checkpoint 时 Latest 应为 nil：%+v %v", st2.Latest, err)
	}
	if _, err := s.LoadTask(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
	if _, err := s.FinalizeAttempt(ctx, verdict("t2", 1)); err != nil {
		t.Fatal(err)
	}
	if ids, err := s.ListActiveTasks(ctx); err != nil || strings.Join(ids, ",") != "t1" {
		t.Fatalf("只有 t1 是非终态：%v %v", ids, err)
	}
}

// TestPersistRunTime：累计运行时间单调（取较大者），提交回复丢失后不重复累计，旧 attempt 被拒绝。
func TestPersistRunTime(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	if got, err := s.PersistRunTime(ctx, "t1", "att-t1", 500); err != nil || got != 500 {
		t.Fatalf("得到 (%d, %v)", got, err)
	}
	if got, err := s.PersistRunTime(ctx, "t1", "att-t1", 300); err != nil || got != 500 {
		t.Fatalf("较小的值不应回退：(%d, %v)", got, err)
	}
	var lost atomic.Bool
	s.hooks.afterCommit = func(op string) error {
		if op == "PersistRunTime" && lost.CompareAndSwap(false, true) {
			return errors.New("模拟：COMMIT 已执行但回复丢失")
		}
		return nil
	}
	if got, err := s.PersistRunTime(ctx, "t1", "att-t1", 800); err != nil || got != 800 || !lost.Load() {
		t.Fatalf("提交回复丢失后应得到 800：(%d, %v)", got, err)
	}
	s.hooks.afterCommit = nil
	retryWithNewAttempt(t, s, "t1")
	_, err := s.PersistRunTime(ctx, "t1", "att-t1", 900)
	expectRejected(t, err, persistence.CodeStaleAttempt)
	if st, _ := s.LoadTask(ctx, "t1"); st.RunTimeMs != 800 {
		t.Fatalf("旧 attempt 不应改变运行时间：%d", st.RunTimeMs)
	}
}

// TestRevokeAttemptAccess：撤销幂等，撤销后该 attempt 的提交被拒绝。
func TestRevokeAttemptAccess(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	for i := 0; i < 2; i++ {
		if err := s.RevokeAttemptAccess(ctx, "att-t1", "stopping"); err != nil {
			t.Fatalf("第 %d 次撤销：%v", i+1, err)
		}
	}
	_, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1",
		AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{}`)})
	expectRejected(t, err, persistence.CodeStaleAttempt)
	if err := s.RevokeAttemptAccess(ctx, "missing", "x"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的 attempt 应为 ErrNotFound，得到 %v", err)
	}
}

// TestListCleanupCandidates：只返回已停止、未清理完成、attempt 已有判决、退避已到的环境。
func TestListCleanupCandidates(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	now := time.Now()
	later := now.Add(time.Hour)
	for _, id := range []string{"t1", "t2", "t3", "t4", "t5"} {
		fixture(t, s, id)
	}
	for _, id := range []string{"t1", "t2", "t4", "t5"} { // t3 无判决
		if _, err := s.FinalizeAttempt(ctx, verdict(id, 1)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"t1", "t3", "t4", "t5"} { // t2 未停止
		if _, err := s.MarkStopped(ctx, "env-"+id, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t4", State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t5", State: resource.CleanupPending, Error: "busy", NextRetryAt: &later}); err != nil {
		t.Fatal(err)
	}
	envs, err := s.ListCleanupCandidates(ctx, now.Add(time.Second), 10)
	if err != nil || len(envs) != 1 || envs[0].EnvID != "env-t1" {
		t.Fatalf("只应返回 env-t1：%+v %v", envs, err)
	}
	if envs, _ := s.ListCleanupCandidates(ctx, later.Add(time.Second), 10); len(envs) != 2 {
		t.Fatalf("退避到期后 env-t5 也应返回：%+v", envs)
	}
}

// TestRecordQuarantine：按路径幂等，不覆盖已有记录。
func TestRecordQuarantine(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	for _, reason := range []string{"无 owner.json", "第二次"} {
		if err := s.RecordQuarantine(ctx, resource.Quarantine{Layer: "env_dir", Path: "/data/envs/x", Reason: reason}); err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM quarantined_resources WHERE resource_path = '/data/envs/x' AND reason = '无 owner.json'"); n != 1 {
		t.Fatalf("应只有首次记录，得到 %d", n)
	}
}

// ---- 恢复与入口补齐（Plan 6 Task 1）：恢复事实、撤销全部访问、未记账运行时间、任务列表、inspect ----

// TestLoadRecoveryFacts：非终态任务及其当前 attempt、未完成的环境、未结束的 intent、可归还的 UID 范围。
func TestLoadRecoveryFacts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1") // 运行中
	fixture(t, s, "t2") // 已终态且清理完成
	if _, err := s.FinalizeAttempt(ctx, verdict("t2", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SeedUIDRanges(ctx, 100000, 4096, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AssignUIDRange(ctx, "env-t2", "alloc-2"); err != nil {
		t.Fatal(err)
	}
	stopAndClean(t, s, "env-t2")
	if _, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "result", Ref: "seq:9"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"}); err != nil {
		t.Fatal(err)
	}
	f, err := s.LoadRecoveryFacts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Tasks) != 1 || f.Tasks[0].TaskID != "t1" || f.Tasks[0].CurrentAttempt == nil ||
		f.Tasks[0].CurrentAttempt.AttemptID != "att-t1" || f.Tasks[0].CurrentAttempt.ProposalKind != "result" || f.Tasks[0].CurrentAttempt.HasVerdict {
		t.Fatalf("任务事实：%+v", f.Tasks)
	}
	if len(f.Environments) != 1 || f.Environments[0].EnvID != "env-t1" || f.Environments[0].StoppedAt != nil {
		t.Fatalf("环境事实只应含未完成的 env-t1：%+v", f.Environments)
	}
	if len(f.PendingIntents) != 1 || f.PendingIntents[0].IntentID != "i1" {
		t.Fatalf("intent 事实：%+v", f.PendingIntents)
	}
	if len(f.UnreleasedRanges) != 1 || f.UnreleasedRanges[0].OwnerID != "env-t2" {
		t.Fatalf("清理完成但未归还的范围：%+v", f.UnreleasedRanges)
	}
}

// TestRevokeAllActive：单事务撤销全部 active 访问，之后旧 attempt 的提交被拒绝；重复调用无新撤销。
func TestRevokeAllActive(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	if n, err := s.RevokeAllActive(ctx, "restart"); err != nil || n != 2 {
		t.Fatalf("应撤销 2 个：(%d, %v)", n, err)
	}
	if n, err := s.RevokeAllActive(ctx, "restart"); err != nil || n != 0 {
		t.Fatalf("重复调用不应再撤销：(%d, %v)", n, err)
	}
	_, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1",
		AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{}`)})
	expectRejected(t, err, persistence.CodeStaleAttempt)
}

// TestAccountUnrecordedRunTime：未记账区间按墙钟差全额计入；同一 until 重复调用不重复计入；旧 attempt 被拒绝。
func TestAccountUnrecordedRunTime(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	if _, err := s.PersistRunTime(ctx, "t1", "att-t1", 1000); err != nil {
		t.Fatal(err)
	}
	var persisted time.Time
	if err := s.pool.QueryRow(ctx, "SELECT run_time_persisted_at FROM tasks WHERE task_id = 't1'").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	until := persisted.Add(5 * time.Second)
	for i := 0; i < 2; i++ {
		if err := s.AccountUnrecordedRunTime(ctx, "t1", "att-t1", until); err != nil {
			t.Fatal(err)
		}
	}
	if st, _ := s.LoadTask(ctx, "t1"); st.RunTimeMs != 6000 {
		t.Fatalf("应计入 5 s 未记账区间且只计一次，得到 %d ms", st.RunTimeMs)
	}
	retryWithNewAttempt(t, s, "t1")
	expectRejected(t, s.AccountUnrecordedRunTime(ctx, "t1", "att-t1", until.Add(time.Second)), persistence.CodeStaleAttempt)
}

// TestListTasksAndInspect：keyset 分页稳定且无重复；inspect 返回 attempt、环境与 checkpoint。
func TestListTasksAndInspect(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	for _, id := range []string{"t1", "t2", "t3"} {
		fixture(t, s, id)
	}
	var seen []string
	cursor := ""
	for page := 0; page < 3; page++ {
		views, next, err := s.ListTasks(ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range views {
			seen = append(seen, v.TaskID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if strings.Join(seen, ",") != "t3,t2,t1" {
		t.Fatalf("应按创建时间倒序、无重复：%v", seen)
	}
	if _, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1",
		AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	in, err := s.Inspect(ctx, "t1")
	if err != nil || in.Task.TaskID != "t1" || len(in.Attempts) != 1 || in.Attempts[0].EnvID != "env-t1" ||
		len(in.Checkpoints) != 1 || in.Checkpoints[0].CommitSeq != 1 {
		t.Fatalf("inspect：%+v %v", in, err)
	}
	if _, err := s.Inspect(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
}

// TestLatestArtifact：返回任务中该产物的最新版本；不存在为 ErrNotFound。
func TestLatestArtifact(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	for _, sha := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		if _, err := s.RegisterArtifact(ctx, runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report", SHA256: sha,
			Size: 9, MediaType: "text/markdown", Visibility: "output"}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.LatestArtifact(ctx, "t1", "report")
	if err != nil || got.Version != 2 || got.SHA256 != strings.Repeat("b", 64) {
		t.Fatalf("应返回第 2 版：%+v %v", got, err)
	}
	if _, err := s.LatestArtifact(ctx, "t1", "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在应为 ErrNotFound，得到 %v", err)
	}
}

// TestAssignUIDRangeExhausted：池耗尽为 resource.ErrNoFreeUIDRange（暂时性），不是 ErrConflict。
func TestAssignUIDRangeExhausted(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	if err := s.SeedUIDRanges(ctx, 100000, 4096, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1"); err != nil {
		t.Fatal(err)
	}
	_, err := s.AssignUIDRange(ctx, "env-t2", "alloc-2")
	if !errors.Is(err, resource.ErrNoFreeUIDRange) || errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("池耗尽应为 ErrNoFreeUIDRange，得到 %v", err)
	}
}

// ---- Gateway：账本、journal、try 与预留（M2 Plan 7 Task 1；规格 §9.4–§9.7、I3、E11b） ----

// gwFixture 建立一个预算为 budget 微美元的任务及其第一次 attempt（att-<taskID>，环境 env-<taskID>）。
func gwFixture(t *testing.T, s *Store, taskID string, budget int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-" + taskID, BodyHash: []byte("h"), TaskID: taskID,
		Spec: json.RawMessage(`{"worker":"sim"}`), Limits: json.RawMessage(fmt.Sprintf(`{"budget_micro":%d}`, budget)), MaxFaultRetries: 3}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: "att-" + taskID, AttemptNo: 1, EnvID: "env-" + taskID}); err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
}

// checkI3 断言账本不变量 I3：每个任务 reserved = Σ held，unknown = Σ charged_unknown，spent = Σ ok try 的实际费用；
// 每笔 reservation 恰在一个桶中，且与其 try 的结算结果一致。
func checkI3(t *testing.T, s *Store) {
	t.Helper()
	var bad *string
	if err := s.pool.QueryRow(context.Background(), `SELECT string_agg(v, '; ') FROM (
		SELECT format('任务 %s：reserved %s / held %s，unknown %s / charged_unknown %s，spent %s / ok %s', b.task_id,
				b.reserved_micro, h.held, b.unknown_micro, h.unk, b.spent_micro, h.ok) AS v
			FROM budgets b, LATERAL (SELECT
				COALESCE((SELECT sum(amount) FROM reservations r WHERE r.task_id = b.task_id AND r.state = 'held'), 0) AS held,
				COALESCE((SELECT sum(amount) FROM reservations r WHERE r.task_id = b.task_id AND r.state = 'charged_unknown'), 0) AS unk,
				COALESCE((SELECT sum(cost_micro) FROM call_tries c WHERE c.task_id = b.task_id AND c.outcome = 'ok'), 0) AS ok) h
			WHERE b.reserved_micro <> h.held OR b.unknown_micro <> h.unk OR b.spent_micro <> h.ok
		UNION ALL
		SELECT format('reservation %s 处于 %s，try 结果 %L', r.reservation_id, r.state, c.outcome)
			FROM reservations r LEFT JOIN call_tries c USING (reservation_id)
			WHERE c.reservation_id IS NULL OR NOT (
				(r.state = 'held' AND c.outcome = '') OR (r.state = 'settled' AND c.outcome = 'ok') OR
				(r.state = 'released' AND c.outcome IN ('retryable', 'fatal')) OR (r.state = 'charged_unknown' AND c.outcome = 'unknown'))
		) x`).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != nil {
		t.Fatalf("违反 I3：%s", *bad)
	}
}

func beginCall(t *testing.T, s *Store, taskID, callID string) call.CallRecord {
	t.Helper()
	res, err := s.BeginCall(context.Background(), call.BeginCallRequest{TaskID: taskID, CallID: callID, AttemptID: "att-" + taskID,
		Fingerprint: "fp-" + callID, Endpoint: "chat", Deadline: 120 * time.Second})
	if err != nil || res.Existing {
		t.Fatalf("BeginCall %s: %+v / %v", callID, res, err)
	}
	checkI3(t, s)
	return res.Record
}

func reserve(s *Store, taskID, callID string, est int64) (call.Try, error) {
	return s.ReserveTry(context.Background(), call.ReserveTryRequest{TaskID: taskID, CallID: callID, AttemptID: "att-" + taskID,
		EnvID: "env-" + taskID, EstimateMicro: est, MaxTries: 3})
}

func mustReserve(t *testing.T, s *Store, taskID, callID string, est int64) call.Try {
	t.Helper()
	tr, err := reserve(s, taskID, callID, est)
	if err != nil {
		t.Fatalf("ReserveTry %s: %v", callID, err)
	}
	checkI3(t, s)
	return tr
}

func settle(t *testing.T, s *Store, st call.Settlement) call.CallRecord {
	t.Helper()
	rec, err := s.SettleTry(context.Background(), st)
	if err != nil {
		t.Fatalf("SettleTry %+v: %v", st, err)
	}
	checkI3(t, s)
	return rec
}

func expectBudget(t *testing.T, s *Store, taskID string, want call.Budget) {
	t.Helper()
	got, err := s.LoadBudget(context.Background(), taskID)
	if err != nil || got != want {
		t.Fatalf("账本应为 %+v，得到 %+v / %v", want, got, err)
	}
}

// TestCreateTaskWritesBudget：CreateTask 在同一事务中写入 budgets（limit 取 limits.budget_micro；缺省为 0，失败关闭）；
// 负数或非整数为 ErrInvalid 且不建任务。
func TestCreateTaskWritesBudget(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	create := func(id, limits string) error {
		_, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r-" + id, BodyHash: []byte("h"), TaskID: id,
			Spec: json.RawMessage(`{}`), Limits: json.RawMessage(limits)})
		return err
	}
	if err := create("t1", `{"budget_micro":2000000,"max_run_time_ms":1000}`); err != nil {
		t.Fatal(err)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 2000000})
	if err := create("t2", ``); err != nil {
		t.Fatal(err)
	}
	expectBudget(t, s, "t2", call.Budget{})
	if err := create("t3", `{"max_run_time_ms":1000}`); err != nil {
		t.Fatal(err)
	}
	expectBudget(t, s, "t3", call.Budget{})
	for i, bad := range []string{`{"budget_micro":-1}`, `{"budget_micro":1.5}`, `{"budget_micro":"5"}`, `[1]`} {
		if err := create(fmt.Sprintf("bad%d", i), bad); !errors.Is(err, persistence.ErrInvalid) {
			t.Fatalf("%s 应为 ErrInvalid，得到 %v", bad, err)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM tasks WHERE task_id LIKE 'bad%'"); n != 0 {
		t.Fatalf("不合法的 limits 不应建任务，得到 %d", n)
	}
	// 重放不重复写入
	if err := create("t1", `{"budget_micro":2000000,"max_run_time_ms":1000}`); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT count(*) FROM budgets"); n != 3 {
		t.Fatalf("应有 3 行预算，得到 %d", n)
	}
	// 缺省预算为 0 时付费调用被拒
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t2", AttemptID: "att-t2", AttemptNo: 1, EnvID: "env-t2"}); err != nil {
		t.Fatal(err)
	}
	beginCall(t, s, "t2", "c1")
	_, err := reserve(s, "t2", "c1", 1)
	expectRejected(t, err, persistence.CodeBudgetExhausted)
}

// TestGatewaySettlementBranches：结算三分支（ok / 释放 / unknown）与赤字；每笔 reservation 只进入一个桶；
// 重复结算幂等（迟到的不同结果不改变记账）；ok 写入 blobs 与 scope_blobs(task)；每次用例后 I3 成立。
func TestGatewaySettlementBranches(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 1000)
	sha := strings.Repeat("ab", 32)

	// ok：实际 500 超出预留 300
	beginCall(t, s, "t1", "c1")
	tr := mustReserve(t, s, "t1", "c1", 300)
	if tr.TryNo != 1 || tr.ReservationID == "" || tr.AttemptID != "att-t1" {
		t.Fatalf("try 不对：%+v", tr)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, ReservedMicro: 300})
	ok := call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 500, LatencyMs: 12, UpstreamRequestID: "up-1", ResultSHA256: sha, ResultSize: 42}
	rec := settle(t, s, ok)
	if rec.State != call.StateCompleted || rec.ResultRef != sha || rec.CostCharged != 500 || rec.UpstreamRequestID != "up-1" || rec.TriesUsed != 1 {
		t.Fatalf("ok 结算后调用不对：%+v", rec)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, SpentMicro: 500})
	if n := count(t, s, "SELECT count(*) FROM scope_blobs WHERE scope_kind = 'task' AND scope_id = 't1' AND sha256 = $1", sha); n != 1 {
		t.Fatalf("结果 blob 应授权到任务 scope，得到 %d", n)
	}
	if n := count(t, s, "SELECT count(*) FROM blobs WHERE sha256 = $1 AND size = 42", sha); n != 1 {
		t.Fatalf("结果 blob 应已登记，得到 %d", n)
	}
	again := settle(t, s, ok)
	unk := ok
	unk.Outcome, unk.ActualMicro, unk.ResultSHA256, unk.ResultSize = "unknown", 0, "", 0
	late := settle(t, s, unk)
	if fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", rec) || fmt.Sprintf("%+v", late) != fmt.Sprintf("%+v", rec) {
		t.Fatalf("重复结算应返回原结果：%+v / %+v", again, late)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, SpentMicro: 500})
	if _, err := reserve(s, "t1", "c1", 1); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("已完成的调用不能新建 try，得到 %v", err)
	}
	wrong := ok
	wrong.Try.ReservationID = "rsv-other"
	if _, err := s.SettleTry(ctx, wrong); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("reservation 不符应为冲突，得到 %v", err)
	}
	other := ok
	other.Try = call.Try{}
	if _, err := s.SettleTry(ctx, call.Settlement{Try: tr, Outcome: "retryable", ActualMicro: 5}); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("非 ok 结算带费用应为 ErrInvalid，得到 %v", err)
	}

	// unknown：全额转入 unknown，调用为 unknown 且标记可能的外部重复
	beginCall(t, s, "t1", "c2")
	tr = mustReserve(t, s, "t1", "c2", 400)
	rec = settle(t, s, call.Settlement{Try: tr, Outcome: "unknown", Error: "read timeout"})
	if rec.State != call.StateUnknown || !rec.PossibleExternalDuplicate || rec.CostCharged != 0 {
		t.Fatalf("unknown 结算后调用不对：%+v", rec)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, SpentMicro: 500, UnknownMicro: 400})

	// 可用 100：估算 200 → budget_insufficient_for_request；估算 100 可以
	beginCall(t, s, "t1", "c3")
	_, err := reserve(s, "t1", "c3", 200)
	expectRejected(t, err, persistence.CodeBudgetInsufficient)
	checkI3(t, s)
	tr = mustReserve(t, s, "t1", "c3", 100)
	// retryable：释放，调用保持 in_flight，可再 try
	rec = settle(t, s, call.Settlement{Try: tr, Outcome: "retryable", LatencyMs: 3, Error: "429"})
	if rec.State != call.StateInFlight || rec.TriesUsed != 1 {
		t.Fatalf("retryable 后调用应保持 in_flight：%+v", rec)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, SpentMicro: 500, UnknownMicro: 400})
	tr2 := mustReserve(t, s, "t1", "c3", 100)
	if tr2.TryNo != 2 {
		t.Fatalf("第二次 try 应为 2：%+v", tr2)
	}
	// fatal：释放，调用 failed
	rec = settle(t, s, call.Settlement{Try: tr2, Outcome: "fatal", Error: "400 unsupported_field"})
	if rec.State != call.StateFailed || rec.FailReason != "400 unsupported_field" || rec.TriesUsed != 2 {
		t.Fatalf("fatal 后调用应为 failed：%+v", rec)
	}
	_, tries, err := s.LoadCall(ctx, "t1", "c3")
	if err != nil || len(tries) != 2 || tries[0].Outcome != "retryable" || tries[0].Error != "429" || tries[0].EnvID != "env-t1" ||
		tries[1].Outcome != "fatal" || tries[1].State != "settled" {
		t.Fatalf("try 记录不对：%+v / %v", tries, err)
	}
	if n := count(t, s, "SELECT count(*) FROM reservations WHERE call_id = 'c3' AND state = 'released'"); n != 2 {
		t.Fatalf("两次 try 都应释放，得到 %d", n)
	}

	// 赤字：可用 100，预留 100，实际 300 → 可用 −200；之后任何付费调用都是 budget_exhausted
	beginCall(t, s, "t1", "c4")
	tr = mustReserve(t, s, "t1", "c4", 100)
	settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 300, ResultSHA256: strings.Repeat("cd", 32), ResultSize: 1})
	b, _ := s.LoadBudget(ctx, "t1")
	if b.Available() != -200 {
		t.Fatalf("应出现赤字 −200：%+v", b)
	}
	beginCall(t, s, "t1", "c5")
	_, err = reserve(s, "t1", "c5", 0)
	expectRejected(t, err, persistence.CodeBudgetExhausted)
	checkI3(t, s)

	calls, err := s.ListCalls(ctx, "t1")
	if err != nil || len(calls) != 5 {
		t.Fatalf("应列出 5 个调用：%d / %v", len(calls), err)
	}
	// 同一 blob 大小不同为冲突
	beginCall(t, s, "t1", "c6")
	if _, err := s.pool.Exec(ctx, "UPDATE budgets SET limit_micro = 10000 WHERE task_id = 't1'"); err != nil {
		t.Fatal(err)
	}
	tr = mustReserve(t, s, "t1", "c6", 10)
	if _, err := s.SettleTry(ctx, call.Settlement{Try: tr, Outcome: "ok", ResultSHA256: sha, ResultSize: 43}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("blob 大小不符应为冲突，得到 %v", err)
	}
	checkI3(t, s)
}

// TestReserveTryCommitLostE11b 覆盖 E11b：reservation 的 COMMIT 回复丢失。重跑与之后的再次调用都以同一 try 身份
// 解析为原记录，不产生第二笔预留。
func TestReserveTryCommitLostE11b(t *testing.T) {
	ctx := context.Background()
	t.Run("一次丢失在期限内解析", func(t *testing.T) {
		s := newStore(t, Options{})
		gwFixture(t, s, "t1", 1000)
		beginCall(t, s, "t1", "c1")
		var lost atomic.Bool
		s.hooks.afterCommit = func(op string) error {
			if op == "ReserveTry" && lost.CompareAndSwap(false, true) {
				return errors.New("模拟：COMMIT 已执行但回复丢失")
			}
			return nil
		}
		tr, err := reserve(s, "t1", "c1", 300)
		if err != nil || !lost.Load() || tr.TryNo != 1 {
			t.Fatalf("应解析为原 try：%+v / %v（钩子触发 %v）", tr, err, lost.Load())
		}
		s.hooks.afterCommit = nil
		if n := count(t, s, "SELECT count(*) FROM reservations"); n != 1 {
			t.Fatalf("应只有 1 笔预留，得到 %d", n)
		}
		expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, ReservedMicro: 300})
		checkI3(t, s)
	})
	t.Run("始终丢失返回 ErrCommitUnknown，再次调用得到原记录", func(t *testing.T) {
		s := newStore(t, Options{OpDeadline: 500 * time.Millisecond})
		gwFixture(t, s, "t1", 1000)
		beginCall(t, s, "t1", "c1")
		s.hooks.afterCommit = func(op string) error {
			if op == "ReserveTry" {
				return errors.New("模拟：回复始终丢失")
			}
			return nil
		}
		_, err := reserve(s, "t1", "c1", 300)
		var unknown *persistence.CommitUnknownError
		if !errors.As(err, &unknown) || unknown.Op != "ReserveTry" {
			t.Fatalf("应为 CommitUnknownError，得到 %v", err)
		}
		s.hooks.afterCommit = nil
		checkI3(t, s)
		tr, err := reserve(s, "t1", "c1", 300)
		if err != nil || tr.TryNo != 1 {
			t.Fatalf("以同一身份再次调用应返回原 try：%+v / %v", tr, err)
		}
		var rid string
		if err := s.pool.QueryRow(ctx, "SELECT reservation_id FROM reservations").Scan(&rid); err != nil || rid != tr.ReservationID {
			t.Fatalf("应返回已提交的 reservation %s，得到 %+v / %v", rid, tr, err)
		}
		if n := count(t, s, "SELECT count(*) FROM reservations"); n != 1 {
			t.Fatalf("不应产生第二笔预留，得到 %d", n)
		}
		if n := count(t, s, "SELECT count(*) FROM calls WHERE tries_used = 1 AND state = 'in_flight'"); n != 1 {
			t.Fatal("tries_used 不应重复累加")
		}
		expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, ReservedMicro: 300})
		checkI3(t, s)
		// 不同身份（其他环境）在 try 仍持有预留时为 call_in_progress
		_, err = s.ReserveTry(ctx, call.ReserveTryRequest{TaskID: "t1", CallID: "c1", AttemptID: "att-t1", EnvID: "env-x", EstimateMicro: 300, MaxTries: 3})
		expectRejected(t, err, persistence.CodeCallInProgress)
		err = s.FailCall(ctx, "t1", "c1", "x")
		expectRejected(t, err, persistence.CodeCallInProgress)
		checkI3(t, s)
		// 提交结果未知之后访问被撤销：以同一身份重试仍返回原 try，不复查访问，不产生新预留
		if err := s.RevokeAttemptAccess(ctx, "att-t1", "stopping"); err != nil {
			t.Fatal(err)
		}
		again, err := reserve(s, "t1", "c1", 300)
		if err != nil || again != tr {
			t.Fatalf("撤销后以同一身份重试应返回原 try：%+v / %v，原为 %+v", again, err, tr)
		}
		if n := count(t, s, "SELECT count(*) FROM reservations"); n != 1 {
			t.Fatalf("不应产生第二笔预留，得到 %d", n)
		}
		checkI3(t, s)
		_, err = reserve(s, "t1", "c1", 301) // 不是同一身份：作为新预留检查访问
		expectRejected(t, err, persistence.CodeAccessRevoked)
		checkI3(t, s)
	})
}

// TestBeginCallJournal：BeginCall 的期限按数据库时间写入并在 ReserveTry 中按数据库时间判断；已有记录原样返回
// （含指纹）；提交回复丢失后的重跑仍是新登记；tries_exhausted；FailCall 幂等。
func TestBeginCallJournal(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 1000)
	var lost atomic.Bool
	s.hooks.afterCommit = func(op string) error {
		if op == "BeginCall" && lost.CompareAndSwap(false, true) {
			return errors.New("模拟：COMMIT 已执行但回复丢失")
		}
		return nil
	}
	rec := beginCall(t, s, "t1", "c1")
	s.hooks.afterCommit = nil
	if !lost.Load() || rec.State != call.StateResolving || rec.TriesUsed != 0 || rec.FirstAttemptID != "att-t1" || rec.Source != "upstream" {
		t.Fatalf("登记的记录不对：%+v（钩子 %v）", rec, lost.Load())
	}
	if d := rec.DeadlineAt.Sub(rec.CreatedAt); d != 120*time.Second {
		t.Fatalf("deadline_at − created_at 应为 120s，得到 %v", d)
	}
	var skew float64
	if err := s.pool.QueryRow(ctx, "SELECT abs(extract(epoch FROM now() - created_at)) FROM calls WHERE call_id = 'c1'").Scan(&skew); err != nil || skew > 5 {
		t.Fatalf("created_at 应取数据库时间：偏差 %v / %v", skew, err)
	}
	res, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: "c1", AttemptID: "att-t1", Fingerprint: "other",
		Endpoint: "chat", Deadline: time.Hour})
	if err != nil || !res.Existing || res.Record.Fingerprint != "fp-c1" || !res.Record.DeadlineAt.Equal(rec.DeadlineAt) {
		t.Fatalf("已有记录应原样返回：%+v / %v", res, err)
	}
	if n := count(t, s, "SELECT count(*) FROM calls"); n != 1 {
		t.Fatalf("应只有 1 条调用，得到 %d", n)
	}
	sup, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: "c1b", AttemptID: "att-t1", Fingerprint: "fp",
		Endpoint: "chat", Deadline: time.Minute, SupersedesCallID: "c1", SupersedeReason: "divergence"})
	if err != nil || sup.Record.SupersedesCallID != "c1" || sup.Record.SupersedeReason != "divergence" {
		t.Fatalf("supersede 关联不对：%+v / %v", sup, err)
	}

	// 数据库时间已到 deadline_at：不再新建 try
	if _, err := s.pool.Exec(ctx, "UPDATE calls SET created_at = now() - interval '10 seconds', deadline_at = now() - interval '1 second' WHERE call_id = 'c1'"); err != nil {
		t.Fatal(err)
	}
	_, err = reserve(s, "t1", "c1", 10)
	expectRejected(t, err, persistence.CodeCallDeadlineExceeded)
	checkI3(t, s)
	for i := 0; i < 2; i++ {
		if err := s.FailCall(ctx, "t1", "c1", "call_deadline_exceeded"); err != nil {
			t.Fatalf("FailCall 第 %d 次：%v", i+1, err)
		}
	}
	r, _, err := s.LoadCall(ctx, "t1", "c1")
	if err != nil || r.State != call.StateFailed || r.FailReason != "call_deadline_exceeded" {
		t.Fatalf("应为 failed：%+v / %v", r, err)
	}

	// 累计 try 上限
	beginCall(t, s, "t1", "c2")
	for i := 1; i <= 2; i++ {
		tr, err := s.ReserveTry(ctx, call.ReserveTryRequest{TaskID: "t1", CallID: "c2", AttemptID: "att-t1", EnvID: "env-t1", EstimateMicro: 10, MaxTries: 2})
		if err != nil {
			t.Fatal(err)
		}
		settle(t, s, call.Settlement{Try: tr, Outcome: "retryable"})
	}
	_, err = s.ReserveTry(ctx, call.ReserveTryRequest{TaskID: "t1", CallID: "c2", AttemptID: "att-t1", EnvID: "env-t1", EstimateMicro: 10, MaxTries: 2})
	expectRejected(t, err, persistence.CodeTriesExhausted)
	if _, _, err := s.LoadCall(ctx, "t1", "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的调用应为 ErrNotFound，得到 %v", err)
	}
	checkI3(t, s)
}

// TestResetResolving：只复位没有 try、仍在解析的 resolving，且不改变 created_at 与 deadline_at（重启不重置期限，
// §9.7、E48）；复位后同指纹的 BeginCall 接管它，不同指纹不接管；复位后已过期的调用在 ReserveTry 被拒。
func TestResetResolving(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 1000)
	begin := func(callID, fp string) call.BeginCallResult {
		t.Helper()
		res, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: callID, AttemptID: "att-t1", Fingerprint: fp,
			Endpoint: "chat", Deadline: 120 * time.Second})
		if err != nil {
			t.Fatalf("BeginCall %s: %v", callID, err)
		}
		checkI3(t, s)
		return res
	}
	orig := beginCall(t, s, "t1", "c1") // resolving，无 try，正在解析
	if orig.ResolvingSince == nil {
		t.Fatalf("新登记的 resolving 应有 resolving_since：%+v", orig)
	}
	if res := begin("c1", "fp-c1"); !res.Existing || res.Record.ResolvingSince == nil {
		t.Fatalf("正在解析时重复请求应得到已有记录：%+v", res)
	}
	beginCall(t, s, "t1", "c2") // in_flight，持有预留
	mustReserve(t, s, "t1", "c2", 100)
	beginCall(t, s, "t1", "c3") // 有 try 的 resolving（人为构造，防御性检查）
	tr3 := mustReserve(t, s, "t1", "c3", 50)
	settle(t, s, call.Settlement{Try: tr3, Outcome: "retryable"})
	if _, err := s.pool.Exec(ctx, "UPDATE calls SET state = 'resolving', resolving_since = now() WHERE call_id = 'c3'"); err != nil {
		t.Fatal(err)
	}
	beginCall(t, s, "t1", "c4") // 复位后过期

	n, err := s.ResetResolving(ctx)
	if err != nil || n != 2 {
		t.Fatalf("应复位 2 个调用，得到 %d / %v", n, err)
	}
	if got := count(t, s, "SELECT count(*) FROM calls WHERE call_id IN ('c2', 'c3') AND (state = 'in_flight' OR resolving_since IS NOT NULL)"); got != 2 {
		t.Fatalf("有 try 的调用不应被复位，得到 %d", got)
	}
	reset, _, err := s.LoadCall(ctx, "t1", "c1")
	if err != nil || reset.State != call.StateResolving || reset.ResolvingSince != nil ||
		!reset.CreatedAt.Equal(orig.CreatedAt) || !reset.DeadlineAt.Equal(orig.DeadlineAt) {
		t.Fatalf("复位只应清空 resolving_since：%+v / %v，原为 %+v", reset, err, orig)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, ReservedMicro: 100})
	checkI3(t, s)

	if res := begin("c1", "other"); !res.Existing || res.Record.ResolvingSince != nil {
		t.Fatalf("不同指纹不应接管：%+v", res)
	}
	taken := begin("c1", "fp-c1")
	if taken.Existing || taken.Record.ResolvingSince == nil || !taken.Record.DeadlineAt.Equal(orig.DeadlineAt) ||
		!taken.Record.CreatedAt.Equal(orig.CreatedAt) {
		t.Fatalf("同指纹应接管且期限不变：%+v，原为 %+v", taken, orig)
	}
	if res := begin("c1", "fp-c1"); !res.Existing {
		t.Fatalf("接管后再次请求应为进行中：%+v", res)
	}
	mustReserve(t, s, "t1", "c1", 10)
	if r, _, _ := s.LoadCall(ctx, "t1", "c1"); r.State != call.StateInFlight || r.ResolvingSince != nil {
		t.Fatalf("预留后应为 in_flight 且 resolving_since 为空：%+v", r)
	}

	if _, err := s.pool.Exec(ctx, "UPDATE calls SET created_at = now() - interval '10 minutes', deadline_at = now() - interval '1 second' WHERE call_id = 'c4'"); err != nil {
		t.Fatal(err)
	}
	if res := begin("c4", "fp-c4"); res.Existing {
		t.Fatalf("复位的调用应可接管：%+v", res)
	}
	_, err = reserve(s, "t1", "c4", 10)
	expectRejected(t, err, persistence.CodeCallDeadlineExceeded)
	checkI3(t, s)
}

// TestMigrationBackfillsBudgets：0003 之前创建的任务在迁移后得到零预算行（失败关闭），而不是找不到预算。
func TestMigrationBackfillsBudgets(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	// 还原为只应用了 0001、0002 的旧库：任务 t1 没有预算行
	if _, err := s.pool.Exec(ctx, `DROP TABLE call_tries, reservations, calls, budgets; DELETE FROM schema_migrations WHERE version >= 3`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	expectBudget(t, s, "t1", call.Budget{})
	beginCall(t, s, "t1", "c1")
	_, err := reserve(s, "t1", "c1", 1)
	expectRejected(t, err, persistence.CodeBudgetExhausted)
}

// TestReserveTryAccessRejections：访问撤销、取消、非当前 attempt 分别被 ReserveTry 拒绝，且不留下预留；
// CheckAccess 报告对应事实。
func TestReserveTryAccessRejections(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		after func(t *testing.T, s *Store)
		code  string
		facts call.AccessFacts
	}{
		{"撤销", func(t *testing.T, s *Store) {
			if err := s.RevokeAttemptAccess(ctx, "att-t1", "stopping"); err != nil {
				t.Fatal(err)
			}
		}, persistence.CodeAccessRevoked, call.AccessFacts{TaskID: "t1", AttemptID: "att-t1", Current: true, Desired: "run"}},
		{"取消", func(t *testing.T, s *Store) {
			if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"}); err != nil {
				t.Fatal(err)
			}
		}, persistence.CodeCancelRequested, call.AccessFacts{TaskID: "t1", AttemptID: "att-t1", Active: true, Current: true, Desired: "cancel"}},
		{"非当前 attempt", func(t *testing.T, s *Store) { retryWithNewAttempt(t, s, "t1") },
			persistence.CodeNotCurrentAttempt, call.AccessFacts{TaskID: "t1", AttemptID: "att-t1", Active: true, Desired: "run"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newStore(t, Options{})
			gwFixture(t, s, "t1", 1000)
			beginCall(t, s, "t1", "c1")
			f, err := s.CheckAccess(ctx, "t1", "att-t1")
			if err != nil || f != (call.AccessFacts{TaskID: "t1", AttemptID: "att-t1", Active: true, Current: true, Desired: "run"}) {
				t.Fatalf("初始访问事实不对：%+v / %v", f, err)
			}
			c.after(t, s)
			if f, err := s.CheckAccess(ctx, "t1", "att-t1"); err != nil || f != c.facts {
				t.Fatalf("访问事实应为 %+v，得到 %+v / %v", c.facts, f, err)
			}
			_, err = reserve(s, "t1", "c1", 10)
			expectRejected(t, err, c.code)
			_, err = s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: "c2", AttemptID: "att-t1", Fingerprint: "fp",
				Endpoint: "chat", Deadline: time.Minute})
			expectRejected(t, err, c.code)
			if n := count(t, s, "SELECT count(*) FROM reservations"); n != 0 {
				t.Fatalf("被拒绝时不应留下预留，得到 %d", n)
			}
			checkI3(t, s)
		})
	}
	s := newStore(t, Options{})
	if _, err := s.CheckAccess(ctx, "missing", "a"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
}

// TestGatewayTaskFactsAndInspect（Plan 7 Task 5）：LookupAttempt 给出 attempt 的任务与环境；AppendHostEvent 经
// task_seq 追加 host 事件且以内容幂等；BlobAuthorized 按 scope_blobs(task) 授权；Inspect 汇总调用与每次 try
// （G11：task_id → attempt_id → call_id → try_no）。
func TestGatewayTaskFactsAndInspect(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 1000)
	if taskID, envID, err := s.LookupAttempt(ctx, "att-t1"); err != nil || taskID != "t1" || envID != "env-t1" {
		t.Fatalf("LookupAttempt = %s, %s, %v", taskID, envID, err)
	}
	if _, _, err := s.LookupAttempt(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的 attempt 应为 ErrNotFound，得到 %v", err)
	}

	before := count(t, s, "SELECT count(*) FROM events WHERE task_id = 't1'")
	for i := 0; i < 2; i++ { // 同一内容（提交结果未知后的重跑）只追加一次
		if err := s.AppendHostEvent(ctx, "t1", "att-t1", "replay_divergence", json.RawMessage(`{"call_id":"c1","detail":"x"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AppendHostEvent(ctx, "t1", "att-t1", "replay_divergence", json.RawMessage(`{"call_id":"c2","detail":"y"}`)); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT count(*) FROM events WHERE task_id = 't1' AND type = 'replay_divergence' AND source = 'host'"); n != 2 {
		t.Fatalf("replay_divergence 事件 %d 条，期望 2", n)
	}
	if n := count(t, s, "SELECT max(task_seq) FROM events WHERE task_id = 't1'"); n != before+2 {
		t.Fatalf("task_seq 应连续分配：max = %d，之前 %d 条", n, before)
	}
	if err := s.AppendHostEvent(ctx, "t1", "att-t1", "replay_divergence", json.RawMessage(`[1]`)); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("非对象 payload 应为 ErrInvalid，得到 %v", err)
	}

	beginCall(t, s, "t1", "c1")
	tr := mustReserve(t, s, "t1", "c1", 100)
	sha := strings.Repeat("a", 64)
	settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 40, LatencyMs: 12, UpstreamRequestID: "req-1",
		ResultSHA256: sha, ResultSize: 3})
	for _, c := range []struct {
		task, sha string
		want      bool
	}{{"t1", sha, true}, {"t2", sha, false}, {"t1", strings.Repeat("b", 64), false}} {
		if ok, err := s.BlobAuthorized(ctx, c.task, c.sha); err != nil || ok != c.want {
			t.Fatalf("BlobAuthorized(%s, %s…) = %v, %v，期望 %v", c.task, c.sha[:4], ok, err, c.want)
		}
	}
	in, err := s.Inspect(ctx, "t1")
	if err != nil || len(in.Calls) != 1 {
		t.Fatalf("Inspect = %+v, %v", in.Calls, err)
	}
	c := in.Calls[0]
	if c.CallID != "c1" || c.State != "completed" || c.FirstAttemptID != "att-t1" || c.TriesUsed != 1 || c.CostChargedMicro != 40 ||
		c.UpstreamRequestID != "req-1" || c.ResultRef != sha || c.DeadlineAt.Sub(c.CreatedAt) != 120*time.Second {
		t.Fatalf("调用视图 = %+v", c)
	}
	want := api.TryView{TryNo: 1, AttemptID: "att-t1", EnvID: "env-t1", State: "settled", Outcome: "ok", LatencyMs: 12, CostMicro: 40}
	if len(c.Tries) != 1 || c.Tries[0] != want {
		t.Fatalf("try 视图 = %+v，期望 %+v", c.Tries, want)
	}
}
