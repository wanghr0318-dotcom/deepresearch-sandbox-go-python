package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/recovery"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/session"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/subrun"
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
		if _, err := s.pool.Exec(ctx, undo0007+undo0006+`DROP TABLE call_tries, reservations, calls, budgets, sessions;
			ALTER TABLE tasks DROP COLUMN owner_user_id; DROP TABLE users;
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

// TestPinnedOutputReads：TaskResult 返回状态与 result_json；PinnedArtifact 按版本选择（0 为最新），
// 授权按 scope_blobs(task)——最新版本未授权时视为不存在，不回退到更早的版本。
func TestPinnedOutputReads(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	if _, err := s.TaskResult(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
	if r, err := s.TaskResult(ctx, "t1"); err != nil || task.IsTerminal(r.Status) || r.Result != nil {
		t.Fatalf("运行中的任务没有结果：%+v %v", r, err)
	}
	shas := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	for _, sha := range shas {
		if _, err := s.RegisterArtifact(ctx, runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report", SHA256: sha,
			Size: 9, MediaType: "text/markdown", Visibility: "output"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		version int64
		want    string
	}{{0, shas[1]}, {1, shas[0]}, {2, shas[1]}, {3, ""}} {
		v, found, err := s.PinnedArtifact(ctx, "t1", "report", c.version)
		if err != nil || found != (c.want != "") || v.SHA256 != c.want {
			t.Fatalf("version %d：%+v found=%v %v，期望 %q", c.version, v, found, err, c.want)
		}
		if found && (v.Size != 9 || v.MediaType != "text/markdown" || (c.version != 0 && v.Version != c.version)) {
			t.Fatalf("version %d：%+v", c.version, v)
		}
	}
	if _, found, err := s.PinnedArtifact(ctx, "t1", "missing", 0); err != nil || found {
		t.Fatalf("不存在的产物：found=%v %v", found, err)
	}
	// internal 产物对下载不可见：与不存在相同；最新版本为 internal 时也不回退到更早的 output 版本。
	for i, vis := range []string{"output", "internal"} {
		if _, err := s.RegisterArtifact(ctx, runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "scratch",
			SHA256: strings.Repeat(string(rune('c'+i)), 64), Size: 3, MediaType: "text/plain", Visibility: vis}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		version int64
		found   bool
	}{{0, false}, {2, false}, {1, true}} {
		if _, found, err := s.PinnedArtifact(ctx, "t1", "scratch", c.version); err != nil || found != c.found {
			t.Fatalf("scratch version %d：found=%v %v，期望 %v", c.version, found, err, c.found)
		}
	}
	if _, _, err := s.PinnedArtifact(ctx, "missing", "report", 0); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
	if _, err := s.pool.Exec(ctx, "DELETE FROM scope_blobs WHERE scope_id = 't1' AND sha256 = $1", shas[1]); err != nil {
		t.Fatal(err)
	}
	if v, found, err := s.PinnedArtifact(ctx, "t1", "report", 0); err != nil || found {
		t.Fatalf("最新版本未授权时不应回退到更早的版本：%+v found=%v %v", v, found, err)
	}
	if _, found, err := s.PinnedArtifact(ctx, "t1", "report", 1); err != nil || !found {
		t.Fatalf("已授权的第 1 版应可读：found=%v %v", found, err)
	}

	zero := int64(0)
	if _, err := s.FinalizeAttempt(ctx, task.Verdict{AttemptID: "att-t1", TaskID: "t1", ControlVersion: 1, FromStatus: "starting",
		AttemptStatus: "ended", OutcomeClass: "succeeded", ExitCode: &zero, TaskStatus: "succeeded",
		Result:    json.RawMessage(`{"summary":"s","outputs":[{"artifact_id":"report","version":1,"sha256":"` + shas[0] + `"}]}`),
		EventType: "attempt_ended", EventPayload: json.RawMessage(`{"exit":0}`)}); err != nil {
		t.Fatal(err)
	}
	r, err := s.TaskResult(ctx, "t1")
	var got struct {
		Outputs []struct {
			ArtifactID string `json:"artifact_id"`
			Version    int64  `json:"version"`
		} `json:"outputs"`
	}
	if err != nil || r.Status != "succeeded" || json.Unmarshal(r.Result, &got) != nil || len(got.Outputs) != 1 || got.Outputs[0].Version != 1 {
		t.Fatalf("终态任务的结果：%+v %v", r, err)
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

// TestConvertLedger：启动账本转换（§14.1 第 4 步）在一个事务中把全部 held 预留转为 charged_unknown（各任务
// reserved 减、unknown 加同额）、其 try 按 unknown 结算、全部 in_flight 调用置为 unknown（deadline_at 不变；
// 持有预留的调用标记 possible_external_duplicate）；resolving 不动；I3 保持；再次运行不转换任何记录；
// 期限之内同指纹的请求在累计上限内新建 try。
func TestConvertLedger(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 1000)
	gwFixture(t, s, "t2", 500)
	c1 := beginCall(t, s, "t1", "c1") // in_flight，持有 300
	mustReserve(t, s, "t1", "c1", 300)
	beginCall(t, s, "t1", "c2") // in_flight，try 已按 retryable 结算（退避中），不持有预留
	settle(t, s, call.Settlement{Try: mustReserve(t, s, "t1", "c2", 100), Outcome: "retryable"})
	beginCall(t, s, "t1", "c3") // resolving：不属于账本转换
	beginCall(t, s, "t2", "c1") // 另一任务，持有 200
	mustReserve(t, s, "t2", "c1", 200)

	got, err := s.ConvertLedger(ctx)
	if want := (recovery.LedgerConversion{Reservations: 2, Calls: 3, UnknownMicro: 500}); err != nil || got != want {
		t.Fatalf("ConvertLedger = %+v / %v，期望 %+v", got, err, want)
	}
	checkI3(t, s)
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, UnknownMicro: 300})
	expectBudget(t, s, "t2", call.Budget{LimitMicro: 500, UnknownMicro: 200})
	rec, tries, err := s.LoadCall(ctx, "t1", "c1")
	if err != nil || rec.State != call.StateUnknown || !rec.PossibleExternalDuplicate || !rec.DeadlineAt.Equal(c1.DeadlineAt) ||
		len(tries) != 1 || tries[0].State != "settled" || tries[0].Outcome != "unknown" || tries[0].Error != LedgerRestartError {
		t.Fatalf("t1/c1 = %+v，tries %+v / %v", rec, tries, err)
	}
	if rec, _, err := s.LoadCall(ctx, "t1", "c2"); err != nil || rec.State != call.StateUnknown || rec.PossibleExternalDuplicate {
		t.Fatalf("无预留的 in_flight 调用应转为 unknown 且不标记重复：%+v / %v", rec, err)
	}
	if rec, _, err := s.LoadCall(ctx, "t1", "c3"); err != nil || rec.State != call.StateResolving || rec.ResolvingSince == nil {
		t.Fatalf("resolving 调用不应被账本转换改动：%+v / %v", rec, err)
	}
	if n := count(t, s, "SELECT count(*) FROM reservations WHERE state = 'charged_unknown'"); n != 2 {
		t.Fatalf("charged_unknown 预留 %d 笔", n)
	}

	again, err := s.ConvertLedger(ctx)
	if err != nil || again != (recovery.LedgerConversion{}) {
		t.Fatalf("再次转换应为空操作：%+v / %v", again, err)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, UnknownMicro: 300})
	checkI3(t, s)

	// 期限之内：同指纹请求见到 unknown（可在上限内新建 try），新 try 正常预留。
	res, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: "c1", AttemptID: "att-t1", Fingerprint: "fp-c1",
		Endpoint: "chat", Deadline: 120 * time.Second})
	if err != nil || !res.Existing || res.Record.State != call.StateUnknown {
		t.Fatalf("转换后的 BeginCall = %+v / %v", res, err)
	}
	if tr := mustReserve(t, s, "t1", "c1", 300); tr.TryNo != 2 {
		t.Fatalf("应新建 try 2，得到 %+v", tr)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000, ReservedMicro: 300, UnknownMicro: 300})
}

// TestMigrationBackfillsBudgets：0003 之前创建的任务在迁移后得到零预算行（失败关闭），而不是找不到预算。
func TestMigrationBackfillsBudgets(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	// 还原为只应用了 0001、0002 的旧库：任务 t1 没有预算行
	if _, err := s.pool.Exec(ctx, undo0007+undo0006+`DROP TABLE call_tries, reservations, calls, budgets, sessions; ALTER TABLE tasks DROP COLUMN owner_user_id; DROP TABLE users;
		DELETE FROM schema_migrations WHERE version >= 3`); err != nil {
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

// TestCompleteFromCache（Plan 9 Task 3；§11.2、E23）：缓存命中的 Tx2 不建 reservation 与 try、不改动账本，结果写入
// scope_blobs(task) 并 completed（source = cache）；同一结果的重复提交原样返回；已有 try、已完成为其他结果或 blob
// 大小不符为冲突；取消先提交时被拒（cancel_requested），结果不被授权，调用保持 resolving（由 Gateway 置为 failed）。
func TestCompleteFromCache(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 1000)
	sha := strings.Repeat("ab", 32)
	beginCall(t, s, "t1", "c1")
	cc := call.CacheCompletion{TaskID: "t1", CallID: "c1", AttemptID: "att-t1", ResultSHA256: sha, ResultSize: 42}
	rec, err := s.CompleteFromCache(ctx, cc)
	if err != nil || rec.State != call.StateCompleted || rec.Source != "cache" || rec.ResultRef != sha || rec.TriesUsed != 0 ||
		rec.ResolvingSince != nil || rec.CostCharged != 0 {
		t.Fatalf("CompleteFromCache = %+v / %v", rec, err)
	}
	if again, err := s.CompleteFromCache(ctx, cc); err != nil || again.ResultRef != sha || again.Source != "cache" {
		t.Fatalf("同一结果的重复提交应原样返回：%+v / %v", again, err)
	}
	if ok, err := s.BlobAuthorized(ctx, "t1", sha); err != nil || !ok {
		t.Fatalf("命中结果应在 scope_blobs(task) 中：%v / %v", ok, err)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 1000})
	if n := count(t, s, "SELECT count(*) FROM reservations") + count(t, s, "SELECT count(*) FROM call_tries"); n != 0 {
		t.Fatalf("命中不应建 reservation 或 try，得到 %d 行", n)
	}
	if n := count(t, s, "SELECT count(*) FROM blob_provenance WHERE sha256 = $1 AND ref = 'c1#cache'", sha); n != 1 {
		t.Fatalf("blob_provenance 应记录 c1#cache 一次，得到 %d", n)
	}
	checkI3(t, s)

	other := cc
	other.ResultSHA256 = strings.Repeat("cd", 32)
	if _, err := s.CompleteFromCache(ctx, other); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("已完成的调用以其他结果提交应为冲突，得到 %v", err)
	}
	beginCall(t, s, "t1", "c2")
	mustReserve(t, s, "t1", "c2", 10)
	withTry := cc
	withTry.CallID = "c2"
	if _, err := s.CompleteFromCache(ctx, withTry); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("已有 try 的调用不能由缓存完成，得到 %v", err)
	}
	beginCall(t, s, "t1", "c3")
	badSize := cc
	badSize.CallID, badSize.ResultSize = "c3", 43
	if _, err := s.CompleteFromCache(ctx, badSize); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("blob 大小与登记不符应为冲突，得到 %v", err)
	}
	if rec, _, err := s.LoadCall(ctx, "t1", "c3"); err != nil || rec.State != call.StateResolving {
		t.Fatalf("冲突回滚后 c3 应仍为 resolving：%+v / %v", rec, err)
	}

	// singleflight follower（§11.4）：source = coalesced，同样无 reservation 与 try；重跑以同一来源幂等，
	// 以其他来源重提为冲突；未知来源无效。
	beginCall(t, s, "t1", "c5")
	co := cc
	co.CallID, co.Source = "c5", call.SourceCoalesced
	if rec, err := s.CompleteFromCache(ctx, co); err != nil || rec.Source != call.SourceCoalesced || rec.State != call.StateCompleted ||
		rec.ResultRef != sha || rec.TriesUsed != 0 || rec.CostCharged != 0 {
		t.Fatalf("coalesced 提交 = %+v / %v", rec, err)
	}
	if again, err := s.CompleteFromCache(ctx, co); err != nil || again.Source != call.SourceCoalesced {
		t.Fatalf("coalesced 重复提交应原样返回：%+v / %v", again, err)
	}
	asCache := co
	asCache.Source = ""
	if _, err := s.CompleteFromCache(ctx, asCache); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("已由 coalesced 完成的调用以 cache 重提应为冲突，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM blob_provenance WHERE sha256 = $1 AND ref = 'c5#coalesced'", sha); n != 1 {
		t.Fatalf("blob_provenance 应记录 c5#coalesced 一次，得到 %d", n)
	}
	bogus := co
	bogus.CallID, bogus.Source = "c3", "upstream"
	if _, err := s.CompleteFromCache(ctx, bogus); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("source 不是 cache 或 coalesced 应为 ErrInvalid，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM call_tries WHERE call_id = 'c5'"); n != 0 {
		t.Fatalf("coalesced 不应建 try，得到 %d", n)
	}
	checkI3(t, s)

	// E23：取消先提交 → 命中结果不被授权。
	beginCall(t, s, "t1", "c4")
	if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "cancel-1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"}); err != nil {
		t.Fatal(err)
	}
	late := cc
	late.CallID, late.ResultSHA256 = "c4", strings.Repeat("ef", 32)
	_, err = s.CompleteFromCache(ctx, late)
	expectRejected(t, err, persistence.CodeCancelRequested)
	if ok, err := s.BlobAuthorized(ctx, "t1", late.ResultSHA256); err != nil || ok {
		t.Fatalf("取消先提交后命中结果不应被授权：%v / %v", ok, err)
	}
	if rec, _, err := s.LoadCall(ctx, "t1", "c4"); err != nil || rec.State != call.StateResolving || rec.ResultRef != "" {
		t.Fatalf("被拒后 c4 应仍为 resolving 且无结果：%+v / %v", rec, err)
	}
	checkI3(t, s)
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
	if c.Model != "" {
		t.Fatalf("未记录模型的调用 model 应为空：%q", c.Model)
	}

	// journal 记录解析后的模型：新建时写入，重放的 BeginCall 不改写；LoadCall 与 Inspect 都读到它。
	for _, m := range []string{"kimi-k2.6", "other"} {
		if _, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: "c2", AttemptID: "att-t1", Fingerprint: "fp-c2",
			Endpoint: "chat", Deadline: time.Minute, Model: m}); err != nil {
			t.Fatal(err)
		}
	}
	if rec, _, err := s.LoadCall(ctx, "t1", "c2"); err != nil || rec.Model != "kimi-k2.6" {
		t.Fatalf("LoadCall model = %q, %v", rec.Model, err)
	}
	if in, err = s.Inspect(ctx, "t1"); err != nil || len(in.Calls) != 2 || in.Calls[1].Model != "kimi-k2.6" {
		t.Fatalf("Inspect 的 model：%+v, %v", in.Calls, err)
	}
}

// ---- 账号与会话（migration 0005，M3 Plan 11） ----

// TestAccountUsersAndSessions：用户名大小写不敏感唯一；登录读取；会话命中、过期、删除；停用吊销全部会话，
// 启用后可重新建会话。
func TestAccountUsersAndSessions(t *testing.T) {
	checkAccounts(t, newStore(t, Options{}))
}

// checkAccounts 是 Plan 11 的账号与登录会话用例（迁移 0006 之后登录会话表为 auth_sessions）；s 中尚无用户。
func checkAccounts(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	alice, err := s.CreateUser(ctx, "Alice", "alice", "hash-a")
	if err != nil || alice.ID <= 0 || alice.Username != "Alice" || alice.Role != "user" || alice.Disabled {
		t.Fatalf("CreateUser = %+v, %v", alice, err)
	}
	if again, err := s.CreateUser(ctx, "Alice", "alice", "hash-a"); err != nil || again.ID != alice.ID {
		t.Fatalf("同一密码哈希的重跑应得到已提交的行：%+v, %v", again, err)
	}
	if _, err := s.CreateUser(ctx, "ALICE", "alice", "hash-other"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("大小写不同的重名应为 ErrConflict，得到 %v", err)
	}
	if u, hash, err := s.UserForLogin(ctx, "alice"); err != nil || u.ID != alice.ID || hash != "hash-a" {
		t.Fatalf("UserForLogin = %+v, %q, %v", u, hash, err)
	}
	if _, _, err := s.UserForLogin(ctx, "nobody"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的用户应为 ErrNotFound，得到 %v", err)
	}

	live, expired, other := []byte("sess-live"), []byte("sess-expired"), []byte("sess-other")
	for _, c := range []struct {
		id  []byte
		exp time.Time
	}{{live, time.Now().Add(time.Hour)}, {expired, time.Now().Add(-time.Second)}, {other, time.Now().Add(time.Hour)}} {
		if err := s.CreateSession(ctx, c.id, alice.ID, c.exp); err != nil {
			t.Fatal(err)
		}
	}
	if u, err := s.SessionUser(ctx, live); err != nil || u.ID != alice.ID || u.Username != "Alice" {
		t.Fatalf("SessionUser = %+v, %v", u, err)
	}
	for name, id := range map[string][]byte{"已过期": expired, "不存在": []byte("nope")} {
		if _, err := s.SessionUser(ctx, id); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("%s的会话应为 ErrNotFound，得到 %v", name, err)
		}
	}
	if err := s.DeleteSession(ctx, live); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SessionUser(ctx, live); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("删除后的会话应为 ErrNotFound，得到 %v", err)
	}

	if err := s.SetDisabled(ctx, "alice", true); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT count(*) FROM auth_sessions WHERE user_id = $1", alice.ID); n != 0 {
		t.Fatalf("停用应删除全部会话，剩 %d", n)
	}
	if _, err := s.SessionUser(ctx, other); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("停用后会话应为 ErrNotFound，得到 %v", err)
	}
	// 停用期间写入的会话行（例如停用与登录并发）也不能通过 SessionUser。
	if err := s.CreateSession(ctx, []byte("sess-while-disabled"), alice.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SessionUser(ctx, []byte("sess-while-disabled")); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("停用用户的会话应为 ErrNotFound，得到 %v", err)
	}
	if err := s.SetDisabled(ctx, "nobody", true); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("停用不存在的用户应为 ErrNotFound，得到 %v", err)
	}
	if err := s.SetDisabled(ctx, "alice", false); err != nil {
		t.Fatal(err)
	}
	fresh := []byte("sess-fresh")
	if err := s.CreateSession(ctx, fresh, alice.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if u, err := s.SessionUser(ctx, fresh); err != nil || u.ID != alice.ID {
		t.Fatalf("启用后的新会话 = %+v, %v", u, err)
	}

	if _, err := s.CreateUser(ctx, "bob", "bob", "hash-b"); err != nil {
		t.Fatal(err)
	}
	users, err := s.ListUsers(ctx)
	if err != nil || len(users) != 2 || users[0].Username != "Alice" || users[1].Username != "bob" {
		t.Fatalf("ListUsers = %+v, %v", users, err)
	}
}

// TestAccountCreateResearch：每用户同时最多 1 个非终态任务；终态后可再提交；request_id 重放不受限制且只属于
// 原用户；并发提交只有一个成功（用户行锁）；停用用户不能提交；ListTasksByOwner 只含自己的任务；无主任务的
// owner 为 0。
func TestAccountCreateResearch(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice, err := s.CreateUser(ctx, "alice", "alice", "hash-a")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "bob", "bob", "hash-b")
	if err != nil {
		t.Fatal(err)
	}
	research := func(user int64, reqID, taskID string) (api.CreateTaskResult, error) {
		return s.CreateResearch(ctx, user, api.CreateTaskRequest{RequestID: reqID, BodyHash: []byte("h-" + reqID), TaskID: taskID,
			Spec: json.RawMessage(`{"topic":"x"}`), Limits: json.RawMessage(`{"budget_micro":100}`)})
	}

	if r, err := research(alice.ID, "ra1", "a1"); err != nil || r.TaskID != "a1" || r.Replayed {
		t.Fatalf("第一次 CreateResearch = %+v, %v", r, err)
	}
	if owner, err := s.TaskOwner(ctx, "a1"); err != nil || owner != alice.ID {
		t.Fatalf("TaskOwner(a1) = %d, %v", owner, err)
	}
	if n := count(t, s, "SELECT count(*) FROM budgets WHERE task_id = 'a1' AND limit_micro = 100"); n != 1 {
		t.Fatal("CreateResearch 应与 CreateTask 一样写入预算行")
	}
	if _, err := research(alice.ID, "ra2", "a2"); !errors.Is(err, api.ErrUserTaskRunning) {
		t.Fatalf("已有非终态任务应为 ErrUserTaskRunning，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM api_requests WHERE request_id = 'ra2'"); n != 0 {
		t.Fatal("被拒绝的请求不应留下 request 记录")
	}
	if r, err := research(alice.ID, "ra1", "a1"); err != nil || r.TaskID != "a1" || !r.Replayed {
		t.Fatalf("重放（任务仍运行中）应返回原任务：%+v, %v", r, err)
	}
	if _, err := s.CreateResearch(ctx, bob.ID, api.CreateTaskRequest{RequestID: "ra1", BodyHash: []byte("h-ra1"), TaskID: "b9",
		Spec: json.RawMessage(`{}`)}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("他人的 request_id 重放应为 ErrConflict，得到 %v", err)
	}
	if r, err := research(bob.ID, "rb1", "b1"); err != nil || r.TaskID != "b1" {
		t.Fatalf("另一用户不受影响：%+v, %v", r, err)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'succeeded' WHERE task_id = 'a1'"); err != nil {
		t.Fatal(err)
	}
	if r, err := research(alice.ID, "ra3", "a3"); err != nil || r.TaskID != "a3" {
		t.Fatalf("终态后应可再提交：%+v, %v", r, err)
	}

	// 并发：两个不同 request_id 同时提交，只有一个成功（用户行 FOR UPDATE 串行化计数与插入）。
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'cancelled' WHERE task_id = 'a3'"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = research(alice.ID, fmt.Sprintf("rc%d", i), fmt.Sprintf("c%d", i))
		}(i)
	}
	close(start)
	wg.Wait()
	ok, running := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, api.ErrUserTaskRunning):
			running++
		default:
			t.Fatalf("并发提交的意外错误：%v", err)
		}
	}
	if ok != 1 || running != 1 {
		t.Fatalf("并发提交应恰好一个成功：成功 %d，被拒 %d", ok, running)
	}

	if err := s.SetDisabled(ctx, "bob", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'failed' WHERE task_id = 'b1'"); err != nil {
		t.Fatal(err)
	}
	if _, err := research(bob.ID, "rb2", "b2"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("停用用户提交应为 ErrNotFound，得到 %v", err)
	}

	fixture(t, s, "ops1") // 经 CreateTask 创建的无主任务
	if owner, err := s.TaskOwner(ctx, "ops1"); err != nil || owner != 0 {
		t.Fatalf("无主任务的 owner = %d, %v", owner, err)
	}
	if _, err := s.TaskOwner(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
	var seen []string
	cursor := ""
	for page := 0; page < 4; page++ {
		views, next, err := s.ListTasksByOwner(ctx, alice.ID, cursor, 1)
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
	if len(seen) != 3 || seen[1] != "a3" || seen[2] != "a1" || (seen[0] != "c0" && seen[0] != "c1") {
		t.Fatalf("ListTasksByOwner 应只含 alice 的任务、倒序：%v", seen)
	}
	all, _, err := s.ListTasks(ctx, "", 10)
	if err != nil || len(all) != 5 {
		t.Fatalf("ListTasks 应含全部任务：%d, %v", len(all), err)
	}

	// 任务视图带 spec 的 topic（没有时为空）与 created_at（UTC，与 tasks.created_at 一致）。
	var dbCreated time.Time
	if err := s.pool.QueryRow(ctx, "SELECT created_at FROM tasks WHERE task_id = 'a1'").Scan(&dbCreated); err != nil {
		t.Fatal(err)
	}
	if v, err := s.GetTask(ctx, "a1"); err != nil || v.Topic != "x" || !v.CreatedAt.Equal(dbCreated) || v.CreatedAt.Location() != time.UTC {
		t.Fatalf("GetTask(a1) 的 topic/created_at = %q, %v（库中 %v）, %v", v.Topic, v.CreatedAt, dbCreated, err)
	}
	if v, err := s.GetTask(ctx, "ops1"); err != nil || v.Topic != "" || v.CreatedAt.IsZero() {
		t.Fatalf("GetTask(ops1) 的 topic/created_at = %q, %v, %v", v.Topic, v.CreatedAt, err)
	}
	for _, v := range all {
		want := "x"
		if v.TaskID == "ops1" {
			want = ""
		}
		if v.Topic != want || v.CreatedAt.IsZero() || v.CreatedAt.Location() != time.UTC {
			t.Fatalf("ListTasks 中 %s 的 topic/created_at = %q, %v", v.TaskID, v.Topic, v.CreatedAt)
		}
	}
	if views, _, err := s.ListTasksByOwner(ctx, alice.ID, "", 10); err != nil || len(views) != 3 || views[2].Topic != "x" || !views[2].CreatedAt.Equal(dbCreated) {
		t.Fatalf("ListTasksByOwner 的 topic/created_at = %+v, %v", views, err)
	}
}

// ---- 会话（migration 0006，M4 Plan 12） ----

// undo0006 把库还原为只应用了 0005 的形状（登录会话表改回 sessions）；回滚到更早迁移的测试先执行它。
const undo0006 = `DROP TABLE incarnations, session_events, session_event_seq, session_progress, session_control;
	ALTER TABLE tasks DROP CONSTRAINT tasks_session_fk, DROP COLUMN turn_index, DROP COLUMN restored_from_task_id,
		DROP COLUMN resume_directive;
	DROP TABLE sessions;
	ALTER TABLE events DROP COLUMN session_id, DROP COLUMN session_seq;
	ALTER TABLE budgets DROP CONSTRAINT budgets_tool_within, DROP COLUMN tool_call_limit, DROP COLUMN tool_calls_used;
	ALTER TABLE auth_sessions RENAME TO sessions;
	ALTER INDEX auth_sessions_user_id RENAME TO sessions_user_id;
	ALTER TABLE sessions RENAME CONSTRAINT auth_sessions_pkey TO sessions_pkey;
	ALTER TABLE sessions RENAME CONSTRAINT auth_sessions_user_id_fkey TO sessions_user_id_fkey;
	DELETE FROM schema_migrations WHERE version >= 6;
`

// TestMigration0006：全新库一次迁移到 0006；0005 的库升级到 0006 后原有登录会话仍有效（auth_sessions），Plan 11 的
// 账号与登录用例全部通过，迁移前的任务与事件不带会话序号。
func TestMigration0006(t *testing.T) {
	ctx := context.Background()
	t.Run("全新库", func(t *testing.T) {
		s := newStore(t, Options{})
		if n := count(t, s, "SELECT count(*) FROM schema_migrations WHERE version <= 6"); n != 6 {
			t.Fatalf("应依次应用 0001–0006，得到 %d 个", n)
		}
		st, err := s.InspectInstallation(ctx)
		if err != nil || !st.HasAgentboxTables {
			t.Fatalf("InspectInstallation = %+v, %v", st, err)
		}
	})
	t.Run("从 0005 升级", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "old")
		if _, err := s.pool.Exec(ctx, undo0007+undo0006); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, `INSERT INTO users (username, username_key, password_hash) VALUES ('carol', 'carol', 'h');
			INSERT INTO sessions (id_hash, user_id, expires_at) SELECT '\x01', id, now() + interval '1 hour' FROM users`); err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if u, err := s.SessionUser(ctx, []byte{1}); err != nil || u.Username != "carol" {
			t.Fatalf("迁移前的登录会话应仍有效：%+v, %v", u, err)
		}
		if n := count(t, s, "SELECT count(*) FROM events WHERE task_id = 'old' AND session_id IS NULL AND session_seq IS NULL"); n == 0 {
			t.Fatal("迁移前的事件应保留且没有会话序号")
		}
		if _, err := s.pool.Exec(ctx, "DELETE FROM auth_sessions; DELETE FROM users"); err != nil {
			t.Fatal(err)
		}
		checkAccounts(t, s)
	})
}

// ---- 会话测试的辅助 ----

func mustUser(t *testing.T, s *Store, name string) int64 {
	t.Helper()
	u, err := s.CreateUser(context.Background(), name, name, "hash-"+name)
	if err != nil {
		t.Fatal(err)
	}
	return u.ID
}

// newSession 创建会话 id（owner 为 0 时无主）。
func newSession(t *testing.T, s *Store, id string, owner int64) {
	t.Helper()
	if _, _, err := s.SessionAPI().CreateSession(context.Background(), api.CreateSessionRequest{RequestID: "rs-" + id,
		BodyHash: []byte("h-" + id), SessionID: id, OwnerUserID: owner}); err != nil {
		t.Fatalf("CreateSession(%s): %v", id, err)
	}
}

func turnReq(sessionID, taskID string, owner int64) api.CreateTurnRequest {
	return api.CreateTurnRequest{RequestID: "rt-" + taskID, BodyHash: []byte("h-" + taskID), SessionID: sessionID, TaskID: taskID,
		OwnerUserID: owner, Text: "问题 " + taskID, DeepResearch: true, Spec: json.RawMessage(`{"kind":"turn"}`),
		Limits: json.RawMessage(`{"budget_micro":100,"max_tool_calls":30}`), MaxFaultRetries: 1}
}

func mustTurn(t *testing.T, s *Store, req api.CreateTurnRequest) api.CreateTurnResult {
	t.Helper()
	res, err := s.SessionAPI().CreateTurn(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateTurn(%s): %v", req.TaskID, err)
	}
	return res
}

// setTask 直接改写任务状态（模拟 task actor 的推进）。
func setTask(t *testing.T, s *Store, taskID, status, reason string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), "UPDATE tasks SET status = $2, status_reason = $3 WHERE task_id = $1",
		taskID, status, reason); err != nil {
		t.Fatal(err)
	}
}

// setSession 直接改写会话状态（模拟 session actor 的推进）。
func setSession(t *testing.T, s *Store, sessionID, status string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), "UPDATE sessions SET status = $2 WHERE session_id = $1", sessionID, status); err != nil {
		t.Fatal(err)
	}
}

// mustAttempt 创建 att-<taskID>。会话 turn 经会话授予（Task 4）：会话没有存活 incarnation 时建立 inc-<sid>（环境
// senv-<sid>），并以直接写入模拟 session actor 的就绪与释放（incarnation idle、会话 idle 且不被占用），不追加会话事件。
func mustAttempt(t *testing.T, s *Store, taskID string) {
	t.Helper()
	ctx := context.Background()
	a := task.NewAttempt{TaskID: taskID, AttemptID: "att-" + taskID, AttemptNo: 1, EnvID: "env-" + taskID}
	var sid *string
	if err := s.pool.QueryRow(ctx, "SELECT session_id FROM tasks WHERE task_id = $1", taskID).Scan(&sid); err != nil {
		t.Fatal(err)
	}
	if sid != nil {
		var inc, env string
		err := s.pool.QueryRow(ctx, "SELECT incarnation_id, env_id FROM incarnations WHERE session_id = $1 AND status <> 'ended'", *sid).
			Scan(&inc, &env)
		if errors.Is(err, pgx.ErrNoRows) {
			var rv int64
			if err := s.pool.QueryRow(ctx, "SELECT row_version FROM sessions WHERE session_id = $1", *sid).Scan(&rv); err != nil {
				t.Fatal(err)
			}
			inc, env = "inc-"+*sid, "senv-"+*sid
			_, err = s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: inc, SessionID: *sid, EnvID: env, FromRowVersion: rv})
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, `UPDATE incarnations SET status = 'idle' WHERE incarnation_id = $1;`, inc); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, `UPDATE sessions SET status = CASE WHEN status IN ('creating', 'running') THEN 'idle' ELSE status END,
			current_task_id = NULL, blocked_by_task_id = NULL WHERE session_id = $1`, *sid); err != nil {
			t.Fatal(err)
		}
		a.SessionID, a.IncarnationID, a.EnvID = *sid, inc, env
	}
	if _, err := s.CreateAttempt(ctx, a); err != nil {
		t.Fatalf("CreateAttempt(%s): %v", taskID, err)
	}
}

func workerEvents(t *testing.T, s *Store, attemptID string, from, to int64, payload string) {
	t.Helper()
	var evs []runner.WorkerEvent
	for i := from; i <= to; i++ {
		evs = append(evs, runner.WorkerEvent{Seq: i, Type: "progress", Payload: json.RawMessage(payload)})
	}
	if _, err := s.AppendWorkerEvents(context.Background(), attemptID, evs); err != nil {
		t.Fatalf("AppendWorkerEvents(%s): %v", attemptID, err)
	}
}

// expectNoDBViolations 断言只依赖数据库的不变量（含 I4、I6、I16）全部成立。
func expectNoDBViolations(t *testing.T, s *Store) {
	t.Helper()
	vs, err := s.DBViolations(context.Background())
	if err != nil || len(vs) != 0 {
		t.Fatalf("不变量违反：%+v, %v", vs, err)
	}
}

// expectSessionSeqContiguous 断言会话的 session_seq（events 与 session_events 合计）从 1 起连续、无重复。
func expectSessionSeqContiguous(t *testing.T, s *Store, sessionID string) int {
	t.Helper()
	var n, distinct, lo, hi int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*), count(DISTINCT seq), COALESCE(min(seq), 0), COALESCE(max(seq), 0) FROM (
			SELECT session_seq AS seq FROM events WHERE session_id = $1
			UNION ALL SELECT session_seq FROM session_events WHERE session_id = $1) x`, sessionID).Scan(&n, &distinct, &lo, &hi); err != nil {
		t.Fatal(err)
	}
	if n != distinct || (n > 0 && (lo != 1 || hi != n)) {
		t.Fatalf("session_seq 应从 1 起连续无重复：%d 条（不同 %d），范围 %d..%d", n, distinct, lo, hi)
	}
	return n
}

// TestSessionEventSeq：同一会话两个 turn 的 host 与 worker 事件、会话生命周期事件交错追加，session_seq 连续且等于
// 提交顺序；独立任务的事件没有会话序号；ListSessionEvents 按序分页。
func TestSessionEventSeq(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	a := s.SessionAPI()
	alice := mustUser(t, s, "alice")
	newSession(t, s, "s1", alice)
	fixture(t, s, "solo") // 独立任务

	mustTurn(t, s, turnReq("s1", "ta", alice))
	if _, err := s.Transition(ctx, session.Transition{SessionID: "s1", FromRowVersion: 0, From: []string{session.StatusCreating},
		To: session.StatusIdle, Event: json.RawMessage(`{"state":"idle"}`)}); err != nil {
		t.Fatal(err)
	}
	mustAttempt(t, s, "ta") // 会话 turn 的 attempt 经会话授予（Task 4），会话须先就绪
	workerEvents(t, s, "att-ta", 1, 2, `{"kind":"thinking","data":{"text":"x"}}`)
	setTask(t, s, "ta", "paused", "")
	mustTurn(t, s, turnReq("s1", "tb", alice)) // ta 的 control_accepted，tb 的 task_created
	mustAttempt(t, s, "tb")
	workerEvents(t, s, "att-tb", 1, 1, `{"kind":"thinking","data":{"text":"y"}}`)
	workerEvents(t, s, "att-ta", 3, 3, `{"kind":"thinking","data":{"text":"z"}}`)
	if err := s.AppendHostEvent(ctx, "ta", "", "probe", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	want := []string{"ta/host/task_created", "/session/session_state", "ta/host/attempt_created", "ta/worker/progress", "ta/worker/progress",
		"ta/host/control_accepted", "tb/host/task_created", "tb/host/attempt_created",
		"tb/worker/progress", "ta/worker/progress", "ta/host/probe"}
	recs, err := a.ListSessionEvents(ctx, "s1", 0, 100)
	if err != nil || len(recs) != len(want) {
		t.Fatalf("ListSessionEvents = %d 条, %v；期望 %d", len(recs), err, len(want))
	}
	for i, r := range recs {
		if got := r.TaskID + "/" + r.Source + "/" + r.Type; r.SessionSeq != int64(i+1) || got != want[i] {
			t.Fatalf("第 %d 条：seq %d %s，期望 seq %d %s", i, r.SessionSeq, got, i+1, want[i])
		}
	}
	if recs[1].TaskSeq != 0 || string(recs[1].Payload) != `{"state": "idle"}` || recs[3].AttemptID != "att-ta" || recs[3].TaskSeq != 3 {
		t.Fatalf("记录字段：%+v / %+v", recs[1], recs[3])
	}
	page, err := a.ListSessionEvents(ctx, "s1", 5, 3)
	if err != nil || len(page) != 3 || page[0].SessionSeq != 6 || page[2].SessionSeq != 8 {
		t.Fatalf("分页 = %+v, %v", page, err)
	}
	if n := count(t, s, "SELECT count(*) FROM events WHERE task_id = 'solo' AND session_id IS NULL AND session_seq IS NULL"); n == 0 ||
		n != count(t, s, "SELECT count(*) FROM events WHERE task_id = 'solo'") {
		t.Fatal("独立任务的事件不应有会话序号")
	}
	expectSessionSeqContiguous(t, s, "s1")
	expectNoDBViolations(t, s)
}

// TestSessionEventSeqConcurrent：两个 goroutine 分别向同一会话的两个 turn 追加事件，同时第三个 goroutine 做生命周期
// Transition，session_seq 无缺口无重复（建议 -count=20 运行）。
func TestSessionEventSeqConcurrent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice := mustUser(t, s, "alice")
	newSession(t, s, "s1", alice)
	mustTurn(t, s, turnReq("s1", "ta", alice))
	setTask(t, s, "ta", "paused", "")
	mustTurn(t, s, turnReq("s1", "tb", alice))
	if _, err := s.Transition(ctx, session.Transition{SessionID: "s1", From: []string{session.StatusCreating}, To: session.StatusIdle}); err != nil {
		t.Fatal(err)
	}
	const perTurn, transitions = 15, 8
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	start := make(chan struct{})
	for _, id := range []string{"ta", "tb"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			for i := 0; i < perTurn; i++ {
				if err := s.AppendHostEvent(ctx, id, "", "probe", json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
					errs <- err
					return
				}
			}
		}(id)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < transitions; i++ {
			st, err := s.LoadSession(ctx, "s1")
			if err != nil {
				errs <- err
				return
			}
			to := session.StatusRunning
			if st.Status == session.StatusRunning {
				to = session.StatusIdle
			}
			if _, err := s.Transition(ctx, session.Transition{SessionID: "s1", FromRowVersion: st.RowVersion, From: []string{st.Status},
				To: to, Event: json.RawMessage(`{"state":"` + to + `"}`)}); err != nil {
				errs <- err
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// 2 个 task_created、1 个 control_accepted（取代 ta）、每 turn perTurn 条、transitions 条生命周期事件
	if n := expectSessionSeqContiguous(t, s, "s1"); n != 3+2*perTurn+transitions {
		t.Fatalf("会话事件应有 %d 条，得到 %d", 3+2*perTurn+transitions, n)
	}
}

// TestSessionTransition：row_version 过期为 ErrConflict；来源不符为 invalid_transition；To=idle/frozen 写时间戳；
// 事件写入 session_events，重跑同一转换不重复；SetIncarnation、AppliedWake、LastError 同事务写入。
func TestSessionTransition(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	newSession(t, s, "s1", 0)
	st, err := s.LoadSession(ctx, "s1")
	if err != nil || st.Status != session.StatusCreating || st.RowVersion != 0 || st.Desired != "active" || st.Latest != nil ||
		st.Incarnation != nil || len(st.NonTerminalTurns) != 0 {
		t.Fatalf("新会话 = %+v, %v", st, err)
	}
	idle := session.Transition{SessionID: "s1", FromRowVersion: 0, From: []string{session.StatusCreating}, To: session.StatusIdle,
		Event: json.RawMessage(`{"state":"idle"}`)}
	for i := 0; i < 2; i++ { // 第二次是重跑
		st, err = s.Transition(ctx, idle)
		if err != nil || st.Status != session.StatusIdle || st.RowVersion != 1 || st.IdleSince == nil || st.FrozenSince != nil {
			t.Fatalf("第 %d 次 creating → idle = %+v, %v", i+1, st, err)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM session_events WHERE session_id = 's1' AND event_key = 'idle:1'"); n != 1 {
		t.Fatalf("重跑不应重复追加事件：%d", n)
	}
	if _, err := s.Transition(ctx, session.Transition{SessionID: "s1", FromRowVersion: 0, From: []string{session.StatusIdle},
		To: session.StatusFrozen}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("过期的 row_version 应为 ErrConflict，得到 %v", err)
	}
	_, err = s.Transition(ctx, session.Transition{SessionID: "s1", FromRowVersion: 1, From: []string{session.StatusRunning},
		To: session.StatusFrozen})
	expectRejected(t, err, session.CodeInvalidTransition)
	inc, wake, lastErr := "inc-x", int64(3), "thaw failed"
	st, err = s.Transition(ctx, session.Transition{SessionID: "s1", FromRowVersion: 1, From: []string{session.StatusIdle},
		To: session.StatusFrozen, SetIncarnation: &inc, AppliedWake: &wake, LastError: &lastErr, Event: json.RawMessage(`{"state":"frozen"}`)})
	if err != nil || st.Status != session.StatusFrozen || st.FrozenSince == nil || st.IdleSince != nil || st.CurrentIncarnationID != "inc-x" ||
		st.AppliedWakeVersion != 3 || st.RowVersion != 2 {
		t.Fatalf("idle → frozen = %+v, %v", st, err)
	}
	if n := count(t, s, "SELECT count(*) FROM sessions WHERE session_id = 's1' AND last_error = 'thaw failed'"); n != 1 {
		t.Fatal("LastError 应写入")
	}
	empty := ""
	if st, err = s.Transition(ctx, session.Transition{SessionID: "s1", FromRowVersion: 2, From: []string{session.StatusFrozen},
		To: session.StatusRunning, SetIncarnation: &empty}); err != nil || st.CurrentIncarnationID != "" || st.FrozenSince != nil {
		t.Fatalf("frozen → running = %+v, %v", st, err)
	}
	if n := count(t, s, "SELECT count(*) FROM session_events WHERE session_id = 's1'"); n != 2 {
		t.Fatalf("应有 2 条生命周期事件，得到 %d", n)
	}
	if _, err := s.Transition(ctx, session.Transition{SessionID: "nope", From: []string{"idle"}, To: "idle"}); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的会话应为 ErrNotFound，得到 %v", err)
	}
	if ids, err := s.ListOpenSessions(ctx); err != nil || len(ids) != 1 || ids[0] != "s1" {
		t.Fatalf("ListOpenSessions = %v, %v", ids, err)
	}
}

// TestSessionIncarnations：一个会话至多一个存活 incarnation（I9）；结束后可再建；以 incarnation_id 幂等；状态 CAS；
// end_reason 以首次为准；UID 范围只保留一个；LRU 候选顺序；FinishClose 的前置条件。
func TestSessionIncarnations(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice := mustUser(t, s, "alice")
	newSession(t, s, "s1", alice)
	first := session.NewIncarnation{IncarnationID: "inc1", SessionID: "s1", EnvID: "senv1"}
	for i := 0; i < 2; i++ {
		inc, err := s.CreateIncarnation(ctx, first)
		if err != nil || inc.Status != session.IncStarting || inc.EnvID != "senv1" {
			t.Fatalf("第 %d 次 CreateIncarnation = %+v, %v", i+1, inc, err)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM environments WHERE env_id = 'senv1' AND kind = 'session' AND session_id = 's1' AND status = 'creating'"); n != 1 {
		t.Fatal("应同事务插入会话环境")
	}
	st, err := s.LoadSession(ctx, "s1")
	if err != nil || st.Incarnation == nil || st.Incarnation.IncarnationID != "inc1" || st.RowVersion != 0 {
		t.Fatalf("LoadSession 的当前 incarnation = %+v, %v", st.Incarnation, err)
	}
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc2", SessionID: "s1", EnvID: "senv2"}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("已有存活 incarnation 应为 ErrConflict，得到 %v", err)
	}
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc1", SessionID: "s1", EnvID: "other"}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("同一 ID 不同内容应为 ErrConflict，得到 %v", err)
	}
	if inc, err := s.SetIncarnationStatus(ctx, "inc1", []string{session.IncStarting}, session.IncIdle); err != nil || inc.Status != session.IncIdle {
		t.Fatalf("starting → idle = %+v, %v", inc, err)
	}
	if inc, err := s.SetIncarnationStatus(ctx, "inc1", []string{session.IncStarting}, session.IncIdle); err != nil || inc.Status != session.IncIdle {
		t.Fatalf("重跑应返回当前行：%+v, %v", inc, err)
	}
	_, err = s.SetIncarnationStatus(ctx, "inc1", []string{session.IncBusy}, session.IncFrozen)
	expectRejected(t, err, session.CodeInvalidTransition)
	if inc, err := s.EndIncarnation(ctx, "inc1", "release_timeout"); err != nil || inc.Status != session.IncEnded || inc.EndedAt == nil ||
		inc.EndReason != "release_timeout" {
		t.Fatalf("EndIncarnation = %+v, %v", inc, err)
	}
	if inc, err := s.EndIncarnation(ctx, "inc1", "other"); err != nil || inc.EndReason != "release_timeout" {
		t.Fatalf("end_reason 应以首次为准：%+v, %v", inc, err)
	}
	if st, err := s.LoadSession(ctx, "s1"); err != nil || st.CurrentIncarnationID != "" || st.Incarnation != nil {
		t.Fatalf("结束当前 incarnation 应清空 current_incarnation_id：%+v, %v", st, err)
	}
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc2", SessionID: "s1", EnvID: "senv2", FromRowVersion: 1}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("row_version 不符应为 ErrConflict，得到 %v", err)
	}
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc2", SessionID: "s1", EnvID: "senv2"}); err != nil {
		t.Fatalf("结束后应可再建：%v", err)
	}

	if err := s.SetUIDRange(ctx, "s1", "u1"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetUIDRange(ctx, "s1", "u1"); err != nil {
		t.Fatalf("同一范围重跑应成功：%v", err)
	}
	if err := s.SetUIDRange(ctx, "s1", "u2"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("另一个范围应为 ErrConflict，得到 %v", err)
	}

	for i, c := range []struct{ id, status string }{{"l1", "idle"}, {"l2", "frozen"}, {"l3", "idle"}, {"l4", "frozen"}, {"l5", "running"}} {
		newSession(t, s, c.id, 0)
		if _, err := s.pool.Exec(ctx, "UPDATE sessions SET status = $2, last_active_at = now() - make_interval(mins => $3::int) WHERE session_id = $1",
			c.id, c.status, 10-i); err != nil {
			t.Fatal(err)
		}
	}
	cands, err := s.LRUCandidates(ctx, 3)
	if err != nil || len(cands) != 3 || cands[0].SessionID != "l2" || cands[1].SessionID != "l4" || cands[2].SessionID != "l1" {
		t.Fatalf("LRU 应 frozen 先于 idle、各按 last_active_at 升序：%+v, %v", cands, err)
	}

	// FinishClose：turn 未终态、incarnation 未结束、环境未停止时 close_not_ready。
	mustTurn(t, s, turnReq("s1", "t1", alice))
	setSession(t, s, "s1", session.StatusClosing)
	st, err = s.LoadSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.NonTerminalTurns) != 1 || st.NonTerminalTurns[0].TaskID != "t1" || st.NonTerminalTurns[0].Desired != "run" {
		t.Fatalf("NonTerminalTurns = %+v", st.NonTerminalTurns)
	}
	_, err = s.FinishClose(ctx, "s1", st.RowVersion)
	expectRejected(t, err, session.CodeCloseNotReady)
	setTask(t, s, "t1", "cancelled", "")
	if _, err := s.EndIncarnation(ctx, "inc2", "session_closed"); err != nil {
		t.Fatal(err)
	}
	_, err = s.FinishClose(ctx, "s1", st.RowVersion)
	expectRejected(t, err, session.CodeCloseNotReady) // 环境尚未确认停止
	stopEnv(t, s, "senv1")
	stopEnv(t, s, "senv2")
	if _, err := s.FinishClose(ctx, "s1", st.RowVersion+1); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("row_version 不符应为 ErrConflict，得到 %v", err)
	}
	for i := 0; i < 2; i++ {
		closed, err := s.FinishClose(ctx, "s1", st.RowVersion)
		if err != nil || closed.Status != session.StatusClosed || closed.RowVersion != st.RowVersion+1 {
			t.Fatalf("第 %d 次 FinishClose = %+v, %v", i+1, closed, err)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM session_events WHERE session_id = 's1' AND payload = '{"state":"closed"}'`); n != 1 {
		t.Fatalf("应恰有一条 closed 事件，得到 %d", n)
	}
	if ids, err := s.ListOpenSessions(ctx); err != nil || slicesContain(ids, "s1") {
		t.Fatalf("closed 的会话不应在 ListOpenSessions 中：%v, %v", ids, err)
	}
}

func slicesContain(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// TestSessionCreateTurnD5：暂停中的 turn 遇到新消息时被取代（cancel/superseded）；进行中的 turn 使新消息
// turn_in_progress；会话关闭后 session_closed；request_id 重放与冲突；每用户同时 1 个运行中跨会话生效；首个 turn
// 自动命名；turn 的预算行带工具额度。
func TestSessionCreateTurnD5(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	a := s.SessionAPI()
	alice, bob := mustUser(t, s, "alice"), mustUser(t, s, "bob")
	newSession(t, s, "s1", alice)
	newSession(t, s, "s2", alice)
	newSession(t, s, "sb", bob)

	first := turnReq("s1", "ta", alice)
	first.Text = strings.Repeat("长", 45)
	if r := mustTurn(t, s, first); r.TurnID != "ta" || r.TurnIndex != 0 || r.SupersededTurnID != "" {
		t.Fatalf("第一个 turn = %+v", r)
	}
	if v, err := a.GetSession(ctx, "s1"); err != nil || v.Title != strings.Repeat("长", 40) || v.State != "idle" {
		t.Fatalf("首个 turn 应把标题设为前 40 个字符：%+v, %v", v, err)
	}
	if n := count(t, s, `SELECT count(*) FROM tasks t JOIN budgets b USING (task_id) WHERE t.task_id = 'ta' AND t.session_id = 's1'
		AND t.turn_index = 0 AND t.owner_user_id = $1 AND b.tool_call_limit = 30 AND b.limit_micro = 100`, alice); n != 1 {
		t.Fatal("turn 应带会话、序号、所有者与工具额度")
	}
	if _, err := a.CreateTurn(ctx, turnReq("s1", "tb", alice)); !errors.Is(err, api.ErrTurnInProgress) {
		t.Fatalf("queued 的 turn 应使新消息 turn_in_progress，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM api_requests WHERE request_id = 'rt-tb'"); n != 0 {
		t.Fatal("被拒绝的消息不应留下 request 记录")
	}

	setTask(t, s, "ta", "paused", "awaiting_input")
	res := mustTurn(t, s, turnReq("s1", "tb", alice))
	if res.TurnID != "tb" || res.TurnIndex != 1 || res.SupersededTurnID != "ta" || res.Replayed {
		t.Fatalf("D5 = %+v", res)
	}
	if n := count(t, s, `SELECT count(*) FROM task_control WHERE task_id = 'ta' AND desired = 'cancel' AND reason = 'superseded'
		AND control_version = 2`); n != 1 {
		t.Fatal("暂停中的 turn 应得到 cancel/superseded 控制")
	}
	if n := count(t, s, "SELECT count(*) FROM tasks WHERE task_id = 'tb' AND status = 'queued'"); n != 1 {
		t.Fatal("新 turn 应为 queued")
	}
	if v, err := a.GetSession(ctx, "s1"); err != nil || v.Title != strings.Repeat("长", 40) {
		t.Fatalf("之后的 turn 不改标题：%+v, %v", v, err)
	}
	again, err := a.CreateTurn(ctx, turnReq("s1", "tb", alice))
	if err != nil || !again.Replayed || again.TurnID != "tb" || again.TurnIndex != 1 || again.SupersededTurnID != "ta" {
		t.Fatalf("重放应返回原结果：%+v, %v", again, err)
	}
	conflict := turnReq("s1", "tb", alice)
	conflict.BodyHash = []byte("other")
	if _, err := a.CreateTurn(ctx, conflict); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("同 request_id 不同内容应为 ErrConflict，得到 %v", err)
	}
	other := turnReq("s2", "tb", alice) // 同 request_id 用于另一个会话
	if _, err := a.CreateTurn(ctx, other); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("另一个会话重用 request_id 应为 ErrConflict，得到 %v", err)
	}

	setTask(t, s, "ta", "cancelled", "superseded")
	setTask(t, s, "tb", "running", "")
	if _, err := a.CreateTurn(ctx, turnReq("s1", "tc", alice)); !errors.Is(err, api.ErrTurnInProgress) {
		t.Fatalf("running 的 turn 应使新消息 turn_in_progress，得到 %v", err)
	}
	if _, err := a.CreateTurn(ctx, turnReq("s2", "td", alice)); !errors.Is(err, api.ErrUserTaskRunning) {
		t.Fatalf("用户在另一会话已有 running turn 应为 ErrUserTaskRunning，得到 %v", err)
	}
	if _, err := a.CreateTurn(ctx, turnReq("sb", "te", bob)); err != nil {
		t.Fatalf("其他用户不受影响：%v", err)
	}
	if _, err := a.CreateTurn(ctx, turnReq("sb", "tf", alice)); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("他人的会话应为 ErrNotFound，得到 %v", err)
	}
	if _, err := a.CloseSession(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CreateTurn(ctx, turnReq("s1", "tg", alice)); !errors.Is(err, api.ErrSessionClosed) {
		t.Fatalf("关闭中的会话应为 ErrSessionClosed，得到 %v", err)
	}
	if again, err := a.CreateTurn(ctx, turnReq("s1", "tb", alice)); err != nil || !again.Replayed {
		t.Fatalf("关闭后重放仍返回原结果：%+v, %v", again, err)
	}
	setSession(t, s, "s1", session.StatusClosed)
	if _, err := a.CreateTurn(ctx, turnReq("s1", "th", alice)); !errors.Is(err, api.ErrSessionClosed) {
		t.Fatalf("已关闭的会话应为 ErrSessionClosed，得到 %v", err)
	}
	turns, err := a.ListTurns(ctx, "s1", -1, 10)
	if err != nil || len(turns) != 2 || turns[0].TurnID != "ta" || turns[0].Status != "cancelled" || turns[0].StatusReason != "superseded" ||
		turns[0].Text != strings.Repeat("长", 45) || !turns[0].DeepResearch || turns[0].ToolCallLimit != 30 || turns[0].Restorable ||
		turns[1].TurnID != "tb" || turns[1].Status != "running" {
		t.Fatalf("ListTurns = %+v, %v", turns, err)
	}
	if page, err := a.ListTurns(ctx, "s1", 0, 10); err != nil || len(page) != 1 || page[0].TurnID != "tb" {
		t.Fatalf("ListTurns(after 0) = %+v, %v", page, err)
	}
	expectNoDBViolations(t, s)
}

// TestSessionRestoreSeed：恢复把源 turn 的最新 checkpoint 复制为新 turn 的种子 seed-<id>（引用已授权到会话 scope，
// 同事务授权到新 task scope）；未授权到会话 scope 的引用、succeeded 的源 turn 与其他会话的 turn 不可恢复。ListTurns
// 读出 route、summary、report 与 restorable。
func TestSessionRestoreSeed(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	a := s.SessionAPI()
	alice := mustUser(t, s, "alice")
	newSession(t, s, "s1", alice)
	newSession(t, s, "s2", alice)
	state, ref, private := strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)

	mustTurn(t, s, turnReq("s1", "ta", alice))
	mustAttempt(t, s, "ta")
	workerEvents(t, s, "att-ta", 1, 2, `{"kind":"route","data":{"route":"research","forced":false}}`)
	for _, sha := range []string{state, ref} {
		authorize(t, s, sha, "task", "ta")
		authorize(t, s, sha, "session", "s1")
	}
	if _, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "ta"}, CheckpointID: "cp1",
		AttemptID: "att-ta", StepID: "s1", StateRef: state, Refs: []string{ref}}); err != nil {
		t.Fatal(err)
	}
	setTask(t, s, "ta", "cancelled", "superseded")

	restore := turnReq("s1", "tr", alice)
	restore.RestoredFromTaskID = "ta"
	res := mustTurn(t, s, restore)
	if res.TurnIndex != 1 || res.SupersededTurnID != "" {
		t.Fatalf("恢复 = %+v", res)
	}
	ts, err := s.LoadTask(ctx, "tr")
	if err != nil || ts.Latest == nil || ts.Latest.CheckpointID != "seed-cp1" || ts.Latest.StateRef != state ||
		len(ts.Latest.Refs) != 1 || ts.Latest.Refs[0] != ref || ts.Status != "queued" {
		t.Fatalf("LoadTask(tr) = %+v, %v", ts.Latest, err)
	}
	if n := count(t, s, `SELECT count(*) FROM checkpoints WHERE scope_kind = 'task' AND scope_id = 'tr' AND checkpoint_id = 'seed-cp1'
		AND commit_seq = 1 AND attempt_id = ''`); n != 1 {
		t.Fatal("种子 checkpoint 的 commit_seq 应为 1、attempt_id 为空")
	}
	if n := count(t, s, "SELECT count(*) FROM scope_blobs WHERE scope_kind = 'task' AND scope_id = 'tr'"); n != 2 {
		t.Fatalf("种子引用应授权到新 task scope，得到 %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM tasks t JOIN events e USING (task_id) WHERE t.task_id = 'tr'
		AND t.restored_from_task_id = 'ta' AND e.event_key = 'task_created' AND e.payload->>'restored_from_turn_id' = 'ta'`); n != 1 {
		t.Fatal("新 turn 应记录 restored_from_task_id 与 task_created.restored_from_turn_id")
	}
	if ok, err := a.TurnBlobAuthorized(ctx, "tr", ref); err != nil || !ok {
		t.Fatalf("TurnBlobAuthorized(tr) = %v, %v", ok, err)
	}

	// 不可恢复：succeeded；引用未授权到会话 scope；其他会话的 turn。
	setTask(t, s, "tr", "succeeded", "")
	if _, err := s.pool.Exec(ctx, `UPDATE tasks SET result_json = '{"summary":"总结","outputs":[{"artifact_id":"report","version":2,"sha256":"x"}]}'
		WHERE task_id = 'tr'`); err != nil {
		t.Fatal(err)
	}
	fromSucceeded := turnReq("s1", "tx", alice)
	fromSucceeded.RestoredFromTaskID = "tr"
	if _, err := a.CreateTurn(ctx, fromSucceeded); !errors.Is(err, api.ErrNotRestorable) {
		t.Fatalf("succeeded 的源 turn 应为 ErrNotRestorable，得到 %v", err)
	}
	mustTurn(t, s, turnReq("s1", "tp", alice))
	mustAttempt(t, s, "tp")
	authorize(t, s, private, "task", "tp") // 只授权到 task scope
	if _, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "tp"}, CheckpointID: "cp2",
		AttemptID: "att-tp", StepID: "s1", State: json.RawMessage(`{"x":1}`), Refs: []string{private}}); err != nil {
		t.Fatal(err)
	}
	setTask(t, s, "tp", "failed", "worker_error")
	fromPrivate := turnReq("s1", "ty", alice)
	fromPrivate.RestoredFromTaskID = "tp"
	if _, err := a.CreateTurn(ctx, fromPrivate); !errors.Is(err, api.ErrNotRestorable) {
		t.Fatalf("引用未授权到会话 scope 应为 ErrNotRestorable，得到 %v", err)
	}
	fromOther := turnReq("s2", "tz", alice)
	fromOther.RestoredFromTaskID = "ta"
	if _, err := a.CreateTurn(ctx, fromOther); !errors.Is(err, api.ErrNotRestorable) {
		t.Fatalf("其他会话的 turn 应为 ErrNotRestorable，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM tasks WHERE task_id IN ('tx', 'ty', 'tz')"); n != 0 {
		t.Fatal("被拒绝的恢复不应创建任务")
	}

	turns, err := a.ListTurns(ctx, "s1", -1, 10)
	if err != nil || len(turns) != 3 {
		t.Fatalf("ListTurns = %+v, %v", turns, err)
	}
	ta, tr, tp := turns[0], turns[1], turns[2]
	if ta.Route != "research" || !ta.Restorable || ta.Status != "cancelled" {
		t.Fatalf("ta = %+v", ta)
	}
	if tr.Summary != "总结" || tr.Report == nil || tr.Report.Version != 2 || tr.RestoredFromTurnID != "ta" || tr.Restorable || tr.Route != "" {
		t.Fatalf("tr = %+v", tr)
	}
	if tp.Status != "failed" || tp.UserMessage != api.FailedTurnMessage || !tp.Restorable || tp.StatusReason != "worker_error" {
		t.Fatalf("tp = %+v", tp)
	}
	expectNoDBViolations(t, s)
}

// TestSessionTurnControl：stop = pause（queued|running）；continue/finish 要求 paused 且不在等待回答；answer 要求
// 等待回答；resume_directive 同事务写入；恢复受每用户 1 个运行中约束；以 request_id 幂等。
func TestSessionTurnControl(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	a := s.SessionAPI()
	alice := mustUser(t, s, "alice")
	newSession(t, s, "s1", alice)
	newSession(t, s, "s2", alice)
	ctl := func(id, action, taskID string, answers string) (api.ControlResult, error) {
		req := api.TurnControlRequest{RequestID: id, BodyHash: []byte("h-" + id), TaskID: taskID, Action: action}
		if answers != "" {
			req.Answers = json.RawMessage(answers)
		}
		return a.TurnControl(ctx, req)
	}
	directive := func(taskID string) string {
		var d string
		if err := s.pool.QueryRow(ctx, "SELECT COALESCE(resume_directive::text, '') FROM tasks WHERE task_id = $1", taskID).Scan(&d); err != nil {
			t.Fatal(err)
		}
		return d
	}

	mustTurn(t, s, turnReq("s1", "ta", alice))
	if _, err := ctl("c0", "continue", "ta", ""); !errors.Is(err, api.ErrInvalidTurnState) {
		t.Fatalf("queued 的 turn continue 应为 ErrInvalidTurnState，得到 %v", err)
	}
	r, err := ctl("c1", "stop", "ta", "")
	if err != nil || r.TaskID != "ta" || r.ControlVersion != 2 {
		t.Fatalf("stop = %+v, %v", r, err)
	}
	if again, err := ctl("c1", "stop", "ta", ""); err != nil || !again.Replayed || again.ControlVersion != 2 {
		t.Fatalf("重放 = %+v, %v", again, err)
	}
	setTask(t, s, "ta", "paused", "awaiting_input")
	if _, err := ctl("c2", "continue", "ta", ""); !errors.Is(err, api.ErrInvalidTurnState) {
		t.Fatalf("等待回答的 turn continue 应为 ErrInvalidTurnState，得到 %v", err)
	}
	if _, err := ctl("c3", "stop", "ta", ""); !errors.Is(err, api.ErrInvalidTurnState) {
		t.Fatalf("paused 的 turn stop 应为 ErrInvalidTurnState，得到 %v", err)
	}

	// 另一个会话中有 running 的 turn 时，恢复（answer）使用户超过 1 个运行中。
	mustTurn(t, s, turnReq("s2", "tb", alice))
	setTask(t, s, "tb", "running", "")
	if _, err := ctl("c4", "answer", "ta", `[{"question_id":"q1","choice":"A"}]`); !errors.Is(err, api.ErrUserTaskRunning) {
		t.Fatalf("应为 ErrUserTaskRunning，得到 %v", err)
	}
	setTask(t, s, "tb", "paused", "")
	r, err = ctl("c5", "answer", "ta", `[{"question_id":"q1","choice":"A"}]`)
	if err != nil || r.ControlVersion != 3 {
		t.Fatalf("answer = %+v, %v", r, err)
	}
	if d := directive("ta"); d != `{"kind": "answer", "answers": [{"choice": "A", "question_id": "q1"}]}` {
		t.Fatalf("answer 的 resume_directive = %s", d)
	}
	if n := count(t, s, "SELECT count(*) FROM task_control WHERE task_id = 'ta' AND desired = 'run'"); n != 1 {
		t.Fatal("answer 应写 desired = run")
	}
	setTask(t, s, "ta", "cancelled", "")
	if _, err := ctl("c6", "finish", "tb", ""); err != nil {
		t.Fatal(err)
	}
	if d := directive("tb"); d != `{"kind": "finish_now"}` {
		t.Fatalf("finish 的 resume_directive = %s", d)
	}
	if _, err := ctl("c7", "answer", "tb", ""); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("answer 缺少 answers 应为 ErrInvalid，得到 %v", err)
	}
	fixture(t, s, "solo")
	if _, err := ctl("c8", "stop", "solo", ""); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("独立任务不是 turn，应为 ErrNotFound，得到 %v", err)
	}
	if sid, owner, err := a.TurnSession(ctx, "tb"); err != nil || sid != "s2" || owner != alice {
		t.Fatalf("TurnSession = %s, %d, %v", sid, owner, err)
	}
	if _, _, err := a.TurnSession(ctx, "solo"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("独立任务的 TurnSession 应为 ErrNotFound，得到 %v", err)
	}
	expectNoDBViolations(t, s)
}

// TestSessionCloseListRenameWake：关闭对 queued 与 paused 的 turn 都写 cancel 控制，重复调用不再递增；用户只见自己
// 未关闭的会话，运维见全部并带 Owner；改名与唤醒；TurnBlobAuthorized 接受会话 scope。
func TestSessionCloseListRenameWake(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	a := s.SessionAPI()
	alice, bob := mustUser(t, s, "alice"), mustUser(t, s, "bob")
	newSession(t, s, "s1", alice)
	newSession(t, s, "s2", alice)
	newSession(t, s, "s3", alice)
	newSession(t, s, "sb", bob)
	newSession(t, s, "ops", 0)

	// s1：ta paused（desired pause）与 tb queued。
	mustTurn(t, s, turnReq("s1", "ta", alice))
	setTask(t, s, "ta", "paused", "")
	mustTurn(t, s, turnReq("s1", "tb", alice))
	if _, err := s.pool.Exec(ctx, "UPDATE task_control SET desired = 'pause', reason = '' WHERE task_id = 'ta'"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		v, err := a.CloseSession(ctx, "s1")
		if err != nil || v.State != "closing" || v.SessionID != "s1" {
			t.Fatalf("第 %d 次 CloseSession = %+v, %v", i+1, v, err)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM task_control WHERE task_id IN ('ta', 'tb') AND desired = 'cancel' AND reason = 'session_closed'`); n != 2 {
		t.Fatalf("queued 与 paused 的 turn 都应得到 cancel，得到 %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM session_control WHERE session_id = 's1' AND desired = 'closed' AND control_version = 1`); n != 1 {
		t.Fatal("重复关闭不应再递增 control_version")
	}
	if n := count(t, s, `SELECT sum(control_version)::int FROM task_control WHERE task_id IN ('ta', 'tb')`); n != 3+2 {
		t.Fatalf("重复关闭不应再写 turn 控制：control_version 合计 %d", n)
	}
	if _, err := a.RenameSession(ctx, "s1", "新名"); !errors.Is(err, api.ErrSessionClosed) {
		t.Fatalf("关闭中的会话改名应为 ErrSessionClosed，得到 %v", err)
	}
	if err := a.WakeSession(ctx, "w0", []byte("h"), "s1"); !errors.Is(err, api.ErrSessionClosed) {
		t.Fatalf("关闭中的会话唤醒应为 ErrSessionClosed，得到 %v", err)
	}

	setTask(t, s, "tb", "cancelled", "") // 否则 s1 中 queued 的 tb 使 alice 不能在 s2 发消息
	if v, err := a.RenameSession(ctx, "s2", "我的研究"); err != nil || v.Title != "我的研究" {
		t.Fatalf("RenameSession = %+v, %v", v, err)
	}
	mustTurn(t, s, turnReq("s2", "tc", alice))
	if v, err := a.GetSession(ctx, "s2"); err != nil || v.Title != "我的研究" || v.Owner != "" || v.InternalState != "" {
		t.Fatalf("用户命名后首个 turn 不改标题、GetSession 不含运维字段：%+v, %v", v, err)
	}
	for i := 0; i < 2; i++ {
		if err := a.WakeSession(ctx, "w1", []byte("h"), "s2"); err != nil {
			t.Fatalf("第 %d 次 WakeSession: %v", i+1, err)
		}
	}
	if err := a.WakeSession(ctx, "w1", []byte("h"), "s3"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("另一个会话重用 request_id 应为 ErrConflict，得到 %v", err)
	}
	if st, err := s.LoadSession(ctx, "s2"); err != nil || st.ControlVersion != 1 || st.WakeRequestedVersion != 1 {
		t.Fatalf("唤醒应推进 wake_requested_version：%+v, %v", st, err)
	}

	setSession(t, s, "s3", session.StatusClosed)
	mine, next, err := a.ListSessions(ctx, alice, "", 10)
	if err != nil || next != "" || len(mine) != 2 {
		t.Fatalf("用户只见自己未关闭的会话：%+v, %v", mine, err)
	}
	for _, v := range mine {
		if v.SessionID == "s3" || v.OwnerUserID != alice || v.Owner != "" || v.InternalState != "" {
			t.Fatalf("用户视图 = %+v", v)
		}
	}
	if mine[0].SessionID != "s2" { // s2 刚有新 turn，last_active_at 最新
		t.Fatalf("应按 last_active_at 倒序：%+v", mine)
	}
	var all []api.SessionView
	cursor := ""
	for page := 0; page < 10; page++ {
		views, next, err := a.ListSessions(ctx, 0, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, views...)
		if next == "" {
			break
		}
		cursor = next
	}
	owners := map[string]string{}
	for _, v := range all {
		owners[v.SessionID] = v.Owner + "/" + v.InternalState
	}
	if len(all) != 5 || owners["sb"] != "bob/creating" || owners["s3"] != "alice/closed" || owners["ops"] != "/creating" {
		t.Fatalf("运维视图 = %v", owners)
	}

	sha := strings.Repeat("e", 64)
	authorize(t, s, sha, "session", "s2")
	if ok, err := a.TurnBlobAuthorized(ctx, "tc", sha); err != nil || !ok {
		t.Fatalf("会话 scope 的 blob 应授权给其 turn：%v, %v", ok, err)
	}
	if ok, err := a.TurnBlobAuthorized(ctx, "ta", sha); err != nil || ok {
		t.Fatalf("其他会话的 turn 不应获得授权：%v, %v", ok, err)
	}
	if _, err := a.GetSession(ctx, "nope"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的会话应为 ErrNotFound，得到 %v", err)
	}
	if _, replayed, err := a.CreateSession(ctx, api.CreateSessionRequest{RequestID: "rs-s2", BodyHash: []byte("h-s2"), SessionID: "s2",
		OwnerUserID: alice}); err != nil || !replayed {
		t.Fatalf("CreateSession 重放 = %v, %v", replayed, err)
	}
	if _, _, err := a.CreateSession(ctx, api.CreateSessionRequest{RequestID: "rs-new", BodyHash: []byte("h"), SessionID: "s2",
		OwnerUserID: alice}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("重复的 session_id 应为 ErrConflict，得到 %v", err)
	}
	expectNoDBViolations(t, s)
}

// TestEvictSessionsOnRestart：frozen 与 running 的会话 → evicted、incarnation ended{lost_on_restart}、各一条事件；
// closing 只结束 incarnation；evicted 与 closed 不变；重跑无变化。
func TestEvictSessionsOnRestart(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	for _, c := range []struct{ id, status string }{
		{"sf", "frozen"}, {"sr", "running"}, {"sc", "closing"}, {"se", "evicted"}, {"si", "idle"},
	} {
		newSession(t, s, c.id, 0)
		if c.id != "se" && c.id != "si" {
			if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc-" + c.id, SessionID: c.id, EnvID: "env-" + c.id}); err != nil {
				t.Fatal(err)
			}
		}
		setSession(t, s, c.id, c.status)
	}
	got, err := s.EvictSessionsOnRestart(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"sc": "inc-sc/env-sc", "sf": "inc-sf/env-sf", "si": "/", "sr": "inc-sr/env-sr"}
	if len(got) != len(want) {
		t.Fatalf("EvictSessionsOnRestart = %+v", got)
	}
	for _, e := range got {
		if want[e.SessionID] != e.IncarnationID+"/"+e.EnvID {
			t.Fatalf("EvictSessionsOnRestart = %+v", got)
		}
	}
	for id, status := range map[string]string{"sf": "evicted", "sr": "evicted", "si": "evicted", "sc": "closing", "se": "evicted"} {
		st, err := s.LoadSession(ctx, id)
		if err != nil || st.Status != status || st.CurrentIncarnationID != "" {
			t.Fatalf("%s = %+v, %v", id, st, err)
		}
		events := count(t, s, `SELECT count(*) FROM session_events WHERE session_id = $1 AND payload = '{"state":"evicted"}'`, id)
		if wantEvents := map[bool]int{true: 1, false: 0}[id == "sf" || id == "sr" || id == "si"]; events != wantEvents {
			t.Fatalf("%s 的 evicted 事件 %d 条，期望 %d", id, events, wantEvents)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM incarnations WHERE status = 'ended' AND end_reason = 'lost_on_restart' AND ended_at IS NOT NULL"); n != 3 {
		t.Fatalf("应结束 3 个 incarnation，得到 %d", n)
	}
	if again, err := s.EvictSessionsOnRestart(ctx); err != nil || len(again) != 0 {
		t.Fatalf("重跑应无变化：%+v, %v", again, err)
	}
	if n := count(t, s, "SELECT count(*) FROM session_events"); n != 3 {
		t.Fatalf("重跑不应追加事件：%d", n)
	}
}

// ---- 按 owner 保留的 UID 范围（M4 Plan 12 Task 5；规格 §12.1、§4.5） ----

// TestOwnerUIDRange：同一 owner 的多个环境共用一段范围（重试返回原分配）；不同 owner 范围不同（I11）；
// 仍有环境未停止并完成清理时归还为冲突；之后归还，再归还为 ErrNotFound；owner 范围不出现在启动核对的未归还范围中。
func TestOwnerUIDRange(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	for _, id := range []string{"t1", "t2", "t3"} {
		fixture(t, s, id)
	}
	if err := s.SeedUIDRanges(ctx, 100000, 4096, 2); err != nil {
		t.Fatal(err)
	}
	const a1 = "owner-uid:session:s1"
	r1, err := s.AssignOwnerUIDRange(ctx, "session:s1", "env-t1", a1)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := s.AssignOwnerUIDRange(ctx, "session:s1", "env-t1", a1); err != nil || again.UIDRangeID != r1.UIDRangeID {
		t.Fatalf("重试 = %+v, %v，期望原分配 %s", again, err, r1.UIDRangeID)
	}
	r2, err := s.AssignOwnerUIDRange(ctx, "session:s2", "env-t2", "owner-uid:session:s2")
	if err != nil || r2.UIDRangeID == r1.UIDRangeID {
		t.Fatalf("另一 owner 的范围 = %+v, %v，期望不同于 %s", r2, err, r1.UIDRangeID)
	}
	stopEnv(t, s, "env-t1")
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT count(*) FROM uid_ranges WHERE uid_range_id = $1 AND state = 'assigned' AND owner_id = 'session:s1'", r1.UIDRangeID); n != 1 {
		t.Fatal("环境清理后 owner 范围应保留")
	}
	f, err := s.LoadRecoveryFacts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range f.UnreleasedRanges {
		if u.UIDRangeID == r1.UIDRangeID {
			t.Fatalf("owner 范围出现在启动核对的未归还范围中：%+v", u)
		}
	}
	if r3, err := s.AssignOwnerUIDRange(ctx, "session:s1", "env-t3", a1); err != nil || r3.UIDRangeID != r1.UIDRangeID {
		t.Fatalf("同一 owner 的新环境 = %+v, %v，期望沿用 %s", r3, err, r1.UIDRangeID)
	}
	if _, err := s.ReleaseOwnerUIDRange(ctx, "session:s1", a1); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("env-t3 未停止时归还 = %v，期望冲突", err)
	}
	stopEnv(t, s, "env-t3")
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t3", State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ReleaseOwnerUIDRange(ctx, "session:s1", a1); err != nil || got.State != "free" {
		t.Fatalf("归还 = %+v, %v", got, err)
	}
	if _, err := s.ReleaseOwnerUIDRange(ctx, "session:s1", a1); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("重复归还 = %v，期望 ErrNotFound（coordinator 视为幂等）", err)
	}
}

// --- sub-run（migration 0007，M4 Plan 14 Task 3；规格 §6、§8.4、§13） ---

// undo0007 把库还原为只应用了 0006 的形状；回滚到更早迁移的测试先执行它（再执行 undo0006）。
const undo0007 = `DROP INDEX reservations_subrun_held;
	ALTER TABLE reservations DROP CONSTRAINT reservations_subrun_fk;
	ALTER TABLE calls DROP CONSTRAINT calls_subrun_fk;
	DROP TABLE subrun_budgets, subruns;
	DELETE FROM schema_migrations WHERE version >= 7;
`

func capOf(v int64) *int64 { return &v }

// subrunDef 是一个合法定义：parent_step_id = step，deadline 60 s。
func subrunDef(step string, budgetCap *int64) subrun.Definition {
	return subrun.Definition{ParentStepID: step, BudgetCapMicro: budgetCap, DeadlineMS: 60_000}
}

func mustStartSubrun(t *testing.T, s *Store, taskID, attemptID, subrunID string) subrun.Record {
	t.Helper()
	r, err := s.StartSubrun(context.Background(), taskID, attemptID, subrunID, subrunDef("plan", nil))
	if err != nil {
		t.Fatalf("StartSubrun(%s): %v", subrunID, err)
	}
	return r
}

// subrunTx 在一个事务中执行包内事务辅助（测试包装：生产中由 CommitCheckpoint、CreateAttempt、裁决事务调用）。
func subrunTx(s *Store, fn txFunc) error {
	return s.run(context.Background(), "subrunTest", "", fn)
}

// subrunRow 返回 status|bound_attempt_id|result_ref|failure_reason|cancel_reason|ended?。
func subrunRow(t *testing.T, s *Store, taskID, subrunID string) string {
	t.Helper()
	var out string
	if err := s.pool.QueryRow(context.Background(), `SELECT concat_ws('|', status, bound_attempt_id, COALESCE(result_ref, '-'),
			failure_reason, cancel_reason, (ended_at IS NOT NULL)::text)
		FROM subruns WHERE task_id = $1 AND subrun_id = $2`, taskID, subrunID).Scan(&out); err != nil {
		t.Fatalf("读取 sub-run %s/%s: %v", taskID, subrunID, err)
	}
	return out
}

// retrySubrunTask 走一次故障重试（retryWithNewAttempt）并撤销旧 attempt 的访问（生产中由 runner 撤销；I7）。
func retrySubrunTask(t *testing.T, s *Store, taskID string) {
	t.Helper()
	retryWithNewAttempt(t, s, taskID)
	if err := s.RevokeAttemptAccess(context.Background(), "att-"+taskID, "test"); err != nil {
		t.Fatal(err)
	}
}

// TestMigration0007：全新库迁移到 0007；0006 的库（含已有任务与调用）升级到 0007；calls/reservations 的 subrun_id
// 外键对 root（NULL）不生效、对未知 sub-run 生效。
func TestMigration0007(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	if n := count(t, s, "SELECT max(version) FROM schema_migrations"); n != 7 {
		t.Fatalf("应迁移到 0007，得到 %d", n)
	}
	gwFixture(t, s, "t1", 1000)
	beginCall(t, s, "t1", "c1")
	if _, err := s.pool.Exec(ctx, undo0007); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT count(*) FROM calls WHERE task_id = 't1' AND subrun_id IS NULL"); n != 1 {
		t.Fatal("升级后 root 调用应保留")
	}
	_, err := s.pool.Exec(ctx, "UPDATE calls SET subrun_id = 'st9' WHERE task_id = 't1'")
	if sqlState(err) != "23503" {
		t.Fatalf("未知 sub-run 的调用应违反外键，得到 %v", err)
	}
	mustStartSubrun(t, s, "t1", "att-t1", "st1")
	if _, err := s.pool.Exec(ctx, "UPDATE calls SET subrun_id = 'st1' WHERE task_id = 't1'"); err != nil {
		t.Fatalf("已存在的 sub-run 的调用：%v", err)
	}
	expectNoDBViolations(t, s)
}

// TestSubrunStart：4 个逻辑 sub-run 上限（第 5 个 subrun_limit）；同 ID 同定义重发返回原记录、deadline_at 不变；
// 同 ID 不同定义 conflict；非当前 attempt stale_attempt；终态上重发 subrun_closed；非法 ID/定义 ErrInvalid。
func TestSubrunStart(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	first, err := s.StartSubrun(ctx, "t1", "att-t1", "st1", subrunDef("plan", capOf(500)))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != subrun.Started || first.BoundAttemptID != "att-t1" || first.ParentStepID != "plan" ||
		first.BudgetCapMicro == nil || *first.BudgetCapMicro != 500 ||
		string(first.DefinitionHash) != string(subrunDef("plan", capOf(500)).Hash()) {
		t.Fatalf("首次启动 = %+v", first)
	}
	if d := first.DeadlineAt.Sub(first.StartedAt); d != time.Minute {
		t.Fatalf("deadline_at - started_at = %v，期望 60 s（数据库 now() + deadline_ms）", d)
	}
	if b, err := s.LoadSubrunBudget(ctx, "t1", "st1"); err != nil || b.CapMicro == nil || *b.CapMicro != 500 || b.Available() != 500 {
		t.Fatalf("sub-run 账本 = %+v, %v", b, err)
	}
	for _, id := range []string{"st2", "st3", "st4"} {
		mustStartSubrun(t, s, "t1", "att-t1", id)
	}
	if b, err := s.LoadSubrunBudget(ctx, "t1", "st2"); err != nil || b.CapMicro != nil || b.Available() != math.MaxInt64 {
		t.Fatalf("无上限的 sub-run 账本 = %+v, %v", b, err)
	}
	_, err = s.StartSubrun(ctx, "t1", "att-t1", "st5", subrunDef("plan", nil))
	expectRejected(t, err, persistence.CodeSubrunLimit)

	time.Sleep(10 * time.Millisecond)
	again, err := s.StartSubrun(ctx, "t1", "att-t1", "st1", subrunDef("plan", capOf(500)))
	if err != nil || !again.DeadlineAt.Equal(first.DeadlineAt) || !again.StartedAt.Equal(first.StartedAt) {
		t.Fatalf("同定义重发 = %+v, %v；期望原 deadline_at %v", again, err, first.DeadlineAt)
	}
	_, err = s.StartSubrun(ctx, "t1", "att-t1", "st1", subrunDef("plan", capOf(501)))
	expectRejected(t, err, persistence.CodeConflict)

	if _, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "st2", "failed", "boom"); err != nil {
		t.Fatal(err)
	}
	_, err = s.StartSubrun(ctx, "t1", "att-t1", "st2", subrunDef("plan", nil))
	expectRejected(t, err, persistence.CodeSubrunClosed)

	fixture(t, s, "t2")
	retrySubrunTask(t, s, "t2")
	_, err = s.StartSubrun(ctx, "t2", "att-t2", "st1", subrunDef("plan", nil))
	expectRejected(t, err, persistence.CodeStaleAttempt)
	if n := count(t, s, "SELECT count(*) FROM subruns WHERE task_id = 't2'"); n != 0 {
		t.Fatalf("被拒绝的启动留下了 %d 行", n)
	}

	for name, d := range map[string]subrun.Definition{
		"空 parent_step_id": {DeadlineMS: 1},
		"deadline 为 0":     {ParentStepID: "p"},
		"上限为负":             {ParentStepID: "p", DeadlineMS: 1, BudgetCapMicro: capOf(-1)},
	} {
		if _, err := s.StartSubrun(ctx, "t2", "att2-t2", "st1", d); !errors.Is(err, persistence.ErrInvalid) {
			t.Errorf("%s: %v，期望 ErrInvalid", name, err)
		}
	}
	for _, id := range []string{"root", "St1", "_a", ""} {
		if _, err := s.StartSubrun(ctx, "t2", "att2-t2", id, subrunDef("plan", nil)); !errors.Is(err, persistence.ErrInvalid) {
			t.Errorf("ID %q: %v，期望 ErrInvalid", id, err)
		}
	}
	expectNoDBViolations(t, s)
}

// TestSubrunStartConcurrentLimit：已有 3 个 sub-run 时并发启动第 4、第 5 个不同 ID，恰一个得到 subrun_limit。
func TestSubrunStartConcurrentLimit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	for i := 0; i < 5; i++ {
		taskID := fmt.Sprintf("t%d", i)
		fixture(t, s, taskID)
		for _, id := range []string{"st1", "st2", "st3"} {
			mustStartSubrun(t, s, taskID, "att-"+taskID, id)
		}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for j, id := range []string{"st4", "st5"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[j] = s.StartSubrun(ctx, taskID, "att-"+taskID, id, subrunDef("plan", nil))
			}()
		}
		wg.Wait()
		limited := 0
		for _, err := range errs {
			var rej *persistence.RejectedError
			switch {
			case err == nil:
			case errors.As(err, &rej) && rej.Code == persistence.CodeSubrunLimit:
				limited++
			default:
				t.Fatalf("%s: 意外错误 %v", taskID, err)
			}
		}
		if limited != 1 || count(t, s, "SELECT count(*) FROM subruns WHERE task_id = $1", taskID) != 4 {
			t.Fatalf("%s: %d 个 subrun_limit（期望 1），错误 %v", taskID, limited, errs)
		}
	}
}

// TestSubrunEndAndCancel：subrun_end 与取消请求经状态机转换：succeeded → end_proposed；deadline 取消后 cancelled →
// timed_out；orchestrator 取消后 → cancelled；未请求取消时 cancelled 视为 orchestrator；failed 截断原因；
// cancel_requested 上的 succeeded 为 invalid_transition；取消请求在 cancel_requested 与终态上幂等。
func TestSubrunEndAndCancel(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	for _, id := range []string{"a", "b", "c", "d"} {
		mustStartSubrun(t, s, "t1", "att-t1", id)
	}
	if r, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "a", "succeeded", "done"); err != nil || r.Status != subrun.EndProposed {
		t.Fatalf("succeeded = %+v, %v", r, err)
	}
	if n := count(t, s, "SELECT count(*) FROM subruns WHERE subrun_id = 'a' AND end_summary = 'done' AND ended_at IS NULL"); n != 1 {
		t.Fatal("end_proposed 应保存 summary 且未结束")
	}
	_, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "a", "succeeded", "again")
	expectRejected(t, err, persistence.CodeInvalidTransition)

	if r, err := s.RequestSubrunCancel(ctx, "t1", "b", subrun.ReasonDeadline); err != nil || r.Status != subrun.CancelRequested || r.CancelReason != subrun.ReasonDeadline {
		t.Fatalf("deadline 取消请求 = %+v, %v", r, err)
	}
	if r, err := s.RequestSubrunCancel(ctx, "t1", "b", subrun.ReasonOrchestrator); err != nil || r.CancelReason != subrun.ReasonDeadline {
		t.Fatalf("重复取消请求应返回原记录：%+v, %v", r, err)
	}
	_, err = s.ProposeSubrunEnd(ctx, "t1", "att-t1", "b", "succeeded", "late")
	expectRejected(t, err, persistence.CodeInvalidTransition)
	if r, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "b", "cancelled", ""); err != nil || r.Status != subrun.TimedOut || r.EndedAt.IsZero() {
		t.Fatalf("deadline 取消后 cancelled = %+v, %v，期望 timed_out", r, err)
	}
	if r, err := s.RequestSubrunCancel(ctx, "t1", "b", subrun.ReasonOrchestrator); err != nil || r.Status != subrun.TimedOut {
		t.Fatalf("终态上的取消请求应幂等：%+v, %v", r, err)
	}

	if _, err := s.RequestSubrunCancel(ctx, "t1", "c", subrun.ReasonOrchestrator); err != nil {
		t.Fatal(err)
	}
	if r, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "c", "cancelled", ""); err != nil || r.Status != subrun.Cancelled {
		t.Fatalf("orchestrator 取消后 cancelled = %+v, %v", r, err)
	}
	if r, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "c", "cancelled", ""); err != nil || r.Status != subrun.Cancelled {
		t.Fatalf("终态的重复确认 = %+v, %v", r, err)
	}

	long := strings.Repeat("错", 100) // 300 字节
	if r, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "a", "failed", long); err != nil || r.Status != subrun.Failed ||
		len(r.FailureReason) > 256 || !strings.HasPrefix(long, r.FailureReason) || len(r.FailureReason) < 250 {
		t.Fatalf("failed = %+v, %v，期望按 UTF-8 截断到 256 字节内", r, err)
	}

	if r, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "d", "cancelled", ""); err != nil || r.Status != subrun.Cancelled || r.CancelReason != subrun.ReasonOrchestrator {
		t.Fatalf("未请求取消时 cancelled = %+v, %v，期望 orchestrator 取消", r, err)
	}

	if _, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "zz", "failed", ""); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("未知 sub-run = %v，期望 ErrNotFound", err)
	}
	if _, err := s.RequestSubrunCancel(ctx, "t1", "zz", subrun.ReasonOrchestrator); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("未知 sub-run 的取消 = %v，期望 ErrNotFound", err)
	}
	if _, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "a", "done", ""); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("未知状态 = %v，期望 ErrInvalid", err)
	}
	if _, err := s.RequestSubrunCancel(ctx, "t1", "a", "whim"); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("未知原因 = %v，期望 ErrInvalid", err)
	}
	_, err = s.ProposeSubrunEnd(ctx, "t1", "att-x", "a", "failed", "")
	expectRejected(t, err, persistence.CodeStaleAttempt)

	list, err := s.ListSubruns(ctx, "t1")
	if err != nil || len(list) != 4 || list[0].SubrunID != "a" || list[3].SubrunID != "d" {
		t.Fatalf("ListSubruns = %+v, %v", list, err)
	}
	expectNoDBViolations(t, s)
}

// TestSubrunCheckpointTx：checkpoint 列出 completed + result_ref 时 end_proposed → completed；cancel_requested 上
// 列 completed 为 invalid_transition 且整个事务回滚（E41）；列出未知 ID 为 invalid_transition；started 为空转换。
func TestSubrunCheckpointTx(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	for _, id := range []string{"a", "b", "c"} {
		mustStartSubrun(t, s, "t1", "att-t1", id)
	}
	if _, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "a", "succeeded", "ok"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestSubrunCancel(ctx, "t1", "b", subrun.ReasonOrchestrator); err != nil {
		t.Fatal(err)
	}
	apply := func(entries ...protocol.CheckpointSubrun) error {
		return subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
			return applyCheckpointSubrunsTx(ctx, tx, "t1", entries)
		})
	}
	// E41：一个合法转换与一个非法转换在同一 checkpoint 中，整体不提交。
	err := apply(protocol.CheckpointSubrun{SubrunID: "a", Status: "completed", ResultRef: "sha-a"},
		protocol.CheckpointSubrun{SubrunID: "b", Status: "completed", ResultRef: "sha-b"})
	expectRejected(t, err, persistence.CodeInvalidTransition)
	if got := subrunRow(t, s, "t1", "a"); got != "end_proposed|att-t1|-|||false" {
		t.Fatalf("回滚后 a = %s", got)
	}
	err = apply(protocol.CheckpointSubrun{SubrunID: "zz", Status: "started"})
	expectRejected(t, err, persistence.CodeInvalidTransition)
	err = apply(protocol.CheckpointSubrun{SubrunID: "a", Status: "completed"})
	expectRejected(t, err, persistence.CodeInvalidTransition)

	if err := apply(protocol.CheckpointSubrun{SubrunID: "a", Status: "completed", ResultRef: "sha-a"},
		protocol.CheckpointSubrun{SubrunID: "b", Status: "cancelled"},
		protocol.CheckpointSubrun{SubrunID: "c", Status: "started"}); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{
		"a": "completed|att-t1|sha-a|||true",
		"b": "cancelled|att-t1|-||orchestrator|true",
		"c": "started|att-t1|-|||false",
	} {
		if got := subrunRow(t, s, "t1", id); got != want {
			t.Errorf("%s = %s，期望 %s", id, got, want)
		}
	}
	// 同一终态的重复确认可以；completed 的 result_ref 不能改。
	if err := apply(protocol.CheckpointSubrun{SubrunID: "a", Status: "completed", ResultRef: "sha-a"}); err != nil {
		t.Fatalf("重复确认：%v", err)
	}
	err = apply(protocol.CheckpointSubrun{SubrunID: "a", Status: "completed", ResultRef: "sha-other"})
	expectRejected(t, err, persistence.CodeInvalidTransition)
	if err := apply(protocol.CheckpointSubrun{SubrunID: "c", Status: "failed"}); err != nil {
		t.Fatal(err)
	}
	if got := subrunRow(t, s, "t1", "c"); !strings.HasPrefix(got, "failed|") || !strings.HasSuffix(got, "|true") {
		t.Fatalf("checkpoint failed 后 c = %s", got)
	}
	expectNoDBViolations(t, s)
}

// TestSubrunRebindTx：恢复时 started 未过期 → 重新绑定；deadline 已过 → timed_out（E45）；cancel_requested →
// cancelled（E40）；end_proposed → 回到 started 并重新绑定（E42）；终态不变。
func TestSubrunRebindTx(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	for _, id := range []string{"a", "b", "c", "d"} {
		mustStartSubrun(t, s, "t1", "att-t1", id)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE subruns SET deadline_at = now() - interval '1s' WHERE subrun_id = 'b'"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestSubrunCancel(ctx, "t1", "c", subrun.ReasonOrchestrator); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "d", "succeeded", "ok"); err != nil {
		t.Fatal(err)
	}
	fixture(t, s, "t2")
	mustStartSubrun(t, s, "t2", "att-t2", "x")
	if _, err := s.ProposeSubrunEnd(ctx, "t2", "att-t2", "x", "failed", "bad"); err != nil {
		t.Fatal(err)
	}
	retrySubrunTask(t, s, "t1")
	retrySubrunTask(t, s, "t2")

	var got []protocol.ResumeSubrun
	if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		got, err = rebindSubrunsTx(ctx, tx, "t1", "att2-t1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want := []protocol.ResumeSubrun{{SubrunID: "a", Status: "started"}, {SubrunID: "b", Status: "timed_out"},
		{SubrunID: "c", Status: "cancelled"}, {SubrunID: "d", Status: "started"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rebind = %+v，期望 %+v", got, want)
	}
	for id, w := range map[string]string{
		"a": "started|att2-t1|-|||false",
		"b": "timed_out|att-t1|-||deadline|true",
		"c": "cancelled|att-t1|-||orchestrator|true",
		"d": "started|att2-t1|-|||false",
	} {
		if g := subrunRow(t, s, "t1", id); g != w {
			t.Errorf("%s = %s，期望 %s", id, g, w)
		}
	}
	// 重新绑定后旧 attempt 不能再用这个 sub-run 发起调用，新 attempt 可以。
	if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
		_, err := lockSubrunForCallTx(ctx, tx, "t1", "a", "att-t1")
		return err
	}); err == nil {
		t.Fatal("旧 attempt 应得到 subrun_closed")
	} else {
		expectRejected(t, err, persistence.CodeSubrunClosed)
	}
	if again, err := s.StartSubrun(ctx, "t1", "att2-t1", "d", subrunDef("plan", nil)); err != nil || again.Status != subrun.Started {
		t.Fatalf("恢复后以同定义重发 = %+v, %v", again, err)
	}

	var t2 []protocol.ResumeSubrun
	if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		t2, err = rebindSubrunsTx(ctx, tx, "t2", "att2-t2")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(t2) != 1 || t2[0].Status != "failed" || subrunRow(t, s, "t2", "x") != "failed|att-t2|-|bad||true" {
		t.Fatalf("终态应保持：%+v / %s", t2, subrunRow(t, s, "t2", "x"))
	}
	expectNoDBViolations(t, s)
}

// TestSubrunCloseOpenTx：cancelled 裁决把非终态置 cancelled（task_cancel）；succeeded 裁决置 failed
// （not_completed_at_result）；其他裁决不改动；终态不受影响。
func TestSubrunCloseOpenTx(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	for _, taskID := range []string{"t1", "t2", "t3"} {
		fixture(t, s, taskID)
		mustStartSubrun(t, s, taskID, "att-"+taskID, "open")
		mustStartSubrun(t, s, taskID, "att-"+taskID, "done")
		if _, err := s.ProposeSubrunEnd(ctx, taskID, "att-"+taskID, "done", "failed", "x"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.RequestSubrunCancel(ctx, "t2", "open", subrun.ReasonDeadline); err != nil {
		t.Fatal(err)
	}
	for taskID, v := range map[string]string{"t1": "cancelled", "t2": "succeeded", "t3": "failed"} {
		if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error { return closeOpenSubrunsTx(ctx, tx, taskID, v) }); err != nil {
			t.Fatalf("%s: %v", taskID, err)
		}
	}
	for key, want := range map[string]string{
		"t1/open": "cancelled|att-t1|-|task_cancel|task_cancel|true",
		"t2/open": "failed|att-t2|-|not_completed_at_result|deadline|true",
		"t3/open": "started|att-t3|-|||false",
		"t1/done": "failed|att-t1|-|x||true",
		"t2/done": "failed|att-t2|-|x||true",
	} {
		taskID, id, _ := strings.Cut(key, "/")
		if got := subrunRow(t, s, taskID, id); got != want {
			t.Errorf("%s = %s，期望 %s", key, got, want)
		}
	}
	expectNoDBViolations(t, s)
}

// TestSubrunCallLockAndBudget：lockSubrunForCallTx 只接受 started 且绑定当前 attempt 的 sub-run（end_proposed、
// cancel_requested、未知 ID 均 subrun_closed）；sub-run 层账本的加锁与增量调整，负值被约束拒绝。
func TestSubrunCallLockAndBudget(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	if _, err := s.StartSubrun(ctx, "t1", "att-t1", "a", subrunDef("plan", capOf(1000))); err != nil {
		t.Fatal(err)
	}
	mustStartSubrun(t, s, "t1", "att-t1", "b")
	mustStartSubrun(t, s, "t1", "att-t1", "c")
	if _, err := s.ProposeSubrunEnd(ctx, "t1", "att-t1", "b", "succeeded", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestSubrunCancel(ctx, "t1", "c", subrun.ReasonDeadline); err != nil {
		t.Fatal(err)
	}
	lock := func(id, attemptID string) (subrun.Record, error) {
		var r subrun.Record
		err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
			var err error
			r, err = lockSubrunForCallTx(ctx, tx, "t1", id, attemptID)
			return err
		})
		return r, err
	}
	if r, err := lock("a", "att-t1"); err != nil || r.Status != subrun.Started {
		t.Fatalf("started = %+v, %v", r, err)
	}
	for _, id := range []string{"b", "c", "zz"} {
		_, err := lock(id, "att-t1")
		expectRejected(t, err, persistence.CodeSubrunClosed)
	}

	if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
		b, err := lockSubrunBudgetTx(ctx, tx, "t1", "a")
		if err != nil {
			return err
		}
		if b.Available() != 1000 {
			return fmt.Errorf("可用 %d", b.Available())
		}
		if err := adjustSubrunBudgetTx(ctx, tx, "t1", "a", 300, 0, 0); err != nil {
			return err
		}
		return adjustSubrunBudgetTx(ctx, tx, "t1", "a", -300, 200, 50)
	}); err != nil {
		t.Fatal(err)
	}
	if b, err := s.LoadSubrunBudget(ctx, "t1", "a"); err != nil || b.ReservedMicro != 0 || b.SpentMicro != 200 || b.UnknownMicro != 50 || b.Available() != 750 {
		t.Fatalf("账本 = %+v, %v", b, err)
	}
	if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
		return adjustSubrunBudgetTx(ctx, tx, "t1", "a", -1, 0, 0)
	}); err == nil {
		t.Fatal("预留为负应被约束拒绝")
	}
	if err := subrunTx(s, func(ctx context.Context, tx pgx.Tx) error {
		_, err := lockSubrunBudgetTx(ctx, tx, "t1", "zz")
		return err
	}); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("未知 sub-run 的账本 = %v", err)
	}
	if _, err := s.LoadSubrunBudget(ctx, "t1", "zz"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("LoadSubrunBudget 未知 = %v", err)
	}
}

// authorize 登记 blob 并授权到 (kind, id) scope。
func authorize(t *testing.T, s *Store, sha, kind, id string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `INSERT INTO blobs (sha256, size) VALUES ($1, 1) ON CONFLICT DO NOTHING;`, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), "INSERT INTO scope_blobs (scope_kind, scope_id, sha256) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
		kind, id, sha); err != nil {
		t.Fatal(err)
	}
}

// ==== M4 Plan 12 Task 4：会话 turn 的 attempt 与裁决、session scope、工具额度（本段到此结束前不含其他任务的用例） ====

// liveSession 建立会话 sid（所有者 owner）及其空闲的 incarnation inc-<sid>（环境 senv-<sid>），会话为 idle。
func liveSession(t *testing.T, s *Store, sid string, owner int64) {
	t.Helper()
	ctx := context.Background()
	newSession(t, s, sid, owner)
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc-" + sid, SessionID: sid, EnvID: "senv-" + sid}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetIncarnationStatus(ctx, "inc-"+sid, []string{session.IncStarting}, session.IncIdle); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, session.Transition{SessionID: sid, FromRowVersion: 0, From: []string{session.StatusCreating},
		To: session.StatusIdle}); err != nil {
		t.Fatal(err)
	}
}

// turnAttempt 是会话 turn 的第 no 个 attempt（att<no>-<taskID>）在会话 sid 的当前 incarnation 中创建的请求。
func turnAttempt(sid, taskID string, no int64) task.NewAttempt {
	return task.NewAttempt{TaskID: taskID, AttemptID: fmt.Sprintf("att%d-%s", no, taskID), AttemptNo: no, EnvID: "senv-" + sid,
		SessionID: sid, IncarnationID: "inc-" + sid}
}

// turnVerdictFor 是 turnAttempt 的 attempt 在 control_version cv 下的裁决。
func turnVerdictFor(a task.NewAttempt, cv int64, status, reason string) task.Verdict {
	return task.Verdict{AttemptID: a.AttemptID, TaskID: a.TaskID, ControlVersion: cv, FromStatus: "starting", AttemptStatus: "ended",
		OutcomeClass: status, TaskStatus: status, TaskStatusReason: reason, EventType: "attempt_ended",
		EventPayload: json.RawMessage(`{"status":"` + status + `"}`)}
}

// releaseIncarnation 模拟 session actor 的释放：incarnation releasing → idle、会话 running → idle。
func releaseIncarnation(t *testing.T, s *Store, sid string) {
	t.Helper()
	if _, err := s.SetIncarnationStatus(context.Background(), "inc-"+sid, []string{session.IncReleasing}, session.IncIdle); err != nil {
		t.Fatal(err)
	}
	setSession(t, s, sid, session.StatusIdle)
}

// sessionRow 返回会话的 status|current_task_id|blocked_by_task_id|latest_checkpoint_id|latest_commit_seq|incarnation 状态。
func sessionRow(t *testing.T, s *Store, sid string) string {
	t.Helper()
	var out string
	if err := s.pool.QueryRow(context.Background(), `SELECT concat_ws('|', s.status, COALESCE(s.current_task_id, '-'),
			COALESCE(s.blocked_by_task_id, '-'), COALESCE(p.latest_checkpoint_id, '-'), p.latest_commit_seq,
			(SELECT i.status FROM incarnations i WHERE i.incarnation_id = s.current_incarnation_id))
		FROM sessions s JOIN session_progress p USING (session_id) WHERE s.session_id = $1`, sid).Scan(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestSessionTurnAttemptAndVerdict 覆盖会话 turn 的 attempt 创建与裁决（§8.1、§12.3；E47 存储侧、I10）：incarnation
// 非 idle 与被其他 turn 阻塞时拒绝；阻塞者本人创建时清除阻塞并接管会话；成功裁决提交 session checkpoint 并推进指针；
// base 不一致为 session_base_mismatch；失败与 awaiting_input 不推进指针；awaiting_input 在 desired = run 时被接受并
// 阻塞会话；paused → cancelled 的 ApplyControl 清除阻塞。
func TestSessionTurnAttemptAndVerdict(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice := mustUser(t, s, "alice")
	liveSession(t, s, "s1", alice)
	mustTurn(t, s, turnReq("s1", "ta", alice))
	sha := strings.Repeat("5a", 32)
	authorize(t, s, sha, "task", "ta")

	// 准入：incarnation 非 idle、会话被其他 turn 阻塞时拒绝，且不留下任何改变。
	a1 := turnAttempt("s1", "ta", 1)
	before := snapshot(t, s, "ta")
	if _, err := s.pool.Exec(ctx, "UPDATE incarnations SET status = 'releasing' WHERE incarnation_id = 'inc-s1'"); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateAttempt(ctx, a1)
	expectRejected(t, err, persistence.CodeIncarnationNotIdle)
	if _, err := s.pool.Exec(ctx, `UPDATE incarnations SET status = 'idle' WHERE incarnation_id = 'inc-s1';
		UPDATE sessions SET blocked_by_task_id = 'other' WHERE session_id = 's1'`); err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateAttempt(ctx, a1)
	expectRejected(t, err, persistence.CodeSessionBlocked)
	if after := snapshot(t, s, "ta"); after != before {
		t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
	}
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "ta", AttemptID: "x", AttemptNo: 1, EnvID: "senv-s1"}); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("会话 turn 不带会话授予应为 ErrInvalid，得到 %v", err)
	}
	wrongEnv := a1
	wrongEnv.EnvID = "elsewhere"
	if _, err := s.CreateAttempt(ctx, wrongEnv); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("不在 incarnation 环境中的 attempt 应为 ErrInvalid，得到 %v", err)
	}

	// 阻塞者本人：清除阻塞、接管会话（idle → running）、incarnation busy、不另建任务环境。
	if _, err := s.pool.Exec(ctx, "UPDATE sessions SET blocked_by_task_id = 'ta' WHERE session_id = 's1'"); err != nil {
		t.Fatal(err)
	}
	first, err := s.CreateAttempt(ctx, a1)
	if err != nil || first.EnvID != "senv-s1" {
		t.Fatalf("阻塞者本人应能创建 attempt：%+v, %v", first, err)
	}
	if again, err := s.CreateAttempt(ctx, a1); err != nil || fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", first) {
		t.Fatalf("重复请求应返回原结果：%+v, %v", again, err)
	}
	if got := sessionRow(t, s, "s1"); got != "running|ta|-|-|0|busy" {
		t.Fatalf("会话 = %s", got)
	}
	if n := count(t, s, "SELECT count(*) FROM environments WHERE attempt_id = 'att1-ta'"); n != 0 {
		t.Fatal("会话 turn 不应另建任务环境")
	}
	if n := count(t, s, "SELECT count(*) FROM sessions WHERE session_id = 's1' AND row_version = 2"); n != 1 {
		t.Fatal("idle → running 应递增会话的 row_version")
	}

	// 成功裁决：提交 session checkpoint、推进指针、补授权会话 scope、清 current_task_id、incarnation releasing。
	if _, err := s.FinalizeAttempt(ctx, turnVerdictFor(a1, 1, "succeeded", "")); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("会话 turn 的成功裁决缺 session_state 应为 ErrInvalid，得到 %v", err)
	}
	v1 := turnVerdictFor(a1, 1, "succeeded", "")
	v1.SessionState = &task.SessionState{CheckpointID: "scp1", State: json.RawMessage(`{"memory":1}`), Refs: []string{sha}}
	for i := 0; i < 2; i++ {
		got, err := s.FinalizeAttempt(ctx, v1)
		if err != nil || got.CommittedSessionCheckpointID != "scp1" {
			t.Fatalf("第 %d 次成功裁决 = %+v, %v", i+1, got, err)
		}
	}
	if got := sessionRow(t, s, "s1"); got != "running|-|-|scp1|1|releasing" {
		t.Fatalf("成功裁决后会话 = %s", got)
	}
	if n := count(t, s, `SELECT count(*) FROM checkpoints WHERE scope_kind = 'session' AND scope_id = 's1' AND checkpoint_id = 'scp1'
		AND commit_seq = 1 AND attempt_id = 'att1-ta' AND state_inline = '{"memory":1}'`); n != 1 {
		t.Fatal("应插入 session checkpoint")
	}
	if n := count(t, s, "SELECT count(*) FROM scope_blobs WHERE scope_kind = 'session' AND scope_id = 's1' AND sha256 = $1", sha); n != 1 {
		t.Fatal("session_state 的引用应补授权到会话 scope")
	}

	// 第二个 turn：base = scp1；指针被移动时成功裁决为 session_base_mismatch；失败裁决不推进指针。
	releaseIncarnation(t, s, "s1")
	mustTurn(t, s, turnReq("s1", "tb", alice))
	b1 := turnAttempt("s1", "tb", 1)
	if _, err := s.CreateAttempt(ctx, b1); err != nil {
		t.Fatal(err)
	}
	st, err := s.LoadTask(ctx, "tb")
	if err != nil || st.SessionID != "s1" || st.BaseSessionCheckpointID != "scp1" || st.TurnIndex != 1 || st.SessionLatest == nil ||
		st.SessionLatest.CheckpointID != "scp1" || string(st.SessionLatest.State) != `{"memory": 1}` || len(st.SessionLatest.Refs) != 1 {
		t.Fatalf("LoadTask 的会话字段 = %+v / %+v, %v", st, st.SessionLatest, err)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE session_progress SET latest_checkpoint_id = 'moved' WHERE session_id = 's1'"); err != nil {
		t.Fatal(err)
	}
	vb := turnVerdictFor(b1, 1, "succeeded", "")
	vb.SessionState = &task.SessionState{CheckpointID: "scp2", State: json.RawMessage(`{"memory":2}`)}
	_, err = s.FinalizeAttempt(ctx, vb)
	expectRejected(t, err, persistence.CodeSessionBaseMismatch)
	if _, err := s.pool.Exec(ctx, "UPDATE session_progress SET latest_checkpoint_id = 'scp1' WHERE session_id = 's1'"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.FinalizeAttempt(ctx, turnVerdictFor(b1, 1, "failed", "worker_error")); err != nil || got.CommittedSessionCheckpointID != "scp1" {
		t.Fatalf("失败裁决 = %+v, %v（指针应保持 base）", got, err)
	}
	if got := sessionRow(t, s, "s1"); got != "running|-|-|scp1|1|releasing" {
		t.Fatalf("失败裁决后会话 = %s", got)
	}

	// 第三个 turn：awaiting_input（desired = run）被接受、阻塞会话、清空续跑指令；不推进指针。
	releaseIncarnation(t, s, "s1")
	mustTurn(t, s, turnReq("s1", "tc", alice))
	c1 := turnAttempt("s1", "tc", 1)
	if _, err := s.CreateAttempt(ctx, c1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE tasks SET resume_directive = '{"kind":"finish_now"}' WHERE task_id = 'tc'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinalizeAttempt(ctx, turnVerdictFor(c1, 1, "paused", "")); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("desired = run 时只有 awaiting_input 的暂停被接受，得到 %v", err)
	}
	if got, err := s.FinalizeAttempt(ctx, turnVerdictFor(c1, 1, "paused", task.ReasonAwaitingInput)); err != nil ||
		got.CommittedSessionCheckpointID != "scp1" {
		t.Fatalf("awaiting_input 裁决 = %+v, %v", got, err)
	}
	if got := sessionRow(t, s, "s1"); got != "running|-|tc|scp1|1|releasing" {
		t.Fatalf("awaiting_input 后会话 = %s", got)
	}
	if n := count(t, s, `SELECT count(*) FROM tasks WHERE task_id = 'tc' AND status = 'paused' AND status_reason = 'awaiting_input'
		AND resume_directive IS NULL`); n != 1 {
		t.Fatal("awaiting_input 应为 paused/awaiting_input 并清空续跑指令")
	}

	// 新消息取代（D5）：cancel 控制，ApplyControl paused → cancelled 清除阻塞。
	releaseIncarnation(t, s, "s1")
	if r := mustTurn(t, s, turnReq("s1", "td", alice)); r.SupersededTurnID != "tc" {
		t.Fatalf("新消息应取代 tc：%+v", r)
	}
	cs, err := s.GetControlState(ctx, "tc")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "tc", ControlVersion: cs.ControlVersion, Status: "cancelled",
		StatusReason: "superseded"}); err != nil {
		t.Fatal(err)
	}
	if got := sessionRow(t, s, "s1"); got != "idle|-|-|scp1|1|idle" {
		t.Fatalf("取代后会话 = %s", got)
	}
	if _, err := s.CreateAttempt(ctx, turnAttempt("s1", "td", 1)); err != nil {
		t.Fatalf("阻塞清除后新 turn 应能运行：%v", err)
	}
	expectNoDBViolations(t, s)
	expectSessionSeqContiguous(t, s, "s1")
}

// TestSessionTurnFaultRetryAndPause：故障重试的 turn 保持占用会话（current_task_id），在新 incarnation 中以 RetryFault
// 创建 attempt 时要求旧会话环境已停止；queued 的 turn 被暂停时阻塞会话。
func TestSessionTurnFaultRetryAndPause(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice, bob := mustUser(t, s, "alice"), mustUser(t, s, "bob")
	liveSession(t, s, "s1", alice)
	mustTurn(t, s, turnReq("s1", "ta", alice))
	a1 := turnAttempt("s1", "ta", 1)
	if _, err := s.CreateAttempt(ctx, a1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinalizeAttempt(ctx, turnVerdictFor(a1, 1, "queued", "worker_crashed")); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAttemptAccess(ctx, a1.AttemptID, "attempt_ended"); err != nil {
		t.Fatal(err)
	}
	if got := sessionRow(t, s, "s1"); got != "running|ta|-|-|0|releasing" {
		t.Fatalf("故障重试的 turn 应保持占用会话：%s", got)
	}
	// 旧 incarnation 销毁，新 incarnation inc2 在 senv2 中。
	if _, err := s.EndIncarnation(ctx, "inc-s1", "worker_crashed"); err != nil {
		t.Fatal(err)
	}
	st, err := s.LoadSession(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc2", SessionID: "s1", EnvID: "senv2",
		FromRowVersion: st.RowVersion}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetIncarnationStatus(ctx, "inc2", []string{session.IncStarting}, session.IncIdle); err != nil {
		t.Fatal(err)
	}
	a2 := task.NewAttempt{TaskID: "ta", AttemptID: "att2-ta", AttemptNo: 2, EnvID: "senv2", Retry: task.RetryFault,
		SessionID: "s1", IncarnationID: "inc2"}
	_, err = s.CreateAttempt(ctx, a2)
	expectRejected(t, err, persistence.CodePreviousNotStopped)
	stopEnv(t, s, "senv-s1")
	if _, err := s.CreateAttempt(ctx, a2); err != nil {
		t.Fatalf("旧会话环境停止后应能在新 incarnation 中重试：%v", err)
	}
	if got := sessionRow(t, s, "s1"); got != "running|ta|-|-|0|busy" {
		t.Fatalf("重试后会话 = %s", got)
	}

	// queued 的 turn 被暂停：阻塞会话、清除占用。
	liveSession(t, s, "s2", bob)
	mustTurn(t, s, turnReq("s2", "tq", bob))
	if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "p-tq", BodyHash: []byte("h"), TaskID: "tq", Desired: "pause"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "tq", ControlVersion: 2, Status: "paused"}); err != nil {
		t.Fatal(err)
	}
	if got := sessionRow(t, s, "s2"); got != "idle|-|tq|-|0|idle" {
		t.Fatalf("暂停 queued turn 后会话 = %s", got)
	}
	expectNoDBViolations(t, s)
}

// TestSessionScopeAndWorkerEvents：session 模式 Worker 事件序号不要求连续、同序号不同内容为冲突；产物与 Gateway 结果
// blob 同时授权到会话 scope，同一会话后续 turn 的 checkpoint 与 blob 读取可以引用它，其他会话不可以；终态提议接受
// awaiting_input。
func TestSessionScopeAndWorkerEvents(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice, bob := mustUser(t, s, "alice"), mustUser(t, s, "bob")
	liveSession(t, s, "s1", alice)
	liveSession(t, s, "s2", bob)
	mustTurn(t, s, turnReq("s1", "ta", alice))
	a1 := turnAttempt("s1", "ta", 1)
	if _, err := s.CreateAttempt(ctx, a1); err != nil {
		t.Fatal(err)
	}

	ev := func(seq int64, payload string) runner.WorkerEvent {
		return runner.WorkerEvent{Seq: seq, Type: "progress", Payload: json.RawMessage(payload)}
	}
	if wm, err := s.AppendSessionWorkerEvents(ctx, a1.AttemptID, []runner.WorkerEvent{ev(3, `{"n":3}`), ev(7, `{"n":7}`), ev(8, `{"n":8}`)}); err != nil ||
		wm.WorkerSeq != 8 {
		t.Fatalf("seq 3、7、8 = %+v, %v", wm, err)
	}
	if wm, err := s.AppendSessionWorkerEvents(ctx, a1.AttemptID, []runner.WorkerEvent{ev(7, `{"n":7}`), ev(8, `{"n":8}`), ev(10, `{"n":10}`)}); err != nil ||
		wm.WorkerSeq != 10 {
		t.Fatalf("重叠的批次应只追加新后缀：%+v, %v", wm, err)
	}
	expectConflicts(t, map[string]func() error{
		"同序号不同内容": func() error {
			_, err := s.AppendSessionWorkerEvents(ctx, a1.AttemptID, []runner.WorkerEvent{ev(7, `{"n":"x"}`)})
			return err
		},
		"回填空隙": func() error {
			_, err := s.AppendSessionWorkerEvents(ctx, a1.AttemptID, []runner.WorkerEvent{ev(5, `{"n":5}`)})
			return err
		},
	})
	if _, err := s.AppendSessionWorkerEvents(ctx, a1.AttemptID, []runner.WorkerEvent{ev(12, `{}`), ev(11, `{}`)}); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("批内不递增应为 ErrInvalid，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM events WHERE attempt_id = $1 AND worker_seq IS NOT NULL AND session_seq IS NOT NULL", a1.AttemptID); n != 4 {
		t.Fatalf("应有 4 条带会话序号的 Worker 事件，得到 %d", n)
	}

	art := strings.Repeat("a1", 32)
	if _, err := s.RegisterArtifact(ctx, runner.Artifact{TaskID: "ta", AttemptID: a1.AttemptID, ArtifactID: "report", SHA256: art, Size: 3,
		MediaType: "text/markdown", Visibility: "output"}); err != nil {
		t.Fatal(err)
	}
	res := strings.Repeat("b2", 32)
	if _, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: "ta", CallID: "c1", AttemptID: a1.AttemptID, Fingerprint: "fp",
		Endpoint: "/v1/fetch", Deadline: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteFromCache(ctx, call.CacheCompletion{TaskID: "ta", CallID: "c1", AttemptID: a1.AttemptID, ResultSHA256: res,
		ResultSize: 9}); err != nil {
		t.Fatal(err)
	}
	for _, sha := range []string{art, res} {
		if n := count(t, s, `SELECT count(*) FROM scope_blobs WHERE sha256 = $1 AND
			((scope_kind = 'task' AND scope_id = 'ta') OR (scope_kind = 'session' AND scope_id = 's1'))`, sha); n != 2 {
			t.Fatalf("blob %s 应同时授权到 task 与会话 scope，得到 %d", sha, n)
		}
	}
	if _, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: a1.AttemptID, Kind: "awaiting_input", Ref: "q1"}); err != nil {
		t.Fatalf("awaiting_input 提议应被记录：%v", err)
	}
	if _, err := s.FinalizeAttempt(ctx, turnVerdictFor(a1, 1, "failed", "worker_error")); err != nil {
		t.Fatal(err)
	}

	// 同一会话的下一个 turn：只授权到会话 scope 的 blob 可被 checkpoint 引用与读取；另一个会话的 turn 不可以。
	releaseIncarnation(t, s, "s1")
	mustTurn(t, s, turnReq("s1", "tb", alice))
	b1 := turnAttempt("s1", "tb", 1)
	if _, err := s.CreateAttempt(ctx, b1); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.BlobAuthorized(ctx, "tb", res); err != nil || !ok {
		t.Fatalf("同会话后续 turn 应可读取会话 scope 的 blob：%v, %v", ok, err)
	}
	if _, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "tb"}, CheckpointID: "cb1",
		AttemptID: b1.AttemptID, StepID: "s", State: json.RawMessage(`{}`), Refs: []string{art}}); err != nil {
		t.Fatalf("会话 scope 的引用应被 checkpoint 接受：%v", err)
	}
	mustTurn(t, s, turnReq("s2", "tx", bob))
	x1 := turnAttempt("s2", "tx", 1)
	if _, err := s.CreateAttempt(ctx, x1); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.BlobAuthorized(ctx, "tx", res); err != nil || ok {
		t.Fatalf("其他会话的 turn 不应读到该 blob：%v, %v", ok, err)
	}
	_, err := s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "tx"}, CheckpointID: "cx1",
		AttemptID: x1.AttemptID, StepID: "s", State: json.RawMessage(`{}`), Refs: []string{art}})
	expectRejected(t, err, persistence.CodeRefNotAuthorized)
	expectNoDBViolations(t, s)
	expectSessionSeqContiguous(t, s, "s1")
}

// toolTask 建立一个预算充足、工具额度为 limit（nil 为不限）的独立任务及其 attempt。
func toolTask(t *testing.T, s *Store, taskID string, limit *int64) {
	t.Helper()
	limits := `{"budget_micro":1000000}`
	if limit != nil {
		limits = fmt.Sprintf(`{"budget_micro":1000000,"max_tool_calls":%d}`, *limit)
	}
	if _, err := s.CreateTask(context.Background(), api.CreateTaskRequest{RequestID: "req-" + taskID, BodyHash: []byte("h"), TaskID: taskID,
		Spec: json.RawMessage(`{"worker":"sim"}`), Limits: json.RawMessage(limits), MaxFaultRetries: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAttempt(context.Background(), task.NewAttempt{TaskID: taskID, AttemptID: "att-" + taskID, AttemptNo: 1,
		EnvID: "env-" + taskID}); err != nil {
		t.Fatal(err)
	}
}

func toolCall(s *Store, taskID, callID, endpoint string) (call.BeginCallResult, error) {
	return s.BeginCall(context.Background(), call.BeginCallRequest{TaskID: taskID, CallID: callID, AttemptID: "att-" + taskID,
		Fingerprint: "fp-" + callID, Endpoint: endpoint, Deadline: time.Minute})
}

// TestToolBudget 覆盖每 turn 的工具调用额度（契约 E，设计 D3）：只在 Tx1 新插入 calls 行时计数，并发下精确；拒绝不登记
// 调用；同 call_id 重放与复位后接管不计数；模型调用不计数；缓存命中在 Tx1 已计数；重启后计数保持；不限的任务不受限。
func TestToolBudget(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	limit := int64(30)
	toolTask(t, s, "t1", &limit)

	var wg sync.WaitGroup
	var ok, exhausted atomic.Int32
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := toolCall(s, "t1", fmt.Sprintf("c%02d", i), "/v1/search")
			var rej *persistence.RejectedError
			switch {
			case err == nil && res.ToolBudget != nil && res.ToolBudget.Limit == 30 && res.ToolBudget.Used >= 1 && res.ToolBudget.Used <= 30:
				ok.Add(1)
			case errors.As(err, &rej) && rej.Code == persistence.CodeToolBudgetExhausted && res.ToolBudget != nil &&
				*res.ToolBudget == (call.ToolBudget{Used: 30, Limit: 30}):
				exhausted.Add(1)
			default:
				errs <- fmt.Errorf("调用 %d：%+v, %v", i, res.ToolBudget, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if ok.Load() != 30 || exhausted.Load() != 10 {
		t.Fatalf("应恰 30 个成功、10 个 tool_budget_exhausted，得到 %d / %d", ok.Load(), exhausted.Load())
	}
	b, err := s.LoadBudget(ctx, "t1")
	if err != nil || b.ToolCallsUsed != 30 || b.ToolCallLimit == nil || *b.ToolCallLimit != 30 {
		t.Fatalf("LoadBudget = %+v, %v", b, err)
	}
	if n := count(t, s, "SELECT count(*) FROM calls WHERE task_id = 't1'"); n != 30 {
		t.Fatalf("被拒绝的调用不应登记，calls 有 %d 行", n)
	}

	// 已登记调用的重放不计数，带当前计数。
	var replay string
	if err := s.pool.QueryRow(ctx, "SELECT call_id FROM calls WHERE task_id = 't1' LIMIT 1").Scan(&replay); err != nil {
		t.Fatal(err)
	}
	if res, err := toolCall(s, "t1", replay, "/v1/search"); err != nil || !res.Existing || res.ToolBudget == nil || res.ToolBudget.Used != 30 {
		t.Fatalf("重放 = %+v, %v", res, err)
	}
	// 模型调用不计数、不受额度限制，也不带额度。
	if res, err := toolCall(s, "t1", "chat-1", "/v1/chat/completions"); err != nil || res.ToolBudget != nil {
		t.Fatalf("模型调用 = %+v, %v", res, err)
	}
	// 新开 Store（模拟重启）：计数保持，第 31 次仍拒绝。
	s2, err := Open(ctx, Options{DSN: s.opt.DSN})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	_, err = toolCall(s2, "t1", "c-after-restart", "/v1/fetch")
	expectRejected(t, err, persistence.CodeToolBudgetExhausted)
	if n := count(t, s, "SELECT count(*) FROM budgets WHERE task_id = 't1' AND tool_calls_used = 30"); n != 1 {
		t.Fatal("重启后计数应保持 30")
	}

	// 复位后接管不计数；缓存命中（CompleteFromCache）的调用已在 Tx1 计数。
	two := int64(2)
	toolTask(t, s, "t2", &two)
	if res, err := toolCall(s, "t2", "r1", "/v1/search"); err != nil || res.ToolBudget == nil || res.ToolBudget.Used != 1 {
		t.Fatalf("t2 第 1 次 = %+v, %v", res, err)
	}
	if _, err := s.ResetResolving(ctx); err != nil {
		t.Fatal(err)
	}
	if res, err := toolCall(s, "t2", "r1", "/v1/search"); err != nil || res.Existing || res.ToolBudget == nil || res.ToolBudget.Used != 1 {
		t.Fatalf("复位后接管应按新登记返回且不计数：%+v, %v", res, err)
	}
	if res, err := toolCall(s, "t2", "r2", "/v1/fetch"); err != nil || res.ToolBudget.Used != 2 {
		t.Fatalf("t2 第 2 次 = %+v, %v", res, err)
	}
	if _, err := s.CompleteFromCache(ctx, call.CacheCompletion{TaskID: "t2", CallID: "r2", AttemptID: "att-t2",
		ResultSHA256: strings.Repeat("c3", 32), ResultSize: 1}); err != nil {
		t.Fatal(err)
	}
	if b, err := s.LoadBudget(ctx, "t2"); err != nil || b.ToolCallsUsed != 2 {
		t.Fatalf("缓存命中不应再计数：%+v, %v", b, err)
	}
	_, err = toolCall(s, "t2", "r3", "/v1/search")
	expectRejected(t, err, persistence.CodeToolBudgetExhausted)

	// tool_call_limit 为 NULL 的独立任务不受限。
	toolTask(t, s, "t3", nil)
	for i := 0; i < 35; i++ {
		if res, err := toolCall(s, "t3", fmt.Sprintf("u%d", i), "/v1/search"); err != nil || res.ToolBudget != nil {
			t.Fatalf("不限的任务第 %d 次 = %+v, %v", i+1, res, err)
		}
	}
	checkI3(t, s)
	expectNoDBViolations(t, s)
}

// ==== M4 Plan 12 Task 4 段结束 ====
