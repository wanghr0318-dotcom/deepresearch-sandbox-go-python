// Package postgres 实现全部 SQL 与事务用例（规格 §6、§7；设计 §1–§3）。
//
// 它实现各消费者包声明的窄接口（api.Store、task.Store、runner.Store、resource.Store、
// ownership.InstallStore）；消费者不依赖本包。每个事务用例的事务体本身幂等：在事务内以
// 唯一约束与行锁按操作身份仲裁（设计 §2.3）。
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// 默认参数（规格 §19）。
const (
	DefaultOpDeadline       = 2 * time.Second
	DefaultLockTimeout      = time.Second
	DefaultStatementTimeout = 2 * time.Second
	migrationTimeout        = 5 * time.Minute
	rollbackTimeout         = time.Second // 延迟回滚的上限，连接卡住时不无限等待
)

// Options 配置 Store。零值字段取默认值。
type Options struct {
	DSN              string
	OpDeadline       time.Duration // 每个操作的整体 deadline，覆盖连接池获取、事务与全部重跑
	LockTimeout      time.Duration // SET LOCAL lock_timeout
	StatementTimeout time.Duration // SET LOCAL statement_timeout
	// Ownership 不为 nil 时，失去所有权后所有操作返回 persistence.ErrOwnershipLost，
	// 在途操作随之取消。
	Ownership *Ownership
}

// Store 是 PostgreSQL 上的事务用例实现。
type Store struct {
	pool  *pgxpool.Pool
	opt   Options
	hooks hooks
}

// hooks 只供本包测试注入故障，生产代码中恒为零值。
type hooks struct {
	// beforeCommit 在 COMMIT 之前调用；返回错误则回滚，模拟提交前中断。可以阻塞。
	beforeCommit func(op string) error
	// afterCommit 在 COMMIT 成功之后调用；返回错误则模拟"已提交但回复丢失"。
	afterCommit func(op string) error
}

// Open 连接数据库并返回 Store。
func Open(ctx context.Context, opt Options) (*Store, error) {
	if opt.OpDeadline == 0 {
		opt.OpDeadline = DefaultOpDeadline
	}
	if opt.LockTimeout == 0 {
		opt.LockTimeout = DefaultLockTimeout
	}
	if opt.StatementTimeout == 0 {
		opt.StatementTimeout = DefaultStatementTimeout
	}
	pool, err := pgxpool.New(ctx, opt.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: 连接池: %w", err)
	}
	return &Store{pool: pool, opt: opt}, nil
}

// Close 关闭连接池。
func (s *Store) Close() { s.pool.Close() }

func (s *Store) lost() bool { return s.opt.Ownership != nil && s.opt.Ownership.IsLost() }

// opContext 为一次操作建立整体 deadline；失去所有权时立即取消。
func (s *Store) opContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return s.boundContext(ctx, s.opt.OpDeadline)
}

// boundContext 从调用方 context 派生带超时的 context；配置了 Ownership 时失去所有权即取消。
func (s *Store) boundContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if s.lost() {
		return nil, nil, persistence.ErrOwnershipLost
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	if s.opt.Ownership == nil {
		return ctx, cancel, nil
	}
	stop := context.AfterFunc(s.opt.Ownership.Context(), cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// rollback 以有界的 context 回滚；已提交或已关闭时为空操作。
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// txFunc 是一个事务体；必须幂等（设计 §2.3）。
type txFunc func(ctx context.Context, tx pgx.Tx) error

// commitUnknown 表示 COMMIT 可能已到达服务端而结果未知。
type commitUnknown struct{ err error }

func (e *commitUnknown) Error() string { return "COMMIT 结果未知: " + e.err.Error() }

// txOnce 执行一次事务尝试。
func (s *Store) txOnce(ctx context.Context, op string, fn txFunc) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx) // 已提交时为空操作
	if _, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
		durationSetting(s.opt.LockTimeout), durationSetting(s.opt.StatementTimeout)); err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if s.hooks.beforeCommit != nil {
		if err := s.hooks.beforeCommit(op); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return commitError(err)
	}
	if s.hooks.afterCommit != nil {
		if err := s.hooks.afterCommit(op); err != nil {
			return &commitUnknown{err: err}
		}
	}
	return nil
}

// commitError 归类 COMMIT 的错误。只有确定未提交时才按普通失败返回；COMMIT 可能已到达
// 服务端时一律为未知（设计 §2.2）。
func commitError(err error) error {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrTxCommitRollback):
		// 事务已处于失败状态，COMMIT 实际执行了回滚：事务体忽略了某条语句的错误（编程错误）。
		return fmt.Errorf("%w: 事务体吞掉了语句错误，事务已回滚: %w", errInvalid, err)
	case pgconn.SafeToRetry(err):
		return err // COMMIT 未发出（例如 context 已结束），确定未提交
	case errors.As(err, &pgErr) && severity(pgErr) == "ERROR" && !isConnectionCode(pgErr.Code):
		return err // 服务端明确拒绝提交（例如提交时的序列化失败）
	default:
		return &commitUnknown{err: err} // 含 FATAL/PANIC 与连接类 SQLSTATE：连接已断，结果未知
	}
}

// severity 优先取不随语言环境变化的严重级别。
func severity(e *pgconn.PgError) string {
	if e.SeverityUnlocalized != "" {
		return e.SeverityUnlocalized
	}
	return e.Severity
}

// run 在一个整体 deadline 内执行幂等事务用例。遇到提交结果未知或可重试的中止、锁超时、
// 语句超时、连接错误时，以同一身份在剩余时间内重跑；查不到不等于没提交，结论只由事务内的
// 仲裁给出。deadline 用完仍无结论时：曾出现提交结果未知 → *persistence.CommitUnknownError；
// 调用方 context 结束 → 包装调用方的 ctx.Err()；否则按最后一次错误归类为 ErrContention 或
// ErrUnavailable。
func (s *Store) run(ctx context.Context, op, identity string, fn txFunc) error {
	caller := ctx
	ctx, cancel, err := s.opContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	var unknown, last error
	contended := false // 本次操作中出现过锁超时或可重试的中止
	for attempt := 0; ; attempt++ {
		err := s.txOnce(ctx, op, fn)
		if err == nil {
			return nil
		}
		var cu *commitUnknown
		switch {
		case errors.As(err, &cu):
			unknown, last = cu.err, err
		case retryable(err):
			last = err
			contended = contended || isLockTimeout(err) || isRetryableAbort(err)
		default:
			return s.final(caller, ctx, op, identity, unknown, contended, err)
		}
		if s.lost() {
			return persistence.ErrOwnershipLost
		}
		if !sleepBackoff(ctx, attempt) {
			return s.final(caller, ctx, op, identity, unknown, contended, last)
		}
	}
}

// final 把最后一次错误归类为对消费者公开的错误，并以 %w 保留底层错误。caller 是调用方的
// context，ctx 是由它派生的操作 context。contended 表示本次操作曾因锁超时或可重试的中止而
// 重跑：此时 deadline 到期归为 ErrContention（不计入 Store 故障阈值）。调用方取消不是存储
// 故障：返回包装调用方 ctx.Err() 的错误，不计入阈值。
func (s *Store) final(caller, ctx context.Context, op, identity string, unknown error, contended bool, err error) error {
	switch {
	case s.lost():
		return persistence.ErrOwnershipLost
	case isDomain(err):
		return err
	case unknown != nil:
		return &persistence.CommitUnknownError{Op: op, Identity: identity, Err: unknown}
	case caller.Err() != nil:
		return fmt.Errorf("%s(%s): 调用方已结束: %w: %w", op, identity, caller.Err(), err)
	case isUniqueViolation(err):
		return fmt.Errorf("%s(%s): %w: %w", op, identity, persistence.ErrConflict, err)
	case isLockTimeout(err) || isRetryableAbort(err) || (contended && ctx.Err() != nil):
		return fmt.Errorf("%s(%s): %w: %w", op, identity, persistence.ErrContention, err)
	case ctx.Err() != nil || isConnection(err) || isStatementTimeout(err):
		return fmt.Errorf("%s(%s): %w: %w", op, identity, persistence.ErrUnavailable, err)
	default:
		return fmt.Errorf("%s(%s): %w", op, identity, err)
	}
}

// read 在整体 deadline 内执行只读查询，不重跑。
// queryer 是只读查询所需的最小接口；连接池与事务都满足它。
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *Store) read(ctx context.Context, op string, fn func(ctx context.Context, q queryer) error) error {
	caller := ctx
	ctx, cancel, err := s.opContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if err := fn(ctx, s.pool); err != nil {
		return s.final(caller, ctx, op, "", nil, false, err)
	}
	return nil
}

func isDomain(err error) bool {
	return errors.Is(err, persistence.ErrConflict) || errors.Is(err, persistence.ErrNotFound) ||
		errors.Is(err, persistence.ErrRejected) || errors.Is(err, errInvalid)
}

// errInvalid 表示调用方传入了不合法的参数（编程错误），不重跑。
var errInvalid = persistence.ErrInvalid

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalid, fmt.Sprintf(format, args...))
}

func conflictf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", persistence.ErrConflict, fmt.Sprintf(format, args...))
}

// rejectf 返回前置条件不满足的错误（code 取 persistence.Code* 常量）。
func rejectf(code, format string, args ...any) error {
	return &persistence.RejectedError{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func notFoundf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", persistence.ErrNotFound, fmt.Sprintf(format, args...))
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func isRetryableAbort(err error) bool  { c := sqlState(err); return c == "40001" || c == "40P01" }
func isLockTimeout(err error) bool     { return sqlState(err) == "55P03" }
func isUniqueViolation(err error) bool { return sqlState(err) == "23505" }

// isStatementTimeout：57014 是 statement_timeout 或取消请求导致的语句中止；context 结束引起的
// 情形由 final 按 context 归类。
func isStatementTimeout(err error) bool { return sqlState(err) == "57014" }

// isConnectionCode 判断 SQLSTATE 是否表示连接已断或服务端暂不可用：类 08（连接异常）、
// 57P01–57P03（管理员终止、崩溃恢复、暂不接受连接）、53300（连接数已满）。
func isConnectionCode(code string) bool {
	return strings.HasPrefix(code, "08") || code == "57P01" || code == "57P02" || code == "57P03" || code == "53300"
}

// isConnection 判断错误是否来自连接层：网络错误、连接建立失败、超时、context 结束，或服务端
// 以连接类 SQLSTATE 报告的错误（含包在 *pgconn.ConnectError 中的）。其他带 SQLSTATE 的错误
// 与没有 SQLSTATE 的编程错误不在此列，不会被重跑。
func isConnection(err error) bool {
	if isDomain(err) {
		return false
	}
	if c := sqlState(err); c != "" {
		return isConnectionCode(c)
	}
	var netErr net.Error
	var connErr *pgconn.ConnectError
	return errors.As(err, &netErr) || errors.As(err, &connErr) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// retryable 判断一次事务尝试的失败是否可以在剩余时间内以同一身份重跑。
// 唯一约束冲突也重跑：并发的同一身份写入在对方提交后由事务内仲裁得出结论。
func retryable(err error) bool {
	return isRetryableAbort(err) || isLockTimeout(err) || isUniqueViolation(err) || isStatementTimeout(err) || isConnection(err)
}

// sleepBackoff 以带抖动的指数退避等待；deadline 不足时返回 false。
func sleepBackoff(ctx context.Context, attempt int) bool {
	d := min(10*time.Millisecond<<min(attempt, 5), 200*time.Millisecond)
	d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < d {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// durationSetting 把时长换成 PostgreSQL 的毫秒设置；不足 1 ms 的正值取 1ms（0 表示不限）。
func durationSetting(d time.Duration) string {
	ms := d.Milliseconds()
	if d > 0 && ms == 0 {
		ms = 1
	}
	return fmt.Sprintf("%dms", ms)
}

// contentHash 计算若干字段的内容哈希；每段带长度前缀，避免拼接歧义。
func contentHash(parts ...[]byte) []byte {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write(p)
	}
	return h.Sum(nil)
}

// lockTask 以 FOR UPDATE 锁住任务行（锁顺序的起点之一，规格 §7.1）。
func lockTask(ctx context.Context, tx pgx.Tx, taskID string) error {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM tasks WHERE task_id = $1 FOR UPDATE", taskID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	return err
}

// lockTaskShared 以 FOR SHARE 锁住任务行，阻止并发的生命周期更新改动它。
func lockTaskShared(ctx context.Context, tx pgx.Tx, taskID string) error {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM tasks WHERE task_id = $1 FOR SHARE", taskID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	return err
}

// nullJSON 把空的 JSON 映射为 SQL NULL。
func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return []byte(b)
}
