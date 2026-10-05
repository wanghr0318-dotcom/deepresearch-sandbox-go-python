package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

var _ ownership.InstallStore = (*Store)(nil)

//go:embed migrations/*.sql
var migrationFS embed.FS

// agentboxTables 是本项目的业务表；任何一个存在而 schema_migrations 不存在即为未知 schema。
var agentboxTables = []string{
	"installation", "api_requests", "tasks", "task_control", "task_progress", "task_event_seq",
	"attempts", "attempt_access", "events", "environments", "uid_ranges", "resource_intents",
	"quarantined_resources", "checkpoints", "blobs", "scope_blobs", "blob_provenance",
	"artifact_heads", "artifacts", "budgets", "calls", "call_tries", "reservations",
}

// inspectTablesSQL 在同一份目录中查三组表名是否存在：pg_catalog 限定 current_schema()。
// pg_class 对所有角色可见，不像 information_schema 那样只列出角色有权限的表；只看表类关系
// （普通表、分区表、视图、物化视图、外部表）。
const inspectTablesSQL = `WITH rel AS (
	SELECT c.relname::text AS name
	FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'f'))
SELECT EXISTS (SELECT 1 FROM rel WHERE name = ANY($1::text[])),
       EXISTS (SELECT 1 FROM rel WHERE name = ANY($2::text[])),
       EXISTS (SELECT 1 FROM rel WHERE name = ANY($3::text[]))`

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var ms []migration
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		v, err := strconv.Atoi(prefix)
		if !ok || err != nil {
			return nil, fmt.Errorf("postgres: 迁移文件名不合法: %s", e.Name())
		}
		b, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		ms = append(ms, migration{version: v, name: e.Name(), sql: string(b)})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	if len(ms) == 0 || ms[0].version != 1 {
		return nil, errors.New("postgres: 缺少初始迁移 0001")
	}
	return ms, nil
}

// InspectInstallation 读取与安装身份有关的数据库事实（实现 ownership.InstallStore）。
func (s *Store) InspectInstallation(ctx context.Context) (ownership.DBState, error) {
	var st ownership.DBState
	err := s.read(ctx, "InspectInstallation", func(ctx context.Context, q queryer) error {
		var hasInstallation bool
		if err := q.QueryRow(ctx, inspectTablesSQL, []string{"schema_migrations"}, agentboxTables, []string{"installation"}).
			Scan(&st.HasMigrations, &st.HasAgentboxTables, &hasInstallation); err != nil {
			return err
		}
		if !hasInstallation {
			return nil
		}
		// 引导先于迁移执行：只应用了 0001 的旧库没有令牌哈希列，按"无哈希"读取。
		var hasTokenColumn bool
		if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
			WHERE attrelid = to_regclass(quote_ident(current_schema()) || '.installation')
				AND attname = 'bootstrap_token_hash' AND NOT attisdropped)`).Scan(&hasTokenColumn); err != nil {
			return err
		}
		sql := "SELECT install_id, state, NULL::bytea FROM installation"
		if hasTokenColumn {
			sql = "SELECT install_id, state, bootstrap_token_hash FROM installation"
		}
		var inst ownership.Installation
		var state string
		err := q.QueryRow(ctx, sql).Scan(&inst.InstallID, &state, &inst.TokenHash)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		inst.Complete = state == "complete"
		st.Installation = &inst
		return nil
	})
	return st, err
}

// InitializeInstallation 在同一事务中执行全部内嵌迁移并插入 installation(installID, pending,
// tokenHash)（规格 §7.4）。事务未提交则库仍为空，重启后重新引导；提交结果未知时返回
// *persistence.CommitUnknownError 且不重试——同进程内查询为空不能证明未提交，由调用方结束本次启动。
func (s *Store) InitializeInstallation(ctx context.Context, installID string, tokenHash []byte) error {
	if installID == "" || len(tokenHash) != ownership.TokenSize {
		return invalidf("InitializeInstallation 需要 install_id 与 %d 字节的令牌哈希", ownership.TokenSize)
	}
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	return s.migrationTx(ctx, "InitializeInstallation", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `CREATE TABLE schema_migrations (
			version    integer PRIMARY KEY,
			name       text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		for _, m := range ms {
			if err := applyMigration(ctx, tx, m); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, "INSERT INTO installation (install_id, state, bootstrap_token_hash) VALUES ($1, 'pending', $2)",
			installID, tokenHash)
		return err
	})
}

// CompleteInstallation 把 installation 置为 complete（实现 ownership.InstallStore）。
func (s *Store) CompleteInstallation(ctx context.Context, installID string) error {
	return s.run(ctx, "CompleteInstallation", installID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE installation SET state = 'complete' WHERE install_id = $1", installID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return conflictf("installation 中没有 install_id %q", installID)
		}
		return nil
	})
}

// Migrate 依次执行尚未应用的迁移，每个迁移一个事务。必须在安装引导完成之后调用。
func (s *Store) Migrate(ctx context.Context) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	applied, err := s.appliedVersion(ctx)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if m.version <= applied {
			continue
		}
		if err := s.migrationTx(ctx, "Migrate", func(ctx context.Context, tx pgx.Tx) error {
			return applyMigration(ctx, tx, m)
		}); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, tx pgx.Tx, m migration) error {
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("postgres: 迁移 %s: %w", m.name, err)
	}
	_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", m.version, m.name)
	return err
}

// appliedVersion 在迁移超时内读取已应用的最高迁移版本；失去所有权时返回 ErrOwnershipLost。
func (s *Store) appliedVersion(ctx context.Context) (int, error) {
	ctx, cancel, err := s.boundContext(ctx, migrationTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	var applied int
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(max(version), 0) FROM schema_migrations").Scan(&applied); err != nil {
		if s.lost() {
			return 0, persistence.ErrOwnershipLost
		}
		return 0, fmt.Errorf("postgres: 读取迁移版本: %w", err)
	}
	return applied, nil
}

// migrationTx 以独立的迁移超时（规格 §7.3：5 min）执行一个事务；不重跑。context 同时由调用方
// 与所有权派生：失去所有权时取消并返回 ErrOwnershipLost。
func (s *Store) migrationTx(ctx context.Context, op string, fn txFunc) error {
	ctx, cancel, err := s.boundContext(ctx, migrationTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if err := s.migrationTxOnce(ctx, op, fn); err != nil {
		if s.lost() {
			return persistence.ErrOwnershipLost
		}
		var cu *commitUnknown
		if errors.As(err, &cu) {
			return &persistence.CommitUnknownError{Op: op, Err: cu.err}
		}
		return err
	}
	return nil
}

func (s *Store) migrationTxOnce(ctx context.Context, op string, fn txFunc) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %s: %w", op, err)
	}
	defer rollback(tx)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if s.hooks.beforeCommit != nil {
		if err := s.hooks.beforeCommit(op); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return commitError(err) // 与事务辅助相同的归类：可能已到达服务端即为未知
	}
	if s.hooks.afterCommit != nil {
		if err := s.hooks.afterCommit(op); err != nil {
			return &commitUnknown{err: err}
		}
	}
	return nil
}
