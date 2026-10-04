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
	"artifact_heads", "artifacts",
}

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
		if err := q.QueryRow(ctx, "SELECT to_regclass('schema_migrations') IS NOT NULL").Scan(&st.HasMigrations); err != nil {
			return err
		}
		var n int
		if err := q.QueryRow(ctx,
			"SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ANY($1)",
			agentboxTables).Scan(&n); err != nil {
			return err
		}
		st.HasAgentboxTables = n > 0
		var hasInstallation bool
		if err := q.QueryRow(ctx, "SELECT to_regclass('installation') IS NOT NULL").Scan(&hasInstallation); err != nil || !hasInstallation {
			return err
		}
		var inst ownership.Installation
		var state string
		err := q.QueryRow(ctx, "SELECT install_id, state FROM installation").Scan(&inst.InstallID, &state)
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

// InitializeInstallation 在同一事务中执行初始迁移并插入 installation(installID, pending)
// （规格 §7.4）。事务未提交则库仍为空，重启后重新引导。
func (s *Store) InitializeInstallation(ctx context.Context, installID string) error {
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
		if err := applyMigration(ctx, tx, ms[0]); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO installation (install_id, state) VALUES ($1, 'pending')", installID)
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
	var applied int
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(max(version), 0) FROM schema_migrations").Scan(&applied); err != nil {
		return fmt.Errorf("postgres: 读取迁移版本: %w", err)
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

// migrationTx 以独立的迁移超时（规格 §7.3：5 min）执行一个事务；不重跑。
func (s *Store) migrationTx(ctx context.Context, op string, fn txFunc) error {
	if s.lost() {
		return persistence.ErrOwnershipLost
	}
	ctx, cancel := context.WithTimeout(ctx, migrationTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %s: %w", op, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if s.hooks.beforeCommit != nil {
		if err := s.hooks.beforeCommit(op); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
