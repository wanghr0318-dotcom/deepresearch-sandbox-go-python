package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

var _ api.Accounts = (*Store)(nil)

// userColumns 是 api.User 的列，顺序与 scanUser 一致。
const userColumns = "u.id, u.username, u.role, u.disabled, u.created_at"

func scanUser(row pgx.Row, extra ...any) (api.User, error) {
	var u api.User
	err := row.Scan(append([]any{&u.ID, &u.Username, &u.Role, &u.Disabled, &u.CreatedAt}, extra...)...)
	return u, err
}

// CreateUser 创建用户（实现 api.Accounts）。key 是规范化（小写）的用户名，唯一；display 保留原样。
// 重复返回 persistence.ErrConflict。提交结果未知而重跑时，以密码哈希（含随机盐）识别本次已提交的行。
func (s *Store) CreateUser(ctx context.Context, display, key, passwordHash string) (api.User, error) {
	if display == "" || key == "" || passwordHash == "" {
		return api.User{}, invalidf("CreateUser 缺少用户名或密码哈希")
	}
	var u api.User
	err := s.run(ctx, "CreateUser", key, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		u, err = scanUser(tx.QueryRow(ctx, `INSERT INTO users AS u (username, username_key, password_hash) VALUES ($1, $2, $3)
			ON CONFLICT (username_key) DO NOTHING RETURNING `+userColumns, display, key, passwordHash))
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var stored string
		u, err = scanUser(tx.QueryRow(ctx, "SELECT "+userColumns+", u.password_hash FROM users u WHERE u.username_key = $1", key), &stored)
		if err != nil {
			return err
		}
		if stored != passwordHash {
			return conflictf("用户名 %s 已存在", key)
		}
		return nil
	})
	if err != nil {
		return api.User{}, err
	}
	return u, nil
}

// UserForLogin 按规范化用户名读取用户与密码哈希（实现 api.Accounts）。停用用户照常返回（Disabled 为 true）。
func (s *Store) UserForLogin(ctx context.Context, key string) (api.User, string, error) {
	var u api.User
	var hash string
	err := s.read(ctx, "UserForLogin", func(ctx context.Context, q queryer) error {
		var err error
		u, err = scanUser(q.QueryRow(ctx, "SELECT "+userColumns+", u.password_hash FROM users u WHERE u.username_key = $1", key), &hash)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("用户")
		}
		return err
	})
	if err != nil {
		return api.User{}, "", err
	}
	return u, hash, nil
}

// CreateSession 登记会话（实现 api.Accounts）。idHash 是会话 ID 的 SHA-256；重跑时同一 idHash 不重复写入。
func (s *Store) CreateSession(ctx context.Context, idHash []byte, userID int64, expiresAt time.Time) error {
	if len(idHash) == 0 || userID <= 0 {
		return invalidf("CreateSession 缺少会话哈希或用户")
	}
	return s.run(ctx, "CreateSession", "", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth_sessions (id_hash, user_id, expires_at) VALUES ($1, $2, $3)
			ON CONFLICT (id_hash) DO NOTHING`, idHash, userID, expiresAt)
		return err
	})
}

// SessionUser 返回会话所属用户（实现 api.Accounts）：会话不存在、已过期或用户停用均为 persistence.ErrNotFound。
func (s *Store) SessionUser(ctx context.Context, idHash []byte) (api.User, error) {
	var u api.User
	err := s.read(ctx, "SessionUser", func(ctx context.Context, q queryer) error {
		var err error
		u, err = scanUser(q.QueryRow(ctx, "SELECT "+userColumns+` FROM auth_sessions s JOIN users u ON u.id = s.user_id
			WHERE s.id_hash = $1 AND s.expires_at > now() AND NOT u.disabled`, idHash))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("会话")
		}
		return err
	})
	if err != nil {
		return api.User{}, err
	}
	return u, nil
}

// DeleteSession 删除会话（实现 api.Accounts）；会话不存在时为空操作。
func (s *Store) DeleteSession(ctx context.Context, idHash []byte) error {
	return s.run(ctx, "DeleteSession", "", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "DELETE FROM auth_sessions WHERE id_hash = $1", idHash)
		return err
	})
}

// SetDisabled 设置用户的停用状态（实现 api.Accounts）；停用时在同一事务中删除其全部会话。
func (s *Store) SetDisabled(ctx context.Context, key string, disabled bool) error {
	return s.run(ctx, "SetDisabled", key, func(ctx context.Context, tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, "UPDATE users SET disabled = $2 WHERE username_key = $1 RETURNING id", key, disabled).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("用户 %s", key)
		}
		if err != nil || !disabled {
			return err
		}
		_, err = tx.Exec(ctx, "DELETE FROM auth_sessions WHERE user_id = $1", id)
		return err
	})
}

// ListUsers 按创建顺序列出全部用户（实现 api.Accounts）。
func (s *Store) ListUsers(ctx context.Context) ([]api.User, error) {
	var out []api.User
	err := s.read(ctx, "ListUsers", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT "+userColumns+" FROM users u ORDER BY u.id")
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.User, error) { return scanUser(r) })
		return err
	})
	return out, err
}

// CreateResearch 为用户创建研究任务（实现 api.Accounts）。以 request_id 幂等（同 CreateTask）：重放先于
// 运行中检查，不受限制，但 request 必须属于同一用户。新请求锁定用户行（FOR UPDATE，串行化同一用户的并发
// 提交）后检查该用户没有非终态任务（终态判定取自 task.IsTerminal），再与 CreateTask 共用事务体创建任务。
// 用户不存在或已停用为 persistence.ErrNotFound；已有非终态任务为 api.ErrUserTaskRunning。
func (s *Store) CreateResearch(ctx context.Context, userID int64, req api.CreateTaskRequest) (api.CreateTaskResult, error) {
	if userID <= 0 {
		return api.CreateTaskResult{}, invalidf("CreateResearch 的 userID 必须为正")
	}
	budget, err := createTaskBudget("CreateResearch", req)
	if err != nil {
		return api.CreateTaskResult{}, err
	}
	var res api.CreateTaskResult
	err = s.run(ctx, "CreateResearch", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.CreateTaskResult{}
		replayed, stored, err := claimRequest(ctx, tx, req.RequestID, "create_task", req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			var owner int64
			if err := tx.QueryRow(ctx, "SELECT COALESCE(owner_user_id, 0) FROM tasks WHERE task_id = $1", res.TaskID).Scan(&owner); err != nil {
				return err
			}
			if owner != userID {
				return conflictf("request_conflict: request_id %s 已用于另一个请求", req.RequestID)
			}
			res.Replayed = true
			return nil
		}
		var id int64
		err = tx.QueryRow(ctx, "SELECT id FROM users WHERE id = $1 AND NOT disabled FOR UPDATE", userID).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("用户 %d", userID)
		}
		if err != nil {
			return err
		}
		if err := admitResearch(ctx, tx, userID); err != nil {
			return err
		}
		if err := createTaskTx(ctx, tx, req, budget, userID, nil); err != nil {
			return err
		}
		res.TaskID = req.TaskID
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// admitResearch 检查用户没有非终态任务（每用户同时最多 1 个）。按状态去重后用 task.IsTerminal 判定，
// 终态集合只有 task 包一处权威定义。调用方须已锁定用户行。
func admitResearch(ctx context.Context, tx pgx.Tx, userID int64) error {
	rows, err := tx.Query(ctx, "SELECT DISTINCT status FROM tasks WHERE owner_user_id = $1", userID)
	if err != nil {
		return err
	}
	statuses, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, st := range statuses {
		if !task.IsTerminal(st) {
			return api.ErrUserTaskRunning
		}
	}
	return nil
}

// TaskOwner 返回任务的 owner（实现 api.Accounts）：无主任务为 0，任务不存在为 persistence.ErrNotFound。
func (s *Store) TaskOwner(ctx context.Context, taskID string) (int64, error) {
	var owner int64
	err := s.read(ctx, "TaskOwner", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, "SELECT COALESCE(owner_user_id, 0) FROM tasks WHERE task_id = $1", taskID).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		return err
	})
	return owner, err
}
