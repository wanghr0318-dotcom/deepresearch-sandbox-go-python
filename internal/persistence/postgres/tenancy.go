package postgres

// 本文件是多租户（模型网关设计 2026-10-11 §4、§5、§8；G1）的持久化：org、成员、平台 API key 的管理用例
// （tenant.Store）、key 校验读取与 last_used_at（apikey.Store），以及 LISTEN apikey_changed（apikey.Listener）。
// 吊销 key、成员迁出与停用用户在同一事务中 NOTIFY apikey_changed（提交时送达），各 server 据此立即驱逐缓存。

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// TenancyStore 实现 tenant.Store 与 apikey.Source。
type TenancyStore struct{ s *Store }

var (
	_ tenant.Store  = (*TenancyStore)(nil)
	_ apikey.Source = (*TenancyStore)(nil)
)

// Tenancy 以 tenant.Store 接口返回多租户管理用例（装配以接口持有，不导入本包）。
func (s *Store) Tenancy() tenant.Store { return &TenancyStore{s: s} }

// APIKeys 以 apikey.Source 接口返回 key 校验读取与变更通知。
func (s *Store) APIKeys() apikey.Source { return &TenancyStore{s: s} }

// TenancyStore 返回具体实现（CLI 与测试使用）。
func (s *Store) TenancyStore() *TenancyStore { return &TenancyStore{s: s} }

// notifyKeyChanged 在事务中 NOTIFY apikey_changed（payload 为 key ID，或 "*" 表示全部）；提交时送达。
func notifyKeyChanged(ctx context.Context, tx pgx.Tx, payload string) error {
	_, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", apikey.Channel, payload)
	return err
}

// ensurePersonalOrg 建立用户的个人 org（user-<id>）并使其成为 owner；已属某个 org 时不变。
func ensurePersonalOrg(ctx context.Context, tx pgx.Tx, userID int64) error {
	if _, err := tx.Exec(ctx, "INSERT INTO orgs (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", tenant.PersonalOrgName(userID)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO org_members (org_id, user_id, role) SELECT id, $2, 'owner' FROM orgs WHERE name = $1
		ON CONFLICT (user_id) DO NOTHING`, tenant.PersonalOrgName(userID), userID)
	return err
}

// revokeUserKeys 吊销用户在 org 中全部未吊销的 key，并逐个 NOTIFY。
func revokeUserKeys(ctx context.Context, tx pgx.Tx, userID, orgID int64) error {
	rows, err := tx.Query(ctx, `UPDATE api_keys SET revoked_at = now() WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL
		RETURNING id`, userID, orgID)
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := notifyKeyChanged(ctx, tx, id); err != nil {
			return err
		}
	}
	return nil
}

// ---- org ----

const orgColumns = "o.id, o.name, o.disabled, o.created_at, (SELECT count(*) FROM org_members x WHERE x.org_id = o.id)"

func scanOrg(row pgx.Row) (tenant.Org, error) {
	var o tenant.Org
	err := row.Scan(&o.ID, &o.Name, &o.Disabled, &o.CreatedAt, &o.Members)
	return o, err
}

// CreateOrg 创建 org（实现 tenant.Store）。名称须通过 tenant.ValidateOrgName；已存在为 persistence.ErrConflict
// （提交结果未知而重跑时同样报告冲突：调用方可按名称查询）。
func (t *TenancyStore) CreateOrg(ctx context.Context, name string) (tenant.Org, error) {
	if err := tenant.ValidateOrgName(name); err != nil {
		return tenant.Org{}, invalidf("%v", err)
	}
	var o tenant.Org
	err := t.s.run(ctx, "CreateOrg", name, func(ctx context.Context, tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, "INSERT INTO orgs (name) VALUES ($1) ON CONFLICT (name) DO NOTHING RETURNING id", name).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return conflictf("org %s 已存在", name)
		}
		if err != nil {
			return err
		}
		o, err = scanOrg(tx.QueryRow(ctx, "SELECT "+orgColumns+" FROM orgs o WHERE o.id = $1", id))
		return err
	})
	return o, err
}

// ListOrgs 按 ID 列出全部 org 及成员数（实现 tenant.Store）。
func (t *TenancyStore) ListOrgs(ctx context.Context) ([]tenant.Org, error) {
	var out []tenant.Org
	err := t.s.read(ctx, "ListOrgs", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT "+orgColumns+" FROM orgs o ORDER BY o.id")
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (tenant.Org, error) { return scanOrg(r) })
		return err
	})
	return out, err
}

// OrgByName 按名称读取 org（实现 tenant.Store）；不存在为 persistence.ErrNotFound。
func (t *TenancyStore) OrgByName(ctx context.Context, name string) (tenant.Org, error) {
	var o tenant.Org
	err := t.s.read(ctx, "OrgByName", func(ctx context.Context, q queryer) error {
		var err error
		o, err = scanOrg(q.QueryRow(ctx, "SELECT "+orgColumns+" FROM orgs o WHERE o.name = $1", name))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("org %s", name)
		}
		return err
	})
	return o, err
}

// ---- 成员 ----

const memberColumns = "m.org_id, o.name, u.id, u.username, m.role"

func scanMember(row pgx.Row) (tenant.Membership, error) {
	var m tenant.Membership
	var role string
	err := row.Scan(&m.OrgID, &m.OrgName, &m.UserID, &m.Username, &role)
	m.Role = tenant.Role(role)
	return m, err
}

// MemberByName 按规范化用户名读取成员关系（实现 tenant.Store）；用户不存在为 persistence.ErrNotFound。
func (t *TenancyStore) MemberByName(ctx context.Context, usernameKey string) (tenant.Membership, error) {
	var m tenant.Membership
	err := t.s.read(ctx, "MemberByName", func(ctx context.Context, q queryer) error {
		var err error
		m, err = scanMember(q.QueryRow(ctx, "SELECT "+memberColumns+` FROM users u JOIN org_members m ON m.user_id = u.id
			JOIN orgs o ON o.id = m.org_id WHERE u.username_key = $1`, usernameKey))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("用户 %s", usernameKey)
		}
		return err
	})
	return m, err
}

// lockOrgForMembers 锁定 org 行（同一 org 的成员变更串行化，"至少一个 owner" 的检查据此成立）。
func lockOrgForMembers(ctx context.Context, tx pgx.Tx, orgID int64) (string, error) {
	var name string
	err := tx.QueryRow(ctx, "SELECT name FROM orgs WHERE id = $1 FOR UPDATE", orgID).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", notFoundf("org %d", orgID)
	}
	return name, err
}

// lockUserByName 锁定用户行并返回其 ID（同一用户的成员变更与建 key 串行化）。
func lockUserByName(ctx context.Context, tx pgx.Tx, usernameKey string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, "SELECT id FROM users WHERE username_key = $1 FOR UPDATE", usernameKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, notFoundf("用户 %s", usernameKey)
	}
	return id, err
}

// otherOwners 返回 org 中除 userID 之外的 owner 数。
func otherOwners(ctx context.Context, tx pgx.Tx, orgID, userID int64) (int64, error) {
	var n int64
	err := tx.QueryRow(ctx, "SELECT count(*) FROM org_members WHERE org_id = $1 AND role = 'owner' AND user_id <> $2", orgID, userID).Scan(&n)
	return n, err
}

// AddMember 把用户加入 org（实现 tenant.Store；规则见接口）。保留 org operator 不接受成员。
func (t *TenancyStore) AddMember(ctx context.Context, orgID int64, usernameKey string, role tenant.Role) (tenant.Membership, error) {
	if _, err := tenant.ParseRole(string(role)); err != nil {
		return tenant.Membership{}, invalidf("%v", err)
	}
	var out tenant.Membership
	err := t.s.run(ctx, "AddMember", usernameKey, func(ctx context.Context, tx pgx.Tx) error {
		orgName, err := lockOrgForMembers(ctx, tx, orgID)
		if err != nil {
			return err
		}
		if orgName == tenant.OperatorOrg {
			return invalidf("保留 org %s 不接受成员", orgName)
		}
		userID, err := lockUserByName(ctx, tx, usernameKey)
		if err != nil {
			return err
		}
		var curOrg, size int64
		var curName, curRole string
		err = tx.QueryRow(ctx, `SELECT m.org_id, o.name, m.role, (SELECT count(*) FROM org_members x WHERE x.org_id = m.org_id)
			FROM org_members m JOIN orgs o ON o.id = m.org_id WHERE m.user_id = $1`, userID).Scan(&curOrg, &curName, &curRole, &size)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// 没有 org（不应发生：注册与迁移都建立个人 org）：直接加入。
		case err != nil:
			return err
		case curOrg == orgID:
			if curRole == string(tenant.Owner) && role != tenant.Owner {
				n, err := otherOwners(ctx, tx, orgID, userID)
				if err != nil {
					return err
				}
				if n == 0 {
					return tenant.ErrLastOwner
				}
			}
			if _, err := tx.Exec(ctx, "UPDATE org_members SET role = $3 WHERE org_id = $1 AND user_id = $2", orgID, userID, string(role)); err != nil {
				return err
			}
			out, err = memberOf(ctx, tx, userID)
			return err
		case !tenant.IsPersonalOrg(curName, userID) || size > 1:
			return tenant.ErrUserInOtherOrg
		default:
			// 从个人 org 迁出：该 org 中的 key 随之吊销（不变量 G4：未吊销 key 的 org 等于其用户的 org）。
			if err := revokeUserKeys(ctx, tx, userID, curOrg); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "DELETE FROM org_members WHERE user_id = $1", userID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "INSERT INTO org_members (org_id, user_id, role) VALUES ($1, $2, $3)", orgID, userID, string(role)); err != nil {
			return err
		}
		out, err = memberOf(ctx, tx, userID)
		return err
	})
	return out, err
}

func memberOf(ctx context.Context, q queryer, userID int64) (tenant.Membership, error) {
	return scanMember(q.QueryRow(ctx, "SELECT "+memberColumns+` FROM org_members m JOIN users u ON u.id = m.user_id
		JOIN orgs o ON o.id = m.org_id WHERE m.user_id = $1`, userID))
}

// RemoveMember 把用户移出 org，回到其个人 org（实现 tenant.Store；规则见接口）。
func (t *TenancyStore) RemoveMember(ctx context.Context, orgID int64, usernameKey string) error {
	return t.s.run(ctx, "RemoveMember", usernameKey, func(ctx context.Context, tx pgx.Tx) error {
		orgName, err := lockOrgForMembers(ctx, tx, orgID)
		if err != nil {
			return err
		}
		userID, err := lockUserByName(ctx, tx, usernameKey)
		if err != nil {
			return err
		}
		var role string
		err = tx.QueryRow(ctx, "SELECT role FROM org_members WHERE org_id = $1 AND user_id = $2", orgID, userID).Scan(&role)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("用户 %s 不是 org %d 的成员", usernameKey, orgID)
		}
		if err != nil {
			return err
		}
		if tenant.IsPersonalOrg(orgName, userID) {
			return tenant.ErrPersonalOrg
		}
		if role == string(tenant.Owner) {
			n, err := otherOwners(ctx, tx, orgID, userID)
			if err != nil {
				return err
			}
			if n == 0 {
				return tenant.ErrLastOwner
			}
		}
		if err := revokeUserKeys(ctx, tx, userID, orgID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM org_members WHERE user_id = $1", userID); err != nil {
			return err
		}
		return ensurePersonalOrg(ctx, tx, userID)
	})
}

// ---- key ----

const keyColumns = `k.id, k.org_id, k.user_id, u.username, k.name, k.scopes, k.created_at, k.expires_at, k.revoked_at, k.last_used_at`

func scanKey(row pgx.Row) (tenant.Key, error) {
	var k tenant.Key
	var scopes []string
	if err := row.Scan(&k.ID, &k.OrgID, &k.UserID, &k.Username, &k.Name, &scopes, &k.CreatedAt, &k.ExpiresAt, &k.RevokedAt,
		&k.LastUsedAt); err != nil {
		return tenant.Key{}, err
	}
	var err error
	k.Scopes, err = tenant.ParseScopes(scopes)
	return k, err
}

func getKey(ctx context.Context, q queryer, id string) (tenant.Key, error) {
	k, err := scanKey(q.QueryRow(ctx, "SELECT "+keyColumns+" FROM api_keys k JOIN users u ON u.id = k.user_id WHERE k.id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return tenant.Key{}, notFoundf("key %s", id)
	}
	return k, err
}

// CreateKey 为用户在其当前 org 中登记 key（实现 tenant.Store）。只存 secret 的 HMAC。用户不存在或已停用为
// persistence.ErrNotFound。提交结果未知而重跑时以 HMAC 识别本次已提交的行。
func (t *TenancyStore) CreateKey(ctx context.Context, k tenant.NewKey) (tenant.Key, error) {
	if !apikey.ValidID(k.ID) || len(k.SecretHMAC) != 32 || k.UserID <= 0 || !apikey.ValidName(k.Name) || k.Scopes == 0 ||
		k.Scopes&^tenant.AllScopes != 0 {
		return tenant.Key{}, invalidf("CreateKey 的 ID、HMAC、用户、名称或 scope 不合法")
	}
	var out tenant.Key
	err := t.s.run(ctx, "CreateKey", k.ID, func(ctx context.Context, tx pgx.Tx) error {
		var orgID int64
		err := tx.QueryRow(ctx, `SELECT m.org_id FROM org_members m JOIN users u ON u.id = m.user_id
			WHERE m.user_id = $1 AND NOT u.disabled FOR SHARE OF m`, k.UserID).Scan(&orgID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("用户 %d", k.UserID)
		}
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO api_keys (id, org_id, user_id, name, secret_hmac, scopes, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING`,
			k.ID, orgID, k.UserID, k.Name, k.SecretHMAC, k.Scopes.Names(), k.ExpiresAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var stored []byte
			if err := tx.QueryRow(ctx, "SELECT secret_hmac FROM api_keys WHERE id = $1", k.ID).Scan(&stored); err != nil {
				return err
			}
			if string(stored) != string(k.SecretHMAC) {
				return conflictf("key %s 已存在", k.ID)
			}
		}
		out, err = getKey(ctx, tx, k.ID)
		return err
	})
	return out, err
}

// ListKeys 按创建时间倒序列出 key（实现 tenant.Store）；f 的零值字段不过滤。
func (t *TenancyStore) ListKeys(ctx context.Context, f tenant.KeyFilter) ([]tenant.Key, error) {
	var out []tenant.Key
	err := t.s.read(ctx, "ListKeys", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT "+keyColumns+` FROM api_keys k JOIN users u ON u.id = k.user_id
			WHERE ($1::bigint = 0 OR k.org_id = $1) AND ($2::bigint = 0 OR k.user_id = $2) ORDER BY k.created_at DESC, k.id`,
			f.OrgID, f.UserID)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (tenant.Key, error) { return scanKey(r) })
		return err
	})
	return out, err
}

// GetKey 读取一个 key（实现 tenant.Store）；不存在为 persistence.ErrNotFound。
func (t *TenancyStore) GetKey(ctx context.Context, id string) (tenant.Key, error) {
	var out tenant.Key
	err := t.s.read(ctx, "GetKey", func(ctx context.Context, q queryer) error {
		var err error
		out, err = getKey(ctx, q, id)
		return err
	})
	return out, err
}

// RevokeKey 吊销 key（实现 tenant.Store；幂等，保留首次吊销时间）并 NOTIFY apikey_changed。
func (t *TenancyStore) RevokeKey(ctx context.Context, id string) (tenant.Key, error) {
	var out tenant.Key
	err := t.s.run(ctx, "RevokeKey", id, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1", id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return notFoundf("key %s", id)
		}
		if err := notifyKeyChanged(ctx, tx, id); err != nil {
			return err
		}
		out, err = getKey(ctx, tx, id)
		return err
	})
	return out, err
}

// ---- 校验（apikey.Store、apikey.Listener） ----

// LookupKey 读取校验所需的 key 事实（实现 apikey.Store）。用户或 org 停用、或用户已不在 key 的 org 中时 Disabled
// 为 true。不存在为 persistence.ErrNotFound。
func (t *TenancyStore) LookupKey(ctx context.Context, id string) (apikey.Record, error) {
	var r apikey.Record
	err := t.s.read(ctx, "LookupKey", func(ctx context.Context, q queryer) error {
		var scopes []string
		var role *string
		err := q.QueryRow(ctx, `SELECT k.id, k.org_id, k.user_id, u.username, m.role, k.scopes, k.secret_hmac, k.expires_at,
				k.revoked_at IS NOT NULL, u.disabled OR o.disabled OR m.user_id IS NULL
			FROM api_keys k JOIN users u ON u.id = k.user_id JOIN orgs o ON o.id = k.org_id
			LEFT JOIN org_members m ON m.user_id = k.user_id AND m.org_id = k.org_id
			WHERE k.id = $1`, id).Scan(&r.ID, &r.OrgID, &r.UserID, &r.Username, &role, &scopes, &r.SecretHMAC, &r.ExpiresAt,
			&r.Revoked, &r.Disabled)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("key %s", id)
		}
		if err != nil {
			return err
		}
		if role != nil {
			r.Role = tenant.Role(*role)
		}
		r.Scopes, err = tenant.ParseScopes(scopes)
		return err
	})
	return r, err
}

// TouchKey 写 last_used_at（实现 apikey.Store）：已在 at 之前一分钟内写过时不写（多个 server 同样至多每分钟一次）。
func (t *TenancyStore) TouchKey(ctx context.Context, id string, at time.Time) error {
	return t.s.run(ctx, "TouchKey", id, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE api_keys SET last_used_at = $2::timestamptz WHERE id = $1
			AND (last_used_at IS NULL OR last_used_at <= $2::timestamptz - make_interval(secs => $3))`, id, at, apikey.TouchInterval.Seconds())
		return err
	})
}

// ListenKeyChanges 在一条独占连接上 LISTEN apikey_changed（实现 apikey.Listener），直到 ctx 结束、失去所有权或连接
// 失败。LISTEN 生效后调用 ready，每条通知调用 onChange(payload)。
func (t *TenancyStore) ListenKeyChanges(ctx context.Context, ready func(), onChange func(payload string)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if o := t.s.opt.Ownership; o != nil {
		stop := context.AfterFunc(o.Context(), cancel)
		defer stop()
	}
	conn, err := t.s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	pc := conn.Hijack() // LISTEN 的连接不归还连接池
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), rollbackTimeout)
		defer ccancel()
		_ = pc.Close(cctx)
	}()
	if _, err := pc.Exec(ctx, "LISTEN "+apikey.Channel); err != nil {
		return err
	}
	ready()
	for {
		n, err := pc.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		onChange(n.Payload)
	}
}
