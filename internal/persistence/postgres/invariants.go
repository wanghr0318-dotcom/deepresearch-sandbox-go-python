package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/invariants"
)

var _ invariants.Store = (*Store)(nil)

// quarantineAlertDeadline 是 I8（[B] 类）的期限：隔离资源须在此时间内报警。与启动恢复期限一致（规格 §19）。
const quarantineAlertDeadline = "60 seconds"

// invariantQueries 每条返回 (subject text, detail text) 行；有行即违反。
var invariantQueries = []struct {
	id, class, sql string
}{
	{"I2", "A", `SELECT a.task_id, count(*)::text || ' 个 attempt 的环境未确认停止'
		FROM attempts a JOIN environments e ON e.attempt_id = a.attempt_id
		WHERE e.stopped_at IS NULL GROUP BY a.task_id HAVING count(*) > 1`},
	{"I4", "A", `SELECT task_id, format('事件 %s 条，task_seq 范围 %s..%s', count(*), min(task_seq), max(task_seq))
		FROM events GROUP BY task_id HAVING count(*) <> max(task_seq) OR min(task_seq) <> 1`},
	{"I6", "A", `SELECT c.scope_id, format('checkpoint %s 引用的 %s 未授权到当前 scope', c.checkpoint_id, refs.r)
		FROM checkpoints c,
			LATERAL (SELECT jsonb_array_elements_text(c.refs_json) AS r UNION ALL SELECT c.state_ref WHERE c.state_ref IS NOT NULL) refs
		WHERE NOT EXISTS (SELECT 1 FROM scope_blobs sb WHERE sb.sha256 = refs.r AND
			((sb.scope_kind = 'task' AND sb.scope_id = c.scope_id) OR (sb.scope_kind = 'attempt' AND sb.scope_id = c.attempt_id)))`},
	{"I6", "A", `SELECT p.task_id, format('指针为 %s@%s，最新已提交为 commit_seq %s', COALESCE(p.latest_checkpoint_id, '∅'), p.latest_commit_seq, COALESCE(m.max_seq, 0))
		FROM task_progress p
		LEFT JOIN (SELECT scope_id, max(commit_seq) AS max_seq FROM checkpoints WHERE scope_kind = 'task' GROUP BY scope_id) m
			ON m.scope_id = p.task_id
		WHERE COALESCE(m.max_seq, 0) <> p.latest_commit_seq
			OR (p.latest_checkpoint_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM checkpoints x WHERE x.scope_kind = 'task'
				AND x.scope_id = p.task_id AND x.checkpoint_id = p.latest_checkpoint_id AND x.commit_seq = p.latest_commit_seq))`},
	{"I7", "A", `SELECT task_id, count(*)::text || ' 个 active 访问身份'
		FROM attempt_access WHERE state = 'active' GROUP BY task_id HAVING count(*) > 1`},
	{"I8", "B", `SELECT resource_path, format('%s 自 %s 起隔离，超过期限仍未报警', kind, detected_at)
		FROM quarantined_resources WHERE NOT alerted AND detected_at < now() - interval '` + quarantineAlertDeadline + `'`},
	{"I16", "A", `SELECT r.request_id, format('%s 请求对应的资源 %L 不存在', r.kind, r.resource_id)
		FROM api_requests r WHERE r.resource_id = '' OR NOT EXISTS (SELECT 1 FROM tasks t WHERE t.task_id = r.resource_id)`},
}

// DBViolations 运行只依赖数据库的不变量检查（实现 invariants.Store）。
func (s *Store) DBViolations(ctx context.Context) ([]invariants.Violation, error) {
	var out []invariants.Violation
	err := s.read(ctx, "DBViolations", func(ctx context.Context, q queryer) error {
		out = nil
		for _, iq := range invariantQueries {
			rows, err := q.Query(ctx, iq.sql)
			if err != nil {
				return fmt.Errorf("%s: %w", iq.id, err)
			}
			vs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (invariants.Violation, error) {
				var subject, detail string
				err := r.Scan(&subject, &detail)
				return invariants.Violation{ID: iq.id, Class: iq.class, Detail: subject + "：" + detail}, err
			})
			if err != nil {
				return fmt.Errorf("%s: %w", iq.id, err)
			}
			out = append(out, vs...)
		}
		return nil
	})
	return out, err
}

// CleanedEnvIDs 返回 cleanup_state = done 的环境（实现 invariants.Store）。
func (s *Store) CleanedEnvIDs(ctx context.Context) ([]string, error) {
	var out []string
	err := s.read(ctx, "CleanedEnvIDs", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT env_id FROM environments WHERE cleanup_state = 'done' ORDER BY env_id")
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return out, err
}

// ReferencedBlobs 返回已登记产物与已提交 checkpoint 引用的 blob（实现 invariants.Store）；
// 未在 blobs 中登记的引用以大小 -1 返回，由内容检查报告。
func (s *Store) ReferencedBlobs(ctx context.Context) ([]invariants.BlobRef, error) {
	var out []invariants.BlobRef
	err := s.read(ctx, "ReferencedBlobs", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT a.sha256, a.size, format('artifact %s/%s@%s', a.task_id, a.artifact_id, a.version)
				FROM artifacts a
			UNION ALL
			SELECT refs.r, COALESCE(b.size, -1), format('checkpoint %s/%s', c.scope_id, c.checkpoint_id)
				FROM checkpoints c,
					LATERAL (SELECT jsonb_array_elements_text(c.refs_json) AS r UNION ALL SELECT c.state_ref WHERE c.state_ref IS NOT NULL) refs
				LEFT JOIN blobs b ON b.sha256 = refs.r
			ORDER BY 3, 1`)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (invariants.BlobRef, error) {
			var b invariants.BlobRef
			err := r.Scan(&b.SHA256, &b.Size, &b.Origin)
			return b, err
		})
		return err
	})
	return out, err
}

// MarkQuarantineAlerted 记录隔离资源已报警（实现 resource.Store；I8）。不存在为 ErrNotFound。
func (s *Store) MarkQuarantineAlerted(ctx context.Context, path string) error {
	return s.run(ctx, "MarkQuarantineAlerted", path, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE quarantined_resources SET alerted = true WHERE resource_path = $1", path)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return notFoundf("隔离资源 %s", path)
		}
		return nil
	})
}
