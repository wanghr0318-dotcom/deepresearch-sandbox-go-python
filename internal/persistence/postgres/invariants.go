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
	// I3（task 层）：reserved = Σ held、unknown = Σ charged_unknown、spent = Σ settled try 的实际费用。
	{"I3", "A", `SELECT b.task_id, format('reserved=%s（held 之和 %s），unknown=%s（charged_unknown 之和 %s），spent=%s（settled 之和 %s）',
			b.reserved_micro, COALESCE(r.held, 0), b.unknown_micro, COALESCE(r.unk, 0), b.spent_micro, COALESCE(s.spent, 0))
		FROM budgets b
		LEFT JOIN (SELECT task_id, sum(amount) FILTER (WHERE state = 'held') AS held,
				sum(amount) FILTER (WHERE state = 'charged_unknown') AS unk
			FROM reservations GROUP BY task_id) r ON r.task_id = b.task_id
		LEFT JOIN (SELECT t.task_id, sum(t.cost_micro) AS spent FROM call_tries t
				JOIN reservations x ON x.reservation_id = t.reservation_id WHERE x.state = 'settled' GROUP BY t.task_id) s
			ON s.task_id = b.task_id
		WHERE b.reserved_micro <> COALESCE(r.held, 0) OR b.unknown_micro <> COALESCE(r.unk, 0) OR b.spent_micro <> COALESCE(s.spent, 0)`},
	// I3：每笔 reservation 恰在一个桶中，且与其 try 的结算一致（held ↔ 在途；settled ↔ ok；released ↔ retryable/fatal；
	// charged_unknown ↔ unknown）。
	{"I3", "A", `SELECT r.task_id, format('reservation %s（%s#%s）为 %s，try 为 %s/%s', r.reservation_id, r.call_id, r.try_no,
			r.state, COALESCE(t.state, '∅'), COALESCE(NULLIF(t.outcome, ''), '∅'))
		FROM reservations r LEFT JOIN call_tries t ON t.reservation_id = r.reservation_id
		WHERE t.reservation_id IS NULL OR NOT ((r.state = 'held' AND t.state = 'in_flight')
			OR (r.state = 'settled' AND t.outcome = 'ok') OR (r.state = 'released' AND t.outcome IN ('retryable', 'fatal'))
			OR (r.state = 'charged_unknown' AND t.outcome = 'unknown'))`},
	{"I4", "A", `SELECT task_id, format('事件 %s 条，task_seq 范围 %s..%s', count(*), min(task_seq), max(task_seq))
		FROM events GROUP BY task_id HAVING count(*) <> max(task_seq) OR min(task_seq) <> 1`},
	// I6：checkpoint 的引用授权到其 scope（session checkpoint 到会话 scope）、提交它的 attempt，或（task checkpoint）
	// 任务所属会话的 scope（§5.5 第 2 条 attempt → task → session 推导）。
	{"I6", "A", `SELECT c.scope_id, format('checkpoint %s 引用的 %s 未授权到当前 scope', c.checkpoint_id, refs.r)
		FROM checkpoints c,
			LATERAL (SELECT jsonb_array_elements_text(c.refs_json) AS r UNION ALL SELECT c.state_ref WHERE c.state_ref IS NOT NULL) refs
		WHERE NOT EXISTS (SELECT 1 FROM scope_blobs sb WHERE sb.sha256 = refs.r AND
			((sb.scope_kind = c.scope_kind AND sb.scope_id = c.scope_id) OR (sb.scope_kind = 'attempt' AND sb.scope_id = c.attempt_id)
				OR (c.scope_kind = 'task' AND sb.scope_kind = 'session'
					AND sb.scope_id = (SELECT t.session_id FROM tasks t WHERE t.task_id = c.scope_id))))`},
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
	// I14（journal 部分）：completed 调用的结果登记在 blobs 并授权到 scope_blobs(task)；result_ref 等于结算时
	// 记录的结果（ok try 的 blob_provenance），即已记录的结果没有被改写。内容完整性由 ReferencedBlobs 复算。
	{"I14", "A", `SELECT c.task_id, format('调用 %s 的结果 %s 未登记到 blobs 或 scope_blobs(task)', c.call_id, c.result_ref)
		FROM calls c WHERE c.state = 'completed' AND (NOT EXISTS (SELECT 1 FROM blobs b WHERE b.sha256 = c.result_ref)
			OR NOT EXISTS (SELECT 1 FROM scope_blobs sb WHERE sb.scope_kind = 'task' AND sb.scope_id = c.task_id AND sb.sha256 = c.result_ref))`},
	{"I14", "A", `SELECT c.task_id, format('调用 %s 的 result_ref %s 不是结算时记录的结果', c.call_id, c.result_ref)
		FROM calls c WHERE c.state = 'completed' AND c.source NOT IN ('cache', 'coalesced') AND NOT EXISTS (SELECT 1 FROM call_tries t JOIN blob_provenance p
			ON p.scope_kind = 'task' AND p.scope_id = t.task_id AND p.source = 'gateway' AND p.ref = t.call_id || '#' || t.try_no
			WHERE t.task_id = c.task_id AND t.call_id = c.call_id AND t.outcome = 'ok' AND p.sha256 = c.result_ref)`},
	// I14（缓存部分，§11.2、§11.4）：source = cache|coalesced 的调用由 Tx2（CompleteFromCache）完成——结果 blob 的
	// 来源记录为 <call_id>#<source>（与 result_ref 相同，即结果未被改写），且该调用没有任何 try（命中与合并不访问
	// 上游、不预留）。登记与授权由上面第一条（对全部 completed 调用）检查。
	{"I14", "A", `SELECT c.task_id, format('%s 来源的调用 %s 的 result_ref %s 不是 Tx2 记录的结果，或它有 %s 个 try',
			c.source, c.call_id, c.result_ref, (SELECT count(*) FROM call_tries t WHERE t.task_id = c.task_id AND t.call_id = c.call_id))
		FROM calls c WHERE c.state = 'completed' AND c.source IN ('cache', 'coalesced') AND (c.tries_used <> 0
			OR EXISTS (SELECT 1 FROM call_tries t WHERE t.task_id = c.task_id AND t.call_id = c.call_id)
			OR NOT EXISTS (SELECT 1 FROM blob_provenance p WHERE p.scope_kind = 'task' AND p.scope_id = c.task_id
				AND p.source = 'gateway' AND p.ref = c.call_id || '#' || c.source AND p.sha256 = c.result_ref))`},
	// I15（Q）：恢复完成后不存在没有活跃 attempt 的进行中 resolving 调用（resolving_since 非空 = 某个请求正在查缓存
	// 或预留，§11.2）。调用不记录正在解析它的 attempt，因此以"任务没有 active 的 attempt_access"判定：撤销访问后，
	// 进行中的 resolving 须已完成、失败，或由启动时的 ResetResolving 复位（resolving_since 为空，可被同指纹接管）。
	{"I15", "Q", `SELECT c.task_id, format('调用 %s 自 %s 起处于 resolving，任务没有 active 的 attempt', c.call_id, c.resolving_since)
		FROM calls c WHERE c.state = 'resolving' AND c.resolving_since IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM attempt_access a WHERE a.task_id = c.task_id AND a.state = 'active')`},
	// I16：已提交的请求都指向存在的资源——会话请求（create_session、wake_session）指向会话，其余指向任务。
	{"I16", "A", `SELECT r.request_id, format('%s 请求对应的资源 %L 不存在', r.kind, r.resource_id)
		FROM api_requests r WHERE r.resource_id = '' OR CASE WHEN r.kind IN ('create_session', 'wake_session')
			THEN NOT EXISTS (SELECT 1 FROM sessions s WHERE s.session_id = r.resource_id)
			ELSE NOT EXISTS (SELECT 1 FROM tasks t WHERE t.task_id = r.resource_id) END`},
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

// ReferencedBlobs 返回已登记产物、已提交 checkpoint 引用与 completed 调用结果的 blob（实现 invariants.Store）；
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
			UNION ALL
			SELECT c.result_ref, COALESCE(b.size, -1), format('call %s/%s', c.task_id, c.call_id)
				FROM calls c LEFT JOIN blobs b ON b.sha256 = c.result_ref
				WHERE c.state = 'completed'
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
