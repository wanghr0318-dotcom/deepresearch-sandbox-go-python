package postgres

import (
	"bytes"
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/subrun"
)

// sub-run 生命周期（规格 §6、§8.4、§13；migration 0007）。状态转换一律经 subrun.Next（恢复时经
// subrun.ResumeDecision）判定，本文件只负责加锁、校验归属与写入。
//
// 锁顺序（events.go）：tasks → … → subruns → attempts → attempt_access → … → budgets → subrun_budgets。
// 同一任务的多行 subruns 一律按 subrun_id 升序加锁。生命周期写入先锁任务行（StartSubrun 取 FOR UPDATE 以串行化
// 插入与 4 个的上限，其余取 FOR SHARE），再锁 subruns 行。

// failureReasonMaxBytes 是 subrun_end{failed} 写入 failure_reason 的上限（UTF-8 字节）。
const failureReasonMaxBytes = 256

// sub-run 被宿主收尾时写入的 failure_reason。
const (
	failureTaskCancel           = "task_cancel"
	failureNotCompletedAtResult = "not_completed_at_result"
	failureTaskFailed           = "task_failed"       // 任务以 failed 结束时仍未终态（P14-T11）
	failureCheckpoint           = "checkpoint_failed" // checkpoint 列出 failed（Worker 未给原因）
)

const subrunColumns = `task_id, subrun_id, parent_step_id, definition_hash, status, bound_attempt_id, deadline_at,
	budget_cap_micro, COALESCE(result_ref, ''), failure_reason, cancel_reason, started_at, ended_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSubrun(row rowScanner) (subrun.Record, error) {
	var r subrun.Record
	var status string
	var ended *time.Time
	if err := row.Scan(&r.TaskID, &r.SubrunID, &r.ParentStepID, &r.DefinitionHash, &status, &r.BoundAttemptID, &r.DeadlineAt,
		&r.BudgetCapMicro, &r.ResultRef, &r.FailureReason, &r.CancelReason, &r.StartedAt, &ended); err != nil {
		return subrun.Record{}, err
	}
	r.Status = subrun.Status(status)
	if ended != nil {
		r.EndedAt = *ended
	}
	return r, nil
}

func collectSubruns(rows pgx.Rows) ([]subrun.Record, error) {
	defer rows.Close()
	var out []subrun.Record
	for rows.Next() {
		r, err := scanSubrun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// lockSubrunTask 以 mode（"UPDATE" 或 "SHARE"）锁住任务行；任务不存在为 ErrNotFound。
func lockSubrunTask(ctx context.Context, tx pgx.Tx, taskID, mode string) error {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM tasks WHERE task_id = $1 FOR "+mode, taskID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	return err
}

// lockSubrunRow 以 FOR UPDATE 锁住一行 sub-run；不存在为 ErrNotFound。
func lockSubrunRow(ctx context.Context, tx pgx.Tx, taskID, subrunID string) (subrun.Record, error) {
	r, err := scanSubrun(tx.QueryRow(ctx, "SELECT "+subrunColumns+" FROM subruns WHERE task_id = $1 AND subrun_id = $2 FOR UPDATE",
		taskID, subrunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, notFoundf("任务 %s 的 sub-run %s", taskID, subrunID)
	}
	return r, err
}

// writeSubrun 把 r 的可变字段写回；终态时 ended_at 取 now()（已结束则保留），否则为 NULL。endSummary 为 nil 时不改。
func writeSubrun(ctx context.Context, tx pgx.Tx, r subrun.Record, endSummary *string) (subrun.Record, error) {
	return scanSubrun(tx.QueryRow(ctx, `UPDATE subruns SET status = $3, bound_attempt_id = $4, result_ref = NULLIF($5, ''),
			failure_reason = $6, cancel_reason = $7, end_summary = COALESCE($8, end_summary),
			ended_at = CASE WHEN $9 THEN COALESCE(ended_at, now()) ELSE NULL END
		WHERE task_id = $1 AND subrun_id = $2 RETURNING `+subrunColumns,
		r.TaskID, r.SubrunID, string(r.Status), r.BoundAttemptID, r.ResultRef, r.FailureReason, r.CancelReason, endSummary,
		r.Status.Terminal()))
}

// defaultCancelReason 在 sub-run 走向取消族终态而此前没有取消原因时补上 reason。
func defaultCancelReason(r *subrun.Record, reason string) {
	if r.CancelReason == "" && (r.Status == subrun.Cancelled || r.Status == subrun.TimedOut) {
		r.CancelReason = reason
	}
}

// truncateUTF8 把 s 截断到至多 n 字节，不切开多字节字符。
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	i := n
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}

// StartSubrun 持久化 subrun_start（规格 §13.1、§13.5）。锁 tasks FOR UPDATE（串行化同一任务的插入与上限判定）→
// 复查 attempt 是当前 attempt、访问 active 且尚无判决 → 锁该任务全部 subruns 行。已有同 ID：定义哈希不同 →
// conflict；已终态 → subrun_closed；绑定的不是本 attempt（恢复时未经 rebindSubrunsTx）→ subrun_closed；否则返回原
// 记录（deadline_at 不变）。无此 ID：已有 4 行 → subrun_limit；否则插入 subruns（deadline_at = now() + deadline_ms）
// 与 subrun_budgets。
func (s *Store) StartSubrun(ctx context.Context, taskID, attemptID, subrunID string, d subrun.Definition) (subrun.Record, error) {
	if taskID == "" || attemptID == "" {
		return subrun.Record{}, invalidf("StartSubrun 缺少 task_id 或 attempt_id")
	}
	if !subrun.ValidID(subrunID) {
		return subrun.Record{}, invalidf("sub-run ID %q 不合法", subrunID)
	}
	if err := d.Validate(); err != nil {
		return subrun.Record{}, invalidf("%v", err)
	}
	hash := d.Hash()
	var out subrun.Record
	err := s.run(ctx, "StartSubrun", taskID+"/"+subrunID, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSubrunTask(ctx, tx, taskID, "UPDATE"); err != nil {
			return err
		}
		if err := fenceAttempt(ctx, tx, taskID, attemptID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT "+subrunColumns+" FROM subruns WHERE task_id = $1 ORDER BY subrun_id FOR UPDATE", taskID)
		if err != nil {
			return err
		}
		existing, err := collectSubruns(rows)
		if err != nil {
			return err
		}
		for _, r := range existing {
			if r.SubrunID != subrunID {
				continue
			}
			switch {
			case !bytes.Equal(r.DefinitionHash, hash):
				return rejectf(persistence.CodeConflict, "sub-run %s 已以不同定义启动", subrunID)
			case r.Status.Terminal():
				return rejectf(persistence.CodeSubrunClosed, "sub-run %s 已是 %s", subrunID, r.Status)
			case r.BoundAttemptID != attemptID:
				return rejectf(persistence.CodeSubrunClosed, "sub-run %s 绑定于 attempt %s，不是 %s", subrunID, r.BoundAttemptID, attemptID)
			}
			out = r
			return nil
		}
		if len(existing) >= subrun.MaxPerTask {
			return rejectf(persistence.CodeSubrunLimit, "任务 %s 已有 %d 个 sub-run", taskID, len(existing))
		}
		out, err = scanSubrun(tx.QueryRow(ctx, `INSERT INTO subruns (task_id, subrun_id, definition_hash, parent_step_id, status,
				bound_attempt_id, deadline_at, budget_cap_micro)
			VALUES ($1, $2, $3, $4, 'started', $5, now() + $6 * interval '1 millisecond', $7)
			RETURNING `+subrunColumns, taskID, subrunID, hash, d.ParentStepID, attemptID, d.DeadlineMS, d.BudgetCapMicro))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO subrun_budgets (task_id, subrun_id, cap_micro) VALUES ($1, $2, $3)",
			taskID, subrunID, d.BudgetCapMicro)
		return err
	})
	return out, err
}

// ProposeSubrunEnd 持久化 subrun_end（规格 §13.3）：succeeded → end_proposed（保存 summary）；failed → failed
// （failure_reason = summary 前 256 字节）；cancelled → 按已记录的取消原因为 cancelled 或 timed_out（未处于
// cancel_requested 时视为 orchestrator 取消）。转换由 subrun.Next 判定，不允许 → invalid_transition；同一终态的重复
// 确认返回原记录。attempt 须为当前 attempt，sub-run 须绑定于它（否则 subrun_closed）；未知 sub-run 为 ErrNotFound。
func (s *Store) ProposeSubrunEnd(ctx context.Context, taskID, attemptID, subrunID, status, summary string) (subrun.Record, error) {
	var ev subrun.Event
	switch status {
	case protocol.SubrunEndSucceeded:
		ev = subrun.EvEndSucceeded
	case protocol.SubrunEndFailed:
		ev = subrun.EvEndFailed
	case protocol.SubrunEndCancelled:
		ev = subrun.EvEndCancelled
	default:
		return subrun.Record{}, invalidf("subrun_end 状态 %q 不合法", status)
	}
	if taskID == "" || attemptID == "" || subrunID == "" {
		return subrun.Record{}, invalidf("ProposeSubrunEnd 缺少 task_id、attempt_id 或 subrun_id")
	}
	var out subrun.Record
	err := s.run(ctx, "ProposeSubrunEnd", taskID+"/"+subrunID, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSubrunTask(ctx, tx, taskID, "SHARE"); err != nil {
			return err
		}
		if err := fenceAttempt(ctx, tx, taskID, attemptID); err != nil {
			return err
		}
		r, err := lockSubrunRow(ctx, tx, taskID, subrunID)
		if err != nil {
			return err
		}
		if r.BoundAttemptID != attemptID {
			return rejectf(persistence.CodeSubrunClosed, "sub-run %s 绑定于 attempt %s，不是 %s", subrunID, r.BoundAttemptID, attemptID)
		}
		to, err := subrun.Next(r.Status, ev, r.CancelReason)
		if err != nil {
			return rejectf(persistence.CodeInvalidTransition, "sub-run %s：%v", subrunID, err)
		}
		if to == r.Status {
			out = r // 同一终态的重复确认
			return nil
		}
		r.Status = to
		var endSummary *string
		switch ev {
		case subrun.EvEndSucceeded:
			endSummary = &summary
		case subrun.EvEndFailed:
			r.FailureReason = truncateUTF8(summary, failureReasonMaxBytes)
		}
		defaultCancelReason(&r, subrun.ReasonOrchestrator)
		out, err = writeSubrun(ctx, tx, r, endSummary)
		return err
	})
	return out, err
}

// RequestSubrunCancel 由宿主发起取消（规格 §13.4）：非终态 → cancel_requested 并记录 reason；已 cancel_requested
// 或终态 → 返回原记录（幂等，原因不变）。reason 取 subrun.Reason*。
func (s *Store) RequestSubrunCancel(ctx context.Context, taskID, subrunID, reason string) (subrun.Record, error) {
	switch reason {
	case subrun.ReasonDeadline, subrun.ReasonOrchestrator, subrun.ReasonTaskCancel, subrun.ReasonPolicy:
	default:
		return subrun.Record{}, invalidf("取消原因 %q 不合法", reason)
	}
	if taskID == "" || subrunID == "" {
		return subrun.Record{}, invalidf("RequestSubrunCancel 缺少 task_id 或 subrun_id")
	}
	var out subrun.Record
	err := s.run(ctx, "RequestSubrunCancel", taskID+"/"+subrunID, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockSubrunTask(ctx, tx, taskID, "SHARE"); err != nil {
			return err
		}
		r, err := lockSubrunRow(ctx, tx, taskID, subrunID)
		if err != nil {
			return err
		}
		if r.Status == subrun.CancelRequested || r.Status.Terminal() {
			out = r
			return nil
		}
		to, err := subrun.Next(r.Status, subrun.EvCancelRequest, reason)
		if err != nil {
			return rejectf(persistence.CodeInvalidTransition, "sub-run %s：%v", subrunID, err)
		}
		r.Status, r.CancelReason = to, reason
		out, err = writeSubrun(ctx, tx, r, nil)
		return err
	})
	return out, err
}

// ListSubruns 返回任务的全部 sub-run（按启动时间、ID 排序）。
func (s *Store) ListSubruns(ctx context.Context, taskID string) ([]subrun.Record, error) {
	var out []subrun.Record
	err := s.read(ctx, "ListSubruns", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT "+subrunColumns+" FROM subruns WHERE task_id = $1 ORDER BY started_at, subrun_id", taskID)
		if err != nil {
			return err
		}
		out, err = collectSubruns(rows)
		return err
	})
	return out, err
}

const subrunBudgetColumns = "cap_micro, reserved_micro, spent_micro, unknown_micro"

func scanSubrunBudget(row rowScanner) (subrun.Budget, error) {
	var b subrun.Budget
	err := row.Scan(&b.CapMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro)
	return b, err
}

// LoadSubrunBudget 读取 sub-run 层账本；不存在为 ErrNotFound。
func (s *Store) LoadSubrunBudget(ctx context.Context, taskID, subrunID string) (subrun.Budget, error) {
	var out subrun.Budget
	err := s.read(ctx, "LoadSubrunBudget", func(ctx context.Context, q queryer) error {
		var err error
		out, err = scanSubrunBudget(q.QueryRow(ctx, "SELECT "+subrunBudgetColumns+" FROM subrun_budgets WHERE task_id = $1 AND subrun_id = $2",
			taskID, subrunID))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s 的 sub-run %s 的账本", taskID, subrunID)
		}
		return err
	})
	return out, err
}

// ---- 事务内辅助：由 CommitCheckpoint、创建 attempt、最终裁决与 Gateway 的事务调用，不自行开启事务 ----

// applyCheckpointSubrunsTx 在 CommitCheckpoint 事务中按 subrun_id 升序锁定 checkpoint 列出的 sub-run 并逐项转换
// （规格 §8.4）：completed 须带 result_ref 并写入；failed、cancelled 收尾；started 只是确认。列出未知 ID、转换不允许、
// completed 缺 result_ref 或与已记录的 result_ref 不同 → invalid_transition，调用方回滚整个 checkpoint（E41）。
func applyCheckpointSubrunsTx(ctx context.Context, tx pgx.Tx, taskID string, entries []protocol.CheckpointSubrun) error {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.SubrunID
	}
	rows, err := tx.Query(ctx, "SELECT "+subrunColumns+` FROM subruns WHERE task_id = $1 AND subrun_id = ANY($2)
		ORDER BY subrun_id FOR UPDATE`, taskID, ids)
	if err != nil {
		return err
	}
	locked, err := collectSubruns(rows)
	if err != nil {
		return err
	}
	byID := make(map[string]subrun.Record, len(locked))
	for _, r := range locked {
		byID[r.SubrunID] = r
	}
	for _, e := range entries {
		r, ok := byID[e.SubrunID]
		if !ok {
			return rejectf(persistence.CodeInvalidTransition, "checkpoint 列出未知 sub-run %q", e.SubrunID)
		}
		var ev subrun.Event
		switch e.Status {
		case protocol.CheckpointSubrunStarted:
			ev = subrun.EvCheckpointStarted
		case protocol.CheckpointSubrunCompleted:
			if e.ResultRef == "" {
				return rejectf(persistence.CodeInvalidTransition, "checkpoint 列出 sub-run %s completed 但没有 result_ref", e.SubrunID)
			}
			ev = subrun.EvCheckpointCompleted
		case protocol.CheckpointSubrunFailed:
			ev = subrun.EvCheckpointFailed
		case protocol.CheckpointSubrunCancelled:
			ev = subrun.EvCheckpointCancelled
		default:
			return rejectf(persistence.CodeInvalidTransition, "checkpoint 中 sub-run %s 的状态 %q 不合法", e.SubrunID, e.Status)
		}
		to, err := subrun.Next(r.Status, ev, r.CancelReason)
		if err != nil {
			return rejectf(persistence.CodeInvalidTransition, "sub-run %s：%v", e.SubrunID, err)
		}
		if to == r.Status {
			if to == subrun.Completed && e.ResultRef != r.ResultRef {
				return rejectf(persistence.CodeInvalidTransition, "sub-run %s 已以 result_ref %s 完成", e.SubrunID, r.ResultRef)
			}
			continue
		}
		r.Status = to
		switch to {
		case subrun.Completed:
			r.ResultRef = e.ResultRef
		case subrun.Failed:
			if r.FailureReason == "" {
				r.FailureReason = failureCheckpoint
			}
		}
		defaultCancelReason(&r, subrun.ReasonOrchestrator)
		if byID[e.SubrunID], err = writeSubrun(ctx, tx, r, nil); err != nil {
			return err
		}
	}
	return nil
}

// rebindSubrunsTx 在创建 attempt 的事务中（新 attempt 行已插入之后）按 subrun.ResumeDecision 处理任务的全部
// sub-run（规格 §13.5）：cancel_requested 按取消原因收尾为 cancelled / timed_out（E40）；deadline_at 已过的
// started / end_proposed → timed_out（E45）；其余 started / end_proposed 绑定到新 attempt 并回到 started（E42）；
// 终态不变。返回告知 Worker 的列表（按 subrun_id 排序）。时间取数据库 now()，与 deadline_at 同源。
//
// 暂停期间不计时（规格 §13.5 执行中修订，M4 验收 2026-10-06）：暂停时记录了 remaining_ms 的未终态 sub-run 先按
// deadline_at = now() + remaining_ms 重新起算（剩余 0 即在此刻过期 → timed_out），并清除 remaining_ms；没有记录的
// （故障重试、server 重启）按原 deadline_at 判定，恢复不重置。
func rebindSubrunsTx(ctx context.Context, tx pgx.Tx, taskID, newAttemptID string) ([]protocol.ResumeSubrun, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE subruns SET remaining_ms = NULL,
			deadline_at = CASE WHEN status IN ('started', 'end_proposed', 'cancel_requested')
				THEN now() + remaining_ms * interval '1 millisecond' ELSE deadline_at END
		WHERE task_id = $1 AND remaining_ms IS NOT NULL`, taskID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, "SELECT "+subrunColumns+" FROM subruns WHERE task_id = $1 ORDER BY subrun_id FOR UPDATE", taskID)
	if err != nil {
		return nil, err
	}
	all, err := collectSubruns(rows)
	if err != nil {
		return nil, err
	}
	out := make([]protocol.ResumeSubrun, 0, len(all))
	for _, r := range all {
		if !r.Status.Terminal() {
			status, rerun := subrun.ResumeDecision(r, now)
			r.Status = status
			if rerun {
				r.BoundAttemptID = newAttemptID
			}
			defaultCancelReason(&r, subrun.ReasonDeadline) // 未请求取消而过期：按 deadline 收尾
			if r, err = writeSubrun(ctx, tx, r, nil); err != nil {
				return nil, err
			}
		}
		out = append(out, protocol.ResumeSubrun{SubrunID: r.SubrunID, Status: string(r.Status), ResultRef: r.ResultRef})
	}
	return out, nil
}

// closeOpenSubrunsTx 在任务进入终态的事务中收尾非终态 sub-run（规格 §8.4；I13：终态任务不留下未终态的 sub-run）：
// status 为 cancelled → cancelled（failure_reason = task_cancel）；succeeded → failed（not_completed_at_result）；
// failed → failed（task_failed）；非终态（queued、paused 等）不改动，留待恢复时重新绑定。调用方：最终裁决
// （FinalizeAttempt）、没有 attempt 的控制转换（ApplyControl → cancelled）与会话 turn 的 FailQueuedTurn。
func closeOpenSubrunsTx(ctx context.Context, tx pgx.Tx, taskID, status string) error {
	var ev subrun.Event
	var reason string
	switch status {
	case "cancelled":
		ev, reason = subrun.EvTaskCancelled, failureTaskCancel
	case "succeeded":
		ev, reason = subrun.EvTaskSucceeded, failureNotCompletedAtResult
	case "failed":
		ev, reason = subrun.EvTaskFailed, failureTaskFailed
	default:
		return nil
	}
	rows, err := tx.Query(ctx, "SELECT "+subrunColumns+` FROM subruns
		WHERE task_id = $1 AND status IN ('started', 'end_proposed', 'cancel_requested') ORDER BY subrun_id FOR UPDATE`, taskID)
	if err != nil {
		return err
	}
	open, err := collectSubruns(rows)
	if err != nil {
		return err
	}
	for _, r := range open {
		to, err := subrun.Next(r.Status, ev, r.CancelReason)
		if err != nil {
			return rejectf(persistence.CodeInvalidTransition, "sub-run %s：%v", r.SubrunID, err)
		}
		r.Status, r.FailureReason = to, reason
		defaultCancelReason(&r, subrun.ReasonTaskCancel)
		if _, err := writeSubrun(ctx, tx, r, nil); err != nil {
			return err
		}
	}
	return nil
}

// lockSubrunForCallTx 在 Gateway 事务中以 FOR SHARE 锁住调用归属的 sub-run（规格 §9.2）：不存在、不是 started 或
// 未绑定当前 attempt → subrun_closed。
func lockSubrunForCallTx(ctx context.Context, tx pgx.Tx, taskID, subrunID, attemptID string) (subrun.Record, error) {
	r, err := scanSubrun(tx.QueryRow(ctx, "SELECT "+subrunColumns+" FROM subruns WHERE task_id = $1 AND subrun_id = $2 FOR SHARE",
		taskID, subrunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, rejectf(persistence.CodeSubrunClosed, "任务 %s 没有 sub-run %s", taskID, subrunID)
	}
	if err != nil {
		return r, err
	}
	if r.Status != subrun.Started || r.BoundAttemptID != attemptID {
		return r, rejectf(persistence.CodeSubrunClosed, "sub-run %s 处于 %s、绑定于 attempt %s（调用方 %s）",
			subrunID, r.Status, r.BoundAttemptID, attemptID)
	}
	return r, nil
}

// lockSubrunBudgetTx 以 FOR UPDATE 锁住 sub-run 层账本（锁顺序位于 budgets 之后）；不存在为 ErrNotFound。
func lockSubrunBudgetTx(ctx context.Context, tx pgx.Tx, taskID, subrunID string) (subrun.Budget, error) {
	b, err := scanSubrunBudget(tx.QueryRow(ctx, "SELECT "+subrunBudgetColumns+` FROM subrun_budgets
		WHERE task_id = $1 AND subrun_id = $2 FOR UPDATE`, taskID, subrunID))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, notFoundf("任务 %s 的 sub-run %s 的账本", taskID, subrunID)
	}
	return b, err
}

// adjustSubrunBudgetTx 按增量调整 sub-run 层账本；任一桶变为负数时由 CHECK 约束拒绝（调用方回滚）。
func adjustSubrunBudgetTx(ctx context.Context, tx pgx.Tx, taskID, subrunID string, dReserved, dSpent, dUnknown int64) error {
	tag, err := tx.Exec(ctx, `UPDATE subrun_budgets SET reserved_micro = reserved_micro + $3, spent_micro = spent_micro + $4,
		unknown_micro = unknown_micro + $5 WHERE task_id = $1 AND subrun_id = $2`, taskID, subrunID, dReserved, dSpent, dUnknown)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return notFoundf("任务 %s 的 sub-run %s 的账本", taskID, subrunID)
	}
	return nil
}

// suspendSubrunDeadlinesTx 在任务被暂停的事务中（暂停裁决 FinalizeAttempt、没有 attempt 的 ApplyControl queued →
// paused）为未终态 sub-run 记录剩余时间 remaining_ms = GREATEST(deadline_at − now(), 0)（规格 §13.5 执行中修订：
// 暂停期间 deadline 不计时，恢复时 rebindSubrunsTx 重新起算）。已记录的不覆盖：恢复前再次暂停（例如继续后尚未
// 创建 attempt 又暂停）时 deadline_at 仍是暂停前的值，重算会把暂停时间计入。调用方已按锁顺序持有 subruns 之前的锁。
func suspendSubrunDeadlinesTx(ctx context.Context, tx pgx.Tx, taskID string) error {
	_, err := tx.Exec(ctx, `UPDATE subruns SET remaining_ms = GREATEST(0, floor(extract(epoch FROM deadline_at - now()) * 1000))::bigint
		WHERE task_id = $1 AND status IN ('started', 'end_proposed', 'cancel_requested') AND remaining_ms IS NULL`, taskID)
	return err
}
