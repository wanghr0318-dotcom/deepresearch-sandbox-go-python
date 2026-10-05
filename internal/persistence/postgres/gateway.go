package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

var _ call.Store = (*Store)(nil)

// Gateway 事务用例（实现 call.Store；规格 §9.2、§9.4–§9.7、§11.2）。
//
// 锁顺序（规格 §7.1 Gateway 子集）：tasks FOR SHARE → task_control FOR SHARE → attempt_access FOR SHARE
// → budgets → calls → reservations。结算不复查访问（撤销不阻止宿主完成记账，§9.5），从 budgets 开始。
// 账本互斥记账：每笔 reservation 从 held 恰转入 settled（spent）、released 或 charged_unknown（unknown）之一，
// 同时从 reserved 扣除（I3）。

// callColumns 与 scanCall 一一对应。
const callColumns = `task_id, call_id, fingerprint, endpoint, state, source, COALESCE(result_ref, ''), tries_used,
	created_at, deadline_at, cost_charged, first_attempt_id, upstream_request_id, COALESCE(supersedes_call_id, ''),
	COALESCE(supersede_reason, ''), possible_external_duplicate, fail_reason, resolving_since`

func scanCall(row pgx.Row) (call.CallRecord, error) {
	var r call.CallRecord
	var state string
	err := row.Scan(&r.TaskID, &r.CallID, &r.Fingerprint, &r.Endpoint, &state, &r.Source, &r.ResultRef, &r.TriesUsed,
		&r.CreatedAt, &r.DeadlineAt, &r.CostCharged, &r.FirstAttemptID, &r.UpstreamRequestID, &r.SupersedesCallID,
		&r.SupersedeReason, &r.PossibleExternalDuplicate, &r.FailReason, &r.ResolvingSince)
	r.State = call.CallState(state)
	return r, err
}

// selectCallForUpdate 锁住调用行并读取它；不存在为 ErrNotFound。
func selectCallForUpdate(ctx context.Context, tx pgx.Tx, taskID, callID string) (call.CallRecord, error) {
	rec, err := scanCall(tx.QueryRow(ctx, "SELECT "+callColumns+" FROM calls WHERE task_id = $1 AND call_id = $2 FOR UPDATE",
		taskID, callID))
	if errors.Is(err, pgx.ErrNoRows) {
		return rec, notFoundf("调用 %s/%s", taskID, callID)
	}
	return rec, err
}

// admitAccess 是规格 §9.2 的访问判定；按 访问撤销 → 非当前 attempt → 已请求取消 的顺序给出原因。
func admitAccess(f call.AccessFacts) error {
	switch {
	case !f.Active:
		return rejectf(persistence.CodeAccessRevoked, "attempt %s 的访问不是 active", f.AttemptID)
	case !f.Current:
		return rejectf(persistence.CodeNotCurrentAttempt, "attempt %s 不是任务 %s 的当前 attempt", f.AttemptID, f.TaskID)
	case f.Desired == "cancel":
		return rejectf(persistence.CodeCancelRequested, "任务 %s 已请求取消", f.TaskID)
	}
	return nil
}

// checkAccessTx 在事务内按锁顺序以共享锁读取访问事实并判定：阻止并发的生命周期更新、控制写入与撤销
// 在本事务提交前改变这些事实（Gateway 请求会等待生命周期更新，§7.1）。
func checkAccessTx(ctx context.Context, tx pgx.Tx, taskID, attemptID string) error {
	f := call.AccessFacts{TaskID: taskID, AttemptID: attemptID}
	var current *string
	err := tx.QueryRow(ctx, "SELECT current_attempt_id FROM tasks WHERE task_id = $1 FOR SHARE", taskID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	if err != nil {
		return err
	}
	f.Current = current != nil && *current == attemptID
	if err := tx.QueryRow(ctx, "SELECT desired FROM task_control WHERE task_id = $1 FOR SHARE", taskID).Scan(&f.Desired); err != nil {
		return err
	}
	var state string
	err = tx.QueryRow(ctx, "SELECT state FROM attempt_access WHERE attempt_id = $1 AND task_id = $2 FOR SHARE", attemptID, taskID).Scan(&state)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	f.Active = state == "active"
	return admitAccess(f)
}

// lockBudget 锁住任务的账本行。
func lockBudget(ctx context.Context, tx pgx.Tx, taskID string) (call.Budget, error) {
	var b call.Budget
	err := tx.QueryRow(ctx, `SELECT limit_micro, reserved_micro, spent_micro, unknown_micro FROM budgets WHERE task_id = $1 FOR UPDATE`,
		taskID).Scan(&b.LimitMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, notFoundf("任务 %s 的预算", taskID)
	}
	return b, err
}

// CheckAccess 在一致快照（单条语句）中读取 §9.2 的访问事实（实现 call.Store）。
func (s *Store) CheckAccess(ctx context.Context, taskID, attemptID string) (call.AccessFacts, error) {
	f := call.AccessFacts{TaskID: taskID, AttemptID: attemptID}
	err := s.read(ctx, "CheckAccess", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT COALESCE(t.current_attempt_id = $2, false), c.desired,
				COALESCE((SELECT a.state = 'active' FROM attempt_access a WHERE a.attempt_id = $2 AND a.task_id = t.task_id), false)
			FROM tasks t JOIN task_control c USING (task_id) WHERE t.task_id = $1`, taskID, attemptID).
			Scan(&f.Current, &f.Desired, &f.Active)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		return err
	})
	return f, err
}

// BeginCall 是 Tx1（实现 call.Store）：复查访问；无记录则登记 resolving，deadline_at 按数据库时间
// = now() + Deadline；有记录则原样返回（Existing），由调用方按 §9.4 比较指纹与状态。
// 已由 ResetResolving 复位的 resolving（resolving_since 为空）且指纹相同时由本次请求接管：置 resolving_since = now()，
// 按新登记返回（Existing=false），created_at 与 deadline_at 不变（§9.7）。
// 提交结果未知后的重跑：本次调用自己登记或接管的行（同指纹、仍在解析、尚无 try）仍按新登记返回。
func (s *Store) BeginCall(ctx context.Context, r call.BeginCallRequest) (call.BeginCallResult, error) {
	if r.TaskID == "" || r.CallID == "" || r.AttemptID == "" || r.Fingerprint == "" || r.Endpoint == "" || r.Deadline <= 0 {
		return call.BeginCallResult{}, invalidf("BeginCall 缺少 task_id、call_id、attempt_id、指纹、端点，或期限不为正")
	}
	var out call.BeginCallResult
	inserted := false // 跨重跑保留：之前的某次尝试已插入（可能已提交）
	err := s.run(ctx, "BeginCall", r.TaskID+"/"+r.CallID, func(ctx context.Context, tx pgx.Tx) error {
		out = call.BeginCallResult{}
		if err := checkAccessTx(ctx, tx, r.TaskID, r.AttemptID); err != nil {
			return err
		}
		rec, err := scanCall(tx.QueryRow(ctx, `INSERT INTO calls (task_id, call_id, fingerprint, endpoint, state, source,
				created_at, deadline_at, first_attempt_id, supersedes_call_id, supersede_reason, resolving_since)
			VALUES ($1, $2, $3, $4, 'resolving', 'upstream', now(), now() + $5::bigint * interval '1 microsecond', $6, NULLIF($7, ''), NULLIF($8, ''), now())
			ON CONFLICT (task_id, call_id) DO NOTHING RETURNING `+callColumns,
			r.TaskID, r.CallID, r.Fingerprint, r.Endpoint, r.Deadline.Microseconds(), r.AttemptID, r.SupersedesCallID, r.SupersedeReason))
		if err == nil {
			inserted = true
			out.Record = rec
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if out.Record, err = selectCallForUpdate(ctx, tx, r.TaskID, r.CallID); err != nil {
			return err
		}
		rec = out.Record
		if rec.State == call.StateResolving && rec.ResolvingSince == nil && rec.TriesUsed == 0 && rec.Fingerprint == r.Fingerprint {
			out.Record, err = scanCall(tx.QueryRow(ctx, `UPDATE calls SET resolving_since = now() WHERE task_id = $1 AND call_id = $2
				RETURNING `+callColumns, r.TaskID, r.CallID))
			inserted = true
			return err
		}
		ours := inserted && rec.State == call.StateResolving && rec.ResolvingSince != nil && rec.TriesUsed == 0 &&
			rec.Fingerprint == r.Fingerprint
		out.Existing = !ours
		return nil
	})
	return out, err
}

// newReservationID 在事务前生成 reservation 身份（规格 §7.3）。
func newReservationID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "rsv-" + hex.EncodeToString(b), nil
}

// ReserveTry 是 Tx2（实现 call.Store）：复查访问、期限、累计次数与预算后分配 try_no、创建 held reservation、
// 增加 reserved、置调用为 in_flight。最近一次 try 仍持有预留时：同一身份（本次生成的 reservation_id，或
// 同 attempt、同环境、同估算的重试——提交结果未知后的再次调用，E11b）返回原 try，否则为 call_in_progress。
// 已存在的 try 先于一切检查查找并原样返回，不复查访问、期限与预算：预留已经发生，复查只会让它悬空。
func (s *Store) ReserveTry(ctx context.Context, r call.ReserveTryRequest) (call.Try, error) {
	if r.TaskID == "" || r.CallID == "" || r.AttemptID == "" || r.EstimateMicro < 0 || r.MaxTries < 1 {
		return call.Try{}, invalidf("ReserveTry 缺少 task_id、call_id、attempt_id，或估算为负、MaxTries < 1")
	}
	rid, err := newReservationID()
	if err != nil {
		return call.Try{}, err
	}
	var out call.Try
	err = s.run(ctx, "ReserveTry", r.TaskID+"/"+r.CallID+"@"+rid, func(ctx context.Context, tx pgx.Tx) error {
		out = call.Try{}
		if prev, ok, err := heldTry(ctx, tx, r, rid); err != nil || ok {
			out = prev
			return err
		}
		if err := checkAccessTx(ctx, tx, r.TaskID, r.AttemptID); err != nil {
			return err
		}
		budget, err := lockBudget(ctx, tx, r.TaskID)
		if err != nil {
			return err
		}
		var state string
		var tries int
		var expired bool
		err = tx.QueryRow(ctx, `SELECT state, tries_used, now() >= deadline_at FROM calls WHERE task_id = $1 AND call_id = $2 FOR UPDATE`,
			r.TaskID, r.CallID).Scan(&state, &tries, &expired)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s", r.TaskID, r.CallID)
		}
		if err != nil {
			return err
		}
		if tries > 0 {
			var prev call.Try
			var rstate, env string
			var amount int64
			if err := tx.QueryRow(ctx, `SELECT r.reservation_id, r.state, r.amount, t.attempt_id, COALESCE(t.env_id, '')
				FROM call_tries t JOIN reservations r USING (reservation_id)
				WHERE t.task_id = $1 AND t.call_id = $2 AND t.try_no = $3 FOR UPDATE OF r`, r.TaskID, r.CallID, tries).
				Scan(&prev.ReservationID, &rstate, &amount, &prev.AttemptID, &env); err != nil {
				return err
			}
			if rstate == "held" {
				if prev.ReservationID == rid || (prev.AttemptID == r.AttemptID && env == r.EnvID && amount == r.EstimateMicro) {
					prev.TaskID, prev.CallID, prev.TryNo = r.TaskID, r.CallID, tries
					out = prev
					return nil
				}
				return rejectf(persistence.CodeCallInProgress, "调用 %s 的 try %d 仍在执行", r.CallID, tries)
			}
		}
		switch {
		case state == string(call.StateCompleted):
			return conflictf("调用 %s/%s 已完成，不能新建 try", r.TaskID, r.CallID)
		case expired:
			return rejectf(persistence.CodeCallDeadlineExceeded, "调用 %s 已超过 deadline_at", r.CallID)
		case tries >= r.MaxTries:
			return rejectf(persistence.CodeTriesExhausted, "调用 %s 已用 %d 次 try（上限 %d）", r.CallID, tries, r.MaxTries)
		case budget.Available() <= 0:
			return rejectf(persistence.CodeBudgetExhausted, "任务 %s 预算可用 %d", r.TaskID, budget.Available())
		case budget.Available() < r.EstimateMicro:
			return rejectf(persistence.CodeBudgetInsufficient, "任务 %s 预算可用 %d，本次估算 %d", r.TaskID, budget.Available(), r.EstimateMicro)
		}
		tryNo := tries + 1
		if _, err := tx.Exec(ctx, `INSERT INTO reservations (reservation_id, task_id, call_id, try_no, kind, amount, state)
			VALUES ($1, $2, $3, $4, 'money', $5, 'held')`, rid, r.TaskID, r.CallID, tryNo, r.EstimateMicro); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO call_tries (task_id, call_id, try_no, attempt_id, env_id, state, reservation_id)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), 'in_flight', $6)`, r.TaskID, r.CallID, tryNo, r.AttemptID, r.EnvID, rid); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE budgets SET reserved_micro = reserved_micro + $2 WHERE task_id = $1", r.TaskID, r.EstimateMicro); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE calls SET state = 'in_flight', tries_used = $3, resolving_since = NULL WHERE task_id = $1 AND call_id = $2",
			r.TaskID, r.CallID, tryNo); err != nil {
			return err
		}
		out = call.Try{TaskID: r.TaskID, CallID: r.CallID, TryNo: tryNo, ReservationID: rid, AttemptID: r.AttemptID}
		return nil
	})
	return out, err
}

// heldTry 不加锁地查找本次请求已创建且仍持有预留的 try（ReserveTry 的幂等身份）：最近一次 try 的
// reservation 为 held，且是本次生成的 reservation_id，或同 attempt、同环境、同估算。读到的是已提交的事实。
func heldTry(ctx context.Context, tx pgx.Tx, r call.ReserveTryRequest, rid string) (call.Try, bool, error) {
	prev := call.Try{TaskID: r.TaskID, CallID: r.CallID}
	var env string
	var amount int64
	err := tx.QueryRow(ctx, `SELECT t.try_no, r.reservation_id, r.amount, t.attempt_id, COALESCE(t.env_id, '')
		FROM calls c JOIN call_tries t ON t.task_id = c.task_id AND t.call_id = c.call_id AND t.try_no = c.tries_used
		JOIN reservations r ON r.reservation_id = t.reservation_id
		WHERE c.task_id = $1 AND c.call_id = $2 AND r.state = 'held'`, r.TaskID, r.CallID).
		Scan(&prev.TryNo, &prev.ReservationID, &amount, &prev.AttemptID, &env)
	if errors.Is(err, pgx.ErrNoRows) {
		return call.Try{}, false, nil
	}
	if err != nil {
		return call.Try{}, false, err
	}
	if prev.ReservationID == rid || (prev.AttemptID == r.AttemptID && env == r.EnvID && amount == r.EstimateMicro) {
		return prev, true, nil
	}
	return call.Try{}, false, nil
}

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validateSettlement(st call.Settlement) error {
	t := st.Try
	if t.TaskID == "" || t.CallID == "" || t.TryNo < 1 || t.ReservationID == "" {
		return invalidf("SettleTry 缺少 task_id、call_id、try_no 或 reservation_id")
	}
	switch st.Outcome {
	case "ok":
		if !isSHA256Hex(st.ResultSHA256) || st.ResultSize < 0 || st.ActualMicro < 0 {
			return invalidf("ok 结算需要小写十六进制 sha256、非负的结果大小与实际费用")
		}
	case "retryable", "fatal", "unknown":
		if st.ActualMicro != 0 || st.ResultSHA256 != "" {
			return invalidf("%s 结算不带实际费用或结果（只有 ok 记入 spent）", st.Outcome)
		}
	default:
		return invalidf("结算结果必须是 ok、retryable、fatal 或 unknown，得到 %q", st.Outcome)
	}
	return nil
}

// recordResultBlob 登记调用结果 blob 并授权到任务 scope；同一 sha256 的大小不同为冲突（同 recordBlob）。
func recordResultBlob(ctx context.Context, tx pgx.Tx, st call.Settlement) error {
	if _, err := tx.Exec(ctx, "INSERT INTO blobs (sha256, size) VALUES ($1, $2) ON CONFLICT DO NOTHING", st.ResultSHA256, st.ResultSize); err != nil {
		return err
	}
	var size int64
	if err := tx.QueryRow(ctx, "SELECT size FROM blobs WHERE sha256 = $1", st.ResultSHA256).Scan(&size); err != nil {
		return err
	}
	if size != st.ResultSize {
		return conflictf("blob %s 已登记为 %d 字节，不是 %d", st.ResultSHA256, size, st.ResultSize)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO scope_blobs (scope_kind, scope_id, sha256) VALUES ('task', $1, $2) ON CONFLICT DO NOTHING",
		st.Try.TaskID, st.ResultSHA256); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "INSERT INTO blob_provenance (scope_kind, scope_id, sha256, source, ref) VALUES ('task', $1, $2, 'gateway', $3)",
		st.Try.TaskID, st.ResultSHA256, fmt.Sprintf("%s#%d", st.Try.CallID, st.Try.TryNo))
	return err
}

// SettleTry 完成一次 try 的结算（实现 call.Store；规格 §9.5、§9.6）。幂等以 reservation 状态为条件：
// 已不是 held 时不再改动账本，返回调用的当前记录（超时转 unknown 后迟到的结果不改变记账）。
func (s *Store) SettleTry(ctx context.Context, st call.Settlement) (call.CallRecord, error) {
	if err := validateSettlement(st); err != nil {
		return call.CallRecord{}, err
	}
	t := st.Try
	var out call.CallRecord
	err := s.run(ctx, "SettleTry", fmt.Sprintf("%s/%s#%d", t.TaskID, t.CallID, t.TryNo), func(ctx context.Context, tx pgx.Tx) error {
		if _, err := lockBudget(ctx, tx, t.TaskID); err != nil {
			return err
		}
		rec, err := selectCallForUpdate(ctx, tx, t.TaskID, t.CallID)
		if err != nil {
			return err
		}
		var rid, rstate string
		var amount int64
		err = tx.QueryRow(ctx, `SELECT reservation_id, state, amount FROM reservations WHERE task_id = $1 AND call_id = $2 AND try_no = $3 FOR UPDATE`,
			t.TaskID, t.CallID, t.TryNo).Scan(&rid, &rstate, &amount)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s 的 try %d", t.TaskID, t.CallID, t.TryNo)
		}
		if err != nil {
			return err
		}
		if rid != t.ReservationID {
			return conflictf("try %s/%s#%d 的 reservation 是 %s，不是 %s", t.TaskID, t.CallID, t.TryNo, rid, t.ReservationID)
		}
		if rstate != "held" {
			out = rec
			return nil
		}
		var newRes, callState string
		var spent, unknown, cost int64
		switch st.Outcome {
		case "ok":
			if err := recordResultBlob(ctx, tx, st); err != nil {
				return err
			}
			newRes, callState, spent, cost = "settled", string(call.StateCompleted), st.ActualMicro, st.ActualMicro
		case "retryable":
			newRes, callState = "released", string(rec.State)
		case "fatal":
			newRes, callState = "released", string(call.StateFailed)
		case "unknown":
			newRes, callState, unknown = "charged_unknown", string(call.StateUnknown), amount
		}
		if _, err := tx.Exec(ctx, `UPDATE budgets SET reserved_micro = reserved_micro - $2, spent_micro = spent_micro + $3,
			unknown_micro = unknown_micro + $4 WHERE task_id = $1`, t.TaskID, amount, spent, unknown); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE reservations SET state = $2 WHERE reservation_id = $1", rid, newRes); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE call_tries SET state = 'settled', outcome = $4, latency_ms = $5, cost_micro = $6, error = $7
			WHERE task_id = $1 AND call_id = $2 AND try_no = $3`, t.TaskID, t.CallID, t.TryNo, st.Outcome, st.LatencyMs, cost, st.Error); err != nil {
			return err
		}
		failReason := ""
		if st.Outcome == "fatal" {
			failReason = st.Error
			if failReason == "" {
				failReason = "fatal"
			}
		}
		out, err = scanCall(tx.QueryRow(ctx, `UPDATE calls SET state = $3,
				result_ref = CASE WHEN $3 = 'completed' THEN $4 ELSE result_ref END,
				cost_charged = cost_charged + $5,
				upstream_request_id = CASE WHEN $6 <> '' THEN $6 ELSE upstream_request_id END,
				possible_external_duplicate = possible_external_duplicate OR $7,
				fail_reason = CASE WHEN $8 <> '' THEN $8 ELSE fail_reason END
			WHERE task_id = $1 AND call_id = $2 RETURNING `+callColumns,
			t.TaskID, t.CallID, callState, st.ResultSHA256, spent, st.UpstreamRequestID, st.Outcome == "unknown", failReason))
		return err
	})
	return out, err
}

// FailCall 把没有执行中 try 的调用置为 failed（实现 call.Store），原因写入 fail_reason。已是 failed 时
// 不改变（幂等）；已完成为冲突；仍有持有预留的 try 时为 call_in_progress（须先结算）。
func (s *Store) FailCall(ctx context.Context, taskID, callID, reason string) error {
	if taskID == "" || callID == "" || reason == "" {
		return invalidf("FailCall 缺少 task_id、call_id 或原因")
	}
	return s.run(ctx, "FailCall", taskID+"/"+callID, func(ctx context.Context, tx pgx.Tx) error {
		rec, err := selectCallForUpdate(ctx, tx, taskID, callID)
		if err != nil {
			return err
		}
		switch rec.State {
		case call.StateFailed:
			return nil
		case call.StateCompleted:
			return conflictf("调用 %s/%s 已完成", taskID, callID)
		}
		var held bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM reservations WHERE task_id = $1 AND call_id = $2 AND state = 'held')`,
			taskID, callID).Scan(&held); err != nil {
			return err
		}
		if held {
			return rejectf(persistence.CodeCallInProgress, "调用 %s 仍有持有预留的 try", callID)
		}
		_, err = tx.Exec(ctx, "UPDATE calls SET state = 'failed', fail_reason = $3, resolving_since = NULL WHERE task_id = $1 AND call_id = $2", taskID, callID, reason)
		return err
	})
}

// ResetResolving 把遗留的、没有任何 try 的 resolving 调用复位为可重新解析（实现 call.Store；启动时调用，
// 规格 §11.2、§14.1 第 4 步）：清空 resolving_since，状态、created_at 与 deadline_at 不变（重启不重置期限，§9.7）。
// 下一次同指纹的 BeginCall 接管它。有 try 的调用不动。
func (s *Store) ResetResolving(ctx context.Context) (int, error) {
	var n int
	err := s.run(ctx, "ResetResolving", "resolving", func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE calls c SET resolving_since = NULL
			WHERE c.state = 'resolving' AND c.resolving_since IS NOT NULL AND c.tries_used = 0
			AND NOT EXISTS (SELECT 1 FROM call_tries t WHERE t.task_id = c.task_id AND t.call_id = c.call_id)`)
		if err != nil {
			return err
		}
		n = int(tag.RowsAffected())
		return nil
	})
	return n, err
}

// LoadBudget 读取任务的账本（实现 call.Store）。
func (s *Store) LoadBudget(ctx context.Context, taskID string) (call.Budget, error) {
	var b call.Budget
	err := s.read(ctx, "LoadBudget", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, "SELECT limit_micro, reserved_micro, spent_micro, unknown_micro FROM budgets WHERE task_id = $1", taskID).
			Scan(&b.LimitMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s 的预算", taskID)
		}
		return err
	})
	return b, err
}

// LoadCall 读取一个调用及其全部 try（按 try_no 升序；实现 call.Store）。单条语句，处于同一快照。
func (s *Store) LoadCall(ctx context.Context, taskID, callID string) (call.CallRecord, []call.TryRecord, error) {
	var rec call.CallRecord
	var tries []call.TryRecord
	err := s.read(ctx, "LoadCall", func(ctx context.Context, q queryer) error {
		tries = nil
		rows, err := q.Query(ctx, `WITH c AS (SELECT `+callColumns+` FROM calls WHERE task_id = $1 AND call_id = $2)
			SELECT c.*, t.try_no, t.attempt_id, COALESCE(t.env_id, ''), t.state, t.outcome, t.latency_ms, t.cost_micro,
				t.reservation_id, t.error
			FROM c LEFT JOIN call_tries t ON t.task_id = $1 AND t.call_id = $2 ORDER BY t.try_no`, taskID, callID)
		if err != nil {
			return err
		}
		defer rows.Close()
		found := false
		for rows.Next() {
			found = true
			var r call.CallRecord
			var state string
			var tryNo *int
			var tr call.TryRecord
			var attempt, env, tstate, outcome, rid, terr *string
			var latency, cost *int64
			if err := rows.Scan(&r.TaskID, &r.CallID, &r.Fingerprint, &r.Endpoint, &state, &r.Source, &r.ResultRef, &r.TriesUsed,
				&r.CreatedAt, &r.DeadlineAt, &r.CostCharged, &r.FirstAttemptID, &r.UpstreamRequestID, &r.SupersedesCallID,
				&r.SupersedeReason, &r.PossibleExternalDuplicate, &r.FailReason, &r.ResolvingSince,
				&tryNo, &attempt, &env, &tstate, &outcome, &latency, &cost, &rid, &terr); err != nil {
				return err
			}
			r.State = call.CallState(state)
			rec = r
			if tryNo == nil {
				continue
			}
			tr = call.TryRecord{TryNo: *tryNo, AttemptID: *attempt, EnvID: *env, State: *tstate, Outcome: *outcome,
				LatencyMs: *latency, CostMicro: *cost, ReservationID: *rid, Error: *terr}
			tries = append(tries, tr)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if !found {
			return notFoundf("调用 %s/%s", taskID, callID)
		}
		return nil
	})
	return rec, tries, err
}

// ListCalls 按登记时间列出任务的调用（实现 call.Store）。
func (s *Store) ListCalls(ctx context.Context, taskID string) ([]call.CallRecord, error) {
	var out []call.CallRecord
	err := s.read(ctx, "ListCalls", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT "+callColumns+" FROM calls WHERE task_id = $1 ORDER BY created_at, call_id", taskID)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (call.CallRecord, error) { return scanCall(r) })
		return err
	})
	return out, err
}
