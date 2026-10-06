package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

var _ call.ExecStore = (*Store)(nil)

// exec 事务用例（实现 call.ExecStore；规格 §10.2–§10.4；Plan 15 D6、D9、D10）。
//
// 锁顺序（§7.1 Gateway 子集）：tasks FOR SHARE → task_control FOR SHARE → subruns FOR SHARE（调用属于 sub-run 时）
// → attempt_access FOR SHARE → exec_quotas → calls → reservations → call_tries。新 try 的 environments 行在 calls
// 加锁之后插入：它是新主键，没有其他事务会先锁住它；此前 try 的环境只做不加锁的读取（stopped_at 只会由空变为非空，
// 读到的至多是偏保守的"未停止"）。exec 不触及 budgets 与 subrun_budgets（CPU 配额是任务级）。
//
// 账本互斥记账（I3 cpu 组）：每笔 cpu reservation 从 held 恰转入 settled（cpu_spent，按实测或全额预留）或
// charged_unknown（cpu_unknown）之一，同时从 cpu_reserved 扣除；exec 没有 released（停止后总有用量可记）。

// execQuotaColumns 与 scanExecQuota 一一对应。
const execQuotaColumns = `exec_count_limit, exec_count_used, cpu_limit_usec, cpu_reserved_usec, cpu_spent_usec, cpu_unknown_usec,
	wall_limit_ms, wall_spent_ms, blocked`

func scanExecQuota(row pgx.Row) (call.ExecQuota, error) {
	var q call.ExecQuota
	err := row.Scan(&q.CountLimit, &q.CountUsed, &q.CPULimitUsec, &q.CPUReservedUsec, &q.CPUSpentUsec, &q.CPUUnknownUsec,
		&q.WallLimitMs, &q.WallSpentMs, &q.Blocked)
	return q, err
}

// lockExecQuotaTx 锁住任务的 exec_quotas 行；policy 非空且行不存在时先按它建立（首次 exec，D9；并发的建立由主键
// 串行，后到者不改动已有行）。不存在（且未建立）为 ErrNotFound。
func lockExecQuotaTx(ctx context.Context, tx pgx.Tx, taskID string, policy *call.ExecPolicy) (call.ExecQuota, error) {
	if policy != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO exec_quotas (task_id, exec_count_limit, cpu_limit_usec, wall_limit_ms)
			VALUES ($1, $2, $3, $4) ON CONFLICT (task_id) DO NOTHING`, taskID, policy.CountLimit, policy.CPULimitUsec, policy.WallLimitMs); err != nil {
			return call.ExecQuota{}, err
		}
	}
	q, err := scanExecQuota(tx.QueryRow(ctx, "SELECT "+execQuotaColumns+" FROM exec_quotas WHERE task_id = $1 FOR UPDATE", taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return q, notFoundf("任务 %s 的 exec 配额", taskID)
	}
	return q, err
}

// execTryByReservation 不加锁地按 reservation 身份查找已创建的 exec try（ReserveExec 的幂等身份）；读到的是已提交的事实。
func execTryByReservation(ctx context.Context, tx pgx.Tx, rid string) (call.ExecTry, bool, error) {
	var out call.ExecTry
	var kind string
	err := tx.QueryRow(ctx, `SELECT t.task_id, t.call_id, t.try_no, t.attempt_id, COALESCE(t.env_id, ''), r.kind
		FROM reservations r JOIN call_tries t USING (reservation_id) WHERE r.reservation_id = $1`, rid).
		Scan(&out.TaskID, &out.CallID, &out.TryNo, &out.AttemptID, &out.EnvID, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return call.ExecTry{}, false, nil
	}
	if err != nil {
		return call.ExecTry{}, false, err
	}
	if kind != "cpu" {
		return call.ExecTry{}, false, conflictf("reservation %s 是 %s 预留，不是 exec try", rid, kind)
	}
	out.ReservationID = rid
	return out, true, nil
}

// ReserveExec 是 exec 的 Tx2（实现 call.ExecStore）。已存在 ReservationID 的 try 先于一切检查原样返回（COMMIT 丢失
// 后的重试，E11b 同法）：预留已经发生，复查只会让它悬空。检查顺序：访问 → 调用（端点、归属）→ 在途（持有预留的
// try、未确认停止的环境）→ 已完成 → 期限 → 累计 try → 配额（blocked → 次数 → CPU → wall）。
//
// 次数按 count_used + 在途（held）exec 判定：exec_count 在启动后的结算中才扣除，在途的 exec 可能都会启动，
// 不计入它们会让并发预留超过上限（保守：启动失败的在途 exec 结算后名额即恢复）。
func (s *Store) ReserveExec(ctx context.Context, r call.ReserveExecRequest) (call.ExecTry, error) {
	if r.TaskID == "" || r.CallID == "" || r.AttemptID == "" || r.ReservationID == "" || r.CPUEstimateUsec < 0 || r.WallMs < 0 ||
		r.QueueMs < 0 || r.MaxTries < 1 || r.Policy.CountLimit < 0 || r.Policy.CPULimitUsec < 0 || r.Policy.WallLimitMs < 0 {
		return call.ExecTry{}, invalidf("ReserveExec 缺少 task_id、call_id、attempt_id、reservation_id，或估算、时间、策略为负、MaxTries < 1")
	}
	var out call.ExecTry
	err := s.run(ctx, "ReserveExec", r.TaskID+"/"+r.CallID+"@"+r.ReservationID, func(ctx context.Context, tx pgx.Tx) error {
		out = call.ExecTry{}
		if prev, ok, err := execTryByReservation(ctx, tx, r.ReservationID); err != nil || ok {
			if ok && (prev.TaskID != r.TaskID || prev.CallID != r.CallID || prev.AttemptID != r.AttemptID) {
				return conflictf("reservation %s 属于 %s/%s（attempt %s）", r.ReservationID, prev.TaskID, prev.CallID, prev.AttemptID)
			}
			out = prev
			return err
		}
		if err := checkAccessTx(ctx, tx, r.TaskID, r.AttemptID, r.SubrunID); err != nil {
			return err
		}
		q, err := lockExecQuotaTx(ctx, tx, r.TaskID, &r.Policy)
		if err != nil {
			return err
		}
		var endpoint, state, callSubrun string
		var tries int
		var expired bool
		err = tx.QueryRow(ctx, `SELECT endpoint, state, tries_used, now() >= deadline_at, COALESCE(subrun_id, '') FROM calls
			WHERE task_id = $1 AND call_id = $2 FOR UPDATE`, r.TaskID, r.CallID).Scan(&endpoint, &state, &tries, &expired, &callSubrun)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s", r.TaskID, r.CallID)
		}
		if err != nil {
			return err
		}
		if endpoint != call.ExecEndpoint {
			return conflictf("调用 %s/%s 的端点是 %s，不是 %s", r.TaskID, r.CallID, endpoint, call.ExecEndpoint)
		}
		if err := sameSubrun(call.CallRecord{TaskID: r.TaskID, CallID: r.CallID, SubrunID: callSubrun}, r.SubrunID); err != nil {
			return err
		}
		var held bool
		var live int
		if err := tx.QueryRow(ctx, `SELECT
				EXISTS (SELECT 1 FROM reservations WHERE task_id = $1 AND call_id = $2 AND state = 'held'),
				(SELECT count(*) FROM call_tries t JOIN environments e ON e.env_id = t.env_id
					WHERE t.task_id = $1 AND t.call_id = $2 AND e.stopped_at IS NULL)`, r.TaskID, r.CallID).Scan(&held, &live); err != nil {
			return err
		}
		switch {
		case held:
			return rejectf(persistence.CodeCallInProgress, "调用 %s 的 try %d 仍持有预留", r.CallID, tries)
		case live > 0:
			return rejectf(persistence.CodeCallInProgress, "调用 %s 有 %d 个此前 try 的 exec 环境尚未确认停止", r.CallID, live)
		case state == string(call.StateCompleted):
			return conflictf("调用 %s/%s 已完成，不能新建 try", r.TaskID, r.CallID)
		case expired:
			return rejectf(persistence.CodeCallDeadlineExceeded, "调用 %s 已超过 deadline_at", r.CallID)
		case tries >= r.MaxTries:
			return rejectf(persistence.CodeTriesExhausted, "调用 %s 已用 %d 次 try（上限 %d）", r.CallID, tries, r.MaxTries)
		}
		var inflight int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM reservations WHERE task_id = $1 AND kind = 'cpu' AND state = 'held'",
			r.TaskID).Scan(&inflight); err != nil {
			return err
		}
		switch {
		case q.Blocked:
			return rejectf(call.CodeExecBlocked, "任务 %s 的 CPU 已超额（spent %d > limit %d），不再接受 exec", r.TaskID, q.CPUSpentUsec, q.CPULimitUsec)
		case q.CountUsed+inflight >= q.CountLimit:
			return rejectf(call.CodeExecQuotaExhausted, "任务 %s 的 exec 次数已用 %d、在途 %d（上限 %d）", r.TaskID, q.CountUsed, inflight, q.CountLimit)
		case q.CPUAvailable() < r.CPUEstimateUsec:
			return rejectf(call.CodeExecCPUExhausted, "任务 %s 的 CPU 可用 %d usec，本次预留 %d", r.TaskID, q.CPUAvailable(), r.CPUEstimateUsec)
		case q.WallSpentMs+r.WallMs > q.WallLimitMs:
			return rejectf(call.CodeExecWallExhausted, "任务 %s 的 wall 已用 %d ms，本次 %d（上限 %d）", r.TaskID, q.WallSpentMs, r.WallMs, q.WallLimitMs)
		}
		tryNo := tries + 1
		envID := call.ExecEnvID(r.TaskID, r.CallID, tryNo)
		if _, err := tx.Exec(ctx, "INSERT INTO environments (env_id, kind, attempt_id, status) VALUES ($1, 'exec', $2, 'creating')",
			envID, r.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO reservations (reservation_id, task_id, call_id, try_no, kind, amount, state)
			VALUES ($1, $2, $3, $4, 'cpu', $5, 'held')`, r.ReservationID, r.TaskID, r.CallID, tryNo, r.CPUEstimateUsec); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO call_tries (task_id, call_id, try_no, attempt_id, env_id, state, reservation_id, queue_ms)
			VALUES ($1, $2, $3, $4, $5, 'in_flight', $6, $7)`, r.TaskID, r.CallID, tryNo, r.AttemptID, envID, r.ReservationID, r.QueueMs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE exec_quotas SET cpu_reserved_usec = cpu_reserved_usec + $2 WHERE task_id = $1",
			r.TaskID, r.CPUEstimateUsec); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE calls SET state = 'in_flight', source = 'exec', tries_used = $3, resolving_since = NULL
			WHERE task_id = $1 AND call_id = $2`, r.TaskID, r.CallID, tryNo); err != nil {
			return err
		}
		out = call.ExecTry{Try: call.Try{TaskID: r.TaskID, CallID: r.CallID, TryNo: tryNo, ReservationID: r.ReservationID,
			AttemptID: r.AttemptID}, EnvID: envID}
		return nil
	})
	return out, err
}

func validExecTry(t call.ExecTry) bool {
	return t.TaskID != "" && t.CallID != "" && t.TryNo >= 1 && t.ReservationID != "" && t.AttemptID != ""
}

// MarkExecStarting 在 StartExec 之前复查访问并写 exec_started_at（实现 call.ExecStore；I12 [A]）。访问检查与撤销、
// 取消在 attempt_access / task_control 上串行：本事务先提交则撤销在其后（exec_started_at 不晚于 revoked_at），
// 撤销先提交则本事务被拒、调用方不启动。try 已结算为 ErrConflict；已写过时保持原值。
func (s *Store) MarkExecStarting(ctx context.Context, t call.ExecTry) error {
	if !validExecTry(t) {
		return invalidf("MarkExecStarting 缺少 task_id、call_id、try_no、reservation_id 或 attempt_id")
	}
	return s.run(ctx, "MarkExecStarting", fmt.Sprintf("%s/%s#%d", t.TaskID, t.CallID, t.TryNo), func(ctx context.Context, tx pgx.Tx) error {
		// 调用的 sub-run 归属在登记后不变：先不加锁读出，以便访问检查按锁顺序锁住该 sub-run。
		var sub string
		err := tx.QueryRow(ctx, "SELECT COALESCE(subrun_id, '') FROM calls WHERE task_id = $1 AND call_id = $2", t.TaskID, t.CallID).Scan(&sub)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s", t.TaskID, t.CallID)
		}
		if err != nil {
			return err
		}
		if err := checkAccessTx(ctx, tx, t.TaskID, t.AttemptID, sub); err != nil {
			return err
		}
		var rid, rstate, kind, attempt string
		var started *time.Time
		err = tx.QueryRow(ctx, `SELECT r.reservation_id, r.state, r.kind, t.attempt_id, t.exec_started_at
			FROM call_tries t JOIN reservations r USING (reservation_id)
			WHERE t.task_id = $1 AND t.call_id = $2 AND t.try_no = $3 FOR UPDATE OF t`, t.TaskID, t.CallID, t.TryNo).
			Scan(&rid, &rstate, &kind, &attempt, &started)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s 的 try %d", t.TaskID, t.CallID, t.TryNo)
		}
		if err != nil {
			return err
		}
		switch {
		case rid != t.ReservationID || kind != "cpu" || attempt != t.AttemptID:
			return conflictf("try %s/%s#%d 是 %s 预留 %s（attempt %s），不是 exec try %s（attempt %s）", t.TaskID, t.CallID, t.TryNo,
				kind, rid, attempt, t.ReservationID, t.AttemptID)
		case rstate != "held":
			return conflictf("exec try %s/%s#%d 已结算（%s），不能启动", t.TaskID, t.CallID, t.TryNo, rstate)
		case started != nil:
			return nil
		}
		_, err = tx.Exec(ctx, "UPDATE call_tries SET exec_started_at = now() WHERE task_id = $1 AND call_id = $2 AND try_no = $3",
			t.TaskID, t.CallID, t.TryNo)
		return err
	})
}

func validateExecSettlement(st call.ExecSettlement) error {
	if !validExecTry(st.Try) {
		return invalidf("SettleExec 缺少 task_id、call_id、try_no、reservation_id 或 attempt_id")
	}
	if st.CPUUsec < 0 || st.WallMs < 0 || st.QueueMs < 0 || st.ResultSize < 0 {
		return invalidf("SettleExec 的 CPU、wall、排队时间与结果大小不能为负")
	}
	switch st.Outcome {
	case call.ExecCompleted, call.ExecTimedOut:
		if !isSHA256Hex(st.ResultSHA256) {
			return invalidf("%s 结算需要小写十六进制的结果 sha256", st.Outcome)
		}
	case call.ExecCancelled, call.ExecStartFailed, call.ExecUnknown:
		if st.ResultSHA256 != "" || len(st.Outputs) != 0 {
			return invalidf("%s 结算不带结果与输出（只有 completed / timed_out 进入 journal）", st.Outcome)
		}
	default:
		return invalidf("exec 结局必须是 completed、timed_out、cancelled、start_failed 或 unknown，得到 %q", st.Outcome)
	}
	if st.Outcome == call.ExecStartFailed && st.Started {
		return invalidf("start_failed 的 exec 没有启动（Started 必须为假）")
	}
	for _, o := range st.Outputs {
		if !isSHA256Hex(o.SHA256) || o.Size < 0 {
			return invalidf("输出 %q 不是小写十六进制 sha256 与非负大小", o.SHA256)
		}
	}
	return nil
}

// SettleExec 结算一次 exec try（实现 call.ExecStore；§10.3、§10.4、D6）。幂等以 reservation 状态为条件：已不是 held
// 时不再改动账本，返回调用的当前记录（重放与迟到的结算不重复计数）。
func (s *Store) SettleExec(ctx context.Context, st call.ExecSettlement) (call.CallRecord, error) {
	if err := validateExecSettlement(st); err != nil {
		return call.CallRecord{}, err
	}
	t := st.Try
	var out call.CallRecord
	err := s.run(ctx, "SettleExec", fmt.Sprintf("%s/%s#%d", t.TaskID, t.CallID, t.TryNo), func(ctx context.Context, tx pgx.Tx) error {
		if _, err := lockExecQuotaTx(ctx, tx, t.TaskID, nil); err != nil {
			return err
		}
		rec, err := selectCallForUpdate(ctx, tx, t.TaskID, t.CallID)
		if err != nil {
			return err
		}
		var rid, rstate, kind string
		var amount int64
		err = tx.QueryRow(ctx, `SELECT reservation_id, state, kind, amount FROM reservations WHERE task_id = $1 AND call_id = $2 AND try_no = $3
			FOR UPDATE`, t.TaskID, t.CallID, t.TryNo).Scan(&rid, &rstate, &kind, &amount)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s 的 try %d", t.TaskID, t.CallID, t.TryNo)
		}
		if err != nil {
			return err
		}
		if rid != t.ReservationID || kind != "cpu" {
			return conflictf("try %s/%s#%d 是 %s 预留 %s，不是 exec try %s", t.TaskID, t.CallID, t.TryNo, kind, rid, t.ReservationID)
		}
		if rstate != "held" {
			out = rec
			return nil
		}
		var newRes, outcome, callState, failReason string
		var spent, unknown int64
		switch st.Outcome {
		case call.ExecCompleted, call.ExecTimedOut:
			newRes, outcome, callState = "settled", "ok", string(call.StateCompleted)
		case call.ExecCancelled:
			newRes, outcome, callState, failReason = "settled", "retryable", string(call.StateFailed), call.CodeExecCancelled
		case call.ExecStartFailed:
			newRes, outcome, callState, failReason = "settled", "fatal", string(call.StateFailed), call.CodeExecStartFailed
		case call.ExecUnknown:
			newRes, outcome, callState = "charged_unknown", "unknown", string(call.StateUnknown)
		}
		var cpuUsec *int64 // NULL：读取失败，按全额预留计入
		if st.CPUKnown {
			v := st.CPUUsec
			cpuUsec = &v
		}
		switch {
		case newRes == "charged_unknown":
			unknown = amount
		case st.CPUKnown:
			spent = st.CPUUsec
		default:
			spent = amount
		}
		started := int64(0)
		if st.Started {
			started = 1
		}
		if callState == string(call.StateCompleted) {
			if err := recordResultBlob(ctx, tx, t.TaskID, st.ResultSHA256, st.ResultSize, fmt.Sprintf("%s#%d", t.CallID, t.TryNo)); err != nil {
				return err
			}
			for _, o := range st.Outputs {
				if err := recordResultBlob(ctx, tx, t.TaskID, o.SHA256, o.Size, fmt.Sprintf("%s#%d/out", t.CallID, t.TryNo)); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE exec_quotas SET cpu_reserved_usec = cpu_reserved_usec - $2, cpu_spent_usec = cpu_spent_usec + $3,
				cpu_unknown_usec = cpu_unknown_usec + $4, exec_count_used = exec_count_used + $5, wall_spent_ms = wall_spent_ms + $6,
				blocked = blocked OR cpu_spent_usec + $3 > cpu_limit_usec
			WHERE task_id = $1`, t.TaskID, amount, spent, unknown, started, st.WallMs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE reservations SET state = $2 WHERE reservation_id = $1", rid, newRes); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE call_tries SET state = 'settled', outcome = $4, latency_ms = $5, wall_ms = $5, queue_ms = $6,
				cpu_usec = $7, error = $8
			WHERE task_id = $1 AND call_id = $2 AND try_no = $3`, t.TaskID, t.CallID, t.TryNo, outcome, st.WallMs, st.QueueMs, cpuUsec,
			st.Error); err != nil {
			return err
		}
		out, err = scanCall(tx.QueryRow(ctx, `UPDATE calls SET state = $3,
				result_ref = CASE WHEN $3 = 'completed' THEN $4 ELSE result_ref END,
				fail_reason = CASE WHEN $5 <> '' THEN $5 ELSE fail_reason END
			WHERE task_id = $1 AND call_id = $2 RETURNING `+callColumns, t.TaskID, t.CallID, callState, st.ResultSHA256, failReason))
		return err
	})
	return out, err
}

// LoadExecQuota 读取任务的 exec 配额（实现 call.ExecStore）；首次 exec 之前没有行，为 ErrNotFound。
func (s *Store) LoadExecQuota(ctx context.Context, taskID string) (call.ExecQuota, error) {
	var q call.ExecQuota
	err := s.read(ctx, "LoadExecQuota", func(ctx context.Context, qr queryer) error {
		var err error
		q, err = scanExecQuota(qr.QueryRow(ctx, "SELECT "+execQuotaColumns+" FROM exec_quotas WHERE task_id = $1", taskID))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s 的 exec 配额", taskID)
		}
		return err
	})
	return q, err
}
