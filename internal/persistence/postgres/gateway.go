package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/recovery"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/subrun"
)

var _ call.Store = (*Store)(nil)

// Gateway 事务用例（实现 call.Store；规格 §9.2、§9.4–§9.7、§11.2）。
//
// 锁顺序（规格 §7.1 Gateway 子集）：tasks FOR SHARE → task_control FOR SHARE → subruns FOR SHARE（请求带 sub-run 时）
// → attempt_access FOR SHARE → budgets → subrun_budgets → calls → reservations。结算不复查访问（撤销不阻止宿主完成
// 记账，§9.5），从 budgets 开始。
// 账本互斥记账：每笔 reservation 从 held 恰转入 settled（spent）、released 或 charged_unknown（unknown）之一，
// 同时从 reserved 扣除（I3）。两层账本（§9.6）：reservation 带 subrun_id 时 sub-run 层（subrun_budgets）在同一事务中
// 做同样的调整；不带 sub-run 的请求不触及 subrun_budgets，语句路径与结果与无 sub-run 时相同。

// callColumns 与 scanCall 一一对应。
const callColumns = `task_id, call_id, fingerprint, endpoint, state, source, COALESCE(result_ref, ''), tries_used,
	created_at, deadline_at, cost_charged, first_attempt_id, upstream_request_id, COALESCE(supersedes_call_id, ''),
	COALESCE(supersede_reason, ''), possible_external_duplicate, fail_reason, resolving_since, model, COALESCE(subrun_id, '')`

func scanCall(row pgx.Row) (call.CallRecord, error) {
	var r call.CallRecord
	var state string
	err := row.Scan(&r.TaskID, &r.CallID, &r.Fingerprint, &r.Endpoint, &state, &r.Source, &r.ResultRef, &r.TriesUsed,
		&r.CreatedAt, &r.DeadlineAt, &r.CostCharged, &r.FirstAttemptID, &r.UpstreamRequestID, &r.SupersedesCallID,
		&r.SupersedeReason, &r.PossibleExternalDuplicate, &r.FailReason, &r.ResolvingSince, &r.Model, &r.SubrunID)
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

// admitAccess 是规格 §9.2 的访问判定；按 访问撤销 → 非当前 attempt → 已请求取消 → sub-run 不可用 的顺序给出原因。
func admitAccess(f call.AccessFacts) error {
	switch {
	case !f.Active:
		return rejectf(persistence.CodeAccessRevoked, "attempt %s 的访问不是 active", f.AttemptID)
	case !f.Current:
		return rejectf(persistence.CodeNotCurrentAttempt, "attempt %s 不是任务 %s 的当前 attempt", f.AttemptID, f.TaskID)
	case f.Desired == "cancel":
		return rejectf(persistence.CodeCancelRequested, "任务 %s 已请求取消", f.TaskID)
	case f.SubrunID != "" && !f.SubrunOpen:
		return rejectf(persistence.CodeSubrunClosed, "任务 %s 的 sub-run %s 不存在、不是 started 或未绑定 attempt %s",
			f.TaskID, f.SubrunID, f.AttemptID)
	}
	return nil
}

// checkAccessTx 在事务内按锁顺序以共享锁读取访问事实并判定：阻止并发的生命周期更新、控制写入与撤销
// 在本事务提交前改变这些事实（Gateway 请求会等待生命周期更新，§7.1）。subrunID 非空时在 attempt_access 之前
// 以共享锁读取该 sub-run（lockSubrunForCallTx），阻止并发的取消请求与收尾在本事务提交前改变其状态。
func checkAccessTx(ctx context.Context, tx pgx.Tx, taskID, attemptID, subrunID string) error {
	f := call.AccessFacts{TaskID: taskID, AttemptID: attemptID, SubrunID: subrunID}
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
	if subrunID != "" {
		_, err := lockSubrunForCallTx(ctx, tx, taskID, subrunID, attemptID)
		var rej *persistence.RejectedError
		switch {
		case err == nil:
			f.SubrunOpen = true
		case errors.As(err, &rej) && rej.Code == persistence.CodeSubrunClosed:
			// 判定留给 admitAccess：访问撤销、非当前 attempt、已请求取消优先于 subrun_closed。
		default:
			return err
		}
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

// CheckAccess 在一致快照（单条语句）中读取 §9.2 的访问事实（实现 call.Store）；subrunID 非空时另读该 sub-run 是否
// 属于本任务、绑定 attemptID 且为 started（SubrunOpen）。
func (s *Store) CheckAccess(ctx context.Context, taskID, attemptID, subrunID string) (call.AccessFacts, error) {
	f := call.AccessFacts{TaskID: taskID, AttemptID: attemptID, SubrunID: subrunID}
	err := s.read(ctx, "CheckAccess", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT COALESCE(t.current_attempt_id = $2, false), c.desired,
				COALESCE((SELECT a.state = 'active' FROM attempt_access a WHERE a.attempt_id = $2 AND a.task_id = t.task_id), false),
				$3 <> '' AND COALESCE((SELECT r.status = 'started' AND r.bound_attempt_id = $2 FROM subruns r
					WHERE r.task_id = t.task_id AND r.subrun_id = $3), false)
			FROM tasks t JOIN task_control c USING (task_id) WHERE t.task_id = $1`, taskID, attemptID, subrunID).
			Scan(&f.Current, &f.Desired, &f.Active, &f.SubrunOpen)
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
//
// 工具额度（/v1/search、/v1/fetch）：在同一 Tx1 中先锁 budgets（锁顺序 budgets → calls）；需要新插入 calls 行时，
// tool_call_limit 非空且 tool_calls_used ≥ tool_call_limit 为 ErrRejected(tool_budget_exhausted)（不插入调用），否则
// tool_calls_used + 1。同 call_id 的重放与复位后接管不计数；缓存命中与合并在其后的 Tx2 完成，此处已计数。
// 新插入的行在 calls.tool_budget_used 记录登记后的计数（migration 0009）。有上限时结果（含拒绝）带 ToolBudget：
// 新登记后的计数；已有记录（重放、复位后接管、提交结果未知后的重跑）时为该调用首次登记时记录的计数，与之后登记了
// 多少调用无关——Worker 把它写进提示，重放必须得到相同的值（0009 之前登记、没有记录值的调用为当前计数）。
func (s *Store) BeginCall(ctx context.Context, r call.BeginCallRequest) (call.BeginCallResult, error) {
	if r.TaskID == "" || r.CallID == "" || r.AttemptID == "" || r.Fingerprint == "" || r.Endpoint == "" || r.Deadline <= 0 {
		return call.BeginCallResult{}, invalidf("BeginCall 缺少 task_id、call_id、attempt_id、指纹、端点，或期限不为正")
	}
	tool := isToolEndpoint(r.Endpoint)
	var out call.BeginCallResult
	inserted := false // 跨重跑保留：之前的某次尝试已插入（可能已提交）
	err := s.run(ctx, "BeginCall", r.TaskID+"/"+r.CallID, func(ctx context.Context, tx pgx.Tx) error {
		out = call.BeginCallResult{}
		if err := checkAccessTx(ctx, tx, r.TaskID, r.AttemptID, r.SubrunID); err != nil {
			return err // subrun_closed 等在工具计数之前拒绝，不计数
		}
		var limit *int64
		var used int64
		if tool {
			err := tx.QueryRow(ctx, "SELECT tool_call_limit, tool_calls_used FROM budgets WHERE task_id = $1 FOR UPDATE", r.TaskID).
				Scan(&limit, &used)
			if errors.Is(err, pgx.ErrNoRows) {
				return notFoundf("任务 %s 的预算", r.TaskID)
			}
			if err != nil {
				return err
			}
			if limit != nil {
				defer func() { out.ToolBudget = &call.ToolBudget{Used: used, Limit: *limit} }()
				var exists bool
				if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM calls WHERE task_id = $1 AND call_id = $2)",
					r.TaskID, r.CallID).Scan(&exists); err != nil {
					return err
				}
				if !exists && used >= *limit {
					return rejectf(persistence.CodeToolBudgetExhausted, "任务 %s 的工具调用额度已用完（%d/%d）", r.TaskID, used, *limit)
				}
			}
		}
		rec, err := scanCall(tx.QueryRow(ctx, `INSERT INTO calls (task_id, call_id, fingerprint, endpoint, state, source,
				created_at, deadline_at, first_attempt_id, supersedes_call_id, supersede_reason, resolving_since, model, subrun_id,
				tool_budget_used)
			VALUES ($1, $2, $3, $4, 'resolving', 'upstream', now(), now() + $5::bigint * interval '1 microsecond', $6, NULLIF($7, ''), NULLIF($8, ''), now(), $9,
				NULLIF($10, ''), $11)
			ON CONFLICT (task_id, call_id) DO NOTHING RETURNING `+callColumns,
			r.TaskID, r.CallID, r.Fingerprint, r.Endpoint, r.Deadline.Microseconds(), r.AttemptID, r.SupersedesCallID, r.SupersedeReason,
			r.Model, r.SubrunID, toolBudgetUsed(tool, used)))
		if err == nil {
			if tool {
				if err := tx.QueryRow(ctx, "UPDATE budgets SET tool_calls_used = tool_calls_used + 1 WHERE task_id = $1 RETURNING tool_calls_used",
					r.TaskID).Scan(&used); err != nil {
					return err
				}
			}
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
		if err := sameSubrun(rec, r.SubrunID); err != nil {
			return err
		}
		if tool {
			var first *int64
			if err := tx.QueryRow(ctx, "SELECT tool_budget_used FROM calls WHERE task_id = $1 AND call_id = $2", r.TaskID, r.CallID).
				Scan(&first); err != nil {
				return err
			}
			if first != nil {
				used = *first
			}
		}
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

// sameSubrun 要求请求的 sub-run 与调用登记的归属相同：同一 call_id 换了 sub-run（或 root）是另一个调用 →
// fingerprint_mismatch（§9.4）。
func sameSubrun(rec call.CallRecord, subrunID string) error {
	if rec.SubrunID != subrunID {
		return rejectf(persistence.CodeFingerprintMismatch, "调用 %s/%s 登记于 sub-run %q，请求为 %q", rec.TaskID, rec.CallID,
			rec.SubrunID, subrunID)
	}
	return nil
}

// toolBudgetUsed 是新插入的调用行的 tool_budget_used：工具端点为本次登记后的计数（budgets 行已锁，used + 1 即随后
// UPDATE 的结果），模型调用为 NULL。
func toolBudgetUsed(tool bool, used int64) *int64 {
	if !tool {
		return nil
	}
	n := used + 1
	return &n
}

// isToolEndpoint 报告端点是否计入每 turn 的工具调用额度（搜索、抓取与 MCP 工具调用；模型调用与 exec 不计）。
func isToolEndpoint(endpoint string) bool {
	return endpoint == "/v1/search" || endpoint == "/v1/fetch" || endpoint == "/v1/mcp"
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
	identity := r.TaskID + "/" + r.CallID + "@" + rid
	body := func(ctx context.Context, tx pgx.Tx) error {
		out = call.Try{}
		if prev, ok, err := heldTry(ctx, tx, r, rid); err != nil || ok {
			out = prev
			return err
		}
		if err := checkAccessTx(ctx, tx, r.TaskID, r.AttemptID, r.SubrunID); err != nil {
			return err
		}
		budget, err := lockBudget(ctx, tx, r.TaskID)
		if err != nil {
			return err
		}
		var sub subrun.Budget // 无 sub-run 时不读取，Available 为无上限
		if r.SubrunID != "" {
			if sub, err = lockSubrunBudgetTx(ctx, tx, r.TaskID, r.SubrunID); err != nil {
				return err
			}
		}
		var state, callSubrun string
		var tries int
		var expired bool
		err = tx.QueryRow(ctx, `SELECT state, tries_used, now() >= deadline_at, COALESCE(subrun_id, '') FROM calls
			WHERE task_id = $1 AND call_id = $2 FOR UPDATE`, r.TaskID, r.CallID).Scan(&state, &tries, &expired, &callSubrun)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("调用 %s/%s", r.TaskID, r.CallID)
		}
		if err != nil {
			return err
		}
		if err := sameSubrun(call.CallRecord{TaskID: r.TaskID, CallID: r.CallID, SubrunID: callSubrun}, r.SubrunID); err != nil {
			return err
		}
		if tries > 0 {
			var prev call.Try
			var rstate, env, provider string
			var amount int64
			if err := tx.QueryRow(ctx, `SELECT r.reservation_id, r.state, r.amount, t.attempt_id, COALESCE(t.env_id, ''), t.provider
				FROM call_tries t JOIN reservations r USING (reservation_id)
				WHERE t.task_id = $1 AND t.call_id = $2 AND t.try_no = $3 FOR UPDATE OF r`, r.TaskID, r.CallID, tries).
				Scan(&prev.ReservationID, &rstate, &amount, &prev.AttemptID, &env, &provider); err != nil {
				return err
			}
			if rstate == "held" {
				if prev.ReservationID == rid || (prev.AttemptID == r.AttemptID && env == r.EnvID && amount == r.EstimateMicro &&
					provider == r.Provider) {
					prev.TaskID, prev.CallID, prev.TryNo = r.TaskID, r.CallID, tries
					out = prev
					return nil
				}
				ok, err := hedgeAllowed(ctx, tx, r, provider)
				if err != nil {
					return err
				}
				if !ok {
					return rejectf(persistence.CodeCallInProgress, "调用 %s 的 try %d 仍在执行", r.CallID, tries)
				}
				// 对冲：另建一个并发的 try，其余检查（期限、次数、预算）照常。
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
		case sub.Available() <= 0:
			return rejectf(persistence.CodeSubrunBudgetExhausted, "任务 %s 的 sub-run %s 层可用 %d", r.TaskID, r.SubrunID, sub.Available())
		case sub.Available() < r.EstimateMicro:
			return rejectf(persistence.CodeBudgetInsufficient, "任务 %s 的 sub-run %s 层可用 %d，本次估算 %d", r.TaskID, r.SubrunID,
				sub.Available(), r.EstimateMicro)
		}
		tryNo := tries + 1
		if _, err := tx.Exec(ctx, `INSERT INTO reservations (reservation_id, task_id, call_id, try_no, kind, amount, state, subrun_id)
			VALUES ($1, $2, $3, $4, 'money', $5, 'held', NULLIF($6, ''))`, rid, r.TaskID, r.CallID, tryNo, r.EstimateMicro, r.SubrunID); err != nil {
			return err
		}
		if r.SubrunID != "" {
			if err := adjustSubrunBudgetTx(ctx, tx, r.TaskID, r.SubrunID, r.EstimateMicro, 0, 0); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO call_tries (task_id, call_id, try_no, attempt_id, env_id, state, reservation_id, provider, skipped, hedge)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), 'in_flight', $6, $7, $8, $9)`, r.TaskID, r.CallID, tryNo, r.AttemptID, r.EnvID, rid,
			r.Provider, r.Skipped, r.Hedge); err != nil {
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
	}
	err = s.run(ctx, "ReserveTry", identity, body)
	if err == nil && faultinject.Lose(faultinject.ReservationCommit) {
		// E11b 故障注入：COMMIT 已执行而回复丢失——与 run 遇到提交结果未知时相同，以同一 reservation_id 重跑事务体。
		err = s.run(ctx, "ReserveTry", identity, body)
	}
	return out, err
}

// heldTry 不加锁地查找本次请求已创建且仍持有预留的 try（ReserveTry 的幂等身份）：最近一次 try 的
// reservation 为 held，且是本次生成的 reservation_id，或同 attempt、同环境、同估算（且调用的 sub-run 与请求相同）。
// 读到的是已提交的事实。
func heldTry(ctx context.Context, tx pgx.Tx, r call.ReserveTryRequest, rid string) (call.Try, bool, error) {
	prev := call.Try{TaskID: r.TaskID, CallID: r.CallID}
	var env, provider string
	var amount int64
	err := tx.QueryRow(ctx, `SELECT t.try_no, r.reservation_id, r.amount, t.attempt_id, COALESCE(t.env_id, ''), t.provider
		FROM calls c JOIN call_tries t ON t.task_id = c.task_id AND t.call_id = c.call_id AND t.try_no = c.tries_used
		JOIN reservations r ON r.reservation_id = t.reservation_id
		WHERE c.task_id = $1 AND c.call_id = $2 AND r.state = 'held' AND COALESCE(c.subrun_id, '') = $3`, r.TaskID, r.CallID, r.SubrunID).
		Scan(&prev.TryNo, &prev.ReservationID, &amount, &prev.AttemptID, &env, &provider)
	if errors.Is(err, pgx.ErrNoRows) {
		return call.Try{}, false, nil
	}
	if err != nil {
		return call.Try{}, false, err
	}
	if prev.ReservationID == rid || (prev.AttemptID == r.AttemptID && env == r.EnvID && amount == r.EstimateMicro && provider == r.Provider) {
		return prev, true, nil
	}
	return call.Try{}, false, nil
}

// hedgeAllowed 报告最近的 try 仍持有预留时能否另建对冲 try：须为对冲请求、Provider 与持有预留的 try 不同，
// 且该调用恰有一个持有预留的 try（对冲最多两条腿）。reservations 行已由调用方按锁顺序锁住。
func hedgeAllowed(ctx context.Context, tx pgx.Tx, r call.ReserveTryRequest, heldProvider string) (bool, error) {
	if !r.Hedge || r.Provider == heldProvider {
		return false, nil
	}
	var held int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM reservations WHERE task_id = $1 AND call_id = $2 AND state = 'held'`,
		r.TaskID, r.CallID).Scan(&held); err != nil {
		return false, err
	}
	return held == 1, nil
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
// ref 是来源记录（上游结果为 <call_id>#<try_no>，缓存命中为 <call_id>#cache，合并的 follower 为 <call_id>#coalesced）。
func recordResultBlob(ctx context.Context, tx pgx.Tx, taskID, sha string, wantSize int64, ref string) error {
	if _, err := tx.Exec(ctx, "INSERT INTO blobs (sha256, size) VALUES ($1, $2) ON CONFLICT DO NOTHING", sha, wantSize); err != nil {
		return err
	}
	var size int64
	if err := tx.QueryRow(ctx, "SELECT size FROM blobs WHERE sha256 = $1", sha).Scan(&size); err != nil {
		return err
	}
	if size != wantSize {
		return conflictf("blob %s 已登记为 %d 字节，不是 %d", sha, size, wantSize)
	}
	if err := authorizeTaskBlob(ctx, tx, taskID, sha); err != nil { // 任务属于会话时同时授权到会话 scope
		return err
	}
	_, err := tx.Exec(ctx, "INSERT INTO blob_provenance (scope_kind, scope_id, sha256, source, ref) VALUES ('task', $1, $2, 'gateway', $3)",
		taskID, sha, ref)
	return err
}

// CompleteFromCache 是缓存命中与 singleflight follower 的 Tx2（实现 call.Store；规格 §11.2、§11.4）：按锁顺序复查
// 访问（含 desired ≠ cancel）→ 锁住调用行 → 复查期限 → 结果 blob 写入 scope_blobs(task) → calls.completed
// （source = cache 或 coalesced）。不建 reservation 与 try、不改动账本。取消先提交时本事务读到 desired = cancel
// 而拒绝，结果不被授权（E23）；本事务先提交时，随后的取消与之串行（task_control 上的共享锁）。调用须为仍在
// 解析中的 resolving 且没有 try，否则为 ErrConflict；已以同一来源、同一结果完成时原样返回（提交结果未知后的重跑）。
// 带 sub-run 时访问检查另要求该 sub-run 可用（subrun_closed），且须与调用登记的归属相同（calls.subrun_id 在 Tx1
// 已写入；不同为 fingerprint_mismatch）；两层账本都不改动。
func (s *Store) CompleteFromCache(ctx context.Context, r call.CacheCompletion) (call.CallRecord, error) {
	if r.TaskID == "" || r.CallID == "" || r.AttemptID == "" || !isSHA256Hex(r.ResultSHA256) || r.ResultSize < 0 {
		return call.CallRecord{}, invalidf("CompleteFromCache 缺少 task_id、call_id、attempt_id，或结果不是小写十六进制 sha256 与非负大小")
	}
	src := r.Source
	if src == "" {
		src = call.SourceCache
	}
	if src != call.SourceCache && src != call.SourceCoalesced {
		return call.CallRecord{}, invalidf("CompleteFromCache 的 source %q 不是 cache 或 coalesced", r.Source)
	}
	var out call.CallRecord
	err := s.run(ctx, "CompleteFromCache", r.TaskID+"/"+r.CallID, func(ctx context.Context, tx pgx.Tx) error {
		if err := checkAccessTx(ctx, tx, r.TaskID, r.AttemptID, r.SubrunID); err != nil {
			return err
		}
		rec, err := selectCallForUpdate(ctx, tx, r.TaskID, r.CallID)
		if err != nil {
			return err
		}
		if err := sameSubrun(rec, r.SubrunID); err != nil {
			return err
		}
		if rec.State == call.StateCompleted && rec.Source == src && rec.ResultRef == r.ResultSHA256 {
			out = rec
			return nil
		}
		if rec.State != call.StateResolving || rec.ResolvingSince == nil || rec.TriesUsed != 0 {
			return conflictf("调用 %s/%s 处于 %s（tries_used %d），不能由缓存完成", r.TaskID, r.CallID, rec.State, rec.TriesUsed)
		}
		var expired bool
		if err := tx.QueryRow(ctx, "SELECT now() >= $1::timestamptz", rec.DeadlineAt).Scan(&expired); err != nil {
			return err
		}
		if expired {
			return rejectf(persistence.CodeCallDeadlineExceeded, "调用 %s 已超过 deadline_at", r.CallID)
		}
		if err := recordResultBlob(ctx, tx, r.TaskID, r.ResultSHA256, r.ResultSize, r.CallID+"#"+src); err != nil {
			return err
		}
		out, err = scanCall(tx.QueryRow(ctx, `UPDATE calls SET state = 'completed', source = $4, result_ref = $3, resolving_since = NULL
			WHERE task_id = $1 AND call_id = $2 RETURNING `+callColumns, r.TaskID, r.CallID, r.ResultSHA256, src))
		return err
	})
	return out, err
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
		// reservation 的归属在插入后不变：先不加锁读出，以便按锁顺序 budgets → subrun_budgets → calls → reservations 加锁。
		var sub string
		err := tx.QueryRow(ctx, `SELECT COALESCE(subrun_id, '') FROM reservations WHERE task_id = $1 AND call_id = $2 AND try_no = $3`,
			t.TaskID, t.CallID, t.TryNo).Scan(&sub)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err // 不存在时由下面加锁的读取报告 ErrNotFound
		}
		if _, err := lockBudget(ctx, tx, t.TaskID); err != nil {
			return err
		}
		if sub != "" {
			if _, err := lockSubrunBudgetTx(ctx, tx, t.TaskID, sub); err != nil {
				return err
			}
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
		ref := st.ResultSHA256 // 成为 result_ref 的结果（Sibling 结算不改变调用的结局）
		switch st.Outcome {
		case "ok":
			if err := recordResultBlob(ctx, tx, t.TaskID, st.ResultSHA256, st.ResultSize, fmt.Sprintf("%s#%d", t.CallID, t.TryNo)); err != nil {
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
		if sub != "" { // sub-run 层同步（§9.6；spent 可超过上限，即赤字）
			if err := adjustSubrunBudgetTx(ctx, tx, t.TaskID, sub, -amount, spent, unknown); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "UPDATE reservations SET state = $2 WHERE reservation_id = $1", rid, newRes); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE call_tries SET state = 'settled', outcome = $4, latency_ms = $5, cost_micro = $6, error = $7,
				hedge_lost = $8
			WHERE task_id = $1 AND call_id = $2 AND try_no = $3`, t.TaskID, t.CallID, t.TryNo, st.Outcome, st.LatencyMs, cost, st.Error,
			st.HedgeLost); err != nil {
			return err
		}
		failReason, reqID := "", st.UpstreamRequestID
		if st.Outcome == "fatal" {
			failReason = st.Error
			if failReason == "" {
				failReason = "fatal"
			}
		}
		if st.Sibling { // 只记账：调用的状态与结局由最终的一条腿决定
			callState, ref, failReason, reqID = string(rec.State), "", "", ""
		}
		out, err = scanCall(tx.QueryRow(ctx, `UPDATE calls SET state = $3,
				result_ref = CASE WHEN $3 = 'completed' AND $4 <> '' THEN $4 ELSE result_ref END,
				cost_charged = cost_charged + $5,
				upstream_request_id = CASE WHEN $6 <> '' THEN $6 ELSE upstream_request_id END,
				possible_external_duplicate = possible_external_duplicate OR $7,
				fail_reason = CASE WHEN $8 <> '' THEN $8 ELSE fail_reason END
			WHERE task_id = $1 AND call_id = $2 RETURNING `+callColumns,
			t.TaskID, t.CallID, callState, ref, spent, reqID, st.Outcome == "unknown", failReason))
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

// LedgerRestartError 是启动账本转换按 unknown 结算的 try 的 call_tries.error。
const LedgerRestartError = "server_restart"

// ConvertLedger 是启动账本转换（实现 recovery.Store；规格 §14.1 第 4 步），在单个事务中完成：
//   - 全部 held 的 reservation → charged_unknown，按 kind 入账（D10）：money → 所属任务账本 reserved -= amount、
//     unknown += amount（I3 money 组保持），带 subrun_id 的另对 sub-run 层账本做同样调整（两层 I3）；cpu →
//     exec_quotas.cpu_reserved -= amount、cpu_unknown += amount（I3 cpu 组保持），该 try 已写 exec_started_at 时
//     exec_count_used += 1（崩溃时处于"启动中"的 exec 保守计入）；
//   - 对应的 in_flight try → settled / unknown（error = server_restart）；
//   - 全部 in_flight 调用 → unknown（deadline_at 不变）；持有 money 预留的调用标记 possible_external_duplicate
//     （上一进程的上游请求可能已经发出；exec 没有外部副作用，其重跑由"旧环境已停止"把关）。
//
// 重启前的进程已不存在，这些 try 不会再结算；转换后调用按 unknown 处理：期限内可在累计上限内新建 try，超期为
// call_deadline_exceeded。幂等：没有 held 与 in_flight 时不改动任何行。锁顺序 budgets → subrun_budgets →
// exec_quotas → calls → reservations。UnknownMicro 只计 money。
func (s *Store) ConvertLedger(ctx context.Context) (recovery.LedgerConversion, error) {
	var out recovery.LedgerConversion
	err := s.run(ctx, "ConvertLedger", "ledger", func(ctx context.Context, tx pgx.Tx) error {
		out = recovery.LedgerConversion{}
		if _, err := tx.Exec(ctx, `UPDATE budgets b SET reserved_micro = b.reserved_micro - h.total, unknown_micro = b.unknown_micro + h.total
			FROM (SELECT task_id, sum(amount) AS total FROM reservations WHERE state = 'held' AND kind = 'money' GROUP BY task_id) h
			WHERE b.task_id = h.task_id`); err != nil {
			return err
		}
		// sub-run 层同步（§9.6）：按 reservation 的归属分组，与 task 层同一事务（锁顺序 budgets → subrun_budgets）。
		if _, err := tx.Exec(ctx, `UPDATE subrun_budgets b SET reserved_micro = b.reserved_micro - h.total, unknown_micro = b.unknown_micro + h.total
			FROM (SELECT task_id, subrun_id, sum(amount) AS total FROM reservations WHERE state = 'held' AND kind = 'money' AND subrun_id IS NOT NULL
				GROUP BY task_id, subrun_id) h
			WHERE b.task_id = h.task_id AND b.subrun_id = h.subrun_id`); err != nil {
			return err
		}
		// exec 配额（D10）：cpu 预留转 unknown；已标记启动的在途 exec 保守计入次数。
		if _, err := tx.Exec(ctx, `UPDATE exec_quotas q SET cpu_reserved_usec = q.cpu_reserved_usec - h.total,
				cpu_unknown_usec = q.cpu_unknown_usec + h.total, exec_count_used = q.exec_count_used + h.started
			FROM (SELECT r.task_id, sum(r.amount) AS total, count(*) FILTER (WHERE t.exec_started_at IS NOT NULL) AS started
				FROM reservations r JOIN call_tries t ON t.reservation_id = r.reservation_id
				WHERE r.state = 'held' AND r.kind = 'cpu' GROUP BY r.task_id) h
			WHERE q.task_id = h.task_id`); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE calls c SET state = 'unknown',
				possible_external_duplicate = c.possible_external_duplicate OR EXISTS (SELECT 1 FROM reservations r
					WHERE r.task_id = c.task_id AND r.call_id = c.call_id AND r.state = 'held' AND r.kind = 'money')
			WHERE c.state = 'in_flight'`)
		if err != nil {
			return err
		}
		out.Calls = int(tag.RowsAffected())
		if _, err := tx.Exec(ctx, `UPDATE call_tries t SET state = 'settled', outcome = 'unknown', error = $1
			FROM reservations r WHERE r.reservation_id = t.reservation_id AND r.state = 'held' AND t.state = 'in_flight'`,
			LedgerRestartError); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `WITH x AS (UPDATE reservations SET state = 'charged_unknown' WHERE state = 'held' RETURNING kind, amount)
			SELECT count(*), COALESCE(sum(amount) FILTER (WHERE kind = 'money'), 0)::bigint FROM x`).Scan(&out.Reservations, &out.UnknownMicro)
	})
	return out, err
}

// LoadBudget 读取任务的账本（实现 call.Store）。
func (s *Store) LoadBudget(ctx context.Context, taskID string) (call.Budget, error) {
	var b call.Budget
	err := s.read(ctx, "LoadBudget", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT limit_micro, reserved_micro, spent_micro, unknown_micro, tool_call_limit, tool_calls_used
			FROM budgets WHERE task_id = $1`, taskID).
			Scan(&b.LimitMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro, &b.ToolCallLimit, &b.ToolCallsUsed)
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
				t.reservation_id, t.error, t.provider, t.skipped, t.hedge, t.hedge_lost
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
			var attempt, env, tstate, outcome, rid, terr, provider, skipped *string
			var latency, cost *int64
			var hedge, hedgeLost *bool
			if err := rows.Scan(&r.TaskID, &r.CallID, &r.Fingerprint, &r.Endpoint, &state, &r.Source, &r.ResultRef, &r.TriesUsed,
				&r.CreatedAt, &r.DeadlineAt, &r.CostCharged, &r.FirstAttemptID, &r.UpstreamRequestID, &r.SupersedesCallID,
				&r.SupersedeReason, &r.PossibleExternalDuplicate, &r.FailReason, &r.ResolvingSince, &r.Model, &r.SubrunID,
				&tryNo, &attempt, &env, &tstate, &outcome, &latency, &cost, &rid, &terr, &provider, &skipped, &hedge, &hedgeLost); err != nil {
				return err
			}
			r.State = call.CallState(state)
			rec = r
			if tryNo == nil {
				continue
			}
			tr = call.TryRecord{TryNo: *tryNo, AttemptID: *attempt, EnvID: *env, State: *tstate, Outcome: *outcome,
				LatencyMs: *latency, CostMicro: *cost, ReservationID: *rid, Error: *terr, Provider: *provider, Skipped: *skipped,
				Hedge: *hedge, HedgeLost: *hedgeLost}
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

// BlobAuthorized 报告 sha 是否在任务 scope 内（实现 call.Store；scope_blobs(task)，§9.3"按 scope 授权"）；会话 turn
// 另接受其会话 scope（§5.5 第 2 条 attempt → task → session 推导，例如 D5 carryover 的 checkpoint blob）。
func (s *Store) BlobAuthorized(ctx context.Context, taskID, sha string) (bool, error) {
	var ok bool
	err := s.read(ctx, "BlobAuthorized", func(ctx context.Context, q queryer) error {
		return q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM scope_blobs WHERE sha256 = $2 AND
			((scope_kind = 'task' AND scope_id = $1)
				OR (scope_kind = 'session' AND scope_id = (SELECT session_id FROM tasks WHERE task_id = $1))))`,
			taskID, sha).Scan(&ok)
	})
	return ok, err
}
