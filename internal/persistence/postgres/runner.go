package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/subrun"
)

var _ runner.Store = (*Store)(nil)

// AppendWorkerEvents 原子追加一批 Worker 事件（实现 runner.Store；设计 §3.1）。
func (s *Store) AppendWorkerEvents(ctx context.Context, attemptID string, events []runner.WorkerEvent) (runner.Watermark, error) {
	if err := validateWorkerBatch(events); err != nil {
		return runner.Watermark{}, err
	}
	out := runner.Watermark{AttemptID: attemptID}
	identity := fmt.Sprintf("%s:%d-%d", attemptID, events[0].Seq, events[len(events)-1].Seq)
	err := s.run(ctx, "AppendWorkerEvents", identity, func(ctx context.Context, tx pgx.Tx) error {
		var taskID string
		err := tx.QueryRow(ctx, "SELECT task_id FROM attempts WHERE attempt_id = $1", attemptID).Scan(&taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", attemptID)
		}
		if err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, taskID); err != nil {
			return err
		}
		out.WorkerSeq, err = appendWorkerBatch(ctx, tx, taskID, attemptID, events)
		return err
	})
	return out, err
}

// AppendSessionWorkerEvents 原子追加一批 session 模式的 Worker 事件（实现 runner.Store）：worker_seq 是 incarnation
// 序号，批内与同一 attempt 内严格递增、不要求连续。已存在的序号逐条校验内容；新序号须大于该 attempt 已提交的最大
// 序号（不回填空隙）。
func (s *Store) AppendSessionWorkerEvents(ctx context.Context, attemptID string, events []runner.WorkerEvent) (runner.Watermark, error) {
	if len(events) == 0 {
		return runner.Watermark{}, invalidf("Worker 事件批次为空")
	}
	for i, e := range events {
		if e.Seq < 1 || (i > 0 && e.Seq <= events[i-1].Seq) {
			return runner.Watermark{}, invalidf("session Worker 事件序号必须为正且严格递增，第 %d 条为 %d", i, e.Seq)
		}
		if e.Type == "" || len(e.Payload) == 0 {
			return runner.Watermark{}, invalidf("Worker 事件 %d 缺少类型或载荷", e.Seq)
		}
	}
	out := runner.Watermark{AttemptID: attemptID}
	identity := fmt.Sprintf("%s:%d-%d", attemptID, events[0].Seq, events[len(events)-1].Seq)
	err := s.run(ctx, "AppendSessionWorkerEvents", identity, func(ctx context.Context, tx pgx.Tx) error {
		var taskID string
		err := tx.QueryRow(ctx, "SELECT task_id FROM attempts WHERE attempt_id = $1", attemptID).Scan(&taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", attemptID)
		}
		if err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, taskID); err != nil {
			return err
		}
		var watermark int64
		if err := tx.QueryRow(ctx, "SELECT COALESCE(max(worker_seq), 0) FROM events WHERE attempt_id = $1 AND worker_seq IS NOT NULL",
			attemptID).Scan(&watermark); err != nil {
			return err
		}
		existing, err := existingWorkerHashes(ctx, tx, attemptID, events[0].Seq, min(events[len(events)-1].Seq, watermark))
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.Seq > watermark {
				if err := insertWorkerEvent(ctx, tx, taskID, attemptID, e); err != nil {
					return err
				}
				continue
			}
			h, ok := existing[e.Seq]
			if !ok {
				return conflictf("attempt %s 的 Worker 事件 %d 不大于已提交的 %d 且不存在", attemptID, e.Seq, watermark)
			}
			if !bytes.Equal(h, workerEventHash(e)) {
				return conflictf("attempt %s 的 Worker 事件 %d 已存在且内容不同", attemptID, e.Seq)
			}
		}
		out.WorkerSeq = max(watermark, events[len(events)-1].Seq)
		return nil
	})
	return out, err
}

// WorkerEventWatermark 返回已提交的最大 worker_seq（实现 runner.Store）。
func (s *Store) WorkerEventWatermark(ctx context.Context, attemptID string) (runner.Watermark, error) {
	out := runner.Watermark{AttemptID: attemptID}
	err := s.read(ctx, "WorkerEventWatermark", func(ctx context.Context, q queryer) error {
		return q.QueryRow(ctx, "SELECT COALESCE(max(worker_seq), 0) FROM events WHERE attempt_id = $1 AND worker_seq IS NOT NULL",
			attemptID).Scan(&out.WorkerSeq)
	})
	return out, err
}

// 带 subruns[] 时其 JSON 作为第 6 段参与哈希；没有时公式与 M4 之前相同（已有 checkpoint 的重放仍是同一内容）。
func checkpointHash(c runner.Checkpoint) []byte {
	parts := [][]byte{[]byte(c.AttemptID), []byte(c.StepID), c.State, []byte(c.StateRef), []byte(strings.Join(c.Refs, "\n"))}
	if len(c.Subruns) > 0 {
		parts = append(parts, subrunsJSON(c.Subruns))
	}
	return contentHash(parts...)
}

// subrunsJSON 是 checkpoints.subruns_json 的内容（没有时为 []）。
func subrunsJSON(s []protocol.CheckpointSubrun) []byte {
	if s == nil {
		s = []protocol.CheckpointSubrun{}
	}
	b, err := json.Marshal(s)
	if err != nil { // 只含字符串
		panic(fmt.Sprintf("postgres: 编码 subruns: %v", err))
	}
	return b
}

// CommitCheckpoint 以 (scope, checkpoint_id) 为身份提交 checkpoint（实现 runner.Store）。
// 已存在的 ID 只比较内容并返回原结果（规格 §5.5 第 3 条）；新 checkpoint 在同一事务内检查
// fencing（提交者是当前 attempt 且访问有效）与引用授权（refs、state_ref 已保存并授权到当前 scope），
// 不通过时指针与事件都不改变。commit_seq 来自 task_progress 的行锁（规格 §7.2）。
// subruns[] 在同一事务中按状态机转换（applyCheckpointSubrunsTx），不允许时整体回滚为 invalid_transition。
// 锁顺序：tasks → task_progress → task_event_seq → subruns → attempt_access。
func (s *Store) CommitCheckpoint(ctx context.Context, c runner.Checkpoint) (runner.CommittedCheckpoint, error) {
	if c.Scope.Kind != "task" || c.Scope.ID == "" {
		return runner.CommittedCheckpoint{}, invalidf("M1 只支持 task 范围的 checkpoint，得到 %+v", c.Scope)
	}
	if c.CheckpointID == "" || c.AttemptID == "" || c.StepID == "" || (len(c.State) == 0) == (c.StateRef == "") {
		return runner.CommittedCheckpoint{}, invalidf("checkpoint 缺少字段，或 state 与 state_ref 未恰好提供一个")
	}
	hash := checkpointHash(c)
	out := runner.CommittedCheckpoint{Scope: c.Scope, CheckpointID: c.CheckpointID, ContentHash: hash}
	err := s.run(ctx, "CommitCheckpoint", c.CheckpointID, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockTask(ctx, tx, c.Scope.ID); err != nil {
			return err
		}
		var latest int64
		if err := tx.QueryRow(ctx, "SELECT latest_commit_seq FROM task_progress WHERE task_id = $1 FOR UPDATE", c.Scope.ID).Scan(&latest); err != nil {
			return err
		}
		existing, err := selectCheckpoint(ctx, tx, c.Scope, c.CheckpointID)
		switch {
		case err == nil && bytes.Equal(existing.ContentHash, hash):
			out = existing
			return nil
		case err == nil:
			return conflictf("checkpoint %s 已存在且内容不同", c.CheckpointID)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		if err := fenceAttempt(ctx, tx, c.Scope.ID, c.AttemptID); err != nil {
			return err
		}
		if err := authorizeRefs(ctx, tx, c); err != nil {
			return err
		}
		refs, _ := json.Marshal(nonNil(c.Refs))
		if _, err := tx.Exec(ctx, `INSERT INTO checkpoints (scope_kind, scope_id, checkpoint_id, commit_seq, attempt_id, step_id,
				content_hash, state_inline, state_ref, refs_json, subruns_json)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10, $11)`,
			c.Scope.Kind, c.Scope.ID, c.CheckpointID, latest+1, c.AttemptID, c.StepID, hash, nullJSON(c.State), c.StateRef, refs,
			subrunsJSON(c.Subruns)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE task_progress SET latest_checkpoint_id = $2, latest_commit_seq = $3 WHERE task_id = $1",
			c.Scope.ID, c.CheckpointID, latest+1); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, c.Scope.ID); err != nil {
			return err
		}
		// subruns[] 与 checkpoint 同一事务（锁顺序 task_event_seq → subruns）：非法转换回滚整个 checkpoint（E41）
		if err := applyCheckpointSubrunsTx(ctx, tx, c.Scope.ID, c.Subruns); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"checkpoint_id": c.CheckpointID, "commit_seq": latest + 1, "step_id": c.StepID})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: c.Scope.ID, key: "checkpoint_committed:" + c.CheckpointID,
			attemptID: c.AttemptID, typ: "checkpoint_committed", payload: payload}); err != nil {
			return err
		}
		out.CommitSeq = latest + 1
		return nil
	})
	return out, err
}

// fenceAttempt 要求 attemptID 是任务的当前 attempt、其访问仍为 active，且尚无判决：判决提交后
// 迟到的写入不得改写终态（规格 §5.8）。调用方已持有任务行锁（FinalizeAttempt 需要 FOR UPDATE），
// 因此检查到提交之间 current_attempt_id 与判决都不会改变。
func fenceAttempt(ctx context.Context, tx pgx.Tx, taskID, attemptID string) error {
	var current *string
	var access string
	var decided bool
	err := tx.QueryRow(ctx, `SELECT t.current_attempt_id, COALESCE(aa.state, ''), COALESCE(a.verdict_hash IS NOT NULL, false)
		FROM tasks t
		LEFT JOIN attempt_access aa ON aa.task_id = t.task_id AND aa.attempt_id = $2
		LEFT JOIN attempts a ON a.task_id = t.task_id AND a.attempt_id = $2
		WHERE t.task_id = $1`, taskID, attemptID).Scan(&current, &access, &decided)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	if err != nil {
		return err
	}
	if current == nil || *current != attemptID || access != "active" || decided {
		return rejectf(persistence.CodeStaleAttempt, "attempt %s 不是任务 %s 的当前 attempt、访问已撤销或已有判决", attemptID, taskID)
	}
	return nil
}

// authorizeRefs 要求 checkpoint 引用的每个 sha256 都已保存并授权到当前 scope（规格 §5.5 第 2 条）：
// scope 由宿主按 attempt → task → session 推导（会话 turn 另接受其会话 scope，例如 D5 carryover 与恢复种子的引用），
// 不接受其他任务或会话的 blob。
func authorizeRefs(ctx context.Context, tx pgx.Tx, c runner.Checkpoint) error {
	refs := append([]string(nil), c.Refs...)
	if c.StateRef != "" {
		refs = append(refs, c.StateRef)
	}
	for _, e := range c.Subruns { // subruns[].result_ref 与 refs 同一授权规则（规格 §5.5 第 2 条）
		if e.ResultRef != "" {
			refs = append(refs, e.ResultRef)
		}
	}
	if len(refs) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT r FROM unnest($1::text[]) AS r WHERE NOT EXISTS (
		SELECT 1 FROM scope_blobs sb WHERE sb.sha256 = r AND
			((sb.scope_kind = 'attempt' AND sb.scope_id = $2) OR (sb.scope_kind = 'task' AND sb.scope_id = $3)
				OR (sb.scope_kind = 'session' AND sb.scope_id = (SELECT session_id FROM tasks WHERE task_id = $3))))`,
		refs, c.AttemptID, c.Scope.ID)
	if err != nil {
		return err
	}
	missing, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return rejectf(persistence.CodeRefNotAuthorized, "引用 %v 不存在或未授权到任务 %s", missing, c.Scope.ID)
	}
	return nil
}

// QueryCheckpoint 读取已提交的 checkpoint（实现 runner.Store）。
func (s *Store) QueryCheckpoint(ctx context.Context, scope runner.Scope, checkpointID string) (runner.CommittedCheckpoint, error) {
	var out runner.CommittedCheckpoint
	err := s.read(ctx, "QueryCheckpoint", func(ctx context.Context, q queryer) error {
		c, err := selectCheckpoint(ctx, q, scope, checkpointID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("checkpoint %s", checkpointID)
		}
		out = c
		return err
	})
	return out, err
}

func selectCheckpoint(ctx context.Context, q queryer, scope runner.Scope, checkpointID string) (runner.CommittedCheckpoint, error) {
	c := runner.CommittedCheckpoint{Scope: scope, CheckpointID: checkpointID}
	err := q.QueryRow(ctx, "SELECT commit_seq, content_hash FROM checkpoints WHERE scope_kind = $1 AND scope_id = $2 AND checkpoint_id = $3",
		scope.Kind, scope.ID, checkpointID).Scan(&c.CommitSeq, &c.ContentHash)
	return c, err
}

// RegisterArtifact 登记已写入 BlobStore 的产物（实现 runner.Store）。以 (task, artifact, sha256)
// 为身份；新登记要求提交者是当前 attempt 且访问有效（与 checkpoint 相同的 fencing）。版本来自
// artifact_heads 的行锁（规格 §7.2）。锁顺序：tasks → task_event_seq → attempt_access → artifact_heads。
func (s *Store) RegisterArtifact(ctx context.Context, a runner.Artifact) (runner.ArtifactVersion, error) {
	if a.TaskID == "" || a.ArtifactID == "" || a.AttemptID == "" || len(a.SHA256) != 64 || a.Size < 0 || a.MediaType == "" {
		return runner.ArtifactVersion{}, invalidf("RegisterArtifact 缺少字段或 sha256 不合法")
	}
	if a.Visibility != "output" && a.Visibility != "internal" {
		return runner.ArtifactVersion{}, invalidf("visibility 必须是 output 或 internal")
	}
	out := runner.ArtifactVersion{TaskID: a.TaskID, ArtifactID: a.ArtifactID, SHA256: a.SHA256}
	err := s.run(ctx, "RegisterArtifact", a.TaskID+"/"+a.ArtifactID+"@"+a.SHA256, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockTaskShared(ctx, tx, a.TaskID); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, a.TaskID); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, "SELECT version FROM artifacts WHERE task_id = $1 AND artifact_id = $2 AND sha256 = $3",
			a.TaskID, a.ArtifactID, a.SHA256).Scan(&out.Version)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := fenceAttempt(ctx, tx, a.TaskID, a.AttemptID); err != nil {
			return err
		}
		if err := recordBlob(ctx, tx, a); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO artifact_heads (task_id, artifact_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
			a.TaskID, a.ArtifactID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `UPDATE artifact_heads SET next_version = next_version + 1
			WHERE task_id = $1 AND artifact_id = $2 RETURNING next_version - 1`, a.TaskID, a.ArtifactID).Scan(&out.Version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO artifacts (task_id, artifact_id, version, sha256, size, media_type, visibility, attempt_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			a.TaskID, a.ArtifactID, out.Version, a.SHA256, a.Size, a.MediaType, a.Visibility, a.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO blob_provenance (scope_kind, scope_id, sha256, source, ref) VALUES ('task', $1, $2, 'artifact', $3)",
			a.TaskID, a.SHA256, fmt.Sprintf("%s@%d", a.ArtifactID, out.Version)); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"artifact_id": a.ArtifactID, "version": out.Version, "sha256": a.SHA256})
		_, err = appendHostEvent(ctx, tx, hostEvent{taskID: a.TaskID, key: fmt.Sprintf("artifact_saved:%s:%d", a.ArtifactID, out.Version),
			attemptID: a.AttemptID, typ: "artifact_saved", payload: payload})
		return err
	})
	return out, err
}

// recordBlob 登记 blob 与其授权关联（task 及其会话，规格 §5.6）；同一 sha256 的大小不同为冲突。
func recordBlob(ctx context.Context, tx pgx.Tx, a runner.Artifact) error {
	if _, err := tx.Exec(ctx, "INSERT INTO blobs (sha256, size) VALUES ($1, $2) ON CONFLICT DO NOTHING", a.SHA256, a.Size); err != nil {
		return err
	}
	var size int64
	if err := tx.QueryRow(ctx, "SELECT size FROM blobs WHERE sha256 = $1", a.SHA256).Scan(&size); err != nil {
		return err
	}
	if size != a.Size {
		return conflictf("blob %s 已登记为 %d 字节，不是 %d", a.SHA256, size, a.Size)
	}
	return authorizeTaskBlob(ctx, tx, a.TaskID, a.SHA256)
}

// authorizeTaskBlob 把 blob 授权到任务 scope，任务属于会话时同事务另授权到会话 scope（§5.5 第 2 条：attempt → task
// → session 推导；会话 scope 使后续 turn 的 carryover、恢复与会话 checkpoint 可以引用它）。
func authorizeTaskBlob(ctx context.Context, tx pgx.Tx, taskID, sha string) error {
	_, err := tx.Exec(ctx, `INSERT INTO scope_blobs (scope_kind, scope_id, sha256)
		SELECT 'task', $1::text, $2::text
		UNION ALL SELECT 'session', session_id, $2::text FROM tasks WHERE task_id = $1::text AND session_id IS NOT NULL
		ON CONFLICT DO NOTHING`, taskID, sha)
	return err
}

// GetArtifact 读取已登记的产物版本（实现 runner.Store）。
func (s *Store) GetArtifact(ctx context.Context, taskID, artifactID, sha256 string) (runner.ArtifactVersion, error) {
	out := runner.ArtifactVersion{TaskID: taskID, ArtifactID: artifactID, SHA256: sha256}
	err := s.read(ctx, "GetArtifact", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, "SELECT version FROM artifacts WHERE task_id = $1 AND artifact_id = $2 AND sha256 = $3",
			taskID, artifactID, sha256).Scan(&out.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("产物 %s/%s@%s", taskID, artifactID, sha256)
		}
		return err
	})
	return out, err
}

// LatestArtifact 返回任务中该产物的最新版本（实现 runner.Store）。
func (s *Store) LatestArtifact(ctx context.Context, taskID, artifactID string) (runner.ArtifactVersion, error) {
	out := runner.ArtifactVersion{TaskID: taskID, ArtifactID: artifactID}
	err := s.read(ctx, "LatestArtifact", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT version, sha256 FROM artifacts WHERE task_id = $1 AND artifact_id = $2
			ORDER BY version DESC LIMIT 1`, taskID, artifactID).Scan(&out.Version, &out.SHA256)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("产物 %s/%s", taskID, artifactID)
		}
		return err
	})
	return out, err
}

func proposalHash(p runner.TerminalProposal) []byte {
	return contentHash([]byte(p.Kind), []byte(p.Ref))
}

// RecordTerminalProposal 记录终态提议（实现 runner.Store）：相同内容返回原结果，不同内容为冲突，
// 不以 WHERE … IS NULL 静默忽略（设计 §2.4）。只写 attempts 的 terminal_proposal* 列。
func (s *Store) RecordTerminalProposal(ctx context.Context, p runner.TerminalProposal) (runner.TerminalProposal, error) {
	switch p.Kind {
	case "result", "error", "paused", "awaiting_input":
	default:
		return runner.TerminalProposal{}, invalidf("终态提议必须是 result、error、paused 或 awaiting_input，得到 %q", p.Kind)
	}
	hash := proposalHash(p)
	var out runner.TerminalProposal
	err := s.run(ctx, "RecordTerminalProposal", p.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		existing, existingHash, err := selectProposal(ctx, tx, p.AttemptID, true)
		if err != nil {
			return err
		}
		switch {
		case existingHash != nil && bytes.Equal(existingHash, hash):
			out = existing
			return nil
		case existingHash != nil:
			return conflictf("attempt %s 已有不同的终态提议 %s", p.AttemptID, existing.Kind)
		}
		var decided bool
		if err := tx.QueryRow(ctx, "SELECT verdict_hash IS NOT NULL FROM attempts WHERE attempt_id = $1", p.AttemptID).Scan(&decided); err != nil {
			return err
		}
		if decided { // 判决已提交（例如无提议即退出）后，迟到的提议不再记录
			return rejectf(persistence.CodeStaleAttempt, "attempt %s 已有判决，不再接受终态提议", p.AttemptID)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET terminal_proposal = $2, terminal_proposal_ref = $3, terminal_proposal_hash = $4
			WHERE attempt_id = $1`, p.AttemptID, p.Kind, p.Ref, hash); err != nil {
			return err
		}
		out = p
		return nil
	})
	return out, err
}

// GetTerminalProposal 读取终态提议（实现 runner.Store）。
func (s *Store) GetTerminalProposal(ctx context.Context, attemptID string) (runner.TerminalProposal, error) {
	var out runner.TerminalProposal
	err := s.read(ctx, "GetTerminalProposal", func(ctx context.Context, q queryer) error {
		p, hash, err := selectProposal(ctx, q, attemptID, false)
		if err == nil && hash == nil {
			return notFoundf("attempt %s 尚无终态提议", attemptID)
		}
		out = p
		return err
	})
	return out, err
}

func selectProposal(ctx context.Context, q queryer, attemptID string, forUpdate bool) (runner.TerminalProposal, []byte, error) {
	sql := "SELECT COALESCE(terminal_proposal, ''), COALESCE(terminal_proposal_ref, ''), terminal_proposal_hash FROM attempts WHERE attempt_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	p := runner.TerminalProposal{AttemptID: attemptID}
	var hash []byte
	err := q.QueryRow(ctx, sql, attemptID).Scan(&p.Kind, &p.Ref, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil, notFoundf("attempt %s", attemptID)
	}
	return p, hash, err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func workerEventHash(e runner.WorkerEvent) []byte {
	return contentHash([]byte(e.Type), e.Payload)
}

// validateWorkerBatch 检查批次非空、序号从 1 起且连续、类型与载荷齐全。
func validateWorkerBatch(events []runner.WorkerEvent) error {
	if len(events) == 0 {
		return invalidf("Worker 事件批次为空")
	}
	for i, e := range events {
		if e.Seq < 1 || (i > 0 && e.Seq != events[i-1].Seq+1) {
			return invalidf("Worker 事件序号必须从 1 起且连续，第 %d 条为 %d", i, e.Seq)
		}
		if e.Type == "" || len(e.Payload) == 0 {
			return invalidf("Worker 事件 %d 缺少类型或载荷", e.Seq)
		}
	}
	return nil
}

// appendWorkerBatch 原子追加一批 Worker 事件（设计 §3.1）：重叠部分逐条校验内容，
// 只追加连续的新后缀，缺口拒绝。调用方必须已持有 lockEventSeq。返回新的水位。
func appendWorkerBatch(ctx context.Context, tx pgx.Tx, taskID, attemptID string, events []runner.WorkerEvent) (int64, error) {
	first, last := events[0].Seq, events[len(events)-1].Seq
	var watermark int64
	if err := tx.QueryRow(ctx, "SELECT COALESCE(max(worker_seq), 0) FROM events WHERE attempt_id = $1 AND worker_seq IS NOT NULL",
		attemptID).Scan(&watermark); err != nil {
		return 0, err
	}
	if first > watermark+1 {
		return 0, conflictf("attempt %s 的 Worker 事件有缺口：已提交到 %d，批次从 %d 开始", attemptID, watermark, first)
	}
	existing, err := existingWorkerHashes(ctx, tx, attemptID, first, min(last, watermark))
	if err != nil {
		return 0, err
	}
	for _, e := range events {
		if e.Seq > watermark {
			if err := insertWorkerEvent(ctx, tx, taskID, attemptID, e); err != nil {
				return 0, err
			}
			continue
		}
		if !bytes.Equal(existing[e.Seq], workerEventHash(e)) {
			return 0, conflictf("attempt %s 的 Worker 事件 %d 已存在且内容不同", attemptID, e.Seq)
		}
	}
	return max(watermark, last), nil
}

func existingWorkerHashes(ctx context.Context, tx pgx.Tx, attemptID string, from, to int64) (map[int64][]byte, error) {
	hashes := map[int64][]byte{}
	if from > to {
		return hashes, nil
	}
	rows, err := tx.Query(ctx, "SELECT worker_seq, content_hash FROM events WHERE attempt_id = $1 AND worker_seq BETWEEN $2 AND $3",
		attemptID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var h []byte
		if err := rows.Scan(&seq, &h); err != nil {
			return nil, err
		}
		hashes[seq] = h
	}
	return hashes, rows.Err()
}

func insertWorkerEvent(ctx context.Context, tx pgx.Tx, taskID, attemptID string, e runner.WorkerEvent) error {
	seq, err := nextEventSeq(ctx, tx, taskID)
	if err != nil {
		return err
	}
	sessionID, sessionSeq, err := appendSessionSeq(ctx, tx, taskID) // 会话 task：同事务分配 session_seq
	if err != nil {
		return err
	}
	var ts any // 零值时用数据库时间
	if !e.TS.IsZero() {
		ts = e.TS
	}
	_, err = tx.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, worker_seq, source, type, payload, content_hash,
			ts, session_id, session_seq, subrun_id)
		VALUES ($1, $2, $3, $4, $5, 'worker', $6, $7, $8, COALESCE($9::timestamptz, now()), NULLIF($10, ''), $11, NULLIF($12, ''))`,
		taskID, seq, fmt.Sprintf("w:%s:%d", attemptID, e.Seq), attemptID, e.Seq, e.Type, []byte(e.Payload), workerEventHash(e),
		ts, sessionID, nullSeq(sessionSeq), eventSubrunID(e.Payload))
	return err
}

// eventSubrunID 是 Worker 事件载荷顶层的 subrun_id（subrun_* 与带 subrun_id 的 progress）；没有、无法解析或不合法时为空。
func eventSubrunID(payload []byte) string {
	var p struct {
		SubrunID string `json:"subrun_id"`
	}
	if json.Unmarshal(payload, &p) != nil || !subrun.ValidID(p.SubrunID) {
		return ""
	}
	return p.SubrunID
}
