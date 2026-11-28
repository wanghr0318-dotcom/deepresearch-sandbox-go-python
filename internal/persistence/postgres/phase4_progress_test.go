package postgres

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
)

func TestRecoveryAtDurableBoundaries(t *testing.T) {
	ctx := t.Context()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	t.Run("before first checkpoint", func(t *testing.T) {
		gwFixture(t, s, "fresh", 1000)
		retryWithNewAttempt(t, s, "fresh")
		state, err := s.LoadTask(ctx, "fresh")
		if err != nil || state.Latest != nil {
			t.Fatalf("fresh attempt bootstrap: %+v, %v", state.Latest, err)
		}
		if n := count(t, s, "SELECT count(*) FROM tasks WHERE task_id='fresh' AND current_attempt_id='att2-fresh' AND spec_json->>'worker'='sim'"); n != 1 {
			t.Fatalf("original spec for new owner: %d", n)
		}
		if n := count(t, s, "SELECT count(*) FROM call_tries WHERE task_id='fresh'"); n != 0 {
			t.Fatalf("unexpected tries: %d", n)
		}
		expectBudget(t, s, "fresh", call.Budget{LimitMicro: 1000})
	})

	t.Run("settled call after last checkpoint", func(t *testing.T) {
		const taskID, callID = "journal", "root/orch/chat/1"
		gwFixture(t, s, taskID, 1000)
		cp := runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: taskID}, CheckpointID: "C1", AttemptID: "att-journal", StepID: "before-chat", State: json.RawMessage(`{"next_call_id":"root/orch/chat/1"}`)}
		// Exercise the real post-commit resolution on a checkpoint and the terminal verdict.
		var dropped string
		dropNext := "CommitCheckpoint"
		s.hooks.afterCommit = func(op string) error {
			if op == dropNext {
				dropped, dropNext = op, ""
				return errors.New("test lost commit reply")
			}
			return nil
		}
		defer func() { s.hooks.afterCommit = nil }()
		if _, err := s.CommitCheckpoint(ctx, cp); err != nil || dropped != "CommitCheckpoint" {
			t.Fatalf("checkpoint reply: dropped=%q, err=%v", dropped, err)
		}
		beginCall(t, s, taskID, callID)
		tr := mustReserve(t, s, taskID, callID, 100)
		const answer = "durable output"
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(answer)))
		committed := settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 35, ResultSHA256: digest, ResultSize: int64(len(answer)), UpstreamRequestID: "fixture-response-1"})
		retryWithNewAttempt(t, s, taskID)
		state, err := s.LoadTask(ctx, taskID)
		if err != nil || state.Latest == nil || state.Latest.CheckpointID != "C1" {
			t.Fatalf("resume checkpoint: %+v, %v", state.Latest, err)
		}
		var cursor struct {
			NextCallID string `json:"next_call_id"`
		}
		if err := json.Unmarshal(state.Latest.State, &cursor); err != nil || cursor.NextCallID != callID {
			t.Fatalf("cursor: %+v %v", cursor, err)
		}
		replay, err := s.BeginCall(ctx, call.BeginCallRequest{TaskID: taskID, CallID: cursor.NextCallID, AttemptID: "att2-journal", Fingerprint: "fp-" + callID, Endpoint: "chat", Deadline: 120 * time.Second})
		if err != nil || !replay.Existing || replay.Record.State != call.StateCompleted || replay.Record.ResultRef != digest || replay.Record.UpstreamRequestID != committed.UpstreamRequestID || replay.Record.TriesUsed != 1 {
			t.Fatalf("recorded replay: %+v, %v", replay, err)
		}
		if n := count(t, s, "SELECT count(*) FROM call_tries WHERE task_id=$1", taskID); n != 1 {
			t.Fatalf("tries: %d", n)
		}
		expectBudget(t, s, taskID, call.Budget{LimitMicro: 1000, SpentMicro: 35})
		v := verdict(taskID, 0)
		v.AttemptID, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = "att2-journal", "succeeded", "succeeded", ""
		v.Result = json.RawMessage(`{"ok":true}`)
		dropNext = "FinalizeAttempt"
		for i := 0; i < 2; i++ {
			if _, err := s.FinalizeAttempt(ctx, v); err != nil {
				t.Fatal(err)
			}
		}
		if dropped != "FinalizeAttempt" {
			t.Fatal("terminal commit ACK was not dropped")
		}
		if n := count(t, s, "SELECT count(*) FROM events WHERE attempt_id=$1 AND type='attempt_ended'", v.AttemptID); n != 1 {
			t.Fatalf("terminal events: %d", n)
		}
		if n := count(t, s, "SELECT count(*) FROM tasks WHERE task_id=$1 AND status='succeeded' AND result_json->>'ok'='true'", taskID); n != 1 {
			t.Fatalf("terminal task and result: %d", n)
		}
		if n := count(t, s, "SELECT count(*) FROM checkpoints WHERE scope_kind='task' AND scope_id=$1", taskID); n != 1 {
			t.Fatalf("checkpoint rows: %d", n)
		}
	})
}
