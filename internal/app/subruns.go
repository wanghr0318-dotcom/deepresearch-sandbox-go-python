package app

// 本文件是 sub-run 扩展（M4 Plan 14 Task 7；规格 §5.2、§13）的装配：扩展协商（--worker-subruns）、runner.SubrunHost
// （postgres Store 的 sub-run 用例 + call.Coordinator.CancelSubrun），以及 task 模式 init.resume 与 session 模式
// task_start.resume 的共同组装（含 resume.subruns）。重新绑定与裁决收尾在 Store 的事务内（CreateAttempt、FinalizeAttempt）。

import (
	"context"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/protocol"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/subrun"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// subrunStore 是 postgres Store 的 sub-run 生命周期用例（Plan 14 Task 3）。
type subrunStore interface {
	StartSubrun(ctx context.Context, taskID, attemptID, subrunID string, d subrun.Definition) (subrun.Record, error)
	ProposeSubrunEnd(ctx context.Context, taskID, attemptID, subrunID, status, summary string) (subrun.Record, error)
	RequestSubrunCancel(ctx context.Context, taskID, subrunID, reason string) (subrun.Record, error)
	ListSubruns(ctx context.Context, taskID string) ([]subrun.Record, error)
}

// subrunCanceller 是 call.Coordinator 中按 sub-run 取消在途调用的部分（Plan 14 Task 5）。
type subrunCanceller interface {
	CancelSubrun(taskID, attemptID, subrunID string)
}

// subrunHost 是 runner.SubrunHost ← postgres Store + call.Coordinator。runner 先经 RequestCancel 提交 cancel_requested，
// 再调用 CancelGateway（规格 §13.4：先持久化、再取消在途调用；之后的新请求由 Store 以 subrun_closed 拒绝）。
type subrunHost struct {
	store subrunStore
	calls subrunCanceller
}

var _ runner.SubrunHost = subrunHost{}

func (h subrunHost) Start(ctx context.Context, taskID, attemptID, subrunID string, d subrun.Definition) (subrun.Record, error) {
	return h.store.StartSubrun(ctx, taskID, attemptID, subrunID, d)
}

func (h subrunHost) ProposeEnd(ctx context.Context, taskID, attemptID, subrunID, status, summary string) (subrun.Record, error) {
	return h.store.ProposeSubrunEnd(ctx, taskID, attemptID, subrunID, status, summary)
}

func (h subrunHost) RequestCancel(ctx context.Context, taskID, subrunID, reason string) (subrun.Record, error) {
	return h.store.RequestSubrunCancel(ctx, taskID, subrunID, reason)
}

func (h subrunHost) List(ctx context.Context, taskID string) ([]subrun.Record, error) {
	return h.store.ListSubruns(ctx, taskID)
}

func (h subrunHost) CancelGateway(taskID, attemptID, subrunID string) {
	h.calls.CancelSubrun(taskID, attemptID, subrunID)
}

// workerExtensions 是宿主在 init（task 模式）与会话 init（session 模式）中请求的扩展（规格 §5.2）：--worker-subruns
// 开启时为 ["subruns"]，Worker 的 ready 须回 subruns: 1，否则以 extension_mismatch 拒绝；关闭时不请求（nil，init 中
// 无 extensions 字段）。仓库尚无 /etc/agentbox/worker.json 清单，是否启用只由 server 标志决定（已知简化）。
func (c Config) workerExtensions() []string {
	if c.WorkerSubruns {
		return []string{protocol.ExtensionSubruns}
	}
	return nil
}

// taskResume 由任务事实组装 init.resume（task 模式）与 task_start.resume（session 模式）：最新已提交 checkpoint，以及
// CreateAttempt 重新绑定后各 sub-run 的状态（resume.subruns，规格 §13.5）。没有 checkpoint 时返回 nil：协议的
// resume 要求 checkpoint_id（protocol.Resume.validate），Worker 从头开始，以同 ID 同定义重发 subrun_start 时由宿主
// 答复（已重新绑定的为 started，已终态的为 rejected/subrun_closed）。
func taskResume(ts task.TaskState) *protocol.Resume {
	cp := ts.Latest
	if cp == nil {
		return nil
	}
	r := &protocol.Resume{CheckpointID: cp.CheckpointID, StepID: cp.StepID, State: cp.State, StateRef: cp.StateRef, Refs: cp.Refs}
	for _, s := range ts.Subruns {
		r.Subruns = append(r.Subruns, protocol.ResumeSubrun{SubrunID: s.SubrunID, Status: s.Status, ResultRef: s.ResultRef})
	}
	return r
}
