// Package api 是 HTTP 入口（后续计划实现）。本文件声明 API 层需要的持久化用例
// （代码组织设计 §4）；实现位于 internal/persistence/postgres。
package api

import (
	"context"
	"encoding/json"
	"time"
)

// Store 是 API 层的窄接口。每个方法是一个完整的事务用例；错误类别见 internal/persistence。
type Store interface {
	// CreateTask 创建任务。以 request_id 幂等：同 ID 同 body_hash 返回原结果（Replayed=true），
	// 同 ID 不同 body_hash 返回 persistence.ErrConflict。
	CreateTask(ctx context.Context, req CreateTaskRequest) (CreateTaskResult, error)
	// AcceptControl 写入控制意图并递增 control_version；以 request_id 幂等。
	AcceptControl(ctx context.Context, req ControlRequest) (ControlResult, error)
	// GetRequest 按 request_id 读取已提交的请求记录；不存在为 persistence.ErrNotFound。
	GetRequest(ctx context.Context, requestID string) (RequestRecord, error)
	// GetTask 读取任务的当前视图。
	GetTask(ctx context.Context, taskID string) (TaskView, error)
	// ListEvents 按 task_seq 升序返回 afterSeq 之后的至多 limit 条事件。
	ListEvents(ctx context.Context, taskID string, afterSeq int64, limit int) ([]Event, error)
}

// CreateTaskRequest 是创建任务的输入。TaskID 由调用方在事务前生成。
type CreateTaskRequest struct {
	RequestID       string
	BodyHash        []byte
	TaskID          string
	Spec            json.RawMessage
	ConfigVersion   string
	Limits          json.RawMessage
	MaxFaultRetries int64
}

// CreateTaskResult 是创建任务的结果。Replayed 表示返回的是同一请求先前提交的结果。
type CreateTaskResult struct {
	TaskID   string `json:"task_id"`
	Replayed bool   `json:"-"`
}

// ControlRequest 是一次控制请求：desired 取 run、pause 或 cancel。
type ControlRequest struct {
	RequestID string
	BodyHash  []byte
	TaskID    string
	Desired   string
	Reason    string
}

// ControlResult 是控制请求的结果。
type ControlResult struct {
	TaskID         string `json:"task_id"`
	ControlVersion int64  `json:"control_version"`
	Replayed       bool   `json:"-"`
}

// RequestRecord 是 api_requests 中的一行。
type RequestRecord struct {
	RequestID  string
	Kind       string
	BodyHash   []byte
	ResourceID string
	Response   json.RawMessage
}

// TaskView 是任务的当前视图。
type TaskView struct {
	TaskID                string
	Status                string
	StatusReason          string
	CurrentAttemptID      string
	Desired               string
	ControlVersion        int64
	AppliedControlVersion int64
	AttemptsTotal         int64
}

// Event 是事件流中的一条；host 事件的 WorkerSeq 为 0。
type Event struct {
	TaskSeq   int64
	AttemptID string
	Source    string
	Type      string
	WorkerSeq int64
	Payload   json.RawMessage
	TS        time.Time
}
