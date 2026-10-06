package protocol

import "encoding/json"

// 消息类型（规格 §5.4，task 模式）。
const (
	TypeInit             = "init"
	TypeCheckpointResult = "checkpoint_result"
	TypeArtifactResult   = "artifact_result"
	TypeCancel           = "cancel"
	TypePause            = "pause"

	TypeReady           = "ready"
	TypeProgress        = "progress"
	TypeArtifact        = "artifact"
	TypeCheckpoint      = "checkpoint"
	TypeCheckpointQuery = "checkpoint_query"
	TypePaused          = "paused"
	TypeResult          = "result"
	TypeError           = "error"
	TypeHandshakeError  = "handshake_error"
)

// 枚举取值。
const (
	ModeTask    = "task"
	ModeSession = "session"

	ScopeTask    = "task"
	ScopeSession = "session"

	VisibilityOutput   = "output"
	VisibilityInternal = "internal"

	CheckpointCommitted      = "committed"
	CheckpointConflict       = "conflict"
	CheckpointRejected       = "rejected"
	CheckpointRetryableError = "retryable_error"
	CheckpointNotFound       = "not_found"

	ArtifactSaved    = "saved"
	ArtifactRejected = "rejected"
)

// Message 是一条协议消息。
type Message interface {
	// MessageType 返回该类型消息的 type 取值。
	MessageType() string
	declaredType() string
	validate() error
}

// event 是带 seq 的 Worker 事件（handshake_error 除外）。
type event interface {
	Message
	header() EventHeader
}

// HostHeader 是宿主控制消息（init 除外）的公共字段。
type HostHeader struct {
	Type string `json:"type"`
	V    int64  `json:"v"`
}

func (h HostHeader) declaredType() string { return h.Type }

// EventHeader 是 Worker 事件的公共字段。ts 仅用于诊断（规格 §5.3）。
// attempt_id 在 session 模式下由 task 相关事件携带（见 session.go）；task 模式不要求。
type EventHeader struct {
	Type      string `json:"type"`
	V         int64  `json:"v"`
	Seq       int64  `json:"seq"`
	TS        string `json:"ts,omitempty"`
	AttemptID string `json:"attempt_id,omitempty"`
}

func (h EventHeader) declaredType() string { return h.Type }
func (h EventHeader) header() EventHeader  { return h }

// Init 是宿主发给 Worker 的第一条消息。type、bootstrap、protocol_versions
// 组成引导信封，永不改变（规格 §5.2）；init 本身不带 v。
// task 模式使用 task_id 至 resume；session 模式使用 session_id、incarnation_id、
// config 与 session_resume（task 字段由之后的 task_start 携带）。
type Init struct {
	Type             string          `json:"type"`
	Bootstrap        int64           `json:"bootstrap"`
	ProtocolVersions []int64         `json:"protocol_versions"`
	Mode             string          `json:"mode"`
	TaskID           string          `json:"task_id,omitempty"`
	AttemptID        string          `json:"attempt_id,omitempty"`
	AttemptNo        int64           `json:"attempt_no,omitempty"`
	Traceparent      string          `json:"traceparent,omitempty"`
	Config           json.RawMessage `json:"config,omitempty"`
	ConfigVersion    string          `json:"config_version,omitempty"`
	BudgetLimits     json.RawMessage `json:"budget_limits,omitempty"`
	InputRefs        []string        `json:"input_refs,omitempty"`
	OutDir           string          `json:"out_dir,omitempty"`
	Resume           *Resume         `json:"resume,omitempty"`
	SessionID        string          `json:"session_id,omitempty"`
	IncarnationID    string          `json:"incarnation_id,omitempty"`
	SessionResume    *SessionResume  `json:"session_resume,omitempty"`
	// Extensions 请求经协商的扩展（规格 §5.2），目前只有 "subruns"；ready 须逐一确认。
	Extensions []string `json:"extensions,omitempty"`
}

// Resume 是恢复时随 init 下发的最近已提交 checkpoint。
type Resume struct {
	CheckpointID string          `json:"checkpoint_id"`
	StepID       string          `json:"step_id"`
	State        json.RawMessage `json:"state,omitempty"`
	StateRef     string          `json:"state_ref,omitempty"`
	Refs         []string        `json:"refs,omitempty"`
	// Subruns 是宿主裁定的各 sub-run 状态（sub-run 扩展，规格 §13.5）。
	Subruns []ResumeSubrun `json:"subruns,omitempty"`
}

// CheckpointResult 是宿主对 checkpoint 或 checkpoint_query 的答复。
// attempt_id 在 session 模式下必需。
type CheckpointResult struct {
	HostHeader
	AttemptID    string `json:"attempt_id,omitempty"`
	CheckpointID string `json:"checkpoint_id"`
	Scope        string `json:"scope"`
	Status       string `json:"status"`
	Code         string `json:"code,omitempty"`
}

// ArtifactResult 是宿主对产物登记的答复。attempt_id 在 session 模式下必需。
type ArtifactResult struct {
	HostHeader
	AttemptID  string `json:"attempt_id,omitempty"`
	ArtifactID string `json:"artifact_id"`
	Status     string `json:"status"`
	Version    int64  `json:"version,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Code       string `json:"code,omitempty"`
}

// Cancel 要求 Worker 在 grace_ms 内停止。
type Cancel struct {
	HostHeader
	AttemptID string `json:"attempt_id"`
	Reason    string `json:"reason"`
	GraceMS   int64  `json:"grace_ms"`
}

// Pause 要求 Worker 在下一个提交边界暂停（协作式，规格 §5.9）。
type Pause struct {
	HostHeader
	AttemptID string `json:"attempt_id"`
	Reason    string `json:"reason"`
	GraceMS   int64  `json:"grace_ms"`
}

// WorkerInfo 标识 Worker 实现。
type WorkerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Ready 是 Worker 完成握手后的第一条事件。
type Ready struct {
	EventHeader
	ProtocolVersion int64      `json:"protocol_version"`
	Mode            string     `json:"mode"`
	Worker          WorkerInfo `json:"worker"`
	Capabilities    []string   `json:"capabilities"`
	// SessionExt 确认 session 扩展，mode = session 时必须为 1（规格 §5.2）。
	SessionExt int64 `json:"session_ext,omitempty"`
	// Subruns 确认 sub-run 扩展：init.extensions 含 "subruns" 时必须为 1，否则必须缺省或为 0。
	Subruns int64 `json:"subruns,omitempty"`
}

// Progress 是业务进度；可恢复的工具失败以 kind=tool_error 报告。
type Progress struct {
	EventHeader
	StepID  string          `json:"step_id,omitempty"`
	Kind    string          `json:"kind"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
	// SubrunID 表示该进度属于某个 sub-run（sub-run 扩展）。
	SubrunID string `json:"subrun_id,omitempty"`
}

// Artifact 登记 out_dir 下的一个产物；声明的哈希与大小由宿主验证。
type Artifact struct {
	EventHeader
	ArtifactID     string `json:"artifact_id"`
	Path           string `json:"path"`
	DeclaredSHA256 string `json:"declared_sha256"`
	DeclaredSize   int64  `json:"declared_size"`
	MediaType      string `json:"media_type"`
	Visibility     string `json:"visibility"`
}

// Checkpoint 提议提交一次进度；是否成立由宿主决定（规格 §5.5）。
type Checkpoint struct {
	EventHeader
	CheckpointID string          `json:"checkpoint_id"`
	Scope        string          `json:"scope"`
	StepID       string          `json:"step_id"`
	State        json.RawMessage `json:"state,omitempty"`
	StateRef     string          `json:"state_ref,omitempty"`
	Refs         []string        `json:"refs,omitempty"`
	// Subruns 是该 checkpoint 时各 sub-run 的状态（sub-run 扩展，规格 §5.5 规则 2、6）。
	Subruns []CheckpointSubrun `json:"subruns,omitempty"`
}

// CheckpointQuery 查询某个 checkpoint 的提交结果。
type CheckpointQuery struct {
	EventHeader
	CheckpointID string `json:"checkpoint_id"`
	Scope        string `json:"scope"`
}

// Paused 是暂停的终态提议，checkpoint_id 必须是最新已提交的 checkpoint。
type Paused struct {
	EventHeader
	CheckpointID string `json:"checkpoint_id"`
}

// Result 是成功的终态提议。session_state 仅在 session 模式下有意义（规格 §12.3），
// task 模式不校验也不使用它。
type Result struct {
	EventHeader
	Summary      string        `json:"summary"`
	Outputs      []string      `json:"outputs"`
	SessionState *SessionState `json:"session_state,omitempty"`
	// Subruns 列出每个 sub-run 的状态与摘要（sub-run 扩展，规格 §13.6）；只供展示，不改变 sub-run 状态。
	Subruns []ResultSubrun `json:"subruns,omitempty"`
}

// ErrorEvent 是失败的终态提议；retryable 只是给宿主的建议。
type ErrorEvent struct {
	EventHeader
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// HandshakeError 在没有共同协议版本时发出，格式固定（规格 §5.2）。
type HandshakeError struct {
	Type      string `json:"type"`
	Bootstrap int64  `json:"bootstrap"`
	Code      string `json:"code"`
}

func (*Init) MessageType() string              { return TypeInit }
func (m *Init) declaredType() string           { return m.Type }
func (*CheckpointResult) MessageType() string  { return TypeCheckpointResult }
func (*ArtifactResult) MessageType() string    { return TypeArtifactResult }
func (*Cancel) MessageType() string            { return TypeCancel }
func (*Pause) MessageType() string             { return TypePause }
func (*Ready) MessageType() string             { return TypeReady }
func (*Progress) MessageType() string          { return TypeProgress }
func (*Artifact) MessageType() string          { return TypeArtifact }
func (*Checkpoint) MessageType() string        { return TypeCheckpoint }
func (*CheckpointQuery) MessageType() string   { return TypeCheckpointQuery }
func (*Paused) MessageType() string            { return TypePaused }
func (*Result) MessageType() string            { return TypeResult }
func (*ErrorEvent) MessageType() string        { return TypeError }
func (*HandshakeError) MessageType() string    { return TypeHandshakeError }
func (m *HandshakeError) declaredType() string { return m.Type }
