package protocol

import (
	"encoding/json"
	"strings"
)

// session 扩展的消息类型（规格 §5.4 session 扩展；M4 Plan 12 修订：awaiting_input、task_start.directive）。
const (
	TypeTaskStart    = "task_start"
	TypeTaskOutcome  = "task_outcome"
	TypeQuiesce      = "quiesce"
	TypeSessionClose = "session_close"

	TypeTaskAccepted     = "task_accepted"
	TypeTaskOutcomeQuery = "task_outcome_query"
	TypeTaskReleased     = "task_released"
	TypeQuiesced         = "quiesced"
	TypeClosed           = "closed"
	TypeAwaitingInput    = "awaiting_input"
)

// session 扩展的枚举取值。task_start 不带 directive 即正常开始或继续（协调者裁定 B），
// 因此没有 "continue" 取值。
const (
	DirectiveFinishNow = "finish_now"
	DirectiveAnswer    = "answer"

	VerdictSucceeded = "succeeded"
	VerdictFailed    = "failed"
	VerdictCancelled = "cancelled"
	VerdictPaused    = "paused"

	// SessionExtVersion 是 ready.session_ext 必须确认的取值。
	SessionExtVersion = 1
	// StagedStateDir 是冷恢复时宿主暂存 session state 的只读目录（规格 §12.2）；
	// staged_state_path 必须是该目录下以 sha256 命名的文件。
	StagedStateDir = "/run/agentbox/restore/"
)

// SessionResume 是冷恢复时随 init 下发的最新已提交 session checkpoint。
// state 与 staged_state_path 必须且只能有一个；不携带 state_ref（规格 §12.2）。
type SessionResume struct {
	CheckpointID    string          `json:"checkpoint_id"`
	State           json.RawMessage `json:"state,omitempty"`
	StagedStatePath string          `json:"staged_state_path,omitempty"`
	Refs            []string        `json:"refs,omitempty"`
	// StateRef 只用于拒绝：session_resume 带 state_ref 是 invalid_field。
	StateRef string `json:"state_ref,omitempty"`
}

// Directive 是续跑指令：finish_now（立即收尾）或 answer（回答 awaiting_input 的提问）。
type Directive struct {
	Kind       string          `json:"kind"`
	QuestionID string          `json:"question_id,omitempty"`
	Answers    json.RawMessage `json:"answers,omitempty"`
}

// Carryover 指向被取代 turn 的最新 task checkpoint（已授权到会话 scope 的 blob），
// Worker 经 Gateway 读取；缺失或损坏时新一轮不带上下文继续（协调者裁定 D）。
type Carryover struct {
	TaskID        string `json:"task_id"`
	CheckpointRef string `json:"checkpoint_ref"`
}

// TaskStart 在会话 incarnation 中开始一个 task attempt。
type TaskStart struct {
	HostHeader
	TaskID        string          `json:"task_id"`
	AttemptID     string          `json:"attempt_id"`
	AttemptNo     int64           `json:"attempt_no"`
	Traceparent   string          `json:"traceparent,omitempty"`
	Config        json.RawMessage `json:"config,omitempty"`
	ConfigVersion string          `json:"config_version,omitempty"`
	BudgetLimits  json.RawMessage `json:"budget_limits,omitempty"`
	InputRefs     []string        `json:"input_refs,omitempty"`
	OutDir        string          `json:"out_dir"`
	// BaseSessionCheckpointID 为空表示会话尚无 checkpoint。
	BaseSessionCheckpointID string     `json:"base_session_checkpoint_id,omitempty"`
	Resume                  *Resume    `json:"resume,omitempty"`
	Directive               *Directive `json:"directive,omitempty"`
	// RestoredFromTaskID 仅用于展示：恢复即从宿主复制的种子 checkpoint（resume）继续（协调者裁定 C）。
	RestoredFromTaskID string     `json:"restored_from_task_id,omitempty"`
	Carryover          *Carryover `json:"carryover,omitempty"`
}

// TaskOutcome 告知 Worker 宿主对该 attempt 的最终裁决。
type TaskOutcome struct {
	HostHeader
	AttemptID                    string `json:"attempt_id"`
	Verdict                      string `json:"verdict"`
	CommittedSessionCheckpointID string `json:"committed_session_checkpoint_id,omitempty"`
}

// Quiesce 要求空闲的 Worker 静止并报告其 session 状态对应的 checkpoint。
type Quiesce struct {
	HostHeader
	GraceMS int64 `json:"grace_ms"`
}

// SessionClose 要求 Worker 结束会话 incarnation。
type SessionClose struct {
	HostHeader
	GraceMS int64 `json:"grace_ms"`
}

// TaskAccepted 确认 Worker 已开始 task_start 指定的 attempt。
type TaskAccepted struct{ EventHeader }

// TaskOutcomeQuery 查询该 attempt 的裁决（task_outcome 丢失时）。
type TaskOutcomeQuery struct{ EventHeader }

// TaskReleased 表示 Worker 已采用裁决并释放该 attempt 的全部工作。
type TaskReleased struct{ EventHeader }

// Quiesced 答复 quiesce；session_checkpoint_id 为空串表示会话尚无 checkpoint。
type Quiesced struct {
	EventHeader
	SessionCheckpointID string `json:"session_checkpoint_id"`
}

// Closed 答复 session_close。
type Closed struct{ EventHeader }

// AwaitingInput 是等待用户回答的终态提议（协调者裁定 A）：checkpoint_id 必须是
// 最新已提交的 task checkpoint，question_id 标识提问，回答经 task_start.directive 送回。
type AwaitingInput struct {
	EventHeader
	CheckpointID string `json:"checkpoint_id"`
	QuestionID   string `json:"question_id"`
}

// SessionState 是 result 携带的新 session 状态提议（规格 §12.3）。
type SessionState struct {
	CheckpointID string          `json:"checkpoint_id"`
	State        json.RawMessage `json:"state,omitempty"`
	StateRef     string          `json:"state_ref,omitempty"`
	Refs         []string        `json:"refs,omitempty"`
}

func (*TaskStart) MessageType() string        { return TypeTaskStart }
func (*TaskOutcome) MessageType() string      { return TypeTaskOutcome }
func (*Quiesce) MessageType() string          { return TypeQuiesce }
func (*SessionClose) MessageType() string     { return TypeSessionClose }
func (*TaskAccepted) MessageType() string     { return TypeTaskAccepted }
func (*TaskOutcomeQuery) MessageType() string { return TypeTaskOutcomeQuery }
func (*TaskReleased) MessageType() string     { return TypeTaskReleased }
func (*Quiesced) MessageType() string         { return TypeQuiesced }
func (*Closed) MessageType() string           { return TypeClosed }
func (*AwaitingInput) MessageType() string    { return TypeAwaitingInput }

// sessionRegistry 是 session 模式可用的消息：task 模式的全部消息加上 session 扩展。
var sessionRegistry = func() messageRegistry {
	r := messageRegistry{}
	for dir, types := range registry {
		r[dir] = map[string]func() Message{}
		for typ, f := range types {
			r[dir][typ] = f
		}
	}
	r[HostToWorker][TypeTaskStart] = func() Message { return &TaskStart{} }
	r[HostToWorker][TypeTaskOutcome] = func() Message { return &TaskOutcome{} }
	r[HostToWorker][TypeQuiesce] = func() Message { return &Quiesce{} }
	r[HostToWorker][TypeSessionClose] = func() Message { return &SessionClose{} }
	r[WorkerToHost][TypeTaskAccepted] = func() Message { return &TaskAccepted{} }
	r[WorkerToHost][TypeTaskOutcomeQuery] = func() Message { return &TaskOutcomeQuery{} }
	r[WorkerToHost][TypeTaskReleased] = func() Message { return &TaskReleased{} }
	r[WorkerToHost][TypeQuiesced] = func() Message { return &Quiesced{} }
	r[WorkerToHost][TypeClosed] = func() Message { return &Closed{} }
	r[WorkerToHost][TypeAwaitingInput] = func() Message { return &AwaitingInput{} }
	return r
}()

// DecodeSessionLine 按 session 模式解析并校验一行消息：判定顺序同 DecodeLine，
// 通过各类型自身的规则后再检查 session 模式的附加规则（attempt_id、ready.session_ext 等）。
func DecodeSessionLine(dir Direction, line []byte) (Message, error) {
	m, err := decodeLine(sessionRegistry, dir, line)
	if err != nil {
		return nil, err
	}
	if err := validateSessionMode(m); err != nil {
		return nil, err
	}
	return m, nil
}

// EncodeSessionLine 按 session 模式校验并编码一条消息。
func EncodeSessionLine(dir Direction, m Message) ([]byte, error) {
	return encodeLine(sessionRegistry, dir, m, true)
}

// validateSessionMode 检查 task 模式消息在 session 模式下的附加规则；session 专属消息的
// 规则已全部在其 validate 中。error 的 attempt_id 由 SessionStream 按阶段检查
// （ready 之前的启动失败不属于任何 attempt）。
func validateSessionMode(m Message) error {
	switch m := m.(type) {
	case *Init:
		return oneOf("mode", m.Mode, ModeSession)
	case *CheckpointResult:
		return required("attempt_id", m.AttemptID)
	case *ArtifactResult:
		return required("attempt_id", m.AttemptID)
	case *Ready:
		return m.validateSessionExt()
	case *Progress, *Artifact, *Checkpoint, *CheckpointQuery, *Paused, *SubrunStart, *SubrunEnd, *SubrunCancel:
		return required("attempt_id", m.(event).header().AttemptID)
	case *Result:
		if err := required("attempt_id", m.AttemptID); err != nil {
			return err
		}
		if m.SessionState != nil {
			return m.SessionState.validate()
		}
	}
	return nil
}

func (m *Ready) validateSessionExt() error {
	if err := oneOf("mode", m.Mode, ModeSession); err != nil {
		return err
	}
	switch m.SessionExt {
	case SessionExtVersion:
		return nil
	case 0:
		return newError(CodeSessionExtMissing, "session 模式的 ready 必须带 session_ext: %d", SessionExtVersion)
	}
	return newError(CodeInvalidField, "session_ext=%d，期望 %d", m.SessionExt, SessionExtVersion)
}

func (m *Init) validateSessionInit() error {
	if err := firstErr(required("session_id", m.SessionID), required("incarnation_id", m.IncarnationID)); err != nil {
		return err
	}
	if m.SessionResume == nil {
		return nil
	}
	return m.SessionResume.validate()
}

func (r *SessionResume) validate() error {
	if err := required("session_resume.checkpoint_id", r.CheckpointID); err != nil {
		return err
	}
	if r.StateRef != "" {
		return newError(CodeInvalidField, "session_resume 不携带 state_ref，冷恢复以 staged_state_path 读取")
	}
	hasState, hasPath := hasJSONValue(r.State), r.StagedStatePath != ""
	if hasState == hasPath {
		return newError(CodeInvalidField, "session_resume 的 state 与 staged_state_path 必须且只能提供一个")
	}
	if hasPath {
		if name, ok := strings.CutPrefix(r.StagedStatePath, StagedStateDir); !ok || !validSHA256(name) {
			return newError(CodeInvalidField, "staged_state_path 必须是 %s<sha256>", StagedStateDir)
		}
	} else if err := checkInlineState(r.State); err != nil {
		return err
	}
	return checkRefs("session_resume.refs", r.Refs, MaxRefsPerCheckpoint)
}

func (s *SessionState) validate() error {
	if err := required("session_state.checkpoint_id", s.CheckpointID); err != nil {
		return err
	}
	if err := checkState(s.State, s.StateRef); err != nil {
		return err
	}
	return checkRefs("session_state.refs", s.Refs, MaxRefsPerCheckpoint)
}

func (m *TaskStart) validate() error {
	if err := firstErr(checkVersion(m.V), required("task_id", m.TaskID), required("attempt_id", m.AttemptID), required("out_dir", m.OutDir)); err != nil {
		return err
	}
	if m.AttemptNo < 1 {
		return newError(CodeInvalidField, "attempt_no=%d，必须 ≥ 1", m.AttemptNo)
	}
	if err := checkRefs("input_refs", m.InputRefs, -1); err != nil {
		return err
	}
	if m.Resume != nil {
		if err := m.Resume.validate(); err != nil {
			return err
		}
	}
	if m.Directive != nil {
		if err := m.Directive.validate(); err != nil {
			return err
		}
	}
	if m.Carryover != nil {
		return m.Carryover.validate()
	}
	return nil
}

func (d *Directive) validate() error {
	if err := oneOf("directive.kind", d.Kind, DirectiveFinishNow, DirectiveAnswer); err != nil {
		return err
	}
	if d.Kind != DirectiveAnswer {
		return nil
	}
	if err := required("directive.question_id", d.QuestionID); err != nil {
		return err
	}
	if !hasJSONValue(d.Answers) {
		return newError(CodeMissingField, "directive.answers 不能为空")
	}
	var answers []json.RawMessage
	if err := json.Unmarshal(d.Answers, &answers); err != nil || len(answers) == 0 {
		return newError(CodeInvalidField, "directive.answers 必须是非空数组")
	}
	return nil
}

func (c *Carryover) validate() error {
	if err := firstErr(required("carryover.task_id", c.TaskID), required("carryover.checkpoint_ref", c.CheckpointRef)); err != nil {
		return err
	}
	if !validSHA256(c.CheckpointRef) {
		return newError(CodeInvalidField, "carryover.checkpoint_ref 不是合法的 sha256")
	}
	return nil
}

func (m *TaskOutcome) validate() error {
	return firstErr(
		checkVersion(m.V),
		required("attempt_id", m.AttemptID),
		oneOf("verdict", m.Verdict, VerdictSucceeded, VerdictFailed, VerdictCancelled, VerdictPaused),
	)
}

func (m *Quiesce) validate() error      { return checkGrace(m.V, m.GraceMS) }
func (m *SessionClose) validate() error { return checkGrace(m.V, m.GraceMS) }

func checkGrace(v, graceMS int64) error {
	if err := checkVersion(v); err != nil {
		return err
	}
	if graceMS < 0 {
		return newError(CodeInvalidField, "grace_ms=%d，必须 ≥ 0", graceMS)
	}
	return nil
}

func (m *TaskAccepted) validate() error     { return checkAttemptEvent(m.EventHeader) }
func (m *TaskOutcomeQuery) validate() error { return checkAttemptEvent(m.EventHeader) }
func (m *TaskReleased) validate() error     { return checkAttemptEvent(m.EventHeader) }
func (m *Quiesced) validate() error         { return checkEvent(m.EventHeader) }
func (m *Closed) validate() error           { return checkEvent(m.EventHeader) }

func (m *AwaitingInput) validate() error {
	return firstErr(checkAttemptEvent(m.EventHeader), required("checkpoint_id", m.CheckpointID), required("question_id", m.QuestionID))
}

func checkAttemptEvent(h EventHeader) error {
	return firstErr(checkEvent(h), required("attempt_id", h.AttemptID))
}

// ---- 事件流 ----

// SessionStream 的阶段。
const (
	PhaseAwaitingInit = "awaiting_init" // 宿主尚未发送 init
	PhaseReady        = "ready"         // 已发送 init，等待 ready
	PhaseIdle         = "idle"          // 无 attempt，可接受 task_start / quiesce
	PhaseStarting     = "starting"      // 已发送 task_start，等待 task_accepted
	PhaseActive       = "active"        // attempt 运行中
	PhaseProposed     = "proposed"      // 已收到终态提议或宿主已发送 task_outcome，等待 task_released
	PhaseQuiescing    = "quiescing"     // 已发送 quiesce，等待 quiesced
	PhaseQuiesced     = "quiesced"      // 已静止；宿主可再发 task_start 或 session_close
	PhaseClosing      = "closing"       // 已发送 session_close，等待 closed
	PhaseClosed       = "closed"        // incarnation 结束（closed、启动失败或 handshake_error）
)

// SessionStream 是 session 模式的事件流状态机（宿主侧使用）：宿主发出的消息经 HostSent
// 推进阶段，Worker 事件经 Accept 检查 seq（每 incarnation 从 1 严格递增）、attempt_id 与阶段。
// 返回错误后流即视为违规，不应继续使用。
type SessionStream struct {
	phase       string
	current     string // 当前 attempt_id；idle、quiescing、quiesced、closing、closed 时为空
	outcomeSent bool   // 宿主已对当前 attempt 发送 task_outcome
	lastSeq     int64
	handshake   bool          // 以 handshake_error 结束
	subruns     subrunTracker // sub-run 扩展：协商结果来自 init，已启动的 ID 按 attempt 清空
}

// NewSessionStream 返回处于 awaiting_init 阶段的状态机。
func NewSessionStream() *SessionStream { return &SessionStream{phase: PhaseAwaitingInit} }

// Phase 返回当前阶段。
func (s *SessionStream) Phase() string { return s.phase }

// Current 返回当前 attempt_id（无则为空）。
func (s *SessionStream) Current() string { return s.current }

// HostSent 记录宿主已发出的一条消息（应在写出成功后调用）。init、task_start、task_outcome、
// quiesce、session_close 推进阶段；其余检查 attempt_id 是否为当前 attempt。
func (s *SessionStream) HostSent(m Message) error {
	if s.phase == PhaseAwaitingInit {
		in, ok := m.(*Init)
		if !ok {
			return newError(CodeUnexpectedEvent, "init 之前发送了 %s", m.MessageType())
		}
		if in.Mode != ModeSession {
			return newError(CodeInvalidField, "session 流的 init.mode=%q", in.Mode)
		}
		s.subruns.negotiated = requestsSubruns(in.Extensions)
		s.phase = PhaseReady
		return nil
	}
	if s.phase == PhaseClosing || s.phase == PhaseClosed {
		return newError(CodeAfterTerminal, "session_close 或 incarnation 结束之后发送了 %s", m.MessageType())
	}
	switch m := m.(type) {
	case *Init:
		return newError(CodeUnexpectedEvent, "重复的 init")
	case *TaskStart:
		if s.phase != PhaseIdle && s.phase != PhaseQuiesced {
			return newError(CodeNotIdle, "%s 阶段不能发送 task_start", s.phase)
		}
		s.phase, s.current, s.outcomeSent = PhaseStarting, m.AttemptID, false
		s.subruns.known = nil
	case *Quiesce:
		if s.phase != PhaseIdle {
			return newError(CodeNotIdle, "%s 阶段不能发送 quiesce", s.phase)
		}
		s.phase = PhaseQuiescing
	case *SessionClose:
		if s.phase == PhaseReady {
			return newError(CodeUnexpectedEvent, "ready 之前发送了 session_close")
		}
		s.phase, s.current = PhaseClosing, ""
	case *TaskOutcome:
		if err := s.checkCurrent(m.AttemptID); err != nil {
			return err
		}
		s.phase, s.outcomeSent = PhaseProposed, true
	case *Cancel:
		return s.checkCurrent(m.AttemptID)
	case *Pause:
		return s.checkCurrent(m.AttemptID)
	case *CheckpointResult:
		return s.checkCurrent(m.AttemptID)
	case *ArtifactResult:
		return s.checkCurrent(m.AttemptID)
	case *SubrunStarted, *SubrunCancelRequested:
		// 不带 attempt_id：针对当前 attempt 中已 subrun_start 的 sub-run
		if s.current == "" {
			return newError(CodeWrongAttempt, "%s 阶段没有当前 attempt，不能发送 %s", s.phase, m.MessageType())
		}
		return s.subruns.hostSent(m)
	default:
		return newError(CodeUnknownType, "%s 不是 session 模式的宿主消息", m.MessageType())
	}
	return nil
}

// Accept 检查下一条 Worker 事件。
func (s *SessionStream) Accept(m Message) error {
	if s.handshake {
		return newError(CodeAfterHandshakeError, "handshake_error 之后出现 %s", m.MessageType())
	}
	if m.MessageType() == TypeHandshakeError {
		if s.phase != PhaseReady || s.lastSeq != 0 {
			return newError(CodeHandshakeMisplaced, "handshake_error 必须是第一条消息")
		}
		s.phase, s.handshake = PhaseClosed, true
		return nil
	}
	ev, ok := m.(event)
	if !ok {
		return newError(CodeUnknownType, "%s 不是 Worker 事件", m.MessageType())
	}
	if seq := ev.header().Seq; seq != s.lastSeq+1 {
		return newError(CodeSeqInvalid, "seq=%d，期望 %d", seq, s.lastSeq+1)
	}
	s.lastSeq++
	return s.advance(m, ev.header().AttemptID)
}

func (s *SessionStream) advance(m Message, attemptID string) error {
	typ := m.MessageType()
	switch s.phase {
	case PhaseAwaitingInit:
		return newError(CodeBeforeReady, "init 之前出现 %s", typ)
	case PhaseReady:
		switch r := m.(type) {
		case *Ready:
			if err := r.validateSessionExt(); err != nil {
				return err
			}
			if err := s.subruns.checkReady(r); err != nil {
				return err
			}
			s.phase = PhaseIdle
		case *ErrorEvent: // 启动失败
			s.phase = PhaseClosed
		default:
			return newError(CodeBeforeReady, "%s 出现在 ready 之前", typ)
		}
		return nil
	case PhaseClosing:
		if typ != TypeClosed {
			return newError(CodeAfterTerminal, "session_close 之后出现 %s", typ)
		}
		s.phase = PhaseClosed
		return nil
	case PhaseClosed:
		return newError(CodeAfterTerminal, "incarnation 结束之后出现 %s", typ)
	}

	switch typ {
	case TypeReady:
		return newError(CodeDuplicateReady, "重复的 ready")
	case TypeClosed:
		return newError(CodeUnexpectedEvent, "未收到 session_close 却出现 closed")
	case TypeQuiesced:
		if s.phase != PhaseQuiescing {
			return newError(CodeNotIdle, "%s 阶段出现 quiesced", s.phase)
		}
		s.phase = PhaseQuiesced
		return nil
	}
	if s.phase == PhaseQuiescing && typ != TypeQuiesced {
		// quiesce 之后只允许 quiesced；此时没有当前 attempt。
		return newError(CodeWrongAttempt, "quiesce 之后出现 %s(attempt_id=%q)", typ, attemptID)
	}

	// 其余事件都属于某个 attempt。
	if err := required("attempt_id", attemptID); err != nil {
		return err
	}
	if err := s.checkCurrent(attemptID); err != nil {
		return err
	}
	switch s.phase {
	case PhaseStarting:
		if typ != TypeTaskAccepted {
			return newError(CodeUnexpectedEvent, "task_accepted 之前出现 %s", typ)
		}
		s.phase = PhaseActive
	case PhaseActive:
		if err := s.subruns.observe(m); err != nil {
			return err
		}
		switch {
		case isSessionTerminal(typ):
			s.phase = PhaseProposed
		case typ == TypeTaskAccepted || typ == TypeTaskOutcomeQuery || typ == TypeTaskReleased:
			return newError(CodeUnexpectedEvent, "终态提议之前出现 %s", typ)
		}
	case PhaseProposed:
		switch typ {
		case TypeCheckpointQuery, TypeTaskOutcomeQuery:
		case TypeTaskReleased:
			if !s.outcomeSent {
				return newError(CodeUnexpectedEvent, "宿主尚未发送 task_outcome 即出现 task_released")
			}
			s.phase, s.current, s.outcomeSent = PhaseIdle, "", false
		default:
			return newError(CodeAfterTerminal, "终态提议或裁决之后出现 %s", typ)
		}
	}
	return nil
}

// checkCurrent 检查 attempt_id 是否为当前 attempt。
func (s *SessionStream) checkCurrent(attemptID string) error {
	if s.current == "" || attemptID != s.current {
		return newError(CodeWrongAttempt, "attempt_id=%q，当前 attempt 为 %q（阶段 %s）", attemptID, s.current, s.phase)
	}
	return nil
}

func isSessionTerminal(typ string) bool {
	return isTerminal(typ) || typ == TypeAwaitingInput
}
