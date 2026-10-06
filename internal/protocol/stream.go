package protocol

// 事件流顺序错误码（规格 §5.3）。
const (
	CodeSeqInvalid          = "seq_invalid"
	CodeBeforeReady         = "before_ready"
	CodeDuplicateReady      = "duplicate_ready"
	CodeAfterTerminal       = "after_terminal"
	CodeHandshakeMisplaced  = "handshake_error_misplaced"
	CodeAfterHandshakeError = "after_handshake_error"

	// session 扩展（SessionStream，见 session.go）。
	CodeWrongAttempt      = "wrong_attempt"       // 消息的 attempt_id 不是当前 attempt
	CodeNotIdle           = "not_idle"            // quiesced 不在 quiesce 之后；或宿主在非空闲时发 task_start / quiesce
	CodeSessionExtMissing = "session_ext_missing" // session 模式的 ready 未确认 session_ext: 1
	CodeUnexpectedEvent   = "unexpected_event"    // 消息类型在当前阶段不允许，且不属于上述任何一种
)

type streamPhase int

const (
	phaseAwaitingReady streamPhase = iota
	phaseRunning
	phaseTerminalSent
	phaseHandshakeFailed
)

// WorkerStream 按规格 §5.3 检查 task 模式下 Worker 事件流的顺序：
// seq 从 1 严格递增；ready 之前只允许 error（启动失败）；ready 只能出现一次；
// 终态提议（result、error、paused）至多一个，其后只允许 checkpoint_query；
// handshake_error 只能是第一条且是唯一一条消息。
// Observe 返回错误后，流即视为违规，不应继续使用。
type WorkerStream struct {
	phase   streamPhase
	lastSeq int64
}

// Observe 检查下一条 Worker 消息。
func (s *WorkerStream) Observe(m Message) error {
	if s.phase == phaseHandshakeFailed {
		return newError(CodeAfterHandshakeError, "handshake_error 之后出现 %s", m.MessageType())
	}
	if m.MessageType() == TypeHandshakeError {
		if s.phase != phaseAwaitingReady || s.lastSeq != 0 {
			return newError(CodeHandshakeMisplaced, "handshake_error 必须是第一条消息")
		}
		s.phase = phaseHandshakeFailed
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
	return s.advance(m.MessageType())
}

func (s *WorkerStream) advance(typ string) error {
	switch s.phase {
	case phaseAwaitingReady:
		switch typ {
		case TypeReady:
			s.phase = phaseRunning
		case TypeError:
			s.phase = phaseTerminalSent
		default:
			return newError(CodeBeforeReady, "%s 出现在 ready 之前", typ)
		}
	case phaseRunning:
		if typ == TypeReady {
			return newError(CodeDuplicateReady, "重复的 ready")
		}
		if isTerminal(typ) {
			s.phase = phaseTerminalSent
		}
	case phaseTerminalSent:
		if typ != TypeCheckpointQuery {
			return newError(CodeAfterTerminal, "终态提议之后出现 %s", typ)
		}
	}
	return nil
}

func isTerminal(typ string) bool {
	return typ == TypeResult || typ == TypeError || typ == TypePaused
}
