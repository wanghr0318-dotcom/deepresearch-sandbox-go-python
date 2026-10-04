package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Direction 是消息的传输方向。
type Direction int

const (
	// HostToWorker 是宿主经 stdin 发给 Worker 的控制消息。
	HostToWorker Direction = iota + 1
	// WorkerToHost 是 Worker 经 stdout 发给宿主的事件。
	WorkerToHost
)

var registry = map[Direction]map[string]func() Message{
	HostToWorker: {
		TypeInit:             func() Message { return &Init{} },
		TypeCheckpointResult: func() Message { return &CheckpointResult{} },
		TypeArtifactResult:   func() Message { return &ArtifactResult{} },
		TypeCancel:           func() Message { return &Cancel{} },
		TypePause:            func() Message { return &Pause{} },
	},
	WorkerToHost: {
		TypeReady:           func() Message { return &Ready{} },
		TypeProgress:        func() Message { return &Progress{} },
		TypeArtifact:        func() Message { return &Artifact{} },
		TypeCheckpoint:      func() Message { return &Checkpoint{} },
		TypeCheckpointQuery: func() Message { return &CheckpointQuery{} },
		TypePaused:          func() Message { return &Paused{} },
		TypeResult:          func() Message { return &Result{} },
		TypeError:           func() Message { return &ErrorEvent{} },
		TypeHandshakeError:  func() Message { return &HandshakeError{} },
	},
}

// DecodeLine 解析并校验一行消息（不含行尾换行符）。
// 未知字段忽略；未知类型、超限、字段类型错误与语义违规均返回 *Error。
func DecodeLine(dir Direction, line []byte) (Message, error) {
	if len(line) > MaxEventBytes {
		return nil, newError(CodeMessageTooLarge, "%d 字节，上限 %d", len(line), MaxEventBytes)
	}
	if t := bytes.TrimSpace(line); len(t) == 0 || t[0] != '{' {
		return nil, newError(CodeMalformedJSON, "消息必须是 JSON 对象")
	}
	var peek struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &peek); err != nil {
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) && te.Field == "type" {
			return nil, newError(CodeUnknownType, "type 不是字符串")
		}
		return nil, newError(CodeMalformedJSON, "%v", err)
	}
	newMsg, ok := registry[dir][peek.Type]
	if !ok {
		return nil, newError(CodeUnknownType, "%q", peek.Type)
	}
	if err := checkSize(dir, peek.Type, len(line)); err != nil {
		return nil, err
	}
	m := newMsg()
	if err := json.Unmarshal(line, m); err != nil {
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) {
			return nil, newError(CodeInvalidField, "%s: %v", te.Field, err)
		}
		return nil, newError(CodeMalformedJSON, "%v", err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// EncodeLine 校验并编码一条消息（不含行尾换行符）。m 的 type 字段必须与其类型一致。
func EncodeLine(dir Direction, m Message) ([]byte, error) {
	if _, ok := registry[dir][m.MessageType()]; !ok {
		return nil, newError(CodeUnknownType, "%s 不属于该方向", m.MessageType())
	}
	if m.declaredType() != m.MessageType() {
		return nil, newError(CodeInvalidField, "type=%q，期望 %q", m.declaredType(), m.MessageType())
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if err := checkSize(dir, m.MessageType(), len(b)); err != nil {
		return nil, err
	}
	return b, nil
}

// checkSize 按方向与类型检查上限：宿主控制消息（init 除外）16 KiB，其余 1 MiB。
func checkSize(dir Direction, typ string, n int) error {
	limit := MaxEventBytes
	if dir == HostToWorker && typ != TypeInit {
		limit = MaxControlBytes
	}
	if n > limit {
		return newError(CodeMessageTooLarge, "%s %d 字节，上限 %d", typ, n, limit)
	}
	return nil
}
