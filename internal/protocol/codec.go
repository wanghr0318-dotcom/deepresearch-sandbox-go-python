package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"unicode/utf8"
)

// Direction 是消息的传输方向。
type Direction int

const (
	// HostToWorker 是宿主经 stdin 发给 Worker 的控制消息。
	HostToWorker Direction = iota + 1
	// WorkerToHost 是 Worker 经 stdout 发给宿主的事件。
	WorkerToHost
)

// messageRegistry 按方向把消息类型映射到其构造函数。
type messageRegistry map[Direction]map[string]func() Message

var registry = messageRegistry{
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

// DecodeLine 解析并校验一行 task 模式消息（不含行尾换行符）。
// 判定顺序：行长 → UTF-8 → JSON 结构限制 → 消息类型 → 类型上限 → 版本 → 键名大小写 → 字段类型 → 语义规则。
// 未知字段忽略；违规均返回 *Error。session 模式使用 DecodeSessionLine。
func DecodeLine(dir Direction, line []byte) (Message, error) {
	return decodeLine(registry, dir, line)
}

func decodeLine(reg messageRegistry, dir Direction, line []byte) (Message, error) {
	if len(line) > maxLineBytes {
		return nil, newError(CodeMessageTooLarge, "%d 字节，上限 %d", len(line), maxLineBytes)
	}
	if !utf8.Valid(line) {
		return nil, newError(CodeMalformedJSON, "行不是合法的 UTF-8")
	}
	if t := bytes.TrimSpace(line); len(t) == 0 || t[0] != '{' {
		return nil, newError(CodeMalformedJSON, "消息必须是 JSON 对象")
	}
	if err := checkSyntax(line); err != nil {
		return nil, err
	}
	var top map[string]json.RawMessage // 键名精确匹配；重复键已被拒绝
	if err := json.Unmarshal(line, &top); err != nil {
		return nil, newError(CodeMalformedJSON, "%v", err)
	}
	typ, err := messageType(top)
	if err != nil {
		return nil, err
	}
	newMsg, ok := reg[dir][typ]
	if !ok {
		return nil, newError(CodeUnknownType, "%q", typ)
	}
	if err := checkSize(dir, typ, len(line)); err != nil {
		return nil, err
	}
	m := newMsg()
	t := reflect.TypeOf(m).Elem()
	if _, versioned := structFields(t)["v"]; versioned {
		if err := checkVersionField(top["v"]); err != nil {
			return nil, err
		}
	}
	if err := checkKeyCase(line, t); err != nil {
		return nil, err
	}
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

// EncodeLine 校验并编码一条 task 模式消息（不含行尾换行符）。m 的 type 字段必须与其类型一致。
// session 模式使用 EncodeSessionLine。
func EncodeLine(dir Direction, m Message) ([]byte, error) {
	return encodeLine(registry, dir, m, false)
}

func encodeLine(reg messageRegistry, dir Direction, m Message, session bool) ([]byte, error) {
	if _, ok := reg[dir][m.MessageType()]; !ok {
		return nil, newError(CodeUnknownType, "%s 不属于该方向", m.MessageType())
	}
	if m.declaredType() != m.MessageType() {
		return nil, newError(CodeInvalidField, "type=%q，期望 %q", m.declaredType(), m.MessageType())
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	if session {
		if err := validateSessionMode(m); err != nil {
			return nil, err
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if err := checkSyntax(b); err != nil { // 编码结果必须能通过对端的 JSON 结构限制
		return nil, newError(CodeInvalidField, "消息违反 JSON 结构限制：%v", err)
	}
	if err := checkSize(dir, m.MessageType(), len(b)); err != nil {
		return nil, err
	}
	return b, nil
}

// messageType 读取精确键名 type 的值；缺失或不是字符串为 unknown_type。
func messageType(top map[string]json.RawMessage) (string, error) {
	raw, ok := top["type"]
	if !ok {
		return "", newError(CodeUnknownType, "缺少 type")
	}
	var typ string
	if err := json.Unmarshal(raw, &typ); err != nil {
		return "", newError(CodeUnknownType, "type 不是字符串")
	}
	return typ, nil
}

// checkVersionField 在字段校验之前检查 v：必须是整数形式的 1，否则为 version_mismatch。
func checkVersionField(raw json.RawMessage) error {
	var v int64
	if raw == nil || json.Unmarshal(raw, &v) != nil {
		return newError(CodeVersionMismatch, "v 缺失或不是整数")
	}
	return checkVersion(v)
}

// checkSize 按方向与类型检查上限：Worker 事件 MaxEventBytes，init 与 task_start
// （携带 config 与 resume.state）MaxInitBytes，其他宿主控制消息 MaxControlBytes。
func checkSize(dir Direction, typ string, n int) error {
	limit := MaxEventBytes
	switch {
	case dir == HostToWorker && (typ == TypeInit || typ == TypeTaskStart):
		limit = MaxInitBytes
	case dir == HostToWorker:
		limit = MaxControlBytes
	}
	if n > limit {
		return newError(CodeMessageTooLarge, "%s %d 字节，上限 %d", typ, n, limit)
	}
	return nil
}
