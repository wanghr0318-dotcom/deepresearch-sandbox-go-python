package protocol

import (
	"errors"
	"fmt"
)

// 消息级错误码。
const (
	CodeMalformedJSON   = "malformed_json"
	CodeUnknownType     = "unknown_type"
	CodeVersionMismatch = "version_mismatch"
	CodeMissingField    = "missing_field"
	CodeInvalidField    = "invalid_field"
	CodeMessageTooLarge = "message_too_large"
	CodeStateTooLarge   = "state_too_large"
	CodeTooManyRefs     = "too_many_refs"
	CodePathInvalid     = "path_invalid"
)

// Error 是带稳定错误码的协议错误。
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

func newError(code, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf 返回 err 的协议错误码；err 不是 *Error 时返回空串。
func CodeOf(err error) string {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}
