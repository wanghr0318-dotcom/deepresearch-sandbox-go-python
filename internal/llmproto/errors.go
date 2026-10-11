package llmproto

import (
	"errors"
	"io"
	"net/http"
)

// UnsupportedFieldError is a field that cannot be decoded, or cannot be converted to the target format
// (§7.4). It maps to 400 unsupported_field.
type UnsupportedFieldError struct {
	Field  string // JSON path, e.g. "messages[1].reasoning_content"
	Reason string
}

func (e *UnsupportedFieldError) Error() string {
	if e.Reason == "" {
		return "unsupported field " + e.Field
	}
	return "unsupported field " + e.Field + ": " + e.Reason
}

// InvalidRequestError is a malformed request or response. It maps to 400 invalid_request_error.
type InvalidRequestError struct {
	Field   string // JSON path; may be empty
	Message string
}

func (e *InvalidRequestError) Error() string {
	if e.Field == "" {
		return "invalid request: " + e.Message
	}
	return "invalid request: " + e.Field + ": " + e.Message
}

// BadResponseError is a provider response (or stream) that cannot be decoded or converted. It is the
// provider's fault, not the client's: it maps to 502 upstream_error (Anthropic api_error).
type BadResponseError struct {
	Field   string // JSON path; may be empty
	Message string
}

func (e *BadResponseError) Error() string {
	if e.Field == "" {
		return "bad provider response: " + e.Message
	}
	return "bad provider response: " + e.Field + ": " + e.Message
}

func badResponse(field, msg string) error { return &BadResponseError{Field: field, Message: msg} }

func invalidf(field, msg string) error { return &InvalidRequestError{Field: field, Message: msg} }

func unsupported(field, reason string) error {
	return &UnsupportedFieldError{Field: field, Reason: reason}
}

// ErrorClass is a situation of §7.5; each has a status and an error shape in both formats.
type ErrorClass string

// Error classes (§7.5). ClassModelNotFound and ClassInternal extend the table for unknown models and
// server faults.
const (
	ClassInvalidAPIKey     ErrorClass = "invalid_api_key"
	ClassPermissionDenied  ErrorClass = "permission_denied"
	ClassInvalidRequest    ErrorClass = "invalid_request"
	ClassUnsupportedField  ErrorClass = "unsupported_field"
	ClassModelNotFound     ErrorClass = "model_not_found"
	ClassRateLimit         ErrorClass = "rate_limit_exceeded"
	ClassInsufficientQuota ErrorClass = "insufficient_quota"
	ClassModelDegraded     ErrorClass = "model_degraded"
	ClassUpstream          ErrorClass = "upstream_error"
	ClassInternal          ErrorClass = "internal_error"
)

type classInfo struct {
	status        int
	openaiType    string
	openaiCode    string
	anthropicType string
}

var classes = map[ErrorClass]classInfo{
	ClassInvalidAPIKey:     {http.StatusUnauthorized, "invalid_request_error", "invalid_api_key", "authentication_error"},
	ClassPermissionDenied:  {http.StatusForbidden, "invalid_request_error", "permission_denied", "permission_error"},
	ClassInvalidRequest:    {http.StatusBadRequest, "invalid_request_error", "invalid_request_error", "invalid_request_error"},
	ClassUnsupportedField:  {http.StatusBadRequest, "invalid_request_error", "unsupported_field", "invalid_request_error"},
	ClassModelNotFound:     {http.StatusNotFound, "invalid_request_error", "model_not_found", "not_found_error"},
	ClassRateLimit:         {http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded", "rate_limit_error"},
	ClassInsufficientQuota: {http.StatusTooManyRequests, "insufficient_quota", "insufficient_quota", "rate_limit_error"},
	ClassModelDegraded:     {http.StatusServiceUnavailable, "server_error", "model_degraded", "overloaded_error"},
	ClassUpstream:          {http.StatusBadGateway, "server_error", "upstream_error", "api_error"},
	ClassInternal:          {http.StatusInternalServerError, "server_error", "internal_error", "api_error"},
}

// Status is the HTTP status of the class (500 for an unknown class).
func (c ErrorClass) Status() int {
	if i, ok := classes[c]; ok {
		return i.status
	}
	return http.StatusInternalServerError
}

func (c ErrorClass) info() classInfo {
	if i, ok := classes[c]; ok {
		return i
	}
	return classes[ClassInternal]
}

// ClassOf returns the class of a codec error: ClassUnsupportedField for *UnsupportedFieldError,
// ClassInvalidRequest for *InvalidRequestError, ClassUpstream for the provider's faults (*BadResponseError,
// ErrBadStream, a cut stream io.ErrUnexpectedEOF, ErrSSETooLarge), ClassInternal otherwise.
func ClassOf(err error) ErrorClass {
	var u *UnsupportedFieldError
	var i *InvalidRequestError
	var b *BadResponseError
	switch {
	case errors.As(err, &u):
		return ClassUnsupportedField
	case errors.As(err, &i):
		return ClassInvalidRequest
	case errors.As(err, &b), errors.Is(err, ErrBadStream), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, ErrSSETooLarge):
		return ClassUpstream
	}
	return ClassInternal
}

// paramOf is the field an error names (OpenAI "param"), or "".
func paramOf(err error) string {
	var u *UnsupportedFieldError
	var i *InvalidRequestError
	switch {
	case errors.As(err, &u):
		return u.Field
	case errors.As(err, &i):
		return i.Field
	}
	return ""
}

// ErrorInfo is an error in the neutral format (an error stream event, or an error to write to a client).
type ErrorInfo struct {
	Class   ErrorClass
	Message string
	Param   string // the field concerned (OpenAI "param"); may be empty
	// ProviderType is the error type or code the provider sent, when the error came from a provider stream.
	ProviderType string
}

// ErrorFrom builds the ErrorInfo of a codec error (class from ClassOf, the field as Param).
func ErrorFrom(err error) ErrorInfo {
	return ErrorInfo{Class: ClassOf(err), Message: err.Error(), Param: paramOf(err)}
}

type openAIErrorBody struct {
	Error openAIErrorObject `json:"error"`
}

type openAIErrorObject struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    string  `json:"code"`
}

type anthropicErrorBody struct {
	Type  string               `json:"type"`
	Error anthropicErrorObject `json:"error"`
}

type anthropicErrorObject struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// EncodeOpenAIError is the OpenAI error object {"error":{"message","type","param","code"}} of e.
func EncodeOpenAIError(e ErrorInfo) []byte {
	i := e.Class.info()
	var param *string
	if e.Param != "" {
		param = &e.Param
	}
	return mustMarshalNoEscape(openAIErrorBody{Error: openAIErrorObject{Message: e.Message, Type: i.openaiType, Param: param, Code: i.openaiCode}})
}

// EncodeAnthropicError is the Anthropic error object {"type":"error","error":{"type","message"}} of e.
func EncodeAnthropicError(e ErrorInfo) []byte {
	return mustMarshalNoEscape(anthropicErrorBody{Type: "error", Error: anthropicErrorObject{Type: e.Class.info().anthropicType, Message: e.Message}})
}

// EncodeError returns the status and body of e in format p.
func EncodeError(p Protocol, e ErrorInfo) (int, []byte) {
	if p == ProtocolAnthropic {
		return e.Class.Status(), EncodeAnthropicError(e)
	}
	return e.Class.Status(), EncodeOpenAIError(e)
}
