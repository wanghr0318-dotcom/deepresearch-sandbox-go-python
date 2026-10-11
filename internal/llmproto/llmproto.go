// Package llmproto is the neutral chat format of the model gateway and its OpenAI and Anthropic codecs
// (model gateway design §7.2–§7.5). It depends on the standard library only.
//
// A request in either wire format decodes to a Request; a Request encodes to either wire format. The same
// holds for non-stream responses (Response) and for streams (Event sequences): stream decoders turn a
// provider's SSE events into Events, stream encoders turn Events into the client's SSE events.
//
// Fields that only one format has are recorded on decode as Features. When a request is encoded to the
// other format, droppable features (cache_control, metadata, sampling hints) are left out and reported by
// EncodeRequest; features that cannot be expressed (thinking, reasoning_effort, images, json_schema
// response_format …) make encoding fail with an *UnsupportedFieldError naming the field. FormatOnly and
// OnlyProtocol tell the router which requests must stay on providers of their own format.
//
// Usage counts are disjoint: InputTokens excludes cached tokens (Anthropic semantics). OpenAI's
// prompt_tokens, which includes cached tokens, is Usage.PromptTotal().
package llmproto

import (
	"encoding/json"
)

// Protocol is a wire format.
type Protocol string

// Wire formats.
const (
	ProtocolOpenAI    Protocol = "openai"
	ProtocolAnthropic Protocol = "anthropic"
)

// Role is the author of a message. System prompts live in Request.System; RoleSystem appears only for an
// OpenAI system message that is not at the start of the conversation (an OpenAI-only feature).
type Role string

// Roles.
const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleSystem    Role = "system"
)

// BlockType is the type of a content block.
type BlockType string

// Block types.
const (
	BlockText       BlockType = "text"
	BlockToolCall   BlockType = "tool_call"
	BlockToolResult BlockType = "tool_result"
	BlockThinking   BlockType = "thinking"
	// BlockOpaque is a block of the source format that the neutral format does not model (images,
	// documents, redacted thinking, audio …). Raw holds it verbatim; it is re-emitted only in its own format.
	BlockOpaque BlockType = "opaque"
)

// Block is one typed piece of a message.
type Block struct {
	Type BlockType

	// Text is the text of a text block, the reasoning of a thinking block, or the content of a tool result.
	Text string

	// ToolCall: ID, Name and Arguments (a JSON document, normally an object).
	// ToolResult: ID is the call it answers; IsError marks a failed call (Anthropic only).
	ID        string
	Name      string
	Arguments string
	IsError   bool

	// Signature is the signature of an Anthropic thinking block.
	Signature string

	// CacheControl is Anthropic's cache_control on this block (dropped when converting to OpenAI).
	CacheControl json.RawMessage

	// Raw is an opaque block verbatim.
	Raw json.RawMessage
}

// Message is one turn of the conversation.
type Message struct {
	Role   Role
	Blocks []Block
	// Name is OpenAI's per-message name (dropped when converting to Anthropic).
	Name string
}

// Tool is a function the model may call.
type Tool struct {
	Name        string
	Description string
	Parameters  json.RawMessage // JSON Schema of the arguments
	// Strict is OpenAI's function.strict (dropped when converting).
	Strict *bool
	// CacheControl is Anthropic's cache_control on the tool (dropped when converting).
	CacheControl json.RawMessage
	// Raw is a tool of a type the neutral format does not model (Anthropic server tools, OpenAI custom tools),
	// re-emitted only in its own format.
	Raw json.RawMessage
}

// ToolChoiceMode says whether and which tool the model must call.
type ToolChoiceMode string

// Tool choice modes.
const (
	ToolChoiceAuto     ToolChoiceMode = "auto"
	ToolChoiceNone     ToolChoiceMode = "none"
	ToolChoiceRequired ToolChoiceMode = "required" // Anthropic "any"
	ToolChoiceTool     ToolChoiceMode = "tool"     // a named tool
)

// ToolChoice is the tool choice of a request.
type ToolChoice struct {
	Mode ToolChoiceMode
	Name string // for ToolChoiceTool
}

// Request is a chat request in the neutral format.
type Request struct {
	// Source is the format the request was decoded from; empty for requests built in code.
	Source Protocol

	Model string
	// System is the system prompt as text blocks (OpenAI: the leading system messages, one block each;
	// Anthropic: the system string as one block, or the system array).
	System   []Block
	Messages []Message

	Tools      []Tool
	ToolChoice *ToolChoice
	// ParallelToolCalls is OpenAI's parallel_tool_calls; false ⇄ Anthropic disable_parallel_tool_use.
	ParallelToolCalls *bool

	MaxTokens   int64 // 0 = not given
	Temperature *float64
	TopP        *float64
	Stop        []string

	Stream bool
	// IncludeUsage is OpenAI's stream_options.include_usage (Anthropic streams always carry usage).
	IncludeUsage bool

	// Extras are top-level fields of the source format that have no neutral equivalent (seed, top_k,
	// metadata, reasoning_effort, thinking …), verbatim. They are re-emitted only in the source format.
	Extras map[string]json.RawMessage

	// Features lists the format-only features this request uses, recorded on decode (see FormatOnly).
	Features []Feature
}

// FeatureKind says what happens to a format-only feature when converting to the other format.
type FeatureKind string

// Feature kinds.
const (
	FeatureDrop        FeatureKind = "drop"        // left out, reported in Encoded.Dropped
	FeatureUnsupported FeatureKind = "unsupported" // conversion fails with *UnsupportedFieldError
)

// Feature is one use of a field that only the request's source format has.
type Feature struct {
	Field string // JSON path in the source request, e.g. "messages[2].content[0].cache_control"
	Kind  FeatureKind
}

// StopReason is why the model stopped.
type StopReason string

// Stop reasons.
const (
	StopEnd       StopReason = "end"
	StopMaxTokens StopReason = "max_tokens"
	StopToolUse   StopReason = "tool_use"
	StopSequence  StopReason = "stop_sequence"
	StopRefusal   StopReason = "refusal" // Anthropic refusal ⇄ OpenAI content_filter
	StopError     StopReason = "error"
)

// Usage is the token usage of one model call. The counts are disjoint: InputTokens does not include the
// cached tokens.
type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
}

// PromptTotal is all input tokens, cached or not (OpenAI's prompt_tokens).
func (u Usage) PromptTotal() int64 { return u.InputTokens + u.CacheReadTokens + u.CacheWriteTokens }

// Response is a non-stream chat response in the neutral format.
type Response struct {
	Source       Protocol
	ID           string
	Model        string
	Created      int64   // unix seconds (OpenAI "created"); 0 when unknown
	Blocks       []Block // assistant content: thinking, text and tool_call blocks
	StopReason   StopReason
	StopSequence string // the stop sequence that matched (Anthropic only)
	Usage        Usage
	HasUsage     bool // the response carried usage
}

// FormatOnly returns the fields of the request that cannot be converted to the other format. A request
// with any of them can only be routed to providers of its source format.
func (r Request) FormatOnly() []string {
	var out []string
	for _, f := range r.Features {
		if f.Kind == FeatureUnsupported {
			out = append(out, f.Field)
		}
	}
	return out
}

// OnlyProtocol reports whether the request must stay on providers of its source format, and which.
func (r Request) OnlyProtocol() (Protocol, bool) {
	if r.Source != "" && len(r.FormatOnly()) > 0 {
		return r.Source, true
	}
	return "", false
}

// Dropped returns the fields that are left out when the request is encoded to target.
func (r Request) Dropped(target Protocol) []string {
	if r.Source == "" || r.Source == target {
		return nil
	}
	var out []string
	for _, f := range r.Features {
		if f.Kind == FeatureDrop {
			out = append(out, f.Field)
		}
	}
	return out
}

// CanConvertTo returns nil when the request can be encoded to target, or an *UnsupportedFieldError naming
// the first field that cannot.
func (r Request) CanConvertTo(target Protocol) error {
	if r.Source == "" || r.Source == target {
		return nil
	}
	if fs := r.FormatOnly(); len(fs) > 0 {
		return &UnsupportedFieldError{Field: fs[0], Reason: "has no equivalent in the " + string(target) + " format"}
	}
	return nil
}

// Encoded is an encoded request and the fields dropped on the way.
type Encoded struct {
	Body    []byte
	Dropped []string
}

// EncodeRequest encodes req in the target format. When req came from the other format, droppable
// format-only fields are left out (listed in Dropped) and a field that cannot be converted fails with an
// *UnsupportedFieldError.
func EncodeRequest(req Request, target Protocol) (Encoded, error) {
	if err := req.CanConvertTo(target); err != nil {
		return Encoded{}, err
	}
	var (
		body []byte
		err  error
	)
	switch target {
	case ProtocolOpenAI:
		body, err = encodeOpenAIRequest(req)
	case ProtocolAnthropic:
		body, err = encodeAnthropicRequest(req)
	default:
		return Encoded{}, &InvalidRequestError{Field: "protocol", Message: "unknown protocol " + string(target)}
	}
	if err != nil {
		return Encoded{}, err
	}
	return Encoded{Body: body, Dropped: req.Dropped(target)}, nil
}

// DecodeRequest decodes a request in the given format.
func DecodeRequest(p Protocol, body []byte) (Request, error) {
	switch p {
	case ProtocolOpenAI:
		return DecodeOpenAIRequest(body)
	case ProtocolAnthropic:
		return DecodeAnthropicRequest(body)
	}
	return Request{}, &InvalidRequestError{Field: "protocol", Message: "unknown protocol " + string(p)}
}

// DecodeResponse decodes a non-stream response in the given format.
func DecodeResponse(p Protocol, body []byte) (Response, error) {
	switch p {
	case ProtocolOpenAI:
		return DecodeOpenAIResponse(body)
	case ProtocolAnthropic:
		return DecodeAnthropicResponse(body)
	}
	return Response{}, &InvalidRequestError{Field: "protocol", Message: "unknown protocol " + string(p)}
}

// EncodeResponse encodes a non-stream response in the given format.
func EncodeResponse(p Protocol, resp Response) ([]byte, error) {
	switch p {
	case ProtocolOpenAI:
		return EncodeOpenAIResponse(resp)
	case ProtocolAnthropic:
		return EncodeAnthropicResponse(resp)
	}
	return nil, &InvalidRequestError{Field: "protocol", Message: "unknown protocol " + string(p)}
}
