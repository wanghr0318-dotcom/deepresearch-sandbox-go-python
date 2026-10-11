package llmproto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// Anthropic request encoding and response decoding/encoding; request decoding is in anthropic_request.go.

type anthropicRequestOut struct {
	Model         string                `json:"model,omitempty"`
	MaxTokens     int64                 `json:"max_tokens"`
	System        json.RawMessage       `json:"system,omitempty"`
	Messages      []anthropicMessageOut `json:"messages"`
	Temperature   *float64              `json:"temperature,omitempty"`
	TopP          *float64              `json:"top_p,omitempty"`
	StopSequences []string              `json:"stop_sequences,omitempty"`
	Stream        bool                  `json:"stream,omitempty"`
	Tools         []json.RawMessage     `json:"tools,omitempty"`
	ToolChoice    *anthropicToolChoice  `json:"tool_choice,omitempty"`
}

type anthropicMessageOut struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicToolChoice struct {
	Type                   string `json:"type"`
	Name                   string `json:"name,omitempty"`
	DisableParallelToolUse bool   `json:"disable_parallel_tool_use,omitempty"`
}

type anthropicToolOut struct {
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"input_schema"`
	CacheControl json.RawMessage `json:"cache_control,omitempty"`
}

// EncodeAnthropicRequest encodes req as an Anthropic messages request (see EncodeRequest; the dropped fields
// are not reported).
func EncodeAnthropicRequest(req Request) ([]byte, error) {
	e, err := EncodeRequest(req, ProtocolAnthropic)
	return e.Body, err
}

func encodeAnthropicRequest(req Request) ([]byte, error) {
	if req.MaxTokens <= 0 {
		return nil, invalidf("max_tokens", "is required by the anthropic format")
	}
	out := anthropicRequestOut{Model: req.Model, MaxTokens: req.MaxTokens, Temperature: req.Temperature, TopP: req.TopP,
		StopSequences: req.Stop, Stream: req.Stream}
	if req.Source != ProtocolAnthropic && out.Temperature != nil && out.TopP != nil {
		out.TopP = nil // current Claude models refuse both (recorded as dropped on decode)
	}
	var err error
	if out.System, err = anthropicSystem(req.System); err != nil {
		return nil, err
	}
	if out.Messages, err = anthropicMessages(req.Messages); err != nil {
		return nil, err
	}
	if out.Tools, err = anthropicTools(req.Tools); err != nil {
		return nil, err
	}
	if out.ToolChoice, err = anthropicToolChoiceOf(req); err != nil {
		return nil, err
	}
	body, err := marshalNoEscape(out)
	if err != nil {
		return nil, invalidf("", err.Error())
	}
	if req.Source == ProtocolAnthropic && len(req.Extras) > 0 {
		return mergeExtras(body, req.Extras)
	}
	return body, nil
}

// anthropicSystem encodes the system prompt: a string for one plain block, else an array of text blocks.
func anthropicSystem(system []Block) (json.RawMessage, error) {
	switch {
	case len(system) == 0:
		return nil, nil
	case len(system) == 1 && system[0].CacheControl == nil:
		return mustMarshalNoEscape(system[0].Text), nil
	}
	blocks := make([]json.RawMessage, 0, len(system))
	for _, s := range system {
		b, err := anthropicBlock(s)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return mustMarshalNoEscape(blocks), nil
}

// anthropicMessages encodes the messages; consecutive messages of the same role become one turn (e.g. tool
// results followed by user text).
func anthropicMessages(msgs []Message) ([]anthropicMessageOut, error) {
	var merged []Message
	for _, m := range msgs {
		if m.Role == RoleAssistant && len(m.Blocks) == 0 {
			continue // an empty assistant turn (recorded as dropped on decode); Anthropic rejects empty content
		}
		if m.Role != RoleUser && m.Role != RoleAssistant {
			return nil, unsupported("messages", "role "+string(m.Role)+" has no anthropic equivalent")
		}
		if n := len(merged); n > 0 && merged[n-1].Role == m.Role {
			merged[n-1].Blocks = append(append([]Block(nil), merged[n-1].Blocks...), m.Blocks...)
			continue
		}
		merged = append(merged, m)
	}
	out := make([]anthropicMessageOut, 0, len(merged))
	for _, m := range merged {
		content, err := anthropicContent(m.Blocks)
		if err != nil {
			return nil, err
		}
		out = append(out, anthropicMessageOut{Role: string(m.Role), Content: content})
	}
	return out, nil
}

func anthropicTools(tools []Tool) ([]json.RawMessage, error) {
	var out []json.RawMessage
	for _, t := range tools {
		if t.Raw != nil {
			out = append(out, t.Raw) // only reachable for an Anthropic source (else unsupported)
			continue
		}
		schema := t.Parameters
		if schema == nil {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		b, err := marshalNoEscape(anthropicToolOut{Name: t.Name, Description: t.Description, InputSchema: schema, CacheControl: t.CacheControl})
		if err != nil {
			return nil, invalidf("tools", err.Error())
		}
		out = append(out, b)
	}
	return out, nil
}

// anthropicToolChoiceOf encodes tool_choice; parallel_tool_calls false becomes disable_parallel_tool_use.
func anthropicToolChoiceOf(req Request) (*anthropicToolChoice, error) {
	var out *anthropicToolChoice
	if len(req.Tools) == 0 && req.Source != ProtocolAnthropic {
		return nil, nil // a converted tool_choice without tools is meaningless
	}
	if tc := req.ToolChoice; tc != nil {
		types := map[ToolChoiceMode]string{ToolChoiceAuto: "auto", ToolChoiceNone: "none", ToolChoiceRequired: "any", ToolChoiceTool: "tool"}
		typ, ok := types[tc.Mode]
		if !ok {
			return nil, invalidf("tool_choice", "unknown mode "+string(tc.Mode))
		}
		out = &anthropicToolChoice{Type: typ}
		if tc.Mode == ToolChoiceTool {
			out.Name = tc.Name
		}
	}
	if p := req.ParallelToolCalls; p != nil && !*p && (len(req.Tools) > 0 || req.Source == ProtocolAnthropic) {
		if out == nil {
			out = &anthropicToolChoice{Type: "auto"}
		}
		out.DisableParallelToolUse = out.Type != "none"
	}
	return out, nil
}

// anthropicContent encodes message blocks: a string for one plain text block, else an array of blocks.
// Empty text blocks are left out when the message has other blocks (the Anthropic API rejects them).
func anthropicContent(blocks []Block) (json.RawMessage, error) {
	if len(blocks) == 1 && blocks[0].Type == BlockText && blocks[0].CacheControl == nil {
		return mustMarshalNoEscape(blocks[0].Text), nil
	}
	var kept []Block
	for _, b := range blocks {
		if b.Type == BlockText && b.Text == "" && b.CacheControl == nil {
			continue
		}
		kept = append(kept, b)
	}
	if len(kept) == 0 {
		return mustMarshalNoEscape(""), nil
	}
	out := make([]json.RawMessage, 0, len(kept))
	for _, b := range kept {
		raw, err := anthropicBlock(b)
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return mustMarshalNoEscape(out), nil
}

type anthropicBlockOut struct {
	Type         string          `json:"type"`
	Text         *string         `json:"text,omitempty"`
	ID           string          `json:"id,omitempty"`
	Name         string          `json:"name,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	ToolUseID    string          `json:"tool_use_id,omitempty"`
	Content      *string         `json:"content,omitempty"`
	IsError      bool            `json:"is_error,omitempty"`
	Thinking     *string         `json:"thinking,omitempty"`
	Signature    *string         `json:"signature,omitempty"`
	CacheControl json.RawMessage `json:"cache_control,omitempty"`
}

// AnthropicToolID maps a tool call id to one Anthropic accepts (^[a-zA-Z0-9_-]+$): every other byte
// becomes '_' and, when anything changed, '_' and the first 8 hex digits of the SHA-256 of the original id
// are appended so that distinct ids stay distinct. Valid ids are returned unchanged. The mapping is
// deterministic, so a tool_use id and the tool_use_id of its result still match.
func AnthropicToolID(id string) string {
	b := []byte(id)
	changed := false
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			b[i], changed = '_', true
		}
	}
	if !changed {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return string(b) + "_" + hex.EncodeToString(sum[:4])
}

// toolInput returns the arguments of a tool call as an Anthropic input object.
func toolInput(args string) (json.RawMessage, error) {
	if strings.TrimSpace(args) == "" {
		return json.RawMessage("{}"), nil
	}
	c, err := compactJSON([]byte(args))
	if err != nil || !strings.HasPrefix(c, "{") {
		return nil, invalidf("tool_calls.function.arguments", "must be a JSON object")
	}
	return json.RawMessage(c), nil
}

func anthropicBlock(b Block) (json.RawMessage, error) {
	var out anthropicBlockOut
	switch b.Type {
	case BlockOpaque:
		return b.Raw, nil
	case BlockText:
		t := b.Text
		out = anthropicBlockOut{Type: "text", Text: &t}
	case BlockToolCall:
		in, err := toolInput(b.Arguments)
		if err != nil {
			return nil, err
		}
		out = anthropicBlockOut{Type: "tool_use", ID: AnthropicToolID(b.ID), Name: b.Name, Input: in}
	case BlockToolResult:
		c := b.Text
		out = anthropicBlockOut{Type: "tool_result", ToolUseID: AnthropicToolID(b.ID), Content: &c, IsError: b.IsError}
	case BlockThinking:
		t, s := b.Text, b.Signature
		out = anthropicBlockOut{Type: "thinking", Thinking: &t, Signature: &s}
	default:
		return nil, invalidf("messages", "unknown block type "+string(b.Type))
	}
	out.CacheControl = b.CacheControl
	return marshalNoEscape(out)
}

// --- responses ---

type anthropicUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

func (u anthropicUsage) neutral() Usage {
	return (Usage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, CacheReadTokens: u.CacheReadInputTokens,
		CacheWriteTokens: u.CacheCreationInputTokens}).clamped()
}

func anthropicUsageOf(u Usage) anthropicUsage {
	u = u.clamped()
	return anthropicUsage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, CacheCreationInputTokens: u.CacheWriteTokens,
		CacheReadInputTokens: u.CacheReadTokens}
}

func stopFromAnthropic(s string) StopReason {
	switch s {
	case "max_tokens", "model_context_window_exceeded":
		return StopMaxTokens
	case "tool_use":
		return StopToolUse
	case "stop_sequence":
		return StopSequence
	case "refusal":
		return StopRefusal
	}
	return StopEnd // end_turn, pause_turn and anything unknown
}

func anthropicStop(r StopReason) string {
	switch r {
	case StopMaxTokens:
		return "max_tokens"
	case StopToolUse:
		return "tool_use"
	case StopSequence:
		return "stop_sequence"
	case StopRefusal:
		return "refusal"
	}
	return "end_turn"
}

type anthropicResponseIn struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Model        string            `json:"model"`
	Content      []json.RawMessage `json:"content"`
	StopReason   *string           `json:"stop_reason"`
	StopSequence *string           `json:"stop_sequence"`
	Usage        *anthropicUsage   `json:"usage"`
}

// DecodeAnthropicResponse decodes a non-stream Anthropic message.
func DecodeAnthropicResponse(body []byte) (Response, error) {
	var in anthropicResponseIn
	if err := json.Unmarshal(body, &in); err != nil {
		return Response{}, badResponse("", "not a message: "+err.Error())
	}
	if in.Type != "message" {
		return Response{}, badResponse("type", "must be message")
	}
	resp := Response{Source: ProtocolAnthropic, ID: in.ID, Model: in.Model}
	for i, raw := range in.Content {
		b, err := responseBlock(raw, idx("content", i))
		if err != nil {
			return Response{}, err
		}
		resp.Blocks = append(resp.Blocks, b)
	}
	if in.StopReason != nil {
		resp.StopReason = stopFromAnthropic(*in.StopReason)
	} else {
		resp.StopReason = StopEnd
	}
	if in.StopSequence != nil {
		resp.StopSequence = *in.StopSequence
	}
	if in.Usage != nil {
		resp.Usage, resp.HasUsage = in.Usage.neutral(), true
	}
	return resp, nil
}

// responseBlock decodes a content block of an Anthropic response; unknown blocks become opaque.
func responseBlock(raw json.RawMessage, field string) (Block, error) {
	var blk struct {
		Type      string          `json:"type"`
		Text      string          `json:"text"`
		ID        string          `json:"id"`
		Name      string          `json:"name"`
		Input     json.RawMessage `json:"input"`
		Thinking  string          `json:"thinking"`
		Signature string          `json:"signature"`
	}
	if err := json.Unmarshal(raw, &blk); err != nil {
		return Block{}, badResponse(field, "must be a content block")
	}
	switch blk.Type {
	case "text":
		return Block{Type: BlockText, Text: blk.Text}, nil
	case "tool_use":
		args := "{}"
		if !isNull(blk.Input) {
			c, err := compactJSON(blk.Input)
			if err != nil {
				return Block{}, badResponse(field+".input", "must be JSON")
			}
			args = c
		}
		return Block{Type: BlockToolCall, ID: blk.ID, Name: blk.Name, Arguments: args}, nil
	case "thinking":
		return Block{Type: BlockThinking, Text: blk.Thinking, Signature: blk.Signature}, nil
	}
	return Block{Type: BlockOpaque, Raw: clone(raw)}, nil
}

type anthropicResponseOut struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Role         string            `json:"role"`
	Model        string            `json:"model"`
	Content      []json.RawMessage `json:"content"`
	StopReason   string            `json:"stop_reason"`
	StopSequence *string           `json:"stop_sequence"`
	Usage        anthropicUsage    `json:"usage"`
}

// EncodeAnthropicResponse encodes resp as a non-stream Anthropic message. Thinking blocks without a
// signature (OpenAI reasoning_content) are left out: they are not valid Anthropic thinking blocks. Tool calls
// whose arguments are not a JSON object (resp.Repaired) are sent with input {}.
func EncodeAnthropicResponse(resp Response) ([]byte, error) {
	out := anthropicResponseOut{ID: resp.ID, Type: "message", Role: "assistant", Model: resp.Model,
		Content: []json.RawMessage{}, StopReason: anthropicStop(resp.StopReason), Usage: anthropicUsageOf(resp.Usage)}
	if resp.StopSequence != "" {
		s := resp.StopSequence
		out.StopSequence = &s
	}
	for _, b := range resp.Blocks {
		if b.Type == BlockThinking && b.Signature == "" {
			continue
		}
		if b.Type == BlockOpaque && resp.Source != ProtocolAnthropic {
			continue
		}
		if b.Type == BlockToolCall && !isJSONObject(b.Arguments) {
			b.Arguments = "{}" // cut or malformed arguments (listed in resp.Repaired): Anthropic input must be an object
		}
		raw, err := anthropicBlock(b)
		if err != nil {
			return nil, badResponse("content", err.Error())
		}
		out.Content = append(out.Content, raw)
	}
	return marshalNoEscape(out)
}
