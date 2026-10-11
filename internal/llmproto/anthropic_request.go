package llmproto

import (
	"encoding/json"
	"slices"
	"strings"
)

// anthropicFields are the accepted top-level fields of an Anthropic messages request. Any other field is an
// *UnsupportedFieldError.
var anthropicFields = []string{
	"model", "messages", "system", "max_tokens", "temperature", "top_p", "top_k", "stop_sequences", "stream",
	"tools", "tool_choice", "metadata", "thinking", "service_tier",
}

// anthropicExtraFields are the top-level fields without a neutral equivalent, kept in Request.Extras
// (metadata is recorded per key by decodeAnthropicMetadata).
var anthropicExtraFields = []string{"top_k", "service_tier", "thinking"}

// anthropicExtra classifies a top-level Anthropic field without a neutral equivalent.
func anthropicExtra(field string, raw json.RawMessage) (FeatureKind, bool) {
	if isNull(raw) {
		return "", false
	}
	if field == "thinking" {
		var th struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &th) == nil && th.Type == "disabled" {
			return "", false
		}
		return FeatureUnsupported, true
	}
	return FeatureDrop, true // top_k, service_tier
}

// DecodeAnthropicRequest decodes an Anthropic messages request.
func DecodeAnthropicRequest(body []byte) (Request, error) {
	obj, err := object(body, "")
	if err != nil {
		return Request{}, err
	}
	if err := checkAllowed(obj, "", anthropicFields...); err != nil {
		return Request{}, err
	}
	req := Request{Source: ProtocolAnthropic}
	f := &fields{obj: obj}
	f.str("model", &req.Model)
	f.positive("max_tokens", &req.MaxTokens)
	f.number("temperature", &req.Temperature)
	f.number("top_p", &req.TopP)
	f.boolean("stream", &req.Stream)
	err = steps(
		func() error { return f.err },
		func() error { return checkMaxTokens(req.MaxTokens) },
		func() error { return decodeStopSequences(&req, obj["stop_sequences"]) },
		func() error { return decodeAnthropicSystem(&req, obj["system"]) },
		func() error { return decodeAnthropicTools(&req, obj["tools"]) },
		func() error { return decodeAnthropicToolChoice(&req, obj["tool_choice"]) },
		func() error { return decodeAnthropicMetadata(&req, obj["metadata"]) },
		func() error { recordExtras(&req, obj, anthropicExtraFields, anthropicExtra); return nil },
		func() error { return decodeAnthropicMessages(&req, obj["messages"]) },
	)
	if err != nil {
		return Request{}, err
	}
	return req, nil
}

// maxOpenAIStop is the most stop sequences OpenAI accepts.
const maxOpenAIStop = 4

func decodeStopSequences(req *Request, raw json.RawMessage) error {
	if !isNull(raw) && json.Unmarshal(raw, &req.Stop) != nil {
		return invalidf("stop_sequences", "must be an array of strings")
	}
	if len(req.Stop) > maxOpenAIStop {
		req.feature("stop_sequences", FeatureUnsupported)
	}
	return nil
}

// decodeAnthropicMetadata keeps metadata verbatim and records each of its keys (user_id) as dropped.
func decodeAnthropicMetadata(req *Request, raw json.RawMessage) error {
	if raw == nil {
		return nil
	}
	if req.Extras == nil {
		req.Extras = map[string]json.RawMessage{}
	}
	req.Extras["metadata"] = clone(raw)
	if isNull(raw) {
		return nil
	}
	md, err := object(raw, "metadata")
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		req.feature("metadata."+k, FeatureDrop)
	}
	return nil
}

func decodeAnthropicSystem(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	if isString(raw) {
		s, err := str(raw, "system")
		req.System = []Block{{Type: BlockText, Text: s}}
		return err
	}
	items, err := array(raw, "system")
	if err != nil {
		return invalidf("system", "must be a string or an array of text blocks")
	}
	for i, it := range items {
		field := idx("system", i)
		b, err := decodeAnthropicBlock(req, it, field, RoleUser)
		if err != nil {
			return err
		}
		if b.Type != BlockText {
			return invalidf(field, "system blocks must be text")
		}
		req.System = append(req.System, b)
	}
	return nil
}

func decodeAnthropicTools(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	items, err := array(raw, "tools")
	if err != nil {
		return err
	}
	for i, it := range items {
		if err := decodeAnthropicTool(req, it, idx("tools", i)); err != nil {
			return err
		}
	}
	return nil
}

func decodeAnthropicTool(req *Request, raw json.RawMessage, field string) error {
	obj, err := object(raw, field)
	if err != nil {
		return err
	}
	typ := ""
	f := &fields{obj: obj, prefix: field}
	f.str("type", &typ)
	if f.err != nil {
		return f.err
	}
	if typ != "" && typ != "custom" { // server tools (web search, code execution …)
		req.Tools = append(req.Tools, Tool{Raw: clone(raw)})
		req.feature(field, FeatureUnsupported)
		return nil
	}
	if err := checkAllowed(obj, field, "type", "name", "description", "input_schema", "cache_control"); err != nil {
		return err
	}
	t := Tool{}
	f.name("name", &t.Name)
	f.str("description", &t.Description)
	f.schema("input_schema", &t.Parameters)
	if f.err != nil {
		return f.err
	}
	if cc, ok := f.get("cache_control"); ok {
		t.CacheControl = clone(cc)
		req.feature(field+".cache_control", FeatureDrop)
	}
	req.Tools = append(req.Tools, t)
	return nil
}

func decodeAnthropicToolChoice(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	obj, err := object(raw, "tool_choice")
	if err != nil {
		return err
	}
	if err := checkAllowed(obj, "tool_choice", "type", "name", "disable_parallel_tool_use"); err != nil {
		return err
	}
	var tc struct {
		Type                   string `json:"type"`
		Name                   string `json:"name"`
		DisableParallelToolUse *bool  `json:"disable_parallel_tool_use"`
	}
	if json.Unmarshal(raw, &tc) != nil {
		return invalidf("tool_choice", "must be an object")
	}
	modes := map[string]ToolChoiceMode{"auto": ToolChoiceAuto, "any": ToolChoiceRequired, "none": ToolChoiceNone, "tool": ToolChoiceTool}
	mode, ok := modes[tc.Type]
	if !ok {
		return invalidf("tool_choice.type", "must be auto, any, tool or none")
	}
	if mode == ToolChoiceTool && tc.Name == "" {
		return invalidf("tool_choice.name", "must be a non-empty string")
	}
	req.ToolChoice = &ToolChoice{Mode: mode}
	if mode == ToolChoiceTool {
		req.ToolChoice.Name = tc.Name
	}
	if d := tc.DisableParallelToolUse; d != nil {
		p := !*d
		req.ParallelToolCalls = &p
	}
	return nil
}

func decodeAnthropicMessages(req *Request, raw json.RawMessage) error {
	items, err := array(raw, "messages")
	if err != nil || len(items) == 0 {
		return invalidf("messages", "must be a non-empty array")
	}
	for i, it := range items {
		msg, err := decodeAnthropicMessage(req, it, idx("messages", i))
		if err != nil {
			return err
		}
		req.Messages = append(req.Messages, msg)
	}
	return nil
}

func decodeAnthropicMessage(req *Request, raw json.RawMessage, field string) (Message, error) {
	m, err := object(raw, field)
	if err != nil {
		return Message{}, err
	}
	if err := checkAllowed(m, field, "role", "content"); err != nil {
		return Message{}, err
	}
	roleS, err := str(m["role"], field+".role")
	if err != nil {
		return Message{}, err
	}
	role := Role(roleS)
	if role != RoleUser && role != RoleAssistant {
		return Message{}, invalidf(field+".role", "must be user or assistant")
	}
	msg := Message{Role: role}
	content := m["content"]
	if isString(content) {
		s, err := str(content, field+".content")
		msg.Blocks = []Block{{Type: BlockText, Text: s}}
		return msg, err
	}
	if !isArray(content) {
		return Message{}, invalidf(field+".content", "must be a string or an array of content blocks")
	}
	blocks, err := array(content, field+".content")
	if err != nil {
		return Message{}, err
	}
	for j, b := range blocks {
		blk, err := decodeAnthropicBlock(req, b, idx(field+".content", j), role)
		if err != nil {
			return Message{}, err
		}
		msg.Blocks = append(msg.Blocks, blk)
	}
	return msg, nil
}

// blockRoles says which role's messages may hold each block type the neutral format models.
var blockRoles = map[string]Role{"tool_use": RoleAssistant, "thinking": RoleAssistant, "tool_result": RoleUser}

// decodeAnthropicBlock decodes one content block of a message (or of the system prompt).
func decodeAnthropicBlock(req *Request, raw json.RawMessage, field string, role Role) (Block, error) {
	obj, err := object(raw, field)
	if err != nil {
		return Block{}, err
	}
	typ, err := str(obj["type"], field+".type")
	if err != nil {
		return Block{}, err
	}
	if want, ok := blockRoles[typ]; ok && want != role {
		return Block{}, invalidf(field, typ+" blocks belong to "+string(want)+" messages")
	}
	var b Block
	switch typ {
	case "text":
		b, err = anthropicTextBlock(req, obj, field)
	case "tool_use":
		b, err = anthropicToolUseBlock(obj, field)
	case "tool_result":
		b, err = anthropicToolResultBlock(req, obj, raw, field)
	case "thinking":
		b, err = anthropicThinkingBlock(obj, field)
		req.feature(field, FeatureUnsupported)
	default: // image, document, redacted_thinking, search_result, server tool blocks …
		req.feature(field, FeatureUnsupported)
		return Block{Type: BlockOpaque, Raw: clone(raw)}, nil
	}
	if err != nil || b.Type == BlockOpaque || b.Type == BlockThinking {
		return b, err
	}
	if cc, ok := obj["cache_control"]; ok && !isNull(cc) {
		b.CacheControl = clone(cc)
		req.feature(field+".cache_control", FeatureDrop)
	}
	return b, nil
}

func anthropicTextBlock(req *Request, obj map[string]json.RawMessage, field string) (Block, error) {
	if err := checkAllowed(obj, field, "type", "text", "cache_control", "citations"); err != nil {
		return Block{}, err
	}
	text, err := str(obj["text"], field+".text")
	if err != nil {
		return Block{}, err
	}
	if c, ok := obj["citations"]; ok && !isNull(c) {
		req.feature(field+".citations", FeatureDrop)
	}
	return Block{Type: BlockText, Text: text}, nil
}

func anthropicToolUseBlock(obj map[string]json.RawMessage, field string) (Block, error) {
	if err := checkAllowed(obj, field, "type", "id", "name", "input", "cache_control"); err != nil {
		return Block{}, err
	}
	b := Block{Type: BlockToolCall}
	f := &fields{obj: obj, prefix: field}
	f.name("id", &b.ID)
	f.name("name", &b.Name)
	var input json.RawMessage
	f.schema("input", &input)
	if f.err != nil {
		return Block{}, f.err
	}
	args, err := compactJSON(input)
	if err != nil {
		return Block{}, invalidf(field+".input", "must be an object")
	}
	b.Arguments = args
	return b, nil
}

// anthropicToolResultBlock decodes a tool result. One with images or documents in its content is kept as
// an opaque block (unsupported when converting).
func anthropicToolResultBlock(req *Request, obj map[string]json.RawMessage, raw json.RawMessage, field string) (Block, error) {
	if err := checkAllowed(obj, field, "type", "tool_use_id", "content", "is_error", "cache_control"); err != nil {
		return Block{}, err
	}
	b := Block{Type: BlockToolResult}
	f := &fields{obj: obj, prefix: field}
	f.name("tool_use_id", &b.ID)
	f.boolean("is_error", &b.IsError)
	if f.err != nil {
		return Block{}, f.err
	}
	text, opaque, err := anthropicToolResultContent(req, obj["content"], field+".content")
	if err != nil {
		return Block{}, err
	}
	if opaque != "" {
		req.feature(opaque, FeatureUnsupported)
		return Block{Type: BlockOpaque, Raw: clone(raw)}, nil
	}
	b.Text = text
	if b.IsError {
		req.feature(field+".is_error", FeatureDrop)
	}
	return b, nil
}

func anthropicThinkingBlock(obj map[string]json.RawMessage, field string) (Block, error) {
	if err := checkAllowed(obj, field, "type", "thinking", "signature"); err != nil {
		return Block{}, err
	}
	text, err := str(obj["thinking"], field+".thinking")
	if err != nil {
		return Block{}, err
	}
	b := Block{Type: BlockThinking, Text: text}
	f := &fields{obj: obj, prefix: field}
	f.str("signature", &b.Signature)
	return b, f.err
}

// anthropicToolResultContent returns the text of a tool result's content (absent, a string, or an array of
// text blocks, joined by newlines), or the field of its first non-text block.
func anthropicToolResultContent(req *Request, raw json.RawMessage, field string) (text, opaque string, err error) {
	if isNull(raw) {
		return "", "", nil
	}
	if isString(raw) {
		s, err := str(raw, field)
		return s, "", err
	}
	items, err := array(raw, field)
	if err != nil {
		return "", "", invalidf(field, "must be a string or an array of content blocks")
	}
	texts := make([]string, 0, len(items))
	for i, it := range items {
		var blk struct {
			Type         string          `json:"type"`
			Text         *string         `json:"text"`
			CacheControl json.RawMessage `json:"cache_control"`
		}
		if json.Unmarshal(it, &blk) != nil {
			return "", "", invalidf(idx(field, i), "must be a content block")
		}
		if blk.Type != "text" {
			return "", idx(field, i), nil
		}
		if blk.Text == nil {
			return "", "", invalidf(idx(field, i)+".text", "must be a string")
		}
		if !isNull(blk.CacheControl) {
			req.feature(idx(field, i)+".cache_control", FeatureDrop)
		}
		texts = append(texts, *blk.Text)
	}
	return strings.Join(texts, "\n"), "", nil
}
