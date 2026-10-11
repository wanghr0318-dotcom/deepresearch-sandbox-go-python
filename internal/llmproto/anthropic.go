package llmproto

import (
	"encoding/json"
	"strings"
)

// anthropicFields are the accepted top-level fields of an Anthropic messages request. Any other field is an
// *UnsupportedFieldError.
var anthropicFields = []string{
	"model", "messages", "system", "max_tokens", "temperature", "top_p", "top_k", "stop_sequences", "stream",
	"tools", "tool_choice", "metadata", "thinking", "service_tier",
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
	if raw, ok := obj["model"]; ok {
		if req.Model, err = str(raw, "model"); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["max_tokens"]; ok && !isNull(raw) {
		if req.MaxTokens, err = positiveInt(raw, "max_tokens"); err != nil {
			return Request{}, err
		}
	}
	if req.Temperature, err = number(obj["temperature"], "temperature"); err != nil {
		return Request{}, err
	}
	if req.TopP, err = number(obj["top_p"], "top_p"); err != nil {
		return Request{}, err
	}
	if raw, ok := obj["stop_sequences"]; ok && !isNull(raw) {
		if json.Unmarshal(raw, &req.Stop) != nil {
			return Request{}, invalidf("stop_sequences", "must be an array of strings")
		}
	}
	if raw, ok := obj["stream"]; ok && !isNull(raw) {
		if req.Stream, err = boolean(raw, "stream"); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["system"]; ok && !isNull(raw) {
		if err := decodeAnthropicSystem(&req, raw); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["tools"]; ok && !isNull(raw) {
		if err := decodeAnthropicTools(&req, raw); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["tool_choice"]; ok && !isNull(raw) {
		if err := decodeAnthropicToolChoice(&req, raw); err != nil {
			return Request{}, err
		}
	}
	for _, f := range []string{"metadata", "top_k", "service_tier", "thinking"} {
		raw, ok := obj[f]
		if !ok {
			continue
		}
		if req.Extras == nil {
			req.Extras = map[string]json.RawMessage{}
		}
		req.Extras[f] = clone(raw)
		if isNull(raw) {
			continue
		}
		switch f {
		case "thinking":
			var th struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(raw, &th) == nil && th.Type == "disabled" {
				continue
			}
			req.Features = append(req.Features, Feature{Field: f, Kind: FeatureUnsupported})
		case "metadata":
			md, err := object(raw, "metadata")
			if err != nil {
				return Request{}, err
			}
			for k := range md {
				req.Features = append(req.Features, Feature{Field: "metadata." + k, Kind: FeatureDrop})
			}
		default:
			req.Features = append(req.Features, Feature{Field: f, Kind: FeatureDrop})
		}
	}
	if err := decodeAnthropicMessages(&req, obj["messages"]); err != nil {
		return Request{}, err
	}
	return req, nil
}

func decodeAnthropicSystem(req *Request, raw json.RawMessage) error {
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
	items, err := array(raw, "tools")
	if err != nil {
		return err
	}
	for i, it := range items {
		field := idx("tools", i)
		obj, err := object(it, field)
		if err != nil {
			return err
		}
		typ := ""
		if t, ok := obj["type"]; ok && !isNull(t) {
			if typ, err = str(t, field+".type"); err != nil {
				return err
			}
		}
		if typ != "" && typ != "custom" { // server tools (web search, code execution …)
			req.Tools = append(req.Tools, Tool{Raw: clone(it)})
			req.Features = append(req.Features, Feature{Field: field, Kind: FeatureUnsupported})
			continue
		}
		if err := checkAllowed(obj, field, "type", "name", "description", "input_schema", "cache_control"); err != nil {
			return err
		}
		t := Tool{}
		if t.Name, err = str(obj["name"], field+".name"); err != nil || t.Name == "" {
			return invalidf(field+".name", "must be a non-empty string")
		}
		if t.Description, err = optStr(obj["description"], field+".description"); err != nil {
			return err
		}
		if _, err := object(obj["input_schema"], field+".input_schema"); err != nil {
			return err
		}
		t.Parameters = clone(obj["input_schema"])
		if cc, ok := obj["cache_control"]; ok && !isNull(cc) {
			t.CacheControl = clone(cc)
			req.Features = append(req.Features, Feature{Field: field + ".cache_control", Kind: FeatureDrop})
		}
		req.Tools = append(req.Tools, t)
	}
	return nil
}

func decodeAnthropicToolChoice(req *Request, raw json.RawMessage) error {
	var tc struct {
		Type                   string `json:"type"`
		Name                   string `json:"name"`
		DisableParallelToolUse *bool  `json:"disable_parallel_tool_use"`
	}
	if json.Unmarshal(raw, &tc) != nil {
		return invalidf("tool_choice", "must be an object")
	}
	switch tc.Type {
	case "auto":
		req.ToolChoice = &ToolChoice{Mode: ToolChoiceAuto}
	case "any":
		req.ToolChoice = &ToolChoice{Mode: ToolChoiceRequired}
	case "none":
		req.ToolChoice = &ToolChoice{Mode: ToolChoiceNone}
	case "tool":
		if tc.Name == "" {
			return invalidf("tool_choice.name", "must be a non-empty string")
		}
		req.ToolChoice = &ToolChoice{Mode: ToolChoiceTool, Name: tc.Name}
	default:
		return invalidf("tool_choice.type", "must be auto, any, tool or none")
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
		field := idx("messages", i)
		m, err := object(it, field)
		if err != nil {
			return err
		}
		if err := checkAllowed(m, field, "role", "content"); err != nil {
			return err
		}
		roleS, err := str(m["role"], field+".role")
		if err != nil {
			return err
		}
		role := Role(roleS)
		if role != RoleUser && role != RoleAssistant {
			return invalidf(field+".role", "must be user or assistant")
		}
		msg := Message{Role: role}
		content := m["content"]
		switch {
		case isString(content):
			s, err := str(content, field+".content")
			if err != nil {
				return err
			}
			msg.Blocks = []Block{{Type: BlockText, Text: s}}
		case isArray(content):
			blocks, _ := array(content, field+".content")
			for j, b := range blocks {
				blk, err := decodeAnthropicBlock(req, b, idx(field+".content", j), role)
				if err != nil {
					return err
				}
				msg.Blocks = append(msg.Blocks, blk)
			}
		default:
			return invalidf(field+".content", "must be a string or an array of content blocks")
		}
		req.Messages = append(req.Messages, msg)
	}
	return nil
}

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
	var b Block
	switch typ {
	case "text":
		if err := checkAllowed(obj, field, "type", "text", "cache_control", "citations"); err != nil {
			return Block{}, err
		}
		b = Block{Type: BlockText}
		if b.Text, err = str(obj["text"], field+".text"); err != nil {
			return Block{}, err
		}
		if c, ok := obj["citations"]; ok && !isNull(c) {
			req.Features = append(req.Features, Feature{Field: field + ".citations", Kind: FeatureDrop})
		}
	case "tool_use":
		if role != RoleAssistant {
			return Block{}, invalidf(field, "tool_use blocks belong to assistant messages")
		}
		if err := checkAllowed(obj, field, "type", "id", "name", "input", "cache_control"); err != nil {
			return Block{}, err
		}
		b = Block{Type: BlockToolCall}
		if b.ID, err = str(obj["id"], field+".id"); err != nil {
			return Block{}, err
		}
		if b.Name, err = str(obj["name"], field+".name"); err != nil {
			return Block{}, err
		}
		if _, err := object(obj["input"], field+".input"); err != nil {
			return Block{}, err
		}
		if b.Arguments, err = compactJSON(obj["input"]); err != nil {
			return Block{}, invalidf(field+".input", "must be an object")
		}
	case "tool_result":
		if role != RoleUser {
			return Block{}, invalidf(field, "tool_result blocks belong to user messages")
		}
		if err := checkAllowed(obj, field, "type", "tool_use_id", "content", "is_error", "cache_control"); err != nil {
			return Block{}, err
		}
		b = Block{Type: BlockToolResult}
		if b.ID, err = str(obj["tool_use_id"], field+".tool_use_id"); err != nil {
			return Block{}, err
		}
		text, opaque, err := anthropicToolResultContent(obj["content"], field+".content")
		if err != nil {
			return Block{}, err
		}
		if opaque != "" { // images or documents in a tool result: keep the whole block verbatim
			req.Features = append(req.Features, Feature{Field: opaque, Kind: FeatureUnsupported})
			return Block{Type: BlockOpaque, Raw: clone(raw)}, nil
		}
		b.Text = text
		if e, ok := obj["is_error"]; ok && !isNull(e) {
			if b.IsError, err = boolean(e, field+".is_error"); err != nil {
				return Block{}, err
			}
			if b.IsError {
				req.Features = append(req.Features, Feature{Field: field + ".is_error", Kind: FeatureDrop})
			}
		}
	case "thinking":
		if role != RoleAssistant {
			return Block{}, invalidf(field, "thinking blocks belong to assistant messages")
		}
		if err := checkAllowed(obj, field, "type", "thinking", "signature"); err != nil {
			return Block{}, err
		}
		b = Block{Type: BlockThinking}
		if b.Text, err = str(obj["thinking"], field+".thinking"); err != nil {
			return Block{}, err
		}
		if b.Signature, err = optStr(obj["signature"], field+".signature"); err != nil {
			return Block{}, err
		}
		req.Features = append(req.Features, Feature{Field: field, Kind: FeatureUnsupported})
		return b, nil
	default: // image, document, redacted_thinking, search_result, server tool blocks …
		req.Features = append(req.Features, Feature{Field: field, Kind: FeatureUnsupported})
		return Block{Type: BlockOpaque, Raw: clone(raw)}, nil
	}
	if cc, ok := obj["cache_control"]; ok && !isNull(cc) {
		b.CacheControl = clone(cc)
		req.Features = append(req.Features, Feature{Field: field + ".cache_control", Kind: FeatureDrop})
	}
	return b, nil
}

// anthropicToolResultContent returns the text of a tool result's content (absent, a string, or an array of
// text blocks), or the field of its first non-text block.
func anthropicToolResultContent(raw json.RawMessage, field string) (text, opaque string, err error) {
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
	var b strings.Builder
	for i, it := range items {
		var blk struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
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
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(*blk.Text)
	}
	return b.String(), "", nil
}

// --- encoding ---

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
	if len(req.System) > 0 {
		if len(req.System) == 1 && req.System[0].CacheControl == nil {
			out.System = mustMarshalNoEscape(req.System[0].Text)
		} else {
			blocks := make([]json.RawMessage, 0, len(req.System))
			for _, s := range req.System {
				b, err := anthropicBlock(s)
				if err != nil {
					return nil, err
				}
				blocks = append(blocks, b)
			}
			out.System = mustMarshalNoEscape(blocks)
		}
	}
	// Consecutive messages of the same role become one turn (e.g. tool results followed by user text).
	var merged []Message
	for _, m := range req.Messages {
		if m.Role != RoleUser && m.Role != RoleAssistant {
			return nil, unsupported("messages", "role "+string(m.Role)+" has no anthropic equivalent")
		}
		if n := len(merged); n > 0 && merged[n-1].Role == m.Role {
			merged[n-1].Blocks = append(append([]Block(nil), merged[n-1].Blocks...), m.Blocks...)
			continue
		}
		merged = append(merged, m)
	}
	for _, m := range merged {
		content, err := anthropicContent(m.Blocks)
		if err != nil {
			return nil, err
		}
		out.Messages = append(out.Messages, anthropicMessageOut{Role: string(m.Role), Content: content})
	}
	for _, t := range req.Tools {
		if t.Raw != nil {
			out.Tools = append(out.Tools, t.Raw) // only reachable for an Anthropic source (else unsupported)
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
		out.Tools = append(out.Tools, b)
	}
	if tc := req.ToolChoice; tc != nil {
		switch tc.Mode {
		case ToolChoiceAuto:
			out.ToolChoice = &anthropicToolChoice{Type: "auto"}
		case ToolChoiceNone:
			out.ToolChoice = &anthropicToolChoice{Type: "none"}
		case ToolChoiceRequired:
			out.ToolChoice = &anthropicToolChoice{Type: "any"}
		case ToolChoiceTool:
			out.ToolChoice = &anthropicToolChoice{Type: "tool", Name: tc.Name}
		default:
			return nil, invalidf("tool_choice", "unknown mode "+string(tc.Mode))
		}
	}
	if p := req.ParallelToolCalls; p != nil && !*p && len(req.Tools) > 0 {
		if out.ToolChoice == nil {
			out.ToolChoice = &anthropicToolChoice{Type: "auto"}
		}
		if out.ToolChoice.Type != "none" {
			out.ToolChoice.DisableParallelToolUse = true
		}
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
		out = anthropicBlockOut{Type: "tool_use", ID: b.ID, Name: b.Name, Input: in}
	case BlockToolResult:
		c := b.Text
		out = anthropicBlockOut{Type: "tool_result", ToolUseID: b.ID, Content: &c, IsError: b.IsError}
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
	return Usage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, CacheReadTokens: u.CacheReadInputTokens,
		CacheWriteTokens: u.CacheCreationInputTokens}
}

func anthropicUsageOf(u Usage) anthropicUsage {
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
		return Response{}, invalidf("", "response is not a message: "+err.Error())
	}
	if in.Type != "message" {
		return Response{}, invalidf("type", "must be message")
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
		return Block{}, invalidf(field, "must be a content block")
	}
	switch blk.Type {
	case "text":
		return Block{Type: BlockText, Text: blk.Text}, nil
	case "tool_use":
		args := "{}"
		if !isNull(blk.Input) {
			c, err := compactJSON(blk.Input)
			if err != nil {
				return Block{}, invalidf(field+".input", "must be JSON")
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
// signature (OpenAI reasoning_content) are left out: they are not valid Anthropic thinking blocks.
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
		raw, err := anthropicBlock(b)
		if err != nil {
			return nil, err
		}
		out.Content = append(out.Content, raw)
	}
	return marshalNoEscape(out)
}
