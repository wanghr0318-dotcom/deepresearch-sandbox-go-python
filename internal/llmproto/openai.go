package llmproto

import (
	"encoding/json"
	"strings"
)

// openAIFields are the accepted top-level fields of an OpenAI chat request: the fields that
// internal/gateway/upstream accepts for sandbox calls, plus the client-side fields the model API needs
// (stream_options, parallel_tool_calls, user, reasoning_effort, n, logprobs, top_logprobs). Any other field
// is an *UnsupportedFieldError, as in gateway/upstream (max_completion_tokens, functions, … are refused).
var openAIFields = []string{
	"model", "messages", "max_tokens", "temperature", "top_p", "stop", "stream",
	"tools", "tool_choice", "response_format", "seed", "presence_penalty", "frequency_penalty",
	"stream_options", "parallel_tool_calls", "user", "reasoning_effort", "n", "logprobs", "top_logprobs",
}

// openAIMessageFields are the accepted message fields (as in gateway/upstream).
var openAIMessageFields = []string{"role", "content", "name", "tool_call_id", "tool_calls", "reasoning_content"}

// openAIExtra classifies a top-level OpenAI field without a neutral equivalent. ok is false for values that
// are no-ops in the other format (n=1, logprobs=false, response_format text).
func openAIExtra(field string, raw json.RawMessage) (kind FeatureKind, ok bool) {
	switch field {
	case "seed", "presence_penalty", "frequency_penalty", "user":
		return FeatureDrop, !isNull(raw)
	case "n":
		return FeatureUnsupported, !isNull(raw) && strings.TrimSpace(string(raw)) != "1"
	case "logprobs":
		return FeatureUnsupported, !isNull(raw) && strings.TrimSpace(string(raw)) != "false"
	case "response_format":
		var rf struct {
			Type string `json:"type"`
		}
		if isNull(raw) || (json.Unmarshal(raw, &rf) == nil && rf.Type == "text") {
			return "", false
		}
		return FeatureUnsupported, true
	}
	// reasoning_effort, top_logprobs
	return FeatureUnsupported, !isNull(raw)
}

// DecodeOpenAIRequest decodes an OpenAI chat completions request.
func DecodeOpenAIRequest(body []byte) (Request, error) {
	obj, err := object(body, "")
	if err != nil {
		return Request{}, err
	}
	if err := checkAllowed(obj, "", openAIFields...); err != nil {
		return Request{}, err
	}
	req := Request{Source: ProtocolOpenAI}
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
	if raw, ok := obj["stop"]; ok && !isNull(raw) {
		if req.Stop, err = decodeStop(raw); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["stream"]; ok && !isNull(raw) {
		if req.Stream, err = boolean(raw, "stream"); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["stream_options"]; ok && !isNull(raw) {
		so, err := object(raw, "stream_options")
		if err != nil {
			return Request{}, err
		}
		if err := checkAllowed(so, "stream_options", "include_usage"); err != nil {
			return Request{}, err
		}
		if iu, ok := so["include_usage"]; ok && !isNull(iu) {
			if req.IncludeUsage, err = boolean(iu, "stream_options.include_usage"); err != nil {
				return Request{}, err
			}
		}
	}
	if raw, ok := obj["parallel_tool_calls"]; ok && !isNull(raw) {
		b, err := boolean(raw, "parallel_tool_calls")
		if err != nil {
			return Request{}, err
		}
		req.ParallelToolCalls = &b
	}
	if raw, ok := obj["tools"]; ok && !isNull(raw) {
		if err := decodeOpenAITools(&req, raw); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := obj["tool_choice"]; ok && !isNull(raw) {
		tc, err := decodeOpenAIToolChoice(raw)
		if err != nil {
			return Request{}, err
		}
		req.ToolChoice = &tc
	}
	for _, f := range []string{"response_format", "seed", "presence_penalty", "frequency_penalty", "user",
		"reasoning_effort", "n", "logprobs", "top_logprobs"} {
		raw, ok := obj[f]
		if !ok {
			continue
		}
		if req.Extras == nil {
			req.Extras = map[string]json.RawMessage{}
		}
		req.Extras[f] = clone(raw)
		if kind, ok := openAIExtra(f, raw); ok {
			req.Features = append(req.Features, Feature{Field: f, Kind: kind})
		}
	}
	if err := decodeOpenAIMessages(&req, obj["messages"]); err != nil {
		return Request{}, err
	}
	return req, nil
}

func decodeStop(raw json.RawMessage) ([]string, error) {
	if isString(raw) {
		s, err := str(raw, "stop")
		return []string{s}, err
	}
	var ss []string
	if json.Unmarshal(raw, &ss) != nil {
		return nil, invalidf("stop", "must be a string or an array of strings")
	}
	return ss, nil
}

func decodeOpenAITools(req *Request, raw json.RawMessage) error {
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
		typ, err := str(obj["type"], field+".type")
		if err != nil {
			return err
		}
		if typ != "function" {
			req.Tools = append(req.Tools, Tool{Raw: clone(it)})
			req.Features = append(req.Features, Feature{Field: field, Kind: FeatureUnsupported})
			continue
		}
		if err := checkAllowed(obj, field, "type", "function"); err != nil {
			return err
		}
		fn, err := object(obj["function"], field+".function")
		if err != nil {
			return err
		}
		if err := checkAllowed(fn, field+".function", "name", "description", "parameters", "strict"); err != nil {
			return err
		}
		t := Tool{}
		if t.Name, err = str(fn["name"], field+".function.name"); err != nil || t.Name == "" {
			return invalidf(field+".function.name", "must be a non-empty string")
		}
		if t.Description, err = optStr(fn["description"], field+".function.description"); err != nil {
			return err
		}
		if p, ok := fn["parameters"]; ok && !isNull(p) {
			if _, err := object(p, field+".function.parameters"); err != nil {
				return err
			}
			t.Parameters = clone(p)
		}
		if s, ok := fn["strict"]; ok && !isNull(s) {
			b, err := boolean(s, field+".function.strict")
			if err != nil {
				return err
			}
			t.Strict = &b
			req.Features = append(req.Features, Feature{Field: field + ".function.strict", Kind: FeatureDrop})
		}
		req.Tools = append(req.Tools, t)
	}
	return nil
}

func decodeOpenAIToolChoice(raw json.RawMessage) (ToolChoice, error) {
	if isString(raw) {
		s, _ := str(raw, "tool_choice")
		switch s {
		case "auto":
			return ToolChoice{Mode: ToolChoiceAuto}, nil
		case "none":
			return ToolChoice{Mode: ToolChoiceNone}, nil
		case "required":
			return ToolChoice{Mode: ToolChoiceRequired}, nil
		}
		return ToolChoice{}, invalidf("tool_choice", "must be auto, none, required or a function")
	}
	var tc struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if json.Unmarshal(raw, &tc) != nil || tc.Type != "function" || tc.Function.Name == "" {
		return ToolChoice{}, invalidf("tool_choice", "must be auto, none, required or a function")
	}
	return ToolChoice{Mode: ToolChoiceTool, Name: tc.Function.Name}, nil
}

// openAIContent decodes a content value (string, null or an array of parts) into blocks. Text parts become
// text blocks; other parts (image_url, input_audio, file, refusal …) become opaque blocks and are recorded
// as unsupported features.
func openAIContent(req *Request, raw json.RawMessage, field string) ([]Block, error) {
	if isNull(raw) {
		return nil, nil
	}
	if isString(raw) {
		s, err := str(raw, field)
		return []Block{{Type: BlockText, Text: s}}, err
	}
	parts, err := array(raw, field)
	if err != nil {
		return nil, invalidf(field, "must be a string or an array of content parts")
	}
	var out []Block
	for j, p := range parts {
		pf := idx(field, j)
		var part struct {
			Type string           `json:"type"`
			Text *json.RawMessage `json:"text"`
		}
		if json.Unmarshal(p, &part) != nil || part.Type == "" {
			return nil, invalidf(pf, "must be a content part with a type")
		}
		if part.Type == "text" {
			if part.Text == nil {
				return nil, invalidf(pf+".text", "must be a string")
			}
			s, err := str(*part.Text, pf+".text")
			if err != nil {
				return nil, err
			}
			out = append(out, Block{Type: BlockText, Text: s})
			continue
		}
		out = append(out, Block{Type: BlockOpaque, Raw: clone(p)})
		req.Features = append(req.Features, Feature{Field: pf, Kind: FeatureUnsupported})
	}
	return out, nil
}

// textOnly joins text blocks; ok is false when blocks holds anything else.
func textOnly(blocks []Block) (string, bool) {
	var b strings.Builder
	for _, bl := range blocks {
		if bl.Type != BlockText {
			return "", false
		}
		b.WriteString(bl.Text)
	}
	return b.String(), true
}

func decodeOpenAIMessages(req *Request, raw json.RawMessage) error {
	items, err := array(raw, "messages")
	if err != nil || len(items) == 0 {
		return invalidf("messages", "must be a non-empty array")
	}
	leading := true   // still in the leading system messages
	lastTool := false // the previous message was a tool message (its result was appended to the last user message)
	for i, it := range items {
		field := idx("messages", i)
		m, err := object(it, field)
		if err != nil {
			return err
		}
		if err := checkAllowed(m, field, openAIMessageFields...); err != nil {
			return err
		}
		role, err := str(m["role"], field+".role")
		if err != nil || role == "" {
			return invalidf(field+".role", "must be a string")
		}
		if _, ok := m["reasoning_content"]; ok && role != "assistant" {
			return unsupported(field+".reasoning_content", "only for assistant messages")
		}
		if _, ok := m["tool_calls"]; ok && role != "assistant" {
			return invalidf(field+".tool_calls", "only for assistant messages")
		}
		if _, ok := m["tool_call_id"]; ok && role != "tool" {
			return invalidf(field+".tool_call_id", "only for tool messages")
		}
		name := ""
		if raw, ok := m["name"]; ok && !isNull(raw) {
			if name, err = str(raw, field+".name"); err != nil {
				return err
			}
			req.Features = append(req.Features, Feature{Field: field + ".name", Kind: FeatureDrop})
		}
		switch role {
		case "system", "developer":
			blocks, err := openAIContent(req, m["content"], field+".content")
			if err != nil {
				return err
			}
			text, ok := textOnly(blocks)
			if !ok || len(blocks) == 0 {
				return invalidf(field+".content", "system content must be text")
			}
			if leading {
				req.System = append(req.System, Block{Type: BlockText, Text: text})
			} else {
				req.Messages = append(req.Messages, Message{Role: RoleSystem, Blocks: []Block{{Type: BlockText, Text: text}}, Name: name})
				req.Features = append(req.Features, Feature{Field: field + ".role", Kind: FeatureUnsupported})
			}
			lastTool = false
			continue
		case "user":
			blocks, err := openAIContent(req, m["content"], field+".content")
			if err != nil {
				return err
			}
			if _, ok := m["content"]; !ok || blocks == nil {
				return invalidf(field+".content", "must be a string or an array of content parts")
			}
			req.Messages = append(req.Messages, Message{Role: RoleUser, Blocks: blocks, Name: name})
			lastTool = false
		case "assistant":
			msg, err := decodeOpenAIAssistant(req, m, field)
			if err != nil {
				return err
			}
			msg.Name = name
			req.Messages = append(req.Messages, msg)
			lastTool = false
		case "tool":
			id, err := str(m["tool_call_id"], field+".tool_call_id")
			if err != nil || id == "" {
				return invalidf(field+".tool_call_id", "must be a non-empty string")
			}
			blocks, err := openAIContent(req, m["content"], field+".content")
			if err != nil {
				return err
			}
			text, ok := textOnly(blocks)
			if !ok {
				return unsupported(field+".content", "tool results must be text")
			}
			b := Block{Type: BlockToolResult, ID: id, Text: text}
			if lastTool {
				last := &req.Messages[len(req.Messages)-1]
				last.Blocks = append(last.Blocks, b)
			} else {
				req.Messages = append(req.Messages, Message{Role: RoleUser, Blocks: []Block{b}})
			}
			lastTool = true
		default:
			return unsupported(field+".role", "role "+role+" is not supported")
		}
		leading = false
	}
	if len(req.Messages) == 0 {
		return invalidf("messages", "must contain a non-system message")
	}
	return nil
}

func decodeOpenAIAssistant(req *Request, m map[string]json.RawMessage, field string) (Message, error) {
	msg := Message{Role: RoleAssistant}
	if raw, ok := m["reasoning_content"]; ok && !isNull(raw) {
		rc, err := str(raw, field+".reasoning_content")
		if err != nil {
			return Message{}, err
		}
		msg.Blocks = append(msg.Blocks, Block{Type: BlockThinking, Text: rc})
		req.Features = append(req.Features, Feature{Field: field + ".reasoning_content", Kind: FeatureUnsupported})
	}
	content, err := openAIContent(req, m["content"], field+".content")
	if err != nil {
		return Message{}, err
	}
	var calls []json.RawMessage
	if raw, ok := m["tool_calls"]; ok && !isNull(raw) {
		if calls, err = array(raw, field+".tool_calls"); err != nil {
			return Message{}, err
		}
	}
	// An assistant message that only calls tools may have content "" or null.
	if len(calls) > 0 && len(content) == 1 && content[0].Type == BlockText && content[0].Text == "" {
		content = nil
	}
	if _, ok := m["content"]; !ok && len(calls) == 0 {
		return Message{}, invalidf(field+".content", "must be given when there are no tool_calls")
	}
	msg.Blocks = append(msg.Blocks, content...)
	for j, c := range calls {
		cf := idx(field+".tool_calls", j)
		var call struct {
			ID       string `json:"id"`
			Type     string `json:"type"`
			Function *struct {
				Name      string           `json:"name"`
				Arguments *json.RawMessage `json:"arguments"`
			} `json:"function"`
		}
		if json.Unmarshal(c, &call) != nil || call.Function == nil {
			return Message{}, invalidf(cf, "must be a function tool call")
		}
		if call.Type != "" && call.Type != "function" {
			return Message{}, unsupported(cf+".type", "only function tool calls are supported")
		}
		if call.ID == "" || call.Function.Name == "" {
			return Message{}, invalidf(cf, "id and function.name must be non-empty")
		}
		args := ""
		if call.Function.Arguments != nil {
			if args, err = optStr(*call.Function.Arguments, cf+".function.arguments"); err != nil {
				return Message{}, err
			}
		}
		if strings.TrimSpace(args) == "" {
			args = "{}" // a call without arguments
		}
		msg.Blocks = append(msg.Blocks, Block{Type: BlockToolCall, ID: call.ID, Name: call.Function.Name, Arguments: args})
	}
	return msg, nil
}

// --- encoding ---

type openAIRequestOut struct {
	Model             string             `json:"model,omitempty"`
	Messages          []openAIMessageOut `json:"messages"`
	MaxTokens         int64              `json:"max_tokens,omitempty"`
	Temperature       *float64           `json:"temperature,omitempty"`
	TopP              *float64           `json:"top_p,omitempty"`
	Stop              []string           `json:"stop,omitempty"`
	Stream            bool               `json:"stream,omitempty"`
	StreamOptions     *openAIStreamOpts  `json:"stream_options,omitempty"`
	Tools             []json.RawMessage  `json:"tools,omitempty"`
	ToolChoice        json.RawMessage    `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool              `json:"parallel_tool_calls,omitempty"`
}

type openAIStreamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

type openAIMessageOut struct {
	Role             string           `json:"role"`
	Content          json.RawMessage  `json:"content,omitempty"`
	ReasoningContent *string          `json:"reasoning_content,omitempty"`
	Name             string           `json:"name,omitempty"`
	ToolCallID       string           `json:"tool_call_id,omitempty"`
	ToolCalls        []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolCall struct {
	Index    *int               `json:"index,omitempty"` // stream deltas only
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type,omitempty"`
	Function openAIFunctionCall `json:"function"`
}

type openAIFunctionCall struct {
	Name      string  `json:"name,omitempty"`
	Arguments *string `json:"arguments,omitempty"`
}

type openAIToolOut struct {
	Type     string            `json:"type"`
	Function openAIFunctionDef `json:"function"`
}

type openAIFunctionDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Strict      *bool           `json:"strict,omitempty"`
}

// EncodeOpenAIRequest encodes req as an OpenAI chat completions request (see EncodeRequest; the dropped
// fields are not reported).
func EncodeOpenAIRequest(req Request) ([]byte, error) {
	e, err := EncodeRequest(req, ProtocolOpenAI)
	return e.Body, err
}

func encodeOpenAIRequest(req Request) ([]byte, error) {
	same := req.Source == ProtocolOpenAI
	out := openAIRequestOut{
		Model: req.Model, MaxTokens: req.MaxTokens, Temperature: req.Temperature, TopP: req.TopP,
		Stop: req.Stop, Stream: req.Stream, ParallelToolCalls: req.ParallelToolCalls,
	}
	// A converted stream always asks for usage: Anthropic streams always carry it and metering needs it.
	if req.Stream && (req.IncludeUsage || (req.Source != "" && !same)) {
		out.StreamOptions = &openAIStreamOpts{IncludeUsage: true}
	}
	for _, s := range req.System {
		out.Messages = append(out.Messages, openAIMessageOut{Role: "system", Content: mustMarshalNoEscape(s.Text)})
	}
	for _, m := range req.Messages {
		msgs, err := encodeOpenAIMessage(m, same)
		if err != nil {
			return nil, err
		}
		out.Messages = append(out.Messages, msgs...)
	}
	for _, t := range req.Tools {
		if t.Raw != nil {
			out.Tools = append(out.Tools, t.Raw) // only reachable for an OpenAI source (else unsupported)
			continue
		}
		def := openAIFunctionDef{Name: t.Name, Description: t.Description, Parameters: t.Parameters}
		if same {
			def.Strict = t.Strict
		}
		b, err := marshalNoEscape(openAIToolOut{Type: "function", Function: def})
		if err != nil {
			return nil, invalidf("tools", err.Error())
		}
		out.Tools = append(out.Tools, b)
	}
	if tc := req.ToolChoice; tc != nil {
		switch tc.Mode {
		case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
			out.ToolChoice = mustMarshalNoEscape(string(tc.Mode))
		case ToolChoiceTool:
			out.ToolChoice = mustMarshalNoEscape(map[string]any{"type": "function", "function": map[string]string{"name": tc.Name}})
		default:
			return nil, invalidf("tool_choice", "unknown mode "+string(tc.Mode))
		}
	}
	body, err := marshalNoEscape(out)
	if err != nil {
		return nil, invalidf("", err.Error())
	}
	if same && len(req.Extras) > 0 {
		return mergeExtras(body, req.Extras)
	}
	return body, nil
}

// mergeExtras adds the extra top-level fields to an encoded object (encoded fields win).
func mergeExtras(body []byte, extras map[string]json.RawMessage) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, invalidf("", err.Error())
	}
	for k, v := range extras {
		if _, ok := obj[k]; !ok {
			obj[k] = v
		}
	}
	b, err := marshalNoEscape(obj)
	if err != nil {
		return nil, invalidf("", err.Error())
	}
	return b, nil
}

// openAIParts encodes text and opaque blocks as a content value: a string for one text block, else an array
// of parts.
func openAIParts(blocks []Block) json.RawMessage {
	if len(blocks) == 1 && blocks[0].Type == BlockText {
		return mustMarshalNoEscape(blocks[0].Text)
	}
	parts := make([]json.RawMessage, 0, len(blocks))
	for _, b := range blocks {
		if b.Type == BlockOpaque {
			parts = append(parts, b.Raw)
			continue
		}
		parts = append(parts, mustMarshalNoEscape(map[string]string{"type": "text", "text": b.Text}))
	}
	return mustMarshalNoEscape(parts)
}

func encodeOpenAIMessage(m Message, same bool) ([]openAIMessageOut, error) {
	name := ""
	if same {
		name = m.Name
	}
	switch m.Role {
	case RoleSystem:
		text, _ := textOnly(m.Blocks)
		return []openAIMessageOut{{Role: "system", Content: mustMarshalNoEscape(text), Name: name}}, nil
	case RoleUser:
		var out []openAIMessageOut
		var run []Block
		flush := func() {
			if len(run) > 0 {
				out = append(out, openAIMessageOut{Role: "user", Content: openAIParts(run), Name: name})
				run = nil
			}
		}
		for _, b := range m.Blocks {
			switch b.Type {
			case BlockToolResult:
				flush()
				out = append(out, openAIMessageOut{Role: "tool", ToolCallID: b.ID, Content: mustMarshalNoEscape(b.Text)})
			case BlockText, BlockOpaque:
				run = append(run, b)
			default:
				return nil, invalidf("messages", "a user message cannot hold a "+string(b.Type)+" block")
			}
		}
		flush()
		if len(out) == 0 { // an empty user message
			out = append(out, openAIMessageOut{Role: "user", Content: mustMarshalNoEscape(""), Name: name})
		}
		return out, nil
	case RoleAssistant:
		msg := openAIMessageOut{Role: "assistant", Name: name}
		var texts []Block
		var reasoning strings.Builder
		hasReasoning := false
		for _, b := range m.Blocks {
			switch b.Type {
			case BlockText, BlockOpaque:
				texts = append(texts, b)
			case BlockThinking:
				reasoning.WriteString(b.Text)
				hasReasoning = true
			case BlockToolCall:
				args := b.Arguments
				if args == "" {
					args = "{}"
				}
				msg.ToolCalls = append(msg.ToolCalls, openAIToolCall{ID: b.ID, Type: "function",
					Function: openAIFunctionCall{Name: b.Name, Arguments: &args}})
			default:
				return nil, invalidf("messages", "an assistant message cannot hold a "+string(b.Type)+" block")
			}
		}
		switch {
		case len(texts) > 0:
			// Assistant text is one string (clients read message.content as a string); Anthropic splits one
			// answer into several text blocks only around citations, so the blocks are joined without a separator.
			if joined, ok := textOnly(texts); ok {
				msg.Content = mustMarshalNoEscape(joined)
			} else {
				msg.Content = openAIParts(texts)
			}
		case len(msg.ToolCalls) > 0:
			msg.Content = json.RawMessage("null")
		default:
			msg.Content = mustMarshalNoEscape("")
		}
		if hasReasoning {
			r := reasoning.String()
			msg.ReasoningContent = &r
		}
		return []openAIMessageOut{msg}, nil
	}
	return nil, invalidf("messages", "unknown role "+string(m.Role))
}

// --- responses ---

type openAIUsageIn struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	CachedTokens        int64 `json:"cached_tokens"` // Moonshot reports cached tokens here
	PromptTokensDetails *struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

func (u *openAIUsageIn) neutral() Usage {
	cached := u.CachedTokens
	if u.PromptTokensDetails != nil && u.PromptTokensDetails.CachedTokens > 0 {
		cached = u.PromptTokensDetails.CachedTokens
	}
	cached = min(cached, u.PromptTokens)
	return Usage{InputTokens: u.PromptTokens - cached, OutputTokens: u.CompletionTokens, CacheReadTokens: cached}
}

type openAIUsageOut struct {
	PromptTokens        int64            `json:"prompt_tokens"`
	CompletionTokens    int64            `json:"completion_tokens"`
	TotalTokens         int64            `json:"total_tokens"`
	PromptTokensDetails *openAICachedOut `json:"prompt_tokens_details,omitempty"`
}

type openAICachedOut struct {
	CachedTokens int64 `json:"cached_tokens"`
}

func openAIUsage(u Usage) *openAIUsageOut {
	out := &openAIUsageOut{PromptTokens: u.PromptTotal(), CompletionTokens: u.OutputTokens, TotalTokens: u.PromptTotal() + u.OutputTokens}
	if u.CacheReadTokens > 0 {
		out.PromptTokensDetails = &openAICachedOut{CachedTokens: u.CacheReadTokens}
	}
	return out
}

func stopFromOpenAI(finish string) StopReason {
	switch finish {
	case "length":
		return StopMaxTokens
	case "tool_calls", "function_call":
		return StopToolUse
	case "content_filter":
		return StopRefusal
	}
	return StopEnd // "stop" and anything unknown
}

func openAIFinish(r StopReason) string {
	switch r {
	case StopMaxTokens:
		return "length"
	case StopToolUse:
		return "tool_calls"
	case StopRefusal:
		return "content_filter"
	}
	return "stop"
}

type openAIResponseIn struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Created int64  `json:"created"`
	Choices []struct {
		Message struct {
			Content          json.RawMessage `json:"content"`
			ReasoningContent *string         `json:"reasoning_content"`
			Refusal          *string         `json:"refusal"`
			ToolCalls        []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason *string        `json:"finish_reason"`
		Usage        *openAIUsageIn `json:"usage"` // some providers put usage in the choice
	} `json:"choices"`
	Usage *openAIUsageIn `json:"usage"`
}

// DecodeOpenAIResponse decodes a non-stream OpenAI chat completion (the first choice).
func DecodeOpenAIResponse(body []byte) (Response, error) {
	var in openAIResponseIn
	if err := json.Unmarshal(body, &in); err != nil {
		return Response{}, invalidf("", "response is not a chat completion: "+err.Error())
	}
	if len(in.Choices) == 0 {
		return Response{}, invalidf("choices", "must be a non-empty array")
	}
	c := in.Choices[0]
	resp := Response{Source: ProtocolOpenAI, ID: in.ID, Model: in.Model, Created: in.Created}
	if rc := c.Message.ReasoningContent; rc != nil && *rc != "" {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockThinking, Text: *rc})
	}
	scratch := Request{}
	content, err := openAIContent(&scratch, c.Message.Content, "choices[0].message.content")
	if err != nil {
		return Response{}, err
	}
	for _, b := range content {
		if b.Type == BlockText && b.Text != "" {
			resp.Blocks = append(resp.Blocks, b)
		}
	}
	if r := c.Message.Refusal; r != nil && *r != "" && len(content) == 0 {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockText, Text: *r})
	}
	for _, tc := range c.Message.ToolCalls {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockToolCall, ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}
	finish := ""
	if c.FinishReason != nil {
		finish = *c.FinishReason
	}
	resp.StopReason = stopFromOpenAI(finish)
	if u := in.Usage; u != nil {
		resp.Usage, resp.HasUsage = u.neutral(), true
	} else if u := c.Usage; u != nil {
		resp.Usage, resp.HasUsage = u.neutral(), true
	}
	return resp, nil
}

type openAIResponseOut struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []openAIChoiceOut `json:"choices"`
	Usage   *openAIUsageOut   `json:"usage,omitempty"`
}

type openAIChoiceOut struct {
	Index        int              `json:"index"`
	Message      openAIMessageOut `json:"message"`
	FinishReason string           `json:"finish_reason"`
}

// EncodeOpenAIResponse encodes resp as a non-stream OpenAI chat completion. Thinking blocks become
// reasoning_content.
func EncodeOpenAIResponse(resp Response) ([]byte, error) {
	msgs, err := encodeOpenAIMessage(Message{Role: RoleAssistant, Blocks: resp.Blocks}, false)
	if err != nil {
		return nil, err
	}
	out := openAIResponseOut{ID: resp.ID, Object: "chat.completion", Created: resp.Created, Model: resp.Model,
		Choices: []openAIChoiceOut{{Message: msgs[0], FinishReason: openAIFinish(resp.StopReason)}}}
	if resp.HasUsage {
		out.Usage = openAIUsage(resp.Usage)
	}
	return marshalNoEscape(out)
}
