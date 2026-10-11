package llmproto

import (
	"encoding/json"
	"slices"
	"strings"
)

// openAIFields are the accepted top-level fields of an OpenAI chat request: the fields that
// internal/gateway/upstream accepts for sandbox calls (whose own allowlist stays strict), plus the
// client-side fields of the Chat Completions API. Fields without a neutral equivalent are kept in
// Request.Extras (openAIExtraFields). Unknown fields (and the deprecated functions / function_call) are an
// *UnsupportedFieldError.
var openAIFields = append([]string{
	"model", "messages", "max_tokens", "max_completion_tokens", "temperature", "top_p", "stop", "stream",
	"tools", "tool_choice", "stream_options", "parallel_tool_calls",
}, openAIExtraFields...)

// openAIExtraFields are the top-level fields without a neutral equivalent, kept in Request.Extras and
// classified by openAIExtra. max_completion_tokens is also kept, so that a same-format encoding uses that
// name.
var openAIExtraFields = []string{"max_completion_tokens", "response_format", "seed", "presence_penalty", "frequency_penalty",
	"user", "reasoning_effort", "n", "logprobs", "top_logprobs", "metadata", "store", "logit_bias", "service_tier",
	"prediction", "modalities", "audio", "web_search_options", "verbosity", "prompt_cache_key", "safety_identifier"}

// openAIMessageFields are the accepted message fields: those of gateway/upstream plus the assistant fields
// clients echo back from a previous response (openAIEchoedFields).
var openAIMessageFields = append([]string{"role", "content", "name", "tool_call_id", "tool_calls", "reasoning_content"},
	openAIEchoedFields...)

// openAIEchoedFields are assistant message fields that the openai SDK echoes back when a client appends a
// response message to its history. Null or empty values are ignored; others are kept in Message.Extras and
// pin the request to OpenAI providers.
var openAIEchoedFields = []string{"refusal", "annotations", "audio", "function_call"}

// openAIExtra classifies a top-level OpenAI field without a neutral equivalent. ok is false for values that
// are no-ops in the other format (n=1, logprobs=false, response_format text) and for max_completion_tokens,
// which is mapped to MaxTokens.
func openAIExtra(field string, raw json.RawMessage) (kind FeatureKind, ok bool) {
	switch field {
	case "max_completion_tokens":
		return "", false
	case "presence_penalty", "frequency_penalty", "user":
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
	// seed, reasoning_effort, top_logprobs, metadata, store, logit_bias, service_tier, prediction, …
	return FeatureUnsupported, !isNull(raw) && !isEmptyJSON(raw)
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
	f := &fields{obj: obj}
	f.str("model", &req.Model)
	f.positive("max_tokens", &req.MaxTokens)
	var maxCompletion int64
	f.positive("max_completion_tokens", &maxCompletion)
	if maxCompletion > 0 && (req.MaxTokens == 0 || maxCompletion < req.MaxTokens) {
		req.MaxTokens = maxCompletion // both given: the smaller one
	}
	f.number("temperature", &req.Temperature)
	f.number("top_p", &req.TopP)
	f.boolean("stream", &req.Stream)
	f.boolPtr("parallel_tool_calls", &req.ParallelToolCalls)
	err = steps(
		func() error { return f.err },
		func() error { return checkMaxTokens(req.MaxTokens) },
		func() error { openAISampling(&req); return nil },
		func() error { return decodeOpenAIStop(&req, obj["stop"]) },
		func() error { return decodeStreamOptions(&req, obj["stream_options"]) },
		func() error { return decodeOpenAITools(&req, obj["tools"]) },
		func() error { return decodeOpenAIToolChoice(&req, obj["tool_choice"]) },
		func() error { recordExtras(&req, obj, openAIExtraFields, openAIExtra); return nil },
		func() error { return decodeOpenAIMessages(&req, obj["messages"]) },
	)
	if err != nil {
		return Request{}, err
	}
	return req, nil
}

// checkMaxTokens refuses max_tokens above MaxTokensLimit.
func checkMaxTokens(n int64) error {
	if n > MaxTokensLimit {
		return invalidf("max_tokens", "must be at most 1048576")
	}
	return nil
}

// openAISampling records the sampling settings Anthropic cannot take: temperature above 1 (Anthropic's
// range is [0, 1]) pins the request to OpenAI; with both temperature and top_p, top_p is dropped (current
// Claude models refuse both).
func openAISampling(req *Request) {
	if req.Temperature != nil && *req.Temperature > 1 {
		req.feature("temperature", FeatureUnsupported)
	}
	if req.Temperature != nil && req.TopP != nil {
		req.feature("top_p", FeatureDrop)
	}
}

func decodeOpenAIStop(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	if isString(raw) {
		s, err := str(raw, "stop")
		req.Stop = []string{s}
		return err
	}
	if json.Unmarshal(raw, &req.Stop) != nil {
		return invalidf("stop", "must be a string or an array of strings")
	}
	return nil
}

func decodeStreamOptions(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	so, err := object(raw, "stream_options")
	if err != nil {
		return err
	}
	if err := checkAllowed(so, "stream_options", "include_usage", "include_obfuscation"); err != nil {
		return err
	}
	if v, ok := so["include_obfuscation"]; ok && !isNull(v) {
		req.feature("stream_options.include_obfuscation", FeatureDrop) // not re-emitted in any format
	}
	f := &fields{obj: so, prefix: "stream_options"}
	f.boolean("include_usage", &req.IncludeUsage)
	return f.err
}

func decodeOpenAITools(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	items, err := array(raw, "tools")
	if err != nil {
		return err
	}
	for i, it := range items {
		if err := decodeOpenAITool(req, it, idx("tools", i)); err != nil {
			return err
		}
	}
	return nil
}

func decodeOpenAITool(req *Request, raw json.RawMessage, field string) error {
	obj, err := object(raw, field)
	if err != nil {
		return err
	}
	typ, err := str(obj["type"], field+".type")
	if err != nil {
		return err
	}
	if typ != "function" { // custom tools and the like
		req.Tools = append(req.Tools, Tool{Raw: clone(raw)})
		req.feature(field, FeatureUnsupported)
		return nil
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
	f := &fields{obj: fn, prefix: field + ".function"}
	f.name("name", &t.Name)
	f.str("description", &t.Description)
	if _, ok := f.get("parameters"); ok {
		f.schema("parameters", &t.Parameters)
	}
	f.boolPtr("strict", &t.Strict)
	if f.err != nil {
		return f.err
	}
	if t.Strict != nil {
		req.feature(field+".function.strict", FeatureDrop)
	}
	req.Tools = append(req.Tools, t)
	return nil
}

func decodeOpenAIToolChoice(req *Request, raw json.RawMessage) error {
	if isNull(raw) {
		return nil
	}
	bad := invalidf("tool_choice", "must be auto, none, required or a function")
	if isString(raw) {
		s, _ := str(raw, "tool_choice")
		switch s {
		case "auto":
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceAuto}
		case "none":
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceNone}
		case "required":
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceRequired}
		default:
			return bad
		}
		return nil
	}
	var tc struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if json.Unmarshal(raw, &tc) != nil || tc.Type != "function" || tc.Function.Name == "" {
		return bad
	}
	req.ToolChoice = &ToolChoice{Mode: ToolChoiceTool, Name: tc.Function.Name}
	return nil
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
			Type string `json:"type"`
		}
		if json.Unmarshal(p, &part) != nil || part.Type == "" {
			return nil, invalidf(pf, "must be a content part with a type")
		}
		if part.Type != "text" {
			out = append(out, Block{Type: BlockOpaque, Raw: clone(p)})
			req.feature(pf, FeatureUnsupported)
			continue
		}
		obj, _ := object(p, pf)
		s, err := str(obj["text"], pf+".text")
		if err != nil {
			return nil, err
		}
		out = append(out, Block{Type: BlockText, Text: s})
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

// openAIMessages decodes the messages array. Leading system (or developer) messages become the system
// prompt; consecutive tool messages become one user message of tool_result blocks.
type openAIMessages struct {
	req      *Request
	leading  bool // still in the leading system messages
	lastTool bool // the previous message was a tool message
}

func decodeOpenAIMessages(req *Request, raw json.RawMessage) error {
	items, err := array(raw, "messages")
	if err != nil || len(items) == 0 {
		return invalidf("messages", "must be a non-empty array")
	}
	d := &openAIMessages{req: req, leading: true}
	for i, it := range items {
		if err := d.message(it, idx("messages", i)); err != nil {
			return err
		}
	}
	// Only system messages, or only empty assistant turns (which are left out for Anthropic): valid for
	// OpenAI, but the Anthropic encoding would have no message.
	if !slices.ContainsFunc(req.Messages, func(m Message) bool { return m.Role != RoleAssistant || len(m.Blocks) > 0 }) {
		req.feature("messages", FeatureUnsupported)
	}
	return nil
}

func (d *openAIMessages) message(raw json.RawMessage, field string) error {
	m, err := object(raw, field)
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
	if err := checkRoleFields(m, role, field); err != nil {
		return err
	}
	name := ""
	f := &fields{obj: m, prefix: field}
	f.str("name", &name)
	if f.err != nil {
		return f.err
	}
	if name != "" {
		d.req.feature(field+".name", FeatureDrop)
	}
	extras := echoedExtras(m)
	for _, k := range openAIEchoedFields {
		if _, ok := extras[k]; ok && role != "assistant" {
			return unsupported(field+"."+k, "only for assistant messages")
		}
	}
	tool := false
	switch role {
	case "system", "developer":
		err = d.system(m, field, name)
	case "user":
		err = d.user(m, field, name)
	case "assistant":
		err = d.assistant(m, field, name, extras)
	case "tool":
		err, tool = d.tool(m, field), true
	default:
		return unsupported(field+".role", "role "+role+" is not supported")
	}
	d.lastTool = tool
	if role != "system" && role != "developer" {
		d.leading = false
	}
	return err
}

func (d *openAIMessages) assistant(m map[string]json.RawMessage, field, name string, extras map[string]json.RawMessage) error {
	msg, err := decodeOpenAIAssistant(d.req, m, field)
	if err != nil {
		return err
	}
	msg.Name, msg.Extras = name, extras
	for _, k := range openAIEchoedFields {
		if _, ok := extras[k]; ok {
			d.req.feature(field+"."+k, FeatureUnsupported)
		}
	}
	if len(msg.Blocks) == 0 { // content "" or null without tool calls: Anthropic rejects an empty turn
		d.req.feature(field, FeatureDrop)
	}
	d.req.Messages = append(d.req.Messages, msg)
	return nil
}

// echoedExtras returns the echoed assistant fields (openAIEchoedFields) that are neither null nor empty.
func echoedExtras(m map[string]json.RawMessage) map[string]json.RawMessage {
	var out map[string]json.RawMessage
	for _, k := range openAIEchoedFields {
		v, ok := m[k]
		if !ok || isNull(v) || isEmptyJSON(v) {
			continue
		}
		if out == nil {
			out = map[string]json.RawMessage{}
		}
		out[k] = clone(v)
	}
	return out
}

// checkRoleFields rejects message fields that belong to another role.
func checkRoleFields(m map[string]json.RawMessage, role, field string) error {
	if _, ok := m["reasoning_content"]; ok && role != "assistant" {
		return unsupported(field+".reasoning_content", "only for assistant messages")
	}
	if _, ok := m["tool_calls"]; ok && role != "assistant" {
		return invalidf(field+".tool_calls", "only for assistant messages")
	}
	if _, ok := m["tool_call_id"]; ok && role != "tool" {
		return invalidf(field+".tool_call_id", "only for tool messages")
	}
	return nil
}

func (d *openAIMessages) system(m map[string]json.RawMessage, field, name string) error {
	blocks, err := openAIContent(d.req, m["content"], field+".content")
	if err != nil {
		return err
	}
	text, ok := textOnly(blocks)
	if !ok || len(blocks) == 0 {
		return invalidf(field+".content", "system content must be text")
	}
	block := Block{Type: BlockText, Text: text}
	if d.leading {
		d.req.System = append(d.req.System, block)
		return nil
	}
	d.req.Messages = append(d.req.Messages, Message{Role: RoleSystem, Blocks: []Block{block}, Name: name})
	d.req.feature(field+".role", FeatureUnsupported)
	return nil
}

func (d *openAIMessages) user(m map[string]json.RawMessage, field, name string) error {
	blocks, err := openAIContent(d.req, m["content"], field+".content")
	if err != nil {
		return err
	}
	if blocks == nil {
		return invalidf(field+".content", "must be a string or an array of content parts")
	}
	d.req.Messages = append(d.req.Messages, Message{Role: RoleUser, Blocks: blocks, Name: name})
	return nil
}

func (d *openAIMessages) tool(m map[string]json.RawMessage, field string) error {
	id, err := str(m["tool_call_id"], field+".tool_call_id")
	if err != nil || id == "" {
		return invalidf(field+".tool_call_id", "must be a non-empty string")
	}
	blocks, err := openAIContent(d.req, m["content"], field+".content")
	if err != nil {
		return err
	}
	text, ok := textOnly(blocks)
	if !ok {
		return unsupported(field+".content", "tool results must be text")
	}
	b := Block{Type: BlockToolResult, ID: id, Text: text}
	if d.lastTool { // consecutive tool messages: one user turn
		last := &d.req.Messages[len(d.req.Messages)-1]
		last.Blocks = append(last.Blocks, b)
		return nil
	}
	d.req.Messages = append(d.req.Messages, Message{Role: RoleUser, Blocks: []Block{b}})
	return nil
}

func decodeOpenAIAssistant(req *Request, m map[string]json.RawMessage, field string) (Message, error) {
	msg := Message{Role: RoleAssistant}
	f := &fields{obj: m, prefix: field}
	var reasoning string
	f.str("reasoning_content", &reasoning)
	if f.err != nil {
		return Message{}, f.err
	}
	if _, ok := f.get("reasoning_content"); ok {
		msg.Blocks = append(msg.Blocks, Block{Type: BlockThinking, Text: reasoning})
		req.feature(field+".reasoning_content", FeatureUnsupported)
	}
	content, err := openAIContent(req, m["content"], field+".content")
	if err != nil {
		return Message{}, err
	}
	var calls []json.RawMessage
	if raw, ok := f.get("tool_calls"); ok {
		if calls, err = array(raw, field+".tool_calls"); err != nil {
			return Message{}, err
		}
	}
	// Empty text is left out (an assistant message that only calls tools has content "" or null).
	content = slices.DeleteFunc(content, func(b Block) bool { return b.Type == BlockText && b.Text == "" })
	if _, ok := m["content"]; !ok && len(calls) == 0 {
		return Message{}, invalidf(field+".content", "must be given when there are no tool_calls")
	}
	msg.Blocks = append(msg.Blocks, content...)
	for j, c := range calls {
		b, err := decodeOpenAIToolCall(req, c, idx(field+".tool_calls", j))
		if err != nil {
			return Message{}, err
		}
		msg.Blocks = append(msg.Blocks, b)
	}
	return msg, nil
}

func decodeOpenAIToolCall(req *Request, raw json.RawMessage, field string) (Block, error) {
	var call struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function *struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"function"`
	}
	if json.Unmarshal(raw, &call) != nil || call.Function == nil {
		return Block{}, invalidf(field, "must be a function tool call")
	}
	if call.Type != "" && call.Type != "function" {
		return Block{}, unsupported(field+".type", "only function tool calls are supported")
	}
	if call.ID == "" || call.Function.Name == "" {
		return Block{}, invalidf(field, "id and function.name must be non-empty")
	}
	args, err := optStr(call.Function.Arguments, field+".function.arguments")
	if err != nil {
		return Block{}, err
	}
	if strings.TrimSpace(args) == "" {
		args = "{}" // a call without arguments
	} else if !isJSONObject(args) { // e.g. cut by max_tokens: OpenAI replays it, Anthropic needs an object
		req.feature(field+".function.arguments", FeatureUnsupported)
	}
	return Block{Type: BlockToolCall, ID: call.ID, Name: call.Function.Name, Arguments: args}, nil
}
