package llmproto

import (
	"encoding/json"
	"strings"
)

// OpenAI request encoding and response decoding/encoding; request decoding is in openai_request.go.

type openAIRequestOut struct {
	Model             string             `json:"model,omitempty"`
	Messages          []openAIMessageOut `json:"messages"`
	MaxTokens         int64              `json:"max_tokens,omitempty"`
	MaxCompletion     int64              `json:"max_completion_tokens,omitempty"`
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
	// Echoed assistant fields (Message.Extras), re-emitted in a same-format encoding only.
	Refusal      json.RawMessage `json:"refusal,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
	Audio        json.RawMessage `json:"audio,omitempty"`
	FunctionCall json.RawMessage `json:"function_call,omitempty"`
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
		Stop: req.Stop, Stream: req.Stream,
	}
	if len(req.Tools) > 0 || same { // OpenAI refuses parallel_tool_calls without tools
		out.ParallelToolCalls = req.ParallelToolCalls
	}
	if _, ok := req.Extras["max_completion_tokens"]; ok && same { // keep the client's field name
		out.MaxTokens, out.MaxCompletion = 0, req.MaxTokens
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
	var err error
	if out.Tools, err = openAITools(req.Tools, same); err != nil {
		return nil, err
	}
	if out.ToolChoice, err = openAIToolChoiceOf(req.ToolChoice); err != nil {
		return nil, err
	}
	if len(req.Tools) == 0 && !same { // OpenAI refuses tool_choice without tools
		out.ToolChoice = nil
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

func openAITools(tools []Tool, same bool) ([]json.RawMessage, error) {
	var out []json.RawMessage
	for _, t := range tools {
		if t.Raw != nil {
			out = append(out, t.Raw) // only reachable for an OpenAI source (else unsupported)
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
		out = append(out, b)
	}
	return out, nil
}

func openAIToolChoiceOf(tc *ToolChoice) (json.RawMessage, error) {
	if tc == nil {
		return nil, nil
	}
	switch tc.Mode {
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
		return mustMarshalNoEscape(string(tc.Mode)), nil
	case ToolChoiceTool:
		return mustMarshalNoEscape(map[string]any{"type": "function", "function": map[string]string{"name": tc.Name}}), nil
	}
	return nil, invalidf("tool_choice", "unknown mode "+string(tc.Mode))
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
		return openAIUserMessages(m.Blocks, name)
	case RoleAssistant:
		msg, err := openAIAssistantMessage(m.Blocks)
		msg.Name = name
		if same {
			msg.Refusal, msg.Annotations, msg.Audio, msg.FunctionCall = m.Extras["refusal"], m.Extras["annotations"],
				m.Extras["audio"], m.Extras["function_call"]
		}
		return []openAIMessageOut{msg}, err
	}
	return nil, invalidf("messages", "unknown role "+string(m.Role))
}

// openAIUserMessages encodes a user turn: each tool result becomes a tool message, each run of text (and
// opaque) blocks a user message.
func openAIUserMessages(blocks []Block, name string) ([]openAIMessageOut, error) {
	var out []openAIMessageOut
	var run []Block
	flush := func() {
		if len(run) > 0 {
			out = append(out, openAIMessageOut{Role: "user", Content: openAIParts(run), Name: name})
			run = nil
		}
	}
	for _, b := range blocks {
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
}

// openAIAssistantMessage encodes an assistant turn: text as content, thinking as reasoning_content, tool
// calls as tool_calls.
func openAIAssistantMessage(blocks []Block) (openAIMessageOut, error) {
	msg := openAIMessageOut{Role: "assistant"}
	var texts []Block
	var reasoning *string
	for _, b := range blocks {
		switch b.Type {
		case BlockText, BlockOpaque:
			texts = append(texts, b)
		case BlockThinking:
			r := b.Text
			if reasoning != nil {
				r = *reasoning + r
			}
			reasoning = &r
		case BlockToolCall:
			args := b.Arguments
			if args == "" {
				args = "{}"
			}
			msg.ToolCalls = append(msg.ToolCalls, openAIToolCall{ID: b.ID, Type: "function",
				Function: openAIFunctionCall{Name: b.Name, Arguments: &args}})
		default:
			return openAIMessageOut{}, invalidf("messages", "an assistant message cannot hold a "+string(b.Type)+" block")
		}
	}
	msg.ReasoningContent = reasoning
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
	return msg, nil
}

// --- responses ---

type openAIUsageIn struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	CachedTokens        int64 `json:"cached_tokens"` // Moonshot reports cached tokens here
	PromptTokensDetails *struct {
		CachedTokens     int64 `json:"cached_tokens"`
		CacheWriteTokens int64 `json:"cache_write_tokens"`
	} `json:"prompt_tokens_details"`
}

// neutral splits prompt_tokens into uncached input, cache reads and cache writes; negative counts are 0.
func (u *openAIUsageIn) neutral() Usage {
	prompt := max(u.PromptTokens, 0)
	cached, written := u.CachedTokens, int64(0)
	if d := u.PromptTokensDetails; d != nil {
		if d.CachedTokens > 0 {
			cached = d.CachedTokens
		}
		written = d.CacheWriteTokens
	}
	cached = min(max(cached, 0), prompt)
	written = min(max(written, 0), prompt-cached)
	return Usage{InputTokens: prompt - cached - written, OutputTokens: max(u.CompletionTokens, 0),
		CacheReadTokens: cached, CacheWriteTokens: written}
}

type openAIUsageOut struct {
	PromptTokens        int64            `json:"prompt_tokens"`
	CompletionTokens    int64            `json:"completion_tokens"`
	TotalTokens         int64            `json:"total_tokens"`
	PromptTokensDetails *openAICachedOut `json:"prompt_tokens_details,omitempty"`
}

type openAICachedOut struct {
	CachedTokens     int64 `json:"cached_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens,omitempty"`
}

func openAIUsage(u Usage) *openAIUsageOut {
	u = u.clamped()
	out := &openAIUsageOut{PromptTokens: u.PromptTotal(), CompletionTokens: u.OutputTokens, TotalTokens: u.Total()}
	if u.CacheReadTokens > 0 || u.CacheWriteTokens > 0 {
		out.PromptTokensDetails = &openAICachedOut{CachedTokens: u.CacheReadTokens, CacheWriteTokens: u.CacheWriteTokens}
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
		return Response{}, badResponse("", "not a chat completion: "+err.Error())
	}
	if len(in.Choices) == 0 {
		return Response{}, badResponse("choices", "must be a non-empty array")
	}
	c := in.Choices[0]
	resp := Response{Source: ProtocolOpenAI, ID: in.ID, Model: in.Model, Created: in.Created}
	if rc := c.Message.ReasoningContent; rc != nil && *rc != "" {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockThinking, Text: *rc})
	}
	scratch := Request{}
	content, err := openAIContent(&scratch, c.Message.Content, "choices[0].message.content")
	if err != nil {
		return Response{}, badResponse("choices[0].message.content", err.Error())
	}
	for _, b := range content {
		if b.Type == BlockText && b.Text != "" {
			resp.Blocks = append(resp.Blocks, b)
		}
	}
	if r := c.Message.Refusal; r != nil && *r != "" && len(content) == 0 {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockText, Text: *r})
	}
	for i, tc := range c.Message.ToolCalls {
		args := tc.Function.Arguments
		if strings.TrimSpace(args) == "" {
			args = "{}"
		} else if !isJSONObject(args) { // e.g. cut by max_tokens
			resp.Repaired = append(resp.Repaired, idx("choices[0].message.tool_calls", i)+".function.arguments")
		}
		resp.Blocks = append(resp.Blocks, Block{Type: BlockToolCall, ID: tc.ID, Name: tc.Function.Name, Arguments: args})
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
// reasoning_content; opaque blocks of another format (redacted thinking, server tool blocks) are left out.
func EncodeOpenAIResponse(resp Response) ([]byte, error) {
	blocks := make([]Block, 0, len(resp.Blocks))
	for _, b := range resp.Blocks {
		if b.Type == BlockOpaque && resp.Source != ProtocolOpenAI {
			continue
		}
		blocks = append(blocks, b)
	}
	msgs, err := encodeOpenAIMessage(Message{Role: RoleAssistant, Blocks: blocks}, false)
	if err != nil {
		return nil, badResponse("", err.Error())
	}
	out := openAIResponseOut{ID: resp.ID, Object: "chat.completion", Created: resp.Created, Model: resp.Model,
		Choices: []openAIChoiceOut{{Message: msgs[0], FinishReason: openAIFinish(resp.StopReason)}}}
	if resp.HasUsage {
		out.Usage = openAIUsage(resp.Usage)
	}
	return marshalNoEscape(out)
}
