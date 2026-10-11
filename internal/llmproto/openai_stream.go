package llmproto

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// openAIStreamDecoder turns OpenAI chat completion chunks into neutral events.
//
// Blocks are numbered in order of appearance. reasoning_content opens a thinking block, content a text block,
// a tool call delta with an id or name opens a tool_call block; a different kind of delta closes the open
// block. Argument fragments of a tool call that is no longer the open block are a malformed stream
// (ErrBadStream): the neutral stream does not interleave blocks. The finish_reason chunk closes the open
// block; message_delta and message_stop follow at "data: [DONE]" so that the usage chunk of
// stream_options.include_usage (or usage inside the last choice, as some providers send it) is included.
type openAIStreamDecoder struct {
	started  bool
	done     bool
	open     int // index of the open block, -1 when none
	openType BlockType
	next     int
	tools    map[int]int // OpenAI tool call index → block index
	finish   *string
	usage    Usage
	hasUsage bool
}

// NewOpenAIStreamDecoder decodes an OpenAI chat completions stream.
func NewOpenAIStreamDecoder() StreamDecoder {
	return &openAIStreamDecoder{open: -1, tools: map[int]int{}}
}

func (d *openAIStreamDecoder) Done() bool { return d.done }

func (d *openAIStreamDecoder) Usage() (Usage, bool) { return d.usage, d.hasUsage }

type openAIChunkIn struct {
	ID      string           `json:"id"`
	Model   string           `json:"model"`
	Created int64            `json:"created"`
	Choices []openAIChoiceIn `json:"choices"`
	Usage   *openAIUsageIn   `json:"usage"`
	Error   *struct {
		Message string          `json:"message"`
		Type    string          `json:"type"`
		Code    json.RawMessage `json:"code"`
	} `json:"error"`
}

type openAIChoiceIn struct {
	Index int `json:"index"`
	Delta struct {
		Content          *string               `json:"content"`
		ReasoningContent *string               `json:"reasoning_content"`
		ToolCalls        []openAIToolCallDelta `json:"tool_calls"`
	} `json:"delta"`
	FinishReason *string        `json:"finish_reason"`
	Usage        *openAIUsageIn `json:"usage"` // some providers put usage in the last choice
}

type openAIToolCallDelta struct {
	Index    *int   `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func (d *openAIStreamDecoder) Decode(ev SSEEvent) ([]Event, error) {
	data := strings.TrimSpace(ev.Data)
	switch {
	case d.done || data == "":
		return nil, nil
	case data == "[DONE]":
		return d.end(), nil
	}
	var c openAIChunkIn
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		return nil, fmt.Errorf("%w: chunk is not JSON: %v", ErrBadStream, err)
	}
	if c.Error != nil {
		d.done = true
		pt := c.Error.Type
		if code := strings.Trim(string(c.Error.Code), `"`); code != "" && code != "null" {
			pt = code
		}
		return []Event{{Type: EventError, Err: &ErrorInfo{Class: ClassUpstream, Message: c.Error.Message, ProviderType: pt}}}, nil
	}
	var out []Event
	if !d.started {
		d.started = true
		out = append(out, Event{Type: EventMessageStart, ID: c.ID, Model: c.Model, Created: c.Created})
	}
	if c.Usage != nil {
		d.usage, d.hasUsage = c.Usage.neutral(), true
	}
	for _, ch := range c.Choices {
		if ch.Index != 0 {
			continue // only the first choice (n > 1 is not converted)
		}
		var err error
		if out, err = d.choice(out, ch); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// choice turns the delta of the first choice into events.
func (d *openAIStreamDecoder) choice(out []Event, ch openAIChoiceIn) ([]Event, error) {
	if ch.Usage != nil {
		d.usage, d.hasUsage = ch.Usage.neutral(), true
	}
	dl := ch.Delta
	if dl.ReasoningContent != nil && *dl.ReasoningContent != "" {
		out = d.ensure(out, BlockThinking)
		out = append(out, Event{Type: EventBlockDelta, Index: d.open, Text: *dl.ReasoningContent})
	}
	if dl.Content != nil && *dl.Content != "" {
		out = d.ensure(out, BlockText)
		out = append(out, Event{Type: EventBlockDelta, Index: d.open, Text: *dl.Content})
	}
	for pos, tc := range dl.ToolCalls {
		var err error
		if out, err = d.toolCall(out, pos, tc); err != nil {
			return nil, err
		}
	}
	if ch.FinishReason != nil && *ch.FinishReason != "" {
		out = d.close(out)
		f := *ch.FinishReason
		d.finish = &f
	}
	return out, nil
}

// toolCall turns one tool call delta into events: a new tool index opens a tool_call block.
func (d *openAIStreamDecoder) toolCall(out []Event, pos int, tc openAIToolCallDelta) ([]Event, error) {
	ti := pos
	if tc.Index != nil {
		ti = *tc.Index
	}
	bi, known := d.tools[ti]
	switch {
	case !known && tc.ID == "" && tc.Function.Name == "":
		return nil, fmt.Errorf("%w: tool call %d has no id or name", ErrBadStream, ti)
	case !known:
		out = d.close(out)
		bi = d.next
		d.next++
		d.tools[ti] = bi
		d.open, d.openType = bi, BlockToolCall
		out = append(out, Event{Type: EventBlockStart, Index: bi, Block: BlockToolCall, ToolID: tc.ID, ToolName: tc.Function.Name})
	case bi != d.open:
		return nil, fmt.Errorf("%w: interleaved tool call deltas", ErrBadStream)
	}
	if tc.Function.Arguments != "" {
		out = append(out, Event{Type: EventBlockDelta, Index: bi, Arguments: tc.Function.Arguments})
	}
	return out, nil
}

// ensure opens a block of type t unless one is open.
func (d *openAIStreamDecoder) ensure(out []Event, t BlockType) []Event {
	if d.open >= 0 && d.openType == t {
		return out
	}
	out = d.close(out)
	d.open, d.openType = d.next, t
	d.next++
	return append(out, Event{Type: EventBlockStart, Index: d.open, Block: t})
}

func (d *openAIStreamDecoder) close(out []Event) []Event {
	if d.open < 0 {
		return out
	}
	out = append(out, Event{Type: EventBlockStop, Index: d.open})
	d.open = -1
	return out
}

func (d *openAIStreamDecoder) end() []Event {
	var out []Event
	if !d.started {
		d.started = true
		out = append(out, Event{Type: EventMessageStart})
	}
	out = d.close(out)
	finish := ""
	if d.finish != nil {
		finish = *d.finish
	}
	md := Event{Type: EventMessageDelta, StopReason: stopFromOpenAI(finish)}
	if d.hasUsage {
		u := d.usage
		md.Usage = &u
	}
	d.done = true
	return append(out, md, Event{Type: EventMessageStop})
}

func (d *openAIStreamDecoder) Finish() ([]Event, error) {
	if d.done {
		return nil, nil
	}
	if d.finish != nil { // complete but without "data: [DONE]"
		return d.end(), nil
	}
	return nil, io.ErrUnexpectedEOF
}

// --- encoder ---

// OpenAIStreamOptions configures an OpenAI stream encoder.
type OpenAIStreamOptions struct {
	// IncludeUsage sends the final usage chunk (choices: []) before [DONE], as stream_options.include_usage asks.
	IncludeUsage bool
	// Created is the "created" of every chunk when message_start does not give one.
	Created int64
}

type openAIStreamEncoder struct {
	opts    OpenAIStreamOptions
	id      string
	model   string
	created int64
	tools   map[int]int // block index → tool call index
	types   map[int]BlockType
	usage   *Usage
	ended   bool
}

// NewOpenAIStreamEncoder encodes neutral events as OpenAI chat completion chunks.
func NewOpenAIStreamEncoder(opts OpenAIStreamOptions) StreamEncoder {
	return &openAIStreamEncoder{opts: opts, created: opts.Created, tools: map[int]int{}, types: map[int]BlockType{}}
}

type openAIChunkOut struct {
	ID      string              `json:"id"`
	Object  string              `json:"object"`
	Created int64               `json:"created"`
	Model   string              `json:"model"`
	Choices []openAIChunkChoice `json:"choices"`
	Usage   *openAIUsageOut     `json:"usage,omitempty"`
}

type openAIChunkChoice struct {
	Index        int         `json:"index"`
	Delta        openAIDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type openAIDelta struct {
	Role             string           `json:"role,omitempty"`
	Content          *string          `json:"content,omitempty"`
	ReasoningContent *string          `json:"reasoning_content,omitempty"`
	ToolCalls        []openAIToolCall `json:"tool_calls,omitempty"`
}

func (e *openAIStreamEncoder) chunk(d openAIDelta, finish *string) []byte {
	c := openAIChunkOut{ID: e.id, Object: "chat.completion.chunk", Created: e.created, Model: e.model,
		Choices: []openAIChunkChoice{{Delta: d, FinishReason: finish}}}
	return FormatSSE("", mustMarshalNoEscape(c))
}

func (e *openAIStreamEncoder) Encode(ev Event) ([]byte, error) {
	if e.ended {
		return nil, nil
	}
	switch ev.Type {
	case EventMessageStart:
		e.id, e.model = ev.ID, ev.Model
		if ev.Created != 0 {
			e.created = ev.Created
		}
		empty := ""
		return e.chunk(openAIDelta{Role: "assistant", Content: &empty}, nil), nil
	case EventBlockStart:
		return e.blockStart(ev), nil
	case EventBlockDelta:
		return e.blockDelta(ev), nil
	case EventMessageDelta:
		if ev.Usage != nil {
			u := *ev.Usage
			e.usage = &u
		}
		f := openAIFinish(ev.StopReason)
		return e.chunk(openAIDelta{}, &f), nil
	case EventMessageStop:
		e.ended = true
		return e.stop(), nil
	case EventError:
		e.ended = true
		info := ErrorInfo{Class: ClassUpstream, Message: "stream failed"}
		if ev.Err != nil {
			info = *ev.Err
		}
		return FormatSSE("", EncodeOpenAIError(info)), nil
	}
	return nil, nil
}

func (e *openAIStreamEncoder) blockStart(ev Event) []byte {
	e.types[ev.Index] = ev.Block
	if ev.Block != BlockToolCall {
		return nil
	}
	ti := len(e.tools)
	e.tools[ev.Index] = ti
	empty := ""
	return e.chunk(openAIDelta{ToolCalls: []openAIToolCall{{Index: &ti, ID: ev.ToolID, Type: "function",
		Function: openAIFunctionCall{Name: ev.ToolName, Arguments: &empty}}}}, nil)
}

func (e *openAIStreamEncoder) blockDelta(ev Event) []byte {
	var d openAIDelta
	switch t, a := ev.Text, ev.Arguments; e.types[ev.Index] {
	case BlockText:
		if t == "" {
			return nil
		}
		d.Content = &t
	case BlockThinking:
		if t == "" {
			return nil // signatures have no OpenAI equivalent
		}
		d.ReasoningContent = &t
	case BlockToolCall:
		if a == "" {
			return nil
		}
		ti := e.tools[ev.Index]
		d.ToolCalls = []openAIToolCall{{Index: &ti, Function: openAIFunctionCall{Arguments: &a}}}
	default:
		return nil
	}
	return e.chunk(d, nil)
}

// stop is the usage chunk (when asked for) and [DONE].
func (e *openAIStreamEncoder) stop() []byte {
	var out []byte
	if e.opts.IncludeUsage {
		u := Usage{}
		if e.usage != nil {
			u = *e.usage
		}
		c := openAIChunkOut{ID: e.id, Object: "chat.completion.chunk", Created: e.created, Model: e.model,
			Choices: []openAIChunkChoice{}, Usage: openAIUsage(u)}
		out = FormatSSE("", mustMarshalNoEscape(c))
	}
	return append(out, FormatSSE("", []byte("[DONE]"))...)
}
