package llmproto

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// anthropicStreamDecoder turns Anthropic message stream events into neutral events. Block indices are kept.
// Blocks of types the neutral format does not model (redacted thinking, server tool blocks …) and unknown
// delta types (citations) are skipped, so that usage is still read from any stream. ping is ignored.
type anthropicStreamDecoder struct {
	done     bool
	skipped  map[int]bool
	usage    Usage
	hasUsage bool
}

// NewAnthropicStreamDecoder decodes an Anthropic messages stream.
func NewAnthropicStreamDecoder() StreamDecoder {
	return &anthropicStreamDecoder{skipped: map[int]bool{}}
}

func (d *anthropicStreamDecoder) Done() bool { return d.done }

func (d *anthropicStreamDecoder) Usage() (Usage, bool) { return d.usage, d.hasUsage }

func (d *anthropicStreamDecoder) Finish() ([]Event, error) {
	if d.done {
		return nil, nil
	}
	return nil, io.ErrUnexpectedEOF
}

// anthropicUsageDelta is the usage of message_delta: output_tokens always, the other counts when present.
type anthropicUsageDelta struct {
	InputTokens              *int64 `json:"input_tokens"`
	OutputTokens             *int64 `json:"output_tokens"`
	CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     *int64 `json:"cache_read_input_tokens"`
}

type anthropicEventIn struct {
	Type    string `json:"type"`
	Message *struct {
		ID    string               `json:"id"`
		Model string               `json:"model"`
		Usage *anthropicUsageDelta `json:"usage"`
	} `json:"message"`
	Index        int `json:"index"`
	ContentBlock *struct {
		Type     string `json:"type"`
		ID       string `json:"id"`
		Name     string `json:"name"`
		Text     string `json:"text"`
		Thinking string `json:"thinking"`
	} `json:"content_block"`
	Delta *struct {
		Type         string  `json:"type"`
		Text         string  `json:"text"`
		PartialJSON  string  `json:"partial_json"`
		Thinking     string  `json:"thinking"`
		Signature    string  `json:"signature"`
		StopReason   *string `json:"stop_reason"`
		StopSequence *string `json:"stop_sequence"`
	} `json:"delta"`
	Usage *anthropicUsageDelta `json:"usage"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (d *anthropicStreamDecoder) apply(u *anthropicUsageDelta) {
	if u == nil {
		return
	}
	d.hasUsage = true
	if u.InputTokens != nil {
		d.usage.InputTokens = *u.InputTokens
	}
	if u.OutputTokens != nil {
		d.usage.OutputTokens = *u.OutputTokens
	}
	if u.CacheCreationInputTokens != nil {
		d.usage.CacheWriteTokens = *u.CacheCreationInputTokens
	}
	if u.CacheReadInputTokens != nil {
		d.usage.CacheReadTokens = *u.CacheReadInputTokens
	}
	d.usage = d.usage.clamped()
}

func (d *anthropicStreamDecoder) Decode(ev SSEEvent) ([]Event, error) {
	data := strings.TrimSpace(ev.Data)
	if d.done || data == "" {
		return nil, nil
	}
	var in anthropicEventIn
	if err := json.Unmarshal([]byte(data), &in); err != nil {
		return nil, fmt.Errorf("%w: event is not JSON: %v", ErrBadStream, err)
	}
	switch in.Type {
	case "message_start":
		if in.Message == nil {
			return nil, fmt.Errorf("%w: message_start without message", ErrBadStream)
		}
		d.apply(in.Message.Usage)
		return []Event{{Type: EventMessageStart, ID: in.Message.ID, Model: in.Message.Model, Usage: d.usageSoFar()}}, nil
	case "content_block_start":
		return d.blockStart(in)
	case "content_block_delta":
		return d.blockDelta(in), nil
	case "content_block_stop":
		if d.skipped[in.Index] {
			return nil, nil
		}
		return []Event{{Type: EventBlockStop, Index: in.Index}}, nil
	case "message_delta":
		d.apply(in.Usage)
		out := Event{Type: EventMessageDelta, StopReason: StopEnd, Usage: d.usageSoFar()}
		if in.Delta != nil && in.Delta.StopReason != nil {
			out.StopReason = stopFromAnthropic(*in.Delta.StopReason)
		}
		if in.Delta != nil && in.Delta.StopSequence != nil {
			out.StopSequence = *in.Delta.StopSequence
		}
		return []Event{out}, nil
	case "message_stop":
		d.done = true
		return []Event{{Type: EventMessageStop}}, nil
	case "error":
		d.done = true
		info := &ErrorInfo{Class: ClassUpstream, Message: "provider stream error"}
		if in.Error != nil {
			info.Message, info.ProviderType = in.Error.Message, in.Error.Type
		}
		return []Event{{Type: EventError, Err: info}}, nil
	}
	return nil, nil // ping and unknown event types
}

// usageSoFar is a copy of the usage seen so far, or nil.
func (d *anthropicStreamDecoder) usageSoFar() *Usage {
	if !d.hasUsage {
		return nil
	}
	u := d.usage
	return &u
}

func (d *anthropicStreamDecoder) blockStart(in anthropicEventIn) ([]Event, error) {
	cb := in.ContentBlock
	if cb == nil {
		return nil, fmt.Errorf("%w: content_block_start without content_block", ErrBadStream)
	}
	types := map[string]BlockType{"text": BlockText, "thinking": BlockThinking, "tool_use": BlockToolCall}
	t, ok := types[cb.Type]
	if !ok {
		d.skipped[in.Index] = true
		return nil, nil
	}
	out := []Event{{Type: EventBlockStart, Index: in.Index, Block: t, ToolID: cb.ID, ToolName: cb.Name}}
	if initial := cb.Text + cb.Thinking; initial != "" {
		out = append(out, Event{Type: EventBlockDelta, Index: in.Index, Text: initial})
	}
	return out, nil
}

func (d *anthropicStreamDecoder) blockDelta(in anthropicEventIn) []Event {
	if d.skipped[in.Index] || in.Delta == nil {
		return nil
	}
	ev := Event{Type: EventBlockDelta, Index: in.Index}
	switch in.Delta.Type {
	case "text_delta":
		ev.Text = in.Delta.Text
	case "input_json_delta":
		ev.Arguments = in.Delta.PartialJSON
	case "thinking_delta":
		ev.Text = in.Delta.Thinking
	case "signature_delta":
		ev.Signature = in.Delta.Signature
	default:
		return nil // citations and unknown deltas
	}
	return []Event{ev}
}

// --- encoder ---

// AnthropicStreamOptions configures an Anthropic stream encoder.
type AnthropicStreamOptions struct {
	// Source is the format of the provider whose events are encoded. Thinking blocks from an OpenAI provider
	// (reasoning_content) have no signature and are left out.
	Source Protocol
	// Model is the message's model when message_start does not give one (e.g. the request's model). An
	// empty message id is replaced by a generated "msg_…" id.
	Model string
}

type anthropicStreamEncoder struct {
	opts  AnthropicStreamOptions
	index map[int]int // source block index → output index (absent: block left out)
	types map[int]BlockType
	next  int
	ended bool
}

// NewAnthropicStreamEncoder encodes neutral events as Anthropic message stream events. Output block indices
// are renumbered from 0 in order of block_start.
func NewAnthropicStreamEncoder(opts AnthropicStreamOptions) StreamEncoder {
	return &anthropicStreamEncoder{opts: opts, index: map[int]int{}, types: map[int]BlockType{}}
}

func anthropicFrame(v any) []byte {
	b := mustMarshalNoEscape(v)
	var head struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(b, &head)
	return FormatSSE(head.Type, b)
}

type anthropicMessageStartOut struct {
	Type    string                `json:"type"`
	Message anthropicStartMessage `json:"message"`
}

type anthropicStartMessage struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Role         string            `json:"role"`
	Model        string            `json:"model"`
	Content      []json.RawMessage `json:"content"`
	StopReason   *string           `json:"stop_reason"`
	StopSequence *string           `json:"stop_sequence"`
	Usage        anthropicUsage    `json:"usage"`
}

type anthropicBlockStartOut struct {
	Type         string         `json:"type"`
	Index        int            `json:"index"`
	ContentBlock map[string]any `json:"content_block"`
}

type anthropicDeltaOut struct {
	Type  string            `json:"type"`
	Index int               `json:"index"`
	Delta map[string]string `json:"delta"`
}

type anthropicIndexOut struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type anthropicMessageDeltaOut struct {
	Type  string `json:"type"`
	Delta struct {
		StopReason   string  `json:"stop_reason"`
		StopSequence *string `json:"stop_sequence"`
	} `json:"delta"`
	Usage anthropicUsage `json:"usage"`
}

func (e *anthropicStreamEncoder) Encode(ev Event) ([]byte, error) {
	if e.ended {
		return nil, nil
	}
	switch ev.Type {
	case EventMessageStart:
		m := anthropicStartMessage{ID: ev.ID, Type: "message", Role: "assistant", Model: ev.Model, Content: []json.RawMessage{}}
		if m.ID == "" {
			m.ID = "msg_" + randomHex()
		}
		if m.Model == "" {
			m.Model = e.opts.Model
		}
		if ev.Usage != nil {
			m.Usage = anthropicUsageOf(*ev.Usage)
			m.Usage.OutputTokens = 0
		}
		return anthropicFrame(anthropicMessageStartOut{Type: "message_start", Message: m}), nil
	case EventBlockStart:
		return e.blockStart(ev), nil
	case EventBlockDelta:
		return e.blockDelta(ev), nil
	case EventBlockStop:
		out, ok := e.index[ev.Index]
		if !ok {
			return nil, nil
		}
		return anthropicFrame(anthropicIndexOut{Type: "content_block_stop", Index: out}), nil
	case EventMessageDelta:
		md := anthropicMessageDeltaOut{Type: "message_delta"}
		md.Delta.StopReason = anthropicStop(ev.StopReason)
		if ev.StopSequence != "" {
			s := ev.StopSequence
			md.Delta.StopSequence = &s
		}
		if ev.Usage != nil {
			md.Usage = anthropicUsageOf(*ev.Usage)
		}
		return anthropicFrame(md), nil
	case EventMessageStop:
		e.ended = true
		return anthropicFrame(map[string]string{"type": "message_stop"}), nil
	case EventError:
		e.ended = true
		info := ErrorInfo{Class: ClassUpstream, Message: "stream failed"}
		if ev.Err != nil {
			info = *ev.Err
		}
		return FormatSSE("error", EncodeAnthropicError(info)), nil
	}
	return nil, nil
}

// blockStart opens an output block (renumbered from 0); thinking from an OpenAI provider is left out.
func (e *anthropicStreamEncoder) blockStart(ev Event) []byte {
	var cb map[string]any
	switch {
	case ev.Block == BlockText:
		cb = map[string]any{"type": "text", "text": ""}
	case ev.Block == BlockThinking && e.opts.Source != ProtocolOpenAI:
		cb = map[string]any{"type": "thinking", "thinking": "", "signature": ""}
	case ev.Block == BlockToolCall:
		cb = map[string]any{"type": "tool_use", "id": AnthropicToolID(ev.ToolID), "name": ev.ToolName, "input": map[string]any{}}
	default:
		return nil
	}
	out := e.next
	e.next++
	e.index[ev.Index] = out
	e.types[ev.Index] = ev.Block
	return anthropicFrame(anthropicBlockStartOut{Type: "content_block_start", Index: out, ContentBlock: cb})
}

func (e *anthropicStreamEncoder) blockDelta(ev Event) []byte {
	out, ok := e.index[ev.Index]
	if !ok {
		return nil
	}
	delta := func(kind, key, value string) []byte {
		if value == "" {
			return nil
		}
		return anthropicFrame(anthropicDeltaOut{Type: "content_block_delta", Index: out, Delta: map[string]string{"type": kind, key: value}})
	}
	switch e.types[ev.Index] {
	case BlockText:
		return delta("text_delta", "text", ev.Text)
	case BlockThinking:
		return append(delta("thinking_delta", "thinking", ev.Text), delta("signature_delta", "signature", ev.Signature)...)
	case BlockToolCall:
		return delta("input_json_delta", "partial_json", ev.Arguments)
	}
	return nil
}
