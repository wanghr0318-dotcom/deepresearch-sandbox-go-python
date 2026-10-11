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
}

func (d *anthropicStreamDecoder) Decode(ev SSEEvent) ([]Event, error) {
	if d.done {
		return nil, nil
	}
	data := strings.TrimSpace(ev.Data)
	if data == "" {
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
		out := Event{Type: EventMessageStart, ID: in.Message.ID, Model: in.Message.Model}
		if d.hasUsage {
			u := d.usage
			out.Usage = &u
		}
		return []Event{out}, nil
	case "content_block_start":
		cb := in.ContentBlock
		if cb == nil {
			return nil, fmt.Errorf("%w: content_block_start without content_block", ErrBadStream)
		}
		var out []Event
		switch cb.Type {
		case "text":
			out = append(out, Event{Type: EventBlockStart, Index: in.Index, Block: BlockText})
			if cb.Text != "" {
				out = append(out, Event{Type: EventBlockDelta, Index: in.Index, Text: cb.Text})
			}
		case "thinking":
			out = append(out, Event{Type: EventBlockStart, Index: in.Index, Block: BlockThinking})
			if cb.Thinking != "" {
				out = append(out, Event{Type: EventBlockDelta, Index: in.Index, Text: cb.Thinking})
			}
		case "tool_use":
			out = append(out, Event{Type: EventBlockStart, Index: in.Index, Block: BlockToolCall, ToolID: cb.ID, ToolName: cb.Name})
		default:
			d.skipped[in.Index] = true
		}
		return out, nil
	case "content_block_delta":
		if d.skipped[in.Index] || in.Delta == nil {
			return nil, nil
		}
		switch in.Delta.Type {
		case "text_delta":
			return []Event{{Type: EventBlockDelta, Index: in.Index, Text: in.Delta.Text}}, nil
		case "input_json_delta":
			return []Event{{Type: EventBlockDelta, Index: in.Index, Arguments: in.Delta.PartialJSON}}, nil
		case "thinking_delta":
			return []Event{{Type: EventBlockDelta, Index: in.Index, Text: in.Delta.Thinking}}, nil
		case "signature_delta":
			return []Event{{Type: EventBlockDelta, Index: in.Index, Signature: in.Delta.Signature}}, nil
		}
		return nil, nil
	case "content_block_stop":
		if d.skipped[in.Index] {
			return nil, nil
		}
		return []Event{{Type: EventBlockStop, Index: in.Index}}, nil
	case "message_delta":
		d.apply(in.Usage)
		out := Event{Type: EventMessageDelta, StopReason: StopEnd}
		if in.Delta != nil {
			if in.Delta.StopReason != nil {
				out.StopReason = stopFromAnthropic(*in.Delta.StopReason)
			}
			if in.Delta.StopSequence != nil {
				out.StopSequence = *in.Delta.StopSequence
			}
		}
		if d.hasUsage {
			u := d.usage
			out.Usage = &u
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

// --- encoder ---

// AnthropicStreamOptions configures an Anthropic stream encoder.
type AnthropicStreamOptions struct {
	// Source is the format of the provider whose events are encoded. Thinking blocks from an OpenAI provider
	// (reasoning_content) have no signature and are left out.
	Source Protocol
}

type anthropicStreamEncoder struct {
	opts    AnthropicStreamOptions
	index   map[int]int // source block index → output index
	skipped map[int]bool
	types   map[int]BlockType
	next    int
	ended   bool
}

// NewAnthropicStreamEncoder encodes neutral events as Anthropic message stream events. Output block indices
// are renumbered from 0 in order of block_start.
func NewAnthropicStreamEncoder(opts AnthropicStreamOptions) StreamEncoder {
	return &anthropicStreamEncoder{opts: opts, index: map[int]int{}, skipped: map[int]bool{}, types: map[int]BlockType{}}
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
		if ev.Usage != nil {
			m.Usage = anthropicUsageOf(*ev.Usage)
			m.Usage.OutputTokens = 0
		}
		return anthropicFrame(anthropicMessageStartOut{Type: "message_start", Message: m}), nil
	case EventBlockStart:
		if ev.Block == BlockThinking && e.opts.Source == ProtocolOpenAI {
			e.skipped[ev.Index] = true
			return nil, nil
		}
		var cb map[string]any
		switch ev.Block {
		case BlockText:
			cb = map[string]any{"type": "text", "text": ""}
		case BlockThinking:
			cb = map[string]any{"type": "thinking", "thinking": "", "signature": ""}
		case BlockToolCall:
			cb = map[string]any{"type": "tool_use", "id": ev.ToolID, "name": ev.ToolName, "input": map[string]any{}}
		default:
			e.skipped[ev.Index] = true
			return nil, nil
		}
		out := e.next
		e.next++
		e.index[ev.Index] = out
		e.types[ev.Index] = ev.Block
		return anthropicFrame(anthropicBlockStartOut{Type: "content_block_start", Index: out, ContentBlock: cb}), nil
	case EventBlockDelta:
		out, ok := e.index[ev.Index]
		if !ok || e.skipped[ev.Index] {
			return nil, nil
		}
		var frames []byte
		switch e.types[ev.Index] {
		case BlockText:
			if ev.Text != "" {
				frames = anthropicFrame(anthropicDeltaOut{Type: "content_block_delta", Index: out, Delta: map[string]string{"type": "text_delta", "text": ev.Text}})
			}
		case BlockThinking:
			if ev.Text != "" {
				frames = anthropicFrame(anthropicDeltaOut{Type: "content_block_delta", Index: out, Delta: map[string]string{"type": "thinking_delta", "thinking": ev.Text}})
			}
			if ev.Signature != "" {
				frames = append(frames, anthropicFrame(anthropicDeltaOut{Type: "content_block_delta", Index: out, Delta: map[string]string{"type": "signature_delta", "signature": ev.Signature}})...)
			}
		case BlockToolCall:
			if ev.Arguments != "" {
				frames = anthropicFrame(anthropicDeltaOut{Type: "content_block_delta", Index: out, Delta: map[string]string{"type": "input_json_delta", "partial_json": ev.Arguments}})
			}
		}
		return frames, nil
	case EventBlockStop:
		out, ok := e.index[ev.Index]
		if !ok || e.skipped[ev.Index] {
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
