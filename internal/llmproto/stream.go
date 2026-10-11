package llmproto

import (
	"errors"
	"io"
	"strings"
)

// EventType is the type of a stream event (§7.2).
type EventType string

// Stream event types.
const (
	EventMessageStart EventType = "message_start"
	EventBlockStart   EventType = "block_start"
	EventBlockDelta   EventType = "block_delta"
	EventBlockStop    EventType = "block_stop"
	EventMessageDelta EventType = "message_delta"
	EventMessageStop  EventType = "message_stop"
	EventError        EventType = "error"
)

// Event is one event of a response stream in the neutral format.
//
// A well-formed stream is: message_start, then for each content block block_start, block_delta*, block_stop
// (blocks do not overlap), then message_delta and message_stop. An error event may end the stream at any
// point.
type Event struct {
	Type EventType

	// message_start: the message id, model and creation time (unix seconds, OpenAI only).
	ID      string
	Model   string
	Created int64

	// Block events: the index of the block. block_start also has Block (text, tool_call or thinking) and,
	// for a tool call, ToolID and ToolName.
	Index    int
	Block    BlockType
	ToolID   string
	ToolName string

	// block_delta: a fragment of text (text and thinking blocks), of the arguments JSON (tool calls) or a
	// thinking signature.
	Text      string
	Arguments string
	Signature string

	// message_delta: why the model stopped.
	StopReason   StopReason
	StopSequence string

	// Usage: on message_start the usage known at the start (Anthropic: input and cache tokens); on
	// message_delta the cumulative usage of the whole response when the stream reported it. Nil when unknown.
	Usage *Usage

	// Err is the error of an error event.
	Err *ErrorInfo
}

// StreamDecoder turns a provider's SSE events into neutral events.
type StreamDecoder interface {
	// Decode takes one SSE event and returns the neutral events it produces (possibly none).
	Decode(ev SSEEvent) ([]Event, error)
	// Finish is called at the end of input. It returns the closing events of a stream that ended without its
	// explicit terminator but is complete (OpenAI: a finish_reason was seen), or io.ErrUnexpectedEOF.
	Finish() ([]Event, error)
	// Done reports whether the stream has ended (message_stop or error emitted).
	Done() bool
	// Usage is the usage seen so far and whether any was seen.
	Usage() (Usage, bool)
}

// StreamEncoder turns neutral events into a client's SSE bytes (possibly none for an event).
type StreamEncoder interface {
	Encode(ev Event) ([]byte, error)
}

// NewStreamDecoder returns the stream decoder of format p.
func NewStreamDecoder(p Protocol) StreamDecoder {
	if p == ProtocolAnthropic {
		return NewAnthropicStreamDecoder()
	}
	return NewOpenAIStreamDecoder()
}

// ErrBadStream is a stream that breaks the event order (e.g. interleaved tool call deltas).
var ErrBadStream = errors.New("llmproto: malformed stream")

// EventReader reads neutral events from a provider's SSE body.
type EventReader struct {
	sse   *SSEReader
	dec   StreamDecoder
	queue []Event
	err   error
	onSSE func(SSEEvent)
}

// OnSSE sets f to be called with every SSE event read, before it is decoded (a same-format gateway forwards the
// provider's events verbatim while still decoding them for usage and completeness). Events read so far are
// not replayed.
func (r *EventReader) OnSSE(f func(SSEEvent)) { r.onSSE = f }

// NewEventReader reads the SSE body r of a provider of format p.
func NewEventReader(r io.Reader, p Protocol) *EventReader {
	return &EventReader{sse: NewSSEReader(r, 0), dec: NewStreamDecoder(p)}
}

// Next returns the next event. After message_stop or an error event it returns io.EOF. A body that ends
// before the stream is complete gives io.ErrUnexpectedEOF; read errors are returned as they are.
func (r *EventReader) Next() (Event, error) {
	for len(r.queue) == 0 {
		if r.err != nil {
			return Event{}, r.err
		}
		if r.dec.Done() {
			r.err = io.EOF
			continue
		}
		ev, err := r.sse.Next()
		if err != nil {
			// End of input, or a cut inside an event (which is dropped): Finish decides whether the stream
			// was complete (an OpenAI stream with a finish_reason but no [DONE] is).
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				r.queue, r.err = r.dec.Finish()
				if r.err == nil {
					r.err = io.EOF
				}
				continue
			}
			r.err = err
			continue
		}
		if r.onSSE != nil {
			r.onSSE(ev)
		}
		evs, err := r.dec.Decode(ev)
		if err != nil {
			r.err = err
			continue
		}
		r.queue = evs
	}
	ev := r.queue[0]
	r.queue = r.queue[1:]
	return ev, nil
}

// Usage is the usage the stream reported so far.
func (r *EventReader) Usage() (Usage, bool) { return r.dec.Usage() }

// Collector assembles a Response from stream events. Fragments are appended to one builder per block, so
// collecting is linear in the size of the stream.
type Collector struct {
	resp    Response
	blocks  map[int]*collected
	order   []int
	errInfo *ErrorInfo
	done    bool
}

// collected is a block being assembled.
type collected struct {
	block           Block
	text, args, sig strings.Builder
}

// Add adds one event.
func (c *Collector) Add(ev Event) {
	if c.blocks == nil {
		c.blocks = map[int]*collected{}
	}
	switch ev.Type {
	case EventMessageStart:
		c.resp.ID, c.resp.Model, c.resp.Created = ev.ID, ev.Model, ev.Created
		if ev.Usage != nil {
			c.resp.Usage, c.resp.HasUsage = *ev.Usage, true
		}
	case EventBlockStart:
		c.blocks[ev.Index] = &collected{block: Block{Type: ev.Block, ID: ev.ToolID, Name: ev.ToolName}}
		c.order = append(c.order, ev.Index)
	case EventBlockDelta:
		if b := c.blocks[ev.Index]; b != nil {
			b.text.WriteString(ev.Text)
			b.args.WriteString(ev.Arguments)
			b.sig.WriteString(ev.Signature)
		}
	case EventMessageDelta:
		c.resp.StopReason, c.resp.StopSequence = ev.StopReason, ev.StopSequence
		if ev.Usage != nil {
			c.resp.Usage, c.resp.HasUsage = *ev.Usage, true
		}
	case EventMessageStop:
		c.done = true
	case EventError:
		c.errInfo, c.done = ev.Err, true
		c.resp.StopReason = StopError
	}
}

// Response is the response assembled so far. A tool call that received no argument fragment has
// arguments {}; one whose arguments are not a JSON object is listed in Repaired.
func (c *Collector) Response() Response {
	r := c.resp
	r.Blocks, r.Repaired = nil, nil
	for n, i := range c.order {
		cb := c.blocks[i]
		b := cb.block
		b.Text, b.Arguments, b.Signature = cb.text.String(), cb.args.String(), cb.sig.String()
		if b.Type == BlockToolCall {
			if strings.TrimSpace(b.Arguments) == "" {
				b.Arguments = "{}"
			} else if !isJSONObject(b.Arguments) {
				r.Repaired = append(r.Repaired, idx("blocks", n)+".arguments")
			}
		}
		r.Blocks = append(r.Blocks, b)
	}
	return r
}

// Complete reports whether message_stop was seen (and no error).
func (c *Collector) Complete() bool { return c.done && c.errInfo == nil }

// Err is the error event's error, if any.
func (c *Collector) Err() *ErrorInfo { return c.errInfo }

// ResponseEvents returns the stream events of a complete response: each block is one start, one delta (two
// for a signed thinking block) and one stop.
func ResponseEvents(resp Response) []Event {
	start := Usage{InputTokens: resp.Usage.InputTokens, CacheReadTokens: resp.Usage.CacheReadTokens, CacheWriteTokens: resp.Usage.CacheWriteTokens}
	evs := []Event{{Type: EventMessageStart, ID: resp.ID, Model: resp.Model, Created: resp.Created}}
	if resp.HasUsage {
		evs[0].Usage = &start
	}
	i := 0
	for _, b := range resp.Blocks {
		switch b.Type {
		case BlockText:
			evs = append(evs, Event{Type: EventBlockStart, Index: i, Block: BlockText},
				Event{Type: EventBlockDelta, Index: i, Text: b.Text})
		case BlockThinking:
			evs = append(evs, Event{Type: EventBlockStart, Index: i, Block: BlockThinking},
				Event{Type: EventBlockDelta, Index: i, Text: b.Text})
			if b.Signature != "" {
				evs = append(evs, Event{Type: EventBlockDelta, Index: i, Signature: b.Signature})
			}
		case BlockToolCall:
			evs = append(evs, Event{Type: EventBlockStart, Index: i, Block: BlockToolCall, ToolID: b.ID, ToolName: b.Name},
				Event{Type: EventBlockDelta, Index: i, Arguments: b.Arguments})
		default:
			continue
		}
		evs = append(evs, Event{Type: EventBlockStop, Index: i})
		i++
	}
	md := Event{Type: EventMessageDelta, StopReason: resp.StopReason, StopSequence: resp.StopSequence}
	if resp.HasUsage {
		u := resp.Usage
		md.Usage = &u
	}
	return append(evs, md, Event{Type: EventMessageStop})
}
