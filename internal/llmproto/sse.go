package llmproto

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

// SSEEvent is one server-sent event: its "event:" name (empty when absent) and its data lines joined by "\n".
type SSEEvent struct {
	Event string
	Data  string
}

// DefaultMaxSSEEvent bounds the size of one SSE event read by SSEReader.
const DefaultMaxSSEEvent = 4 << 20

// ErrSSETooLarge is returned when one SSE event exceeds the reader's limit.
var ErrSSETooLarge = errors.New("llmproto: SSE event too large")

// SSEReader reads server-sent events (WHATWG event-stream framing: "event:", "data:", comments, LF, CRLF or
// CR line ends). Fields other than event and data (id, retry) are ignored.
type SSEReader struct {
	r       *bufio.Reader
	max     int
	afterCR bool // the last line ended with CR: a following LF belongs to it
}

// NewSSEReader reads events from r; max ≤ 0 means DefaultMaxSSEEvent.
func NewSSEReader(r io.Reader, max int) *SSEReader {
	if max <= 0 {
		max = DefaultMaxSSEEvent
	}
	return &SSEReader{r: bufio.NewReaderSize(r, 64<<10), max: max}
}

// Next returns the next event. At the end of input it returns io.EOF. An event that the input cuts off (a
// partial line, or no blank line after it) is discarded, as the WHATWG rules say, and Next returns
// io.ErrUnexpectedEOF: the stream was cut.
func (s *SSEReader) Next() (SSEEvent, error) {
	var (
		ev      SSEEvent
		data    strings.Builder
		hasData bool
		size    int
	)
	for {
		line, err := s.readLine()
		if err != nil {
			if errors.Is(err, io.EOF) && (hasData || ev.Event != "") {
				return SSEEvent{}, io.ErrUnexpectedEOF
			}
			return SSEEvent{}, err
		}
		size += len(line)
		if size > s.max {
			return SSEEvent{}, ErrSSETooLarge
		}
		if line == "" {
			if hasData {
				ev.Data = data.String()
				return ev, nil
			}
			ev = SSEEvent{} // an event without data is not dispatched
			continue
		}
		if line[0] == ':' {
			continue // comment
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.Event = value
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			hasData = true
		}
	}
}

// readLine reads one line without its terminator (LF, CRLF or CR). A line cut off by the end of input is
// io.ErrUnexpectedEOF; io.EOF means the input ended at a line boundary.
func (s *SSEReader) readLine() (string, error) {
	var buf []byte
	for {
		b, err := s.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(buf) > 0 {
				return "", io.ErrUnexpectedEOF
			}
			return "", err
		}
		if s.afterCR {
			s.afterCR = false
			if b == '\n' {
				continue // the LF of a CRLF; never wait for it after a bare CR
			}
		}
		switch b {
		case '\n':
			return string(buf), nil
		case '\r':
			s.afterCR = true
			return string(buf), nil
		}
		buf = append(buf, b)
		if len(buf) > s.max {
			return "", ErrSSETooLarge
		}
	}
}

// FormatSSE frames one event: an "event:" line when event is not empty, one "data:" line per line of data,
// and a blank line.
func FormatSSE(event string, data []byte) []byte {
	var b bytes.Buffer
	if event != "" {
		b.WriteString("event: ")
		b.WriteString(event)
		b.WriteByte('\n')
	}
	for line := range bytes.SplitSeq(data, []byte("\n")) {
		b.WriteString("data: ")
		b.Write(bytes.TrimSuffix(line, []byte("\r")))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.Bytes()
}

// WriteSSE writes one framed event to w.
func WriteSSE(w io.Writer, event string, data []byte) error {
	_, err := w.Write(FormatSSE(event, data))
	return err
}

// SSEContentType is the content type of an event stream response.
const SSEContentType = "text/event-stream"
