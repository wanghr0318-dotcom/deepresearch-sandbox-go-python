package llmproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// -update rewrites the golden outputs (the *_to_*.json and *_to_*.sse files) from the current code. The
// inputs and the expected errors, drops and usage are hand-written; review any golden change by hand.
var update = flag.Bool("update", false, "rewrite golden fixture outputs")

var protocols = []Protocol{ProtocolOpenAI, ProtocolAnthropic}

func other(p Protocol) Protocol {
	if p == ProtocolOpenAI {
		return ProtocolAnthropic
	}
	return ProtocolOpenAI
}

func caseDirs(t *testing.T, kind string) []string {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join("testdata", kind, "*"))
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no fixtures under testdata/%s: %v", kind, err)
	}
	return dirs
}

func readFile(t *testing.T, path string) ([]byte, bool) {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return b, true
}

// jsonEqual compares two JSON documents semantically.
func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func pretty(b []byte) []byte {
	var out bytes.Buffer
	if json.Indent(&out, b, "", "  ") != nil {
		return b
	}
	out.WriteByte('\n')
	return out.Bytes()
}

// golden compares got with the JSON golden file, or writes it with -update.
func golden(t *testing.T, path string, got []byte) {
	t.Helper()
	if *update {
		if err := os.WriteFile(path, pretty(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, ok := readFile(t, path)
	if !ok {
		t.Fatalf("missing golden %s (run with -update and review it)", path)
	}
	if !jsonEqual(got, want) {
		t.Errorf("%s mismatch\n got: %s\nwant: %s", path, got, bytes.TrimSpace(want))
	}
}

func TestRequestFixtures(t *testing.T) {
	for _, dir := range caseDirs(t, "request") {
		for _, src := range protocols {
			in, ok := readFile(t, filepath.Join(dir, string(src)+".json"))
			if !ok {
				continue
			}
			dst := other(src)
			name := filepath.Base(dir) + "/" + string(src) + "_to_" + string(dst)
			t.Run(name, func(t *testing.T) {
				req, err := DecodeRequest(src, in)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				// Same format: encoding and decoding again gives the same request.
				same, err := EncodeRequest(req, src)
				if err != nil {
					t.Fatalf("encode %s: %v", src, err)
				}
				if len(same.Dropped) != 0 {
					t.Errorf("same-format encode dropped %v", same.Dropped)
				}
				again, err := DecodeRequest(src, same.Body)
				if err != nil {
					t.Fatalf("decode re-encoded: %v\n%s", err, same.Body)
				}
				if !reflect.DeepEqual(normalizeRequest(again), normalizeRequest(req)) {
					t.Errorf("same-format round trip changed the request\n got: %+v\nwant: %+v", again, req)
				}

				base := filepath.Join(dir, string(src)+"_to_"+string(dst))
				enc, err := EncodeRequest(req, dst)
				if wantField, ok := readFile(t, base+".error"); ok {
					var uf *UnsupportedFieldError
					if !errors.As(err, &uf) || uf.Field != strings.TrimSpace(string(wantField)) {
						t.Fatalf("encode error = %v, want unsupported field %q", err, strings.TrimSpace(string(wantField)))
					}
					if p, only := req.OnlyProtocol(); !only || p != src {
						t.Errorf("OnlyProtocol = %v, %v; want %v, true", p, only, src)
					}
					if ClassOf(err) != ClassUnsupportedField {
						t.Errorf("ClassOf = %v", ClassOf(err))
					}
					return
				}
				if err != nil {
					t.Fatalf("encode %s: %v", dst, err)
				}
				if _, only := req.OnlyProtocol(); only {
					t.Errorf("OnlyProtocol is true for a convertible request: %v", req.FormatOnly())
				}
				golden(t, base+".json", enc.Body)
				var wantDropped []string
				if b, ok := readFile(t, base+".dropped.json"); ok {
					if err := json.Unmarshal(b, &wantDropped); err != nil {
						t.Fatal(err)
					}
				}
				gotDropped := slices.Clone(enc.Dropped)
				slices.Sort(gotDropped)
				slices.Sort(wantDropped)
				if !slices.Equal(gotDropped, wantDropped) {
					t.Errorf("dropped = %v, want %v", gotDropped, wantDropped)
				}
				// The converted request decodes in the target format.
				if _, err := DecodeRequest(dst, enc.Body); err != nil {
					t.Errorf("converted request does not decode as %s: %v\n%s", dst, err, enc.Body)
				}
			})
		}
	}
}

// normalizeRequest compacts the raw JSON values of a request (encoding compacts them) so that requests can
// be compared with reflect.DeepEqual.
func normalizeRequest(r Request) Request {
	c := func(raw json.RawMessage) json.RawMessage {
		if raw == nil {
			return nil
		}
		s, err := compactJSON(raw)
		if err != nil {
			return raw
		}
		return json.RawMessage(s)
	}
	blocks := func(bs []Block) []Block {
		out := slices.Clone(bs)
		for i := range out {
			out[i].Raw, out[i].CacheControl = c(out[i].Raw), c(out[i].CacheControl)
		}
		return out
	}
	r.System = blocks(r.System)
	r.Messages = slices.Clone(r.Messages)
	for i := range r.Messages {
		r.Messages[i].Blocks = blocks(r.Messages[i].Blocks)
	}
	r.Tools = slices.Clone(r.Tools)
	for i := range r.Tools {
		r.Tools[i].Raw, r.Tools[i].Parameters, r.Tools[i].CacheControl = c(r.Tools[i].Raw), c(r.Tools[i].Parameters), c(r.Tools[i].CacheControl)
	}
	if r.Extras != nil {
		ex := map[string]json.RawMessage{}
		for k, v := range r.Extras {
			ex[k] = c(v)
		}
		r.Extras = ex
	}
	return r
}

func TestResponseFixtures(t *testing.T) {
	for _, dir := range caseDirs(t, "response") {
		for _, src := range protocols {
			in, ok := readFile(t, filepath.Join(dir, string(src)+".json"))
			if !ok {
				continue
			}
			dst := other(src)
			t.Run(filepath.Base(dir)+"/"+string(src)+"_to_"+string(dst), func(t *testing.T) {
				resp, err := DecodeResponse(src, in)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				out, err := EncodeResponse(dst, resp)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				golden(t, filepath.Join(dir, string(src)+"_to_"+string(dst)+".json"), out)
				back, err := DecodeResponse(dst, out)
				if err != nil {
					t.Fatalf("converted response does not decode: %v", err)
				}
				if back.Usage != convertedUsage(resp.Usage, dst) || back.StopReason != convertedStop(resp.StopReason, dst) {
					t.Errorf("usage/stop changed: %+v %v -> %+v %v", resp.Usage, resp.StopReason, back.Usage, back.StopReason)
				}
				// Same format round trip is lossless.
				sameOut, err := EncodeResponse(src, resp)
				if err != nil {
					t.Fatal(err)
				}
				same, err := DecodeResponse(src, sameOut)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(same, resp) {
					t.Errorf("same-format round trip\n got: %+v\nwant: %+v", same, resp)
				}
			})
		}
	}
}

// convertedStop is the stop reason after a trip through format p (OpenAI has no stop_sequence).
func convertedStop(r StopReason, p Protocol) StopReason {
	if p == ProtocolOpenAI && r == StopSequence {
		return StopEnd
	}
	return r
}

// convertedUsage is the usage after a trip through format p (OpenAI has no cache write count: those tokens
// are ordinary prompt tokens there).
func convertedUsage(u Usage, p Protocol) Usage {
	if p == ProtocolOpenAI {
		u.InputTokens += u.CacheWriteTokens
		u.CacheWriteTokens = 0
	}
	return u
}

// sseFrame is one parsed frame of an SSE golden file.
type sseFrame struct {
	Event string
	Data  string
}

func parseSSE(t *testing.T, b []byte) []sseFrame {
	t.Helper()
	r := NewSSEReader(bytes.NewReader(b), 0)
	var out []sseFrame
	for {
		ev, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, sseFrame{ev.Event, ev.Data})
	}
}

func sseEqual(a, b []sseFrame) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Event != b[i].Event {
			return false
		}
		if a[i].Data != b[i].Data && !jsonEqual([]byte(a[i].Data), []byte(b[i].Data)) {
			return false
		}
	}
	return true
}

func newEncoder(p, source Protocol) StreamEncoder {
	if p == ProtocolAnthropic {
		return NewAnthropicStreamEncoder(AnthropicStreamOptions{Source: source})
	}
	return NewOpenAIStreamEncoder(OpenAIStreamOptions{IncludeUsage: true, Created: 1700000000})
}

func TestStreamFixtures(t *testing.T) {
	for _, dir := range caseDirs(t, "stream") {
		for _, src := range protocols {
			in, ok := readFile(t, filepath.Join(dir, string(src)+".sse"))
			if !ok {
				continue
			}
			dst := other(src)
			t.Run(filepath.Base(dir)+"/"+string(src)+"_to_"+string(dst), func(t *testing.T) {
				rd := NewEventReader(bytes.NewReader(in), src)
				enc := newEncoder(dst, src)
				var out []byte
				var col Collector
				for {
					ev, err := rd.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("decode: %v", err)
					}
					checkEventOrder(t, &col, ev)
					col.Add(ev)
					b, err := enc.Encode(ev)
					if err != nil {
						t.Fatalf("encode: %v", err)
					}
					out = append(out, b...)
				}
				path := filepath.Join(dir, string(src)+"_to_"+string(dst)+".sse")
				if *update {
					if err := os.WriteFile(path, out, 0o644); err != nil {
						t.Fatal(err)
					}
				} else {
					want, ok := readFile(t, path)
					if !ok {
						t.Fatalf("missing golden %s", path)
					}
					if got, w := parseSSE(t, out), parseSSE(t, want); !sseEqual(got, w) {
						t.Errorf("%s mismatch\n got:\n%s", path, out)
					}
				}
				if b, ok := readFile(t, filepath.Join(dir, "usage.json")); ok {
					var want Usage
					if err := json.Unmarshal(b, &want); err != nil {
						t.Fatal(err)
					}
					got, seen := rd.Usage()
					if !seen || got != want {
						t.Errorf("usage = %+v (seen %v), want %+v", got, seen, want)
					}
					if r := col.Response(); !r.HasUsage || r.Usage != want {
						t.Errorf("collected usage = %+v", r.Usage)
					}
				}
				// The converted stream decodes in the target format to the same content.
				rd2 := NewEventReader(bytes.NewReader(out), dst)
				var col2 Collector
				for {
					ev, err := rd2.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("converted stream does not decode as %s: %v", dst, err)
					}
					col2.Add(ev)
				}
				a, b := col.Response(), col2.Response()
				if col.Err() != nil {
					if col2.Err() == nil {
						t.Errorf("error event lost in conversion")
					}
					return
				}
				if text(a, dst == ProtocolAnthropic && src == ProtocolOpenAI) != text(b, false) || toolCalls(a) != toolCalls(b) {
					t.Errorf("content changed in conversion\n src: %+v\n dst: %+v", a.Blocks, b.Blocks)
				}
				if convertedUsage(a.Usage, dst) != b.Usage || convertedStop(a.StopReason, dst) != b.StopReason {
					t.Errorf("usage/stop changed: %+v %v -> %+v %v", a.Usage, a.StopReason, b.Usage, b.StopReason)
				}
			})
		}
	}
}

// text joins the text (and, unless skipThinking, thinking) of a response.
func text(r Response, skipThinking bool) string {
	var b strings.Builder
	for _, bl := range r.Blocks {
		if bl.Type == BlockText || (bl.Type == BlockThinking && !skipThinking) {
			b.WriteString(string(bl.Type) + ":" + bl.Text + "|")
		}
	}
	return b.String()
}

func toolCalls(r Response) string {
	var b strings.Builder
	for _, bl := range r.Blocks {
		if bl.Type == BlockToolCall {
			var v any
			_ = json.Unmarshal([]byte(bl.Arguments), &v)
			args, _ := json.Marshal(v)
			b.WriteString(bl.ID + "/" + bl.Name + "/" + string(args) + "|")
		}
	}
	return b.String()
}

// checkEventOrder fails the test when ev breaks the neutral stream grammar (see Event).
func checkEventOrder(t *testing.T, col *Collector, ev Event) {
	t.Helper()
	r := col.Response()
	switch ev.Type {
	case EventMessageStart:
		if r.ID != "" || len(r.Blocks) > 0 {
			t.Fatalf("message_start twice")
		}
	case EventBlockDelta, EventBlockStop:
		if _, ok := col.blocks[ev.Index]; !ok {
			t.Fatalf("%s for unknown block %d", ev.Type, ev.Index)
		}
	case EventBlockStart:
		if _, ok := col.blocks[ev.Index]; ok {
			t.Fatalf("block %d started twice", ev.Index)
		}
	}
}
