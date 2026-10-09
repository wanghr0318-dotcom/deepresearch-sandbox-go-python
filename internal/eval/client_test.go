package eval

import (
	"strings"
	"testing"
)

func TestSSEParser(t *testing.T) {
	stream := ": heartbeat\r\n" +
		"id: 7\nevent: progress\ndata: {\"task_seq\":7,\"source\":\"worker\",\n" +
		"data: \"type\":\"progress\"}\n\n" +
		"\n" + // a blank line with no pending data dispatches nothing
		"id: 8\nevent: task_terminal\ndata: {\"source\":\"host\"}\r\n\r\n" +
		"id: x\ndata: {}\n\n" + // invalid id
		"id: 9\ndata: not json\n\n" +
		"id: 10\ndata: {\"type\":\"partial\"" // no terminating blank line: never dispatched
	var p sseParser
	var frames []sseFrame
	for _, l := range strings.Split(stream, "\n") {
		if f, ok := p.line(l); ok {
			frames = append(frames, f)
		}
	}
	if len(frames) != 4 {
		t.Fatalf("frames %+v", frames)
	}
	if frames[0].ID != "7" || frames[0].Type != "progress" || !strings.Contains(frames[0].Data, "\n") {
		t.Fatalf("multi-line data frame %+v", frames[0])
	}
	ev, ok := frames[0].event()
	if !ok || ev.TaskSeq != 7 || ev.Type != "progress" || ev.Source != "worker" {
		t.Fatalf("event %+v %v", ev, ok)
	}
	ev, ok = frames[1].event() // type and seq filled from the SSE fields
	if !ok || ev.TaskSeq != 8 || ev.Type != "task_terminal" {
		t.Fatalf("event %+v %v", ev, ok)
	}
	if _, ok := frames[2].event(); ok {
		t.Fatal("invalid id accepted")
	}
	if _, ok := frames[3].event(); ok {
		t.Fatal("invalid JSON accepted")
	}
}
