package eval

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestListTasksPages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks" || r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "bad", 400)
			return
		}
		switch r.URL.Query().Get("after") {
		case "":
			if r.URL.Query().Get("limit") != "2" {
				http.Error(w, "limit", 400)
				return
			}
			_, _ = w.Write([]byte(`{"tasks":[{"task_id":"t3","status":"failed"},{"task_id":"t2","status":"succeeded"}],"next":"c2"}`))
		case "c2":
			_, _ = w.Write([]byte(`{"tasks":[{"task_id":"t1","status":"succeeded"}]}`))
		}
	}))
	defer srv.Close()
	c := &Client{Base: srv.URL, Token: "tok"}
	p1, err := c.ListTasks(context.Background(), "", 2)
	if err != nil || len(p1.Tasks) != 2 || p1.Next != "c2" || p1.Tasks[0].TaskID != "t3" || p1.Tasks[0].Status != "failed" {
		t.Fatalf("page 1 %+v %v", p1, err)
	}
	p2, err := c.ListTasks(context.Background(), p1.Next, 2)
	if err != nil || len(p2.Tasks) != 1 || p2.Next != "" {
		t.Fatalf("page 2 %+v %v", p2, err)
	}
}
