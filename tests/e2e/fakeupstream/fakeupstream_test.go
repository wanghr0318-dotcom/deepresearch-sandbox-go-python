package fakeupstream

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// TestCodeReply：[stage:code] 的 chat 按用户消息开头的 [eval-task:<id>] 回复 SetCodeReply 的内容；未设置的任务与
// 其他阶段不受影响。
func TestCodeReply(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetCodeReply("add", "```python\ndef add(a, b):\n    return a + b\n```")
	ask := func(system, user string) string {
		body, _ := json.Marshal(map[string]any{"model": "m", "messages": []map[string]string{
			{"role": "system", "content": system}, {"role": "user", "content": user}}})
		resp, err := http.Post(s.ModelBaseURL()+"/chat/completions", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var r ChatReply
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			t.Fatal(err)
		}
		return r.Choices[0].Message.Content
	}
	if got := ask(StageCode+" write code", "[eval-task:add]\nWrite add."); got != "```python\ndef add(a, b):\n    return a + b\n```" {
		t.Fatalf("code reply = %q", got)
	}
	if got := ask(StageCode+" write code", "[eval-task:other]\nx"); got == "" || got[0] == '`' {
		t.Fatalf("unset task reply = %q", got)
	}
	if got := ask("plain", "[eval-task:add]\nx"); got[0] == '`' {
		t.Fatalf("non-code stage got the code reply: %q", got)
	}
	if s.StageCount(StageCode) != 2 {
		t.Fatalf("StageCount = %d", s.StageCount(StageCode))
	}
}

// TestSetEvery：序号为 k 的倍数的请求执行周期动作，其余正常；按序号注入的故障优先；k ≤ 0 取消。
func TestSetEvery(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetEvery(Search, 3, Action{Status: http.StatusServiceUnavailable})
	s.Inject(Search, 2, Action{Status: http.StatusTooManyRequests})
	status := func() int {
		resp, err := http.Post(s.URL()+"/search", "application/json", bytes.NewReader([]byte(`{"query":"q","max_results":1}`)))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	var got []int
	for range 7 {
		got = append(got, status())
	}
	want := []int{200, 429, 503, 200, 200, 503, 200}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statuses = %v, want %v", got, want)
		}
	}
	s.SetEvery(Search, 0, Action{})
	for range 3 {
		if st := status(); st != 200 {
			t.Fatalf("after SetEvery(0) status = %d", st)
		}
	}
}
