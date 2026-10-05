// Package fakeupstream 是只供测试使用的进程内上游（规格 §16.4 E17–E20、E48、E11b；Plan 8 Task 5 复用）：
// 一个本机 HTTP 服务器，按 Gateway adapter 访问的协议扮演模型、搜索与抓取目标，并提供故障注入与计数。
// 不访问外网。
//
// 端点（与 internal/gateway/upstream 的 adapter 对应）：
//   - POST /v1/chat/completions：OpenAI 兼容（Gateway 原样透传给 Worker，Worker 读 choices[0].message.content）。
//     第一条 system 消息以阶段标记（"[stage:plan]"、"[stage:summarize]"、"[stage:report]"）开头时按
//     SetStageReply 的脚本回复，否则回复确定性的占位文本；响应带 usage（prompt_tokens、completion_tokens）。
//     模型 adapter 的 BaseURL 为 ModelBaseURL()（= URL()+"/v1"）。
//   - POST /search：tavily 兼容（{query, max_results} → {request_id, results:[{title, url, content, snippet}]}）；
//     结果 URL 指向本服务器的 /pages/...，可以继续抓取。Gateway 的 fake 搜索供应商配置了搜索上游地址
//     （app.Config.SearchBaseURL / --search-base-url = URL()）时访问本端点；Gateway 返回给 Worker 的形状是
//     results[]{title, url, snippet}。
//   - GET 其他路径：抓取目标。SetPage 设置的页面按原样返回，否则返回确定性的 text/plain 正文（Gateway 的
//     fetch adapter 把它包装为 {url, final_url, status, content_type, truncated, encoding, content}）。
//
// 故障注入按类别与"该类别的第 n 次请求"声明（Inject）：返回指定状态（可带 Retry-After）、挂起（直到 Release
// 或客户端离开）、或发出响应头与部分正文后切断连接（响应中途断开）。
//
// 计数：Count 是该类别收到的请求数（含注入故障的请求）；Distinct 按调用身份去重——请求带 X-Agentbox-Call-Id
// 时以它为身份，否则以请求体的 sha256 为身份（Gateway 不把 call id 转发给上游，因此经 Gateway 的请求按请求体
// 去重：同一逻辑调用的每次 try 请求体相同）。Requests 返回每个请求的记录（到达时间、注入动作、结局），测试据此
// 等待可观察的条件而不是时间。
package fakeupstream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kind 是请求类别。
type Kind string

const (
	Chat   Kind = "chat"
	Search Kind = "search"
	Fetch  Kind = "fetch"
)

// Model 是本上游声明的模型名（与 Gateway 配置的模型名一致）。
const Model = "fake-model"

// 阶段标记（与 worker/deepresearch/prompts.py 的 STAGE_* 一致）。
const (
	StagePlan      = "[stage:plan]"
	StageSummarize = "[stage:summarize]"
	StageReport    = "[stage:report]"
)

// Action 是注入到某次请求的动作。零值表示正常回复。
type Action struct {
	// Status 非零时以该状态与 JSON 错误体回复（例如 429、503）；RetryAfter 非空时带 Retry-After 头。
	Status     int
	RetryAfter string
	// Hang 为真时挂起：不发出任何响应，直到 Release（随后正常回复）或客户端离开。
	Hang bool
	// Disconnect 为真时发出 200 响应头与部分正文后切断连接（Content-Length 声明的长度不足）。
	Disconnect bool
}

func (a Action) String() string {
	switch {
	case a.Hang:
		return "hang"
	case a.Disconnect:
		return "disconnect"
	case a.Status != 0 && a.RetryAfter != "":
		return fmt.Sprintf("status %d (Retry-After %s)", a.Status, a.RetryAfter)
	case a.Status != 0:
		return fmt.Sprintf("status %d", a.Status)
	}
	return "ok"
}

// 请求结局。
const (
	OutcomeOK           = "ok"           // 正常回复（含挂起后被 Release 的请求）
	OutcomeStatus       = "status"       // 按注入的状态回复
	OutcomeDisconnected = "disconnected" // 发出部分响应后切断
	OutcomeAborted      = "aborted"      // 挂起期间客户端离开（取消或超时）
)

// Request 是一次请求的记录。
type Request struct {
	Seq           int       // 全部请求中的序号（从 1 开始）
	Kind          Kind      // 类别
	N             int       // 该类别中的序号（从 1 开始）
	Key           string    // 调用身份（X-Agentbox-Call-Id，或 "sha256:<请求体哈希>"）
	Path          string    // 请求路径
	Authorization string    // Authorization 头（测试据此确认 Key 只在上游请求头中出现）
	Action        Action    // 注入的动作
	At            time.Time // 到达时间
	Outcome       string    // 结局；处理中为空
}

// Server 是进程内 fake upstream。并发安全。
type Server struct {
	srv *httptest.Server

	mu       sync.Mutex
	seq      int
	counts   map[Kind]int
	reqs     []*Request
	faults   map[Kind]map[int]Action
	stages   map[string]string
	pages    map[string]page
	released chan struct{} // Release 时关闭，随后换新
	hanging  int
}

type page struct {
	contentType string
	body        string
}

// New 启动一个监听 127.0.0.1 随机端口的 fake upstream；测试结束时调用 Close。
func New() *Server {
	s := &Server{
		counts:   map[Kind]int{},
		faults:   map[Kind]map[int]Action{},
		pages:    map[string]page{},
		released: make(chan struct{}),
		stages: map[string]string{
			StagePlan:      `{"tasks":[{"title":"fake 任务","intent":"fake 意图","query":"fake 查询"}]}`,
			StageSummarize: "## 任务总结\n- fake 发现 [1]",
			StageReport:    "## 核心洞见\nfake 报告 [1]。",
		},
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// Close 放行全部挂起的请求并关闭服务器。
func (s *Server) Close() {
	s.Release()
	s.srv.Close()
}

// URL 是服务器的根地址（http://127.0.0.1:<port>）。
func (s *Server) URL() string { return s.srv.URL }

// ModelBaseURL 是 OpenAI 兼容模型 adapter 的 BaseURL（请求发往 <BaseURL>/chat/completions）。
func (s *Server) ModelBaseURL() string { return s.srv.URL + "/v1" }

// HostPort 是服务器的 "127.0.0.1:<port>"（Gateway 的 upstream_allow_private 项）。
func (s *Server) HostPort() string {
	u, err := url.Parse(s.srv.URL)
	if err != nil {
		panic(err) // httptest 的 URL 总是合法
	}
	return u.Host
}

// Inject 声明类别 kind 的第 n 次请求（从 1 开始）执行动作 a。
func (s *Server) Inject(kind Kind, n int, a Action) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.faults[kind]
	if m == nil {
		m = map[int]Action{}
		s.faults[kind] = m
	}
	m[n] = a
}

// SetStageReply 设置以阶段标记 stage 开头的 system 消息对应的 chat 回复内容。
func (s *Server) SetStageReply(stage, content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages[stage] = content
}

// SetPage 设置抓取目标 path（以 / 开头）的内容。
func (s *Server) SetPage(path, contentType, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pages[path] = page{contentType: contentType, body: body}
}

// Release 放行当前全部挂起的请求（它们随后正常回复）。之后到达的挂起请求等待下一次 Release。
func (s *Server) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.released)
	s.released = make(chan struct{})
}

// Count 返回类别 kind 收到的请求数。
func (s *Server) Count(kind Kind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[kind]
}

// Distinct 返回类别 kind 中不同调用身份的数目。
func (s *Server) Distinct(kind Kind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := map[string]bool{}
	for _, r := range s.reqs {
		if r.Kind == kind {
			keys[r.Key] = true
		}
	}
	return len(keys)
}

// Hanging 返回当前挂起中的请求数。
func (s *Server) Hanging() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hanging
}

// Requests 返回类别 kind 的请求记录（副本，按到达顺序）。
func (s *Server) Requests(kind Kind) []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Request
	for _, r := range s.reqs {
		if r.Kind == kind {
			out = append(out, *r)
		}
	}
	return out
}

func kindOf(r *http.Request) (Kind, bool) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		return Chat, true
	case r.Method == http.MethodPost && r.URL.Path == "/search":
		return Search, true
	case r.Method == http.MethodGet:
		return Fetch, true
	}
	return "", false
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	kind, ok := kindOf(r)
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody("not_found"))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("bad_body"))
		return
	}
	key := r.Header.Get("X-Agentbox-Call-Id")
	if key == "" {
		sum := sha256.Sum256(append([]byte(r.Method+" "+r.URL.RequestURI()+"\n"), body...))
		key = "sha256:" + hex.EncodeToString(sum[:])
	}
	s.mu.Lock()
	s.seq++
	s.counts[kind]++
	rec := &Request{Seq: s.seq, Kind: kind, N: s.counts[kind], Key: key, Path: r.URL.Path,
		Authorization: r.Header.Get("Authorization"), Action: s.faults[kind][s.counts[kind]], At: time.Now()}
	s.reqs = append(s.reqs, rec)
	released := s.released
	if rec.Action.Hang {
		s.hanging++
	}
	s.mu.Unlock()

	a := rec.Action
	switch {
	case a.Hang:
		select {
		case <-released:
			s.finishHang(rec, OutcomeOK)
		case <-r.Context().Done():
			s.finishHang(rec, OutcomeAborted)
			return
		}
	case a.Disconnect:
		s.disconnect(w, rec)
		return
	case a.Status != 0:
		if a.RetryAfter != "" {
			w.Header().Set("Retry-After", a.RetryAfter)
		}
		writeJSON(w, a.Status, errBody(fmt.Sprintf("injected_%d", a.Status)))
		s.setOutcome(rec, OutcomeStatus)
		return
	}
	switch kind {
	case Chat:
		s.chat(w, rec, body)
	case Search:
		s.search(w, rec, body)
	default:
		s.fetch(w, r, rec)
	}
}

func (s *Server) finishHang(rec *Request, outcome string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hanging--
	if outcome != OutcomeOK { // 正常回复的结局在写出响应后记录
		rec.Outcome = outcome
	}
}

func (s *Server) setOutcome(rec *Request, outcome string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.Outcome = outcome
}

// disconnect 发出 200 响应头与部分正文后切断连接：客户端读正文时得到意外的 EOF（已发出、结果无法确认）。
func (s *Server) disconnect(w http.ResponseWriter, rec *Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("fakeupstream: ResponseWriter 不支持 Hijack")
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		s.setOutcome(rec, OutcomeAborted)
		return
	}
	partial := `{"id":"chatcmpl-fake-cut","choices":[{"message":{"role":"assistant","content":"cut`
	_, werr := fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		len(partial)+4096, partial)
	if werr == nil {
		werr = buf.Flush()
	}
	cerr := conn.Close()
	if werr != nil || cerr != nil {
		s.setOutcome(rec, OutcomeAborted)
		return
	}
	s.setOutcome(rec, OutcomeDisconnected)
}

type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

// ChatReply 是 OpenAI 兼容的非流式回复（只含 Worker 与 Gateway 读取的字段）。
type ChatReply struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Model   string       `json:"model"`
	Choices []ChatChoice `json:"choices"`
	Usage   ChatUsage    `json:"usage"`
}

// ChatChoice 是一个候选回复。
type ChatChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

// ChatMessage 是一条消息。
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatUsage 是用量（Gateway 据此按价格表结算 ok 的 try）。
type ChatUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

func (s *Server) chat(w http.ResponseWriter, rec *Request, body []byte) {
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		writeJSON(w, http.StatusBadRequest, errBody("invalid_request"))
		s.setOutcome(rec, OutcomeStatus)
		return
	}
	sum := sha256.Sum256(body)
	content := "fake 回复 " + hex.EncodeToString(sum[:4])
	s.mu.Lock()
	for stage, reply := range s.stages {
		if req.Messages[0].Role == "system" && strings.HasPrefix(req.Messages[0].Content, stage) {
			content = reply
		}
	}
	s.mu.Unlock()
	prompt := int64(len(body)+3) / 4
	completion := int64(len(content)+3) / 4
	w.Header().Set("X-Request-Id", fmt.Sprintf("fake-req-%d", rec.Seq))
	writeJSON(w, http.StatusOK, ChatReply{
		ID: fmt.Sprintf("chatcmpl-fake-%d", rec.Seq), Object: "chat.completion", Model: req.Model,
		Choices: []ChatChoice{{Message: ChatMessage{Role: "assistant", Content: content}, FinishReason: "stop"}},
		Usage:   ChatUsage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion},
	})
	s.setOutcome(rec, OutcomeOK)
}

// SearchResult 是 tavily 兼容的一条结果（同时给出 snippet，与 Gateway 返回给 Worker 的字段同名）。
type SearchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Content string `json:"content"`
	Snippet string `json:"snippet"`
}

func (s *Server) search(w http.ResponseWriter, rec *Request, body []byte) {
	var req struct {
		Query      string `json:"query"`
		MaxResults int    `json:"max_results"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Query == "" {
		writeJSON(w, http.StatusBadRequest, errBody("invalid_request"))
		s.setOutcome(rec, OutcomeStatus)
		return
	}
	if req.MaxResults <= 0 {
		req.MaxResults = 5
	}
	sum := sha256.Sum256([]byte(req.Query))
	slug := hex.EncodeToString(sum[:4])
	results := make([]SearchResult, 0, req.MaxResults)
	for i := 1; i <= req.MaxResults; i++ {
		text := fmt.Sprintf("关于 %s 的第 %d 条片段", req.Query, i)
		results = append(results, SearchResult{Title: fmt.Sprintf("%s %d", req.Query, i),
			URL: fmt.Sprintf("%s/pages/%s/%d", s.srv.URL, slug, i), Content: text, Snippet: text})
	}
	writeJSON(w, http.StatusOK, map[string]any{"request_id": fmt.Sprintf("fake-search-%d", rec.Seq), "results": results})
	s.setOutcome(rec, OutcomeOK)
}

func (s *Server) fetch(w http.ResponseWriter, r *http.Request, rec *Request) {
	s.mu.Lock()
	p, ok := s.pages[r.URL.Path]
	s.mu.Unlock()
	if !ok {
		p = page{contentType: "text/plain; charset=utf-8", body: r.URL.Path + " 的正文"}
	}
	w.Header().Set("Content-Type", p.contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(p.body)))
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, p.body); err != nil {
		s.setOutcome(rec, OutcomeAborted)
		return
	}
	s.setOutcome(rec, OutcomeOK)
}

func errBody(code string) map[string]any {
	return map[string]any{"error": map[string]string{"code": code, "message": "fakeupstream"}}
}

// writeJSON 写出 JSON 响应；写失败（客户端已离开）时无事可做。
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // 只编码本包构造的值
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b) // 客户端可能已离开；结局由调用方记录
}
