package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

// Error codes.
const (
	CodeToolNotAllowed = "mcp_tool_not_allowed" // 403: not on the server's allowlist (nothing sent)
	CodeServerNotFound = "mcp_server_not_found" // 404
	CodeMCPError       = "mcp_error"            // 502: the server answered with a JSON-RPC error
)

// MaxTextBytes bounds the text content handed to the model per call.
const MaxTextBytes = 256 << 10

// Tool is an allowlisted tool as exposed to the worker (GET /v1/mcp/tools).
type Tool struct {
	Name        string          `json:"name"` // mcp__<server>__<tool>
	Server      string          `json:"server"`
	Tool        string          `json:"tool"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// Hub owns one client per configured server.
type Hub struct {
	log     *slog.Logger
	order   []string
	servers map[string]*server
	closed  chan struct{}
	once    sync.Once
}

type server struct {
	cfg    ServerConfig
	client *client

	mu     sync.Mutex
	tools  []Tool // cached allowlisted list; nil until a successful tools/list
	listed bool
}

// NewHub validates cfg and prepares clients (stdio processes start on first use).
func NewHub(cfg Config, log *slog.Logger) (*Hub, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &Hub{log: log, servers: map[string]*server{}, closed: make(chan struct{})}
	for _, s := range cfg.Servers {
		h.order = append(h.order, s.Name)
		h.servers[s.Name] = &server{cfg: s, client: newClient(s, log)}
	}
	return h, nil
}

// Close stops all servers.
func (h *Hub) Close() {
	h.once.Do(func() {
		close(h.closed)
		for _, s := range h.servers {
			_ = s.client.close() // shutdown: errors do not matter
		}
	})
}

// Server returns the config of a server.
func (h *Hub) Server(name string) (ServerConfig, bool) {
	s, ok := h.servers[name]
	if !ok {
		return ServerConfig{}, false
	}
	return s.cfg, true
}

// Tools returns the allowlisted tools of all servers, in config order. Each server's list is fetched once and
// cached for the Gateway's lifetime; a server that fails to list is omitted (logged) and retried next time.
func (h *Hub) Tools(ctx context.Context) []Tool {
	out := []Tool{}
	for _, name := range h.order {
		s := h.servers[name]
		tools, err := s.list(ctx)
		if err != nil {
			h.log.Warn("gateway: mcp tools/list failed", "server", name, "err", err)
			continue
		}
		out = append(out, tools...)
	}
	return out
}

func (s *server) list(ctx context.Context) ([]Tool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listed {
		return s.tools, nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout())
	defer cancel()
	infos, err := s.client.listTools(ctx)
	if err != nil {
		return nil, err
	}
	tools := []Tool{}
	for _, t := range infos {
		if !s.cfg.Allowed(t.Name) {
			continue
		}
		schema := t.InputSchema
		if len(schema) == 0 || string(schema) == "null" {
			schema = json.RawMessage(`{"type":"object"}`)
		}
		tools = append(tools, Tool{Name: ToolName(s.cfg.Name, t.Name), Server: s.cfg.Name, Tool: t.Name,
			Description: t.Description, InputSchema: schema})
	}
	s.tools, s.listed = tools, true
	return tools, nil
}

// CallResult is the Gateway's response body for one tools/call.
type CallResult struct {
	Server            string          `json:"server"`
	Tool              string          `json:"tool"`
	IsError           bool            `json:"is_error"`
	Content           []ContentItem   `json:"content"`
	StructuredContent json.RawMessage `json:"structured_content,omitempty"`
	Truncated         bool            `json:"truncated,omitempty"`
}

// ContentItem is one content block: text is kept (bounded); other types are reported, not forwarded.
type ContentItem struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	Note string `json:"note,omitempty"`
}

// Call runs tools/call on an allowlisted tool and classifies the outcome for the call coordinator.
func (h *Hub) Call(ctx context.Context, srv, tool string, args json.RawMessage) (CallResult, *upstream.Error) {
	s, ok := h.servers[srv]
	if !ok {
		return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeFatal, Status: http.StatusNotFound, Code: CodeServerNotFound,
			Err: errors.New("unknown server")}
	}
	if !s.cfg.Allowed(tool) {
		return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeFatal, Status: http.StatusForbidden, Code: CodeToolNotAllowed,
			Err: errors.New("tool not allowlisted")}
	}
	select {
	case <-h.closed:
		return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeRetryable, Status: http.StatusBadGateway,
			Code: upstream.CodeUpstreamUnreachable, Err: errClosed}
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout())
	defer cancel()
	raw, err := s.client.callTool(ctx, tool, args)
	if err != nil {
		var re *rpcError
		switch {
		case errors.As(err, &re):
			return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeFatal, Status: http.StatusBadGateway, Code: CodeMCPError, Err: err}
		case wasSent(err):
			return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeUnknown, Status: http.StatusBadGateway,
				Code: upstream.CodeUpstreamUnconfirmed, Err: err}
		default:
			return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeRetryable, Status: http.StatusBadGateway,
				Code: upstream.CodeUpstreamUnreachable, Err: err}
		}
	}
	var r struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StructuredContent json.RawMessage `json:"structuredContent"`
		IsError           bool            `json:"isError"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return CallResult{}, &upstream.Error{Outcome: upstream.OutcomeFatal, Status: http.StatusBadGateway,
			Code: upstream.CodeUpstreamBadResponse, Err: err}
	}
	out := CallResult{Server: srv, Tool: tool, IsError: r.IsError, Content: []ContentItem{}}
	budget := MaxTextBytes
	for _, c := range r.Content {
		if c.Type != "text" {
			out.Content = append(out.Content, ContentItem{Type: c.Type, Note: "non-text content omitted"})
			continue
		}
		text := c.Text
		if len(text) > budget {
			text = truncateUTF8(text, budget)
			out.Truncated = true
		}
		budget -= len(text)
		out.Content = append(out.Content, ContentItem{Type: "text", Text: text})
	}
	if len(r.StructuredContent) > 0 && string(r.StructuredContent) != "null" && len(r.StructuredContent) <= MaxTextBytes {
		out.StructuredContent = r.StructuredContent
	}
	return out, nil
}

func truncateUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
