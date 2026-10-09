package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

// KindMCP is the upstream kind of MCP tool calls; the call coordinator journals them under endpoint /v1/mcp.
const KindMCP upstream.Kind = "mcp"

// MaxArgumentsBytes bounds the arguments of one tools/call (they reach a host process; the edge already caps the body
// at 4 MiB).
const MaxArgumentsBytes = 64 << 10

// CodeArgumentsTooLarge rejects arguments over MaxArgumentsBytes (413, nothing sent).
const CodeArgumentsTooLarge = "mcp_arguments_too_large"

// Adapter executes POST /v1/mcp/call bodies {"server","tool","arguments"} as upstream calls (price 0).
type Adapter struct{ hub *Hub }

// NewAdapter returns the adapter for hub.
func NewAdapter(hub *Hub) *Adapter { return &Adapter{hub: hub} }

var _ upstream.Adapter = (*Adapter)(nil)

func (a *Adapter) Kind() upstream.Kind { return KindMCP }
func (a *Adapter) Provider() string    { return "mcp" }
func (a *Adapter) Version() string     { return "mcp/1" }

type callRequest struct {
	Server    string          `json:"server"`
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

func fatal(status int, code string, msg string) *upstream.Error {
	return &upstream.Error{Outcome: upstream.OutcomeFatal, Status: status, Code: code, Err: errors.New(msg)}
}

// Resolve validates the body and enforces the allowlist before anything is sent.
func (a *Adapter) Resolve(body []byte) ([]byte, map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var r callRequest
	if err := dec.Decode(&r); err != nil {
		var ue *json.UnmarshalTypeError
		if !errors.As(err, &ue) && bytes.Contains([]byte(err.Error()), []byte("unknown field")) {
			return nil, nil, fatal(http.StatusBadRequest, upstream.CodeUnsupportedField, "unknown field")
		}
		return nil, nil, fatal(http.StatusBadRequest, upstream.CodeInvalidRequest, "body must be {server, tool, arguments}")
	}
	if dec.More() || r.Server == "" || r.Tool == "" {
		return nil, nil, fatal(http.StatusBadRequest, upstream.CodeInvalidRequest, "server and tool are required")
	}
	cfg, ok := a.hub.Server(r.Server)
	if !ok {
		return nil, nil, fatal(http.StatusNotFound, CodeServerNotFound, "unknown server")
	}
	if !cfg.Allowed(r.Tool) {
		return nil, nil, fatal(http.StatusForbidden, CodeToolNotAllowed, "tool not allowlisted")
	}
	if len(r.Arguments) > MaxArgumentsBytes {
		return nil, nil, fatal(http.StatusRequestEntityTooLarge, CodeArgumentsTooLarge, "arguments too large")
	}
	args := bytes.TrimSpace(r.Arguments)
	if len(args) == 0 || string(args) == "null" {
		args = []byte(`{}`)
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(args, &obj) != nil || obj == nil {
		return nil, nil, fatal(http.StatusBadRequest, upstream.CodeInvalidRequest, "arguments must be an object")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, args); err != nil {
		return nil, nil, fatal(http.StatusBadRequest, upstream.CodeInvalidRequest, "arguments must be JSON")
	}
	resolved, err := json.Marshal(callRequest{Server: r.Server, Tool: r.Tool, Arguments: buf.Bytes()})
	if err != nil {
		return nil, nil, fatal(http.StatusBadRequest, upstream.CodeInvalidRequest, "arguments")
	}
	return resolved, map[string]any{}, nil
}

// Estimate is 0: MCP tools are not billed (they count against the per-turn tool budget instead).
func (a *Adapter) Estimate([]byte) (int64, error) { return 0, nil }

// Do runs tools/call.
func (a *Adapter) Do(ctx context.Context, resolved []byte) (upstream.Response, *upstream.Error) {
	var r callRequest
	if err := json.Unmarshal(resolved, &r); err != nil {
		return upstream.Response{}, fatal(http.StatusBadRequest, upstream.CodeInvalidRequest, "resolved body")
	}
	res, uerr := a.hub.Call(ctx, r.Server, r.Tool, r.Arguments)
	if uerr != nil {
		return upstream.Response{}, uerr
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(res); err != nil {
		return upstream.Response{}, fatal(http.StatusBadGateway, upstream.CodeUpstreamBadResponse, "encode result")
	}
	body := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	return upstream.Response{Body: body, Usage: upstream.Usage{Requests: 1, ResponseBytes: int64(len(body))}}, nil
}
