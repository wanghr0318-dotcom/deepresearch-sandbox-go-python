// Package mcpdemo is a tiny MCP server used by tests and the demo (`agentbox mcp-demo-server`, stdio). It needs no
// network and no credentials. Tools:
//
//   - calculate {expression}: arithmetic with + - * / % ^, parentheses and decimals;
//   - unit_convert {value, from, to}: length (m, km, cm, mm, mi, ft, in), mass (kg, g, lb, oz), temperature (c, f, k);
//   - echo_env {name}: returns an environment variable of the server process. It exists to demonstrate the Gateway's
//     allowlist: the demo config does not allow it, so the agent never sees it and calls to it are rejected.
//
// Transports: newline-delimited JSON-RPC over stdio (ServeStdio) and a minimal streamable-HTTP handler (Handler) that
// answers with a JSON body or, when sse is set, an SSE stream.
package mcpdemo

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// ProtocolVersion matches the Gateway client.
const ProtocolVersion = "2025-06-18"

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

var tools = []tool{
	{Name: "calculate", Description: "Evaluate an arithmetic expression (+ - * / % ^, parentheses, decimals) exactly as written and return the number.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"expression": map[string]any{"type": "string", "description": "e.g. (1.5 + 2) * 4 ^ 2"}},
			"required": []string{"expression"}, "additionalProperties": false}},
	{Name: "unit_convert", Description: "Convert a value between units of length (m, km, cm, mm, mi, ft, in), mass (kg, g, lb, oz) or temperature (c, f, k).",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"value": map[string]any{"type": "number"}, "from": map[string]any{"type": "string"}, "to": map[string]any{"type": "string"}},
			"required": []string{"value", "from", "to"}, "additionalProperties": false}},
	{Name: "echo_env", Description: "Return an environment variable of the server process (demonstrates the Gateway allowlist; not allowed by the demo config).",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}},
			"required": []string{"name"}}},
}

// Handle answers one JSON-RPC message; ok is false for notifications (no response).
func Handle(raw []byte) (resp []byte, ok bool) {
	var m message
	if err := json.Unmarshal(raw, &m); err != nil {
		return encode(json.RawMessage("null"), nil, &rpcErr{-32700, "parse error"}), true
	}
	if len(m.ID) == 0 {
		return nil, false
	}
	switch m.Method {
	case "initialize":
		return encode(m.ID, map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}},
			"serverInfo": map[string]any{"name": "agentbox-mcp-demo", "version": "1"}}, nil), true
	case "ping":
		return encode(m.ID, map[string]any{}, nil), true
	case "tools/list":
		return encode(m.ID, map[string]any{"tools": tools}, nil), true
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(m.Params, &p) != nil {
			return encode(m.ID, nil, &rpcErr{-32602, "invalid params"}), true
		}
		text, structured, err := call(p.Name, p.Arguments)
		if errors.Is(err, errUnknownTool) {
			return encode(m.ID, nil, &rpcErr{-32602, "unknown tool " + p.Name}), true
		}
		result := map[string]any{"content": []map[string]any{{"type": "text", "text": text}}, "isError": err != nil}
		if err != nil {
			result["content"] = []map[string]any{{"type": "text", "text": err.Error()}}
		} else if structured != nil {
			result["structuredContent"] = structured
		}
		return encode(m.ID, result, nil), true
	}
	return encode(m.ID, nil, &rpcErr{-32601, "method not found"}), true
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func encode(id json.RawMessage, result any, e *rpcErr) []byte {
	m := map[string]any{"jsonrpc": "2.0", "id": id}
	if e != nil {
		m["error"] = e
	} else {
		m["result"] = result
	}
	b, _ := json.Marshal(m) // plain maps always encode
	return b
}

var errUnknownTool = errors.New("unknown tool")

func call(name string, args json.RawMessage) (string, map[string]any, error) {
	switch name {
	case "calculate":
		var a struct {
			Expression string `json:"expression"`
		}
		if json.Unmarshal(args, &a) != nil || strings.TrimSpace(a.Expression) == "" {
			return "", nil, errors.New("expression is required")
		}
		v, err := Evaluate(a.Expression)
		if err != nil {
			return "", nil, err
		}
		s := format(v)
		return a.Expression + " = " + s, map[string]any{"value": v}, nil
	case "unit_convert":
		var a struct {
			Value    *float64 `json:"value"`
			From, To string
		}
		if json.Unmarshal(args, &a) != nil || a.Value == nil {
			return "", nil, errors.New("value, from and to are required")
		}
		v, err := convert(*a.Value, strings.ToLower(a.From), strings.ToLower(a.To))
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s = %s %s", format(*a.Value), a.From, format(v), a.To), map[string]any{"value": v}, nil
	case "echo_env":
		var a struct{ Name string }
		_ = json.Unmarshal(args, &a)
		return os.Getenv(a.Name), nil, nil
	}
	return "", nil, errUnknownTool
}

func format(v float64) string { return strconv.FormatFloat(v, 'g', 12, 64) }

var factors = map[string]struct {
	dim string
	f   float64
}{
	"m": {"len", 1}, "km": {"len", 1000}, "cm": {"len", 0.01}, "mm": {"len", 0.001}, "mi": {"len", 1609.344},
	"ft": {"len", 0.3048}, "in": {"len", 0.0254}, "kg": {"mass", 1}, "g": {"mass", 0.001}, "lb": {"mass", 0.45359237},
	"oz": {"mass", 0.028349523125},
}

func convert(v float64, from, to string) (float64, error) {
	temp := func(u string) bool { return u == "c" || u == "f" || u == "k" }
	if temp(from) && temp(to) {
		c := v
		switch from {
		case "f":
			c = (v - 32) * 5 / 9
		case "k":
			c = v - 273.15
		}
		switch to {
		case "f":
			return c*9/5 + 32, nil
		case "k":
			return c + 273.15, nil
		}
		return c, nil
	}
	a, ok1 := factors[from]
	b, ok2 := factors[to]
	if !ok1 || !ok2 || a.dim != b.dim {
		return 0, fmt.Errorf("cannot convert %s to %s", from, to)
	}
	return v * a.f / b.f, nil
}

// ---- expression evaluation (recursive descent; ^ is right-associative) ----

type parser struct {
	s   string
	pos int
}

// Evaluate computes an arithmetic expression.
func Evaluate(expr string) (float64, error) {
	if len(expr) > 1024 {
		return 0, errors.New("expression too long")
	}
	p := &parser{s: expr}
	v, err := p.sum()
	if err != nil {
		return 0, err
	}
	p.skip()
	if p.pos != len(p.s) {
		return 0, fmt.Errorf("unexpected %q at %d", p.s[p.pos:], p.pos)
	}
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0, errors.New("result is not a finite number")
	}
	return v, nil
}

func (p *parser) skip() {
	for p.pos < len(p.s) && unicode.IsSpace(rune(p.s[p.pos])) {
		p.pos++
	}
}

func (p *parser) peek() byte {
	p.skip()
	if p.pos < len(p.s) {
		return p.s[p.pos]
	}
	return 0
}

func (p *parser) sum() (float64, error) {
	v, err := p.product()
	for err == nil {
		switch p.peek() {
		case '+', '-':
			op := p.s[p.pos]
			p.pos++
			var r float64
			if r, err = p.product(); err == nil {
				if op == '+' {
					v += r
				} else {
					v -= r
				}
			}
		default:
			return v, nil
		}
	}
	return 0, err
}

func (p *parser) product() (float64, error) {
	v, err := p.power()
	for err == nil {
		switch p.peek() {
		case '*', '/', '%':
			op := p.s[p.pos]
			p.pos++
			var r float64
			if r, err = p.power(); err != nil {
				break
			}
			switch {
			case op == '*':
				v *= r
			case r == 0:
				return 0, errors.New("division by zero")
			case op == '/':
				v /= r
			default:
				v = math.Mod(v, r)
			}
		default:
			return v, nil
		}
	}
	return 0, err
}

func (p *parser) power() (float64, error) {
	base, err := p.unary()
	if err != nil {
		return 0, err
	}
	if p.peek() == '^' {
		p.pos++
		exp, err := p.power()
		if err != nil {
			return 0, err
		}
		return math.Pow(base, exp), nil
	}
	return base, nil
}

func (p *parser) unary() (float64, error) {
	switch p.peek() {
	case '-': // binds looser than ^: -2^2 = -4
		p.pos++
		v, err := p.power()
		return -v, err
	case '+':
		p.pos++
		return p.power()
	case '(':
		p.pos++
		v, err := p.sum()
		if err != nil {
			return 0, err
		}
		if p.peek() != ')' {
			return 0, errors.New("missing )")
		}
		p.pos++
		return v, nil
	}
	start := p.pos
	for p.pos < len(p.s) && (p.s[p.pos] >= '0' && p.s[p.pos] <= '9' || p.s[p.pos] == '.') {
		p.pos++
	}
	if start == p.pos {
		if p.pos >= len(p.s) {
			return 0, errors.New("unexpected end of expression")
		}
		return 0, fmt.Errorf("unexpected %q at %d", p.s[p.pos:p.pos+1], p.pos)
	}
	return strconv.ParseFloat(p.s[start:p.pos], 64)
}

// ---- transports ----

// ServeStdio serves newline-delimited JSON-RPC until r ends.
func ServeStdio(r io.Reader, w io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	bw := bufio.NewWriter(w)
	for sc.Scan() {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		if resp, ok := Handle(sc.Bytes()); ok {
			if _, err := bw.Write(append(resp, '\n')); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// Handler is a minimal streamable-HTTP endpoint: one JSON-RPC message per POST; requests are answered with JSON
// (or one SSE event when sse is true), notifications with 202. It issues an Mcp-Session-Id on initialize and, when
// requireSession is set, rejects other requests without it (404 for unknown sessions).
func Handler(sse, requireSession bool) http.Handler {
	var mu sync.Mutex
	sessions := map[string]bool{}
	n := 0
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var m message
		_ = json.Unmarshal(body, &m)
		if m.Method == "initialize" {
			mu.Lock()
			n++
			sid := fmt.Sprintf("demo-session-%d", n)
			sessions[sid] = true
			mu.Unlock()
			w.Header().Set("Mcp-Session-Id", sid)
		} else if requireSession {
			mu.Lock()
			ok := sessions[r.Header.Get("Mcp-Session-Id")]
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
		resp, ok := Handle(body)
		if !ok {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if sse {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", resp)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(resp)
	})
}
