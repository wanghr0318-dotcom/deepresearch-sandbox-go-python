// Package mcp lets the agent use tools of operator-configured MCP servers through the Gateway (design
// docs/design/2026-10-10-shell-file-mcp-design.md §3.4). The sandbox never talks to an MCP server: the Gateway runs
// the MCP clients (stdio subprocess or streamable HTTP), exposes the allowlisted tools (Hub.Tools → GET
// /v1/mcp/tools) and executes tools/call as upstream kind "mcp" through the call coordinator (Adapter), which gives
// the call journal, idempotent replay, deadlines and the per-turn tool budget.
//
// Only tools/list and tools/call are implemented. Server credentials (env_from, headers_from_env) stay on the host
// and never appear in errors, the journal or events.
package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"time"
)

// Transports.
const (
	TransportStdio = "stdio"
	TransportHTTP  = "http"
)

// Defaults and bounds.
const (
	DefaultTimeout = 30 * time.Second
	MaxTimeout     = 120 * time.Second
	MaxServers     = 16
	MaxToolsPerSrv = 64
	// MaxToolNameBytes is the function-name limit of OpenAI-compatible model APIs.
	MaxToolNameBytes = 64
)

var (
	namePattern = regexp.MustCompile(`^[a-z0-9_]{1,32}$`)
	toolPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,57}$`)
	envPattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	hdrPattern  = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
)

// ServerConfig is one configured MCP server.
type ServerConfig struct {
	Name           string            `json:"name"`
	Transport      string            `json:"transport"`
	Command        []string          `json:"command,omitempty"`          // stdio: argv (no shell)
	Env            map[string]string `json:"env,omitempty"`              // stdio: literal environment
	EnvFrom        map[string]string `json:"env_from,omitempty"`         // stdio: NAME → host env var read at start
	URL            string            `json:"url,omitempty"`              // http
	HeadersFromEnv map[string]string `json:"headers_from_env,omitempty"` // http: header → host env var
	AllowedTools   []string          `json:"allowed_tools"`
	TimeoutMs      int64             `json:"timeout_ms,omitempty"`
}

// Timeout is the per-call timeout (default 30 s, at most 120 s).
func (s ServerConfig) Timeout() time.Duration {
	if s.TimeoutMs <= 0 {
		return DefaultTimeout
	}
	return min(time.Duration(s.TimeoutMs)*time.Millisecond, MaxTimeout)
}

// Allowed reports whether tool is on the server's allowlist.
func (s ServerConfig) Allowed(tool string) bool { return slices.Contains(s.AllowedTools, tool) }

// Config is the --mcp-config file.
type Config struct {
	Servers []ServerConfig `json:"servers"`
}

// LoadConfig reads and validates a config file.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("mcp: read config: %w", err)
	}
	return ParseConfig(b)
}

// ParseConfig parses and validates a config (unknown members are rejected).
func ParseConfig(b []byte) (Config, error) {
	var c Config
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("mcp: parse config: %w", err)
	}
	return c, c.Validate()
}

// Validate checks the config.
func (c Config) Validate() error {
	if len(c.Servers) == 0 || len(c.Servers) > MaxServers {
		return fmt.Errorf("mcp: config needs 1–%d servers", MaxServers)
	}
	seen := map[string]bool{}
	for i, s := range c.Servers {
		where := fmt.Sprintf("mcp: servers[%d]", i)
		if !namePattern.MatchString(s.Name) {
			return fmt.Errorf("%s: name %q must match %s", where, s.Name, namePattern)
		}
		if seen[s.Name] {
			return fmt.Errorf("%s: duplicate name %q", where, s.Name)
		}
		seen[s.Name] = true
		if len(s.AllowedTools) == 0 || len(s.AllowedTools) > MaxToolsPerSrv {
			return fmt.Errorf("%s (%s): allowed_tools must list 1–%d tools", where, s.Name, MaxToolsPerSrv)
		}
		for _, t := range s.AllowedTools {
			if !toolPattern.MatchString(t) || len(ToolName(s.Name, t)) > MaxToolNameBytes {
				return fmt.Errorf("%s (%s): tool name %q must match %s and mcp__<server>__<tool> must fit %d bytes",
					where, s.Name, t, toolPattern, MaxToolNameBytes)
			}
		}
		if s.TimeoutMs < 0 {
			return fmt.Errorf("%s (%s): timeout_ms must be ≥ 0", where, s.Name)
		}
		switch s.Transport {
		case TransportStdio:
			if len(s.Command) == 0 || s.Command[0] == "" || s.URL != "" || len(s.HeadersFromEnv) > 0 {
				return fmt.Errorf("%s (%s): stdio needs command and no url/headers_from_env", where, s.Name)
			}
			for k := range s.Env {
				if !envPattern.MatchString(k) {
					return fmt.Errorf("%s (%s): env name %q", where, s.Name, k)
				}
			}
			for k, v := range s.EnvFrom {
				if !envPattern.MatchString(k) || !envPattern.MatchString(v) {
					return fmt.Errorf("%s (%s): env_from %q → %q", where, s.Name, k, v)
				}
			}
		case TransportHTTP:
			u, err := url.Parse(s.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
				return fmt.Errorf("%s (%s): url must be http(s)://host/... without credentials", where, s.Name)
			}
			if len(s.Command) > 0 || len(s.Env) > 0 || len(s.EnvFrom) > 0 {
				return fmt.Errorf("%s (%s): http takes no command/env/env_from", where, s.Name)
			}
			for k, v := range s.HeadersFromEnv {
				if !hdrPattern.MatchString(k) || !envPattern.MatchString(v) {
					return fmt.Errorf("%s (%s): headers_from_env %q → %q", where, s.Name, k, v)
				}
			}
		default:
			return fmt.Errorf("%s (%s): transport must be stdio or http", where, s.Name)
		}
	}
	return nil
}

// ToolName is the agent-facing name of a server tool.
func ToolName(server, tool string) string { return "mcp__" + server + "__" + tool }

var errClosed = errors.New("mcp: hub closed")
