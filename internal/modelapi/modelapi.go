// Package modelapi serves the model API of the model gateway (design 2026-10-11 §7): OpenAI
// POST /v1/chat/completions, Anthropic POST /v1/messages and GET /v1/models, in front of the Gateway's model routes.
//
// A request goes through the §7.7 steps in order: the caller is already authenticated (internal/api puts the
// tenant.Principal in the context and checks the models scope); the body is decoded in the client's format; rate
// limits are taken (requests and estimated tokens per minute at the key, user and org levels, plus a stream slot);
// the logical model is resolved to candidate routes (format-only features pin the request to routes of its own
// format); the worst-case cost is reserved as a quota hold; routes are tried in the routing plan's order, failing
// over before the first byte; the reply is streamed or returned in the client's format (same-format streams are
// forwarded event by event); and finally the hold is settled with the actual cost (unknown outcomes conservatively,
// as charged_unknown), unused tokens are refunded, the stream slot is released and the request is recorded in
// model_requests with its tries.
//
// No model call runs without a hold: when PostgreSQL is unavailable the API answers 503 (fail closed).
package modelapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
)

// Paths of the model API.
const (
	PathChatCompletions = "/v1/chat/completions"
	PathMessages        = "/v1/messages"
	PathModels          = "/v1/models"
)

// Defaults.
const (
	DefaultMaxDuration = 10 * time.Minute
	DefaultMaxTokens   = 4096
	DefaultMaxBody     = 8 << 20
	DefaultBodyTimeout = 30 * time.Second
	// writeGrace is how long past the maximum duration a write may take (a reading client still gets the final error
	// event; a client that stopped reading is cut).
	writeGrace = 2 * time.Second
	// holdGrace is added to the maximum duration for the hold's deadline: settlement happens after the stream ends.
	holdGrace = 5 * time.Minute
	// finishTimeout bounds settlement and recording after the request (the client may be gone).
	finishTimeout = 30 * time.Second
)

// Config wires the model API.
type Config struct {
	// Routing returns the model routes; nil (no model adapter yet, e.g. diagnostic mode) answers 503.
	Routing func() *call.ModelRouting
	// Models lists the configured logical models (GET /v1/models shows those some route serves).
	Models []string
	// Holder reserves and settles the request's money (required).
	Holder quota.Holder
	// Limits reads the rpm, tpm and max_streams limits of the caller's levels (required).
	Limits Limits
	// Limiter takes rpm and tpm tokens; nil disables request and token limits.
	Limiter ratelimit.Limiter
	// Streams counts concurrent streams; nil uses a counter of its own.
	Streams *ratelimit.Concurrency
	// Store records model_requests (required).
	Store Store
	// MaxDuration bounds a whole request, streams included (default 10 min, --model-api-max-duration).
	MaxDuration time.Duration
	// DefaultMaxTokens is sent when the client gives no max_tokens, so the worst-case cost is bounded (default 4096).
	DefaultMaxTokens int64
	// MaxBody bounds the request body (default 8 MiB, --model-api-max-body).
	MaxBody int64
	// BodyTimeout bounds reading the request body (default 30 s).
	BodyTimeout time.Duration
	Logger  *slog.Logger
	Now     func() time.Time
	NewID   func() string
}

// Handler serves the model API. internal/api routes the three paths to it after authentication.
type Handler struct {
	cfg Config
}

// New returns the handler; Holder, Limits and Store are required.
func New(cfg Config) *Handler {
	if cfg.MaxDuration <= 0 {
		cfg.MaxDuration = DefaultMaxDuration
	}
	if cfg.DefaultMaxTokens <= 0 {
		cfg.DefaultMaxTokens = DefaultMaxTokens
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = DefaultMaxBody
	}
	if cfg.BodyTimeout <= 0 {
		cfg.BodyTimeout = DefaultBodyTimeout
	}
	if cfg.Streams == nil {
		cfg.Streams = ratelimit.NewConcurrency()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = randomID
	}
	if cfg.Routing == nil {
		cfg.Routing = func() *call.ModelRouting { return nil }
	}
	return &Handler{cfg: cfg}
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "mr_" + hex.EncodeToString(b[:])
}

// ProtocolOf is the client's format of a model API request: Anthropic for /v1/messages, and for /v1/models when
// the anthropic-version header is present; OpenAI otherwise.
func ProtocolOf(r *http.Request) llmproto.Protocol {
	if r.URL.Path == PathMessages || (r.URL.Path == PathModels && r.Header.Get("anthropic-version") != "") {
		return llmproto.ProtocolAnthropic
	}
	return llmproto.ProtocolOpenAI
}

// ServeHTTP dispatches the three model API paths.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proto := ProtocolOf(r)
	if v := r.Header.Get("anthropic-version"); v != "" && proto == llmproto.ProtocolAnthropic {
		w.Header().Set("anthropic-version", v)
	}
	switch {
	case r.Method == http.MethodPost && (r.URL.Path == PathChatCompletions || r.URL.Path == PathMessages):
		h.serveChat(w, r, proto)
	case r.Method == http.MethodGet && r.URL.Path == PathModels:
		h.serveModels(w, proto)
	default:
		WriteError(w, proto, llmproto.ClassInvalidRequest, "no such model API endpoint")
	}
}

// WriteError writes an error in the client's format (design §7.5).
func WriteError(w http.ResponseWriter, p llmproto.Protocol, class llmproto.ErrorClass, msg string) {
	writeErrorInfo(w, p, llmproto.ErrorInfo{Class: class, Message: msg})
}

func writeErrorInfo(w http.ResponseWriter, p llmproto.Protocol, e llmproto.ErrorInfo) {
	status, body := llmproto.EncodeError(p, e)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// reject writes a refusal before any provider was tried and counts it.
func reject(w http.ResponseWriter, p llmproto.Protocol, reason string, e llmproto.ErrorInfo) {
	obs.M().ModelRejected(string(p), reason)
	writeErrorInfo(w, p, e)
}

type openAIModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type anthropicModel struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	CreatedAt   string `json:"created_at"`
}

// serveModels lists the logical models some route serves (every key may use every configured model).
func (h *Handler) serveModels(w http.ResponseWriter, p llmproto.Protocol) {
	rt := h.cfg.Routing()
	if rt == nil {
		WriteError(w, p, llmproto.ClassUnavailable, "the model routes are not ready")
		return
	}
	var ids []string
	for _, m := range h.cfg.Models {
		if !slices.Contains(ids, m) && !rt.Plan(m, "").NoRoute() {
			ids = append(ids, m)
		}
	}
	var v any
	if p == llmproto.ProtocolAnthropic {
		data := make([]anthropicModel, 0, len(ids))
		for _, id := range ids {
			data = append(data, anthropicModel{Type: "model", ID: id, DisplayName: id, CreatedAt: "1970-01-01T00:00:00Z"})
		}
		page := map[string]any{"data": data, "has_more": false, "first_id": nil, "last_id": nil}
		if len(ids) > 0 {
			page["first_id"], page["last_id"] = ids[0], ids[len(ids)-1]
		}
		v = page
	} else {
		data := make([]openAIModel, 0, len(ids))
		for _, id := range ids {
			data = append(data, openAIModel{ID: id, Object: "model", OwnedBy: "agentbox"})
		}
		v = map[string]any{"object": "list", "data": data}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(v)
}
