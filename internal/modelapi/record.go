package modelapi

import (
	"context"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
)

// Record is one model_requests row and its tries (design §4). It is written after the request's hold was settled
// or released, so every row's hold is no longer held (invariant G3).
type Record struct {
	ID     string
	OrgID  int64 // 0: the operator (the store resolves the reserved org "operator")
	UserID int64 // 0: none (operator)
	KeyID  string

	ClientProtocol llmproto.Protocol
	Model          string
	Stream         bool

	Provider         string // the route that answered (or the last one tried); "" when none was tried
	ProviderProtocol llmproto.Protocol
	Converted        bool

	Status    int    // HTTP status sent to the client
	ErrorCode string // "" on success; an error class or stream_error, client_disconnected, max_duration

	Usage      llmproto.Usage
	UsageKnown bool // the provider reported the final usage
	CostMicro  int64
	TTFB       *time.Duration // arrival to the provider's first content; nil when no content arrived
	Latency    time.Duration

	StreamComplete *bool // nil for non-stream requests
	Dropped        []string
	HoldID         quota.HoldID
	CreatedAt      time.Time

	Tries []Try
}

// Try is one model_request_tries row.
type Try struct {
	No        int
	Provider  string
	Outcome   string // ok, retryable, fatal, unknown
	Status    int
	Latency   time.Duration
	CostMicro int64
	Skipped   string // routes skipped before this try ("name:reason,…")
}

// Store persists model requests (implemented by internal/persistence/postgres).
type Store interface {
	// RecordModelRequest writes the request and its tries in one transaction; writing the same ID twice is a no-op.
	RecordModelRequest(ctx context.Context, r Record) error
}

// Limits reads the rate and concurrency limits of a scope's levels (quota.Store satisfies it).
type Limits interface {
	LimitsFor(ctx context.Context, s quota.Scope) (map[quota.Level]quota.Limits, error)
}
