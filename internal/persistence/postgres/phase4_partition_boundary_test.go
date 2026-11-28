package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// The controller's missing probes are represented by withheld renewals. Use
// wall time and the production PostgreSQL store; no live hosts are partitioned.
func TestPhase4PartitionBoundary(t *testing.T) {
	if os.Getenv("AGENTBOX_PHASE4_BOUNDARY") != "1" {
		t.Skip("opt-in 160-second boundary experiment")
	}
	const ttl = 120 * time.Second
	s := newStore(t, Options{AttemptLeaseTTL: ttl})
	ctx := context.Background()
	scenarios := []struct {
		name   string
		gaps   []int
		expiry bool
	}{
		{"half", []int{60}, false},
		{"near", []int{108}, false},
		{"past", []int{132}, true},
		{"jitter", []int{10, 15, 20}, false},
		// Emulate successful process probes during a quiet turn: no task
		// output or model events are submitted across four renewals.
		{"quiet", []int{40, 40, 40, 40}, false},
	}
	for _, sc := range scenarios {
		for round := 1; round <= 3; round++ {
			id := fmt.Sprintf("%s-%d", sc.name, round)
			t.Run(id, func(t *testing.T) {
				t.Parallel()
				fixture(t, s, id)
				original := "att-" + id
				next := task.NewAttempt{TaskID: id, AttemptID: "new-" + id, AttemptNo: 2, EnvID: "next-env-" + id, Retry: task.RetryFault}
				renewed := 0
				start := time.Now()
				for _, seconds := range sc.gaps {
					time.Sleep(time.Duration(seconds) * time.Second)
					expired, err := s.ExpiredAttemptLease(ctx, id, original)
					if err != nil || expired != sc.expiry {
						t.Fatalf("expired=%v: %v", expired, err)
					}
					err = s.RenewAttemptLease(ctx, id, original)
					if expired {
						expectRejected(t, err, persistence.CodeStaleAttempt)
						v := verdict(id, 1)
						v.TaskStatus, v.TaskStatusReason = "queued", "worker_error"
						if _, err = s.FinalizeAttempt(ctx, v); err != nil {
							t.Fatal(err)
						}
						stopEnv(t, s, "env-"+id)
						if _, err = s.CreateAttempt(ctx, next); err != nil {
							t.Fatal(err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						renewed++
						if _, err = s.CreateAttempt(ctx, next); !errors.Is(err, persistence.ErrRejected) && !errors.Is(err, persistence.ErrConflict) {
							t.Fatalf("concurrent claim: %v", err)
						}
					}
				}
				validOwners := 0
				var current string
				for _, attempt := range []string{original, next.AttemptID} {
					expired, err := s.ExpiredAttemptLease(ctx, id, attempt)
					if err != nil {
						t.Fatal(err)
					}
					if !expired {
						validOwners++
						current = attempt
					}
				}
				if validOwners != 1 || (current == next.AttemptID) != sc.expiry {
					t.Fatalf("owners=%d current=%s", validOwners, current)
				}
				t.Logf("boundary round=%s ttl_s=120 gaps_s=%v elapsed_s=%.3f renewals=%d replacement=%t valid_owners_at_end=%d", id, sc.gaps, time.Since(start).Seconds(), renewed, current == next.AttemptID, validOwners)
			})
		}
	}
}
