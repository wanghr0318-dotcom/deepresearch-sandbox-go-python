package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

func TestLeaseWatchRequiresDefinitiveExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	next := make(chan error)
	observed := make(chan Attempt)
	o := Options{AttemptLeaseProbeInterval: time.Millisecond, AttemptLeaseProbe: func(ctx context.Context, env, task, id string) error {
		select {
		case observed <- Attempt{TaskID: task, AttemptID: id, EnvID: env}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case answer := <-next:
			return answer
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	a := Attempt{TaskID: "t1", AttemptID: "a1", EnvID: "e1"}
	expired, stop := o.watchLease(ctx, a)
	defer stop()
	for _, err := range []error{errors.New("temporarily unavailable"), nil, persistence.ErrConflict} {
		select {
		case <-expired:
			t.Fatal("transient error revoked grant")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case got := <-observed:
			if got.TaskID != a.TaskID || got.AttemptID != a.AttemptID || got.EnvID != a.EnvID {
				t.Fatalf("probe identity %+v", got)
			}
		}
		select {
		case next <- err:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-expired:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
