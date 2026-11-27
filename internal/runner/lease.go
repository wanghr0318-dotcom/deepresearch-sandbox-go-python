package runner

import (
	"context"
	"errors"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// A transient pod probe or store failure is inconclusive. Only the database's
// definitive stale grant ends this watch; callers bound each remote probe.
func (o Options) watchLease(ctx context.Context, a Attempt) (<-chan struct{}, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	if o.AttemptLeaseProbe == nil || o.AttemptLeaseProbeInterval <= 0 {
		return nil, cancel
	}
	expired := make(chan struct{})
	go func() {
		timer := time.NewTicker(o.AttemptLeaseProbeInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := o.AttemptLeaseProbe(ctx, a.EnvID, a.TaskID, a.AttemptID); errors.Is(err, persistence.ErrConflict) {
					close(expired)
					return
				}
			}
		}
	}()
	return expired, cancel
}
