package postgres

import (
	"context"
	"testing"
)

// EnvironmentStats: live environments by kind and the cleanup backlog (stopped, cleanup not done).
func TestEnvironmentStats(t *testing.T) {
	s := newStore(t, Options{})
	ctx := context.Background()
	running, backlog, err := s.EnvironmentStats(ctx)
	if err != nil || len(running) != 0 || backlog != 0 {
		t.Fatalf("empty: %v %d %v", running, backlog, err)
	}
	fixture(t, s, "t1") // task t1 with environment env-t1
	fixture(t, s, "t2")
	stopEnv(t, s, "env-t2")
	running, backlog, err = s.EnvironmentStats(ctx)
	if err != nil || running["task"] != 1 || backlog != 1 {
		t.Fatalf("got running=%v backlog=%d err=%v", running, backlog, err)
	}
}
