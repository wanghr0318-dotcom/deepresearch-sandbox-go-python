package call

import (
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

func TestFencedOpaqueExecHoldsUnknown(t *testing.T) {
	h := newExecHarness(t, func(cfg *ExecConfig) { cfg.HoldUnknown = true }, execScript{startErr: provider.ErrControlLost})
	in := xinv("c1", execBody("command-with-side-effect", ""))
	for _, retry := range []bool{false, true} {
		in.Retry = retry
		if got := h.exec(t, in); got.Code != CodeExecUnknown {
			t.Fatalf("retry=%v got %+v", retry, got)
		}
	}
	if n := len(h.es.tries("t1", "c1")); n != 1 {
		t.Fatalf("started %d tries for unresolved command", n)
	}
}
