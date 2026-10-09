package session

import (
	"context"
	"errors"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

func TestOpSpans(t *testing.T) {
	tr, _ := obstest.Install(t)
	_, sp := opSpan(context.Background(), "s1", FreezeIncarnation{IncarnationID: "i1", EnvID: "e1"})
	endOpSpan(sp, OpDone{Op: OpFreeze})
	_, sp = opSpan(context.Background(), "s1", DestroyIncarnation{IncarnationID: "i1", EnvID: "e1", Reason: "evicted"})
	endOpSpan(sp, OpDone{Op: OpDestroy, Failed: errors.New("x")})
	if _, sp = opSpan(context.Background(), "s1", DoTransition{}); sp != nil {
		t.Error("store transitions must not be traced")
	}
	endOpSpan(sp, OpDone{}) // nil-safe
	s := tr.Spans()
	if len(s) != 2 || s[0].Name != "session.freeze" || s[0].Attrs["session.id"] != "s1" || s[0].Failed != "" || !s[0].Ended {
		t.Fatalf("freeze span %+v", s)
	}
	if s[1].Name != "session.destroy_incarnation" || s[1].Failed != "op_failed" || s[1].Attrs["reason"] != "evicted" {
		t.Errorf("destroy span %+v", s[1])
	}
}
