package call

import (
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// 复审 F13：insufficient_quota 的说明给 Worker 看"本月配额已用尽"及层级，不带 ID 与金额。
func TestQuotaMessageOf(t *testing.T) {
	d := persistence.QuotaExhaustedMessage("user") + "（user 7/3：可用 1 / 上限 10，本次需要 5）"
	if got := persistence.QuotaMessageOf(d); got != "本月配额已用尽（user）" {
		t.Fatalf("%q", got)
	}
	if got := persistence.QuotaMessageOf("other"); got != "本月配额已用尽" {
		t.Fatalf("%q", got)
	}
}

// 复审 F13：Tx2 以 insufficient_quota 拒绝时，结果仍是 402 insufficient_quota（处理不变），说明为本月配额及层级。
func TestInsufficientQuotaResultMessage(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p"))
	h.store.reserveErr = &persistence.RejectedError{Code: persistence.CodeInsufficientQuota,
		Detail: persistence.QuotaExhaustedMessage("org") + "（org 2：可用 0 / 上限 10，本次需要 5）"}
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 402 || r.Code != persistence.CodeInsufficientQuota || r.Message != "本月配额已用尽（org）" {
		t.Fatalf("%+v", r)
	}
}
