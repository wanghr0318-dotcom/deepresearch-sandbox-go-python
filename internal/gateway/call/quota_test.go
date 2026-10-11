package call

import (
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// 模型网关 G2：月度预算不足（insufficient_quota）与任务预算耗尽同类——402（Worker 按 BudgetExhausted 处理），可在并发
// 预留释放后重试。
func TestInsufficientQuotaIsABudgetRejection(t *testing.T) {
	if got := statusFor(persistence.CodeInsufficientQuota); got != 402 {
		t.Fatalf("statusFor(insufficient_quota) = %d", got)
	}
	if !retryableReason(persistence.CodeInsufficientQuota) {
		t.Fatal("insufficient_quota 应可重试")
	}
}
