package app

// 本文件装配模型网关 G5 的用量统计（只在启用账号、即多租户开启时）：GET /v1/usage 与 GET /v1/bills 的存储，以及
// 周期汇总作业（把已结算的预算 hold 重算进 usage_daily；按 watermark 增量、幂等，各 server 的汇总经 advisory lock 串行）。

import (
	"context"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

// usageStorer 是 Store 的可选能力（internal/persistence/postgres 实现；测试用的 Store 不必实现）。
type usageStorer interface {
	Usage() usage.Store
}

// usageRollupInterval 是汇总间隔：用量页与账单至多落后这么久（响应中的 as_of 是上次汇总的时间）。
const usageRollupInterval = time.Minute

// rollupUsage 启动时汇总一次，然后周期汇总，直到 ctx 取消。every 为 0 时取 usageRollupInterval。
func (s *server) rollupUsage(ctx context.Context, u usage.Store, every time.Duration) {
	if every <= 0 {
		every = usageRollupInterval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := u.Rollup(ctx, time.Time{}); err != nil && ctx.Err() == nil {
			s.log.Warn("用量汇总失败（下次重试）", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
