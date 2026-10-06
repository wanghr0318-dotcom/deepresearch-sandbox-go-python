-- 0010：规格 §13.5 执行中修订（M4 验收，2026-10-06）——暂停期间 sub-run 的 deadline 不计时。
-- subruns.remaining_ms：任务被暂停（暂停裁决，或没有 attempt 的 queued → paused）时未终态 sub-run 的剩余时间
-- （GREATEST(deadline_at − now(), 0)，毫秒）；恢复时重新绑定的事务按 deadline_at = now() + remaining_ms 重新起算并
-- 清除它。故障重试与 server 重启不写它，deadline 仍为绝对时间、恢复不重置。
ALTER TABLE subruns ADD COLUMN remaining_ms bigint CHECK (remaining_ms IS NULL OR remaining_ms >= 0);
