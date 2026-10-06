-- 0009：M4 真实验收（2026-10-06）后的两项修复。
--
-- 1. calls.tool_budget_used：搜索与抓取调用在 Tx1 新登记时的工具调用计数（tool_calls_used 加 1 之后的值）。
--    同 call_id 的重放、复位后接管与提交结果未知后的重跑返回这个值，而不是任务当前的计数：Worker 把
--    X-Agentbox-Tool-Budget 写进模型提示，崩溃恢复时重放出的请求体必须与首次相同（否则 fingerprint_mismatch）。
--    模型调用与 0009 之前登记的调用为 NULL（后者按当前计数返回）。
-- 2. 收尾遗留的未终态 sub-run：P14-T11 之前的二进制在没有 attempt 的取消（例如暂停中的会话 turn 被新消息取代，D5）
--    时不收尾 sub-run，终态任务留下 started 等行（I13）。按 closeOpenSubrunsTx 的规则一次性收尾：cancelled →
--    cancelled{task_cancel}（已有取消原因时保留），succeeded → failed{not_completed_at_result}，failed → failed{task_failed}。
ALTER TABLE calls ADD COLUMN tool_budget_used bigint CHECK (tool_budget_used IS NULL OR tool_budget_used >= 1);

UPDATE subruns s SET
    status         = CASE WHEN t.status = 'cancelled' THEN 'cancelled' ELSE 'failed' END,
    failure_reason = CASE t.status WHEN 'cancelled' THEN 'task_cancel' WHEN 'succeeded' THEN 'not_completed_at_result'
                     ELSE 'task_failed' END,
    cancel_reason  = CASE WHEN t.status = 'cancelled' AND s.cancel_reason = '' THEN 'task_cancel' ELSE s.cancel_reason END,
    ended_at       = COALESCE(s.ended_at, now())
FROM tasks t
WHERE t.task_id = s.task_id AND t.status IN ('succeeded', 'failed', 'cancelled')
    AND s.status IN ('started', 'end_proposed', 'cancel_requested');
