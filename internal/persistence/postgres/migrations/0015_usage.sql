-- 0015：按日用量汇总（模型网关设计 2026-10-11 §4、§8；G5）。
-- usage_daily：每 (UTC 日, org, user, key, model, source) 一行，由周期汇总作业从已结算的预算 hold 重算：
--   source = model_api：source = model_request 的 hold，模型与 token 取自对应的 model_requests 行；
--   source = runtime：source = task_call 的 hold（沙箱的 Gateway 调用），模型取自调用（calls.model；搜索、抓取等取端点名），
--   运行时调用不记录 token，token 为 0。
--   cost_micro 是 settled hold 的 charged，unknown_micro 是 charged_unknown hold 的 charged（与 quota_budgets 的
--   spent_micro、unknown_micro 一一对应，对账因此精确）；released 的 hold 不计。日期取 hold 的 created_at（UTC），
--   与 hold 的 period 同一个 now()，所以某月各日之和就是该月的预算用量。
--   user_id 0 表示没有用户（运维），key_id '' 表示没有 key（cookie、运维）。
-- usage_rollup_state：单行；watermark 是下次汇总须从哪一天起重算（其 hold 仍可能变化的最早一天，至多为一小时前所在的一天），
-- last_run_at 是上次汇总的数据库时间。

CREATE TABLE usage_daily (
    day               date NOT NULL,
    org_id            bigint NOT NULL REFERENCES orgs(id),
    user_id           bigint NOT NULL DEFAULT 0,
    key_id            text NOT NULL DEFAULT '',
    model             text NOT NULL DEFAULT '',
    source            text NOT NULL CHECK (source IN ('runtime', 'model_api')),
    requests          bigint NOT NULL DEFAULT 0 CHECK (requests >= 0),
    prompt_tokens     bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
    completion_tokens bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
    cost_micro        bigint NOT NULL DEFAULT 0 CHECK (cost_micro >= 0),
    unknown_micro     bigint NOT NULL DEFAULT 0 CHECK (unknown_micro >= 0),
    PRIMARY KEY (day, org_id, user_id, key_id, model, source)
);
CREATE INDEX usage_daily_org_day ON usage_daily (org_id, day);

CREATE TABLE usage_rollup_state (
    id          boolean PRIMARY KEY DEFAULT true CHECK (id),
    watermark   date,
    last_run_at timestamptz
);
INSERT INTO usage_rollup_state (id) VALUES (true);

-- 汇总按 hold 的创建时间扫描，并找仍可能变化的最早一天（held，或期限未过：结算后的 model_requests 行可能还在路上）。
-- 复审 N5：这些索引不带 CONCURRENTLY，建索引时短暂持有 quota_holds 的 SHARE 锁（阻塞预留）；迁移在 server 开始服务之前
-- 执行，可以接受。已有大量 hold 的安装升级时，建索引的时间计入启动时间。
CREATE INDEX quota_holds_created ON quota_holds (created_at);
CREATE INDEX quota_holds_held_created ON quota_holds (created_at) WHERE state = 'held';
CREATE INDEX quota_holds_deadline ON quota_holds (deadline_at);
