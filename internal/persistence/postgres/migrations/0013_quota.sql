-- 0013：月度预算与预留（模型网关设计 2026-10-11 §4、§6.2；G2）。
-- quota_budgets：每个层级（org、user、key）每个 UTC 月一行，懒创建：新月份的行沿用该层级最近一个月份的 limit_micro
-- （NULL = 不限）。spent + reserved + unknown + 新预留 ≤ limit 才能预留（不变量 G2）。
-- quota_holds：一笔预留在其主体存在的每个层级预留同一金额（org 总在；user、key 视主体）；从 held 恰转入 settled、
-- released 或 charged_unknown 之一（不变量 G1：reserved = Σ held，spent + unknown = Σ 已结算的 charged）。
-- 运行时调用（source = task_call）的 hold 与任务账本的 reservation 同一事务创建与结算，hold_id 即 reservation_id。

CREATE TABLE quota_budgets (
    scope_kind     text NOT NULL CHECK (scope_kind IN ('org', 'user', 'key')),
    scope_id       text NOT NULL,
    period         date NOT NULL CHECK (extract(day FROM period) = 1),
    limit_micro    bigint CHECK (limit_micro IS NULL OR limit_micro >= 0),
    reserved_micro bigint NOT NULL DEFAULT 0 CHECK (reserved_micro >= 0),
    spent_micro    bigint NOT NULL DEFAULT 0 CHECK (spent_micro >= 0),
    unknown_micro  bigint NOT NULL DEFAULT 0 CHECK (unknown_micro >= 0),
    PRIMARY KEY (scope_kind, scope_id, period)
);

CREATE TABLE quota_holds (
    hold_id       text PRIMARY KEY,
    org_id        bigint NOT NULL REFERENCES orgs(id),
    user_id       bigint REFERENCES users(id),
    key_id        text REFERENCES api_keys(id),
    period        date NOT NULL CHECK (extract(day FROM period) = 1),
    source        text NOT NULL CHECK (source IN ('task_call', 'model_request')),
    ref           text NOT NULL,
    amount_micro  bigint NOT NULL CHECK (amount_micro >= 0),
    state         text NOT NULL CHECK (state IN ('held', 'settled', 'released', 'charged_unknown')),
    charged_micro bigint NOT NULL DEFAULT 0 CHECK (charged_micro >= 0),
    deadline_at   timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CHECK (state <> 'released' OR charged_micro = 0)
);
CREATE INDEX quota_holds_expiry ON quota_holds (deadline_at) WHERE state = 'held';
CREATE INDEX quota_holds_org_period ON quota_holds (org_id, period);
