-- 0003：Gateway 的 task 层账本与调用 journal（规格 §6、§9.4–§9.7）。
-- sub-run 层（subrun_budgets）与 exec 配额（exec_quotas）属 M4，此处不建表；subrun_id 列预留为 NULL。
-- 相对 §6 的补充列：calls.fail_reason（置为 failed 的原因）、call_tries.error（try 的错误摘要，审计元数据）、
-- calls.resolving_since（resolving 正在由某次请求解析的起点；NULL 表示已复位、可重新解析，§11.2）。

CREATE TABLE budgets (
    task_id        text PRIMARY KEY REFERENCES tasks (task_id),
    limit_micro    bigint NOT NULL CHECK (limit_micro >= 0),
    reserved_micro bigint NOT NULL DEFAULT 0 CHECK (reserved_micro >= 0),
    spent_micro    bigint NOT NULL DEFAULT 0 CHECK (spent_micro >= 0),
    unknown_micro  bigint NOT NULL DEFAULT 0 CHECK (unknown_micro >= 0)
);

CREATE TABLE calls (
    task_id                     text NOT NULL REFERENCES tasks (task_id),
    call_id                     text NOT NULL,
    subrun_id                   text,
    fingerprint                 text NOT NULL,
    endpoint                    text NOT NULL,
    state                       text NOT NULL CHECK (state IN ('resolving', 'in_flight', 'completed', 'failed', 'unknown')),
    source                      text NOT NULL CHECK (source IN ('upstream', 'cache', 'coalesced', 'exec')),
    result_ref                  text,
    tries_used                  bigint NOT NULL DEFAULT 0 CHECK (tries_used >= 0),
    created_at                  timestamptz NOT NULL,
    deadline_at                 timestamptz NOT NULL,
    cost_charged                bigint NOT NULL DEFAULT 0,
    first_attempt_id            text NOT NULL,
    upstream_request_id         text NOT NULL DEFAULT '',
    supersedes_call_id          text,
    supersede_reason            text,
    possible_external_duplicate boolean NOT NULL DEFAULT false,
    fail_reason                 text NOT NULL DEFAULT '',
    resolving_since             timestamptz,
    PRIMARY KEY (task_id, call_id),
    CHECK ((state = 'completed') = (result_ref IS NOT NULL)),
    CHECK (resolving_since IS NULL OR state = 'resolving'),
    CHECK (deadline_at > created_at),
    FOREIGN KEY (task_id, first_attempt_id) REFERENCES attempts (task_id, attempt_id)
);

CREATE TABLE reservations (
    reservation_id text PRIMARY KEY,
    task_id        text NOT NULL REFERENCES tasks (task_id),
    subrun_id      text,
    call_id        text NOT NULL,
    try_no         bigint NOT NULL CHECK (try_no >= 1),
    kind           text NOT NULL CHECK (kind IN ('money')),
    amount         bigint NOT NULL CHECK (amount >= 0),
    state          text NOT NULL CHECK (state IN ('held', 'settled', 'released', 'charged_unknown')),
    UNIQUE (task_id, call_id, try_no),
    FOREIGN KEY (task_id, call_id) REFERENCES calls (task_id, call_id)
);

CREATE TABLE call_tries (
    task_id        text NOT NULL,
    call_id        text NOT NULL,
    try_no         bigint NOT NULL CHECK (try_no >= 1),
    attempt_id     text NOT NULL,
    env_id         text,
    state          text NOT NULL CHECK (state IN ('in_flight', 'settled')),
    outcome        text NOT NULL DEFAULT '' CHECK (outcome IN ('', 'ok', 'retryable', 'fatal', 'unknown')),
    latency_ms     bigint NOT NULL DEFAULT 0,
    cost_micro     bigint NOT NULL DEFAULT 0,
    reservation_id text NOT NULL UNIQUE REFERENCES reservations (reservation_id),
    error          text NOT NULL DEFAULT '',
    PRIMARY KEY (task_id, call_id, try_no),
    CHECK ((state = 'settled') = (outcome <> '')),
    FOREIGN KEY (task_id, call_id) REFERENCES calls (task_id, call_id),
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);

-- 迁移前已存在的任务补一行零预算：付费调用以 budget_exhausted 失败关闭，而不是找不到预算行。
INSERT INTO budgets (task_id, limit_micro, reserved_micro, spent_micro, unknown_micro)
    SELECT task_id, 0, 0, 0, 0 FROM tasks
    ON CONFLICT DO NOTHING;
