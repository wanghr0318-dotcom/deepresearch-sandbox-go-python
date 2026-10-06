-- 0007：sub-run 定义与状态（规格 §6、§8.4、§13）与 sub-run 层账本（§9.6）。
-- 锁顺序见 events.go：subruns 位于 task_event_seq/session_event_seq 之后、attempts 之前；subrun_budgets 紧随 budgets。
CREATE TABLE subruns (
    task_id          text NOT NULL REFERENCES tasks (task_id),
    subrun_id        text NOT NULL CHECK (subrun_id ~ '^[a-z0-9][a-z0-9_-]{0,31}$' AND subrun_id <> 'root'),
    definition_hash  bytea NOT NULL,
    parent_step_id   text NOT NULL,
    status           text NOT NULL CHECK (status IN ('started', 'end_proposed', 'cancel_requested',
                                                     'completed', 'cancelled', 'failed', 'timed_out')),
    bound_attempt_id text NOT NULL,
    deadline_at      timestamptz NOT NULL,
    budget_cap_micro bigint CHECK (budget_cap_micro IS NULL OR budget_cap_micro >= 0),
    result_ref       text,
    failure_reason   text NOT NULL DEFAULT '',
    cancel_reason    text NOT NULL DEFAULT '',
    end_summary      text NOT NULL DEFAULT '',
    started_at       timestamptz NOT NULL DEFAULT now(),
    ended_at         timestamptz,
    PRIMARY KEY (task_id, subrun_id),
    CHECK ((status = 'completed') = (result_ref IS NOT NULL)),
    CHECK ((status IN ('completed', 'cancelled', 'failed', 'timed_out')) = (ended_at IS NOT NULL)),
    FOREIGN KEY (task_id, bound_attempt_id) REFERENCES attempts (task_id, attempt_id)
);

CREATE TABLE subrun_budgets (
    task_id        text NOT NULL,
    subrun_id      text NOT NULL,
    cap_micro      bigint CHECK (cap_micro IS NULL OR cap_micro >= 0),
    reserved_micro bigint NOT NULL DEFAULT 0 CHECK (reserved_micro >= 0),
    spent_micro    bigint NOT NULL DEFAULT 0 CHECK (spent_micro >= 0),
    unknown_micro  bigint NOT NULL DEFAULT 0 CHECK (unknown_micro >= 0),
    PRIMARY KEY (task_id, subrun_id),
    FOREIGN KEY (task_id, subrun_id) REFERENCES subruns (task_id, subrun_id)
);

-- 0003 预留的 subrun_id 列加外键（MATCH SIMPLE：root 调用为 NULL 不受约束）。约束显式命名，便于测试回滚。
ALTER TABLE calls ADD CONSTRAINT calls_subrun_fk FOREIGN KEY (task_id, subrun_id) REFERENCES subruns (task_id, subrun_id);
ALTER TABLE reservations ADD CONSTRAINT reservations_subrun_fk FOREIGN KEY (task_id, subrun_id) REFERENCES subruns (task_id, subrun_id);
CREATE INDEX reservations_subrun_held ON reservations (task_id, subrun_id) WHERE state = 'held' AND subrun_id IS NOT NULL;
