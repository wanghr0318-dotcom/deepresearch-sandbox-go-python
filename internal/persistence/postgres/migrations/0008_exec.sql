-- 0008：exec 配额与运行记录（规格 §6、§10.3；Plan 15 D9）。
-- 相对 §6 的补充：exec_quotas.cpu_unknown_usec（结果未知的 CPU 预留，同 budgets.unknown_micro）与 blocked（CPU 超额后
-- 阻止该任务后续 exec）；reservations.kind 增加 cpu（exec 的 CPU 预留，单位 usec，任务级、不带 subrun_id）；
-- call_tries 记录 exec 的 CPU 实测（NULL = 读取失败，按全额预留计入）、wall 与排队时间、启动标记（I12 [A]）。
-- exec_quotas 行在首次 exec 的预留事务中按 server 策略建立（CreateTask 不变）。
ALTER TABLE reservations DROP CONSTRAINT reservations_kind_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_kind_check CHECK (kind IN ('money', 'cpu'));

CREATE TABLE exec_quotas (
    task_id           text PRIMARY KEY REFERENCES tasks (task_id),
    exec_count_limit  bigint NOT NULL CHECK (exec_count_limit >= 0),
    exec_count_used   bigint NOT NULL DEFAULT 0 CHECK (exec_count_used >= 0),
    cpu_limit_usec    bigint NOT NULL CHECK (cpu_limit_usec >= 0),
    cpu_reserved_usec bigint NOT NULL DEFAULT 0 CHECK (cpu_reserved_usec >= 0),
    cpu_spent_usec    bigint NOT NULL DEFAULT 0 CHECK (cpu_spent_usec >= 0),
    cpu_unknown_usec  bigint NOT NULL DEFAULT 0 CHECK (cpu_unknown_usec >= 0),
    wall_limit_ms     bigint NOT NULL CHECK (wall_limit_ms >= 0),
    wall_spent_ms     bigint NOT NULL DEFAULT 0 CHECK (wall_spent_ms >= 0),
    blocked           boolean NOT NULL DEFAULT false
);

ALTER TABLE call_tries ADD COLUMN cpu_usec bigint;
ALTER TABLE call_tries ADD COLUMN wall_ms bigint;
ALTER TABLE call_tries ADD COLUMN queue_ms bigint;
ALTER TABLE call_tries ADD COLUMN exec_started_at timestamptz;
CREATE INDEX environments_exec_attempt ON environments (attempt_id) WHERE kind = 'exec';
