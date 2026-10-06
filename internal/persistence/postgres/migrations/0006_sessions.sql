-- 0006：会话（规格 §6、§12；M4 Plan 12）。Plan 11 的认证会话表改名为 auth_sessions，sessions 归规格 §6 的会话。
-- 相对 §6 的补充：sessions.owner_user_id / title / title_source / next_turn_index / last_error / created_at，
-- session_control.applied_wake_version，session_event_seq 与 session_events（会话生命周期事件），
-- events.session_id / session_seq（会话 task 事件的会话序号），tasks.turn_index / restored_from_task_id /
-- resume_directive，budgets.tool_call_limit / tool_calls_used（每 turn 的工具调用额度）。
ALTER TABLE sessions RENAME TO auth_sessions;
ALTER INDEX sessions_user_id RENAME TO auth_sessions_user_id;
ALTER TABLE auth_sessions RENAME CONSTRAINT sessions_pkey TO auth_sessions_pkey;
ALTER TABLE auth_sessions RENAME CONSTRAINT sessions_user_id_fkey TO auth_sessions_user_id_fkey;

CREATE TABLE sessions (
    session_id             text PRIMARY KEY,
    owner_user_id          bigint REFERENCES users(id),
    title                  text NOT NULL DEFAULT '',
    title_source           text NOT NULL DEFAULT 'auto' CHECK (title_source IN ('auto', 'user')),
    status                 text NOT NULL CHECK (status IN ('creating','idle','running','quiescing','frozen',
                               'evicting','evicted','restoring','closing','closed')),
    worker_image           text NOT NULL DEFAULT '',
    uid_range_id           text,
    current_incarnation_id text,
    current_task_id        text,
    blocked_by_task_id     text,
    next_turn_index        bigint NOT NULL DEFAULT 0,
    idle_since             timestamptz,
    frozen_since           timestamptz,
    last_active_at         timestamptz NOT NULL DEFAULT now(),
    expires_at             timestamptz,             -- 设计 D4：保留到用户删除，恒为 NULL
    last_error             text NOT NULL DEFAULT '',
    row_version            bigint NOT NULL DEFAULT 0,
    created_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sessions_owner_active ON sessions(owner_user_id, last_active_at DESC) WHERE status <> 'closed';
CREATE TABLE session_control (
    session_id             text PRIMARY KEY REFERENCES sessions(session_id),
    control_version        bigint NOT NULL DEFAULT 0,
    desired                text NOT NULL DEFAULT 'active' CHECK (desired IN ('active', 'closed')),
    wake_requested_version bigint NOT NULL DEFAULT 0,
    applied_wake_version   bigint NOT NULL DEFAULT 0
);
CREATE TABLE session_progress (
    session_id           text PRIMARY KEY REFERENCES sessions(session_id),
    latest_checkpoint_id text,
    latest_commit_seq    bigint NOT NULL DEFAULT 0
);
CREATE TABLE session_event_seq (
    session_id text PRIMARY KEY REFERENCES sessions(session_id),
    next       bigint NOT NULL DEFAULT 0
);
CREATE TABLE session_events (
    session_id  text NOT NULL REFERENCES sessions(session_id),
    session_seq bigint NOT NULL,
    event_key   text NOT NULL,
    type        text NOT NULL,
    payload     jsonb NOT NULL,
    ts          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, session_seq),
    UNIQUE (session_id, event_key)
);
CREATE TABLE incarnations (
    incarnation_id text PRIMARY KEY,
    session_id     text NOT NULL REFERENCES sessions(session_id),
    env_id         text NOT NULL,
    status         text NOT NULL CHECK (status IN ('starting','idle','busy','releasing','quiescing','frozen','ended')),
    started_at     timestamptz NOT NULL DEFAULT now(),
    ended_at       timestamptz,
    end_reason     text NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX incarnations_one_live ON incarnations(session_id) WHERE status <> 'ended';  -- I9

ALTER TABLE tasks ADD CONSTRAINT tasks_session_fk FOREIGN KEY (session_id) REFERENCES sessions(session_id);
ALTER TABLE tasks ADD COLUMN turn_index bigint;
ALTER TABLE tasks ADD COLUMN restored_from_task_id text REFERENCES tasks(task_id);
ALTER TABLE tasks ADD COLUMN resume_directive jsonb;
CREATE UNIQUE INDEX tasks_session_turn ON tasks(session_id, turn_index) WHERE session_id IS NOT NULL;

ALTER TABLE events ADD COLUMN session_id text;
ALTER TABLE events ADD COLUMN session_seq bigint;
CREATE UNIQUE INDEX events_session_seq ON events(session_id, session_seq) WHERE session_id IS NOT NULL;

ALTER TABLE budgets ADD COLUMN tool_call_limit bigint CHECK (tool_call_limit IS NULL OR tool_call_limit >= 0);
ALTER TABLE budgets ADD COLUMN tool_calls_used bigint NOT NULL DEFAULT 0 CHECK (tool_calls_used >= 0);
ALTER TABLE budgets ADD CONSTRAINT budgets_tool_within CHECK (tool_call_limit IS NULL OR tool_calls_used <= tool_call_limit);
