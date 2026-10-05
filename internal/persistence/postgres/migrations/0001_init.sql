-- 0001：M1 需要的全部表（规格 §6；设计 §1.3）。
-- 与 installation(新 ID, pending) 在同一事务中执行（规格 §7.4）。
-- 相对 §6 的补充列：events.worker_seq / content_hash，attempts.verdict_hash /
-- terminal_proposal_hash，uid_ranges.allocation_id（设计 §1.3、§2.4）。

CREATE TABLE installation (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    install_id text NOT NULL,
    state      text NOT NULL CHECK (state IN ('pending', 'complete')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_requests (
    request_id  text PRIMARY KEY,
    kind        text NOT NULL,
    body_hash   bytea NOT NULL,
    resource_id text NOT NULL,
    response    jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    task_id                    text PRIMARY KEY,
    session_id                 text,
    spec_json                  jsonb NOT NULL,
    config_version             text NOT NULL DEFAULT '',
    limits_json                jsonb,
    status                     text NOT NULL,
    status_reason              text NOT NULL DEFAULT '',
    current_attempt_id         text,
    resume_point               jsonb,
    base_session_checkpoint_id text,
    result_json                jsonb,
    attempts_total             bigint NOT NULL DEFAULT 0,
    fault_retries_used         bigint NOT NULL DEFAULT 0,
    max_fault_retries          bigint NOT NULL,
    oom_retries_used           bigint NOT NULL DEFAULT 0,
    run_time_ms                bigint NOT NULL DEFAULT 0,
    run_time_persisted_at      timestamptz,
    not_before                 timestamptz,
    applied_control_version    bigint NOT NULL DEFAULT 0,
    row_version                bigint NOT NULL DEFAULT 0,
    created_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE task_control (
    task_id         text PRIMARY KEY REFERENCES tasks (task_id),
    control_version bigint NOT NULL,
    desired         text NOT NULL CHECK (desired IN ('run', 'pause', 'cancel')),
    reason          text NOT NULL DEFAULT ''
);

CREATE TABLE task_progress (
    task_id              text PRIMARY KEY REFERENCES tasks (task_id),
    latest_checkpoint_id text,
    latest_commit_seq    bigint NOT NULL DEFAULT 0
);

CREATE TABLE task_event_seq (
    task_id text PRIMARY KEY REFERENCES tasks (task_id),
    next    bigint NOT NULL DEFAULT 0
);

CREATE TABLE attempts (
    attempt_id             text PRIMARY KEY,
    task_id                text NOT NULL REFERENCES tasks (task_id),
    attempt_no             bigint NOT NULL,
    env_id                 text NOT NULL,
    status                 text NOT NULL,
    outcome_class          text NOT NULL DEFAULT '',
    terminal_proposal      text,
    terminal_proposal_ref  text,
    terminal_proposal_hash bytea,
    verdict_hash           bytea,
    exit_code              bigint,
    exit_signal            bigint,
    oom_kill_delta         bigint NOT NULL DEFAULT 0,
    platform_killed        boolean NOT NULL DEFAULT false,
    created_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (task_id, attempt_id),
    UNIQUE (task_id, attempt_no)
);

CREATE TABLE attempt_access (
    attempt_id text PRIMARY KEY,
    task_id    text NOT NULL,
    state      text NOT NULL CHECK (state IN ('active', 'revoked')),
    revoked_at timestamptz,
    reason     text NOT NULL DEFAULT '',
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);

CREATE TABLE events (
    task_id      text NOT NULL REFERENCES tasks (task_id),
    task_seq     bigint NOT NULL,
    event_key    text NOT NULL,
    attempt_id   text,
    subrun_id    text,
    worker_seq   bigint,
    source       text NOT NULL CHECK (source IN ('host', 'worker')),
    type         text NOT NULL,
    payload      jsonb NOT NULL,
    content_hash bytea NOT NULL,
    ts           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, task_seq),
    UNIQUE (task_id, event_key),
    CHECK ((source = 'worker') = (worker_seq IS NOT NULL)),
    -- 任务内引用用复合外键，防止跨任务引用（规格 §6）；MATCH SIMPLE：attempt_id 为 NULL 的宿主事件不受约束
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);
CREATE UNIQUE INDEX events_worker_seq ON events (attempt_id, worker_seq) WHERE worker_seq IS NOT NULL;

CREATE TABLE environments (
    env_id        text PRIMARY KEY,
    kind          text NOT NULL CHECK (kind IN ('task', 'session', 'exec')),
    session_id    text,
    attempt_id    text,
    status        text NOT NULL,
    sandbox_name  text NOT NULL DEFAULT '',
    uid_range_id  text,
    stopped_at    timestamptz,
    cleanup_state text NOT NULL DEFAULT 'none' CHECK (cleanup_state IN ('none', 'pending', 'done')),
    cleanup_tries bigint NOT NULL DEFAULT 0,
    next_retry_at timestamptz,
    cleanup_error text NOT NULL DEFAULT '',
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE uid_ranges (
    uid_range_id  text PRIMARY KEY,
    base          bigint NOT NULL UNIQUE,
    size          bigint NOT NULL,
    state         text NOT NULL CHECK (state IN ('free', 'assigned', 'quarantined')),
    owner_kind    text NOT NULL DEFAULT '',
    owner_id      text NOT NULL DEFAULT '',
    allocation_id text NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX uid_ranges_owner ON uid_ranges (owner_id) WHERE state = 'assigned';

CREATE TABLE resource_intents (
    intent_id text PRIMARY KEY,
    env_id    text NOT NULL,
    kind      text NOT NULL,
    name      text NOT NULL,
    state     text NOT NULL CHECK (state IN ('pending', 'acquired', 'released', 'failed')),
    UNIQUE (kind, name)
);

CREATE TABLE quarantined_resources (
    resource_path  text PRIMARY KEY,
    kind           text NOT NULL,
    observed_owner text NOT NULL DEFAULT '',
    reason         text NOT NULL,
    detected_at    timestamptz NOT NULL DEFAULT now(),
    alerted        boolean NOT NULL DEFAULT false
);

CREATE TABLE checkpoints (
    scope_kind    text NOT NULL,
    scope_id      text NOT NULL,
    checkpoint_id text NOT NULL,
    commit_seq    bigint NOT NULL,
    attempt_id    text NOT NULL,
    step_id       text NOT NULL,
    content_hash  bytea NOT NULL,
    state_inline  jsonb,
    state_ref     text,
    refs_json     jsonb NOT NULL DEFAULT '[]',
    subruns_json  jsonb NOT NULL DEFAULT '[]',
    committed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope_kind, scope_id, checkpoint_id),
    UNIQUE (scope_kind, scope_id, commit_seq),
    CHECK ((state_inline IS NULL) <> (state_ref IS NULL))
);

CREATE TABLE blobs (
    sha256 text PRIMARY KEY,
    size   bigint NOT NULL
);

CREATE TABLE scope_blobs (
    scope_kind text NOT NULL,
    scope_id   text NOT NULL,
    sha256     text NOT NULL REFERENCES blobs (sha256),
    PRIMARY KEY (scope_kind, scope_id, sha256)
);

CREATE TABLE blob_provenance (
    provenance_id bigserial PRIMARY KEY,
    scope_kind    text NOT NULL,
    scope_id      text NOT NULL,
    sha256        text NOT NULL REFERENCES blobs (sha256),
    source        text NOT NULL,
    ref           text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE artifact_heads (
    task_id      text NOT NULL REFERENCES tasks (task_id),
    artifact_id  text NOT NULL,
    next_version bigint NOT NULL DEFAULT 1,
    PRIMARY KEY (task_id, artifact_id)
);

CREATE TABLE artifacts (
    task_id     text NOT NULL,
    artifact_id text NOT NULL,
    version     bigint NOT NULL,
    sha256      text NOT NULL REFERENCES blobs (sha256),
    size        bigint NOT NULL,
    media_type  text NOT NULL,
    visibility  text NOT NULL CHECK (visibility IN ('output', 'internal')),
    attempt_id  text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, artifact_id, version),
    UNIQUE (task_id, artifact_id, sha256),
    FOREIGN KEY (task_id, artifact_id) REFERENCES artifact_heads (task_id, artifact_id),
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);
