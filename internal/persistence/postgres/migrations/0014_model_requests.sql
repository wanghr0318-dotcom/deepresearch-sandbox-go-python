-- 0014：模型 API 请求记录（模型网关设计 2026-10-11 §4、§7.7 第 8 步；G3）。
-- model_requests：每个取得了预算 hold 的模型 API 请求一行，在 hold 结算（或释放）之后写入；被认证、限流、预算拒绝的请求
-- 没有 hold，也没有行（只计入指标）。不变量 G3：每行恰好对应一个 source = model_request、ref = 行 id 的 hold，且该 hold
-- 已不是 held。prompt_tokens 是全部输入 token（含缓存读写，OpenAI 的 prompt_tokens 语义），缓存部分另列。
-- dropped_fields 是格式转换时丢弃的只有源格式才有的字段（§7.4），原样转发时为空。
-- model_request_tries：每次尝试一个供应商一行（try_no 从 1 起；被跳过的路由记在下一次尝试的 skipped_reason 中）。
-- 两表都按 created_at（请求开始时间）按 UTC 月分区；分区由 agentbox_model_request_partitions(t) 按需创建（写入前，
-- 以及本迁移为当月与下月），旧分区可整体删除（tries 随之级联）。

CREATE TABLE model_requests (
    id                text NOT NULL,
    org_id            bigint NOT NULL REFERENCES orgs(id),
    user_id           bigint REFERENCES users(id),
    key_id            text REFERENCES api_keys(id),
    client_protocol   text NOT NULL CHECK (client_protocol IN ('openai', 'anthropic')),
    model             text NOT NULL,
    stream            boolean NOT NULL,
    provider          text,
    provider_protocol text CHECK (provider_protocol IN ('openai', 'anthropic')),
    converted         boolean NOT NULL DEFAULT false,
    status            integer NOT NULL,
    error_code        text,
    prompt_tokens      bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
    completion_tokens  bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
    cache_read_tokens  bigint NOT NULL DEFAULT 0 CHECK (cache_read_tokens >= 0),
    cache_write_tokens bigint NOT NULL DEFAULT 0 CHECK (cache_write_tokens >= 0),
    usage_known       boolean NOT NULL DEFAULT false,
    cost_micro        bigint NOT NULL DEFAULT 0 CHECK (cost_micro >= 0),
    ttfb_ms           integer,
    latency_ms        integer NOT NULL,
    stream_complete   boolean,
    dropped_fields    text[] NOT NULL DEFAULT '{}',
    hold_id           text NOT NULL REFERENCES quota_holds(hold_id),
    created_at        timestamptz NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX model_requests_org_created ON model_requests (org_id, created_at);
CREATE INDEX model_requests_hold ON model_requests (hold_id);
CREATE INDEX quota_holds_model_request ON quota_holds (ref) WHERE source = 'model_request';

CREATE TABLE model_request_tries (
    request_id     text NOT NULL,
    created_at     timestamptz NOT NULL,
    try_no         integer NOT NULL CHECK (try_no >= 1),
    provider       text NOT NULL,
    outcome        text NOT NULL CHECK (outcome IN ('ok', 'retryable', 'fatal', 'unknown')),
    status         integer NOT NULL,
    latency_ms     integer NOT NULL,
    cost_micro     bigint NOT NULL DEFAULT 0 CHECK (cost_micro >= 0),
    skipped_reason text NOT NULL DEFAULT '',
    PRIMARY KEY (request_id, created_at, try_no),
    FOREIGN KEY (request_id, created_at) REFERENCES model_requests (id, created_at) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

CREATE FUNCTION agentbox_model_request_partitions(t timestamptz) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    m  date := date_trunc('month', t AT TIME ZONE 'UTC')::date;
    lo timestamptz := m::timestamp AT TIME ZONE 'UTC';
    hi timestamptz := (m + interval '1 month')::timestamp AT TIME ZONE 'UTC';
    sx text := to_char(m, 'YYYYMM');
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF model_requests FOR VALUES FROM (%L) TO (%L)',
        'model_requests_' || sx, lo, hi);
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF model_request_tries FOR VALUES FROM (%L) TO (%L)',
        'model_request_tries_' || sx, lo, hi);
END
$$;

SELECT agentbox_model_request_partitions(now());
SELECT agentbox_model_request_partitions(now() + interval '1 month');
