-- 0012：多租户（模型网关设计 2026-10-11 §4、§5；G1）。org → user → key：orgs、org_members（每个用户恰属一个 org）、
-- api_keys（只存 secret 的 HMAC）、limits（G2 使用）；tasks 与 sessions 增加 org_id / key_id。
-- 回填：每个已有用户一个个人 org "user-<id>"（该用户为 owner）；运维 token 创建的任务与会话归保留 org "operator"。
-- 新行的 org_id 由触发器填写：有 key_id 时取 key 的 org（不变量 G5 由构造成立），否则取 owner 的 org，无主为 operator。

CREATE TABLE orgs (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    disabled   boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE org_members (
    org_id     bigint NOT NULL REFERENCES orgs(id),
    user_id    bigint NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,  -- 目前每个用户只属一个 org
    role       text NOT NULL CHECK (role IN ('owner', 'member')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, user_id)
);
CREATE TABLE api_keys (
    id           text PRIMARY KEY CHECK (id ~ '^k_[a-z2-7]{16}$'),
    org_id       bigint NOT NULL REFERENCES orgs(id),
    user_id      bigint NOT NULL REFERENCES users(id),
    name         text NOT NULL,
    secret_hmac  bytea NOT NULL CHECK (length(secret_hmac) = 32),
    scopes       text[] NOT NULL CHECK (cardinality(scopes) > 0 AND scopes <@ ARRAY['runtime', 'models']),
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz,
    revoked_at   timestamptz,
    last_used_at timestamptz
);
CREATE INDEX api_keys_org ON api_keys(org_id);
CREATE INDEX api_keys_user ON api_keys(user_id);
CREATE TABLE limits (
    scope_kind    text NOT NULL CHECK (scope_kind IN ('org', 'user', 'key')),
    scope_id      text NOT NULL,
    rpm           integer CHECK (rpm IS NULL OR rpm >= 0),
    tpm           integer CHECK (tpm IS NULL OR tpm >= 0),
    max_sandboxes integer CHECK (max_sandboxes IS NULL OR max_sandboxes >= 0),
    max_streams   integer CHECK (max_streams IS NULL OR max_streams >= 0),
    PRIMARY KEY (scope_kind, scope_id)
);

INSERT INTO orgs (name) VALUES ('operator');
INSERT INTO orgs (name) SELECT 'user-' || id FROM users ORDER BY id;
INSERT INTO org_members (org_id, user_id, role)
    SELECT o.id, u.id, 'owner' FROM users u JOIN orgs o ON o.name = 'user-' || u.id;

ALTER TABLE tasks ADD COLUMN org_id bigint REFERENCES orgs(id);
ALTER TABLE tasks ADD COLUMN key_id text REFERENCES api_keys(id);
ALTER TABLE sessions ADD COLUMN org_id bigint REFERENCES orgs(id);
ALTER TABLE sessions ADD COLUMN key_id text REFERENCES api_keys(id);
UPDATE tasks t SET org_id = COALESCE((SELECT m.org_id FROM org_members m WHERE m.user_id = t.owner_user_id),
    (SELECT id FROM orgs WHERE name = 'operator'));
UPDATE sessions s SET org_id = COALESCE((SELECT m.org_id FROM org_members m WHERE m.user_id = s.owner_user_id),
    (SELECT id FROM orgs WHERE name = 'operator'));
ALTER TABLE tasks ALTER COLUMN org_id SET NOT NULL;
ALTER TABLE sessions ALTER COLUMN org_id SET NOT NULL;
CREATE INDEX tasks_org ON tasks(org_id);
CREATE INDEX sessions_org ON sessions(org_id);

-- 新任务与会话的 org：key 的 org > owner 的 org > operator。owner 没有 org 是程序错误，直接拒绝。
CREATE FUNCTION agentbox_fill_org() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.org_id IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.key_id IS NOT NULL THEN
        SELECT k.org_id INTO NEW.org_id FROM api_keys k WHERE k.id = NEW.key_id;
    ELSIF NEW.owner_user_id IS NOT NULL THEN
        SELECT m.org_id INTO NEW.org_id FROM org_members m WHERE m.user_id = NEW.owner_user_id;
        IF NEW.org_id IS NULL THEN
            RAISE EXCEPTION 'user % has no org', NEW.owner_user_id;
        END IF;
    ELSE
        SELECT o.id INTO NEW.org_id FROM orgs o WHERE o.name = 'operator';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER tasks_fill_org BEFORE INSERT ON tasks FOR EACH ROW EXECUTE FUNCTION agentbox_fill_org();
CREATE TRIGGER sessions_fill_org BEFORE INSERT ON sessions FOR EACH ROW EXECUTE FUNCTION agentbox_fill_org();
