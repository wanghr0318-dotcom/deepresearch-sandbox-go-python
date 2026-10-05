-- 0005：用户账号、服务端会话与任务归属（M3 Plan 11）。会话表只存会话 ID 的 SHA-256；
-- tasks.owner_user_id 为空表示经运维 token 或 CLI 创建的无主任务。

CREATE TABLE users (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username      text NOT NULL,
    username_key  text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    role          text NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    disabled      boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE sessions (
    id_hash      bytea PRIMARY KEY,
    user_id      bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sessions_user_id ON sessions(user_id);
ALTER TABLE tasks ADD COLUMN owner_user_id bigint REFERENCES users(id);
CREATE INDEX tasks_owner_created ON tasks(owner_user_id, created_at DESC) WHERE owner_user_id IS NOT NULL;
