ALTER TABLE tasks ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS workspace_manifest_sha256 text CHECK (workspace_manifest_sha256 ~ '^[0-9a-f]{64}$');
ALTER TABLE environments ADD COLUMN IF NOT EXISTS fenced_at timestamptz;
CREATE INDEX IF NOT EXISTS environments_pending_fence ON environments(session_id) WHERE fenced_at IS NOT NULL AND stopped_at IS NULL;
