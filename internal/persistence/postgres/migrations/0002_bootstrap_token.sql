-- 安装身份引导令牌（规格 §7.4 修订）：发起引导的数据目录令牌的 SHA-256。
-- 修订前创建的记录为 NULL；非空时必须为 32 字节。
ALTER TABLE installation ADD COLUMN bootstrap_token_hash bytea
    CHECK (bootstrap_token_hash IS NULL OR octet_length(bootstrap_token_hash) = 32);
