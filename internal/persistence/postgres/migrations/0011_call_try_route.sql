-- 0011：模型降级链（docs/design/2026-10-10-model-fallback-design.md）的审计元数据，记在每次 try 上：
--   provider：执行该 try 的路由名（primary、backup……；单供应商配置为空串）；
--   skipped：本 try 之前跳过的路由与原因（"name:reason,..."，reason 为 circuit_open、tried、model_not_served）；
--   hedge：对冲请求的第二条腿（与另一个 try 并发持有预留）；
--   hedge_lost：对冲中另一条腿得出决定性结果、本 try 被取消（error 仍是它自己的错误码）。
-- 迁移前的 try 读作空串与 false，与单供应商配置相同。
ALTER TABLE call_tries
    ADD COLUMN provider text NOT NULL DEFAULT '',
    ADD COLUMN skipped text NOT NULL DEFAULT '',
    ADD COLUMN hedge boolean NOT NULL DEFAULT false,
    ADD COLUMN hedge_lost boolean NOT NULL DEFAULT false;
