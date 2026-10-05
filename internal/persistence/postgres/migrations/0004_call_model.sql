-- 0004：calls.model 记录调用解析后的模型（chat；搜索与抓取为空），供 inspect 与工作台的调用表展示（Plan 10）。
-- 只是审计元数据：指纹已包含模型，结算按 Coordinator 内存中的模型取价格；迁移前的调用读作空串。

ALTER TABLE calls ADD COLUMN model text NOT NULL DEFAULT '';
