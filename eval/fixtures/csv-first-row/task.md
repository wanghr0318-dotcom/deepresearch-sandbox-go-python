# CSV 首条记录丢失

`read_records("name,city\nAda,London\n")` 应返回一条包含 Ada 和 London 的记录，目前返回空列表。修复 `records.py`，保留 CSV 的字段解析与记录顺序。只有表头时应返回空列表。

任务工作区从 `repo/` 创建。验收测试由评测端保管。
