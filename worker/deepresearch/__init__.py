"""DeepResearch Worker：计划 → 检索与抓取 → 总结 → 报告。

研究算法与提示词移植自 helloagents-deepresearch（planner / reporter / prompts）。本包零运行时
依赖：只用标准库与 agentbox_worker SDK；全部外部访问经 Gateway（Unix socket），不导入数据库、
HTTP 客户端、socket，不创建子进程（import-linter 与 ruff TID251 约束）。
"""

from deepresearch.models import Evidence, ResearchState, ResearchTask
from deepresearch.state import SCHEMA_VERSION, migrate
from deepresearch.steps import (
    EVIDENCE_MAX_BYTES,
    fallback_task,
    is_fallback,
    parse_tasks,
    plan,
    search_and_fetch,
    summarize,
    write_report,
)

__all__ = [
    "EVIDENCE_MAX_BYTES",
    "SCHEMA_VERSION",
    "Evidence",
    "ResearchState",
    "ResearchTask",
    "fallback_task",
    "is_fallback",
    "migrate",
    "parse_tasks",
    "plan",
    "search_and_fetch",
    "summarize",
    "write_report",
]
