"""每轮配置：task_start.config 由服务端生成（共享契约 turn config），Worker 只做校验与缺省。"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from agentbox_worker.errors import WorkerFailure
from agentbox_worker.tools.todo import ITEM_IDS, MAX_SUBTOPICS, MIN_SUBTOPICS, TodoItem

TEXT_MAX_CHARS = 4000
TOOL_BUDGET_RANGE = (1, 100)
MAX_TOKENS_RANGE = (256, 131072)
ROUNDS_RANGE = (1, 200)
SCHEDULING = ("parallel", "serial")
DEFAULT_SUBRUN_DEADLINE_MS = 600_000
SUBRUN_DEADLINE_RANGE = (1, 3_600_000)
# 输出单价（微美元 / 百万 token）的保守缺省：Worker 看不到服务端的价格表，服务端可经
# config.research 给出实际价格；只用于报告预留与"每个 sub-run 至少一次模型调用"的估算。
DEFAULT_ORCH_OUTPUT_PRICE = 16_000_000
DEFAULT_WORKER_OUTPUT_PRICE = 8_000_000
PRICE_RANGE = (0, 10**12)
RESERVE_RANGE = (0, 10**15)


@dataclass(frozen=True)
class TurnConfig:
    text: str  # 1–4000 字，去首尾空白
    deep_research: bool = False
    orchestrator_model: str | None = None
    worker_model: str | None = None
    tool_budget: int = 30  # 1–100
    orchestrator_max_tokens: int = 16384
    subtopic_max_tokens: int = 8192
    report_max_tokens: int = 16384
    max_orchestrator_rounds: int = 40  # 编排模型调用次数上限（防循环）
    max_subtopic_rounds: int = 16
    # 研究配置（config.research，Plan 14）：用户路径从不设置，只有串并行对比（运维路径）设置
    scheduling: str = "parallel"  # parallel | serial（serial 也走 sub-run，只差调度）
    fixed_plan: tuple[dict[str, Any], ...] | None = None  # 给定时跳过规划，按此清单执行
    subrun_deadline_ms: int = DEFAULT_SUBRUN_DEADLINE_MS
    report_reserve_micro: int | None = None  # 缺省按 report_max_tokens × 编排输出单价 × 1.5
    orchestrator_output_micro_per_mtok: int = DEFAULT_ORCH_OUTPUT_PRICE
    worker_output_micro_per_mtok: int = DEFAULT_WORKER_OUTPUT_PRICE


_INT_RANGES: dict[str, tuple[int, int]] = {
    "tool_budget": TOOL_BUDGET_RANGE,
    "orchestrator_max_tokens": MAX_TOKENS_RANGE,
    "subtopic_max_tokens": MAX_TOKENS_RANGE,
    "report_max_tokens": MAX_TOKENS_RANGE,
    "max_orchestrator_rounds": ROUNDS_RANGE,
    "max_subtopic_rounds": ROUNDS_RANGE,
}


def _invalid(message: str) -> WorkerFailure:
    return WorkerFailure("invalid_config", message)


def parse_turn_config(raw: Any) -> TurnConfig:
    """校验 task_start.config；不合法 → WorkerFailure("invalid_config")。未知字段忽略。

    模型名为 null 或缺省时请求不带 model（Gateway 用服务端默认模型）。
    """
    if not isinstance(raw, dict):
        raise _invalid("config 须为 JSON 对象")
    text = raw.get("text")
    if not isinstance(text, str) or not (text := text.strip()):
        raise _invalid("config.text 须为非空字符串")
    if len(text) > TEXT_MAX_CHARS:
        raise _invalid(f"config.text 超过 {TEXT_MAX_CHARS} 字")
    values: dict[str, Any] = {"text": text}
    deep = raw.get("deep_research", False)
    if not isinstance(deep, bool):
        raise _invalid("config.deep_research 须为布尔值")
    values["deep_research"] = deep
    for name in ("orchestrator_model", "worker_model"):
        model = raw.get(name)
        if model is not None and (not isinstance(model, str) or not model.strip()):
            raise _invalid(f"config.{name} 须为非空字符串或 null")
        values[name] = model.strip() if isinstance(model, str) else None
    known = {f.name for f in fields(TurnConfig)}
    for name, (lo, hi) in _INT_RANGES.items():
        if name not in raw or raw[name] is None:
            continue
        v = raw[name]
        if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
            raise _invalid(f"config.{name} 须为 {lo}–{hi} 的整数")
        assert name in known
        values[name] = v
    values.update(_research(raw.get("research")))
    return TurnConfig(**values)


def _research(raw: Any) -> dict[str, Any]:
    """config.research：scheduling、fixed_plan、subrun_deadline_ms、report_reserve_micro、
    orchestrator/worker_output_micro_per_mtok。缺省或 null → 全部取缺省值。"""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise _invalid("config.research 须为 JSON 对象")
    out: dict[str, Any] = {}
    scheduling = raw.get("scheduling")
    if scheduling is not None:
        if scheduling not in SCHEDULING:
            raise _invalid("config.research.scheduling 须为 parallel 或 serial")
        out["scheduling"] = scheduling
    if raw.get("fixed_plan") is not None:
        out["fixed_plan"] = _fixed_plan(raw["fixed_plan"])
    ranges = {
        "subrun_deadline_ms": SUBRUN_DEADLINE_RANGE,
        "report_reserve_micro": RESERVE_RANGE,
        "orchestrator_output_micro_per_mtok": PRICE_RANGE,
        "worker_output_micro_per_mtok": PRICE_RANGE,
    }
    for name, (lo, hi) in ranges.items():
        v = raw.get(name)
        if v is None:
            continue
        if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
            raise _invalid(f"config.research.{name} 须为 {lo}–{hi} 的整数")
        out[name] = v
    return out


def _fixed_plan(plan: Any) -> tuple[dict[str, Any], ...]:
    """固定计划：todo_write 的清单项（id、title、budget、brief；status 缺省 pending），
    其中 2–4 个子主题（budget > 0）。"""
    if not isinstance(plan, list) or not 1 <= len(plan) <= len(ITEM_IDS):
        raise _invalid(f"config.research.fixed_plan 须为 1–{len(ITEM_IDS)} 项的数组")
    items = []
    for d in plan:
        try:
            item = TodoItem.from_json({"status": "pending", **d} if isinstance(d, dict) else d)
        except ValueError as exc:
            raise _invalid(f"config.research.fixed_plan 的项不合法：{exc}") from exc
        items.append(item.to_json())
    subtopics = sum(1 for i in items if i["budget"] > 0)
    if not MIN_SUBTOPICS <= subtopics <= MAX_SUBTOPICS:
        span = f"{MIN_SUBTOPICS}–{MAX_SUBTOPICS}"
        raise _invalid(f"config.research.fixed_plan 须有 {span} 个子主题（budget > 0）")
    if len({i["id"] for i in items}) != len(items):
        raise _invalid("config.research.fixed_plan 的 id 重复")
    return tuple(items)
