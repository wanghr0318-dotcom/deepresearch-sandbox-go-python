"""每轮配置：task_start.config 由服务端生成（共享契约 turn config），Worker 只做校验与缺省。"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from agentbox_worker.errors import WorkerFailure

TEXT_MAX_CHARS = 4000
TOOL_BUDGET_RANGE = (1, 100)
MAX_TOKENS_RANGE = (256, 131072)
ROUNDS_RANGE = (1, 200)


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
    return TurnConfig(**values)
