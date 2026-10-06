"""本轮工具额度的 Worker 侧视图（web_search + web_fetch；设计 D3）。

额度由 Gateway 强制：每个计数调用的响应带 X-Agentbox-Tool-Budget: <used>/<limit>，有头时以
Gateway 的值为准（used 单调不减）；无头时本地计 1。子主题的分配额（shares）只是 Agent 的软约束。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

DEFAULT_TOOL_BUDGET = 30


@dataclass
class TurnBudget:
    limit: int = DEFAULT_TOOL_BUDGET
    used: int = 0
    shares: dict[str, int] = field(default_factory=dict)  # 子主题 → 分配额
    spent: dict[str, int] = field(default_factory=dict)  # 子主题 → 已用

    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def share_left(self, subtopic_id: str) -> int | None:
        """子主题分配额的剩余（无分配时 None）；不超过全局剩余。"""
        share = self.shares.get(subtopic_id)
        if share is None:
            return None
        return max(0, min(share - self.spent.get(subtopic_id, 0), self.remaining()))

    def record(self, result_budget: tuple[int, int] | None, subtopic_id: str | None) -> None:
        """记一次成功的计数调用。"""
        if result_budget is None:
            self.used += 1
        else:
            used, limit = result_budget
            self.used = max(self.used, used)
            self.limit = limit
        self._spend(subtopic_id)

    def record_failure(self, subtopic_id: str | None) -> None:
        """失败的调用也计 1 次（设计 §8：记录并继续）。"""
        self.used += 1
        self._spend(subtopic_id)

    def exhaust(self) -> None:
        """Gateway 返回 429 tool_budget_exhausted：本轮额度已用尽。"""
        self.used = max(self.used, self.limit)

    def line(self, subtopic_id: str | None = None) -> str:
        text = f"工具额度：已用 {self.used}/{self.limit}，剩余 {self.remaining()}"
        left = self.share_left(subtopic_id) if subtopic_id is not None else None
        return text if left is None else f"{text}（本子主题剩余 {left}）"

    def to_json(self) -> dict[str, Any]:
        return {
            "limit": self.limit,
            "used": self.used,
            "shares": dict(self.shares),
            "spent": dict(self.spent),
        }

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TurnBudget:
        if not isinstance(d, dict):
            raise ValueError("工具额度状态不是对象")
        limit, used = d.get("limit", DEFAULT_TOOL_BUDGET), d.get("used", 0)
        if not (_count(limit) and _count(used)):
            raise ValueError(f"工具额度状态不合法：limit={limit!r} used={used!r}")
        return cls(
            limit=limit,
            used=used,
            shares=_counts(d.get("shares", {}), "shares"),
            spent=_counts(d.get("spent", {}), "spent"),
        )

    def _spend(self, subtopic_id: str | None) -> None:
        if subtopic_id is not None:
            self.spent[subtopic_id] = self.spent.get(subtopic_id, 0) + 1


def _count(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


def _counts(d: Any, name: str) -> dict[str, int]:
    if not isinstance(d, dict) or not all(isinstance(k, str) and _count(v) for k, v in d.items()):
        raise ValueError(f"工具额度状态的 {name} 不合法")
    return dict(d)
