"""todo_write 工具：研究计划清单与子主题额度分配（设计 D3）。

每次调用给出完整清单（覆盖上一份）。budget > 0 的项是子主题（2–4 个），其 budget 是本子主题
web_search + web_fetch 的份额；各项尚未用掉的份额之和不得超过本轮剩余额度。不计额度。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from agentbox_worker.tools.base import ToolContext, ToolResult
from agentbox_worker.tools.text import truncate_utf8

STATUSES = ("pending", "in_progress", "done", "skipped")
ITEM_IDS = tuple(str(i) for i in range(1, 7))
MIN_SUBTOPICS, MAX_SUBTOPICS = 2, 4
TITLE_MAX_CHARS = 80
BRIEF_MAX_CHARS = 1200
INLINE_RAW_MAX_BYTES = 16 * 1024
_MARKS = {"pending": "[ ]", "in_progress": "[…]", "done": "[x]", "skipped": "[-]"}


@dataclass
class TodoItem:
    id: str  # "1"…"6"
    title: str  # 1–80
    status: str  # pending | in_progress | done | skipped
    budget: int  # ≥ 0，本子主题 web_search + web_fetch 份额
    brief: str = ""  # 子主题简报（≤ 1200 字）

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: Any) -> TodoItem:
        if not isinstance(d, dict):
            raise ValueError("清单项不是对象")
        item = cls(
            id=d.get("id"),
            title=d.get("title"),
            status=d.get("status"),
            budget=d.get("budget"),
            brief=d.get("brief", ""),
        )
        if (
            item.id not in ITEM_IDS
            or not isinstance(item.title, str)
            or not 1 <= len(item.title) <= TITLE_MAX_CHARS
            or item.status not in STATUSES
            or not isinstance(item.budget, int)
            or isinstance(item.budget, bool)
            or item.budget < 0
            or not isinstance(item.brief, str)
            or len(item.brief) > BRIEF_MAX_CHARS
        ):
            raise ValueError(f"清单项不合法：{d!r}")
        return item


class TodoWrite:
    name = "todo_write"
    description = (
        "写出或更新研究计划清单（完整清单，覆盖上一份）。budget > 0 的项是子主题（2–4 个），"
        "budget 为"
        "其 web_search + web_fetch 份额，brief 为子主题简报；各项未用份额之和不得超过剩余额度。"
        "可随时更新状态或改写未开始的子主题。不消耗工具额度。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "minItems": 1,
                "maxItems": 6,
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "enum": list(ITEM_IDS)},
                        "title": {"type": "string", "minLength": 1, "maxLength": TITLE_MAX_CHARS},
                        "status": {"type": "string", "enum": list(STATUSES)},
                        "budget": {"type": "integer", "minimum": 0},
                        "brief": {"type": "string", "maxLength": BRIEF_MAX_CHARS},
                    },
                    "required": ["id", "title", "status", "budget"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["items"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        items = [TodoItem.from_json(d) for d in args["items"]]
        if (reason := _refusal(items, ctx)) is not None:
            return ToolResult(content=f"计划不合法：{reason}", ok=False)
        ctx.flags.planned = True
        ctx.budget.shares = {i.id: i.budget for i in items}
        text = checklist(items)
        content = f"计划已更新：\n{text}\n{ctx.budget.line()}"
        return ToolResult(
            content=content,
            preview={"kind": "text", "text": text},
            raw={
                "inline": {
                    "request": dict(args),
                    "response": truncate_utf8(content, INLINE_RAW_MAX_BYTES),
                }
            },
            data={"items": [i.to_json() for i in items]},
        )


def checklist(items: list[TodoItem]) -> str:
    """清单的文本形式（预览与回给模型的内容）。"""
    return "\n".join(
        f"{_MARKS[i.status]} {i.id}. {i.title}" + (f"（份额 {i.budget}）" if i.budget else "")
        for i in items
    )


def _refusal(items: list[TodoItem], ctx: ToolContext) -> str | None:
    ids = [i.id for i in items]
    if len(set(ids)) != len(ids):
        return "id 重复"
    subtopics = [i for i in items if i.budget > 0]
    if not MIN_SUBTOPICS <= len(subtopics) <= MAX_SUBTOPICS:
        return (
            f"须有 {MIN_SUBTOPICS}–{MAX_SUBTOPICS} 个子主题（budget > 0 的项），"
            f"当前 {len(subtopics)} 个"
        )
    spent = ctx.budget.spent
    unspent = sum(max(0, i.budget - spent.get(i.id, 0)) for i in subtopics)
    if unspent > (left := ctx.budget.remaining()):
        return f"各子主题未用份额合计 {unspent}，超过本轮剩余额度 {left}；请减少份额"
    return None
