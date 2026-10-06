"""ask_user 工具：在研究开始前向用户提一轮选择题（设计 D8）。

工具只产出提问内容与 control="await_user"；Agent 循环据此写 checkpoint 并以
awaiting_input{question_id} 暂停 turn，用户的回答作为该调用的 tool 消息恢复同一位置。不计额度。
"""

from __future__ import annotations

from typing import Any

from agentbox_worker.tools.base import ToolContext, ToolResult
from agentbox_worker.tools.text import truncate_utf8

INLINE_RAW_MAX_BYTES = 16 * 1024


class AskUser:
    name = "ask_user"
    description = (
        "研究开始前、范围不明确时向用户提一轮选择题（至多 3 题，每题 2–4 个互斥选项；界面自动追加"
        "“其他…”）。只用于时间、地区、目的、深度；须先 read_skill，写计划或开始搜索后不可用；"
        "每轮只能提问一次。调用后本轮暂停，等待用户回答。不消耗工具额度。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "questions": {
                "type": "array",
                "minItems": 1,
                "maxItems": 3,
                "items": {
                    "type": "object",
                    "properties": {
                        "question": {"type": "string", "minLength": 1, "maxLength": 200},
                        "options": {
                            "type": "array",
                            "minItems": 2,
                            "maxItems": 4,
                            "items": {"type": "string", "minLength": 1, "maxLength": 60},
                        },
                    },
                    "required": ["question", "options"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["questions"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        questions = [
            {"question": q["question"], "options": list(q["options"])} for q in args["questions"]
        ]
        if (reason := _refusal(ctx, questions)) is not None:
            return ToolResult(content=f"不能提问：{reason}", ok=False)
        ctx.flags.asked = True
        question_id = f"q-{ctx.step_id}-{ctx.call_index}"
        lines = [
            f"{i}. {q['question']}（{' / '.join(q['options'])}）"
            for i, q in enumerate(questions, 1)
        ]
        text = "\n".join(lines)
        content = f"已向用户提问，等待回答：\n{text}"
        return ToolResult(
            content=content,
            preview={"kind": "text", "text": text},
            raw={
                "inline": {
                    "request": dict(args),
                    "response": truncate_utf8(content, INLINE_RAW_MAX_BYTES),
                }
            },
            control="await_user",
            data={"question_id": question_id, "questions": questions},
        )


def _refusal(ctx: ToolContext, questions: list[dict[str, Any]]) -> str | None:
    flags = ctx.flags
    if not flags.skill_read:
        return '先调用 read_skill("deep-research") 了解提问规则'
    if flags.asked:
        return "本轮已提问过一次，不再提问；按用户的回答或默认假设继续"
    if flags.planned:
        return "计划（todo_write）已写出，之后不再提问；在报告“背景与范围”中写明默认假设"
    if flags.researching:
        return "研究已开始，之后不再提问；在报告“背景与范围”中写明默认假设"
    for i, q in enumerate(questions, 1):
        if len(set(q["options"])) != len(q["options"]):
            return f"第 {i} 题的选项有重复；选项须互斥"
    return None
