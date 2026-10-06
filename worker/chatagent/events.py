"""进度事件（共享契约裁定 L，字段以 api/openapi.yaml 的 <Type>Data 为准）。

progress.kind 即会话事件 type，progress.data 即 data（宿主按字段允许列表过滤、去掉内部键）。
message 为简短中文，供运维时间线阅读。只有经 Gateway 的调用（搜索、抓取、run_python 的 exec、
模型）带 raw{request, response_ref}；本地工具（read_skill、todo_write、ask_user、read_source）
不带 raw。
单条事件 > 64 KiB 时依次截断 raw.request、preview 与 input。
"""

from __future__ import annotations

import json
from typing import Any

from agentbox_worker.gateway import GatewayClient
from agentbox_worker.runtime import ArtifactRef, TaskContext
from agentbox_worker.tools import ToolResult
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.text import truncate_utf8
from agentbox_worker.tools.todo import TodoItem
from chatagent.state import SubtopicState, json_size

EVENT_MAX_BYTES = 64 * 1024
REQUEST_MAX_BYTES = 32 * 1024
THINKING_MAX_CHARS = 600
PREVIEW_MAX_CHARS = 600
DELTA_MAX_CHARS = 400
_GATEWAY_TOOLS = frozenset({"web_search", "web_fetch", "run_python"})
_TOOL_LABELS = {
    "web_search": "搜索",
    "web_fetch": "抓取",
    "read_source": "重读来源",
    "read_skill": "读取 skill",
    "ask_user": "提问",
    "todo_write": "更新计划",
    "research_subtopic": "研究子主题",
    "run_python": "运行代码",
}


def raw_ref(call_id: str, request: Any, response_ref: str | None) -> dict[str, Any]:
    """经 Gateway 的调用的 ⟨/⟩：请求内联（≤ 32 KiB，超出时为截断的 JSON 文本），响应为 blob sha。

    call_id 只给运维（宿主对用户去掉该键）。"""
    raw: dict[str, Any] = {"call_id": call_id}
    text = json.dumps(request, ensure_ascii=False)
    if len(text.encode("utf-8")) > REQUEST_MAX_BYTES:
        raw["request"] = truncate_utf8(text, REQUEST_MAX_BYTES)
        raw["request_truncated"] = True
    else:
        raw["request"] = request
    if response_ref:
        raw["response_ref"] = response_ref
    return raw


def _fit(data: dict[str, Any]) -> dict[str, Any]:
    """单条事件 data ≤ 64 KiB（留出信封余量）：依次截断 raw.request、preview 与 input。"""
    limit = EVENT_MAX_BYTES - 2048
    if json_size(data) <= limit:
        return data
    raw = data.get("raw")
    if isinstance(raw, dict) and "request" in raw:
        text = raw["request"] if isinstance(raw["request"], str) else json.dumps(raw["request"])
        data["raw"] = {**raw, "request": truncate_utf8(text, 8 * 1024), "request_truncated": True}
    if json_size(data) > limit and isinstance(data.get("preview"), dict):
        text = json.dumps(data["preview"], ensure_ascii=False)
        data["preview"] = {"kind": "text", "text": truncate_utf8(text, 16 * 1024)}
    if json_size(data) > limit and "input" in data:
        text = json.dumps(data["input"], ensure_ascii=False)
        data["input"] = {"truncated": truncate_utf8(text, 16 * 1024)}
    for key in ("text", "summary"):
        if json_size(data) > limit and isinstance(data.get(key), str):
            data[key] = truncate_utf8(data[key], 32 * 1024)
    return data


class Emitter:
    def __init__(self, ctx: TaskContext, sink: Any = None) -> None:
        self.ctx = ctx
        self.message_id = f"{ctx.task_id}-reply"
        # progress 的发送者：缺省为 TaskContext；sub-run 中为 SubrunHandle（事件带 subrun_id）
        self._sink = sink if sink is not None else ctx

    def for_subrun(self, handle: Any) -> Emitter:
        """同一 turn 的发射器，事件经 handle.progress 发出（带 subrun_id）。"""
        return Emitter(self.ctx, sink=handle)

    async def _send(self, kind: str, message: str, data: dict[str, Any], step: str | None) -> None:
        await self._sink.progress(kind, message, step_id=step, data=_fit(data))

    async def route(self, route: str, forced: bool = False) -> None:
        label = "深度研究" if route == "research" else "直接回答"
        await self._send("route", f"路线：{label}", {"route": route, "forced": forced}, None)

    async def skill_read(self, step_id: str, name: str, description: str, file: str | None) -> None:
        data: dict[str, Any] = {"step_id": step_id, "name": name, "description": description}
        if file is not None:
            data["file"] = file
        await self._send(
            "skill_read", f"读取 skill {name}" + (f"/{file}" if file else ""), data, step_id
        )

    async def thinking(
        self,
        step_id: str,
        text: str,
        call_id: str,
        request: Any = None,
        response_ref: str | None = None,
    ) -> None:
        data = {
            "step_id": step_id,
            "text": text[:THINKING_MAX_CHARS],
            "raw": raw_ref(call_id, request if request is not None else {}, response_ref),
        }
        await self._send("thinking", "思考", data, step_id)

    async def ask_user(
        self, step_id: str, question_id: str, questions: list[dict[str, Any]]
    ) -> None:
        qs = [
            {
                "id": str(i),
                "question": q["question"],
                "options": list(q["options"]),
                "allow_other": True,
            }
            for i, q in enumerate(questions, 1)
        ]
        data = {"step_id": step_id, "question_id": question_id, "questions": qs}
        await self._send("ask_user", f"向用户提问（{len(qs)} 题）", data, step_id)

    async def todo(self, step_id: str, items: list[TodoItem]) -> None:
        data = {
            "items": [
                {"id": i.id, "title": i.title, "status": i.status, "budget_share": i.budget}
                for i in items
            ]
        }
        await self._send("todo_updated", f"计划更新（{len(items)} 项）", data, step_id)

    async def tool_call(
        self,
        step_id: str,
        tool_call_id: str,
        tool: str,
        args: Any,
        subtopic_id: str | None,
    ) -> None:
        data: dict[str, Any] = {
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "tool": tool,
            "input": args if isinstance(args, dict) else {"arguments": str(args)[:2000]},
        }
        if subtopic_id is not None:
            data["subtopic_id"] = subtopic_id
        label = _TOOL_LABELS.get(tool, tool)
        await self._send("tool_call", f"调用 {tool}（{label}）", data, step_id)

    async def tool_result(
        self,
        step_id: str,
        tool_call_id: str,
        tool: str,
        result: ToolResult,
        subtopic_id: str | None,
        args: Any = None,
    ) -> None:
        preview = result.preview or {"kind": "text", "text": result.content[:PREVIEW_MAX_CHARS]}
        data: dict[str, Any] = {
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "tool": tool,
            "ok": result.ok,
        }
        if subtopic_id is not None:
            data["subtopic_id"] = subtopic_id
        data["preview"] = preview
        if not result.ok:
            text = preview.get("text") if isinstance(preview.get("text"), str) else ""
            data["error"] = (text or result.content.split("\n", 1)[0])[:PREVIEW_MAX_CHARS]
        raw = result.raw or {}
        if tool in _GATEWAY_TOOLS and isinstance(raw.get("call_id"), str):
            data["raw"] = raw_ref(
                raw["call_id"],
                _gateway_request(tool, args),
                result.blobs[0] if result.blobs else None,
            )
        status = "完成" if result.ok else "失败"
        await self._send("tool_result", f"{tool} {status}", data, step_id)

    async def budget(self, b: TurnBudget) -> None:
        data = {"used": b.used, "limit": b.limit}
        await self._send("budget", f"工具额度 {b.used}/{b.limit}", data, None)

    async def subtopic(self, sub: SubtopicState, title: str, *, with_summary: bool = True) -> None:
        data: dict[str, Any] = {"id": sub.id, "title": title, "status": sub.status}
        if with_summary and sub.status == "done" and sub.summary:
            data["summary"] = sub.summary
        await self._send("subtopic", f"子主题 {sub.id}：{sub.status}", data, f"sub-{sub.id}")

    async def assistant_text(self, text: str) -> None:
        """按 ≤ 400 字切块发 assistant_delta，最后一块 final=True。"""
        chunks = [text[i : i + DELTA_MAX_CHARS] for i in range(0, len(text), DELTA_MAX_CHARS)]
        chunks = chunks or [""]
        for k, chunk in enumerate(chunks):
            data = {"message_id": self.message_id, "text": chunk, "final": k == len(chunks) - 1}
            await self._send("assistant_delta", "回复", data, None)

    async def report_ready(
        self,
        ref: ArtifactRef,
        title: str,
        partial: bool,
        note: str | None,
        tool_budget_reached: bool = False,
    ) -> None:
        data: dict[str, Any] = {
            "artifact_id": ref.artifact_id,
            "version": ref.version,
            "title": title,
            "partial": partial,
            "tool_budget_reached": tool_budget_reached,
        }
        if note:
            data["note"] = note
        await self._send("report_ready", f"报告已生成：{title}", data, None)

    async def turn_stopped(self, card: dict[str, Any], findings: str, can_finish: bool) -> None:
        data = {"card": card, "findings": findings, "can_finish": can_finish}
        await self._send("turn_stopped", "研究已停止", data, None)


def _gateway_request(tool: str, args: Any) -> dict[str, Any]:
    """搜索、抓取与 exec 发给 Gateway 的请求体（与工具实现一致）。"""
    args = args if isinstance(args, dict) else {}
    if tool == "run_python":
        return _exec_request(args)
    if tool == "web_search":
        return {
            "query": str(args.get("query", "")).strip(),
            "max_results": args.get("max_results", 5),
        }
    return {"url": str(args.get("url", "")).strip()}


def _exec_request(args: dict[str, Any]) -> dict[str, Any]:
    """run_python 的 /v1/exec 请求体（RunPython.run 的参数映射 + GatewayClient.exec_body）。"""
    items = args.get("inputs") if isinstance(args.get("inputs"), list) else []
    inputs = [(i.get("sha256"), i.get("path")) for i in items if isinstance(i, dict)]
    timeout_s = args.get("timeout_s")
    wall_ms = timeout_s * 1000 if isinstance(timeout_s, int) and timeout_s > 0 else None
    try:
        return GatewayClient.exec_body(str(args.get("code", "")), inputs=inputs, wall_ms=wall_ms)
    except (TypeError, ValueError):  # 只在调用已发生时生成：参数已通过工具的 schema 校验
        return {"code": str(args.get("code", ""))}
