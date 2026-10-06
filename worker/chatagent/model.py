"""模型调用：OpenAI 兼容的函数调用请求、回复解析、空回复判定与有界重试。

kimi-k3 / kimi-k2.6 是推理模型：回复去掉思考段后既无正文又无工具调用一律视为错误（从不当作
空回答），以加倍的 max_tokens 重试一次；Gateway 超时、5xx 与 429 也重试一次。重试使用新的
call id（CallIds 递增），恢复后按同样顺序确定性重放。仍失败 → ModelUnavailable（面向用户的
固定提示）；费用预算、访问撤销与调用指纹冲突原样上抛。
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Any

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    GatewayError,
    WorkerFailure,
)
from agentbox_worker.gateway import GatewayResult
from agentbox_worker.tools import GatewayLike
from agentbox_worker.tools.text import strip_thinking

USER_MESSAGE = "模型服务暂时不可用，请重试"


class ModelUnavailable(WorkerFailure):
    """模型或供应商失败（重试一次之后）：turn 失败，会话仍可继续。"""

    def __init__(self, detail: str = "") -> None:
        super().__init__("model_unavailable", USER_MESSAGE, retryable=False)
        self.detail = detail


class _Empty(Exception):
    """回复去掉思考段后既无正文又无工具调用。"""


@dataclass(frozen=True)
class ModelReply:
    content: str  # 去掉思考段并 strip
    tool_calls: list[dict[str, Any]]  # [{"id","type":"function","function":{"name","arguments"}}]
    reasoning: str  # reasoning_content（仅用于 thinking 事件，不回传）
    call_id: str
    blob: str | None
    request: dict[str, Any] = field(default_factory=dict)  # 实际发出的请求体（⟨/⟩ 用，不含 model）


def _log(message: str) -> None:
    print(f"chatagent: {message}", file=sys.stderr)


def _transient(exc: GatewayError) -> bool:
    if isinstance(exc, CallDeadlineExceeded):
        return True
    return exc.status >= 500 or exc.status == 429 or exc.code == "client_timeout"


def call_model(
    gw: GatewayLike,
    step_id: str,
    messages: list[dict[str, Any]],
    *,
    model: str | None,
    max_tokens: int,
    tools: list[dict[str, Any]] | None,
    tool_choice: str | None = None,
) -> ModelReply:
    """发起一次编排或子主题的模型调用（同步，调用方经 asyncio.to_thread 使用）。

    tools 为 None 时请求不带工具，回复中的 tool_calls 被忽略（按正文判定是否为空）。
    """
    tokens = max_tokens
    last = ""
    for attempt in (1, 2):
        try:
            res = gw.chat(
                step_id,
                messages,
                model=model,
                max_tokens=tokens,
                tools=tools,
                tool_choice=tool_choice,
            )
            return _parse(res, messages, tokens, tools, tool_choice)
        except (BudgetExhausted, AccessRevoked, CallDivergence):
            raise
        except _Empty as exc:
            last = str(exc)
            tokens = max_tokens * 2
        except GatewayError as exc:
            last = f"{exc.status} {exc.code}"
            if not _transient(exc):
                if exc.status == 0:  # 与 Gateway 的连接丢失：不是模型的问题，原样上抛
                    raise
                break
        _log(f"{step_id} 第 {attempt} 次模型调用失败：{last}")
    _log(f"{step_id} 模型调用放弃：{last}")
    raise ModelUnavailable(last)


def _parse(
    res: GatewayResult,
    messages: list[dict[str, Any]],
    max_tokens: int,
    tools: list[dict[str, Any]] | None,
    tool_choice: str | None,
) -> ModelReply:
    try:
        choice = res.body["choices"][0]
        msg = choice["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise GatewayError(
            502, "invalid_chat_response", f"缺少 choices[0].message：{exc!r}"
        ) from exc
    if not isinstance(msg, dict):
        raise GatewayError(502, "invalid_chat_response", "choices[0].message 不是对象")
    raw = msg.get("content")
    content = strip_thinking(raw if isinstance(raw, str) else "").strip()
    reasoning = msg.get("reasoning_content")
    reasoning = reasoning.strip() if isinstance(reasoning, str) else ""
    calls = _tool_calls(msg.get("tool_calls"), res.call_id) if tools is not None else []
    if not content and not calls:
        finish = choice.get("finish_reason") if isinstance(choice, dict) else None
        raise _Empty(f"{res.call_id} 模型未返回正文（finish_reason={finish}）")
    request: dict[str, Any] = {"messages": messages, "max_tokens": max_tokens}
    if tools is not None:
        request["tools"] = tools
    if tool_choice is not None:
        request["tool_choice"] = tool_choice
    return ModelReply(content, calls, reasoning, res.call_id, res.blob_sha256, request)


def _tool_calls(raw: Any, call_id: str) -> list[dict[str, Any]]:
    """规范化工具调用：只保留 id/type/function{name, arguments(str)}；缺 id 时按位置补一个。"""
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for i, tc in enumerate(raw):
        if not isinstance(tc, dict) or not isinstance(fn := tc.get("function"), dict):
            continue
        name = fn.get("name")
        if not isinstance(name, str) or not name:
            continue
        args = fn.get("arguments")
        if not isinstance(args, str):
            args = "" if args is None else _dumps(args)
        tc_id = tc.get("id") if isinstance(tc.get("id"), str) and tc.get("id") else None
        out.append(
            {
                "id": tc_id or f"{call_id}#{i}",
                "type": "function",
                "function": {"name": name, "arguments": args},
            }
        )
    return out


def _dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def assistant_message(reply: ModelReply) -> dict[str, Any]:
    """转录中的 assistant 消息：content 总是字符串；不含 reasoning_content。"""
    msg: dict[str, Any] = {"role": "assistant", "content": reply.content or ""}
    if reply.tool_calls:
        msg["tool_calls"] = reply.tool_calls
    return msg
