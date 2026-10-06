"""进程内的测试替身（不依赖 AF_UNIX，Windows 上可运行）。

ScriptedModel：ScriptedGateway 的 chat 脚本，按 step_id 与轮次返回预设的回复或工具调用。

ScriptedGateway：实现 tools.base.GatewayLike，按脚本回答 chat/search/fetch，并像真实 Gateway 一样
分配确定性的 call id、为每个结果生成 blob、对同一 call id 的重发返回同一结果（replayed=True）。

SessionHost：session 模式的脚本化宿主，经 MemoryTransport 驱动 run_session_worker，按 Worker
事件作答（checkpoint/artifact 结果、task_outcome），空闲时依次执行排好的动作，并记录双向转录。
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from agentbox_worker.gateway import CallIds, GatewayClient, GatewayResult
from agentbox_worker.runtime import Timing
from agentbox_worker.session import run_session_worker
from agentbox_worker.transport import MemoryTransport

BudgetScript = tuple[int, int] | Callable[[int], tuple[int, int] | None] | None


@dataclass(frozen=True)
class GatewayCall:
    kind: str  # chat | search | fetch
    step_id: str
    call_id: str
    body: dict[str, Any]


class ScriptedGateway:
    """脚本化 Gateway。

    - chat(step_id, body) / search(query, max_results) / fetch(url) 返回响应体（dict）或
      GatewayResult，也可抛出 GatewayError 子类模拟失败；未给出脚本的端点调用时 AssertionError。
    - tool_budget：search/fetch 成功结果的 (used, limit)；可为常量、按第 k 次计数调用（从 1 起，
      含失败的调用）返回值的函数，或 None（模拟没有 X-Agentbox-Tool-Budget 头）。
    - calls 按发起顺序记录每次调用（含失败的）；blobs 为全部结果 blob；read_blob_error 不为 None 时
      read_blob 抛出它。
    """

    def __init__(
        self,
        *,
        chat: Callable[[str, dict[str, Any]], Any] | None = None,
        search: Callable[[str, int], Any] | None = None,
        fetch: Callable[[str], Any] | None = None,
        tool_budget: BudgetScript = None,
        call_ids: CallIds | None = None,
    ) -> None:
        self.chat_script = chat
        self.search_script = search
        self.fetch_script = fetch
        self.tool_budget = tool_budget
        self.call_ids = call_ids if call_ids is not None else CallIds()
        self.calls: list[GatewayCall] = []
        self.blobs: dict[str, bytes] = {}
        self.blob_reads: list[str] = []
        self.read_blob_error: Exception | None = None
        self._results: dict[str, GatewayResult] = {}
        self._counted = 0

    # ---- GatewayLike ----

    def chat(
        self,
        step_id: str,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | None = None,
    ) -> GatewayResult:
        body = GatewayClient.chat_body(
            messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
        )
        script = self.chat_script
        return self._call("chat", step_id, body, lambda: _need(script, "chat")(step_id, body))

    def search(
        self, step_id: str, query: str, *, max_results: int = 5, no_cache: bool = False
    ) -> GatewayResult:
        body = {"query": query, "max_results": max_results}
        script = self.search_script
        return self._call(
            "search", step_id, body, lambda: _need(script, "search")(query, max_results)
        )

    def fetch(self, step_id: str, url: str, *, no_cache: bool = False) -> GatewayResult:
        script = self.fetch_script
        return self._call("fetch", step_id, {"url": url}, lambda: _need(script, "fetch")(url))

    def read_blob(self, sha256: str) -> bytes:
        self.blob_reads.append(sha256)
        if self.read_blob_error is not None:
            raise self.read_blob_error
        return self.blobs[sha256]

    # ---- 测试辅助 ----

    def counts(self) -> dict[str, int]:
        return dict(Counter(c.kind for c in self.calls))

    def put_blob(self, data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        self.blobs[sha] = data
        return sha

    # ---- 内部 ----

    def _call(
        self, kind: str, step_id: str, body: dict[str, Any], run: Callable[[], Any]
    ) -> GatewayResult:
        call_id = self.call_ids.next(step_id, kind)
        self.calls.append(GatewayCall(kind, step_id, call_id, body))
        old = self._results.get(call_id)
        if old is not None:  # 同一 call id 重发：重放已记录的结果（真实 Gateway 的重放也带额度头）
            return GatewayResult(
                old.call_id, old.body, old.blob_sha256, True, old.status, old.tool_budget
            )
        budget = None
        if kind in ("search", "fetch"):
            self._counted += 1
            budget = self._budget(self._counted)
        out = run()
        if isinstance(out, GatewayResult):
            res = out
        else:
            sha = self.put_blob(json.dumps(out, ensure_ascii=False, sort_keys=True).encode())
            res = GatewayResult(call_id, out, sha, False, 200, budget)
        self._results[call_id] = res
        return res

    def _budget(self, k: int) -> tuple[int, int] | None:
        if callable(self.tool_budget):
            return self.tool_budget(k)
        return self.tool_budget


def _need(script: Callable[..., Any] | None, kind: str) -> Callable[..., Any]:
    if script is None:
        raise AssertionError(f"ScriptedGateway 没有 {kind} 脚本")
    return script


# ---- session 模式的脚本化宿主 ----

FAST_TIMING = Timing(ack_timeout=0.05, max_ack_attempts=3, retry_backoff=0.01, artifact_timeout=0.5)
_DEFAULT_VERDICT = {
    "result": "succeeded",
    "error": "failed",
    "paused": "paused",
    "awaiting_input": "paused",
}
_PROPOSALS = frozenset(_DEFAULT_VERDICT)

EventHook = Callable[[dict[str, Any], "SessionHost"], None]


class SessionHost:
    """脚本化宿主。

    - start_task / quiesce / close_when_idle 排入动作；Worker 空闲（ready、task_released、
      quiesced 之后）时取出下一个执行；动作用完时关闭 stdin。
    - checkpoint、artifact 自动以 checkpoint_status / saved 作答；终态提议按 verdicts
      （attempt_id → verdict，缺省按提议类型）发出 task_outcome；成功且提议了 session_state
      时 committed_session_checkpoint_id 为其 checkpoint_id。drop_first_outcome 时第一条
      task_outcome 不送达（模拟丢失），之后按 task_outcome_query 重发。
    - on_event(event, host) 在自动作答之前调用，可经 send 注入任意宿主消息。
    - transcript 按时间顺序记录 ("host" | "worker", 消息)。
    """

    def __init__(
        self,
        *,
        session_id: str = "s-1",
        incarnation_id: str = "i-1",
        session_resume: dict[str, Any] | None = None,
        verdicts: dict[str, str] | None = None,
        drop_first_outcome: bool = False,
        checkpoint_status: str = "committed",
        on_event: EventHook | None = None,
        answer_outcome_queries: bool = True,
    ) -> None:
        self.init: dict[str, Any] = {
            "type": "init",
            "bootstrap": 1,
            "protocol_versions": [1],
            "mode": "session",
            "session_id": session_id,
            "incarnation_id": incarnation_id,
        }
        if session_resume is not None:
            self.init["session_resume"] = session_resume
        self.verdicts = dict(verdicts or {})
        self.drop_first_outcome = drop_first_outcome
        self.checkpoint_status = checkpoint_status
        self.on_event = on_event
        self.answer_outcome_queries = answer_outcome_queries
        self.transcript: list[tuple[str, dict[str, Any]]] = []
        self.committed: str = ""  # 宿主侧会话指针
        self._actions: list[dict[str, Any] | None] = []
        self._outcomes: dict[str, dict[str, Any]] = {}
        self._dropped = False
        self._transport: MemoryTransport | None = None

    # ---- 排入动作 ----

    def start_task(self, task_id: str, attempt_id: str, **fields: Any) -> None:
        msg = {
            "type": "task_start",
            "v": 1,
            "task_id": task_id,
            "attempt_id": attempt_id,
            "attempt_no": 1,
            "out_dir": f"/workspace/out/{attempt_id}",
        }
        self._actions.append({**msg, **fields})

    def quiesce(self) -> None:
        self._actions.append({"type": "quiesce", "v": 1, "grace_ms": 0})

    def close_when_idle(self) -> None:
        self._actions.append({"type": "session_close", "v": 1, "grace_ms": 0})

    # ---- 运行 ----

    def run(
        self,
        app: Any,
        *,
        timing: Timing = FAST_TIMING,
        load_staged: Callable[[str], Any] | None = None,
        timeout: float = 5.0,
    ) -> tuple[int, list[dict[str, Any]]]:
        async def go() -> int:
            self._transport = MemoryTransport()
            self._transport.on_send = self._on_send
            self.send(self.init)
            counter = itertools.count(1)
            return await asyncio.wait_for(
                run_session_worker(
                    app,
                    self._transport,
                    name="test-worker",
                    version="0",
                    timing=timing,
                    new_id=lambda: f"id-{next(counter)}",
                    load_staged=load_staged,
                ),
                timeout=timeout,
            )

        code = asyncio.run(go())
        return code, self.events

    @property
    def events(self) -> list[dict[str, Any]]:
        return [msg for side, msg in self.transcript if side == "worker"]

    def send(self, msg: dict[str, Any] | None) -> None:
        """立即送出一条宿主消息；None 关闭 stdin。"""
        assert self._transport is not None
        if msg is None:
            self._transport.feed(None)
            return
        self.transcript.append(("host", msg))
        self._transport.feed(json.dumps(msg, ensure_ascii=False).encode())

    def outcome_for(self, event: dict[str, Any]) -> dict[str, Any]:
        """按提议计算裁决（已发过的裁决原样重发）。"""
        attempt = event["attempt_id"]
        if attempt in self._outcomes:
            return self._outcomes[attempt]
        verdict = self.verdicts.get(attempt, _DEFAULT_VERDICT.get(event["type"], "cancelled"))
        proposed = (event.get("session_state") or {}).get("checkpoint_id")
        if verdict == "succeeded" and proposed:
            self.committed = proposed
        outcome = {"type": "task_outcome", "v": 1, "attempt_id": attempt, "verdict": verdict}
        if self.committed:
            outcome["committed_session_checkpoint_id"] = self.committed
        self._outcomes[attempt] = outcome
        return outcome

    def _on_send(self, line: bytes) -> None:
        event = json.loads(line)
        self.transcript.append(("worker", event))
        if self.on_event is not None:
            self.on_event(event, self)
        self._respond(event)

    def _respond(self, event: dict[str, Any]) -> None:
        typ = event["type"]
        if typ in ("ready", "task_released", "quiesced"):
            self.send(self._actions.pop(0) if self._actions else None)
        elif typ == "checkpoint":
            self.send(self._checkpoint_result(event))
        elif typ == "artifact":
            self.send(self._artifact_result(event))
        elif typ in _PROPOSALS and event.get("attempt_id"):
            outcome = self.outcome_for(event)
            if self.drop_first_outcome and not self._dropped:
                self._dropped = True
                return
            self.send(outcome)
        elif typ == "task_outcome_query" and self.answer_outcome_queries:
            if event["attempt_id"] in self._outcomes:
                self.send(self._outcomes[event["attempt_id"]])

    def _checkpoint_result(self, event: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "checkpoint_result",
            "v": 1,
            "attempt_id": event["attempt_id"],
            "checkpoint_id": event["checkpoint_id"],
            "scope": "task",
            "status": self.checkpoint_status,
        }

    def _artifact_result(self, event: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "artifact_result",
            "v": 1,
            "attempt_id": event["attempt_id"],
            "artifact_id": event["artifact_id"],
            "status": "saved",
            "version": 1,
            "sha256": event["declared_sha256"],
        }


# ---- 脚本化的模型（chat 端点） ----

ModelStep = dict[str, Any] | BaseException | Callable[[dict[str, Any]], Any]


def reply(
    text: str, *, reasoning: str | None = None, finish_reason: str = "stop"
) -> dict[str, Any]:
    """一次不带工具调用的 assistant 回复（OpenAI 兼容的 message）。"""
    msg: dict[str, Any] = {"role": "assistant", "content": text}
    if reasoning is not None:
        msg["reasoning_content"] = reasoning
    return {"message": msg, "finish_reason": finish_reason}


def call(tool: str, /, **args: Any) -> dict[str, Any]:
    """一次只含单个工具调用的 assistant 回复；工具调用 id 由 ScriptedModel 分配。"""
    return calls((tool, args))


def calls(*items: tuple[str, dict[str, Any]], content: str = "") -> dict[str, Any]:
    """一次含多个工具调用的 assistant 回复：calls(("web_search", {...}), ("web_fetch", {...}))。"""
    tool_calls = [
        {
            "type": "function",
            "function": {"name": n, "arguments": json.dumps(a, ensure_ascii=False)},
        }
        for n, a in items
    ]
    msg = {"role": "assistant", "content": content, "tool_calls": tool_calls}
    return {"message": msg, "finish_reason": "tool_calls"}


class ScriptedModel:
    """按 step_id 与轮次回答 chat：orch 用于 "orch"，sub[id] 用于 "sub-<id>"，
    other[step_id] 用于其余。

    每个脚本项是 reply()/call()/calls() 的结果、要抛出的异常（如 GatewayError(502, …)），或接收
    请求体、返回上述之一的函数。工具调用 id 为全局递增的 "call_<n>"。requests 按顺序记录
    (step_id, 请求体)（只含真正送达脚本的调用，重放不计）。脚本用完时 AssertionError。
    """

    def __init__(
        self,
        *,
        orch: list[ModelStep] | None = None,
        sub: dict[str, list[ModelStep]] | None = None,
        other: dict[str, list[ModelStep]] | None = None,
    ) -> None:
        self.scripts: dict[str, list[ModelStep]] = {"orch": list(orch or [])}
        for sid, steps in (sub or {}).items():
            self.scripts[f"sub-{sid}"] = list(steps)
        self.scripts.update({k: list(v) for k, v in (other or {}).items()})
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self._used: Counter[str] = Counter()
        self._ids = itertools.count(1)

    def __call__(self, step_id: str, body: dict[str, Any]) -> dict[str, Any]:
        self.requests.append((step_id, json.loads(json.dumps(body))))
        script = self.scripts.get(step_id)
        k = self._used[step_id]
        if script is None or k >= len(script):
            raise AssertionError(f"ScriptedModel：{step_id} 第 {k + 1} 次调用没有脚本")
        self._used[step_id] += 1
        item = script[k]
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            item = item(body)
        choice = json.loads(json.dumps(item))
        for tc in choice["message"].get("tool_calls") or []:
            tc.setdefault("id", f"call_{next(self._ids)}")
        return {"id": "resp", "choices": [{"index": 0, **choice}], "usage": {"total_tokens": 1}}

    def bodies(self, step_id: str) -> list[dict[str, Any]]:
        return [b for s, b in self.requests if s == step_id]
