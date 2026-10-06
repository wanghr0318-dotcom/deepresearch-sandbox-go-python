"""进程内的测试替身（不依赖 AF_UNIX，Windows 上可运行）。

ScriptedGateway：实现 tools.base.GatewayLike，按脚本回答 chat/search/fetch，并像真实 Gateway 一样
分配确定性的 call id、为每个结果生成 blob、对同一 call id 的重发返回同一结果（replayed=True）。
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from agentbox_worker.gateway import CallIds, GatewayClient, GatewayResult

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
        if old is not None:  # 同一 call id 重发：重放已记录的结果
            return GatewayResult(old.call_id, old.body, old.blob_sha256, True, old.status, None)
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
