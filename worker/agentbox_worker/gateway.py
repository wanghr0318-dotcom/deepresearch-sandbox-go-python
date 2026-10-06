"""Gateway 客户端：经 Unix socket 的 HTTP/1.1 访问模型、搜索、抓取、exec 与 blob（规格 §9.3–§10）。

只用标准库（http.client + socket.AF_UNIX），方法均为同步阻塞调用；在 asyncio 应用中应经
asyncio.to_thread 调用，避免阻塞控制消息处理。每个请求使用一条新连接，不复用 keep-alive。

计费调用都携带确定性的 X-Agentbox-Call-Id（<subrun_id|root>/<step_id>/<kind>/<n>），序号由
CallIds 持有；TaskContext 在 checkpoint 时把 CallIds 快照并入状态，恢复后续号不重复、不回退。
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import socket
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    CallInProgress,
    GatewayError,
    ToolBudgetExhausted,
)
from agentbox_worker.protocol import valid_subrun_id

DEFAULT_SOCKET_PATH = "/run/agentbox/gateway.sock"
MAX_CALL_ID_BYTES = 256

# 客户端等待一次 Gateway 请求的默认上限（秒）：须长于服务端最长的调用期限
# （模型调用默认 300 s，`--model-call-deadline`；搜索与抓取 120 s），让 Gateway 先给出
# 504 call_deadline_exceeded，而不是客户端先放弃、应用以新调用 ID 重做一次可能已计费的调用。
# 宿主调高期限时经 --worker-env 设置 GATEWAY_TIMEOUT_ENV。
DEFAULT_TIMEOUT_S = 330.0
GATEWAY_TIMEOUT_ENV = "AGENTBOX_GATEWAY_TIMEOUT_S"
MAX_TIMEOUT_S = 86400.0  # 过大的值会使 socket 超时溢出

# kind → 端点（M2 范围；exec 为 M4 Plan 15）
ENDPOINTS: dict[str, str] = {
    "chat": "/v1/chat/completions",
    "search": "/v1/search",
    "fetch": "/v1/fetch",
    "exec": "/v1/exec",
}

# exec（规格 §10、§19 补充）：服务端的调用期限 = 排队上限 + 生效 wall + 30 s（§9.7）。客户端超时按
# server 默认值估算并加余量，且不短于普通调用的超时，让 Gateway 先给出 504 而不是客户端先放弃。
EXEC_LANGUAGE = "python3"
EXEC_QUEUE_TIMEOUT_S = 60.0
EXEC_DEFAULT_WALL_MS = 60_000
EXEC_DEADLINE_GRACE_S = 30.0
EXEC_CLIENT_MARGIN_S = 15.0


def exec_timeout_s(wall_ms: int | None) -> float:
    """一次 exec 的客户端等待上限（秒）：排队上限 + wall + 30 s + 余量。

    wall_ms 为 None 时按 server 默认值估算。"""
    wall = EXEC_DEFAULT_WALL_MS if wall_ms is None else wall_ms
    return EXEC_QUEUE_TIMEOUT_S + wall / 1000 + EXEC_DEADLINE_GRACE_S + EXEC_CLIENT_MARGIN_S


def _positive_int(name: str, value: int | None) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} 须为整数，得到 {value!r}")
    if value <= 0:
        raise ValueError(f"{name} 须为正数，得到 {value}")


_SHA256 = re.compile(r"[0-9a-f]{64}")
# 工具额度头（M4）：search/fetch 的每个成功响应带 "<used>/<limit>"
TOOL_BUDGET_HEADER = "x-agentbox-tool-budget"
_TOOL_BUDGET = re.compile(r"(\d{1,9})/(\d{1,9})")


class CallIds:
    """调用 ID 计数器：每个 (subrun|root, step_id, kind) 一个从 1 开始的序号。

    由单一状态所有者（TaskContext）持有；snapshot/restore 的键是 "<scope>/<step_id>/<kind>"，
    值是已分配的最大序号。线程安全：应用可在线程池中并发发起调用。
    """

    def __init__(self) -> None:
        self._last: dict[str, int] = {}
        self._lock = threading.Lock()

    def next(self, step_id: str, kind: str, subrun_id: str | None = None) -> str:
        if not step_id or not kind:
            raise ValueError("step_id 与 kind 不能为空")
        if subrun_id == "":
            raise ValueError("subrun_id 不能为空字符串")
        prefix = f"{subrun_id or 'root'}/{step_id}/{kind}"
        with self._lock:
            n = self._last.get(prefix, 0) + 1
            call_id = f"{prefix}/{n}"
            if len(call_id.encode("utf-8")) > MAX_CALL_ID_BYTES:
                raise ValueError(f"call id 超过 {MAX_CALL_ID_BYTES} 字节：{call_id[:64]!r}…")
            self._last[prefix] = n
        return call_id

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._last)

    @classmethod
    def restore(cls, data: dict[str, int]) -> CallIds:
        ids = cls()
        for prefix, n in data.items():
            if not isinstance(prefix, str) or isinstance(n, bool) or not isinstance(n, int):
                raise ValueError(f"call id 快照项不合法：{prefix!r}: {n!r}")
            if n < 0:
                raise ValueError(f"call id 快照序号为负：{prefix!r}: {n}")
            ids._last[prefix] = n
        return ids


@dataclass(frozen=True)
class GatewayResult:
    call_id: str
    body: dict
    blob_sha256: str | None
    replayed: bool
    status: int
    # X-Agentbox-Tool-Budget 的 (used, limit)；无头或格式错为 None（调用方按本地计数）
    tool_budget: tuple[int, int] | None = None


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        family = getattr(socket, "AF_UNIX", None)
        if family is None:
            raise GatewayError(0, "unsupported_platform", "当前平台没有 AF_UNIX")
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout)
            sock.connect(self._socket_path)
        except BaseException:
            sock.close()
            raise
        self.sock = sock


@dataclass(frozen=True)
class _Response:
    status: int
    headers: dict[str, str]  # 名称小写
    data: bytes


def _cache_headers(no_cache: bool) -> dict[str, str]:
    """缓存指令头（规格 §11.4）：no-cache 时为 X-Agentbox-Cache: no-cache，否则不发送。"""
    return {"X-Agentbox-Cache": "no-cache"} if no_cache else {}


def _error_from(status: int, data: bytes) -> GatewayError:
    code, message = f"http_{status}", ""
    try:
        err = json.loads(data)["error"]
        if isinstance(err.get("code"), str) and err["code"]:
            code = err["code"]
        if isinstance(err.get("message"), str):
            message = err["message"]
    except (ValueError, TypeError, KeyError, AttributeError):
        message = data[:512].decode("utf-8", "replace")
    if status == 402:
        return BudgetExhausted(status, code, message)
    # input_not_authorized（exec 的输入 blob 不在任务 scope 内）是请求参数问题，不是访问撤销
    if status == 403 and code != "input_not_authorized":
        return AccessRevoked(status, code, message)
    if status == 504:
        return CallDeadlineExceeded(status, code, message)
    if status == 409 and code == "fingerprint_mismatch":
        return CallDivergence(status, code, message)
    if status == 409 and code == "call_in_progress":
        return CallInProgress(status, code, message)
    if status == 429 and code == "tool_budget_exhausted":
        return ToolBudgetExhausted(status, code, message)
    return GatewayError(status, code, message)


def _tool_budget(raw: str | None) -> tuple[int, int] | None:
    """解析 X-Agentbox-Tool-Budget: <used>/<limit>；缺失或格式错误为 None。"""
    m = _TOOL_BUDGET.fullmatch(raw.strip()) if raw else None
    return (int(m.group(1)), int(m.group(2))) if m else None


def default_timeout_s() -> float:
    """客户端超时的默认值：AGENTBOX_GATEWAY_TIMEOUT_S（正数秒）优先，否则 DEFAULT_TIMEOUT_S。

    取值不合法（非数值、≤ 0、NaN 或超过 MAX_TIMEOUT_S）时抛出 ValueError：与收尾期限不同，这里不静默
    退回默认值——宿主设置它正是因为期限长于默认值，退回会重新引入客户端先放弃、重做已计费调用的问题。
    """
    raw = os.environ.get(GATEWAY_TIMEOUT_ENV, "").strip()
    if not raw:
        return DEFAULT_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    if not 0 < value <= MAX_TIMEOUT_S:  # NaN 的比较为假
        raise ValueError(
            f"{GATEWAY_TIMEOUT_ENV} 须为 (0, {MAX_TIMEOUT_S:g}] 内的秒数，得到 {raw!r}"
        )
    return value


class GatewayClient:
    """Gateway 的同步客户端。错误映射见 errors.py；call_in_progress 在客户端有界重试（同一 ID）。"""

    # call_in_progress 的有界等待：指数退避，总等待不超过 in_progress_wait_s
    in_progress_wait_s: float = 30.0
    in_progress_backoff_s: float = 0.5
    in_progress_backoff_max_s: float = 4.0

    def __init__(
        self,
        socket_path: str = DEFAULT_SOCKET_PATH,
        *,
        call_ids: CallIds,
        timeout_s: float | None = None,
    ) -> None:
        self.socket_path = socket_path
        self.call_ids = call_ids
        self.timeout_s = default_timeout_s() if timeout_s is None else timeout_s

    # 调用归属：None = root（task 本身）；SubrunGateway 设为其 sub-run ID
    subrun_id: str | None = None

    def _next_id(self, step_id: str, kind: str) -> str:
        return self.call_ids.next(step_id, kind, self.subrun_id)

    # ---- 计费调用 ----

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
        """model 为 None 时不写入请求体（Gateway 用服务端默认模型）；给出时须在服务端
        声明的白名单中，否则为 400 unsupported_model。模型名进入调用指纹：恢复后重发
        同一调用须给出同一模型。tools/tool_choice 为 OpenAI 兼容的函数调用参数，None 时不写入。"""
        body = self.chat_body(
            messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
        )
        return self._call("chat", self._next_id(step_id, "chat"), body, {})

    @staticmethod
    def chat_body(
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | None = None,
    ) -> dict[str, Any]:
        """chat 的请求体（取代重发须发送与原调用相同的请求体）。

        值为 None 的可选字段不写入，故不带工具的调用与之前的请求体（调用指纹）相同。"""
        body: dict[str, Any] = {"messages": messages}
        if model is not None:
            body["model"] = model
        if max_tokens is not None:
            body["max_tokens"] = max_tokens
        if temperature is not None:
            body["temperature"] = temperature
        if tools is not None:
            body["tools"] = tools
        if tool_choice is not None:
            body["tool_choice"] = tool_choice
        return body

    def search(
        self, step_id: str, query: str, *, max_results: int = 5, no_cache: bool = False
    ) -> GatewayResult:
        """no_cache=True 发送 X-Agentbox-Cache: no-cache：不读共享缓存、不加入合并，结果仍写入缓存；
        该指令计入调用指纹（规格 §11.4），恢复后重发同一调用须给出同一取值。"""
        body = {"query": query, "max_results": max_results}
        return self._call(
            "search", self._next_id(step_id, "search"), body, _cache_headers(no_cache)
        )

    def fetch(self, step_id: str, url: str, *, no_cache: bool = False) -> GatewayResult:
        """no_cache 的含义同 search。"""
        return self._call(
            "fetch", self._next_id(step_id, "fetch"), {"url": url}, _cache_headers(no_cache)
        )

    def exec(
        self,
        step_id: str,
        code: str,
        *,
        inputs: Sequence[tuple[str, str]] = (),
        wall_ms: int | None = None,
        memory_bytes: int | None = None,
        retry: bool = False,
    ) -> GatewayResult:
        """POST /v1/exec（规格 §10）：在全新的 exec 环境中以 python3 执行 code；call id 为
        <root|subrun>/<step_id>/exec/<n>。

        inputs 是 (sha256, path)：已授权到本任务的 blob 放到 /in/<path>（只读）。wall_ms、
        memory_bytes 为 None 时不写入（server 默认值）；超过 server 上限时被截断，生效值见结果。
        请求内容进入调用指纹，恢复后重发同一调用须给出相同参数。retry=True 发送
        X-Agentbox-Retry: true：以同一 call id 重跑此前可重试地失败的 exec（如 exec_cancelled）。
        客户端超时 = 排队上限 + wall + 30 s + 余量（见 exec_timeout_s），不短于 timeout_s。
        """
        body = self.exec_body(code, inputs=inputs, wall_ms=wall_ms, memory_bytes=memory_bytes)
        extra = {"X-Agentbox-Retry": "true"} if retry else {}
        return self._call("exec", self._next_id(step_id, "exec"), body, extra)

    @staticmethod
    def exec_body(
        code: str,
        *,
        inputs: Sequence[tuple[str, str]] = (),
        wall_ms: int | None = None,
        memory_bytes: int | None = None,
    ) -> dict[str, Any]:
        """exec 的请求体：inputs 为空时不写 inputs，两个限制都为 None 时不写 limits。参数类型在本地
        检查（不占 call id）；内容规则（路径、sha、大小）由 Gateway 校验（400）。"""
        if not isinstance(code, str):
            raise TypeError(f"code 须为 str，得到 {type(code).__name__}")
        _positive_int("wall_ms", wall_ms)
        _positive_int("memory_bytes", memory_bytes)
        items = []
        for item in inputs:
            if (
                not isinstance(item, tuple | list)
                or len(item) != 2
                or not all(isinstance(v, str) for v in item)
            ):
                raise ValueError(f"inputs 的每项须为 (sha256, path)，得到 {item!r}")
            items.append({"sha256": item[0], "path": item[1]})
        body: dict[str, Any] = {"language": EXEC_LANGUAGE, "code": code}
        if items:
            body["inputs"] = items
        limits = {
            k: v for k, v in (("wall_ms", wall_ms), ("memory_bytes", memory_bytes)) if v is not None
        }
        if limits:
            body["limits"] = limits
        return body

    def retry(
        self, step_id: str, kind: str, call_id: str, body: dict, *, no_cache: bool = False
    ) -> GatewayResult:
        """以同一 call id 重发一次已失败的调用（X-Agentbox-Retry: true，规格 §9.4 failed 行）。
        no_cache 须与原调用一致（缓存指令计入指纹）。"""
        return self._call(
            kind, call_id, body, {"X-Agentbox-Retry": "true", **_cache_headers(no_cache)}
        )

    def supersede(
        self, step_id: str, kind: str, old_call_id: str, reason: str, body: dict
    ) -> GatewayResult:
        """以新 call id 显式取代旧调用（CallDivergence 之后由编排层发起，规格 §9.4）。"""
        headers = {"X-Agentbox-Supersedes": old_call_id, "X-Agentbox-Supersede-Reason": reason}
        return self._call(kind, self._next_id(step_id, kind), body, headers)

    # ---- 只读端点 ----

    def budget(self) -> dict[str, int]:
        resp = self._send("GET", "/v1/budget", None, {})
        if resp.status != 200:
            raise _error_from(resp.status, resp.data)
        return self._json(resp)

    def read_blob(self, sha256: str) -> bytes:
        """读取已授权到当前 scope 的 blob，并校验内容哈希。"""
        if not _SHA256.fullmatch(sha256):
            raise ValueError(f"不是 sha256：{sha256!r}")
        resp = self._send("GET", f"/blobs/{sha256}", None, {})
        if resp.status != 200:
            raise _error_from(resp.status, resp.data)
        if hashlib.sha256(resp.data).hexdigest() != sha256:
            raise GatewayError(resp.status, "blob_corrupt", f"blob {sha256} 内容与哈希不符")
        return resp.data

    # ---- 内部 ----

    def _call(self, kind: str, call_id: str, body: dict, extra: dict[str, str]) -> GatewayResult:
        path = ENDPOINTS.get(kind)
        if path is None:
            raise ValueError(f"未知的调用类型：{kind!r}")
        if len(call_id.encode("utf-8")) > MAX_CALL_ID_BYTES:
            raise ValueError(f"call id 超过 {MAX_CALL_ID_BYTES} 字节")
        payload = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        headers = {"Content-Type": "application/json", "X-Agentbox-Call-Id": call_id, **extra}
        # exec（含 retry/supersede 发出的 exec）的等待上限随 wall 变化；其他调用用 timeout_s
        send_kw: dict[str, float] = {}
        if kind == "exec":
            limits = body.get("limits")
            wall = limits.get("wall_ms") if isinstance(limits, dict) else None
            wall_ms = wall if isinstance(wall, int) and not isinstance(wall, bool) else None
            send_kw["timeout_s"] = max(self.timeout_s, exec_timeout_s(wall_ms))
        deadline = time.monotonic() + self.in_progress_wait_s
        delay = self.in_progress_backoff_s
        while True:
            resp = self._send("POST", path, payload, headers, **send_kw)
            if 200 <= resp.status < 300:
                return self._result(call_id, resp)
            err = _error_from(resp.status, resp.data)
            if not isinstance(err, CallInProgress) or time.monotonic() + delay > deadline:
                raise err
            time.sleep(delay)
            delay = min(delay * 2, self.in_progress_backoff_max_s)

    def _result(self, call_id: str, resp: _Response) -> GatewayResult:
        blob = resp.headers.get("x-agentbox-blob") or None
        if blob is not None and not _SHA256.fullmatch(blob):
            raise GatewayError(resp.status, "invalid_response", f"X-Agentbox-Blob 不合法：{blob!r}")
        replayed = resp.headers.get("x-agentbox-replayed", "").lower() == "true"
        budget = _tool_budget(resp.headers.get(TOOL_BUDGET_HEADER))
        return GatewayResult(call_id, self._json(resp), blob, replayed, resp.status, budget)

    @staticmethod
    def _json(resp: _Response) -> dict:
        try:
            body = json.loads(resp.data)
        except ValueError as exc:
            raise GatewayError(resp.status, "invalid_response", f"响应不是 JSON：{exc}") from exc
        if not isinstance(body, dict):
            raise GatewayError(resp.status, "invalid_response", "响应不是 JSON 对象")
        return body

    def _send(
        self,
        method: str,
        path: str,
        payload: bytes | None,
        headers: dict[str, str],
        *,
        timeout_s: float | None = None,
    ) -> _Response:
        timeout = self.timeout_s if timeout_s is None else timeout_s
        conn = _UnixHTTPConnection(self.socket_path, timeout)
        try:
            try:
                conn.connect()
            except (FileNotFoundError, ConnectionRefusedError) as exc:
                # socket 已删除或不再接受连接：访问已被撤销（规格 §9.1、E20）
                raise AccessRevoked(0, "connection_refused", str(exc)) from exc
            conn.request(method, path, body=payload, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return _Response(resp.status, {k.lower(): v for k, v in resp.getheaders()}, data)
        except TimeoutError as exc:
            raise CallDeadlineExceeded(0, "client_timeout", f"{timeout} s 内无响应") from exc
        except (OSError, http.client.HTTPException) as exc:
            raise GatewayError(0, "connection_lost", f"{type(exc).__name__}: {exc}") from exc
        finally:
            conn.close()


# sub-run 归属头（规格 §9.2）与每 sub-run 的上游在途上限（规格 §9.7）
SUBRUN_HEADER = "X-Agentbox-Subrun"
MAX_SUBRUN_IN_FLIGHT = 2


class SubrunGateway(GatewayClient):
    """GatewayClient 的 sub-run 视图（规格 §9.2、§9.7、§13）。

    - 每个请求（含 /v1/budget 与 /blobs）带 X-Agentbox-Subrun: <subrun_id>；计费调用的
      call id 前缀为 <subrun_id>/。与 root 共用同一个 CallIds（键按前缀区分），因此随
      checkpoint 一起保存、恢复后续号。
    - 同一视图的计费调用在途至多 MAX_SUBRUN_IN_FLIGHT 个（与 Gateway 的上限一致）；
      多出的调用在其线程中等待槽位，不发出请求。
    - 不提供创建 sub-run 的能力：sub-run 不能递归（规格 §13.2），of() 拒绝从视图再派生。
    - close() 之后的新请求在本地以 GatewayError(0, "subrun_closed") 失败、不发出。已经在途的
      同步请求（经 asyncio.to_thread 发出）不能被协程取消终止：宿主取消该 sub-run 后它们会很快
      以 409 subrun_closed 返回，结果由调用方丢弃。SDK 不宣称能停止这些线程。
    """

    def __init__(
        self,
        socket_path: str = DEFAULT_SOCKET_PATH,
        *,
        call_ids: CallIds,
        subrun_id: str,
        timeout_s: float | None = None,
    ) -> None:
        if not valid_subrun_id(subrun_id):
            raise ValueError(f"sub-run ID 不合法：{subrun_id!r}")
        super().__init__(socket_path, call_ids=call_ids, timeout_s=timeout_s)
        self.subrun_id = subrun_id
        self._slots = threading.BoundedSemaphore(MAX_SUBRUN_IN_FLIGHT)
        self._closed = threading.Event()

    @classmethod
    def of(cls, client: GatewayClient, subrun_id: str) -> SubrunGateway:
        """由 root 客户端派生 sub-run 视图：同一 socket、超时、退避参数与 CallIds。"""
        if isinstance(client, SubrunGateway):
            raise TypeError("sub-run 视图不能再派生 sub-run（不允许递归）")
        view = cls(
            client.socket_path,
            call_ids=client.call_ids,
            subrun_id=subrun_id,
            timeout_s=client.timeout_s,
        )
        view.in_progress_wait_s = client.in_progress_wait_s
        view.in_progress_backoff_s = client.in_progress_backoff_s
        view.in_progress_backoff_max_s = client.in_progress_backoff_max_s
        return view

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def close(self) -> None:
        """sub-run 已结束或被取消：此后的请求在本地失败。"""
        self._closed.set()

    def retry(
        self, step_id: str, kind: str, call_id: str, body: dict, *, no_cache: bool = False
    ) -> GatewayResult:
        if not call_id.startswith(f"{self.subrun_id}/"):
            raise ValueError(f"call id {call_id!r} 不属于 sub-run {self.subrun_id}")
        return super().retry(step_id, kind, call_id, body, no_cache=no_cache)

    def _call(self, kind: str, call_id: str, body: dict, extra: dict[str, str]) -> GatewayResult:
        self._check_open()
        with self._slots:
            return super()._call(kind, call_id, body, extra)

    def _send(
        self,
        method: str,
        path: str,
        payload: bytes | None,
        headers: dict[str, str],
        *,
        timeout_s: float | None = None,
    ) -> _Response:
        self._check_open()
        assert self.subrun_id is not None
        # 只在给出时转交 timeout_s（与 GatewayClient._call 一致：只有 exec 传它）
        kw = {} if timeout_s is None else {"timeout_s": timeout_s}
        return super()._send(
            method, path, payload, {**headers, SUBRUN_HEADER: self.subrun_id}, **kw
        )

    def _check_open(self) -> None:
        if self._closed.is_set():
            raise GatewayError(0, "subrun_closed", f"sub-run {self.subrun_id} 已结束或被取消")
