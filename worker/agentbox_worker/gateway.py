"""Gateway 客户端：经 Unix socket 的 HTTP/1.1 访问模型、搜索、抓取与 blob（规格 §9.3、§9.4）。

只用标准库（http.client + socket.AF_UNIX），方法均为同步阻塞调用；在 asyncio 应用中应经
asyncio.to_thread 调用，避免阻塞控制消息处理。每个请求使用一条新连接，不复用 keep-alive。

计费调用都携带确定性的 X-Agentbox-Call-Id（<subrun_id|root>/<step_id>/<kind>/<n>），序号由
CallIds 持有；TaskContext 在 checkpoint 时把 CallIds 快照并入状态，恢复后续号不重复、不回退。
"""

from __future__ import annotations

import hashlib
import http.client
import json
import re
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    CallInProgress,
    GatewayError,
)

DEFAULT_SOCKET_PATH = "/run/agentbox/gateway.sock"
MAX_CALL_ID_BYTES = 256

# kind → 端点（M2 范围）
ENDPOINTS: dict[str, str] = {
    "chat": "/v1/chat/completions",
    "search": "/v1/search",
    "fetch": "/v1/fetch",
}

_SHA256 = re.compile(r"[0-9a-f]{64}")


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
    if status == 403:
        return AccessRevoked(status, code, message)
    if status == 504:
        return CallDeadlineExceeded(status, code, message)
    if status == 409 and code == "fingerprint_mismatch":
        return CallDivergence(status, code, message)
    if status == 409 and code == "call_in_progress":
        return CallInProgress(status, code, message)
    return GatewayError(status, code, message)


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
        timeout_s: float = 130.0,
    ) -> None:
        self.socket_path = socket_path
        self.call_ids = call_ids
        self.timeout_s = timeout_s

    # ---- 计费调用 ----

    def chat(
        self,
        step_id: str,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> GatewayResult:
        """model 为 None 时不写入请求体（Gateway 用服务端默认模型）；给出时须在服务端
        声明的白名单中，否则为 400 unsupported_model。模型名进入调用指纹：恢复后重发
        同一调用须给出同一模型。"""
        body = self.chat_body(messages, model=model, max_tokens=max_tokens, temperature=temperature)
        return self._call("chat", self.call_ids.next(step_id, "chat"), body, {})

    @staticmethod
    def chat_body(
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> dict[str, Any]:
        """chat 的请求体（取代重发须发送与原调用相同的请求体）。"""
        body: dict[str, Any] = {"messages": messages}
        if model is not None:
            body["model"] = model
        if max_tokens is not None:
            body["max_tokens"] = max_tokens
        if temperature is not None:
            body["temperature"] = temperature
        return body

    def search(self, step_id: str, query: str, *, max_results: int = 5) -> GatewayResult:
        body = {"query": query, "max_results": max_results}
        return self._call("search", self.call_ids.next(step_id, "search"), body, {})

    def fetch(self, step_id: str, url: str) -> GatewayResult:
        return self._call("fetch", self.call_ids.next(step_id, "fetch"), {"url": url}, {})

    def retry(self, step_id: str, kind: str, call_id: str, body: dict) -> GatewayResult:
        """以同一 call id 重发一次已失败的调用（X-Agentbox-Retry: true，规格 §9.4 failed 行）。"""
        return self._call(kind, call_id, body, {"X-Agentbox-Retry": "true"})

    def supersede(
        self, step_id: str, kind: str, old_call_id: str, reason: str, body: dict
    ) -> GatewayResult:
        """以新 call id 显式取代旧调用（CallDivergence 之后由编排层发起，规格 §9.4）。"""
        headers = {"X-Agentbox-Supersedes": old_call_id, "X-Agentbox-Supersede-Reason": reason}
        return self._call(kind, self.call_ids.next(step_id, kind), body, headers)

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
        deadline = time.monotonic() + self.in_progress_wait_s
        delay = self.in_progress_backoff_s
        while True:
            resp = self._send("POST", path, payload, headers)
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
        return GatewayResult(call_id, self._json(resp), blob, replayed, resp.status)

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
        self, method: str, path: str, payload: bytes | None, headers: dict[str, str]
    ) -> _Response:
        conn = _UnixHTTPConnection(self.socket_path, self.timeout_s)
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
            raise CallDeadlineExceeded(0, "client_timeout", f"{self.timeout_s} s 内无响应") from exc
        except (OSError, http.client.HTTPException) as exc:
            raise GatewayError(0, "connection_lost", f"{type(exc).__name__}: {exc}") from exc
        finally:
            conn.close()
