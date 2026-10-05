"""测试共用夹具：fake Gateway（Unix socket 上的 HTTP/1.1 服务器，契约见 Plan 7 Task 4）。

没有 AF_UNIX 的平台（Windows 上的 Python）跳过依赖它的测试；设置
AGENTBOX_REQUIRE_UNIX_TESTS=1 时改为失败，用于证明这些测试确实在 Linux 上运行过。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import tempfile
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from typing import Any

import pytest

ENDPOINT_KINDS = {"/v1/chat/completions": "chat", "/v1/search": "search", "/v1/fetch": "fetch"}


@dataclass
class Recorded:
    method: str
    path: str
    headers: dict[str, str]  # 名称小写
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class Reply:
    status: int
    body: Any  # dict → JSON；bytes 原样发送
    headers: dict[str, str] = field(default_factory=dict)


def error_reply(status: int, code: str, message: str = "") -> Reply:
    return Reply(status, {"error": {"code": code, "message": message}})


class FakeGateway:
    """按 Plan 7 Task 4 的端点与头部工作的最小 Gateway。

    计费调用：先消费 replies 队列中的预设回复；否则生成结果 blob 并以 X-Agentbox-Blob 返回，
    同一 call id 再次到达时返回同一 blob 并带 X-Agentbox-Replayed: true。delay 秒内不响应
    （测试结束时立即放行），用于客户端超时测试。
    """

    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self.requests: list[Recorded] = []
        self.replies: list[Reply] = []
        self.blobs: dict[str, bytes] = {}
        self.calls: dict[str, str] = {}  # call id → 结果 blob sha
        self.budget = {"budget_micro": 1_000_000, "remaining_micro": 750_000}
        self.delay = 0.0
        self.release = threading.Event()
        self._lock = threading.Lock()

    def put_blob(self, data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        self.blobs[sha] = data
        return sha

    def handle(self, req: Recorded) -> Reply:
        with self._lock:
            self.requests.append(req)
            queued = self.replies.pop(0) if self.replies else None
        if self.delay:
            self.release.wait(self.delay)
        if queued is not None:
            return queued
        if req.method == "GET" and req.path == "/v1/budget":
            return Reply(200, self.budget)
        if req.method == "GET" and req.path.startswith("/blobs/"):
            data = self.blobs.get(req.path.removeprefix("/blobs/"))
            if data is None:
                return error_reply(404, "not_found")
            return Reply(200, data, {"ETag": req.path.removeprefix("/blobs/")})
        kind = ENDPOINT_KINDS.get(req.path)
        if req.method != "POST" or kind is None:
            return error_reply(404, "not_found")
        call_id = req.headers.get("x-agentbox-call-id")
        if not call_id:
            return error_reply(400, "missing_call_id")
        with self._lock:
            sha = self.calls.get(call_id)
            replayed = sha is not None
            if sha is None:
                result = {"kind": kind, "call_id": call_id, "request": req.json()}
                sha = self.put_blob(json.dumps(result, ensure_ascii=False).encode())
                self.calls[call_id] = sha
        headers = {"X-Agentbox-Blob": sha}
        if replayed:
            headers["X-Agentbox-Replayed"] = "true"
        return Reply(200, self.blobs[sha], headers)


def _handler_for(gateway: FakeGateway) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            headers = {k.lower(): v for k, v in self.headers.items()}
            reply = gateway.handle(Recorded(self.command, self.path, headers, body))
            data = reply.body if isinstance(reply.body, bytes) else json.dumps(reply.body).encode()
            self.send_response(reply.status)
            for name, value in reply.headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
            self.close_connection = True

        do_GET = _serve
        do_POST = _serve

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002  基类签名
            pass

    return Handler


@pytest.fixture
def fake_gateway() -> Iterator[FakeGateway]:
    if not hasattr(socket, "AF_UNIX") or os.name == "nt":
        if os.environ.get("AGENTBOX_REQUIRE_UNIX_TESTS") == "1":
            pytest.fail("AGENTBOX_REQUIRE_UNIX_TESTS=1 但当前平台没有 AF_UNIX")
        pytest.skip("需要 AF_UNIX（在 Linux/WSL 中运行）")
    import socketserver

    directory = tempfile.mkdtemp(prefix="agw-")  # 短路径：sun_path 上限 108 字节
    gateway = FakeGateway(os.path.join(directory, "gateway.sock"))
    server = socketserver.ThreadingUnixStreamServer(gateway.socket_path, _handler_for(gateway))
    server.daemon_threads = True
    server.handle_error = lambda request, address: None  # 客户端超时离开后的写失败不是测试错误
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
    thread.start()
    try:
        yield gateway
    finally:
        gateway.release.set()
        server.shutdown()
        server.server_close()
        thread.join(5)
        shutil.rmtree(directory, ignore_errors=True)
