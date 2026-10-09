"""W3C Trace Context：traceparent（版本 00）的最小实现，只用标准库。

宿主在 init / task_start 的可选字段 traceparent 中给出当前 worker.run span；SDK 把它原样作为
Gateway 请求的 traceparent 头，使 Worker 发起的模型、搜索、抓取与 exec 调用加入同一条 trace。
SDK 不生成也不导出 span（沙箱内没有 OpenTelemetry 依赖）。

规则与 Go 侧 internal/obs.ParseTraceparent 一致（共享向量 protocol/fixtures/v1/traceparent.json）：
只接受 ``00-<32 位小写十六进制 trace-id>-<16 位小写十六进制 parent-id>-<2 位小写十六进制 flags>``，
trace-id 与 parent-id 不得全为 0；其他取值一律丢弃
（宿主同样会校验，并忽略不属于本 attempt 的 trace）。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

HEADER = "traceparent"

_TRACEPARENT = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})")


@dataclass(frozen=True)
class TraceParent:
    trace_id: str
    parent_id: str
    flags: int

    @property
    def sampled(self) -> bool:
        return bool(self.flags & 1)


def parse_traceparent(value: Any) -> TraceParent | None:
    """解析 traceparent；不合法（含非字符串）时返回 None。"""
    if not isinstance(value, str):
        return None
    m = _TRACEPARENT.fullmatch(value)
    if m is None:
        return None
    trace_id, parent_id, flags = m.groups()
    if trace_id == "0" * 32 or parent_id == "0" * 16:
        return None
    return TraceParent(trace_id, parent_id, int(flags, 16))


def valid_traceparent(value: Any) -> str | None:
    """合法时原样返回 value，否则返回 None（用于决定是否发送 traceparent 头）。"""
    return value if parse_traceparent(value) is not None else None
