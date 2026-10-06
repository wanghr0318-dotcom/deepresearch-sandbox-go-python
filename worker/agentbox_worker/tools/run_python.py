"""run_python：经 Gateway /v1/exec 在一次性的隔离环境中运行 Python 代码（M4 设计 D3、规格 §10）。

Worker 内不执行任何代码：只经 SDK 的 GatewayClient.exec() 提交。不计入每轮 web 工具额度
（counts_budget=False），受 Gateway 的任务级 exec 配额约束。exec 配额用尽（402 exec_*）、
输入未授权、排队超时、取消等转为 ok=False 的结果回给模型；费用预算、访问撤销、指纹冲突与
Gateway 连接丢失按工具约定（tools.base.is_fatal）上抛给 Agent 循环。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, cast

from agentbox_worker.errors import BudgetExhausted, GatewayError
from agentbox_worker.gateway import GatewayResult
from agentbox_worker.tools.base import ToolContext, ToolResult, is_fatal

STDOUT_HEAD_BYTES = 4096
STDOUT_TAIL_BYTES = 1024
STDERR_TAIL_BYTES = 2048
GATEWAY_STREAM_CAP = "1 MiB"  # Gateway 对 stdout/stderr 各保留的上限（规格 §19 补充）
PREVIEW_STDOUT_CHARS = 2000  # 步骤行预览（面向用户）中 stdout 的开头
PREVIEW_STDERR_CHARS = 600

# 402 exec 配额拒绝（与费用预算的 budget_exhausted 同一 HTTP 状态，靠 code 区分）
_QUOTA_TEXT = {
    "exec_quota_exhausted": "本任务的代码执行次数配额已用完",
    "exec_cpu_exhausted": "本任务的代码执行 CPU 配额已用完",
    "exec_wall_exhausted": "本任务的代码运行时间配额已用完",
    "exec_blocked": "本任务的代码执行已停止（此前超出 CPU 配额）",
}
_ERROR_TEXT = {
    "input_not_authorized": "输入文件不属于本任务（sha256 未授权），请只使用本任务已得到的 blob",
    "inputs_too_large": "输入文件合计超过 256 MiB",
    "exec_queue_timeout": "排队等待执行环境超时，可稍后再试",
    "exec_cancelled": "执行被取消（任务停止或 attempt 被替换）",
}


class ExecGateway(Protocol):
    """run_python 所需的 Gateway 能力（SDK 的 GatewayClient.exec）。

    tools.base.GatewayLike（Plan 13）不含 exec，故在此单独声明。
    """

    def exec(
        self,
        step_id: str,
        code: str,
        *,
        inputs: Sequence[tuple[str, str]] = (),
        wall_ms: int | None = None,
        memory_bytes: int | None = None,
        retry: bool = False,
    ) -> GatewayResult: ...


class RunPython:
    name = "run_python"
    description = (
        "在隔离沙箱中运行一段 Python 3 代码（无网络、无状态、每次全新环境）。"
        "用于计算、数据处理与核对数字。可读取 /in 下的输入文件，写入 /out 的文件会作为输出返回。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "code": {"type": "string", "maxLength": 262144},
            "inputs": {
                "type": "array",
                "maxItems": 64,
                "items": {
                    "type": "object",
                    "properties": {
                        "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                        "path": {"type": "string"},
                    },
                    "required": ["sha256", "path"],
                    "additionalProperties": False,
                },
            },
            "timeout_s": {"type": "integer", "minimum": 1, "maximum": 300},
        },
        "required": ["code"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        inputs = [(item["sha256"], item["path"]) for item in args.get("inputs", [])]
        timeout_s = args.get("timeout_s")
        wall_ms = None if timeout_s is None else timeout_s * 1000
        gateway = cast(ExecGateway, ctx.gateway)
        try:
            res = gateway.exec(ctx.step_id, args["code"], inputs=inputs, wall_ms=wall_ms)
        except GatewayError as exc:
            if _is_exec_quota(exc):
                return _failure(f"{_QUOTA_TEXT[exc.code]}（{exc.code}），不要再调用 run_python。")
            if is_fatal(exc):
                raise
            return _failure(_error_text(exc))
        return _result(res)


def _is_exec_quota(exc: GatewayError) -> bool:
    return isinstance(exc, BudgetExhausted) and exc.code in _QUOTA_TEXT


def _error_text(exc: GatewayError) -> str:
    detail = _ERROR_TEXT.get(exc.code)
    reason = f"{exc.code}（{exc.message}）" if exc.message else exc.code
    return f"代码未能运行：{detail}（{reason}）。" if detail else f"代码未能运行：{reason}。"


def _failure(message: str) -> ToolResult:
    return ToolResult(content=message, ok=False, preview={"kind": "text", "text": message})


def _result(res: GatewayResult) -> ToolResult:
    body = res.body if isinstance(res.body, dict) else {}
    status = str(body.get("status", ""))
    exit_info = body.get("exit") if isinstance(body.get("exit"), dict) else {}
    limits = body.get("limits") if isinstance(body.get("limits"), dict) else {}
    diag = body.get("diag") if isinstance(body.get("diag"), dict) else {}
    lines = [_status_line(status, exit_info, limits), _timing_line(body)]
    lines += _stream("stdout", body, head=STDOUT_HEAD_BYTES, tail=STDOUT_TAIL_BYTES)
    lines += _stream("stderr", body, head=0, tail=STDERR_TAIL_BYTES)
    lines += _outputs(body)
    if diag.get("oom_observed") or diag.get("oom_kill_delta"):
        lines.append(
            f"内存不足：进程被 OOM 终止（内存上限 {limits.get('memory_bytes')} 字节），"
            "请减少数据量或分批处理。"
        )
    outputs = [o for o in body.get("outputs") or [] if isinstance(o, dict)]
    blobs = [res.blob_sha256] if res.blob_sha256 else []
    blobs += [o["sha256"] for o in outputs if isinstance(o.get("sha256"), str)]
    ok = status == "completed" and exit_info.get("code") == 0 and not exit_info.get("signal")
    return ToolResult(
        content="\n".join(lines),
        ok=ok,
        preview=_preview(body, status, exit_info, limits, outputs, ok),
        raw={"call_id": res.call_id},
        blobs=tuple(dict.fromkeys(blobs)),
        data=body,
    )


def _preview(
    body: dict[str, Any],
    status: str,
    exit_info: dict[str, Any],
    limits: dict[str, Any],
    outputs: list[dict[str, Any]],
    ok: bool,
) -> dict[str, Any]:
    """步骤行的预览（kind text）：退出码或状态、stdout 开头、失败时 stderr 末尾、输出文件名。"""
    if status == "completed" and not exit_info.get("signal"):
        lines = [f"退出码 {exit_info.get('code')}"]
    else:
        lines = [_status_line(status, exit_info, limits).removeprefix("状态：")]
    stdout = body.get("stdout") if isinstance(body.get("stdout"), str) else ""
    if stdout:
        lines.append(_head_chars(stdout.rstrip("\n"), PREVIEW_STDOUT_CHARS))
    stderr = body.get("stderr") if isinstance(body.get("stderr"), str) else ""
    if stderr and not ok:
        text = stderr.rstrip("\n")
        tail = text if len(text) <= PREVIEW_STDERR_CHARS else "…" + text[-PREVIEW_STDERR_CHARS:]
        lines.append("stderr：\n" + tail)
    names = [str(o.get("path")) for o in outputs]
    if names:
        lines.append("输出文件：" + "、".join(names))
    return {"kind": "text", "text": "\n".join(lines)}


def _head_chars(text: str, n: int) -> str:
    if len(text) <= n:
        return text
    return text[:n] + f"\n…（共 {len(text)} 字符，已截断）"


def _status_line(status: str, exit_info: dict[str, Any], limits: dict[str, Any]) -> str:
    code, signal = exit_info.get("code"), exit_info.get("signal")
    if status == "timed_out":
        return (
            f"状态：timed_out（运行超过时限 {limits.get('wall_ms')} ms 被终止"
            + (f"，信号 {signal}" if signal else "")
            + "）"
        )
    if signal:
        return f"状态：{status}（被信号 {signal} 终止）"
    return f"状态：{status}（退出码 {code}）"


def _timing_line(body: dict[str, Any]) -> str:
    return f"用时 {body.get('wall_ms')} ms（排队 {body.get('queue_ms')} ms）"


def _stream(name: str, body: dict[str, Any], *, head: int, tail: int) -> list[str]:
    text = body.get(name) if isinstance(body.get(name), str) else ""
    data = text.encode("utf-8")
    lines = [f"{name}（{len(data)} 字节）：" if data else f"{name}：（空）"]
    if data:
        lines.append(_clip(data, head, tail))
    if body.get(f"{name}_truncated"):
        cap = GATEWAY_STREAM_CAP
        lines.append(f"（{name} 超过 {cap}，Gateway 已截断，只保留前 {cap}）")
    if body.get(f"{name}_invalid_utf8"):
        lines.append(f"（{name} 含非法 UTF-8 字节，已替换为 U+FFFD）")
    return lines


def _clip(data: bytes, head: int, tail: int) -> str:
    """保留前 head 与末 tail 字节（按 UTF-8 字节，切口处不完整的字符丢弃），中间标注省略量。"""
    if len(data) <= head + tail:
        return data.decode("utf-8", errors="replace")
    omitted = len(data) - head - tail
    parts = []
    if head:
        parts.append(data[:head].decode("utf-8", errors="ignore"))
    parts.append(f"…（中间省略 {omitted} 字节）…" if head else f"…（前面省略 {omitted} 字节）…")
    parts.append(data[len(data) - tail :].decode("utf-8", errors="ignore"))
    return "\n".join(parts)


def _outputs(body: dict[str, Any]) -> list[str]:
    outputs = [o for o in body.get("outputs") or [] if isinstance(o, dict)]
    if not outputs:
        lines = ["输出文件：无"]
    else:
        lines = ["输出文件（/out）："]
        lines += [
            f"- {o.get('path')}（{o.get('size')} 字节，sha256 {o.get('sha256')}）" for o in outputs
        ]
    skipped = [s for s in body.get("skipped_outputs") or [] if isinstance(s, dict)]
    if skipped:
        lines.append("未收集的输出：")
        lines += [f"- {s.get('path')}：{s.get('reason')}" for s in skipped]
    return lines
