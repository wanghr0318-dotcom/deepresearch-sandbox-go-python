"""sim-worker 的步骤执行。配置格式见 Plan 3 Task 7；恢复时从 checkpoint 状态的 next_index 继续。

Gateway 操作（chat/search/fetch/exec）把结果 blob 的 sha256 收集起来，放入之后每个 checkpoint 的
refs（恢复时从 resume.refs 继续），供 Plan 7/8 的 e2e 验证 refs 授权。exec 另把输出文件的 sha256
加入 refs；给出 expect_status 而结果状态不符时任务以 sim_exec_status_mismatch 失败（e2e 判定用）。
Gateway 操作给出 expect_error 时，得到该错误码的 GatewayError 视为预期（继续下一步），未得到则失败。

sub-run 操作（M4 Plan 14 Task 11，故障实验 E24、E40–E45 用；需协商 subruns 扩展）：
- subrun_start {subrun_id, deadline_ms?, budget_cap_micro?, parent_step_id?}：启动并保存
  句柄；该 ID 已终态（宿主在 resume.subruns 中告知，或答复 subrun_closed）时记为已关闭，
  此后引用它的步骤一律跳过。恢复时 next_index 之前的 subrun_start 先以同一定义重发
  （宿主重新绑定未终态的 sub-run，规格 §13.5）。
- 任何步骤可带 subrun：Gateway 操作经该 sub-run 的视图发出（带归属头、call id 前缀
  <subrun_id>/）；该 sub-run 已关闭时跳过该步。任何步骤可带 attempt：只在该 attempt_no
  执行（例如只在第 1 个 attempt 崩溃）。
- subrun_end {subrun_id, status: succeeded|failed, summary?}：succeeded 先登记 internal
  产物 subruns/<subrun_id>.json（{summary, sources, partial}），以其 sha256 为 result_ref；
  已被取消时由 SDK 发出 subrun_end{cancelled}；之后记为已关闭。
- subrun_cancel {subrun_id, reason?}：编排层取消（subrun_cancel → subrun_end{cancelled}）。
- subrun_ignore_cancel：丢弃宿主的 subrun_cancel_requested（模拟忽略取消的 SDK，E44）。
- checkpoint 可带 forge_completed: [subrun_id]（把已取消的 sub-run 在本地伪造为 completed
  后提交，提交后还原；E41）与 expect_rejected: <code>（含该码的拒绝视为预期）。
- exit 可带 signal（例如 9）：向自身发送该信号，代替 os._exit。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from agentbox_worker import (
    CheckpointRejected,
    Paused,
    Result,
    SubrunCancelled,
    SubrunHandle,
    SubrunRejected,
    TaskContext,
    WorkerFailure,
)
from agentbox_worker.errors import GatewayError
from agentbox_worker.gateway import GatewayClient, GatewayResult

Op = dict[str, Any]


@dataclass
class Held:
    """一次运行中跨步骤保留的数据：分配的内存、待放入 checkpoint 的 blob 引用与 sub-run 句柄。"""

    memory: list[bytearray] = field(default_factory=list)
    refs: list[str] = field(default_factory=list)
    subruns: dict[str, SubrunHandle] = field(default_factory=dict)
    closed: set[str] = field(default_factory=set)  # 已终态或已结束的 sub-run：引用它的步骤跳过


Handler = Callable[[TaskContext, Op, int, Held], Awaitable["Paused | None"]]


async def run(ctx: TaskContext) -> Result | Paused:
    config = ctx.config if isinstance(ctx.config, dict) else {}
    steps: list[Op] = config.get("steps", [])
    exec_log = config.get("exec_log")
    held = Held(refs=list(ctx.resume.refs) if ctx.resume is not None else [])
    start = _start_index(ctx)
    # 恢复：先以同一定义重发此前的 subrun_start（宿主重新绑定未终态的 sub-run；已终态的记为已关闭）
    for op in steps[:start]:
        if op.get("op") == "subrun_start":
            await _subrun_start(ctx, op, -1, held)
    for index in range(start, len(steps)):
        op = steps[index]
        handler = _HANDLERS.get(op.get("op", ""))
        if handler is None:
            raise WorkerFailure("sim_bad_config", f"第 {index} 步：未知操作 {op.get('op')!r}")
        if _skipped(ctx, op, index, held):
            _record(exec_log, ctx, index, "skip:" + op["op"])
            continue
        _record(exec_log, ctx, index, op["op"])
        outcome = await handler(ctx, op, index, held)
        if outcome is not None:
            return outcome
    _record(exec_log, ctx, len(steps), "result")
    return Result(summary=config.get("summary", "done"), outputs=list(config.get("outputs", [])))


def _skipped(ctx: TaskContext, op: Op, index: int, held: Held) -> bool:
    """attempt 不符，或引用的 sub-run 已关闭时跳过该步；引用未启动的 sub-run 是配置错误。"""
    if "attempt" in op and op["attempt"] != ctx.attempt_no:
        return True
    sid = op.get("subrun") or (op.get("subrun_id") if op["op"] != "subrun_start" else None)
    if sid is None or sid in held.closed:
        return sid is not None
    if sid not in held.subruns:
        raise WorkerFailure("sim_bad_config", f"第 {index} 步：sub-run {sid!r} 尚未启动")
    return False


def _record(path: str | None, ctx: TaskContext, index: int, op: str) -> None:
    """执行计数（故障实验用）：config.exec_log 设置时，每步执行前与产生结果前向该文件追加一行 JSON。

    以 O_APPEND 单次写入整行，多个进程同时追加也不会交错；由测试据此判断步骤是否被重复执行。
    """
    if not path:
        return
    line = json.dumps(
        {
            "task_id": ctx.task_id,
            "attempt_id": ctx.attempt_id,
            "attempt_no": ctx.attempt_no,
            "index": index,
            "op": op,
            "pid": os.getpid(),
            "t_ns": time.time_ns(),
        }
    )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, (line + "\n").encode("utf-8"))
    finally:
        os.close(fd)


def _start_index(ctx: TaskContext) -> int:
    if ctx.resume is None or not isinstance(ctx.resume.state, dict):
        return 0
    return int(ctx.resume.state.get("next_index", 0))


async def _progress(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    await ctx.progress(
        op.get("kind", "step_started"), op.get("message", ""), step_id=op.get("step_id")
    )


async def _sleep(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    await asyncio.sleep(op["ms"] / 1000)


async def _checkpoint(ctx: TaskContext, op: Op, index: int, held: Held) -> Paused | None:
    forged = []
    for sid in op.get("forge_completed", []):
        # E41：本地把（已取消的）sub-run 伪造为 completed，result_ref 指向一个已授权的产物
        ref = await _register_subrun_result(ctx, sid, "forged")
        entry = ctx.subruns._entries[sid]
        forged.append((entry, entry.status, entry.result_ref))
        entry.status, entry.result_ref = "completed", ref
    try:
        checkpoint_id = await ctx.checkpoint(
            op["step_id"], state={"next_index": index + 1}, refs=held.refs
        )
    except CheckpointRejected as exc:
        want = op.get("expect_rejected")
        if want is None or want not in str(exc):
            raise
        return None
    finally:
        for entry, status, ref in forged:
            entry.status, entry.result_ref = status, ref
    if op.get("expect_rejected") is not None:
        raise WorkerFailure("sim_expect_mismatch", f"第 {index} 步：checkpoint 未被拒绝")
    return Paused(checkpoint_id) if ctx.should_pause() else None


async def _allocate(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    buffer = bytearray(op["mb"] * 1024 * 1024)
    for offset in range(0, len(buffer), 4096):
        buffer[offset] = 1  # 逐页写入，确保真实占用内存
    held.memory.append(buffer)


async def _fail(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    raise WorkerFailure(op["code"], op.get("message", ""), retryable=bool(op.get("retryable")))


async def _exit(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    sys.stderr.flush()
    if "signal" in op:
        os.kill(os.getpid(), int(op["signal"]))
        await asyncio.sleep(60)  # 信号送达前不继续执行后续步骤
    os._exit(int(op["code"]))


async def _artifact(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    path = ctx.out_dir / op["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(op["content"].encode("utf-8"))
    await ctx.register_artifact(
        op["artifact_id"],
        op["path"],
        media_type=op.get("media_type", "application/octet-stream"),
        visibility=op.get("visibility", "output"),
    )


async def _print(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    print(op["message"])


async def _flood(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    message = "x" * op.get("size", 1000)
    for _ in range(op["count"]):
        await ctx.progress("flood", message, step_id=op.get("step_id"))


async def _gateway_call(
    ctx: TaskContext, op: Op, held: Held, call: Callable[[], GatewayResult]
) -> GatewayResult | None:
    """发出一次 Gateway 调用；op.expect_error 给出时，得到该码的错误返回 None，成功则失败。"""
    want = op.get("expect_error")
    try:
        result = await asyncio.to_thread(call)
    except GatewayError as exc:
        if want is not None and exc.code == want:
            return None
        raise WorkerFailure(exc.code, str(exc)) from exc
    if want is not None:
        raise WorkerFailure("sim_expect_mismatch", f"期望 Gateway 错误 {want}，调用成功")
    _add_ref(held, result.blob_sha256)
    return result


def _add_ref(held: Held, sha: str | None) -> None:
    if sha is not None and sha not in held.refs:
        held.refs.append(sha)


def _gw(ctx: TaskContext, op: Op, held: Held) -> GatewayClient:
    """该步使用的 Gateway：带 subrun 时为该 sub-run 的视图，否则为 root 客户端。"""
    sid = op.get("subrun")
    return ctx.gateway if sid is None else held.subruns[sid].gateway


async def _chat(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    gw = _gw(ctx, op, held)
    await _gateway_call(
        ctx,
        op,
        held,
        lambda: gw.chat(op["step_id"], op["messages"], max_tokens=op.get("max_tokens")),
    )


async def _search(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    gw = _gw(ctx, op, held)
    max_results = op.get("max_results", 5)
    await _gateway_call(
        ctx, op, held, lambda: gw.search(op["step_id"], op["query"], max_results=max_results)
    )


async def _fetch(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    gw = _gw(ctx, op, held)
    await _gateway_call(ctx, op, held, lambda: gw.fetch(op["step_id"], op["url"]))


async def _exec(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    gw = _gw(ctx, op, held)
    inputs = [(sha, path) for sha, path in op.get("inputs", [])]
    result = await _gateway_call(
        ctx,
        op,
        held,
        lambda: gw.exec(op["step_id"], op["code"], inputs=inputs, wall_ms=op.get("wall_ms")),
    )
    if result is None:
        return
    body = result.body if isinstance(result.body, dict) else {}
    for output in body.get("outputs") or []:
        _add_ref(held, output.get("sha256"))
    want = op.get("expect_status")
    if want is not None and body.get("status") != want:
        raise WorkerFailure(
            "sim_exec_status_mismatch",
            f"第 {index} 步：exec 状态 {body.get('status')!r}，期望 {want!r}",
        )


# ---- sub-run（M4 Plan 14 Task 11）----


async def _subrun_start(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    sid = op["subrun_id"]
    try:
        handle = await ctx.subruns.start(
            sid,
            parent_step_id=op.get("parent_step_id", "plan"),
            deadline_ms=int(op.get("deadline_ms", 600_000)),
            budget_cap_micro=op.get("budget_cap_micro"),
        )
    except SubrunRejected as exc:
        if exc.code != "subrun_closed":
            raise
        # 已终态（E40 取消、E44/E45 timed_out、已 completed）：不再执行它的步骤
        held.closed.add(sid)
        return
    held.subruns[sid] = handle


async def _register_subrun_result(ctx: TaskContext, sid: str, summary: str) -> str:
    """登记 sub-run 的结果产物 subruns/<sid>.json（internal），返回其 sha256（result_ref）。"""
    rel = f"subruns/{sid}.json"
    path = ctx.out_dir / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    content = {"summary": summary, "sources": [], "partial": False}
    path.write_bytes(json.dumps(content, ensure_ascii=False).encode("utf-8"))
    ref = await ctx.register_artifact(
        f"subrun-{sid}", rel, media_type="application/json", visibility="internal"
    )
    return ref.sha256


async def _subrun_end(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    sid = op["subrun_id"]
    handle = held.subruns[sid]
    summary = op.get("summary", f"{sid} done")
    try:
        if op.get("status", "succeeded") == "succeeded":
            ref = await _register_subrun_result(ctx, sid, summary)
            await ctx.subruns.complete(handle, summary=summary, result_ref=ref)
        else:
            await ctx.subruns.fail(handle, summary=summary)
    except SubrunCancelled:
        pass  # 宿主已取消：SDK 已发出 subrun_end{cancelled}
    held.closed.add(sid)


async def _subrun_cancel(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    sid = op["subrun_id"]
    await ctx.subruns.cancel(held.subruns[sid], reason=op.get("reason", "orchestrator"))
    held.closed.add(sid)


async def _subrun_ignore_cancel(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    manager = ctx.subruns
    original = manager.handle_control

    def handle_control(msg: dict[str, Any]) -> None:
        if msg.get("type") != "subrun_cancel_requested":
            original(msg)

    manager.handle_control = handle_control  # type: ignore[method-assign]


_HANDLERS: dict[str, Handler] = {
    "progress": _progress,
    "sleep": _sleep,
    "checkpoint": _checkpoint,
    "allocate_mb": _allocate,
    "fail": _fail,
    "exit": _exit,
    "artifact": _artifact,
    "print": _print,
    "flood": _flood,
    "chat": _chat,
    "search": _search,
    "fetch": _fetch,
    "exec": _exec,
    "subrun_start": _subrun_start,
    "subrun_end": _subrun_end,
    "subrun_cancel": _subrun_cancel,
    "subrun_ignore_cancel": _subrun_ignore_cancel,
}
