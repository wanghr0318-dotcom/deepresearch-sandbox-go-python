"""sim-worker 的步骤执行。配置格式见 Plan 3 Task 7；恢复时从 checkpoint 状态的 next_index 继续。"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from agentbox_worker import Paused, Result, TaskContext, WorkerFailure

Op = dict[str, Any]
Held = list[bytearray]
Handler = Callable[[TaskContext, Op, int, Held], Awaitable["Paused | None"]]


async def run(ctx: TaskContext) -> Result | Paused:
    config = ctx.config if isinstance(ctx.config, dict) else {}
    steps: list[Op] = config.get("steps", [])
    held: Held = []
    for index in range(_start_index(ctx), len(steps)):
        op = steps[index]
        handler = _HANDLERS.get(op.get("op", ""))
        if handler is None:
            raise WorkerFailure("sim_bad_config", f"第 {index} 步：未知操作 {op.get('op')!r}")
        outcome = await handler(ctx, op, index, held)
        if outcome is not None:
            return outcome
    return Result(summary=config.get("summary", "done"), outputs=list(config.get("outputs", [])))


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
    checkpoint_id = await ctx.checkpoint(op["step_id"], state={"next_index": index + 1})
    return Paused(checkpoint_id) if ctx.should_pause() else None


async def _allocate(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    buffer = bytearray(op["mb"] * 1024 * 1024)
    for offset in range(0, len(buffer), 4096):
        buffer[offset] = 1  # 逐页写入，确保真实占用内存
    held.append(buffer)


async def _fail(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    raise WorkerFailure(op["code"], op.get("message", ""), retryable=bool(op.get("retryable")))


async def _exit(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    sys.stderr.flush()
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
}
