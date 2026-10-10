"""Dispatch: coding tasks to the coding harness, everything else to deepresearch."""

from __future__ import annotations

from agentbox_worker import Paused, Result, TaskContext
from deepresearch.app import run as research_run
from evalworker.coding import run_coding


def eval_kind(config: object) -> str | None:
    if isinstance(config, dict) and isinstance(config.get("eval"), dict):
        kind = config["eval"].get("kind")
        return kind if isinstance(kind, str) else None
    return None


async def run(ctx: TaskContext) -> Result | Paused:
    if eval_kind(ctx.config) == "coding":
        return await run_coding(ctx, ctx.config["eval"])
    return await research_run(ctx)
