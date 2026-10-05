"""DeepResearch 的研究循环：计划 → 逐任务检索、抓取与总结 → 报告，每一步之后提交 checkpoint。

状态即数据：每个 checkpoint 的 state 是 ResearchState.to_json()（内联 JSON 对象，SDK 在其中
保存 call id 计数器）。refs 是累积的：本任务至今经 Gateway 调用得到的全部结果 blob（计划调用、
各证据、各摘要调用、最后的报告调用），按首次出现顺序去重（规格 §5.5 规则 2）。
恢复时从 resume.state 继续：已完成的计划与任务不重做，call id 随计数器续号；累积 refs 由
resume.refs（上一 checkpoint 的累积集合）与状态中的证据 sha 重建，恢复后的下一个 checkpoint
得到与未中断运行相同的集合。

模型路由：config.orchestrator_model 用于计划与报告（编排），config.worker_model 用于任务内的
总结；未配置时请求不带 model，由 Gateway 用服务端默认模型。

失败处理（Plan 8 Task 3 规则 4–6）：
- CallDivergence：以新 ID + X-Agentbox-Supersedes 显式重发一次并记入 failures；再冲突则任务失败。
- CallDeadlineExceeded / 5xx：该任务以新 ID 重做一次，再失败则任务失败，继续其余任务。
- BudgetExhausted：不再发起调用；已有完成的任务时用已有摘要拼接降级报告，否则任务失败。
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from agentbox_worker import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    GatewayClient,
    GatewayError,
    GatewayResult,
    Paused,
    Result,
    TaskContext,
    WorkerFailure,
)
from deepresearch import steps
from deepresearch.models import ResearchState, ResearchTask
from deepresearch.state import migrate

NAME = "deepresearch"
VERSION = "0.1.0"

PLAN_STEP = steps.PLAN_STEP_ID
REPORT_STEP = "report"
REPORT_PATH = "report.md"
REPORT_MEDIA_TYPE = "text/markdown"
SUMMARY_MAX_BYTES = 2048
BUDGET_NOTE = "预算耗尽，部分结果"
REPORT_FAILED_NOTE = "报告生成失败，部分结果"


@dataclass(frozen=True)
class Config:
    topic: str
    max_tasks: int = 4
    max_results: int = 5
    max_fetch: int = 3
    max_loops: int = 1
    report_artifact_id: str = "report"
    # 主/从模型路由：编排（plan、write_report）用 orchestrator_model，任务内的 chat（summarize）用
    # worker_model；None 表示不在请求中写 model（Gateway 用服务端默认模型）。两者都须在服务端声明的
    # 白名单中。config 在每个 attempt 的 init 中不变，恢复后同一步骤重发同一模型（指纹稳定）。
    orchestrator_model: str | None = None
    worker_model: str | None = None


def parse_config(raw: Any) -> Config:
    """校验 init.config；不合法时以不可重试的 invalid_config 失败。"""
    if not isinstance(raw, dict):
        raise WorkerFailure("invalid_config", "config 必须是 JSON 对象")
    topic = raw.get("topic")
    if not isinstance(topic, str) or not topic.strip():
        raise WorkerFailure("invalid_config", "config.topic 必须是非空字符串")
    defaults = Config(topic="")
    ints = {}
    for key in ("max_tasks", "max_results", "max_fetch", "max_loops"):
        value = raw.get(key, getattr(defaults, key))
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
            raise WorkerFailure("invalid_config", f"config.{key} 必须是 1–100 的整数：{value!r}")
        ints[key] = value
    artifact_id = raw.get("report_artifact_id", defaults.report_artifact_id)
    if not isinstance(artifact_id, str) or not artifact_id:
        raise WorkerFailure("invalid_config", "config.report_artifact_id 必须是非空字符串")
    models = {}
    for key in ("orchestrator_model", "worker_model"):
        value = raw.get(key)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise WorkerFailure(
                "invalid_config", f"config.{key} 必须是非空字符串或 null：{value!r}"
            )
        models[key] = value.strip() if value is not None else None
    return Config(topic=topic.strip(), report_artifact_id=artifact_id, **ints, **models)


def task_step(task: ResearchTask) -> str:
    return f"task-{task.id}"


# ---- Gateway 包装：CallDivergence → 显式取代 ----


class _Superseding:
    """与 GatewayClient 同签名的 chat/search/fetch；CallDivergence 时以新 ID 取代旧调用重发一次。

    请求体与 GatewayClient 的构造方式一致（取代调用必须发送同一请求）。第二次冲突原样上抛。
    """

    def __init__(self, gw: GatewayClient, on_divergence: Callable[[str], None]) -> None:
        self._gw = gw
        self._on_divergence = on_divergence

    def chat(
        self,
        step_id: str,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> GatewayResult:
        body = GatewayClient.chat_body(
            messages, model=model, max_tokens=max_tokens, temperature=temperature
        )
        return self._guard(
            step_id,
            "chat",
            body,
            lambda: self._gw.chat(
                step_id, messages, model=model, max_tokens=max_tokens, temperature=temperature
            ),
        )

    def search(self, step_id: str, query: str, *, max_results: int = 5) -> GatewayResult:
        body = {"query": query, "max_results": max_results}
        return self._guard(
            step_id,
            "search",
            body,
            lambda: self._gw.search(step_id, query, max_results=max_results),
        )

    def fetch(self, step_id: str, url: str) -> GatewayResult:
        return self._guard(step_id, "fetch", {"url": url}, lambda: self._gw.fetch(step_id, url))

    def _guard(
        self, step_id: str, kind: str, body: dict, call: Callable[[], GatewayResult]
    ) -> GatewayResult:
        try:
            return call()
        except CallDivergence as exc:
            # 冲突的调用就是该 (step, kind) 刚分配的最后一个序号
            prefix = f"root/{step_id}/{kind}"
            old = f"{prefix}/{self._gw.call_ids.snapshot()[prefix]}"
            self._on_divergence(f"{old}: 指纹冲突（{exc.code}），以新 ID 取代重发")
            return self._gw.supersede(step_id, kind, old, "divergence", body)


class _ReplayChat:
    """把已保存的 chat 结果 blob 当作 chat 响应返回：恢复到 report 之后重建报告，不再调用模型。"""

    def __init__(self, result: GatewayResult) -> None:
        self._result = result

    def chat(self, step_id: str, messages: list[dict[str, str]], **_: Any) -> GatewayResult:
        return self._result


def _transient(exc: GatewayError) -> bool:
    """可在任务内以新 ID 重做一次的错误：调用超时、Gateway 5xx 与连接中断。"""
    if isinstance(exc, CallDeadlineExceeded):
        return True
    return exc.status >= 500 or (exc.status == 0 and exc.code == "connection_lost")


def _blobs(calls: list[GatewayResult]) -> list[str]:
    return [c.blob_sha256 for c in calls if c.blob_sha256 is not None]


def _dedupe(items: list[str]) -> list[str]:
    return list(dict.fromkeys(items))


# ---- 研究循环 ----


class _Loop:
    def __init__(self, ctx: TaskContext, cfg: Config) -> None:
        self.ctx = ctx
        self.cfg = cfg
        self.state = ResearchState(topic=cfg.topic)
        self.refs: list[str] = []  # 累积 refs：本任务至今得到的全部结果 blob，首次出现顺序
        self._gw: _Superseding | None = None

    @property
    def gw(self) -> _Superseding:
        if self._gw is None:
            self._gw = _Superseding(self.ctx.gateway, self.state.failures.append)
        return self._gw

    async def commit(self, step_id: str, new_refs: list[str]) -> str:
        """提交 checkpoint：refs = 已累积的 blob + 本步骤新得到的 blob（去重，首次出现顺序）。"""
        self.refs = _dedupe(self.refs + new_refs)
        return await self.ctx.checkpoint(step_id, state=self.state.to_json(), refs=self.refs)

    async def checkpoint(self, step_id: str, new_refs: list[str]) -> Paused | None:
        checkpoint_id = await self.commit(step_id, new_refs)
        if self.ctx.cancel_reason is not None:
            raise WorkerFailure("cancelled", f"任务已取消：{self.ctx.cancel_reason}")
        return Paused(checkpoint_id) if self.ctx.should_pause() else None

    async def run(self) -> Result | Paused:
        resume = self.ctx.resume
        if resume is not None:
            self.state = _restore(resume.state)
            restored = list(resume.refs) + list(self.state.evidence)
            if self.state.report_sha256 is not None:
                restored.append(self.state.report_sha256)
            self.refs = _dedupe(restored)
            if resume.step_id == REPORT_STEP:
                return await self.rebuild_report()
        else:
            paused = await self.plan()
            if paused is not None:
                return paused
        for task in self.state.tasks:
            if task.status != "pending":
                continue
            try:
                refs = await self.research(task)
            except BudgetExhausted as exc:
                return await self.degraded(f"{task_step(task)}: {exc}", BUDGET_NOTE)
            paused = await self.checkpoint(task_step(task), refs)
            if paused is not None:
                return paused
        return await self.report()

    async def plan(self) -> Paused | None:
        calls: list[GatewayResult] = []
        for attempt in (1, 2):
            try:
                tasks, _ = await asyncio.to_thread(
                    steps.plan,
                    self.gw,
                    self.cfg.topic,
                    max_tasks=self.cfg.max_tasks,
                    model=self.cfg.orchestrator_model,
                    calls=calls,
                )
                break
            except BudgetExhausted as exc:
                raise WorkerFailure("budget_exhausted", f"计划阶段预算耗尽：{exc}") from exc
            except CallDivergence as exc:
                raise WorkerFailure("call_divergence", f"计划调用取代后仍冲突：{exc}") from exc
            except GatewayError as exc:
                if attempt == 2 or not _transient(exc):
                    raise WorkerFailure(
                        "plan_failed", f"计划调用失败：{exc}", retryable=_transient(exc)
                    ) from exc
                self.state.failures.append(f"{PLAN_STEP}: {exc}，以新 ID 重试")
        if steps.is_fallback(tasks):
            self.state.failures.append(f"{PLAN_STEP}: 模型输出无法解析为任务，回退为单任务")
        self.state.tasks = tasks
        return await self.checkpoint(PLAN_STEP, _blobs(calls))

    async def research(self, task: ResearchTask) -> list[str]:
        """检索、抓取并总结一个任务；返回新得到的 blob（证据 sha + 摘要调用）。预算耗尽上抛。"""
        step = task_step(task)
        for attempt in (1, 2):
            summary_calls: list[GatewayResult] = []
            try:
                evidence = await asyncio.to_thread(
                    steps.search_and_fetch,
                    self.gw,
                    task,
                    max_results=self.cfg.max_results,
                    max_fetch=self.cfg.max_fetch,
                    step_id=step,
                )
                summary = await asyncio.to_thread(
                    steps.summarize,
                    self.gw,
                    task,
                    evidence,
                    step_id=step,
                    model=self.cfg.worker_model,
                    calls=summary_calls,
                )
            except (BudgetExhausted, AccessRevoked):
                raise
            except CallDivergence as exc:
                self.state.failures.append(f"{step}: 取代后仍指纹冲突，任务失败：{exc}")
                task.status = "failed"
                return []
            except GatewayError as exc:
                if attempt == 2 or not _transient(exc):
                    self.state.failures.append(f"{step}: {exc}，任务失败")
                    task.status = "failed"
                    return []
                self.state.failures.append(f"{step}: {exc}，以新 ID 重做")
                continue
            # 顺序与传给 summarize 的证据一致：摘要中的 [n] 即 task.evidence[n-1]
            task.evidence = [e.sha256 for e in evidence]
            task.summary = summary
            task.status = "done"
            for e in evidence:
                self.state.evidence.setdefault(e.sha256, e)
            return task.evidence + _blobs(summary_calls)
        raise AssertionError("unreachable")

    async def report(self) -> Result:
        if not any(t.status == "done" for t in self.state.tasks):
            raise WorkerFailure("research_failed", "没有完成的研究任务", retryable=True)
        calls: list[GatewayResult] = []
        for attempt in (1, 2):
            try:
                text = await asyncio.to_thread(
                    steps.write_report,
                    self.gw,
                    self.state,
                    step_id=REPORT_STEP,
                    model=self.cfg.orchestrator_model,
                    calls=calls,
                )
                break
            except BudgetExhausted as exc:
                return await self.degraded(f"{REPORT_STEP}: {exc}", BUDGET_NOTE)
            except AccessRevoked:
                raise
            except GatewayError as exc:
                if attempt == 2 or not _transient(exc):
                    return await self.degraded(f"{REPORT_STEP}: {exc}", REPORT_FAILED_NOTE)
                self.state.failures.append(f"{REPORT_STEP}: {exc}，以新 ID 重试")
        refs = _blobs(calls)
        self.state.report_sha256 = refs[-1] if refs else None
        await self.publish(text)
        await self.commit(REPORT_STEP, refs)
        return Result(summary=first_paragraph(text), outputs=[self.cfg.report_artifact_id])

    async def degraded(self, failure: str, note: str) -> Result:
        """不再调用模型：用已完成任务的摘要拼接报告；没有完成的任务时失败。"""
        self.state.failures.append(failure)
        for task in self.state.tasks:
            if task.status == "pending":
                task.status = "skipped"
        if not any(t.status == "done" for t in self.state.tasks):
            raise WorkerFailure("budget_exhausted", failure, retryable=False)
        self.state.report_sha256 = None
        text = template_report(self.state, note)
        await self.publish(text)
        await self.commit(REPORT_STEP, [])
        return Result(
            summary=f"{note}：{first_paragraph(text)}", outputs=[self.cfg.report_artifact_id]
        )

    async def rebuild_report(self) -> Result:
        """report checkpoint 已提交但结果未送达：由已保存的报告调用结果重建同一份报告并重新登记。"""
        if self.state.report_sha256 is None:
            note = _degraded_note(self.state)
            text = template_report(self.state, note)
            summary = f"{note}：{first_paragraph(text)}"
        else:
            sha = self.state.report_sha256
            try:
                data = await asyncio.to_thread(self.ctx.gateway.read_blob, sha)
                body = json.loads(data)
            except (GatewayError, ValueError) as exc:
                raise WorkerFailure("report_unavailable", f"读取报告 blob 失败：{exc}") from exc
            replay = _ReplayChat(GatewayResult("", body, sha, True, 200))
            text = steps.write_report(replay, self.state, step_id=REPORT_STEP)
            summary = first_paragraph(text)
        await self.publish(text)
        return Result(summary=summary, outputs=[self.cfg.report_artifact_id])

    async def publish(self, text: str) -> None:
        (self.ctx.out_dir / REPORT_PATH).write_text(text, encoding="utf-8", newline="\n")
        await self.ctx.register_artifact(
            self.cfg.report_artifact_id, REPORT_PATH, media_type=REPORT_MEDIA_TYPE
        )


def _restore(raw: Any) -> ResearchState:
    try:
        return ResearchState.from_json(migrate(raw))
    except ValueError as exc:
        raise WorkerFailure("invalid_state", f"checkpoint 中的研究状态不合法：{exc}") from exc


def _degraded_note(state: ResearchState) -> str:
    budget = any("budget" in f for f in state.failures)
    return BUDGET_NOTE if budget else REPORT_FAILED_NOTE


# ---- 报告文本 ----


def template_report(state: ResearchState, note: str) -> str:
    """降级报告：各完成任务的摘要（局部 [n] 改为全局编号）与证据列表，不调用模型。"""
    numbers = steps._global_numbers(state)
    lines = [f"# {state.topic}" if state.topic else "# 研究报告", ""]
    lines += [f"{note}：以下为已完成任务的摘要拼接，未经模型综合。", ""]
    for task in state.tasks:
        lines.append(f"## 任务 {task.id}：{task.title}")
        lines.append("")
        if task.status == "done":
            local = {i: numbers[s] for i, s in enumerate(task.evidence, start=1) if s in numbers}
            lines.append(steps._renumber(task.summary or steps.NO_INFO, local).strip())
        else:
            lines.append(f"未完成（{task.status}）。")
        lines.append("")
    lines += ["## 证据", ""]
    ordered = sorted(numbers, key=numbers.__getitem__)
    for sha in ordered:
        ev = state.evidence[sha]
        lines.append(f"- [{numbers[sha]}] {ev.title} — {ev.url} — sha256:{sha}")
    if not ordered:
        lines.append("暂无证据。")
    return "\n".join(lines) + "\n"


_HEADING = re.compile(r"^\s*#{1,6}\s")


def first_paragraph(markdown: str) -> str:
    """报告正文的首段：第一个含非标题行的段落（去掉其中的标题行），按 UTF-8 截断。"""
    for block in re.split(r"\n\s*\n", markdown):
        text = "\n".join(
            line for line in block.splitlines() if line.strip() and not _HEADING.match(line)
        ).strip()
        if text:
            return steps.truncate_utf8(text, SUMMARY_MAX_BYTES)
    return "研究报告已生成。"


async def run(ctx: TaskContext) -> Result | Paused:
    try:
        return await _Loop(ctx, parse_config(ctx.config)).run()
    except GatewayError as exc:  # 访问被撤销等无法在任务内处理的 Gateway 错误
        raise WorkerFailure(exc.code, str(exc), retryable=True) from exc
