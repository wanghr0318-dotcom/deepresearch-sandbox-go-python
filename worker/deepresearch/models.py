"""研究流程的数据模型（移植自 helloagents 的 TodoItem/SummaryState，去掉笔记与流式字段）。

状态即数据：ResearchState 只保存任务、摘要与证据引用（blob sha256 及来源元数据），不保存
网页正文；正文在 Gateway 的 blob 中，checkpoint 以 refs 引用。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from deepresearch.state import SCHEMA_VERSION

TASK_STATUSES = ("pending", "done", "skipped", "failed")


@dataclass
class ResearchTask:
    id: int
    title: str
    intent: str
    query: str
    status: str = "pending"
    summary: str | None = None
    # 证据 sha256 列表；顺序即该任务摘要中局部引用 [n] 的编号（第 n 条为 evidence[n-1]）
    evidence: list[str] = field(default_factory=list)


@dataclass
class Evidence:
    sha256: str  # fetch 结果 blob（X-Agentbox-Blob）
    url: str
    title: str
    snippet: str  # 来自搜索结果片段与抓取正文摘录，≤ 4 KiB（UTF-8）
    call_id: str  # 产生该 blob 的 fetch 调用


@dataclass
class ResearchState:
    schema_version: int = SCHEMA_VERSION
    topic: str = ""
    tasks: list[ResearchTask] = field(default_factory=list)
    evidence: dict[str, Evidence] = field(default_factory=dict)  # sha256 → 证据，保持插入顺序
    loop_count: int = 0
    report_sha256: str | None = None
    failures: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "topic": self.topic,
            "tasks": [asdict(t) for t in self.tasks],
            "evidence": {sha: asdict(e) for sha, e in self.evidence.items()},
            "loop_count": self.loop_count,
            "report_sha256": self.report_sha256,
            "failures": list(self.failures),
        }

    @classmethod
    def from_json(cls, d: dict) -> ResearchState:
        """从当前版本的 JSON 构造状态（旧版本先经 migrate）；字段缺失或类型不符抛 ValueError。"""
        if not isinstance(d, dict):
            raise ValueError("研究状态必须是 JSON 对象")
        if d.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"研究状态版本 {d.get('schema_version')!r} 需先 migrate")
        tasks = [_task(t) for t in _typed(d, "tasks", list)]
        evidence: dict[str, Evidence] = {}
        for sha, raw in _typed(d, "evidence", dict).items():
            ev = _evidence(raw)
            if ev.sha256 != sha:
                raise ValueError(f"证据键 {sha!r} 与其 sha256 {ev.sha256!r} 不一致")
            evidence[sha] = ev
        report = d.get("report_sha256")
        if report is not None and not isinstance(report, str):
            raise ValueError("report_sha256 必须是字符串或 null")
        failures = _typed(d, "failures", list)
        if not all(isinstance(f, str) for f in failures):
            raise ValueError("failures 必须是字符串列表")
        return cls(
            schema_version=SCHEMA_VERSION,
            topic=_typed(d, "topic", str),
            tasks=tasks,
            evidence=evidence,
            loop_count=_int(d, "loop_count"),
            report_sha256=report,
            failures=list(failures),
        )


def _typed(d: dict, key: str, kind: type) -> Any:
    value = d.get(key)
    if not isinstance(value, kind):
        raise ValueError(f"字段 {key} 必须是 {kind.__name__}，得到 {type(value).__name__}")
    return value


def _int(d: dict, key: str) -> int:
    value = d.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"字段 {key} 必须是非负整数，得到 {value!r}")
    return value


def _task(raw: Any) -> ResearchTask:
    if not isinstance(raw, dict):
        raise ValueError("任务必须是 JSON 对象")
    status = _typed(raw, "status", str)
    if status not in TASK_STATUSES:
        raise ValueError(f"未知的任务状态：{status!r}")
    summary = raw.get("summary")
    if summary is not None and not isinstance(summary, str):
        raise ValueError("summary 必须是字符串或 null")
    evidence = _typed(raw, "evidence", list)
    if not all(isinstance(s, str) for s in evidence):
        raise ValueError("任务 evidence 必须是 sha256 字符串列表")
    return ResearchTask(
        id=_int(raw, "id"),
        title=_typed(raw, "title", str),
        intent=_typed(raw, "intent", str),
        query=_typed(raw, "query", str),
        status=status,
        summary=summary,
        evidence=list(evidence),
    )


def _evidence(raw: Any) -> Evidence:
    if not isinstance(raw, dict):
        raise ValueError("证据必须是 JSON 对象")
    return Evidence(
        **{k: _typed(raw, k, str) for k in ("sha256", "url", "title", "snippet", "call_id")}
    )
