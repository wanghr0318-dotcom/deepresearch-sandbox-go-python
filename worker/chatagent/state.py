"""每轮的工作状态（task checkpoint）与会话记忆（session_state）。

TurnState 是 checkpoint 的全部内容：编排与子主题的转录、计划、来源、额度与标志；恢复后提示词
只由它确定性地生成。compact 在写 checkpoint 之前作用于内存中的状态（之后的提示词也使用压缩
后的状态），保证 inline state ≤ 200 KiB。SessionMemory 是跨轮的会话记忆（最近的轮次与来源）。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from agentbox_worker.errors import WorkerFailure
from agentbox_worker.tools import TurnFlags
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.sources import Source, SourceStore
from agentbox_worker.tools.sources import _source as source_from_json
from agentbox_worker.tools.text import renumber_citations, truncate_utf8
from agentbox_worker.tools.todo import TodoItem

SCHEMA_VERSION = 1
STATE_LIMIT_BYTES = 200 * 1024
MEMORY_LIMIT_BYTES = 192 * 1024
MEMORY_MAX_TURNS = 20
MEMORY_MAX_SOURCES = 200
MEMORY_EXCERPT_MAX_BYTES = 300
REPLY_MAX_CHARS = 2000

PHASES = ("start", "orchestrating", "subtopic", "reporting", "done")
ROUTES = (None, "answer", "research")
SUB_STATUSES = ("pending", "running", "done", "skipped", "failed")
CLOSING_REASONS = (None, "user_finish", "budget", "rounds")
OMITTED_SOURCE = "[已省略，可用 read_source(n) 重读]"
OMITTED = "[已省略]"
_SOURCE_TOOLS = frozenset({"web_fetch", "read_source"})


def json_size(value: Any) -> int:
    """紧凑 JSON 的 UTF-8 字节数（与 SDK 的 checkpoint / session_state 计量一致）。"""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _check(cond: bool, what: str) -> None:
    if not cond:
        raise ValueError(f"轮次状态不合法：{what}")


def _messages(v: Any, what: str) -> list[dict[str, Any]]:
    _check(isinstance(v, list) and all(isinstance(m, dict) for m in v), what)
    for m in v:
        _check(m.get("role") in ("system", "user", "assistant", "tool"), f"{what}.role")
        _check(isinstance(m.get("content"), str), f"{what}.content")
    return [dict(m) for m in v]


def _int(v: Any, what: str) -> int:
    _check(isinstance(v, int) and not isinstance(v, bool) and v >= 0, what)
    return v


@dataclass
class SubtopicState:
    id: str
    status: str = "pending"  # pending | running | done | skipped | failed
    messages: list[dict[str, Any]] = field(default_factory=list)  # kimi-k2.6 子循环转录
    rounds: int = 0
    summary: str | None = None
    sources: list[int] = field(default_factory=list)
    closing: bool = False  # 已要求"现在写子主题摘要"（之后不再提供工具）
    # ---- 作为 sub-run 执行时（Plan 14；未协商 sub-run 扩展时均为缺省值） ----
    subrun_id: str | None = None  # st<id>
    share: int = 0  # 本 sub-run 的工具份额（软上限；合并的子主题为份额之和）
    spent: int = 0  # 本 sub-run 已用的工具次数（与 tool 消息一起写入，恢复后确定）
    exhausted: bool = False  # 本 sub-run 的工具调用已遇到 task 级额度用尽
    cap: int | None = None  # subrun_start.budget_cap_micro（定义的一部分，恢复时原样重发）
    deadline_ms: int = 0
    base: int = 0  # 启动时全局来源表的最大编号；本 sub-run 的新来源本地编号从 base + 1 起
    found: list[dict[str, Any]] = field(default_factory=list)  # 本 sub-run 的新来源（本地编号）
    merged: bool = False  # found 已并入全局来源表（summary 已改写为全局编号）
    call_ids: dict[str, int] = field(default_factory=dict)  # 本 sub-run 的 call id 计数器
    partial: bool = False  # 摘要为抽取式（费用上限已到）
    incomplete: str | None = None  # 不完整的原因（报告中标注）
    result_ref: str | None = None  # subruns/<id>.json 产物的 sha256
    members: list[str] = field(default_factory=list)  # 费用不足时并入本 sub-run 的其他子主题

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: Any) -> SubtopicState:
        _check(isinstance(d, dict), "子主题不是对象")
        _check(isinstance(d.get("id"), str) and bool(d["id"]), "子主题 id")
        _check(d.get("status") in SUB_STATUSES, "子主题 status")
        summary = d.get("summary")
        _check(summary is None or isinstance(summary, str), "子主题 summary")
        sources = d.get("sources", [])
        _check(isinstance(sources, list), "子主题 sources")
        for key in ("subrun_id", "incomplete", "result_ref"):
            _check(d.get(key) is None or isinstance(d.get(key), str), f"子主题 {key}")
        cap = d.get("cap")
        _check(cap is None or (isinstance(cap, int) and not isinstance(cap, bool)), "子主题 cap")
        found, members, ids = d.get("found", []), d.get("members", []), d.get("call_ids", {})
        _check(isinstance(found, list) and all(isinstance(s, dict) for s in found), "子主题 found")
        _check(isinstance(members, list) and all(isinstance(m, str) for m in members), "members")
        _check(isinstance(ids, dict), "子主题 call_ids")
        return cls(
            id=d["id"],
            status=d["status"],
            messages=_messages(d.get("messages", []), "子主题 messages"),
            rounds=_int(d.get("rounds", 0), "子主题 rounds"),
            summary=summary,
            sources=[_int(n, "子主题 sources[]") for n in sources],
            closing=bool(d.get("closing", False)),
            subrun_id=d.get("subrun_id"),
            share=_int(d.get("share", 0), "子主题 share"),
            spent=_int(d.get("spent", 0), "子主题 spent"),
            exhausted=bool(d.get("exhausted", False)),
            cap=cap,
            deadline_ms=_int(d.get("deadline_ms", 0), "子主题 deadline_ms"),
            base=_int(d.get("base", 0), "子主题 base"),
            found=[asdict(source_from_json(s)) for s in found],
            merged=bool(d.get("merged", False)),
            call_ids={str(k): _int(v, "子主题 call_ids[]") for k, v in ids.items()},
            partial=bool(d.get("partial", False)),
            incomplete=d.get("incomplete"),
            result_ref=d.get("result_ref"),
            members=list(members),
        )

    def found_sources(self) -> list[Source]:
        return [source_from_json(s) for s in self.found]


@dataclass
class TurnState:
    schema_version: int = SCHEMA_VERSION
    phase: str = "start"  # start | orchestrating | subtopic | reporting | done
    route: str | None = None  # answer | research
    messages: list[dict[str, Any]] = field(default_factory=list)  # 编排转录（OpenAI 格式）
    rounds: int = 0
    todo: list[TodoItem] = field(default_factory=list)
    subtopics: dict[str, SubtopicState] = field(default_factory=dict)
    current_subtopic: str | None = None
    sources: SourceStore = field(default_factory=SourceStore)
    budget: TurnBudget = field(default_factory=TurnBudget)
    flags: TurnFlags = field(default_factory=TurnFlags)
    pending_question: dict[str, Any] | None = None  # {"question_id","tool_call_id","questions"}
    stops: int = 0
    finish_requested: bool = False
    partial: bool = False
    notes: list[str] = field(default_factory=list)  # "已达工具额度（30/30）"、"部分研究"等
    refs: list[str] = field(default_factory=list)  # 累积的 Gateway 结果 blob，去重保序
    closing: str | None = None  # 已追加的收尾指令的原因（user_finish | budget | rounds）
    task_id: str = ""  # 写出该状态的 turn（恢复种子来自另一个 task 时与本 task 不同）
    stop_findings: str | None = None  # 停止时的"目前发现"（"stop" checkpoint）
    restore_note: str | None = None  # 恢复说明：下一次编排模型调用之前追加到转录末尾
    answered: list[str] = field(default_factory=list)  # 已回答的 question_id（answer 幂等）
    phase_subs: list[str] | None = None  # 进行中的 sub-run 研究阶段：各组主子主题 id（计划顺序）
    question: str = ""  # 本轮用户的问题（被取代时 carryover 带给下一轮；旧 checkpoint 无此字段）
    carried_question: str = ""  # 本轮经 carryover 接续的原始问题（多次停止后仍是最初的那个）
    # 停止时被放弃的调用已占到的 root call id 序号（放弃时回退之前的计数器）。继续时同一 ID 重发
    # 得到 journal 中的结果；改变下一次请求内容的指令（立即写报告、回答）先前进到这些序号之后
    abandoned_ids: dict[str, int] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "phase": self.phase,
            "route": self.route,
            "messages": self.messages,
            "rounds": self.rounds,
            "todo": [i.to_json() for i in self.todo],
            "subtopics": {k: s.to_json() for k, s in self.subtopics.items()},
            "current_subtopic": self.current_subtopic,
            "sources": self.sources.to_json(),
            "budget": self.budget.to_json(),
            "flags": asdict(self.flags),
            "pending_question": self.pending_question,
            "stops": self.stops,
            "finish_requested": self.finish_requested,
            "partial": self.partial,
            "notes": list(self.notes),
            "refs": list(self.refs),
            "closing": self.closing,
            "task_id": self.task_id,
            "stop_findings": self.stop_findings,
            "restore_note": self.restore_note,
            "answered": list(self.answered),
            "phase_subs": list(self.phase_subs) if self.phase_subs is not None else None,
            "question": self.question,
            "carried_question": self.carried_question,
            "abandoned_ids": dict(self.abandoned_ids),
        }

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TurnState:
        """版本不符或字段不合法 → ValueError。"""
        _check(isinstance(d, dict), "不是对象")
        if d.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"轮次状态版本不符：{d.get('schema_version')!r}")
        _check(d.get("phase") in PHASES, "phase")
        _check(d.get("route") in ROUTES, "route")
        _check(d.get("closing") in CLOSING_REASONS, "closing")
        subs = d.get("subtopics", {})
        _check(isinstance(subs, dict), "subtopics")
        flags = d.get("flags", {})
        _check(
            isinstance(flags, dict) and all(isinstance(v, bool) for v in flags.values()), "flags"
        )
        try:
            turn_flags = TurnFlags(**flags)
        except TypeError as exc:
            raise ValueError(f"轮次状态不合法：flags（{exc}）") from exc
        pq = d.get("pending_question")
        _check(
            pq is None or (isinstance(pq, dict) and isinstance(pq.get("question_id"), str)),
            "pending_question",
        )
        current = d.get("current_subtopic")
        _check(current is None or isinstance(current, str), "current_subtopic")
        notes, refs, todo = d.get("notes", []), d.get("refs", []), d.get("todo", [])
        _check(isinstance(notes, list) and all(isinstance(n, str) for n in notes), "notes")
        _check(isinstance(refs, list) and all(isinstance(r, str) for r in refs), "refs")
        _check(isinstance(todo, list), "todo")
        task_id, answered = d.get("task_id", ""), d.get("answered", [])
        _check(isinstance(task_id, str), "task_id")
        _check(isinstance(answered, list) and all(isinstance(a, str) for a in answered), "answered")
        for key in ("question", "carried_question"):
            _check(isinstance(d.get(key, ""), str), key)
        for key in ("stop_findings", "restore_note"):
            _check(d.get(key) is None or isinstance(d.get(key), str), key)
        abandoned = d.get("abandoned_ids", {})
        _check(
            isinstance(abandoned, dict)
            and all(
                isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool) and v >= 0
                for k, v in abandoned.items()
            ),
            "abandoned_ids",
        )
        phase_subs = d.get("phase_subs")
        _check(
            phase_subs is None
            or (isinstance(phase_subs, list) and all(isinstance(s, str) for s in phase_subs)),
            "phase_subs",
        )
        return cls(
            phase=d["phase"],
            route=d.get("route"),
            messages=_messages(d.get("messages", []), "messages"),
            rounds=_int(d.get("rounds", 0), "rounds"),
            todo=[TodoItem.from_json(i) for i in todo],
            subtopics={k: SubtopicState.from_json(v) for k, v in subs.items()},
            current_subtopic=current,
            sources=SourceStore.from_json(d.get("sources", [])),
            budget=TurnBudget.from_json(d.get("budget", {})),
            flags=turn_flags,
            pending_question=pq,
            stops=_int(d.get("stops", 0), "stops"),
            finish_requested=bool(d.get("finish_requested", False)),
            partial=bool(d.get("partial", False)),
            notes=list(notes),
            refs=list(refs),
            closing=d.get("closing"),
            task_id=task_id,
            stop_findings=d.get("stop_findings"),
            restore_note=d.get("restore_note"),
            answered=list(answered),
            phase_subs=list(phase_subs) if phase_subs is not None else None,
            question=d.get("question", ""),
            carried_question=d.get("carried_question", ""),
            abandoned_ids=dict(abandoned),
        )

    def merge_found(self, sub: SubtopicState) -> None:
        """把 sub-run 的新来源并入全局来源表（同 sha 或同 URL 只登记一次），摘要中的本地编号
        改写为全局编号。各 sub-run 在阶段结束时按计划顺序合并，故编号与调度方式无关。"""
        if sub.merged:
            return
        mapping = {s.n: s.n for s in self.sources.all() if s.n <= sub.base}
        for src in sub.found_sources():
            new, _ = self.sources.add(
                sha256=src.sha256,
                url=src.url,
                title=src.title,
                excerpt=src.excerpt,
                call_id=src.call_id,
            )
            mapping[src.n] = new.n
        if sub.summary:
            sub.summary = renumber_citations(sub.summary, mapping)
        sub.sources = sorted({mapping[s.n] for s in sub.found_sources()})
        sub.found, sub.merged = [], True

    def merged_copy(self) -> TurnState:
        """副本：尚未合并的 sub-run 来源按计划顺序并入（停止卡、停止摘要与 carryover 使用）。"""
        copy = TurnState.from_json(self.to_json())
        order = [i.id for i in copy.todo] + [k for k in copy.subtopics if copy.todo_item(k) is None]
        for sid in order:
            sub = copy.subtopics.get(sid)
            if sub is not None and (sub.found or sub.subrun_id) and not sub.merged:
                copy.merge_found(sub)
        return copy

    def add_refs(self, shas: Any) -> None:
        for sha in shas:
            if sha and sha not in self.refs:
                self.refs.append(sha)

    def todo_item(self, item_id: str) -> TodoItem | None:
        return next((i for i in self.todo if i.id == item_id), None)

    def compact(self, limit: int = STATE_LIMIT_BYTES) -> None:
        """超限时从最早的工具消息起把 content 换成占位文本，直到 JSON ≤ limit。

        顺序（确定性）：已结束子主题的转录 → 编排转录 → 进行中子主题的转录，各自从早到晚；
        来源类（web_fetch、read_source）换成 "[已省略，可用 read_source(n) 重读]"，其余 "[已省略]"。
        仍超限时清空沿用来源（之前轮次）的摘录；仍超限 → WorkerFailure("state_too_large")。
        """
        size = json_size(self.to_json())
        if size <= limit:
            return
        for msg in self._compactable():
            if msg["content"] in (OMITTED_SOURCE, OMITTED):
                continue
            placeholder = OMITTED_SOURCE if msg.get("name") in _SOURCE_TOOLS else OMITTED
            size -= json_size(msg["content"]) - json_size(placeholder)
            msg["content"] = placeholder
            if size <= limit:
                break
        if size > limit:
            for src in self.sources.all():
                if src.origin and src.excerpt:
                    size -= json_size(src.excerpt) - json_size("")
                    src.excerpt = ""
                    if size <= limit:
                        break
        size = json_size(self.to_json())
        if size > limit:
            raise WorkerFailure("state_too_large", f"轮次状态压缩后仍有 {size} 字节，上限 {limit}")

    def _compactable(self) -> list[dict[str, Any]]:
        ended = [s for s in self.subtopics.values() if s.status != "running"]
        running = [s for s in self.subtopics.values() if s.status == "running"]
        groups = [
            [m for s in ended for m in s.messages],
            self.messages,
            [m for s in running for m in s.messages],
        ]
        return [m for g in groups for m in g if m.get("role") == "tool"]


@dataclass
class TurnRecord:
    task_id: str
    user: str
    reply: str  # ≤ 2000 字（报告轮为报告摘要段）
    route: str
    report: dict[str, Any] | None  # {"artifact_id","version","title"}

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: Any) -> TurnRecord:
        if not isinstance(d, dict):
            raise ValueError("会话记忆的轮次不是对象")
        vals = {k: d.get(k) for k in ("task_id", "user", "reply", "route")}
        if not all(isinstance(v, str) for v in vals.values()):
            raise ValueError("会话记忆的轮次字段不合法")
        report = d.get("report")
        if report is not None and not isinstance(report, dict):
            raise ValueError("会话记忆的报告字段不合法")
        return cls(report=report, **vals)


@dataclass
class SessionMemory:
    schema_version: int = SCHEMA_VERSION
    turns: list[TurnRecord] = field(default_factory=list)  # 最近 20 轮
    sources: list[Source] = field(default_factory=list)  # 最近 200 个，origin=<task_id>

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "turns": [t.to_json() for t in self.turns],
            "sources": [asdict(s) for s in self.sources],
        }

    @classmethod
    def from_json(cls, d: Any) -> SessionMemory:
        """None → 空记忆；版本不符或字段不合法 → ValueError。"""
        if d is None:
            return cls()
        if not isinstance(d, dict) or d.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("会话记忆版本不符或不是对象")
        turns, sources = d.get("turns", []), d.get("sources", [])
        if not isinstance(turns, list) or not isinstance(sources, list):
            raise ValueError("会话记忆字段不合法")
        return cls(
            turns=[TurnRecord.from_json(t) for t in turns],
            sources=[source_from_json(s) for s in sources],
        )

    def with_turn(self, record: TurnRecord, sources: list[Source]) -> SessionMemory:
        """返回新对象（不修改已提交状态）：追加本轮记录与来源（origin=本轮 task_id），按上限裁剪；
        to_json 超过 192 KiB 时继续裁剪最旧的轮次（连同其来源），最后裁剪最旧的来源。"""
        rec = TurnRecord(
            record.task_id, record.user, record.reply[:REPLY_MAX_CHARS], record.route, record.report
        )
        new = [
            Source(
                s.n,
                s.sha256,
                s.url,
                s.title,
                truncate_utf8(s.excerpt, MEMORY_EXCERPT_MAX_BYTES),
                s.call_id,
                s.origin or record.task_id,
            )
            for s in sources
        ]
        mem = SessionMemory(
            turns=[*self.turns, rec][-MEMORY_MAX_TURNS:],
            sources=[*self.sources, *new][-MEMORY_MAX_SOURCES:],
        )
        while json_size(mem.to_json()) > MEMORY_LIMIT_BYTES:
            if len(mem.turns) > 1:
                gone = mem.turns.pop(0).task_id
                mem.sources = [s for s in mem.sources if s.origin != gone]
            elif mem.sources:
                mem.sources.pop(0)
            else:
                raise WorkerFailure("state_too_large", "会话记忆超过上限")
        return mem
