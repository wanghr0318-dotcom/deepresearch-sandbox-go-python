"""引用核对、报告文本与产物登记。

citation_pass 机械地保证报告中的 [n] 都指向本轮来源表中的来源（编造或不存在的编号被删除），
并去掉模型自写的证据/参考来源节；render_report 附上系统生成的证据列表（每项带结果 blob 的
sha256，可经 ⟨/⟩ 核对原文）。
"""

from __future__ import annotations

import re

from agentbox_worker.runtime import ArtifactRef, TaskContext
from agentbox_worker.tools.sources import Source, SourceStore
from agentbox_worker.tools.text import renumber_citations, strip_thinking

REPORT_ARTIFACT = "report"
REPORT_PATH = "report.md"
REPORT_MEDIA_TYPE = "text/markdown"
TITLE_MAX_CHARS = 80

_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_EVIDENCE_TITLES = re.compile(
    r"^(证据|证据列表|参考来源|参考文献|参考资料|来源|来源列表|引用|references|sources|bibliography)$",
    re.IGNORECASE,
)
_CITE = re.compile(r"\[(\d+)\]")


def _drop_evidence_sections(text: str) -> str:
    out: list[str] = []
    skip_level: int | None = None
    for line in text.split("\n"):
        m = _HEADING.match(line)
        if m is not None:
            level = len(m.group(1))
            if skip_level is not None and level <= skip_level:
                skip_level = None
            title = m.group(2).strip().strip("*").strip()
            if skip_level is None and _EVIDENCE_TITLES.match(title):
                skip_level = level
                continue
        if skip_level is None:
            out.append(line)
    return "\n".join(out)


def citable(src: Source) -> bool:
    """可作为证据引用的来源：有摘录（正文或搜索摘要）。本轮摘录为空的来源（旧版本登记的 PDF 等
    无正文抓取）不可引用；沿用来源（之前轮次）的摘录可能被状态压缩清空，仍可引用。"""
    return bool(src.excerpt) or bool(src.origin)


def citation_pass(text: str, sources: SourceStore) -> tuple[str, list[int]]:
    """去掉思考段；删除模型自写的"证据/参考来源"节；不在 sources 中或不可引用（citable）的
    [n] 删除。

    返回正文与按首次出现排序的已引用编号。"""
    body = _drop_evidence_sections(strip_thinking(text))
    known = {s.n: s.n for s in sources.all() if citable(s)}
    body = renumber_citations(body, known)
    cited: list[int] = []
    for m in _CITE.finditer(body):
        n = int(m.group(1))
        if n not in cited:
            cited.append(n)
    return body.strip(), cited


def split_title(body: str, default: str) -> tuple[str, str]:
    """正文以 "# 标题" 开头时取出标题；否则用 default。"""
    lines = body.split("\n")
    if lines and (m := re.match(r"^#\s+(.+?)\s*$", lines[0])):
        return _one_line(m.group(1)), "\n".join(lines[1:]).strip()
    return _one_line(default), body.strip()


def _one_line(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= TITLE_MAX_CHARS else text[: TITLE_MAX_CHARS - 1] + "…"


def render_report(
    body: str, title: str, sources: SourceStore, cited: list[int], notes: list[str]
) -> str:
    """标题；notes 非空时在标题下加引用块；正文；"## 证据" 列表
    "- [n] 标题 — URL — sha256:<sha>"（被引用的在前按引用顺序，其后为本轮其余已阅读来源）。"""
    parts = [f"# {title}"]
    if notes:
        parts.append("\n".join(f"> {n}" for n in notes))
    parts.append(body.strip())
    listed = [s for n in cited if (s := sources.get(n)) is not None]
    listed += [s for s in sources.all() if not s.origin and s.n not in cited and citable(s)]
    if listed:
        lines = [
            f"- [{s.n}] {_one_line(s.title) or s.url} — {s.url} — sha256:{s.sha256}" for s in listed
        ]
        parts.append("## 证据\n\n" + "\n".join(lines))
    return "\n\n".join(p for p in parts if p) + "\n"


async def publish_report(ctx: TaskContext, markdown: str) -> ArtifactRef:
    """写 out_dir/report.md 并登记产物 "report"（text/markdown）。"""
    ctx.out_dir.mkdir(parents=True, exist_ok=True)
    (ctx.out_dir / REPORT_PATH).write_text(markdown, encoding="utf-8", newline="\n")
    return await ctx.register_artifact(REPORT_ARTIFACT, REPORT_PATH, media_type=REPORT_MEDIA_TYPE)


def report_summary(markdown: str, limit: int = 600) -> str:
    """报告的摘要节（标题含"摘要"或"结论"）或正文首段，去掉引用块，≤ limit 字。"""
    lines = markdown.split("\n")
    section: list[str] | None = None
    for i, line in enumerate(lines):
        m = _HEADING.match(line)
        if m and len(m.group(1)) >= 2 and re.search(r"摘要|结论|summary", m.group(2), re.I):
            level = len(m.group(1))
            section = []
            for nxt in lines[i + 1 :]:
                h = _HEADING.match(nxt)
                if h and len(h.group(1)) <= level:
                    break
                section.append(nxt)
            break
    if section is not None and (text := "\n".join(section).strip()):
        return _limit(text, limit)
    for para in "\n".join(lines).split("\n\n"):
        para = para.strip()
        if para and not para.startswith(("#", ">")):
            return _limit(para, limit)
    return ""


def _limit(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"
