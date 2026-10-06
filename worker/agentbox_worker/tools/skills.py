"""Skill 目录与 read_skill 工具（设计 D7、§4.2）。

每个含 SKILL.md 的子目录是一个 skill；SKILL.md 的 front matter（name、description）进入系统提示的
skill 目录，正文与 references/ 下的文件只在 read_skill 时按需加载（渐进披露）。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import agentbox_worker
from agentbox_worker.tools.base import ToolArgsError, ToolContext, ToolResult
from agentbox_worker.tools.text import truncate_utf8

SKILL_FILE = "SKILL.md"
DESCRIPTION_MAX_CHARS = 200
INLINE_RAW_MAX_BYTES = 16 * 1024
PREVIEW_MAX_CHARS = 600
RESEARCH_SKILL = "deep-research"


@dataclass(frozen=True)
class Skill:
    name: str
    description: str  # ≤ 200 字，单行
    root: Path  # skill 目录


def default_skills_root() -> Path:
    """仓库内为 worker/skills；安装后为 /opt/agentbox/skills（与包目录同级）。"""
    return Path(agentbox_worker.__file__).resolve().parent.parent / "skills"


class SkillCatalog:
    def __init__(self, skills: dict[str, Skill]) -> None:
        self._skills = dict(sorted(skills.items()))

    @classmethod
    def load(cls, root: Path) -> SkillCatalog:
        """读取 root 下每个含 SKILL.md 的子目录；front matter 不合法时 ValueError。"""
        if not root.is_dir():
            raise ValueError(f"skill 根目录不存在：{root}")
        skills: dict[str, Skill] = {}
        for d in sorted(root.iterdir()):
            if d.is_dir() and (d / SKILL_FILE).is_file():
                front, _ = _split(_read_text(d / SKILL_FILE), d.name)
                skills[d.name] = _skill(front, d)
        return cls(skills)

    def get(self, name: str) -> Skill | None:
        return self._skills.get(name)

    def names(self) -> list[str]:
        return list(self._skills)

    def catalog_text(self) -> str:
        """系统提示中的 skill 目录：每行 "- <name>：<description>"，按名称排序。"""
        return "\n".join(f"- {s.name}：{s.description}" for s in self._skills.values())

    def read(self, name: str, file: str | None = None) -> str:
        """file=None → SKILL.md 去掉 front matter 的正文；否则为 skill 目录内的相对路径文件。

        未知 skill、绝对路径、含 ".."、经过符号链接或不是普通文件 → ToolArgsError。
        """
        skill = self._skills.get(name)
        if skill is None:
            known = "、".join(self._skills) or "无"
            raise ToolArgsError(f"没有名为 {name!r} 的 skill（可用：{known}）")
        if file is None:
            _, body = _split(_read_text(skill.root / SKILL_FILE), name)
            return body
        return _read_text(_confined(skill.root, file))


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def _split(text: str, where: str) -> tuple[dict[str, str], str]:
    """拆出 front matter（"---" 行包围的 key: value 行）与正文。"""
    lines = text.split("\n")
    if not lines or lines[0].strip() != "---":
        raise ValueError(f"skill {where} 的 {SKILL_FILE} 缺少 front matter")
    try:
        end = next(i for i in range(1, len(lines)) if lines[i].strip() == "---")
    except StopIteration:
        raise ValueError(f"skill {where} 的 front matter 未闭合") from None
    front: dict[str, str] = {}
    for line in lines[1:end]:
        if not line.strip():
            continue
        key, sep, value = line.partition(":")
        if not sep:
            raise ValueError(f"skill {where} 的 front matter 行不合法：{line!r}")
        front[key.strip()] = value.strip()
    return front, "\n".join(lines[end + 1 :]).lstrip("\n")


def _skill(front: dict[str, str], root: Path) -> Skill:
    name, desc = front.get("name", ""), front.get("description", "")
    if not name or not desc:
        raise ValueError(f"skill {root.name} 的 front matter 须有 name 与 description")
    if name != root.name:
        raise ValueError(f"skill 名称 {name!r} 与目录名 {root.name!r} 不符")
    if len(desc) > DESCRIPTION_MAX_CHARS:
        raise ValueError(f"skill {name} 的 description 超过 {DESCRIPTION_MAX_CHARS} 字")
    return Skill(name=name, description=desc, root=root)


def _confined(root: Path, file: str) -> Path:
    """把 file 解析为 root 内的普通文件；路径的每一级都不得是符号链接。"""
    posix, win = PurePosixPath(file), PureWindowsPath(file)
    if not file or posix.is_absolute() or win.is_absolute() or win.drive or win.root:
        raise ToolArgsError(f"文件须为 skill 目录内的相对路径：{file!r}")
    parts = win.parts  # Windows 解析同时按 "/" 与 "\\" 切分
    if any(p in ("..", ".") for p in parts):
        raise ToolArgsError(f"文件路径不得含 '..' 或 '.'：{file!r}")
    path = root
    for part in parts:
        path = path / part
        if path.is_symlink():
            raise ToolArgsError(f"文件路径不得经过符号链接：{file!r}")
    base = root.resolve()
    real = path.resolve()
    if real != base and base not in real.parents:
        raise ToolArgsError(f"文件不在 skill 目录内：{file!r}")
    if not real.is_file():
        raise ToolArgsError(f"skill 中没有文件 {file!r}")
    return real


class ReadSkill:
    """按需加载 skill 正文或其参考文件。不计额度。"""

    name = "read_skill"
    description = (
        "读取 skill 的说明：只给 name 时返回 SKILL.md 正文（流程与规则）；给出 file（如 "
        '"references/report-template.md"）时返回该参考文件。不访问网络，不消耗工具额度。'
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "minLength": 1,
                "description": "skill 名称，如 deep-research",
            },
            "file": {
                "type": "string",
                "minLength": 1,
                "description": "skill 目录内的相对路径；省略则读 SKILL.md",
            },
        },
        "required": ["name"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        if ctx.skills is None:
            raise ToolArgsError("本轮没有可用的 skill")
        name, file = args["name"], args.get("file")
        text = ctx.skills.read(name, file)
        skill = ctx.skills.get(name)
        assert skill is not None  # read 已校验
        if name == RESEARCH_SKILL and file is None:
            ctx.flags.skill_read = True
        return ToolResult(
            content=text,
            preview={"kind": "text", "text": text[:PREVIEW_MAX_CHARS]},
            raw={
                "inline": {
                    "request": dict(args),
                    "response": truncate_utf8(text, INLINE_RAW_MAX_BYTES),
                }
            },
            data={"name": name, "description": skill.description, "file": file},
        )
