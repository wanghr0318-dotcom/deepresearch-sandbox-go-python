"""deep-research skill 与 read_skill / ask_user / todo_write 工具（Plan 13 Task 3）。"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
from agentfakes import ScriptedGateway

from agentbox_worker.tools import ToolRegistry
from agentbox_worker.tools.ask_user import AskUser
from agentbox_worker.tools.base import ToolArgsError, ToolContext, TurnFlags
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.skills import ReadSkill, SkillCatalog, default_skills_root
from agentbox_worker.tools.sources import SourceStore
from agentbox_worker.tools.todo import TodoItem, TodoWrite

REPO_SKILLS = Path(__file__).resolve().parents[1] / "skills"


def make_skill(root: Path, name: str, front: str | None = None, body: str = "正文\n") -> Path:
    d = root / name
    (d / "references").mkdir(parents=True)
    if front is None:
        front = f"name: {name}\ndescription: 演示用 skill。"
    (d / "SKILL.md").write_text(f"---\n{front}\n---\n\n{body}", encoding="utf-8")
    (d / "references" / "a.md").write_text("参考 A", encoding="utf-8")
    return d


def ctx(**kw: Any) -> ToolContext:
    return ToolContext(
        gateway=ScriptedGateway(),
        step_id="orch",
        budget=kw.pop("budget", TurnBudget()),
        sources=SourceStore(),
        flags=kw.pop("flags", TurnFlags()),
        skills=kw.pop("skills", SkillCatalog.load(REPO_SKILLS)),
        **kw,
    )


REGISTRY = ToolRegistry([ReadSkill(), AskUser(), TodoWrite()])


def call(name: str, args: Any, c: ToolContext):
    return REGISTRY.dispatch(name, json.dumps(args, ensure_ascii=False), c)


# ---- SkillCatalog ----


def test_repo_skill_catalog_and_front_matter():
    cat = SkillCatalog.load(REPO_SKILLS)
    s = cat.get("deep-research")
    assert s and 0 < len(s.description) <= 200 and "\n" not in s.description
    assert s.root == REPO_SKILLS / "deep-research"
    assert cat.catalog_text().startswith("- deep-research：")
    body = cat.read("deep-research")
    assert not body.startswith("---")
    assert "\r" not in body
    for rule in (
        "一轮",
        "3 个选择题",
        "时间、地区、目的、深度",
        "30 次",
        "todo_write",
        "research_subtopic",
        "[n]",
        "引用自查",
        "现在结束",
    ):
        assert rule in body, rule
    assert len(body.splitlines()) < 300
    assert "## 摘要" in cat.read("deep-research", "references/report-template.md")
    assert cat.read("deep-research", "references/source-quality.md")


def test_catalog_text_sorted_and_unknown_skill(tmp_path: Path):
    make_skill(tmp_path, "zeta")
    make_skill(tmp_path, "alpha")
    (tmp_path / "not-a-skill").mkdir()
    cat = SkillCatalog.load(tmp_path)
    assert cat.catalog_text() == "- alpha：演示用 skill。\n- zeta：演示用 skill。"
    assert cat.get("not-a-skill") is None
    with pytest.raises(ToolArgsError):
        cat.read("missing")


def test_skill_file_paths_are_confined(tmp_path: Path):
    d = make_skill(tmp_path, "demo")
    (tmp_path / "x").write_text("outside", encoding="utf-8")
    cat = SkillCatalog.load(tmp_path)
    assert cat.read("demo", "references/a.md") == "参考 A"
    for bad in (
        "../x",
        "/etc/passwd",
        "references/../../x",
        "references/../SKILL.md",
        str(tmp_path / "x"),
        "C:/Windows/win.ini",
        "references/missing.md",
        "references",
        "",
    ):
        with pytest.raises(ToolArgsError):
            cat.read("demo", bad)
    try:
        os.symlink(tmp_path / "x", d / "references" / "link.md")
        os.symlink(tmp_path, d / "dirlink", target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("平台不支持创建符号链接")
    for bad in ("references/link.md", "dirlink/x"):
        with pytest.raises(ToolArgsError):
            cat.read("demo", bad)


@pytest.mark.parametrize(
    "front",
    [
        "name: demo",  # 缺 description
        "description: 演示",  # 缺 name
        "name: other\ndescription: 演示",  # name 与目录名不符
        "name: demo\ndescription: " + "长" * 201,  # 描述过长
        "name: demo\ndescription:",  # 描述为空
    ],
)
def test_bad_front_matter_is_rejected(tmp_path: Path, front: str):
    make_skill(tmp_path, "demo", front=front)
    with pytest.raises(ValueError):
        SkillCatalog.load(tmp_path)


def test_missing_front_matter_is_rejected(tmp_path: Path):
    d = tmp_path / "demo"
    d.mkdir()
    (d / "SKILL.md").write_text("# 没有 front matter\n", encoding="utf-8")
    with pytest.raises(ValueError):
        SkillCatalog.load(tmp_path)


def test_crlf_skill_file_parses(tmp_path: Path):
    d = tmp_path / "demo"
    d.mkdir()
    (d / "SKILL.md").write_bytes(
        "---\r\nname: demo\r\ndescription: 演示\r\n---\r\n正文\r\n".encode()
    )
    cat = SkillCatalog.load(tmp_path)
    assert cat.get("demo").description == "演示"
    assert cat.read("demo") == "正文\n"


def test_default_skills_root_points_to_repo_skills():
    assert default_skills_root() == REPO_SKILLS


# ---- read_skill ----


def test_read_skill_sets_flag_and_inline_raw():
    c = ctx()
    res = call("read_skill", {"name": "deep-research"}, c)
    assert res.ok and c.flags.skill_read
    assert c.budget.used == 0 and ReadSkill.counts_budget is False
    assert res.content == c.skills.read("deep-research")
    assert res.raw["inline"]["request"] == {"name": "deep-research"}
    assert len(res.raw["inline"]["response"].encode()) <= 16 * 1024
    assert res.data == {
        "name": "deep-research",
        "description": c.skills.get("deep-research").description,
        "file": None,
    }


def test_read_skill_reference_file_does_not_set_flag_and_errors_are_results():
    c = ctx()
    res = call("read_skill", {"name": "deep-research", "file": "references/report-template.md"}, c)
    assert res.ok and not c.flags.skill_read
    assert res.data["file"] == "references/report-template.md"
    for args in ({"name": "nope"}, {"name": "deep-research", "file": "../x"}):
        bad = call("read_skill", args, c)
        assert not bad.ok and bad.content.startswith("工具参数错误")
    assert not call("read_skill", {"name": "deep-research"}, ctx(skills=None)).ok


# ---- ask_user ----


def question(n_options: int = 2, text: str = "关注哪个时间范围？") -> dict[str, Any]:
    return {"question": text, "options": [f"选项{i}" for i in range(n_options)]}


def test_ask_user_rules():
    c = ctx(call_index=3)
    # 未读 skill
    res = call("ask_user", {"questions": [question()]}, c)
    assert not res.ok and res.control is None and "read_skill" in res.content
    c.flags.skill_read = True
    # 题数与选项数越界、选项重复
    for qs in ([question()] * 4, [question(1)], [question(5)], [question(0)], []):
        assert not call("ask_user", {"questions": qs}, c).ok
    dup = {"question": "地区？", "options": ["中国", "中国"]}
    assert not call("ask_user", {"questions": [dup]}, c).ok
    assert not c.flags.asked
    # 合法
    qs = [question(), question(4, "用途？")]
    res = call("ask_user", {"questions": qs}, c)
    assert res.ok and res.control == "await_user" and c.flags.asked
    assert res.data == {"question_id": "q-orch-3", "questions": qs}
    assert res.raw["inline"]["request"] == {"questions": qs}
    assert c.budget.used == 0 and AskUser.counts_budget is False
    # 第二次
    again = call("ask_user", {"questions": qs}, c)
    assert not again.ok and again.control is None
    # 计划之后、研究开始之后
    for flag in ("planned", "researching"):
        flags = TurnFlags(skill_read=True)
        setattr(flags, flag, True)
        res = call("ask_user", {"questions": qs}, ctx(flags=flags))
        assert not res.ok and res.control is None, flag


# ---- todo_write ----


def plan(*budgets: int, status: str = "pending") -> list[dict[str, Any]]:
    return [
        {
            "id": str(i + 1),
            "title": f"子主题 {i + 1}",
            "status": status,
            "budget": b,
            "brief": "简报",
        }
        for i, b in enumerate(budgets)
    ]


def test_todo_write_allocation_rules():
    c = ctx()
    for items in (plan(8), plan(5, 5, 5, 5, 5), plan(8, 8, 15), plan(0, 0, 0)):
        res = call("todo_write", {"items": items}, c)
        assert not res.ok, items
    dup = plan(8, 8)
    dup[1]["id"] = "1"
    assert not call("todo_write", {"items": dup}, c).ok
    assert not call("todo_write", {"items": plan(8, 8) + [{"id": "9"}]}, c).ok
    assert not c.flags.planned and c.budget.shares == {}

    res = call("todo_write", {"items": plan(8, 8, 8)}, c)
    assert res.ok and c.flags.planned and res.control is None
    assert c.budget.shares == {"1": 8, "2": 8, "3": 8}
    assert [i["budget"] for i in res.data["items"]] == [8, 8, 8]
    assert res.preview["kind"] == "text" and "子主题 1" in res.preview["text"]
    assert c.budget.used == 0 and TodoWrite.counts_budget is False

    # 更新状态后再次 todo_write；非研究项（budget 0）可加入
    items = plan(8, 8, 8)
    items[0]["status"] = "done"
    items.append({"id": "4", "title": "写报告", "status": "pending", "budget": 0})
    res = call("todo_write", {"items": items}, c)
    assert res.ok and res.data["items"][0]["status"] == "done"
    assert res.data["items"][3]["brief"] == ""


def test_todo_write_counts_only_unspent_share_against_remaining():
    budget = TurnBudget(used=10, spent={"1": 8, "2": 2})
    c = ctx(budget=budget)
    # 未用份额：0 + 6 + 8 + 6 = 20 ≤ 剩余 20
    assert call("todo_write", {"items": plan(8, 8, 8, 6)}, c).ok
    assert not call("todo_write", {"items": plan(8, 8, 8, 7)}, c).ok


def test_todo_item_json_round_trip():
    item = TodoItem(id="1", title="市场规模", status="pending", budget=6, brief="目标：…")
    assert TodoItem.from_json(item.to_json()) == item
    with pytest.raises(ValueError):
        TodoItem.from_json({"id": "1", "title": "x", "status": "bogus", "budget": 1})
