"""读取 protocol/fixtures/v1 下的跨语言 fixtures（Go 侧使用同一份文件）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parents[2] / "protocol" / "fixtures" / "v1"


def load_messages() -> dict[str, list[dict[str, Any]]]:
    return json.loads((FIXTURES / "messages.json").read_text(encoding="utf-8"))


def load_scenarios() -> list[dict[str, Any]]:
    paths = sorted((FIXTURES / "scenarios").glob("*.json"))
    return [json.loads(p.read_text(encoding="utf-8")) for p in paths]


def load_session_messages() -> dict[str, list[dict[str, Any]]]:
    return json.loads((FIXTURES / "session_messages.json").read_text(encoding="utf-8"))


def load_session_scenarios() -> list[dict[str, Any]]:
    """读取 scenarios/session_*.jsonl：带 scenario 键的行开始一个新场景（含 description 与
    expect），其后带 from 的行是该场景按时间顺序的消息（格式见 protocol/README.md）。"""
    out: list[dict[str, Any]] = []
    for path in sorted((FIXTURES / "scenarios").glob("session_*.jsonl")):
        for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not raw.strip():
                continue
            rec = json.loads(raw)
            if "scenario" in rec:
                out.append({"name": rec["scenario"], "expect": rec["expect"], "lines": []})
            elif out and "from" in rec:
                out[-1]["lines"].append(rec)
            else:
                raise ValueError(f"{path.name} 第 {number} 行既不是场景头也不属于任何场景")
    return out


def line_bytes(item: dict[str, Any]) -> bytes:
    """消息 fixture 或场景行对应的原始行。"""
    if "raw" in item:
        return item["raw"].encode("utf-8")
    return json.dumps(item["message"], ensure_ascii=False).encode("utf-8")
