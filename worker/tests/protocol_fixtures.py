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


def line_bytes(item: dict[str, Any]) -> bytes:
    """消息 fixture 或场景行对应的原始行。"""
    if "raw" in item:
        return item["raw"].encode("utf-8")
    return json.dumps(item["message"], ensure_ascii=False).encode("utf-8")
