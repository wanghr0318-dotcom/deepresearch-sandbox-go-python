"""研究状态的版本与迁移。

checkpoint 中的研究状态是带 schema_version 的 JSON 对象（由 ResearchState.to_json 生成）。
恢复时先 migrate 升级到当前版本，再 ResearchState.from_json；未知版本一律报错，不猜测。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

SCHEMA_VERSION = 1

# 旧版本 → 升级到下一版本的函数；新增版本时在此登记 {旧版本: 升级函数}。
_UPGRADES: dict[int, Callable[[dict[str, Any]], dict[str, Any]]] = {}


def migrate(d: dict) -> dict:
    """把 checkpoint 中的研究状态升级到 SCHEMA_VERSION。

    当前版本原样返回（恒等）；缺少版本、版本不是整数、版本高于当前或没有升级路径时抛 ValueError。
    """
    if not isinstance(d, dict):
        raise ValueError(f"研究状态必须是 JSON 对象，得到 {type(d).__name__}")
    version = d.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise ValueError(f"研究状态的 schema_version 不合法：{version!r}")
    while version != SCHEMA_VERSION:
        upgrade = _UPGRADES.get(version)
        if upgrade is None:
            raise ValueError(f"未知的研究状态版本：{version}（当前 {SCHEMA_VERSION}）")
        d = upgrade(d)
        version = d["schema_version"]
    return d
