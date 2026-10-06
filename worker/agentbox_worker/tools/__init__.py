"""Agent 工具（设计 D9）：每个工具一个模块、统一接口（tools.base.Tool），注册表生成函数调用 schema。

validate_args 实现工具参数所需的 JSON Schema 子集；ToolRegistry.dispatch 解析并校验模型给出的
参数，参数错误作为 ok=False 的结果回给模型（不计额度），其余异常上抛给 Agent 循环。
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import Any

from agentbox_worker.tools.base import (
    GatewayLike,
    Tool,
    ToolArgsError,
    ToolContext,
    ToolResult,
    TurnFlags,
)
from agentbox_worker.tools.run_python import RunPython

__all__ = [
    "GatewayLike",
    "RunPython",
    "Tool",
    "ToolArgsError",
    "ToolContext",
    "ToolRegistry",
    "ToolResult",
    "TurnFlags",
    "validate_args",
]

_TYPES: dict[str, tuple[type, ...]] = {
    "object": (dict,),
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "array": (list,),
}


def validate_args(schema: dict[str, Any], args: Any) -> None:
    """按 JSON Schema 子集校验 args，不合法时抛 ToolArgsError。

    支持 type（object/string/integer/number/boolean/array）、required、properties、
    additionalProperties=false、enum、minimum/maximum、minLength/maxLength、minItems/maxItems、items、
    pattern（按 JSON Schema 语义搜索匹配；末尾 $ 视为字符串结尾，不放过结尾换行）。
    """
    _check(schema, args, "参数")


def _pattern_ok(pattern: str, value: str) -> bool:
    if pattern.endswith("$") and not pattern.endswith("\\$"):
        pattern = pattern[:-1] + r"\Z"
    return re.search(pattern, value) is not None


def _check(schema: dict[str, Any], value: Any, path: str) -> None:
    kind = schema.get("type")
    if kind is not None:
        _check_type(kind, value, path)
    if "enum" in schema and value not in schema["enum"]:
        raise ToolArgsError(f"{path} 须为 {schema['enum']} 之一")
    if isinstance(value, int | float) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise ToolArgsError(f"{path} 须 ≥ {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            raise ToolArgsError(f"{path} 须 ≤ {schema['maximum']}")
    if isinstance(value, str):
        _check_len(schema, len(value), path, "minLength", "maxLength", "字符")
        if "pattern" in schema and not _pattern_ok(schema["pattern"], value):
            raise ToolArgsError(f"{path} 须匹配 {schema['pattern']}")
    if isinstance(value, list):
        _check_len(schema, len(value), path, "minItems", "maxItems", "项")
        if isinstance(items := schema.get("items"), dict):
            for i, item in enumerate(value):
                _check(items, item, f"{path}[{i}]")
    if isinstance(value, dict):
        _check_object(schema, value, path)


def _check_type(kind: str, value: Any, path: str) -> None:
    types = _TYPES.get(kind)
    if types is None:
        raise ValueError(f"schema 不支持的类型：{kind!r}")
    if isinstance(value, bool) and kind != "boolean":
        raise ToolArgsError(f"{path} 须为 {kind}")
    if not isinstance(value, types):
        raise ToolArgsError(f"{path} 须为 {kind}")


def _check_len(schema: dict[str, Any], n: int, path: str, lo: str, hi: str, unit: str) -> None:
    if lo in schema and n < schema[lo]:
        raise ToolArgsError(f"{path} 至少 {schema[lo]} {unit}")
    if hi in schema and n > schema[hi]:
        raise ToolArgsError(f"{path} 至多 {schema[hi]} {unit}")


def _check_object(schema: dict[str, Any], value: dict[str, Any], path: str) -> None:
    props: dict[str, Any] = schema.get("properties", {})
    for name in schema.get("required", ()):
        if name not in value:
            raise ToolArgsError(f"{path} 缺少 {name}")
    for name, item in value.items():
        if name in props:
            _check(props[name], item, f"{name}" if path == "参数" else f"{path}.{name}")
        elif schema.get("additionalProperties") is False:
            raise ToolArgsError(f"{path} 含未知字段 {name}")


class ToolRegistry:
    """按注册顺序持有工具；名称唯一。"""

    def __init__(self, tools: Iterable[Tool]) -> None:
        self._tools: dict[str, Tool] = {}
        for tool in tools:
            if tool.name in self._tools:
                raise ValueError(f"工具重名：{tool.name}")
            self._tools[tool.name] = tool

    def names(self) -> list[str]:
        return list(self._tools)

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def schemas(self, names: Iterable[str] | None = None) -> list[dict[str, Any]]:
        """OpenAI 兼容的 tools 列表，按注册顺序；names 给出时只含其中的工具。"""
        wanted = None if names is None else set(names)
        if wanted is not None and (unknown := wanted - self._tools.keys()):
            raise ValueError(f"未注册的工具：{sorted(unknown)}")
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
                },
            }
            for t in self._tools.values()
            if wanted is None or t.name in wanted
        ]

    def dispatch(self, name: str, raw_args: str, ctx: ToolContext) -> ToolResult:
        """解析、校验并运行一次工具调用。

        未知工具、参数不是 JSON 对象、校验失败或工具抛 ToolArgsError → ok=False 的
        "工具参数错误：…"结果（不发起调用、不计额度）；其余异常上抛。
        """
        tool = self._tools.get(name)
        if tool is None:
            return _args_error(f"没有名为 {name!r} 的工具（可用：{', '.join(self._tools)}）")
        try:
            args = json.loads(raw_args) if raw_args.strip() else {}
        except (ValueError, RecursionError) as exc:
            return _args_error(f"参数不是合法的 JSON：{exc}")
        try:
            validate_args(tool.parameters, args)
            return tool.run(args, ctx)
        except ToolArgsError as exc:
            return _args_error(str(exc))


def _args_error(message: str) -> ToolResult:
    return ToolResult(content=f"工具参数错误：{message}", ok=False)
