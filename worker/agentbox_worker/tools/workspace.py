"""工作区工具：exec_shell、read_file、write_file、list_dir。

设计见 docs/design/2026-10-10-shell-file-mcp-design.md。本轮有一个由 Gateway 持有的
工作区：文件在工具调用之间保留，进程不保留。exec_shell 的每条命令在一个全新的 exec
沙箱（无网络、独立 UID、seccomp、CPU/内存/进程数/磁盘配额）中运行，工作区文件先复制
进去、命令结束后再收回，按 exec 配额记账（不计入每轮 web 工具额度）。文件操作在
Gateway 中完成，不计费、不计额度。
Worker 内不执行任何命令。
"""

from __future__ import annotations

from typing import Any, Protocol, cast

from agentbox_worker.errors import GatewayError
from agentbox_worker.gateway import GatewayResult
from agentbox_worker.tools.base import ToolArgsError, ToolContext, ToolResult, is_fatal
from agentbox_worker.tools.run_python import (
    _QUOTA_TEXT,
    STDERR_TAIL_BYTES,
    STDOUT_HEAD_BYTES,
    STDOUT_TAIL_BYTES,
    _clip,
    _is_exec_quota,
    _status_line,
)

READ_PREVIEW_CHARS = 1200
OUTPUT_PREVIEW_CHARS = 2000
MAX_LIST_ENTRIES = 300

# 工作区与 exec 的错误码 → 给模型的说明
_ERRORS = {
    "invalid_path": (
        "路径不合法：只能用工作区内的相对路径（如 src/app.py），不能含 ..、不能以 / 开头，"
        "不能用 .agentbox 目录、控制字符或超过 255 字节的文件名"
    ),
    "workspace_write_quota": (
        "本轮写入文件的次数或总量已达上限，请改用 exec_shell 在工作区内生成文件"
    ),
    "workspace_staging_failed": (
        "工作区未能完整放入执行环境（可能空间不足），工作区保持不变；请删除大文件后再试"
    ),
    "workspace_destroyed": "本轮已结束，工作区已被清理",
    "mcp_arguments_too_large": "参数过大",
    "not_found": "文件或目录不存在",
    "is_directory": "这是一个目录，请用 list_dir",
    "not_a_directory": "这是一个文件，请用 read_file",
    "path_conflict": "路径与已有的文件或目录冲突（文件不能放在文件下面）",
    "workspace_full": "工作区已满（文件数或总大小超过上限），请先删除不需要的文件",
    "content_too_large": "内容超过 1 MiB",
    "workspace_expired": "工作区因长时间未使用已过期，之前的文件已不存在",
    "workspace_lost": "工作区在服务重启后无法恢复，之前的文件已不存在",
    "endpoint_not_configured": "服务端没有启用工作区工具",
    "exec_queue_timeout": "排队等待执行环境超时，可稍后再试",
    "exec_cancelled": "执行被取消（任务停止或 attempt 被替换）",
}


class WorkspaceGateway(Protocol):
    def workspace_exec(
        self, step_id: str, command: str, *, timeout_ms: int | None = None, retry: bool = False
    ) -> GatewayResult: ...

    def workspace(self, op: str, body: dict[str, Any]) -> dict[str, Any]: ...


def _gw(ctx: ToolContext) -> WorkspaceGateway:
    return cast(WorkspaceGateway, ctx.gateway)


def _failure(message: str, raw: dict[str, Any] | None = None) -> ToolResult:
    return ToolResult(content=message, ok=False, preview={"kind": "text", "text": message}, raw=raw)


def _error(exc: GatewayError, action: str) -> ToolResult:
    if is_fatal(exc):
        raise exc
    detail = _ERRORS.get(exc.code, exc.code)
    return _failure(f"{action}失败：{detail}（{exc.code}）。")


def _inline(request: dict[str, Any], response: Any) -> dict[str, Any]:
    return {"inline": {"request": request, "response": response}}


def _workspace_line(body: dict[str, Any]) -> str:
    ws = body.get("workspace") if isinstance(body.get("workspace"), dict) else {}
    return f"工作区：{ws.get('files', 0)} 个文件，{ws.get('bytes', 0)} 字节"


class ExecShell:
    name = "exec_shell"
    description = (
        "在本轮工作区中运行一条 bash 命令（无网络的隔离沙箱，当前目录即工作区根目录）。"
        "文件在命令之间保留，进程不保留（不要启动后台服务）；适合运行测试、脚本、编译、grep、"
        "查看目录等。返回退出码、stdout/stderr 与本次新增、修改、删除的文件。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "command": {"type": "string", "minLength": 1, "maxLength": 65536},
            "timeout_s": {"type": "integer", "minimum": 1, "maximum": 300},
        },
        "required": ["command"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        command = args["command"]
        if not command.strip():
            raise ToolArgsError("command 不能为空白")
        timeout_s = args.get("timeout_s")
        timeout_ms = None if timeout_s is None else timeout_s * 1000
        try:
            res = _gw(ctx).workspace_exec(ctx.step_id, command, timeout_ms=timeout_ms)
        except GatewayError as exc:
            if _is_exec_quota(exc):
                text = _QUOTA_TEXT[exc.code]
                return _failure(f"{text}（{exc.code}），不要再调用 exec_shell 或 run_python。")
            return _error(exc, "命令未能运行")
        return _shell_result(res)


def _shell_result(res: GatewayResult) -> ToolResult:
    body = res.body if isinstance(res.body, dict) else {}
    status = str(body.get("status", ""))
    exit_info = body.get("exit") if isinstance(body.get("exit"), dict) else {}
    limits = body.get("limits") if isinstance(body.get("limits"), dict) else {}
    lines = [_status_line(status, exit_info, limits), f"用时 {body.get('wall_ms')} ms"]
    for name, head, tail in (
        ("stdout", STDOUT_HEAD_BYTES, STDOUT_TAIL_BYTES),
        ("stderr", 0, STDERR_TAIL_BYTES),
    ):
        text = body.get(name) if isinstance(body.get(name), str) else ""
        data = text.encode("utf-8")
        if data:
            lines.append(f"{name}（{len(data)} 字节）：")
            lines.append(_clip(data, head, tail))
            if body.get(f"{name}_truncated"):
                lines.append(f"（{name} 超过 1 MiB，已截断）")
        else:
            lines.append(f"{name}：（空）")
    ch = body.get("changes") if isinstance(body.get("changes"), dict) else {}
    changed = []
    for key, label in (("added", "新增"), ("modified", "修改"), ("deleted", "删除")):
        names = [str(p) for p in ch.get(key) or []]
        if names:
            changed.append(f"{label}：" + "、".join(names[:50]) + ("…" if len(names) > 50 else ""))
    lines.append("文件变化：" + ("；".join(changed) if changed else "无"))
    skipped = [s for s in body.get("skipped_outputs") or [] if isinstance(s, dict)]
    if skipped:
        lines.append(
            "未保留："
            + "、".join(f"{s.get('path')}（{s.get('reason')}）" for s in skipped[:20])
            + "（符号链接、特殊文件与超出 256 个的文件不会保留在工作区）"
        )
    if status == "timed_out":
        lines.append("命令超时被终止；终止前写入的文件已保留。")
    lines.append(_workspace_line(body))
    ok = status == "completed" and exit_info.get("code") == 0 and not exit_info.get("signal")
    preview_lines = [f"退出码 {exit_info.get('code')}" if status == "completed" else status]
    out = body.get("stdout") if isinstance(body.get("stdout"), str) else ""
    err = body.get("stderr") if isinstance(body.get("stderr"), str) else ""
    if out:
        preview_lines.append(out[:OUTPUT_PREVIEW_CHARS])
    if err and not ok:
        preview_lines.append("stderr：\n" + err[-600:])
    return ToolResult(
        content="\n".join(lines),
        ok=ok,
        preview={"kind": "text", "text": "\n".join(preview_lines)},
        raw={"call_id": res.call_id},
        blobs=(res.blob_sha256,) if res.blob_sha256 else (),
        data=body,
    )


class ReadFile:
    name = "read_file"
    description = (
        "读取本轮工作区中的文本文件（相对路径），可指定起始行与行数；一次至多 2000 行、256 KiB。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "path": {"type": "string", "minLength": 1, "maxLength": 512},
            "start_line": {"type": "integer", "minimum": 1},
            "max_lines": {"type": "integer", "minimum": 1, "maximum": 2000},
        },
        "required": ["path"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        req = {k: args[k] for k in ("path", "start_line", "max_lines") if k in args}
        try:
            body = _gw(ctx).workspace("read", req)
        except GatewayError as exc:
            return _error(exc, f"读取 {args['path']} ")
        if body.get("binary"):
            text = f"{body.get('path')} 是二进制文件（{body.get('size')} 字节），无法以文本读取。"
            return ToolResult(
                content=text, preview={"kind": "text", "text": text}, raw=_inline(req, body)
            )
        first, last, total = body.get("start_line"), body.get("end_line"), body.get("total_lines")
        total_text = f"共 {total} 行" if total is not None else "文件更长"
        head = f"{body.get('path')}（第 {first}–{last} 行，{total_text}）"
        if body.get("truncated"):
            head += "；未读完，可用 start_line 继续"
        content = str(body.get("content", ""))
        preview = content[:READ_PREVIEW_CHARS] + ("…" if len(content) > READ_PREVIEW_CHARS else "")
        return ToolResult(
            content=f"{head}\n{content}",
            preview={"kind": "text", "text": preview},
            raw=_inline(req, {k: v for k, v in body.items() if k != "content"}),
        )


class WriteFile:
    name = "write_file"
    description = (
        "在本轮工作区中写入（新建或覆盖）一个文本文件，路径为相对路径，上级目录自动建立；"
        "内容至多 1 MiB。executable=true 时设为可执行。delete=true 时删除该文件或整个目录"
        "（不给 content）。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "path": {"type": "string", "minLength": 1, "maxLength": 512},
            "content": {"type": "string", "maxLength": 1048576},
            "executable": {"type": "boolean"},
            "delete": {"type": "boolean"},
        },
        "required": ["path"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        if args.get("delete"):
            return self._delete(args, ctx)
        if "content" not in args:
            raise ToolArgsError("写入时须给出 content（删除请用 delete=true）")
        req = {k: args[k] for k in ("path", "content", "executable") if k in args}
        try:
            body = _gw(ctx).workspace("write", req)
        except GatewayError as exc:
            return _error(exc, f"写入 {args['path']} ")
        verb = "新建" if body.get("created") else "覆盖"
        text = f"已{verb} {body.get('path')}（{body.get('size')} 字节）。{_workspace_line(body)}"
        shown = {k: v for k, v in req.items() if k != "content"}
        shown["content_bytes"] = len(args["content"].encode("utf-8"))
        return ToolResult(
            content=text, preview={"kind": "text", "text": text}, raw=_inline(shown, body)
        )

    def _delete(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        req = {"path": args["path"], "delete": True}
        try:
            body = _gw(ctx).workspace("write", req)
        except GatewayError as exc:
            return _error(exc, f"删除 {args['path']} ")
        gone = [str(p) for p in body.get("deleted") or []]
        text = f"已删除 {len(gone)} 个文件：" + "、".join(gone[:50]) + f"。{_workspace_line(body)}"
        return ToolResult(
            content=text, preview={"kind": "text", "text": text}, raw=_inline(req, body)
        )


class ListDir:
    name = "list_dir"
    description = "列出本轮工作区中的目录（缺省为根目录）；recursive=true 时列出其下全部文件。"
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "path": {"type": "string", "maxLength": 512},
            "recursive": {"type": "boolean"},
        },
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        req = {k: args[k] for k in ("path", "recursive") if k in args}
        try:
            body = _gw(ctx).workspace("list", req)
        except GatewayError as exc:
            return _error(exc, f"列出 {args.get('path') or '根目录'} ")
        entries = [e for e in body.get("entries") or [] if isinstance(e, dict)]
        where = body.get("path") or "（根目录）"
        if not entries:
            text = f"{where} 为空。{_workspace_line(body)}"
        else:
            rows = []
            for e in entries[:MAX_LIST_ENTRIES]:
                if e.get("type") == "dir":
                    rows.append(f"{e.get('name')}/")
                else:
                    mark = "*" if e.get("executable") else ""
                    rows.append(f"{e.get('name')}{mark}  {e.get('size')} B")
            if len(entries) > MAX_LIST_ENTRIES:
                rows.append(f"…（共 {len(entries)} 项）")
            text = f"{where}：\n" + "\n".join(rows) + f"\n{_workspace_line(body)}"
        return ToolResult(
            content=text,
            preview={"kind": "text", "text": text[:READ_PREVIEW_CHARS]},
            raw=_inline(req, body),
        )


def workspace_tools() -> list[Any]:
    return [ExecShell(), ReadFile(), WriteFile(), ListDir()]


WORKSPACE_TOOL_NAMES = ("exec_shell", "read_file", "write_file", "list_dir")
