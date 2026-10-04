"""SDK 与应用共用的失败类型；SDK 把 WorkerFailure 转换为 error 事件（规格 §5.4）。"""

from __future__ import annotations


class WorkerFailure(Exception):
    """任务失败。retryable 只是给宿主的建议，是否重试由宿主决定。"""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        if not code:
            raise ValueError("WorkerFailure 的 code 不能为空")
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable


class CheckpointRejected(WorkerFailure):
    """宿主对 checkpoint 返回 conflict 或 rejected。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, retryable=False)


class CheckpointUnresolved(WorkerFailure):
    """重发与查询达到上限仍未得到结果；提交是否成立由宿主记录为准。"""

    def __init__(self, message: str) -> None:
        super().__init__("checkpoint_unresolved", message, retryable=True)


class ArtifactRejected(WorkerFailure):
    """宿主拒绝了登记的产物。"""

    def __init__(self, message: str) -> None:
        super().__init__("artifact_rejected", message, retryable=False)


class TransportBroken(Exception):
    """协议输出通道已不可用：某次发送失败或在途被取消，结果不确定。

    此后不再发送任何事件（不会复用 seq），也无法再发出 error 事件；Worker 以失败退出。
    """
