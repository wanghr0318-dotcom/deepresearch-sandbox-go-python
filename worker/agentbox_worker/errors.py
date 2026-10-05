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


class GatewayError(Exception):
    """Gateway 返回错误或无法完成请求。status 为 HTTP 状态码（未得到响应时为 0），code 来自
    错误体 {"error":{"code","message"}}，客户端自身判定的失败使用 connection_lost、
    connection_refused、client_timeout、invalid_response 等代码。"""

    def __init__(self, status: int, code: str, message: str = "") -> None:
        super().__init__(f"{status} {code}: {message}" if message else f"{status} {code}")
        self.status = status
        self.code = code
        self.message = message


class BudgetExhausted(GatewayError):
    """402 budget_exhausted / budget_insufficient_for_request（规格 §9.6：不等于任务终止）。"""


class CallDivergence(GatewayError):
    """409 fingerprint_mismatch：同一 call id 的请求内容与已记录的不同（规格 §9.4）。

    SDK 不自动换 ID；由编排层以新 ID 与 X-Agentbox-Supersedes 显式重发。
    """


class CallInProgress(GatewayError):
    """409 call_in_progress：客户端以同一 ID 有界等待重试后仍未结束。"""


class AccessRevoked(GatewayError):
    """403 access_revoked 或连接被拒：本 attempt 已无权访问 Gateway。"""


class CallDeadlineExceeded(GatewayError):
    """504 call_deadline_exceeded，或客户端 HTTP 超时（code 为 client_timeout）。"""
