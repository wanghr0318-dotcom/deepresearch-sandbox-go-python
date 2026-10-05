"""go-agentbox Python Worker SDK：协议 v1（task 模式）。

SDK 只提供运行协议与执行能力：握手、事件、checkpoint、产物登记、暂停与取消。
研究策略、提示词等业务逻辑属于具体 Worker。
"""

from agentbox_worker.errors import (
    AccessRevoked,
    ArtifactRejected,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    CallInProgress,
    CheckpointRejected,
    CheckpointUnresolved,
    GatewayError,
    TransportBroken,
    WorkerFailure,
)
from agentbox_worker.gateway import CallIds, GatewayClient, GatewayResult
from agentbox_worker.runtime import (
    EXIT_FAILURE,
    EXIT_HANDSHAKE,
    EXIT_OK,
    ArtifactRef,
    Paused,
    Result,
    ResumeInfo,
    TaskContext,
    Timing,
    main,
    run_worker,
)
from agentbox_worker.transport import MemoryTransport, StdioTransport, Transport

__all__ = [
    "EXIT_FAILURE",
    "EXIT_HANDSHAKE",
    "EXIT_OK",
    "ArtifactRef",
    "ArtifactRejected",
    "CheckpointRejected",
    "CheckpointUnresolved",
    "MemoryTransport",
    "Paused",
    "Result",
    "ResumeInfo",
    "StdioTransport",
    "TaskContext",
    "Timing",
    "Transport",
    "TransportBroken",
    "WorkerFailure",
    "main",
    "run_worker",
    "AccessRevoked",
    "BudgetExhausted",
    "CallDeadlineExceeded",
    "CallDivergence",
    "CallInProgress",
    "GatewayError",
    "CallIds",
    "GatewayClient",
    "GatewayResult",
]
