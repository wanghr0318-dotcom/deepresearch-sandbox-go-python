"""沙箱化深度研究助手的 Python Worker SDK：协议 v1（task 模式与 session 扩展）。

SDK 只提供运行协议与执行能力：握手、事件、checkpoint、产物登记、暂停与取消，以及 session
模式的多轮 task 循环（task_outcome、awaiting_input、会话状态提议），以及 sub-run 扩展
（SubrunManager 与带归属的 Gateway 视图）。
研究策略、提示词等业务逻辑属于具体 Worker。
"""

from agentbox_worker.errors import (
    AccessRevoked,
    ArtifactRejected,
    BudgetExhausted,
    CallAbandoned,
    CallDeadlineExceeded,
    CallDivergence,
    CallInProgress,
    CheckpointRejected,
    CheckpointUnresolved,
    GatewayError,
    ToolBudgetExhausted,
    TransportBroken,
    WorkerFailure,
)
from agentbox_worker.gateway import CallIds, GatewayClient, GatewayResult, SubrunGateway
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
from agentbox_worker.session import (
    SessionApp,
    SessionInfo,
    main_session,
    read_staged_state,
    run_session_worker,
)
from agentbox_worker.subruns import (
    SubrunCancelled,
    SubrunHandle,
    SubrunInfo,
    SubrunManager,
    SubrunRejected,
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
    "SessionApp",
    "SessionInfo",
    "main_session",
    "read_staged_state",
    "run_session_worker",
    "AccessRevoked",
    "BudgetExhausted",
    "CallAbandoned",
    "CallDeadlineExceeded",
    "CallDivergence",
    "CallInProgress",
    "GatewayError",
    "ToolBudgetExhausted",
    "CallIds",
    "GatewayClient",
    "GatewayResult",
    "SubrunGateway",
    "SubrunCancelled",
    "SubrunHandle",
    "SubrunInfo",
    "SubrunManager",
    "SubrunRejected",
]
