"""chatagent：skill 化的对话 Agent（M4 Plan 13）。

kimi-k3 编排循环决定直接回答还是按 deep-research skill 研究；子主题由 kimi-k2.6 子循环执行；
报告经引用核对后登记为产物。只用标准库与 SDK（agentbox_worker）。
"""

from chatagent.app import make_app, run
from chatagent.config import TurnConfig, parse_turn_config
from chatagent.loop import Agent, LoopOutcome
from chatagent.model import ModelReply, ModelUnavailable, assistant_message, call_model
from chatagent.state import SessionMemory, SubtopicState, TurnRecord, TurnState

__all__ = [
    "Agent",
    "LoopOutcome",
    "ModelReply",
    "ModelUnavailable",
    "SessionMemory",
    "SubtopicState",
    "TurnConfig",
    "TurnRecord",
    "TurnState",
    "assistant_message",
    "call_model",
    "make_app",
    "parse_turn_config",
    "run",
]
