"""python3 -m chatagent：session 模式的对话 Agent Worker（协调者裁定 K）。"""

from agentbox_worker import main_session
from chatagent.app import run

main_session(run, name="chatagent", version="0.1.0")
