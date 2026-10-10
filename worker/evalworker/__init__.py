"""Eval worker for `agentbox eval`: coding/terminal tasks and research questions.

`python3 -m evalworker` is started by a server running with `--worker-argv python3,-m,evalworker`.
Tasks whose config has `eval.kind == "coding"` run the coding harness (solution from the model via
the Gateway, or the suite's reference; checker in the exec sandbox via /v1/exec); every other task
is delegated unchanged to the `deepresearch` agent. Like every worker, it reaches the outside only
through the Gateway (import-linter and ruff TID251 enforce this).
"""

from deepresearch.app import VERSION as DEEPRESEARCH_VERSION

NAME = "evalworker"
VERSION = f"0.1.0+deepresearch.{DEEPRESEARCH_VERSION}"

__all__ = ["NAME", "VERSION"]
