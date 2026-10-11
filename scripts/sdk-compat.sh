#!/usr/bin/env bash
# sdk-compat.sh：以官方 openai 与 anthropic Python SDK（不修改）调用模型 API 的兼容性测试（模型网关设计 §10）：
# 每个 SDK 分别对 fake upstream 的 OpenAI 模式与 Anthropic 模式，覆盖非流式、流式与工具调用（2 个 SDK × 2 种供应商格式）。
# 不需要 root，不产生模型费用。需要 go、uv 与一个 PostgreSQL（管理员 DSN；测试在其中建一个临时库并在结束时删除）：
#
#   AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable' bash scripts/sdk-compat.sh
#
# 构建 tests/sdk/harness（真实的 internal/api 与 internal/modelapi，fake upstream 为供应商），再在 tests/sdk 中以 uv 运行 pytest。
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${AGENTBOX_TEST_DATABASE_URL:?需要 AGENTBOX_TEST_DATABASE_URL（PostgreSQL 管理员 DSN）}"
command -v go >/dev/null || { echo "sdk-compat: 需要 go" >&2; exit 1; }
command -v uv >/dev/null || { echo "sdk-compat: 需要 uv（https://docs.astral.sh/uv/）" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
(cd "$root" && go build -o "$work/sdk-harness" ./tests/sdk/harness)
cd "$root/tests/sdk"
AGENTBOX_SDK_HARNESS="$work/sdk-harness" uv run --locked pytest -q -p no:cacheprovider "$@"
