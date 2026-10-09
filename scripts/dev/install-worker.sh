#!/usr/bin/env bash
# 把 Worker 包安装到沙箱默认模板包含的目录（默认 /opt/agentbox，rootfs.WorkerDir）：
#   agentbox_worker（SDK 与工具）、deepresearch（task 模式研究 Worker）、chatagent（会话 Worker）、
#   skills（chatagent 经 read_skill 按需加载；与包目录同级）。sim_worker、evalworker（agentbox eval 的评测 Worker）
#   存在时一并安装（演示、测试与评测用）。
# 用法：sudo bash scripts/dev/install-worker.sh [目标目录]
# 只复制源码（去掉 __pycache__），目录 0755、文件 0644；安装后校验入口与 skills/deep-research/SKILL.md。
set -euo pipefail

DEST=${1:-/opt/agentbox}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
SRC="$REPO/worker"

fail() { echo "install-worker: $*" >&2; exit 1; }

for d in agentbox_worker deepresearch chatagent skills; do
  [ -d "$SRC/$d" ] || fail "源目录缺失：$SRC/$d"
done
[ -f "$SRC/skills/deep-research/SKILL.md" ] || fail "源目录缺少 skills/deep-research/SKILL.md"

PKGS=(agentbox_worker deepresearch chatagent skills)
[ -d "$SRC/sim_worker" ] && PKGS+=(sim_worker)
[ -d "$SRC/evalworker" ] && PKGS+=(evalworker)

install -d -m 0755 "$DEST"
for d in "${PKGS[@]}"; do
  rm -rf "${DEST:?}/$d"
  cp -r "$SRC/$d" "$DEST/$d"
done
find "$DEST" -name __pycache__ -prune -exec rm -rf {} +
chmod -R u=rwX,go=rX "$DEST"

for f in deepresearch/__main__.py chatagent/__main__.py agentbox_worker/__init__.py skills/deep-research/SKILL.md; do
  [ -f "$DEST/$f" ] || fail "安装后缺少 $DEST/$f"
done
echo "install-worker: 已安装到 $DEST：${PKGS[*]}（$(find "$DEST" -name '*.py' | wc -l) 个 .py，$(find "$DEST/skills" -type f | wc -l) 个 skill 文件）"
