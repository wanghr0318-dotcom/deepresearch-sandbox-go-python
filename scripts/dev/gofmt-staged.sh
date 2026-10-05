#!/usr/bin/env bash
# 用法（在 WSL 中，以普通用户运行）：bash scripts/dev/gofmt-staged.sh
# 对 git 暂存区中的每个 .go 文件运行 gofmt；任何失败非零退出。
set -uo pipefail
export PATH="$PATH:/usr/local/go/bin"
cd "$(dirname "$0")/../.." || exit 1
tmp=$(mktemp)
status=0
files=$(git ls-files '*.go') || { echo "git ls-files 失败"; exit 1; }
for f in $files; do
  if ! git show ":$f" > "$tmp"; then
    echo "读取暂存内容失败: $f"; status=1; continue
  fi
  if ! out=$(gofmt -l "$tmp" 2>&1); then
    echo "gofmt 执行失败: $f: $out"; status=1; continue
  fi
  if [ -n "$out" ]; then
    echo "未格式化: $f"; status=1
  fi
done
rm -f "$tmp"
[ "$status" -eq 0 ] && echo GOFMT-OK
exit "$status"
