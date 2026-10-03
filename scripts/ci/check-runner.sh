#!/usr/bin/env bash
# 在运行 Linux 集成测试前验证 runner 确实具备所需条件。
# sudo 可用不等于 cgroup、挂载与 namespace 条件满足；任何一项不满足都直接失败，
# 而不是让测试在条件缺失时静默跳过。
set -euo pipefail

fail() { echo "runner 不满足条件: $*" >&2; exit 1; }

[ "$(id -u)" = "0" ] || fail "需要以 root 运行本脚本"

echo "内核: $(uname -r)"

fstype=$(stat -fc %T /sys/fs/cgroup)
[ "$fstype" = "cgroup2fs" ] || fail "/sys/fs/cgroup 不是 cgroup v2（实际 $fstype）"

probe=/sys/fs/cgroup/agentbox-ci-probe
mkdir "$probe" || fail "无法在 cgroup 根下创建子组"
rmdir "$probe"

grep -qw overlay /proc/filesystems || fail "内核未提供 overlay 文件系统"

tmp=$(mktemp -d)
mkdir "$tmp/src" "$tmp/dst"
mount --bind "$tmp/src" "$tmp/dst" || fail "bind mount 失败"
umount "$tmp/dst"
rm -rf "$tmp"

unshare --mount --pid --uts --ipc --net --fork true || fail "无法创建 mount/pid/uts/ipc/net namespace"

echo "runner 条件满足"
