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

unshare --user true || fail "无法创建 user namespace"

max_userns=$(cat /proc/sys/user/max_user_namespaces 2>/dev/null || echo 0)
[ "${max_userns:-0}" -gt 0 ] 2>/dev/null || fail "user namespace 被禁用（max_user_namespaces=${max_userns}）"

actions=$(cat /proc/sys/kernel/seccomp/actions_avail 2>/dev/null || true)
for a in kill_process errno allow; do
  case " $actions " in
    *" $a "*) ;;
    *) fail "seccomp 缺少动作 $a（actions_avail: ${actions:-不可读}）" ;;
  esac
done

# CLONE_INTO_CGROUP 需要内核 ≥ 5.7。
IFS=. read -r kmajor kminor _ < /proc/sys/kernel/osrelease
kminor=${kminor%%[!0-9]*}
if [ "$kmajor" -lt 5 ] || { [ "$kmajor" -eq 5 ] && [ "$kminor" -lt 7 ]; }; then
  fail "内核 $(uname -r) 不支持 CLONE_INTO_CGROUP（需要 ≥ 5.7）"
fi

case "$(uname -m)" in
  x86_64) ;;
  aarch64) echo "警告: aarch64 未经单独验证，不构成支持主张（规格 §16.2）" >&2 ;;
  *) fail "架构 $(uname -m) 不受支持" ;;
esac

# 新挂载 API 与 close_range：用 python3 ctypes 直接发系统调用（号段 x86_64/aarch64 相同）。
python3 - <<'PY' || fail "新挂载 API（open_tree/mount_setattr）、close_range 或 pidfd 不可用"
import ctypes, os, sys
libc = ctypes.CDLL(None, use_errno=True)
libc.syscall.restype = ctypes.c_long
AT_FDCWD, OPEN_TREE_CLONE, OPEN_TREE_CLOEXEC, AT_EMPTY_PATH = -100, 1, 0x80000, 0x1000
ENOSYS, EPERM = 38, 1

fd = libc.syscall(428, AT_FDCWD, b"/", OPEN_TREE_CLONE | OPEN_TREE_CLOEXEC)
if fd < 0:
    print("open_tree 失败: errno=%d" % ctypes.get_errno(), file=sys.stderr); sys.exit(1)
os.close(fd)

ctypes.set_errno(0)
libc.syscall(442, -1, b"", AT_EMPTY_PATH, None, 0)  # 非法参数：只区分存在与否
if ctypes.get_errno() in (ENOSYS, EPERM):
    print("mount_setattr 不可用: errno=%d" % ctypes.get_errno(), file=sys.stderr); sys.exit(1)

ctypes.set_errno(0)
r = libc.syscall(436, 2, 1, 0)  # 空区间，不关闭任何 fd
if r < 0 and ctypes.get_errno() in (ENOSYS, EPERM):
    print("close_range 不可用: errno=%d" % ctypes.get_errno(), file=sys.stderr); sys.exit(1)

# pidfd：非法参数调用，不打开也不向任何进程发信号；ENOSYS/EPERM 即不可用（号段在所有架构统一）。
for name, args in (("pidfd_open", (434, 0, 0)), ("pidfd_send_signal", (424, -1, 0, None, 0))):
    ctypes.set_errno(0)
    r = libc.syscall(*args)
    if r < 0 and ctypes.get_errno() in (ENOSYS, EPERM):
        print("%s 不可用: errno=%d" % (name, ctypes.get_errno()), file=sys.stderr); sys.exit(1)
PY

echo "runner 条件满足"
