#!/usr/bin/env bash
# Plan 1B spike: reproduce every result. Run as root on the target Linux/WSL2
# host from the repository root:
#   sudo experiments/spike-1b/run.sh            (all scenarios)
#   sudo experiments/spike-1b/run.sh final-classic b3-candidate
# Uses only /tmp/spike-1b and the cgroup /sys/fs/cgroup/agentbox-spike-1b,
# and removes both at the end. Logs go to experiments/spike-1b/results/.
set -u
cd "$(dirname "$0")"
HERE=$(pwd)
S=/tmp/spike-1b
OUT=$HERE/results
[ "$(id -u)" = 0 ] || { echo "must run as root"; exit 1; }

cleanup() {
  for p in $S/rootfs/in $S/rootfs/etc $S/rootfs/usr $S/rootfs; do umount -l "$p" 2>/dev/null; done
  if [ -d /sys/fs/cgroup/agentbox-spike-1b ]; then
    echo 1 > /sys/fs/cgroup/agentbox-spike-1b/cgroup.kill 2>/dev/null
    sleep 0.2; rmdir /sys/fs/cgroup/agentbox-spike-1b
  fi
  rm -rf "$S"
}
trap cleanup EXIT

cleanup
mkdir -p $S/bin $S/in "$OUT"
CGO_ENABLED=0 go build -trimpath -o $S/bin/spike1b . || exit 1
# Template rootfs: an image directory with mount points; /usr and /etc come
# from the host as the "image" contents (read-only binds inside the sandbox).
R=$S/rootfs
mkdir -p $R/usr $R/etc $R/proc $R/dev $R/sys $R/tmp $R/run $R/in $R/out $R/workspace $R/opt/spike
for l in bin lib lib64 sbin; do ln -s usr/$l $R/$l; done
install -m 0755 $S/bin/spike1b $R/opt/spike/spike1b
echo "read-only input" > $S/in/input.txt
chmod -R go-w $S

{
  echo "date: $(date -u +%FT%TZ)"
  echo "uname: $(uname -a)"
  grep PRETTY_NAME /etc/os-release
  echo "glibc: $(ldd --version | head -1)"
  echo "python: $(python3 -V)"
  echo "go: $(go version)"
  echo "cap_last_cap: $(cat /proc/sys/kernel/cap_last_cap)"
  echo "unprivileged_userns_clone: $(cat /proc/sys/kernel/unprivileged_userns_clone 2>/dev/null || echo n/a)"
  echo "max_user_namespaces: $(cat /proc/sys/user/max_user_namespaces)"
  echo "root cgroup subtree_control: $(cat /sys/fs/cgroup/cgroup.subtree_control)"
} > "$OUT/environment.txt"

L="$S/bin/spike1b launch"
declare -A SC
SC[final-classic]="-mounts classic -dump"
SC[final-newapi]="-mounts newapi"
SC[final-hostprep]="-mounts hostprep"
SC[s-gateway-orchestrator]="-env orchestrator"
SC[s-gateway-exec]="-env exec"
SC[s-session-phase1]="-env orchestrator -ws-phase 1"
SC[s-session-phase2]="-env orchestrator -ws-phase 2"
SC[s-session-foreign-range]="-env orchestrator -ws-phase 2-foreign -idbase 310000"
SC[b2-classic-nonrec]="-mounts classic-nonrec"
SC[b2-hostprep-norebind]="-mounts hostprep-norebind"
SC[b2-self-procexe]="-self procexe"
SC[b2-self-procexe-nodetach]="-self procexe -skip-detach"
SC[b2-self-newroot]="-self newroot"
SC[b2-self-after]="-self after"
SC[b1-inherit]="-groups inherit"
SC[b1-credential]="-groups credential"
SC[b1-credential-nonempty]="-groups credential-nonempty"
SC[b3-candidate]="-order candidate"
SC[b3-init-noroot]="-init-noroot"
SC[b3-final-noclear]="-order final-noclear -dump"
SC[b4-perthread]="-capmode perthread -dump"
SC[b4-clone3-eperm]="-clone3 eperm"
SC[b6-start-helper_sigkill-naive]="-fail helper_sigkill -naive-ack"
SC[b5-leak-ctl-nohygiene]="-leak ctl -no-fd-hygiene"
SC[b5-leak-ctl]="-leak ctl"
SC[b5-leak-self-nohygiene]="-leak self -no-fd-hygiene"
SC[b5-leak-self]="-leak self"
for f in mount_private mount_rootfs mount_tmpfs mount_proc mask_proc open_self pivot_root umount_oldroot init_caps; do
  SC[b6-init-$f]="-fail-init $f"
done
for f in helper_sigkill execveat fd_hygiene drop_bounding securebits setresgid setresuid clear_caps clear_ambient no_new_privs seccomp execve; do
  SC[b6-start-$f]="-fail $f"
done
ORDER="final-classic final-newapi final-hostprep final-repeat s-gateway-orchestrator s-gateway-exec s-session-phase1 s-session-phase2 s-session-foreign-range b2-classic-nonrec b2-hostprep-norebind b2-self-procexe b2-self-procexe-nodetach b2-self-newroot b2-self-after b1-inherit b1-credential b1-credential-nonempty b3-candidate b3-init-noroot b3-final-noclear b4-perthread b4-clone3-eperm b4-nolock
 b5-leak-ctl-nohygiene b5-leak-ctl b5-leak-self-nohygiene b5-leak-self"
for k in $(printf '%s\n' "${!SC[@]}" | grep '^b6-' | sort); do ORDER="$ORDER $k"; done
[ $# -gt 0 ] && ORDER="$*"

: > "$OUT/summary.txt"
for name in $ORDER; do
  if [ "$name" = final-repeat ]; then
    # stability: the final sequence 20 times in a row
    : > "$OUT/final-repeat.log"
    for i in $(seq 1 20); do
      $L -label final-repeat-$i >> "$OUT/final-repeat.log" 2>&1
    done
    grep '^RESULT' "$OUT/final-repeat.log" | tee -a "$OUT/summary.txt"
    continue
  fi
  if [ "$name" = b4-nolock ]; then
    # B4 negative variant, repeated: identity changes without LockOSThread.
    : > "$OUT/b4-nolock.log"
    for i in $(seq 1 20); do
      echo "=== iteration $i" >> "$OUT/b4-nolock.log"
      $L -label b4-nolock-$i -capmode perthread-nolock -dump >> "$OUT/b4-nolock.log" 2>&1
    done
    grep '^RESULT' "$OUT/b4-nolock.log" | tee -a "$OUT/summary.txt"
    continue
  fi
  $L -label "$name" ${SC[$name]} > "$OUT/$name.log" 2>&1
  grep '^RESULT' "$OUT/$name.log" | tee -a "$OUT/summary.txt"
done
# leftovers check (must print nothing)
grep spike-1b /proc/self/mountinfo
ls -d /sys/fs/cgroup/agentbox-spike-1b 2>/dev/null
exit 0
