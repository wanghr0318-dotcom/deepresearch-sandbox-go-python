#!/usr/bin/env bash
# restore.sh：把 backup.sh 生成的加密归档恢复到一个空数据库与一个空数据目录，并逐项校验。以 root 运行。
#
#   sudo AGENTBOX_BACKUP_PASSPHRASE_FILE=/root/.agentbox-backup-pass \
#        AGENTBOX_RESTORE_DATA_DIR=/var/lib/agentbox \
#        AGENTBOX_RESTORE_DATABASE_URL='postgres://...' \
#        bash scripts/restore.sh agentbox-backup-<时间>.tar.zst.gpg
#
# 校验：归档的 SHA-256 与同名 .sha256 一致 → 解密（GnuPG 的完整性校验失败即中止）→ manifest 中每个文件的
# SHA-256 → 目标数据库为空、目标数据目录不存在或为空 → pg_restore → 每个 blob 的内容哈希等于其文件名 →
# 恢复后的任务、用户、blob 计数与 manifest 一致。任何一步失败即退出，不覆盖已有数据。
#
# 恢复后需另行提供（备份中没有的机密）：<data>/api.token（新生成即可）、模型/搜索 Key 与数据库连接串所在的
# env 文件、TLS 私钥。<data>/cache.key 由 server 启动时自动生成（缓存条目随之失效，属预期）。
#
# 环境变量：
#   AGENTBOX_BACKUP_PASSPHRASE_FILE  必填：口令文件
#   AGENTBOX_RESTORE_DATA_DIR        必填：目标数据目录（不存在或为空）
#   AGENTBOX_RESTORE_DATABASE_URL    必填：目标数据库（须已存在且没有任何表）
#   AGENTBOX_RESTORE_CONFIG_DIR      可选：把备份中的非机密配置复制到此目录（默认不复制）
set -euo pipefail
umask 077

fail() { echo "restore: $*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || fail "须以 root 运行"
for c in pg_restore psql tar zstd gpg sha256sum python3; do command -v "$c" >/dev/null || fail "缺少 $c"; done

ARCHIVE="${1:-}"
[ -n "$ARCHIVE" ] && [ -f "$ARCHIVE" ] || fail "用法：restore.sh <归档.tar.zst.gpg>"
PASS_FILE="${AGENTBOX_BACKUP_PASSPHRASE_FILE:-}"
[ -n "$PASS_FILE" ] && [ -r "$PASS_FILE" ] || fail "须设置 AGENTBOX_BACKUP_PASSPHRASE_FILE"
DATA="${AGENTBOX_RESTORE_DATA_DIR:-}"
DB_URL="${AGENTBOX_RESTORE_DATABASE_URL:-}"
[ -n "$DATA" ] && [ -n "$DB_URL" ] || fail "须设置 AGENTBOX_RESTORE_DATA_DIR 与 AGENTBOX_RESTORE_DATABASE_URL"
if [ -e "$DATA" ] && [ -n "$(ls -A "$DATA" 2>/dev/null)" ]; then fail "目标数据目录 $DATA 不为空，拒绝覆盖"; fi

export PGCONNECT_TIMEOUT=10
eval "$(python3 - "$DB_URL" <<'PY'
import shlex, sys, urllib.parse as u
p = u.urlparse(sys.argv[1])
q = dict(u.parse_qsl(p.query))
env = {"PGHOST": p.hostname or "127.0.0.1", "PGPORT": str(p.port or 5432),
       "PGUSER": u.unquote(p.username or ""), "PGPASSWORD": u.unquote(p.password or ""),
       "PGDATABASE": p.path.lstrip("/"), "PGSSLMODE": q.get("sslmode", "prefer")}
print("\n".join(f"export {k}={shlex.quote(v)}" for k, v in env.items()))
PY
)"
q() { psql -X -qAt -v ON_ERROR_STOP=1 -c "$1"; }

echo "[1/6] 归档 SHA-256"
SUM_FILE="$ARCHIVE.sha256"
[ -f "$SUM_FILE" ] || fail "缺少 $SUM_FILE（不校验就不恢复）"
want=$(cut -d' ' -f1 "$SUM_FILE"); got=$(sha256sum "$ARCHIVE" | cut -d' ' -f1)
[ "$want" = "$got" ] || fail "SHA-256 不符：期望 $want，得到 $got"
echo "      $got"

echo "[2/6] 解密、解压"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
gpg --batch --quiet --pinentry-mode loopback --passphrase-file "$PASS_FILE" --decrypt "$ARCHIVE" |
  zstd -q -d | tar -C "$WORK" -xf -
NAME=$(ls "$WORK")
[ "$(echo "$NAME" | wc -l)" = 1 ] && [ -f "$WORK/$NAME/manifest.json" ] || fail "归档结构不符（缺 manifest.json）"
SRC="$WORK/$NAME"

echo "[3/6] manifest 中每个文件的 SHA-256"
python3 - "$SRC" <<'PY'
import hashlib, json, os, sys
root = sys.argv[1]
m = json.load(open(os.path.join(root, "manifest.json")))
if m.get("format") != "agentbox-backup/1":
    sys.exit(f"未知的备份格式：{m.get('format')}")
present = {os.path.relpath(os.path.join(d, n), root).replace(os.sep, "/")
           for d, _, ns in os.walk(root) for n in ns} - {"manifest.json"}
if present != set(m["files"]):
    sys.exit(f"文件集合与 manifest 不符：多出 {sorted(present - set(m['files']))[:5]}，缺少 {sorted(set(m['files']) - present)[:5]}")
for rel, want in m["files"].items():
    h = hashlib.sha256(open(os.path.join(root, rel), "rb").read()).hexdigest()
    if h != want:
        sys.exit(f"{rel} 的 SHA-256 不符")
print(f"      {len(m['files'])} 个文件一致；备份时间 {m['created_at']}，schema {m['schema_version']}，install_id {m['install_id']}")
PY

echo "[4/6] 目标数据库为空；pg_restore"
tables=$(q "SELECT count(*) FROM pg_tables WHERE schemaname = current_schema()")
[ "$tables" = 0 ] || fail "目标数据库已有 $tables 张表，拒绝覆盖"
pg_restore --no-owner --no-privileges --exit-on-error -d "$PGDATABASE" "$SRC/db.dump"

echo "[5/6] 数据目录：blobs、install_id、bootstrap_token；逐个校验 blob 内容哈希"
mkdir -p "$DATA"
chmod 700 "$DATA"
cp -a "$SRC/data/blobs" "$DATA/blobs"
install -m 600 "$SRC/data/install_id" "$DATA/install_id"
install -m 600 "$SRC/data/bootstrap_token" "$DATA/bootstrap_token"
python3 - "$DATA/blobs" <<'PY'
import hashlib, os, sys
root, n = sys.argv[1], 0
for d, _, names in os.walk(root):
    for name in names:
        p = os.path.join(d, name)
        rel = os.path.relpath(p, root).replace(os.sep, "/")
        parts = rel.split("/")  # sha256/<前 2 位>/<其余 62 位>
        if len(parts) != 3 or parts[0] != "sha256":
            continue
        if hashlib.sha256(open(p, "rb").read()).hexdigest() != parts[1] + parts[2]:
            sys.exit(f"blob {rel} 的内容哈希与文件名不符")
        n += 1
print(f"      {n} 个 blob 的内容哈希与文件名一致")
PY
if [ -n "${AGENTBOX_RESTORE_CONFIG_DIR:-}" ]; then
  mkdir -p "$AGENTBOX_RESTORE_CONFIG_DIR" && cp -a "$SRC/config/." "$AGENTBOX_RESTORE_CONFIG_DIR/"
fi

echo "[6/6] 计数与 manifest 比较"
python3 - "$SRC/manifest.json" "$(q 'SELECT count(*) FROM tasks')" "$(q 'SELECT count(*) FROM users' 2>/dev/null || echo 0)" \
  "$(q 'SELECT count(*) FROM blobs')" "$(q 'SELECT install_id FROM installation')" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
tasks, users, blobs, install_id = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
c = m["counts"]
bad = [f"{k}: 备份 {c[k]}，恢复后 {v}" for k, v in (("tasks", tasks), ("users", users), ("blobs_db", blobs)) if c[k] != v]
if install_id != m["install_id"]:
    bad.append(f"install_id: 备份 {m['install_id']}，恢复后 {install_id}")
if bad:
    sys.exit("计数不符：" + "；".join(bad))
print(f"      tasks {tasks}、users {users}、blobs {blobs}、install_id 一致")
PY
echo "恢复完成：数据目录 $DATA。启动前请提供 <data>/api.token（0600）与含 Key、数据库连接串的 env 文件。"
