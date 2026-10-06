#!/usr/bin/env bash
# backup.sh：把一个 agentbox 安装（数据库 + 数据目录中的持久内容）备份为一个加密归档。以 root 运行。
#
#   sudo AGENTBOX_BACKUP_PASSPHRASE_FILE=/root/.agentbox-backup-pass bash scripts/backup.sh
#
# 产物：<输出目录>/agentbox-backup-<UTC 时间>.tar.zst.gpg 与同名 .sha256。产物不进 Git（.gitignore），
# 应放到私有 GitHub Release asset 或对象存储，并保存 SHA-256。
#
# 包含：PostgreSQL 逻辑备份（pg_dump -Fc）、<data>/blobs、<data>/install_id、<data>/bootstrap_token（数据库只存其
# 哈希，恢复必须带上）、非机密配置（systemd 单元、TLS 证书公钥部分）、manifest.json（计数与各组件 SHA-256）。
# 不包含（机密，恢复时重新生成或另行提供）：<data>/api.token、<data>/cache.key、Key 与数据库口令所在的 env 文件、
# TLS 私钥。运行时状态（envs、workspaces、gateway）不备份。
#
# 一致性：先 pg_dump 再归档 blobs。blob 在数据库引用它之前写入且内容寻址、只增不改，所以先取的数据库快照引用的
# 每个 blob 都已存在；不需要停服务。
#
# 环境变量：
#   AGENTBOX_BACKUP_PASSPHRASE_FILE  必填：口令文件（0600），口令不出现在命令行
#   AGENTBOX_ENV_FILE                读取 AGENTBOX_DATABASE_URL 的文件（默认 /etc/agentbox/agentbox.env；只读这一行）
#   AGENTBOX_DATABASE_URL            直接给出时优先于 env 文件
#   AGENTBOX_DATA_DIR                数据目录（默认 /var/lib/agentbox）
#   AGENTBOX_BACKUP_DIR              输出目录（默认 /var/backups/agentbox）
#   AGENTBOX_BACKUP_CONFIG           额外备份的非机密配置文件（空格分隔；默认 systemd 单元与 TLS 证书）
set -euo pipefail
umask 077

fail() { echo "backup: $*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || fail "须以 root 运行"
for c in pg_dump psql tar zstd gpg sha256sum python3; do command -v "$c" >/dev/null || fail "缺少 $c"; done

PASS_FILE="${AGENTBOX_BACKUP_PASSPHRASE_FILE:-}"
[ -n "$PASS_FILE" ] && [ -r "$PASS_FILE" ] || fail "须设置 AGENTBOX_BACKUP_PASSPHRASE_FILE（可读的口令文件）"
[ -s "$PASS_FILE" ] || fail "口令文件为空"
ENV_FILE="${AGENTBOX_ENV_FILE:-/etc/agentbox/agentbox.env}"
DB_URL="${AGENTBOX_DATABASE_URL:-}"
if [ -z "$DB_URL" ] && [ -r "$ENV_FILE" ]; then
  DB_URL=$(sed -n 's/^AGENTBOX_DATABASE_URL=//p' "$ENV_FILE" | tail -n 1)
fi
[ -n "$DB_URL" ] || fail "找不到 AGENTBOX_DATABASE_URL（环境变量或 $ENV_FILE）"
DATA="${AGENTBOX_DATA_DIR:-/var/lib/agentbox}"
OUT="${AGENTBOX_BACKUP_DIR:-/var/backups/agentbox}"
CONFIG="${AGENTBOX_BACKUP_CONFIG:-/etc/systemd/system/agentbox-demo.service /etc/agentbox/tls.crt}"
[ -f "$DATA/install_id" ] && [ -f "$DATA/bootstrap_token" ] || fail "$DATA 不是已引导的数据目录（缺 install_id 或 bootstrap_token）"

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
NAME="agentbox-backup-$STAMP"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/$NAME/data" "$WORK/$NAME/config" "$OUT"

export PGCONNECT_TIMEOUT=10
# 连接串拆成 libpq 的 PG* 环境变量交给 psql/pg_dump：口令不出现在命令行，也不写入文件。
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

echo "[1/5] 数据库逻辑备份（pg_dump -Fc）"
pg_dump -Fc --no-owner --no-privileges -f "$WORK/$NAME/db.dump"

echo "[2/5] 数据目录：blobs、install_id、bootstrap_token"
mkdir -p "$WORK/$NAME/data/blobs"
# 只备份内容寻址的 blobs/sha256/；blobs/tmp/ 是进行中的写入，不属于已提交的内容。
[ -d "$DATA/blobs/sha256" ] && cp -a "$DATA/blobs/sha256" "$WORK/$NAME/data/blobs/sha256"
cp -a "$DATA/install_id" "$DATA/bootstrap_token" "$WORK/$NAME/data/"

echo "[3/5] 非机密配置"
for f in $CONFIG; do
  [ -f "$f" ] || continue
  case "$f" in *.key|*.env|*token*|*secret*) fail "拒绝备份疑似机密的配置文件：$f" ;; esac
  cp -a "$f" "$WORK/$NAME/config/$(basename "$f")"
done

echo "[4/5] manifest"
blob_files=$(find "$WORK/$NAME/data/blobs/sha256" -type f 2>/dev/null | wc -l)
python3 - "$WORK/$NAME" "$STAMP" "$(hostname)" "$blob_files" \
  "$(q 'SELECT coalesce(max(version)::text, '"''"') FROM schema_migrations')" \
  "$(q 'SELECT count(*) FROM tasks')" "$(q 'SELECT count(*) FROM users' 2>/dev/null || echo 0)" \
  "$(q 'SELECT count(*) FROM blobs')" "$(q 'SELECT install_id FROM installation')" <<'PY'
import hashlib, json, os, sys
root, stamp, host, blob_files, schema, tasks, users, blobs_db, install_id = sys.argv[1:]
def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
files = {}
for d, _, names in os.walk(root):
    for n in names:
        p = os.path.join(d, n)
        files[os.path.relpath(p, root).replace(os.sep, "/")] = sha(p)
json.dump({
    "format": "agentbox-backup/1", "created_at": stamp, "host": host,
    "schema_version": schema, "install_id": install_id,
    "counts": {"tasks": int(tasks), "users": int(users), "blobs_db": int(blobs_db), "blob_files": int(blob_files)},
    "files": dict(sorted(files.items())),
}, open(os.path.join(root, "manifest.json"), "w"), indent=2, sort_keys=True)
PY

echo "[5/5] 压缩（zstd）并加密（GnuPG 对称 AES-256）"
ARCHIVE="$OUT/$NAME.tar.zst.gpg"
tar -C "$WORK" -cf - "$NAME" | zstd -q -19 -T0 |
  gpg --batch --yes --quiet --pinentry-mode loopback --passphrase-file "$PASS_FILE" \
    --symmetric --cipher-algo AES256 --s2k-digest-algo SHA512 --s2k-count 65011712 -o "$ARCHIVE"
(cd "$OUT" && sha256sum "$NAME.tar.zst.gpg" > "$NAME.tar.zst.gpg.sha256")
chmod 600 "$ARCHIVE" "$ARCHIVE.sha256"

python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print("计数：", json.dumps(m["counts"], ensure_ascii=False), "schema", m["schema_version"])' "$WORK/$NAME/manifest.json"
echo "归档：$ARCHIVE（$(du -h "$ARCHIVE" | cut -f1)）"
echo "SHA-256：$(cut -d' ' -f1 "$ARCHIVE.sha256")"
