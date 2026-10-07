# 备份与恢复验证记录

> 日期：2026-10-06。对象：演示服务器上的常驻服务（数据库 `agentbox_svc`、数据目录 `/var/lib/agentbox`）。脚本：`scripts/backup.sh`、`scripts/restore.sh`。
> 本记录不含口令、token、cookie 或 Key。

## 备份

`sudo AGENTBOX_BACKUP_PASSPHRASE_FILE=/root/.agentbox-backup-pass bash scripts/backup.sh`（服务在线，不停机）：

| 项 | 值 |
|---|---|
| 归档 | `agentbox-backup-20261006T013948Z.tar.zst.gpg`（248 KiB；zstd + GnuPG 对称 AES-256） |
| SHA-256 | `01860fd3eb9865f70bb249ee2b2006dcbffde3b009b544e0cc2de4b205e512e7` |
| 内容 | `db.dump`（pg_dump -Fc）、`data/blobs/sha256/…`（25 个 blob）、`data/install_id`、`data/bootstrap_token`、`config/agentbox-demo.service`、`config/tls.crt`、`manifest.json`（30 个文件的 SHA-256 与计数） |
| 计数 | tasks 3、users 4、blobs（库中）24、schema 5 |
| 不含 | `api.token`、`cache.key`、env 文件（Key、数据库口令）、TLS 私钥——解包列表中检查为 0 个 |
| 错误口令 | 解密被拒绝 |

第一次备份把 `blobs/tmp/`（进行中的写入）也带上了；已改为只备份 `blobs/sha256/`，并删除了那份归档。

## 异地副本

- 维护者本机：`F:\go-agentbox\backups\`（git 忽略），`sha256sum -c` 通过。
- 私有仓库的**草稿** release `backup-20261006T013948Z`（仅仓库成员可见，不建标签），附归档与 `.sha256`。
- 口令：维护者本机 `F:\go-agentbox\.backup-passphrase`（git 忽略，64 个十六进制字符）与服务器 `/root/.agentbox-backup-pass`（root 0600）。**口令应另存一份到密码管理器**——口令丢失则归档不可恢复。

## 恢复验证（真实执行）

恢复所用归档是**从 GitHub release 重新下载**的副本（SHA-256 一致），恢复到全新的数据库 `agentbox_restore` 与全新的数据目录 `/var/lib/agentbox-restore`：

| 步骤 | 结果 |
|---|---|
| 归档 SHA-256 | 与 `.sha256` 一致 |
| 解密、解压 | 成功 |
| manifest 中 30 个文件的 SHA-256 | 全部一致 |
| pg_restore 到空库 | 成功 |
| 25 个 blob 的内容哈希与文件名 | 全部一致 |
| 恢复后计数与 install_id | tasks 3、users 4、blobs 24、install_id 一致 |
| `agentbox verify-invariants --quiescent` | 通过 |
| 在恢复的安装上启动 server（127.0.0.1:8090） | 进入 normal 模式；日志 0 条 ERROR |
| 运维 `GET /tasks` | 3 个任务（与备份一致） |
| 下载已完成研究的报告 | 8023 字节，`ETag` 等于正文 SHA-256 |
| inspect | 14 个调用、243,785 µ$（与原安装一致） |
| 备份前注册的用户 `<test-user-1>` 用已知口令登录 | 200；`/auth/me` 返回该用户（密码哈希随备份恢复） |

验证后删除了恢复用的数据库与数据目录（它们与原安装有相同的 install_id，留着会造成混淆）。

## 经验与注意

- 恢复出的安装与原安装有**相同的 install_id**，同一宿主上两者不能同时运行（cgroup 与 UID 区间按安装命名，启动清理可能互相干扰）。本次验证期间停止了常驻服务约 1 分钟；当时一个用户研究正在运行，服务重启后按设计自动恢复（新 attempt 继续执行）。**今后的恢复演练应在另一台主机上进行**（例如 M4 开始时的新服务器），或在维护窗口内进行。
- 验证脚本中的 `grep -c`（零匹配时退出码 1）在 `set -e` 下提前中止，导致常驻服务没有被立即重启（约 1 分钟后手动启动）。演练脚本中重启服务的步骤应放在 `trap … EXIT` 中。
