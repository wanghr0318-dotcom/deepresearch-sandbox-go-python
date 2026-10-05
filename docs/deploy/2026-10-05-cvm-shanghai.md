# 演示服务器：腾讯云 CVM（上海）

> 用途：M2 真实模型演示与 M3 联调验收（宿主为原生 Linux，同时作为发布门槛的原生 Linux 复验目标）。M3 完成、最终备份与恢复验证后释放；M4 开始时按本文重新购买。
> 本文不记录公网 IP、密钥、口令或 API Key。

## 实例配置（2026-10-05 下单）

| 项 | 取值 |
|---|---|
| 计费 | 按量计费 |
| 地域 / 可用区 | 上海 / 上海五区 |
| 实例 | SA9.LARGE8（标准型 SA9，4 vCPU / 8 GiB，x86_64） |
| 镜像 | 公共镜像 Ubuntu Server 24.04 LTS 64 位（img-mmytdhbn） |
| 系统盘 | 通用型 SSD 云硬盘 100 GiB；无数据盘 |
| 网络 | Default-VPC / Default-Subnet；BGP；按流量计费，带宽上限 20 Mbps；无 IPv6 |
| 登录 | 用户 `ubuntu`，SSH 密钥对（私钥只在本机，不进仓库） |
| 保护 | 实例销毁保护开启；不开定时销毁；不装自动化助手 |

## 访问与安全组

- 入站只放通 TCP 22，来源为维护者当前公网 IP/32；出站全部放通（模型 API、搜索、抓取）。
- server 只监听 `127.0.0.1:8080`；PostgreSQL 5432、Redis 6379 只在本机。
- 日常与演示：`ssh -L 8080:127.0.0.1:8080 ubuntu@<IP>`，本机打开 `http://127.0.0.1:8080`。
- 面试官需要自己操作时：仅在面试窗口临时放通 TCP 443（尽量限定面试官 IP），server 以 `--tls-cert/--tls-key`（自签证书）监听 443，发放临时 token 并设预算上限，结束后立即删除规则并作废 token。大陆机房未备案，不作长期网站。

## 8 GiB 运行参数

- `agentbox server --memory-bytes 4294967296 --run-slots 3`（任务环境内存池 4 GiB，最多 3 个并发任务；单任务默认 1 GiB）。
- 宿主 4 GiB swapfile 兜底构建；任务环境自身 `memory.swap.max=0` 不变。

## 数据保存规则

- Git（私有仓库）只放代码与脱敏演示证据（`docs/evidence/`）。
- PostgreSQL dump 与 BlobStore：加密压缩后放私有 GitHub Release asset 或对象存储，记录 SHA-256；不进 Git。
- `.env` 与 API Key 永不进入备份与 Git。
- 释放前：最终备份 → 实际执行一次恢复验证 → 推送证据 → 释放实例、系统盘与公网 IP。

## 密钥与轮换（均不写进代码，可随时更换）

服务器上所有机密集中在 `~/.agentbox.env`（0600，只有 `ubuntu` 可读）：`AGENTBOX_MODEL_API_KEY`、`AGENTBOX_SEARCH_API_KEY`、`AGENTBOX_DATABASE_URL`（数据库口令为随机生成，旧的开发默认口令已被拒绝）。演示与启动时只读取这几个变量名对应的行，不 source 整个文件、不打印值。

| 机密 | 轮换方法 |
|---|---|
| 模型 / 搜索 Key | 在供应商控制台作废并新建 → 改 `~/.agentbox.env` 对应行 → 重启 server |
| 数据库口令 | `P=$(openssl rand -hex 24)`；`sudo -u postgres psql -c "ALTER ROLE agentbox PASSWORD '$P'"`；改 `AGENTBOX_DATABASE_URL` 行 → 重启 server |
| API token | 替换 `<data>/api.token`（0600）→ 重启 server；浏览器重新输入 |
| 缓存签名密钥 | 停止 server → `agentbox cache rotate-key --data-dir <data>` → 启动（上一代 kid 仍可验证一个周期） |
| TLS 证书 | 替换 `--tls-cert/--tls-key` 指向的文件 → 重启 server |
| SSH 密钥 | 腾讯云控制台重新绑定密钥对；私钥只在维护者本机 |

本地开发与 CI 的 `agentbox/agentbox` 是只监听本机的开发默认值，不用于服务器。Redis 无口令、只监听 127.0.0.1，缓存条目带 HMAC 签名（篡改即未命中）。释放服务器前删除 `~/.agentbox.env`。
