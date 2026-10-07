# M3 服务器真实验收记录

> 日期：2026-10-06。宿主：腾讯云 CVM 上海五区 SA9.LARGE8（4 vCPU / 8 GiB，x86_64），Ubuntu 24.04.4 LTS，内核 6.8.0-138-generic，cgroup v2，原生 Linux。
> 代码：`m3-batch`（含 Plan 9 缓存、Plan 10 工作台、serper 搜索、按端点的调用期限）。机密只从宿主 root 0600 文件按变量名读取，不打印、不出现在命令行。

## 1. root 全量测试（原生 Linux，连续三次）

`sudo CI=true go test -json -count=1 ./...`（PostgreSQL 16 + Redis 7，本机）：三次均 **1268 通过、0 失败**，唯一跳过为 `TestCacheBenefit`（测量而非正确性用例，只由 `bench/cache/run.sh` 运行；CI 允许清单已注明原因）。

## 2. 真实研究演示（serper + Redis 缓存 + 工作台）

`sudo bash ~/run-demo.sh <仓库> serper 127.0.0.1:6379 <仓库>/web/dist`（即 `scripts/demo-m2.sh`，`AGENTBOX_DEMO_SEARCH_PROVIDER=serper`、`AGENTBOX_DEMO_REDIS_ADDR`、`AGENTBOX_DEMO_WEB_DIR`）：**全部 15 步通过**。

| 指标 | 值 |
|---|---|
| 任务 | succeeded；plan → task-1…4 → report |
| 模型 | plan/report = kimi-k3，4 个总结 = kimi-k2.6；全部一次成功（报告 99.8 s，在 300 s 模型调用期限内） |
| 搜索 | serper（Google 结果），4 次 |
| Gateway 调用 | 22 个，全部 completed，来源均为 upstream |
| 缓存计数 | miss 16、hit 0（全新缓存的首次运行，预期如此；命中、合并与故障行为由 E21–E25 覆盖，收益见 [缓存收益](2026-10-05-m3-cache-benefit.md)） |
| 证据 | 12 条，每条 blob 在 BlobStore 中并校验 sha256 |
| 估算费用 | 339,809 µ$（0.34 USD，配置单价） |
| 不变量 / 泄漏 / Key | `verify-invariants --quiescent` 通过；无残留环境、进程或带进程的 cgroup；沙箱进程、日志、事件、inspect 均不含任何 Key（含 serper Key） |

## 3. HTTPS 常驻服务（工作台同源提供）

`deploy/systemd/agentbox-demo.service`：`--listen 0.0.0.0:443`、自签证书（SAN = IP <server>）、`--web-dir /opt/agentbox-web`、serper、Redis、4 GiB 内存池、3 个 run slot、单任务预算 2 USD（上限 3 USD）。从公网（维护者本机）探测：

| 请求 | 结果 |
|---|---|
| `GET https://<server>/` | 200 `text/html`（工作台） |
| `GET /status`、`GET /tasks` 无 token | 401 `unauthorized` |
| `http://<server>/`（明文 80） | 不可达（安全组未放通） |

浏览器中由项目负责人试用（工作台与随后的用户账号功能）待 Plan 11 完成后进行，结果追加到 [用户账号验收记录](2026-10-06-m3-accounts.md)。

## 演示中暴露并修复的问题

1. 报告调用超过固定的 120 s 调用期限（`unknown`，worker 以新 ID 重做后成功）→ 按端点的调用期限：搜索/抓取 120 s（`--call-deadline`），模型 300 s（`--model-call-deadline`）；worker 默认等待 330 s（`AGENTBOX_GATEWAY_TIMEOUT_S`）。
2. 演示检查把 `unknown` 当作未结算 → `unknown` 是终态；每个模型步骤以最后一次调用判定；模型取自 journal。
3. CI 的 root 作业拒绝 `TestCacheBenefit` 的有意跳过 → 加入允许清单并注明原因。

## 完整演示输出

```

[01] 环境检查：root、cgroup v2、PostgreSQL、python3、模型与搜索配置
     OK  root，/sys/fs/cgroup 为 cgroup2fs，内核 6.8.0-138-generic x86_64
     OK  PostgreSQL 127.0.0.1:5432 可达
     OK  /usr/bin/python3：Python 3.12.3
     OK  AGENTBOX_MODEL_API_KEY 已设置
     OK  模型上游：https://api.moonshot.cn/v1
     OK  搜索：serper；AGENTBOX_SERPER_API_KEY 已设置（作为 AGENTBOX_SEARCH_API_KEY 交给 server）
     OK  模型路由：编排（计划、报告）= kimi-k3；worker（总结）= kimi-k2.6（server 默认模型）；白名单 kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3
     OK  题目：固态电池的产业化进展与主要技术瓶颈

[02] 准备全新的数据目录 /var/lib/agentbox-demo-m2 与数据库 agentbox_demo_m2
     OK  已新建空库 agentbox_demo_m2
     OK  数据目录已清空：/var/lib/agentbox-demo-m2

[03] 安装 worker 包到 /opt/agentbox（agentbox_worker、deepresearch）
     OK  18 个 .py 文件；/opt/agentbox/deepresearch 存在

[04] 构建（CGO_ENABLED=0）并运行 doctor
     OK  ~/<checkout>/bin/agentbox
         [通过] cgroup v2：cgroup v2、cgroup.kill（内核 ≥ 5.14）与必需控制器可用；内核 6.8.0-138-generic
         [通过] 新挂载 API（open_tree/mount_setattr）：open_tree 与 mount_setattr 可用
         [通过] close_range：close_range 可用（空区间返回 EINVAL）
         [通过] user namespace 可创建：可创建 user namespace
         宿主环境检查通过

[05] 启动 agentbox server（生产启动器、Gateway、Worker = python3 -m deepresearch）
         agentbox server --data-dir /var/lib/agentbox-demo-m2 --listen 127.0.0.1:8080 --worker-argv python3,-m,deepresearch --model-base-url https://api.moonshot.cn/v1 --model-name kimi-k2.6 --models kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3 --search-provider serper --web-dir ~/<checkout>/web/dist --redis-addr 127.0.0.1:6379
     OK  server pid 81863 已就绪（http://127.0.0.1:8080，日志 /tmp/agentbox-demo-m2.BREBYV/server.log）
         工作台：浏览器打开 http://127.0.0.1:8080/（远程主机经 ssh -L 8080:127.0.0.1:8080 隧道访问）

[06] 提交研究任务
     OK  task_id task_fcbcc69f5fa607c3edc9822589c5de6a，spec {"topic": "固态电池的产业化进展与主要技术瓶颈", "orchestrator_model": "kimi-k3", "worker_model": "kimi-k2.6"}

[07] G3：Worker 运行中，宿主读取沙箱内每个进程的 environ 与 cmdline
         pid 81908 uid 1000000：/proc/self/exe init；环境变量 0 个：（无）
         pid 81917 uid 1001000：python3 -m deepresearch；环境变量 1 个：PYTHONPATH
     OK  沙箱进程的 environ 与 cmdline 不含 Key 的值，也没有 AGENTBOX_MODEL_API_KEY / AGENTBOX_SEARCH_API_KEY / AGENTBOX_SERPER_API_KEY

[08] 等待 task_terminal（最长 1800s）
         task_created: {}
         control_applied: {"status": "queued", "control_version": 1}
         attempt_created: {"env_id": "f053f3764e73526ac36e7e4f03624160", "attempt_no": 1}
         ready: {"v": 1, "ts": "2026-10-05T17:06:10.323Z", "seq": 1, "mode": "task", "type": "ready", "worker": {"name": "deepresearch", "version": "0.1.0"}…
         checkpoint: {"v": 1, "ts": "2026-10-05T17:06:38.286Z", "seq": 2, "refs": ["b9341a932ceb025d9faf6df926c98498bdf6eaf9a62a93bb75a4bf76abc1fa7b"], "type": "…
         checkpoint_committed: {"step_id": "plan", "commit_seq": 1, "checkpoint_id": "00cf821da68a42049e5da2b0944f312f"}
         checkpoint: {"v": 1, "ts": "2026-10-05T17:07:26.583Z", "seq": 3, "refs": ["b9341a932ceb025d9faf6df926c98498bdf6eaf9a62a93bb75a4bf76abc1fa7b", "07d5d61a2…
         checkpoint_committed: {"step_id": "task-1", "commit_seq": 2, "checkpoint_id": "0ff52809578d4cd5a6533860fbbce740"}
         checkpoint: {"v": 1, "ts": "2026-10-05T17:08:09.157Z", "seq": 4, "refs": ["b9341a932ceb025d9faf6df926c98498bdf6eaf9a62a93bb75a4bf76abc1fa7b", "07d5d61a2…
         checkpoint_committed: {"step_id": "task-2", "commit_seq": 3, "checkpoint_id": "853c259403984089842d536fbf37d905"}
         checkpoint: {"v": 1, "ts": "2026-10-05T17:09:18.983Z", "seq": 5, "refs": ["b9341a932ceb025d9faf6df926c98498bdf6eaf9a62a93bb75a4bf76abc1fa7b", "07d5d61a2…
         checkpoint_committed: {"step_id": "task-3", "commit_seq": 4, "checkpoint_id": "ced419b1c4334d548ec3e8c98ed6bc88"}
         checkpoint: {"v": 1, "ts": "2026-10-05T17:09:59.967Z", "seq": 6, "refs": ["b9341a932ceb025d9faf6df926c98498bdf6eaf9a62a93bb75a4bf76abc1fa7b", "07d5d61a2…
         checkpoint_committed: {"step_id": "task-4", "commit_seq": 5, "checkpoint_id": "427f7376f2b9400c8b97092ad5ed92f2"}
         artifact: {"v": 1, "ts": "2026-10-05T17:11:39.786Z", "seq": 7, "path": "report.md", "type": "artifact", "media_type": "text/markdown", "visibility": "…
         artifact_saved: {"sha256": "b42a8ede2b95804df3f2c178069c884bb1c4c49fde62cb9447669ad76018d2d8", "version": 1, "artifact_id": "report"}
         checkpoint: {"v": 1, "ts": "2026-10-05T17:11:39.793Z", "seq": 8, "refs": ["b9341a932ceb025d9faf6df926c98498bdf6eaf9a62a93bb75a4bf76abc1fa7b", "07d5d61a2…
         checkpoint_committed: {"step_id": "report", "commit_seq": 6, "checkpoint_id": "0d97a11ccfbf4b0bb0dd6e003412146e"}
         result: {"v": 1, "ts": "2026-10-05T17:11:39.801Z", "seq": 9, "type": "result", "outputs": ["report"], "summary": "当前主流液态锂离子电池能量密度（150–300 Wh/kg）已接近物…
         task_terminal: {"attempt_id": "c1f08960a72be677a6b43af6aaa3b02b", "task_status": "succeeded", "outcome_class": "succeeded", "status_reason": "succeeded", "…
     OK  succeeded；checkpoints：plan → task-1 → task-2 → task-3 → task-4 → report

[09] inspect：Gateway 调用明细（端点、模型、tries、费用、延迟；不含请求与响应正文）
         root/plan/chat/1         /v1/chat/completions   kimi-k3      completed  tries=1(ok) cost=15210µ$ latency=27948ms
         root/task-1/search/1     /v1/search             -            completed  tries=1(ok) cost=2000µ$ latency=2070ms
         root/task-1/fetch/1      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=245ms
         root/task-1/fetch/2      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=14532ms
         root/task-1/fetch/3      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=272ms
         root/task-1/chat/1       /v1/chat/completions   kimi-k2.6    completed  tries=1(ok) cost=44613µ$ latency=31116ms
         root/task-2/search/1     /v1/search             -            completed  tries=1(ok) cost=2000µ$ latency=1325ms
         root/task-2/fetch/1      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=145ms
         root/task-2/fetch/2      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=608ms
         root/task-2/fetch/3      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=296ms
         root/task-2/chat/1       /v1/chat/completions   kimi-k2.6    completed  tries=1(ok) cost=53337µ$ latency=40127ms
         root/task-3/search/1     /v1/search             -            completed  tries=1(ok) cost=2000µ$ latency=1321ms
         root/task-3/fetch/1      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=79ms
         root/task-3/fetch/2      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=177ms
         root/task-3/fetch/3      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=225ms
         root/task-3/chat/1       /v1/chat/completions   kimi-k2.6    completed  tries=1(ok) cost=88950µ$ latency=67972ms
         root/task-4/search/1     /v1/search             -            completed  tries=1(ok) cost=2000µ$ latency=1151ms
         root/task-4/fetch/1      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=805ms
         root/task-4/fetch/2      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=873ms
         root/task-4/fetch/3      /v1/fetch              -            completed  tries=1(ok) cost=0µ$ latency=1428ms
         root/task-4/chat/1       /v1/chat/completions   kimi-k2.6    completed  tries=1(ok) cost=46593µ$ latency=36671ms
         root/report/chat/1       /v1/chat/completions   kimi-k3      completed  tries=1(ok) cost=83106µ$ latency=99801ms
         调用 22 个；总费用 339809 微美元（0.339809 USD，按配置价格估算）
     OK  模型路由：plan/report = kimi-k3，任务内 chat = kimi-k2.6
     OK  全部调用处于终态：completed 22；每个模型步骤的最后一次调用 completed
     OK  调用来源：upstream 22
     OK  缓存计数（/status）：{'breaker_open': 0, 'bypass': 0, 'coalesced': 0, 'error': 0, 'hit': 0, 'integrity_failure': 0, 'miss': 16}

[10] 研究报告（从 BlobStore 读取并校验 sha256）：前 40 行与证据列表
         report sha256:b42a8ede2b95804df3f2c178069c884bb1c4c49fde62cb9447669ad76018d2d8（10522 字节，74 行）
         ---- 前 40 行 ----
         | # 固态电池的产业化进展与主要技术瓶颈
         | 
         | # 固态电池的产业化进展与主要技术瓶颈研究报告
         | 
         | ## 1. 背景概览
         | 
         | 当前主流液态锂离子电池能量密度（150–300 Wh/kg）已接近物理极限，全固态电池凭借 400–500 Wh/kg 的能量密度潜力，被视为实现续航突破 1000 公里（"北京到上海中途不充电"）的下一代核心技术 [2]。全球固态电池产业呈现"两步走"格局：第一步以半固态为量产主力，第二步以 2027 年为关键节点实现全固态小批量装车、2030 年进入大规模商业化，当前整体正处于"半固态规模化导入期"向"全固态小批量验证期"过渡的阶段 [2][3]。与此同时，中国率先发布全球首个车用固态电池国家标准 GB/T 43568-2026，预计于 2026 年 7 月 1 日实施，严格区分半固态与全固态，为产业化奠定制度基础 [2]。市场层面，固态电池被普遍认为处于高速增长通道，电动汽车消费需求是市场扩张的首要驱动力 [10][11][12]。
         | 
         | ## 2. 核心洞见
         | 
         | **洞见一：半固态电池已实现规模化装车，是当前产业化过渡的绝对主力。** 2025 年国内半固态电池装车量达 31.7 GWh，同比增长 272%，占全年动力电池累计装机量（769.7 GWh）约 4.1%，商业化导入速度远超全固态路线，印证了 2026 年为"半固态量产主力年"的产业判断 [1][2]。
         | 
         | **洞见二：全固态量产时间表普遍后移至 2027—2030 年，产业化节奏较早期预期更保守。** 丰田由原定 2026 年推迟至 2027—2028 年进入实用化阶段、2030 年以后大规模生产；宁德时代预计 2027 年实现小批量生产；比亚迪计划 2027 年前后启动批量示范装车、2030 年后大规模商业化；苗圩指出全球范围内 2027 年前后仅能实现小批量生产 [2][3]。
         | 
         | **洞见三：硫化物路线已成为全固态电池的主流聚焦方向，但材料体系间权衡显著。** 丰田、本田、宁德时代、比亚迪、吉利等企业均采用或倾向硫化物路线 [3]；硫化物室温电导率高、接近液态水平，但对空气/水分极敏感、化学稳定性差且制造成本高 [8][9]。行业形成"全固态主攻硫化物/卤化物复合、半固态采用氧化物/聚合物复合"的分化格局 [9]。
         | 
         | **洞见四：固-固界面阻抗与锂枝晶是最核心技术瓶颈，成本高企是产业化最大障碍。** 固态电池固-固界面阻抗高达 20–50 Ω·cm²，与量产目标（≤10 Ω·cm²）差距悬殊，并与锂枝晶形成"枝晶生长—阻抗攀升—性能衰减"的恶性循环 [7]；全固态电芯成本约 2 元/Wh，为传统液态电池的 3–5 倍，设备投入亦为液态产线的 3–5 倍 [7]。
         | 
         | **洞见五：市场呈指数级增长预期，但统计口径分化显著，引用时须明确边界。** 不同机构对 2025 年市场规模的测算从 2480 万美元（仅 EV 固态电池）到 27.8 亿美元（含消费电子等多场景）不等，基期规模相差一个数量级；2026—2034/2035 年复合年增长率预测介于 36.4%–61.2% [10][11][12]。
         | 
         | ## 3. 证据与数据
         | 
         | ### 3.1 产业化进展与企业时间表
         | - 2025 年国内半固态电池装车量 31.7 GWh，同比增长 272%，占全年动力电池装机量约 4.1% [1][2]。
         | - 丰田：量产时间表推迟至 2027—2028 年实用化，2030 年以后大规模生产 [2][3]。
         | - 宁德时代：主攻硫化物路线，目前开展 20 安时样件试制，预计 2027 年小批量生产 [3]。
         | - 比亚迪：2027 年前后启动全固态批量示范装车，2030 年后大规模商业化；璧山 20 GWh 产线预计 2026 年第三季度开工 [2][3]。
         | - QuantumScape、卫蓝等企业的量产时间表与装车数据：暂无相关信息。
         | 
         | ### 3.2 技术路线与研发路径
         | - 欧阳明高分阶段技术目标：2025—2027 年开发石墨/低硅负极硫化物全固态电池（200–300 Wh/kg）；2027—2030 年开发高硅负极方案（400 Wh/kg、800 Wh/L）；2030—2035 年攻关锂负极（500 Wh/kg、1000 Wh/L）[3]。
         | - 聚合物电解质：黏弹性好、机械加工性能优、质量轻，但电导率低、耐氧化性差 [6][9]。
         | - 氧化物电解质：研究历史较长，晶体结构类型包括钙钛矿型、NASICON 型、Garnet 型；本征安全性好、制造成本相对较低，但加工性能差、电导率低 [6][9]。
         | - 硫化物电解质：离子电导率高、接近液态水平、机械加工性能好，但对空气/水分极敏感、化学稳定性差、制造成本高 [6][8][9]。
         | - 卤化物电解质：离子电导率能满足要求、成本相对较低、电化学窗口较宽，目前多用于正极包覆 [9]。
         | 
         | ### 3.3 技术瓶颈量化指标
         | - 固-固界面阻抗：固态电池为 20–50 Ω·cm²，液态电池通常＜5 Ω·cm²，规模化量产要求降至 ≤10 Ω·cm²；已有界面包覆、等静压、中科院自适应界面层等缓解方案 [7]。
         | - 锂枝晶：锂金属负极充放电体积膨胀超 300%，枝晶可穿透电解质导致短路并形成非均匀 SEI 膜；复合电解质设计、人工界面层、电极梯度致密化等技术可降低风险 [7][8]。
         | - 成本：全固态电芯成本约 2 元/Wh，为液态电池的 3–5 倍；半固态高出液态电池 1–2 倍；设备投入为液态产线的 3–5 倍，硫化物路线需超干环境与干法电极工艺，氧化物路线需高温烧结设备 [7]。
         ---- 证据 ----
         | ## 证据
         | 
         | - [1] 固态电池研究进展到哪一步了？何时能量产？ — https://m.36kr.com/p/3878339285577737 — sha256:07d5d61a27054d46bb68ecbf1af6c521314659de2f012054df663d5552bdcb74
         | - [2] 固态电池量产倒计时：丰田、宁德、比亚迪的三条路 — https://www.tmtpost.com/8035711.html — sha256:3e8c6ba5a578a48af82c4c96cf81feefa8b4e6d2d4a300f45bcb5409f335a4bf
         | - [3] 苗圩：固态电池技术工艺未成熟，距离大规模量产需更长时间 — https://www.yicai.com/news/102475998.html — sha256:071b5985e8981b3691284d4e47065e199740f91d54b712b0edd8c069f0dfcbc7
         | - [4] 固态电池技术路径都难在哪里？氧化物固态电解质破局？ — https://zhuanlan.zhihu.com/p/688843508 — sha256:52ca15dd1af35f50e36de2f3f98a0695cf6844f394374bf663851a84a8a7ec74
         | - [5] 固态电池行业观察：技术路线、商业化、行业影响 — https://www.3c-ate.com/industry/350.html — sha256:ead3ff735184e6a28fff3d0008f93853d38e1684a2b80ad132de4395ab4a2cd0
         | - [6] 全固态锂电池的固态电解质进展与专利分析 — https://esst.cip.com.cn/article/2021/2095-4239/2095-4239-2021-10-1-77.shtml — sha256:f2fc537e4a802d2949dabe207b2ff1fd93a468c6069cc6d92bd09bb06552dde3
         | - [7] 2026年纯固态电池电动汽车的技术瓶颈分析 — https://www.nxebattery.com/news/industry/20260311772.html — sha256:971585273686563af29e597d65cc6d0fc9b914f86c4d4e82f76747f3619eddc9
         | - [8] 固态电池到底什么时候能量产？目前最大的技术瓶颈是什么？ — https://zhuanlan.zhihu.com/p/2073036638121472186 — sha256:30d5fccd4e77be56e1bc97eb7577a191cda83358b2c66c0a2994171085c63423
         | - [9] 加速固态电池产业化 — https://lw.xinhuanet.com/20251015/1b31137c1ab542f795209974ee84e92c/c.html — sha256:bb6b1022f8c23a8d00f1189e58fa230ac6e0181987358e892fd99436092cde5c
         | - [10] 下一代固态电池市场规模报告（2035年） — https://www.gminsights.com/zh/industry-analysis/ev-next-generation-solid-state-battery-market — sha256:aa64eee7db2f8b232324f14936b3c6a72fde2ffa9c9ab45dae9a803377e8bf77
         | - [11] 电动汽车固态电池市场规模、份额|预测[2026-2034] — https://www.fortunebusinessinsights.com/zh/ev-solid-state-battery-market-115751 — sha256:03a390b2f6dca1c9a5a816402212bc2dcff2b11489cbd92c0b450026dffe0633
         | - [12] 固态电池市场规模、份额、增长、分析（至2034年） — https://straitsresearch.com/zh/report/solid-state-battery-market — sha256:11cf054f43dfe590b34a5409847728e80ff1a3b10fc80331f178121d8edc9620
     OK  证据 12 条，每条的 blob 均在 BlobStore 中

[11] 清理：环境停止、销毁，环境目录删除
     OK  attempt 的环境 cleanup_state=done，/var/lib/agentbox-demo-m2/envs 为空

[12] 正常停止 server（SIGTERM）
     OK  退出码 0

[13] verify-invariants --quiescent
         不变量检查通过
     OK  通过

[14] 泄漏检查：agentbox cgroup、环境目录、沙箱进程
         注意：其他安装或测试留下的空 cgroup /sys/fs/cgroup/agentbox-1207181c5b8372aa9f8c2b82a9229535（不属于本次演示）
         注意：其他安装或测试留下的空 cgroup /sys/fs/cgroup/agentbox-4682c40f634abeca189bc2d955e4180c（不属于本次演示）
     OK  没有环境目录、没有带进程或环境的 agentbox cgroup（本安装的空 cgroup 已删除）、没有沙箱进程

[15] Key 泄漏检查：server 日志、事件流、inspect 输出与演示日志目录
     OK  events.jsonl inspect.json server.log watch.err 均不含 Key 的值

演示完成：全部 15 步通过。日志目录：/tmp/agentbox-demo-m2.BREBYV
exit=0
```
