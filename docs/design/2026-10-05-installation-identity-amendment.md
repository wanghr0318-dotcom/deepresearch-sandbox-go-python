# 修订提案：安装身份引导的数据目录引导令牌（规格 §7.4）

> 状态：**已审阅（两条更正已并入：§3 提交结果未知、§2 令牌严格解析），已在 `m1-4-persistence` 实现**（Plan 4 Task 9）。Plan 4 的验收在本修订经 CI 验证之前保持待定。
> 范围：只修改规格 §7.4 的安装身份引导，以及 Plan 4 中实现它的 `datadir`、`ownership`、`persistence/postgres`（迁移与安装存储）。不涉及其他设计决定。

## 1. 问题

§7.4 的决策表中，"`installation.state = pending` + 数据目录无 `install_id` → 继续中断的引导"无法区分两种情况：

- 本数据目录在初始化事务提交后、写入 `install_id` 前崩溃（应继续）；
- **另一个数据目录**指向同一数据库（应拒绝）。

数据目录 A 在该窗口崩溃后，另一个数据目录 B 启动时取得 advisory lock（A 已退出），看到 pending 且自己没有身份文件，便写入 A 的 `install_id` 并置 complete。A 重启后身份一致而正常启动——两个数据目录（可能在两台主机上）先后合法地拥有同一安装，主机本地资源（`owner.json`、cgroup 命名、恢复扫描）随之混淆。advisory lock 只防同时运行，不防先后接管。

## 2. 方案：数据目录引导令牌，绑定到 pending 记录

- **令牌文件** `<data>/bootstrap_token`：32 字节随机数（`crypto/rand`），以十六进制存储，权限 0600；写入方式与 `install_id` 相同（临时文件 → 写入 → fsync → rename → fsync 父目录）。令牌是数据目录内容本身的持久事实，不依赖路径（路径在不同主机、容器与挂载下可能相同或改变）。
- **数据库**：`installation` 增加列 `bootstrap_token_hash bytea`，存令牌的 SHA-256。只存哈希：能读数据库的人不能据此伪造数据目录。
- **严格解析**：文件不存在、读取失败、格式错误是三种不同状态。令牌必须严格解析为 32 字节（64 个小写十六进制字符，可带一个结尾换行）；读取失败或格式错误直接使本次启动失败，**不当作不存在**——否则一次读错误会被误判为首次安装而生成新身份。已存在的有效令牌只能复用，写入时若文件已存在则失败，不覆盖、不重新生成。数据库中的非空哈希必须为 32 字节（列上的检查约束；读取时再次校验，不符则启动失败）。
- **用途**：只用于判定"谁可以继续一个 pending 安装"。安装 complete 之后不再读取令牌；令牌文件保留（不删除，避免引入新的崩溃窗口）。

## 3. 持久化顺序

启动顺序不变：确认数据目录 → `flock(<data>/agentbox.lock)` → advisory lock → **安装身份引导** → 迁移 → 恢复。引导步骤：

1. 读取数据库状态（迁移表、agentbox 表、`installation` 记录及其令牌哈希）与数据目录状态（`install_id`、`bootstrap_token`）。
2. 按第 4 节决策。全新安装时：
   1. **令牌**：数据目录已有令牌则复用；否则生成并持久写入。令牌在初始化事务之前持久化。
   2. **初始化事务**（单一事务）：创建 `schema_migrations`，执行**全部内嵌迁移**，插入 `installation(新 install_id, pending, hash(令牌))`。事务未提交则库仍为空。
   3. **身份文件**：持久写入 `install_id`。
   4. **完成**：`UPDATE installation SET state = 'complete' WHERE install_id = $1`（已是 complete 时幂等）。
3. 继续 pending 时，执行上面的第 3、4 步。

**为何令牌必须先于事务持久化**：若事务先提交、令牌后写，两者之间崩溃会留下一条带哈希、但本数据目录没有对应令牌的 pending 记录——本数据目录将无法继续，只能人工处理。先写令牌，任何时刻的 pending 记录都能由持有令牌的数据目录继续。

## 4. 决策表（修订）

只修改带 ★ 的行，其余行与现行 §7.4 相同。

| 数据库状态 | `install_id` | `bootstrap_token` | 处理 |
|---|---|---|---|
| 空库 | 不存在 | 不存在或存在 ★ | 全新安装（有令牌则复用） |
| 空库 | 存在 | 任意 | 拒绝：数据库丢失或被替换 |
| 有 `schema_migrations`，无 `installation` 记录 | 任意 | 任意 | 拒绝：外部改动或损坏 |
| 无 `schema_migrations` 但有 agentbox 表 | 任意 | 任意 | 拒绝：未知 schema |
| `pending`，有哈希 ★ | 不存在 | 存在且哈希一致 | 继续：写身份文件 → 置 complete |
| `pending`，有哈希 ★ | 不存在 | 不存在或哈希不一致 | **拒绝**：该 pending 安装由另一个数据目录发起，或本目录的令牌已丢失 |
| `pending`，无哈希（旧记录）★ | 不存在 | 任意 | **拒绝**：旧 pending 记录无法证明由本目录发起（见第 6 节） |
| `pending`（有无哈希均可） | 与数据库一致 | 不检查 | 置 complete |
| `pending` 或 `complete` | 与数据库不一致 | 任意 | 拒绝 |
| `complete` | 不存在 | 任意 | 拒绝：数据目录丢失或被替换 |
| `complete` | 一致 | 不检查 | 正常启动 |

"`pending` + 身份文件一致 → 置 complete"不检查令牌：身份文件只会由发起者（初始化时）或通过令牌校验的继续路径写入，因此一致的身份文件本身就证明了归属。人工复制他人的数据目录文件不在威胁模型内（与现行 complete 行相同）。

## 5. 崩溃窗口

| 崩溃点 | 重启时的状态 | 处理 |
|---|---|---|
| 令牌临时文件写入中（rename 之前） | 空库；无令牌（残留临时文件） | 全新安装，生成新令牌；残留临时文件不被读取 |
| 令牌已持久，初始化事务未提交（含事务中途、COMMIT 被服务端拒绝） | 空库；有令牌 | 全新安装，复用同一令牌 |
| 初始化事务已提交，或提交结果未知 | pending（本目录令牌的哈希）；无 `install_id` | 继续：哈希一致 → 写身份文件 → 置 complete |
| 身份文件临时文件写入中 | pending；无 `install_id` | 同上（令牌一致则继续） |
| 身份文件已持久，未置 complete | pending；`install_id` 一致 | 置 complete |
| 置 complete 的提交结果未知 | pending 或 complete；`install_id` 一致 | 置 complete（幂等）或正常启动 |

**初始化事务的提交结果未知时，本次启动失败退出，不在同一进程内重新检查**：原事务可能尚未结束，同进程内查询为空不能证明它未提交。下次启动重新取得 flock 与 advisory lock 之后，读取到的状态必然落在上表的第二或第三行之一（原事务此时已结束），两者都由本数据目录按决策表安全地继续。实现上，初始化事务不重试，提交结果未知时返回 `persistence.ErrCommitUnknown`，`Bootstrap` 不返回 install_id。

## 6. 拒绝其他数据目录，与既有 pending 记录

**另一个数据目录**（令牌不同或没有令牌）在任何窗口启动：

- 库为空 → 正常全新安装（与本目录无关）；
- 库为 pending（他人的哈希）且自己无身份文件 → 拒绝，原因写明"该 pending 安装由另一个数据目录发起；若确认原数据目录已永久丢失，需人工处理"；
- 库为 complete 且自己无身份文件 → 拒绝（现行行为）。

两个数据目录同时启动：advisory lock 使引导串行；后者看到前者的 pending 或 complete 记录并被拒绝。同一数据目录的两个进程：`flock` 拒绝后者。

**既有 pending 记录**：Plan 4 已推送（Draft PR #10）但未合并、未发布，可能存在用其代码创建的开发或 CI 数据库，其 `installation` 没有该列。处理：

- **不修改已有迁移 `0001`**，新增迁移 `0002_bootstrap_token.sql`：`ALTER TABLE installation ADD COLUMN bootstrap_token_hash bytea`（可空）。修改 `0001` 会让已按旧 `0001` 建成的库与 `schema_migrations` 的记录无声分歧。
- 新安装的初始化事务执行全部内嵌迁移，因此新记录总是带哈希。
- 引导先于迁移执行：读取旧库时若该列不存在，按"无哈希"处理。
- 旧 `complete` 记录：令牌不参与 complete 的判定，行为不变；之后 `0002` 正常加列。
- 旧 `pending` 记录：有一致的身份文件 → 置 complete（身份文件证明归属）；没有身份文件 → 拒绝（表中的"无哈希"行），因为无法证明由本目录发起。处理方式为人工确认后清空该数据库并重新引导；不提供自动接管。

## 7. 契约变更

| 位置 | 变更 |
|---|---|
| 规格 §7.4 | 引导步骤加入令牌（第 3 节）；决策表按第 4 节修订；"初始化事务执行初始迁移"改为"执行全部内嵌迁移" |
| 迁移 | 新增 `0002_bootstrap_token.sql`（第 6 节） |
| `datadir` | 新增令牌文件：`Read() (token []byte, exists bool, err error)`、`Write(token []byte) error`，持久写入方式与 `IDFile` 相同 |
| `ownership` | `FileState` 增加 `TokenHash []byte`；`Installation` 增加 `TokenHash []byte`（旧记录为 nil）；`InstallStore.InitializeInstallation(ctx, installID string, tokenHash []byte)`；`Bootstrap` 增加令牌文件参数并在全新安装时按第 3 节顺序准备令牌；`Decide` 按第 4 节 |
| `persistence/postgres` | `InspectInstallation` 读取哈希（列不存在时为 nil）；`InitializeInstallation` 执行全部内嵌迁移并写入哈希 |

## 8. 验收（实现后）

在真实 PostgreSQL 上（扩展现有 `TestInstallationBootstrapE46`）与纯决策表测试中覆盖：

1. 数据目录 A 在初始化事务提交后崩溃 → 数据目录 B 被拒绝且库不变 → A 重启后完成引导。
2. 令牌写入后、事务提交前崩溃 → 重启复用同一令牌，库中哈希与令牌一致。
3. 初始化事务提交结果未知（提交后回复丢失）→ 本次引导失败且不返回 install_id；以同一数据目录再次引导（模拟下次启动）→ 继续并完成。
4. 令牌文件损坏 → 启动失败，数据库仍为空；读取失败（如路径为目录）→ 返回错误而不是"不存在"；已有令牌不被覆盖。
5. pending 记录存在而本目录令牌丢失 → 拒绝。
6. 旧库（只有 `0001`、pending、无该列）：无身份文件 → 拒绝；身份文件一致 → 置 complete，随后 `0002` 加列。
7. 决策表：第 4 节每一行一个用例。
8. 回退检查：去掉令牌比较必须使用例 1 失败；把损坏的令牌当作不存在必须使用例 4 失败。
