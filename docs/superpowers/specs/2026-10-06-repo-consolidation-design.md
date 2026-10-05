# 仓库整合：M1–M3 合并为单一 `main`

> 日期：2026-10-06。状态：设计已批准（项目负责人，2026-10-06）。
> 前提：只在 M3 门槛判定通过之后执行（服务器真实演示、浏览器验收、CI 全绿）。

## 目标

M3 完成后，项目只剩一个结构化的仓库形态：GitHub 上只有 `main` 加 5 个标签；本机只有一个工作目录 `F:\go-agentbox`，位于 `main`。代码目录结构不变（`cmd/ internal/ worker/ web/ api/ docs/ scripts/ deploy/ tests/ tools/ bench/ protocol/ experiments/`），文档不重组。

## 现状（2026-10-06）

- `origin/main` 是 `m3-batch` 的祖先（落后 217 个提交、领先 0 个），合并无冲突。
- 远程分支 8 个：`main`、`m1-local-provider`、`m1-1a-rename-sandbox`、`m1-3-protocol-worker`、`m1-4-persistence`、`m1-batch2`、`m2-batch`、`m3-batch`。
- 未合并的草稿 PR：#1、#8、#9、#10、#11。
- 本地工作目录 6 个（`F:\go-agentbox`、`-batch2`、`-m1-3`、`-m1-4`、`-m2`、`-m3`），本地任务分支约 70 个。
- 不被 `m3-batch` 包含的提交：`m1-local-provider` 41 个（`internal/runtime` 改名前的早期版本）、`m1-1a-rename-sandbox` 1 个（改名本身）；两者均已被后续工作取代，但删除分支会丢失这段历史。

## 方案：PR + 合并提交

选择理由：保留全部提交历史（演示证据链可追溯），GitHub 上有一条"M1–M3 合入 main"的 PR 记录，合并前 CI 在 PR 上再跑一次。未采用：直接快进（没有 PR 记录）、squash（抹掉 217 个提交的历史）。

## 步骤

1. **先打标签（确保不丢东西）**：`m1-gate` → f03922f、`m2-gate` → 15ecfff、`m3-gate` → M3 门槛判定所在提交；`archive/m1-local-provider`、`archive/m1-1a-rename-sandbox` → 两个被取代分支的末端。全部推送到 GitHub。
2. **合并**：开 PR `m3-batch` → `main`（描述：M1–M3 内容、三个门槛、证据链接）；CI 全绿后以合并提交合入；关闭草稿 PR #1、#8–#11，注明"已被 #<新 PR> 取代"。
3. **清理 GitHub**：删除除 `main` 外的所有远程分支。结果：`main` + 5 个标签。
4. **清理本机**：移除 `-batch2`、`-m1-3`、`-m1-4`、`-m2`、`-m3` 工作目录与全部本地任务分支；`F:\go-agentbox` 切到 `main` 并拉取。`.env`（git 忽略，只在 `F:\go-agentbox`）保留不动。
5. **验证**：`main` 上构建与非 root 全量测试通过；`git branch -a` 只有 `main`；`git worktree list` 只有一个目录；README 链接有效；记忆中记录"之后在 `main` 上工作"。

## 安全规则

- 删除任何分支前检查：已合入 `main`，或其末端已被标签覆盖；否则停止并询问项目负责人。
- 删除工作目录前检查没有未提交的改动；有则停止并报告。
- 不使用 `--force` 推送 `main`；合并只经 GitHub PR。
- `.env`、密钥文件、服务器上的 `~/.agentbox.env` 不受影响，也不进入任何提交。

## 不在本设计内

- 文档重组（`docs/history/` 等）与 README 改写为作品集首页——未选择（选项 B）。
- 打包文件（zip/tarball）——未选择（选项 C）。
- 服务器备份与释放——按 `docs/deploy/2026-10-05-cvm-shanghai.md` 另行执行。
