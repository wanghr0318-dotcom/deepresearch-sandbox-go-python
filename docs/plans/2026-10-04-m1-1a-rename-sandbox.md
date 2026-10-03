# Plan 1A：`internal/runtime` 纯改名为 `internal/sandbox` 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `internal/runtime` 包改名为 `internal/sandbox`，不改变任何行为。

**Architecture:** 一次纯重命名提交：只改目录路径、包名、导入与调用、两处引用了旧包名的注释。改名前后用同一组命令记录测试结果并逐行比对，证明行为未变。依据：[代码组织设计](../design/2026-10-03-code-organization.md) §2.1 与 §8，[计划索引](2026-10-03-m1-index.md)。

**Tech Stack:** Go 1.23（`go.mod`）；WSL2 Ubuntu（本地 Linux 验证）；GitHub Actions（远程验证）。

## Global Constraints

- **不混入行为变化**：不修改任何函数体、常量、注释内容（除下文列出的两处包名引用）、测试逻辑或构建配置。发现其他问题记录下来，另开任务，不在本提交中修。
- 不修改历史文档 `docs/plans/2026-09-29-m1-local-provider.md`（其中 43 处 `internal/runtime` 是历史记录）。
- 现行文档中的 4 处 `internal/runtime` 都在描述这次改名本身（代码组织设计 2 处、v0.2 规格 1 处、计划索引 1 处），保持不变。
- 本地 Windows 没有 Go 工具链；所有 Go 命令在 WSL2 Ubuntu 中执行，仓库路径为 `/mnt/f/go-agentbox`，Go 位于 `/usr/local/go/bin`。
- 以 root 运行测试时设置 `GOFLAGS=-buildvcs=false`：root 下 git 会因仓库属主不同拒绝读取 VCS 信息，而 `TestMainDispatchesInitToRunInit` 会构建 `cmd/agentbox`。
- 提交信息结尾：`Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`。
- 写计划、执行代码、推送、合并是四个独立步骤。Task 2 的推送与建 PR 须得到明确同意后才执行；合并不在本计划范围内。

---

### Task 1：改名并证明行为不变

**Files:**
- Rename: `internal/runtime/` → `internal/sandbox/`（`init.go`、`spawn.go`、`spawn_test.go`）
- Modify: `internal/sandbox/init.go:3`（包名）
- Modify: `internal/sandbox/spawn.go:3-4`（包注释与包名）
- Modify: `internal/sandbox/spawn_test.go:3`（包名）、`:155`、`:187`（注释中的旧包名）
- Modify: `cmd/agentbox/init_linux.go`（导入与调用）

**Interfaces:**
- Consumes: 无。
- Produces: 导入路径 `github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox`；导出标识符不变——`sandbox.RunInit() error`、`sandbox.Spawn(SpawnConfig) (*os.Process, error)`、`sandbox.SpawnConfig`、`sandbox.InitArg`、`sandbox.CloneFlags`。后续计划（1B、2）以此为准。

- [ ] **Step 1：确认起点干净**

在 Windows 的 Git Bash 中运行：

```bash
cd /f/go-agentbox && git status --short && git log --oneline -1
```

Expected: `git status --short` 无输出；当前分支 `m1-local-provider`。若有未提交改动，停止并先处理。

- [ ] **Step 2：从当前提交建分支**

```bash
cd /f/go-agentbox && git switch -c m1-1a-rename-sandbox
```

Expected: `Switched to a new branch 'm1-1a-rename-sandbox'`

- [ ] **Step 3：记录改名前的基线**

在 WSL 中运行（非 root 与 root 各一次）：

```bash
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox && go test -count=1 -v ./... 2>&1 | grep -E "^(--- |ok|FAIL|\?)" | sed "s/([0-9.]*s)//" > /tmp/before-user.txt; wc -l /tmp/before-user.txt'
```

```bash
wsl -d Ubuntu -u root -- bash -c 'export PATH=$PATH:/usr/local/go/bin GOFLAGS=-buildvcs=false; cd /mnt/f/go-agentbox && go test -count=1 -v ./... 2>&1 | grep -E "^(--- |ok|FAIL|\?)" | sed "s/([0-9.]*s)//" > /tmp/before-root.txt; wc -l /tmp/before-root.txt; grep -c "^--- PASS" /tmp/before-root.txt; grep -c "^--- FAIL" /tmp/before-root.txt'
```

Expected: 两个文件都非空；root 基线中 `--- FAIL` 计数为 0。若基线本身有失败，停止：改名不能在失败的基线上进行。

> 若 PowerShell 对引号处理出错，把命令写入 `.superpowers/` 下的临时脚本（该目录已被 gitignore），再以 `wsl -d Ubuntu [-u root] -- bash /mnt/f/go-agentbox/.superpowers/<脚本名>` 执行。

- [ ] **Step 4：移动目录**

```bash
cd /f/go-agentbox && git mv internal/runtime internal/sandbox
```

Expected: 无输出；`git status --short` 显示三行 `R  internal/runtime/... -> internal/sandbox/...`。

- [ ] **Step 5：改包名**

```bash
cd /f/go-agentbox && sed -i 's/^package runtime$/package sandbox/' internal/sandbox/init.go internal/sandbox/spawn.go internal/sandbox/spawn_test.go && sed -i 's#^// Package runtime 负责沙箱进程的启动与其内部的 1 号进程。#// Package sandbox 负责沙箱进程的启动与其内部的 1 号进程。#' internal/sandbox/spawn.go
```

改完后 `internal/sandbox/spawn.go` 第 1–4 行应为：

```go
//go:build linux

// Package sandbox 负责沙箱进程的启动与其内部的 1 号进程。
package sandbox
```

`init.go` 与 `spawn_test.go` 第 3 行应为 `package sandbox`。

- [ ] **Step 6：改注释中的旧包名**

```bash
cd /f/go-agentbox && sed -i 's#^// runSandboxInit() -> runtime.RunInit()。#// runSandboxInit() -> sandbox.RunInit()。#; s#^// （internal/runtime），仓库根就是它的上两级。#// （internal/sandbox），仓库根就是它的上两级。#' internal/sandbox/spawn_test.go
```

改完后 `spawn_test.go` 中这两行应为：

```go
// runSandboxInit() -> sandbox.RunInit()。
```

```go
// （internal/sandbox），仓库根就是它的上两级。
```

（`repoRootDir` 通过上两级定位仓库根；`internal/sandbox` 与 `internal/runtime` 深度相同，逻辑无需改变。）

- [ ] **Step 7：改入口的导入与调用**

把 `cmd/agentbox/init_linux.go` 整个文件改为：

```go
//go:build linux

package main

import "github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"

func runSandboxInit() error { return sandbox.RunInit() }
```

- [ ] **Step 8：确认没有遗漏的引用**

```bash
cd /f/go-agentbox && git grep -n "internal/runtime\|package runtime\|Package runtime\|runtime\.RunInit" -- '*.go'; echo "exit=$?"
```

Expected: 无匹配，`exit=1`。

```bash
cd /f/go-agentbox && git grep -n "internal/runtime" -- docs/design docs/plans/2026-10-03-m1-index.md README.md
```

Expected: 恰好 4 行——代码组织设计 2 行、v0.2 规格 1 行、计划索引 1 行（均描述改名本身）。

- [ ] **Step 9：确认差异只有改名**

```bash
cd /f/go-agentbox && git add -A && git diff --cached -M --stat && git diff --cached -M -- internal cmd
```

Expected:
- `--stat` 只列出 3 个重命名文件与 `cmd/agentbox/init_linux.go`；
- 内容差异只有：3 行 `package runtime` → `package sandbox`、1 行包注释、2 行测试注释、`init_linux.go` 的 import 与调用各 1 行。出现任何其他差异，撤回该差异后再继续。

- [ ] **Step 10：构建与静态检查**

```bash
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox && go build ./... && GOOS=windows go build ./... && go vet ./... && echo BUILD-VET-OK'
```

Expected: 输出 `BUILD-VET-OK`。

格式检查须针对暂存内容（Windows 工作区为 CRLF，直接对工作区运行 gofmt 会误报所有文件）：

```bash
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox && for f in $(git ls-files "*.go"); do git show ":$f" > /tmp/g.go; [ -n "$(gofmt -l /tmp/g.go)" ] && echo "未格式化: $f"; done; echo GOFMT-DONE'
```

Expected: 只输出 `GOFMT-DONE`。

- [ ] **Step 11：改名后重跑测试并与基线比对**

```bash
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox && go test -count=1 -v ./... 2>&1 | grep -E "^(--- |ok|FAIL|\?)" | sed "s/([0-9.]*s)//" > /tmp/after-user.txt; diff <(sed "s#internal/runtime#internal/sandbox#" /tmp/before-user.txt | sed -E "s/[[:space:]]+[0-9.]+s$//") <(sed -E "s/[[:space:]]+[0-9.]+s$//" /tmp/after-user.txt) && echo USER-SAME'
```

```bash
wsl -d Ubuntu -u root -- bash -c 'export PATH=$PATH:/usr/local/go/bin GOFLAGS=-buildvcs=false; cd /mnt/f/go-agentbox && go test -count=1 -v ./... 2>&1 | grep -E "^(--- |ok|FAIL|\?)" | sed "s/([0-9.]*s)//" > /tmp/after-root.txt; diff <(sed "s#internal/runtime#internal/sandbox#" /tmp/before-root.txt | sed -E "s/[[:space:]]+[0-9.]+s$//") <(sed -E "s/[[:space:]]+[0-9.]+s$//" /tmp/after-root.txt) && echo ROOT-SAME'
```

Expected: 分别输出 `USER-SAME` 与 `ROOT-SAME`——同一组用例、同样的 PASS/SKIP 结果，唯一差别是包路径（比对前已统一替换、去除耗时）。若 `go test` 输出包的顺序因路径排序变化而不同，改为对两个文件分别 `sort` 后再 `diff`。

另确认 cgroup 测试没有留下残留：

```bash
wsl -d Ubuntu -u root -- bash -c 'ls -d /sys/fs/cgroup/agentbox-test 2>/dev/null && echo 残留 || echo 无残留'
```

Expected: `无残留`

- [ ] **Step 12：提交（只在本地）**

```bash
cd /f/go-agentbox && git commit -m "refactor: internal/runtime 改名为 internal/sandbox

纯重命名：只改目录、包名、导入与调用、两处引用旧包名的注释；
不改任何行为。改名前后非 root 与 root 测试结果逐行一致。

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>" && git log --oneline -1 && git show --stat HEAD | tail -6
```

Expected: 一个新提交；`--stat` 只含 3 个重命名文件与 `cmd/agentbox/init_linux.go`。

---

### Task 2：远程验证（须明确同意后执行）

**Files:** 无。

**Interfaces:**
- Consumes: Task 1 的本地提交。
- Produces: 分支 `m1-1a-rename-sandbox` 上的 Draft PR。

- [ ] **Step 1：推送前检查待推送内容**

```bash
cd /f/go-agentbox && git log --oneline m1-local-provider..m1-1a-rename-sandbox && git diff --stat m1-local-provider..m1-1a-rename-sandbox
```

Expected: 只有 Task 1 的 1 个提交，改动范围与 Task 1 Step 12 相同。

- [ ] **Step 2：推送并建 Draft PR**

PR #1 尚未合并，因此以 `m1-local-provider` 为 base（堆叠 PR）；PR #1 合并后改为以 `main` 为 base。

```bash
cd /f/go-agentbox && git push -u origin m1-1a-rename-sandbox && gh pr create --draft --base m1-local-provider --head m1-1a-rename-sandbox --title "Plan 1A：internal/runtime 纯改名为 internal/sandbox" --body "纯重命名，不改行为。Refs #2（Plan 1 的 1A 部分；Plan 1B 启动序列 spike 另行提交，最终验收 PR 才关闭 #2）。

本地验证：linux/windows 构建、vet、暂存内容 gofmt；改名前后非 root 与 root 测试结果逐行一致；无 cgroup 残留。

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

Expected: 输出 PR 链接。

- [ ] **Step 3：确认 CI 实际执行**

在 PR 页面（或应用的 PR 面板）确认：`correctness`、`complexity-report`、`linux-integration` 三个 job 均实际执行并通过；`linux-integration` 日志中 `通过的用例数` 与 PR #1 最近一次运行相同（当前为 41），且无非预期 skip。

合并另行决定：CI 通过只是必要条件，还需审阅 diff。

---

## 自查记录

- **规格覆盖**：代码组织设计 §8 要求的"纯重命名提交：只改包名、路径、引用与文档，不改启动、收割或降权逻辑，并重跑适用的编译与测试"——Task 1 Step 4–11 覆盖；文档无需修改的理由见 Global Constraints。
- **占位符**：无。
- **一致性**：Produces 中的导出标识符与现有 `spawn.go`、`init.go` 一致；未引入新名称。
