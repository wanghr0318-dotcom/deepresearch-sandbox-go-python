# Plan 1A：`internal/runtime` 纯改名为 `internal/sandbox` 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `internal/runtime` 包改名为 `internal/sandbox`，不改变任何行为。

**Architecture:** 一次纯重命名提交：只改目录路径、包名、导入与调用、两处引用了旧包名的注释。验收方式是**限定 diff 范围，并验证改名前后测试集合与结果一致**：改名前后各运行一次测试，先以真实退出码确认测试成功，再比较归一化包路径后的 `(Package, Test, 最终 Action)` 集合与包级结束事件。依据：[代码组织设计](../design/2026-10-03-code-organization.md) §2.1 与 §8，[计划索引](2026-10-03-m1-index.md)。

**Tech Stack:** Go 1.23（`go.mod`）；WSL2 Ubuntu（Go 1.27、Python 3）；Git Bash（Windows）；GitHub Actions。

## Global Constraints

- **不混入行为变化**：不修改任何函数体、常量、测试逻辑或构建配置；除下文列出的两处包名注释外不改注释。发现其他问题记录下来，另开任务。
- **只暂存四个目标**：`internal/runtime/` → `internal/sandbox/` 的三个文件与 `cmd/agentbox/init_linux.go`。禁止 `git add -A` 或 `git add .`。
- **出现预期外差异时停止并检查归属**，不撤回、不覆盖任何不属于本计划的修改。
- **保留换行符**：Windows 工作区为 CRLF（`core.autocrlf=true`）。编辑使用能匹配 `\r?` 的 `perl -pi`，只替换目标行，不转换整份文件的换行符。
- 不修改历史文档 `docs/plans/2026-09-29-m1-local-provider.md`。现行文档中的 4 处 `internal/runtime` 都在描述这次改名本身（代码组织设计 2 处、v0.2 规格 1 处、计划索引 1 处），保持不变。
- **执行环境**：Windows 没有 Go 工具链。Go 与测试命令在 WSL2 Ubuntu 中执行（仓库路径 `/mnt/f/go-agentbox`，Go 位于 `/usr/local/go/bin`）；git 命令在 Git Bash 中执行。**任何读取 git 的步骤以普通用户运行**：root 下 git 会以"dubious ownership"拒绝该仓库。root 只用于运行测试，并设置 `GOFLAGS=-buildvcs=false`（`TestMainDispatchesInitToRunInit` 会构建 `cmd/agentbox`）。
- **判定以退出码为主**：`go test` 的真实退出码必须为 0；root 运行另用 `tools/citestjson`（与 CI 相同的判定）检查非预期 skip、中断与零执行；比较只在两次运行都成功后进行。
- 提交信息结尾：`Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`。
- 写计划、执行代码、推送、合并是四个独立步骤。Task 2 的推送与建 PR 须得到明确同意后才执行；合并不在本计划范围内。

---

### Task 1：改名并验证测试集合与结果不变

**Files:**
- Rename: `internal/runtime/` → `internal/sandbox/`（`init.go`、`spawn.go`、`spawn_test.go`）
- Modify: `internal/sandbox/init.go`（包名）、`internal/sandbox/spawn.go`（包注释与包名）、`internal/sandbox/spawn_test.go`（包名与两处注释）
- Modify: `cmd/agentbox/init_linux.go`（导入与调用）
- Create（不提交，`.superpowers/` 已被 gitignore）：`.superpowers/plan1a/run-tests.sh`、`.superpowers/plan1a/summarize.py`

**Interfaces:**
- Consumes: 无。
- Produces: 导入路径 `github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox`；导出标识符不变——`sandbox.RunInit() error`、`sandbox.Spawn(SpawnConfig) (*os.Process, error)`、`sandbox.SpawnConfig`、`sandbox.InitArg`、`sandbox.CloneFlags`。

- [ ] **Step 1：确认起点**

在 Git Bash 中：

```bash
cd /f/go-agentbox && git status --short && git branch --show-current && git log --oneline -1
```

Expected: `git status --short` 无输出；分支 `m1-local-provider`。若有未提交改动，停止并检查归属。

- [ ] **Step 2：建分支**

```bash
cd /f/go-agentbox && git switch -c m1-1a-rename-sandbox
```

Expected: `Switched to a new branch 'm1-1a-rename-sandbox'`

- [ ] **Step 3：写入验证脚本**

`.superpowers/plan1a/run-tests.sh`：

```bash
#!/usr/bin/env bash
# 用法：run-tests.sh <label> <user|root>
# 运行全部测试，保存完整 go test -json 输出与真实退出码；以退出码为主判据。
# root 运行另用 tools/citestjson（与 CI 相同）检查失败、非预期 skip、中断与零执行。
set -uo pipefail
label=$1
mode=$2
out=/tmp/plan1a
mkdir -p "$out"
export PATH="$PATH:/usr/local/go/bin"
cd /mnt/f/go-agentbox

if [ "$mode" = root ]; then
  export GOFLAGS=-buildvcs=false
fi

go test -json -count=1 ./... > "$out/$label.json"
test_exit=$?
echo "$test_exit" > "$out/$label.exit"
echo "$label: go test 退出码=$test_exit"

judge_exit=0
if [ "$mode" = root ]; then
  go run ./tools/citestjson "$out/$label.json" scripts/ci/allowed-skips.txt
  judge_exit=$?
  echo "$label: citestjson 退出码=$judge_exit"
fi

[ "$test_exit" -eq 0 ] && [ "$judge_exit" -eq 0 ]
```

`.superpowers/plan1a/summarize.py`：

```python
"""把 go test -json 输出归一化为排序后的 (Package, Test, 最终 Action) 集合。

- 包路径中的 internal/runtime 统一替换为 internal/sandbox；
- 包级结束事件以 Test 为空的行输出；
- 已开始但没有结束事件的用例、没有结束事件的包、无法解析的行 → 非零退出。
用法：summarize.py <test.json>   结果写到标准输出（制表符分隔）。
"""
import json
import sys

ENDS = {"pass", "fail", "skip"}


def norm(pkg):
    return pkg.replace("/internal/runtime", "/internal/sandbox")


final = {}
running = set()
open_pkgs = set()
with open(sys.argv[1], encoding="utf-8") as f:
    for n, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            sys.exit(f"第 {n} 行不是合法 JSON")
        pkg, test, action = norm(e.get("Package", "")), e.get("Test", ""), e.get("Action", "")
        key = (pkg, test)
        if test:
            open_pkgs.add(pkg)
            if action == "run":
                running.add(key)
            elif action in ENDS:
                running.discard(key)
                final[key] = action
        elif action in ENDS:
            open_pkgs.discard(pkg)
            final[key] = action
        elif pkg:
            open_pkgs.add(pkg)

if running or open_pkgs:
    sys.exit(f"测试流不完整：未结束用例 {sorted(running)}，未结束包 {sorted(open_pkgs)}")

for (pkg, test), action in sorted(final.items()):
    print(f"{pkg}\t{test}\t{action}")
```

（`.superpowers/` 已被 gitignore；脚本只用于本次验证，不提交。）

- [ ] **Step 4：记录改名前的基线**

```powershell
wsl -d Ubuntu -- bash /mnt/f/go-agentbox/.superpowers/plan1a/run-tests.sh before-user user
wsl -d Ubuntu -u root -- bash /mnt/f/go-agentbox/.superpowers/plan1a/run-tests.sh before-root root
```

Expected: 两条命令均以 0 退出；输出 `go test 退出码=0`，root 运行另有 `citestjson 退出码=0`。**任一非零即停止**：改名不能在失败的基线上进行。

再生成基线摘要（普通用户运行）：

```powershell
wsl -d Ubuntu -- bash -c "python3 /mnt/f/go-agentbox/.superpowers/plan1a/summarize.py /tmp/plan1a/before-user.json > /tmp/plan1a/before-user.tsv && python3 /mnt/f/go-agentbox/.superpowers/plan1a/summarize.py /tmp/plan1a/before-root.json > /tmp/plan1a/before-root.tsv && wc -l /tmp/plan1a/before-*.tsv"
```

Expected: 退出码 0；两个 `.tsv` 都非空。

- [ ] **Step 5：移动目录**

```bash
cd /f/go-agentbox && git mv internal/runtime internal/sandbox && git status --short
```

Expected: 恰好三行 `R  internal/runtime/<文件> -> internal/sandbox/<文件>`。

- [ ] **Step 6：改包名与注释（保留换行符）**

```bash
cd /f/go-agentbox && perl -pi -e 's/^package runtime(\r?)$/package sandbox$1/' internal/sandbox/init.go internal/sandbox/spawn.go internal/sandbox/spawn_test.go && perl -pi -e 's/^\/\/ Package runtime 负责/\/\/ Package sandbox 负责/' internal/sandbox/spawn.go && perl -pi -e 's/^\/\/ runSandboxInit\(\) -> runtime\.RunInit\(\)/\/\/ runSandboxInit() -> sandbox.RunInit()/; s/^\/\/ （internal\/runtime），/\/\/ （internal\/sandbox），/' internal/sandbox/spawn_test.go
```

立即断言：

```bash
cd /f/go-agentbox && for f in internal/sandbox/init.go internal/sandbox/spawn.go internal/sandbox/spawn_test.go; do grep -cE $'^package sandbox\r?$' "$f" | grep -qx 1 || { echo "包声明未改: $f"; exit 1; }; done && grep -q '^// Package sandbox 负责' internal/sandbox/spawn.go && grep -q '^// runSandboxInit() -> sandbox.RunInit()' internal/sandbox/spawn_test.go && grep -q '^// （internal/sandbox），' internal/sandbox/spawn_test.go && echo PKG-OK
```

Expected: 输出 `PKG-OK`。

- [ ] **Step 7：改入口的导入与调用（保留换行符）**

```bash
cd /f/go-agentbox && perl -pi -e 's#^import "github.com/wanghr0318-dotcom/go-agentbox/internal/runtime"#import "github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"#; s/return runtime\.RunInit\(\)/return sandbox.RunInit()/' cmd/agentbox/init_linux.go && grep -q 'internal/sandbox"' cmd/agentbox/init_linux.go && grep -q 'return sandbox.RunInit()' cmd/agentbox/init_linux.go && echo ENTRY-OK
```

Expected: 输出 `ENTRY-OK`。改后文件内容为：

```go
//go:build linux

package main

import "github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"

func runSandboxInit() error { return sandbox.RunInit() }
```

- [ ] **Step 8：确认没有遗漏的引用**

```bash
cd /f/go-agentbox && if git grep -n "internal/runtime\|package runtime\|Package runtime\|runtime\.RunInit" -- '*.go'; then echo "仍有旧引用"; exit 1; else echo REFS-OK; fi
cd /f/go-agentbox && test "$(git grep -c 'internal/runtime' -- docs/design docs/plans/2026-10-03-m1-index.md README.md | awk -F: '{s+=$2} END {print s}')" = 4 && echo DOCS-OK
```

Expected: `REFS-OK` 与 `DOCS-OK`。

- [ ] **Step 9：只暂存四个目标并限定 diff 范围**

```bash
cd /f/go-agentbox && git add internal/sandbox/init.go internal/sandbox/spawn.go internal/sandbox/spawn_test.go cmd/agentbox/init_linux.go && git diff --cached -M --name-status && git status --short
```

Expected:
- `--name-status` 恰好 4 行：三行 `R…  internal/runtime/<文件>  internal/sandbox/<文件>` 与一行 `M  cmd/agentbox/init_linux.go`；
- `git status --short` 没有任何未暂存或未跟踪项。

```bash
cd /f/go-agentbox && git diff --cached -M -U0 -- internal cmd | grep -E '^[+-][^+-]'
```

Expected: 恰好 16 行（8 删 8 增）：3 处 `package`、1 处包注释、2 处测试注释、`init_linux.go` 的 import 与调用各 1 处。出现其他行即**停止并检查其归属**。

- [ ] **Step 10：构建、vet 与格式检查**

```powershell
wsl -d Ubuntu -- bash -c "export PATH=`$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox && go build ./... && GOOS=windows go build ./... && go vet ./... && echo BUILD-VET-OK"
```

Expected: `BUILD-VET-OK`。（若 PowerShell 引号出错，把命令写入 `.superpowers/plan1a/` 下的脚本再执行。）

格式检查针对**暂存内容**（工作区为 CRLF，直接检查会误报），以普通用户运行，任何失败非零退出。写入 `.superpowers/plan1a/gofmt-staged.sh`：

```bash
#!/usr/bin/env bash
set -uo pipefail
export PATH="$PATH:/usr/local/go/bin"
cd /mnt/f/go-agentbox
tmp=$(mktemp)
status=0
files=$(git ls-files '*.go') || { echo "git ls-files 失败"; exit 1; }
for f in $files; do
  if ! git show ":$f" > "$tmp"; then
    echo "读取暂存内容失败: $f"; status=1; continue
  fi
  if ! out=$(gofmt -l "$tmp" 2>&1); then
    echo "gofmt 执行失败: $f: $out"; status=1; continue
  fi
  if [ -n "$out" ]; then
    echo "未格式化: $f"; status=1
  fi
done
rm -f "$tmp"
[ "$status" -eq 0 ] && echo GOFMT-OK
exit "$status"
```

```powershell
wsl -d Ubuntu -- bash /mnt/f/go-agentbox/.superpowers/plan1a/gofmt-staged.sh
```

Expected: 以 0 退出并输出 `GOFMT-OK`。

- [ ] **Step 11：改名后重跑并比较测试集合与结果**

```powershell
wsl -d Ubuntu -- bash /mnt/f/go-agentbox/.superpowers/plan1a/run-tests.sh after-user user
wsl -d Ubuntu -u root -- bash /mnt/f/go-agentbox/.superpowers/plan1a/run-tests.sh after-root root
```

Expected: 两条均以 0 退出。任一非零即停止。

```powershell
wsl -d Ubuntu -- bash -c "cd /tmp/plan1a && python3 /mnt/f/go-agentbox/.superpowers/plan1a/summarize.py after-user.json > after-user.tsv && python3 /mnt/f/go-agentbox/.superpowers/plan1a/summarize.py after-root.json > after-root.tsv && diff before-user.tsv after-user.tsv && diff before-root.tsv after-root.tsv && echo SAME-TESTS-SAME-RESULTS"
```

Expected: `SAME-TESTS-SAME-RESULTS`——归一化包路径后，用例集合（含子测试）、每个用例的最终 Action、各包的结束事件全部一致。

```powershell
wsl -d Ubuntu -u root -- bash -c "ls -d /sys/fs/cgroup/agentbox-test 2>/dev/null && echo 残留 || echo 无残留"
```

Expected: `无残留`

- [ ] **Step 12：提交（只在本地）**

```bash
cd /f/go-agentbox && git commit -m "refactor: internal/runtime 改名为 internal/sandbox

纯重命名：只改目录、包名、导入与调用、两处引用旧包名的注释；不改任何行为。
diff 限定在四个文件；改名前后普通用户与 root 的测试集合与结果一致（按
(Package, Test, 最终 Action) 与包级结束事件比较）。

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>" && git show --stat --format=%s HEAD | tail -6
```

Expected: 一个新提交，`--stat` 只含 3 个重命名文件与 `cmd/agentbox/init_linux.go`。

---

### Task 2：发布与远程验证（每一步都须明确同意后执行）

**Files:** 无。

**Interfaces:**
- Consumes: Task 1 的本地提交。
- Produces: 分支 `m1-1a-rename-sandbox` 上的 Draft PR。

- [ ] **Step 1：先发布计划提交**

本计划所在的文档提交位于 `m1-local-provider`、尚未推送。先把它推送到 `m1-local-provider`（属于 PR #1 的设计基线），使改名分支相对远程 base 只多出改名提交：

```bash
cd /f/go-agentbox && git fetch origin && git log --oneline origin/m1-local-provider..m1-local-provider
```

Expected: 只列出文档提交（本计划及索引更新）。确认后：

```bash
cd /f/go-agentbox && git push origin m1-local-provider
```

- [ ] **Step 2：推送前检查相对最新远程 base 的实际差异**

```bash
cd /f/go-agentbox && git fetch origin && git log --oneline origin/m1-local-provider..m1-1a-rename-sandbox && git diff --name-status -M origin/m1-local-provider...m1-1a-rename-sandbox
```

Expected: 只有 Task 1 的 1 个提交；`--name-status` 与 Task 1 Step 9 相同的 4 行。不一致即停止并检查。

- [ ] **Step 3：推送并建 Draft PR**

PR #1 尚未合并，以 `m1-local-provider` 为 base（堆叠 PR）；PR #1 合并后改为以 `main` 为 base。

```bash
cd /f/go-agentbox && git push -u origin m1-1a-rename-sandbox && gh pr create --draft --base m1-local-provider --head m1-1a-rename-sandbox --title "Plan 1A：internal/runtime 纯改名为 internal/sandbox" --body "纯重命名，不改行为。Refs #2（Plan 1 的 1A 部分；最终验收 PR 才关闭 #2）。

本地验证：diff 限定在四个文件；linux/windows 构建、vet、暂存内容 gofmt；改名前后普通用户与 root 测试均以退出码 0 通过，按 (Package, Test, 最终 Action) 与包级结束事件比较一致；无 cgroup 残留。

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

Expected: 输出 PR 链接。

- [ ] **Step 4：比较 CI 中的实际测试集合**

不使用固定的用例数。取本 PR 与 base 分支最近一次 CI 的 `linux-integration-test-json` 产物，按 Task 1 Step 11 的同一方法比较：

```bash
cd /f/go-agentbox && mkdir -p .superpowers/plan1a/ci && base_run=$(gh run list --branch m1-local-provider --workflow ci --limit 1 --json databaseId --jq '.[0].databaseId') && pr_run=$(gh run list --branch m1-1a-rename-sandbox --workflow ci --limit 1 --json databaseId --jq '.[0].databaseId') && gh run download "$base_run" -n linux-integration-test-json -D .superpowers/plan1a/ci/base && gh run download "$pr_run" -n linux-integration-test-json -D .superpowers/plan1a/ci/pr
```

```powershell
wsl -d Ubuntu -- bash -c "cd /mnt/f/go-agentbox/.superpowers/plan1a/ci && python3 ../summarize.py base/test.json > base.tsv && python3 ../summarize.py pr/test.json > pr.tsv && diff base.tsv pr.tsv && echo CI-SAME"
```

Expected: 两次运行都已完成且三个 job 均实际执行通过；输出 `CI-SAME`。

合并另行决定：CI 通过只是必要条件，还需审阅 diff。

---

## 自查记录

- **规格覆盖**：代码组织设计 §8"纯重命名提交：只改包名、路径、引用与文档，不改启动、收割或降权逻辑，并重跑适用的编译与测试"——Task 1 Step 5–11；文档无需修改的理由见 Global Constraints。
- **评审修正**：退出码以 `go test` 真实值为主并复用 `citestjson`（Step 3、4、11）；比较 `(Package, Test, 最终 Action)` 与包级结束事件（`summarize.py`，含子测试）；`perl -pi` 匹配 `\r?` 并立即断言（Step 6、7）；gofmt 累计失败并检查 `git show`，以普通用户运行（Step 10）；先发布计划提交、检查相对最新远程 base 的实际差异（Task 2 Step 1–2）；不使用固定用例数（Task 2 Step 4）；只暂存四个目标，预期外差异停止并检查归属。
- **占位符**：无。
- **一致性**：Produces 中的导出标识符与现有 `spawn.go`、`init.go` 一致。

---

## 验收记录（2026-10-05）

**状态：已验收**（Draft PR #8，未合并；合并另行决定）。

- **diff 范围**：PR #8 恰为 4 个文件（与 Task 1 Step 9 一致），单个提交 44beee9。
- **CI**：PR #8 的 push 运行 37197137662 与 pull_request 运行 37197161890 中 correctness、linux-integration、complexity-report 全部通过。
- **测试集合（Task 2 Step 4）**：取 base 分支 `m1-local-provider` 最近一次 CI（运行 37215913082；自 PR #8 分出后 base 只有文档提交）与 PR #8 最近一次 CI（运行 37197161890）的 `linux-integration-test-json` 产物，按 `summarize.py` 归一化包路径后比较 `(Package, Test, 最终 Action)` 与包级结束事件：两侧各 50 项，`diff` 无差异，输出 `CI-SAME`。

