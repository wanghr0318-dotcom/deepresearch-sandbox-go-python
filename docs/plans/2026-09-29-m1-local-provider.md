# M1: LocalProvider 最小可用 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个能建箱、在箱内执行命令、正确销毁且不留残留的本地沙箱 Provider。

**Architecture:** 用 Linux namespace 做隔离、cgroup v2 做限额、overlayfs 做根文件系统分层。宿主进程通过 `re-exec /proc/self/exe init` 启动沙箱内 1 号进程；该 init 承担收割孤儿进程与接收执行请求两项职责，宿主与它之间用 `socketpair` 预传的 fd 通信，不经过文件系统。

**Tech Stack:** Go 1.23+、Linux namespace（NS/PID/UTS/IPC/NET）、cgroup v2、overlayfs、alpine minirootfs。仅标准库，无第三方依赖。

## Global Constraints

- Go 版本 ≥ 1.23（本机使用 go1.27.1）
- 模块路径：`github.com/wanghr0318-dotcom/go-agentbox`
- 仅支持 Linux。开发环境为 WSL2 Ubuntu（内核 6.6.87.2）；Windows 侧无法运行任何本任务的测试
- **WSL 无外网**：直连超时，经 Windows 代理（`127.0.0.1:7897`）亦不通 —— WSL2 有独立网络栈，其 `127.0.0.1` 并非 Windows 的。所有外部依赖一律在 Windows 侧下载到 `F:\wsl-assets\`，WSL 通过 `/mnt/f/wsl-assets/` 读取。本计划不引入任何第三方 Go 依赖，故无需 `go mod download`
- 仓库工作副本在 WSL 原生文件系统（`~/go-agentbox`），**不可在 `/mnt/f` 上开发** —— 那是 9p，不支持 overlayfs
- sudo 需要密码，涉及 root 的命令须在 WSL 终端手工执行
- 必须 cgroup v2（`stat -fc %T /sys/fs/cgroup` 输出 `cgroup2fs`）
- **M1 以 root 运行**，不启用 `CLONE_NEWUSER`。rootless 是 spec §9 待定项 #7，不在本计划范围
- 沙箱根目录：`/var/lib/agentbox`
- cgroup 根：`/sys/fs/cgroup/agentbox`
- 不引入 Redis、不引入 Eino —— 那是 M2/M4 的范围
- 所有需要 Linux 或 root 的测试，必须通过 `testutil.RequireLinuxRoot(t)` 在条件不满足时跳过，不得直接失败
- 涉及 Linux 专有 syscall 的文件必须带 `//go:build linux` 标签

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `go.mod` | 模块定义 |
| `cmd/agentbox/main.go` | 入口；**首行判断 `init` 子命令并分流**，其余为正常 CLI |
| `internal/testutil/testutil.go` | 测试前置条件检查与跳过 |
| `internal/hostcheck/hostcheck.go` | 宿主环境自检：Linux / root / cgroup v2 / overlayfs |
| `internal/cgroup/cgroup.go` | cgroup v2 组的创建、限额、加进程、冻结、销毁 |
| `internal/rootfs/overlay.go` | overlayfs 与 bind mount 的挂载/卸载 |
| `internal/rootfs/template.go` | 模板 rootfs 的就位检查与解包 |
| `internal/runtime/protocol.go` | 宿主 ↔ 沙箱 init 的执行协议编解码 |
| `internal/runtime/spawn.go` | 用 namespace 启动沙箱 init 进程（宿主侧） |
| `internal/runtime/init.go` | 沙箱内 1 号进程：pivot_root、挂伪文件系统、收割、服务执行请求 |
| `internal/runtime/client.go` | 宿主侧执行请求客户端 |
| `internal/provider/provider.go` | Provider 接口与公共类型 |
| `internal/provider/local/local.go` | LocalProvider：串起上述所有组件 |
| `internal/provider/local/state.go` | `state.json` 读写，支撑 `Get` / `List` |

---

## Task 1: 项目骨架与宿主环境自检

**Files:**
- Create: `go.mod`
- Create: `cmd/agentbox/main.go`
- Create: `internal/testutil/testutil.go`
- Create: `internal/hostcheck/hostcheck.go`
- Test: `internal/hostcheck/hostcheck_test.go`

**Interfaces:**
- Consumes: 无
- Produces:
  - `testutil.RequireLinuxRoot(t *testing.T)`
  - `hostcheck.Report{IsLinux, IsRoot, CgroupV2, CgroupRoot, OverlayFS bool/string}`
  - `hostcheck.Check() Report`
  - `func (r Report) Err() error`

- [ ] **Step 1: 在 WSL2 Ubuntu 里装 Go 并验证环境**

> **本机 WSL 无外网**：直连与经 Windows 代理均不通（详见 Global Constraints）。
> 因此安装包一律在 Windows 侧下载好，放在 `F:\wsl-assets\`，WSL 通过 `/mnt/f` 读取。

在 Windows 上打开 WSL：

```bash
wsl -d Ubuntu
```

装 Go（apt 源里的版本偏旧，用官网 tar 包）：

```bash
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /mnt/f/wsl-assets/go1.27.1.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
go version
```

Expected: `go version go1.27.1 linux/amd64`

验证 cgroup v2：

```bash
stat -fc %T /sys/fs/cgroup
```

Expected: `cgroup2fs`

验证 overlayfs：

```bash
grep overlay /proc/filesystems
```

Expected: `nodev	overlay`

- [ ] **Step 2: 初始化模块**

Windows 上的 `F:\go-agentbox` 在 WSL 里是 `/mnt/f/go-agentbox`。但 **`/mnt/f` 是 9p 文件系统，不支持 overlayfs 和部分 syscall**，必须把仓库放在 WSL 原生文件系统里：

```bash
git clone /mnt/f/go-agentbox ~/go-agentbox
cd ~/go-agentbox
git remote set-url origin https://github.com/wanghr0318-dotcom/go-agentbox.git
git config user.name  "wanghr0318-dotcom"
git config user.email "wanghr0318@163.com"
go mod init github.com/wanghr0318-dotcom/go-agentbox
```

从本地路径克隆而非 GitHub，是因为 WSL 没有外网；remote 随后指回 GitHub，
但 `git push` 需在 Windows 侧的 `F:\go-agentbox` 完成。

Expected: 生成 `go.mod`，内容含 `module github.com/wanghr0318-dotcom/go-agentbox` 与 `go 1.27`

- [ ] **Step 3: 写失败测试**

`internal/hostcheck/hostcheck_test.go`:

```go
package hostcheck

import (
	"os"
	"testing"
)

func TestCheckCgroupV2_TempDirIsNotCgroup2(t *testing.T) {
	dir := t.TempDir()
	ok, err := checkCgroupV2(dir)
	if err != nil {
		t.Fatalf("checkCgroupV2(%q) returned error: %v", dir, err)
	}
	if ok {
		t.Fatalf("checkCgroupV2(%q) = true, want false for a plain temp dir", dir)
	}
}

func TestCheckCgroupV2_MissingPath(t *testing.T) {
	ok, err := checkCgroupV2("/definitely/not/here")
	if err == nil {
		t.Fatal("checkCgroupV2 on a missing path should return an error")
	}
	if ok {
		t.Fatal("checkCgroupV2 on a missing path should return false")
	}
}

func TestCheck_ReportsRootCorrectly(t *testing.T) {
	r := Check()
	wantRoot := os.Geteuid() == 0
	if r.IsRoot != wantRoot {
		t.Fatalf("Report.IsRoot = %v, want %v", r.IsRoot, wantRoot)
	}
}
```

- [ ] **Step 4: 跑测试确认失败**

```bash
go test ./internal/hostcheck/ -v
```

Expected: 编译失败，`undefined: checkCgroupV2`、`undefined: Check`

- [ ] **Step 5: 实现 hostcheck**

`internal/hostcheck/hostcheck.go`:

```go
// Package hostcheck 检查宿主机是否具备运行本地沙箱的条件。
package hostcheck

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
)

// DefaultCgroupRoot 是 cgroup v2 的标准挂载点。
const DefaultCgroupRoot = "/sys/fs/cgroup"

// cgroup2Magic 是 cgroup2 文件系统的 statfs 魔数（见 linux/magic.h）。
const cgroup2Magic = 0x63677270

// Report 是一次宿主环境自检的结果。
type Report struct {
	IsLinux    bool
	IsRoot     bool
	CgroupV2   bool
	CgroupRoot string
	OverlayFS  bool
	Problems   []string
}

// Check 对当前宿主机做一次自检。
func Check() Report {
	r := Report{
		IsLinux:    runtime.GOOS == "linux",
		IsRoot:     os.Geteuid() == 0,
		CgroupRoot: DefaultCgroupRoot,
	}

	if !r.IsLinux {
		r.Problems = append(r.Problems, "本地沙箱仅支持 Linux，当前为 "+runtime.GOOS)
		return r
	}
	if !r.IsRoot {
		r.Problems = append(r.Problems, "需要 root 权限（M1 未启用 user namespace）")
	}

	ok, err := checkCgroupV2(r.CgroupRoot)
	switch {
	case err != nil:
		r.Problems = append(r.Problems, fmt.Sprintf("无法检查 %s：%v", r.CgroupRoot, err))
	case !ok:
		r.Problems = append(r.Problems, r.CgroupRoot+" 不是 cgroup2fs，需要 cgroup v2")
	default:
		r.CgroupV2 = true
	}

	if r.OverlayFS = checkOverlayFS(); !r.OverlayFS {
		r.Problems = append(r.Problems, "内核未提供 overlay 文件系统")
	}

	return r
}

// Err 在存在任何问题时返回一个汇总错误，否则返回 nil。
func (r Report) Err() error {
	if len(r.Problems) == 0 {
		return nil
	}
	return errors.New("宿主环境不满足要求：\n  - " + strings.Join(r.Problems, "\n  - "))
}

func checkOverlayFS() bool {
	b, err := os.ReadFile("/proc/filesystems")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(strings.TrimPrefix(line, "nodev")) == "overlay" {
			return true
		}
	}
	return false
}
```

`internal/hostcheck/statfs_linux.go`:

```go
//go:build linux

package hostcheck

import "syscall"

func checkCgroupV2(path string) (bool, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false, err
	}
	return int64(st.Type) == cgroup2Magic, nil
}
```

`internal/hostcheck/statfs_other.go`:

```go
//go:build !linux

package hostcheck

import "errors"

func checkCgroupV2(path string) (bool, error) {
	return false, errors.New("cgroup v2 检查仅在 Linux 上可用")
}
```

`internal/testutil/testutil.go`:

```go
// Package testutil 提供测试前置条件检查。
package testutil

import (
	"os"
	"runtime"
	"testing"
)

// RequireLinuxRoot 在当前环境不是 Linux 或不是 root 时跳过测试。
// 沙箱相关测试必须调用它——条件不满足时应跳过而非失败，
// 否则在开发者的 Windows/macOS 上跑 go test ./... 会全线报错。
func RequireLinuxRoot(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skipf("需要 Linux，当前为 %s", runtime.GOOS)
	}
	if os.Geteuid() != 0 {
		t.Skip("需要 root 权限")
	}
}
```

`cmd/agentbox/main.go`:

```go
package main

import (
	"fmt"
	"os"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/hostcheck"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "doctor" {
		r := hostcheck.Check()
		if err := r.Err(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("宿主环境检查通过")
		return
	}
	fmt.Fprintln(os.Stderr, "用法: agentbox doctor")
	os.Exit(2)
}
```

- [ ] **Step 6: 跑测试确认通过**

```bash
go test ./internal/hostcheck/ -v
```

Expected: 三个用例全部 PASS

```bash
sudo go run ./cmd/agentbox doctor
```

Expected: `宿主环境检查通过`

- [ ] **Step 7: 提交**

```bash
git add go.mod cmd internal
git commit -m "feat(hostcheck): 项目骨架与宿主环境自检

检查 Linux / root / cgroup v2 / overlayfs 四项前置条件，
不满足时由 agentbox doctor 给出可读的汇总错误。
testutil.RequireLinuxRoot 让沙箱测试在非 Linux 环境跳过而非失败。"
```

---

## Task 2: cgroup v2 组管理

**Files:**
- Create: `internal/cgroup/cgroup.go`
- Test: `internal/cgroup/cgroup_test.go`

**Interfaces:**
- Consumes: `testutil.RequireLinuxRoot`
- Produces:
  - `cgroup.Limits{CPUMax string; MemoryMax int64; PidsMax int}`
  - `cgroup.New(root, name string) (*Group, error)`
  - `func (g *Group) Path() string`
  - `func (g *Group) Apply(l Limits) error`
  - `func (g *Group) AddProc(pid int) error`
  - `func (g *Group) Procs() ([]int, error)`
  - `func (g *Group) Freeze() error`
  - `func (g *Group) Thaw() error`
  - `func (g *Group) Destroy() error`

- [ ] **Step 1: 写失败测试**

`internal/cgroup/cgroup_test.go`:

```go
//go:build linux

package cgroup

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

const testRoot = "/sys/fs/cgroup/agentbox-test"

func TestGroupLifecycle(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "lifecycle")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Destroy()

	if _, err := os.Stat(g.Path()); err != nil {
		t.Fatalf("cgroup 目录未创建: %v", err)
	}

	if err := g.Apply(Limits{CPUMax: "50000 100000", MemoryMax: 64 << 20, PidsMax: 32}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertFile(t, filepath.Join(g.Path(), "cpu.max"), "50000 100000")
	assertFile(t, filepath.Join(g.Path(), "memory.max"), "67108864")
	assertFile(t, filepath.Join(g.Path(), "pids.max"), "32")
}

func TestAddProcAndProcs(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "addproc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Destroy()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	defer cmd.Process.Kill()

	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}

	pids, err := g.Procs()
	if err != nil {
		t.Fatalf("Procs: %v", err)
	}
	if len(pids) != 1 || pids[0] != cmd.Process.Pid {
		t.Fatalf("Procs() = %v, want [%d]", pids, cmd.Process.Pid)
	}
}

func TestFreezeAndThaw(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "freeze")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Destroy()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	defer cmd.Process.Kill()
	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}

	if err := g.Freeze(); err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	waitFrozen(t, g, true)

	if err := g.Thaw(); err != nil {
		t.Fatalf("Thaw: %v", err)
	}
	waitFrozen(t, g, false)
}

func TestDestroyFailsWhileProcsRemain(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "destroy-busy")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}

	if err := g.Destroy(); err == nil {
		cmd.Process.Kill()
		g.Destroy()
		t.Fatal("cgroup 内仍有进程时 Destroy 应当失败")
	}

	cmd.Process.Kill()
	cmd.Wait()
	// 内核回收是异步的，重试到成功或超时。
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := g.Destroy(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("进程退出后 Destroy 仍持续失败")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	if got := strings.TrimSpace(string(b)); got != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func waitFrozen(t *testing.T, g *Group, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(g.Path(), "cgroup.events"))
		if err != nil {
			t.Fatalf("读取 cgroup.events: %v", err)
		}
		frozen := strings.Contains(string(b), "frozen 1")
		if frozen == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 frozen=%v 超时，events:\n%s", want, b)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/cgroup/ -v
```

Expected: 编译失败，`undefined: New`、`undefined: Limits`

> 注：`sudo go test` 会丢失 PATH 里的 go，必须用 `sudo -E env "PATH=$PATH" go test`。后续所有需要 root 的测试同理。

- [ ] **Step 3: 实现 cgroup**

`internal/cgroup/cgroup.go`:

```go
//go:build linux

// Package cgroup 提供 cgroup v2 组的最小管理能力。
//
// cgroup v2 的接口全部是文件读写：建组是 mkdir，限额是往
// cpu.max/memory.max/pids.max 写值，加进程是把 pid 写进 cgroup.procs，
// 销毁是 rmdir——但 rmdir 要求组内已无进程。
package cgroup

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Limits 是一个 cgroup 的资源限额。零值字段表示不设该项限制。
type Limits struct {
	// CPUMax 形如 "50000 100000"，含义是每 100ms 周期内最多用 50ms（即 0.5 核）。
	CPUMax string
	// MemoryMax 是内存上限，单位字节。
	MemoryMax int64
	// PidsMax 是进程数上限，用于防 fork 炸弹。
	PidsMax int
}

// Group 是一个 cgroup v2 组。
type Group struct {
	path string
}

// New 在 root 下创建名为 name 的 cgroup，并确保 root 已委派所需控制器。
func New(root, name string) (*Group, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("创建 cgroup 根 %s: %w", root, err)
	}
	// 子组要能设 cpu/memory/pids，父组必须先在 subtree_control 里启用它们。
	// 已启用时重复写不会报错；父组不支持某控制器时才会失败，此处容忍。
	_ = os.WriteFile(filepath.Join(filepath.Dir(root), "cgroup.subtree_control"),
		[]byte("+cpu +memory +pids"), 0o644)
	_ = os.WriteFile(filepath.Join(root, "cgroup.subtree_control"),
		[]byte("+cpu +memory +pids"), 0o644)

	path := filepath.Join(root, name)
	if err := os.Mkdir(path, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("创建 cgroup %s: %w", path, err)
	}
	return &Group{path: path}, nil
}

// Path 返回该 cgroup 在 sysfs 中的绝对路径。
func (g *Group) Path() string { return g.path }

// Apply 写入资源限额。零值字段被跳过。
func (g *Group) Apply(l Limits) error {
	if l.CPUMax != "" {
		if err := g.write("cpu.max", l.CPUMax); err != nil {
			return err
		}
	}
	if l.MemoryMax > 0 {
		if err := g.write("memory.max", strconv.FormatInt(l.MemoryMax, 10)); err != nil {
			return err
		}
	}
	if l.PidsMax > 0 {
		if err := g.write("pids.max", strconv.Itoa(l.PidsMax)); err != nil {
			return err
		}
	}
	return nil
}

// AddProc 把进程移入该 cgroup。移入后其全部子进程自动继承。
func (g *Group) AddProc(pid int) error {
	return g.write("cgroup.procs", strconv.Itoa(pid))
}

// Procs 返回当前在该 cgroup 内的所有进程号。
func (g *Group) Procs() ([]int, error) {
	b, err := os.ReadFile(filepath.Join(g.path, "cgroup.procs"))
	if err != nil {
		return nil, fmt.Errorf("读取 cgroup.procs: %w", err)
	}
	var pids []int
	for _, line := range strings.Fields(string(b)) {
		pid, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("解析 pid %q: %w", line, err)
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

// Freeze 冻结组内全部进程。冻结只停调度，不释放内存。
func (g *Group) Freeze() error { return g.write("cgroup.freeze", "1") }

// Thaw 解冻组内全部进程。
func (g *Group) Thaw() error { return g.write("cgroup.freeze", "0") }

// Destroy 删除该 cgroup。组内仍有进程时内核会拒绝，调用方需先确认进程已清零。
func (g *Group) Destroy() error {
	if err := os.Remove(g.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("删除 cgroup %s: %w", g.path, err)
	}
	return nil
}

func (g *Group) write(file, value string) error {
	p := filepath.Join(g.path, file)
	if err := os.WriteFile(p, []byte(value), 0o644); err != nil {
		return fmt.Errorf("写 %s = %q: %w", p, value, err)
	}
	return nil
}
```

- [ ] **Step 4: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/cgroup/ -v
```

Expected: 四个用例全部 PASS

- [ ] **Step 5: 提交**

```bash
git add internal/cgroup
git commit -m "feat(cgroup): cgroup v2 组的创建、限额、冻结与销毁

New 时在父组的 subtree_control 里启用 cpu/memory/pids 控制器，
否则子组里根本不会出现这几个限额文件。
Destroy 不做隐式清理——组内有进程时直接返回内核的拒绝，
由调用方负责先杀进程再删组（顺序见 spec §4.4）。"
```

---

## Task 3: overlayfs 与 bind mount

**Files:**
- Create: `internal/rootfs/overlay.go`
- Test: `internal/rootfs/overlay_test.go`

**Interfaces:**
- Consumes: `testutil.RequireLinuxRoot`
- Produces:
  - `rootfs.Overlay{Lower, Upper, Work, Merged string}`
  - `rootfs.Mount(o Overlay) error`
  - `rootfs.BindMount(src, dst string) error`
  - `rootfs.Unmount(path string) error`

- [ ] **Step 1: 写失败测试**

`internal/rootfs/overlay_test.go`:

```go
//go:build linux

package rootfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

func TestOverlayWritesLandInUpper(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	base := t.TempDir()
	o := Overlay{
		Lower:  filepath.Join(base, "lower"),
		Upper:  filepath.Join(base, "upper"),
		Work:   filepath.Join(base, "work"),
		Merged: filepath.Join(base, "merged"),
	}
	for _, d := range []string{o.Lower, o.Upper, o.Work, o.Merged} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	// lower 里放一个只读文件，验证它在 merged 里可见。
	if err := os.WriteFile(filepath.Join(o.Lower, "from-lower.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("写 lower 文件: %v", err)
	}

	if err := Mount(o); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	defer Unmount(o.Merged)

	// lower 的内容应当在 merged 里可见
	if b, err := os.ReadFile(filepath.Join(o.Merged, "from-lower.txt")); err != nil {
		t.Fatalf("merged 里读不到 lower 的文件: %v", err)
	} else if string(b) != "hello" {
		t.Fatalf("内容 = %q, want %q", b, "hello")
	}

	// 往 merged 里写，文件应落在 upper 而不是 lower
	if err := os.WriteFile(filepath.Join(o.Merged, "new.txt"), []byte("world"), 0o644); err != nil {
		t.Fatalf("写 merged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(o.Upper, "new.txt")); err != nil {
		t.Fatalf("新文件未落在 upper 层: %v", err)
	}
	if _, err := os.Stat(filepath.Join(o.Lower, "new.txt")); !os.IsNotExist(err) {
		t.Fatal("新文件不应出现在 lower 层——lower 必须保持只读")
	}
}

func TestUnmountIsIdempotent(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	dir := t.TempDir()
	if err := Unmount(dir); err != nil {
		t.Fatalf("对未挂载路径 Unmount 应当成功返回，实际: %v", err)
	}
}

func TestBindMount(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("bind"), 0o644); err != nil {
		t.Fatalf("写 src 文件: %v", err)
	}

	if err := BindMount(src, dst); err != nil {
		t.Fatalf("BindMount: %v", err)
	}
	defer Unmount(dst)

	b, err := os.ReadFile(filepath.Join(dst, "f.txt"))
	if err != nil {
		t.Fatalf("bind 目标读不到文件: %v", err)
	}
	if string(b) != "bind" {
		t.Fatalf("内容 = %q, want %q", b, "bind")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/rootfs/ -v
```

Expected: 编译失败，`undefined: Overlay`、`undefined: Mount`

- [ ] **Step 3: 实现 overlay**

`internal/rootfs/overlay.go`:

```go
//go:build linux

// Package rootfs 负责沙箱根文件系统的挂载。
//
// 采用 overlayfs 分层：模板作为只读 lower 层被所有实例共享，
// 每个实例只持有一个空的 upper 层。建箱因此不需要拷贝整个 rootfs，
// 打快照也只需打包 upper 层。
package rootfs

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

// Overlay 描述一次 overlayfs 挂载所需的四个目录。
type Overlay struct {
	Lower  string // 只读底层，通常是模板 rootfs
	Upper  string // 可写层，实例的全部改动落在这里
	Work   string // overlayfs 内部工作目录，必须与 Upper 同一文件系统
	Merged string // 挂载点，即沙箱看到的根
}

// Mount 执行 overlayfs 挂载。四个目录都必须已存在。
func (o Overlay) validate() error {
	for name, dir := range map[string]string{
		"Lower": o.Lower, "Upper": o.Upper, "Work": o.Work, "Merged": o.Merged,
	} {
		if dir == "" {
			return fmt.Errorf("Overlay.%s 为空", name)
		}
		if strings.ContainsAny(dir, ",:") {
			// overlayfs 挂载选项用逗号分隔、冒号分隔多个 lowerdir，
			// 路径里带这两个字符会把选项串解析坏。
			return fmt.Errorf("Overlay.%s = %q 含有 overlayfs 选项分隔符（, 或 :）", name, dir)
		}
		if fi, err := os.Stat(dir); err != nil {
			return fmt.Errorf("Overlay.%s = %q: %w", name, dir, err)
		} else if !fi.IsDir() {
			return fmt.Errorf("Overlay.%s = %q 不是目录", name, dir)
		}
	}
	return nil
}

// Mount 把 Lower/Upper/Work 三层联合挂载到 Merged。
func Mount(o Overlay) error {
	if err := o.validate(); err != nil {
		return err
	}
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", o.Lower, o.Upper, o.Work)
	if err := syscall.Mount("overlay", o.Merged, "overlay", 0, opts); err != nil {
		return fmt.Errorf("挂载 overlay 到 %s (%s): %w", o.Merged, opts, err)
	}
	return nil
}

// BindMount 把 src 绑定挂载到 dst，递归包含其下的子挂载。
func BindMount(src, dst string) error {
	if err := syscall.Mount(src, dst, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("bind 挂载 %s -> %s: %w", src, dst, err)
	}
	return nil
}

// Unmount 卸载 path。它是幂等的：路径本就未挂载时返回 nil。
//
// 常规卸载遇到 EBUSY（仍有进程持有该挂载）时，退化为 lazy umount：
// 先把挂载点从命名空间摘除，等引用归零后由内核自行清理。
// 这是清理路径上必须的兜底——否则一个还没退干净的进程就能让整箱卸不掉。
func Unmount(path string) error {
	err := syscall.Unmount(path, 0)
	switch {
	case err == nil:
		return nil
	case err == syscall.EINVAL, err == syscall.ENOENT:
		// EINVAL：该路径不是挂载点。视作已卸载。
		return nil
	case err == syscall.EBUSY:
		if err := syscall.Unmount(path, syscall.MNT_DETACH); err != nil {
			return fmt.Errorf("lazy 卸载 %s: %w", path, err)
		}
		return nil
	default:
		return fmt.Errorf("卸载 %s: %w", path, err)
	}
}
```

- [ ] **Step 4: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/rootfs/ -v
```

Expected: 三个用例全部 PASS

- [ ] **Step 5: 提交**

```bash
git add internal/rootfs
git commit -m "feat(rootfs): overlayfs 与 bind mount

validate 拒绝含逗号或冒号的路径——overlayfs 的挂载选项正是用
这两个字符分隔，路径里带上它们会把选项串解析坏。
Unmount 幂等，且 EBUSY 时退化为 MNT_DETACH lazy 卸载，
否则一个尚未退净的进程就能让整箱卸不掉。"
```

---

## Task 4: 模板 rootfs 就位

**Files:**
- Create: `internal/rootfs/template.go`
- Test: `internal/rootfs/template_test.go`

**Interfaces:**
- Consumes: 无
- Produces:
  - `rootfs.EnsureTemplate(dir string) error`
  - `rootfs.ExtractTarGz(tarPath, dstDir string) error`

- [ ] **Step 1: 手工准备模板**

alpine minirootfs 只有几 MB，手工解包一次即可，代码里不联网（WSL 也没有外网）：

```bash
sudo mkdir -p /var/lib/agentbox/templates/default
sudo tar -xzf /mnt/f/wsl-assets/alpine-minirootfs-3.24.2-x86_64.tar.gz \
     -C /var/lib/agentbox/templates/default
ls /var/lib/agentbox/templates/default
```

Expected: 列出 `bin dev etc home lib ... usr var`

```bash
sudo chroot /var/lib/agentbox/templates/default /bin/sh -c "echo ok"
```

Expected: `ok`

- [ ] **Step 2: 写失败测试**

`internal/rootfs/template_test.go`:

```go
//go:build linux

package rootfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureTemplateRejectsMissingDir(t *testing.T) {
	if err := EnsureTemplate("/definitely/not/here"); err == nil {
		t.Fatal("模板目录不存在时 EnsureTemplate 应当报错")
	}
}

func TestEnsureTemplateRejectsIncompleteRootfs(t *testing.T) {
	dir := t.TempDir()
	// 只有 bin，缺 etc 和 usr——这是解包到一半或解错目录的典型症状。
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := EnsureTemplate(dir)
	if err == nil {
		t.Fatal("模板缺少必需目录时 EnsureTemplate 应当报错")
	}
}

func TestEnsureTemplateAcceptsCompleteRootfs(t *testing.T) {
	dir := t.TempDir()
	for _, d := range requiredTemplateDirs {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("写 bin/sh: %v", err)
	}
	if err := EnsureTemplate(dir); err != nil {
		t.Fatalf("EnsureTemplate 对完整模板应当通过，实际: %v", err)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

```bash
go test ./internal/rootfs/ -run TestEnsureTemplate -v
```

Expected: 编译失败，`undefined: EnsureTemplate`、`undefined: requiredTemplateDirs`

- [ ] **Step 4: 实现**

`internal/rootfs/template.go`:

```go
//go:build linux

package rootfs

import (
	"fmt"
	"os"
	"path/filepath"
)

// requiredTemplateDirs 是一份可用模板 rootfs 必须具备的目录。
// 缺项通常意味着解包到一半或解错了目录——与其等 pivot_root 之后
// 在沙箱里报一个难以归因的错误，不如在建箱前就失败。
var requiredTemplateDirs = []string{"bin", "etc", "usr", "proc", "sys", "dev"}

// EnsureTemplate 校验模板 rootfs 是否就位可用。
func EnsureTemplate(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("模板目录 %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("模板路径 %s 不是目录", dir)
	}
	for _, d := range requiredTemplateDirs {
		p := filepath.Join(dir, d)
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("模板 %s 缺少必需目录 %s（模板可能未解包完整）: %w", dir, d, err)
		}
	}
	shell := filepath.Join(dir, "bin", "sh")
	if _, err := os.Stat(shell); err != nil {
		return fmt.Errorf("模板 %s 缺少 /bin/sh: %w", dir, err)
	}
	return nil
}
```

> `proc` / `sys` / `dev` 在 alpine minirootfs 的 tar 包里是空目录，解包后存在。它们是 Task 6 挂伪文件系统的挂载点，缺了 pivot_root 之后会挂不上去。

- [ ] **Step 5: 跑测试确认通过**

```bash
go test ./internal/rootfs/ -v
```

Expected: 六个用例全部 PASS（Task 3 的三个 + 本任务的三个）

- [ ] **Step 6: 提交**

```bash
git add internal/rootfs/template.go internal/rootfs/template_test.go
git commit -m "feat(rootfs): 模板 rootfs 就位校验

在建箱前校验模板完整性，而不是等 pivot_root 之后在沙箱里
报一个难以归因的错误。proc/sys/dev 三个空目录也在必需列表里——
它们是伪文件系统的挂载点，缺了之后挂不上去。"
```

---

## Task 5: namespace 启动沙箱进程与 re-exec init

**Files:**
- Create: `internal/runtime/spawn.go`
- Create: `internal/runtime/init.go`
- Modify: `cmd/agentbox/main.go`
- Test: `internal/runtime/spawn_test.go`

**Interfaces:**
- Consumes: `testutil.RequireLinuxRoot`
- Produces:
  - `runtime.SpawnConfig{Root string; Hostname string; ControlFD *os.File}`
  - `runtime.Spawn(cfg SpawnConfig) (*os.Process, error)`
  - `runtime.RunInit() error`（在子进程中调用，永不正常返回）
  - `runtime.InitArg = "init"`（子命令名）

- [ ] **Step 1: 写失败测试**

`internal/runtime/spawn_test.go`:

```go
//go:build linux

package runtime

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

// TestNamespacesAreIsolated 验证 clone flags 确实生效：
// 子进程在自己的 PID namespace 里是 1 号，且改主机名不影响宿主。
func TestNamespacesAreIsolated(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	hostBefore, err := os.Hostname()
	if err != nil {
		t.Fatalf("读取宿主 hostname: %v", err)
	}

	cmd := exec.Command("/bin/sh", "-c", "hostname sandbox-test && echo $$ && hostname")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: CloneFlags,
		Setpgid:    true,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("运行隔离进程: %v\n输出:\n%s", err, out)
	}

	lines := strings.Fields(string(out))
	if len(lines) < 2 {
		t.Fatalf("输出格式意外: %q", out)
	}
	if lines[0] != "1" {
		t.Fatalf("子进程 PID = %s, want 1（PID namespace 未生效）", lines[0])
	}
	if lines[1] != "sandbox-test" {
		t.Fatalf("沙箱内 hostname = %s, want sandbox-test", lines[1])
	}

	hostAfter, err := os.Hostname()
	if err != nil {
		t.Fatalf("重新读取宿主 hostname: %v", err)
	}
	if hostAfter != hostBefore {
		t.Fatalf("宿主 hostname 被改成了 %s（UTS namespace 未生效）", hostAfter)
	}
}

// TestReExecRunsInitBranch 验证 /proc/self/exe init 这条分流确实被走到。
func TestReExecRunsInitBranch(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	if os.Getenv(envInitProbe) == "1" {
		// 这是被 re-exec 出来的那一侧，打个标记就退出。
		os.Stdout.WriteString("init-branch-reached")
		os.Exit(0)
	}

	cmd := exec.Command("/proc/self/exe", "-test.run=TestReExecRunsInitBranch")
	cmd.Env = append(os.Environ(), envInitProbe+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("re-exec 自身: %v\n输出:\n%s", err, out)
	}
	if !strings.Contains(string(out), "init-branch-reached") {
		t.Fatalf("未走到 init 分支，输出: %q", out)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -v
```

Expected: 编译失败，`undefined: CloneFlags`、`undefined: envInitProbe`

- [ ] **Step 3: 实现 spawn**

`internal/runtime/spawn.go`:

```go
//go:build linux

// Package runtime 负责沙箱进程的启动与其内部的 1 号进程。
package runtime

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// InitArg 是 re-exec 时传给自身的子命令名。
const InitArg = "init"

// envInitProbe 仅供测试用于识别被 re-exec 出来的那一侧。
const envInitProbe = "AGENTBOX_INIT_PROBE"

// envSandboxRoot 把新根目录传给 init 侧。
const envSandboxRoot = "AGENTBOX_ROOT"

// envSandboxHostname 把沙箱主机名传给 init 侧。
const envSandboxHostname = "AGENTBOX_HOSTNAME"

// controlFD 是控制连接在 init 侧的文件描述符号。
// 0/1/2 是标准流，ExtraFiles 的第一个元素落在 3。
const controlFD = 3

// CloneFlags 是沙箱进程要进入的命名空间集合。
//
// 不含 CLONE_NEWUSER：M1 以 root 运行，rootless 是后续待定项。
// 其中 CLONE_NEWPID 尤为关键——它不只隔离进程视图，更提供了一个
// 可靠的"整组杀"边界：杀掉该 namespace 的 1 号进程，内核会连带
// 清理其中所有进程，孙子进程也跑不掉。
const CloneFlags = syscall.CLONE_NEWNS |
	syscall.CLONE_NEWPID |
	syscall.CLONE_NEWUTS |
	syscall.CLONE_NEWIPC |
	syscall.CLONE_NEWNET

// SpawnConfig 是启动一个沙箱 init 进程所需的参数。
type SpawnConfig struct {
	// Root 是 overlayfs 的 merged 目录，将成为沙箱的新根。
	Root string
	// Hostname 是沙箱内的主机名。
	Hostname string
	// ControlFD 是 socketpair 的沙箱侧端点，会作为 fd 3 传给 init。
	ControlFD *os.File
}

// Spawn 以新的命名空间启动沙箱 init 进程。
//
// 启动的是 /proc/self/exe init——即本程序自身。这么做是因为
// pivot_root 一类调用是线程级的，而 goroutine 会在 OS 线程间迁移；
// 只有在 Go runtime 尚未铺开的新进程最早期执行它们才安全。
func Spawn(cfg SpawnConfig) (*os.Process, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("SpawnConfig.Root 不能为空")
	}
	if cfg.ControlFD == nil {
		return nil, fmt.Errorf("SpawnConfig.ControlFD 不能为空")
	}

	cmd := exec.Command("/proc/self/exe", InitArg)
	cmd.Env = []string{
		envSandboxRoot + "=" + cfg.Root,
		envSandboxHostname + "=" + cfg.Hostname,
	}
	cmd.ExtraFiles = []*os.File{cfg.ControlFD}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: CloneFlags,
		// 单独的进程组，便于在 init 尚未就绪时也能整组发信号。
		Setpgid: true,
		// 父进程意外退出时，内核给 init 发 SIGKILL，避免沙箱变孤儿。
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stderr // init 自身的日志并入宿主 stderr，便于排障
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("启动沙箱 init: %w", err)
	}
	return cmd.Process, nil
}
```

`internal/runtime/init.go`（本任务先只放骨架，Task 6/7/8 逐步填充）：

```go
//go:build linux

package runtime

import (
	"fmt"
	"os"
	"syscall"
)

// RunInit 是沙箱内 1 号进程的主函数。它永不正常返回。
//
// 调用时机：进程以 `/proc/self/exe init` 启动，且 main 在做任何
// 其他初始化之前就分流到这里。
func RunInit() error {
	root := os.Getenv(envSandboxRoot)
	if root == "" {
		return fmt.Errorf("缺少环境变量 %s", envSandboxRoot)
	}
	if h := os.Getenv(envSandboxHostname); h != "" {
		if err := syscall.Sethostname([]byte(h)); err != nil {
			return fmt.Errorf("设置沙箱 hostname: %w", err)
		}
	}
	// pivot_root 与伪文件系统挂载在 Task 6 补齐。
	// 收割与控制连接服务在 Task 7 / Task 8 补齐。
	return fmt.Errorf("init 尚未实现完整")
}
```

- [ ] **Step 4: 在 main 里加 init 分流**

`cmd/agentbox/main.go`（完整替换）:

```go
package main

import (
	"fmt"
	"os"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/hostcheck"
)

func main() {
	// init 分流必须是 main 的第一件事，早于任何其他初始化。
	// 这个分支是被 Spawn 以 /proc/self/exe init 启动的沙箱 1 号进程。
	if len(os.Args) > 1 && os.Args[1] == "init" {
		if err := runSandboxInit(); err != nil {
			fmt.Fprintln(os.Stderr, "sandbox init:", err)
			os.Exit(1)
		}
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "doctor" {
		r := hostcheck.Check()
		if err := r.Err(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("宿主环境检查通过")
		return
	}

	fmt.Fprintln(os.Stderr, "用法: agentbox doctor")
	os.Exit(2)
}
```

`cmd/agentbox/init_linux.go`:

```go
//go:build linux

package main

import "github.com/wanghr0318-dotcom/go-agentbox/internal/runtime"

func runSandboxInit() error { return runtime.RunInit() }
```

`cmd/agentbox/init_other.go`:

```go
//go:build !linux

package main

import "errors"

func runSandboxInit() error { return errors.New("沙箱 init 仅在 Linux 上可用") }
```

- [ ] **Step 5: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -v
go build ./...
```

Expected: 两个用例 PASS；`go build ./...` 无输出

- [ ] **Step 6: 提交**

```bash
git add internal/runtime cmd/agentbox
git commit -m "feat(runtime): namespace 启动沙箱进程与 re-exec init 分流

启动 /proc/self/exe init 而非直接 fork：pivot_root 一类调用是线程级的，
而 goroutine 会在 OS 线程间迁移，只有在 Go runtime 尚未铺开的新进程
最早期执行才安全。main 的 init 分流因此必须是第一条语句。

CloneFlags 不含 CLONE_NEWUSER——M1 以 root 运行。
Pdeathsig 保证父进程意外退出时沙箱不会变成孤儿。"
```

---

## Task 6: pivot_root 与伪文件系统

**Files:**
- Modify: `internal/runtime/init.go`
- Test: `internal/runtime/pivot_test.go`

**Interfaces:**
- Consumes: `rootfs.Mount`、`rootfs.BindMount`
- Produces: `runtime.pivotRoot(newRoot string) error`、`runtime.mountPseudoFS() error`（包内）

- [ ] **Step 1: 写失败测试**

`internal/runtime/pivot_test.go`:

```go
//go:build linux

package runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

const templateDir = "/var/lib/agentbox/templates/default"

// TestPivotRootHidesHostFS 在一个子进程里完成 pivot_root，
// 验证换根之后看不到宿主的文件系统。
func TestPivotRootHidesHostFS(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	if _, err := os.Stat(templateDir); err != nil {
		t.Skipf("模板 rootfs 未就位（见 Task 4）: %v", err)
	}

	if os.Getenv(envPivotProbe) == "1" {
		// 子进程侧：换根后检查宿主特征文件是否还可见。
		root := os.Getenv(envSandboxRoot)
		if err := pivotRoot(root); err != nil {
			os.Stderr.WriteString("pivotRoot: " + err.Error())
			os.Exit(1)
		}
		if err := mountPseudoFS(); err != nil {
			os.Stderr.WriteString("mountPseudoFS: " + err.Error())
			os.Exit(1)
		}
		// /var/lib/agentbox 是宿主路径，换根后不该存在。
		if _, err := os.Stat("/var/lib/agentbox"); err == nil {
			os.Stdout.WriteString("HOST-VISIBLE")
			os.Exit(0)
		}
		// /proc 应当挂上了，自检 pid 为 1。
		b, err := os.ReadFile("/proc/self/stat")
		if err != nil {
			os.Stdout.WriteString("NO-PROC")
			os.Exit(0)
		}
		if !strings.HasPrefix(string(b), "1 ") {
			os.Stdout.WriteString("BAD-PID")
			os.Exit(0)
		}
		os.Stdout.WriteString("ISOLATED")
		os.Exit(0)
	}

	base := t.TempDir()
	o := rootfs.Overlay{
		Lower:  templateDir,
		Upper:  filepath.Join(base, "upper"),
		Work:   filepath.Join(base, "work"),
		Merged: filepath.Join(base, "merged"),
	}
	for _, d := range []string{o.Upper, o.Work, o.Merged} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := rootfs.Mount(o); err != nil {
		t.Fatalf("挂载 overlay: %v", err)
	}
	defer rootfs.Unmount(o.Merged)

	cmd := exec.Command("/proc/self/exe", "-test.run=TestPivotRootHidesHostFS")
	cmd.Env = append(os.Environ(), envPivotProbe+"=1", envSandboxRoot+"="+o.Merged)
	cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: CloneFlags, Setpgid: true}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("子进程失败: %v\n输出:\n%s", err, out)
	}
	if !strings.Contains(string(out), "ISOLATED") {
		t.Fatalf("换根未生效，输出: %q", out)
	}
}
```

同时在 `internal/runtime/spawn.go` 的常量区补上：

```go
// envPivotProbe 仅供测试识别换根探针子进程。
const envPivotProbe = "AGENTBOX_PIVOT_PROBE"
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -run TestPivotRoot -v
```

Expected: 编译失败，`undefined: pivotRoot`、`undefined: mountPseudoFS`

- [ ] **Step 3: 实现换根与伪文件系统**

`internal/runtime/init.go`（完整替换）:

```go
//go:build linux

package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// pivotOldDir 是 pivot_root 期间旧根的临时挂载点，换根后立即卸载删除。
const pivotOldDir = ".pivot_old"

// RunInit 是沙箱内 1 号进程的主函数。它永不正常返回。
func RunInit() error {
	root := os.Getenv(envSandboxRoot)
	if root == "" {
		return fmt.Errorf("缺少环境变量 %s", envSandboxRoot)
	}
	if h := os.Getenv(envSandboxHostname); h != "" {
		if err := syscall.Sethostname([]byte(h)); err != nil {
			return fmt.Errorf("设置沙箱 hostname: %w", err)
		}
	}
	if err := pivotRoot(root); err != nil {
		return fmt.Errorf("换根: %w", err)
	}
	if err := mountPseudoFS(); err != nil {
		return fmt.Errorf("挂载伪文件系统: %w", err)
	}
	// 收割与控制连接服务在 Task 7 / Task 8 补齐。
	return fmt.Errorf("init 尚未实现完整")
}

// pivotRoot 把 newRoot 变成当前挂载命名空间的根。
func pivotRoot(newRoot string) error {
	// 1. 把根挂载点标记为 private。不这么做，后续挂载会传播回宿主，
	//    沙箱里挂的 /proc 会出现在宿主的挂载表里。
	if err := syscall.Mount("", "/", "", syscall.MS_PRIVATE|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("将 / 标记为 private: %w", err)
	}
	// 2. pivot_root 要求 newRoot 本身是一个挂载点。overlayfs 的 merged
	//    已经是了，但为兼容其它情形，这里把它 bind 到自身以确保成立。
	if err := syscall.Mount(newRoot, newRoot, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("将 %s bind 到自身: %w", newRoot, err)
	}
	// 3. 准备旧根的临时挂载点。
	old := filepath.Join(newRoot, pivotOldDir)
	if err := os.MkdirAll(old, 0o700); err != nil {
		return fmt.Errorf("创建 %s: %w", old, err)
	}
	// 4. 换根。
	if err := syscall.PivotRoot(newRoot, old); err != nil {
		return fmt.Errorf("pivot_root(%s, %s): %w", newRoot, old, err)
	}
	// 5. 换根后当前工作目录仍指向旧根，必须显式回到新根。
	if err := syscall.Chdir("/"); err != nil {
		return fmt.Errorf("chdir /: %w", err)
	}
	// 6. 摘掉旧根并删除挂载点。用 MNT_DETACH 是因为此刻旧根上
	//    可能仍有引用，lazy 卸载让内核在引用归零后自行清理。
	oldInNewRoot := "/" + pivotOldDir
	if err := syscall.Unmount(oldInNewRoot, syscall.MNT_DETACH); err != nil {
		return fmt.Errorf("卸载旧根 %s: %w", oldInNewRoot, err)
	}
	if err := os.Remove(oldInNewRoot); err != nil {
		return fmt.Errorf("删除 %s: %w", oldInNewRoot, err)
	}
	return nil
}

// mountPseudoFS 挂载沙箱内必需的伪文件系统。
func mountPseudoFS() error {
	// /proc 必须在新的 PID namespace 内挂载，这样 ps 之类才看到隔离后的视图。
	if err := syscall.Mount("proc", "/proc", "proc",
		syscall.MS_NOSUID|syscall.MS_NOEXEC|syscall.MS_NODEV, ""); err != nil {
		return fmt.Errorf("挂载 /proc: %w", err)
	}
	if err := syscall.Mount("sysfs", "/sys", "sysfs",
		syscall.MS_NOSUID|syscall.MS_NOEXEC|syscall.MS_NODEV|syscall.MS_RDONLY, ""); err != nil {
		return fmt.Errorf("挂载 /sys: %w", err)
	}
	if err := syscall.Mount("tmpfs", "/dev", "tmpfs",
		syscall.MS_NOSUID|syscall.MS_STRICTATIME, "mode=755,size=64k"); err != nil {
		return fmt.Errorf("挂载 /dev: %w", err)
	}
	// /dev 是新挂的 tmpfs，里面空无一物；连 /dev/null 都得自己造，
	// 否则沙箱里任何 `cmd > /dev/null` 都会失败。
	for _, d := range []struct {
		name        string
		major, minor int
	}{
		{"null", 1, 3},
		{"zero", 1, 5},
		{"random", 1, 8},
		{"urandom", 1, 9},
		{"tty", 5, 0},
	} {
		path := "/dev/" + d.name
		dev := int(uint32(d.major)<<8 | uint32(d.minor))
		if err := syscall.Mknod(path, syscall.S_IFCHR|0o666, dev); err != nil {
			return fmt.Errorf("创建 %s: %w", path, err)
		}
	}
	return nil
}
```

- [ ] **Step 4: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -run TestPivotRoot -v
```

Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/runtime
git commit -m "feat(runtime): pivot_root 换根与伪文件系统挂载

先把 / 标记为 MS_PRIVATE|MS_REC，否则沙箱内的挂载会传播回宿主，
沙箱挂的 /proc 会出现在宿主的挂载表里。

/dev 挂的是空 tmpfs，连 /dev/null 都要用 mknod 自己造——
不造的话沙箱里任何重定向到 /dev/null 的命令都会失败。"
```

---

## Task 7: init 的孤儿进程收割

**Files:**
- Modify: `internal/runtime/init.go`
- Test: `internal/runtime/reap_test.go`

**Interfaces:**
- Produces: `runtime.reapLoop(done <-chan struct{})`（包内）

- [ ] **Step 1: 写失败测试**

`internal/runtime/reap_test.go`:

```go
//go:build linux

package runtime

import (
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

// TestReapLoopCollectsOrphans 起一个自己不 wait 的子进程，
// 验证 reapLoop 能把它收掉而不是留成僵尸。
func TestReapLoopCollectsOrphans(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	done := make(chan struct{})
	defer close(done)
	go reapLoop(done)

	cmd := exec.Command("/bin/true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程: %v", err)
	}
	pid := cmd.Process.Pid

	// 故意不调用 cmd.Wait()——收割应由 reapLoop 完成。
	// 进程被收割后，向它发 0 号信号会返回 ESRCH。
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if err == syscall.ESRCH {
			return // 已被收割
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d 在 3s 内未被收割，kill(0) 返回 %v", pid, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -run TestReapLoop -v
```

Expected: 编译失败，`undefined: reapLoop`

- [ ] **Step 3: 实现收割**

在 `internal/runtime/init.go` 中，把 import 块补上 `os/signal`，并在 `RunInit` 的末尾之前加入收割启动，同时新增 `reapLoop`：

```go
// reapLoop 持续收割已退出的子进程。
//
// 沙箱内的 1 号进程继承了 init 的职责：所有父进程先退出的孤儿进程
// 都会被重新挂到它名下。不收割的话僵尸会不断累积，最终撑满
// cgroup 的 pids.max，沙箱把自己饿死。
func reapLoop(done <-chan struct{}) {
	ch := make(chan os.Signal, 64)
	signal.Notify(ch, syscall.SIGCHLD)
	defer signal.Stop(ch)

	for {
		select {
		case <-done:
			return
		case <-ch:
			// 一次 SIGCHLD 可能对应多个已退出的子进程（信号会合并），
			// 因此必须循环收到 Wait4 无更多可收为止。
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if pid <= 0 || err != nil {
					break
				}
			}
		}
	}
}
```

`RunInit` 中在挂载伪文件系统之后加入：

```go
	done := make(chan struct{})
	defer close(done)
	go reapLoop(done)

	// 控制连接服务在 Task 8 补齐。
	return fmt.Errorf("init 尚未实现完整")
```

import 块变为：

```go
import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)
```

- [ ] **Step 4: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -run TestReapLoop -v
```

Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/runtime
git commit -m "feat(runtime): init 收割孤儿进程

沙箱内 1 号进程继承 init 职责，所有孤儿进程会挂到它名下。
不收割的话僵尸持续累积，最终撑满 cgroup 的 pids.max，
沙箱把自己饿死。

一次 SIGCHLD 可能对应多个已退出子进程（信号会合并），
所以必须循环 Wait4(WNOHANG) 直到无更多可收。"
```

---

## Task 8: 控制连接与沙箱内执行

**Files:**
- Create: `internal/runtime/protocol.go`
- Create: `internal/runtime/client.go`
- Modify: `internal/runtime/init.go`
- Test: `internal/runtime/exec_test.go`

**Interfaces:**
- Produces:
  - `runtime.ExecRequest{Cmd []string; Env []string; WorkDir string; TimeoutMS int}`
  - `runtime.Frame{Type string; Data string; ExitCode int; Err string}`（`Type` 取 `stdout`/`stderr`/`exit`）
  - `runtime.NewControlPair() (host, sandbox *os.File, err error)`
  - `runtime.Client{}`、`runtime.NewClient(host *os.File) (*Client, error)`
  - `func (c *Client) Exec(req ExecRequest, stdout, stderr io.Writer) (exitCode int, err error)`
  - `runtime.serveControl(fd int) error`（包内）

- [ ] **Step 1: 写失败测试**

`internal/runtime/exec_test.go`:

```go
//go:build linux

package runtime

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

func TestExecInSandbox(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	if _, err := os.Stat(templateDir); err != nil {
		t.Skipf("模板 rootfs 未就位（见 Task 4）: %v", err)
	}

	base := t.TempDir()
	o := rootfs.Overlay{
		Lower:  templateDir,
		Upper:  filepath.Join(base, "upper"),
		Work:   filepath.Join(base, "work"),
		Merged: filepath.Join(base, "merged"),
	}
	for _, d := range []string{o.Upper, o.Work, o.Merged} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := rootfs.Mount(o); err != nil {
		t.Fatalf("挂载 overlay: %v", err)
	}
	defer rootfs.Unmount(o.Merged)

	host, sandbox, err := NewControlPair()
	if err != nil {
		t.Fatalf("NewControlPair: %v", err)
	}
	defer host.Close()

	proc, err := Spawn(SpawnConfig{Root: o.Merged, Hostname: "box", ControlFD: sandbox})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	sandbox.Close() // 父进程这一侧不再需要沙箱端点
	defer proc.Kill()

	c, err := NewClient(host)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	t.Run("stdout 与退出码", func(t *testing.T) {
		var out, errBuf bytes.Buffer
		code, err := c.Exec(ExecRequest{Cmd: []string{"/bin/sh", "-c", "echo hello"}}, &out, &errBuf)
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}
		if code != 0 {
			t.Fatalf("退出码 = %d, want 0（stderr: %s）", code, errBuf.String())
		}
		if got := strings.TrimSpace(out.String()); got != "hello" {
			t.Fatalf("stdout = %q, want %q", got, "hello")
		}
	})

	t.Run("非零退出码", func(t *testing.T) {
		var out, errBuf bytes.Buffer
		code, err := c.Exec(ExecRequest{Cmd: []string{"/bin/sh", "-c", "exit 42"}}, &out, &errBuf)
		if err != nil {
			t.Fatalf("Exec: %v", err)
		}
		if code != 42 {
			t.Fatalf("退出码 = %d, want 42", code)
		}
	})

	t.Run("沙箱内 PID 隔离", func(t *testing.T) {
		var out, errBuf bytes.Buffer
		if _, err := c.Exec(ExecRequest{Cmd: []string{"/bin/sh", "-c", "ls /proc | grep -c '^1$'"}}, &out, &errBuf); err != nil {
			t.Fatalf("Exec: %v", err)
		}
		if got := strings.TrimSpace(out.String()); got != "1" {
			t.Fatalf("沙箱内 /proc 中 pid 1 的数量 = %q, want %q", got, "1")
		}
	})

	t.Run("看不到宿主文件系统", func(t *testing.T) {
		var out, errBuf bytes.Buffer
		code, _ := c.Exec(ExecRequest{Cmd: []string{"/bin/sh", "-c", "test -e /var/lib/agentbox"}}, &out, &errBuf)
		if code == 0 {
			t.Fatal("沙箱内不应看到宿主的 /var/lib/agentbox")
		}
	})
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -run TestExecInSandbox -v
```

Expected: 编译失败，`undefined: NewControlPair`、`undefined: NewClient`

- [ ] **Step 3: 实现协议**

`internal/runtime/protocol.go`:

```go
//go:build linux

package runtime

import (
	"fmt"
	"os"
	"syscall"
)

// ExecRequest 是一次沙箱内命令执行请求。
type ExecRequest struct {
	Cmd       []string `json:"cmd"`
	Env       []string `json:"env,omitempty"`
	WorkDir   string   `json:"work_dir,omitempty"`
	TimeoutMS int      `json:"timeout_ms,omitempty"`
}

// Frame 是沙箱回传的一帧。Type 为 stdout/stderr 时 Data 有值，
// 为 exit 时 ExitCode 有值、Err 非空表示执行本身失败。
type Frame struct {
	Type     string `json:"type"`
	Data     string `json:"data,omitempty"`
	ExitCode int    `json:"exit_code,omitempty"`
	Err      string `json:"err,omitempty"`
}

// NewControlPair 创建一对已连接的 unix 域套接字端点。
//
// 用 socketpair 而非套接字文件，是因为沙箱有独立的挂载命名空间，
// 宿主看不到沙箱内的路径、沙箱也看不到宿主的路径。fd 在 fork 时
// 直接继承，绕开文件系统，两侧天然连通。
func NewControlPair() (host, sandbox *os.File, err error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("创建 socketpair: %w", err)
	}
	host = os.NewFile(uintptr(fds[0]), "agentbox-control-host")
	sandbox = os.NewFile(uintptr(fds[1]), "agentbox-control-sandbox")
	return host, sandbox, nil
}
```

- [ ] **Step 4: 实现沙箱侧服务**

在 `internal/runtime/init.go` 中新增 `serveControl`，并把 `RunInit` 的收尾改为调用它：

```go
// serveControl 在控制连接上循环处理执行请求，直到对端关闭。
func serveControl(f *os.File) error {
	defer f.Close()
	dec := json.NewDecoder(f)
	enc := json.NewEncoder(f)

	for {
		var req ExecRequest
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil // 宿主关闭连接，正常收摊
			}
			return fmt.Errorf("解码执行请求: %w", err)
		}
		if err := runOne(req, enc); err != nil {
			// 执行层面的失败也要回一帧，否则宿主会一直等。
			_ = enc.Encode(Frame{Type: "exit", ExitCode: -1, Err: err.Error()})
		}
	}
}

func runOne(req ExecRequest, enc *json.Encoder) error {
	if len(req.Cmd) == 0 {
		return fmt.Errorf("ExecRequest.Cmd 为空")
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if req.TimeoutMS > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, req.Cmd[0], req.Cmd[1:]...)
	cmd.Env = req.Env
	if req.WorkDir != "" {
		cmd.Dir = req.WorkDir
	}
	// 子进程放进独立进程组，超时时可以整组杀而不只是杀直接子进程。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("建立 stdout 管道: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("建立 stderr 管道: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动 %v: %w", req.Cmd, err)
	}

	// 两条流并发转发；同一个 encoder 不能并发写，用锁串起来。
	var mu sync.Mutex
	var wg sync.WaitGroup
	pump := func(r io.Reader, kind string) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				mu.Lock()
				_ = enc.Encode(Frame{Type: kind, Data: string(buf[:n])})
				mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pump(stdout, "stdout")
	go pump(stderr, "stderr")
	wg.Wait()

	code := 0
	if err := cmd.Wait(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else {
			return fmt.Errorf("等待 %v: %w", req.Cmd, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return enc.Encode(Frame{Type: "exit", ExitCode: code})
}
```

`RunInit` 的结尾改为：

```go
	done := make(chan struct{})
	defer close(done)
	go reapLoop(done)

	return serveControl(os.NewFile(controlFD, "agentbox-control"))
```

`internal/runtime/init.go` 的 import 块变为：

```go
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)
```

- [ ] **Step 5: 实现宿主侧客户端**

`internal/runtime/client.go`:

```go
//go:build linux

package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// Client 是宿主侧的沙箱执行客户端。
type Client struct {
	mu  sync.Mutex // 串行化请求：一条控制连接同一时刻只跑一个命令
	f   *os.File
	enc *json.Encoder
	dec *json.Decoder
}

// NewClient 基于控制连接的宿主端点创建客户端。
func NewClient(host *os.File) (*Client, error) {
	if host == nil {
		return nil, fmt.Errorf("控制连接为空")
	}
	return &Client{f: host, enc: json.NewEncoder(host), dec: json.NewDecoder(host)}, nil
}

// Exec 在沙箱内执行一条命令，把 stdout/stderr 实时写入给定的 writer，
// 返回命令的退出码。
//
// 同一个 Client 的多次 Exec 会被串行化——一条控制连接上的帧没有
// 请求标识，并发发送会让两条命令的输出交错到一起。需要并发时
// 应当建立多条控制连接，而不是复用同一条。
func (c *Client) Exec(req ExecRequest, stdout, stderr io.Writer) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.enc.Encode(req); err != nil {
		return -1, fmt.Errorf("发送执行请求: %w", err)
	}
	for {
		var fr Frame
		if err := c.dec.Decode(&fr); err != nil {
			return -1, fmt.Errorf("读取回帧: %w", err)
		}
		switch fr.Type {
		case "stdout":
			if stdout != nil {
				if _, err := io.WriteString(stdout, fr.Data); err != nil {
					return -1, fmt.Errorf("写 stdout: %w", err)
				}
			}
		case "stderr":
			if stderr != nil {
				if _, err := io.WriteString(stderr, fr.Data); err != nil {
					return -1, fmt.Errorf("写 stderr: %w", err)
				}
			}
		case "exit":
			if fr.Err != "" {
				return fr.ExitCode, fmt.Errorf("沙箱内执行失败: %s", fr.Err)
			}
			return fr.ExitCode, nil
		default:
			return -1, fmt.Errorf("未知帧类型 %q", fr.Type)
		}
	}
}

// Close 关闭控制连接，沙箱侧的 serveControl 会随之收摊。
func (c *Client) Close() error { return c.f.Close() }
```

- [ ] **Step 6: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/runtime/ -v
```

Expected: 全部 PASS，含 `TestExecInSandbox` 的四个子用例

- [ ] **Step 7: 提交**

```bash
git add internal/runtime
git commit -m "feat(runtime): 控制连接与沙箱内命令执行

用 socketpair 而非套接字文件：沙箱有独立挂载命名空间，
两侧互相看不到对方的路径；fd 在 fork 时直接继承，绕开文件系统。

被执行的命令放进独立进程组，超时时整组 SIGKILL，
否则 CommandContext 只杀直接子进程，孙子进程会漏网。

Client.Exec 串行化：帧上没有请求标识，并发发送会让
两条命令的输出交错。需要并发应建多条控制连接。"
```

---

## Task 9: Provider 接口与 LocalProvider 建箱

**Files:**
- Create: `internal/provider/provider.go`
- Create: `internal/provider/local/local.go`
- Test: `internal/provider/local/local_test.go`

**Interfaces:**
- Consumes: `cgroup.*`、`rootfs.*`、`runtime.*`
- Produces:
  - `provider.CreateSpec{Name, Template, Workspace string; Limits ResourceLimits}`
  - `provider.ResourceLimits{CPUMax string; MemoryMax int64; PidsMax int}`
  - `provider.Instance{ID, Name, State string}`
  - `provider.ExecRequest{Cmd, Env []string; WorkDir string; Stdout, Stderr io.Writer; Timeout time.Duration}`
  - `provider.ExecResult{ExitCode int; Duration time.Duration; Killed, OOM bool}`
  - `local.New(root string) (*Provider, error)`
  - `func (p *Provider) Create(ctx, spec) (*provider.Instance, error)`
  - `func (p *Provider) Exec(ctx, id string, req provider.ExecRequest) (*provider.ExecResult, error)`

- [ ] **Step 1: 写失败测试**

`internal/provider/local/local_test.go`:

```go
//go:build linux

package local

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

func newTestProvider(t *testing.T) (*Provider, string) {
	t.Helper()
	testutil.RequireLinuxRoot(t)
	if _, err := os.Stat("/var/lib/agentbox/templates/default"); err != nil {
		t.Skipf("模板 rootfs 未就位（见 Task 4）: %v", err)
	}
	root := t.TempDir()
	// 模板必须与实例同在一个文件系统，overlayfs 才挂得起来。
	// 直接复用宿主上的模板目录作为 lower。
	p, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, root
}

func TestCreateAndExec(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	ws := filepath.Join(root, "ws", "chat-1")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	inst, err := p.Create(ctx, provider.CreateSpec{
		Name:      "sb:chat-1",
		Template:  "/var/lib/agentbox/templates/default",
		Workspace: ws,
		Limits:    provider.ResourceLimits{CPUMax: "50000 100000", MemoryMax: 128 << 20, PidsMax: 64},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer p.Close(ctx, inst.ID)

	var out, errBuf bytes.Buffer
	res, err := p.Exec(ctx, inst.ID, provider.ExecRequest{
		Cmd:    []string{"/bin/sh", "-c", "echo from-sandbox"},
		Stdout: &out,
		Stderr: &errBuf,
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("退出码 = %d（stderr: %s）", res.ExitCode, errBuf.String())
	}
	if got := strings.TrimSpace(out.String()); got != "from-sandbox" {
		t.Fatalf("stdout = %q, want %q", got, "from-sandbox")
	}
}

func TestWorkspaceIsPersistedOnHost(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	ws := filepath.Join(root, "ws", "chat-2")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	inst, err := p.Create(ctx, provider.CreateSpec{
		Name:      "sb:chat-2",
		Template:  "/var/lib/agentbox/templates/default",
		Workspace: ws,
		Limits:    provider.ResourceLimits{PidsMax: 64},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer p.Close(ctx, inst.ID)

	var out, errBuf bytes.Buffer
	if _, err := p.Exec(ctx, inst.ID, provider.ExecRequest{
		Cmd:    []string{"/bin/sh", "-c", "echo persisted > /workspace/f.txt"},
		Stdout: &out,
		Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("Exec: %v（stderr: %s）", err, errBuf.String())
	}

	// 沙箱销毁前，文件就应当已经在宿主的 workspace 目录里。
	b, err := os.ReadFile(filepath.Join(ws, "f.txt"))
	if err != nil {
		t.Fatalf("宿主 workspace 里读不到文件: %v", err)
	}
	if strings.TrimSpace(string(b)) != "persisted" {
		t.Fatalf("内容 = %q, want %q", b, "persisted")
	}
}

func TestMemoryLimitKillsHog(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	ws := filepath.Join(root, "ws", "chat-3")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	inst, err := p.Create(ctx, provider.CreateSpec{
		Name:      "sb:chat-3",
		Template:  "/var/lib/agentbox/templates/default",
		Workspace: ws,
		Limits:    provider.ResourceLimits{MemoryMax: 32 << 20, PidsMax: 64},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer p.Close(ctx, inst.ID)

	// 往 tmpfs 里灌远超限额的数据，应当被 OOM killer 干掉。
	var out, errBuf bytes.Buffer
	res, err := p.Exec(ctx, inst.ID, provider.ExecRequest{
		Cmd:    []string{"/bin/sh", "-c", "dd if=/dev/zero of=/dev/shm/hog bs=1M count=256 2>/dev/null"},
		Stdout: &out,
		Stderr: &errBuf,
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.ExitCode == 0 {
		t.Fatal("超出内存限额的命令不应成功退出（memory.max 未生效）")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/provider/... -v
```

Expected: 编译失败，`undefined: New`、`no required module provides package .../internal/provider`

- [ ] **Step 3: 实现 Provider 接口类型**

`internal/provider/provider.go`:

```go
// Package provider 定义沙箱后端的统一接口。
//
// 接口按云沙箱服务的能力面定义，本地实现在能力不足处诚实降级，
// 差异通过 Capabilities 显式暴露给上层策略。
package provider

import (
	"context"
	"io"
	"time"
)

// 实例状态。跨 provider 归一，上层只认这几个值。
const (
	StateCreating   = "creating"
	StateRunning    = "running"
	StateHibernated = "hibernated"
	StateClosed     = "closed"
	StateError      = "error"
)

// ResourceLimits 是沙箱的资源限额。零值字段表示不限制。
type ResourceLimits struct {
	CPUMax    string // 形如 "50000 100000"
	MemoryMax int64  // 字节
	PidsMax   int
}

// CreateSpec 是建箱请求。
type CreateSpec struct {
	Name      string // 全局复用标识，见 spec §3.1
	Template  string // 模板 rootfs 路径
	Workspace string // 宿主上的持久化工作区，将挂到沙箱的 /workspace
	Limits    ResourceLimits
}

// Instance 是一个沙箱实例。
type Instance struct {
	ID    string
	Name  string
	State string
}

// ExecRequest 是一次沙箱内执行请求。
type ExecRequest struct {
	Cmd     []string
	Env     []string
	WorkDir string
	Stdout  io.Writer // 流式直出，不在结果里返回
	Stderr  io.Writer
	Timeout time.Duration
}

// ExecResult 是一次执行的结果。
//
// OOM 单独成字段：被内存限额杀掉与命令自身失败对上层是不同信号，
// 前者应当降低并发或提高限额，后者应当把错误交给模型判断。
type ExecResult struct {
	ExitCode int
	Duration time.Duration
	Killed   bool // 因超时被杀
	OOM      bool // 被 cgroup OOM killer 杀
}

// Capabilities 描述一个 provider 的能力边界。
type Capabilities struct {
	HibernateFreesMemory   bool
	SnapshotIncludesMemory bool
	MaxConcurrent          int
}

// Provider 是沙箱后端的统一接口。
type Provider interface {
	Name() string
	Capabilities() Capabilities
	Create(ctx context.Context, spec CreateSpec) (*Instance, error)
	Get(ctx context.Context, id string) (*Instance, error)
	List(ctx context.Context, prefix string) ([]*Instance, error)
	Close(ctx context.Context, id string) error
	Exec(ctx context.Context, id string, req ExecRequest) (*ExecResult, error)
}
```

- [ ] **Step 4: 实现 LocalProvider 的 Create 与 Exec**

`internal/provider/local/local.go`:

```go
//go:build linux

// Package local 用 Linux namespace、cgroup v2 与 overlayfs 实现本地沙箱。
package local

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runtime"
)

// cgroupRoot 是所有沙箱 cgroup 的父目录。
const cgroupRoot = "/sys/fs/cgroup/agentbox"

// box 是一个运行中的本地沙箱。
type box struct {
	inst    provider.Instance
	dir     string // instances/{id}
	overlay rootfs.Overlay
	cg      *cgroup.Group
	proc    *os.Process
	client  *runtime.Client
}

// Provider 是本地沙箱 provider。
type Provider struct {
	root string // 数据根，形如 /var/lib/agentbox

	mu    sync.RWMutex
	boxes map[string]*box
}

// New 创建一个本地 provider，数据存放在 root 下。
func New(root string) (*Provider, error) {
	if root == "" {
		return nil, fmt.Errorf("root 不能为空")
	}
	for _, d := range []string{"instances", "ws"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, fmt.Errorf("创建 %s: %w", d, err)
		}
	}
	return &Provider{root: root, boxes: make(map[string]*box)}, nil
}

// Name 返回 provider 名。
func (p *Provider) Name() string { return "local" }

// Capabilities 声明本地实现的能力边界。
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		// 本地 hibernate 用 cgroup freezer 实现，只停调度不释放内存。
		HibernateFreesMemory: false,
		// 本地快照只打包 overlayfs 的 upper 层，不含进程与内存状态。
		SnapshotIncludesMemory: false,
		MaxConcurrent:          0, // 0 表示不设固定上限，由宿主资源决定
	}
}

// Create 建箱：挂 overlay、挂 workspace、建 cgroup、起 init 进程。
func (p *Provider) Create(ctx context.Context, spec provider.CreateSpec) (*provider.Instance, error) {
	if err := rootfs.EnsureTemplate(spec.Template); err != nil {
		return nil, err
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}

	dir := filepath.Join(p.root, "instances", id)
	o := rootfs.Overlay{
		Lower:  spec.Template,
		Upper:  filepath.Join(dir, "upper"),
		Work:   filepath.Join(dir, "work"),
		Merged: filepath.Join(dir, "merged"),
	}
	for _, d := range []string{o.Upper, o.Work, o.Merged} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("创建 %s: %w", d, err)
		}
	}

	// 失败时按相反顺序回滚已完成的步骤，不留半截实例。
	var rollback []func()
	defer func() {
		for i := len(rollback) - 1; i >= 0; i-- {
			rollback[i]()
		}
	}()

	if err := rootfs.Mount(o); err != nil {
		return nil, err
	}
	rollback = append(rollback, func() { _ = rootfs.Unmount(o.Merged) })

	if spec.Workspace != "" {
		wsInBox := filepath.Join(o.Merged, "workspace")
		if err := os.MkdirAll(wsInBox, 0o755); err != nil {
			return nil, fmt.Errorf("创建沙箱内 /workspace: %w", err)
		}
		if err := rootfs.BindMount(spec.Workspace, wsInBox); err != nil {
			return nil, err
		}
		rollback = append(rollback, func() { _ = rootfs.Unmount(wsInBox) })
	}

	cg, err := cgroup.New(cgroupRoot, id)
	if err != nil {
		return nil, err
	}
	rollback = append(rollback, func() { _ = cg.Destroy() })
	if err := cg.Apply(cgroup.Limits{
		CPUMax:    spec.Limits.CPUMax,
		MemoryMax: spec.Limits.MemoryMax,
		PidsMax:   spec.Limits.PidsMax,
	}); err != nil {
		return nil, err
	}

	host, sandboxFD, err := runtime.NewControlPair()
	if err != nil {
		return nil, err
	}
	rollback = append(rollback, func() { _ = host.Close() })

	proc, err := runtime.Spawn(runtime.SpawnConfig{
		Root:      o.Merged,
		Hostname:  "agentbox",
		ControlFD: sandboxFD,
	})
	sandboxFD.Close() // 宿主侧不再需要沙箱端点
	if err != nil {
		return nil, err
	}
	rollback = append(rollback, func() { _ = proc.Kill() })

	// 把 init 移入 cgroup。它的所有子进程自动继承，限额从此覆盖整箱。
	if err := cg.AddProc(proc.Pid); err != nil {
		return nil, err
	}

	client, err := runtime.NewClient(host)
	if err != nil {
		return nil, err
	}

	b := &box{
		inst:    provider.Instance{ID: id, Name: spec.Name, State: provider.StateRunning},
		dir:     dir,
		overlay: o,
		cg:      cg,
		proc:    proc,
		client:  client,
	}
	p.mu.Lock()
	p.boxes[id] = b
	p.mu.Unlock()

	rollback = nil // 全部成功，取消回滚
	inst := b.inst
	return &inst, nil
}

// Exec 在指定沙箱内执行命令。
func (p *Provider) Exec(ctx context.Context, id string, req provider.ExecRequest) (*provider.ExecResult, error) {
	p.mu.RLock()
	b, ok := p.boxes[id]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("沙箱 %s 不存在", id)
	}

	start := time.Now()
	code, err := b.client.Exec(runtime.ExecRequest{
		Cmd:       req.Cmd,
		Env:       req.Env,
		WorkDir:   req.WorkDir,
		TimeoutMS: int(req.Timeout / time.Millisecond),
	}, req.Stdout, req.Stderr)
	if err != nil {
		return nil, err
	}

	res := &provider.ExecResult{ExitCode: code, Duration: time.Since(start)}
	res.Killed = req.Timeout > 0 && res.Duration >= req.Timeout
	res.OOM = b.oomOccurred()
	return res, nil
}

// oomOccurred 读取 cgroup 的 memory.events，判断是否发生过 OOM kill。
func (b *box) oomOccurred() bool {
	data, err := os.ReadFile(filepath.Join(b.cg.Path(), "memory.events"))
	if err != nil {
		return false
	}
	for _, line := range splitLines(string(data)) {
		var key string
		var n int
		if _, err := fmt.Sscanf(line, "%s %d", &key, &n); err == nil && key == "oom_kill" && n > 0 {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func newID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("生成实例 ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
```

> 本任务只实现 `Create` / `Exec`；`Close` / `Get` / `List` 在 Task 10、11 补齐。测试里的 `defer p.Close(...)` 届时才会真正生效，此前编译不过——因此 Task 10 必须紧接着做。为让本任务可独立验证，先加一个临时的 `Close` 占位：

```go
// Close 在 Task 10 实现完整的八步清理，此处先保证编译与基本回收。
func (p *Provider) Close(ctx context.Context, id string) error {
	p.mu.Lock()
	b, ok := p.boxes[id]
	delete(p.boxes, id)
	p.mu.Unlock()
	if !ok {
		return nil // 幂等
	}
	_ = b.client.Close()
	_ = b.proc.Kill()
	_, _ = b.proc.Wait()
	_ = rootfs.Unmount(filepath.Join(b.overlay.Merged, "workspace"))
	_ = rootfs.Unmount(b.overlay.Merged)
	_ = b.cg.Destroy()
	return os.RemoveAll(b.dir)
}
```

- [ ] **Step 5: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/provider/... -v
```

Expected: 三个用例全部 PASS

- [ ] **Step 6: 提交**

```bash
git add internal/provider
git commit -m "feat(provider): Provider 接口与 LocalProvider 建箱执行

Create 用 rollback 栈按相反顺序回滚已完成的步骤，
任一步失败都不留半截实例（挂了一半的 overlay、空 cgroup）。

init 进程移入 cgroup 后其所有子进程自动继承，
限额因此覆盖整箱而不只是 init 本身。

ExecResult.OOM 从 memory.events 的 oom_kill 计数读取——
被限额杀掉与命令自身失败对上层是完全不同的信号。"
```

---

## Task 10: Close 的八步清理与 fork 炸弹回归

**Files:**
- Modify: `internal/provider/local/local.go`
- Test: `internal/provider/local/close_test.go`

**Interfaces:**
- Produces: 完整的 `func (p *Provider) Close(ctx context.Context, id string) error`

- [ ] **Step 1: 写失败测试**

`internal/provider/local/close_test.go`:

```go
//go:build linux

package local

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// TestCloseLeavesNoResidue 在沙箱里放一个 fork 炸弹，
// 然后销毁，验证进程、cgroup、挂载点、实例目录全部清干净。
func TestCloseLeavesNoResidue(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	ws := filepath.Join(root, "ws", "chat-bomb")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	inst, err := p.Create(ctx, provider.CreateSpec{
		Name:      "sb:chat-bomb",
		Template:  "/var/lib/agentbox/templates/default",
		Workspace: ws,
		Limits:    provider.ResourceLimits{MemoryMax: 128 << 20, PidsMax: 64},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	p.mu.RLock()
	b := p.boxes[inst.ID]
	p.mu.RUnlock()
	cgPath := b.cg.Path()
	instDir := b.dir
	merged := b.overlay.Merged

	// pids.max=64 会让 fork 炸弹撞墙而不是打垮宿主。
	// 这条命令预期失败，我们要的是它留下一堆进程。
	var out, errBuf bytes.Buffer
	_, _ = p.Exec(ctx, inst.ID, provider.ExecRequest{
		Cmd:    []string{"/bin/sh", "-c", "for i in $(seq 1 40); do sleep 60 & done; echo spawned"},
		Stdout: &out,
		Stderr: &errBuf,
	})
	if !strings.Contains(out.String(), "spawned") {
		t.Fatalf("未能在沙箱内拉起后台进程，stdout=%q stderr=%q", out.String(), errBuf.String())
	}

	if err := p.Close(ctx, inst.ID); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(cgPath); !os.IsNotExist(err) {
		t.Fatalf("cgroup 目录 %s 未被删除: %v", cgPath, err)
	}
	if _, err := os.Stat(instDir); !os.IsNotExist(err) {
		t.Fatalf("实例目录 %s 未被删除: %v", instDir, err)
	}
	assertNotMounted(t, merged)
	assertNotMounted(t, filepath.Join(merged, "workspace"))

	// workspace 是持久化的，必须留下。
	if _, err := os.Stat(ws); err != nil {
		t.Fatalf("workspace 目录不应被删除: %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	ws := filepath.Join(root, "ws", "chat-idem")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	inst, err := p.Create(ctx, provider.CreateSpec{
		Name:      "sb:chat-idem",
		Template:  "/var/lib/agentbox/templates/default",
		Workspace: ws,
		Limits:    provider.ResourceLimits{PidsMax: 64},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := p.Close(ctx, inst.ID); err != nil {
		t.Fatalf("第一次 Close: %v", err)
	}
	// Reaper 可能重复投递回收任务，Close 必须幂等。
	if err := p.Close(ctx, inst.ID); err != nil {
		t.Fatalf("第二次 Close 应当成功返回，实际: %v", err)
	}
	if err := p.Close(ctx, "never-existed"); err != nil {
		t.Fatalf("Close 不存在的实例应当成功返回，实际: %v", err)
	}
}

func assertNotMounted(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatalf("读取 mountinfo: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		// mountinfo 第 5 个字段是挂载点。
		if len(fields) >= 5 && fields[4] == path {
			t.Fatalf("%s 仍处于挂载状态: %s", path, line)
		}
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/provider/local/ -run TestClose -v
```

Expected: `TestCloseLeavesNoResidue` FAIL —— 临时版 Close 没有等待进程清零就删 cgroup，`rmdir` 会因组内仍有进程而失败，cgroup 目录残留

- [ ] **Step 3: 实现完整的八步 Close**

把 `internal/provider/local/local.go` 里的临时 `Close` 完整替换为：

```go
// Close 销毁沙箱。它是幂等的：重复调用或对不存在的实例调用都返回 nil。
//
// 八个步骤的顺序不可调换，每一步都在解决一个具体的泄漏：
//
//  1. 冻结 cgroup —— 不冻结的话，杀进程的同时它还在 fork，永远杀不干净
//  2. SIGTERM 给 1 号进程，留出优雅退出的窗口
//  3. 超时则 SIGKILL —— 杀 PID namespace 的 1 号进程，内核连带清理整个 namespace
//  4. 轮询确认进程清零 —— 内核回收是异步的，杀完立刻删 cgroup 会偶发失败
//  5. 卸载 workspace（bind mount）
//  6. 卸载 merged（overlayfs）
//  7. 删 cgroup 目录 —— 组内非空会失败，所以第 4 步不能跳
//  8. 删实例目录，但保留宿主上的 workspace
func (p *Provider) Close(ctx context.Context, id string) error {
	p.mu.Lock()
	b, ok := p.boxes[id]
	delete(p.boxes, id)
	p.mu.Unlock()
	if !ok {
		return nil // 幂等：已销毁或从未存在
	}

	_ = b.client.Close()

	// ① 冻结，止住 fork
	_ = b.cg.Freeze()

	// ② 优雅退出窗口
	_ = b.proc.Signal(syscall.SIGTERM)
	if !waitProcExit(b.proc, gracePeriod) {
		// ③ 强杀 1 号进程，内核连带清理整个 PID namespace
		_ = b.proc.Kill()
		_, _ = b.proc.Wait()
	}

	// 冻结状态下进程收不到 KILL 之外的信号，且已冻结的组无法回收，
	// 解冻让内核把剩余进程清理掉。
	_ = b.cg.Thaw()

	// ④ 轮询确认组内进程清零
	if err := waitProcsGone(b.cg, reapTimeout); err != nil {
		return fmt.Errorf("沙箱 %s 进程未能清零: %w", id, err)
	}

	// ⑤⑥ 卸载，顺序与挂载相反
	if err := rootfs.Unmount(filepath.Join(b.overlay.Merged, "workspace")); err != nil {
		return fmt.Errorf("卸载 workspace: %w", err)
	}
	if err := rootfs.Unmount(b.overlay.Merged); err != nil {
		return fmt.Errorf("卸载 merged: %w", err)
	}

	// ⑦ 删 cgroup
	if err := b.cg.Destroy(); err != nil {
		return fmt.Errorf("删除 cgroup: %w", err)
	}

	// ⑧ 删实例目录。workspace 在 p.root/ws 下，不在 b.dir 里，故不受影响。
	if err := os.RemoveAll(b.dir); err != nil {
		return fmt.Errorf("删除实例目录: %w", err)
	}
	return nil
}

const (
	// gracePeriod 是 SIGTERM 之后等待优雅退出的时间。
	gracePeriod = 2 * time.Second
	// reapTimeout 是等待内核回收完组内进程的上限。
	reapTimeout = 5 * time.Second
)

// waitProcExit 等待进程退出，返回是否在超时前退出。
func waitProcExit(proc *os.Process, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		_, _ = proc.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// waitProcsGone 轮询直到 cgroup 内进程清零。
//
// 必须轮询而非杀完就删：SIGKILL 返回不代表进程已消失，
// 内核回收是异步的，立刻 rmdir 会偶发 EBUSY——这种偶发失败最难查。
func waitProcsGone(cg *cgroup.Group, d time.Duration) error {
	deadline := time.Now().Add(d)
	for {
		pids, err := cg.Procs()
		if err != nil {
			// cgroup 目录已经不在了，视作已清零。
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if len(pids) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("超时，仍有 %d 个进程: %v", len(pids), pids)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
```

import 块补上 `syscall`：

```go
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runtime"
)
```

- [ ] **Step 4: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./internal/provider/local/ -v
```

Expected: 五个用例全部 PASS

验证宿主上确实没有残留：

```bash
ls /sys/fs/cgroup/agentbox/ 2>/dev/null
cat /proc/self/mountinfo | grep agentbox
```

Expected: 两条命令都无输出

- [ ] **Step 5: 提交**

```bash
git add internal/provider/local
git commit -m "feat(provider): Close 的八步有序清理

顺序不可调换，每一步解决一个具体泄漏：
先冻结止住 fork，再杀 PID namespace 的 1 号进程由内核连带清理，
然后必须轮询确认进程清零才能删 cgroup——SIGKILL 返回不代表
进程已消失，内核回收是异步的，立刻 rmdir 会偶发 EBUSY。

Close 幂等：Reaper 可能重复投递回收任务。
workspace 存放在 root/ws 下而非实例目录内，因此销毁沙箱
不会动到持久化产物。"
```

---

## Task 11: state.json 与 Get / List

**Files:**
- Create: `internal/provider/local/state.go`
- Modify: `internal/provider/local/local.go`
- Test: `internal/provider/local/state_test.go`

**Interfaces:**
- Produces:
  - `func (p *Provider) Get(ctx context.Context, id string) (*provider.Instance, error)`
  - `func (p *Provider) List(ctx context.Context, prefix string) ([]*provider.Instance, error)`

- [ ] **Step 1: 写失败测试**

`internal/provider/local/state_test.go`:

```go
//go:build linux

package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

func TestGetAndList(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	mk := func(chat string) *provider.Instance {
		t.Helper()
		ws := filepath.Join(root, "ws", chat)
		if err := os.MkdirAll(ws, 0o755); err != nil {
			t.Fatalf("mkdir workspace: %v", err)
		}
		inst, err := p.Create(ctx, provider.CreateSpec{
			Name:      "sb:" + chat,
			Template:  "/var/lib/agentbox/templates/default",
			Workspace: ws,
			Limits:    provider.ResourceLimits{PidsMax: 64},
		})
		if err != nil {
			t.Fatalf("Create %s: %v", chat, err)
		}
		return inst
	}

	a := mk("alpha")
	defer p.Close(ctx, a.ID)
	b := mk("beta")
	defer p.Close(ctx, b.ID)

	got, err := p.Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "sb:alpha" || got.State != provider.StateRunning {
		t.Fatalf("Get = %+v, want Name=sb:alpha State=running", got)
	}

	if _, err := p.Get(ctx, "no-such-id"); err == nil {
		t.Fatal("Get 不存在的实例应当报错")
	}

	all, err := p.List(ctx, "sb:")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("List(\"sb:\") 返回 %d 个，want 2", len(all))
	}

	only, err := p.List(ctx, "sb:alpha")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(only) != 1 || only[0].Name != "sb:alpha" {
		t.Fatalf("List(\"sb:alpha\") = %+v, want 只含 sb:alpha", only)
	}
}

// TestListFindsOrphansFromDisk 模拟进程重启：内存里的 boxes 丢了，
// List 仍须能从磁盘上的 state.json 扫出实例，否则孤儿箱无法对账。
func TestListFindsOrphansFromDisk(t *testing.T) {
	p, root := newTestProvider(t)
	ctx := context.Background()

	ws := filepath.Join(root, "ws", "orphan")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	inst, err := p.Create(ctx, provider.CreateSpec{
		Name:      "sb:orphan",
		Template:  "/var/lib/agentbox/templates/default",
		Workspace: ws,
		Limits:    provider.ResourceLimits{PidsMax: 64},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer p.Close(ctx, inst.ID)

	// 模拟进程重启：清空内存态，磁盘上的 state.json 应当还在。
	p.mu.Lock()
	saved := p.boxes
	p.boxes = make(map[string]*box)
	p.mu.Unlock()

	found, err := p.List(ctx, "sb:")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(found) != 1 || found[0].Name != "sb:orphan" {
		t.Fatalf("重启后 List = %+v, want 从磁盘扫出 sb:orphan", found)
	}

	p.mu.Lock()
	p.boxes = saved
	p.mu.Unlock()
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
sudo -E env "PATH=$PATH" go test ./internal/provider/local/ -run "TestGetAndList|TestListFindsOrphans" -v
```

Expected: 编译失败，`p.Get undefined`、`p.List undefined`

- [ ] **Step 3: 实现 state**

`internal/provider/local/state.go`:

```go
//go:build linux

package local

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// stateFile 是实例元数据在实例目录下的文件名。
const stateFile = "state.json"

// state 是落盘的实例元数据。
//
// 它存在的意义是让 List 在进程重启后仍能扫出实例。没有它，
// 一次崩溃重启就会让所有在跑的沙箱变成无人认领的孤儿——
// 管控面对账（spec §3.4）正是靠 List 发现这类残留。
type state struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	State     string    `json:"state"`
	Workspace string    `json:"workspace"`
	CreatedAt time.Time `json:"created_at"`
}

func writeState(dir string, s state) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化 state: %w", err)
	}
	// 先写临时文件再原子改名，避免崩在写一半时留下损坏的 state.json。
	tmp := filepath.Join(dir, stateFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("写 %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, stateFile)); err != nil {
		return fmt.Errorf("改名 state 文件: %w", err)
	}
	return nil
}

func readState(dir string) (state, error) {
	var s state
	b, err := os.ReadFile(filepath.Join(dir, stateFile))
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("解析 %s/%s: %w", dir, stateFile, err)
	}
	return s, nil
}
```

- [ ] **Step 4: 接进 LocalProvider**

在 `local.go` 的 `Create` 中，`rollback = nil` 之前插入落盘：

```go
	if err := writeState(dir, state{
		ID:        id,
		Name:      spec.Name,
		State:     provider.StateRunning,
		Workspace: spec.Workspace,
		CreatedAt: time.Now(),
	}); err != nil {
		return nil, err
	}
```

新增 `Get` 与 `List`：

```go
// Get 返回指定实例。优先读内存态，未命中则回落到磁盘上的 state.json。
func (p *Provider) Get(ctx context.Context, id string) (*provider.Instance, error) {
	p.mu.RLock()
	b, ok := p.boxes[id]
	p.mu.RUnlock()
	if ok {
		inst := b.inst
		return &inst, nil
	}

	s, err := readState(filepath.Join(p.root, "instances", id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("沙箱 %s 不存在", id)
		}
		return nil, err
	}
	return &provider.Instance{ID: s.ID, Name: s.Name, State: s.State}, nil
}

// List 扫描磁盘，返回名字以 prefix 开头的全部实例。
//
// 必须扫磁盘而不是只读内存：进程重启后内存态为空，但沙箱进程和
// cgroup 还在。管控面正是靠 List 发现这类孤儿并回收（spec §3.4）。
func (p *Provider) List(ctx context.Context, prefix string) ([]*provider.Instance, error) {
	dir := filepath.Join(p.root, "instances")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取 %s: %w", dir, err)
	}

	var out []*provider.Instance
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		s, err := readState(filepath.Join(dir, e.Name()))
		if err != nil {
			// 建箱建到一半崩了会留下没有 state.json 的目录，跳过即可，
			// 不该让一个坏目录导致整次 List 失败。
			continue
		}
		if prefix != "" && !strings.HasPrefix(s.Name, prefix) {
			continue
		}
		out = append(out, &provider.Instance{ID: s.ID, Name: s.Name, State: s.State})
	}
	return out, nil
}
```

import 块补上 `strings`。

- [ ] **Step 5: 跑测试确认通过**

```bash
sudo -E env "PATH=$PATH" go test ./... -v
```

Expected: 全部包 PASS

- [ ] **Step 6: M1 整体验收**

```bash
go vet ./...
go build ./...
sudo -E env "PATH=$PATH" go test ./... -count=1
```

Expected: `go vet` 无输出；`go build` 无输出；全部测试 PASS

残留检查：

```bash
ls /sys/fs/cgroup/agentbox/ 2>/dev/null
grep agentbox /proc/self/mountinfo
ls /var/lib/agentbox/instances/
```

Expected: 三条命令均无输出（或 instances 为空目录）

- [ ] **Step 7: 提交**

```bash
git add internal/provider/local
git commit -m "feat(provider): state.json 与 Get/List

List 扫磁盘而非只读内存：进程重启后内存态为空但沙箱进程和
cgroup 还在，管控面正是靠 List 发现这类孤儿并回收（spec §3.4）。

state.json 先写临时文件再原子改名，避免崩在写一半时留下
损坏的元数据。List 跳过没有 state.json 的目录——建箱建到
一半崩溃会留下这种残迹，不该让它导致整次 List 失败。

M1 完成：建箱、执行、销毁全链路通过，无残留。"
```

---

## 自查记录

**Spec 覆盖检查**（对照 spec §4.4 LocalProvider）

| Spec 要求 | 对应任务 |
|---|---|
| 目录布局 templates / instances / ws / snapshots | Task 4、Task 9（snapshots 属 M2 范围，本计划不实现） |
| overlayfs 分层，建箱不拷贝 rootfs | Task 3、Task 9 |
| 创建流程七步 | Task 9 |
| re-exec `/proc/self/exe` 处理线程敏感 syscall | Task 5 |
| 沙箱内 mini-envd（收割 + 接收 exec + 转发信号） | Task 7、Task 8（信号转发属 M2，本计划不实现） |
| cgroup v2 freezer 实现 hibernate | Task 2 提供 Freeze/Thaw，Task 10 在 Close 中使用；`Hibernate`/`Resume` 接口方法属 M2 |
| Snapshot 打包 upper 层 | 不在 M1 范围，属 M2 |
| Close 八步顺序 | Task 10 |
| `Capabilities()` 显式暴露能力差异 | Task 9 |

**有意留到 M2 的**：`Hibernate` / `Resume` / `Snapshot` 三个接口方法、信号转发、`snapshots/` 目录。M1 的出口条件是"建箱、执行命令、正确销毁、无残留"，上述三项不属于该条件。

**类型一致性检查**：`provider.ResourceLimits` 与 `cgroup.Limits` 字段名刻意保持一致（`CPUMax` / `MemoryMax` / `PidsMax`），Task 9 中逐字段转换；`runtime.ExecRequest` 用 `TimeoutMS int`，`provider.ExecRequest` 用 `Timeout time.Duration`，转换点在 `Provider.Exec` 内，已在 Task 9 代码中体现。
