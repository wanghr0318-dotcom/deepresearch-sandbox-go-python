//go:build linux

// Package cgroup 提供 cgroup v2 组的最小管理能力。
//
// cgroup v2 的接口全部是文件读写：建组是 mkdir，限额是往
// cpu.max/memory.max/pids.max 写值，加进程是把 pid 写进 cgroup.procs，
// 销毁是 rmdir——但 rmdir 要求组内已无进程。
package cgroup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
//
// name 必须是单层名字。调用方会把沙箱 ID 传进来，一旦其中混入 "/" 或 ".."，
// 拼出的路径就能逃出 cgroup 根——限额会落到别的组上，而且不会有任何报错。
// 安全边界不能依赖调用方自律，所以在这里挡住。
func New(root, name string) (*Group, error) {
	if name == "" || name == "." || name == ".." || name != filepath.Base(name) {
		return nil, fmt.Errorf("非法 cgroup 名 %q：必须是单层名字，不得含路径分隔符或 . 与 ..", name)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("创建 cgroup 根 %s: %w", root, err)
	}
	delegateErrs := ensureControllers(root)

	path := filepath.Join(root, name)
	if err := os.Mkdir(path, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("创建 cgroup %s: %w", path, err)
	}
	// 控制器未委派时，子组里根本不会出现这几个限额文件（实测：WSL2 的
	// /sys/fs/cgroup/cgroup.subtree_control 默认为空）。此时若不早报，
	// 失败会推迟到 Apply 写文件，错误只剩一句 no such file，
	// 完全看不出病根是控制器没启用。
	for _, f := range []string{"cpu.max", "memory.max", "pids.max"} {
		if _, err := os.Stat(filepath.Join(path, f)); err != nil {
			msg := fmt.Sprintf(
				"cgroup %s 缺少 %s：控制器未委派，请确认 %s/cgroup.subtree_control 已启用 cpu/memory/pids",
				path, f, root)
			if len(delegateErrs) > 0 {
				msg += "；委派写入报错：" + strings.Join(delegateErrs, "、")
			}
			return nil, errors.New(msg)
		}
	}
	return &Group{path: path}, nil
}

// ensureControllers 在 root 及其父级的 subtree_control 里启用 cpu/memory/pids，
// 返回写入失败的线索（可能为空）。
//
// 子组要能设 cpu/memory/pids，祖先组必须逐级在 subtree_control 里启用它们：
// /sys/fs/cgroup 启用后 root 才有这些文件，root 启用后子组才有。
// 已启用时重复写不报错，故这里容忍写入失败——真正的判据是子组里限额文件是否出现。
// 写入失败原因可能是权限不足而非未委派。完全吞掉的话，诊断
// 会一律说"控制器未委派"，把排查者引向错误方向，所以留作线索。
func ensureControllers(root string) []string {
	var errs []string
	for _, dir := range []string{filepath.Dir(root), root} {
		f := filepath.Join(dir, "cgroup.subtree_control")
		if err := os.WriteFile(f, []byte("+cpu +memory +pids"), 0o644); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", f, err))
		}
	}
	return errs
}

// Exists 报告 root/name 是否存在且是一个 cgroup 目录。
// 判据是目录里有 cgroup.procs——普通目录没有这个内核生成的文件。
func Exists(root, name string) (bool, error) {
	dir := filepath.Join(root, name)
	fi, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("检查 cgroup %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return false, nil
	}
	if _, err := os.Stat(filepath.Join(dir, "cgroup.procs")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("检查 cgroup %s: %w", dir, err)
	}
	return true, nil
}

// Path 返回该 cgroup 在 sysfs 中的绝对路径。
func (g *Group) Path() string { return g.path }

// Apply 写入资源限额。零值字段被跳过。
//
// 非原子：三项限额依次写入，任一步失败即返回，已写入的不回滚。
// 建组时调用（本项目的唯一用法）是安全的——部分限额总比没有限额安全。
// 但若将来用它去【降低】运行中沙箱的限额，半途失败会留下状态不一致的组，
// 调用方需自行处理。
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
	if l.MemoryMax > 0 {
		// 规格 §4.5：禁止换出，否则 swap 会悄悄放宽 memory.max。
		if err := g.write("memory.swap.max", "0"); err != nil {
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

// Kill 写 cgroup.kill，向组内全部进程（含已 setsid、双重 fork 脱离的后代）
// 发 SIGKILL，并阻止其在被杀期间继续 fork。需要内核 ≥ 5.14。
// 它只发信号；进程是否已清零用 WaitEmpty / Populated 判断。
func (g *Group) Kill() error { return g.write("cgroup.kill", "1") }

// Populated 读 cgroup.events 的 populated：组内（含子孙组）是否还有进程。
// 未收割的僵尸仍计入。
func (g *Group) Populated() (bool, error) {
	v, err := g.readKey("cgroup.events", "populated")
	if err != nil {
		return false, err
	}
	return v != 0, nil
}

// WaitEmpty 轮询等待组内进程清零；ctx 结束时返回 ctx 的错误。
func (g *Group) WaitEmpty(ctx context.Context) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, err := g.Populated()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// OOMKills 返回 memory.events 的 oom_kill：组内因超出 memory.max 被杀的进程累计数。
func (g *Group) OOMKills() (uint64, error) { return g.readKey("memory.events", "oom_kill") }

// CPUUsageUsec 返回 cpu.stat 的 usage_usec：组累计消耗的 CPU 微秒数。
func (g *Group) CPUUsageUsec() (uint64, error) { return g.readKey("cpu.stat", "usage_usec") }

// readKey 读取 "key value" 行格式的 cgroup 文件中指定键的数值。
func (g *Group) readKey(file, key string) (uint64, error) {
	p := filepath.Join(g.path, file)
	b, err := os.ReadFile(p)
	if err != nil {
		return 0, fmt.Errorf("读取 %s: %w", p, err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == key {
			v, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("解析 %s 的 %s=%q: %w", p, key, f[1], err)
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("%s 中没有 %s", p, key)
}

// Destroy 删除该 cgroup。只删除空组：组内仍有进程时内核返回 EBUSY，
// 原样（可用 errors.Is 判断）返回给调用方，由其先 Kill 并 WaitEmpty。
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
