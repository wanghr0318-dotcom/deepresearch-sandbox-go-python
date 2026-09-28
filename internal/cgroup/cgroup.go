//go:build linux

// Package cgroup 提供 cgroup v2 组的最小管理能力。
//
// cgroup v2 的接口全部是文件读写：建组是 mkdir，限额是往
// cpu.max/memory.max/pids.max 写值，加进程是把 pid 写进 cgroup.procs，
// 销毁是 rmdir——但 rmdir 要求组内已无进程。
package cgroup

import (
	"errors"
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
	// 子组要能设 cpu/memory/pids，祖先组必须逐级在 subtree_control 里启用它们：
	// /sys/fs/cgroup 启用后 root 才有这些文件，root 启用后子组才有。
	// 已启用时重复写不报错，故这里容忍写入失败——真正的判据是下面文件是否出现。
	// 写入失败原因可能是权限不足而非未委派。完全吞掉的话，下面的诊断
	// 会一律说"控制器未委派"，把排查者引向错误方向，所以留作线索。
	var delegateErrs []string
	for _, dir := range []string{filepath.Dir(root), root} {
		f := filepath.Join(dir, "cgroup.subtree_control")
		if err := os.WriteFile(f, []byte("+cpu +memory +pids"), 0o644); err != nil {
			delegateErrs = append(delegateErrs, fmt.Sprintf("%s: %v", f, err))
		}
	}

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
