//go:build linux

package local

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// 本文件实现独立原始扫描（契约第 3 节 Scan 行、规格 §14.1 第 5 步）与挂载表解析。
// Scan 不依赖本进程的内存状态来发现资源：逐层读取磁盘、挂载表与 cgroup 文件系统，
// 只在分类"完整"时参考 init 是否就绪。UID 范围文件属主一层在 Plan 1B 引入 UID 映射后补充。

// Scan 逐层报告：
//   - env_dir：<data>/envs/* 的每一项（含无或损坏 owner.json 的），EnvID 一律取目录名；
//   - mount：挂载表中位于数据目录下的挂载点；
//   - cgroup：<cgroup 根>/agentbox-*（含其他 install_id）下的环境 cgroup；
//   - listener：<data>/envs/*/gw.sock。
//
// 分类：owner.json 属于本安装且 init 就绪 → OwnedComplete，属于本安装但不完整 → OwnedPartial；
// 属于其他安装 → Foreign；owner.json 缺失或损坏 → Unknown。挂载与 listener 随所在环境目录分类；
// 本安装 cgroup 下的环境 cgroup 按命名属于本安装（完整 → OwnedComplete，否则 OwnedPartial），
// 其他安装的为 Foreign。
func (p *Provider) Scan(ctx context.Context) (provider.ScanReport, error) {
	var r provider.ScanReport
	add := func(layer, path, envID string, o provider.Owner) {
		r.Items = append(r.Items, provider.ScanItem{Layer: layer, Path: path, EnvID: envID, Owner: o})
	}

	// 环境目录与 listener。
	ents, err := os.ReadDir(p.envsDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return r, fmt.Errorf("local: 扫描环境目录: %w", err)
	}
	dirOwner := make(map[string]provider.Owner, len(ents))
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		name := e.Name()
		o := provider.Unknown
		if e.IsDir() {
			o, err = p.classifyDir(name)
			if err != nil {
				return r, err
			}
		}
		dirOwner[name] = o
		add("env_dir", p.envDir(name), name, o)
		sock := filepath.Join(p.envDir(name), "gw.sock")
		if _, err := os.Lstat(sock); err == nil {
			add("listener", sock, name, o)
		}
	}

	// 数据目录下的挂载。
	ms, err := mountsUnder(p.dataDir)
	if err != nil {
		return r, err
	}
	for _, m := range ms {
		envID, o := "", provider.Unknown
		if rel, err := filepath.Rel(p.envsDir, m); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
			envID = strings.SplitN(rel, string(filepath.Separator), 2)[0]
			if do, ok := dirOwner[envID]; ok {
				o = do
			}
		}
		add("mount", m, envID, o)
	}

	// agentbox-* cgroup。
	if err := ctx.Err(); err != nil {
		return r, err
	}
	roots, err := os.ReadDir(p.cgroupRoot)
	if err != nil {
		return r, fmt.Errorf("local: 扫描 cgroup 根: %w", err)
	}
	ours := filepath.Base(p.installCgroup())
	for _, e := range roots {
		name := e.Name()
		if !e.IsDir() || !strings.HasPrefix(name, "agentbox-") {
			continue
		}
		if ok, err := cgroup.Exists(p.cgroupRoot, name); err != nil {
			return r, err
		} else if !ok {
			continue
		}
		dir := filepath.Join(p.cgroupRoot, name)
		subs, err := os.ReadDir(dir)
		if err != nil {
			return r, fmt.Errorf("local: 扫描 cgroup %s: %w", dir, err)
		}
		found := false
		for _, s := range subs {
			if !s.IsDir() || !strings.HasPrefix(s.Name(), "env-") {
				continue
			}
			found = true
			envID := strings.TrimPrefix(s.Name(), "env-")
			o := provider.Foreign
			if name == ours {
				o = provider.OwnedPartial
				if dirOwner[envID] == provider.OwnedComplete {
					o = provider.OwnedComplete
				}
			}
			add("cgroup", filepath.Join(dir, s.Name()), envID, o)
		}
		if !found && name != ours {
			add("cgroup", dir, "", provider.Foreign)
		}
	}
	return r, nil
}

// classifyDir 把 <envs>/<name> 的归属映射为扫描分类。
func (p *Provider) classifyDir(name string) (provider.Owner, error) {
	own, _, err := p.readOwner(name)
	if err != nil {
		return 0, err
	}
	switch own {
	case dirOwned:
		if p.state(name).ready() {
			return provider.OwnedComplete, nil
		}
		return provider.OwnedPartial, nil
	case dirForeign:
		return provider.Foreign, nil
	default:
		return provider.Unknown, nil
	}
}

// ---------------------------------------------------------------------------
// 挂载表

// mountsUnder 返回本进程挂载表（/proc/self/mountinfo）中位于 dir 或其下的挂载点，按出现顺序。
func mountsUnder(dir string) ([]string, error) {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("local: 读取 mountinfo: %w", err)
	}
	return parseMountsUnder(b, dir), nil
}

func parseMountsUnder(mountinfo []byte, dir string) []string {
	dir = filepath.Clean(dir)
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(mountinfo))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		// 字段：mount_id parent_id major:minor root mount_point options ... - fstype source super_options
		f := strings.Fields(sc.Text())
		if len(f) < 5 {
			continue
		}
		mp := unescapeMount(f[4])
		if mp == dir || strings.HasPrefix(mp, dir+"/") {
			out = append(out, mp)
		}
	}
	return out
}

// unescapeMount 还原 mountinfo 中以 \ooo 八进制转义的空白与反斜杠。
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// unmountUnder 卸载 dir 或其下的全部挂载，深者先卸；同一挂载点叠加的多层逐层卸载，直到挂载表中
// 不再出现。卸载失败（例如 EBUSY）作为宿主错误返回。
func unmountUnder(dir string) error {
	for round := 0; ; round++ {
		ms, err := mountsUnder(dir)
		if err != nil {
			return err
		}
		if len(ms) == 0 {
			return nil
		}
		if round >= 64 {
			return fmt.Errorf("local: %s 下的挂载无法全部卸载：%v", dir, ms)
		}
		sort.SliceStable(ms, func(i, j int) bool { return len(ms[i]) > len(ms[j]) })
		for _, m := range ms {
			if err := syscall.Unmount(m, 0); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
				return fmt.Errorf("local: 卸载 %s: %w", m, err)
			}
		}
	}
}
