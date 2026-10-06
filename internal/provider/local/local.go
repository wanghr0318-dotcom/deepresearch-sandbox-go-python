//go:build linux

package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"
)

// 本文件实现环境生命周期（Provider 契约第 3、4、6 节）：Create、Stop、Destroy、List、
// ResourceDiag，以及 StartExec 的环境查找。Scan 与挂载表解析见 scan.go。

// EnvStarter 在环境 cgroup 内启动 init 并返回宿主端控制连接与 init 的 pid。生产实现
// （namespace、UID 映射、挂载、pivot_root、降权）在 Plan 1B 的结论回写规格 §4.6 之后提供。
//
// 约定：init 从创建起就在 cg 内（例如 clone 时以 CLONE_INTO_CGROUP 放入），在此之前不运行任何
// workload 代码；返回成功即 init 已就绪、可经连接接收 start。init 进程的收割由实现负责。
// ctx 结束时实现停止推进并返回 ctx 的错误；已启动的部分留在 cg 内，由 Stop 回收。
type EnvStarter interface {
	StartInit(ctx context.Context, spec provider.EnvSpec, dir string, cg *cgroup.Group) (*sandbox.Conn, int, error)
}

// Options 配置 Provider。
type Options struct {
	DataDir    string // 数据目录；环境目录为 <DataDir>/envs/<env_id>
	CgroupRoot string // cgroup v2 挂载点（生产为 /sys/fs/cgroup）；环境 cgroup 为 agentbox-<install_id>/env-<env_id>
	InstallID  string // 本安装的标识，写入 owner.json 并决定 cgroup 路径
	Starter    EnvStarter
}

// defaultStopTimeout 是 ctx 没有期限时 Stop 等待执行树清空的上限（Stop 必须有界）。
const defaultStopTimeout = 30 * time.Second

// Provider 是 provider.Provider 的 Linux 实现。
type Provider struct {
	dataDir    string // 已解析为绝对、无符号链接的路径
	envsDir    string
	cgroupRoot string
	installID  string
	starter    EnvStarter

	// afterOwner 是测试钩子：Create 写完 owner.json 之后、建 cgroup 之前调用。
	afterOwner func()

	mu   sync.Mutex
	envs map[string]*envState // 本进程创建的环境；进程重启后为空（不接管存活环境）
}

// envState 是本进程中一个环境的执行面状态。
type envState struct {
	kind    provider.EnvKind
	gate    *gate
	client  *execClient
	pid     int    // init 的 pid（仅诊断）
	oomBase uint64 // Create 时 memory.events 的 oom_kill
}

var _ provider.Provider = (*Provider)(nil)

// New 返回一个 Provider。Starter 为 nil 时返回错误：生产启动器在 Plan 1B 之后提供，
// 在此之前没有可用于生产的启动器。
func New(opt Options) (*Provider, error) {
	switch {
	case opt.Starter == nil:
		return nil, errors.New("local: 没有可用的环境启动器（生产 EnvStarter 在 Plan 1B 之后提供）")
	case opt.DataDir == "" || opt.CgroupRoot == "":
		return nil, errors.New("local: Options 缺少 DataDir 或 CgroupRoot")
	}
	if err := checkName("install_id", opt.InstallID); err != nil {
		return nil, err
	}
	data, err := filepath.Abs(opt.DataDir)
	if err != nil {
		return nil, fmt.Errorf("local: 数据目录: %w", err)
	}
	// 环境目录根 0711：exec 环境的 init 以映射 root 运行，只有"其他人"的权限，须能经过它到达
	// <envs>/<env_id>/in 与 out（Plan 15 D1、D2）。没有 r 位，不能列出其中的环境。
	envs := filepath.Join(data, "envs")
	if err := os.MkdirAll(envs, 0o711); err != nil {
		return nil, fmt.Errorf("local: 创建环境目录根: %w", err)
	}
	if err := os.Chmod(envs, 0o711); err != nil { // 已存在的目录与 umask 都不影响结果
		return nil, fmt.Errorf("local: 设置环境目录根权限: %w", err)
	}
	// 挂载表中的路径没有符号链接；解析后才能按前缀匹配数据目录下的挂载。
	if data, err = filepath.EvalSymlinks(data); err != nil {
		return nil, fmt.Errorf("local: 数据目录: %w", err)
	}
	return &Provider{
		dataDir:    data,
		envsDir:    filepath.Join(data, "envs"),
		cgroupRoot: filepath.Clean(opt.CgroupRoot),
		installID:  opt.InstallID,
		starter:    opt.Starter,
		envs:       make(map[string]*envState),
	}, nil
}

// checkName 要求 s 是单层路径名：它被拼进目录与 cgroup 路径，混入 "/" 或 ".." 会逃出数据目录或 cgroup 根。
func checkName(what, s string) error {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/\x00") {
		return fmt.Errorf("local: 非法的 %s %q", what, s)
	}
	return nil
}

func (p *Provider) envDir(envID string) string { return filepath.Join(p.envsDir, envID) }

// installCgroup 是本安装的 cgroup 目录 <root>/agentbox-<install_id>。
func (p *Provider) installCgroup() string {
	return filepath.Join(p.cgroupRoot, "agentbox-"+p.installID)
}

func cgroupName(envID string) string { return "env-" + envID }

// group 返回已存在的环境 cgroup；不存在时 ok 为 false，且不创建它。
func (p *Provider) group(envID string) (g *cgroup.Group, ok bool, err error) {
	ok, err = cgroup.Exists(p.installCgroup(), cgroupName(envID))
	if err != nil || !ok {
		return nil, false, err
	}
	g, err = cgroup.New(p.installCgroup(), cgroupName(envID))
	if err != nil {
		return nil, false, err
	}
	return g, true, nil
}

func (p *Provider) state(envID string) *envState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.envs[envID]
}

// ready 报告环境是否完整且 init 就绪：本进程创建、闸门打开、控制连接未断开。
func (s *envState) ready() bool {
	return s != nil && s.gate.isOpen() && !isClosed(s.client.done)
}

func (g *gate) isOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.closed
}

// ---------------------------------------------------------------------------
// owner.json

// ownerFile 是 owner.json 的内容（契约第 3、6 节）。
type ownerFile struct {
	InstallID string `json:"install_id"`
	EnvID     string `json:"env_id"`
	SpecHash  string `json:"spec_hash"`
}

// ownership 是环境目录的归属分类。
type ownership int

const (
	dirAbsent  ownership = iota // 目录不存在
	dirUnknown                  // 目录存在，owner.json 缺失或损坏
	dirForeign                  // owner.json 属于其他安装（或与目录名不符）
	dirOwned                    // owner.json 属于本安装
)

// readOwner 读取并分类 <envs>/<name>。owner.json 的 I/O 错误（不存在除外）作为宿主错误返回。
func (p *Provider) readOwner(name string) (ownership, ownerFile, error) {
	dir := p.envDir(name)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return dirAbsent, ownerFile{}, nil
	} else if err != nil {
		return 0, ownerFile{}, fmt.Errorf("local: 检查环境目录 %s: %w", dir, err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "owner.json"))
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return dirUnknown, ownerFile{}, nil
	}
	if err != nil {
		return 0, ownerFile{}, fmt.Errorf("local: 读取 %s/owner.json: %w", dir, err)
	}
	var o ownerFile
	if err := json.Unmarshal(b, &o); err != nil || o.InstallID == "" || o.EnvID == "" || o.SpecHash == "" {
		return dirUnknown, ownerFile{}, nil
	}
	if o.InstallID != p.installID || o.EnvID != name {
		return dirForeign, o, nil
	}
	return dirOwned, o, nil
}

// writeOwner 原子写入 owner.json：临时文件 + fsync + rename + fsync 目录。
func writeOwner(dir string, o ownerFile) error {
	b, err := json.Marshal(o)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "owner.json.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("local: 写 owner.json: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("local: 写 owner.json: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("local: fsync owner.json: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("local: 写 owner.json: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "owner.json")); err != nil {
		return fmt.Errorf("local: rename owner.json: %w", err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("local: fsync %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("local: fsync %s: %w", dir, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Create

// Create 按契约第 3 节建立环境：目录 → owner.json → cgroup 与限制 → 在 cgroup 内启动 init →
// 打开闸门。只把完整且 init 就绪的环境作为成功返回。目录已存在时不补完、不认领：
// 属于本安装且 spec_hash 一致、init 就绪 → 现状；spec_hash 不同 → ErrConflict；
// 属于本安装但不完整 → ErrIncomplete；owner.json 缺失、损坏或属于其他安装 → ErrForeign。
// ctx 结束时停止推进并返回 ctx 的错误，已建的部分留在原处。
//
// 沙箱内的挂载（rootfs、workspace、Gateway socket、/in、/out 的 bind）由 init 在自己的 mount namespace 中建立；
// 宿主侧唯一的挂载是 exec 环境 <envdir>/out 的 tmpfs（prepareExecDirs），在 owner.json 之后、启动 init 之前。
func (p *Provider) Create(ctx context.Context, spec provider.EnvSpec) (provider.EnvInfo, error) {
	if err := spec.Validate(); err != nil {
		return provider.EnvInfo{}, err
	}
	if err := checkName("env_id", spec.EnvID); err != nil {
		return provider.EnvInfo{}, err
	}
	if spec.InstallID != p.installID {
		return provider.EnvInfo{}, fmt.Errorf("local: spec 的 install_id %q 不是本安装 %q", spec.InstallID, p.installID)
	}
	if err := ctx.Err(); err != nil {
		return provider.EnvInfo{}, err
	}
	hash := provider.SpecHash(spec)
	dir := p.envDir(spec.EnvID)

	// exec 环境的目录 0711：init 须经过它到达 in/ 与 out/（D2）；其余环境 0700。
	mode := os.FileMode(0o700)
	if spec.Kind == provider.KindExec {
		mode = 0o711
	}
	if err := os.Mkdir(dir, mode); errors.Is(err, os.ErrExist) {
		return p.existing(spec, hash)
	} else if err != nil {
		return provider.EnvInfo{}, fmt.Errorf("local: 创建环境目录: %w", err)
	}
	if err := os.Chmod(dir, mode); err != nil { // 不受 umask 影响
		return provider.EnvInfo{}, fmt.Errorf("local: 设置环境目录权限: %w", err)
	}
	if err := syncDir(p.envsDir); err != nil {
		return provider.EnvInfo{}, err
	}
	if err := writeOwner(dir, ownerFile{InstallID: p.installID, EnvID: spec.EnvID, SpecHash: hash}); err != nil {
		return provider.EnvInfo{}, err
	}
	if p.afterOwner != nil {
		p.afterOwner()
	}
	if err := ctx.Err(); err != nil {
		return provider.EnvInfo{}, err
	}
	if spec.Kind == provider.KindExec {
		// 挂载在 owner.json 之后、启动 init 之前；失败即残留（ErrIncomplete），由 Stop、Destroy 清理。
		if err := prepareExecDirs(spec, dir); err != nil {
			return provider.EnvInfo{}, err
		}
		if err := ctx.Err(); err != nil {
			return provider.EnvInfo{}, err
		}
	}

	// 同名 cgroup 已存在说明有更早的残留（目录被外部删除）；不复用其中的进程与计数。
	if ok, err := cgroup.Exists(p.installCgroup(), cgroupName(spec.EnvID)); err != nil {
		return provider.EnvInfo{}, err
	} else if ok {
		return provider.EnvInfo{}, fmt.Errorf("%w: 环境 cgroup 已存在", provider.ErrIncomplete)
	}
	cg, err := cgroup.New(p.installCgroup(), cgroupName(spec.EnvID))
	if err != nil {
		return provider.EnvInfo{}, fmt.Errorf("local: 创建环境 cgroup: %w", err)
	}
	if err := cg.Apply(cgroupLimits(spec.Limits)); err != nil {
		return provider.EnvInfo{}, fmt.Errorf("local: 设置环境 cgroup 限制: %w", err)
	}
	oomBase, err := cg.OOMKills()
	if err != nil {
		return provider.EnvInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return provider.EnvInfo{}, err
	}

	conn, pid, err := p.starter.StartInit(ctx, spec, dir, cg)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return provider.EnvInfo{}, cerr
		}
		return provider.EnvInfo{}, fmt.Errorf("local: 启动 init: %w", err)
	}
	if err := ctx.Err(); err != nil {
		// 不再推进：不打开闸门。关闭连接后 init 读到 EOF 退出；残留由 Stop、Destroy 清理。
		conn.Close()
		return provider.EnvInfo{}, err
	}
	st := &envState{kind: spec.Kind, gate: &gate{}, client: newExecClient(conn), pid: pid, oomBase: oomBase}
	p.mu.Lock()
	p.envs[spec.EnvID] = st
	p.mu.Unlock()
	return provider.EnvInfo{EnvID: spec.EnvID, Kind: spec.Kind, Complete: true, Running: true, InDir: inDir(spec, dir)}, nil
}

// exec 环境的宿主侧目录（Plan 15 D1、D2）：<envdir>/in 是输入暂存（调用方未提供 Mounts.In 时），
// <envdir>/out 是宿主挂载的 tmpfs，init 把两者分别只读、可写地 bind 到沙箱内的 /in 与 /out。
const (
	execInName  = "in"
	execOutName = "out"
	outInodes   = 1024 // /out 的 nr_inodes（§19 补充）
)

// inDir 返回 exec 环境由 provider 建立的输入暂存目录；其他情况为空。
func inDir(spec provider.EnvSpec, dir string) string {
	if spec.Kind != provider.KindExec || spec.Mounts.In != "" {
		return ""
	}
	return filepath.Join(dir, execInName)
}

// prepareExecDirs 建立 exec 环境的 in/（root 0755，需要时）与 out/，并在 out/ 上挂载 tmpfs：
// size=OutBytes、nr_inodes=1024、mode=0700、属主为映射 uid/gid 1000（宿主 UIDBase+1000）、nosuid、nodev。
// tmpfs 页面按写入者计入其 memory cgroup（即环境 cgroup）。卸载由 Destroy 的 unmountUnder 负责。
func prepareExecDirs(spec provider.EnvSpec, dir string) error {
	if in := inDir(spec, dir); in != "" {
		if err := os.Mkdir(in, 0o755); err != nil {
			return fmt.Errorf("local: 建立 exec 输入目录: %w", err)
		}
		if err := os.Chmod(in, 0o755); err != nil {
			return fmt.Errorf("local: 设置 exec 输入目录权限: %w", err)
		}
	}
	out := filepath.Join(dir, execOutName)
	if err := os.Mkdir(out, 0o700); err != nil {
		return fmt.Errorf("local: 建立 exec 输出挂载点: %w", err)
	}
	id := spec.UIDBase + workloadID
	opts := fmt.Sprintf("size=%d,nr_inodes=%d,mode=0700,uid=%d,gid=%d", spec.Mounts.OutBytes, outInodes, id, id)
	if err := syscall.Mount("tmpfs", out, "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, opts); err != nil {
		return fmt.Errorf("local: 挂载 exec /out tmpfs: %w", err)
	}
	return nil
}

// existing 分类已存在的环境目录（契约第 3 节 Create 的幂等列）。
func (p *Provider) existing(spec provider.EnvSpec, hash string) (provider.EnvInfo, error) {
	own, o, err := p.readOwner(spec.EnvID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	switch own {
	case dirAbsent: // Mkdir 报告已存在之后被删除：不猜测，按残留处理
		return provider.EnvInfo{}, fmt.Errorf("%w: 环境目录在创建期间消失", provider.ErrIncomplete)
	case dirUnknown:
		return provider.EnvInfo{}, fmt.Errorf("%w: %s 没有有效的 owner.json", provider.ErrForeign, p.envDir(spec.EnvID))
	case dirForeign:
		return provider.EnvInfo{}, fmt.Errorf("%w: %s 属于安装 %q", provider.ErrForeign, p.envDir(spec.EnvID), o.InstallID)
	}
	if o.SpecHash != hash {
		return provider.EnvInfo{}, provider.ErrConflict
	}
	if !p.state(spec.EnvID).ready() {
		return provider.EnvInfo{}, provider.ErrIncomplete
	}
	info, err := p.info(spec.EnvID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	info.InDir = inDir(spec, p.envDir(spec.EnvID))
	return info, nil
}

// info 返回属于本安装的环境的 EnvInfo。
func (p *Provider) info(envID string) (provider.EnvInfo, error) {
	st := p.state(envID)
	info := provider.EnvInfo{EnvID: envID, Complete: st.ready()}
	if st != nil {
		info.Kind = st.kind
	}
	g, ok, err := p.group(envID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	if ok {
		if info.Running, err = g.Populated(); err != nil {
			return provider.EnvInfo{}, err
		}
	}
	return info, nil
}

func cgroupLimits(l provider.Limits) cgroup.Limits {
	out := cgroup.Limits{MemoryMax: l.MemoryMax, PidsMax: int(l.PidsMax)}
	if l.CPUQuotaUs > 0 {
		out.CPUMax = strconv.FormatInt(l.CPUQuotaUs, 10) + " 100000"
	}
	return out
}

// ---------------------------------------------------------------------------
// StartExec

// StartExec 经环境的控制连接启动一次执行（契约第 3、5 节；见 exec.go）。本进程没有该环境的
// init（未创建、已销毁或进程重启前创建）→ ErrNotFound。
func (p *Provider) StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	st := p.state(envID)
	if st == nil {
		return nil, fmt.Errorf("%w: 环境 %q 没有可用的 init", provider.ErrNotFound, envID)
	}
	h, err := st.client.start(ctx, st.gate, spec)
	if errors.Is(err, provider.ErrNotFound) && !st.gate.isOpen() {
		// 在途启动在闸门关闭前登记，而 Stop 已回收 init、断开连接：start 没有发出。
		return nil, fmt.Errorf("%w: %v", provider.ErrStopping, err)
	}
	return h, err
}

// ---------------------------------------------------------------------------
// Stop

// Stop 关闭执行闸门 → cgroup.kill → 等待 populated 0（契约第 3 节）。成功仅当权威检查成立：
// 环境 cgroup 不存在，或存在且 populated 为 0。期限内未确认 → ErrStopUnconfirmed。
// 不等待在途启动；不卸载、不删除。
func (p *Provider) Stop(ctx context.Context, envID string) error {
	if err := checkName("env_id", envID); err != nil {
		return err
	}
	st := p.state(envID)
	if st != nil {
		st.gate.close()
	}
	if err := p.killAndWait(ctx, envID); err != nil {
		return err
	}
	if st != nil {
		// 执行树已不存在，init 已退出；关闭宿主端并等读 goroutine 结束。
		st.client.close()
	}
	return nil
}

func (p *Provider) killAndWait(ctx context.Context, envID string) error {
	g, ok, err := p.group(envID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	gone := func() bool {
		ok, err := cgroup.Exists(p.installCgroup(), cgroupName(envID))
		return err == nil && !ok
	}
	if err := g.Kill(); err != nil {
		if gone() {
			return nil
		}
		return fmt.Errorf("local: cgroup.kill: %w", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultStopTimeout)
		defer cancel()
	}
	if err := g.WaitEmpty(ctx); err != nil {
		if gone() {
			return nil
		}
		return fmt.Errorf("%w: %w", provider.ErrStopUnconfirmed, err)
	}
	return nil
}

// stopped 是 Stop 的权威检查：环境 cgroup 不存在，或存在且 populated 为 0。
func (p *Provider) stopped(envID string) (bool, error) {
	g, ok, err := p.group(envID)
	if err != nil || !ok {
		return err == nil, err
	}
	populated, err := g.Populated()
	if err != nil {
		return false, err
	}
	return !populated, nil
}

// ---------------------------------------------------------------------------
// Destroy

// Destroy 逐层清理（契约第 3 节）：数据目录下属于该环境的挂载 → 环境 cgroup → 环境目录。
// 前置条件是 Stop 的权威检查（否则 ErrNotStopped）；目录存在但不能证明属于本安装 → ErrForeign，
// 不触碰任何一层。成功前逐层核对三层都已不存在；已不存在的层跳过。
func (p *Provider) Destroy(ctx context.Context, envID string) error {
	if err := checkName("env_id", envID); err != nil {
		return err
	}
	own, o, err := p.readOwner(envID)
	if err != nil {
		return err
	}
	switch own {
	case dirUnknown:
		return fmt.Errorf("%w: %s 没有有效的 owner.json", provider.ErrForeign, p.envDir(envID))
	case dirForeign:
		return fmt.Errorf("%w: %s 属于安装 %q", provider.ErrForeign, p.envDir(envID), o.InstallID)
	}
	if ok, err := p.stopped(envID); err != nil {
		return err
	} else if !ok {
		return provider.ErrNotStopped
	}
	// 执行树已不存在：闸门与连接不再有用。
	if st := p.state(envID); st != nil {
		st.gate.close()
		st.client.close()
	}
	dir := p.envDir(envID)

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := unmountUnder(dir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cgPath := filepath.Join(p.installCgroup(), cgroupName(envID))
	if err := os.Remove(cgPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("local: 删除环境 cgroup: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// RemoveAll 会进入挂载点删除其中内容；删除目录前确认其下已没有挂载。
	if ms, err := mountsUnder(dir); err != nil {
		return err
	} else if len(ms) > 0 {
		return fmt.Errorf("local: %s 下仍有挂载 %v", dir, ms)
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("local: 删除环境目录: %w", err)
	}

	// 逐层核对。
	if ms, err := mountsUnder(dir); err != nil {
		return err
	} else if len(ms) > 0 {
		return fmt.Errorf("local: 核对：%s 下仍有挂载 %v", dir, ms)
	}
	if ok, err := cgroup.Exists(p.installCgroup(), cgroupName(envID)); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("local: 核对：环境 cgroup %s 仍存在", cgPath)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("local: 核对：环境目录 %s 仍存在（%v）", dir, err)
	}
	p.mu.Lock()
	delete(p.envs, envID)
	p.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// List、ResourceDiag

// List 报告 owner.json 属于本安装的环境：Complete 为闸门打开且 init 存活，Running 为 cgroup populated。
// Kind 只对本进程创建的环境已知（owner.json 不记录它）。
func (p *Provider) List(ctx context.Context) ([]provider.EnvInfo, error) {
	ents, err := os.ReadDir(p.envsDir)
	if err != nil {
		return nil, fmt.Errorf("local: 读取环境目录: %w", err)
	}
	var out []provider.EnvInfo
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !e.IsDir() {
			continue
		}
		own, _, err := p.readOwner(e.Name())
		if err != nil {
			return nil, err
		}
		if own != dirOwned {
			continue
		}
		info, err := p.info(e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// ResourceDiag 读取环境 cgroup 的 OOM 与 CPU 统计（规格 §4.1）；OOMKillDelta 相对 Create 时的基线。
// 环境 cgroup 不存在 → ErrNotFound。
func (p *Provider) ResourceDiag(ctx context.Context, envID string) (provider.ResourceDiag, error) {
	if err := checkName("env_id", envID); err != nil {
		return provider.ResourceDiag{}, err
	}
	g, ok, err := p.group(envID)
	if err != nil {
		return provider.ResourceDiag{}, err
	}
	if !ok {
		return provider.ResourceDiag{}, fmt.Errorf("%w: 环境 %q 的 cgroup", provider.ErrNotFound, envID)
	}
	oom, err := g.OOMKills()
	if err != nil {
		return provider.ResourceDiag{}, err
	}
	cpu, err := g.CPUUsageUsec()
	if err != nil {
		return provider.ResourceDiag{}, err
	}
	var base uint64
	if st := p.state(envID); st != nil {
		base = st.oomBase
	}
	var delta uint64
	if oom > base {
		delta = oom - base
	}
	return provider.ResourceDiag{OOMKillDelta: delta, OOMObserved: delta > 0, CPUUsageUsec: cpu}, nil
}
