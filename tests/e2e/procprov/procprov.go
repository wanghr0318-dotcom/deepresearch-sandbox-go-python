//go:build linux

// Package procprov 是只供系统级测试（tests/e2e）使用的进程型 provider：环境是数据目录下的一个目录，
// 执行是宿主上的普通进程组（不隔离）。与内存中的 provider/fake 不同，它的全部事实都在磁盘与 /proc 上，
// server 被 SIGKILL 后重启的进程能够看到上一进程留下的环境与仍存活的进程——系统级故障实验（规格 §16.4
// E5、E13、E14）需要这一点。
//
// 布局与归属（与 provider/local 相同的字段）：
//   - <data>/envs/<env_id>/owner.json：{install_id, env_id, spec_hash}。Create 先在 <data>/envs-tmp 中
//     写好 owner.json，再原子 rename 到 envs/ 下，因此 envs/ 下的目录从出现起就带有 owner.json；
//   - <data>/envs/<env_id>/procs/<exec_id>.json：执行的进程（pid 与 /proc/<pid>/stat 第 22 项的启动时间，
//     防止 pid 复用）；
//   - 进程环境变量 AGENTBOX_PROCPROV_ENV=<环境目录>：执行树的权威标记（相当于 cgroup 成员关系）。setsid、
//     双重 fork 的后代同样继承它；Stop 与 Destroy 的前置检查按它扫描 /proc，不依赖本进程的内存状态；
//   - <data>/procprov.journal：执行开始、Worker 退出与停止确认的时间（测试据此检查 I2：替代执行不早于
//     旧执行树确认停止）。
//
// 执行：Worker 进程与一个保持进程（keeper，持有 Worker stdin 的写端与 stdout 的读端）同在一个新的进程组
// 中。server 正常运行时 keeper 不起作用（关闭 stdin 时随之终止）；server 被 SIGKILL 后 keeper 使 Worker
// 的管道保持打开，Worker 不因 EOF 退出——与真实环境中执行树在 server 死亡后继续存活相同。重启的 server
// 不接管存活环境，而是停止它们（§14.1 第 6 步）。
//
// "完整"与 provider/local 一致：只有本进程创建且闸门打开的环境是完整的；重启后上一进程的环境报告为
// 不完整（OwnedPartial），StartExec 返回 ErrNotFound。
package procprov

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// MarkerEnv 是执行树的标记环境变量。
const MarkerEnv = "AGENTBOX_PROCPROV_ENV"

// workspaceMount 是环境内 workspace 的路径（规格 §4.5）；宿主上以 EnvSpec.Mounts.Workspace 代替。
const workspaceMount = "/workspace"

const defaultStopTimeout = 10 * time.Second

// Options 配置 provider。
type Options struct {
	DataDir   string // 数据目录（绝对路径）
	InstallID string // 本安装的标识，写入 owner.json
	// Keeper 是保持进程的命令，默认 sleep infinity。
	Keeper []string
	// StopTimeout 是 ctx 没有期限时 Stop 等待执行树清空的上限，默认 10 s。
	StopTimeout time.Duration
	// ReadOnly 只用于扫描（verify-invariants、测试检查）：不清除临时目录。服务运行中也可以构造。
	ReadOnly bool
}

// Provider 实现 provider.Provider。
type Provider struct {
	opt     Options
	envsDir string
	tmpDir  string

	mu    sync.Mutex
	envs  map[string]*envState // 本进程创建的环境
	block map[string]*startBlock
}

type envState struct {
	spec provider.EnvSpec
	open bool // 执行闸门
}

type startBlock struct {
	blocked chan struct{}
	release chan struct{}
}

type ownerFile struct {
	InstallID string `json:"install_id"`
	EnvID     string `json:"env_id"`
	SpecHash  string `json:"spec_hash"`
}

// New 返回以 DataDir 为根的 provider，并清除中断的创建与销毁留下的临时目录（它们不在 envs/ 下，从未
// 成为环境）。
func New(opt Options) (*Provider, error) {
	if !filepath.IsAbs(opt.DataDir) || opt.InstallID == "" {
		return nil, errors.New("procprov: 需要绝对路径的 DataDir 与 InstallID")
	}
	if len(opt.Keeper) == 0 {
		opt.Keeper = []string{"sleep", "infinity"}
	}
	if opt.StopTimeout <= 0 {
		opt.StopTimeout = defaultStopTimeout
	}
	p := &Provider{opt: opt, envsDir: filepath.Join(opt.DataDir, "envs"), tmpDir: filepath.Join(opt.DataDir, "envs-tmp"),
		envs: map[string]*envState{}, block: map[string]*startBlock{}}
	if opt.ReadOnly {
		return p, nil
	}
	if err := os.RemoveAll(p.tmpDir); err != nil {
		return nil, fmt.Errorf("procprov: 清除临时目录: %w", err)
	}
	for _, d := range []string{p.envsDir, p.tmpDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("procprov: %w", err)
		}
	}
	return p, nil
}

func (p *Provider) envDir(envID string) string { return filepath.Join(p.envsDir, envID) }

func checkName(envID string) error {
	if envID == "" || envID == "." || envID == ".." || strings.ContainsAny(envID, `/\`) {
		return fmt.Errorf("procprov: 非法的 env_id %q", envID)
	}
	return nil
}

// ---- 归属 ----

type ownership int

const (
	dirAbsent  ownership = iota
	dirUnknown           // 目录存在但 owner.json 缺失或损坏
	dirForeign           // 属于其他安装（或与目录名不符）
	dirOwned
)

func (p *Provider) readOwner(envID string) (ownership, ownerFile, error) {
	dir := p.envDir(envID)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return dirAbsent, ownerFile{}, nil
	} else if err != nil {
		return 0, ownerFile{}, err
	}
	b, err := os.ReadFile(filepath.Join(dir, "owner.json"))
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return dirUnknown, ownerFile{}, nil
	} else if err != nil {
		return 0, ownerFile{}, err
	}
	var o ownerFile
	if json.Unmarshal(b, &o) != nil || o.InstallID == "" || o.EnvID == "" || o.SpecHash == "" {
		return dirUnknown, ownerFile{}, nil
	}
	if o.InstallID != p.opt.InstallID || o.EnvID != envID {
		return dirForeign, o, nil
	}
	return dirOwned, o, nil
}

// ---- Create ----

// Create：owner.json 先于一切写入（在临时目录中写好后原子 rename 进 envs/）。已存在时不补完、不认领：
// 属于本安装、spec 相同且本进程创建、闸门打开 → 现状；spec 不同 → ErrConflict；其余属于本安装的 →
// ErrIncomplete；owner.json 缺失、损坏或属于其他安装 → ErrForeign。
func (p *Provider) Create(ctx context.Context, spec provider.EnvSpec) (provider.EnvInfo, error) {
	if err := spec.Validate(); err != nil {
		return provider.EnvInfo{}, err
	}
	if err := checkName(spec.EnvID); err != nil {
		return provider.EnvInfo{}, err
	}
	if spec.InstallID != p.opt.InstallID {
		return provider.EnvInfo{}, fmt.Errorf("procprov: spec 的 install_id %q 不是本安装 %q", spec.InstallID, p.opt.InstallID)
	}
	if err := ctx.Err(); err != nil {
		return provider.EnvInfo{}, err
	}
	hash := provider.SpecHash(spec)
	p.mu.Lock()
	defer p.mu.Unlock()
	own, o, err := p.readOwner(spec.EnvID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	switch own {
	case dirUnknown:
		return provider.EnvInfo{}, fmt.Errorf("%w: %s 没有有效的 owner.json", provider.ErrForeign, p.envDir(spec.EnvID))
	case dirForeign:
		return provider.EnvInfo{}, fmt.Errorf("%w: %s 属于安装 %q", provider.ErrForeign, p.envDir(spec.EnvID), o.InstallID)
	case dirOwned:
		if o.SpecHash != hash {
			return provider.EnvInfo{}, provider.ErrConflict
		}
		if st := p.envs[spec.EnvID]; st == nil || !st.open {
			return provider.EnvInfo{}, provider.ErrIncomplete
		}
		return p.infoLocked(spec.EnvID)
	}
	tmp, err := os.MkdirTemp(p.tmpDir, spec.EnvID+"-")
	if err != nil {
		return provider.EnvInfo{}, fmt.Errorf("procprov: %w", err)
	}
	b, _ := json.Marshal(ownerFile{InstallID: p.opt.InstallID, EnvID: spec.EnvID, SpecHash: hash})
	if err := writeSync(filepath.Join(tmp, "owner.json"), b); err != nil {
		return provider.EnvInfo{}, err
	}
	if err := os.Mkdir(filepath.Join(tmp, "procs"), 0o700); err != nil {
		return provider.EnvInfo{}, fmt.Errorf("procprov: %w", err)
	}
	if err := os.Rename(tmp, p.envDir(spec.EnvID)); err != nil {
		return provider.EnvInfo{}, fmt.Errorf("procprov: 登记环境目录: %w", err)
	}
	if err := syncDir(p.envsDir); err != nil {
		return provider.EnvInfo{}, err
	}
	p.envs[spec.EnvID] = &envState{spec: spec, open: true}
	return provider.EnvInfo{EnvID: spec.EnvID, Kind: spec.Kind, Complete: true}, nil
}

func writeSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("procprov: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("procprov: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("procprov: %w", err)
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("procprov: %w", err)
	}
	defer d.Close()
	return d.Sync()
}

// ---- StartExec ----

// StartExec 在宿主上启动执行：keeper 为进程组首进程，Worker 加入同一进程组。闸门检查与启动在同一把锁下，
// Stop 关闭闸门后不会再有新进程。ExecSpec.Dir 与 init 的 out_dir 中的 /workspace 前缀换成宿主 workspace。
func (p *Provider) StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	if len(spec.Argv) == 0 || spec.ExecID == "" || checkName(spec.ExecID) != nil {
		return nil, fmt.Errorf("procprov: ExecSpec 需要 Argv 与合法的 ExecID")
	}
	p.mu.Lock()
	st := p.envs[envID]
	switch {
	case st == nil:
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: 环境 %q 不是本进程创建的", provider.ErrNotFound, envID)
	case !st.open:
		p.mu.Unlock()
		return nil, provider.ErrStopping
	}
	blk := p.block[envID]
	delete(p.block, envID)
	p.mu.Unlock()

	if blk != nil { // 在途启动：已通过闸门，尚未启动进程
		close(blk.blocked)
		select {
		case <-blk.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if !st.open { // Stop 已在在途期间关闭闸门：启动没有发出
		return nil, provider.ErrControlLost
	}
	return p.spawn(envID, st.spec, spec)
}

// spawn 启动 keeper 与 Worker（调用方持有 p.mu）。
func (p *Provider) spawn(envID string, env provider.EnvSpec, spec provider.ExecSpec) (provider.ExecHandle, error) {
	dir := p.envDir(envID)
	ws := env.Mounts.Workspace
	workDir := spec.Dir
	if ws != "" && (workDir == workspaceMount || strings.HasPrefix(workDir, workspaceMount+"/")) {
		workDir = ws + strings.TrimPrefix(workDir, workspaceMount)
	}
	if workDir == "" {
		workDir = dir
	}
	marker := MarkerEnv + "=" + dir

	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("procprov: %w", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		closeAll(inR, inW)
		return nil, fmt.Errorf("procprov: %w", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		closeAll(inR, inW, outR, outW)
		return nil, fmt.Errorf("procprov: %w", err)
	}
	fail := func(err error) (provider.ExecHandle, error) {
		closeAll(inR, inW, outR, outW, errR, errW)
		return nil, fmt.Errorf("%w: %v", provider.ErrStartFailed, err)
	}

	keeper := exec.Command(p.opt.Keeper[0], p.opt.Keeper[1:]...)
	keeper.Env = append(os.Environ(), marker)
	keeper.ExtraFiles = []*os.File{inW, outR}
	keeper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := keeper.Start(); err != nil {
		return fail(err)
	}
	pgid := keeper.Process.Pid
	worker := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	worker.Dir = workDir
	worker.Env = append(append(os.Environ(), spec.Env...), marker)
	worker.Stdin, worker.Stdout, worker.Stderr = inR, outW, errW
	worker.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: pgid}
	if err := worker.Start(); err != nil {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		_ = keeper.Wait()
		return fail(err)
	}
	closeAll(inR, outW, errW)

	rec := procRecord{ExecID: spec.ExecID, Pgid: pgid}
	for _, pid := range []int{pgid, worker.Process.Pid} {
		start, _ := startTime(pid)
		rec.Procs = append(rec.Procs, procID{Pid: pid, Start: start})
	}
	b, _ := json.Marshal(rec)
	_ = writeSync(filepath.Join(dir, "procs", spec.ExecID+".json"), b)
	p.journal(journalEntry{Event: "start", EnvID: envID, ExecID: spec.ExecID, Pids: []int{pgid, worker.Process.Pid}})

	h := &handle{p: p, envID: envID, pgid: pgid, keeper: keeper, worker: worker, stdout: outR, stderr: errR,
		done: make(chan struct{}), killKeeper: sync.OnceFunc(func() { _ = keeper.Process.Kill() })}
	pr, pw := io.Pipe()
	h.stdin = pw
	go h.pumpStdin(pr, inW, ws)
	go func() { _ = keeper.Wait() }()
	go h.wait()
	return h, nil
}

func closeAll(fs ...*os.File) {
	for _, f := range fs {
		_ = f.Close()
	}
}

// ---- 执行句柄 ----

type handle struct {
	p          *Provider
	envID      string
	pgid       int
	keeper     *exec.Cmd
	worker     *exec.Cmd
	stdin      *io.PipeWriter
	stdout     *os.File
	stderr     *os.File
	killKeeper func()

	done   chan struct{}
	status provider.ExitStatus
}

// pumpStdin 把宿主写入的内容转给 Worker；第一行（init）中以 /workspace 开头的 out_dir 换成宿主路径。
// 宿主关闭 stdin 时关闭 Worker 的 stdin 并终止 keeper（keeper 持有的写端不再阻止 EOF）。
func (h *handle) pumpStdin(pr *io.PipeReader, inW *os.File, ws string) {
	defer func() {
		_ = inW.Close()
		h.killKeeper()
	}()
	br := bufio.NewReaderSize(pr, 1<<20)
	first := true
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if first {
				line, first = rewriteOutDir(line, ws), false
			}
			if _, werr := inW.Write(line); werr != nil {
				_ = pr.CloseWithError(werr)
				return
			}
		}
		if err != nil {
			_ = pr.CloseWithError(err)
			return
		}
	}
}

func rewriteOutDir(line []byte, ws string) []byte {
	var m map[string]json.RawMessage
	if ws == "" || json.Unmarshal(line, &m) != nil {
		return line
	}
	var out string
	if json.Unmarshal(m["out_dir"], &out) != nil || (out != workspaceMount && !strings.HasPrefix(out, workspaceMount+"/")) {
		return line
	}
	b, err := json.Marshal(ws + strings.TrimPrefix(out, workspaceMount))
	if err != nil {
		return line
	}
	m["out_dir"] = b
	nb, err := json.Marshal(m)
	if err != nil {
		return line
	}
	return append(nb, '\n')
}

func (h *handle) wait() {
	err := h.worker.Wait()
	h.killKeeper()
	st := provider.ExitStatus{Code: 1}
	if ps := h.worker.ProcessState; ps != nil {
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			st = provider.ExitStatus{Signal: ws.Signal()}
		} else {
			st = provider.ExitStatus{Code: ps.ExitCode()}
		}
	} else if err == nil {
		st = provider.ExitStatus{}
	}
	h.status = st
	h.p.journal(journalEntry{Event: "exit", EnvID: h.envID, Pids: []int{h.worker.Process.Pid}})
	close(h.done)
}

func (h *handle) Stdin() io.WriteCloser { return h.stdin }
func (h *handle) Stdout() io.ReadCloser { return h.stdout }
func (h *handle) Stderr() io.ReadCloser { return h.stderr }

// Wait 返回 Worker 的退出状态；被信号终止时 Signal 非 0。
func (h *handle) Wait() (provider.ExitStatus, error) {
	<-h.done
	return h.status, nil
}

// Terminate 向进程组发送 SIGTERM，grace 后 SIGKILL（grace ≤ 0 时直接 SIGKILL）。
func (h *handle) Terminate(grace time.Duration) error {
	if grace <= 0 {
		_ = syscall.Kill(-h.pgid, syscall.SIGKILL)
		return nil
	}
	_ = syscall.Kill(-h.pgid, syscall.SIGTERM)
	go func() {
		select {
		case <-h.done:
		case <-time.After(grace):
			_ = syscall.Kill(-h.pgid, syscall.SIGKILL)
		}
	}()
	return nil
}

// ---- Stop、Destroy ----

// Stop 关闭闸门，杀死该环境的全部进程（记录的进程组与带标记的全部进程），并确认没有存活进程（权威
// 检查，与本进程的内存状态无关）。期限内未确认 → ErrStopUnconfirmed。不等待在途启动。
func (p *Provider) Stop(ctx context.Context, envID string) error {
	if err := checkName(envID); err != nil {
		return err
	}
	p.mu.Lock()
	own, _, err := p.readOwner(envID)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	if own == dirUnknown || own == dirForeign {
		p.mu.Unlock()
		return provider.ErrForeign
	}
	if st := p.envs[envID]; st != nil {
		st.open = false
	}
	p.mu.Unlock()
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.opt.StopTimeout)
		defer cancel()
	}
	dir := p.envDir(envID)
	for {
		live, err := p.live(dir)
		if err != nil {
			return err
		}
		if len(live) == 0 {
			p.journal(journalEntry{Event: "stopped", EnvID: envID})
			return nil
		}
		for _, pid := range live {
			_ = syscall.Kill(-pid, syscall.SIGKILL) // 若它是进程组首进程
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: 仍有进程 %v", provider.ErrStopUnconfirmed, live)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// live 返回环境 dir 的存活进程：记录中启动时间一致的进程，以及带标记的全部进程。
func (p *Provider) live(dir string) ([]int, error) {
	set := map[int]bool{}
	ents, err := os.ReadDir(filepath.Join(dir, "procs"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, "procs", e.Name()))
		if err != nil {
			continue
		}
		var rec procRecord
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		for _, pr := range rec.Procs {
			if s, ok := startTime(pr.Pid); ok && s == pr.Start {
				set[pr.Pid] = true
			}
		}
	}
	marked, err := Marked(dir)
	if err != nil {
		return nil, err
	}
	for _, pid := range marked {
		set[pid] = true
	}
	out := make([]int, 0, len(set))
	for pid := range set {
		out = append(out, pid)
	}
	sort.Ints(out)
	return out, nil
}

// Destroy 要求没有存活进程（否则 ErrNotStopped），然后把环境目录原子移出 envs/ 再删除。目录不能证明
// 属于本安装 → ErrForeign，不触碰。目录已不存在 → nil。
func (p *Provider) Destroy(ctx context.Context, envID string) error {
	if err := checkName(envID); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	own, o, err := p.readOwner(envID)
	if err != nil {
		return err
	}
	switch own {
	case dirAbsent:
		delete(p.envs, envID)
		return nil
	case dirUnknown:
		return fmt.Errorf("%w: %s 没有有效的 owner.json", provider.ErrForeign, p.envDir(envID))
	case dirForeign:
		return fmt.Errorf("%w: %s 属于安装 %q", provider.ErrForeign, p.envDir(envID), o.InstallID)
	}
	dir := p.envDir(envID)
	if live, err := p.live(dir); err != nil {
		return err
	} else if len(live) > 0 {
		return provider.ErrNotStopped
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	trash := filepath.Join(p.tmpDir, fmt.Sprintf("destroy-%s-%d", envID, time.Now().UnixNano()))
	if err := os.Rename(dir, trash); err != nil {
		return fmt.Errorf("procprov: 移出环境目录: %w", err)
	}
	if err := syncDir(p.envsDir); err != nil {
		return err
	}
	delete(p.envs, envID)
	if err := os.RemoveAll(trash); err != nil {
		return fmt.Errorf("procprov: 删除环境目录: %w", err)
	}
	return nil
}

// ---- List、Scan、ResourceDiag ----

// List 报告 owner.json 属于本安装的环境：Complete 为本进程创建且闸门打开，Running 为有存活进程。
func (p *Provider) List(ctx context.Context) ([]provider.EnvInfo, error) {
	ids, err := p.dirNames()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []provider.EnvInfo
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		own, _, err := p.readOwner(id)
		if err != nil {
			return nil, err
		}
		if own != dirOwned {
			continue
		}
		info, err := p.infoLocked(id)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

func (p *Provider) infoLocked(envID string) (provider.EnvInfo, error) {
	info := provider.EnvInfo{EnvID: envID}
	if st := p.envs[envID]; st != nil {
		info.Kind, info.Complete = st.spec.Kind, st.open
	}
	live, err := p.live(p.envDir(envID))
	if err != nil {
		return provider.EnvInfo{}, err
	}
	info.Running = len(live) > 0
	return info, nil
}

func (p *Provider) dirNames() ([]string, error) {
	ents, err := os.ReadDir(p.envsDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("procprov: 读取环境目录: %w", err)
	}
	var ids []string
	for _, e := range ents {
		ids = append(ids, e.Name())
	}
	return ids, nil
}

// Scan 是独立原始扫描：env_dir 层为 envs/ 下的每一项（无或损坏 owner.json → Unknown，其他安装 →
// Foreign，本安装 → 本进程创建且闸门打开时 OwnedComplete，否则 OwnedPartial）；process 层为带本数据
// 目录标记的每个存活进程，随所在环境目录分类（目录已不存在时为本安装的残留 OwnedPartial）。
func (p *Provider) Scan(ctx context.Context) (provider.ScanReport, error) {
	ids, err := p.dirNames()
	if err != nil {
		return provider.ScanReport{}, err
	}
	var r provider.ScanReport
	owner := map[string]provider.Owner{}
	p.mu.Lock()
	for _, id := range ids {
		o := provider.Unknown
		own, _, err := p.readOwner(id)
		if err != nil {
			p.mu.Unlock()
			return r, err
		}
		switch own {
		case dirForeign:
			o = provider.Foreign
		case dirOwned:
			o = provider.OwnedPartial
			if st := p.envs[id]; st != nil && st.open {
				o = provider.OwnedComplete
			}
		}
		owner[id] = o
		r.Items = append(r.Items, provider.ScanItem{Layer: "env_dir", Path: p.envDir(id), EnvID: id, Owner: o})
	}
	p.mu.Unlock()
	procs, err := markedUnder(p.envsDir + "/")
	if err != nil {
		return r, err
	}
	for _, mp := range procs {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		id := filepath.Base(mp.dir)
		o, ok := owner[id]
		if !ok {
			o = provider.OwnedPartial
		}
		r.Items = append(r.Items, provider.ScanItem{Layer: "process", Path: "/proc/" + strconv.Itoa(mp.pid), EnvID: id, Owner: o})
	}
	sort.SliceStable(r.Items, func(i, j int) bool { return r.Items[i].EnvID < r.Items[j].EnvID })
	return r, nil
}

// ResourceDiag：进程型 provider 没有 cgroup 统计，存在的环境返回零值。
func (p *Provider) ResourceDiag(_ context.Context, envID string) (provider.ResourceDiag, error) {
	if err := checkName(envID); err != nil {
		return provider.ResourceDiag{}, err
	}
	own, _, err := p.readOwner(envID)
	if err != nil {
		return provider.ResourceDiag{}, err
	}
	if own != dirOwned {
		return provider.ResourceDiag{}, provider.ErrNotFound
	}
	return provider.ResourceDiag{}, nil
}

// OpenOutputs：进程型 provider 不支持 exec 环境（没有宿主侧 /out），存在的环境一律报错。
func (p *Provider) OpenOutputs(_ context.Context, envID string, _ int) ([]provider.OutputFile, []provider.SkippedOutput, error) {
	if err := checkName(envID); err != nil {
		return nil, nil, err
	}
	own, _, err := p.readOwner(envID)
	if err != nil {
		return nil, nil, err
	}
	if own != dirOwned {
		return nil, nil, provider.ErrNotFound
	}
	return nil, nil, errors.New("procprov: 不支持 exec 环境，没有 /out")
}

// ---- 测试注入 ----

// BlockStart 使 envID 上的下一次 StartExec 在通过闸门之后、启动进程之前阻塞；blocked 在阻塞时关闭。
func (p *Provider) BlockStart(envID string) (blocked <-chan struct{}, release func()) {
	b := &startBlock{blocked: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	p.block[envID] = b
	p.mu.Unlock()
	return b.blocked, sync.OnceFunc(func() { close(b.release) })
}

// ---- 进程记录与 /proc 扫描 ----

type procID struct {
	Pid   int    `json:"pid"`
	Start uint64 `json:"start"` // /proc/<pid>/stat 第 22 项（启动时间，时钟滴答）
}

type procRecord struct {
	ExecID string   `json:"exec_id"`
	Pgid   int      `json:"pgid"`
	Procs  []procID `json:"procs"`
}

// startTime 读取存活（非僵尸）进程的启动时间。
func startTime(pid int) (uint64, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	i := bytes.LastIndexByte(b, ')') // comm 可能含空格与括号
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(string(b[i+1:]))
	// f[0] 是第 3 项（state），第 22 项是 f[19]。
	if len(f) < 20 || f[0] == "Z" || f[0] == "X" {
		return 0, false
	}
	s, err := strconv.ParseUint(f[19], 10, 64)
	return s, err == nil
}

type markedProc struct {
	pid int
	dir string
}

// Marked 返回标记为环境目录 dir 的存活进程。
func Marked(dir string) ([]int, error) {
	ps, err := markedUnder(dir)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, mp := range ps {
		if mp.dir == dir {
			out = append(out, mp.pid)
		}
	}
	return out, nil
}

// LiveUnder 返回数据目录 dataDir 下任何环境的存活进程（测试检查泄漏用）。
func LiveUnder(dataDir string) ([]int, error) {
	ps, err := markedUnder(filepath.Join(dataDir, "envs") + "/")
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, len(ps))
	for _, mp := range ps {
		out = append(out, mp.pid)
	}
	return out, nil
}

// markedUnder 扫描 /proc，返回标记值以 prefix 开头的存活进程。读不到 environ 的进程（其他用户、已退出、
// 僵尸）跳过：僵尸已没有执行树。
func markedUnder(prefix string) ([]markedProc, error) {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("procprov: 读取 /proc: %w", err)
	}
	key := []byte(MarkerEnv + "=")
	self := os.Getpid()
	var out []markedProc
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err != nil || len(b) == 0 {
			continue
		}
		for _, kv := range bytes.Split(b, []byte{0}) {
			if v, ok := bytes.CutPrefix(kv, key); ok && strings.HasPrefix(string(v), prefix) {
				if _, alive := startTime(pid); alive {
					out = append(out, markedProc{pid: pid, dir: string(v)})
				}
				break
			}
		}
	}
	return out, nil
}

// ---- 日志 ----

// JournalEntry 是 <data>/procprov.journal 的一行。
type JournalEntry struct {
	Event  string `json:"event"` // start | exit | stopped
	EnvID  string `json:"env_id"`
	ExecID string `json:"exec_id,omitempty"`
	Pids   []int  `json:"pids,omitempty"`
	TimeNs int64  `json:"t_ns"`
}

type journalEntry = JournalEntry

func (p *Provider) journal(e JournalEntry) {
	e.TimeNs = time.Now().UnixNano()
	b, _ := json.Marshal(e)
	f, err := os.OpenFile(filepath.Join(p.opt.DataDir, "procprov.journal"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
}

// ReadJournal 读取数据目录 dataDir 的执行日志。
func ReadJournal(dataDir string) ([]JournalEntry, error) {
	b, err := os.ReadFile(filepath.Join(dataDir, "procprov.journal"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var out []JournalEntry
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var e JournalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
