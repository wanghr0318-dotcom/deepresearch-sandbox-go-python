// Package fake 是 provider.Provider 的内存实现，只供测试使用（cmd/agentbox 不得导入，archtest 检查）。
//
// 它实现契约的全部语义（Create 的四种结果、执行闸门与仲裁顺序、Stop 的确认、Destroy 的前置条件），
// 执行由测试注入的 Program 驱动：Program 可以是纯 Go 函数，也可以在宿主上启动真实进程（不隔离）。
package fake

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// Program 是一次执行的行为。它必须在 ctx 结束时尽快返回（Terminate 与 Stop 通过取消 ctx 终止执行）。
type Program func(ctx context.Context, spec provider.ExecSpec, stdin io.Reader, stdout, stderr io.Writer) provider.ExitStatus

// Provider 是内存中的 provider。
type Provider struct {
	prog Program

	mu         sync.Mutex
	envs       map[string]*env
	files      map[string]uidFile // PlantUIDFile 植入的文件属主（UID 范围回收与核查，M4 Plan 15 Task 5）
	reclaimErr error              // FailReclaim 注入，一次性
}

type env struct {
	spec      provider.EnvSpec
	hash      string
	complete  bool // 各层齐全且 init 就绪
	foreign   bool // 无法证明属于本安装
	open      bool // 执行闸门
	stopped   bool // Stop 的权威检查已成立
	running   map[*handle]struct{}
	diag      provider.ResourceDiag
	block     *startBlock
	stopErr   error  // 注入：Stop 返回该错误
	dir       string // 仅 exec：临时目录，其下 in/（Mounts.In 为空时）与 out/ 模拟 /in 暂存与宿主侧 /out
	frozen    bool   // Freeze 已确认、尚未 Thaw
	freezeErr error  // 注入：Freeze 返回该错误（且不冻结）
	procs     []int  // SetProcs 设置的 Procs 结果
}

type startBlock struct {
	blocked chan struct{}
	release chan struct{}
}

// New 返回以 prog 执行 workload 的 fake provider。
func New(prog Program) *Provider {
	return &Provider{prog: prog, envs: make(map[string]*env), files: make(map[string]uidFile)}
}

// Create 实现契约第 3 节的 Create。
func (p *Provider) Create(_ context.Context, spec provider.EnvSpec) (provider.EnvInfo, error) {
	if err := spec.Validate(); err != nil {
		return provider.EnvInfo{}, err
	}
	hash := provider.SpecHash(spec)
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.envs[spec.EnvID]; ok {
		switch {
		case e.foreign:
			return provider.EnvInfo{}, provider.ErrForeign
		case e.hash != hash:
			return provider.EnvInfo{}, provider.ErrConflict
		case !e.complete || !e.open:
			return provider.EnvInfo{}, provider.ErrIncomplete
		}
		return p.info(e), nil
	}
	e := &env{spec: spec, hash: hash, complete: true, open: true, running: make(map[*handle]struct{})}
	if spec.Kind == provider.KindExec {
		dir, err := makeExecDir(spec)
		if err != nil {
			return provider.EnvInfo{}, err
		}
		e.dir = dir
	}
	p.envs[spec.EnvID] = e
	return p.info(e), nil
}

// makeExecDir 建立 exec 环境的临时目录：out/ 总是建立，in/ 只在调用方未提供 Mounts.In 时建立。
func makeExecDir(spec provider.EnvSpec) (string, error) {
	dir, err := os.MkdirTemp("", "agentbox-fake-exec-")
	if err != nil {
		return "", fmt.Errorf("fake: exec 环境目录: %w", err)
	}
	sub := []string{"out"}
	if spec.Mounts.In == "" {
		sub = append(sub, "in")
	}
	for _, s := range sub {
		if err := os.Mkdir(filepath.Join(dir, s), 0o755); err != nil {
			_ = os.RemoveAll(dir) // 尽力清理；Create 已失败
			return "", fmt.Errorf("fake: exec 环境目录: %w", err)
		}
	}
	return dir, nil
}

// StartExec 实现契约第 3 节与第 5 节：闸门锁内检查并登记在途启动，"发送"在锁外。
func (p *Provider) StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	p.mu.Lock()
	e, ok := p.envs[envID]
	switch {
	case !ok || e.foreign || !e.complete: // 没有可用的 init
		p.mu.Unlock()
		return nil, provider.ErrNotFound
	case !e.open:
		p.mu.Unlock()
		return nil, provider.ErrStopping
	}
	block := e.block
	e.block = nil
	p.mu.Unlock()

	if block != nil { // 在途启动：已通过闸门，尚未"发送 start"
		close(block.blocked)
		select {
		case <-block.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if !e.open { // Stop 已在途启动期间杀死 init：控制连接在确认前断开
		return nil, provider.ErrControlLost
	}
	h := newHandle(p, e)
	e.running[h] = struct{}{}
	go h.run(p.prog, spec)
	return h, nil
}

// Stop 关闭闸门、终止全部执行并确认清空（契约第 3 节）。不等待在途启动。
func (p *Provider) Stop(ctx context.Context, envID string) error {
	p.mu.Lock()
	e, ok := p.envs[envID]
	if !ok {
		p.mu.Unlock()
		return nil // 从未创建：执行树不存在
	}
	if e.foreign {
		p.mu.Unlock()
		return provider.ErrForeign
	}
	e.open = false
	if e.stopErr != nil {
		err := e.stopErr
		p.mu.Unlock()
		return err
	}
	handles := make([]*handle, 0, len(e.running))
	for h := range e.running {
		handles = append(handles, h)
	}
	p.mu.Unlock()
	for _, h := range handles {
		h.kill()
	}
	for _, h := range handles {
		select {
		case <-h.done:
		case <-ctx.Done():
			return provider.ErrStopUnconfirmed
		}
	}
	p.mu.Lock()
	e.stopped = true
	p.mu.Unlock()
	return nil
}

// Destroy 要求 Stop 的权威检查成立，然后删除环境（契约第 3 节）。外来资源不删除。
func (p *Provider) Destroy(_ context.Context, envID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.envs[envID]
	switch {
	case !ok:
		return nil
	case e.foreign:
		return provider.ErrForeign
	case !e.stopped: // 前置条件只能是 Stop 的权威检查成立，"当前没有进程"不算
		return provider.ErrNotStopped
	}
	if e.dir != "" {
		if err := os.RemoveAll(e.dir); err != nil {
			return fmt.Errorf("fake: 删除 exec 环境目录: %w", err)
		}
	}
	delete(p.envs, envID)
	return nil
}

// OutDir 返回 exec 环境模拟 /out 的宿主目录（测试的 Program 在其中写输出）；非 exec 或不存在时为空。
func (p *Provider) OutDir(envID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.envs[envID]; ok && e.dir != "" {
		return filepath.Join(e.dir, "out")
	}
	return ""
}

// OpenOutputs 实现契约：要求 Stop 的权威检查成立；按路径字典序收集 out/ 下的普通文件，至多 max 个；
// 符号链接、非普通文件与超额的普通文件记入跳过列表（分类与 provider/local 相同，但不做 openat2 级别的防护：
// fake 只供测试，没有不可信的写入者）。
func (p *Provider) OpenOutputs(ctx context.Context, envID string, max int) ([]provider.OutputFile, []provider.SkippedOutput, error) {
	p.mu.Lock()
	e, ok := p.envs[envID]
	var dir string
	var stopped, foreign bool
	if ok {
		dir, stopped, foreign = e.dir, e.stopped, e.foreign
	}
	p.mu.Unlock()
	switch {
	case !ok:
		return nil, nil, provider.ErrNotFound
	case foreign:
		return nil, nil, provider.ErrForeign
	case dir == "":
		return nil, nil, errors.New("fake: 不是 exec 环境，没有 /out")
	case !stopped:
		return nil, nil, provider.ErrNotStopped
	}
	out := filepath.Join(dir, "out")
	type entry struct {
		rel  string
		mode fs.FileMode
	}
	var ents []entry
	err := filepath.WalkDir(out, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return ctx.Err()
		}
		rel, err := filepath.Rel(out, path)
		if err != nil {
			return err
		}
		ents = append(ents, entry{filepath.ToSlash(rel), d.Type()})
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].rel < ents[j].rel })
	var files []provider.OutputFile
	var skipped []provider.SkippedOutput
	skip := func(rel, reason string) {
		skipped = append(skipped, provider.SkippedOutput{Path: rel, Reason: reason})
	}
	for _, en := range ents {
		switch {
		case en.mode&fs.ModeSymlink != 0:
			skip(en.rel, provider.SkipSymlink)
		case !en.mode.IsRegular():
			skip(en.rel, provider.SkipNotRegular)
		case len(files) >= max:
			skip(en.rel, provider.SkipTooMany)
		default:
			f, err := os.Open(filepath.Join(out, filepath.FromSlash(en.rel)))
			if err != nil {
				skip(en.rel, provider.SkipOpenFailed)
				continue
			}
			fi, err := f.Stat()
			if err != nil {
				_ = f.Close() // 已按 open_failed 跳过，关闭错误无关紧要
				skip(en.rel, provider.SkipOpenFailed)
				continue
			}
			files = append(files, provider.OutputFile{Path: en.rel, Size: fi.Size(), File: f})
		}
	}
	return files, skipped, nil
}

// List 报告本安装的环境。
func (p *Provider) List(context.Context) ([]provider.EnvInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []provider.EnvInfo
	for _, e := range p.envs {
		if !e.foreign {
			out = append(out, p.info(e))
		}
	}
	return out, nil
}

// Scan 逐个环境报告归属。
func (p *Provider) Scan(context.Context) (provider.ScanReport, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var r provider.ScanReport
	for id, e := range p.envs {
		owner := provider.OwnedComplete
		switch {
		case e.foreign:
			owner = provider.Unknown
		case !e.complete || !e.open:
			owner = provider.OwnedPartial
		}
		r.Items = append(r.Items, provider.ScanItem{Layer: "env_dir", Path: "envs/" + id, EnvID: id, Owner: owner})
	}
	return r, nil
}

// ResourceDiag 返回测试设置的诊断。
func (p *Provider) ResourceDiag(_ context.Context, envID string) (provider.ResourceDiag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.envs[envID]
	if !ok || e.foreign {
		return provider.ResourceDiag{}, provider.ErrNotFound
	}
	return e.diag, nil
}

// Freeze 记录环境已冻结（契约第 3 节修订）。不存在或外来 → ErrNotFound；FailFreeze 注入的错误原样返回且不冻结
// （与 local 的"未确认即尝试解冻"一致）。冻结不影响 Stop。
func (p *Provider) Freeze(_ context.Context, envID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.envs[envID]
	switch {
	case !ok || e.foreign:
		return provider.ErrNotFound
	case e.freezeErr != nil:
		return e.freezeErr
	}
	e.frozen = true
	return nil
}

// Thaw 清除冻结标记。不存在或外来 → ErrNotFound。
func (p *Provider) Thaw(_ context.Context, envID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.envs[envID]
	if !ok || e.foreign {
		return provider.ErrNotFound
	}
	e.frozen = false
	return nil
}

// Procs 返回 SetProcs 设置的 pid（默认为空）。不存在或外来 → ErrNotFound。
func (p *Provider) Procs(_ context.Context, envID string) ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.envs[envID]
	if !ok || e.foreign {
		return nil, provider.ErrNotFound
	}
	return append([]int(nil), e.procs...), nil
}

// Frozen 报告 envID 是否处于冻结状态。
func (p *Provider) Frozen(envID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.envs[envID]
	return ok && e.frozen
}

// uidFile 是 PlantUIDFile 植入的一个文件属主事实。
type uidFile struct {
	uid       uint32
	workspace bool // 位于 <data>/workspaces 之下（ReclaimUIDFiles 只改这些）
}

// ReclaimUIDFiles 把属主落在范围内的 workspace 文件改回 0（模拟 chown 0:0），返回改动数量；FailReclaim 注入的
// 错误只返回一次。
func (p *Provider) ReclaimUIDFiles(_ context.Context, base, size uint32) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.reclaimErr; err != nil {
		p.reclaimErr = nil
		return 0, err
	}
	n := 0
	for path, f := range p.files {
		if f.workspace && inSpan(f.uid, base, size) {
			p.files[path] = uidFile{uid: 0, workspace: true}
			n++
		}
	}
	return n, nil
}

// UIDFiles 返回属主落在范围内的植入文件（按路径排序，至多 limit 个）。
func (p *Provider) UIDFiles(_ context.Context, base, size uint32, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("fake: UIDFiles 的上限 %d 必须为正", limit)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for path, f := range p.files {
		if inSpan(f.uid, base, size) {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func inSpan(uid, base, size uint32) bool {
	return uint64(uid) >= uint64(base) && uint64(uid) < uint64(base)+uint64(size)
}

// PlantUIDFile 植入一个属主为 uid 的文件（workspace 为真表示位于 <data>/workspaces 之下）。
func (p *Provider) PlantUIDFile(path string, uid uint32, workspace bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.files[path] = uidFile{uid: uid, workspace: workspace}
}

// UIDFileOwner 返回植入文件当前的属主。
func (p *Provider) UIDFileOwner(path string) (uint32, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.files[path]
	return f.uid, ok
}

// ---- 故障注入 ----

// FailReclaim 使下一次 ReclaimUIDFiles 返回 err。
func (p *Provider) FailReclaim(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reclaimErr = err
}

// FailFreeze 使 envID 上的 Freeze 返回 err（例如包装 provider.ErrFreezeUnconfirmed）；nil 取消注入。
func (p *Provider) FailFreeze(envID string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.envs[envID]; ok {
		e.freezeErr = err
	}
}

// SetProcs 设置 envID 的 Procs 结果（释放核验的进程基线测试）。
func (p *Provider) SetProcs(envID string, pids []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.envs[envID]; ok {
		e.procs = append([]int(nil), pids...)
	}
}

// InjectResidue 使 spec 对应的环境成为属于本安装但不完整的残留。
func (p *Provider) InjectResidue(spec provider.EnvSpec) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.envs[spec.EnvID] = &env{spec: spec, hash: provider.SpecHash(spec), running: make(map[*handle]struct{})}
}

// InjectForeign 使 envID 成为无法证明属于本安装的资源。
func (p *Provider) InjectForeign(envID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.envs[envID] = &env{foreign: true, running: make(map[*handle]struct{})}
}

// BlockStart 使 envID 上的下一次 StartExec 在通过闸门之后、"发送 start"之前阻塞。
func (p *Provider) BlockStart(envID string) (blocked <-chan struct{}, release func()) {
	b := &startBlock{blocked: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	if e, ok := p.envs[envID]; ok {
		e.block = b
	}
	p.mu.Unlock()
	var once sync.Once
	return b.blocked, func() { once.Do(func() { close(b.release) }) }
}

// FailStop 使 envID 上的 Stop 返回 err（例如 provider.ErrStopUnconfirmed）。
func (p *Provider) FailStop(envID string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.envs[envID]; ok {
		e.stopErr = err
	}
}

// SetDiag 设置 envID 的资源诊断。
func (p *Provider) SetDiag(envID string, d provider.ResourceDiag) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.envs[envID]; ok {
		e.diag = d
	}
}

func (p *Provider) info(e *env) provider.EnvInfo {
	info := provider.EnvInfo{EnvID: e.spec.EnvID, Kind: e.spec.Kind, Complete: e.complete && e.open, Running: len(e.running) > 0}
	if e.dir != "" && e.spec.Mounts.In == "" {
		info.InDir = filepath.Join(e.dir, "in")
	}
	return info
}

// ---- 执行句柄 ----

type handle struct {
	p       *Provider
	e       *env
	ctx     context.Context
	cancel  context.CancelFunc
	stdinR  *io.PipeReader
	stdinW  *io.PipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter
	done    chan struct{}
	status  provider.ExitStatus
	killed  bool
	mu      sync.Mutex
}

func newHandle(p *Provider, e *env) *handle {
	h := &handle{p: p, e: e, done: make(chan struct{})}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	h.stdinR, h.stdinW = io.Pipe()
	h.stdoutR, h.stdoutW = io.Pipe()
	h.stderrR, h.stderrW = io.Pipe()
	return h
}

func (h *handle) run(prog Program, spec provider.ExecSpec) {
	status := prog(h.ctx, spec, h.stdinR, h.stdoutW, h.stderrW)
	h.mu.Lock()
	if h.killed && status.Signal == 0 {
		status = provider.ExitStatus{Signal: syscall.SIGKILL}
	}
	h.status = status
	h.mu.Unlock()
	_ = h.stdoutW.Close()
	_ = h.stderrW.Close()
	_ = h.stdinR.Close()
	h.p.mu.Lock()
	delete(h.e.running, h)
	h.p.mu.Unlock()
	close(h.done)
}

func (h *handle) kill() {
	h.mu.Lock()
	h.killed = true
	h.mu.Unlock()
	h.cancel()
}

func (h *handle) Stdin() io.WriteCloser { return h.stdinW }
func (h *handle) Stdout() io.ReadCloser { return h.stdoutR }
func (h *handle) Stderr() io.ReadCloser { return h.stderrR }

// Wait 返回退出状态；被终止的执行以 SIGKILL 状态结束（不是错误）。
func (h *handle) Wait() (provider.ExitStatus, error) {
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status, nil
}

// Terminate 取消执行；Program 须在 ctx 结束时返回（grace 对内存程序没有意义）。
func (h *handle) Terminate(time.Duration) error {
	h.kill()
	return nil
}
