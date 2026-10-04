// Package fake 是 provider.Provider 的内存实现，只供测试使用（cmd/agentbox 不得导入，archtest 检查）。
//
// 它实现契约的全部语义（Create 的四种结果、执行闸门与仲裁顺序、Stop 的确认、Destroy 的前置条件），
// 执行由测试注入的 Program 驱动：Program 可以是纯 Go 函数，也可以在宿主上启动真实进程（不隔离）。
package fake

import (
	"context"
	"io"
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

	mu   sync.Mutex
	envs map[string]*env
}

type env struct {
	spec     provider.EnvSpec
	hash     string
	complete bool // 各层齐全且 init 就绪
	foreign  bool // 无法证明属于本安装
	open     bool // 执行闸门
	stopped  bool // Stop 的权威检查已成立
	running  map[*handle]struct{}
	diag     provider.ResourceDiag
	block    *startBlock
	stopErr  error // 注入：Stop 返回该错误
}

type startBlock struct {
	blocked chan struct{}
	release chan struct{}
}

// New 返回以 prog 执行 workload 的 fake provider。
func New(prog Program) *Provider {
	return &Provider{prog: prog, envs: make(map[string]*env)}
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
	p.envs[spec.EnvID] = e
	return p.info(e), nil
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
	delete(p.envs, envID)
	return nil
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

// ---- 故障注入 ----

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
	return provider.EnvInfo{EnvID: e.spec.EnvID, Kind: e.spec.Kind, Complete: e.complete && e.open, Running: len(e.running) > 0}
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
