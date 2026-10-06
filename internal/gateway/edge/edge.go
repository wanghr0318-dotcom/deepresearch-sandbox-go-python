// Package edge 是 Gateway 面向 Worker 的入口（规格 §9.1–§9.3）：独立 task 每个 attempt 一个 Unix socket，
// session 每个 incarnation 一个 Unix socket；其上是 HTTP/1.1 服务，连接在接受时绑定到该入口当前的
// attempt；端点经 Calls 交给 call 协调器处理。
//
// edge 不判定权限：每个请求的访问检查（§9.2）在 Calls.Invoke / CheckAccess 内部的一致快照中完成；
// Revoke/Detach 被调用时 attempt_access 已是 revoked（由 actor 保证顺序），edge 只做清理（关闭连接，
// 独立 task 同时关闭 listener 并删除 socket），并把离开原因转交 Calls.CancelAttempt——上游 try 是否
// 取消只按原因决定（§9.1 表）。
//
// 本包不导入 internal/persistence，只依赖下方声明的窄接口 Calls 与 Attempts。
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/subrun"
)

// Calls 是 edge 使用的 call 协调器子集（*call.Coordinator 满足它）。
type Calls interface {
	Invoke(ctx context.Context, in call.Invoke) (call.Result, error)
	// CheckAccess 的 subrunID 来自 X-Agentbox-Subrun（空为 root）：非空时另查该 sub-run 是否可用（409 subrun_closed）。
	CheckAccess(ctx context.Context, taskID, attemptID, subrunID string) (call.Result, error)
	Budget(ctx context.Context, taskID string) (call.Budget, error)
	// SubrunBudget 返回 sub-run 层账本（/v1/budget 带 X-Agentbox-Subrun 时附加）。
	SubrunBudget(ctx context.Context, taskID, subrunID string) (call.SubrunBudget, error)
	OpenBlob(ctx context.Context, taskID, sha string) (io.ReadCloser, error)
	CancelAttempt(attemptID, reason string)
	// Exec 处理 POST /v1/exec（§10）；拒绝与失败以 Result 的 Status/Code 返回。
	Exec(ctx context.Context, in call.ExecInvoke) (call.Result, error)
	// ExecQuota 返回任务的 exec 配额（bool 为是否已有配额行；没有时为策略值、用量 0）；未配置 exec 时为
	// call.ErrExecNotConfigured，/v1/budget 此时不输出 exec。
	ExecQuota(ctx context.Context, taskID string) (call.ExecQuota, bool, error)
}

// Attempts 把 attempt 映射到它所属的任务与环境（由 Task 5 以 task.Store.LookupAttempt 实现）。
type Attempts interface {
	Lookup(ctx context.Context, attemptID string) (taskID, envID string, err error)
}

// Config 配置 Edge。零值字段取 §19 默认值。
type Config struct {
	SocketDir string       // <data>/gateway；socket 为 <SocketDir>/<attempt_id>.sock 或 inc-<incarnation_id>.sock
	MaxConns  int          // 每 attempt 连接上限，默认 16
	MaxActive int          // 每 attempt 活跃请求上限，默认 32
	MaxBody   int64        // 请求体上限（字节），默认 4 MiB
	Logger    *slog.Logger // nil 时不输出
}

// 默认值（§19）。
const (
	DefaultMaxConns  = 16
	DefaultMaxActive = 32
	DefaultMaxBody   = 4 << 20
	// MaxCallIDBytes 是 X-Agentbox-Call-Id 的最大长度（§9.3）。
	MaxCallIDBytes = 256
)

// edge 自身产生的错误码（其余原样来自 call.Result.Code）。
const (
	CodeMissingCallID      = "missing_call_id"
	CodeNotImplemented     = "not_implemented"
	CodeTooManyRequests    = "too_many_requests"
	CodeRequestTooLarge    = "request_too_large"
	CodeInvalidRequest     = "invalid_request"
	CodeNotFound           = "not_found"
	CodeMethodNotAllowed   = "method_not_allowed"
	CodeInternal           = "internal_error"
	CodeGatewayUnavailable = "gateway_unavailable"
)

// 请求与响应头（§9.3）。
const (
	HeaderCallID          = "X-Agentbox-Call-Id"
	HeaderRetry           = "X-Agentbox-Retry"
	HeaderSupersedes      = "X-Agentbox-Supersedes"
	HeaderSupersedeReason = "X-Agentbox-Supersede-Reason"
	HeaderSubrun          = "X-Agentbox-Subrun"
	HeaderBlob            = "X-Agentbox-Blob"
	HeaderReplayed        = "X-Agentbox-Replayed"
	// HeaderCache 是缓存指令（§11.4）：唯一接受的取值是 no-cache（不读缓存，结果仍写入；计入指纹）。
	HeaderCache = "X-Agentbox-Cache"
	// HeaderToolBudget 是搜索与抓取响应（含 429 与重放）的工具调用额度 "<used>/<limit>"；任务不限时不写（契约 E）。
	HeaderToolBudget = "X-Agentbox-Tool-Budget"
)

// incarnationSocketPrefix 是 incarnation socket 文件名的前缀：<SocketDir>/inc-<incarnation_id>.sock。
const incarnationSocketPrefix = "inc-"

// ErrClosed 表示 Edge 已关闭。
var ErrClosed = errors.New("edge: 已关闭")

// ErrAlreadyBound 表示该 attempt 已有入口，或该 incarnation 已有入口 / 已有 Attach 的 attempt。
var ErrAlreadyBound = errors.New("edge: attempt 已绑定")

// ErrNotBound 表示 incarnation 没有入口（未 BindIncarnation 或已撤销）。
var ErrNotBound = errors.New("edge: incarnation 未绑定")

var (
	shaPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// idPattern 约束用作 socket 文件名的 attempt_id 与 incarnation_id。
	idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

// billable 是计费端点到调用类别的映射（M2 范围）。
var billable = map[string]upstream.Kind{
	"/v1/chat/completions": upstream.KindChat,
	"/v1/search":           upstream.KindSearch,
	"/v1/fetch":            upstream.KindFetch,
}

// Edge 管理每个 attempt / incarnation 的 listener 与连接集合。并发安全。
type Edge struct {
	cfg      Config
	calls    Calls
	attempts Attempts
	log      *slog.Logger

	mu       sync.Mutex
	closed   bool
	bindings map[string]*binding // attempt_id → 独立 task 的入口
	incs     map[string]*binding // incarnation_id → session 的入口

	// connStateHook 是只供测试的观察点（生产中为 nil）：连接状态变化时在 edge 已更新自身连接集合之后调用；
	// StateClosed 时该连接已从 attempt 的连接集合中移除。须在 Bind 之前设置。
	connStateHook func(attemptID string, state http.ConnState)
}

// New 创建 Edge。
func New(cfg Config, calls Calls, attempts Attempts) *Edge {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = DefaultMaxConns
	}
	if cfg.MaxActive <= 0 {
		cfg.MaxActive = DefaultMaxActive
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = DefaultMaxBody
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Edge{
		cfg: cfg, calls: calls, attempts: attempts, log: log,
		bindings: make(map[string]*binding), incs: make(map[string]*binding),
	}
}

// SocketPath 返回 attempt 的 socket 路径 <SocketDir>/<attempt_id>.sock。
func (e *Edge) SocketPath(attemptID string) string {
	return filepath.Join(e.cfg.SocketDir, attemptID+".sock")
}

// IncarnationSocketPath 返回 incarnation 的 socket 路径 <SocketDir>/inc-<incarnation_id>.sock。
func (e *Edge) IncarnationSocketPath(incarnationID string) string {
	return filepath.Join(e.cfg.SocketDir, incarnationSocketPrefix+incarnationID+".sock")
}

// Bind 为 attempt 创建 <SocketDir>/<attempt_id>.sock（0600）并启动 HTTP/1.1 服务，返回 socket 路径。
// envID 非空时须与 Attempts.Lookup 给出的环境一致。属主 chown 由 provider 启动器完成。
//
// socket 先在 0700 的私有临时目录中 bind、chmod 0600，再原子 rename 到最终路径：最终路径上的 socket
// 从出现起就是 0600，不依赖进程级 umask（修改 umask 会与其他 goroutine 的文件创建竞争）。
func (e *Edge) Bind(ctx context.Context, attemptID, envID string) (string, error) {
	if !idPattern.MatchString(attemptID) {
		return "", fmt.Errorf("edge: attempt_id %q 不能用作 socket 文件名", attemptID)
	}
	taskID, gotEnv, err := e.attempts.Lookup(ctx, attemptID)
	if err != nil {
		return "", fmt.Errorf("edge: 查找 attempt %s: %w", attemptID, err)
	}
	if envID != "" && gotEnv != envID {
		return "", fmt.Errorf("edge: attempt %s 属于环境 %s，不是 %s", attemptID, gotEnv, envID)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", ErrClosed
	}
	if _, ok := e.bindings[attemptID]; ok {
		return "", fmt.Errorf("%w: %s", ErrAlreadyBound, attemptID)
	}
	path := e.SocketPath(attemptID)
	ln, err := listenPrivate(e.log, e.cfg.SocketDir, path)
	if err != nil {
		return "", fmt.Errorf("edge: 为 attempt %s 建立 socket: %w", attemptID, err)
	}
	b := newBinding(e, "attempt_id", attemptID, gotEnv, path, ln)
	// 独立 task：入口固定绑定这一个 attempt，直到撤销。
	if err := b.ln.attach(newTarget(e, attemptID, taskID, gotEnv)); err != nil {
		return "", errors.Join(err, ln.Close(), os.Remove(path))
	}
	e.bindings[attemptID] = b
	go b.serve()
	e.log.Info("gateway: attempt 入口已绑定", "attempt_id", attemptID, "task_id", taskID, "env_id", gotEnv, "socket", path)
	return path, nil
}

// BindIncarnation 为 session incarnation 建立 <SocketDir>/inc-<incarnation_id>.sock（0600，规则同 Bind）
// 并启动 HTTP/1.1 服务，返回 socket 路径。未 Attach 时拒绝新连接（接受后立即关闭，§9.1"session 空闲时
// 拒绝连接"）。envID 是 incarnation 的环境，之后 Attach 的 attempt 必须属于它。
func (e *Edge) BindIncarnation(ctx context.Context, incarnationID, envID string) (string, error) {
	if !idPattern.MatchString(incarnationID) {
		return "", fmt.Errorf("edge: incarnation_id %q 不能用作 socket 文件名", incarnationID)
	}
	if envID == "" {
		return "", fmt.Errorf("edge: incarnation %s 缺少环境", incarnationID)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", ErrClosed
	}
	if _, ok := e.incs[incarnationID]; ok {
		return "", fmt.Errorf("%w: incarnation %s", ErrAlreadyBound, incarnationID)
	}
	path := e.IncarnationSocketPath(incarnationID)
	ln, err := listenPrivate(e.log, e.cfg.SocketDir, path)
	if err != nil {
		return "", fmt.Errorf("edge: 为 incarnation %s 建立 socket: %w", incarnationID, err)
	}
	b := newBinding(e, "incarnation_id", incarnationID, envID, path, ln)
	e.incs[incarnationID] = b
	go b.serve()
	e.log.Info("gateway: incarnation 入口已绑定", "incarnation_id", incarnationID, "env_id", envID, "socket", path)
	return path, nil
}

// Attach 把 incarnation 入口之后接受的连接绑定到 attemptID（经 Attempts.Lookup 核对其环境 = incarnation
// 的环境）。已有 Attach 的 attempt 时返回 ErrAlreadyBound；incarnation 未绑定或已撤销时返回 ErrNotBound。
// 已接受的连接不改变归属：Attach 之前没有存活连接（空闲时拒绝，Detach 关闭全部）。
func (e *Edge) Attach(ctx context.Context, incarnationID, attemptID string) error {
	b, err := e.incarnation(incarnationID)
	if err != nil {
		return err
	}
	taskID, envID, err := e.attempts.Lookup(ctx, attemptID)
	if err != nil {
		return fmt.Errorf("edge: 查找 attempt %s: %w", attemptID, err)
	}
	if envID != b.envID {
		return fmt.Errorf("edge: attempt %s 属于环境 %s，不是 incarnation %s 的环境 %s", attemptID, envID, incarnationID, b.envID)
	}
	t := newTarget(e, attemptID, taskID, envID)
	if err := b.ln.attach(t); err != nil {
		t.cancel()
		if errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("%w: %s", ErrNotBound, incarnationID)
		}
		return fmt.Errorf("%w: incarnation %s 已绑定 attempt %s", err, incarnationID, b.ln.currentAttempt())
	}
	e.log.Info("gateway: attempt 已接入 incarnation 入口", "incarnation_id", incarnationID, "attempt_id", attemptID, "task_id", taskID)
	return nil
}

// Detach 使 incarnation 入口不再接受属于 attemptID 的连接（之后的新连接被拒绝，直到下一次 Attach），
// 结束面向 Worker 的请求上下文、关闭绑定到 attemptID 的全部连接，并等待其在途请求结束（ctx 为期限，
// 超时返回 ctx 的错误），最后把 reason 转交 Calls.CancelAttempt（只有 call.ReasonCancel 取消在途 try）。
// 幂等：attemptID 不是当前 Attach 的 attempt（已 Detach、或 incarnation 已撤销）时只转交 reason。
func (e *Edge) Detach(ctx context.Context, incarnationID, attemptID, reason string) error {
	var err error
	e.mu.Lock()
	b := e.incs[incarnationID]
	e.mu.Unlock()
	if b != nil {
		if t, conns := b.ln.detach(attemptID); t != nil {
			idle := t.detach()
			t.cancel()
			for _, c := range conns {
				_ = c.Close() // 撤销清理，连接可能已被对端关闭
			}
			select {
			case <-idle:
			case <-ctx.Done():
				err = fmt.Errorf("edge: 等待 attempt %s 的在途请求结束: %w", attemptID, ctx.Err())
			}
			e.log.Info("gateway: attempt 已离开 incarnation 入口", "incarnation_id", incarnationID, "attempt_id", attemptID, "reason", reason)
		}
	}
	e.calls.CancelAttempt(attemptID, reason)
	return err
}

// RevokeIncarnation 关闭 incarnation 的 listener 与全部连接、删除 socket（幂等）。它不调用
// CancelAttempt：attempt 的离开原因经 Detach 转交，调用方应在撤销 incarnation 之前 Detach 当前 attempt。
func (e *Edge) RevokeIncarnation(_ context.Context, incarnationID string) error {
	e.mu.Lock()
	b := e.incs[incarnationID]
	delete(e.incs, incarnationID)
	e.mu.Unlock()

	if b != nil {
		b.shutdown()
		e.log.Info("gateway: incarnation 入口已撤销", "incarnation_id", incarnationID)
	}
	if idPattern.MatchString(incarnationID) {
		if err := os.Remove(e.IncarnationSocketPath(incarnationID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("edge: 删除 incarnation %s 的 socket: %w", incarnationID, err)
		}
	}
	return nil
}

func (e *Edge) incarnation(incarnationID string) (*binding, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrClosed
	}
	b := e.incs[incarnationID]
	if b == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotBound, incarnationID)
	}
	return b, nil
}

// listenPrivate 在 dir 内的 0700 临时目录中 bind，chmod 0600 后 rename 到 path。
func listenPrivate(log *slog.Logger, dir, path string) (*net.UnixListener, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	tmpDir, err := os.MkdirTemp(dir, ".bind-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if rerr := os.RemoveAll(tmpDir); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			log.Warn("gateway: 删除临时 bind 目录失败", "dir", tmpDir, "err", rerr)
		}
	}()
	tmp := filepath.Join(tmpDir, "s")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: tmp, Net: "unix"})
	if err != nil {
		return nil, err
	}
	// 文件路径在 rename 后改变；删除由 Revoke 按最终路径完成。
	ln.SetUnlinkOnClose(false)
	if err := os.Chmod(tmp, 0o600); err != nil {
		return nil, errors.Join(err, ln.Close())
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, errors.Join(err, ln.Close())
	}
	return ln, nil
}

// Revoke 关闭该 attempt 的全部连接与 listener、删除 socket 文件，并把 reason 转交 Calls.CancelAttempt
// （只有 call.ReasonCancel 取消在途 try）。未绑定的 attempt 也会删除残留 socket 并转交 reason（幂等）。
func (e *Edge) Revoke(ctx context.Context, attemptID string, reason string) error {
	e.mu.Lock()
	b := e.bindings[attemptID]
	delete(e.bindings, attemptID)
	e.mu.Unlock()

	var err error
	if b != nil {
		b.shutdown()
		e.log.Info("gateway: attempt 入口已撤销", "attempt_id", attemptID, "reason", reason)
	}
	if idPattern.MatchString(attemptID) {
		if rerr := os.Remove(e.SocketPath(attemptID)); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			err = fmt.Errorf("edge: 删除 attempt %s 的 socket: %w", attemptID, rerr)
		}
	}
	e.calls.CancelAttempt(attemptID, reason)
	return err
}

// Close 关闭全部入口（attempt 与 incarnation）并删除其 socket 文件；之后 Bind/BindIncarnation 返回
// ErrClosed。Close 不调用 CancelAttempt：Gateway 关闭时在途 try 的处理由 call 协调器自身的 Close 决定。
func (e *Edge) Close() error {
	e.mu.Lock()
	e.closed = true
	bs := make([]*binding, 0, len(e.bindings)+len(e.incs))
	for _, b := range e.bindings {
		bs = append(bs, b)
	}
	for _, b := range e.incs {
		bs = append(bs, b)
	}
	e.bindings = make(map[string]*binding)
	e.incs = make(map[string]*binding)
	e.mu.Unlock()

	var errs []error
	for _, b := range bs {
		b.shutdown()
		if err := os.Remove(b.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("edge: 删除 %s %s 的 socket: %w", b.idKey, b.id, err))
		}
	}
	return errors.Join(errs...)
}

// binding 是一个入口：listener、HTTP 服务与连接集合。独立 task 的入口固定绑定一个 attempt；
// incarnation 的入口在 Attach/Detach 之间切换当前 attempt（见 limitListener.cur）。
type binding struct {
	e         *Edge
	idKey, id string // 日志用："attempt_id"/"incarnation_id" 与其取值
	envID     string // 入口所属环境
	path      string
	ln        *limitListener
	srv       *http.Server
	served    chan struct{}
}

func newBinding(e *Edge, idKey, id, envID, path string, ul *net.UnixListener) *binding {
	b := &binding{e: e, idKey: idKey, id: id, envID: envID, path: path, served: make(chan struct{})}
	hook := e.connStateHook
	b.ln = &limitListener{Listener: ul, max: e.cfg.MaxConns, conns: make(map[*trackedConn]struct{})}
	if hook != nil {
		b.ln.onRemove = func(attemptID string) { hook(attemptID, http.StateClosed) }
	}
	b.srv = &http.Server{
		Handler:           b,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          slog.NewLogLogger(e.log.Handler(), slog.LevelDebug),
		// 处理函数经请求上下文取得自己的连接，从而取得连接接受时绑定的 attempt（target），
		// 并在 Worker 断开时立即释放连接名额（见 invoke）。
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if tc, ok := c.(*trackedConn); ok {
				return context.WithValue(ctx, connKey{}, tc)
			}
			return ctx
		},
	}
	if hook != nil {
		b.srv.ConnState = func(c net.Conn, s http.ConnState) {
			// StateClosed/StateHijacked 由 trackedConn 在移出连接集合之后报告（onRemove）。
			if tc, ok := c.(*trackedConn); ok && s != http.StateClosed && s != http.StateHijacked {
				hook(tc.t.attemptID, s)
			}
		}
	}
	return b
}

func (b *binding) serve() {
	defer close(b.served)
	if err := b.srv.Serve(b.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		b.e.log.Warn("gateway: 入口服务结束", b.idKey, b.id, "err", err)
	}
}

// shutdown 关闭 listener 与全部连接，并结束当前 attempt 交给 Invoke 的上下文（面向 Worker 的请求随之结束）。
func (b *binding) shutdown() {
	if err := b.srv.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		b.e.log.Debug("gateway: 关闭入口服务", b.idKey, b.id, "err", err)
	}
	b.ln.closeAll()
	<-b.served
}

func (b *binding) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tc, ok := r.Context().Value(connKey{}).(*trackedConn)
	if !ok {
		// 只有经 limitListener 接受的连接会到达这里；防御性拒绝。
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	tc.t.serveHTTP(w, r, tc)
}

// target 是连接在接受时绑定的 attempt：请求以它的 task/attempt/env 调用 Calls。
type target struct {
	e                        *Edge
	attemptID, taskID, envID string

	// ctx 是交给 Calls.Invoke 的上下文：它只在撤销、Detach 或关闭时结束，与 Worker 的请求上下文分离——
	// Worker 断开连接或取消 HTTP 请求不会取消 Invoke（§9.1 表第一行；协调器的后台 try 本来也不随
	// Invoke 的 ctx 取消，这里在 edge 一侧显式保证）。
	ctx    context.Context
	cancel context.CancelFunc

	conns int // 已接受且未关闭的连接数，由 limitListener.mu 保护

	mu       sync.Mutex
	active   int           // 处理中的请求数（含超限被拒的瞬时计数）
	detached bool          // Detach 之后：新请求被拒绝
	idle     chan struct{} // detached 且 active 降为 0 时关闭
}

func newTarget(e *Edge, attemptID, taskID, envID string) *target {
	t := &target{e: e, attemptID: attemptID, taskID: taskID, envID: envID, idle: make(chan struct{})}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t
}

// enter 登记一个请求。detached 时 ok=false 且 tooMany=false。
func (t *target) enter() (ok, tooMany bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached {
		return false, false
	}
	if t.active >= t.e.cfg.MaxActive {
		return false, true
	}
	t.active++
	return true, false
}

func (t *target) exit() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active--
	if t.detached && t.active == 0 {
		close(t.idle)
	}
}

// detach 拒绝之后的请求，返回在途请求全部结束时关闭的通道。只调用一次（limitListener.detach 保证）。
func (t *target) detach() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.detached = true
	if t.active == 0 {
		close(t.idle)
	}
	return t.idle
}

func (t *target) serveHTTP(w http.ResponseWriter, r *http.Request, tc *trackedConn) {
	if ok, tooMany := t.enter(); !ok {
		if tooMany {
			t.writeError(w, http.StatusTooManyRequests, CodeTooManyRequests, "活跃请求超过上限")
		} else {
			t.writeError(w, http.StatusServiceUnavailable, CodeGatewayUnavailable, "入口已撤销")
		}
		return
	}
	defer t.exit()

	// X-Agentbox-Subrun（§9.2、§9.3）：非空时须满足 sub-run ID 规则；归属（属于本任务、绑定本 attempt、started）
	// 由 Calls 在访问检查中判定。计费端点另要求 call id 以 <subrun_id>/ 开头（invoke）；只读端点（/v1/budget、
	// /blobs）不要求 call id。
	subrunID := r.Header.Get(HeaderSubrun)
	if subrunID != "" && !subrun.ValidID(subrunID) {
		t.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "X-Agentbox-Subrun 不是合法的 sub-run ID")
		return
	}
	path := r.URL.Path
	switch {
	case path == call.ExecEndpoint:
		if r.Method != http.MethodPost {
			t.methodNotAllowed(w, http.MethodPost)
			return
		}
		t.exec(w, r, tc, subrunID)
	case billable[path] != "":
		if r.Method != http.MethodPost {
			t.methodNotAllowed(w, http.MethodPost)
			return
		}
		t.invoke(w, r, tc, billable[path], subrunID)
	case path == "/v1/budget":
		if r.Method != http.MethodGet {
			t.methodNotAllowed(w, http.MethodGet)
			return
		}
		t.budget(w, r, subrunID)
	case strings.HasPrefix(path, "/blobs/"):
		if r.Method != http.MethodGet {
			t.methodNotAllowed(w, http.MethodGet)
			return
		}
		t.blob(w, r, strings.TrimPrefix(path, "/blobs/"), subrunID)
	default:
		t.writeError(w, http.StatusNotFound, CodeNotFound, "未知端点")
	}
}

// callRequest 读取计费调用与 exec 共用的部分：call id（1–256 字节；带 X-Agentbox-Subrun 时须以 <subrun_id>/
// 开头）与请求体（≤ MaxBody）。失败时已写出错误，ok 为 false。
func (t *target) callRequest(w http.ResponseWriter, r *http.Request, subrunID string) (callID string, body []byte, ok bool) {
	callID = r.Header.Get(HeaderCallID)
	if callID == "" || len(callID) > MaxCallIDBytes {
		t.writeError(w, http.StatusBadRequest, CodeMissingCallID, "计费调用须携带 1–256 字节的 X-Agentbox-Call-Id")
		return "", nil, false
	}
	if prefix := subrunID + "/"; subrunID != "" && (!strings.HasPrefix(callID, prefix) || len(callID) == len(prefix)) {
		t.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "带 X-Agentbox-Subrun 时 X-Agentbox-Call-Id 须以 <subrun_id>/ 开头")
		return "", nil, false
	}
	// 不按 Content-Length 提前拒绝：未读请求体时立即响应会让仍在写入的客户端得到 broken pipe 而非 413。
	// MaxBytesReader 至多读入 MaxBody 字节，超限时响应后关闭连接。
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, t.e.cfg.MaxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			t.writeError(w, http.StatusRequestEntityTooLarge, CodeRequestTooLarge, "请求体超过上限")
			return "", nil, false
		}
		t.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "读取请求体失败")
		return "", nil, false
	}
	return callID, body, true
}

// detached 在 Worker 于响应前断开时立即关闭服务端连接、释放连接名额（net/http 读到 EOF 后取消 r.Context()）；
// 调用本身以 target 的上下文进行、不受影响，响应写入失败即丢弃。返回的 stop 须在处理函数返回前调用，
// 正常结束不会触发。
func detached(r *http.Request, tc *trackedConn) (stop func() bool) {
	return context.AfterFunc(r.Context(), func() {
		_ = tc.Close() // Worker 已断开，关闭错误无关紧要
	})
}

// exec 处理 POST /v1/exec（§10）：头与请求体规则同计费端点（call id、Retry、Supersedes、sub-run 前缀、4 MiB）；
// exec 没有缓存，带 X-Agentbox-Cache 为 400。以 target 的上下文调用 Calls.Exec：Worker 断开不取消 exec。
func (t *target) exec(w http.ResponseWriter, r *http.Request, tc *trackedConn, subrunID string) {
	callID, body, ok := t.callRequest(w, r, subrunID)
	if !ok {
		return
	}
	if r.Header.Get(HeaderCache) != "" {
		t.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "exec 不接受 X-Agentbox-Cache")
		return
	}
	in := call.ExecInvoke{
		TaskID: t.taskID, AttemptID: t.attemptID, CallID: callID, SubrunID: subrunID, Body: body,
		Retry:           strings.EqualFold(strings.TrimSpace(r.Header.Get(HeaderRetry)), "true"),
		Supersedes:      r.Header.Get(HeaderSupersedes),
		SupersedeReason: r.Header.Get(HeaderSupersedeReason),
	}
	stop := detached(r, tc)
	defer stop()
	res, err := t.e.calls.Exec(t.ctx, in)
	if err != nil {
		t.internalError(w, "exec", err, "call_id", callID)
		return
	}
	t.writeResult(w, callID, res)
}

func (t *target) invoke(w http.ResponseWriter, r *http.Request, tc *trackedConn, kind upstream.Kind, subrunID string) {
	callID, body, ok := t.callRequest(w, r, subrunID)
	if !ok {
		return
	}
	noCache := false
	switch v := strings.TrimSpace(r.Header.Get(HeaderCache)); {
	case v == "":
	case strings.EqualFold(v, call.CacheDirectiveNoCache):
		noCache = true
	default:
		t.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "X-Agentbox-Cache 只接受 no-cache")
		return
	}
	in := call.Invoke{
		TaskID: t.taskID, AttemptID: t.attemptID, EnvID: t.envID,
		CallID: callID, Kind: kind, Body: body,
		Retry:           strings.EqualFold(strings.TrimSpace(r.Header.Get(HeaderRetry)), "true"),
		Supersedes:      r.Header.Get(HeaderSupersedes),
		SupersedeReason: r.Header.Get(HeaderSupersedeReason),
		NoCache:         noCache,
		SubrunID:        subrunID,
	}
	stop := detached(r, tc)
	defer stop()
	// 以 target 的上下文而非 r.Context() 调用：Worker 断开不取消 Invoke（见 target.ctx）。
	res, err := t.e.calls.Invoke(t.ctx, in)
	if err != nil {
		t.internalError(w, "invoke", err, "call_id", callID)
		return
	}
	if tb := res.ToolBudget; tb != nil {
		w.Header().Set(HeaderToolBudget, fmt.Sprintf("%d/%d", tb.Used, tb.Limit))
	}
	t.writeResult(w, callID, res)
}

// writeResult 写出计费调用或 exec 的结果：拒绝与失败为错误体（Status/Code 原样）；成功时带结果 blob 与重放头。
func (t *target) writeResult(w http.ResponseWriter, callID string, res call.Result) {
	if res.Status != 0 && res.Code != "" {
		t.writeError(w, res.Status, res.Code, http.StatusText(res.Status))
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if res.BlobSHA256 != "" {
		h.Set(HeaderBlob, res.BlobSHA256)
	}
	if res.Replayed {
		h.Set(HeaderReplayed, "true")
	}
	status := res.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if _, err := w.Write(res.Body); err != nil {
		t.e.log.Debug("gateway: 写响应失败", "attempt_id", t.attemptID, "call_id", callID, "err", err)
	}
}

// access 做 §9.2 访问检查（经 Calls.CheckAccess）；拒绝时写出错误并返回 false。
func (t *target) access(w http.ResponseWriter, r *http.Request, subrunID string) bool {
	res, err := t.e.calls.CheckAccess(r.Context(), t.taskID, t.attemptID, subrunID)
	if err != nil {
		t.internalError(w, "check_access", err)
		return false
	}
	if res.Status != 0 && res.Code != "" {
		t.writeError(w, res.Status, res.Code, http.StatusText(res.Status))
		return false
	}
	return true
}

// budgetBody 是 GET /v1/budget 的响应（微美元整数；available 可为负）。
type budgetBody struct {
	LimitMicro     int64 `json:"limit_micro"`
	ReservedMicro  int64 `json:"reserved_micro"`
	SpentMicro     int64 `json:"spent_micro"`
	UnknownMicro   int64 `json:"unknown_micro"`
	AvailableMicro int64 `json:"available_micro"`
	ToolCallsUsed  int64 `json:"tool_calls_used"`
	// ToolCallLimit 是每 turn 的工具调用上限；不限时为 null。
	ToolCallLimit *int64 `json:"tool_call_limit"`
	// Subrun 只在请求带 X-Agentbox-Subrun 时出现（不带头时响应与之前逐字节相同）。
	Subrun *subrunBudgetBody `json:"subrun,omitempty"`
	// Exec 是任务级 exec 配额（§10.3）；未配置 exec 时不出现。
	Exec *execQuotaBody `json:"exec,omitempty"`
}

// execQuotaBody 是 exec 配额（尚无配额行时为策略值、用量 0）。cpu_available_usec = limit − reserved − spent −
// unknown，可为负（超额如实记账并置 blocked）。
type execQuotaBody struct {
	CountLimit       int64 `json:"count_limit"`
	CountUsed        int64 `json:"count_used"`
	CPULimitUsec     int64 `json:"cpu_limit_usec"`
	CPUAvailableUsec int64 `json:"cpu_available_usec"`
	WallLimitMs      int64 `json:"wall_limit_ms"`
	WallSpentMs      int64 `json:"wall_spent_ms"`
	Blocked          bool  `json:"blocked"`
}

// subrunBudgetBody 是 sub-run 层账本（§9.6）。CapMicro 为 null 表示只做归属、不设上限，此时 AvailableMicro
// 也为 null（没有上限就没有可用额度的概念；不输出超出 JSON 安全整数的哨兵值）。
type subrunBudgetBody struct {
	CapMicro       *int64 `json:"cap_micro"`
	ReservedMicro  int64  `json:"reserved_micro"`
	SpentMicro     int64  `json:"spent_micro"`
	UnknownMicro   int64  `json:"unknown_micro"`
	AvailableMicro *int64 `json:"available_micro"`
}

func (t *target) budget(w http.ResponseWriter, r *http.Request, subrunID string) {
	if !t.access(w, r, subrunID) {
		return
	}
	bg, err := t.e.calls.Budget(r.Context(), t.taskID)
	if err != nil {
		t.internalError(w, "budget", err)
		return
	}
	body := budgetBody{
		LimitMicro: bg.LimitMicro, ReservedMicro: bg.ReservedMicro, SpentMicro: bg.SpentMicro,
		UnknownMicro: bg.UnknownMicro, AvailableMicro: bg.Available(),
		ToolCallsUsed: bg.ToolCallsUsed, ToolCallLimit: bg.ToolCallLimit,
	}
	if subrunID != "" {
		sb, err := t.e.calls.SubrunBudget(r.Context(), t.taskID, subrunID)
		if err != nil {
			t.internalError(w, "subrun_budget", err, "subrun_id", subrunID)
			return
		}
		sub := &subrunBudgetBody{CapMicro: sb.CapMicro, ReservedMicro: sb.ReservedMicro, SpentMicro: sb.SpentMicro,
			UnknownMicro: sb.UnknownMicro}
		if sb.CapMicro != nil {
			avail := sb.Available()
			sub.AvailableMicro = &avail
		}
		body.Subrun = sub
	}
	// 是否已有配额行不影响输出：没有时 Calls 已给出策略值与用量 0。
	switch q, _, err := t.e.calls.ExecQuota(r.Context(), t.taskID); {
	case errors.Is(err, call.ErrExecNotConfigured):
	case err != nil:
		t.internalError(w, "exec_quota", err)
		return
	default:
		body.Exec = &execQuotaBody{CountLimit: q.CountLimit, CountUsed: q.CountUsed, CPULimitUsec: q.CPULimitUsec,
			CPUAvailableUsec: q.CPUAvailable(), WallLimitMs: q.WallLimitMs, WallSpentMs: q.WallSpentMs, Blocked: q.Blocked}
	}
	t.writeJSON(w, http.StatusOK, body)
}

func (t *target) blob(w http.ResponseWriter, r *http.Request, sha, subrunID string) {
	if !shaPattern.MatchString(sha) {
		t.writeError(w, http.StatusNotFound, CodeNotFound, "blob 不存在")
		return
	}
	if !t.access(w, r, subrunID) {
		return
	}
	rc, err := t.e.calls.OpenBlob(r.Context(), t.taskID, sha)
	if errors.Is(err, blob.ErrNotFound) {
		t.writeError(w, http.StatusNotFound, CodeNotFound, "blob 不存在")
		return
	}
	if err != nil {
		t.internalError(w, "open_blob", err, "sha", sha)
		return
	}
	defer func() {
		if cerr := rc.Close(); cerr != nil {
			t.e.log.Debug("gateway: 关闭 blob 失败", "sha", sha, "err", cerr)
		}
	}()
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("ETag", `"`+sha+`"`)
	w.WriteHeader(http.StatusOK)
	// 流式复制，不整体读入内存。
	if _, err := io.Copy(w, rc); err != nil {
		t.e.log.Debug("gateway: 流式返回 blob 中断", "attempt_id", t.attemptID, "sha", sha, "err", err)
	}
}

func (t *target) methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	t.writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed, "方法不允许")
}

func (t *target) internalError(w http.ResponseWriter, op string, err error, attrs ...any) {
	if errors.Is(err, call.ErrClosed) || t.ctx.Err() != nil {
		t.writeError(w, http.StatusServiceUnavailable, CodeGatewayUnavailable, "Gateway 正在关闭或入口已撤销")
		return
	}
	t.e.log.Warn("gateway: 请求处理失败", append([]any{"op", op, "attempt_id", t.attemptID, "err", err}, attrs...)...)
	t.writeError(w, http.StatusInternalServerError, CodeInternal, "内部错误")
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (t *target) writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code, body.Error.Message = code, message
	t.writeJSON(w, status, body)
}

func (t *target) writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		t.e.log.Error("gateway: 编码响应失败", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		t.e.log.Debug("gateway: 写响应失败", "attempt_id", t.attemptID, "err", err)
	}
}

// limitListener 跟踪已接受的连接并在接受时把连接绑定到当前 attempt（cur）：cur 为 nil（incarnation 空闲）
// 或该 attempt 的连接数已达 max 时，新连接在接受后立即关闭；closeAll 关闭 listener 与全部连接。
type limitListener struct {
	net.Listener
	max      int
	onRemove func(attemptID string) // 连接移出集合之后调用（只供测试，见 Edge.connStateHook）

	mu     sync.Mutex
	closed bool
	cur    *target
	conns  map[*trackedConn]struct{}
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			// 连接在关闭过程中到达：拒绝，listener 随即报告关闭。
			_ = c.Close() // 被拒绝的连接，关闭错误无关紧要
			return nil, net.ErrClosed
		}
		t := l.cur
		if t == nil || t.conns >= l.max {
			l.mu.Unlock()
			_ = c.Close() // 空闲入口或超限连接立即关闭，关闭错误无关紧要
			continue
		}
		tc := &trackedConn{Conn: c, l: l, t: t}
		l.conns[tc] = struct{}{}
		t.conns++
		l.mu.Unlock()
		return tc, nil
	}
}

// attach 设置当前 attempt。已关闭时返回 net.ErrClosed，已有当前 attempt 时返回 ErrAlreadyBound。
func (l *limitListener) attach(t *target) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	if l.cur != nil {
		return ErrAlreadyBound
	}
	l.cur = t
	return nil
}

func (l *limitListener) currentAttempt() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cur == nil {
		return ""
	}
	return l.cur.attemptID
}

// detach 在 attemptID 是当前 attempt 时清除它（之后的新连接被拒绝），返回该 target 与其全部连接；
// 否则返回 nil。与 Accept 在同一把锁下：不会有连接在 detach 之后仍绑定到该 attempt 而未被返回。
func (l *limitListener) detach(attemptID string) (*target, []*trackedConn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.cur
	if t == nil || t.attemptID != attemptID {
		return nil, nil
	}
	l.cur = nil
	var conns []*trackedConn
	for c := range l.conns {
		if c.t == t {
			conns = append(conns, c)
		}
	}
	return t, conns
}

// closeAll 关闭 listener 与全部连接，并结束当前 attempt 的 Invoke 上下文。
func (l *limitListener) closeAll() {
	l.mu.Lock()
	l.closed = true
	cur := l.cur
	l.cur = nil
	conns := make([]*trackedConn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.mu.Unlock()
	_ = l.Listener.Close() // http.Server.Close 通常已关闭它；重复关闭的错误无关紧要
	for _, c := range conns {
		_ = c.Close() // 撤销清理，连接可能已被对端或 http.Server 关闭
	}
	if cur != nil {
		cur.cancel()
	}
}

func (l *limitListener) remove(c *trackedConn) {
	l.mu.Lock()
	delete(l.conns, c)
	c.t.conns--
	l.mu.Unlock()
	if l.onRemove != nil {
		l.onRemove(c.t.attemptID)
	}
}

// connKey 是请求上下文中 *trackedConn 的键。
type connKey struct{}

// trackedConn 是接受时绑定到 attempt（t）的连接；关闭时从 limitListener 的集合中移除。
type trackedConn struct {
	net.Conn
	l    *limitListener
	t    *target
	once sync.Once
	err  error
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.l.remove(c)
	})
	return c.err
}
