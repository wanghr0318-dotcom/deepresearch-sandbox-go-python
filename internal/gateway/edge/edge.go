// Package edge 是 Gateway 面向 Worker 的入口（规格 §9.1–§9.3）：每个 attempt 一个 Unix socket 上的
// HTTP/1.1 服务，连接在接受时绑定到该 socket 的 attempt；端点经 Calls 交给 call 协调器处理。
//
// edge 不判定权限：每个请求的访问检查（§9.2）在 Calls.Invoke / CheckAccess 内部的一致快照中完成；
// Revoke 被调用时 attempt_access 已是 revoked（由 actor 保证顺序），edge 只做清理（关闭连接与
// listener、删除 socket），并把离开原因转交 Calls.CancelAttempt——上游 try 是否取消只按原因决定（§9.1 表）。
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
	"sync/atomic"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
)

// Calls 是 edge 使用的 call 协调器子集（*call.Coordinator 满足它）。
type Calls interface {
	Invoke(ctx context.Context, in call.Invoke) (call.Result, error)
	CheckAccess(ctx context.Context, taskID, attemptID string) (call.Result, error)
	Budget(ctx context.Context, taskID string) (call.Budget, error)
	OpenBlob(ctx context.Context, taskID, sha string) (io.ReadCloser, error)
	CancelAttempt(attemptID, reason string)
}

// Attempts 把 attempt 映射到它所属的任务与环境（由 Task 5 以 task.Store.LookupAttempt 实现）。
type Attempts interface {
	Lookup(ctx context.Context, attemptID string) (taskID, envID string, err error)
}

// Config 配置 Edge。零值字段取 §19 默认值。
type Config struct {
	SocketDir string       // <data>/gateway；socket 为 <SocketDir>/<attempt_id>.sock
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
	CodeSubrunUnsupported  = "subrun_unsupported"
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
)

// ErrClosed 表示 Edge 已关闭。
var ErrClosed = errors.New("edge: 已关闭")

// ErrAlreadyBound 表示该 attempt 已有入口。
var ErrAlreadyBound = errors.New("edge: attempt 已绑定")

var (
	shaPattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	attemptPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

// billable 是计费端点到调用类别的映射（M2 范围）。
var billable = map[string]upstream.Kind{
	"/v1/chat/completions": upstream.KindChat,
	"/v1/search":           upstream.KindSearch,
	"/v1/fetch":            upstream.KindFetch,
}

// Edge 管理每个 attempt 的 listener 与连接集合。并发安全。
type Edge struct {
	cfg      Config
	calls    Calls
	attempts Attempts
	log      *slog.Logger

	mu       sync.Mutex
	closed   bool
	bindings map[string]*binding

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
	return &Edge{cfg: cfg, calls: calls, attempts: attempts, log: log, bindings: make(map[string]*binding)}
}

// SocketPath 返回 attempt 的 socket 路径 <SocketDir>/<attempt_id>.sock。
func (e *Edge) SocketPath(attemptID string) string {
	return filepath.Join(e.cfg.SocketDir, attemptID+".sock")
}

// Bind 为 attempt 创建 <SocketDir>/<attempt_id>.sock（0600）并启动 HTTP/1.1 服务，返回 socket 路径。
// envID 非空时须与 Attempts.Lookup 给出的环境一致。属主 chown 由 provider 启动器完成。
//
// socket 先在 0700 的私有临时目录中 bind、chmod 0600，再原子 rename 到最终路径：最终路径上的 socket
// 从出现起就是 0600，不依赖进程级 umask（修改 umask 会与其他 goroutine 的文件创建竞争）。
func (e *Edge) Bind(ctx context.Context, attemptID, envID string) (string, error) {
	if !attemptPattern.MatchString(attemptID) {
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
	b := newBinding(e, attemptID, taskID, gotEnv, path, ln)
	e.bindings[attemptID] = b
	go b.serve()
	e.log.Info("gateway: attempt 入口已绑定", "attempt_id", attemptID, "task_id", taskID, "env_id", gotEnv, "socket", path)
	return path, nil
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
	if attemptPattern.MatchString(attemptID) {
		if rerr := os.Remove(e.SocketPath(attemptID)); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			err = fmt.Errorf("edge: 删除 attempt %s 的 socket: %w", attemptID, rerr)
		}
	}
	e.calls.CancelAttempt(attemptID, reason)
	return err
}

// Close 关闭全部入口并删除其 socket 文件；之后 Bind 返回 ErrClosed。Close 不调用 CancelAttempt：
// Gateway 关闭时在途 try 的处理由 call 协调器自身的 Close 决定。
func (e *Edge) Close() error {
	e.mu.Lock()
	e.closed = true
	bs := e.bindings
	e.bindings = make(map[string]*binding)
	e.mu.Unlock()

	var errs []error
	for id, b := range bs {
		b.shutdown()
		if err := os.Remove(b.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("edge: 删除 attempt %s 的 socket: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// binding 是一个 attempt 的入口：listener、HTTP 服务与连接集合。
type binding struct {
	e                        *Edge
	attemptID, taskID, envID string
	path                     string
	ln                       *limitListener
	srv                      *http.Server
	served                   chan struct{}
	active                   atomic.Int64

	// ctx 是交给 Calls.Invoke 的上下文：它只在撤销或关闭时结束，与 Worker 的请求上下文分离——
	// Worker 断开连接或取消 HTTP 请求不会取消 Invoke（§9.1 表第一行；协调器的后台 try 本来也不随
	// Invoke 的 ctx 取消，这里在 edge 一侧显式保证）。
	ctx    context.Context
	cancel context.CancelFunc
}

func newBinding(e *Edge, attemptID, taskID, envID, path string, ul *net.UnixListener) *binding {
	b := &binding{
		e: e, attemptID: attemptID, taskID: taskID, envID: envID, path: path,
		served: make(chan struct{}),
	}
	hook := e.connStateHook
	b.ln = &limitListener{Listener: ul, max: e.cfg.MaxConns, conns: make(map[*trackedConn]struct{})}
	if hook != nil {
		b.ln.onRemove = func() { hook(attemptID, http.StateClosed) }
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.srv = &http.Server{
		Handler:           b,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          slog.NewLogLogger(e.log.Handler(), slog.LevelDebug),
		// 处理函数经请求上下文取得自己的连接（见 invoke：Worker 断开时立即释放连接名额）。
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if tc, ok := c.(*trackedConn); ok {
				return context.WithValue(ctx, connKey{}, tc)
			}
			return ctx
		},
	}
	if hook != nil {
		b.srv.ConnState = func(_ net.Conn, s http.ConnState) {
			// StateClosed/StateHijacked 由 trackedConn 在移出连接集合之后报告（onRemove）。
			if s != http.StateClosed && s != http.StateHijacked {
				hook(attemptID, s)
			}
		}
	}
	return b
}

func (b *binding) serve() {
	defer close(b.served)
	if err := b.srv.Serve(b.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		b.e.log.Warn("gateway: attempt 入口服务结束", "attempt_id", b.attemptID, "err", err)
	}
}

// shutdown 关闭 listener 与全部连接，并结束交给 Invoke 的上下文（面向 Worker 的请求随之结束）。
func (b *binding) shutdown() {
	if err := b.srv.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		b.e.log.Debug("gateway: 关闭入口服务", "attempt_id", b.attemptID, "err", err)
	}
	b.ln.closeAll()
	b.cancel()
	<-b.served
}

func (b *binding) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if n := b.active.Add(1); n > int64(b.e.cfg.MaxActive) {
		b.active.Add(-1)
		b.writeError(w, http.StatusTooManyRequests, CodeTooManyRequests, "活跃请求超过上限")
		return
	}
	defer b.active.Add(-1)

	if r.Header.Get(HeaderSubrun) != "" {
		b.writeError(w, http.StatusBadRequest, CodeSubrunUnsupported, "本版本不支持 sub-run 调用")
		return
	}
	path := r.URL.Path
	switch {
	case path == "/v1/exec":
		b.writeError(w, http.StatusNotImplemented, CodeNotImplemented, "exec 尚未实现")
	case billable[path] != "":
		if r.Method != http.MethodPost {
			b.methodNotAllowed(w, http.MethodPost)
			return
		}
		b.invoke(w, r, billable[path])
	case path == "/v1/budget":
		if r.Method != http.MethodGet {
			b.methodNotAllowed(w, http.MethodGet)
			return
		}
		b.budget(w, r)
	case strings.HasPrefix(path, "/blobs/"):
		if r.Method != http.MethodGet {
			b.methodNotAllowed(w, http.MethodGet)
			return
		}
		b.blob(w, r, strings.TrimPrefix(path, "/blobs/"))
	default:
		b.writeError(w, http.StatusNotFound, CodeNotFound, "未知端点")
	}
}

func (b *binding) invoke(w http.ResponseWriter, r *http.Request, kind upstream.Kind) {
	callID := r.Header.Get(HeaderCallID)
	if callID == "" || len(callID) > MaxCallIDBytes {
		b.writeError(w, http.StatusBadRequest, CodeMissingCallID, "计费调用须携带 1–256 字节的 X-Agentbox-Call-Id")
		return
	}
	// 不按 Content-Length 提前拒绝：未读请求体时立即响应会让仍在写入的客户端得到 broken pipe 而非 413。
	// MaxBytesReader 至多读入 MaxBody 字节，超限时响应后关闭连接。
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, b.e.cfg.MaxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			b.writeError(w, http.StatusRequestEntityTooLarge, CodeRequestTooLarge, "请求体超过上限")
			return
		}
		b.writeError(w, http.StatusBadRequest, CodeInvalidRequest, "读取请求体失败")
		return
	}
	in := call.Invoke{
		TaskID: b.taskID, AttemptID: b.attemptID, EnvID: b.envID,
		CallID: callID, Kind: kind, Body: body,
		Retry:           strings.EqualFold(strings.TrimSpace(r.Header.Get(HeaderRetry)), "true"),
		Supersedes:      r.Header.Get(HeaderSupersedes),
		SupersedeReason: r.Header.Get(HeaderSupersedeReason),
	}
	// Worker 在响应前断开（net/http 读到 EOF 后取消 r.Context()）：立即关闭服务端连接、释放连接名额，
	// Invoke 不受影响，响应写入失败即丢弃。处理函数返回前 stop，正常结束不会触发。
	if tc, ok := r.Context().Value(connKey{}).(*trackedConn); ok {
		stop := context.AfterFunc(r.Context(), func() {
			_ = tc.Close() // Worker 已断开，关闭错误无关紧要
		})
		defer stop()
	}
	// 以 binding 的上下文而非 r.Context() 调用：Worker 断开不取消 Invoke（见 binding.ctx）。
	res, err := b.e.calls.Invoke(b.ctx, in)
	if err != nil {
		b.internalError(w, "invoke", err, "call_id", callID)
		return
	}
	if res.Status != 0 && res.Code != "" {
		b.writeError(w, res.Status, res.Code, http.StatusText(res.Status))
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
		b.e.log.Debug("gateway: 写响应失败", "attempt_id", b.attemptID, "call_id", callID, "err", err)
	}
}

// access 做 §9.2 访问检查（经 Calls.CheckAccess）；拒绝时写出错误并返回 false。
func (b *binding) access(w http.ResponseWriter, r *http.Request) bool {
	res, err := b.e.calls.CheckAccess(r.Context(), b.taskID, b.attemptID)
	if err != nil {
		b.internalError(w, "check_access", err)
		return false
	}
	if res.Status != 0 && res.Code != "" {
		b.writeError(w, res.Status, res.Code, http.StatusText(res.Status))
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
}

func (b *binding) budget(w http.ResponseWriter, r *http.Request) {
	if !b.access(w, r) {
		return
	}
	bg, err := b.e.calls.Budget(r.Context(), b.taskID)
	if err != nil {
		b.internalError(w, "budget", err)
		return
	}
	b.writeJSON(w, http.StatusOK, budgetBody{
		LimitMicro: bg.LimitMicro, ReservedMicro: bg.ReservedMicro, SpentMicro: bg.SpentMicro,
		UnknownMicro: bg.UnknownMicro, AvailableMicro: bg.Available(),
	})
}

func (b *binding) blob(w http.ResponseWriter, r *http.Request, sha string) {
	if !shaPattern.MatchString(sha) {
		b.writeError(w, http.StatusNotFound, CodeNotFound, "blob 不存在")
		return
	}
	if !b.access(w, r) {
		return
	}
	rc, err := b.e.calls.OpenBlob(r.Context(), b.taskID, sha)
	if errors.Is(err, blob.ErrNotFound) {
		b.writeError(w, http.StatusNotFound, CodeNotFound, "blob 不存在")
		return
	}
	if err != nil {
		b.internalError(w, "open_blob", err, "sha", sha)
		return
	}
	defer func() {
		if cerr := rc.Close(); cerr != nil {
			b.e.log.Debug("gateway: 关闭 blob 失败", "sha", sha, "err", cerr)
		}
	}()
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("ETag", `"`+sha+`"`)
	w.WriteHeader(http.StatusOK)
	// 流式复制，不整体读入内存。
	if _, err := io.Copy(w, rc); err != nil {
		b.e.log.Debug("gateway: 流式返回 blob 中断", "attempt_id", b.attemptID, "sha", sha, "err", err)
	}
}

func (b *binding) methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	b.writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed, "方法不允许")
}

func (b *binding) internalError(w http.ResponseWriter, op string, err error, attrs ...any) {
	if errors.Is(err, call.ErrClosed) || b.ctx.Err() != nil {
		b.writeError(w, http.StatusServiceUnavailable, CodeGatewayUnavailable, "Gateway 正在关闭或入口已撤销")
		return
	}
	b.e.log.Warn("gateway: 请求处理失败", append([]any{"op", op, "attempt_id", b.attemptID, "err", err}, attrs...)...)
	b.writeError(w, http.StatusInternalServerError, CodeInternal, "内部错误")
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (b *binding) writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code, body.Error.Message = code, message
	b.writeJSON(w, status, body)
}

func (b *binding) writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		b.e.log.Error("gateway: 编码响应失败", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		b.e.log.Debug("gateway: 写响应失败", "attempt_id", b.attemptID, "err", err)
	}
}

// limitListener 跟踪已接受的连接：超过 max 的新连接在接受后立即关闭；closeAll 关闭 listener 与全部连接。
type limitListener struct {
	net.Listener
	max      int
	onRemove func() // 连接移出集合之后调用（只供测试，见 Edge.connStateHook）

	mu     sync.Mutex
	closed bool
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
		if len(l.conns) >= l.max {
			l.mu.Unlock()
			_ = c.Close() // 超限连接立即关闭，关闭错误无关紧要
			continue
		}
		tc := &trackedConn{Conn: c, l: l}
		l.conns[tc] = struct{}{}
		l.mu.Unlock()
		return tc, nil
	}
}

func (l *limitListener) closeAll() {
	l.mu.Lock()
	l.closed = true
	conns := make([]*trackedConn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.mu.Unlock()
	_ = l.Listener.Close() // http.Server.Close 通常已关闭它；重复关闭的错误无关紧要
	for _, c := range conns {
		_ = c.Close() // 撤销清理，连接可能已被对端或 http.Server 关闭
	}
}

func (l *limitListener) remove(c *trackedConn) {
	l.mu.Lock()
	delete(l.conns, c)
	l.mu.Unlock()
	if l.onRemove != nil {
		l.onRemove()
	}
}

// connKey 是请求上下文中 *trackedConn 的键。
type connKey struct{}

// trackedConn 在关闭时从 limitListener 的集合中移除。
type trackedConn struct {
	net.Conn
	l    *limitListener
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
