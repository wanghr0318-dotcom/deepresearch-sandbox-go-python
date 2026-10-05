package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// Mode 是服务的运行模式（规格 §15.1 `/status`）。
type Mode string

const (
	ModeNormal        Mode = "normal"         // 全部端点可用
	ModeDiagnostic    Mode = "diagnostic"     // 只开放 status 与 inspect
	ModeOwnershipLost Mode = "ownership_lost" // 拒绝写操作
)

// TokenFile 是数据目录下 API token 的文件名（规格 §15.3）。
const TokenFile = "api.token"

const (
	defaultHeartbeat       = 15 * time.Second
	defaultPollInterval    = 200 * time.Millisecond
	defaultMaxFaultRetries = 3
	maxBodyBytes           = 1 << 20
	maxRequestIDLen        = 128
	defaultListLimit       = 50
	maxListLimit           = 200
	eventPageSize          = 100
)

// Config 是 HTTP 处理器的装配参数。
type Config struct {
	Store Store
	// Blobs 是产物下载读取内容的 BlobStore（必填）。
	Blobs Blobs
	// Mode 返回当前运行模式；nil 视为 normal。
	Mode func() Mode
	// ListenAddr 是服务绑定的地址。非 loopback 地址必须配置 Token（规格 §15.3）。
	ListenAddr string
	// Token 是静态 Bearer token（来自 `<data>/api.token`，见 LoadToken）。非空时每个请求都须携带。
	Token string
	// AllowedHosts 是 Host 头的显式允许列表；为空且 ListenAddr 为 loopback 时取
	// 127.0.0.1:<port>、localhost:<port>、[::1]:<port>。
	AllowedHosts []string
	// AllowedOrigins 是浏览器 Origin 的显式允许列表（不使用通配）；不带 Origin 的请求（CLI）不受限。
	// 为空时取 <scheme>://ListenAddr 与 <scheme>://<每个允许的 Host>（scheme 由 TLS 决定）。
	AllowedOrigins []string
	// TLS 表示监听端由内置 TLS 终止：决定默认 Origin 的 scheme。非 loopback 监听且 TLS 为 false 时
	// New 记录一条警告（规格 §15.3 要求可信 TLS 终止，但可以在外部完成，故不拒绝启动）。
	TLS bool
	// WebDir 非空时在 API 之外的路径上提供该目录下的静态文件（同源工作台），不存在的路径回退到
	// index.html（SPA）。API 路径（/status、/tasks…、/events…）从不由静态文件应答。静态文件不需要 token。
	WebDir string
	// Logger 记录请求的方法、路径与状态；不记录请求头与查询串。nil 时不记录。
	Logger *slog.Logger
	// ConfigVersion 与 MaxFaultRetries 写入新建的任务；MaxFaultRetries 为 0 时取 3（规格默认值）。
	ConfigVersion   string
	MaxFaultRetries int64
	// Heartbeat 是 SSE 注释行心跳间隔，默认 15 s；PollInterval 是读取新事件的间隔。
	Heartbeat    time.Duration
	PollInterval time.Duration
	// NewTaskID 生成任务 ID；nil 时使用 128 位随机数。
	NewTaskID func() string
	// EffectiveLimits 可选：创建任务时由请求中的 limits（省略时为 nil）得出随任务持久化的有效 limits，
	// 例如补入默认的累计运行时限（规格 §14.4：创建时确定并存储）；返回错误时以 400 invalid_limits 拒绝
	// （例如显式时限不是正数、超过服务端上限，或 memory_max 永远无法被授予）。body_hash 仍按原始请求计算。
	// nil 时按原样存储请求中的 limits。
	EffectiveLimits func(limits json.RawMessage) (json.RawMessage, error)
	// CacheMetrics 可选：返回 Gateway 共享缓存的指标（规格 §11.5），由 GET /status 的 cache 字段给出；
	// nil 或返回 nil（缓存关闭）时省略该字段。
	CacheMetrics func() map[string]int64
}

// Handler 实现 api/openapi.yaml 描述的 REST 与 SSE 接口。
type Handler struct {
	cfg     Config
	token   []byte
	hosts   map[string]bool
	origins map[string]bool
	mux     *http.ServeMux
}

// route 是一个已实现的操作。statuses 是该操作可能返回的状态码集合，须与 openapi.yaml 一致。
type route struct {
	method, path string
	access       access
	statuses     []int
	handle       func(h *Handler, w http.ResponseWriter, r *http.Request)
}

// access 表示操作在受限模式下是否可用。
type access int

const (
	accessRead       access = iota // diagnostic 模式下拒绝
	accessWrite                    // diagnostic 与 ownership_lost 模式下拒绝
	accessDiagnostic               // 任何模式下可用
)

var (
	readStatuses    = []int{200, 401, 403, 404, 500, 503}
	controlStatuses = []int{200, 400, 401, 403, 404, 409, 500, 503}
	resultStatuses  = []int{200, 401, 403, 404, 409, 500, 503}
	blobStatuses    = []int{200, 400, 401, 403, 404, 500, 503}
)

// routes 是已实现操作的唯一列表；测试据此与 openapi.yaml 比对。
var routes = []route{
	{"GET", "/status", accessDiagnostic, []int{200, 401, 403}, (*Handler).getStatus},
	{"GET", "/tasks", accessRead, []int{200, 400, 401, 403, 500, 503}, (*Handler).listTasks},
	{"POST", "/tasks", accessWrite, []int{201, 400, 401, 403, 409, 500, 503}, (*Handler).createTask},
	{"GET", "/tasks/{id}", accessRead, readStatuses, (*Handler).getTask},
	{"POST", "/tasks/{id}/cancel", accessWrite, controlStatuses, control("cancel")},
	{"POST", "/tasks/{id}/pause", accessWrite, controlStatuses, control("pause")},
	{"POST", "/tasks/{id}/resume", accessWrite, controlStatuses, control("run")},
	{"GET", "/tasks/{id}/events", accessRead, []int{200, 400, 401, 403, 404, 500, 503}, (*Handler).streamEvents},
	{"GET", "/tasks/{id}/result", accessRead, resultStatuses, (*Handler).getResult},
	{"GET", "/tasks/{id}/artifacts/{artifact_id}", accessRead, blobStatuses, (*Handler).getArtifact},
	{"GET", "/tasks/{id}/artifacts/{artifact_id}/versions/{v}", accessRead, blobStatuses, (*Handler).getArtifact},
	{"GET", "/tasks/{id}/inspect", accessDiagnostic, readStatuses, (*Handler).inspect},
}

// New 校验访问配置并构造处理器。
func New(cfg Config) (*Handler, error) {
	if cfg.Store == nil {
		return nil, errors.New("api: 缺少 Store")
	}
	if cfg.Blobs == nil {
		return nil, errors.New("api: 缺少 Blobs")
	}
	host, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("api: ListenAddr %q: %w", cfg.ListenAddr, err)
	}
	loopback := isLoopback(host)
	if !loopback && cfg.Token == "" {
		return nil, fmt.Errorf("api: 绑定非 loopback 地址 %s 需要 %s 中的 token", cfg.ListenAddr, TokenFile)
	}
	hosts := cfg.AllowedHosts
	if len(hosts) == 0 {
		if !loopback {
			return nil, fmt.Errorf("api: 绑定非 loopback 地址 %s 需要显式的 Host 允许列表", cfg.ListenAddr)
		}
		hosts = []string{net.JoinHostPort("127.0.0.1", port), net.JoinHostPort("localhost", port), net.JoinHostPort("::1", port)}
	}
	h := &Handler{cfg: cfg, hosts: map[string]bool{}, origins: map[string]bool{}, mux: http.NewServeMux()}
	for _, v := range hosts {
		if v == "*" || v == "" {
			return nil, fmt.Errorf("api: Host 允许列表不能含通配或空值")
		}
		h.hosts[strings.ToLower(v)] = true
	}
	origins := cfg.AllowedOrigins
	if len(origins) == 0 {
		scheme := "http://"
		if cfg.TLS {
			scheme = "https://"
		}
		origins = []string{scheme + strings.ToLower(cfg.ListenAddr)}
		for v := range h.hosts {
			origins = append(origins, scheme+v)
		}
	}
	for _, v := range origins {
		if v == "*" || v == "" || v == "null" {
			return nil, fmt.Errorf("api: Origin 允许列表不能含通配、空值或 null")
		}
		h.origins[v] = true
	}
	if cfg.Token != "" {
		h.token = []byte(cfg.Token)
	}
	if !loopback && !cfg.TLS && cfg.Logger != nil {
		cfg.Logger.Warn("api: 非 loopback 监听未启用内置 TLS；远程访问须经可信 TLS 终止（--tls-cert/--tls-key 或外部代理），否则 token 以明文传输",
			"listen", cfg.ListenAddr)
	}
	if h.cfg.Mode == nil {
		h.cfg.Mode = func() Mode { return ModeNormal }
	}
	if h.cfg.MaxFaultRetries == 0 {
		h.cfg.MaxFaultRetries = defaultMaxFaultRetries
	}
	if h.cfg.Heartbeat <= 0 {
		h.cfg.Heartbeat = defaultHeartbeat
	}
	if h.cfg.PollInterval <= 0 {
		h.cfg.PollInterval = defaultPollInterval
	}
	if h.cfg.NewTaskID == nil {
		h.cfg.NewTaskID = randomTaskID
	}
	for _, rt := range routes {
		h.mux.HandleFunc(rt.method+" "+rt.path, func(w http.ResponseWriter, r *http.Request) {
			if code, msg, ok := h.modeAllows(rt.access); !ok {
				writeError(w, http.StatusServiceUnavailable, code, msg)
				return
			}
			rt.handle(h, w, r)
		})
	}
	h.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "没有这个端点")
	})
	return h, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// LoadToken 读取 `<data>/api.token`。文件须只对所有者可读写（0600，Windows 上不检查权限位）。
func LoadToken(dataDir string) (string, error) {
	path := filepath.Join(dataDir, TokenFile)
	fi, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("api: 读取 %s: %w", TokenFile, err)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("api: %s 的权限为 %v，须为 0600", TokenFile, fi.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("api: 读取 %s: %w", TokenFile, err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("api: %s 为空", TokenFile)
	}
	return tok, nil
}

func randomTaskID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand 失败时无法安全生成 ID
	}
	return "task_" + hex.EncodeToString(b[:])
}

// securityHeaders 加在每个响应上（静态文件与 API）：工作台只从同源加载脚本与连接，且不可被嵌入。
var securityHeaders = map[string]string{
	"Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; " +
		"connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
	"X-Content-Type-Options": "nosniff",
	"Referrer-Policy":        "no-referrer",
}

// apiPrefixes 是保留给 API 的首段路径（routes 的首段，加上为 SSE 保留的 events）：这些路径总由 API
// 处理（未知端点为 JSON 404），从不由静态文件或 SPA 回退应答。
var apiPrefixes = func() map[string]bool {
	m := map[string]bool{"events": true}
	for _, rt := range routes {
		seg, _, _ := strings.Cut(strings.TrimPrefix(rt.path, "/"), "/")
		m[seg] = true
	}
	return m
}()

func isAPIPath(p string) bool {
	seg, _, _ := strings.Cut(strings.TrimPrefix(path.Clean("/"+p), "/"), "/")
	return apiPrefixes[seg]
}

// ServeHTTP 依次校验 Host、Origin 与 token，再分派到操作。访问日志只记录方法、路径、状态与耗时：
// 不记录任何请求头（包括 Authorization）与查询串。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sw := &statusWriter{ResponseWriter: w}
	h.serve(sw, r)
	if h.cfg.Logger != nil {
		// 只记录方法、路径与状态：token 不会出现在路径中，请求头与查询串不记录。
		h.cfg.Logger.Info("api request", "method", r.Method, "path", r.URL.Path,
			"status", sw.status(), "duration_ms", time.Since(start).Milliseconds())
	}
}

func (h *Handler) serve(w http.ResponseWriter, r *http.Request) {
	for k, v := range securityHeaders {
		w.Header().Set(k, v)
	}
	if !h.hosts[strings.ToLower(r.Host)] {
		writeError(w, http.StatusForbidden, "forbidden_host", "Host 不在允许列表中")
		return
	}
	if h.cfg.WebDir != "" && !isAPIPath(r.URL.Path) {
		h.serveStatic(w, r)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		if !h.origins[origin] {
			writeError(w, http.StatusForbidden, "forbidden_origin", "Origin 不在允许列表中")
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Add("Vary", "Origin")
		if r.Method == http.MethodOptions { // 预检不携带凭据
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Last-Event-ID")
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if h.token != nil {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), h.token) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="agentbox"`)
			writeError(w, http.StatusUnauthorized, "unauthorized", "缺少或无效的 Bearer token")
			return
		}
	}
	h.mux.ServeHTTP(w, r)
}

// serveStatic 从 WebDir 提供静态文件；末段无扩展名的不存在路径回退到根下的 index.html（SPA），带扩展名的
// 为纯文本 404；目录取其 index.html，不列目录。
func (h *Handler) serveStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "静态文件只支持 GET 与 HEAD")
		return
	}
	root := http.Dir(h.cfg.WebDir) // http.Dir 拒绝越出根目录的路径
	name := path.Clean("/" + r.URL.Path)
	f, fi, ok := openRegular(root, name)
	if !ok {
		// 末段带扩展名的路径是缺失的资源（如 /assets/x.js、/favicon.ico），不回退到 index.html。
		if path.Ext(name) != "" {
			http.Error(w, "404 page not found", http.StatusNotFound)
			return
		}
		if f, fi, ok = openRegular(root, "/index.html"); !ok {
			writeError(w, http.StatusNotFound, "not_found", "没有这个文件")
			return
		}
	}
	defer func() { _ = f.Close() }() // 只读，关闭错误不影响已发送的内容
	if fi.Name() == "index.html" {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
}

// openRegular 打开普通文件；目录取其 index.html。
func openRegular(root http.FileSystem, name string) (http.File, os.FileInfo, bool) {
	for range 2 {
		f, err := root.Open(name)
		if err != nil {
			return nil, nil, false
		}
		fi, err := f.Stat()
		if err == nil && fi.Mode().IsRegular() {
			return f, fi, true
		}
		_ = f.Close() // 只读且未发送内容，关闭错误无影响
		if err != nil || !fi.IsDir() {
			return nil, nil, false
		}
		name = path.Join(name, "index.html")
	}
	return nil, nil, false
}

func (h *Handler) modeAllows(a access) (code, msg string, ok bool) {
	switch h.cfg.Mode() {
	case ModeDiagnostic:
		if a != accessDiagnostic {
			return "diagnostic_mode", "诊断模式只开放 status 与 inspect", false
		}
	case ModeOwnershipLost:
		if a == accessWrite {
			return "ownership_lost", "已失去数据库所有权，拒绝写操作", false
		}
	}
	return "", "", true
}

// statusWriter 记录状态码，并透传 Flush 以支持 SSE。
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *statusWriter) status() int {
	if s.code == 0 {
		return http.StatusOK
	}
	return s.code
}

// ---- 响应与错误 ----

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Code: code, Message: msg})
}

// writeStoreError 把持久化错误类别映射为状态码与错误码。
func writeStoreError(w http.ResponseWriter, err error) {
	var rej *persistence.RejectedError
	switch {
	case errors.As(err, &rej):
		writeError(w, http.StatusConflict, rej.Code, rejectedMessage(rej))
	case errors.Is(err, persistence.ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, persistence.ErrNotFound):
		writeError(w, http.StatusNotFound, "task_not_found", "任务不存在")
	case errors.Is(err, persistence.ErrConflict):
		writeError(w, http.StatusConflict, "request_conflict", "request_id 已用于不同的请求")
	case errors.Is(err, persistence.ErrOwnershipLost):
		writeError(w, http.StatusServiceUnavailable, "ownership_lost", "已失去数据库所有权")
	case errors.Is(err, persistence.ErrCommitUnknown):
		writeError(w, http.StatusServiceUnavailable, "commit_unknown", "提交结果未知，请以同一 request_id 重试")
	case errors.Is(err, persistence.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "存储不可用")
	case errors.Is(err, persistence.ErrContention):
		writeError(w, http.StatusServiceUnavailable, "contention", "锁争用，请重试")
	default:
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
	}
}

var rejectedMessages = map[string]string{
	persistence.CodeTaskEnded:     "任务已终态，不再接受控制",
	persistence.CodeCancelPending: "已接受的取消不可被暂停或恢复覆盖",
	persistence.CodeNotPaused:     "恢复要求任务处于暂停状态",
}

func rejectedMessage(rej *persistence.RejectedError) string {
	msg, ok := rejectedMessages[rej.Code]
	if !ok {
		msg = "请求不满足任务的当前状态"
	}
	if rej.Detail != "" {
		msg += ": " + rej.Detail
	}
	return msg
}

// ---- 请求体与 body_hash ----

// decodeBody 严格解码 JSON 请求体（拒绝未知字段与多余内容）。
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "请求体不是合法的 JSON 对象: "+err.Error())
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "请求体只能包含一个 JSON 对象")
		return false
	}
	return true
}

func validRequestID(w http.ResponseWriter, id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("request_id 必填且不超过 %d 字节", maxRequestIDLen))
		return false
	}
	return true
}

func bodyHash(canonical []byte) []byte {
	sum := sha256.Sum256(canonical)
	return sum[:]
}

// ---- 操作 ----

type statusResponse struct {
	Mode  Mode             `json:"mode"`
	Cache map[string]int64 `json:"cache,omitempty"`
}

func (h *Handler) getStatus(w http.ResponseWriter, _ *http.Request) {
	resp := statusResponse{Mode: h.cfg.Mode()}
	if h.cfg.CacheMetrics != nil {
		resp.Cache = h.cfg.CacheMetrics()
	}
	writeJSON(w, http.StatusOK, resp)
}

type taskJSON struct {
	TaskID                string `json:"task_id"`
	Status                string `json:"status"`
	StatusReason          string `json:"status_reason,omitempty"`
	CurrentAttemptID      string `json:"current_attempt_id,omitempty"`
	Desired               string `json:"desired"`
	ControlVersion        int64  `json:"control_version"`
	AppliedControlVersion int64  `json:"applied_control_version"`
	AttemptsTotal         int64  `json:"attempts_total"`
}

func toTaskJSON(v TaskView) taskJSON {
	return taskJSON(v) // 字段一一对应；TaskView 增加字段时此转换编译失败，提醒同步 JSON 形状
}

type taskListJSON struct {
	Tasks []taskJSON `json:"tasks"`
	Next  string     `json:"next,omitempty"`
}

func (h *Handler) listTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := defaultListLimit
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit 须为 1 到 %d 的整数", maxListLimit))
			return
		}
		limit = n
	}
	views, next, err := h.cfg.Store.ListTasks(r.Context(), q.Get("after"), limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := taskListJSON{Tasks: make([]taskJSON, 0, len(views)), Next: next}
	for _, v := range views {
		out.Tasks = append(out.Tasks, toTaskJSON(v))
	}
	writeJSON(w, http.StatusOK, out)
}

type createTaskBody struct {
	RequestID string          `json:"request_id"`
	Spec      json.RawMessage `json:"spec"`
	Limits    json.RawMessage `json:"limits,omitempty"`
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	var body createTaskBody
	if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
		return
	}
	if !isJSONObject(body.Spec) {
		writeError(w, http.StatusBadRequest, "invalid_request", "spec 必填且须为 JSON 对象")
		return
	}
	if string(bytes.TrimSpace(body.Limits)) == "null" {
		body.Limits = nil // 与省略等价，body_hash 相同
	}
	if len(body.Limits) > 0 && !isJSONObject(body.Limits) {
		writeError(w, http.StatusBadRequest, "invalid_request", "limits 须为 JSON 对象")
		return
	}
	stored := body.Limits
	if h.cfg.EffectiveLimits != nil {
		eff, err := h.cfg.EffectiveLimits(body.Limits)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_limits", err.Error())
			return
		}
		if len(eff) > 0 && !isJSONObject(eff) {
			writeError(w, http.StatusInternalServerError, "internal", "内部错误")
			return
		}
		stored = eff
	}
	canon, err := jcs.Canonical(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "请求体无法规范化")
		return
	}
	req := CreateTaskRequest{
		RequestID: body.RequestID, BodyHash: bodyHash(canon), TaskID: h.cfg.NewTaskID(),
		Spec: mustCanonical(body.Spec), ConfigVersion: h.cfg.ConfigVersion,
		MaxFaultRetries: h.cfg.MaxFaultRetries,
	}
	if len(stored) > 0 {
		req.Limits = mustCanonical(stored)
	}
	res, err := h.cfg.Store.CreateTask(r.Context(), req)
	if errors.Is(err, persistence.ErrCommitUnknown) {
		if got, ok := resolveUnknown(r.Context(), h.cfg.Store, req.RequestID, req.BodyHash, func(b json.RawMessage) (CreateTaskResult, bool) {
			var c CreateTaskResult
			return c, json.Unmarshal(b, &c) == nil && c.TaskID != ""
		}); ok {
			res, err = got, nil
		}
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, res) // 重放时返回首次结果，状态码相同
}

func isJSONObject(b json.RawMessage) bool {
	b = bytes.TrimSpace(b)
	return len(b) > 0 && b[0] == '{'
}

func mustCanonical(raw json.RawMessage) json.RawMessage {
	b, err := jcs.Canonical(raw)
	if err != nil {
		return raw // 已由 decodeBody 解析过，不会发生
	}
	return b
}

// resolveUnknown 在提交结果未知时按 request_id 核对：已提交且 body_hash 相同则返回其结果。
func resolveUnknown[T any](ctx context.Context, s Store, requestID string, hash []byte, decode func(json.RawMessage) (T, bool)) (T, bool) {
	var zero T
	rec, err := s.GetRequest(ctx, requestID)
	if err != nil || !bytes.Equal(rec.BodyHash, hash) {
		return zero, false
	}
	return decode(rec.Response)
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	v, err := h.cfg.Store.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskJSON(v))
}

type controlBody struct {
	RequestID string `json:"request_id"`
	Reason    string `json:"reason,omitempty"`
}

// controlHashInput 是控制请求 body_hash 的输入：请求体加上路径中的任务与操作，
// 使同一 request_id 用于另一个任务或另一种操作时判为冲突。
type controlHashInput struct {
	controlBody
	TaskID  string `json:"task_id"`
	Desired string `json:"desired"`
}

func control(desired string) func(h *Handler, w http.ResponseWriter, r *http.Request) {
	return func(h *Handler, w http.ResponseWriter, r *http.Request) {
		var body controlBody
		if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
			return
		}
		taskID := r.PathValue("id")
		canon, err := jcs.Canonical(controlHashInput{controlBody: body, TaskID: taskID, Desired: desired})
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "请求体无法规范化")
			return
		}
		req := ControlRequest{RequestID: body.RequestID, BodyHash: bodyHash(canon), TaskID: taskID, Desired: desired, Reason: body.Reason}
		res, err := h.cfg.Store.AcceptControl(r.Context(), req)
		if errors.Is(err, persistence.ErrCommitUnknown) {
			if got, ok := resolveUnknown(r.Context(), h.cfg.Store, req.RequestID, req.BodyHash, func(b json.RawMessage) (ControlResult, bool) {
				var c ControlResult
				return c, json.Unmarshal(b, &c) == nil && c.TaskID == taskID
			}); ok {
				res, err = got, nil
			}
		}
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

// ---- 固定输出下载（规格 §5.6、§15.1） ----

// isTerminal 报告任务状态是否为终态（与 internal/task.IsTerminal 一致）。
func isTerminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "cancelled"
}

// inlineSafeTypes 是可以内联显示的媒体类型；其余类型（含 text/html、image/svg+xml 等主动内容）
// 一律以 attachment 下载（规格 §15.4）。
var inlineSafeTypes = map[string]bool{
	"text/plain": true, "text/markdown": true, "application/json": true,
	"image/png": true, "image/jpeg": true, "application/pdf": true,
}

// getResult 返回终态任务的固定结果（含固定的输出 (artifact_id, version, sha256)）。正文为结果的规范 JSON，
// ETag 为其带引号的 sha256。任务未终态为 409 task_not_terminal；终态但没有结果为 404 not_ready。
func (h *Handler) getResult(w http.ResponseWriter, r *http.Request) {
	v, err := h.cfg.Store.TaskResult(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !isTerminal(v.Status) {
		writeError(w, http.StatusConflict, "task_not_terminal", "任务尚未终态，没有固定结果")
		return
	}
	if len(bytes.TrimSpace(v.Result)) == 0 || string(bytes.TrimSpace(v.Result)) == "null" {
		writeError(w, http.StatusNotFound, "not_ready", "任务已终态但没有结果")
		return
	}
	body, err := jcs.Canonical(v.Result)
	if err != nil {
		h.logError("结果无法规范化", err)
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
		return
	}
	sum := sha256.Sum256(body)
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	hdr.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		h.logError("写结果", err)
	}
}

// getArtifact 下载任务中一个产物的版本：`/artifacts/{artifact_id}/versions/{v}` 是固定版本的规范路径（§15.1），
// `/artifacts/{artifact_id}` 默认最新版本，`?version=N` 是规范路径的别名。只返回 visibility = output 且授权到
// scope_blobs(task) 的版本（Store 判定）；internal 版本与不存在的返回相同的 404 artifact_not_found。
// 正文从 BlobStore 流式读取，ETag 为登记的 sha256，Content-Type 为登记的媒体类型；主动内容强制 attachment。
//
// 正文边发送边校验：读到的字节与登记的 sha256 或大小不符（blob 被篡改或损坏）时中止连接，使客户端
// 得到不完整的响应而不是一份看似完整的错误内容。为此不设置 Content-Length（分块传输在中止时必然不完整）。
func (h *Handler) getArtifact(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("v")
	if raw == "" {
		raw = r.URL.Query().Get("version")
	}
	var version int64
	if raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "invalid_request", "version 须为正整数")
			return
		}
		version = n
	}
	a, found, err := h.cfg.Store.PinnedArtifact(r.Context(), r.PathValue("id"), r.PathValue("artifact_id"), version)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "artifact_not_found", "产物或版本不存在")
		return
	}
	rc, err := h.cfg.Blobs.Open(a.SHA256)
	if err != nil {
		h.logError("打开产物 blob", err, "sha256", a.SHA256)
		writeError(w, http.StatusInternalServerError, "blob_unavailable", "产物内容不可读")
		return
	}
	defer func() { _ = rc.Close() }() // 只读，关闭错误不影响已发送的内容

	hdr := w.Header()
	mediaType, inline := "application/octet-stream", false
	if mt, params, err := mime.ParseMediaType(a.MediaType); err == nil {
		if s := mime.FormatMediaType(mt, params); s != "" {
			mediaType, inline = s, inlineSafeTypes[mt]
		}
	}
	hdr.Set("Content-Type", mediaType)
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("ETag", `"`+a.SHA256+`"`)
	if !inline {
		disp := mime.FormatMediaType("attachment", map[string]string{"filename": a.ArtifactID})
		if disp == "" {
			disp = "attachment"
		}
		hdr.Set("Content-Disposition", disp)
	}
	w.WriteHeader(http.StatusOK)
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, hash), rc)
	if err == nil && (n != a.Size || hex.EncodeToString(hash.Sum(nil)) != a.SHA256) {
		err = fmt.Errorf("内容与登记不符：%d 字节，登记 %d 字节、sha256 %s", n, a.Size, a.SHA256)
	}
	if err != nil {
		h.logError("发送产物", err, "sha256", a.SHA256)
		panic(http.ErrAbortHandler) // 中止连接：客户端不会把不完整或被篡改的内容当作完整响应
	}
}

func (h *Handler) logError(msg string, err error, attrs ...any) {
	if h.cfg.Logger != nil {
		h.cfg.Logger.Error("api: "+msg, append([]any{"error", err.Error()}, attrs...)...)
	}
}

type attemptJSON struct {
	AttemptID      string     `json:"attempt_id"`
	AttemptNo      int64      `json:"attempt_no"`
	Status         string     `json:"status"`
	OutcomeClass   string     `json:"outcome_class,omitempty"`
	ExitCode       *int64     `json:"exit_code,omitempty"`
	ExitSignal     *int64     `json:"exit_signal,omitempty"`
	OOMKillDelta   int64      `json:"oom_kill_delta"`
	PlatformKilled bool       `json:"platform_killed"`
	EnvID          string     `json:"env_id,omitempty"`
	EnvStatus      string     `json:"env_status,omitempty"`
	CleanupState   string     `json:"cleanup_state,omitempty"`
	StoppedAt      *time.Time `json:"stopped_at,omitempty"`
	CleanupTries   int64      `json:"cleanup_tries"`
	CleanupError   string     `json:"cleanup_error,omitempty"`
}

type checkpointJSON struct {
	CheckpointID string    `json:"checkpoint_id"`
	StepID       string    `json:"step_id"`
	AttemptID    string    `json:"attempt_id"`
	CommitSeq    int64     `json:"commit_seq"`
	CommittedAt  time.Time `json:"committed_at"`
}

type tryJSON struct {
	TryNo     int64  `json:"try_no"`
	AttemptID string `json:"attempt_id"`
	EnvID     string `json:"env_id,omitempty"`
	State     string `json:"state"`
	Outcome   string `json:"outcome,omitempty"`
	LatencyMs int64  `json:"latency_ms"`
	CostMicro int64  `json:"cost_micro"`
	Error     string `json:"error,omitempty"`
}

type callJSON struct {
	CallID                    string    `json:"call_id"`
	Endpoint                  string    `json:"endpoint"`
	Model                     string    `json:"model,omitempty"` // chat 调用解析后的模型；其他端点省略
	State                     string    `json:"state"`
	Source                    string    `json:"source"`
	FirstAttemptID            string    `json:"first_attempt_id"`
	TriesUsed                 int64     `json:"tries_used"`
	CostChargedMicro          int64     `json:"cost_charged_micro"`
	UpstreamRequestID         string    `json:"upstream_request_id,omitempty"`
	ResultRef                 string    `json:"result_ref,omitempty"`
	FailReason                string    `json:"fail_reason,omitempty"`
	SupersedesCallID          string    `json:"supersedes_call_id,omitempty"`
	SupersedeReason           string    `json:"supersede_reason,omitempty"`
	PossibleExternalDuplicate bool      `json:"possible_external_duplicate"`
	CreatedAt                 time.Time `json:"created_at"`
	DeadlineAt                time.Time `json:"deadline_at"`
	Tries                     []tryJSON `json:"tries"`
}

type inspectionJSON struct {
	Task        taskJSON         `json:"task"`
	Attempts    []attemptJSON    `json:"attempts"`
	Checkpoints []checkpointJSON `json:"checkpoints"`
	Calls       []callJSON       `json:"calls"`
}

func (h *Handler) inspect(w http.ResponseWriter, r *http.Request) {
	in, err := h.cfg.Store.Inspect(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := inspectionJSON{Task: toTaskJSON(in.Task), Attempts: []attemptJSON{}, Checkpoints: []checkpointJSON{}, Calls: []callJSON{}}
	for _, a := range in.Attempts {
		out.Attempts = append(out.Attempts, attemptJSON{
			AttemptID: a.AttemptID, AttemptNo: a.AttemptNo, Status: a.Status, OutcomeClass: a.OutcomeClass,
			ExitCode: a.ExitCode, ExitSignal: a.ExitSignal, OOMKillDelta: a.OOMKillDelta, PlatformKilled: a.PlatformKilled,
			EnvID: a.EnvID, EnvStatus: a.EnvStatus, CleanupState: a.CleanupState, StoppedAt: a.StoppedAt,
			CleanupTries: a.CleanupTries, CleanupError: a.CleanupError,
		})
	}
	for _, c := range in.Checkpoints {
		out.Checkpoints = append(out.Checkpoints, checkpointJSON(c))
	}
	for _, c := range in.Calls {
		cj := callJSON{CallID: c.CallID, Endpoint: c.Endpoint, Model: c.Model, State: c.State, Source: c.Source, FirstAttemptID: c.FirstAttemptID,
			TriesUsed: c.TriesUsed, CostChargedMicro: c.CostChargedMicro, UpstreamRequestID: c.UpstreamRequestID,
			ResultRef: c.ResultRef, FailReason: c.FailReason, SupersedesCallID: c.SupersedesCallID, SupersedeReason: c.SupersedeReason,
			PossibleExternalDuplicate: c.PossibleExternalDuplicate, CreatedAt: c.CreatedAt, DeadlineAt: c.DeadlineAt, Tries: []tryJSON{}}
		for _, t := range c.Tries {
			cj.Tries = append(cj.Tries, tryJSON{TryNo: t.TryNo, AttemptID: t.AttemptID, EnvID: t.EnvID, State: t.State,
				Outcome: t.Outcome, LatencyMs: t.LatencyMs, CostMicro: t.CostMicro, Error: t.Error})
		}
		out.Calls = append(out.Calls, cj)
	}
	writeJSON(w, http.StatusOK, out)
}
