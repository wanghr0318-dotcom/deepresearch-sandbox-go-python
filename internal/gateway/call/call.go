package call

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// Coordinator 的流程（规格 §9.4、§9.5、§9.7、§11.2）：
//
//	解析（拒绝重复键）→ 指纹 → Tx1 BeginCall → 按已有记录分流（重放 / 进行中 / 指纹分歧 / unknown / failed）
//	→ 新登记的搜索与抓取：查缓存（事务外）→ 命中：校验 blob → Tx2 CompleteFromCache（无预留）→ 返回
//	→ 未命中：循环：等待并发槽位 → Tx2 ReserveTry → 上游 try → 先保存结果 blob 再 SettleTry → 可重试时退避
//	→ ok 结算之后异步写入缓存（准入与新鲜度由 cache 判定）
//
// 请求合并（§11.4）：新登记、未带 no-cache 的可缓存调用以 (task_id, attempt_id, subrun_id|"root", cache_key) 为键
// 在进程内 singleflight。已有同键的共享请求时作为 follower 加入（不查缓存、不预留、不访问上游）；否则查缓存，
// 未命中则成为 leader 执行上述 try 循环（leader 付费）。共享请求就是 leader 的后台执行：Coordinator 自有 context
// 与 leader 调用的 deadline_at，任何 waiter（含 leader 的 Worker）离开都不终止它；只有取消类原因取消它——
// 键限定在同一 attempt 内，CancelAttempt(attempt, ReasonCancel) 同时取消 leader 与全部 follower。共享请求结束后
// 每个 follower 各自经 CompleteFromCache 复查访问与期限并提交 journal（source = coalesced，无 try、无费用）；
// leader 失败时 follower 以同一原因置为 failed。no-cache 请求与模型调用从不加入或发起合并。
//
// journal 优先：已有记录（含 completed 的重放）从不查缓存，缓存结果也不会覆盖已记录的结果。模型调用从不缓存。
// Redis 的任何问题都只是未命中；PostgreSQL 或访问检查失败返回错误，不绕过（§11.2）。
//
// 一次逻辑调用的执行在 Coordinator 自有的后台 goroutine 中进行：Worker 的请求上下文取消或连接断开只让 Invoke
// 提前返回，不取消 try（继续至期限并结算，供重放，§9.1）；只有 CancelAttempt(attemptID, ReasonCancel) 与
// CancelSubrun（只限该 sub-run 的调用与共享请求，以 subrun_closed 结束）取消在途 try。
// 同一 (task_id, call_id) 在进程内同时只有一个执行者，其余请求得到 call_in_progress（不会两个 goroutine 同时进入 Tx2）。

// 本包产生的错误码（其余沿用 persistence.Code* 与 upstream.Code*）。
const (
	CodeDuplicateJSONKey      = "duplicate_json_key"      // 请求体含重复属性名（400）
	CodeEndpointNotConfigured = "endpoint_not_configured" // 该类别没有配置 adapter（404）
	CodeStoreUnavailable      = "store_unavailable"       // Tx2 因存储故障失败（fail_reason；503）
	CodeGatewayShutdown       = "gateway_shutdown"        // Coordinator 关闭时中止的调用（fail_reason；503）
	CodeBlobWriteFailed       = "blob_write_failed"       // 结果 blob 保存失败，try 按 unknown 结算（call_tries.error）
	CodeCallFailed            = "call_failed"             // 持久化失败但没有原因（不应出现）
)

// ReasonCancel 是取消类离开原因（task_control.desired = cancel 生效时的撤销）：只有它取消在途 try（§9.1 表）。
const ReasonCancel = "cancel"

// errNoSlot 是不等待的槽位获取（对冲）在槽位已满时的错误。
var errNoSlot = errors.New("call: 没有空闲的在途槽位")

// ErrClosed 表示 Coordinator 已关闭。
var ErrClosed = errors.New("call: coordinator 已关闭")

// errCancelRequested 是 CancelAttempt 取消在途 try 时的取消原因。
var errCancelRequested = errors.New("call: 任务已请求取消")

// errSubrunCancelRequested 是 CancelSubrun 取消在途 try 时的取消原因：与 errCancelRequested 同属取消类
// （同一结算路径，§9.1 表），调用以 subrun_closed 结束（Worker 得到 409）。
var errSubrunCancelRequested = errors.New("call: sub-run 已请求取消")

// Limits 是 Gateway 的调用限额（§19 默认值）。零值字段取默认值。调用期限在 Tx1 按端点类别选定并写入
// deadline_at，之后不再改变（deadlineFor）；模型调用单独设期限，因为推理模型的长输出可以合法地超过 120 s。
type Limits struct {
	CallDeadline        time.Duration // 搜索与抓取的调用期限，默认 120 s
	ModelCallDeadline   time.Duration // 模型调用（/v1/chat/completions）的调用期限，默认 300 s
	MaxTries            int           // 每逻辑调用累计 try，默认 3
	PerTaskInflight     int           // 每任务上游在途，默认 4
	PerProviderInflight int           // 每 provider 上游在途，默认 8
	PerSubrunInflight   int           // 每 sub-run 上游在途，默认 2（§9.7；root 调用不受它限制）
	BackoffBase         time.Duration // 退避基数，默认 2 s（翻倍）
	BackoffMax          time.Duration // 退避上限，默认 60 s
}

func (l Limits) withDefaults() Limits {
	if l.CallDeadline <= 0 {
		l.CallDeadline = DefaultCallDeadline
	}
	if l.ModelCallDeadline <= 0 {
		l.ModelCallDeadline = DefaultModelCallDeadline
	}
	if l.MaxTries <= 0 {
		l.MaxTries = 3
	}
	if l.PerTaskInflight <= 0 {
		l.PerTaskInflight = 4
	}
	if l.PerProviderInflight <= 0 {
		l.PerProviderInflight = 8
	}
	if l.PerSubrunInflight <= 0 {
		l.PerSubrunInflight = 2
	}
	if l.BackoffBase <= 0 {
		l.BackoffBase = 2 * time.Second
	}
	if l.BackoffMax <= 0 {
		l.BackoffMax = 60 * time.Second
	}
	return l
}

// 调用期限的默认值（§9、§19）：搜索与抓取 120 s；模型调用 300 s（推理模型的长输出）。
const (
	DefaultCallDeadline      = 120 * time.Second
	DefaultModelCallDeadline = 300 * time.Second
)

// deadlineFor 返回端点类别的调用期限（Tx1 写入 deadline_at = created_at + 它）：模型调用用 ModelCallDeadline，
// 其余用 CallDeadline。
func (l Limits) deadlineFor(k upstream.Kind) time.Duration {
	if k == upstream.KindChat {
		return l.ModelCallDeadline
	}
	return l.CallDeadline
}

// BlobStore 是 Coordinator 使用的内容寻址存储子集（internal/blob.Store 满足它）。
type BlobStore interface {
	Put(ctx context.Context, r io.Reader) (blob.Ref, error)
	Open(sha string) (io.ReadCloser, error)
}

// HostEvents 写入宿主事件 replay_divergence（由 Task 5 以 task.Store 的窄接口实现）。
type HostEvents interface {
	ReplayDivergence(ctx context.Context, taskID, attemptID, callID string, detail string) error
}

// CacheSource 是 call 对共享缓存的窄接口（§11.2；internal/gateway/cache.Source 实现）。它不记账、不授权，
// 自身的任何故障都表现为未命中；journal 提交由 Coordinator 完成。只用于搜索与抓取。
type CacheSource interface {
	// Lookup 查找 (kind, provider, ver, params) 的缓存条目；命中时 blobSHA 与 size 已通过 HMAC、expires_at
	// 与内容哈希校验。rawURL 是抓取的目标 URL（搜索为空），供按当前准入规则复查。
	Lookup(ctx context.Context, kind upstream.Kind, provider, ver string, params []byte, rawURL string) (blobSHA string, size int64, hit bool)
	// Store 在结果 completed 之后提交一次异步、可丢弃的写入；resp.Size 是结果 blob 的大小。
	Store(kind upstream.Kind, provider, ver string, params []byte, rawURL string, resp cache.Response, blobSHA string)
	// Bypass 记录一次 no-cache 请求跳过读取。
	Bypass()
	// CoalesceKey 返回调用的 cache_key（singleflight 合并键的一段，§11.4）；ok 为 false 表示不可缓存、不参与合并。
	CoalesceKey(kind upstream.Kind, provider, ver string, params []byte, rawURL string) (key string, ok bool)
	// Coalesced 记录一个 follower 加入共享请求（指标 coalesced，每个 follower 一次）。
	Coalesced()
}

// Config 装配 Coordinator。
type Config struct {
	Store    Store
	Adapters []upstream.Adapter // 每个 Kind 至多一个
	// Pricing 按类别给出价格表，用于由 Response.Usage 计算 ok 结算的实际费用（应与 adapter 的价格表一致）；
	// 缺少某类别时按该次估算结算（保守）。
	Pricing map[upstream.Kind]upstream.Pricing
	// ChatPricing 是 chat 按（解析后的）模型的价格表（与 chat adapter 的 PricingByModel 为同一份）；
	// 缺项的模型用 Pricing[KindChat]。
	ChatPricing map[string]upstream.Pricing
	Blobs       BlobStore
	Events      HostEvents
	Limits      Limits
	Logger      *slog.Logger     // nil 时不输出
	Now         func() time.Time // nil 时为 time.Now
	// StoreTimeout 是每个 Store 事务与 blob 写入的超时；≤ 0 时取 30 s。
	StoreTimeout time.Duration
	// Cache 是共享缓存（§11）；nil 表示关闭（全部走上游）。
	Cache CacheSource
	// Exec 装配 POST /v1/exec（§10，exec.go）；nil 表示关闭（404 endpoint_not_configured）。
	Exec *ExecConfig
	// Routing 配置模型降级链（routing.go）：只对实现 upstream.Router 且路由多于一条的 adapter 生效。
	Routing RoutingConfig
}

// Invoke 是一次 Worker 请求。
type Invoke struct {
	TaskID, AttemptID, EnvID    string
	CallID                      string
	Kind                        upstream.Kind
	Body                        []byte
	Retry                       bool // X-Agentbox-Retry
	Supersedes, SupersedeReason string
	NoCache                     bool // X-Agentbox-Cache: no-cache（计入指纹；不读缓存、不合并，结果仍写入）
	// SubrunID 是 X-Agentbox-Subrun（空为 root；edge 已校验 ID 规则与 call id 前缀 <subrun_id>/）。它进入
	// Tx1/Tx2 的访问检查与两层记账、singleflight 合并键（§11.4）与每 sub-run 在途槽位（§9.7）。
	SubrunID string
}

// Result 是返回给 edge 的结果。Status 是 HTTP 状态；Code 只在失败时非空（稳定错误码）。
// 成功时 Body 是结果正文，BlobSHA256 是结果 blob（X-Agentbox-Blob），Replayed 表示来自 journal 重放。
type Result struct {
	Body       []byte
	Replayed   bool
	BlobSHA256 string
	Status     int
	Code       string
	// ToolBudget 是搜索与抓取在任务有上限时的工具调用额度（X-Agentbox-Tool-Budget），含 429 与重放；其余为 nil。
	ToolBudget *ToolBudget
}

// Coordinator 是 Gateway 唯一的记账与 journal 所有者。
type Coordinator struct {
	store        Store
	adapters     map[upstream.Kind]upstream.Adapter
	pricing      map[upstream.Kind]upstream.Pricing
	chatPricing  map[string]upstream.Pricing
	blobs        BlobStore
	events       HostEvents
	limits       Limits
	log          *slog.Logger
	now          func() time.Time
	storeTimeout time.Duration
	cache        CacheSource
	exec         *ExecConfig // 已取默认值；nil 表示未配置 exec
	execInputMax int64       // exec 输入累计上限（MaxExecInputBytes；测试可调小）
	routing      RoutingConfig
	routes       map[upstream.Kind]*routeSet // 多路由的类别（模型降级链）；单供应商的类别不在其中

	root context.Context
	stop context.CancelFunc
	wg   sync.WaitGroup

	mu        sync.Mutex
	busy      map[callKey]bool
	byAttempt map[string]map[*job]bool
	bySubrun  map[subrunKey]map[*job]bool // 只含 sub-run 调用（CancelSubrun）
	subSem    map[string]*semaphore       // 键 subrunSemKey(task_id, subrun_id)
	taskSem   map[string]*semaphore
	provSem   map[string]*semaphore
	flights   map[flightKey]*flight
	// exec 的进程内执行（任何撤销原因都终止它们，D7）。
	execByAttempt map[string]map[*execJob]bool
	execBySubrun  map[subrunKey]map[*execJob]bool
}

type callKey struct{ taskID, callID string }

// subrunKey 标识一个 attempt 内某 sub-run 的在途执行（CancelSubrun 的范围）。
type subrunKey struct{ taskID, attemptID, subrunID string }

// subrunSemKey 是每 sub-run 在途槽位的键 (task_id, subrun_id)：同一逻辑 sub-run 跨 attempt 共用上限。
func subrunSemKey(taskID, subrunID string) string { return taskID + "\x00" + subrunID }

// flightKey 是 singleflight 合并键（§11.4）：不跨任务、attempt、sub-run。
type flightKey struct{ taskID, attemptID, subrun, cacheKey string }

// flight 是一个进行中的共享请求（leader 的执行）。done 关闭之后 res 与 err 只读。
type flight struct {
	done chan struct{}
	res  Result
	err  error
}

type semaphore struct {
	ch   chan struct{}
	refs int
}

// job 是一次逻辑调用在本进程中的执行。
type job struct {
	in       Invoke
	ad       upstream.Adapter
	resolved []byte
	model    string // 解析后的模型（chat）；按它取价格表结算
	fp       string
	est      int64
	ctx      context.Context // Coordinator 自有；CancelAttempt / CancelSubrun 以取消类原因取消
	cancel   context.CancelCauseFunc
}

// New 构造 Coordinator。
func New(cfg Config) (*Coordinator, error) {
	if cfg.Store == nil || cfg.Blobs == nil || cfg.Events == nil {
		return nil, errors.New("call: Store、Blobs、Events 必填")
	}
	c := &Coordinator{
		store:         cfg.Store,
		adapters:      map[upstream.Kind]upstream.Adapter{},
		pricing:       cfg.Pricing,
		chatPricing:   cfg.ChatPricing,
		blobs:         cfg.Blobs,
		events:        cfg.Events,
		limits:        cfg.Limits.withDefaults(),
		log:           cfg.Logger,
		now:           cfg.Now,
		storeTimeout:  cfg.StoreTimeout,
		cache:         cfg.Cache,
		busy:          map[callKey]bool{},
		byAttempt:     map[string]map[*job]bool{},
		bySubrun:      map[subrunKey]map[*job]bool{},
		subSem:        map[string]*semaphore{},
		taskSem:       map[string]*semaphore{},
		provSem:       map[string]*semaphore{},
		flights:       map[flightKey]*flight{},
		execInputMax:  MaxExecInputBytes,
		execByAttempt: map[string]map[*execJob]bool{},
		execBySubrun:  map[subrunKey]map[*execJob]bool{},
	}
	if cfg.Exec != nil {
		x, err := cfg.Exec.withDefaults()
		if err != nil {
			return nil, err
		}
		c.exec = &x
	}
	for _, a := range cfg.Adapters {
		if _, dup := c.adapters[a.Kind()]; dup {
			return nil, fmt.Errorf("call: 类别 %s 有多个 adapter", a.Kind())
		}
		c.adapters[a.Kind()] = a
	}
	if c.log == nil {
		c.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.storeTimeout <= 0 {
		c.storeTimeout = 30 * time.Second
	}
	c.routing = cfg.Routing.withDefaults()
	c.routes = map[upstream.Kind]*routeSet{}
	for k, a := range c.adapters {
		if rs := c.newRouteSet(a); rs != nil {
			c.routes[k] = rs
		}
	}
	c.root, c.stop = context.WithCancel(context.Background())
	return c, nil
}

// Close 取消全部在途 try 并等待它们结算完毕。
func (c *Coordinator) Close() {
	c.mu.Lock()
	c.stop()
	c.mu.Unlock()
	c.wg.Wait()
}

// endpointOf 是类别对应的 Gateway 端点（进入指纹与日志）。
func endpointOf(k upstream.Kind) string {
	switch k {
	case upstream.KindChat:
		return "/v1/chat/completions"
	case upstream.KindSearch:
		return "/v1/search"
	case upstream.KindFetch:
		return "/v1/fetch"
	}
	return "/v1/" + string(k)
}

// modelOf 取规范化请求体中的 model（没有时为空，例如搜索与抓取）。
func modelOf(resolved []byte) string {
	var v struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(resolved, &v)
	return v.Model
}

func reject(code string) Result { return Result{Status: statusFor(code), Code: code} }

// statusFor 把错误码映射为 HTTP 状态（用于 Gateway 自身的拒绝与持久化失败的重放）。
func statusFor(code string) int {
	switch code {
	case persistence.CodeAccessRevoked, upstream.CodeEgressBlocked, CodeInputNotAuthorized:
		return 403
	case persistence.CodeNotCurrentAttempt, persistence.CodeCancelRequested,
		persistence.CodeFingerprintMismatch, persistence.CodeCallInProgress, persistence.CodeSubrunClosed, CodeExecCancelled:
		return 409
	case persistence.CodeBudgetExhausted, persistence.CodeBudgetInsufficient, persistence.CodeSubrunBudgetExhausted,
		CodeExecQuotaExhausted, CodeExecCPUExhausted, CodeExecWallExhausted, CodeExecBlocked:
		return 402
	case persistence.CodeTriesExhausted, persistence.CodeToolBudgetExhausted:
		return 429
	case persistence.CodeCallDeadlineExceeded, CodeExecQueueTimeout:
		return 504
	case CodeDuplicateJSONKey, upstream.CodeInvalidRequest, upstream.CodeUnsupportedField,
		upstream.CodeUnsupportedModel, upstream.CodeUnsupportedProvider, upstream.CodeInvalidURL, CodeInputsTooLarge:
		return 400
	case CodeEndpointNotConfigured:
		return 404
	case CodeStoreUnavailable, CodeGatewayShutdown, CodeExecEnvUnavailable, CodeModelDegraded:
		return 503
	}
	return 502 // 含 exec_start_failed、exec_unknown
}

// retryableReason 报告 failed 调用的原因是否属于可重试类别（§9.4：带 X-Agentbox-Retry 时可在上限内新建 try）：
// 上游的暂时失败、预算或访问在 Tx2 被拒、存储故障与关闭中止。期限与次数耗尽、上游明确拒绝不可重试；
// subrun_closed 不可重试（sub-run 不会重新打开），sub-run 层预算耗尽与 task 层相同可重试（并发预留释放后可能恢复）。
// exec（§10.4、D6）：exec_cancelled 可重试（新 attempt 带 Retry 重跑）；排队超时、环境不可用与启动失败可重试
// （环境创建失败也按 start_failed 结算；启动失败不计 exec_count，重跑受累计 try 上限约束）；次数与 CPU 配额
// 可能在在途 exec 结算后恢复，与预算同样可重试；wall 配额与 blocked 不会恢复，不可重试。
func retryableReason(reason string) bool {
	switch reason {
	case upstream.CodeUpstreamRateLimited, upstream.CodeUpstreamUnavailable, upstream.CodeUpstreamUnreachable,
		persistence.CodeBudgetExhausted, persistence.CodeBudgetInsufficient, persistence.CodeSubrunBudgetExhausted,
		persistence.CodeAccessRevoked, persistence.CodeNotCurrentAttempt, persistence.CodeCancelRequested,
		CodeStoreUnavailable, CodeGatewayShutdown, CodeModelDegraded,
		CodeExecCancelled, CodeExecQueueTimeout, CodeExecEnvUnavailable, CodeExecStartFailed,
		CodeExecQuotaExhausted, CodeExecCPUExhausted:
		return true
	}
	return false
}

// admit 是规格 §9.2 的访问判定，顺序与持久化层一致：访问撤销 → 非当前 attempt → 已请求取消 → sub-run 不可用。
func admit(f AccessFacts) string {
	switch {
	case !f.Active:
		return persistence.CodeAccessRevoked
	case !f.Current:
		return persistence.CodeNotCurrentAttempt
	case f.Desired == "cancel":
		return persistence.CodeCancelRequested
	case f.SubrunID != "" && !f.SubrunOpen:
		return persistence.CodeSubrunClosed
	}
	return ""
}

// rejection 把 Store 的 *persistence.RejectedError 转为结果。
func rejection(err error) (Result, bool) {
	var rej *persistence.RejectedError
	if errors.As(err, &rej) {
		return reject(rej.Code), true
	}
	return Result{}, false
}

func (c *Coordinator) opCtx() (context.Context, context.CancelFunc) {
	// 记账与 journal 写入不随 Worker 或取消而中断（撤销不阻止宿主完成记账，§9.5）。
	return context.WithTimeout(context.Background(), c.storeTimeout)
}

// CheckAccess 在一致快照中做 §9.2 访问检查（供 /v1/budget 与 blob 读取使用）。零值 Result 表示允许。
// subrunID 非空（请求带 X-Agentbox-Subrun）时另须该 sub-run 属于本任务、绑定本 attempt 且为 started，
// 否则 409 subrun_closed。
func (c *Coordinator) CheckAccess(ctx context.Context, taskID, attemptID, subrunID string) (Result, error) {
	f, err := c.store.CheckAccess(ctx, taskID, attemptID, subrunID)
	if err != nil {
		return Result{}, err
	}
	if code := admit(f); code != "" {
		return reject(code), nil
	}
	return Result{}, nil
}

// Budget 返回任务账本（/v1/budget）。
func (c *Coordinator) Budget(ctx context.Context, taskID string) (Budget, error) {
	return c.store.LoadBudget(ctx, taskID)
}

// SubrunBudget 返回 sub-run 层账本，供 GET /v1/budget（带 X-Agentbox-Subrun 时附加 sub-run 层）；
// 不存在为 persistence.ErrNotFound。
func (c *Coordinator) SubrunBudget(ctx context.Context, taskID, subrunID string) (SubrunBudget, error) {
	return c.store.LoadSubrunBudget(ctx, taskID, subrunID)
}

// OpenBlob 只读打开任务 scope 内的 blob：sha 须在 scope_blobs(task) 中（Store.BlobAuthorized；调用结果由
// 结算事务加入）。不存在与未授权都返回 blob.ErrNotFound，不泄露存在性。
func (c *Coordinator) OpenBlob(ctx context.Context, taskID, sha string) (io.ReadCloser, error) {
	ok, err := c.store.BlobAuthorized(ctx, taskID, sha)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, blob.ErrNotFound
	}
	return c.blobs.Open(sha)
}

// CancelAttempt 由 edge 在撤销 attempt 时调用。只有 reason == ReasonCancel 取消该 attempt 的在途 try
// （按"未发出释放 / 有 usage 按实际 / 无法确认转 unknown"结算）；其他离开原因的 try 继续至期限并结算（§9.1）。
// exec 不看原因（§10.4、D7）：任何原因都终止该 attempt 的全部 exec（停止执行树 → cancelled；尚未启动的不再启动）。
func (c *Coordinator) CancelAttempt(attemptID, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for j := range c.execByAttempt[attemptID] {
		j.cancel(errExecCancelled)
	}
	if reason != ReasonCancel {
		return
	}
	for j := range c.byAttempt[attemptID] {
		j.cancel(errCancelRequested)
	}
}

// CancelSubrun 取消 attempt 内 sub-run subrunID 的全部在途执行（规格 §13.4；取消类原因，§9.1 表）：在途 try 按
// "未发出释放 / 有 usage 按实际 / 无法确认转 unknown"结算，等待 sub-run 或 task 槽位的调用不再预留；该 sub-run
// 的共享请求（合并键含 subrun_id，§11.4）随 leader 取消，follower 以同一原因失败。这些调用以 subrun_closed 结束
// （Worker 得到 409）。其他 sub-run 与 root 的调用不受影响。幂等；没有在途执行时无操作。
//
// 调用方（宿主取消 sub-run 的路径，以及 Plan 15 的 exec 协调）须先在同一事务中把 sub-run 置为 cancel_requested：
// 之后到达的请求由 Store 的访问复查以 subrun_closed 拒绝，本方法只负责已在进程内执行的调用。
//
// 该 sub-run 的 exec（D16）同样被终止：停止执行树后按 cancelled 结算（journal failed{exec_cancelled}，Worker 得到
// 409 exec_cancelled），尚未启动的不再启动。
func (c *Coordinator) CancelSubrun(taskID, attemptID, subrunID string) {
	if subrunID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := subrunKey{taskID, attemptID, subrunID}
	for j := range c.bySubrun[k] {
		j.cancel(errSubrunCancelRequested)
	}
	for j := range c.execBySubrun[k] {
		j.cancel(errExecCancelled)
	}
}

// Invoke 执行一次计费调用（§9.4–§9.7）。Gateway 的拒绝与上游失败以 Result.Status/Code 返回、err 为 nil；
// err 非空表示存储等内部故障，或 ctx 结束（此时调用仍在后台继续并结算）。
func (c *Coordinator) Invoke(ctx context.Context, in Invoke) (Result, error) {
	if in.TaskID == "" || in.AttemptID == "" || in.CallID == "" {
		return Result{}, fmt.Errorf("%w: Invoke 缺少 task_id、attempt_id 或 call_id", persistence.ErrInvalid)
	}
	if c.root.Err() != nil {
		return Result{}, ErrClosed
	}
	ad, ok := c.adapters[in.Kind]
	if !ok {
		return reject(CodeEndpointNotConfigured), nil
	}
	// 1. 解析：拒绝重复属性名（§9.4），再由 adapter 补默认并拒绝不支持的字段。
	if _, err := jcs.Canonical(json.RawMessage(in.Body)); err != nil {
		if errors.Is(err, jcs.ErrDuplicateKey) {
			return reject(CodeDuplicateJSONKey), nil
		}
		return reject(upstream.CodeInvalidRequest), nil
	}
	resolved, defaults, err := ad.Resolve(in.Body)
	if err != nil {
		var ue *upstream.Error
		if errors.As(err, &ue) {
			return Result{Status: ue.Status, Code: ue.Code}, nil
		}
		return reject(upstream.CodeInvalidRequest), nil
	}
	model := modelOf(resolved)
	directive := ""
	if in.NoCache {
		directive = CacheDirectiveNoCache
	}
	fp, err := Fingerprint(endpointOf(in.Kind), ad.Version(), ad.Provider(), model, defaults, directive, in.Body)
	if err != nil {
		return reject(upstream.CodeInvalidRequest), nil
	}
	est, err := ad.Estimate(resolved)
	if err != nil {
		return reject(upstream.CodeInvalidRequest), nil
	}

	key := callKey{in.TaskID, in.CallID}
	if !c.claim(key) {
		return c.contended(ctx, in, fp)
	}
	j := &job{in: in, ad: ad, resolved: resolved, model: model, fp: fp, est: est}
	j.ctx, j.cancel = context.WithCancelCause(c.root)
	if !c.register(j) {
		j.cancel(nil)
		c.unclaim(key)
		return Result{}, ErrClosed
	}
	type outcome struct {
		res Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		defer c.wg.Done()
		r, err := c.run(j)
		// 先释放进程内占用，再交付结果：调用方收到结果后立即以同 ID 重放不会得到 call_in_progress。
		j.cancel(nil)
		c.unregister(j)
		c.unclaim(key)
		done <- outcome{r, err}
	}()
	select {
	case o := <-done:
		return o.res, o.err
	case <-ctx.Done():
		return Result{}, ctx.Err() // 调用在后台继续至期限并结算，供同 ID 重放
	}
}

func (c *Coordinator) claim(k callKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy[k] {
		return false
	}
	c.busy[k] = true
	return true
}

func (c *Coordinator) unclaim(k callKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.busy, k)
}

// register 登记后台执行（供 CancelAttempt 与 Close）；已关闭时返回 false。wg.Add 与 Close 的 stop 在同一把锁下互斥。
func (c *Coordinator) register(j *job) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.root.Err() != nil {
		return false
	}
	c.wg.Add(1)
	m := c.byAttempt[j.in.AttemptID]
	if m == nil {
		m = map[*job]bool{}
		c.byAttempt[j.in.AttemptID] = m
	}
	m[j] = true
	if j.in.SubrunID != "" {
		sk := subrunKey{j.in.TaskID, j.in.AttemptID, j.in.SubrunID}
		sm := c.bySubrun[sk]
		if sm == nil {
			sm = map[*job]bool{}
			c.bySubrun[sk] = sm
		}
		sm[j] = true
	}
	return true
}

func (c *Coordinator) unregister(j *job) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.byAttempt[j.in.AttemptID]
	delete(m, j)
	if len(m) == 0 {
		delete(c.byAttempt, j.in.AttemptID)
	}
	if j.in.SubrunID != "" {
		sk := subrunKey{j.in.TaskID, j.in.AttemptID, j.in.SubrunID}
		sm := c.bySubrun[sk]
		delete(sm, j)
		if len(sm) == 0 {
			delete(c.bySubrun, sk)
		}
	}
}

// contended 处理进程内已有执行者的同 ID 请求：仍按 §9.2 检查访问；指纹不同报告分歧，否则 call_in_progress。
// 搜索与抓取的响应带当前工具调用额度（读取失败时省略）。
func (c *Coordinator) contended(ctx context.Context, in Invoke, fp string) (res Result, err error) {
	if cacheable(in.Kind) {
		defer func() {
			if b, berr := c.store.LoadBudget(ctx, in.TaskID); berr == nil {
				res.ToolBudget = b.ToolBudget()
			}
		}()
	}
	if r, err := c.CheckAccess(ctx, in.TaskID, in.AttemptID, in.SubrunID); err != nil || r.Code != "" {
		return r, err
	}
	rec, _, err := c.store.LoadCall(ctx, in.TaskID, in.CallID)
	if err == nil && rec.Fingerprint != fp {
		return c.diverge(in.TaskID, in.AttemptID, in.CallID, endpointOf(in.Kind), rec.Fingerprint, fp), nil
	}
	return reject(persistence.CodeCallInProgress), nil
}

// diverge 写入 host 事件 replay_divergence 并返回 409 fingerprint_mismatch。
func (c *Coordinator) diverge(taskID, attemptID, callID, endpoint, stored, got string) Result {
	ctx, cancel := c.opCtx()
	defer cancel()
	detail := fmt.Sprintf("endpoint=%s stored_fingerprint=%s request_fingerprint=%s", endpoint, stored, got)
	if err := c.events.ReplayDivergence(ctx, taskID, attemptID, callID, detail); err != nil {
		c.log.Warn("gateway: 写入 replay_divergence 失败", "task_id", taskID, "attempt_id", attemptID,
			"call_id", callID, "err", err)
	}
	return reject(persistence.CodeFingerprintMismatch)
}

// run 是一次逻辑调用的后台执行：Tx1 与按已有记录分流，然后进入 try 循环。Tx1 给出的工具调用额度随每个结果返回
// （含 tool_budget_exhausted 的 429 与重放）。
func (c *Coordinator) run(j *job) (out Result, outErr error) {
	in := j.in
	ctx, cancel := c.opCtx()
	res, err := c.store.BeginCall(ctx, BeginCallRequest{
		TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID, Fingerprint: j.fp, Endpoint: endpointOf(in.Kind),
		Deadline: c.limits.deadlineFor(in.Kind), SupersedesCallID: in.Supersedes, SupersedeReason: in.SupersedeReason,
		Model: j.model, SubrunID: in.SubrunID,
	})
	cancel()
	if tb := res.ToolBudget; tb != nil {
		defer func() { out.ToolBudget = tb }()
	}
	if err != nil {
		if r, ok := rejection(err); ok {
			return r, nil
		}
		return Result{}, err
	}
	rec := res.Record
	if res.Existing {
		if rec.Fingerprint != j.fp {
			return c.diverge(in.TaskID, in.AttemptID, in.CallID, endpointOf(in.Kind), rec.Fingerprint, j.fp), nil
		}
		switch rec.State {
		case StateCompleted:
			return c.replay(rec)
		case StateResolving, StateInFlight:
			return reject(persistence.CodeCallInProgress), nil
		case StateUnknown:
			// 按不确定结果策略，在累计上限内新建 try（ReserveTry 检查上限）。
		case StateFailed:
			if !in.Retry || !retryableReason(rec.FailReason) || rec.TriesUsed >= c.limits.MaxTries {
				return persistedFailure(rec), nil
			}
		default:
			return Result{}, fmt.Errorf("call: 调用 %s/%s 的状态 %q 未知", rec.TaskID, rec.CallID, rec.State)
		}
	} else if c.cache != nil && cacheable(in.Kind) {
		// 只有本次新登记（或接管复位）的 resolving 调用查缓存或合并：已有记录一律按 journal 处理（journal 优先）。
		return c.resolveCacheable(j, rec)
	}
	return c.execute(j, rec)
}

// resolveCacheable 是新登记的搜索与抓取调用的分支（§11.2、§11.4）：no-cache 跳过读取与合并，直接走上游；
// 否则先加入同键的共享请求，没有则查缓存，未命中再成为 leader（查缓存期间别人已成为 leader 时加入它）。
func (c *Coordinator) resolveCacheable(j *job, rec CallRecord) (Result, error) {
	in := j.in
	if in.NoCache {
		c.cache.Bypass()
		return c.execute(j, rec)
	}
	ck, coalescible := c.cache.CoalesceKey(in.Kind, j.ad.Provider(), j.ad.Version(), j.resolved, fetchURL(in.Kind, j.resolved))
	subrun := in.SubrunID
	if subrun == "" {
		subrun = "root"
	}
	fk := flightKey{in.TaskID, in.AttemptID, subrun, ck}
	if coalescible {
		if f, leader := c.flightFor(fk, false); f != nil && !leader {
			return c.follow(j, rec, f)
		}
	}
	if r, done, err := c.resolveFromCache(j); done {
		return r, err
	}
	if !coalescible {
		return c.execute(j, rec)
	}
	f, leader := c.flightFor(fk, true)
	if !leader {
		return c.follow(j, rec, f)
	}
	return c.lead(j, rec, fk, f)
}

// flightFor 返回 fk 的共享请求：已存在时 leader 为 false；不存在且 create 时新建并由调用方作为 leader 执行，
// 不存在且不 create 时返回 nil。
func (c *Coordinator) flightFor(fk flightKey, create bool) (*flight, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f := c.flights[fk]; f != nil {
		return f, false
	}
	if !create {
		return nil, false
	}
	f := &flight{done: make(chan struct{})}
	c.flights[fk] = f
	return f, true
}

// lead 以 leader 身份执行共享请求（Coordinator 自有 context 与本调用的 deadline_at），结束后先移除该键再唤醒
// follower：之后到达的同键请求不再加入已结束的请求，而是查缓存或发起新的共享请求。
func (c *Coordinator) lead(j *job, rec CallRecord, fk flightKey, f *flight) (res Result, err error) {
	defer func() {
		c.mu.Lock()
		delete(c.flights, fk)
		c.mu.Unlock()
		f.res, f.err = res, err
		close(f.done)
	}()
	return c.execute(j, rec)
}

// follow 以 follower 身份等待共享请求（§11.4）。等待受本调用自身的 context（取消类原因、关闭）与 deadline_at
// 约束；共享请求成功后经 CompleteFromCache 复查访问与期限并提交 journal（source = coalesced，无 try、无费用），
// 被拒（例如取消先提交、attempt 已被替换）或存储故障时按缓存命中被拒的方式置为 failed。共享请求失败时本调用
// 以同一原因置为 failed 并返回 leader 的状态与错误码（leader 的存储故障按可重试的 store_unavailable）。
func (c *Coordinator) follow(j *job, rec CallRecord, f *flight) (Result, error) {
	in := j.in
	c.cache.Coalesced()
	ctx, cancel := context.WithDeadline(j.ctx, rec.DeadlineAt)
	defer cancel()
	select {
	case <-f.done:
	case <-ctx.Done():
		return c.giveUp(j, StateResolving, c.stopCode(ctx, rec.DeadlineAt))
	}
	lr, lerr := f.res, f.err
	switch {
	case lerr != nil:
		return c.giveUp(j, StateResolving, CodeStoreUnavailable)
	case lr.Code != "" || lr.BlobSHA256 == "":
		code := lr.Code
		if code == "" {
			code = CodeCallFailed
		}
		r, err := c.giveUp(j, StateResolving, code)
		if err != nil {
			return r, err
		}
		if lr.Status != 0 {
			r.Status = lr.Status
		}
		return r, nil
	}
	sctx, scancel := c.opCtx()
	_, err := c.store.CompleteFromCache(sctx, CacheCompletion{TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID,
		ResultSHA256: lr.BlobSHA256, ResultSize: int64(len(lr.Body)), Source: SourceCoalesced, SubrunID: in.SubrunID})
	scancel()
	if err != nil {
		var rej *persistence.RejectedError
		if errors.As(err, &rej) {
			return c.giveUp(j, StateResolving, rej.Code)
		}
		if _, ferr := c.giveUp(j, StateResolving, CodeStoreUnavailable); ferr != nil {
			c.log.Warn("gateway: 合并结果提交失败后无法把调用置为 failed", "task_id", in.TaskID, "call_id", in.CallID, "err", ferr)
		}
		return Result{}, err
	}
	c.log.Info("gateway: coalesced", "task_id", in.TaskID, "attempt_id", in.AttemptID, "call_id", in.CallID,
		"endpoint", endpointOf(in.Kind), "provider", j.ad.Provider(), "blob", lr.BlobSHA256)
	return Result{Body: lr.Body, BlobSHA256: lr.BlobSHA256, Status: 200}, nil
}

// cacheable 报告类别是否使用共享缓存（§11.1：公开搜索结果与抓取页面；不缓存模型调用）。
func cacheable(k upstream.Kind) bool { return k == upstream.KindSearch || k == upstream.KindFetch }

// fetchURL 取抓取请求（Resolve 之后）的目标 URL；其他类别为空。
func fetchURL(k upstream.Kind, resolved []byte) string {
	if k != upstream.KindFetch {
		return ""
	}
	var v struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(resolved, &v) != nil {
		return ""
	}
	return v.URL
}

// resolveFromCache 是缓存分支（§11.2）：查缓存（事务外）→ 命中则读取并复核结果 blob → Tx2 CompleteFromCache。
// done 为 false 表示未命中（含 Redis 故障与 blob 复核失败），调用方照常走上游（no-cache 不到这里）。
// Tx2 被拒（取消先提交、访问撤销、期限已过）或存储故障时与 ReserveTry 失败的处理相同：调用置为 failed
// （存储故障为可重试的 store_unavailable），结果不被授权（E23）。
func (c *Coordinator) resolveFromCache(j *job) (Result, bool, error) {
	in := j.in
	sha, size, hit := c.cache.Lookup(j.ctx, in.Kind, j.ad.Provider(), j.ad.Version(), j.resolved, fetchURL(in.Kind, j.resolved))
	if !hit {
		return Result{}, false, nil
	}
	body, err := c.readResult(sha, size)
	if err != nil {
		c.log.Warn("gateway: 缓存命中的结果 blob 复核失败，按未命中处理", "task_id", in.TaskID, "call_id", in.CallID, "err", err)
		return Result{}, false, nil
	}
	ctx, cancel := c.opCtx()
	_, err = c.store.CompleteFromCache(ctx, CacheCompletion{TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID,
		ResultSHA256: sha, ResultSize: size, Source: SourceCache, SubrunID: in.SubrunID})
	cancel()
	if err != nil {
		var rej *persistence.RejectedError
		if errors.As(err, &rej) {
			r, ferr := c.giveUp(j, StateResolving, rej.Code)
			return r, true, ferr
		}
		if _, ferr := c.giveUp(j, StateResolving, CodeStoreUnavailable); ferr != nil {
			c.log.Warn("gateway: 缓存命中提交失败后无法把调用置为 failed", "task_id", in.TaskID, "call_id", in.CallID, "err", ferr)
		}
		return Result{}, true, err
	}
	c.log.Info("gateway: cache hit", "task_id", in.TaskID, "attempt_id", in.AttemptID, "call_id", in.CallID,
		"endpoint", endpointOf(in.Kind), "provider", j.ad.Provider(), "blob", sha)
	return Result{Body: body, BlobSHA256: sha, Status: 200}, true, nil
}

// readResult 读取结果 blob，并确认其大小与 sha256 与期望一致（命中的内容在返回给 Worker 前再核对一次）。
func (c *Coordinator) readResult(sha string, size int64) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("call: blob %s 的大小 %d 无效", sha, size)
	}
	rc, err := c.blobs.Open(sha)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, size+1))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	if int64(len(body)) != size || hex.EncodeToString(sum[:]) != sha {
		return nil, fmt.Errorf("call: blob %s 的内容与缓存条目不一致", sha)
	}
	return body, nil
}

// offerCache 在 ok 结算使调用 completed 之后，把结果交给缓存（异步、可丢弃；准入与新鲜度由 cache 判定）。
// 搜索按成功结果缓存（状态 200，寿命为 adapter TTL）；抓取需要目标站点的 HTTP 元数据，截断的正文不缓存。
func (c *Coordinator) offerCache(j *job, resp upstream.Response, ref blob.Ref, latency time.Duration) {
	if c.cache == nil || !cacheable(j.in.Kind) {
		return
	}
	var r cache.Response
	switch j.in.Kind {
	case upstream.KindSearch:
		end := c.now()
		r = cache.Response{Status: 200, Size: ref.Size, RequestTime: end.Add(-latency), ResponseTime: end}
	case upstream.KindFetch:
		m := resp.HTTP
		if m == nil || m.Truncated {
			return
		}
		r = cache.Response{Status: m.Status, Header: map[string][]string(m.Header), Size: ref.Size,
			RequestTime: m.RequestTime, ResponseTime: m.ResponseTime}
	}
	c.cache.Store(j.in.Kind, j.ad.Provider(), j.ad.Version(), j.resolved, fetchURL(j.in.Kind, j.resolved), r, ref.SHA256)
}

func persistedFailure(rec CallRecord) Result {
	if rec.FailReason == "" {
		return reject(CodeCallFailed)
	}
	return reject(rec.FailReason)
}

// replay 返回 completed 调用的持久化结果（X-Agentbox-Replayed: true）。
func (c *Coordinator) replay(rec CallRecord) (Result, error) {
	rc, err := c.blobs.Open(rec.ResultRef)
	if err != nil {
		return Result{}, fmt.Errorf("call: 打开调用 %s 的结果 blob: %w", rec.CallID, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return Result{}, fmt.Errorf("call: 读取调用 %s 的结果 blob: %w", rec.CallID, err)
	}
	return Result{Body: body, Replayed: true, BlobSHA256: rec.ResultRef, Status: 200}, nil
}

// stopCode 报告 try 循环是否必须停止及其原因：取消 → cancel_requested；sub-run 取消 → subrun_closed；
// 到期 → call_deadline_exceeded；关闭 → gateway_shutdown。
func (c *Coordinator) stopCode(ctx context.Context, deadline time.Time) string {
	switch {
	case errors.Is(context.Cause(ctx), errCancelRequested):
		return persistence.CodeCancelRequested
	case errors.Is(context.Cause(ctx), errSubrunCancelRequested):
		return persistence.CodeSubrunClosed
	case !c.now().Before(deadline) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return persistence.CodeCallDeadlineExceeded
	case c.root.Err() != nil:
		return CodeGatewayShutdown
	case ctx.Err() != nil:
		return persistence.CodeCallDeadlineExceeded
	}
	return ""
}

// giveUp 结束调用而不再新建 try：没有持有预留的调用置为 failed（原因即 code）；unknown 调用保持 unknown
// （费用已转入 unknown，状态如实反映无法确认的结果，之后在上限内仍可新建 try）。
func (c *Coordinator) giveUp(j *job, state CallState, code string) (Result, error) {
	if state != StateUnknown {
		ctx, cancel := c.opCtx()
		err := c.store.FailCall(ctx, j.in.TaskID, j.in.CallID, code)
		cancel()
		if err != nil && !errors.Is(err, persistence.ErrRejected) && !errors.Is(err, persistence.ErrConflict) {
			return Result{}, err
		}
	}
	return reject(code), nil
}

// execute 是 try 循环（§9.5、§9.7）。期限来自持久化的 deadline_at（等待槽位、退避与全部 try 都计入）。
// 多路由的 adapter（模型降级链，routing.go）每次 try 先选路由：可重试或 unknown 的失败之后立即换下一个就绪的
// 路由，本轮全部失败才退避；没有任何路由放行时以 model_degraded 失败（不预留）。单供应商与之前完全相同。
func (c *Coordinator) execute(j *job, rec CallRecord) (Result, error) {
	in := j.in
	deadline := rec.DeadlineAt
	ctx, cancel := context.WithDeadline(j.ctx, deadline)
	defer cancel()
	state := rec.State
	rs := c.routes[in.Kind]
	tried := map[int]bool{} // 本轮已失败的路由
	var lastCode string
	var lastRetryAfter time.Duration
	for n := 1; ; n++ {
		if code := c.stopCode(ctx, deadline); code != "" {
			return c.giveUp(j, state, code)
		}
		l := leg{route: -1, est: j.est}
		if rs != nil {
			p := rs.pick(j, tried)
			switch {
			case p.ok:
				l = p.leg
			case p.anyTried:
				// 本轮剩余的路由在失败之后、选择之前打开了：本轮结束，退避后开始新一轮。
				tried = map[int]bool{}
				if r, stop, err := c.waitBackoff(ctx, j, deadline, state, n, lastRetryAfter, lastCode); stop {
					return r, err
				}
				continue
			default:
				c.log.Warn("gateway: degraded", "task_id", in.TaskID, "attempt_id", in.AttemptID, "call_id", in.CallID,
					"endpoint", endpointOf(in.Kind), "model", j.model, "routes", rs.states(), "route_skipped", p.leg.skipped)
				return c.giveUp(j, state, CodeModelDegraded)
			}
		}
		release, err := c.acquire(ctx, in.TaskID, in.SubrunID, j.semKey(l), false)
		if err != nil {
			rs.release(l)
			return c.giveUp(j, state, c.stopCode(ctx, deadline))
		}
		// 2. Tx2：复查访问、期限、累计次数与预算（两层）后预留；提交之后才发起上游请求（第一个原子提交点）。
		try, err := c.reserve(j, l)
		if err != nil {
			release()
			rs.release(l)
			var rej *persistence.RejectedError
			if errors.As(err, &rej) {
				if rej.Code == persistence.CodeCallInProgress {
					return reject(rej.Code), nil
				}
				return c.giveUp(j, state, rej.Code)
			}
			if _, ferr := c.giveUp(j, state, CodeStoreUnavailable); ferr != nil {
				c.log.Warn("gateway: Tx2 失败后无法把调用置为 failed", "task_id", in.TaskID, "call_id", in.CallID, "err", ferr)
			}
			return Result{}, err
		}
		// 3. 上游 try：使用 Coordinator 自有上下文（期限 = deadline_at；adapter 的客户端另有自身超时）。启用对冲时
		// 可能另有一条并发的腿；除最终结果之外的腿已在 runTry 中结算。
		fr, legsTried := c.runTry(ctx, j, rs, l, try, release, tried)
		outcome, status, code := upstream.OutcomeOK, 200, ""
		if fr.uerr != nil {
			outcome, status, code = fr.uerr.Outcome, fr.uerr.Status, fr.uerr.Code
		}
		switch outcome {
		case upstream.OutcomeOK:
			return c.complete(j, fr.try, fr.resp, fr.latency, c.costOn(j, rs, fr.leg, fr.resp.Usage))
		case upstream.OutcomeFatal:
			if _, err := c.settle(Settlement{Try: fr.try, Outcome: "fatal", LatencyMs: fr.latency.Milliseconds(), Error: code}); err != nil {
				return Result{}, err
			}
			return Result{Status: status, Code: code}, nil
		case upstream.OutcomeRetryable:
			r, err := c.settle(Settlement{Try: fr.try, Outcome: "retryable", LatencyMs: fr.latency.Milliseconds(), Error: code})
			if err != nil {
				return Result{}, err
			}
			state = r.State
			if sc := c.stopCode(ctx, deadline); sc != "" {
				return c.giveUp(j, state, sc)
			}
			if max(r.TriesUsed, fr.try.TryNo) >= c.limits.MaxTries {
				return c.giveUp(j, state, persistence.CodeTriesExhausted)
			}
		default: // unknown：已发出、无法确认（含取消或到期时已发出的请求，记 possible_external_duplicate）
			r, err := c.settle(Settlement{Try: fr.try, Outcome: "unknown", LatencyMs: fr.latency.Milliseconds(), Error: code})
			if err != nil {
				return Result{}, err
			}
			state = r.State
			if sc := c.stopCode(ctx, deadline); sc != "" {
				return c.giveUp(j, state, sc)
			}
			if max(r.TriesUsed, fr.try.TryNo) >= c.limits.MaxTries {
				return Result{Status: status, Code: code}, nil
			}
		}
		lastCode, lastRetryAfter = code, fr.resp.RetryAfter
		// 暂停（desired = pause）不撤销访问，刚结束的 try 照常结算（§9.1）；但不再为本调用自动新建 try：以该 try 的
		// 原因（可重试类别）结束，Worker 尽快到达提交边界写 checkpoint，继续后可带 X-Agentbox-Retry 在上限内重试。
		if c.pauseRequested(in) {
			return c.giveUp(j, state, code)
		}
		// 降级链：本轮还有就绪的路由时立即换路由（不退避）；否则本轮结束、退避后开始新一轮。
		if rs != nil {
			for _, rt := range legsTried {
				tried[rt] = true
			}
			if rs.ready(j.model, tried) {
				continue
			}
			tried = map[int]bool{}
		}
		// 4. 退避：基数翻倍、上限、抖动；遵从 Retry-After；不超过剩余期限（会超过则不再新建 try）。
		if r, stop, err := c.waitBackoff(ctx, j, deadline, state, n, fr.resp.RetryAfter, code); stop {
			return r, err
		}
	}
}

// waitBackoff 是第 n 次 try 之后的退避：会超过期限则以 call_deadline_exceeded 结束；等待被取消则按 stopCode 结束；
// 退避期间到达的暂停以上一 try 的原因 code 结束。stop 为 false 时继续下一次 try。
func (c *Coordinator) waitBackoff(ctx context.Context, j *job, deadline time.Time, state CallState, n int,
	retryAfter time.Duration, code string) (Result, bool, error) {
	wait := c.backoff(n, retryAfter)
	if !c.now().Add(wait).Before(deadline) {
		r, err := c.giveUp(j, state, persistence.CodeCallDeadlineExceeded)
		return r, true, err
	}
	t := time.NewTimer(wait)
	select {
	case <-t.C:
	case <-ctx.Done():
		t.Stop()
		r, err := c.giveUp(j, state, c.stopCode(ctx, deadline))
		return r, true, err
	}
	if c.pauseRequested(j.in) { // 退避期间到达的暂停
		r, err := c.giveUp(j, state, code)
		return r, true, err
	}
	return Result{}, false, nil
}

// reserve 是一次 try 的 Tx2（ReserveTry）：估算、provider、跳过的路由与对冲标记来自 leg。
func (c *Coordinator) reserve(j *job, l leg) (Try, error) {
	in := j.in
	sctx, scancel := c.opCtx()
	defer scancel()
	return c.store.ReserveTry(sctx, ReserveTryRequest{
		TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID, EnvID: in.EnvID,
		EstimateMicro: l.est, MaxTries: c.limits.MaxTries, SubrunID: in.SubrunID,
		Provider: l.provider, Skipped: l.skipped, Hedge: l.hedge,
	})
}

// legResult 是一条腿（一次 try）的上游结果。aborted 表示结果源于 Coordinator 自己的取消。
type legResult struct {
	leg     leg
	try     Try
	resp    upstream.Response
	uerr    *upstream.Error
	latency time.Duration
	aborted bool
}

func (r legResult) outcome() upstream.Outcome {
	if r.uerr == nil {
		return upstream.OutcomeOK
	}
	return r.uerr.Outcome
}

// runTry 发出已预留的 try 并返回最终结果（由调用方结算）与本次用过的路由。启用对冲（多路由且 HedgeDelay > 0）时
// 交给 runHedged。release 在上游请求结束后释放在途槽位。
func (c *Coordinator) runTry(ctx context.Context, j *job, rs *routeSet, l leg, try Try, release func(),
	tried map[int]bool) (legResult, []int) {
	if rs != nil && c.routing.HedgeDelay > 0 {
		return c.runHedged(ctx, j, rs, l, try, release, tried)
	}
	start := c.now()
	resp, uerr, aborted := c.do(ctx, j, rs, l)
	faultinject.Point(faultinject.CallInFlight)
	r := legResult{leg: l, try: try, resp: resp, uerr: uerr, latency: c.now().Sub(start), aborted: aborted}
	release()
	rs.report(l, r.outcome(), aborted)
	c.logTry(j, rs, r)
	return r, []int{l.route}
}

// logTry 写 "gateway: try" 日志行（不含请求与响应正文）。按路由执行时另带 route、route_skipped、hedge 与该路由
// 熔断器的当前状态 breaker（属性名稳定，供指标与追踪使用）。
func (c *Coordinator) logTry(j *job, rs *routeSet, r legResult) {
	in := j.in
	status, code := 200, ""
	if r.uerr != nil {
		status, code = r.uerr.Status, r.uerr.Code
	}
	attrs := []any{"task_id", in.TaskID, "attempt_id", in.AttemptID, "call_id", in.CallID,
		"try_no", r.try.TryNo, "endpoint", endpointOf(in.Kind), "provider", j.ad.Provider(), "status", status,
		"latency_ms", r.latency.Milliseconds(), "outcome", string(r.outcome()), "code", code}
	if rs != nil && r.leg.route >= 0 {
		attrs = append(attrs, "route", r.leg.provider, "route_skipped", r.leg.skipped, "hedge", r.leg.hedge,
			"breaker", rs.breakers[r.leg.route].State().String())
	}
	c.log.Info("gateway: try", attrs...)
}

// pauseRequested 报告调用所属任务是否已请求暂停（task_control.desired = pause）：try 循环据此不再自动新建 try。
// 读取失败时按未暂停处理（照常重试，与修复前相同）。
func (c *Coordinator) pauseRequested(in Invoke) bool {
	ctx, cancel := c.opCtx()
	defer cancel()
	f, err := c.store.CheckAccess(ctx, in.TaskID, in.AttemptID, in.SubrunID)
	if err != nil {
		c.log.Warn("gateway: 重试前读取控制意图失败，照常重试", "task_id", in.TaskID, "call_id", in.CallID, "err", err)
		return false
	}
	return f.Desired == "pause"
}

// complete 是第二个原子提交点：先完整保存结果 blob，再在单个事务中结算、completed 与 scope_blobs。
// blob 保存失败时不能 completed：结果没有落地，try 按 unknown 结算（全额估算转 unknown，§9.6），不再重试。
// actual 是按执行该 try 的供应商价格计算的实际费用（costOn）。
func (c *Coordinator) complete(j *job, try Try, resp upstream.Response, latency time.Duration, actual int64) (Result, error) {
	ctx, cancel := c.opCtx()
	ref, err := c.blobs.Put(ctx, bytes.NewReader(resp.Body))
	cancel()
	if err != nil {
		if _, serr := c.settle(Settlement{Try: try, Outcome: "unknown", LatencyMs: latency.Milliseconds(),
			UpstreamRequestID: resp.UpstreamRequestID, Error: CodeBlobWriteFailed}); serr != nil {
			c.log.Warn("gateway: blob 写入失败后的 unknown 结算失败", "task_id", j.in.TaskID, "call_id", j.in.CallID,
				"try_no", try.TryNo, "err", serr)
		}
		return Result{}, fmt.Errorf("call: 保存调用 %s 的结果 blob: %w", j.in.CallID, err)
	}
	rec, err := c.settle(Settlement{
		Try: try, Outcome: "ok", ActualMicro: actual, LatencyMs: latency.Milliseconds(),
		UpstreamRequestID: resp.UpstreamRequestID, ResultSHA256: ref.SHA256, ResultSize: ref.Size,
	})
	if err != nil {
		return Result{}, err
	}
	// 只有本次结算使调用 completed 时才写缓存（迟到的结算不改变 journal，也不产生缓存条目）。
	if rec.State == StateCompleted && rec.ResultRef == ref.SHA256 {
		c.offerCache(j, resp, ref, latency)
	}
	return Result{Body: resp.Body, BlobSHA256: ref.SHA256, Status: 200}, nil
}

func (c *Coordinator) settle(s Settlement) (CallRecord, error) {
	ctx, cancel := c.opCtx()
	defer cancel()
	return c.store.SettleTry(ctx, s)
}

// backoff 返回第 n 次 try 之后的等待：base × 2^(n−1)，上限 BackoffMax，在 [d/2, d] 内抖动；至少为 Retry-After。
func (c *Coordinator) backoff(n int, retryAfter time.Duration) time.Duration {
	d := c.limits.BackoffBase
	for i := 1; i < n && d < c.limits.BackoffMax; i++ {
		d *= 2
	}
	d = min(d, c.limits.BackoffMax)
	if half := d / 2; half > 0 {
		d = half + rand.N(half+1)
	}
	return max(d, retryAfter)
}

// cost 由用量与价格表计算 ok 结算的实际费用（微美元，向上取整）；没有价格表或溢出时按估算。
// chat 先取该模型的价格表（ChatPricing），缺项时用类别价格表。
func (c *Coordinator) cost(kind upstream.Kind, model string, u upstream.Usage, est int64) int64 {
	p, ok := c.pricing[kind]
	if kind == upstream.KindChat {
		if mp, has := c.chatPricing[model]; has {
			p, ok = mp, true
		}
	}
	if !ok {
		return est
	}
	var total int64
	var bad bool
	add := func(a, b, div int64) {
		v, ok := mulCeil(a, b, div)
		if !ok || total > math.MaxInt64-v {
			bad = true
			return
		}
		total += v
	}
	switch kind {
	case upstream.KindChat:
		add(u.InputTokens, p.InputMicroPerMTok, 1_000_000)
		add(u.OutputTokens, p.OutputMicroPerMTok, 1_000_000)
	case upstream.KindSearch:
		add(int64(u.Requests), p.SearchMicroPerRequest, 1)
	case upstream.KindFetch:
		add(int64(u.Requests), p.FetchMicroPerRequest, 1)
	default:
		return est
	}
	if bad {
		return est
	}
	return total
}

// mulCeil 返回 ceil(a × b / div)；参数为负或溢出时 ok 为 false。
func mulCeil(a, b, div int64) (int64, bool) {
	if a < 0 || b < 0 || div <= 0 {
		return 0, false
	}
	if a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	p := a * b
	q := p / div
	if p%div != 0 {
		q++
	}
	return q, true
}

// acquire 等待在途槽位（§9.7）：sub-run 调用先取每 sub-run 槽位，再取每任务、每 provider 槽位（固定顺序，
// 避免持有 task 槽位等待 sub-run 槽位，也避免交叉等待）；root 调用（subrunID 为空）没有 sub-run 槽位。释放顺序
// 与获取相反。等待受 ctx（期限、取消）约束；不承诺公平性。nowait 时不等待：任一槽位已满即返回 errNoSlot
// （对冲请求只在有空闲槽位时发出）。
func (c *Coordinator) acquire(ctx context.Context, taskID, subrunID, provider string, nowait bool) (func(), error) {
	type slot struct {
		m   map[string]*semaphore
		key string
		s   *semaphore
	}
	var order []slot
	if subrunID != "" {
		k := subrunSemKey(taskID, subrunID)
		order = append(order, slot{c.subSem, k, c.semRef(c.subSem, k, c.limits.PerSubrunInflight)})
	}
	order = append(order,
		slot{c.taskSem, taskID, c.semRef(c.taskSem, taskID, c.limits.PerTaskInflight)},
		slot{c.provSem, provider, c.semRef(c.provSem, provider, c.limits.PerProviderInflight)})
	// releaseN 释放已取得的前 n 个槽位（逆序），并解除全部引用。
	releaseN := func(n int) {
		for i := n - 1; i >= 0; i-- {
			<-order[i].s.ch
		}
		for _, o := range order {
			c.semUnref(o.m, o.key)
		}
	}
	for i, o := range order {
		if nowait {
			select {
			case o.s.ch <- struct{}{}:
			default:
				releaseN(i)
				return nil, errNoSlot
			}
			continue
		}
		select {
		case o.s.ch <- struct{}{}:
		case <-ctx.Done():
			releaseN(i)
			return nil, ctx.Err()
		}
	}
	var once sync.Once
	return func() { once.Do(func() { releaseN(len(order)) }) }, nil
}

func (c *Coordinator) semRef(m map[string]*semaphore, key string, n int) *semaphore {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := m[key]
	if s == nil {
		s = &semaphore{ch: make(chan struct{}, n)}
		m[key] = s
	}
	s.refs++
	return s
}

func (c *Coordinator) semUnref(m map[string]*semaphore, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := m[key]; s != nil {
		if s.refs--; s.refs == 0 {
			delete(m, key)
		}
	}
}
