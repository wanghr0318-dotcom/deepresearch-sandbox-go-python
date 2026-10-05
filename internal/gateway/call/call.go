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

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/faultinject"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// Coordinator 的流程（规格 §9.4、§9.5、§9.7、§11.2）：
//
//	解析（拒绝重复键）→ 指纹 → Tx1 BeginCall → 按已有记录分流（重放 / 进行中 / 指纹分歧 / unknown / failed）
//	→ 新登记的搜索与抓取：查缓存（事务外）→ 命中：校验 blob → Tx2 CompleteFromCache（无预留）→ 返回
//	→ 未命中：循环：等待并发槽位 → Tx2 ReserveTry → 上游 try → 先保存结果 blob 再 SettleTry → 可重试时退避
//	→ ok 结算之后异步写入缓存（准入与新鲜度由 cache 判定）
//
// journal 优先：已有记录（含 completed 的重放）从不查缓存，缓存结果也不会覆盖已记录的结果。模型调用从不缓存。
// Redis 的任何问题都只是未命中；PostgreSQL 或访问检查失败返回错误，不绕过（§11.2）。
//
// 一次逻辑调用的执行在 Coordinator 自有的后台 goroutine 中进行：Worker 的请求上下文取消或连接断开只让 Invoke
// 提前返回，不取消 try（继续至期限并结算，供重放，§9.1）；只有 CancelAttempt(attemptID, ReasonCancel) 取消在途 try。
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

// ErrClosed 表示 Coordinator 已关闭。
var ErrClosed = errors.New("call: coordinator 已关闭")

// errCancelRequested 是 CancelAttempt 取消在途 try 时的取消原因。
var errCancelRequested = errors.New("call: 任务已请求取消")

// Limits 是 Gateway 的调用限额（§19 默认值）。零值字段取默认值。
type Limits struct {
	CallDeadline        time.Duration // 调用期限，默认 120 s
	MaxTries            int           // 每逻辑调用累计 try，默认 3
	PerTaskInflight     int           // 每任务上游在途，默认 4
	PerProviderInflight int           // 每 provider 上游在途，默认 8
	BackoffBase         time.Duration // 退避基数，默认 2 s（翻倍）
	BackoffMax          time.Duration // 退避上限，默认 60 s
}

func (l Limits) withDefaults() Limits {
	if l.CallDeadline <= 0 {
		l.CallDeadline = 120 * time.Second
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
	if l.BackoffBase <= 0 {
		l.BackoffBase = 2 * time.Second
	}
	if l.BackoffMax <= 0 {
		l.BackoffMax = 60 * time.Second
	}
	return l
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
}

// Invoke 是一次 Worker 请求。
type Invoke struct {
	TaskID, AttemptID, EnvID    string
	CallID                      string
	Kind                        upstream.Kind
	Body                        []byte
	Retry                       bool // X-Agentbox-Retry
	Supersedes, SupersedeReason string
	NoCache                     bool // X-Agentbox-Cache: no-cache（计入指纹；不读缓存，结果仍写入）
}

// Result 是返回给 edge 的结果。Status 是 HTTP 状态；Code 只在失败时非空（稳定错误码）。
// 成功时 Body 是结果正文，BlobSHA256 是结果 blob（X-Agentbox-Blob），Replayed 表示来自 journal 重放。
type Result struct {
	Body       []byte
	Replayed   bool
	BlobSHA256 string
	Status     int
	Code       string
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

	root context.Context
	stop context.CancelFunc
	wg   sync.WaitGroup

	mu        sync.Mutex
	busy      map[callKey]bool
	byAttempt map[string]map[*job]bool
	taskSem   map[string]*semaphore
	provSem   map[string]*semaphore
}

type callKey struct{ taskID, callID string }

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
	ctx      context.Context // Coordinator 自有；CancelAttempt 以 errCancelRequested 取消
	cancel   context.CancelCauseFunc
}

// New 构造 Coordinator。
func New(cfg Config) (*Coordinator, error) {
	if cfg.Store == nil || cfg.Blobs == nil || cfg.Events == nil {
		return nil, errors.New("call: Store、Blobs、Events 必填")
	}
	c := &Coordinator{
		store:        cfg.Store,
		adapters:     map[upstream.Kind]upstream.Adapter{},
		pricing:      cfg.Pricing,
		chatPricing:  cfg.ChatPricing,
		blobs:        cfg.Blobs,
		events:       cfg.Events,
		limits:       cfg.Limits.withDefaults(),
		log:          cfg.Logger,
		now:          cfg.Now,
		storeTimeout: cfg.StoreTimeout,
		cache:        cfg.Cache,
		busy:         map[callKey]bool{},
		byAttempt:    map[string]map[*job]bool{},
		taskSem:      map[string]*semaphore{},
		provSem:      map[string]*semaphore{},
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
	case persistence.CodeAccessRevoked, upstream.CodeEgressBlocked:
		return 403
	case persistence.CodeNotCurrentAttempt, persistence.CodeCancelRequested,
		persistence.CodeFingerprintMismatch, persistence.CodeCallInProgress:
		return 409
	case persistence.CodeBudgetExhausted, persistence.CodeBudgetInsufficient:
		return 402
	case persistence.CodeTriesExhausted:
		return 429
	case persistence.CodeCallDeadlineExceeded:
		return 504
	case CodeDuplicateJSONKey, upstream.CodeInvalidRequest, upstream.CodeUnsupportedField,
		upstream.CodeUnsupportedModel, upstream.CodeUnsupportedProvider, upstream.CodeInvalidURL:
		return 400
	case CodeEndpointNotConfigured:
		return 404
	case CodeStoreUnavailable, CodeGatewayShutdown:
		return 503
	}
	return 502
}

// retryableReason 报告 failed 调用的原因是否属于可重试类别（§9.4：带 X-Agentbox-Retry 时可在上限内新建 try）：
// 上游的暂时失败、预算或访问在 Tx2 被拒、存储故障与关闭中止。期限与次数耗尽、上游明确拒绝不可重试。
func retryableReason(reason string) bool {
	switch reason {
	case upstream.CodeUpstreamRateLimited, upstream.CodeUpstreamUnavailable, upstream.CodeUpstreamUnreachable,
		persistence.CodeBudgetExhausted, persistence.CodeBudgetInsufficient,
		persistence.CodeAccessRevoked, persistence.CodeNotCurrentAttempt, persistence.CodeCancelRequested,
		CodeStoreUnavailable, CodeGatewayShutdown:
		return true
	}
	return false
}

// admit 是规格 §9.2 的访问判定，顺序与持久化层一致：访问撤销 → 非当前 attempt → 已请求取消。
func admit(f AccessFacts) string {
	switch {
	case !f.Active:
		return persistence.CodeAccessRevoked
	case !f.Current:
		return persistence.CodeNotCurrentAttempt
	case f.Desired == "cancel":
		return persistence.CodeCancelRequested
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
func (c *Coordinator) CheckAccess(ctx context.Context, taskID, attemptID string) (Result, error) {
	f, err := c.store.CheckAccess(ctx, taskID, attemptID)
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
func (c *Coordinator) CancelAttempt(attemptID, reason string) {
	if reason != ReasonCancel {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for j := range c.byAttempt[attemptID] {
		j.cancel(errCancelRequested)
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
}

// contended 处理进程内已有执行者的同 ID 请求：仍按 §9.2 检查访问；指纹不同报告分歧，否则 call_in_progress。
func (c *Coordinator) contended(ctx context.Context, in Invoke, fp string) (Result, error) {
	if r, err := c.CheckAccess(ctx, in.TaskID, in.AttemptID); err != nil || r.Code != "" {
		return r, err
	}
	rec, _, err := c.store.LoadCall(ctx, in.TaskID, in.CallID)
	if err == nil && rec.Fingerprint != fp {
		return c.diverge(in, rec.Fingerprint, fp), nil
	}
	return reject(persistence.CodeCallInProgress), nil
}

// diverge 写入 host 事件 replay_divergence 并返回 409 fingerprint_mismatch。
func (c *Coordinator) diverge(in Invoke, stored, got string) Result {
	ctx, cancel := c.opCtx()
	defer cancel()
	detail := fmt.Sprintf("endpoint=%s stored_fingerprint=%s request_fingerprint=%s", endpointOf(in.Kind), stored, got)
	if err := c.events.ReplayDivergence(ctx, in.TaskID, in.AttemptID, in.CallID, detail); err != nil {
		c.log.Warn("gateway: 写入 replay_divergence 失败", "task_id", in.TaskID, "attempt_id", in.AttemptID,
			"call_id", in.CallID, "err", err)
	}
	return reject(persistence.CodeFingerprintMismatch)
}

// run 是一次逻辑调用的后台执行：Tx1 与按已有记录分流，然后进入 try 循环。
func (c *Coordinator) run(j *job) (Result, error) {
	in := j.in
	ctx, cancel := c.opCtx()
	res, err := c.store.BeginCall(ctx, BeginCallRequest{
		TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID, Fingerprint: j.fp, Endpoint: endpointOf(in.Kind),
		Deadline: c.limits.CallDeadline, SupersedesCallID: in.Supersedes, SupersedeReason: in.SupersedeReason,
	})
	cancel()
	if err != nil {
		if r, ok := rejection(err); ok {
			return r, nil
		}
		return Result{}, err
	}
	rec := res.Record
	if res.Existing {
		if rec.Fingerprint != j.fp {
			return c.diverge(in, rec.Fingerprint, j.fp), nil
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
		// 只有本次新登记（或接管复位）的 resolving 调用查缓存：已有记录一律按 journal 处理（journal 优先）。
		if r, done, err := c.resolveFromCache(j); done {
			return r, err
		}
	}
	return c.execute(j, rec)
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
// done 为 false 表示未命中（含 no-cache、Redis 故障与 blob 复核失败），调用方照常走上游。
// Tx2 被拒（取消先提交、访问撤销、期限已过）或存储故障时与 ReserveTry 失败的处理相同：调用置为 failed
// （存储故障为可重试的 store_unavailable），结果不被授权（E23）。
func (c *Coordinator) resolveFromCache(j *job) (Result, bool, error) {
	in := j.in
	if in.NoCache {
		c.cache.Bypass()
		return Result{}, false, nil
	}
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
		ResultSHA256: sha, ResultSize: size})
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

// stopCode 报告 try 循环是否必须停止及其原因：取消 → cancel_requested；到期 → call_deadline_exceeded；关闭 → gateway_shutdown。
func (c *Coordinator) stopCode(ctx context.Context, deadline time.Time) string {
	switch {
	case errors.Is(context.Cause(ctx), errCancelRequested):
		return persistence.CodeCancelRequested
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
func (c *Coordinator) execute(j *job, rec CallRecord) (Result, error) {
	in := j.in
	deadline := rec.DeadlineAt
	ctx, cancel := context.WithDeadline(j.ctx, deadline)
	defer cancel()
	state := rec.State
	provider := j.ad.Provider()
	for n := 1; ; n++ {
		if code := c.stopCode(ctx, deadline); code != "" {
			return c.giveUp(j, state, code)
		}
		release, err := c.acquire(ctx, in.TaskID, provider)
		if err != nil {
			return c.giveUp(j, state, c.stopCode(ctx, deadline))
		}
		// 2. Tx2：复查访问、期限、累计次数与预算后预留；提交之后才发起上游请求（第一个原子提交点）。
		sctx, scancel := c.opCtx()
		try, err := c.store.ReserveTry(sctx, ReserveTryRequest{
			TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID, EnvID: in.EnvID,
			EstimateMicro: j.est, MaxTries: c.limits.MaxTries,
		})
		scancel()
		if err != nil {
			release()
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
		// 3. 上游 try：使用 Coordinator 自有上下文（期限 = deadline_at；adapter 的客户端另有自身超时）。
		start := c.now()
		resp, uerr := j.ad.Do(ctx, j.resolved)
		faultinject.Point(faultinject.CallInFlight)
		latency := c.now().Sub(start)
		release()
		outcome, status, code := upstream.OutcomeOK, 200, ""
		if uerr != nil {
			outcome, status, code = uerr.Outcome, uerr.Status, uerr.Code
		}
		c.log.Info("gateway: try", "task_id", in.TaskID, "attempt_id", in.AttemptID, "call_id", in.CallID,
			"try_no", try.TryNo, "endpoint", endpointOf(in.Kind), "provider", provider, "status", status,
			"latency_ms", latency.Milliseconds(), "outcome", string(outcome), "code", code)

		switch outcome {
		case upstream.OutcomeOK:
			return c.complete(j, try, resp, latency)
		case upstream.OutcomeFatal:
			if _, err := c.settle(Settlement{Try: try, Outcome: "fatal", LatencyMs: latency.Milliseconds(), Error: code}); err != nil {
				return Result{}, err
			}
			return Result{Status: status, Code: code}, nil
		case upstream.OutcomeRetryable:
			r, err := c.settle(Settlement{Try: try, Outcome: "retryable", LatencyMs: latency.Milliseconds(), Error: code})
			if err != nil {
				return Result{}, err
			}
			state = r.State
			if sc := c.stopCode(ctx, deadline); sc != "" {
				return c.giveUp(j, state, sc)
			}
			if try.TryNo >= c.limits.MaxTries {
				return c.giveUp(j, state, persistence.CodeTriesExhausted)
			}
		default: // unknown：已发出、无法确认（含取消或到期时已发出的请求，记 possible_external_duplicate）
			r, err := c.settle(Settlement{Try: try, Outcome: "unknown", LatencyMs: latency.Milliseconds(), Error: code})
			if err != nil {
				return Result{}, err
			}
			state = r.State
			if sc := c.stopCode(ctx, deadline); sc != "" {
				return c.giveUp(j, state, sc)
			}
			if try.TryNo >= c.limits.MaxTries {
				return Result{Status: status, Code: code}, nil
			}
		}
		// 4. 退避：基数翻倍、上限、抖动；遵从 Retry-After；不超过剩余期限（会超过则不再新建 try）。
		wait := c.backoff(n, resp.RetryAfter)
		if !c.now().Add(wait).Before(deadline) {
			return c.giveUp(j, state, persistence.CodeCallDeadlineExceeded)
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return c.giveUp(j, state, c.stopCode(ctx, deadline))
		}
	}
}

// complete 是第二个原子提交点：先完整保存结果 blob，再在单个事务中结算、completed 与 scope_blobs。
// blob 保存失败时不能 completed：结果没有落地，try 按 unknown 结算（全额估算转 unknown，§9.6），不再重试。
func (c *Coordinator) complete(j *job, try Try, resp upstream.Response, latency time.Duration) (Result, error) {
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
		Try: try, Outcome: "ok", ActualMicro: c.cost(j.in.Kind, j.model, resp.Usage, j.est), LatencyMs: latency.Milliseconds(),
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

// acquire 等待每任务与每 provider 的在途槽位（先任务后 provider，避免交叉等待）；等待受 ctx（期限、取消）约束。
func (c *Coordinator) acquire(ctx context.Context, taskID, provider string) (func(), error) {
	ts := c.semRef(c.taskSem, taskID, c.limits.PerTaskInflight)
	ps := c.semRef(c.provSem, provider, c.limits.PerProviderInflight)
	done := func() {
		c.semUnref(c.taskSem, taskID)
		c.semUnref(c.provSem, provider)
	}
	select {
	case ts.ch <- struct{}{}:
	case <-ctx.Done():
		done()
		return nil, ctx.Err()
	}
	select {
	case ps.ch <- struct{}{}:
	case <-ctx.Done():
		<-ts.ch
		done()
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			<-ps.ch
			<-ts.ch
			done()
		})
	}, nil
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
