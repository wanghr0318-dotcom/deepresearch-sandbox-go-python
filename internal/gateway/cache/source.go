package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
)

// Source 是 Gateway 调用路径的缓存接入层（§11.2、§11.5），实现 gateway/call 的 CacheSource 窄接口：
//
//   - Lookup：键 → Redis GET（熔断与超时由 Guarded 处理，任何错误都是未命中）→ 校验 HMAC 与 expires_at
//     （Gateway 时钟）→ 重新读取 blob 并复算 sha256 与大小。任一校验失败：未命中、删除条目、记
//     IntegrityFailure。
//   - Store：准入（Admit）与新鲜度（抓取取 Lifetime，搜索取 adapter TTL）→ Seal → 异步、可丢弃的 SET。
//   - Bypass：no-cache 请求跳过读取时计数。
//
// 指标：Guarded 只记 Error 与 BreakerOpen；Hit、Miss、Bypass、IntegrityFailure 在这里记，每次查找恰得到
// 一个结局（Redis 出错或熔断打开的查找只计 Error / BreakerOpen，不再计 Miss）。Source 不记账、不授权：
// 命中只给出 blob 引用，journal 提交（CompleteFromCache）由 gateway/call 完成。
type Source struct {
	kv        KV
	m         *Metrics
	signer    *Signer
	blobs     BlobReader
	searchTTL time.Duration
	now       func() time.Time

	mu      sync.Mutex
	closed  bool
	pending chan struct{} // 在途异步写入的名额；满时丢弃新的写入
	wg      sync.WaitGroup
}

// BlobReader 是 Source 用来复算缓存正文哈希的只读 BlobStore 子集（internal/blob.Store 满足它）。
type BlobReader interface {
	Open(sha string) (io.ReadCloser, error)
}

// DefaultSearchTTL 是搜索结果的缓存寿命（§11.3、§19：adapter 定义，默认 6 h）。
const DefaultSearchTTL = 6 * time.Hour

const (
	defaultPendingWrites = 64
	// asyncWriteBudget 是一次异步写入的上限（Redis 的单次操作另有 100 ms 写超时）。
	asyncWriteBudget = time.Second
)

// SourceConfig 配置 Source。KV、Signer、Blobs 必填；Breaker、Metrics 为 nil 时使用新的零值。
type SourceConfig struct {
	KV        KV // 原始 KV（通常是 *Redis）；Source 自行以 Breaker 与 Metrics 包装（Guarded）
	Breaker   *Breaker
	Metrics   *Metrics
	Signer    *Signer
	Blobs     BlobReader
	SearchTTL time.Duration    // ≤ 0 时取 DefaultSearchTTL；不超过 MaxLifetime
	Now       func() time.Time // Gateway 时钟；nil 时为 time.Now
	// MaxPendingWrites 是同时在途的异步写入上限，≤ 0 时取 64；超过时新的写入被丢弃（写入可丢弃，§11.5）。
	MaxPendingWrites int
}

// NewSource 构造 Source。
func NewSource(cfg SourceConfig) (*Source, error) {
	if cfg.KV == nil || cfg.Signer == nil || cfg.Blobs == nil {
		return nil, errors.New("cache: Source 需要 KV、Signer 与 Blobs")
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &Metrics{}
	}
	if cfg.SearchTTL <= 0 {
		cfg.SearchTTL = DefaultSearchTTL
	}
	cfg.SearchTTL = min(cfg.SearchTTL, MaxLifetime)
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxPendingWrites <= 0 {
		cfg.MaxPendingWrites = defaultPendingWrites
	}
	return &Source{
		kv:        Guarded(probeKV{cfg.KV}, cfg.Breaker, cfg.Metrics),
		m:         cfg.Metrics,
		signer:    cfg.Signer,
		blobs:     cfg.Blobs,
		searchTTL: cfg.SearchTTL,
		now:       cfg.Now,
		pending:   make(chan struct{}, cfg.MaxPendingWrites),
	}, nil
}

// probe 记录一次 Get 是否真的到达了 Redis（熔断放行）以及是否出错；经 ctx 传给 probeKV。
type probe struct{ reached, failed bool }

type probeCtxKey struct{}

// probeKV 位于 Guarded 之内：Guarded 吞掉错误并计数，probe 让 Lookup 区分"真未命中"与"出错或熔断"，
// 从而每个结局只计一次。
type probeKV struct{ kv KV }

func (p probeKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, ok, err := p.kv.Get(ctx, key)
	if pr, _ := ctx.Value(probeCtxKey{}).(*probe); pr != nil {
		pr.reached, pr.failed = true, err != nil
	}
	return v, ok, err
}

func (p probeKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return p.kv.Set(ctx, key, val, ttl)
}

func (p probeKV) Del(ctx context.Context, key string) error { return p.kv.Del(ctx, key) }

// Lookup 查找缓存（§11.2"查缓存（事务外）"）。命中时返回已校验的 blob 引用；任何问题都只是未命中。
// rawURL 是抓取的目标 URL（搜索为空）：每次查找按当前准入规则复查（凭据类参数、userinfo），不合规的 URL
// 即使存在旧条目也不读取（计 Bypass）。
func (s *Source) Lookup(ctx context.Context, kind upstream.Kind, provider, ver string, params []byte, rawURL string) (string, int64, bool) {
	key, err := Key(kind, provider, ver, params)
	if err != nil {
		s.m.Bypass.Add(1)
		return "", 0, false
	}
	if kind == upstream.KindFetch {
		if ok, _ := Admit(kind, rawURL, Response{Status: 200}); !ok {
			s.m.Bypass.Add(1)
			return "", 0, false
		}
	}
	pr := &probe{}
	// Guarded 从不返回错误：Redis 出错或熔断打开都表现为未命中，并已由 Guarded 计数。
	raw, ok, _ := s.kv.Get(context.WithValue(ctx, probeCtxKey{}, pr), key)
	if !ok {
		if pr.reached && !pr.failed {
			s.m.Miss.Add(1)
		}
		return "", 0, false
	}
	v, err := s.signer.Open(key, raw, s.now())
	if err == nil {
		err = s.verifyBlob(v)
	}
	if err != nil {
		s.m.IntegrityFailure.Add(1)
		dctx, cancel := context.WithTimeout(context.Background(), asyncWriteBudget)
		defer cancel()
		// Guarded 的 Del 不返回错误（失败只计数）：删除失败时条目仍会在下次命中时被同样拒绝。
		_ = s.kv.Del(dctx, key)
		return "", 0, false
	}
	s.m.Hit.Add(1)
	return v.BlobSHA256, v.Size, true
}

// verifyBlob 确认条目引用的 blob 存在，且内容的 sha256 与大小与条目一致（§11.5）。
func (s *Source) verifyBlob(v Value) error {
	rc, err := s.blobs.Open(v.BlobSHA256)
	if err != nil {
		return fmt.Errorf("cache: 打开 blob %s: %w", v.BlobSHA256, err)
	}
	defer rc.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(rc, v.Size+1))
	if err != nil {
		return fmt.Errorf("cache: 读取 blob %s: %w", v.BlobSHA256, err)
	}
	if n != v.Size || hex.EncodeToString(h.Sum(nil)) != v.BlobSHA256 {
		return fmt.Errorf("cache: blob %s 的内容与条目不一致", v.BlobSHA256)
	}
	return nil
}

// Bypass 记录一次 no-cache 请求跳过缓存读取（§11.4：不读旧缓存，结果仍写入）。
func (s *Source) Bypass() { s.m.Bypass.Add(1) }

// Store 在结果已结算为 completed 之后提交一次缓存写入（异步、可丢弃）。resp 是上游响应的元数据，其 Size
// 须为结果 blob 的大小（准入按它判定 ≤ 2 MiB，命中时按它校验）；blobSHA 是结果 blob。
// 不可缓存（Admit 拒绝、没有显式新鲜度或已不新鲜）时什么也不做。搜索的 expires_at = 响应时刻 + TTL。
func (s *Source) Store(kind upstream.Kind, provider, ver string, params []byte, rawURL string, resp Response, blobSHA string) {
	if ok, _ := Admit(kind, rawURL, resp); !ok {
		return
	}
	now := s.now()
	fetched := resp.ResponseTime
	if fetched.IsZero() {
		fetched = now
	}
	var expires time.Time
	switch kind {
	case upstream.KindSearch:
		expires = fetched.Add(s.searchTTL)
	case upstream.KindFetch:
		e, ok := Lifetime(resp, now)
		if !ok {
			return
		}
		expires = e
	default:
		return
	}
	ttl := expires.Sub(now)
	if ttl <= 0 {
		return
	}
	key, err := Key(kind, provider, ver, params)
	if err != nil {
		return
	}
	contentType := ""
	if v := headerValues(resp.Header, "Content-Type"); len(v) > 0 {
		contentType = v[0]
	}
	val, err := s.signer.Seal(key, Value{BlobSHA256: blobSHA, Status: resp.Status, ContentType: contentType,
		Size: resp.Size, FetchedAt: fetched, ExpiresAt: expires})
	if err != nil {
		return // 例如编码后超过 4 KiB：不可缓存
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.pending <- struct{}{}:
	default:
		return // 在途写入已满：丢弃
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.pending }()
		ctx, cancel := context.WithTimeout(context.Background(), asyncWriteBudget)
		defer cancel()
		// Guarded 的 Set 不返回错误（失败只计数）：写入可丢弃。
		_ = s.kv.Set(ctx, key, val, ttl)
	}()
}

// Metrics 返回指标快照（§11.5 的名称）。
func (s *Source) Metrics() map[string]int64 {
	return map[string]int64{
		"hit":               s.m.Hit.Load(),
		"miss":              s.m.Miss.Load(),
		"bypass":            s.m.Bypass.Load(),
		"coalesced":         s.m.Coalesced.Load(),
		"error":             s.m.Error.Load(),
		"breaker_open":      s.m.BreakerOpen.Load(),
		"integrity_failure": s.m.IntegrityFailure.Load(),
	}
}

// Close 停止接受新的写入并等待在途的异步写入结束（每个至多 asyncWriteBudget）。
func (s *Source) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.wg.Wait()
}
