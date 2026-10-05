// Package app 是服务的装配与启动顺序（规格 §14.1、§7.4；代码组织 §5、§6.1）：按顺序取得所有权、
// 引导安装身份、迁移、启动恢复，然后依次启动 cleanup loop、task actor 与 API，并处理恢复期限、
// ownership_lost 与 actor 致命错误。
//
// 本包以接口接收依赖（Store、Provider、BlobStore 等）；具体实现只在 cmd/agentbox（生产）与只供测试的
// main 包中构造（代码组织规则 5）。本包不导入 provider/fake 或 persistence/postgres。
package app

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/edge"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/reconcile"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/recovery"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// ErrOwnershipLost 是失去数据库所有权后 Run 返回的错误（errors.Is 同时匹配 persistence.ErrOwnershipLost）。
var ErrOwnershipLost = fmt.Errorf("app: ownership_lost: %w", persistence.ErrOwnershipLost)

// Config 是服务配置。零值字段取默认值（规格 §19；规格未给出的项为 M1 默认值，注释中标明）。
type Config struct {
	// Listen 是 API 的监听地址，默认 127.0.0.1:8080（与 CLI 的默认地址一致）。非 loopback 地址需要
	// APIToken 与 AllowedHosts（§15.3）。
	Listen string
	// RecoveryDeadline 是启动恢复期限，默认 60 s（§14.1、§19）：到期未就绪则 API 以诊断模式启动。
	RecoveryDeadline time.Duration
	// Capacity 是 admission 的总容量。M1 默认 4 个 run slot、8 GiB。
	Capacity admission.Capacity
	// DefaultMemoryBytes 是 limits 未设置 memory_max 时申请与占用的内存。M1 默认 1 GiB；不得超过总容量。
	DefaultMemoryBytes int64
	// DefaultLimits 是环境资源限制的默认值（limits 中设置的字段覆盖它；MemoryMax 总取 memory_max 或
	// DefaultMemoryBytes）。M1 默认 pids 512、cpu.max 一个 CPU、nofile 1024、tmpfs 64 MiB。
	DefaultLimits provider.Limits
	// DefaultRunTime 是累计运行时限的默认值（规格 §14.4、§19：3600 s）：创建任务时未指定
	// limits.max_run_time_ms 则以此补入并随任务存储；之后修改不影响已创建任务。
	DefaultRunTime time.Duration
	// RunTimeCap 是显式时限的服务端上限（§19：24 h）；超过时创建失败。DefaultRunTime 不得超过它。
	RunTimeCap time.Duration
	// ShutdownTimeout 是退出时有期限清理的上限：物理停止全部环境、等待 actor 退出、关闭 API。默认 30 s。
	ShutdownTimeout time.Duration
	// Template 是任务环境的只读 rootfs 模板标识。M1 默认 "default"。
	Template string
	// UIDBase 与 UIDCount 是 UID 范围池（每段 4096，§19）：首次创建前幂等登记。M1 默认基址 1000000，
	// 段数 max(64, 4 × run slots)。
	UIDBase  int64
	UIDCount int
	// WorkerArgv 与 WorkerEnv 是环境内启动 Worker 的命令（task spec 经 init.config 交给 Worker）。
	// M1 默认 python3 -m sim_worker。
	WorkerArgv []string
	WorkerEnv  []string
	// ConfigVersion 与 MaxFaultRetries 写入新建的任务（MaxFaultRetries 默认 3，§19）。
	ConfigVersion   string
	MaxFaultRetries int64
	// APIToken、AllowedHosts、AllowedOrigins 见 api.Config（§15.3）。
	APIToken       string
	AllowedHosts   []string
	AllowedOrigins []string
	// WebDir 非空时同源提供工作台静态文件（见 api.Config.WebDir）。
	WebDir string
	// TLSCertFile 与 TLSKeyFile（PEM）须同时设置或同时为空；设置时 API 以内置 TLS 监听（§15.3）。
	TLSCertFile, TLSKeyFile string
	// Runner是 AttemptRunner 的选项（零值取 §19 默认值）。
	Runner runner.Options
	// RetryBackoff 是恢复与报警标记失败后的重试退避；默认 resource.DefaultBackoff（基数 2 s，上限 60 s）。
	RetryBackoff func(try int) time.Duration

	// DefaultBudgetMicro 是 task 层预算（微美元，§9.6）的默认值：创建任务时未指定 limits.budget_micro 则以此
	// 补入并随任务存储。默认 2_000_000（2.00 USD，待审批的默认值）；须为正且不超过 BudgetCapMicro。
	DefaultBudgetMicro int64
	// BudgetCapMicro 是显式 limits.budget_micro 的服务端上限；默认 50_000_000（50.00 USD）。
	BudgetCapMicro int64
	// Model 是模型上游（OpenAI 兼容，默认模型 + 声明的白名单）；BaseURL 为空时不提供 /v1/chat/completions。
	Model ModelConfig
	// SearchProvider 是搜索供应商：fake | tavily | ddg_lite | serper（默认 ddg_lite）。fake 只用于测试，要求设置
	// UpstreamAllowPrivate（指向本机 fake upstream）；tavily 与 serper 要求 SearchAPIKey。
	SearchProvider string
	// SearchBaseURL 覆盖搜索供应商地址（http/https；空时取供应商默认）。供应商为 fake 时设置它，fake 搜索经
	// 验证 dialer 向 <SearchBaseURL>/search 发出 HTTP 请求（测试用 fake upstream，主机须在 UpstreamAllowPrivate
	// 中），否则 fake 是进程内的脚本化结果。
	SearchBaseURL string
	// SearchAPIKey 是搜索供应商的 Key（只从宿主环境变量 AGENTBOX_SEARCH_API_KEY 加载，§9.9）。
	SearchAPIKey string
	// UpstreamAllowPrivate 是显式放行的私有上游主机（§9.8 规则 2；"host" 或 "host:port"），所有 adapter 共用。
	UpstreamAllowPrivate []string
	// Gateway 是调用限额（零值取 §19 默认值）。
	Gateway call.Limits
	// RedisAddr 是共享缓存的 Redis 地址（§11；host:port）。为空时缓存关闭（等价于 CacheOff）。
	RedisAddr string
	// CacheOff 关闭共享缓存（--cache=off）。缓存默认开启，但只在配置了 RedisAddr 时生效。
	CacheOff bool
}

// ModelConfig 是模型上游的配置。APIKey 只从宿主环境变量 AGENTBOX_MODEL_API_KEY 加载，只交给 chat adapter
// （放在 Authorization 头中），不进入日志、init 或沙箱环境（§9.9、G3）。
type ModelConfig struct {
	BaseURL string           // 例如 https://api.moonshot.cn/v1
	Name    string           // 默认模型名：请求未指定 model 时使用
	Models  []string         // 声明的模型白名单（§9.3）；空时只有 Name，非空时须包含 Name
	Pricing upstream.Pricing // 默认单价（每百万 token 的微美元）；Version 为空时由单价生成
	// PricingByModel 是按模型的单价（键须为声明的模型）；缺项的模型用 Pricing。Version 为空时由单价生成。
	// 价格只是配置，用于预留估算与结算，不代表供应商的实际计费。
	PricingByModel map[string]upstream.Pricing
	APIKey         string
	// MaxTokensCap 是 max_tokens 的上限（超出者截断，§9.6）；≤ 0 时取 adapter 默认值。推理模型先消耗
	// 推理 token，上限过低会得到空的 content。
	MaxTokensCap int
}

// declared 报告模型是否在声明的白名单中（Models 为空时只有 Name）。
func (m ModelConfig) declared(name string) bool {
	return name == m.Name || slices.Contains(m.Models, name)
}

func configPricingVersion(p upstream.Pricing) string {
	return fmt.Sprintf("config/in=%d,out=%d", p.InputMicroPerMTok, p.OutputMicroPerMTok)
}

// workerEnvAllowed 是沙箱内 Worker 环境变量的白名单（Config.WorkerEnv 的键）：凭据不能经由环境变量进入沙箱。
var workerEnvAllowed = map[string]bool{
	"PYTHONPATH": true, "PYTHONUNBUFFERED": true, "PYTHONDONTWRITEBYTECODE": true, "PYTHONHASHSEED": true,
	"PYTHONIOENCODING": true, "LANG": true, "LC_ALL": true, "TZ": true,
	// Worker 的 Gateway 客户端超时（秒；默认 330 s）。不是凭据：--model-call-deadline 或 --call-deadline 调到
	// 330 s 及以上时须把它设为更长，让 Gateway 先给出 504，而不是客户端先放弃并以新调用 ID 重做。
	"AGENTBOX_GATEWAY_TIMEOUT_S": true,
}

func (c Config) withDefaults() Config {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	if c.RecoveryDeadline <= 0 {
		c.RecoveryDeadline = 60 * time.Second
	}
	if c.Capacity == (admission.Capacity{}) {
		c.Capacity = admission.Capacity{RunSlots: 4, MemoryBytes: 8 << 30}
	}
	if c.DefaultMemoryBytes <= 0 {
		c.DefaultMemoryBytes = 1 << 30
	}
	if c.DefaultLimits.PidsMax <= 0 {
		c.DefaultLimits.PidsMax = 512
	}
	if c.DefaultLimits.CPUQuotaUs <= 0 {
		c.DefaultLimits.CPUQuotaUs = 100000
	}
	if c.DefaultLimits.NoFile == 0 {
		c.DefaultLimits.NoFile = 1024
	}
	if c.DefaultLimits.TmpBytes <= 0 {
		c.DefaultLimits.TmpBytes = 64 << 20
	}
	if c.DefaultRunTime <= 0 {
		c.DefaultRunTime = time.Hour
	}
	if c.RunTimeCap <= 0 {
		c.RunTimeCap = 24 * time.Hour
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.Template == "" {
		c.Template = "default"
	}
	if c.UIDBase <= 0 {
		c.UIDBase = 1000000
	}
	if c.UIDCount <= 0 {
		c.UIDCount = max(64, 4*c.Capacity.RunSlots)
	}
	if len(c.WorkerArgv) == 0 {
		c.WorkerArgv = []string{"python3", "-m", "sim_worker"}
	}
	if c.MaxFaultRetries <= 0 {
		c.MaxFaultRetries = 3
	}
	if c.RetryBackoff == nil {
		c.RetryBackoff = resource.DefaultBackoff
	}
	if c.DefaultBudgetMicro <= 0 {
		c.DefaultBudgetMicro = 2_000_000
	}
	if c.BudgetCapMicro <= 0 {
		c.BudgetCapMicro = 50_000_000
	}
	if c.SearchProvider == "" {
		c.SearchProvider = upstream.SearchDDGLite
	}
	if p := &c.Model.Pricing; p.Version == "" {
		p.Version = configPricingVersion(*p)
	}
	if len(c.Model.PricingByModel) > 0 {
		byModel := make(map[string]upstream.Pricing, len(c.Model.PricingByModel)) // 不改调用方的 map
		for m, p := range c.Model.PricingByModel {
			if p.Version == "" {
				p.Version = configPricingVersion(p)
			}
			byModel[m] = p
		}
		c.Model.PricingByModel = byModel
	}
	return c
}

func (c Config) validate() error {
	if c.RedisAddr != "" {
		if host, port, err := net.SplitHostPort(c.RedisAddr); err != nil || host == "" || port == "" {
			return fmt.Errorf("app: Redis 地址 %q 须为 host:port", c.RedisAddr)
		}
	}
	switch {
	case (c.TLSCertFile == "") != (c.TLSKeyFile == ""):
		return errors.New("app: TLS 证书与私钥须同时设置")
	case c.Capacity.RunSlots < 1 || c.Capacity.MemoryBytes <= 0:
		return fmt.Errorf("app: 容量须至少 1 个 run slot 且内存为正：%+v", c.Capacity)
	case c.DefaultRunTime > c.RunTimeCap:
		return fmt.Errorf("app: 默认运行时限 %s 超过服务端上限 %s", c.DefaultRunTime, c.RunTimeCap)
	case c.DefaultMemoryBytes > c.Capacity.MemoryBytes:
		return fmt.Errorf("app: 默认内存 %d 超过总容量 %d", c.DefaultMemoryBytes, c.Capacity.MemoryBytes)
	case c.DefaultBudgetMicro > c.BudgetCapMicro:
		return fmt.Errorf("app: 默认预算 %d 超过服务端上限 %d（微美元）", c.DefaultBudgetMicro, c.BudgetCapMicro)
	case c.Model.BaseURL != "" && c.Model.Name == "":
		return errors.New("app: 配置了模型上游地址但没有模型名")
	case c.Model.Pricing.InputMicroPerMTok < 0 || c.Model.Pricing.OutputMicroPerMTok < 0:
		return errors.New("app: 模型单价不能为负数")
	case len(c.Model.Models) > 0 && !slices.Contains(c.Model.Models, c.Model.Name):
		return fmt.Errorf("app: 模型白名单 %v 不含默认模型 %q", c.Model.Models, c.Model.Name)
	}
	for m, p := range c.Model.PricingByModel {
		if !c.Model.declared(m) {
			return fmt.Errorf("app: 模型 %q 有单价但未声明", m)
		}
		if p.InputMicroPerMTok < 0 || p.OutputMicroPerMTok < 0 {
			return fmt.Errorf("app: 模型 %q 的单价不能为负数", m)
		}
	}
	switch c.SearchProvider {
	case upstream.SearchFake:
		if len(c.UpstreamAllowPrivate) == 0 {
			return errors.New("app: 搜索供应商 fake 只用于测试，须同时设置 upstream_allow_private 指向本机 fake upstream")
		}
	case upstream.SearchTavily, upstream.SearchSerper:
		if c.SearchAPIKey == "" {
			return fmt.Errorf("app: 搜索供应商 %s 需要宿主环境变量 AGENTBOX_SEARCH_API_KEY", c.SearchProvider)
		}
	case upstream.SearchDDGLite:
	default:
		return fmt.Errorf("app: 搜索供应商须为 fake、tavily、serper 或 ddg_lite，得到 %q", c.SearchProvider)
	}
	if c.SearchBaseURL != "" {
		if u, err := url.Parse(c.SearchBaseURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("app: 搜索上游地址 %q 须为 http(s) 的绝对 URL", c.SearchBaseURL)
		}
	}
	for _, kv := range c.WorkerEnv {
		k, _, _ := strings.Cut(kv, "=")
		if !workerEnvAllowed[k] {
			return fmt.Errorf("app: Worker 环境变量 %q 不在白名单中（凭据不得进入沙箱）", k)
		}
	}
	return nil
}

// Ownership 是数据库所有权（会话级 advisory lock，§7.4）。Lost 在失去所有权时关闭（不可逆）。
type Ownership interface {
	Lost() <-chan struct{}
	Close() error
}

// Store 是全部持久化用例：各消费者的窄接口由同一实现提供（internal/persistence/postgres）。
type Store interface {
	ownership.InstallStore
	task.Store
	recovery.Store
	resource.Store
	runner.Store
	api.Store
	call.Store
	// Migrate 执行尚未应用的迁移（安装引导之后）。
	Migrate(ctx context.Context) error
	Close()
}

// Hooks 是可选的观察点（测试与只供测试的 main 使用），在调用它的 goroutine 中同步执行，不得阻塞。
type Hooks struct {
	// Step 在启动顺序的每一步完成后调用（名称见 Run 的说明）。
	Step func(name string)
	// Mode 在 API 运行模式变化时调用。
	Mode func(m api.Mode)
	// Recovered 在恢复完成、admission 重建与报警处理之后调用。
	Recovered func(r recovery.Report)
	// Listening 在 API 开始监听后调用，参数为实际地址。
	Listening func(addr string)
}

// Deps 是 Run 的依赖。DataDir、AcquireOwnership、OpenStore、NewProvider 必填；其余为空时取默认值。
type Deps struct {
	// DataDir 是数据目录（§7.4）。
	DataDir string
	// LockDataDir 取得 <data>/agentbox.lock 上的 flock，返回释放函数；默认 datadir.Acquire。
	LockDataDir func(dir string) (release func() error, err error)
	// AcquireOwnership 在专用连接上取得 advisory lock。
	AcquireOwnership func(ctx context.Context) (Ownership, error)
	// OpenStore 打开与 own 绑定的 Store（失去所有权后操作返回 persistence.ErrOwnershipLost）。
	OpenStore func(ctx context.Context, own Ownership) (Store, error)
	// NewProvider 以安装身份构造 provider（安装身份引导之后调用）。
	NewProvider func(installID string) (provider.Provider, error)
	// NewBlobs 构造 BlobStore；默认 blob.NewLocal(<data>/blobs)。
	NewBlobs func(dataDir string) (blob.Store, error)
	// Clock 是 actor、恢复期限与重试退避的时间来源；默认 task.SystemClock()。
	Clock task.Clock
	// NewID 生成 install_id、attempt_id 与 env_id；默认 128 位随机数的十六进制。
	NewID func() string
	// NewToken 生成数据目录引导令牌；默认 32 字节随机数。
	NewToken func() ([]byte, error)
	// Listen 建立 API 的监听；默认 net.Listen("tcp", addr)。
	Listen func(addr string) (net.Listener, error)
	// Logger 是结构化日志（§14.6）；默认 stderr 上的 JSON。
	Logger *slog.Logger
	Hooks  Hooks
}

func (d Deps) withDefaults() Deps {
	if d.LockDataDir == nil {
		d.LockDataDir = func(dir string) (func() error, error) {
			l, err := datadir.Acquire(dir)
			if err != nil {
				return nil, err
			}
			return l.Release, nil
		}
	}
	if d.NewBlobs == nil {
		d.NewBlobs = func(dir string) (blob.Store, error) { return blob.NewLocal(filepath.Join(dir, "blobs")) }
	}
	if d.Clock == nil {
		d.Clock = task.SystemClock()
	}
	if d.NewID == nil {
		d.NewID = randomHex
	}
	if d.NewToken == nil {
		d.NewToken = func() ([]byte, error) {
			b := make([]byte, datadir.TokenSize)
			_, err := rand.Read(b)
			return b, err
		}
	}
	if d.Listen == nil {
		d.Listen = func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	return d
}

func randomHex() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand 失败时无法安全生成 ID
	}
	return hex.EncodeToString(b[:])
}

// Run 按规格 §14.1 启动服务并运行到 ctx 结束、失去所有权或致命错误；返回即进程应退出。
//
// 顺序（Hooks.Step 的名称）：确认数据目录（data_dir）→ flock（flock）→ advisory lock（advisory_lock）
// → 安装身份引导与校验（install_identity）→ 迁移（migrate）→ 撤销全部 active 访问（revoke_access）→
// （账本转换：M2，recovery 中的空操作）→ provider.List 与独立原始扫描（scan）→ 读取恢复事实并生成计划
// （recovery_plan）→ recovery.Execute：停止、提交 lost 与恢复安排、运行时限补记（recovery_execute）→
// 报警并标记 → admission.Rebuild（admission_rebuild）→ cleanup loop（cleanup_loop）→ actor
// （scheduler）→ API（api）。恢复失败（例如 Store 不可用）按退避整体重跑（各步幂等）。
//
// 恢复期限（Config.RecoveryDeadline，自 Run 开始计）到期仍未就绪：API 以诊断模式启动（只开放 status 与
// inspect），不启动 actor、不执行；恢复之后完成时再依次启动 cleanup loop 与 actor 并切换到正常模式。
//
// 失去所有权（Ownership.Lost，或 actor/cleanup 遇到 persistence.ErrOwnershipLost）→ 不可逆的
// ownership_lost（§7.4）：API 拒绝写操作，取消在途操作与全部 actor，物理停止本机全部执行环境（不写
// 数据库），有期限清理后返回 ErrOwnershipLost。actor panic 或其他致命错误 → 致命停止（代码组织 §5），
// 步骤相同，返回该错误。ctx 结束 → 同样停止全部执行环境后返回 nil；重启走完整恢复。
func Run(ctx context.Context, cfg Config, d Deps) error {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	d = d.withDefaults()
	switch {
	case d.DataDir == "":
		return errors.New("app: 缺少数据目录")
	case d.AcquireOwnership == nil || d.OpenStore == nil || d.NewProvider == nil:
		return errors.New("app: 缺少 AcquireOwnership、OpenStore 或 NewProvider")
	}
	s := &server{cfg: cfg, d: d, log: d.Logger, fatalCh: make(chan error, 1)}
	s.mode.Store(api.Mode("")) // API 启动前总先设置模式
	deadline, stopDeadline := d.Clock.At(d.Clock.Now().Add(cfg.RecoveryDeadline))
	defer stopDeadline()
	return s.run(ctx, deadline)
}

// server 是一次 Run 的状态。
type server struct {
	cfg Config
	d   Deps
	log *slog.Logger

	store     Store
	own       Ownership
	prov      provider.Provider
	installID string
	adm       *admission.Admission
	coord     *resource.Coordinator
	taskDeps  task.Deps
	calls     *call.Coordinator // Gateway 的记账与 journal 所有者
	edge      *edge.Edge        // Gateway 的每 attempt 入口（task.Access）
	blobs     blob.Store        // runner、Gateway 与 API 产物下载共用的 BlobStore

	cache *cache.Source // Gateway 的共享缓存；nil 表示关闭
	redis *cache.Redis

	mode    atomic.Value // api.Mode
	sched   atomic.Pointer[task.Scheduler]
	fatalCh chan error

	workCtx    context.Context // 恢复、cleanup loop、actor、报警重试；停止时先取消
	cancelWork context.CancelFunc
	bg         sync.WaitGroup // 后台 goroutine（cleanup loop、报警重试）

	apiSrv    *http.Server
	apiCtx    context.Context // API 请求的基础 ctx：关闭 API 时取消（结束 SSE）
	cancelAPI context.CancelFunc
}

func (s *server) step(name string) {
	s.log.Info("启动步骤完成", "step", name)
	if s.d.Hooks.Step != nil {
		s.d.Hooks.Step(name)
	}
}

func (s *server) setMode(m api.Mode) {
	if s.mode.Swap(m) == m {
		return
	}
	s.log.Info("API 模式", "mode", string(m))
	if s.d.Hooks.Mode != nil {
		s.d.Hooks.Mode(m)
	}
}

func (s *server) currentMode() api.Mode { return s.mode.Load().(api.Mode) }

// fail 报告致命错误（非阻塞；只保留第一个）。
func (s *server) fail(err error) {
	select {
	case s.fatalCh <- err:
	default:
	}
}

func (s *server) run(ctx context.Context, deadline <-chan time.Time) error {
	dir := s.d.DataDir
	// §14.1 第 1 步：数据目录、flock、advisory lock、安装身份。
	// 数据目录 0711：沙箱 init 须能经过它到达任务 workspace（见 makeWorkspace）。
	if err := os.MkdirAll(dir, 0o711); err != nil {
		return fmt.Errorf("app: 数据目录: %w", err)
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return fmt.Errorf("app: 数据目录 %s 不可用: %v", dir, err)
	} else if err := searchable(dir, fi); err != nil {
		return err
	}
	s.step("data_dir")
	release, err := s.d.LockDataDir(dir)
	if err != nil {
		return fmt.Errorf("app: 数据目录锁: %w", err)
	}
	defer func() { _ = release() }()
	s.step("flock")
	own, err := s.d.AcquireOwnership(ctx)
	if err != nil {
		return fmt.Errorf("app: advisory lock: %w", err)
	}
	defer func() { _ = own.Close() }()
	s.own = own
	s.step("advisory_lock")
	store, err := s.d.OpenStore(ctx, own)
	if err != nil {
		return fmt.Errorf("app: 打开 Store: %w", err)
	}
	defer store.Close()
	s.store = store
	installID, err := ownership.Bootstrap(ctx, store, datadir.NewIDFile(dir), datadir.NewTokenFile(dir), s.d.NewID, s.d.NewToken)
	if err != nil {
		return fmt.Errorf("app: 安装身份（数据目录 %s）: %w", dir, err)
	}
	s.installID = installID
	s.step("install_identity")
	// 第 2 步：迁移。
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("app: 迁移: %w", err)
	}
	s.step("migrate")
	if err := s.assemble(); err != nil {
		return err
	}
	return s.serve(ctx, deadline)
}

// assemble 构造 provider、BlobStore、admission、coordinator、runner、Gateway 与 actor 的依赖。
func (s *server) assemble() error {
	prov, err := s.d.NewProvider(s.installID)
	if err != nil {
		return fmt.Errorf("app: provider: %w", err)
	}
	s.prov = prov
	blobs, err := s.d.NewBlobs(s.d.DataDir)
	if err != nil {
		return fmt.Errorf("app: BlobStore: %w", err)
	}
	s.blobs = blobs
	s.adm = admission.New(s.cfg.Capacity)
	s.coord = resource.NewCoordinator(s.store, prov, resource.Options{InstallID: s.installID,
		UIDBase: s.cfg.UIDBase, UIDCount: s.cfg.UIDCount, Backoff: s.cfg.RetryBackoff})
	run := runner.New(s.store, blobs, prov, prov.ResourceDiag, s.cfg.Runner)
	if err := s.assembleGateway(blobs); err != nil {
		return err
	}
	s.taskDeps = task.Deps{
		Store:     s.store,
		Admission: admissionAdapter{a: s.adm, cfg: s.cfg},
		Env:       envAdapter{c: s.coord, cfg: s.cfg, dataDir: s.d.DataDir},
		Runner:    runnerAdapter{r: run, cfg: s.cfg, dataDir: s.d.DataDir},
		Access:    s.edge,
		Clock:     s.d.Clock,
		IDs:       s.d.NewID,
		OnFatal: func(taskID string, err error) {
			s.fail(fmt.Errorf("app: 任务 %s 的 actor 致命错误: %w", taskID, err))
		},
		RunTimeLimit: s.cfg.runTimeLimit,
		// 恢复交来的 stop_blocked attempt 确认停止后补记运行时间（§14.1 第 8 步）。
		OnStopRecorded: func(ctx context.Context, attemptID string, stoppedAt time.Time) error {
			att, err := s.store.GetAttempt(ctx, attemptID)
			if err != nil {
				return err
			}
			return s.store.AccountUnrecordedRunTime(ctx, att.TaskID, attemptID, stoppedAt)
		},
	}
	return nil
}

// gatewayDir 是 Gateway socket 的目录 <data>/gateway（每个 attempt 一个 <attempt_id>.sock，规格 §9.1 按
// Plan 7 执行中修订：不在环境目录内，不作为 listener intent 记录）。
func gatewayDir(dataDir string) string { return filepath.Join(dataDir, "gateway") }

// assembleGateway 构造 Gateway：共用的验证 dialer（§9.8）→ 上游 adapter → call 协调器 → edge。
// 启动时清空 <data>/gateway：上一次运行的 socket 都已失效（重启时全部访问已撤销），edge 只为本次运行的
// attempt 建立入口。目录为 0711：沙箱 init 在 user namespace 中以映射 root 运行，须能经过它到达 socket
// （socket 本身 0600、属主为环境映射 uid 1000，由 provider 启动环境时 chown）。
func (s *server) assembleGateway(blobs blob.Store) error {
	dir := gatewayDir(s.d.DataDir)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("app: 清空 Gateway socket 目录: %w", err)
	}
	if err := os.Mkdir(dir, 0o711); err != nil {
		return fmt.Errorf("app: 建立 Gateway socket 目录: %w", err)
	}
	if err := os.Chmod(dir, 0o711); err != nil { // 不受 umask 影响
		return fmt.Errorf("app: 设置 Gateway socket 目录权限: %w", err)
	}
	dialer := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: s.cfg.UpstreamAllowPrivate})
	adapters, pricing, chatPricing := s.cfg.gatewayAdapters(dialer)
	cfg := call.Config{Store: s.store, Adapters: adapters, Pricing: pricing, ChatPricing: chatPricing,
		Blobs: blobs, Events: hostEvents{s: s.store}, Limits: s.cfg.Gateway, Logger: s.log}
	if err := s.assembleCache(blobs); err != nil {
		return err
	}
	if s.cache != nil {
		cfg.Cache = s.cache
	}
	calls, err := call.New(cfg)
	if err != nil {
		return fmt.Errorf("app: Gateway 调用协调器: %w", err)
	}
	s.calls = calls
	s.edge = edge.New(edge.Config{SocketDir: dir, Logger: s.log}, calls, attemptLookup{s: s.store})
	return nil
}

// assembleCache 加载缓存签名密钥并按配置构造共享缓存（§11.5）。密钥 `<data>/cache.key` 总在启动时加载，
// 不存在则生成（0600；即"安装时生成"，此后 `agentbox cache rotate-key` 可轮换，重启后生效），缓存关闭时也一样。
// 缓存默认开启，但没有 RedisAddr 时等价于关闭；Redis 在启动时不可达不阻止启动（之后的查找视为未命中）。
func (s *server) assembleCache(blobs blob.Store) error {
	signer, err := cache.LoadKeys(s.d.DataDir)
	if err != nil {
		return fmt.Errorf("app: 缓存签名密钥: %w", err)
	}
	switch {
	case s.cfg.CacheOff:
		s.log.Info("Gateway 共享缓存已关闭（--cache=off）")
		return nil
	case s.cfg.RedisAddr == "":
		s.log.Info("Gateway 共享缓存未启用：没有配置 Redis 地址（--redis-addr）")
		return nil
	}
	r := cache.NewRedis(cache.RedisConfig{Addr: s.cfg.RedisAddr})
	src, err := cache.NewSource(cache.SourceConfig{KV: r, Signer: signer, Blobs: blobs})
	if err != nil {
		_ = r.Close() // 尚未使用的客户端，关闭错误无关紧要
		return fmt.Errorf("app: Gateway 共享缓存: %w", err)
	}
	s.redis, s.cache = r, src
	s.log.Info("Gateway 共享缓存已启用", "redis_addr", s.cfg.RedisAddr)
	return nil
}

// cacheMetrics 返回共享缓存的指标（/status）；缓存关闭时为 nil。
func (s *server) cacheMetrics() map[string]int64 {
	if s.cache == nil {
		return nil
	}
	return s.cache.Metrics()
}

// recovered 是恢复 goroutine 的结果。
type recovered struct {
	report recovery.Report
	err    error
}

// serve 运行恢复并按就绪情况启动执行与 API，直到停止条件出现。
func (s *server) serve(ctx context.Context, deadline <-chan time.Time) error {
	s.workCtx, s.cancelWork = context.WithCancel(context.Background())
	defer s.cancelWork()
	s.apiCtx, s.cancelAPI = context.WithCancel(context.Background())
	defer s.cancelAPI()

	recCh := make(chan recovered, 1)
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		r, err := s.recoverLoop(s.workCtx)
		recCh <- recovered{r, err}
	}()

	var reason error
	ready := false
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-s.own.Lost():
			reason = ErrOwnershipLost
			break loop
		case err := <-s.fatalCh:
			reason = err
			break loop
		case <-deadline:
			deadline = nil
			if ready {
				continue
			}
			s.log.Error("启动恢复超过期限仍未就绪：API 以诊断模式启动，禁止执行", "deadline", s.cfg.RecoveryDeadline.String())
			s.setMode(api.ModeDiagnostic)
			if err := s.startAPI(); err != nil {
				reason = err
				break loop
			}
		case r := <-recCh:
			recCh = nil
			if r.err != nil { // 只有失去所有权或 ctx 结束时 recoverLoop 才放弃
				if errors.Is(r.err, persistence.ErrOwnershipLost) {
					reason = ErrOwnershipLost
					break loop
				}
				continue
			}
			ready = true
			if err := s.startExecution(r.report); err != nil {
				reason = err
				break loop
			}
			s.setMode(api.ModeNormal)
			if err := s.startAPI(); err != nil {
				reason = err
				break loop
			}
		}
	}
	if errors.Is(reason, persistence.ErrOwnershipLost) {
		reason = ErrOwnershipLost
	}
	return s.shutdown(reason)
}

// recoverLoop 执行恢复（§14.1 第 3–8 步），失败后按退避整体重跑，直到就绪、失去所有权或 ctx 结束。
func (s *server) recoverLoop(ctx context.Context) (recovery.Report, error) {
	for try := 0; ; try++ {
		r, err := s.recoverOnce(ctx)
		if err == nil {
			return r, nil
		}
		if ctx.Err() != nil {
			return r, ctx.Err()
		}
		if errors.Is(err, persistence.ErrOwnershipLost) {
			return r, err
		}
		s.log.Error("启动恢复失败，退避后重跑", "error", err.Error(), "try", try)
		if !s.sleep(ctx, s.cfg.RetryBackoff(try)) {
			return r, ctx.Err()
		}
	}
}

func (s *server) recoverOnce(ctx context.Context) (recovery.Report, error) {
	// 第 3 步：单事务撤销全部 active 访问。第 4 步的账本转换在 recovery.Execute 中（见下）。
	n, err := s.store.RevokeAllActive(ctx, "server_restart")
	if err != nil {
		return recovery.Report{}, fmt.Errorf("撤销 active 访问: %w", err)
	}
	s.log.Info("已撤销 active 访问", "count", n)
	// 第 4 步（M2 的 journal 部分）：遗留的 resolving 调用（无 try）复位为可重新解析，期限不变（§9.7、§11.2）。
	reset, err := s.store.ResetResolving(ctx)
	if err != nil {
		return recovery.Report{}, fmt.Errorf("复位 resolving 调用: %w", err)
	}
	s.log.Info("已复位遗留的 resolving 调用", "count", reset)
	s.step("revoke_access")
	// 第 5 步：provider.List 与独立原始扫描，逐层报告。
	envs, err := s.prov.List(ctx)
	if err != nil {
		return recovery.Report{}, fmt.Errorf("provider.List: %w", err)
	}
	scan, err := s.prov.Scan(ctx)
	if err != nil {
		return recovery.Report{}, fmt.Errorf("provider.Scan: %w", err)
	}
	layers := map[string]int{}
	for _, it := range scan.Items {
		layers[it.Layer]++
	}
	s.log.Info("恢复扫描", "listed_envs", len(envs), "scan_items", len(scan.Items), "layers", layers)
	s.step("scan")
	facts, err := s.store.LoadRecoveryFacts(ctx)
	if err != nil {
		return recovery.Report{}, fmt.Errorf("读取恢复事实: %w", err)
	}
	plan := reconcile.Plan(recovery.ReconcileFacts(facts), scan, s.installID)
	s.step("recovery_plan")
	// 第 6–8 步：停止、提交 lost 与恢复安排、运行时限补记（由 recovery.Execute 完成，装配不重复）。
	r, err := recovery.Execute(ctx, plan, recovery.Deps{Store: s.store, Tasks: s.store, Resources: s.store,
		Coordinator: s.coord, Clock: s.d.Clock.Now, Options: recovery.Options{DefaultMemoryBytes: s.cfg.DefaultMemoryBytes}})
	// 第 4 步（账本转换）在 Execute 的第一步完成：上一进程留下的 held 预留与 in_flight 调用转为 unknown。
	s.log.Info("账本转换", "reservations", r.Ledger.Reservations, "calls", r.Ledger.Calls, "unknown_micro", r.Ledger.UnknownMicro)
	if err != nil {
		return r, err
	}
	if !r.Ready {
		return r, errors.New("recovery: 报告未就绪")
	}
	s.log.Info("启动恢复完成", "steps", len(r.Steps), "stop_blocked", len(r.StopBlocked),
		"quarantined", len(r.Quarantined), "excluded", len(r.Excluded), "occupied", len(r.Occupied))
	s.step("recovery_execute")
	return r, nil
}

// startExecution：报警、重建 admission（第 9 步），然后依次启动 cleanup loop 与 actor（第 10 步）。
func (s *server) startExecution(r recovery.Report) error {
	s.alert(r.Alerts)
	s.adm.Rebuild(r.Occupied)
	s.step("admission_rebuild")
	if s.d.Hooks.Recovered != nil {
		s.d.Hooks.Recovered(r)
	}

	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		if err := s.coord.RunCleanup(s.workCtx); errors.Is(err, persistence.ErrOwnershipLost) {
			s.fail(err)
		}
	}()
	s.step("cleanup_loop")

	h := stopBlockedHandoff(r)
	sched := task.NewScheduler(s.workCtx, s.taskDeps)
	if err := sched.Start(h.opts); err != nil {
		return fmt.Errorf("app: 启动 actor: %w", err)
	}
	s.sched.Store(sched)
	// 交给 actor 的任务若没有建立 actor（任务已终态，ListActiveTasks 不返回），其环境同样由后台重试停止。
	// stop_blocked 的 actor 在确认停止并归还槽位之前不会退出，因此此时没有 actor 即未建立。
	orphans := h.orphans
	for taskID, env := range h.actorEnv {
		if sched.Actor(taskID) == nil {
			orphans = append(orphans, orphanStop{EnvID: env, Grant: h.grants[taskID], HasGrant: true})
		}
	}
	for _, o := range orphans {
		s.bg.Add(1)
		go s.stopRetrier(o)
	}
	s.step("scheduler")
	return nil
}

// stopBlockedHandoff 把恢复报告转换为 Scheduler 的启动选项：Excluded 的任务不建立 actor；当前 attempt
// 的环境未确认停止（步骤状态 stop_blocked）的任务以 stop_blocked 启动 actor，并以恢复为它重建的占用
// （Occupied 中同一任务的授予）作为已持有的槽位。
//
// 其余 stop_blocked 的环境（所属任务已终态、被排除，或不是任务当前 attempt 的环境）没有 actor 负责：
// 作为 orphans 返回，由后台重试停止（stopRetrier）。每个环境取一份 Occupied 中的授予（同一任务的，
// 否则无任务的），确认停止后归还。recovery 的占用是每个环境一份、按任务计内存，报告没有环境到授予的
// 映射；同一任务（或同为无任务）的授予在计数与内存上等价，取哪一份不影响 admission 的账。
func stopBlockedHandoff(r recovery.Report) handoff {
	h := handoff{opts: task.StartOptions{StopBlocked: map[string]task.SlotGrant{}, Excluded: r.Excluded},
		actorEnv: map[string]string{}, grants: map[string]admission.Grant{}}
	excluded := map[string]bool{}
	for _, id := range r.Excluded {
		excluded[id] = true
	}
	pool := map[string][]admission.Grant{}
	for _, g := range r.Occupied {
		pool[g.TaskID] = append(pool[g.TaskID], g)
	}
	take := func(keys ...string) (admission.Grant, bool) {
		for _, k := range keys {
			if gs := pool[k]; len(gs) > 0 {
				pool[k] = gs[1:]
				return gs[0], true
			}
		}
		return admission.Grant{}, false
	}
	envTask := map[string]string{}
	for _, st := range r.Steps {
		if st.EnvID != "" && st.TaskID != "" {
			envTask[st.EnvID] = st.TaskID
		}
		// 当前 attempt 的环境未确认停止（settle 步骤 stop_blocked）：交给该任务的 actor。
		settle := st.Kind == reconcile.MarkAttemptLost || st.Kind == reconcile.CancelTask || st.Kind == reconcile.PauseTask
		if !settle || st.Status != recovery.StepStopBlocked || st.TaskID == "" || st.EnvID == "" || excluded[st.TaskID] {
			continue
		}
		if _, done := h.actorEnv[st.TaskID]; done {
			continue
		}
		if g, ok := take(st.TaskID); ok {
			h.opts.StopBlocked[st.TaskID] = slotGrant(g)
			h.actorEnv[st.TaskID] = st.EnvID
			h.grants[st.TaskID] = g
		}
	}
	handled := map[string]bool{}
	for _, env := range h.actorEnv {
		handled[env] = true
	}
	for _, env := range r.StopBlocked {
		if handled[env] {
			continue
		}
		handled[env] = true
		g, ok := take(envTask[env], "")
		h.orphans = append(h.orphans, orphanStop{EnvID: env, Grant: g, HasGrant: ok})
	}
	return h
}

// handoff 是恢复报告交给执行的部分。
type handoff struct {
	opts     task.StartOptions
	actorEnv map[string]string          // 任务 → 交给其 actor 的环境
	grants   map[string]admission.Grant // 任务 → 交给其 actor 的授予
	orphans  []orphanStop
}

// orphanStop 是没有 actor 负责的 stop_blocked 环境及其占用。
type orphanStop struct {
	EnvID    string
	Grant    admission.Grant
	HasGrant bool
}

// stopRetrier 停止没有 actor 负责的 stop_blocked 环境（§14.1"停止失败"：占用容量、退避重试）：按
// task.RetryBackoff 的退避（基数 2 s、上限 60 s、抖动）调用 Coordinator.StopEnv，直到结果为 Recorded，
// 然后经 admission 归还其授予，清理交给 cleanup loop。Blocked、Stopped 而未 Recorded（Store 不可用）
// 与错误都继续占用并重试。在 Run 的生命周期内运行，停止时随 workCtx 结束。
func (s *server) stopRetrier(o orphanStop) {
	defer s.bg.Done()
	for try := int64(0); ; try++ {
		r, err := s.coord.StopEnv(s.workCtx, o.EnvID)
		if s.workCtx.Err() != nil {
			return
		}
		if err == nil && r.Recorded {
			if o.HasGrant {
				s.adm.Release(o.Grant)
			}
			s.log.Info("stop_blocked 环境已确认停止，归还占用", "env_id", o.EnvID, "grant_id", o.Grant.ID, "tries", try+1)
			return
		}
		if errors.Is(err, persistence.ErrOwnershipLost) {
			s.fail(err)
			return
		}
		detail := "停止无法确认"
		if err != nil {
			detail = err.Error()
		} else if r.Stopped {
			detail = "停止已确认，stopped_at 尚未持久化"
		}
		s.log.Warn("stop_blocked 环境停止未完成，退避重试", "env_id", o.EnvID, "detail", detail, "try", try)
		if !s.sleep(s.workCtx, task.RetryBackoff(try, mrand.Float64())) {
			return
		}
	}
}

// alert 以结构化错误日志发出报警（M1 的报警出口），再记录已报警（I8）；标记失败时在后台按退避重试。
func (s *server) alert(alerts []recovery.Alert) {
	var retry []string
	for _, a := range alerts {
		s.log.Error("隔离资源报警", "alert", "quarantine", "step_id", a.StepID, "task_id", a.TaskID, "env_id", a.EnvID,
			"layer", a.Layer, "path", a.Path, "reason", a.Reason)
		if err := s.store.MarkQuarantineAlerted(s.workCtx, a.Path); err != nil && !s.markDone(a.Path, err) {
			retry = append(retry, a.Path)
		}
	}
	if len(retry) == 0 {
		return
	}
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		for _, path := range retry {
			for try := 0; ; try++ {
				if !s.sleep(s.workCtx, s.cfg.RetryBackoff(try)) {
					return
				}
				err := s.store.MarkQuarantineAlerted(s.workCtx, path)
				if err == nil || s.markDone(path, err) {
					break
				}
			}
		}
	}()
}

// markDone 判断标记失败是否不必重试（隔离记录不存在、失去所有权或正在停止），并记录日志。
func (s *server) markDone(path string, err error) bool {
	if s.workCtx.Err() != nil || errors.Is(err, persistence.ErrOwnershipLost) {
		return true
	}
	if errors.Is(err, persistence.ErrNotFound) {
		s.log.Error("标记报警：隔离记录不存在", "path", path, "error", err.Error())
		return true
	}
	s.log.Warn("标记报警失败，退避重试", "path", path, "error", err.Error())
	return false
}

// sleep 按 Clock 等待 d；ctx 先结束时返回 false。
func (s *server) sleep(ctx context.Context, d time.Duration) bool {
	c, stop := s.d.Clock.At(s.d.Clock.Now().Add(d))
	defer stop()
	select {
	case <-c:
		return true
	case <-ctx.Done():
		return false
	}
}

// startAPI 启动 API（已启动时为空操作）。
func (s *server) startAPI() error {
	if s.apiSrv != nil {
		return nil
	}
	var tlsCfg *tls.Config
	if s.cfg.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("app: 加载 TLS 证书: %w", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	ln, err := s.d.Listen(s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("app: API 监听 %s: %w", s.cfg.Listen, err)
	}
	h, err := api.New(api.Config{
		Store: notifyingStore{Store: s.store, s: s}, Blobs: s.blobs, Mode: s.currentMode, ListenAddr: ln.Addr().String(),
		Token: s.cfg.APIToken, AllowedHosts: s.cfg.AllowedHosts, AllowedOrigins: s.cfg.AllowedOrigins,
		TLS: tlsCfg != nil, WebDir: s.cfg.WebDir,
		Logger: s.log, ConfigVersion: s.cfg.ConfigVersion, MaxFaultRetries: s.cfg.MaxFaultRetries,
		EffectiveLimits: s.cfg.effectiveLimits, CacheMetrics: s.cacheMetrics,
	})
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("app: API: %w", err)
	}
	s.apiSrv = &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second,
		BaseContext: func(net.Listener) context.Context { return s.apiCtx }, TLSConfig: tlsCfg}
	go func() {
		serve := s.apiSrv.Serve
		if tlsCfg != nil {
			serve = func(l net.Listener) error { return s.apiSrv.ServeTLS(l, "", "") } // 证书已在 TLSConfig 中
		}
		if err := serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.fail(fmt.Errorf("app: API 服务: %w", err))
		}
	}()
	s.step("api")
	if s.d.Hooks.Listening != nil {
		s.d.Hooks.Listening(ln.Addr().String())
	}
	return nil
}

// shutdown 是停止（§7.4 ownership_lost、代码组织 §5 致命停止，以及正常退出）：先拒绝新操作，再取消在途
// 操作与全部 actor，物理停止本机全部执行环境（不写数据库），关闭 API，再有期限地等待 actor 与后台任务
// 退出。返回 reason（正常退出为 nil）。
func (s *server) shutdown(reason error) error {
	switch {
	case errors.Is(reason, persistence.ErrOwnershipLost):
		s.log.Error("失去数据库所有权：进入 ownership_lost，停止全部执行后退出")
		s.setMode(api.ModeOwnershipLost)
	case reason != nil:
		s.log.Error("致命停止：停止全部执行后退出", "error", reason.Error())
		s.setMode(api.ModeDiagnostic)
	default:
		s.log.Info("服务停止：停止全部执行后退出")
		s.setMode(api.ModeDiagnostic)
	}
	s.cancelWork()
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	s.stopAllEnvs(ctx)
	// 先关闭 API（等待在途请求结束），之后不会再有 Submit 建立 actor，再等待 actor 退出。
	if s.apiSrv != nil {
		s.cancelAPI()
		if err := s.apiSrv.Shutdown(ctx); err != nil {
			_ = s.apiSrv.Close()
		}
	}
	waitOrTimeout(ctx, func() {
		if sc := s.sched.Load(); sc != nil {
			sc.Wait()
		}
		s.bg.Wait()
	}, func() { s.log.Error("等待 actor 与后台任务退出超时") })
	// Gateway：关闭全部入口（删除 socket），再让 call 协调器取消在途 try 并等待其结算（有期限）。
	if s.edge != nil {
		if err := s.edge.Close(); err != nil {
			s.log.Error("关闭 Gateway 入口失败", "error", err.Error())
		}
	}
	if s.calls != nil {
		waitOrTimeout(ctx, s.calls.Close, func() { s.log.Error("等待 Gateway 在途调用结算超时") })
	}
	// 共享缓存：在途调用结算之后不再有新的写入；等待已提交的异步写入（每个有上限）后关闭 Redis 连接。
	if s.cache != nil {
		waitOrTimeout(ctx, s.cache.Close, func() { s.log.Error("等待缓存异步写入超时") })
		if err := s.redis.Close(); err != nil {
			s.log.Error("关闭 Redis 连接失败", "error", err.Error())
		}
	}
	return reason
}

// stopAllEnvs 物理停止本安装的全部执行环境（provider.Stop，不写数据库；stopped_at 由重启后的恢复记录）。
func (s *server) stopAllEnvs(ctx context.Context) {
	envs, err := s.prov.List(ctx)
	if err != nil {
		s.log.Error("停止全部环境：provider.List 失败", "error", err.Error())
		return
	}
	var wg sync.WaitGroup
	for _, e := range envs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := s.prov.Stop(ctx, id); err != nil {
				s.log.Error("停止环境失败", "env_id", id, "error", err.Error())
			}
		}(e.EnvID)
	}
	wg.Wait()
	s.log.Info("已停止全部执行环境", "count", len(envs))
}

func waitOrTimeout(ctx context.Context, wait func(), onTimeout func()) {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		onTimeout()
	}
}

// notifyingStore 在任务创建与控制写入提交后通知 Scheduler（§8.1"提交后才确认接受，再尽力通知
// actor"）：新任务建立 actor，已有 actor 被通知。提交结果未知后经 GetRequest 核对到的请求同样通知。
type notifyingStore struct {
	api.Store
	s *server
}

func (n notifyingStore) submit(taskID string) {
	if sc := n.s.sched.Load(); sc != nil && taskID != "" && n.s.workCtx.Err() == nil {
		sc.Submit(taskID)
	}
}

func (n notifyingStore) CreateTask(ctx context.Context, req api.CreateTaskRequest) (api.CreateTaskResult, error) {
	r, err := n.Store.CreateTask(ctx, req)
	if err == nil {
		n.submit(r.TaskID)
	}
	return r, err
}

func (n notifyingStore) AcceptControl(ctx context.Context, req api.ControlRequest) (api.ControlResult, error) {
	r, err := n.Store.AcceptControl(ctx, req)
	if err == nil {
		n.submit(r.TaskID)
	}
	return r, err
}

func (n notifyingStore) GetRequest(ctx context.Context, requestID string) (api.RequestRecord, error) {
	r, err := n.Store.GetRequest(ctx, requestID)
	if err == nil {
		n.submit(r.ResourceID)
	}
	return r, err
}
