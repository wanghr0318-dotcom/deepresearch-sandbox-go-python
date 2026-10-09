//go:build linux

package main

// 本文件是 `agentbox server`：解析参数，构造具体实现（PostgreSQL Store、advisory lock、provider/local），
// 调用 app.Run（代码组织规则 5：具体实现只在 cmd/agentbox 装配）。

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/admission"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/hostcheck"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/local"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/rootfs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/subrun"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/telemetry"
)

const (
	databaseURLEnv    = "AGENTBOX_DATABASE_URL"
	defaultCgroupRoot = "/sys/fs/cgroup"
	// 供应商 Key 的宿主环境变量（规格 §9.9：只从宿主环境加载到 core）。
	modelKeyEnv  = "AGENTBOX_MODEL_API_KEY"
	searchKeyEnv = "AGENTBOX_SEARCH_API_KEY"
)

// defaultWorkerEnv 是沙箱内 Worker 的默认环境：worker 包安装在默认模板包含的 rootfs.WorkerDir 中。
const defaultWorkerEnv = "PYTHONPATH=" + rootfs.WorkerDir

// prepareIsolation 在取得任何锁、连接数据库之前确认能够以隔离方式执行任务（规格 §4.5、§4.6）：
// 默认 rootfs 模板的宿主路径齐全（缺失时报告路径）；启用 exec 时 exec 模板齐全且 python3 可解析
// （rootfs.ExecTemplate().EnsureExec），并计算其摘要（exec 指纹的 image_digest，Plan 15 D5）；构造生产启动器
// （本进程成为 child subreaper）。任何一项失败都拒绝启动，不以未隔离的方式运行任务。
func prepareIsolation(exec bool) (local.EnvStarter, string, error) {
	tpl, err := rootfs.ResolveTemplate(rootfs.DefaultTemplateName)
	if err != nil {
		return nil, "", err
	}
	if err := tpl.Ensure(); err != nil {
		return nil, "", fmt.Errorf("rootfs 模板 %q 不可用（worker 包须安装在 %s）: %w", rootfs.DefaultTemplateName, rootfs.WorkerDir, err)
	}
	digest := ""
	if exec {
		et := rootfs.ExecTemplate()
		if err := et.EnsureExec(); err != nil {
			return nil, "", fmt.Errorf("exec 模板 %q 不可用（--exec-slots 0 可关闭 exec）: %w", rootfs.ExecTemplateName, err)
		}
		if digest, err = rootfs.TemplateDigest(et); err != nil {
			return nil, "", fmt.Errorf("exec 模板 %q 的摘要: %w", rootfs.ExecTemplateName, err)
		}
	}
	starter, err := local.NewProcessStarter()
	if err != nil {
		return nil, "", err
	}
	return starter, digest, nil
}

func runServer(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", "", "数据目录（必填）")
	dsn := fs.String("database-url", os.Getenv(databaseURLEnv), "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	listen := fs.String("listen", "127.0.0.1:8080", "API 监听地址；非 loopback 地址需要 <data>/api.token 与 --allowed-host")
	cgroupRoot := fs.String("cgroup-root", defaultCgroupRoot, "cgroup v2 挂载点")
	runSlots := fs.Int("run-slots", 4, "并发执行的任务数（run slots）")
	memory := fs.Int64("memory-bytes", 8<<30, "全部任务环境的内存总量（字节）")
	defaultMemory := fs.Int64("default-memory-bytes", 1<<30, "limits 未设置 memory_max 时每个任务的内存（字节）")
	defaultRunTime := fs.Duration("default-run-time", time.Hour, "累计运行时限的默认值：创建时未指定 limits.max_run_time_ms 的任务以此存储")
	runTimeCap := fs.Duration("run-time-cap", 24*time.Hour, "显式累计运行时限的服务端上限")
	allowedHost := fs.String("allowed-host", "", "非 loopback 监听时 Host 头的允许值（逗号分隔）")
	allowedOrigin := fs.String("allowed-origin", "", "浏览器 Origin 的允许值（逗号分隔，不允许通配）；默认为 <scheme>://监听地址 与 <scheme>://每个允许的 Host")
	webDir := fs.String("web-dir", "", "同源提供的工作台静态文件目录（SPA 回退到 index.html；不影响 API 路径）")
	tlsCert := fs.String("tls-cert", "", "内置 TLS 的证书（PEM，须与 --tls-key 同用）；非 loopback 监听且未设置时启动警告")
	tlsKey := fs.String("tls-key", "", "内置 TLS 的私钥（PEM，须与 --tls-cert 同用）")
	workerArgv := fs.String("worker-argv", "python3,-m,sim_worker", "沙箱内启动 Worker 的命令（逗号分隔的 argv）")
	workerEnv := fs.String("worker-env", defaultWorkerEnv, "沙箱内 Worker 的环境变量（逗号分隔的 KEY=VALUE；键须在白名单中，如 PYTHONPATH）")
	defaultBudget := fs.Int64("default-budget-micro", 2_000_000, "task 层预算的默认值（微美元）：创建时未指定 limits.budget_micro 的任务以此存储")
	budgetCap := fs.Int64("budget-cap-micro", 50_000_000, "显式 limits.budget_micro 的服务端上限（微美元）")
	modelBaseURL := fs.String("model-base-url", "", "OpenAI 兼容模型上游地址（例如 https://api.openai.com/v1）；为空时不提供 /v1/chat/completions。Key 只读环境变量 "+modelKeyEnv)
	modelName := fs.String("model-name", "", "默认模型名：请求未指定 model 时使用（配置了 --model-base-url 时必填）")
	models := fs.String("models", "", "声明的模型白名单（逗号分隔；须包含 --model-name）；请求可在其中选择 model，白名单外为 400 unsupported_model。空时只声明 --model-name")
	priceIn := fs.Int64("model-price-in-micro-per-mtok", 3_000_000, "默认模型输入单价（每百万 token 的微美元），用于预留估算与结算；未在 --model-price 中给出的模型用它")
	priceOut := fs.Int64("model-price-out-micro-per-mtok", 15_000_000, "默认模型输出单价（每百万 token 的微美元），用于预留估算与结算；未在 --model-price 中给出的模型用它")
	var modelPrices listFlag
	fs.Var(&modelPrices, "model-price", "按模型的单价 model=IN:OUT（每百万 token 的微美元；可重复或逗号分隔；模型须已声明）")
	searchBaseURL := fs.String("search-base-url", "", "搜索供应商地址覆盖（空时取供应商默认）；与 --search-provider fake 同用时 fake 搜索向 <地址>/search 发出请求（测试用 fake upstream，主机须在 --upstream-allow-private 中）")
	searchProvider := fs.String("search-provider", upstream.SearchDDGLite, "搜索供应商：ddg_lite | tavily | serper | fake（tavily 与 serper 的 Key 只读环境变量 "+searchKeyEnv+"；serper 为经 Serper.dev 的 Google 结果；fake 只用于测试，须同时设置 --upstream-allow-private）")
	fallbackFile := fs.String("model-fallback-file", "", "模型降级链：按顺序列出后备供应商的 JSON 文件（name、base_url、key_env、models、price、model_prices；见 docs/usage.zh-CN.md）。为空时只有主供应商（--model-base-url），行为不变")
	breakerFailures := fs.Int("model-breaker-failures", call.DefaultBreakerFailures, "降级链：供应商连续失败多少次后熔断（跳过它，直到 --model-breaker-open 之后的半开试探成功）")
	breakerOpen := fs.Duration("model-breaker-open", call.DefaultBreakerOpen, "降级链：熔断打开时长（之后放行一次半开试探）")
	tryTimeout := fs.Duration("model-try-timeout", 0, "降级链：每次模型 try 的超时（0 = 不设，只受 --model-call-deadline 约束；不设时挂起的供应商无法转到下一个）")
	hedgeDelay := fs.Duration("model-hedge-delay", 0, "降级链：对冲请求的延迟（0 = 关闭）。try 发出这么久仍未结束时向下一个就绪供应商并发发出同一请求，先成功者胜出；落败方已发出时按估算全额计入 unknown（最多约两倍费用）")
	maxTokensCap := fs.Int("model-max-tokens-cap", 32768, "模型请求 max_tokens 的上限（超出者截断并进入 applied_defaults）；推理模型先消耗推理 token，过低会得到空回答")
	allowPrivate := fs.String("upstream-allow-private", "", "显式放行的私有上游主机（逗号分隔的 host 或 host:port；例如本机模型服务或测试用 fake upstream）")
	redisAddr := fs.String("redis-addr", "", "共享缓存的 Redis 地址 host:port（例如 deploy/docker-compose.yml 的 127.0.0.1:6379）；为空时缓存不启用")
	cacheMode := fs.String("cache", "on", "搜索与抓取的共享缓存：on | off（on 且配置了 --redis-addr 时生效；Redis 不可用时视为未命中）")
	callDeadline := fs.Duration("call-deadline", call.DefaultCallDeadline, "搜索与抓取（/v1/search、/v1/fetch）的调用期限：自 Tx1 起计，含排队、退避与全部 try（须 > 0）")
	modelCallDeadline := fs.Duration("model-call-deadline", call.DefaultModelCallDeadline, "模型调用（/v1/chat/completions）的调用期限：推理模型的长输出可能超过 120 s（须 > 0）")
	userOrchestrator := fs.String("user-orchestrator-model", "kimi-k3", "用户研究的编排模型（spec.orchestrator_model；须为声明的模型）。配置了 --model-base-url 时启用用户账号")
	userWorker := fs.String("user-worker-model", "kimi-k2.6", "用户研究的 worker 模型（spec.worker_model；须为声明的模型）")
	turnToolBudget := fs.Int("turn-tool-budget", app.DefaultTurnToolBudget, "会话每个 turn 的 web_search 与 web_fetch 调用额度（1–1000；第 N+1 次 429 tool_budget_exhausted）")
	sessionIdleFreeze := fs.Duration("session-idle-freeze", app.DefaultSessionIdleFreeze, "会话空闲多久后冻结（须 > 0）")
	sessionEvictAfter := fs.Duration("session-evict-after", app.DefaultSessionEvictAfter, "会话自空闲起多久后驱逐（须大于 --session-idle-freeze；内存压力下按 LRU 提前驱逐）")
	sessionWorkerArgv := fs.String("session-worker-argv", "", "会话 incarnation 内启动 Worker 的命令（逗号分隔的 argv，例如 python3,-m,chatagent）；为空时不启用会话（会话端点 503 sessions_unavailable），非空时需要 --model-base-url")
	workerSubruns := fs.Bool("worker-subruns", true, "在 init 中请求 sub-run 扩展（init.extensions = [\"subruns\"]；Worker 的 ready 须回 subruns: 1，否则以 extension_mismatch 拒绝）")
	subrunCancelTimeout := fs.Duration("subrun-cancel-timeout", subrun.DefaultCancelTimeout, "宿主取消 sub-run 后等待其 subrun_end 的时限 T_subrun_cancel（须 > 0；超时终止整个 attempt，会话模式终止 incarnation）")
	var ex app.ExecConfig
	fs.IntVar(&ex.Slots, "exec-slots", app.DefaultExecSlots, "全局并发 exec 数（/v1/exec 的独立沙箱）；0 关闭 exec（/v1/exec 为 404）。exec 内存总量 = 本值 × --exec-memory-max，不计入 --memory-bytes，须一并规划")
	fs.IntVar(&ex.PerTask, "exec-per-task", app.DefaultExecPerTask, "每个任务的并发 exec 上限（不超过 --exec-slots）")
	fs.Int64Var(&ex.CountLimit, "exec-count-limit", app.DefaultExecCountLimit, "每个任务的 exec 次数配额")
	fs.Int64Var(&ex.CPUSeconds, "exec-cpu-seconds", app.DefaultExecCPUSeconds, "每个任务的 exec CPU 配额（CPU 秒）")
	fs.DurationVar(&ex.WallLimit, "exec-wall-limit", app.DefaultExecWallLimit, "每个任务的 exec 累计运行时间配额")
	fs.DurationVar(&ex.WallDefault, "exec-wall-default", app.DefaultExecWallDefault, "单次 exec 未指定 limits.wall_ms 时的运行时限")
	fs.DurationVar(&ex.WallMax, "exec-wall-max", app.DefaultExecWallMax, "单次 exec 运行时限的上限（请求值截断到它）")
	fs.Int64Var(&ex.MemoryDefault, "exec-memory-default", app.DefaultExecMemoryDefault, "单次 exec 未指定 limits.memory_bytes 时的内存（字节）")
	fs.Int64Var(&ex.MemoryMax, "exec-memory-max", app.DefaultExecMemoryMax, "单次 exec 内存的上限（字节；请求值截断到它）")
	fs.DurationVar(&ex.QueueTimeout, "exec-queue-timeout", app.DefaultExecQueueTimeout, "exec 等待 slot 的上限（超过为 504 exec_queue_timeout）")
	fs.Int64Var(&ex.PidsMax, "exec-pids-max", app.DefaultExecPidsMax, "exec 环境的 pids.max")
	fs.Int64Var(&ex.CPUQuotaUs, "exec-cpu-quota-us", app.DefaultExecCPUQuotaUs, "exec 环境 cpu.max 的 quota（period 100000；100000 = 1 核），也是 CPU 预留的速率")
	fs.Int64Var(&ex.TmpBytes, "exec-tmp-bytes", app.DefaultExecTmpBytes, "exec 环境 /tmp tmpfs 的大小（字节）")
	fs.Int64Var(&ex.OutBytes, "exec-out-bytes", app.DefaultExecOutBytes, "exec 环境 /out tmpfs 的大小（字节；也是 RLIMIT_FSIZE）")
	// 可观测性（docs/design/2026-10-10-observability-design.md）：默认全部关闭，行为与此前相同。
	var tel telemetry.Config
	fs.StringVar(&tel.OTLPEndpoint, "otlp-endpoint", "", "OpenTelemetry trace 导出地址（OTLP/HTTP，例如 http://127.0.0.1:4318）；为空时不追踪")
	fs.Float64Var(&tel.SampleRatio, "trace-sample-ratio", 1, "新 trace 的采样比例 (0, 1]（父 span 已采样的子 span 跟随父 span）")
	fs.StringVar(&tel.MetricsListen, "metrics-listen", "", "Prometheus /metrics 的监听地址（只供运维，例如 127.0.0.1:9464；无鉴权，不得暴露给用户）；为空时不提供指标")
	logFile := fs.String("log-file", "", "结构化日志另外追加写入的文件（0600；例如供 Loki 的采集器读取）；为空时只写 stderr")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := tel.Validate(); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	if *dataDir == "" || *dsn == "" {
		fmt.Fprintln(stderr, "agentbox server: 需要 --data-dir 与 --database-url（或环境变量 "+databaseURLEnv+"）")
		return 2
	}
	if *cacheMode != "on" && *cacheMode != "off" {
		fmt.Fprintf(stderr, "agentbox server: --cache 须为 on 或 off，得到 %q\n", *cacheMode)
		return 2
	}
	if err := checkSearchProvider(*searchProvider, *allowPrivate, os.Getenv(searchKeyEnv) != ""); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	gatewayLim, err := gatewayLimits(*callDeadline, *modelCallDeadline)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	model, err := modelConfig(modelFlags{BaseURL: *modelBaseURL, Name: *modelName, Models: *models,
		Prices: modelPrices, PriceIn: *priceIn, PriceOut: *priceOut, MaxTokensCap: *maxTokensCap})
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	// 供应商 Key 只从宿主环境变量读取（不设标志，避免出现在进程参数与 shell 历史中），只交给 Gateway 的
	// upstream adapter；不写日志、不进入 init 与沙箱环境（§9.9）。后备供应商的 Key 同样只来自环境变量（文件只给变量名）。
	model.APIKey = os.Getenv(modelKeyEnv)
	if model.Fallbacks, err = modelFallbacks(*fallbackFile, os.Getenv); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	if model.Routing, err = modelRouting(*breakerFailures, *breakerOpen, *tryTimeout, *hedgeDelay); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	// 配置了模型上游时启用用户账号：用户研究的两个模型须在声明的白名单中（在取得锁、连接数据库之前拒绝）。
	accounts := model.BaseURL != ""
	if accounts {
		for _, m := range []struct{ flag, name string }{{"--user-orchestrator-model", *userOrchestrator}, {"--user-worker-model", *userWorker}} {
			if m.name != model.Name && !slices.Contains(model.Models, m.name) {
				fmt.Fprintf(stderr, "agentbox server: %s %q 不在声明的模型白名单中（--model-name / --models）\n", m.flag, m.name)
				return 2
			}
		}
	}
	sess := app.Config{Accounts: accounts}
	if err := sessionFlags(&sess, *turnToolBudget, *sessionIdleFreeze, *sessionEvictAfter, *sessionWorkerArgv); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	if err := subrunFlags(&sess, *workerSubruns, *subrunCancelTimeout); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	execCfg, err := execFlags(ex)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 2
	}
	if w := plaintextListenWarning(*listen, *tlsCert != ""); w != "" {
		fmt.Fprintln(stderr, w)
	}
	// 宿主自检的警告（例如 aarch64 未经单独验证）只记录，不阻止启动（Plan 15 Task 4）。
	for _, w := range hostcheck.Check().Warnings {
		fmt.Fprintln(stderr, "warning: hostcheck:", w)
	}
	// 在取得任何锁、连接数据库之前确认能够安全执行任务。
	starter, execDigest, err := prepareIsolation(execCfg.Enabled())
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server: 拒绝启动:", err)
		return 1
	}
	dir, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 1
	}
	cfg := app.Config{
		Listen:                *listen,
		Capacity:              admission.Capacity{RunSlots: *runSlots, MemoryBytes: *memory},
		DefaultMemoryBytes:    *defaultMemory,
		DefaultRunTime:        *defaultRunTime,
		RunTimeCap:            *runTimeCap,
		AllowedHosts:          splitList(*allowedHost),
		AllowedOrigins:        splitList(*allowedOrigin),
		WebDir:                *webDir,
		TLSCertFile:           *tlsCert,
		TLSKeyFile:            *tlsKey,
		WorkerArgv:            splitList(*workerArgv),
		WorkerEnv:             splitList(*workerEnv),
		DefaultBudgetMicro:    *defaultBudget,
		BudgetCapMicro:        *budgetCap,
		Model:                 model,
		SearchProvider:        *searchProvider,
		SearchBaseURL:         *searchBaseURL,
		SearchAPIKey:          os.Getenv(searchKeyEnv),
		UpstreamAllowPrivate:  splitList(*allowPrivate),
		RedisAddr:             strings.TrimSpace(*redisAddr),
		CacheOff:              *cacheMode == "off",
		Gateway:               gatewayLim,
		Accounts:              accounts,
		UserOrchestratorModel: *userOrchestrator,
		UserWorkerModel:       *userWorker,
		TurnToolBudget:        sess.TurnToolBudget,
		SessionIdleFreeze:     sess.SessionIdleFreeze,
		SessionEvictAfter:     sess.SessionEvictAfter,
		SessionWorkerArgv:     sess.SessionWorkerArgv,
		WorkerSubruns:         sess.WorkerSubruns,
		Runner:                sess.Runner,
		Exec:                  execCfg,
	}
	if _, err := os.Stat(filepath.Join(dir, api.TokenFile)); err == nil {
		if cfg.APIToken, err = api.LoadToken(dir); err != nil {
			fmt.Fprintln(stderr, "agentbox server:", err)
			return 1
		}
	}
	deps := app.Deps{
		DataDir:         dir,
		ExecImageDigest: func() (string, error) { return execDigest, nil }, // prepareIsolation 已检查并计算
		AcquireOwnership: func(ctx context.Context) (app.Ownership, error) {
			o, err := postgres.AcquireOwnership(ctx, *dsn, postgres.OwnershipOptions{})
			if err != nil {
				return nil, err
			}
			return o, nil
		},
		OpenStore: func(ctx context.Context, own app.Ownership) (app.Store, error) {
			o, ok := own.(*postgres.Ownership)
			if !ok {
				return nil, errors.New("所有权不是 PostgreSQL advisory lock")
			}
			s, err := postgres.Open(ctx, postgres.Options{DSN: *dsn, Ownership: o})
			if err != nil {
				return nil, err
			}
			return s, nil
		},
		NewProvider: func(installID string) (provider.Provider, error) {
			p, err := local.New(local.Options{DataDir: dir, CgroupRoot: *cgroupRoot, InstallID: installID, Starter: starter})
			if err != nil {
				return nil, err
			}
			return p, nil
		},
	}
	logger, closeLog, err := serverLogger(*logFile, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 1
	}
	defer closeLog()
	deps.Logger = logger
	if w := telemetry.MetricsWarning(tel.MetricsListen); w != "" {
		fmt.Fprintln(stderr, w)
	}
	tel.Logger, tel.ServiceVersion = logger, buildVersion()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	t, err := telemetry.Setup(ctx, tel)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 1
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := t.Shutdown(sctx); err != nil {
			fmt.Fprintln(stderr, "agentbox server: telemetry shutdown:", err)
		}
	}()
	if a := t.MetricsAddr(); a != "" {
		logger.Info("metrics listening", "addr", a)
	}
	if err := app.Run(ctx, cfg, deps); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 1
	}
	return 0
}
