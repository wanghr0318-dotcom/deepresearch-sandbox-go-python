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
	"strings"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/app"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/local"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
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
// 默认 rootfs 模板的宿主路径齐全（缺失时报告路径），并构造生产启动器（本进程成为 child subreaper）。
// 任何一项失败都拒绝启动，不以未隔离的方式运行任务。
func prepareIsolation() (local.EnvStarter, error) {
	tpl, err := rootfs.ResolveTemplate(rootfs.DefaultTemplateName)
	if err != nil {
		return nil, err
	}
	if err := tpl.Ensure(); err != nil {
		return nil, fmt.Errorf("rootfs 模板 %q 不可用（worker 包须安装在 %s）: %w", rootfs.DefaultTemplateName, rootfs.WorkerDir, err)
	}
	starter, err := local.NewProcessStarter()
	if err != nil {
		return nil, err
	}
	return starter, nil
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
	workerArgv := fs.String("worker-argv", "python3,-m,sim_worker", "沙箱内启动 Worker 的命令（逗号分隔的 argv）")
	workerEnv := fs.String("worker-env", defaultWorkerEnv, "沙箱内 Worker 的环境变量（逗号分隔的 KEY=VALUE；键须在白名单中，如 PYTHONPATH）")
	defaultBudget := fs.Int64("default-budget-micro", 2_000_000, "task 层预算的默认值（微美元）：创建时未指定 limits.budget_micro 的任务以此存储")
	budgetCap := fs.Int64("budget-cap-micro", 50_000_000, "显式 limits.budget_micro 的服务端上限（微美元）")
	modelBaseURL := fs.String("model-base-url", "", "OpenAI 兼容模型上游地址（例如 https://api.openai.com/v1）；为空时不提供 /v1/chat/completions。Key 只读环境变量 "+modelKeyEnv)
	modelName := fs.String("model-name", "", "唯一声明的模型名（配置了 --model-base-url 时必填）")
	priceIn := fs.Int64("model-price-in-micro-per-mtok", 3_000_000, "模型输入单价（每百万 token 的微美元），用于预留估算与结算")
	priceOut := fs.Int64("model-price-out-micro-per-mtok", 15_000_000, "模型输出单价（每百万 token 的微美元），用于预留估算与结算")
	searchProvider := fs.String("search-provider", upstream.SearchDDGLite, "搜索供应商：fake | tavily | ddg_lite（tavily 的 Key 只读环境变量 "+searchKeyEnv+"；fake 只用于测试，须同时设置 --upstream-allow-private）")
	allowPrivate := fs.String("upstream-allow-private", "", "显式放行的私有上游主机（逗号分隔的 host 或 host:port；例如本机模型服务或测试用 fake upstream）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" || *dsn == "" {
		fmt.Fprintln(stderr, "agentbox server: 需要 --data-dir 与 --database-url（或环境变量 "+databaseURLEnv+"）")
		return 2
	}
	switch *searchProvider {
	case upstream.SearchTavily, upstream.SearchDDGLite:
	case upstream.SearchFake:
		if strings.TrimSpace(*allowPrivate) == "" {
			fmt.Fprintln(stderr, "agentbox server: --search-provider fake 只用于测试，须同时设置 --upstream-allow-private 指向本机 fake upstream")
			return 2
		}
	default:
		fmt.Fprintf(stderr, "agentbox server: --search-provider 须为 fake、tavily 或 ddg_lite，得到 %q\n", *searchProvider)
		return 2
	}
	// 在取得任何锁、连接数据库之前确认能够安全执行任务。
	starter, err := prepareIsolation()
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
		Listen:             *listen,
		Capacity:           admission.Capacity{RunSlots: *runSlots, MemoryBytes: *memory},
		DefaultMemoryBytes: *defaultMemory,
		DefaultRunTime:     *defaultRunTime,
		RunTimeCap:         *runTimeCap,
		AllowedHosts:       splitList(*allowedHost),
		WorkerArgv:         splitList(*workerArgv),
		WorkerEnv:          splitList(*workerEnv),
		DefaultBudgetMicro: *defaultBudget,
		BudgetCapMicro:     *budgetCap,
		// 供应商 Key 只从宿主环境变量读取（不设标志，避免出现在进程参数与 shell 历史中），只交给 Gateway 的
		// upstream adapter；不写日志、不进入 init 与沙箱环境（§9.9）。
		Model: app.ModelConfig{BaseURL: *modelBaseURL, Name: *modelName, APIKey: os.Getenv(modelKeyEnv),
			Pricing: upstream.Pricing{InputMicroPerMTok: *priceIn, OutputMicroPerMTok: *priceOut}},
		SearchProvider:       *searchProvider,
		SearchAPIKey:         os.Getenv(searchKeyEnv),
		UpstreamAllowPrivate: splitList(*allowPrivate),
	}
	if _, err := os.Stat(filepath.Join(dir, api.TokenFile)); err == nil {
		if cfg.APIToken, err = api.LoadToken(dir); err != nil {
			fmt.Fprintln(stderr, "agentbox server:", err)
			return 1
		}
	}
	deps := app.Deps{
		DataDir: dir,
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg, deps); err != nil {
		fmt.Fprintln(stderr, "agentbox server:", err)
		return 1
	}
	return 0
}

// splitList 拆分逗号分隔的列表，忽略空项。
func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
