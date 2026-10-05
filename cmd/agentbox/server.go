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
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/local"
)

const (
	databaseURLEnv    = "AGENTBOX_DATABASE_URL"
	defaultCgroupRoot = "/sys/fs/cgroup"
)

// errNoProductionStarter：生产环境启动器（namespace、UID 映射、挂载、pivot_root、降权；规格 §4.6）
// 尚未提供。§4.6 的启动序列须经 Plan 1B spike 的结论回写并审阅后才实现；在此之前 server 拒绝启动，
// 不以未隔离的方式运行任务。
var errNoProductionStarter = errors.New("没有可用于生产的环境启动器：规格 §4.6 的降权启动序列待 Plan 1B spike 结论审阅后实现；" +
	"在此之前 server 拒绝启动，不以未隔离的方式运行任务")

// productionStarter 返回生产 EnvStarter；Plan 1B 结论审阅前不存在。
func productionStarter() (local.EnvStarter, error) { return nil, errNoProductionStarter }

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
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" || *dsn == "" {
		fmt.Fprintln(stderr, "agentbox server: 需要 --data-dir 与 --database-url（或环境变量 "+databaseURLEnv+"）")
		return 2
	}
	// 在取得任何锁、连接数据库之前确认能够安全执行任务。
	starter, err := productionStarter()
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
