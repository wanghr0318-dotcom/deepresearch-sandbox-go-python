//go:build linux

// agentbox-e2e 是只供系统级测试（tests/e2e）使用的 server：与 cmd/agentbox server 调用同一个 app.Run，
// 但以进程型测试 provider（tests/e2e/procprov，不隔离）代替 provider/local，并开启故障注入
// （internal/faultinject：AGENTBOX_FAULT=<点>:<次数>）。测试把它作为真实子进程运行，SIGKILL 与重启都是真实的。
//
//	agentbox-e2e server --data-dir D --database-url U [--lock-database-url L] --addr-file F --pythonpath P ...
//	agentbox-e2e verify-invariants --data-dir D --database-url U [--quiescent]
//
// --lock-database-url 使 advisory lock 的专用连接可以绕过测试控制的 TCP 代理（E14：业务连接中断而所有权
// 保持）；缺省与 --database-url 相同。生产二进制不得导入本包或 procprov（archtest）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/admission"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/app"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/faultinject"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/invariants"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/tests/e2e/procprov"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: agentbox-e2e server ... | verify-invariants ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		os.Exit(runServer(os.Args[2:], os.Stderr))
	case "verify-invariants":
		os.Exit(runVerify(os.Args[2:], os.Stdout, os.Stderr))
	}
	fmt.Fprintln(os.Stderr, "未知命令", os.Args[1])
	os.Exit(2)
}

func runServer(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", "", "数据目录（必填）")
	dsn := fs.String("database-url", "", "业务连接的 PostgreSQL 连接串（必填）")
	lockDSN := fs.String("lock-database-url", "", "advisory lock 专用连接的连接串（缺省同 --database-url）")
	listen := fs.String("listen", "127.0.0.1:0", "API 监听地址")
	addrFile := fs.String("addr-file", "", "开始监听后把实际地址写入该文件")
	pythonPath := fs.String("pythonpath", "", "Worker 的 PYTHONPATH（sim_worker 所在目录）")
	runSlots := fs.Int("run-slots", 4, "run slots")
	checkInterval := fs.Duration("check-interval", 200*time.Millisecond, "失锁检测周期")
	retryBackoff := fs.Duration("retry-backoff", 200*time.Millisecond, "恢复、报警与 coordinator 持久化的重试退避（固定值）")
	shutdown := fs.Duration("shutdown-timeout", 20*time.Second, "退出时有期限清理的上限")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" || *dsn == "" {
		fmt.Fprintln(stderr, "agentbox-e2e server: 需要 --data-dir 与 --database-url")
		return 2
	}
	if *lockDSN == "" {
		*lockDSN = *dsn
	}
	// 只有本测试 main 开启故障注入；cmd/agentbox 从不调用 Enable。
	if err := faultinject.Enable(); err != nil {
		fmt.Fprintln(stderr, "agentbox-e2e server:", err)
		return 2
	}
	dir, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox-e2e server:", err)
		return 1
	}
	backoff := *retryBackoff
	cfg := app.Config{
		Listen:          *listen,
		Capacity:        admission.Capacity{RunSlots: *runSlots, MemoryBytes: 8 << 30},
		DefaultRunTime:  1000 * time.Hour,
		RunTimeCap:      1000 * time.Hour,
		ShutdownTimeout: *shutdown,
		RetryBackoff:    func(int) time.Duration { return backoff },
	}
	if *pythonPath != "" {
		cfg.WorkerEnv = []string{"PYTHONPATH=" + *pythonPath}
	}
	deps := app.Deps{
		DataDir: dir,
		AcquireOwnership: func(ctx context.Context) (app.Ownership, error) {
			o, err := postgres.AcquireOwnership(ctx, *lockDSN, postgres.OwnershipOptions{CheckInterval: *checkInterval, CheckTimeout: 2 * time.Second})
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
			p, err := procprov.New(procprov.Options{DataDir: dir, InstallID: installID})
			if err != nil {
				return nil, err
			}
			return p, nil
		},
		Logger: slog.New(slog.NewJSONHandler(stderr, nil)),
		Hooks: app.Hooks{Listening: func(addr string) {
			if *addrFile == "" {
				return
			}
			tmp := *addrFile + ".tmp"
			if err := os.WriteFile(tmp, []byte(addr), 0o600); err == nil {
				_ = os.Rename(tmp, *addrFile)
			}
		}},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg, deps); err != nil {
		fmt.Fprintln(stderr, "agentbox-e2e server:", err)
		return 1
	}
	return 0
}

// runVerify 与 `agentbox verify-invariants` 相同，只是独立原始扫描由 procprov 提供。退出码：0 通过，
// 1 有违反，2 无法检查。
func runVerify(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("verify-invariants", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", "", "数据目录（必填）")
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（必填）")
	quiescent := fs.Bool("quiescent", false, "另检查 Q 类")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" || *dsn == "" {
		fmt.Fprintln(stderr, "agentbox-e2e verify-invariants: 需要 --data-dir 与 --database-url")
		return 2
	}
	dir, err := filepath.Abs(*dataDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	installID, ok, err := datadir.NewIDFile(dir).Read()
	if err != nil || !ok {
		fmt.Fprintf(stderr, "agentbox-e2e verify-invariants: 数据目录 %s 没有 install_id（%v）\n", dir, err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := postgres.Open(ctx, postgres.Options{DSN: *dsn})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	defer store.Close()
	scanner, err := procprov.New(procprov.Options{DataDir: dir, InstallID: installID, ReadOnly: true})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	blobs, err := blob.NewLocal(filepath.Join(dir, "blobs"))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	vs, err := invariants.Verify(ctx, store, scanner, blobs, *quiescent)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox-e2e verify-invariants:", err)
		return 2
	}
	for _, v := range vs {
		fmt.Fprintf(stdout, "%s [%s] %s\n", v.ID, v.Class, v.Detail)
	}
	if len(vs) > 0 {
		return 1
	}
	fmt.Fprintln(stdout, "不变量检查通过")
	return 0
}
