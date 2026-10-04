//go:build linux

package main

// 本文件是 `agentbox verify-invariants [--quiescent]`（规格 §16.3）：以 PostgreSQL Store、provider/local
// 的独立原始扫描与本地 BlobStore 运行 invariants.Verify，打印违反项；有违反时退出码为 1。
//
// 只读检查：不取数据目录锁与 advisory lock（服务运行中也可检查 A/B 类），不写数据库。

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/blob"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/invariants"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/local"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"
)

// scanOnlyStarter 是只供扫描的 provider/local 的启动器：local.New 要求非空的 EnvStarter，而 Scan 不使用它。
// 它拒绝启动任何环境，因此 verify-invariants 不可能创建或运行执行环境。
type scanOnlyStarter struct{}

func (scanOnlyStarter) StartInit(context.Context, provider.EnvSpec, string, *cgroup.Group) (*sandbox.Conn, int, error) {
	return nil, 0, errors.New("verify-invariants 只扫描，不启动环境")
}

func runVerify(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("verify-invariants", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", "", "数据目录（必填）")
	dsn := fs.String("database-url", os.Getenv(databaseURLEnv), "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	cgroupRoot := fs.String("cgroup-root", defaultCgroupRoot, "cgroup v2 挂载点")
	quiescent := fs.Bool("quiescent", false, "另检查 Q 类（实验结束、清理完成后成立的不变量）")
	timeout := fs.Duration("timeout", 2*time.Minute, "整体期限")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" || *dsn == "" {
		fmt.Fprintln(stderr, "agentbox verify-invariants: 需要 --data-dir 与 --database-url（或环境变量 "+databaseURLEnv+"）")
		return 2
	}
	vs, err := verify(*dataDir, *dsn, *cgroupRoot, *quiescent, *timeout)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox verify-invariants:", err)
		return 2
	}
	for _, v := range vs {
		fmt.Fprintf(stdout, "%s [%s] %s\n", v.ID, v.Class, v.Detail)
	}
	if len(vs) > 0 {
		fmt.Fprintf(stderr, "agentbox verify-invariants: %d 项违反\n", len(vs))
		return 1
	}
	fmt.Fprintln(stdout, "不变量检查通过")
	return 0
}

func verify(dataDir, dsn, cgroupRoot string, quiescent bool, timeout time.Duration) ([]invariants.Violation, error) {
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	installID, ok, err := datadir.NewIDFile(dir).Read()
	switch {
	case err != nil:
		return nil, err
	case !ok:
		return nil, fmt.Errorf("数据目录 %s 没有 install_id：不是已引导的安装", dir)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	store, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		return nil, err
	}
	defer store.Close()
	scanner, err := local.New(local.Options{DataDir: dir, CgroupRoot: cgroupRoot, InstallID: installID, Starter: scanOnlyStarter{}})
	if err != nil {
		return nil, err
	}
	blobs, err := blob.NewLocal(filepath.Join(dir, "blobs"))
	if err != nil {
		return nil, err
	}
	return invariants.Verify(ctx, store, scanner, blobs, quiescent)
}
