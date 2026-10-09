//go:build linux

package main

// 本文件是 `agentbox server --provider k8s` 的装配（设计 docs/design/2026-10-10-k8s-provider-design.md）：
// 解析 --k8s-* 参数、启动时把镜像固定到 digest、构造 provider/k8s。默认 --provider local，行为不变。

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s"
)

type k8sFlags struct {
	kubeconfig, namespace, image, runtimeClass, sharedDir, nodeSharedDir, plainHTTP string
	strict                                                                          bool
	warmPool                                                                        int
}

func (f *k8sFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.kubeconfig, "k8s-kubeconfig", "", "--provider k8s：kubeconfig 路径（空时使用 in-cluster 配置）")
	fs.StringVar(&f.namespace, "k8s-namespace", "agentbox-sandbox", "--provider k8s：沙箱 Pod 所在的命名空间（须已存在）")
	fs.StringVar(&f.image, "k8s-image", "", "--provider k8s：worker 镜像（repo@sha256:… 或 repo:tag；tag 在启动时解析为 digest 并固定）")
	fs.BoolVar(&f.strict, "k8s-image-strict", false, "--provider k8s：拒绝可变的 tag，--k8s-image 须为 repo@sha256:…")
	fs.StringVar(&f.plainHTTP, "k8s-registry-plain-http", "", "--provider k8s：以 HTTP（而非 HTTPS）解析 tag 的镜像仓库 host:port（逗号分隔，例如本地 kind-registry:5000）")
	fs.StringVar(&f.runtimeClass, "k8s-runtime-class", "", "--provider k8s：Pod 的 runtimeClassName（例如 gVisor 的 runsc 对应的 RuntimeClass）；空为集群默认")
	fs.StringVar(&f.sharedDir, "k8s-shared-dir", "", "--provider k8s：与节点共享的槽位目录（workspace 与 Gateway socket 经它进入 Pod；须与 --data-dir 同一文件系统）；默认 <data-dir>/k8s-slots")
	fs.StringVar(&f.nodeSharedDir, "k8s-node-shared-dir", "", "--provider k8s：同一目录在节点上的路径（hostPath）；默认与 --k8s-shared-dir 相同")
	fs.IntVar(&f.warmPool, "k8s-warm-pool", 0, "--provider k8s：保持的预热 Pod 数（按默认任务资源规格；0 关闭）")
}

// k8sWarmProfile 是预热 Pod 的资源规格：与未设置 limits 的任务环境相同（app 的默认值：内存取
// --default-memory-bytes，cpu.max quota 100000，/tmp 64 MiB），因此默认任务都能取得预热 Pod。
func k8sWarmProfile(defaultMemory int64) provider.Limits {
	return provider.Limits{MemoryMax: defaultMemory, CPUQuotaUs: 100000, TmpBytes: 64 << 20}
}

// prepareK8s 在取得锁、连接数据库之前固定镜像 digest 并连接集群；返回按 install_id 构造 provider 的函数。
func prepareK8s(f k8sFlags, dataDir string, defaultMemory int64, stderr io.Writer) (func(string) (provider.Provider, error), error) {
	if f.image == "" {
		return nil, errors.New("--provider k8s 需要 --k8s-image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ref, err := k8s.PinImage(ctx, f.image, f.strict, splitList(f.plainHTTP), nil)
	if err != nil {
		return nil, err
	}
	client, ex, err := k8s.Connect(f.kubeconfig)
	if err != nil {
		return nil, err
	}
	shared := f.sharedDir
	if shared == "" {
		shared = filepath.Join(dataDir, "k8s-slots")
	}
	fmt.Fprintf(stderr, "agentbox server: provider k8s, namespace %s, image %s\n", f.namespace, ref.Pinned())
	return func(installID string) (provider.Provider, error) {
		p, err := k8s.New(context.Background(), k8s.Options{Client: client, Executor: ex, Namespace: f.namespace,
			InstallID: installID, Image: ref.Pinned(), RuntimeClass: f.runtimeClass, SlotHostDir: shared,
			SlotNodeDir: f.nodeSharedDir, WarmPool: f.warmPool, WarmProfile: k8sWarmProfile(defaultMemory),
			EnsureNetworkPolicy: true})
		if err != nil {
			return nil, err
		}
		return p, nil
	}, nil
}
