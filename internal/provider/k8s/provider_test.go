package k8s

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/providertest"
)

const testNS = "agentbox-test"

var testImage = "kind-registry:5000/agentbox-worker@" + testDigest

func testLimits() provider.Limits {
	return provider.Limits{MemoryMax: 256 << 20, PidsMax: 64, CPUQuotaUs: 50000, NoFile: 1024, TmpBytes: 16 << 20}
}

func specFor(install string) func(string) provider.EnvSpec {
	return func(envID string) provider.EnvSpec {
		return provider.EnvSpec{EnvID: envID, InstallID: install, Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096,
			Template: "default", Limits: testLimits()}
	}
}

type fakeCluster struct {
	client  *fake.Clientset
	exec    *fakeExec
	kubelet *fakeKubelet
}

func newFakeCluster(t *testing.T, startDelay time.Duration) *fakeCluster {
	c := fake.NewClientset()
	fe := newFakeExec(c, testNS)
	return &fakeCluster{client: c, exec: fe, kubelet: startFakeKubelet(t, c, testNS, fe, startDelay)}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestProvider(t *testing.T, client kubernetes.Interface, ex Executor, mod func(*Options)) *Provider {
	t.Helper()
	opt := Options{Client: client, Executor: ex, Namespace: testNS, InstallID: "inst-a", Image: testImage,
		StopTimeout: 10 * time.Second, CreateTimeout: 20 * time.Second, Logger: quietLogger()}
	if mod != nil {
		mod(&opt)
	}
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// harness builds the conformance harness around a provider constructor; echo/sleep are programs that the
// target (fake helper or real image) understands.
func harness(newProv func(t *testing.T) *Provider, install string, echo, sleep []string) providertest.Harness {
	return providertest.Harness{
		New:        func(t *testing.T) provider.Provider { return newProv(t) },
		Spec:       specFor(install),
		Echo:       provider.ExecSpec{ExecID: "echo-1", Argv: echo},
		EchoOutput: "hello",
		EchoCode:   3,
		Sleep:      provider.ExecSpec{ExecID: "sleep-1", Argv: sleep},
		Residue: func(t *testing.T, pp provider.Provider, envID string) {
			p := pp.(*Provider)
			s := specFor(install)(envID)
			o := owner{InstallID: install, EnvID: envID, SpecHash: provider.SpecHash(s), Kind: s.Kind}
			pod, err := p.createPod(context.Background(), profileOf(s.Limits), &o, "")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := p.waitPod(ctx, pod.Name, func(po *corev1.Pod) (bool, error) { return ready(po), nil }); err != nil {
				t.Fatal(err)
			}
		},
		Foreign: func(t *testing.T, pp provider.Provider, envID string) {
			p := pp.(*Provider)
			pod := buildPod(podParams{Name: newPodName(), Namespace: p.opt.Namespace, InstallID: "someone-else", Image: p.opt.Image,
				PoolKey: "x", Profile: profileOf(testLimits())})
			pod.Labels[LabelEnv] = labelValue(envID)
			pod.Labels[LabelState] = StateAssigned
			if _, err := p.client.CoreV1().Pods(p.opt.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = p.client.CoreV1().Pods(p.opt.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))})
			})
		},
		BlockStart: func(t *testing.T, pp provider.Provider, envID string) (<-chan struct{}, func()) {
			p := pp.(*Provider)
			blocked, release := make(chan struct{}), make(chan struct{})
			var once bool
			p.beforeSend = func(id string) {
				if id != envID || once {
					return
				}
				once = true
				close(blocked)
				<-release
			}
			return blocked, func() { close(release) }
		},
	}
}

func TestConformanceOnFakeCluster(t *testing.T) {
	providertest.Run(t, harness(func(t *testing.T) *Provider {
		c := newFakeCluster(t, 0)
		return newTestProvider(t, c.client, c.exec, func(o *Options) { o.InstallID = "inst-a" })
	}, "inst-a", []string{"echo", "hello", "3"}, []string{"sleep"}))
}

func TestNewRequiresPinnedImage(t *testing.T) {
	c := fake.NewClientset()
	_, err := New(context.Background(), Options{Client: c, Executor: newFakeExec(c, testNS), InstallID: "i", Image: "reg/x:dev"})
	if err == nil {
		t.Fatal("a tag must be refused by New")
	}
}

func TestPodSpecIsLockedDown(t *testing.T) {
	c := newFakeCluster(t, 0)
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.RuntimeClass = "gvisor"; o.EnsureNetworkPolicy = true })
	if _, err := p.Create(context.Background(), specFor("inst-a")("env-sec")); err != nil {
		t.Fatal(err)
	}
	pod, err := p.boundPod(context.Background(), "env-sec")
	if err != nil {
		t.Fatal(err)
	}
	s, c0 := pod.Spec, pod.Spec.Containers[0]
	checks := map[string]bool{
		"no SA token":         s.AutomountServiceAccountToken != nil && !*s.AutomountServiceAccountToken,
		"no service links":    s.EnableServiceLinks != nil && !*s.EnableServiceLinks,
		"non-root":            s.SecurityContext.RunAsNonRoot != nil && *s.SecurityContext.RunAsNonRoot && *s.SecurityContext.RunAsUser == SandboxUID,
		"seccomp":             s.SecurityContext.SeccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault,
		"read-only root":      *c0.SecurityContext.ReadOnlyRootFilesystem,
		"no escalation":       !*c0.SecurityContext.AllowPrivilegeEscalation,
		"drop ALL":            len(c0.SecurityContext.Capabilities.Drop) == 1 && c0.SecurityContext.Capabilities.Drop[0] == "ALL",
		"image pinned":        c0.Image == testImage && pod.Annotations[AnnoImage] == testImage,
		"runtime class":       s.RuntimeClassName != nil && *s.RuntimeClassName == "gvisor",
		"restart never":       s.RestartPolicy == corev1.RestartPolicyNever,
		"memory limit":        c0.Resources.Limits.Memory().Value() == 256<<20 && c0.Resources.Requests.Memory().Value() == 256<<20,
		"cpu limit":           c0.Resources.Limits.Cpu().MilliValue() == 500,
		"podagent is PID 1":   len(c0.Command) == 2 && c0.Command[1] == "init",
		"no hostPath w/o dir": len(s.Volumes) == 2,
	}
	for name, ok := range checks {
		if !ok {
			t.Errorf("%s: violated", name)
		}
	}
	np, err := c.client.NetworkingV1().NetworkPolicies(testNS).Get(context.Background(), NetworkPolicyName, metav1.GetOptions{})
	if err != nil || len(np.Spec.PolicyTypes) != 2 || len(np.Spec.Egress) != 0 || len(np.Spec.Ingress) != 0 ||
		np.Spec.PodSelector.MatchLabels[LabelManaged] != "true" {
		t.Fatalf("deny-all policy: %+v %v", np, err)
	}
}

func TestExecRejectedAndSlotsRequired(t *testing.T) {
	c := newFakeCluster(t, 0)
	p := newTestProvider(t, c.client, c.exec, nil)
	ex := specFor("inst-a")("env-x")
	ex.Kind, ex.Mounts.OutBytes = provider.KindExec, 1<<20
	if _, err := p.Create(context.Background(), ex); err == nil {
		t.Fatal("exec environments must be rejected")
	}
	s := specFor("inst-a")("env-w")
	s.Mounts.Workspace = t.TempDir()
	if _, err := p.Create(context.Background(), s); err == nil {
		t.Fatal("workspace without slot dir must be rejected")
	}
}

func TestExecSignalStatusAndTerminate(t *testing.T) {
	c := newFakeCluster(t, 0)
	p := newTestProvider(t, c.client, c.exec, nil)
	ctx := context.Background()
	if _, err := p.Create(ctx, specFor("inst-a")("env-t")); err != nil {
		t.Fatal(err)
	}
	h, err := p.StartExec(ctx, "env-t", provider.ExecSpec{ExecID: "s1", Argv: []string{"sleep"}})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, h.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
	if err := h.Terminate(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	st, err := h.Wait()
	if err != nil || st.Signal != 15 || st.Code != 0 {
		t.Fatalf("status %+v %v", st, err)
	}
	// Start error from the helper.
	if _, err := p.StartExec(ctx, "env-t", provider.ExecSpec{ExecID: "bad", Argv: []string{"nope"}}); !errors.Is(err, provider.ErrStartFailed) {
		t.Fatalf("start error: %v", err)
	}
	// stdin reaches the workload; stdout excludes the acknowledgement.
	h, err = p.StartExec(ctx, "env-t", provider.ExecSpec{ExecID: "c1", Argv: []string{"cat"}})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = h.Stdin().Write([]byte("ping\n")); _ = h.Stdin().Close() }()
	out, _ := io.ReadAll(h.Stdout())
	if st, err := h.Wait(); string(out) != "ping\n" || err != nil || st != (provider.ExitStatus{}) {
		t.Fatalf("cat: %q %+v %v", out, st, err)
	}
	if d, err := p.ResourceDiag(ctx, "env-t"); err != nil || d.CPUUsageUsec != 4242 {
		t.Fatalf("diag %+v %v", d, err)
	}
	if err := p.Freeze(ctx, "env-t"); err != nil || !c.exec.frozen[mustPod(t, p, "env-t")] {
		t.Fatalf("freeze: %v", err)
	}
	if err := p.Thaw(ctx, "env-t"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Procs(ctx, "env-t"); err != nil {
		t.Fatal(err)
	}
	if err := p.Freeze(ctx, "missing"); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("freeze missing: %v", err)
	}
}

func mustPod(t *testing.T, p *Provider, envID string) string {
	t.Helper()
	pod, err := p.boundPod(context.Background(), envID)
	if err != nil {
		t.Fatal(err)
	}
	return pod.Name
}

func TestWarmPoolClaimRefillAndLatency(t *testing.T) {
	c := newFakeCluster(t, 300*time.Millisecond)
	old := poolInterval
	poolInterval = 20 * time.Millisecond
	t.Cleanup(func() { poolInterval = old })
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.WarmPool = 2; o.WarmProfile = testLimits() })
	ctx := context.Background()
	waitWarm := func(n int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			pods, _ := p.pool.warmPods(ctx)
			readyN := 0
			for i := range pods {
				if ready(&pods[i]) {
					readyN++
				}
			}
			if readyN >= n && len(pods) == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("pool did not reach %d ready warm pods", n)
	}
	waitWarm(2)
	// Warm Pods are not environments: List and Scan do not report them.
	if infos, _ := p.List(ctx); len(infos) != 0 {
		t.Fatalf("List reports warm pods: %+v", infos)
	}
	if r, _ := p.Scan(ctx); len(r.Items) != 0 {
		t.Fatalf("Scan reports warm pods: %+v", r.Items)
	}
	if _, err := p.Create(ctx, specFor("inst-a")("env-w1")); err != nil {
		t.Fatal(err)
	}
	other := specFor("inst-a")("env-c1")
	other.Limits.MemoryMax *= 2 // different profile → cold start
	if _, err := p.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	s := p.Stats()
	if s["warm"].Count != 1 || s["cold"].Count != 1 || s["warm"].P50 >= s["cold"].P50 || s["cold"].P50 < 300*time.Millisecond {
		t.Fatalf("stats %+v", s)
	}
	waitWarm(2) // refilled after the claim
	// Stopping and destroying a claimed Pod does not return it to the pool.
	if err := p.Stop(ctx, "env-w1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Destroy(ctx, "env-w1"); err != nil {
		t.Fatal(err)
	}
	waitWarm(2)
}

func TestWarmPoolReplacesForeignKeyPods(t *testing.T) {
	c := newFakeCluster(t, 0)
	old := poolInterval
	poolInterval = 20 * time.Millisecond
	t.Cleanup(func() { poolInterval = old })
	stale := buildPod(podParams{Name: "abx-stale", Namespace: testNS, InstallID: "inst-a", Image: "old@" + testDigest, PoolKey: "oldkey"})
	if _, err := c.client.CoreV1().Pods(testNS).Create(context.Background(), stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.WarmPool = 1; o.WarmProfile = testLimits() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pods, _ := p.pool.warmPods(context.Background())
		if len(pods) == 1 && pods[0].Labels[LabelPool] == p.pool.key {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stale warm pod was not replaced")
}

func TestSlotsBindWorkspaceAndGateway(t *testing.T) {
	root := t.TempDir()
	s := slots{hostRoot: filepath.Join(root, "slots"), nodeRoot: "/node/slots", uid: -1}
	if err := os.MkdirAll(s.hostRoot, 0o711); err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(root, "workspaces", "task-1")
	if err := os.MkdirAll(filepath.Join(ws, "out", "a1"), 0o700); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(ws, "out", "a1", "report.md"), []byte("r1"), 0o600)
	gw := filepath.Join(root, "gw.sock")
	_ = os.WriteFile(gw, nil, 0o600) // a regular file stands in for the socket inode
	if err := s.create("pod1"); err != nil {
		t.Fatal(err)
	}
	if err := s.bind("pod1", provider.Mounts{Workspace: ws, GatewaySocket: gw}); err != nil {
		t.Fatal(err)
	}
	if dst, err := os.Readlink(ws); err != nil || dst != s.workspace("pod1") {
		t.Fatalf("workspace symlink %q %v", dst, err)
	}
	if b, err := os.ReadFile(filepath.Join(ws, "out", "a1", "report.md")); err != nil || string(b) != "r1" {
		t.Fatalf("content through symlink: %q %v", b, err)
	}
	a, _ := os.Stat(gw)
	b, err := os.Stat(filepath.Join(s.hostDir("pod1"), slotRun, gatewayName))
	if err != nil || !os.SameFile(a, b) {
		t.Fatalf("gateway link: %v", err)
	}
	if s.nodeDir("pod1") != "/node/slots/pod1" {
		t.Fatal(s.nodeDir("pod1"))
	}
	// A second environment for the same workspace moves the content on; the first slot disappears.
	if err := s.create("pod2"); err != nil {
		t.Fatal(err)
	}
	if err := s.remove("pod1", ws); err != nil { // destroyed first: its workspace is kept (still referenced)
		t.Fatal(err)
	}
	if _, err := os.Stat(s.workspace("pod1")); err != nil {
		t.Fatalf("referenced workspace slot removed: %v", err)
	}
	if err := s.bind("pod2", provider.Mounts{Workspace: ws}); err != nil {
		t.Fatal(err)
	}
	if dst, _ := os.Readlink(ws); dst != s.workspace("pod2") {
		t.Fatalf("swing: %q", dst)
	}
	if _, err := os.Stat(s.hostDir("pod1")); !os.IsNotExist(err) {
		t.Fatalf("old slot should be gone: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(ws, "out", "a1", "report.md")); err != nil || string(b) != "r1" {
		t.Fatalf("content after swing: %q %v", b, err)
	}
	if err := s.unbindRun("pod2"); err != nil {
		t.Fatal(err)
	}
	if err := s.remove("pod2", ""); err != nil {
		t.Fatal(err)
	}
}

func TestSummarize(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 20; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	s := Summarize(ds)
	if s.Count != 20 || s.P50 != 10*time.Millisecond || s.P95 != 19*time.Millisecond {
		t.Fatalf("%+v", s)
	}
	if Summarize(nil) != (LatencySummary{}) {
		t.Fatal("empty")
	}
}

func TestLabelValue(t *testing.T) {
	if labelValue("env_01HXYZ") != "env_01HXYZ" {
		t.Fatal("valid value changed")
	}
	if v := labelValue("bad/value"); len(v) != 42 || v[:2] != "h-" {
		t.Fatalf("hashed %q", v)
	}
}
