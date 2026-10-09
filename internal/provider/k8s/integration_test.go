package k8s

// Integration tests against a real cluster (opt-in):
//
//	AGENTBOX_K8S_TEST_KUBECONFIG=<kubeconfig> AGENTBOX_K8S_TEST_IMAGE=<repo@sha256:…> \
//	  go test -run 'Real' -v ./internal/provider/k8s/
//
// AGENTBOX_K8S_TEST_SAMPLES sets the number of cold and warm starts measured (default 20);
// AGENTBOX_K8S_TEST_REPORT writes the latency summary as JSON to that path.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/providertest"
)

const itNS = "agentbox-it"

func realCluster(t *testing.T) (kubernetes.Interface, Executor, string) {
	t.Helper()
	kc, img := os.Getenv("AGENTBOX_K8S_TEST_KUBECONFIG"), os.Getenv("AGENTBOX_K8S_TEST_IMAGE")
	if kc == "" || img == "" {
		t.Skip("set AGENTBOX_K8S_TEST_KUBECONFIG and AGENTBOX_K8S_TEST_IMAGE to run against a real cluster")
	}
	cs, ex, err := Connect(kc)
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: itNS}}
	if _, err := cs.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	return cs, ex, img
}

func randomInstall() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "it-" + hex.EncodeToString(b)
}

// newRealProvider returns a provider with a fresh install id; its Pods are deleted at cleanup.
func newRealProvider(t *testing.T, install string, mod func(*Options)) *Provider {
	t.Helper()
	cs, ex, img := realCluster(t)
	opt := Options{Client: cs, Executor: ex, Namespace: itNS, InstallID: install, Image: img, EnsureNetworkPolicy: true,
		Logger: quietLogger()}
	if mod != nil {
		mod(&opt)
	}
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.Close()
		ctx := context.Background()
		sel := labels.Set{LabelManaged: "true", LabelInstall: labelValue(install)}.AsSelector().String()
		_ = cs.CoreV1().Pods(itNS).DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))},
			metav1.ListOptions{LabelSelector: sel})
	})
	return p
}

func TestConformanceOnRealCluster(t *testing.T) {
	realCluster(t)
	var install string
	h := harness(func(t *testing.T) *Provider {
		install = randomInstall()
		return newRealProvider(t, install, nil)
	}, func() string { return install }, []string{"sh", "-c", "echo hello; exit 3"}, []string{"sleep", "100000"})
	providertest.Run(t, h)
}

// startOnce measures Create → workload acknowledged → first output line of a Python process.
func startOnce(t *testing.T, p *Provider, envID string) (ready, firstOutput time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	t0 := time.Now()
	if _, err := p.Create(ctx, specFor(p.opt.InstallID)(envID)); err != nil {
		t.Fatal(err)
	}
	ready = time.Since(t0)
	h, err := p.StartExec(ctx, envID, provider.ExecSpec{ExecID: "lat", Argv: []string{"python3", "-c", "print('ok')"},
		Env: []string{"PYTHONPATH=/opt/agentbox"}})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
	buf := make([]byte, 3)
	if _, err := io.ReadFull(h.Stdout(), buf); err != nil || string(buf) != "ok\n" {
		t.Fatalf("first output %q %v", buf, err)
	}
	firstOutput = time.Since(t0)
	if st, err := h.Wait(); err != nil || st.Code != 0 {
		t.Fatalf("exit %+v %v", st, err)
	}
	if err := p.Stop(ctx, envID); err != nil {
		t.Fatal(err)
	}
	if err := p.Destroy(ctx, envID); err != nil {
		t.Fatal(err)
	}
	return ready, firstOutput
}

func TestStartLatencyOnRealCluster(t *testing.T) {
	realCluster(t)
	n := 20
	if v, err := strconv.Atoi(os.Getenv("AGENTBOX_K8S_TEST_SAMPLES")); err == nil && v > 0 {
		n = v
	}
	measure := func(p *Provider, wait func()) (r, f []time.Duration) {
		for i := 0; i < n; i++ {
			wait()
			a, b := startOnce(t, p, "lat-"+strconv.Itoa(i))
			r, f = append(r, a), append(f, b)
		}
		return r, f
	}
	cold := newRealProvider(t, randomInstall(), nil)
	coldReady, coldFirst := measure(cold, func() {})
	poolSize := 2
	warm := newRealProvider(t, randomInstall(), func(o *Options) { o.WarmPool = poolSize; o.WarmProfile = testLimits() })
	waitPool := func() {
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			pods, _ := warm.pool.warmPods(context.Background())
			n := 0
			for i := range pods {
				if ready(&pods[i]) {
					n++
				}
			}
			if n >= 1 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("warm pool did not fill")
	}
	warmReady, warmFirst := measure(warm, waitPool)
	if s := warm.Stats(); s["cold"].Count != 0 {
		t.Fatalf("warm run fell back to cold starts: %+v", s)
	}
	report := map[string]LatencySummary{
		"cold_ready": Summarize(coldReady), "cold_first_output": Summarize(coldFirst),
		"warm_ready": Summarize(warmReady), "warm_first_output": Summarize(warmFirst),
	}
	for _, k := range []string{"cold_ready", "warm_ready", "cold_first_output", "warm_first_output"} {
		s := report[k]
		t.Logf("%-18s n=%d P50=%v P95=%v", k, s.Count, s.P50.Round(time.Millisecond), s.P95.Round(time.Millisecond))
	}
	if path := os.Getenv("AGENTBOX_K8S_TEST_REPORT"); path != "" {
		b, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestBurstOnRealCluster: AGENTBOX_K8S_TEST_BURST (default 6) concurrent Creates against a warm pool of 2 —
// the pool is exhausted and the rest start cold, so this is the "pool too small" case the warm numbers above
// exclude.
func TestBurstOnRealCluster(t *testing.T) {
	realCluster(t)
	n := 6
	if v, err := strconv.Atoi(os.Getenv("AGENTBOX_K8S_TEST_BURST")); err == nil && v > 0 {
		n = v
	}
	p := newRealProvider(t, randomInstall(), func(o *Options) { o.WarmPool = 2; o.WarmProfile = testLimits() })
	deadline := time.Now().Add(3 * time.Minute)
	for {
		pods, _ := p.pool.warmPods(context.Background())
		r := 0
		for i := range pods {
			if ready(&pods[i]) {
				r++
			}
		}
		if r >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("warm pool did not fill")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var wg sync.WaitGroup
	lat := make([]time.Duration, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			t0 := time.Now()
			_, errs[i] = p.Create(ctx, specFor(p.opt.InstallID)("burst-"+strconv.Itoa(i)))
			lat[i] = time.Since(t0)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	s := p.Stats()
	all := Summarize(lat)
	t.Logf("burst n=%d pool=2: warm=%d cold=%d; all P50=%v P95=%v; warm P50=%v; cold P50=%v P95=%v", n, s["warm"].Count,
		s["cold"].Count, all.P50.Round(time.Millisecond), all.P95.Round(time.Millisecond), s["warm"].P50.Round(time.Millisecond),
		s["cold"].P50.Round(time.Millisecond), s["cold"].P95.Round(time.Millisecond))
	if s["warm"].Count > 2 || s["warm"].Count+s["cold"].Count != n {
		t.Fatalf("unexpected paths %+v", s)
	}
}
