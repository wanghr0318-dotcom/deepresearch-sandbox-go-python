package k8s

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// enforceRV makes the fake clientset behave like the API server for Pod creates and updates: creation
// timestamps are set and an Update must carry the current resourceVersion (the fake tracker does not check
// it). Merge patches (the fake kubelet) do not change the resourceVersion here.
func enforceRV(c *fake.Clientset) {
	var mu sync.Mutex
	rv := map[string]int{}
	gr := schema.GroupResource{Resource: "pods"}
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		pod := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		mu.Lock()
		rv[pod.Name] = 1
		mu.Unlock()
		pod.ResourceVersion, pod.CreationTimestamp = "1", metav1.Now()
		return false, nil, nil
	})
	c.PrependReactor("update", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		pod := a.(k8stesting.UpdateAction).GetObject().(*corev1.Pod)
		mu.Lock()
		defer mu.Unlock()
		cur := rv[pod.Name]
		if pod.ResourceVersion != strconv.Itoa(cur) {
			return true, nil, apierrors.NewConflict(gr, pod.Name, nil)
		}
		rv[pod.Name] = cur + 1
		pod.ResourceVersion = strconv.Itoa(cur + 1)
		return false, nil, nil
	})
}

func fastPool(t *testing.T) {
	old := poolInterval
	poolInterval = 20 * time.Millisecond
	t.Cleanup(func() { poolInterval = old })
}

func readyWarm(t *testing.T, p *Provider, n int) []corev1.Pod {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pods, _ := p.pool.warmPods(context.Background())
		var r []corev1.Pod
		for i := range pods {
			if ready(&pods[i]) {
				r = append(r, pods[i])
			}
		}
		if len(r) >= n {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d ready warm pods", n)
	return nil
}

// TestWarmClaimFallsBackOnConflict: a claim whose Update conflicts (another claimer won) moves on to the
// next warm Pod instead of failing or starting cold.
func TestWarmClaimFallsBackOnConflict(t *testing.T) {
	fastPool(t)
	c := newFakeCluster(t, 0)
	enforceRV(c.client)
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.WarmPool = 2; o.WarmProfile = testLimits() })
	readyWarm(t, p, 2)
	var conflicts atomic.Int32
	c.client.PrependReactor("update", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if conflicts.Add(1) == 1 { // the first claim attempt loses the race
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "x", nil)
		}
		return false, nil, nil
	})
	if _, err := p.Create(context.Background(), specFor("inst-a")("env-c")); err != nil {
		t.Fatal(err)
	}
	if s := p.Stats(); s["warm"].Count != 1 || s["cold"].Count != 0 || conflicts.Load() < 2 {
		t.Fatalf("stats %+v, update attempts %d", s, conflicts.Load())
	}
}

// TestTwoClaimersNeverShareAPod: concurrent Creates against a pool of two each get their own Pod.
func TestTwoClaimersNeverShareAPod(t *testing.T) {
	fastPool(t)
	c := newFakeCluster(t, 0)
	enforceRV(c.client)
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.WarmPool = 2; o.WarmProfile = testLimits() })
	readyWarm(t, p, 2)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.Create(context.Background(), specFor("inst-a")("env-"+strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	a, b := mustPod(t, p, "env-0"), mustPod(t, p, "env-1")
	if a == b {
		t.Fatalf("both environments bound to %s", a)
	}
	for _, name := range []string{a, b} {
		pod, _ := c.client.CoreV1().Pods(testNS).Get(context.Background(), name, metav1.GetOptions{})
		if o, ok := parseOwner(pod); !ok || pod.Labels[LabelEnv] != labelValue(o.EnvID) {
			t.Fatalf("pod %s owner/label mismatch: %+v %v", name, pod.Labels, pod.Annotations)
		}
	}
}

// TestStuckWarmPodIsReplaced: a warm Pod that never becomes Ready (image pull failure, unschedulable) is
// deleted and replaced after WarmReadyTimeout.
func TestStuckWarmPodIsReplaced(t *testing.T) {
	fastPool(t)
	c := newFakeCluster(t, time.Hour) // the fake kubelet never starts them
	enforceRV(c.client)
	p := newTestProvider(t, c.client, c.exec, func(o *Options) {
		o.WarmPool = 1
		o.WarmProfile = testLimits()
		o.WarmReadyTimeout = 100 * time.Millisecond
	})
	seen := map[string]bool{}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(seen) < 3 {
		pods, _ := p.pool.warmPods(context.Background())
		for _, po := range pods {
			seen[po.Name] = true
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(seen) < 3 {
		t.Fatalf("stuck warm pod was not replaced (saw %d pods)", len(seen))
	}
	if pods, _ := p.pool.warmPods(context.Background()); len(pods) > 1 {
		t.Fatalf("pool grew beyond its size: %d", len(pods))
	}
}
