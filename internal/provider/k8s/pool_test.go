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
	gvr := corev1.SchemeGroupVersion.WithResource("pods")
	// The reactors store a deep copy through the tracker themselves and never mutate the action's object
	// (which the caller still owns).
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		pod := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.ResourceVersion, pod.CreationTimestamp = "1", metav1.Now()
		mu.Lock()
		defer mu.Unlock()
		if err := c.Tracker().Create(gvr, pod, a.GetNamespace()); err != nil {
			return true, nil, err
		}
		rv[pod.Name] = 1
		return true, pod.DeepCopy(), nil
	})
	c.PrependReactor("update", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		pod := a.(k8stesting.UpdateAction).GetObject().(*corev1.Pod).DeepCopy()
		mu.Lock()
		defer mu.Unlock()
		cur := rv[pod.Name]
		if pod.ResourceVersion != strconv.Itoa(cur) {
			return true, nil, apierrors.NewConflict(gr, pod.Name, nil)
		}
		pod.ResourceVersion = strconv.Itoa(cur + 1)
		if err := c.Tracker().Update(gvr, pod, a.GetNamespace()); err != nil {
			return true, nil, err
		}
		rv[pod.Name] = cur + 1
		return true, pod.DeepCopy(), nil
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
	var armed atomic.Bool
	var conflicts atomic.Int32
	c := newFakeCluster(t, 0, enforceRV, func(cs *fake.Clientset) {
		cs.PrependReactor("update", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
			if armed.Load() && conflicts.Add(1) == 1 { // the first claim attempt loses the race
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "x", nil)
			}
			return false, nil, nil
		})
	})
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.WarmPool = 2; o.WarmProfile = testLimits() })
	readyWarm(t, p, 2)
	armed.Store(true)
	if _, err := p.Create(context.Background(), specFor("inst-a")("env-c")); err != nil {
		t.Fatal(err)
	}
	if s := p.Stats(); s["warm"].Count != 1 || s["cold"].Count != 0 || conflicts.Load() < 2 {
		t.Fatalf("stats %+v, update attempts %d", s, conflicts.Load())
	}
}

// TestTwoClaimersNeverShareAPod: concurrent Creates against a pool of two each get their own Pod. A barrier
// in the update path holds both claimers' first Update until both have arrived, so both carry the same
// (listed) resourceVersion for the same oldest Pod: exactly one wins, the other gets a Conflict and claims
// the next Pod.
func TestTwoClaimersNeverShareAPod(t *testing.T) {
	fastPool(t)
	c := newFakeCluster(t, 0, enforceRV)
	p := newTestProvider(t, c.client, c.exec, func(o *Options) { o.WarmPool = 2; o.WarmProfile = testLimits() })
	readyWarm(t, p, 2)
	// Barrier: the first claim attempt of each claimer waits until both have listed and chosen a Pod.
	var arrived atomic.Int32
	both := make(chan struct{})
	var firstTarget sync.Map
	p.pool.beforeClaim = func(pod string) {
		n := arrived.Add(1)
		firstTarget.Store(n, pod)
		switch {
		case n == 2:
			close(both)
		case n < 2:
			select {
			case <-both:
			case <-time.After(5 * time.Second):
			}
		}
	}
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
	t1, _ := firstTarget.Load(int32(1))
	t2, _ := firstTarget.Load(int32(2))
	if t1 != t2 || arrived.Load() != 3 {
		t.Fatalf("both first claims should target the same Pod (%v, %v) and the loser retry once (%d attempts)", t1, t2, arrived.Load())
	}
	if s := p.Stats(); s["warm"].Count != 2 || s["cold"].Count != 0 {
		t.Fatalf("both claims should be warm: %+v", s)
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
	c := newFakeCluster(t, time.Hour, enforceRV) // the fake kubelet never starts them
	oldBase := warmBackoffBase
	warmBackoffBase = time.Millisecond
	t.Cleanup(func() { warmBackoffBase = oldBase })
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
	if p.WarmStuck() < 2 {
		t.Fatalf("WarmStuck = %d", p.WarmStuck())
	}
}

// TestStuckWarmPodsBackOff: after a stuck warm Pod is replaced, creation waits warmBackoffBase·2^(n-1), so a
// permanently failing image does not churn Pods every WarmReadyTimeout.
func TestStuckWarmPodsBackOff(t *testing.T) {
	fastPool(t)
	c := newFakeCluster(t, time.Hour, enforceRV)
	oldBase := warmBackoffBase
	warmBackoffBase = time.Second
	t.Cleanup(func() { warmBackoffBase = oldBase })
	p := newTestProvider(t, c.client, c.exec, func(o *Options) {
		o.WarmPool = 1
		o.WarmProfile = testLimits()
		o.WarmReadyTimeout = 50 * time.Millisecond
	})
	deadline := time.Now().Add(5 * time.Second)
	for p.WarmStuck() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if p.WarmStuck() == 0 {
		t.Fatal("no stuck replacement")
	}
	time.Sleep(500 * time.Millisecond) // well inside the 1 s backoff
	if pods, _ := p.pool.warmPods(context.Background()); len(pods) != 0 || p.WarmStuck() != 1 {
		t.Fatalf("during backoff: %d warm pods, %d stuck replacements", len(pods), p.WarmStuck())
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pods, _ := p.pool.warmPods(context.Background()); len(pods) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pool did not recreate after the backoff")
}
