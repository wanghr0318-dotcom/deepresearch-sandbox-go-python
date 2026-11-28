package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

func TestWarmRelayClaimAndRestart(t *testing.T) {
	fastPool(t)
	ctx := t.Context()
	c := newFakeCluster(t, 0, enforceRV, func(cs *fake.Clientset) {
		cs.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
			pod := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
			pod.UID = types.UID(newPodName())
			return false, nil, nil
		})
	})
	newPool := func(addr string) *Provider {
		return newTestProvider(t, c.client, c.exec, func(opt *Options) {
			opt.GatewayListen, opt.WarmPool, opt.WarmProfile = addr, 2, testLimits()
		})
	}
	p := newPool("127.0.0.1:0")
	grantFor := func(pod corev1.Pod) podapi.GatewayGrant {
		t.Helper()
		for _, v := range pod.Spec.Volumes {
			if v.HostPath != nil {
				t.Fatal("warm relay has a host mount")
			}
		}
		for _, mount := range pod.Spec.Containers[0].VolumeMounts {
			if mount.Name == "gateway-grant" {
				t.Fatal("worker mounts relay credentials")
			}
		}
		secret, err := c.client.CoreV1().Secrets(testNS).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].UID != pod.UID {
			t.Fatal("credential is not owned by Pod UID")
		}
		var grant podapi.GatewayGrant
		if err := json.Unmarshal(secret.Data[relayConfigFile], &grant); err != nil {
			t.Fatal(err)
		}
		return grant
	}
	check := func(grant podapi.GatewayGrant, status int, want string) {
		t.Helper()
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(grant.CAPEM)) {
			t.Fatal("invalid CA")
		}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}, Timeout: 5 * time.Second}
		defer client.CloseIdleConnections()
		req, err := http.NewRequestWithContext(ctx, "GET", grant.URL+"/tool", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+grant.Token)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != status || string(body) != want {
			t.Fatalf("status/body = %d %q, err=%v", resp.StatusCode, body, err)
		}
	}
	initial := readyWarm(t, p, 2)
	for _, pod := range initial {
		check(grantFor(pod), 401, "Unauthorized\n")
	}
	claimed := map[string]podapi.GatewayGrant{}
	for _, env := range []string{"first", "second"} {
		socket := filepath.Join(t.TempDir(), "gw")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, env) })}
		go server.Serve(ln)
		t.Cleanup(func() { server.Close() })
		spec := specFor(p.opt.InstallID)(env)
		spec.Mounts.GatewaySocket = socket
		if _, err := p.Create(ctx, spec); err != nil {
			t.Fatal(err)
		}
		pod, err := p.boundPod(ctx, env)
		if err != nil {
			t.Fatal(err)
		}
		claimed[env] = grantFor(*pod)
		check(claimed[env], 200, env)
	}
	if p.Stats()["warm"].Count != 2 || claimed["first"].Token == claimed["second"].Token {
		t.Fatal("expected two separately bound warm Pods")
	}
	if err := p.Stop(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	check(claimed["first"], 401, "Unauthorized\n")
	check(claimed["second"], 200, "second")

	leftover := readyWarm(t, p, 1)[0]
	old := grantFor(leftover)
	addr := old.URL[len("https://"):]
	p.Close()
	next := newPool(addr)
	var current podapi.GatewayGrant
	deadline := time.Now().Add(5 * time.Second)
	for current.Token == "" && time.Now().Before(deadline) {
		for _, pod := range readyWarm(t, next, 2) {
			if pod.Labels[LabelPool] == next.pool.key {
				current = grantFor(pod)
				break
			}
		}
		if current.Token == "" {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if current.Token == "" || old.CAPEM == current.CAPEM {
		t.Fatal("warm relay generation was not replaced")
	}
	old.CAPEM = current.CAPEM
	check(old, 401, "Unauthorized\n")
	if _, err := next.pods().Get(ctx, leftover.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old warm Pod remains: %v", err)
	}
}

func TestFailedWarmRelayClaimLeavesNoRoute(t *testing.T) {
	fastPool(t)
	failure := errors.New("test rejected claim")
	c := newFakeCluster(t, 0, func(cs *fake.Clientset) {
		cs.PrependReactor("update", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, failure
		})
	})
	p := newTestProvider(t, c.client, c.exec, func(opt *Options) {
		opt.GatewayListen = "127.0.0.1:0"
		opt.WarmPool = 1
		opt.WarmProfile = testLimits()
	})
	readyWarm(t, p, 1)
	spec := specFor(p.opt.InstallID)("failed")
	spec.Mounts.GatewaySocket = "/unused"
	if _, err := p.Create(t.Context(), spec); !errors.Is(err, failure) {
		t.Fatalf("claim error: %v", err)
	}
	p.relay.mu.RLock()
	defer p.relay.mu.RUnlock()
	if len(p.relay.grants) != 0 {
		t.Fatal("claim failure authorized a Pod")
	}
}

func TestRevokedWarmClaimDoesNotActivateOnLateUpdate(t *testing.T) {
	fastPool(t)
	updated := make(chan string, 1)
	release := make(chan struct{})
	c := newFakeCluster(t, 0, func(cs *fake.Clientset) {
		cs.PrependReactor("update", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
			pod := a.(k8stesting.UpdateAction).GetObject().(*corev1.Pod).DeepCopy()
			if err := cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, a.GetNamespace()); err != nil {
				return true, nil, err
			}
			updated <- pod.Name
			<-release
			return true, pod, nil
		})
	})
	p := newTestProvider(t, c.client, c.exec, func(opt *Options) {
		opt.GatewayListen, opt.WarmPool, opt.WarmProfile = "127.0.0.1:0", 1, testLimits()
	})
	readyWarm(t, p, 1)
	spec := specFor(p.opt.InstallID)("revoked")
	spec.Mounts.GatewaySocket = "/unused"
	done := make(chan error, 1)
	go func() { _, err := p.Create(t.Context(), spec); done <- err }()
	select {
	case name := <-updated:
		p.revokeGateway(name)
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("warm Pod was not assigned")
	}
	close(release)
	if err := <-done; !errors.Is(err, provider.ErrIncomplete) {
		t.Fatalf("retired in-flight claim: %v", err)
	}
	p.relay.mu.RLock()
	defer p.relay.mu.RUnlock()
	if len(p.relay.grants) != 0 {
		t.Fatal("retired claim activated after Update reply")
	}
}
