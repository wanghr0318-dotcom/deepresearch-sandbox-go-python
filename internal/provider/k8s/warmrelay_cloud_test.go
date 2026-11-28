package k8s

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typed "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
)

// Pin only Pods created by this isolated test. The underlying request still goes through
// the real API server, scheduler, kubelet, container runtime and CNI.
type nodePinnedClient struct {
	kubernetes.Interface
	node string
}
type nodePinnedCore struct {
	typed.CoreV1Interface
	node string
}
type nodePinnedPods struct {
	typed.PodInterface
	node string
}

func (c nodePinnedClient) CoreV1() typed.CoreV1Interface {
	return nodePinnedCore{c.Interface.CoreV1(), c.node}
}
func (c nodePinnedCore) Pods(ns string) typed.PodInterface {
	return nodePinnedPods{c.CoreV1Interface.Pods(ns), c.node}
}
func (c nodePinnedPods) Create(ctx context.Context, pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
	pod = pod.DeepCopy()
	pod.Spec.NodeSelector = map[string]string{corev1.LabelHostname: c.node}
	return c.PodInterface.Create(ctx, pod, opts)
}

// The test's Unix socket represents one task and attempt. Storage and both file tools
// are production implementations; this fixture has no PG, shell executor or task scheduler.
type warmFileIdentity struct{}

func (warmFileIdentity) CheckAccess(_ context.Context, task, attempt, subrun string) (call.Result, error) {
	if task != "warm-test" || attempt != "warm-attempt" || subrun != "" {
		return call.Result{}, errors.New("incorrect tool identity")
	}
	return call.Result{}, nil
}
func (s warmFileIdentity) PublishWorkspace(ctx context.Context, task, attempt, subrun string, publish func() error) (call.Result, error) {
	res, err := s.CheckAccess(ctx, task, attempt, subrun)
	if err != nil {
		return res, err
	}
	return res, publish()
}
func (warmFileIdentity) TaskTerminal(context.Context, string) (bool, error) { return false, nil }
func (warmFileIdentity) ExecShell(context.Context, call.ShellInvoke) (call.Result, error) {
	return call.Result{}, errors.New("unexpected shell call")
}

func TestWarmRelayOnRealCluster(t *testing.T) {
	kubeconfig, image, node, addr := os.Getenv("AGENTBOX_K8S_TEST_KUBECONFIG"), os.Getenv("AGENTBOX_K8S_TEST_IMAGE"), os.Getenv("AGENTBOX_WARM_TEST_NODE"), os.Getenv("AGENTBOX_WARM_TEST_LISTEN")
	if kubeconfig == "" || image == "" || node == "" || addr == "" {
		t.Skip("opt-in warm relay test: set kubeconfig, pinned worker image, node and private listen address")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, ex, err := Connect(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "agentbox-warm-it-"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created namespace=%s uid=%s", ns.Name, ns.UID)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := client.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &ns.UID}}); err != nil {
			t.Error(err)
			return
		}
		if err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := client.CoreV1().Namespaces().Get(ctx, ns.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Errorf("namespace cleanup: %v", err)
		} else {
			t.Logf("namespace_deleted=%s", ns.Name)
		}
	})
	_, err = client.CoreV1().ResourceQuotas(ns.Name).Create(ctx, &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "one-pod"}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spec := specFor("warm-integration")("warm-env")
	spec.Limits.CPUQuotaUs = 10000
	spec.Limits.MemoryMax = 128 << 20
	p, err := New(ctx, Options{Client: nodePinnedClient{client, node}, Executor: ex, Namespace: ns.Name, InstallID: spec.InstallID, Image: image, GatewayListen: addr, WarmPool: 1, WarmProfile: spec.Limits, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	var warm *corev1.Pod
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 80*time.Second, true, func(ctx context.Context) (bool, error) {
		pods, err := p.pool.warmPods(ctx)
		if err != nil {
			return false, err
		}
		for i := range pods {
			if ready(&pods[i]) {
				warm = &pods[i]
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("warm Ready: %v", err)
	}
	if warm.UID == "" || warm.Spec.NodeName != node {
		t.Fatalf("warm uid/node: %s %s", warm.UID, warm.Spec.NodeName)
	}
	t.Logf("warm_ready node=%s pod=%s uid=%s image=%s", node, warm.Name, warm.UID, image)
	local, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id := warmFileIdentity{}
	m, err := workspace.New(workspace.Config{Dir: t.TempDir(), Blobs: local, Exec: id, Access: id, Tasks: id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	spec.Mounts.GatewaySocket = filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", spec.Mounts.GatewaySocket)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, tool := range []struct {
		name string
		run  func(context.Context, workspace.Request) (call.Result, error)
	}{{"write_file", m.Write}, {"read_file", m.Read}} {
		mux.HandleFunc("POST /"+tool.name, func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
			if err != nil {
				http.Error(w, "body", 400)
				return
			}
			res, err := tool.run(r.Context(), workspace.Request{TaskID: "warm-test", AttemptID: "warm-attempt", Body: body})
			if err != nil {
				http.Error(w, "tool error", 500)
				return
			}
			w.WriteHeader(res.Status)
			w.Write(res.Body)
		})
	}
	server := &http.Server{Handler: mux}
	go server.Serve(ln)
	t.Cleanup(func() { server.Close() })
	const throughSocket = `import http.client,json,socket,sys
conn=http.client.HTTPConnection("localhost",timeout=12)
conn.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
conn.sock.settimeout(12)
conn.sock.connect("/run/agentbox/gateway.sock")
conn.request("POST","/"+sys.argv[1],body=sys.argv[2],headers={"Content-Type":"application/json"})
r=conn.getresponse()
print(json.dumps({"status":r.status,"body":r.read().decode()}))`
	request := func(tool, body string, status int) string {
		t.Helper()
		var out, errOut bytes.Buffer
		if err := ex.Stream(ctx, ns.Name, warm.Name, []string{"python3", "-c", throughSocket, tool, body}, nil, &out, &errOut); err != nil {
			t.Fatalf("pod exec: %v %s", err, errOut.String())
		}
		var got struct {
			Status int
			Body   string
		}
		if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Status != status {
			t.Fatalf("%s expected %d got %+v, parse=%v", tool, status, got, err)
		}
		t.Logf("pod_uid=%s tool=%s status=%d body=%s", warm.UID, tool, got.Status, got.Body)
		return got.Body
	}
	request("read_file", `{"path":"witness.txt"}`, 401)
	if _, err = p.Create(ctx, spec); err != nil {
		t.Fatal(err)
	}
	claimed, err := p.boundPod(ctx, spec.EnvID)
	if err != nil || claimed == nil || claimed.UID != warm.UID || p.Stats()["warm"].Count != 1 {
		t.Fatalf("warm claim: %v", err)
	}
	t.Logf("claimed env=%s uid=%s node=%s", spec.EnvID, claimed.UID, claimed.Spec.NodeName)
	request("write_file", `{"path":"witness.txt","content":"warm-claim-proof"}`, 200)
	body := request("read_file", `{"path":"witness.txt"}`, 200)
	var result struct{ Content string }
	if err := json.Unmarshal([]byte(body), &result); err != nil || result.Content != "warm-claim-proof" {
		t.Fatal(fmt.Errorf("tool result mismatch: %v", err))
	}
	p.revokeGateway(claimed.Name)
	request("read_file", `{"path":"witness.txt"}`, 401)
}
