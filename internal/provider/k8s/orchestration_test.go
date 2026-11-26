package k8s

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodWorkspaceAndGatewayGrant(t *testing.T) {
	ctx := context.Background()
	c := newFakeCluster(t, 0)
	p := newTestProvider(t, c.client, c.exec, func(opt *Options) {
		opt.GatewayListen = "127.0.0.1:0"
	})
	spec := specFor(p.opt.InstallID)("env-pod-workspace")
	spec.Mounts.Workspace = "/controller/workspace"
	spec.Mounts.GatewaySocket = "/controller/gateway.sock"
	if _, err := p.Create(ctx, spec); err != nil {
		t.Fatal(err)
	}
	pod, err := p.boundPod(ctx, spec.EnvID)
	if err != nil {
		t.Fatal(err)
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath != nil {
			t.Fatalf("Pod-local workspace must not use hostPath: %s", volume.Name)
		}
	}
	if len(pod.Spec.Containers) != 2 || pod.Spec.Containers[1].Name != relayContainerName {
		t.Fatal("expected one Gateway relay sidecar")
	}
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name == "gateway-grant" || mount.Name == "run" && !mount.ReadOnly {
			t.Fatalf("worker must only receive a read-only socket mount: %+v", mount)
		}
	}
	grant, err := c.client.CoreV1().Secrets(testNS).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(grant.OwnerReferences) != 1 || grant.OwnerReferences[0].Name != pod.Name ||
		grant.OwnerReferences[0].UID != pod.UID || len(grant.Data[relayConfigFile]) == 0 {
		t.Fatal("grant must belong to the exact Pod")
	}
	if err := p.Stop(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	_, active := p.grants[pod.Name]
	p.mu.Unlock()
	if active {
		t.Fatal("Stop left the Gateway grant active")
	}
	if err := p.Destroy(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
}
