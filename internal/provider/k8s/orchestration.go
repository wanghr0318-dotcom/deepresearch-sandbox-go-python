package k8s

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

const (
	relayLabel         = "agentbox.io/gateway-relay"
	relayContainerName = "gateway-relay"
	relayConfigDir     = "/etc/agentbox"
	relayConfigFile    = "gateway.json"
	relaySocketDir     = "/run/agentbox"
)

// UsesPodWorkspace reports whether artifacts live in the Pod instead of a host slot.
func (p *Provider) UsesPodWorkspace() bool { return p.relay != nil }

func buildPodWorkspace(pod *corev1.Pod, grantSecret string, memDir func(int64) corev1.VolumeSource) {
	pod.Spec.SecurityContext.FSGroup = ptr(SandboxUID)
	workspace := &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(512<<20, resource.BinarySI)}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: workspace}})
	sandbox := &pod.Spec.Containers[0]
	sandbox.VolumeMounts = append(sandbox.VolumeMounts, corev1.VolumeMount{Name: "workspace", MountPath: "/workspace"})
	if grantSecret == "" {
		return
	}

	pod.Labels[relayLabel] = "true"
	socketMount := corev1.VolumeMount{Name: "run", MountPath: relaySocketDir}
	sandbox.VolumeMounts = append(sandbox.VolumeMounts, corev1.VolumeMount{Name: socketMount.Name, MountPath: socketMount.MountPath, ReadOnly: true})
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{Name: socketMount.Name, VolumeSource: memDir(1 << 20)},
		corev1.Volume{Name: "gateway-grant", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: grantSecret, DefaultMode: ptr(int32(0o440)),
		}}},
	)
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name: relayContainerName, Image: sandbox.Image, ImagePullPolicy: sandbox.ImagePullPolicy,
		Command:   []string{podapi.Binary, "gateway-relay", relayConfigDir + "/" + relayConfigFile},
		Resources: sidecarResources(), SecurityContext: sandbox.SecurityContext.DeepCopy(),
		VolumeMounts: []corev1.VolumeMount{
			socketMount,
			{Name: "gateway-grant", MountPath: relayConfigDir, ReadOnly: true},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"test", "-S", relaySocketDir + "/gateway.sock"}}},
			PeriodSeconds: 1,
		},
	})
}

func (p *Provider) createWorkspacePod(ctx context.Context, pr profile, own *owner, socketPath string) (*corev1.Pod, error) {
	params := podParams{Name: newPodName(), Profile: pr, Owner: own, PodWorkspace: true}
	if socketPath == "" {
		return p.createBuiltPod(ctx, params)
	}
	grant, revoke, err := p.relay.bind(socketPath)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.grants[params.Name] = revoke
	p.mu.Unlock()
	defer func() {
		if err != nil {
			p.revokeGateway(params.Name)
		}
	}()
	payload, err := json.Marshal(grant)
	if err != nil {
		return nil, err
	}
	params.RelaySecret = params.Name
	pod, err := p.createBuiltPod(ctx, params)
	if err != nil {
		return nil, err
	}
	// The Pod waits for the mounted Secret; the owner reference ties its
	// lifetime to its exact Pod UID, including after a controller restart.
	_, err = p.client.CoreV1().Secrets(p.opt.Namespace).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            params.RelaySecret,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}},
		},
		Data: map[string][]byte{relayConfigFile: payload},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8s: create Gateway grant for pod %s: %w", pod.Name, err)
	}
	return pod, nil
}

func (p *Provider) revokeGateway(pod string) {
	p.mu.Lock()
	revoke := p.grants[pod]
	delete(p.grants, pod)
	p.mu.Unlock()
	if revoke != nil {
		revoke()
	}
}

func (p *Provider) startGatewayRelay(ctx context.Context) error {
	relay, err := newGatewayRelay(p.opt.GatewayListen)
	if err != nil {
		return err
	}
	np := gatewayRelayPolicy(p.opt.Namespace, p.opt.InstallID, relay)
	policies := p.client.NetworkingV1().NetworkPolicies(p.opt.Namespace)
	_, err = policies.Create(ctx, np, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		var existing *networkingv1.NetworkPolicy
		existing, err = policies.Get(ctx, np.Name, metav1.GetOptions{})
		if err == nil && !equality.Semantic.DeepEqual(existing.Spec, np.Spec) {
			err = fmt.Errorf("Gateway relay policy %s uses a different endpoint", np.Name)
		}
	}
	if err != nil {
		relay.close()
		return fmt.Errorf("k8s: create Gateway relay NetworkPolicy: %w", err)
	}
	p.relay = relay
	p.grants = make(map[string]func())
	return nil
}

func gatewayRelayPolicy(namespace, installID string, relay *gatewayRelay) *networkingv1.NetworkPolicy {
	bits := 128
	if relay.ip.To4() != nil {
		bits = 32
	}
	port := intstr.FromInt32(relay.port)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "agentbox-gateway-relay-" + pathHash(installID)[:12], Namespace: namespace},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{
				LabelManaged: "true", LabelInstall: labelValue(installID), relayLabel: "true",
			}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{{
				To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{
					CIDR: fmt.Sprintf("%s/%d", relay.ip, bits),
				}}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: ptr(corev1.ProtocolTCP), Port: &port}},
			}},
		},
	}
}
