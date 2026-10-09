package k8s

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

// Labels and annotations (design §2.3).
const (
	LabelManaged  = "agentbox.io/managed"
	LabelInstall  = "agentbox.io/install"
	LabelPool     = "agentbox.io/pool"
	LabelState    = "agentbox.io/state"
	LabelEnv      = "agentbox.io/env"
	AnnoOwner     = "agentbox.io/owner"
	AnnoImage     = "agentbox.io/image"
	// AnnoWorkspace carries a hash of the host workspace path (the path itself is not published to the API).
	AnnoWorkspace = "agentbox.io/workspace-sha256"

	StateWarm     = "warm"
	StateAssigned = "assigned"

	containerName = "sandbox"
	// SandboxUID is the fixed non-root user of every sandbox Pod.
	SandboxUID int64 = 10001
	// NetworkPolicyName is the namespace's deny-all policy for sandbox Pods.
	NetworkPolicyName = "agentbox-sandbox-deny-all"
)

// owner is the content of the agentbox.io/owner annotation (the owner.json equivalent).
type owner struct {
	InstallID string           `json:"install_id"`
	EnvID     string           `json:"env_id"`
	SpecHash  string           `json:"spec_hash"`
	Kind      provider.EnvKind `json:"kind"`
}

func (o owner) encode() string {
	b, _ := json.Marshal(o)
	return string(b)
}

// parseOwner returns the owner annotation of pod; ok is false if it is missing or malformed.
func parseOwner(pod *corev1.Pod) (owner, bool) {
	s, found := pod.Annotations[AnnoOwner]
	if !found {
		return owner{}, false
	}
	var o owner
	if err := json.Unmarshal([]byte(s), &o); err != nil || o.InstallID == "" || o.EnvID == "" || o.SpecHash == "" {
		return owner{}, false
	}
	return o, true
}

var labelValueRE = regexp.MustCompile(`^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$`)

// labelValue returns s if it is a valid label value, otherwise "h-" + a hash of it.
func labelValue(s string) string {
	if len(s) <= 63 && labelValueRE.MatchString(s) {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return "h-" + hex.EncodeToString(sum[:])[:40]
}

// pathHash identifies a host path without publishing it.
func pathHash(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])[:32]
}

// newPodName returns a random DNS-1123 Pod name.
func newPodName() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "abx-" + hex.EncodeToString(b)
}

// profile is the resource shape of a Pod; Pods with equal profiles are interchangeable (warm pool key).
type profile struct {
	MemoryBytes int64
	CPUMilli    int64
	TmpBytes    int64
}

func profileOf(l provider.Limits) profile {
	return profile{MemoryBytes: l.MemoryMax, CPUMilli: l.CPUQuotaUs / 100, TmpBytes: l.TmpBytes}
}

// poolKey identifies interchangeable warm Pods: same image digest, profile and runtime class.
func poolKey(image string, pr profile, runtimeClass string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d|%d|%s", image, pr.MemoryBytes, pr.CPUMilli, pr.TmpBytes, runtimeClass)))
	return hex.EncodeToString(sum[:])[:16]
}

// podParams are the inputs of buildPod.
type podParams struct {
	Name, Namespace, InstallID, Image, RuntimeClass, PoolKey string
	Profile                                                  profile
	// SlotNodeDir is the Pod's slot directory as seen by the node (hostPath); empty for no shared mounts.
	SlotNodeDir string
	// Owner and EnvLabel are set for a cold (directly bound) Pod and empty for a warm one.
	Owner     *owner
	Workspace string
}

func ptr[T any](v T) *T { return &v }

// buildPod returns the sandbox Pod (design §2.4).
func buildPod(pp podParams) *corev1.Pod {
	labels := map[string]string{LabelManaged: "true", LabelInstall: labelValue(pp.InstallID), LabelPool: pp.PoolKey}
	annotations := map[string]string{AnnoImage: pp.Image}
	if pp.Owner != nil {
		labels[LabelState] = StateAssigned
		labels[LabelEnv] = labelValue(pp.Owner.EnvID)
		annotations[AnnoOwner] = pp.Owner.encode()
		if pp.Workspace != "" {
			annotations[AnnoWorkspace] = pathHash(pp.Workspace)
		}
	} else {
		labels[LabelState] = StateWarm
	}
	res := corev1.ResourceList{}
	if pp.Profile.MemoryBytes > 0 {
		res[corev1.ResourceMemory] = *resource.NewQuantity(pp.Profile.MemoryBytes, resource.BinarySI)
	}
	if pp.Profile.CPUMilli > 0 {
		res[corev1.ResourceCPU] = *resource.NewMilliQuantity(pp.Profile.CPUMilli, resource.DecimalSI)
	}
	memDir := func(limit int64) corev1.VolumeSource {
		v := &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}
		if limit > 0 {
			v.SizeLimit = resource.NewQuantity(limit, resource.BinarySI)
		}
		return corev1.VolumeSource{EmptyDir: v}
	}
	volumes := []corev1.Volume{
		{Name: "tmp", VolumeSource: memDir(pp.Profile.TmpBytes)},
		{Name: "exec", VolumeSource: memDir(1 << 20)},
	}
	mounts := []corev1.VolumeMount{
		{Name: "tmp", MountPath: "/tmp"},
		{Name: "exec", MountPath: podapi.ExecDir},
	}
	if pp.SlotNodeDir != "" {
		dir := corev1.HostPathDirectory
		volumes = append(volumes,
			corev1.Volume{Name: "workspace", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: path.Join(pp.SlotNodeDir, slotWorkspace), Type: &dir}}},
			corev1.Volume{Name: "run", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: path.Join(pp.SlotNodeDir, slotRun), Type: &dir}}})
		mounts = append(mounts,
			corev1.VolumeMount{Name: "workspace", MountPath: "/workspace"},
			corev1.VolumeMount{Name: "run", MountPath: "/run/agentbox"})
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: pp.Name, Namespace: pp.Namespace, Labels: labels, Annotations: annotations},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			AutomountServiceAccountToken:  ptr(false),
			EnableServiceLinks:            ptr(false),
			TerminationGracePeriodSeconds: ptr(int64(2)),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   ptr(true),
				RunAsUser:      ptr(SandboxUID),
				RunAsGroup:     ptr(SandboxUID),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            containerName,
				Image:           pp.Image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{podapi.Binary, "init"},
				WorkingDir:      "/",
				Resources:       corev1.ResourceRequirements{Requests: res, Limits: res},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr(false),
					Privileged:               ptr(false),
					ReadOnlyRootFilesystem:   ptr(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: mounts,
			}},
			Volumes: volumes,
		},
	}
	if pp.RuntimeClass != "" {
		pod.Spec.RuntimeClassName = ptr(pp.RuntimeClass)
	}
	return pod
}

// denyAllPolicy selects every sandbox Pod and allows no ingress and no egress.
func denyAllPolicy(namespace string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: NetworkPolicyName, Namespace: namespace},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{LabelManaged: "true"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
		},
	}
}

// Pod state helpers.

func terminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func ready(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// stopped reports that no process of the Pod can exist: the Pod is terminal, or every container has
// terminated (restartPolicy Never: they never restart; the kubelet updates the phase a sync later).
func stopped(pod *corev1.Pod) bool {
	if terminal(pod) {
		return true
	}
	cs := pod.Status.ContainerStatuses
	if len(cs) == 0 || len(cs) < len(pod.Spec.Containers) {
		return false
	}
	for _, c := range cs {
		if c.State.Terminated == nil {
			return false
		}
	}
	return true
}

// running reports whether processes may exist: the Pod is bound to a node and not stopped.
func running(pod *corev1.Pod) bool {
	return !stopped(pod) && pod.Spec.NodeName != ""
}

func oomKilled(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		for _, st := range []corev1.ContainerState{cs.State, cs.LastTerminationState} {
			if st.Terminated != nil && st.Terminated.Reason == "OOMKilled" {
				return true
			}
		}
	}
	return false
}
