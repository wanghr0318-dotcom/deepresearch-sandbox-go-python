// Package k8s is a provider.Provider that runs every environment in one Kubernetes Pod (design
// docs/design/2026-10-10-k8s-provider-design.md). The control channel is the pods/exec API running the
// in-image helper cmd/agentbox-podagent; Stop kills the Pod through activeDeadlineSeconds; ownership is
// carried by labels and the agentbox.io/owner annotation; an optional warm pool keeps pre-started Pods.
package k8s

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// Options configure a Provider.
type Options struct {
	Client    kubernetes.Interface
	Executor  Executor
	Namespace string // default "agentbox-sandbox"
	InstallID string
	// Image is the pinned worker image (repo@sha256:…; see PinImage).
	Image        string
	RuntimeClass string // optional runtimeClassName, e.g. "gvisor"
	// SlotHostDir is the shared slot directory as seen by this process; SlotNodeDir the same directory as
	// seen by the node (default: SlotHostDir). Empty disables workspace/Gateway mounts (Create then rejects
	// specs that need them).
	SlotHostDir, SlotNodeDir string
	// WarmPool is the number of warm Pods kept for WarmProfile (0 disables the pool).
	WarmPool    int
	WarmProfile provider.Limits
	// CreateTimeout bounds waiting for a Pod to become ready when ctx has no deadline (default 2 min).
	CreateTimeout time.Duration
	// StopTimeout bounds Stop and Destroy when ctx has no deadline (default 30 s).
	StopTimeout time.Duration
	// WarmReadyTimeout: a warm Pod not Ready after this long (Pending, ImagePullBackOff, …) is replaced
	// (default 3 min).
	WarmReadyTimeout time.Duration
	// SlotGCInterval is how often slot directories of vanished Pods with unreferenced workspaces are removed
	// (default 1 min; slots younger than 10 min are kept).
	SlotGCInterval time.Duration
	// EnsureNetworkPolicy creates the namespace's deny-all NetworkPolicy if missing.
	EnsureNetworkPolicy bool
	Logger              *slog.Logger
}

// Provider implements provider.Provider on Kubernetes.
type Provider struct {
	opt    Options
	client kubernetes.Interface
	exec   Executor
	slots  slots
	log    *slog.Logger
	pool   *pool
	stats  *stats

	ctx    context.Context // provider lifetime: exec streams and the pool loop
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// beforeSend is a test hook: called by StartExec after registering the in-flight start.
	beforeSend func(envID string)

	mu   sync.Mutex
	envs map[string]*envState // environments created by this process (gates are not rebuilt after a restart)
}

type envState struct {
	kind   provider.EnvKind
	pod    string
	gate   *gate
	ws     string // host workspace path bound to the Pod's slot
	limits provider.Limits
}

var _ provider.Provider = (*Provider)(nil)

// New validates opt, ensures the NetworkPolicy and starts the warm pool loop.
func New(ctx context.Context, opt Options) (*Provider, error) {
	switch {
	case opt.Client == nil || opt.Executor == nil:
		return nil, errors.New("k8s: Options need Client and Executor")
	case opt.InstallID == "":
		return nil, errors.New("k8s: Options need InstallID")
	case opt.Image == "":
		return nil, errors.New("k8s: Options need a pinned Image")
	}
	if r, err := ParseImageRef(opt.Image); err != nil {
		return nil, err
	} else if r.Digest == "" {
		return nil, fmt.Errorf("k8s: image %q is not pinned to a digest (see PinImage)", opt.Image)
	}
	if opt.Namespace == "" {
		opt.Namespace = "agentbox-sandbox"
	}
	if opt.CreateTimeout <= 0 {
		opt.CreateTimeout = 2 * time.Minute
	}
	if opt.StopTimeout <= 0 {
		opt.StopTimeout = 30 * time.Second
	}
	if opt.WarmReadyTimeout <= 0 {
		opt.WarmReadyTimeout = 3 * time.Minute
	}
	if opt.SlotGCInterval <= 0 {
		opt.SlotGCInterval = time.Minute
	}
	if opt.SlotNodeDir == "" {
		opt.SlotNodeDir = opt.SlotHostDir
	}
	if opt.Logger == nil {
		opt.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	uid := -1
	if os.Geteuid() == 0 {
		uid = int(SandboxUID)
	}
	p := &Provider{
		opt: opt, client: opt.Client, exec: opt.Executor, log: opt.Logger, stats: newStats(),
		slots: slots{hostRoot: opt.SlotHostDir, nodeRoot: opt.SlotNodeDir, uid: uid},
		envs:  make(map[string]*envState),
	}
	if p.slots.enabled() {
		if err := os.MkdirAll(opt.SlotHostDir, 0o711); err != nil {
			return nil, fmt.Errorf("k8s: slot directory: %w", err)
		}
	}
	if opt.EnsureNetworkPolicy {
		np := denyAllPolicy(opt.Namespace)
		if _, err := p.client.NetworkingV1().NetworkPolicies(opt.Namespace).Create(ctx, np, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("k8s: NetworkPolicy: %w", err)
		}
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	if p.slots.enabled() {
		p.wg.Add(1)
		go func() { defer p.wg.Done(); p.slotGCLoop(p.ctx) }()
	}
	if opt.WarmPool > 0 {
		pr := profileOf(opt.WarmProfile)
		p.pool = &pool{p: p, size: opt.WarmPool, profile: pr, key: poolKey(opt.Image, pr, opt.RuntimeClass), kick: make(chan struct{}, 1)}
		p.wg.Add(1)
		go func() { defer p.wg.Done(); p.pool.run(p.ctx) }()
	}
	return p, nil
}

// slotGCMinAge protects slots created just before their Pod.
var slotGCMinAge = 10 * time.Minute

func (p *Provider) slotGCLoop(ctx context.Context) {
	t := time.NewTicker(p.opt.SlotGCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.gcSlots(ctx)
		}
	}
}

// gcSlots removes slot directories whose Pod no longer exists and whose workspace is no longer referenced by
// any workspace symlink (e.g. a session was closed and its workspace deleted).
func (p *Provider) gcSlots(ctx context.Context) {
	l, err := p.pods().List(ctx, metav1.ListOptions{LabelSelector: labels.Set{LabelManaged: "true"}.AsSelector().String()})
	if err != nil {
		return
	}
	live := make(map[string]bool, len(l.Items))
	for i := range l.Items {
		live[l.Items[i].Name] = true
	}
	removed, err := p.slots.gc(live, slotGCMinAge)
	if err != nil {
		p.log.Warn("k8s: slot gc", "err", err)
	}
	if len(removed) > 0 {
		p.log.Info("k8s: removed unreferenced slots", "count", len(removed))
	}
}

// Close stops the warm pool loop and open exec streams. Pods are left in place (the pool's warm Pods are
// reused by the next process of the same installation).
func (p *Provider) Close() {
	p.cancel()
	p.wg.Wait()
}

// Image returns the pinned image reference every Pod runs (for run manifests).
func (p *Provider) Image() string { return p.opt.Image }

// Stats returns the start-latency summary per path ("warm", "cold").
func (p *Provider) Stats() map[string]LatencySummary { return p.stats.summary() }

func (p *Provider) pods() podClient { return p.client.CoreV1().Pods(p.opt.Namespace) }

type podClient = interface {
	Create(context.Context, *corev1.Pod, metav1.CreateOptions) (*corev1.Pod, error)
	Update(context.Context, *corev1.Pod, metav1.UpdateOptions) (*corev1.Pod, error)
	Delete(context.Context, string, metav1.DeleteOptions) error
	Get(context.Context, string, metav1.GetOptions) (*corev1.Pod, error)
	List(context.Context, metav1.ListOptions) (*corev1.PodList, error)
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
	Patch(context.Context, string, types.PatchType, []byte, metav1.PatchOptions, ...string) (*corev1.Pod, error)
}

func (p *Provider) state(envID string) *envState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.envs[envID]
}

// envPods lists every managed Pod labelled with envID, of any installation.
func (p *Provider) envPods(ctx context.Context, envID string) ([]corev1.Pod, error) {
	sel := labels.Set{LabelManaged: "true", LabelEnv: labelValue(envID)}.AsSelector().String()
	l, err := p.pods().List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, fmt.Errorf("k8s: list pods: %w", err)
	}
	return l.Items, nil
}

// lookup classifies the Pods bound to envID: ours is this installation's Pod (nil if none); foreign is
// true if any Pod with the env label cannot be proven to belong to this installation.
func (p *Provider) lookup(ctx context.Context, envID string) (ours *corev1.Pod, o owner, foreign bool, err error) {
	pods, err := p.envPods(ctx, envID)
	if err != nil {
		return nil, owner{}, false, err
	}
	for i := range pods {
		po, ok := parseOwner(&pods[i])
		switch {
		case !ok || po.InstallID != p.opt.InstallID || po.EnvID != envID:
			foreign = true
		case ours == nil:
			ours, o = &pods[i], po
		default: // two Pods of this installation for one env: report the older, the other is residue too
			if pods[i].CreationTimestamp.Before(&ours.CreationTimestamp) {
				ours, o = &pods[i], po
			}
		}
	}
	return ours, o, foreign, nil
}

// Create binds envID to a Pod (warm claim or cold start) and waits until it is ready (contract §3).
func (p *Provider) Create(ctx context.Context, spec provider.EnvSpec) (provider.EnvInfo, error) {
	if err := spec.Validate(); err != nil {
		return provider.EnvInfo{}, err
	}
	if spec.Kind == provider.KindExec {
		return provider.EnvInfo{}, errors.New("k8s: exec environments are not supported (run the server with --exec-slots 0)")
	}
	needSlots := spec.Mounts.Workspace != "" || spec.Mounts.GatewaySocket != "" || spec.Mounts.RestoreDir != ""
	if needSlots && !p.slots.enabled() {
		return provider.EnvInfo{}, errors.New("k8s: workspace/Gateway mounts need a shared slot directory (--k8s-shared-dir)")
	}
	ours, o, foreign, err := p.lookup(ctx, spec.EnvID)
	if err != nil {
		return provider.EnvInfo{}, err
	}
	hash := provider.SpecHash(spec)
	switch {
	case foreign:
		return provider.EnvInfo{}, fmt.Errorf("%w: pod labelled for %s", provider.ErrForeign, spec.EnvID)
	case ours != nil && o.SpecHash != hash:
		return provider.EnvInfo{}, provider.ErrConflict
	case ours != nil:
		if st := p.state(spec.EnvID); st != nil && st.pod == ours.Name && st.gate.isOpen() && ready(ours) {
			return provider.EnvInfo{EnvID: spec.EnvID, Kind: spec.Kind, Complete: true, Running: true}, nil
		}
		return provider.EnvInfo{}, provider.ErrIncomplete
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.opt.CreateTimeout)
		defer cancel()
	}
	start := time.Now()
	own := owner{InstallID: p.opt.InstallID, EnvID: spec.EnvID, SpecHash: hash, Kind: spec.Kind}
	pr := profileOf(spec.Limits)
	path := "cold"
	var pod *corev1.Pod
	if p.pool != nil && pr == p.pool.profile {
		pod = p.pool.claim(ctx, own, spec.Mounts.Workspace)
	}
	if pod != nil {
		path = "warm"
	} else {
		if pod, err = p.createPod(ctx, pr, &own, spec.Mounts.Workspace); err != nil {
			return provider.EnvInfo{}, err
		}
	}
	// From here on the Pod carries the owner annotation: any failure leaves residue (ErrIncomplete next time).
	if needSlots {
		if err := p.slots.bind(pod.Name, spec.Mounts); err != nil {
			return provider.EnvInfo{}, err
		}
	}
	if !ready(pod) {
		if pod, err = p.waitPod(ctx, pod.Name, func(po *corev1.Pod) (bool, error) {
			if stopped(po) {
				return false, fmt.Errorf("k8s: pod %s ended before becoming ready (%s)", po.Name, po.Status.Reason)
			}
			return ready(po), nil
		}); err != nil {
			return provider.EnvInfo{}, err
		}
	}
	p.mu.Lock()
	p.envs[spec.EnvID] = &envState{kind: spec.Kind, pod: pod.Name, gate: &gate{}, ws: spec.Mounts.Workspace, limits: spec.Limits}
	p.mu.Unlock()
	d := time.Since(start)
	p.stats.add(path, d)
	p.log.Info("k8s: environment ready", "env_id", spec.EnvID, "pod", pod.Name, "path", path, "start_ms", d.Milliseconds())
	return provider.EnvInfo{EnvID: spec.EnvID, Kind: spec.Kind, Complete: true, Running: true}, nil
}

// createPod creates a Pod (cold start when o is set, a warm Pod when nil).
func (p *Provider) createPod(ctx context.Context, pr profile, o *owner, ws string) (*corev1.Pod, error) {
	name := newPodName()
	nodeDir := ""
	if p.slots.enabled() {
		if err := p.slots.create(name); err != nil {
			return nil, err
		}
		nodeDir = p.slots.nodeDir(name)
	}
	key := poolKey(p.opt.Image, pr, p.opt.RuntimeClass)
	pod := buildPod(podParams{Name: name, Namespace: p.opt.Namespace, InstallID: p.opt.InstallID, Image: p.opt.Image,
		RuntimeClass: p.opt.RuntimeClass, PoolKey: key, Profile: pr, SlotNodeDir: nodeDir, Owner: o, Workspace: ws})
	created, err := p.pods().Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if p.slots.enabled() {
			_ = p.slots.remove(name)
		}
		return nil, fmt.Errorf("k8s: create pod: %w", err)
	}
	return created, nil
}

// waitPod watches pod name until cond is true, cond fails, or ctx ends. A deleted Pod is an error
// wrapping provider.ErrNotFound.
func (p *Provider) waitPod(ctx context.Context, name string, cond func(*corev1.Pod) (bool, error)) (*corev1.Pod, error) {
	for {
		w, werr := p.pods().Watch(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String()})
		pod, err := p.pods().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if w != nil {
				w.Stop()
			}
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("%w: pod %s", provider.ErrNotFound, name)
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("k8s: get pod: %w", err)
		}
		if ok, err := cond(pod); ok || err != nil {
			if w != nil {
				w.Stop()
			}
			return pod, err
		}
		var events <-chan watch.Event
		if werr == nil {
			events = w.ResultChan()
		}
		tick := time.NewTimer(time.Second) // safety net: re-Get even if the watch is silent or broken
	loop:
		for {
			select {
			case <-ctx.Done():
				tick.Stop()
				if w != nil {
					w.Stop()
				}
				return nil, ctx.Err()
			case <-tick.C:
				break loop
			case ev, open := <-events:
				if !open {
					break loop
				}
				po, isPod := ev.Object.(*corev1.Pod)
				if !isPod || po.Name != name {
					continue
				}
				if ev.Type == watch.Deleted {
					tick.Stop()
					w.Stop()
					return nil, fmt.Errorf("%w: pod %s", provider.ErrNotFound, name)
				}
				if ok, err := cond(po); ok || err != nil {
					tick.Stop()
					w.Stop()
					return po, err
				}
			}
		}
		if w != nil {
			w.Stop()
		}
	}
}

// StartExec runs spec in the environment's Pod (contract §3, §5).
func (p *Provider) StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	st := p.state(envID)
	if st == nil {
		return nil, fmt.Errorf("%w: environment %s has no ready pod in this process", provider.ErrNotFound, envID)
	}
	if err := st.gate.enter(); err != nil {
		return nil, err
	}
	defer st.gate.leave()
	if p.beforeSend != nil {
		p.beforeSend(envID)
	}
	if len(spec.Argv) == 0 {
		return nil, &provider.StartError{Reason: "empty argv"}
	}
	return p.startExec(ctx, st.pod, st.limits.NoFile, st.limits.FSize, spec)
}

// Stop closes the gate and kills the Pod's containers by setting activeDeadlineSeconds; success means the
// Pod is gone, terminal, or all its containers have terminated (contract §3: the authoritative check).
func (p *Provider) Stop(ctx context.Context, envID string) error {
	if st := p.state(envID); st != nil {
		st.gate.close()
	}
	ours, _, foreign, err := p.lookup(ctx, envID)
	if err != nil {
		return err
	}
	if ours == nil {
		if foreign {
			return fmt.Errorf("%w: pod labelled for %s", provider.ErrForeign, envID)
		}
		return nil
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.opt.StopTimeout)
		defer cancel()
	}
	if !stopped(ours) && ours.Spec.NodeName == "" {
		// Re-read: the list may be stale and the scheduler may have bound the Pod since.
		if fresh, err := p.pods().Get(ctx, ours.Name, metav1.GetOptions{}); err == nil {
			ours = fresh
		} else if apierrors.IsNotFound(err) {
			return nil
		}
	}
	if !stopped(ours) {
		if ours.Spec.NodeName == "" {
			// Not scheduled: no containers exist yet. Delete it (UID precondition) so it can never start; if the
			// scheduler binds it concurrently, the deletion still removes it before any container stays running.
			if err := p.deletePod(ctx, ours); err != nil {
				return err
			}
		} else {
			if ours.Spec.ActiveDeadlineSeconds == nil || *ours.Spec.ActiveDeadlineSeconds > 1 {
				patch := []byte(`{"spec":{"activeDeadlineSeconds":1}}`)
				if _, err := p.pods().Patch(ctx, ours.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("k8s: stop pod %s: %w", ours.Name, err)
				}
			}
			// Fast path: the kubelet acts on the deadline at its next sync (seconds); asking PID 1 to shut down
			// ends the container at once. Best effort — the deadline is the authoritative kill.
			if ready(ours) {
				go func(name string) {
					sctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
					defer cancel()
					_, _ = p.helper(sctx, name, "shutdown")
				}(ours.Name)
			}
		}
		_, err := p.waitPod(ctx, ours.Name, func(po *corev1.Pod) (bool, error) { return stopped(po), nil })
		if err != nil && !errors.Is(err, provider.ErrNotFound) {
			return fmt.Errorf("%w: pod %s: %v", provider.ErrStopUnconfirmed, ours.Name, err)
		}
	}
	if p.slots.enabled() {
		if err := p.slots.unbindRun(ours.Name); err != nil {
			p.log.Warn("k8s: remove slot links", "pod", ours.Name, "err", err)
		}
	}
	return nil
}

func (p *Provider) deletePod(ctx context.Context, pod *corev1.Pod) error {
	err := p.pods().Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0)),
		Preconditions: &metav1.Preconditions{UID: &pod.UID}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("k8s: delete pod %s: %w", pod.Name, err)
	}
	return nil
}

// Destroy deletes a stopped environment's Pod and slot and verifies the Pod is gone (contract §3).
func (p *Provider) Destroy(ctx context.Context, envID string) error {
	if st := p.state(envID); st != nil {
		st.gate.close()
	}
	ours, _, foreign, err := p.lookup(ctx, envID)
	if err != nil {
		return err
	}
	if foreign && ours == nil {
		return fmt.Errorf("%w: pod labelled for %s", provider.ErrForeign, envID)
	}
	if ours != nil {
		if !stopped(ours) && ours.Spec.NodeName != "" {
			return provider.ErrNotStopped
		}
		if _, has := ctx.Deadline(); !has {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.opt.StopTimeout)
			defer cancel()
		}
		if err := p.deletePod(ctx, ours); err != nil {
			return err
		}
		if _, err := p.waitPod(ctx, ours.Name, func(*corev1.Pod) (bool, error) { return false, nil }); !errors.Is(err, provider.ErrNotFound) {
			return fmt.Errorf("k8s: pod %s still present after delete: %v", ours.Name, err)
		}
		if p.slots.enabled() {
			if err := p.slots.remove(ours.Name); err != nil {
				return fmt.Errorf("k8s: remove slot: %w", err)
			}
		}
	}
	p.mu.Lock()
	delete(p.envs, envID)
	p.mu.Unlock()
	if again, _, _, err := p.lookup(ctx, envID); err != nil {
		return err
	} else if again != nil {
		return fmt.Errorf("k8s: environment %s still has pod %s", envID, again.Name)
	}
	return nil
}

// List reports this installation's bound Pods.
func (p *Provider) List(ctx context.Context) ([]provider.EnvInfo, error) {
	sel := labels.Set{LabelManaged: "true", LabelInstall: labelValue(p.opt.InstallID), LabelState: StateAssigned}.AsSelector().String()
	l, err := p.pods().List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, fmt.Errorf("k8s: list pods: %w", err)
	}
	var out []provider.EnvInfo
	for i := range l.Items {
		pod := &l.Items[i]
		o, ok := parseOwner(pod)
		if !ok || o.InstallID != p.opt.InstallID || pod.Labels[LabelEnv] != labelValue(o.EnvID) {
			continue
		}
		st := p.state(o.EnvID)
		complete := st != nil && st.pod == pod.Name && st.gate.isOpen() && ready(pod)
		out = append(out, provider.EnvInfo{EnvID: o.EnvID, Kind: o.Kind, Complete: complete, Running: running(pod)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EnvID < out[j].EnvID })
	return out, nil
}

// LayerPod is the ScanItem layer of a sandbox Pod.
const LayerPod = "pod"

// Scan reports every managed Pod in the namespace with its ownership. This installation's warm Pods are pool
// capacity, not environments, and are not reported.
func (p *Provider) Scan(ctx context.Context) (provider.ScanReport, error) {
	l, err := p.pods().List(ctx, metav1.ListOptions{LabelSelector: labels.Set{LabelManaged: "true"}.AsSelector().String()})
	if err != nil {
		return provider.ScanReport{}, fmt.Errorf("k8s: list pods: %w", err)
	}
	var r provider.ScanReport
	for i := range l.Items {
		pod := &l.Items[i]
		mine := pod.Labels[LabelInstall] == labelValue(p.opt.InstallID)
		o, ok := parseOwner(pod)
		if mine && !ok && pod.Labels[LabelState] == StateWarm && pod.Labels[LabelEnv] == "" {
			continue
		}
		it := provider.ScanItem{Layer: LayerPod, Path: LayerPod + "/" + p.opt.Namespace + "/" + pod.Name}
		switch {
		case !ok:
			it.Owner = provider.Unknown
			if v := pod.Labels[LabelEnv]; v != "" && v[:min(2, len(v))] != "h-" {
				it.EnvID = v
			}
		case o.InstallID != p.opt.InstallID:
			it.Owner, it.EnvID = provider.Foreign, o.EnvID
		default:
			it.EnvID, it.Owner = o.EnvID, provider.OwnedPartial
			if st := p.state(o.EnvID); st != nil && st.pod == pod.Name && st.gate.isOpen() && ready(pod) {
				it.Owner = provider.OwnedComplete
			}
		}
		r.Items = append(r.Items, it)
	}
	sort.Slice(r.Items, func(i, j int) bool { return r.Items[i].Path < r.Items[j].Path })
	return r, nil
}

// boundPod returns this installation's Pod for envID or ErrNotFound.
func (p *Provider) boundPod(ctx context.Context, envID string) (*corev1.Pod, error) {
	ours, _, _, err := p.lookup(ctx, envID)
	if err != nil {
		return nil, err
	}
	if ours == nil {
		return nil, fmt.Errorf("%w: no pod for %s", provider.ErrNotFound, envID)
	}
	return ours, nil
}

// ResourceDiag reads the container cgroup through the helper while the Pod runs, and the container status
// (OOMKilled) otherwise.
func (p *Provider) ResourceDiag(ctx context.Context, envID string) (provider.ResourceDiag, error) {
	pod, err := p.boundPod(ctx, envID)
	if err != nil {
		return provider.ResourceDiag{}, err
	}
	var d provider.ResourceDiag
	if oomKilled(pod) {
		d.OOMObserved, d.OOMKillDelta = true, 1
	}
	if !ready(pod) {
		return d, nil
	}
	out, err := p.helper(ctx, pod.Name, "diag")
	if err != nil {
		return d, err
	}
	var pd podDiag
	if err := jsonUnmarshal(out, &pd); err != nil {
		return d, fmt.Errorf("k8s: diag output: %w", err)
	}
	d.CPUUsageUsec = pd.CPUUsageUsec
	if pd.OOMKill > d.OOMKillDelta {
		d.OOMKillDelta = pd.OOMKill
	}
	d.OOMObserved = d.OOMObserved || pd.OOMKill > 0
	return d, nil
}

type podDiag = struct {
	CPUUsageUsec uint64 `json:"cpu_usage_usec"`
	OOMKill      uint64 `json:"oom_kill"`
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// OpenOutputs exists for exec environments, which this provider does not create.
func (p *Provider) OpenOutputs(ctx context.Context, envID string, _ int) ([]provider.OutputFile, []provider.SkippedOutput, error) {
	if _, err := p.boundPod(ctx, envID); err != nil {
		return nil, nil, err
	}
	return nil, nil, fmt.Errorf("k8s: %s is not an exec environment (exec environments are not supported)", envID)
}

// Freeze stops every process in the Pod with SIGSTOP and confirms it (cgroup.freeze is not writable from an
// unprivileged container). On failure it thaws and returns ErrFreezeUnconfirmed.
func (p *Provider) Freeze(ctx context.Context, envID string) error {
	pod, err := p.runningPod(ctx, envID)
	if err != nil {
		return err
	}
	timeout := 10 * time.Second
	if dl, has := ctx.Deadline(); has {
		timeout = time.Until(dl)
	}
	if _, err := p.helper(ctx, pod.Name, "freeze", "--timeout-ms", fmt.Sprint(max(timeout.Milliseconds()-500, 100))); err != nil {
		tctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = p.helper(tctx, pod.Name, "thaw")
		return fmt.Errorf("%w: %v", provider.ErrFreezeUnconfirmed, err)
	}
	return nil
}

// Thaw resumes the Pod's processes with SIGCONT.
func (p *Provider) Thaw(ctx context.Context, envID string) error {
	pod, err := p.runningPod(ctx, envID)
	if err != nil {
		return err
	}
	_, err = p.helper(ctx, pod.Name, "thaw")
	return err
}

// Procs returns the pids of the Pod's processes (container PID namespace, PID 1 excluded).
func (p *Provider) Procs(ctx context.Context, envID string) ([]int, error) {
	pod, err := p.runningPod(ctx, envID)
	if err != nil {
		return nil, err
	}
	out, err := p.helper(ctx, pod.Name, "procs")
	if err != nil {
		return nil, err
	}
	var pids []int
	if err := jsonUnmarshal(out, &pids); err != nil {
		return nil, fmt.Errorf("k8s: procs output: %w", err)
	}
	return pids, nil
}

func (p *Provider) runningPod(ctx context.Context, envID string) (*corev1.Pod, error) {
	pod, err := p.boundPod(ctx, envID)
	if err != nil {
		return nil, err
	}
	if !ready(pod) {
		return nil, fmt.Errorf("%w: pod %s is not running", provider.ErrNotFound, pod.Name)
	}
	return pod, nil
}

// ReclaimUIDFiles: sandbox Pods run as one fixed UID outside the control plane's UID ranges, so no file
// in the workspaces belongs to a range.
func (p *Provider) ReclaimUIDFiles(context.Context, uint32, uint32) (int, error) { return 0, nil }

// UIDFiles: see ReclaimUIDFiles.
func (p *Provider) UIDFiles(context.Context, uint32, uint32, int) ([]string, error) { return nil, nil }
