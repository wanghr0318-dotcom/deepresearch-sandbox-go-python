package k8s

import (
	"context"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// pool keeps `size` warm Pods (pre-started, not bound to an environment) for one pool key (design §2.6).
type pool struct {
	p       *Provider
	size    int
	profile profile
	key     string
	kick    chan struct{}

	mu       sync.Mutex
	creating int
}

// poolInterval is how often the pool reconciles without being kicked (dead warm Pods, foreign keys).
var poolInterval = 2 * time.Second

func (pl *pool) run(ctx context.Context) {
	t := time.NewTicker(poolInterval)
	defer t.Stop()
	for {
		pl.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-pl.kick:
		case <-t.C:
		}
	}
}

func (pl *pool) poke() {
	select {
	case pl.kick <- struct{}{}:
	default:
	}
}

// warmPods lists this installation's unbound Pods (all pool keys).
func (pl *pool) warmPods(ctx context.Context) ([]corev1.Pod, error) {
	sel := labels.Set{LabelManaged: "true", LabelInstall: labelValue(pl.p.opt.InstallID), LabelState: StateWarm}.AsSelector().String()
	l, err := pl.p.pods().List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, err
	}
	return l.Items, nil
}

// reconcile deletes warm Pods of other pool keys and dead warm Pods, then creates Pods up to size.
func (pl *pool) reconcile(ctx context.Context) {
	pods, err := pl.warmPods(ctx)
	if err != nil {
		if ctx.Err() == nil {
			pl.p.log.Warn("k8s: warm pool list", "err", err)
		}
		return
	}
	alive := 0
	for i := range pods {
		pod := &pods[i]
		if pod.Labels[LabelPool] != pl.key || stopped(pod) || pod.DeletionTimestamp != nil {
			if pod.DeletionTimestamp == nil {
				if err := pl.p.deletePod(ctx, pod); err == nil && pl.p.slots.enabled() {
					_ = pl.p.slots.remove(pod.Name, "")
				}
			}
			continue
		}
		alive++
	}
	pl.mu.Lock()
	missing := pl.size - alive - pl.creating
	if missing > 0 {
		pl.creating += missing
	}
	pl.mu.Unlock()
	for i := 0; i < missing; i++ {
		_, err := pl.p.createPod(ctx, pl.profile, nil, "")
		pl.mu.Lock()
		pl.creating--
		pl.mu.Unlock()
		if err != nil {
			if ctx.Err() == nil {
				pl.p.log.Warn("k8s: warm pool create", "err", err)
			}
			return
		}
	}
}

// claim binds a ready warm Pod to the environment: the owner annotation, env label and state are written
// with the listed resourceVersion as precondition, so concurrent claimers never share a Pod. Returns nil if
// no warm Pod could be claimed (the caller starts a cold Pod).
func (pl *pool) claim(ctx context.Context, o owner, ws string) *corev1.Pod {
	defer pl.poke()
	pods, err := pl.warmPods(ctx)
	if err != nil {
		return nil
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp) })
	for i := range pods {
		pod := pods[i].DeepCopy()
		if pod.Labels[LabelPool] != pl.key || !ready(pod) {
			continue
		}
		pod.Labels[LabelState] = StateAssigned
		pod.Labels[LabelEnv] = labelValue(o.EnvID)
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[AnnoOwner] = o.encode()
		if ws != "" {
			pod.Annotations[AnnoWorkspace] = ws
		}
		updated, err := pl.p.pods().Update(ctx, pod, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil
		}
		return updated
	}
	return nil
}
