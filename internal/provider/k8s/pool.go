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
	// stuckStreak counts consecutive stuck replacements (reset when a warm Pod becomes Ready); while it is
	// positive, creation waits until holdUntil (exponential backoff), so a permanently failing image or an
	// unschedulable profile does not churn Pods every WarmReadyTimeout.
	stuckStreak int
	holdUntil   time.Time
	stuckTotal  int

	// beforeClaim is a test hook: called with the Pod name just before the claiming Update.
	beforeClaim func(pod string)
}

// poolInterval is how often the pool reconciles without being kicked (dead warm Pods, foreign keys).
var poolInterval = 2 * time.Second

// warmBackoffBase and warmBackoffMax bound the creation backoff after stuck warm Pods.
var (
	warmBackoffBase = 10 * time.Second
	warmBackoffMax  = 5 * time.Minute
)

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
		if pod.DeletionTimestamp != nil {
			continue
		}
		stuck := !ready(pod) && !pod.CreationTimestamp.IsZero() && time.Since(pod.CreationTimestamp.Time) > pl.p.opt.WarmReadyTimeout
		if pod.Labels[LabelPool] == pl.key && !stopped(pod) && !stuck {
			alive++
			if ready(pod) {
				pl.mu.Lock()
				pl.stuckStreak = 0
				pl.mu.Unlock()
			}
			continue
		}
		if stuck {
			pl.mu.Lock()
			pl.stuckStreak++
			pl.stuckTotal++
			wait := warmBackoffBase << min(pl.stuckStreak-1, 20)
			if wait > warmBackoffMax || wait <= 0 {
				wait = warmBackoffMax
			}
			pl.holdUntil = time.Now().Add(wait)
			streak := pl.stuckStreak
			pl.mu.Unlock()
			pl.p.log.Warn("k8s: warm pod not ready, replacing", "pod", pod.Name, "phase", pod.Status.Phase,
				"reason", waitingReason(pod), "stuck_streak", streak, "next_create_in", wait.String())
		}
		// Other pool key (image or profile changed), dead, or stuck: replace it.
		if err := pl.p.deletePod(ctx, pod); err == nil && pl.p.slots.enabled() {
			_ = pl.p.slots.remove(pod.Name)
		}
	}
	pl.mu.Lock()
	missing := pl.size - alive - pl.creating
	if time.Now().Before(pl.holdUntil) {
		missing = 0 // backing off after stuck warm Pods
	}
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

// waitingReason is the first container's waiting reason (e.g. ImagePullBackOff), for logs.
func waitingReason(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			return cs.State.Waiting.Reason
		}
	}
	return pod.Status.Reason
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
	// Oldest first; ties (timestamps have second resolution) broken by name so every claimer sees one order.
	sort.Slice(pods, func(i, j int) bool {
		if !pods[i].CreationTimestamp.Equal(&pods[j].CreationTimestamp) {
			return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
		}
		return pods[i].Name < pods[j].Name
	})
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
			pod.Annotations[AnnoWorkspace] = pathHash(ws)
		}
		if pl.beforeClaim != nil {
			pl.beforeClaim(pod.Name)
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
