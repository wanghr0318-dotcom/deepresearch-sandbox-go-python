package k8s

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

// fakeKubelet plays scheduler and kubelet for a fake clientset: it starts Pods (Running + Ready after
// startDelay) and kills Pods whose activeDeadlineSeconds is set (phase Failed), notifying the fake executor.
type fakeKubelet struct {
	client     kubernetes.Interface
	ns         string
	startDelay time.Duration
	exec       *fakeExec

	mu      sync.Mutex
	seen    map[string]time.Time
	started int
}

func startFakeKubelet(t *testing.T, client kubernetes.Interface, ns string, fe *fakeExec, startDelay time.Duration) *fakeKubelet {
	k := &fakeKubelet{client: client, ns: ns, startDelay: startDelay, exec: fe, seen: map[string]time.Time{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			k.sync(ctx)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	return k
}

func (k *fakeKubelet) patch(ctx context.Context, name string, v any) {
	b, _ := json.Marshal(v)
	_, _ = k.client.CoreV1().Pods(k.ns).Patch(ctx, name, types.MergePatchType, b, metav1.PatchOptions{})
}

func (k *fakeKubelet) sync(ctx context.Context) {
	l, err := k.client.CoreV1().Pods(k.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	now := time.Now()
	for i := range l.Items {
		pod := &l.Items[i]
		k.mu.Lock()
		first, ok := k.seen[pod.Name]
		if !ok {
			k.seen[pod.Name], first = now, now
		}
		k.mu.Unlock()
		switch {
		case terminal(pod):
		case pod.Spec.ActiveDeadlineSeconds != nil && pod.Spec.NodeName != "":
			k.exec.killPod(pod.Name)
			k.patch(ctx, pod.Name, map[string]any{"status": map[string]any{
				"phase": corev1.PodFailed, "reason": "DeadlineExceeded",
				"conditions": []map[string]any{{"type": "Ready", "status": "False"}},
				"containerStatuses": []map[string]any{{"name": containerName, "state": map[string]any{
					"terminated": map[string]any{"exitCode": 137, "reason": "Error"}}}},
			}})
		case pod.Spec.NodeName == "":
			k.patch(ctx, pod.Name, map[string]any{"spec": map[string]any{"nodeName": "fake-node"},
				"status": map[string]any{"phase": corev1.PodPending}})
		case pod.Status.Phase == corev1.PodPending && now.Sub(first) >= k.startDelay:
			k.mu.Lock()
			k.started++
			k.mu.Unlock()
			k.patch(ctx, pod.Name, map[string]any{"status": map[string]any{
				"phase":             corev1.PodRunning,
				"startTime":         metav1.NewTime(now),
				"conditions":        []map[string]any{{"type": "Ready", "status": "True"}},
				"containerStatuses": []map[string]any{{"name": containerName, "ready": true, "state": map[string]any{"running": map[string]any{}}}},
			}})
		}
	}
}

// fakeExec emulates agentbox-podagent inside fake Pods. Programs (argv after "--"):
//
//	echo <text> <code>   print text, exit with code
//	sleep                run until killed
//	cat                  copy stdin to stdout
type fakeExec struct {
	client kubernetes.Interface
	ns     string

	mu     sync.Mutex
	procs  map[string]map[string]*fakeProc // pod → exec id → process
	status map[string]podapi.Status        // pod/id → status
	frozen map[string]bool
	nextID int
}

type fakeProc struct {
	pid  int
	stop chan int // receives the terminating signal
}

type exitErr int

func (e exitErr) Error() string   { return "command terminated with exit code " + strconv.Itoa(int(e)) }
func (e exitErr) ExitStatus() int { return int(e) }

var errStreamBroken = errors.New("fake: container terminated, stream closed")

func newFakeExec(client kubernetes.Interface, ns string) *fakeExec {
	return &fakeExec{client: client, ns: ns, procs: map[string]map[string]*fakeProc{},
		status: map[string]podapi.Status{}, frozen: map[string]bool{}, nextID: 100}
}

func (f *fakeExec) killPod(pod string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.procs[pod] {
		select {
		case p.stop <- -1:
		default:
		}
	}
	delete(f.procs, pod)
}

func (f *fakeExec) Stream(ctx context.Context, ns, pod string, cmd []string, stdin io.Reader, stdout, stderr io.Writer) error {
	po, err := f.client.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || (err == nil && !ready(po)) {
		return fmt.Errorf("fake: container not running in pod %s", pod)
	} else if err != nil {
		return err
	}
	if len(cmd) < 2 || cmd[0] != podapi.Binary {
		return fmt.Errorf("fake: unexpected command %q", cmd)
	}
	fs := flag.NewFlagSet(cmd[1], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("id", "", "")
	fs.String("dir", "", "")
	fs.Uint64("nofile", 0, "")
	fs.Uint64("fsize", 0, "")
	fs.Int("grace-ms", 0, "")
	fs.Int("timeout-ms", 0, "")
	var envs listFlag
	fs.Var(&envs, "env", "")
	if err := fs.Parse(cmd[2:]); err != nil {
		return err
	}
	switch cmd[1] {
	case "exec":
		return f.run(ctx, pod, *id, fs.Args(), stdin, stdout)
	case "kill":
		f.mu.Lock()
		p := f.procs[pod][*id]
		f.mu.Unlock()
		if p != nil {
			select {
			case p.stop <- 15:
			default:
			}
		}
		return nil
	case "status":
		f.mu.Lock()
		st, ok := f.status[pod+"/"+*id]
		f.mu.Unlock()
		if !ok {
			return exitErr(1)
		}
		return json.NewEncoder(stdout).Encode(st)
	case "freeze", "thaw":
		f.mu.Lock()
		f.frozen[pod] = cmd[1] == "freeze"
		f.mu.Unlock()
		return nil
	case "procs":
		f.mu.Lock()
		pids := []int{}
		for _, p := range f.procs[pod] {
			pids = append(pids, p.pid)
		}
		f.mu.Unlock()
		return json.NewEncoder(stdout).Encode(pids)
	case "diag":
		return json.NewEncoder(stdout).Encode(podapi.Diag{CPUUsageUsec: 4242})
	}
	return fmt.Errorf("fake: unknown subcommand %q", cmd[1])
}

func (f *fakeExec) run(ctx context.Context, pod, id string, argv []string, stdin io.Reader, stdout io.Writer) error {
	if len(argv) == 0 || (argv[0] != "echo" && argv[0] != "sleep" && argv[0] != "cat") {
		fmt.Fprintf(stdout, "%sfake: %q not found\n", podapi.AckStartErr, argv)
		return exitErr(podapi.StartErrCode)
	}
	f.mu.Lock()
	f.nextID++
	p := &fakeProc{pid: f.nextID, stop: make(chan int, 1)}
	if f.procs[pod] == nil {
		f.procs[pod] = map[string]*fakeProc{}
	}
	f.procs[pod][id] = p
	f.mu.Unlock()
	finish := func(st podapi.Status) error {
		f.mu.Lock()
		delete(f.procs[pod], id)
		f.status[pod+"/"+id] = st
		f.mu.Unlock()
		if st.Signal != 0 {
			return exitErr(128 + st.Signal)
		}
		if st.Code != 0 {
			return exitErr(st.Code)
		}
		return nil
	}
	fmt.Fprintf(stdout, "%s%d\n", podapi.AckStarted, p.pid)
	switch argv[0] {
	case "echo":
		code, _ := strconv.Atoi(argv[2])
		fmt.Fprintln(stdout, argv[1])
		return finish(podapi.Status{Code: code})
	case "cat":
		_, _ = io.Copy(stdout, stdin)
		return finish(podapi.Status{})
	}
	select {
	case sig := <-p.stop:
		if sig < 0 {
			return errStreamBroken
		}
		return finish(podapi.Status{Signal: sig})
	case <-ctx.Done():
		return ctx.Err()
	}
}

// listFlag mirrors the helper's repeatable flag.
type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

var _ = bytes.NewBuffer
