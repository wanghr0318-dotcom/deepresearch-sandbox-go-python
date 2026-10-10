package k8s

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// Connect returns a clientset and an Executor for the cluster in kubeconfig (in-cluster config when empty).
func Connect(kubeconfig string) (kubernetes.Interface, Executor, error) {
	var (
		cfg *rest.Config
		err error
	)
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("k8s: cluster config: %w", err)
	}
	cfg.QPS, cfg.Burst = 50, 100
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("k8s: clientset: %w", err)
	}
	return cs, &RemoteExecutor{Config: cfg, Client: cs}, nil
}

// RemoteExecutor runs commands through the pods/exec subresource (WebSocket, falling back to SPDY), like
// kubectl exec.
type RemoteExecutor struct {
	Config *rest.Config
	Client kubernetes.Interface
}

// Stream implements Executor.
func (e *RemoteExecutor) Stream(ctx context.Context, namespace, pod string, cmd []string, stdin io.Reader, stdout, stderr io.Writer) error {
	req := e.Client.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: containerName, Command: cmd, Stdin: stdin != nil, Stdout: true, Stderr: true},
			scheme.ParameterCodec)
	spdy, err := remotecommand.NewSPDYExecutor(e.Config, "POST", req.URL())
	if err != nil {
		return err
	}
	ws, err := remotecommand.NewWebSocketExecutor(e.Config, "GET", req.URL().String())
	if err != nil {
		return err
	}
	ex, err := remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return err
	}
	return ex.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: stdout, Stderr: stderr})
}
