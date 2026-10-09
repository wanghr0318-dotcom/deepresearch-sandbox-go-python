package k8s

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

// Executor runs a command in a Pod's sandbox container through the pods/exec API. Stream returns nil when
// the command exited 0, an error implementing ExitStatus() int for a non-zero exit, and any other error
// when the stream failed (the exit status is then unknown).
type Executor interface {
	Stream(ctx context.Context, namespace, pod string, cmd []string, stdin io.Reader, stdout, stderr io.Writer) error
}

// exitCoder matches client-go's k8s.io/client-go/util/exec.ExitError.
type exitCoder interface {
	error
	ExitStatus() int
}

// gate is an environment's execution gate (contract §5): open → closed, irreversible. StartExec registers
// an in-flight start under the lock; Stop closes the gate and does not wait for in-flight starts (the Pod
// is killed, they end with ErrControlLost or a StartError).
type gate struct {
	mu       sync.Mutex
	closed   bool
	inflight int
}

func (g *gate) enter() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return provider.ErrStopping
	}
	g.inflight++
	return nil
}

func (g *gate) leave() {
	g.mu.Lock()
	g.inflight--
	g.mu.Unlock()
}

func (g *gate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

func (g *gate) isOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.closed
}

// ackWriter receives the helper's stdout: it holds back the first line (the acknowledgement), reports it
// once, and forwards everything after it to out.
type ackWriter struct {
	out   io.Writer
	buf   bytes.Buffer
	acked bool
	ack   chan string // receives the first line once
}

const maxAckLine = 4096

func (w *ackWriter) Write(p []byte) (int, error) {
	if w.acked {
		return w.out.Write(p)
	}
	n := len(p)
	w.buf.Write(p)
	i := bytes.IndexByte(w.buf.Bytes(), '\n')
	if i < 0 {
		if w.buf.Len() > maxAckLine {
			return 0, errors.New("k8s: acknowledgement line too long")
		}
		return n, nil
	}
	line := string(w.buf.Bytes()[:i])
	rest := append([]byte(nil), w.buf.Bytes()[i+1:]...)
	w.acked = true
	w.ack <- line
	if len(rest) > 0 {
		if _, err := w.out.Write(rest); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// execHandle implements provider.ExecHandle for one `podagent exec` stream.
type execHandle struct {
	p      *Provider
	pod    string
	execID string

	stdinW *io.PipeWriter
	stdout *io.PipeReader
	stderr *io.PipeReader

	done   chan struct{} // closed when the stream ended; status/err are then set
	status provider.ExitStatus
	err    error
}

func (h *execHandle) Stdin() io.WriteCloser { return h.stdinW }
func (h *execHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *execHandle) Stderr() io.ReadCloser { return h.stderr }
func (h *execHandle) Wait() (provider.ExitStatus, error) {
	<-h.done
	return h.status, h.err
}

// Terminate asks the helper to SIGTERM the workload's process group and SIGKILL it after grace.
func (h *execHandle) Terminate(grace time.Duration) error {
	select {
	case <-h.done:
		return nil
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), grace+15*time.Second)
	defer cancel()
	_, err := h.p.helper(ctx, h.pod, "kill", "--id", h.execID, "--grace-ms", fmt.Sprint(grace.Milliseconds()))
	select {
	case <-h.done:
		return nil // the exec ended anyway (e.g. the Pod was stopped)
	default:
	}
	return err
}

// startExec runs the helper's exec in pod and returns once the workload is acknowledged.
func (p *Provider) startExec(ctx context.Context, pod string, nofile, fsize uint64, spec provider.ExecSpec) (provider.ExecHandle, error) {
	stdinR, stdinW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	h := &execHandle{p: p, pod: pod, execID: spec.ExecID, stdinW: stdinW, stdout: outR, stderr: errR, done: make(chan struct{})}
	aw := &ackWriter{out: outW, ack: make(chan string, 1)}
	cmd := podapi.ExecArgv(spec.ExecID, spec.Env, spec.Dir, nofile, fsize, spec.Argv)
	go func() {
		// The stream outlives StartExec's ctx (that only bounds the wait for the acknowledgement); it ends when
		// the workload exits, the Pod is stopped, or the provider is closed.
		err := p.exec.Stream(p.ctx, p.opt.Namespace, pod, cmd, stdinR, aw, errW)
		h.status, h.err = p.exitStatus(pod, spec.ExecID, err)
		_ = stdinW.Close() // remotecommand's stdin copy then sees EOF instead of a closed-pipe error
		outW.Close()
		errW.Close()
		close(h.done)
	}()
	select {
	case line := <-aw.ack:
		started, _, reason, err := podapi.ParseAck(line)
		switch {
		case err != nil:
			return nil, fmt.Errorf("%w: %v", provider.ErrControlLost, err)
		case !started:
			return nil, &provider.StartError{Reason: reason}
		}
		return h, nil
	case <-h.done:
		if h.err == nil {
			h.err = errors.New("stream ended before the acknowledgement")
		}
		return nil, fmt.Errorf("%w: %v", provider.ErrControlLost, h.err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// exitStatus converts the stream result. Exit codes ≥ 128 are ambiguous ("killed by signal N" or "exited
// 128+N"); the helper's status file decides, and if it cannot be read (Pod gone) the code is taken as a
// signal.
func (p *Provider) exitStatus(pod, execID string, err error) (provider.ExitStatus, error) {
	if err == nil {
		return provider.ExitStatus{}, nil
	}
	var ec exitCoder
	if !errors.As(err, &ec) {
		return provider.ExitStatus{}, fmt.Errorf("%w: %v", provider.ErrControlLost, err)
	}
	code := ec.ExitStatus()
	if code < 128 {
		return provider.ExitStatus{Code: code}, nil
	}
	ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
	defer cancel()
	if out, serr := p.helper(ctx, pod, "status", "--id", execID); serr == nil {
		var st podapi.Status
		if jerr := jsonUnmarshal(out, &st); jerr == nil {
			return provider.ExitStatus{Code: st.Code, Signal: syscall.Signal(st.Signal)}, nil
		}
	}
	if code > 128 && code < 128+65 {
		return provider.ExitStatus{Signal: syscall.Signal(code - 128)}, nil
	}
	return provider.ExitStatus{Code: code}, nil
}

// helper runs a short helper subcommand and returns its stdout.
func (p *Provider) helper(ctx context.Context, pod string, args ...string) ([]byte, error) {
	var out, errb bytes.Buffer
	cmd := append([]string{podapi.Binary}, args...)
	if err := p.exec.Stream(ctx, p.opt.Namespace, pod, cmd, nil, &out, &errb); err != nil {
		return nil, fmt.Errorf("k8s: %s in pod %s: %w (%s)", args[0], pod, err, bytes.TrimSpace(errb.Bytes()))
	}
	return out.Bytes(), nil
}
