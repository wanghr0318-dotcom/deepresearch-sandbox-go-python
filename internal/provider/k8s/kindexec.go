package k8s

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

type containerExecutor interface {
	StreamContainer(ctx context.Context, namespace, pod, container string, cmd []string, stdin io.Reader, stdout, stderr io.Writer) error
}

type execWorkspace struct {
	dir, input string
	ownedInput bool
	outBytes   int64
	stage      sync.Once
	stageErr   error
}

func (w *execWorkspace) inDir() string {
	if w.ownedInput {
		return w.input
	}
	return ""
}

func (st *envState) info(envID string, complete, running bool) provider.EnvInfo {
	info := provider.EnvInfo{EnvID: envID, Kind: st.kind, Complete: complete, Running: running}
	if st.workspace != nil {
		info.InDir = st.workspace.inDir()
	}
	return info
}

func (p *Provider) createBuiltPod(ctx context.Context, params podParams) (*corev1.Pod, error) {
	params.Namespace, params.InstallID, params.Image = p.opt.Namespace, p.opt.InstallID, p.opt.Image
	params.RuntimeClass = p.opt.RuntimeClass
	candidate := buildPod(params)
	if name := p.opt.PreferredNode; name != "" {
		candidate.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
				Weight: 100, Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{name},
					}},
				},
			}},
		}}
	}
	pod, err := p.pods().Create(ctx, candidate, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8s: create pod: %w", err)
	}
	return pod, nil
}

func (p *Provider) createExecPod(ctx context.Context, pr profile, own *owner, mounts provider.Mounts) (*corev1.Pod, *execWorkspace, error) {
	name := newPodName()
	dir := filepath.Join(p.opt.ExecStagingDir, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, nil, err
	}
	w := &execWorkspace{dir: dir, input: mounts.In, outBytes: mounts.OutBytes}
	if w.input == "" {
		w.input = filepath.Join(dir, "in")
		w.ownedInput = true
		if err := os.Mkdir(w.input, 0o755); err != nil {
			return nil, nil, errors.Join(err, p.removeExecWorkspace(name))
		}
	}
	deadlineSeconds := int64(p.opt.ExecPodLifetime / time.Second)
	if p.opt.ExecPodLifetime%time.Second != 0 {
		deadlineSeconds++
	}
	pod, err := p.createBuiltPod(ctx, podParams{
		Name: name, Profile: pr, Owner: own,
		ExecOutBytes: mounts.OutBytes, ExecDeadlineSeconds: deadlineSeconds,
	})
	if err != nil {
		return nil, nil, errors.Join(err, p.removeExecWorkspace(name))
	}
	return pod, w, nil
}

func (p *Provider) removeExecWorkspace(pod string) error {
	if p.opt.ExecStagingDir == "" {
		return nil
	}
	return os.RemoveAll(filepath.Join(p.opt.ExecStagingDir, pod))
}

func (p *Provider) stageExecInput(ctx context.Context, st *envState) error {
	stream, write := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := writeInputArchive(write, st.workspace.input)
		_ = write.CloseWithError(err)
		done <- err
	}()
	var stderr bytes.Buffer
	err := p.transfer.StreamContainer(ctx, p.opt.Namespace, st.pod, transferContainerName,
		[]string{podapi.Binary, "stage-input"}, stream, io.Discard, &stderr)
	_ = stream.Close()
	writeErr := <-done
	if err != nil {
		return fmt.Errorf("k8s: stage input in pod %s: %w (%s)", st.pod, err, bytes.TrimSpace(stderr.Bytes()))
	}
	return writeErr
}

func writeInputArchive(dst io.Writer, dir string) error {
	src := os.DirFS(dir)
	w := tar.NewWriter(dst)
	err := fs.WalkDir(src, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("k8s: input %q is not a regular file", name)
		}
		f, err := src.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := w.WriteHeader(&tar.Header{Name: name, Size: info.Size(), Mode: int64(info.Mode().Perm())}); err != nil {
			return err
		}
		_, err = io.CopyN(w, f, info.Size())
		return err
	})
	if err != nil {
		return err
	}
	return w.Close()
}

// environmentStopped excludes the read-only transfer container from the exec
// workload. Only Kubernetes can confirm that the sandbox actually terminated.
func environmentStopped(pod *corev1.Pod, kind provider.EnvKind) bool {
	if kind == provider.KindExec {
		for _, c := range pod.Status.ContainerStatuses {
			if c.Name == containerName && c.State.Terminated != nil {
				return true
			}
		}
	}
	return stopped(pod)
}

func (p *Provider) stopExecSandbox(ctx context.Context, pod *corev1.Pod) error {
	if environmentStopped(pod, provider.KindExec) {
		return nil
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, _ = p.helper(sctx, pod.Name, "shutdown")
	cancel()
	_, err := p.waitPod(ctx, pod.Name, func(current *corev1.Pod) (bool, error) {
		return environmentStopped(current, provider.KindExec), nil
	})
	if err == nil || errors.Is(err, provider.ErrNotFound) {
		return nil
	}
	// Stop failed to preserve the transfer container. A deadline requests
	// kubelet cleanup, but this attempt must not publish any output.
	reclaimCtx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	err = errors.Join(err, p.expirePod(reclaimCtx, pod))
	return fmt.Errorf("%w: pod %s: %v", provider.ErrStopUnconfirmed, pod.Name, err)
}

func (p *Provider) OpenOutputs(ctx context.Context, envID string, maxFiles int) ([]provider.OutputFile, []provider.SkippedOutput, error) {
	pod, err := p.boundPod(ctx, envID)
	if err != nil {
		return nil, nil, err
	}
	own, _ := parseOwner(pod)
	if own.Kind != provider.KindExec {
		return nil, nil, fmt.Errorf("k8s: %s is not an exec environment", envID)
	}
	if !environmentStopped(pod, own.Kind) {
		return nil, nil, provider.ErrNotStopped
	}
	st := p.state(envID)
	if st == nil || st.pod != pod.Name || st.workspace == nil {
		return nil, nil, provider.ErrIncomplete
	}
	if maxFiles < 0 {
		return nil, nil, fmt.Errorf("k8s: negative output file limit: %d", maxFiles)
	}
	archive, err := os.CreateTemp(st.workspace.dir, "transfer-")
	if err != nil {
		return nil, nil, err
	}
	defer os.Remove(archive.Name())
	defer archive.Close()
	args := []string{podapi.Binary, "collect-output", "--max", strconv.Itoa(maxFiles), "--bytes", strconv.FormatInt(st.workspace.outBytes, 10)}
	var stderr bytes.Buffer
	// Account for tar headers and skipped entries separately from the payload.
	wire := &outputArchiveWriter{dst: archive, remaining: st.workspace.outBytes + (8 << 20)}
	streamErr := p.transfer.StreamContainer(ctx, p.opt.Namespace, pod.Name, transferContainerName, args, nil, wire, &stderr)
	// client-go may drain a stream after a local stdout write failure.
	if err := errors.Join(streamErr, wire.err); err != nil {
		return nil, nil, fmt.Errorf("k8s: collect output in pod %s: %w (%s)", pod.Name, err, bytes.TrimSpace(stderr.Bytes()))
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	return unpackOutputs(archive, st.workspace.dir)
}

type outputArchiveWriter struct {
	dst       io.Writer
	remaining int64
	err       error
}

func (w *outputArchiveWriter) Write(chunk []byte) (int, error) {
	if int64(len(chunk)) > w.remaining {
		w.err = errors.New("k8s: output archive exceeds transfer limit")
		return 0, w.err
	}
	n, err := w.dst.Write(chunk)
	if err != nil {
		w.err = err
	}
	w.remaining -= int64(n)
	return n, err
}

func unpackOutputs(src io.Reader, dir string) (files []provider.OutputFile, skipped []provider.SkippedOutput, err error) {
	defer func() {
		if err != nil {
			for _, f := range files {
				_ = f.File.Close()
				_ = os.Remove(f.File.Name())
			}
			files, skipped = nil, nil
		}
	}()
	r := tar.NewReader(src)
	for {
		header, readErr := r.Next()
		if errors.Is(readErr, io.EOF) {
			return files, skipped, nil
		}
		if readErr != nil {
			return files, skipped, readErr
		}
		if reason := header.PAXRecords["AGENTBOX.skip"]; reason != "" {
			skipped = append(skipped, provider.SkippedOutput{Path: header.Name, Reason: reason})
			continue
		}
		f, err := storeOutputFile(dir, r, header)
		if err != nil {
			return files, skipped, err
		}
		files = append(files, provider.OutputFile{Path: header.Name, Size: header.Size, File: f})
	}
}

func storeOutputFile(dir string, src io.Reader, header *tar.Header) (*os.File, error) {
	f, err := os.CreateTemp(dir, "output-")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	_, copyErr := io.CopyN(f, src, header.Size)
	if copyErr == nil && header.Mode&0o111 != 0 {
		copyErr = f.Chmod(0o700)
	}
	if err := errors.Join(copyErr, f.Close()); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	opened, err := os.Open(path)
	if err != nil {
		_ = os.Remove(path)
	}
	return opened, err
}
