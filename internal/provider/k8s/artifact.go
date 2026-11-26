package k8s

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

// OpenPodArtifact streams a live workspace artifact from the assigned sandbox.
// The caller closes the reader and verifies the artifact's size and digest.
func (p *Provider) OpenPodArtifact(ctx context.Context, envID, outDir, rel string, limit int64) (io.ReadCloser, error) {
	base, ok := strings.CutPrefix(outDir, "/workspace/out/")
	if !ok || base == "" || strings.Contains(base, "/") || base == "." || base == ".." {
		return nil, fmt.Errorf("unexpected Pod output directory %q", outDir)
	}
	pod, err := p.runningPod(ctx, envID)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()
	go func() {
		err := p.exec.Stream(ctx, p.opt.Namespace, pod.Name,
			[]string{podapi.Binary, "read-artifact", path.Join(base, rel), strconv.FormatInt(limit, 10)},
			nil, writer, io.Discard)
		_ = writer.CloseWithError(err)
	}()
	return reader, nil
}
