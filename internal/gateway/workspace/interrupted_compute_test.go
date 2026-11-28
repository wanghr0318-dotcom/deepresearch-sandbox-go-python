//go:build linux

package workspace

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// The pipe synchronizes failure injection after a verified partial sum. Only
// private files are involved; retrying arbitrary side effects would be unsafe.
const sumCommand = `set -eu
n=$(<n)
s=0
for ((i=1; i<=n; i++)); do
  ((s+=i*i))
  if ((i==n/2)); then
    printf '%s %s %s\n' "$BASHPID" "$i" "$s"
    read -r ack
  fi
done
printf '%s\n' "$s" >answer
`

type sumProcess struct {
	h         *harness
	t         *testing.T
	interrupt bool
	pid       int
	calls     int
}

func sumSquares(n int64) int64 { return n * (n + 1) * (2*n + 1) / 6 }

func (p *sumProcess) ExecShell(parent context.Context, in call.ShellInvoke) (call.Result, error) {
	t := p.t
	t.Helper()
	p.calls++
	if len(in.Files) != 1 || in.Files[0].Path != "n" || in.Command != sumCommand {
		t.Fatal("unexpected input to test executor")
	}
	if p.h.blobs.m[in.Files[0].SHA256] == nil {
		t.Fatal("input blob missing")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "n"), p.h.blobs.m[in.Files[0].SHA256], 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, "bash", "--noprofile", "--norc", "-c", in.Command)
	c.Dir = dir
	output, err := c.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	input, err := c.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if c.ProcessState == nil {
			_ = c.Process.Kill()
			_ = c.Wait()
		}
	}()
	p.pid = c.Process.Pid
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	var i, partial int64
	if _, err := fmt.Sscan(line, &pid, &i, &partial); err != nil || pid != p.pid || i != 15000 || partial != sumSquares(i) {
		t.Fatalf("unexpected real process marker %q", line)
	}
	if _, err := os.Stat(filepath.Join(dir, "answer")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("premature final answer", err)
	}
	t.Logf("attempt=%s pid=%d work_completed=%d partial=%d", in.AttemptID, pid, i, partial)
	if p.interrupt {
		if err := c.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		err := c.Wait()
		var ex *exec.ExitError
		if !errors.As(err, &ex) || ex.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
			t.Fatalf("unexpected termination: %v", err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, "answer")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatal("terminated child published answer", statErr)
		}
		return call.Result{}, err
	}
	if _, err := fmt.Fprintln(input, "continue"); err != nil {
		t.Fatal(err)
	}
	if err := c.Wait(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "answer"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := p.h.blobs.Put(ctx, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"status": "completed", "exit": map[string]int{"code": 0}, "workspace_staged": true, "outputs": []map[string]any{
		{"path": "n", "sha256": in.Files[0].SHA256, "size": len(p.h.blobs.m[in.Files[0].SHA256])},
		{"path": "answer", "sha256": ref.SHA256, "size": ref.Size},
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.h.blobs.Put(ctx, bytes.NewReader(body))
	return call.Result{Status: 200, Body: body, BlobSHA256: result.SHA256}, err
}

func TestInterruptedPureCompute(t *testing.T) {
	src, dst := newH(t), newH(t)
	archive := &memoryArchive{objects: map[string][]byte{}}
	src.m.cfg.Archive, dst.m.cfg.Archive = archive, archive
	first, second := &sumProcess{h: src, t: t, interrupt: true}, &sumProcess{h: dst, t: t}
	src.m.cfg.Exec, dst.m.cfg.Exec = first, second
	do(t, src.m.Write, req(`{"path":"n","content":"30000"}`))
	ctx := t.Context()
	c1, err := src.m.Snapshot(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	run := shellReq("tool-sum-first", sumCommand)
	_, err = src.m.Exec(ctx, run)
	var term *exec.ExitError
	if !errors.As(err, &term) {
		t.Fatalf("interruption not propagated: %v", err)
	}
	head, err := src.m.Snapshot(ctx, "t1")
	if err != nil || head != c1 {
		t.Fatalf("interruption changed C1: %s %v", head, err)
	}
	if err := dst.m.Restore(ctx, "t1", c1); err != nil {
		t.Fatal(err)
	}
	run.AttemptID, run.CallID = "a2", "tool-sum-repeat"
	result, _ := do(t, dst.m.Exec, run)
	if result.Status != 200 || result.Replayed {
		t.Fatal("fresh execution failed")
	}
	st, err := readState(dst.m.path("t1"), "t1")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%d\n", sumSquares(30000))
	got := dst.blobs.m[st.Files["answer"].SHA256]
	if string(got) != want || st.Files["answer"].SHA256 != digestBytes([]byte(want)) || len(st.Applied) != 1 {
		t.Fatalf("independent oracle mismatch: %q", got)
	}
	replay, _ := do(t, dst.m.Exec, run)
	if !replay.Replayed || !bytes.Equal(result.Body, replay.Body) || first.calls != 1 || second.calls != 1 {
		t.Fatal("completed call failed to replay")
	}
	t.Logf("C1=%s first_exit=SIGTERM first_completion=false recovered_terms=30000 expected=%s answer_sha256=%s actual_processes=%d completed_receipts=%d duplicate_spawned=0", c1, strings.TrimSpace(want), st.Files["answer"].SHA256, first.calls+second.calls, len(st.Applied))
}
