package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
)

type memoryArchive struct {
	objects   map[string][]byte
	interrupt string
	puts      int
}

func (a *memoryArchive) Get(ctx context.Context, sha string) (io.ReadCloser, error) {
	body, ok := a.objects[sha]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(body)), ctx.Err()
}

func (a *memoryArchive) Put(ctx context.Context, sha string, body io.Reader, n int64) error {
	if sha == a.interrupt {
		return errors.New("interrupted upload")
	}
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(b)) != n {
		return io.ErrUnexpectedEOF
	}
	a.objects[sha] = b
	a.puts++
	return ctx.Err()
}

func TestPortableWorkspaceSnapshot(t *testing.T) {
	ctx := t.Context()
	a := &memoryArchive{objects: make(map[string][]byte)}
	src, dst := newH(t), newH(t) // independent hot caches and empty data directories
	src.m.cfg.Archive, dst.m.cfg.Archive = a, a
	src.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"script": "#!/bin/sh", "empty": "", "a/b": "hello"}, map[string]bool{"script": true}, nil
	}
	cmd := shellReq("tool-1", "produce")
	result, _ := do(t, src.m.Exec, cmd)
	c1, err := src.m.Snapshot(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	n := a.puts
	second, err := src.m.Snapshot(ctx, "t1")
	if second != c1 || err != nil || a.puts != n {
		t.Fatalf("unstable snapshot %s %v (%d extra uploads)", second, err, a.puts-n)
	}
	do(t, src.m.Write, req(`{"path":"a/b","content":"next"}`))
	a.interrupt = digestBytes([]byte("next"))
	if sha, err := src.m.Snapshot(ctx, "t1"); err == nil || sha != "" {
		t.Fatalf("interrupted snapshot %q %v", sha, err)
	}
	a.interrupt = ""
	c2, err := src.m.Snapshot(ctx, "t1")
	if err != nil || c1 == c2 {
		t.Fatalf("second version %s %v", c2, err)
	}
	// Simulate a crash before publication of C2: the database still names C1.
	if err := dst.m.Restore(ctx, "t1", c1); err != nil {
		t.Fatal(err)
	}
	st, err := readState(dst.m.path("t1"), "t1")
	if err != nil || !st.Files["script"].Exec || st.Files["empty"].SHA256 != digestBytes(nil) || st.Files["a/b"].SHA256 != digestBytes([]byte("hello")) {
		t.Fatalf("C1: %+v %v", st, err)
	}
	replay, _ := do(t, dst.m.Exec, cmd)
	if !replay.Replayed || replay.BlobSHA256 != result.BlobSHA256 || !bytes.Equal(replay.Body, result.Body) || len(dst.exec.calls) != 0 {
		t.Fatal("applied tool was not replayed")
	}
	if err := dst.m.Restore(ctx, "other-task", c1); err == nil {
		t.Fatal("foreign checkpoint accepted")
	}
	key := digestBytes([]byte("next"))
	a.objects[key] = []byte("oops")
	if err := dst.m.Restore(ctx, "t1", c2); err == nil {
		t.Fatal("corrupt archive accepted")
	}
	st, _ = readState(dst.m.path("t1"), "t1")
	if st.Files["a/b"].SHA256 != digestBytes([]byte("hello")) {
		t.Fatal("incomplete C2 became visible")
	}
	a.objects[key] = []byte("next")
	if err := dst.m.Restore(ctx, "t1", c2); err != nil {
		t.Fatal(err)
	}
	if got := dst.blobs.m[digestBytes([]byte("hello"))]; string(got) != "hello" {
		t.Fatalf("missing historic output: %q", got)
	}
	fresh := newH(t)
	fresh.m.cfg.Archive = a
	if err := fresh.m.Restore(ctx, "t1", c2); err != nil || string(fresh.blobs.m[digestBytes([]byte("hello"))]) != "hello" {
		t.Fatalf("cold restore lost superseded output: %v", err)
	}
	if err := dst.m.ResetUncommitted(ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	st, _ = readState(dst.m.path("t1"), "t1")
	if len(st.Files) != 0 || len(st.Applied) != 0 || st.WriteOps != 1 || st.WrittenBytes != int64(len("next")) {
		t.Fatalf("empty recovery lost meter or retained files: %+v", st)
	}
}

func TestSnapshotDetectsCorruptLocalBlob(t *testing.T) {
	h := newH(t)
	a := &memoryArchive{objects: map[string][]byte{}}
	h.m.cfg.Archive = a
	do(t, h.m.Write, req(`{"path":"a","content":"hello"}`))
	h.blobs.m[digestBytes([]byte("hello"))] = []byte(strings.Repeat("x", 5))
	if _, err := h.m.Snapshot(t.Context(), "t1"); err == nil || len(a.objects) != 0 {
		t.Fatal("corrupt local object was uploaded")
	}
}

type meteredArchive struct {
	SnapshotArchive
	uploaded int64
	puts     int
}

func (a *meteredArchive) Put(ctx context.Context, sha string, r io.Reader, size int64) error {
	err := a.SnapshotArchive.Put(ctx, sha, r, size)
	if err == nil {
		a.uploaded += size
		a.puts++
	}
	return err
}

func verifyLargeWorkspaceTransfer(t *testing.T, backend SnapshotArchive) {
	t.Helper()
	const files = 64
	const fileBytes = 512 << 10
	a := &meteredArchive{SnapshotArchive: backend}
	src, dst := newH(t), newH(t)
	src.m.cfg.Archive, dst.m.cfg.Archive = a, a
	src.m.cfg.MaxBytes, src.m.cfg.MaxFiles = DefaultMaxBytes, DefaultMaxFiles
	// Unique per run so a retained cloud prefix cannot warm the initial transfer.
	nonce := strconv.FormatInt(time.Now().UnixNano(), 10)
	write := func(index int, revision string) {
		label := fmt.Sprintf("%s-%d-%s", nonce, index, revision)
		content := label + strings.Repeat("x", fileBytes-len(label))
		request, _ := json.Marshal(map[string]string{"path": fmt.Sprintf("data/%d", index), "content": content})
		res, _ := do(t, src.m.Write, req(string(request)))
		if res.Status != 200 {
			t.Fatalf("write %d: %d %s", index, res.Status, res.Code)
		}
	}
	for i := 0; i < files; i++ {
		write(i, "initial")
	}
	start := time.Now()
	c1, err := src.m.Snapshot(t.Context(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	fullTime, fullBytes := time.Since(start), a.uploaded
	if a.puts != files+1 {
		t.Fatalf("full upload object count %d", a.puts)
	}
	write(0, "changed")
	start = time.Now()
	c2, err := src.m.Snapshot(t.Context(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	deltaTime, deltaBytes := time.Since(start), a.uploaded-fullBytes
	if c1 == c2 || a.puts != files+3 || deltaBytes < fileBytes || deltaBytes >= 2*fileBytes {
		t.Fatalf("incremental sha=%s puts=%d bytes=%d", c2, a.puts, deltaBytes)
	}
	start = time.Now()
	if err := dst.m.Restore(t.Context(), "t1", c2); err != nil {
		t.Fatal(err)
	}
	restored, err := readState(dst.m.path("t1"), "t1")
	if err != nil || restored.Version != src.m.get("t1").st.Version || len(restored.Files) != files {
		t.Fatalf("restored manifest mismatch: %v", err)
	}
	t.Logf("files=%d workspace_bytes=%d full_put_bytes=%d delta_put_bytes=%d full=%s delta=%s restore=%s",
		files, files*fileBytes, fullBytes, deltaBytes, fullTime, deltaTime, time.Since(start))
}

func TestLargeWorkspaceIncrementalTransfer(t *testing.T) {
	verifyLargeWorkspaceTransfer(t, &memoryArchive{objects: map[string][]byte{}})
}
