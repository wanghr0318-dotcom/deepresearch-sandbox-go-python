//go:build cos_integration

package workspace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
)

// This tagged test makes a real COS request and fails when endpoint or credentials are absent.
// Supply a dedicated experiment prefix; the immutable checkpoint is retained there.
func TestCOSPortableWorkspace(t *testing.T) {
	archive, err := blob.NewCOSArchive(os.Getenv("AGENTBOX_COS_BUCKET_URL"), os.Getenv("AGENTBOX_COS_PREFIX"),
		blob.COSCredentials{CVMRole: os.Getenv("AGENTBOX_COS_CVM_ROLE"), SecretID: os.Getenv("TENCENTCLOUD_SECRET_ID"), SecretKey: os.Getenv("TENCENTCLOUD_SECRET_KEY"), SessionToken: os.Getenv("TENCENTCLOUD_SESSION_TOKEN")})
	if err != nil {
		t.Fatal(err)
	}
	verifyLargeWorkspaceTransfer(t, archive)
	src, dst := newH(t), newH(t)
	src.m.cfg.Archive, dst.m.cfg.Archive = archive, archive
	do(t, src.m.Write, req(`{"path":"probe.txt","content":"round-trip via COS"}`))
	sha, err := src.m.Snapshot(t.Context(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if err = dst.m.Restore(t.Context(), "t1", sha); err != nil {
		t.Fatal(err)
	}
	st, err := readState(dst.m.path("t1"), "t1")
	if err != nil || st.Files["probe.txt"].SHA256 != digestBytes([]byte("round-trip via COS")) {
		t.Fatalf("restored workspace: %v %v", st, err)
	}
	t.Logf("verified manifest %s; content sha256=%s bytes=%d", sha, st.Files["probe.txt"].SHA256, st.Files["probe.txt"].Size)
}

// damagedCOSRead introduces a one-bit error after COS returns an actual object.
// The object in COS remains intact, so a later retry can verify the original bytes.
type damagedCOSRead struct {
	SnapshotArchive
	key string
}

func (a damagedCOSRead) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := a.SnapshotArchive.Get(ctx, key)
	if err != nil || key != a.key {
		return r, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("expected nonempty corruption target")
	}
	data[0] ^= 1
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestCOSPortableReceiptAndCorruptRead(t *testing.T) {
	ctx := t.Context()
	prefix := os.Getenv("AGENTBOX_COS_PREFIX")
	if prefix == "" {
		t.Fatal("dedicated COS prefix required")
	}
	archive, err := blob.NewCOSArchive(os.Getenv("AGENTBOX_COS_BUCKET_URL"), fmt.Sprintf("%s/receipt-%d", prefix, time.Now().UnixNano()), blob.COSCredentials{CVMRole: os.Getenv("AGENTBOX_COS_CVM_ROLE")})
	if err != nil {
		t.Fatal(err)
	}
	src, dst := newH(t), newH(t)
	src.m.cfg.Archive = archive
	dst.m.cfg.Archive = archive
	src.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"script": "#!/bin/sh", "empty": "", "a/b": "initial"}, map[string]bool{"script": true}, nil
	}
	request := shellReq("tool-1", "produce")
	first, _ := do(t, src.m.Exec, request)
	c1, err := src.m.Snapshot(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	do(t, src.m.Write, req(`{"path":"a/b","content":"changed"}`))
	c2, err := src.m.Snapshot(ctx, "t1")
	if err != nil || c2 == c1 {
		t.Fatalf("second snapshot %s %v", c2, err)
	}
	check := func(value string) {
		t.Helper()
		st, err := readState(dst.m.path("t1"), "t1")
		if err != nil || len(st.Files) != 3 || len(st.Applied) != 1 || st.Version != version(st.Files) {
			t.Fatalf("bad restored manifest: %v", err)
		}
		for path, content := range map[string]string{"script": "#!/bin/sh", "empty": "", "a/b": value} {
			entry, ok := st.Files[path]
			if !ok || entry.Exec != (path == "script") || entry.Size != int64(len(content)) || entry.SHA256 != digestBytes([]byte(content)) || string(dst.blobs.m[entry.SHA256]) != content {
				t.Fatal("restored file mismatch", path)
			}
		}
		replay, _ := do(t, dst.m.Exec, request)
		if !replay.Replayed || replay.BlobSHA256 != first.BlobSHA256 || !bytes.Equal(replay.Body, first.Body) || len(dst.exec.calls) != 0 {
			t.Fatal("restored receipt did not replay")
		}
	}
	if err := dst.m.Restore(ctx, "t1", c1); err != nil {
		t.Fatal(err)
	}
	check("initial")
	dst.m.cfg.Archive = damagedCOSRead{archive, digestBytes([]byte("changed"))}
	if err := dst.m.Restore(ctx, "t1", c2); err == nil {
		t.Fatal("corrupt C2 accepted")
	} else {
		t.Logf("C2 corruption rejected: %v", err)
	}
	check("initial")
	dst.m.cfg.Archive = archive
	if err := dst.m.Restore(ctx, "t1", c2); err != nil {
		t.Fatal(err)
	}
	check("changed")
	t.Logf("C1=%s C2=%s verified_files=3 empty=1 executable=1 replayed_after_each_check=1 new_execs=0", c1, c2)
}
