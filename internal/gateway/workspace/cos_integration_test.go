//go:build cos_integration

package workspace

import (
	"os"
	"testing"

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
