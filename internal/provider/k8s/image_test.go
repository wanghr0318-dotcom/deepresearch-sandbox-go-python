package k8s

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestParseImageRef(t *testing.T) {
	cases := []struct {
		in   string
		want ImageRef
		name string
	}{
		{"python", ImageRef{"docker.io", "library/python", "latest", ""}, "python"},
		{"python:3.12-slim", ImageRef{"docker.io", "library/python", "3.12-slim", ""}, "python"},
		{"kind-registry:5000/agentbox-worker:dev", ImageRef{"kind-registry:5000", "agentbox-worker", "dev", ""}, "kind-registry:5000/agentbox-worker"},
		{"ghcr.io/a/b@" + testDigest, ImageRef{"ghcr.io", "a/b", "", testDigest}, "ghcr.io/a/b"},
		{"localhost/x:1", ImageRef{"localhost", "x", "1", ""}, "localhost/x"},
		{"user/repo:t@" + testDigest, ImageRef{"docker.io", "user/repo", "t", testDigest}, "user/repo"},
	}
	for _, c := range cases {
		got, err := ParseImageRef(c.in)
		if err != nil || got != c.want || got.Name() != c.name {
			t.Errorf("ParseImageRef(%q) = %+v %q %v", c.in, got, got.Name(), err)
		}
	}
	for _, bad := range []string{"", "UPPER/x", "x@sha256:short", "x:bad tag", "x@md5:00"} {
		if _, err := ParseImageRef(bad); err == nil {
			t.Errorf("ParseImageRef(%q) should fail", bad)
		}
	}
}

func TestPinImageDigestPassesThroughAndStrictRefusesTags(t *testing.T) {
	r, err := PinImage(context.Background(), "reg:5000/w:dev@"+testDigest, true, nil, nil)
	if err != nil || r.Pinned() != "reg:5000/w@"+testDigest {
		t.Fatalf("got %q %v", r.Pinned(), err)
	}
	if _, err := PinImage(context.Background(), "reg:5000/w:dev", true, nil, nil); !errors.Is(err, ErrMutableTag) {
		t.Fatalf("strict tag: %v", err)
	}
}

func TestPinImageResolvesTagThroughRegistry(t *testing.T) {
	var gotPath, gotAccept, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAccept, gotMethod = r.URL.Path, r.Header.Get("Accept"), r.Method
		if r.URL.Path == "/v2/team/worker/manifests/dev" {
			w.Header().Set("Docker-Content-Digest", testDigest)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	r, err := PinImage(context.Background(), host+"/team/worker:dev", false, []string{host}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if r.Pinned() != host+"/team/worker@"+testDigest || gotMethod != http.MethodHead ||
		gotPath != "/v2/team/worker/manifests/dev" || !strings.Contains(gotAccept, "oci.image.index") {
		t.Fatalf("pinned %q, request %s %s accept %q", r.Pinned(), gotMethod, gotPath, gotAccept)
	}
	if _, err := PinImage(context.Background(), host+"/team/worker:missing", false, []string{host}, srv.Client()); err == nil {
		t.Fatal("unknown tag should fail")
	}
}
