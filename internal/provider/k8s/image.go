package k8s

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// ImageRef is a parsed OCI image reference: [registry/]repository[:tag][@sha256:digest].
type ImageRef struct {
	Registry   string // host[:port]; "docker.io" when omitted
	Repository string // e.g. "library/python" for docker.io
	Tag        string
	Digest     string // "sha256:<64 hex>"
}

var (
	digestRE = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	repoRE   = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*$`)
	tagRE    = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
)

// ParseImageRef parses ref. A reference without tag or digest gets tag "latest".
func ParseImageRef(ref string) (ImageRef, error) {
	var r ImageRef
	rest := ref
	if i := strings.Index(rest, "@"); i >= 0 {
		r.Digest, rest = rest[i+1:], rest[:i]
		if !digestRE.MatchString(r.Digest) {
			return ImageRef{}, fmt.Errorf("k8s: image %q: digest must be sha256:<64 hex>", ref)
		}
	}
	// A tag is a ":" after the last "/" (a ":" before it belongs to the registry port).
	if i := strings.LastIndex(rest, ":"); i > strings.LastIndex(rest, "/") {
		r.Tag, rest = rest[i+1:], rest[:i]
		if !tagRE.MatchString(r.Tag) {
			return ImageRef{}, fmt.Errorf("k8s: image %q: invalid tag", ref)
		}
	}
	first, remainder, found := strings.Cut(rest, "/")
	if found && (strings.ContainsAny(first, ".:") || first == "localhost") {
		r.Registry, r.Repository = first, remainder
	} else {
		r.Registry, r.Repository = "docker.io", rest
		if !found {
			r.Repository = "library/" + rest
		}
	}
	if !repoRE.MatchString(r.Repository) {
		return ImageRef{}, fmt.Errorf("k8s: image %q: invalid repository", ref)
	}
	if r.Tag == "" && r.Digest == "" {
		r.Tag = "latest"
	}
	return r, nil
}

// Name is the reference without tag and digest, in the form given by the user (docker.io kept implicit).
func (r ImageRef) Name() string {
	if r.Registry == "docker.io" {
		return strings.TrimPrefix(r.Repository, "library/")
	}
	return r.Registry + "/" + r.Repository
}

// Pinned is name@digest; it panics if the digest is unknown.
func (r ImageRef) Pinned() string {
	if r.Digest == "" {
		panic("k8s: image reference is not pinned")
	}
	return r.Name() + "@" + r.Digest
}

// ErrMutableTag is returned by PinImage in strict mode for a reference without a digest.
var ErrMutableTag = errors.New("k8s: strict image pinning refuses a mutable tag; use repo@sha256:<digest>")

// manifestAccept lists the manifest media types accepted when resolving a tag (index first, so the
// digest is the multi-platform one that the registry would serve to a plain pull).
var manifestAccept = strings.Join([]string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}, ", ")

// PinImage returns ref pinned to a digest. A digest reference is returned unchanged (its tag dropped). A
// tag is refused in strict mode; otherwise it is resolved through the registry's HTTP API (anonymous; plain
// HTTP for registries listed in plainHTTP).
func PinImage(ctx context.Context, ref string, strict bool, plainHTTP []string, client *http.Client) (ImageRef, error) {
	r, err := ParseImageRef(ref)
	if err != nil {
		return ImageRef{}, err
	}
	if r.Digest != "" {
		r.Tag = ""
		return r, nil
	}
	if strict {
		return ImageRef{}, fmt.Errorf("%w (got %q)", ErrMutableTag, ref)
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	scheme, host := "https", r.Registry
	for _, h := range plainHTTP {
		if h == r.Registry {
			scheme = "http"
		}
	}
	if host == "docker.io" {
		host = "registry-1.docker.io"
	}
	url := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", scheme, host, r.Repository, r.Tag)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return ImageRef{}, err
	}
	req.Header.Set("Accept", manifestAccept)
	resp, err := client.Do(req)
	if err != nil {
		return ImageRef{}, fmt.Errorf("k8s: resolve %s: %w", ref, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ImageRef{}, fmt.Errorf("k8s: resolve %s: registry answered %s (anonymous access only)", ref, resp.Status)
	}
	d := resp.Header.Get("Docker-Content-Digest")
	if !digestRE.MatchString(d) {
		return ImageRef{}, fmt.Errorf("k8s: resolve %s: registry returned no usable digest (%q)", ref, d)
	}
	r.Digest, r.Tag = d, ""
	return r, nil
}
