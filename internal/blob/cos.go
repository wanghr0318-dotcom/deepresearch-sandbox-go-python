package blob

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// COSArchive holds checkpoint objects under a bucket prefix. The object name is the SHA-256
// of the exact stored bytes (the caller verifies the bytes on each operation).
type COSArchive struct {
	client *cos.Client
	prefix string
}

func NewCOSArchive(bucketURL, prefix, secretID, secretKey, sessionToken string) (*COSArchive, error) {
	u, err := url.Parse(bucketURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("COS archive requires an HTTPS bucket URL")
	}
	prefix = strings.Trim(prefix, "/")
	if prefix == "" || path.Clean(prefix) != prefix || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") {
		return nil, fmt.Errorf("COS archive requires a valid bucket prefix")
	}
	if secretID == "" || secretKey == "" {
		return nil, fmt.Errorf("COS archive credentials missing")
	}
	httpClient := &http.Client{Transport: &cos.AuthorizationTransport{
		SecretID: secretID, SecretKey: secretKey, SessionToken: sessionToken,
	}}
	return &COSArchive{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, httpClient), prefix: prefix}, nil
}

func (a *COSArchive) Get(ctx context.Context, sha string) (io.ReadCloser, error) {
	if !validSHA256(sha) {
		return nil, fmt.Errorf("invalid COS object digest %q", sha)
	}
	resp, err := a.client.Object.Get(ctx, a.prefix+"/"+sha, nil)
	if cos.IsNotFoundError(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (a *COSArchive) Put(ctx context.Context, sha string, r io.Reader, size int64) error {
	if !validSHA256(sha) {
		return fmt.Errorf("invalid COS object digest %q", sha)
	}
	_, err := a.client.Object.Put(ctx, a.prefix+"/"+sha, r, &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{ContentLength: size},
	})
	return err
}
