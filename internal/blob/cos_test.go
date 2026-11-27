package blob

import (
	"bytes"
	"errors"
	"hash/crc64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

func TestCOSArchiveObjectRoundTrip(t *testing.T) {
	const sha = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	var stored []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/phase4/"+sha || r.Header.Get("Authorization") == "" {
			t.Errorf("unexpected object path %s or missing signature", r.URL.Path)
			http.Error(w, "bad request", 400)
			return
		}
		switch r.Method {
		case "PUT":
			stored, _ = io.ReadAll(r.Body)
			w.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(stored, crc64.MakeTable(crc64.ECMA)), 10))
		case "GET":
			if stored == nil {
				w.WriteHeader(404)
				return
			}
			w.Write(stored)
		default:
			http.Error(w, "unsupported", 405)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{Transport: &cos.AuthorizationTransport{SecretID: "test", SecretKey: "test"}})
	a := COSArchive{prefix: "phase4", client: client}
	if _, err := a.Get(t.Context(), sha); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if err := a.Put(t.Context(), sha, bytes.NewReader([]byte("hello")), 5); err != nil {
		t.Fatal(err)
	}
	body, err := a.Get(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "hello" {
		t.Fatalf("GET: %q, %v", data, err)
	}
}

func TestCOSSigningIdentity(t *testing.T) {
	for _, creds := range []COSCredentials{{CVMRole: "checkpoint-worker"}, {CVMRole: "checkpoint-worker", SecretID: "unused", SecretKey: "unused"}} {
		transport, err := creds.transport()
		role, ok := transport.(*cos.CVMCredentialTransport)
		if err != nil || !ok || role.RoleName != "checkpoint-worker" {
			t.Fatalf("role identity selection: %T %v", transport, err)
		}
	}
	transport, err := (COSCredentials{SecretID: "id", SecretKey: "secret", SessionToken: "token"}).transport()
	keys, ok := transport.(*cos.AuthorizationTransport)
	if err != nil || !ok || keys.SecretID != "id" || keys.SecretKey != "secret" || keys.SessionToken != "token" {
		t.Fatalf("explicit identity selection: %T %v", transport, err)
	}
	if _, err := (COSCredentials{SecretID: "incomplete"}).transport(); err == nil {
		t.Fatal("incomplete key credentials accepted")
	}
}
