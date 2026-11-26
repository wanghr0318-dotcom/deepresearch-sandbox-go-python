package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestGatewayRelay(t *testing.T) {
	relay, err := newGatewayRelay("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(relay.close)

	streamClosed := make(chan struct{})
	unixGateway := func(label string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "gw.sock")
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.URL.Path == "/stream" {
				fmt.Fprintln(w, "started")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(streamClosed)
				return
			}
			fmt.Fprintf(w, "%s: %s", label, r.URL.RequestURI())
		})}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() { _ = srv.Close() })
		return path
	}

	first, revoke, err := relay.bind(unixGateway("first"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(revoke)
	second, release, err := relay.bind(unixGateway("second"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(first.CAPEM)) {
		t.Fatal("parse relay CA")
	}
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}},
	}
	t.Cleanup(client.CloseIdleConnections)
	request := func(token, path string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, first.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	check := func(token string, status int, want string) {
		t.Helper()
		resp := request(token, "/tool?scope=task")
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != status || string(data) != want {
			t.Fatalf("response = %d %q, error = %v", resp.StatusCode, data, err)
		}
	}
	check(first.Token, http.StatusOK, "first: /tool?scope=task")
	check(second.Token, http.StatusOK, "second: /tool?scope=task")
	check("unrecognized", http.StatusUnauthorized, "Unauthorized\n")

	// Revocation must cancel an active response as well as disallow new requests.
	stream := request(first.Token, "/stream")
	defer stream.Body.Close()
	prefix := make([]byte, len("started\n"))
	if _, err := io.ReadFull(stream.Body, prefix); err != nil || string(prefix) != "started\n" {
		t.Fatalf("stream prefix %q: %v", prefix, err)
	}
	revoke()
	select {
	case <-streamClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("revocation did not cancel the upstream request")
	}
	check(first.Token, http.StatusUnauthorized, "Unauthorized\n")
	check(second.Token, http.StatusOK, "second: /tool?scope=task")
}
