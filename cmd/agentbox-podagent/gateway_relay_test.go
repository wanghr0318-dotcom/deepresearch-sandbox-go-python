package main

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

func TestGatewayRelayProxy(t *testing.T) {
	finish := make(chan struct{})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/v1/lookup?q=one" || r.Header.Get("Authorization") != "Bearer test-grant" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		<-finish
	}))
	defer upstream.Close()

	proxy, err := gatewayRelayProxy(podapi.GatewayGrant{
		URL: upstream.URL, Token: "test-grant",
		CAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})),
	})
	if err != nil {
		t.Fatal(err)
	}
	relay := httptest.NewServer(proxy)
	defer relay.Close()
	defer close(finish)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, relay.URL+"/v1/lookup?q=one", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := relay.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	chunk := make([]byte, len("first"))
	if _, err := io.ReadFull(response.Body, chunk); err != nil || string(chunk) != "first" || response.StatusCode != http.StatusOK {
		t.Fatalf("response status %d, first bytes %q, error %v", response.StatusCode, chunk, err)
	}
}
