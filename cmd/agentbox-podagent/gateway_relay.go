package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

const gatewaySocket = "/run/agentbox/gateway.sock"

func runGatewayRelay(args []string) error {
	if len(args) != 1 {
		return errors.New("gateway-relay requires a grant file")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	var grant podapi.GatewayGrant
	if err := json.Unmarshal(data, &grant); err != nil {
		return err
	}
	proxy, err := gatewayRelayProxy(grant)
	if err != nil {
		return err
	}
	listener, err := net.Listen("unix", gatewaySocket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(gatewaySocket, 0o600); err != nil {
		return err
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	unregister := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer unregister()
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func gatewayRelayProxy(grant podapi.GatewayGrant) (*httputil.ReverseProxy, error) {
	endpoint, err := url.Parse(grant.URL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || grant.Token == "" {
		return nil, errors.New("gateway relay requires an HTTPS endpoint and token")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(grant.CAPEM)) {
		return nil, errors.New("invalid gateway relay CA")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{RootCAs: roots}
	return &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(endpoint)
			r.Out.Header.Set("Authorization", "Bearer "+grant.Token)
		},
	}, nil
}
