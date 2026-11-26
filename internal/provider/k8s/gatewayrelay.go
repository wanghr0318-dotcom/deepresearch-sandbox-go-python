package k8s

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

type gatewayRelay struct {
	ip     net.IP
	port   int32
	url    string
	caPEM  string
	server *http.Server
	ctx    context.Context
	stop   context.CancelFunc

	mu     sync.RWMutex
	grants map[string]socketGrant
}

type socketGrant struct {
	ctx   context.Context
	proxy *httputil.ReverseProxy
}

func newGatewayRelay(addr string) (*gatewayRelay, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("k8s: Gateway relay address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) {
		return nil, fmt.Errorf("k8s: Gateway relay requires a private bind IP: %q", host)
	}

	cert, caPEM, err := newRelayCertificate(ip)
	if err != nil {
		return nil, fmt.Errorf("k8s: Gateway relay certificate: %w", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("k8s: listen for Gateway relay: %w", err)
	}
	port := int32(ln.Addr().(*net.TCPAddr).Port)
	ctx, cancel := context.WithCancel(context.Background())
	r := &gatewayRelay{
		ip: ip, port: port, url: "https://" + net.JoinHostPort(host, strconv.Itoa(int(port))),
		caPEM: caPEM, ctx: ctx, stop: cancel, grants: make(map[string]socketGrant),
	}
	r.server = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
		},
	}
	go func() { _ = r.server.ServeTLS(ln, "", "") }()
	return r, nil
}

func newRelayCertificate(ip net.IP) (tls.Certificate, string, error) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, "", err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{ip},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, string(caPEM), nil
}

// bind pairs a one-Pod credential with the host Unix socket already bound by Gateway.
func (r *gatewayRelay) bind(socketPath string) (podapi.GatewayGrant, func(), error) {
	if err := r.ctx.Err(); err != nil {
		return podapi.GatewayGrant{}, nil, err
	}
	token := rand.Text()

	// Keep the existing Unix listener as the source of task identity. The HTTP
	// relay forwards to this socket regardless of headers or URL sent by the Pod.
	upstream := &url.URL{Scheme: "http", Host: "localhost"}
	dial := net.Dialer{}
	proxy := &httputil.ReverseProxy{
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dial.DialContext(ctx, "unix", socketPath)
			},
		},
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(upstream)
			req.Out.Header.Del("Authorization")
		},
	}
	ctx, cancel := context.WithCancel(r.ctx)
	r.mu.Lock()
	r.grants[token] = socketGrant{ctx: ctx, proxy: proxy}
	r.mu.Unlock()
	revoke := func() {
		r.mu.Lock()
		delete(r.grants, token)
		r.mu.Unlock()
		cancel()
	}
	return podapi.GatewayGrant{URL: r.url, CAPEM: r.caPEM, Token: token}, revoke, nil
}

func (r *gatewayRelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	token, bearer := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
	r.mu.RLock()
	g, ok := r.grants[token]
	r.mu.RUnlock()
	if !bearer || !ok || g.ctx.Err() != nil {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	g.proxy.ServeHTTP(w, req.WithContext(ctx))
}

func (r *gatewayRelay) close() {
	r.stop()
	_ = r.server.Close()
}
