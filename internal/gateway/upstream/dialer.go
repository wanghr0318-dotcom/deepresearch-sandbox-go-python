package upstream

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// 出站防护错误（§9.8）。均以 %w 包装，调用方用 errors.Is 判断。
var (
	ErrBlocked          = errors.New("upstream: 出站目标被拒绝")
	ErrInvalidURL       = errors.New("upstream: URL 不合法")
	ErrTooManyRedirects = errors.New("upstream: 重定向超过上限")
	ErrTruncated        = errors.New("upstream: 响应正文超过上限，已截断")
)

// DialerConfig 配置验证 dialer。
type DialerConfig struct {
	// AllowPrivate 显式放行的上游主机（§9.8 规则 2，配置 upstream_allow_private）。
	// 每项是主机名或 IP 字面量，可带端口（"host:port"、"[::1]:8080"）；命中的主机跳过地址类别与端口检查。
	AllowPrivate []string
	// Resolver 解析主机名；nil 时用系统解析器。测试以此注入确定性结果（含 DNS rebinding）。
	Resolver func(ctx context.Context, host string) ([]netip.Addr, error)
	// Dial 连接已检查的 "ip:port"；nil 时用 net.Dialer。测试以此把连接导向进程内服务器。
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// MaxRedirects 重定向上限；0 取默认 5。
	MaxRedirects int
}

// Dialer 是 §9.8 的验证拨号器：自行解析 DNS，拒绝不可达类别的地址，连接到已检查的 IP。
// 由于只替换 TCP 拨号，http.Transport 仍以原始主机名做 TLS SNI 与证书校验，Host 头不变。
type Dialer struct {
	allow        map[string]bool
	resolve      func(ctx context.Context, host string) ([]netip.Addr, error)
	dial         func(ctx context.Context, network, addr string) (net.Conn, error)
	maxRedirects int
}

// NewDialer 构造验证 dialer。
func NewDialer(cfg DialerConfig) *Dialer {
	d := &Dialer{allow: map[string]bool{}, resolve: cfg.Resolver, dial: cfg.Dial, maxRedirects: cfg.MaxRedirects}
	for _, h := range cfg.AllowPrivate {
		if k := normalizeAllow(h); k != "" {
			d.allow[k] = true
		}
	}
	if d.resolve == nil {
		d.resolve = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	if d.dial == nil {
		nd := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		d.dial = nd.DialContext
	}
	if d.maxRedirects <= 0 {
		d.maxRedirects = DefaultMaxRedirects
	}
	return d
}

// normalizeAllow 把放行项规范为 "host" 或 "host:port"（主机小写，IP 取规范文本）。
func normalizeAllow(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if h, p, err := net.SplitHostPort(s); err == nil {
		return net.JoinHostPort(normalizeHost(h), p)
	}
	return normalizeHost(s)
}

func normalizeHost(h string) string {
	h = strings.ToLower(strings.TrimSuffix(strings.Trim(h, "[]"), "."))
	if a, err := netip.ParseAddr(h); err == nil {
		return a.WithZone("").String()
	}
	return h
}

func (d *Dialer) allowed(host, port string) bool {
	h := normalizeHost(host)
	return d.allow[h] || d.allow[net.JoinHostPort(h, port)]
}

// 额外拒绝的地址段（netip 的分类方法之外）。
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "本网络"
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF 协议分配
	netip.MustParsePrefix("198.18.0.0/15"), // 基准测试
	netip.MustParsePrefix("240.0.0.0/4"),   // 保留（含广播）
	netip.MustParsePrefix("::/96"),         // IPv4 兼容地址（已废弃）
	netip.MustParsePrefix("64:ff9b::/96"),  // NAT64：可能映射到私有 IPv4
	netip.MustParsePrefix("fc00::/7"),      // ULA
}

// blockedAddr 报告地址是否属于禁止类别：loopback、私有、link-local、CGNAT、组播、未指定、ULA，
// 以及它们的 IPv4-mapped 形式（§9.8 规则 3）。
func blockedAddr(a netip.Addr) bool {
	a = a.WithZone("")
	if !a.IsValid() {
		return true
	}
	for _, x := range []netip.Addr{a, a.Unmap()} {
		if x.IsLoopback() || x.IsPrivate() || x.IsLinkLocalUnicast() || x.IsLinkLocalMulticast() ||
			x.IsInterfaceLocalMulticast() || x.IsMulticast() || x.IsUnspecified() {
			return true
		}
		for _, p := range blockedPrefixes {
			if p.Contains(x) {
				return true
			}
		}
	}
	return false
}

// DialContext 拨号 addr（"host:port"）：端口须为 80/443（放行主机除外）；主机名自行解析一次，
// 只连接通过检查的 IP（不再交给系统二次解析，杜绝 DNS rebinding）。
func (d *Dialer) DialContext(ctx context.Context, _, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("%w: 地址 %q: %v", ErrBlocked, addr, err)
	}
	pn, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("%w: 端口 %q", ErrBlocked, port)
	}
	allowed := d.allowed(host, port)
	if !allowed && pn != 80 && pn != 443 {
		return nil, fmt.Errorf("%w: 端口 %s 不允许", ErrBlocked, port)
	}
	var addrs []netip.Addr
	if a, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		addrs = []netip.Addr{a}
	} else {
		addrs, err = d.resolve(ctx, host)
		if err != nil {
			return nil, err
		}
	}
	var lastErr error
	tried := false
	for _, a := range addrs {
		a = a.WithZone("")
		if !allowed && blockedAddr(a) {
			continue
		}
		tried = true
		conn, err := d.dial(ctx, "tcp", netip.AddrPortFrom(a.Unmap(), uint16(pn)).String())
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if !tried {
		return nil, fmt.Errorf("%w: %s 只解析到禁止的地址", ErrBlocked, host)
	}
	return nil, lastErr
}

// CheckURL 校验出站 URL（§9.8 规则 1）：仅 http/https；不含 userinfo；端口为默认或 80/443（放行主机除外）；
// IP 字面量主机须通过地址类别检查。主机名的地址类别在拨号时检查。
func (d *Dialer) CheckURL(u *url.URL) error {
	if u == nil {
		return fmt.Errorf("%w: 空 URL", ErrInvalidURL)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: 只允许 http/https", ErrInvalidURL)
	}
	if u.User != nil {
		return fmt.Errorf("%w: 不允许 userinfo", ErrInvalidURL)
	}
	host := u.Hostname()
	if host == "" || u.Opaque != "" {
		return fmt.Errorf("%w: 缺少主机", ErrInvalidURL)
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	allowed := d.allowed(host, port)
	if !allowed && port != "80" && port != "443" {
		return fmt.Errorf("%w: 端口 %s 不允许", ErrBlocked, port)
	}
	if a, err := netip.ParseAddr(host); err == nil && !allowed && blockedAddr(a) {
		return fmt.Errorf("%w: 地址 %s 属于禁止类别", ErrBlocked, a)
	}
	return nil
}

func (d *Dialer) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > d.maxRedirects {
		return fmt.Errorf("%w（%d 跳）", ErrTooManyRedirects, d.maxRedirects)
	}
	return d.CheckURL(req.URL)
}

// HTTPClient 返回只经本 dialer 拨号的客户端：不继承代理环境变量（规则 7）；重定向 ≤ 上限且每跳重新检查（规则 5）；
// 固定 Accept-Encoding: gzip 并自行解压，maxBody（> 0 时）作用于解压后正文（规则 6）——超出时 Body.Read 返回
// ErrTruncated，此前的字节恰为 maxBody。timeout 为 0 时不设整体超时（由 ctx 期限约束）。
func (d *Dialer) HTTPClient(maxBody int64, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           d.DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   8,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
	}
	return &http.Client{
		Transport:     &guardTransport{inner: tr, max: maxBody},
		Timeout:       timeout,
		CheckRedirect: d.checkRedirect,
	}
}

// guardTransport 固定 Accept-Encoding、解压 gzip，并对解压后正文施加上限。
type guardTransport struct {
	inner http.RoundTripper
	max   int64
}

func (g *guardTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := g.inner.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	var body io.ReadCloser = resp.Body
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "gzip") {
		body = &gzipBody{src: resp.Body}
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.ContentLength = -1
		resp.Uncompressed = true
	}
	resp.Body = &limitBody{rc: body, remaining: g.max, limited: g.max > 0}
	return resp, nil
}

// gzipBody 在首次读取时才建立 gzip 读取器（读取 gzip 头可能阻塞）。
type gzipBody struct {
	src io.ReadCloser
	zr  *gzip.Reader
	err error
}

func (b *gzipBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.zr == nil {
		zr, err := gzip.NewReader(b.src)
		if err != nil {
			b.err = err
			return 0, err
		}
		b.zr = zr
	}
	return b.zr.Read(p)
}

func (b *gzipBody) Close() error { return b.src.Close() }

// limitBody 至多交付 remaining 字节；之后若底层还有数据则返回 ErrTruncated。
type limitBody struct {
	rc        io.ReadCloser
	remaining int64
	limited   bool
}

func (b *limitBody) Read(p []byte) (int, error) {
	if !b.limited {
		return b.rc.Read(p)
	}
	if b.remaining <= 0 {
		var one [1]byte
		for {
			n, err := b.rc.Read(one[:])
			if n > 0 {
				return 0, ErrTruncated
			}
			if err != nil {
				return 0, err
			}
		}
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.rc.Read(p)
	b.remaining -= int64(n)
	return n, err
}

func (b *limitBody) Close() error { return b.rc.Close() }
