package upstream

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

// FetchConfig 配置抓取 adapter。
type FetchConfig struct {
	Dialer   *Dialer // 验证 dialer；nil 时用默认配置
	Pricing  Pricing
	MaxBytes int64         // 解压后正文上限；≤ 0 时取 5 MiB
	Timeout  time.Duration // 整体超时；≤ 0 时取 20 s
}

type fetchAdapter struct {
	cfg    FetchConfig
	dialer *Dialer
	http   *http.Client
}

// NewFetch 构造抓取 adapter：只 GET，不接受 Worker 指定的请求头或请求体；5 MiB / 20 s / 5 跳（§19）。
func NewFetch(cfg FetchConfig) Adapter {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultFetchMaxBody
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultFetchTimeout
	}
	d := cfg.Dialer
	if d == nil {
		d = NewDialer(DialerConfig{})
	}
	return &fetchAdapter{cfg: cfg, dialer: d, http: d.HTTPClient(cfg.MaxBytes, cfg.Timeout)}
}

func (a *fetchAdapter) Kind() Kind       { return KindFetch }
func (a *fetchAdapter) Provider() string { return "http_get" }
func (a *fetchAdapter) Version() string  { return "fetch/1" }

// Resolve：只接受 {url}；URL 须通过 §9.8 规则 1 的检查；规范化时去掉片段。
func (a *fetchAdapter) Resolve(body []byte) ([]byte, map[string]any, error) {
	obj, e := decodeObject(body)
	if e != nil {
		return nil, nil, e
	}
	if e := checkFields(obj, "url"); e != nil {
		return nil, nil, e
	}
	var raw string
	if json.Unmarshal(obj["url"], &raw) != nil || raw == "" {
		return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "url 须为非空字符串")
	}
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() {
		return nil, nil, newErr(OutcomeFatal, http.StatusBadRequest, CodeInvalidURL, ErrInvalidURL)
	}
	if err := a.dialer.CheckURL(u); err != nil {
		return nil, nil, transportError(err, false)
	}
	u.Fragment, u.RawFragment = "", ""
	out, _ := json.Marshal(map[string]string{"url": u.String()})
	return out, map[string]any{}, nil
}

// Estimate：按次计价（响应字节与并发另有配额，由 call 层执行）。
func (a *fetchAdapter) Estimate(resolved []byte) (int64, error) {
	if a.cfg.Pricing.FetchMicroPerRequest < 0 {
		return 0, errors.New("抓取单价为负")
	}
	return a.cfg.Pricing.FetchMicroPerRequest, nil
}

// FetchResult 是抓取结果（Response.Body 的 JSON）。Encoding 为 utf-8 时 Content 是原文，为 base64 时是编码后的字节。
type FetchResult struct {
	URL         string `json:"url"`
	FinalURL    string `json:"final_url"`
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Truncated   bool   `json:"truncated"`
	Encoding    string `json:"encoding"`
	Content     string `json:"content"`
	Error       string `json:"error,omitempty"` // 目标站点返回 4xx/5xx 时为 "http_status"
}

// Do 发出 GET；请求头固定（User-Agent、Accept、Accept-Encoding: gzip），正文按解压后字节截断并置 truncated。
// 目标站点返回的 4xx/5xx 是确定、可重放的答案：结果为 ok，Body 带 status、截短的正文与 error: "http_status"。
// 只有传输层失败（拨号、TLS、超时、重定向被拒、读响应失败）按 roundTrip 的类别归类。
func (a *fetchAdapter) Do(ctx context.Context, resolved []byte) (Response, *Error) {
	var in struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(resolved, &in) != nil || in.URL == "" {
		return Response{}, fatalf(http.StatusBadRequest, CodeInvalidRequest, "请求未经 Resolve")
	}
	req, err := http.NewRequest(http.MethodGet, in.URL, nil)
	if err != nil {
		return Response{}, newErr(OutcomeFatal, http.StatusBadRequest, CodeInvalidURL, ErrInvalidURL)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")
	r, e := roundTrip(ctx, a.http, req)
	if e != nil {
		return Response{}, e
	}
	content, truncated := r.body, r.truncated
	res := FetchResult{
		URL:         in.URL,
		FinalURL:    r.resp.Request.URL.String(),
		Status:      r.resp.StatusCode,
		ContentType: r.resp.Header.Get("Content-Type"),
	}
	if sc := r.resp.StatusCode; sc < 200 || sc >= 300 {
		res.Error = "http_status"
		if len(content) > fetchErrorBodyMax {
			content, truncated = content[:fetchErrorBodyMax], true
		}
	}
	res.Truncated = truncated
	if utf8.Valid(content) && !strings.ContainsRune(string(content), 0) {
		res.Encoding, res.Content = "utf-8", string(content)
	} else {
		res.Encoding, res.Content = "base64", base64.StdEncoding.EncodeToString(content)
	}
	body, _ := json.Marshal(res)
	return Response{Body: body, Usage: Usage{Requests: 1, ResponseBytes: int64(len(r.body))}}, nil
}
