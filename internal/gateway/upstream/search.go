package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// 搜索供应商。
const (
	SearchFake    = "fake"
	SearchTavily  = "tavily"
	SearchDDGLite = "ddg_lite" // 脆弱、仅演示：解析 DuckDuckGo Lite 的 HTML，页面改版即失效
)

// SearchResult 是一条规范化的搜索结果。
type SearchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet"`
}

// SearchConfig 配置搜索 adapter。
type SearchConfig struct {
	Provider string // fake | tavily | ddg_lite
	APIKey   string // tavily：来自宿主环境变量 AGENTBOX_SEARCH_API_KEY；只放在 Authorization 头
	BaseURL  string // 覆盖供应商地址（测试或自托管）；空时 tavily 为 https://api.tavily.com，ddg_lite 为 https://lite.duckduckgo.com/lite/
	Pricing  Pricing
	HTTP     *http.Client // 须来自验证 Dialer.HTTPClient；nil 时用默认 Dialer（8 MiB）
	// Fake 为 fake 供应商注入结果；nil 时按查询生成确定性的占位结果。返回 *Error 时原样作为结果类别。
	Fake func(ctx context.Context, query string, maxResults int) ([]SearchResult, error)
}

type searchAdapter struct {
	cfg  SearchConfig
	http *http.Client
}

// NewSearch 构造搜索 adapter。未知供应商的 adapter 在 Resolve 时返回 fatal unsupported_provider。
func NewSearch(cfg SearchConfig) Adapter {
	if cfg.BaseURL == "" {
		switch cfg.Provider {
		case SearchTavily:
			cfg.BaseURL = "https://api.tavily.com"
		case SearchDDGLite:
			cfg.BaseURL = "https://lite.duckduckgo.com/lite/"
		}
	}
	a := &searchAdapter{cfg: cfg}
	if cfg.Provider == SearchTavily || cfg.Provider == SearchDDGLite {
		a.http = defaultClient(cfg.HTTP, DefaultModelMaxBody)
	}
	return a
}

func (a *searchAdapter) Kind() Kind       { return KindSearch }
func (a *searchAdapter) Provider() string { return a.cfg.Provider }
func (a *searchAdapter) Version() string  { return "search/" + a.cfg.Provider + "/1" }

type searchRequest struct {
	Query      string `json:"query"`
	MaxResults int    `json:"max_results"`
}

// Resolve：只接受 {query, max_results}；query 非空；max_results 缺省补 5，取值 1..20。
func (a *searchAdapter) Resolve(body []byte) ([]byte, map[string]any, error) {
	switch a.cfg.Provider {
	case SearchFake, SearchTavily, SearchDDGLite:
	default:
		return nil, nil, fatalf(http.StatusBadRequest, CodeUnsupportedProvider, "搜索供应商 %q 未声明", a.cfg.Provider)
	}
	obj, e := decodeObject(body)
	if e != nil {
		return nil, nil, e
	}
	if e := checkFields(obj, "query", "max_results"); e != nil {
		return nil, nil, e
	}
	var r searchRequest
	if json.Unmarshal(obj["query"], &r.Query) != nil || strings.TrimSpace(r.Query) == "" {
		return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "query 须为非空字符串")
	}
	defaults := map[string]any{}
	if raw, ok := obj["max_results"]; ok {
		n, e := positiveInt(raw, "max_results")
		if e != nil {
			return nil, nil, e
		}
		if n > maxSearchResults {
			return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "max_results 不得超过 %d", maxSearchResults)
		}
		r.MaxResults = int(n)
	} else {
		r.MaxResults = defaultSearchResults
		defaults["max_results"] = defaultSearchResults
	}
	out, _ := json.Marshal(r)
	return out, defaults, nil
}

// Estimate：按次计价。
func (a *searchAdapter) Estimate(resolved []byte) (int64, error) {
	if a.cfg.Pricing.SearchMicroPerRequest < 0 {
		return 0, errors.New("搜索单价为负")
	}
	return a.cfg.Pricing.SearchMicroPerRequest, nil
}

type searchReply struct {
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
}

func (a *searchAdapter) Do(ctx context.Context, resolved []byte) (Response, *Error) {
	var r searchRequest
	if json.Unmarshal(resolved, &r) != nil || r.MaxResults <= 0 {
		return Response{}, fatalf(http.StatusBadRequest, CodeInvalidRequest, "请求未经 Resolve")
	}
	var (
		results []SearchResult
		reqID   string
		resp    Response
		e       *Error
	)
	switch a.cfg.Provider {
	case SearchFake:
		results, e = a.fake(ctx, r)
	case SearchTavily:
		results, reqID, resp, e = a.tavily(ctx, r)
	case SearchDDGLite:
		results, resp, e = a.ddgLite(ctx, r)
	default:
		e = fatalf(http.StatusBadRequest, CodeUnsupportedProvider, "搜索供应商 %q 未声明", a.cfg.Provider)
	}
	if e != nil {
		return resp, e
	}
	if len(results) > r.MaxResults {
		results = results[:r.MaxResults]
	}
	if results == nil {
		results = []SearchResult{}
	}
	body, _ := json.Marshal(searchReply{Query: r.Query, Results: results})
	return Response{Body: body, Usage: Usage{Requests: 1, ResponseBytes: int64(len(body))}, UpstreamRequestID: reqID}, nil
}

func (a *searchAdapter) fake(ctx context.Context, r searchRequest) ([]SearchResult, *Error) {
	if a.cfg.Fake != nil {
		res, err := a.cfg.Fake(ctx, r.Query, r.MaxResults)
		if err != nil {
			var ue *Error
			if errors.As(err, &ue) {
				return nil, ue // 测试可直接注入某类结果
			}
			return nil, newErr(OutcomeFatal, http.StatusBadGateway, CodeUpstreamRejected, err)
		}
		return res, nil
	}
	out := make([]SearchResult, 0, r.MaxResults)
	for i := 1; i <= r.MaxResults; i++ {
		out = append(out, SearchResult{
			Title:   fmt.Sprintf("结果 %d：%s", i, r.Query),
			URL:     fmt.Sprintf("https://example.invalid/search/%d?q=%s", i, url.QueryEscape(r.Query)),
			Snippet: fmt.Sprintf("fake 供应商为查询 %q 生成的第 %d 条占位结果。", r.Query, i),
		})
	}
	return out, nil
}

// tavily：POST {BaseURL}/search，Key 走 Authorization: Bearer，不进请求体。
func (a *searchAdapter) tavily(ctx context.Context, r searchRequest) ([]SearchResult, string, Response, *Error) {
	payload, _ := json.Marshal(map[string]any{"query": r.Query, "max_results": r.MaxResults})
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(a.cfg.BaseURL, "/")+"/search", bytes.NewReader(payload))
	if err != nil {
		return nil, "", Response{}, newErr(OutcomeFatal, http.StatusBadGateway, CodeInvalidURL, errors.New("搜索上游地址不合法"))
	}
	req.Header.Set("Content-Type", contentTypeJSON)
	req.Header.Set("User-Agent", userAgent)
	if a.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}
	res, e := exchange(ctx, a.http, req, true)
	if e != nil {
		return nil, "", Response{RetryAfter: res.retryAfter}, e
	}
	var reply struct {
		RequestID string `json:"request_id"`
		Results   []struct {
			Title   string `json:"title"`
			URL     string `json:"url"`
			Content string `json:"content"`
		} `json:"results"`
	}
	if json.Unmarshal(res.body, &reply) != nil {
		return nil, "", Response{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, errors.New("搜索响应不是合法 JSON"))
	}
	out := make([]SearchResult, 0, len(reply.Results))
	for _, x := range reply.Results {
		out = append(out, SearchResult{Title: x.Title, URL: x.URL, Snippet: x.Content})
	}
	id := res.resp.Header.Get("X-Request-Id")
	if id == "" {
		id = reply.RequestID
	}
	return out, id, Response{}, nil
}

// ddgLite：GET {BaseURL}?q=...，解析结果链接与摘要。脆弱、仅演示；解析失败为 fatal。
func (a *searchAdapter) ddgLite(ctx context.Context, r searchRequest) ([]SearchResult, Response, *Error) {
	u, err := url.Parse(a.cfg.BaseURL)
	if err != nil {
		return nil, Response{}, newErr(OutcomeFatal, http.StatusBadGateway, CodeInvalidURL, errors.New("搜索上游地址不合法"))
	}
	q := u.Query()
	q.Set("q", r.Query)
	u.RawQuery = q.Encode()
	req, _ := http.NewRequest(http.MethodGet, u.String(), nil)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html")
	res, e := exchange(ctx, a.http, req, true)
	if e != nil {
		return nil, Response{RetryAfter: res.retryAfter}, e
	}
	out, err := parseDDGLite(res.body)
	if err != nil {
		return nil, Response{}, newErr(OutcomeFatal, http.StatusBadGateway, CodeUpstreamBadResponse, err)
	}
	return out, Response{}, nil
}

var (
	ddgLinkRe    = regexp.MustCompile(`(?is)<a\s([^>]*class=['"]result-link['"][^>]*)>(.*?)</a>`)
	ddgHrefRe    = regexp.MustCompile(`(?is)\bhref=['"]([^'"]*)['"]`)
	ddgSnippetRe = regexp.MustCompile(`(?is)<td[^>]*class=['"]result-snippet['"][^>]*>(.*?)</td>`)
	ddgTagRe     = regexp.MustCompile(`(?s)<[^>]*>`)
	ddgSpaceRe   = regexp.MustCompile(`\s+`)
	ddgNoResults = regexp.MustCompile(`(?i)no\s+results`)
)

// parseDDGLite 从 DuckDuckGo Lite 页面中按出现顺序配对结果链接（a.result-link）与摘要（td.result-snippet）。
// 页面明示无结果时返回空列表；既无结果链接也无"无结果"标记时视为页面结构已变化，返回错误。
func parseDDGLite(page []byte) ([]SearchResult, error) {
	links := ddgLinkRe.FindAllSubmatch(page, -1)
	if len(links) == 0 {
		if ddgNoResults.Match(page) {
			return []SearchResult{}, nil
		}
		return nil, errors.New("ddg_lite: 无法解析结果页面（页面结构可能已变化）")
	}
	snippets := ddgSnippetRe.FindAllSubmatch(page, -1)
	out := make([]SearchResult, 0, len(links))
	for i, l := range links {
		m := ddgHrefRe.FindSubmatch(l[1])
		if m == nil {
			continue
		}
		target := ddgTarget(html.UnescapeString(string(m[1])))
		if target == "" {
			continue
		}
		res := SearchResult{Title: ddgText(l[2]), URL: target}
		if i < len(snippets) {
			res.Snippet = ddgText(snippets[i][1])
		}
		out = append(out, res)
	}
	if len(out) == 0 {
		return nil, errors.New("ddg_lite: 结果链接缺少可用地址")
	}
	return out, nil
}

// ddgTarget 还原跳转链接（//duckduckgo.com/l/?uddg=<目标>）中的真实地址；只接受 http/https。
func ddgTarget(href string) string {
	if strings.HasPrefix(href, "//") {
		href = "https:" + href
	}
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	if strings.HasSuffix(u.Hostname(), "duckduckgo.com") && strings.HasPrefix(u.Path, "/l/") {
		if t := u.Query().Get("uddg"); t != "" {
			return ddgTarget(t)
		}
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	return u.String()
}

func ddgText(b []byte) string {
	s := ddgTagRe.ReplaceAllString(string(b), "")
	return strings.TrimSpace(ddgSpaceRe.ReplaceAllString(html.UnescapeString(s), " "))
}
