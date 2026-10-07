package cache

import (
	"net/textproto"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

// MaxCacheableBytes 是可缓存响应的大小上限（§19：2 MiB）。
const MaxCacheableBytes int64 = 2 << 20

// Response 是准入与新鲜度判定所需的上游响应元数据。Header 的类型是 http.Header 的底层类型（调用方以
// map[string][]string(h) 转换，本包不导入 net/http）；它只读，本包不修改它，按名字不区分大小写查找。
//
// Date 是调用方已解析的 Date 头（零值表示由 Lifetime 从 Header 解析）；RequestTime 与 ResponseTime 是
// Gateway 时钟上发出请求与收到响应的时刻（RFC 9111 §4.2.3 的 request_time 与 response_time）。
type Response struct {
	Status                          int
	Header                          map[string][]string
	Size                            int64
	Date, RequestTime, ResponseTime time.Time
}

// 准入拒绝原因（稳定字符串，供日志与指标使用）。
const (
	ReasonKind           = "kind_not_cacheable"     // 只缓存 search 与 fetch（不缓存模型调用）
	ReasonStatus         = "status_not_200"         // 只缓存 200；不缓存错误响应
	ReasonSize           = "size_out_of_range"      // > 2 MiB 或大小未知
	ReasonSetCookie      = "set_cookie"             // 含 Set-Cookie
	ReasonNoStore        = "cache_control_no_store" // Cache-Control: no-store
	ReasonPrivate        = "cache_control_private"  // Cache-Control: private
	ReasonNoCache        = "cache_control_no_cache" // Cache-Control: no-cache
	ReasonVaryStar       = "vary_star"              // Vary: *
	ReasonVaryDimension  = "vary_dimension"         // Vary 含 Accept-Encoding 以外的维度
	ReasonURLInvalid     = "url_invalid"            // 抓取 URL 缺失或无法解析
	ReasonURLCredentials = "url_credentials"        // URL 含 userinfo 或凭据类查询参数
)

// Admit 按 §11.3 判定响应能否进入缓存；不可缓存时 reason 为上面的 Reason* 之一。
//
// 两类调用都要求状态 200 与 0 ≤ Size ≤ 2 MiB。抓取（只由 fetch adapter 以 GET 发出，"仅 GET" 由构造保证）
// 另检查响应头（Set-Cookie、Cache-Control 的 no-store/private/no-cache、Vary）与 URL（userinfo、凭据类
// 查询参数）。搜索是对供应商 API 的调用，寿命取 adapter 的 TTL 而不是响应头（§11.3），这里只缓存成功结果。
// 新鲜度另由 Lifetime 判定。
func Admit(kind upstream.Kind, rawURL string, r Response) (ok bool, reason string) {
	if kind != upstream.KindSearch && kind != upstream.KindFetch {
		return false, ReasonKind
	}
	if r.Status != 200 {
		return false, ReasonStatus
	}
	if r.Size < 0 || r.Size > MaxCacheableBytes {
		return false, ReasonSize
	}
	if kind == upstream.KindSearch {
		return true, ""
	}
	if len(headerValues(r.Header, "Set-Cookie")) > 0 || len(headerValues(r.Header, "Set-Cookie2")) > 0 {
		return false, ReasonSetCookie
	}
	for _, d := range headerTokens(r.Header, "Cache-Control") {
		name, _, _ := strings.Cut(d, "=")
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "no-store":
			return false, ReasonNoStore
		case "private":
			return false, ReasonPrivate
		case "no-cache":
			return false, ReasonNoCache
		}
	}
	for _, v := range headerTokens(r.Header, "Vary") {
		switch {
		case v == "*":
			return false, ReasonVaryStar
		case !strings.EqualFold(v, "Accept-Encoding"):
			return false, ReasonVaryDimension
		}
	}
	if rawURL == "" {
		return false, ReasonURLInvalid
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, ReasonURLInvalid
	}
	if u.User != nil {
		return false, ReasonURLCredentials
	}
	if bad, parsed := queryHasCredential(u.RawQuery); !parsed {
		return false, ReasonURLInvalid
	} else if bad {
		return false, ReasonURLCredentials
	}
	return true, ""
}

// headerValues 返回名字与 name 不区分大小写相等的全部字段行：先取规范形式的键（net/http 产生的 Header
// 总是规范形式），再补上其他大小写写法的键（按键排序，结果确定）。
func headerValues(h map[string][]string, name string) []string {
	canon := textproto.CanonicalMIMEHeaderKey(name)
	out := append([]string(nil), h[canon]...)
	var others []string
	for k := range h {
		if k != canon && strings.EqualFold(k, name) {
			others = append(others, k)
		}
	}
	sort.Strings(others)
	for _, k := range others {
		out = append(out, h[k]...)
	}
	return out
}

// headerTokens 返回同名头全部字段行按逗号拆开、去空白后的非空元素。引号内的逗号不拆分。
func headerTokens(h map[string][]string, name string) []string {
	var out []string
	for _, line := range headerValues(h, name) {
		for _, t := range splitHeaderList(line) {
			if t = strings.TrimSpace(t); t != "" {
				out = append(out, t)
			}
		}
	}
	return out
}

// splitHeaderList 按逗号拆分字段值，跳过 quoted-string 内的逗号（含反斜杠转义）。
func splitHeaderList(s string) []string {
	var out []string
	start, quoted := 0, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case quoted && c == '\\':
			i++
		case c == '"':
			quoted = !quoted
		case c == ',' && !quoted:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// credentialParams 是视为凭据的查询参数名（小写、'-' 归一为 '_' 后精确匹配）。
var credentialParams = map[string]bool{
	"token": true, "sig": true, "signature": true, "key": true, "auth": true,
	"apikey": true, "api_key": true, "access_key": true, "secret": true, "password": true, "passwd": true,
	"pwd": true, "jwt": true, "session": true, "sessionid": true, "session_id": true, "sid": true,
	"credential": true, "credentials": true, "authorization": true, "code": true,
}

// credentialPrefixes 与 credentialSubstrings 补充精确匹配：云存储预签名参数（X-Amz-*、X-Goog-*）以及
// 名字里带 token/secret/password/signature 的参数（access_token、client_secret 等）。
var (
	credentialPrefixes   = []string{"x_amz_", "x_goog_"}
	credentialSubstrings = []string{"token", "secret", "password", "signature", "credential"}
)

// queryHasCredential 判断原样查询串中是否有凭据类参数名；名字无法解码时 parsed 为 false。
func queryHasCredential(rawQuery string) (bad, parsed bool) {
	for _, pair := range strings.FieldsFunc(rawQuery, func(r rune) bool { return r == '&' || r == ';' }) {
		rawName, _, _ := strings.Cut(pair, "=")
		name, err := url.QueryUnescape(rawName)
		if err != nil {
			return false, false
		}
		if isCredentialParam(name) {
			return true, true
		}
	}
	return false, true
}

func isCredentialParam(name string) bool {
	n := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "-", "_")
	if credentialParams[n] {
		return true
	}
	for _, p := range credentialPrefixes {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	for _, s := range credentialSubstrings {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}
