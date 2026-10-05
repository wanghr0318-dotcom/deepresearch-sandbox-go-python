// Package cache 实现 Gateway 的共享缓存（规格 §11）：缓存键、准入规则、RFC 9111 子集新鲜度、
// 带 HMAC 签名的缓存值、密钥文件，以及 Redis 适配与熔断。
//
// 本包不记账、不授权，不依赖 persistence 与 gateway/call，也不导入 net/http（Response.Header 是
// http.Header 的底层类型 map[string][]string）。
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
)

// FetchAcceptEncoding 是 Gateway 抓取时固定发送的 Accept-Encoding；它计入抓取的缓存键（§11.3）。
const FetchAcceptEncoding = "gzip"

// keyAcceptEncodingField 是抓取键材料中记录 Accept-Encoding 的字段，参数自身不得占用。
const keyAcceptEncodingField = "accept_encoding"

// Key 返回缓存键 `v1:{search|fetch}:{provider}:{adapter_version}:sha256(JCS(规范化参数))`（§11.3）。
//
// params 是 adapter Resolve 后的 JSON 对象。规范化：按 RFC 8785 排序与编码（重复属性名被拒绝）；抓取的
// `url` 字段经 NormalizeURL 规范化，并加入固定的 Accept-Encoding。只有 search 与 fetch 可缓存；
// provider 与 adapterVersion 不得为空或含 ':'、空白与控制字符（避免键段之间产生歧义）。
func Key(kind upstream.Kind, provider, adapterVersion string, params []byte) (string, error) {
	if kind != upstream.KindSearch && kind != upstream.KindFetch {
		return "", fmt.Errorf("cache: %q 类调用不可缓存", kind)
	}
	if err := checkKeySegment("provider", provider); err != nil {
		return "", err
	}
	if err := checkKeySegment("adapter_version", adapterVersion); err != nil {
		return "", err
	}
	// 先整体规范化一次：拒绝重复属性名与非 I-JSON 输入（解码到 map 会静默丢弃重复项）。
	canon, err := jcs.Canonical(json.RawMessage(params))
	if err != nil {
		return "", fmt.Errorf("cache: 参数: %w", err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(canon, &obj); err != nil || obj == nil {
		return "", errors.New("cache: 参数须为 JSON 对象")
	}
	if kind == upstream.KindFetch {
		var raw string
		u, ok := obj["url"]
		if !ok || json.Unmarshal(u, &raw) != nil {
			return "", errors.New("cache: 抓取参数须含字符串 url")
		}
		norm, err := NormalizeURL(raw)
		if err != nil {
			return "", err
		}
		if _, taken := obj[keyAcceptEncodingField]; taken {
			return "", fmt.Errorf("cache: 抓取参数不得含保留字段 %s", keyAcceptEncodingField)
		}
		nu, err := json.Marshal(norm)
		if err != nil {
			return "", fmt.Errorf("cache: 编码 url: %w", err)
		}
		ae, err := json.Marshal(FetchAcceptEncoding)
		if err != nil {
			return "", fmt.Errorf("cache: 编码 accept_encoding: %w", err)
		}
		obj["url"], obj[keyAcceptEncodingField] = nu, ae
	}
	material, err := jcs.Canonical(obj)
	if err != nil {
		return "", fmt.Errorf("cache: 参数: %w", err)
	}
	sum := sha256.Sum256(material)
	return "v1:" + string(kind) + ":" + provider + ":" + adapterVersion + ":" + hex.EncodeToString(sum[:]), nil
}

func checkKeySegment(name, s string) error {
	if s == "" {
		return fmt.Errorf("cache: %s 为空", name)
	}
	for _, r := range s {
		if r == ':' || r <= ' ' || r == 0x7f {
			return fmt.Errorf("cache: %s %q 含 ':'、空白或控制字符", name, s)
		}
	}
	return nil
}

// NormalizeURL 规范化抓取 URL（§11.3）：scheme 与 host 小写、去掉默认端口（http 80、https 443）、
// 去掉 fragment；路径与查询串原样保留。只接受带 host 的 http/https 绝对 URL。
func NormalizeURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("cache: URL 无法解析: %w", err)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("cache: 只支持 http/https URL，得到 scheme %q", u.Scheme)
	}
	if u.Opaque != "" || u.Host == "" {
		return "", errors.New("cache: URL 须为带 host 的绝对 URL")
	}
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if host == "" {
		return "", errors.New("cache: URL 的 host 为空")
	}
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		port = ""
	}
	if strings.Contains(host, ":") { // IPv6 字面量
		host = "[" + host + "]"
	}
	if port != "" {
		host += ":" + port
	}
	u.Host = host
	u.Fragment, u.RawFragment = "", ""
	return u.String(), nil
}
