package call

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
)

// CacheDirectiveNoCache 是请求头 X-Agentbox-Cache 唯一接受的取值（§11.4）：不读旧缓存、不加入合并，结果仍写入缓存。
const CacheDirectiveNoCache = "no-cache"

// fingerprintInput 是规格 §9.4 指纹的输入。cache_directive 没有指令时为 null，no-cache 时为 "no-cache"
// （缓存指令计入指纹，§11.4）：没有指令的请求与 M2 的指纹相同。
type fingerprintInput struct {
	Endpoint       string          `json:"endpoint"`
	AdapterVersion string          `json:"adapter_version"`
	Resolved       resolvedInput   `json:"resolved"`
	CacheDirective *string         `json:"cache_directive"`
	Body           json.RawMessage `json:"body"`
}

type resolvedInput struct {
	Provider        string         `json:"provider"`
	Model           string         `json:"model"`
	AppliedDefaults map[string]any `json:"applied_defaults"`
}

// Fingerprint 返回 sha256(JCS({endpoint, adapter_version, resolved:{provider, model, applied_defaults},
// cache_directive, body}))（规格 §9.4，RFC 8785）的小写十六进制。cacheDirective 为空时 cache_directive 为 null。
// body 是 Worker 提交的原始请求体，按 JCS 规范化（字段顺序与空白不影响指纹）；含重复属性名时返回 jcs.ErrDuplicateKey。
func Fingerprint(endpoint, adapterVersion, provider, model string, appliedDefaults map[string]any, cacheDirective string, body []byte) (string, error) {
	if appliedDefaults == nil {
		appliedDefaults = map[string]any{}
	}
	in := fingerprintInput{
		Endpoint:       endpoint,
		AdapterVersion: adapterVersion,
		Resolved:       resolvedInput{Provider: provider, Model: model, AppliedDefaults: appliedDefaults},
		Body:           json.RawMessage(body),
	}
	if cacheDirective != "" {
		in.CacheDirective = &cacheDirective
	}
	canon, err := jcs.Canonical(in)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}
