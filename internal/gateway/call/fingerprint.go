package call

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
)

// fingerprintInput 是规格 §9.4 指纹的输入。cache_directive 在 M2 恒为 null（无缓存分支，§11.2）。
type fingerprintInput struct {
	Endpoint       string          `json:"endpoint"`
	AdapterVersion string          `json:"adapter_version"`
	Resolved       resolvedInput   `json:"resolved"`
	CacheDirective *struct{}       `json:"cache_directive"`
	Body           json.RawMessage `json:"body"`
}

type resolvedInput struct {
	Provider        string         `json:"provider"`
	Model           string         `json:"model"`
	AppliedDefaults map[string]any `json:"applied_defaults"`
}

// Fingerprint 返回 sha256(JCS({endpoint, adapter_version, resolved:{provider, model, applied_defaults},
// cache_directive:null, body}))（规格 §9.4，RFC 8785）的小写十六进制。body 是 Worker 提交的原始请求体，
// 按 JCS 规范化（字段顺序与空白不影响指纹）；含重复属性名时返回 jcs.ErrDuplicateKey。
func Fingerprint(endpoint, adapterVersion, provider, model string, appliedDefaults map[string]any, body []byte) (string, error) {
	if appliedDefaults == nil {
		appliedDefaults = map[string]any{}
	}
	canon, err := jcs.Canonical(fingerprintInput{
		Endpoint:       endpoint,
		AdapterVersion: adapterVersion,
		Resolved:       resolvedInput{Provider: provider, Model: model, AppliedDefaults: appliedDefaults},
		Body:           json.RawMessage(body),
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}
