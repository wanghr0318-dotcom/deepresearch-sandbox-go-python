//go:build linux

package rootfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
)

// TemplateDigest 是 exec 模板的摘要（Plan 15 D5，规格 §10.1 指纹中的 image_digest）：
//
//	sha256(JCS({"paths": 排序后的模板路径, "python3": 沙箱视图解析出的宿主真实路径, "python3_sha256": 该文件内容哈希}))
//
// 以十六进制返回。模板须通过 EnsureExec。它只覆盖路径集合与解释器本身，不覆盖其余宿主文件的内容
// （例如标准库或共享库的升级）：这是"同一模板"的近似标识，server 启动时计算一次。
func TemplateDigest(t Template) (string, error) {
	py, err := t.ensureExec()
	if err != nil {
		return "", err
	}
	f, err := os.Open(t.host(py))
	if err != nil {
		return "", fmt.Errorf("模板摘要: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("模板摘要: 读取 %s: %w", py, err)
	}
	paths := append([]string(nil), t.Paths...)
	sort.Strings(paths)
	b, err := jcs.Canonical(struct {
		Paths    []string `json:"paths"`
		Python3  string   `json:"python3"`
		PySHA256 string   `json:"python3_sha256"`
	}{paths, py, hex.EncodeToString(h.Sum(nil))})
	if err != nil {
		return "", fmt.Errorf("模板摘要: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
