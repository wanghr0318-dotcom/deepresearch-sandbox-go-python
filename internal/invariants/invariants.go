// Package invariants 实现 `agentbox verify-invariants` 的检查（规格 §16.3，M1 范围：I1、I2、I4–I8、I16；
// M2 Gateway：I3 的 task 层账本、I14 的 journal 部分——已完成调用的结果不变且在 blobs 与 scope_blobs(task) 中；
// M3 缓存：I14 的缓存部分——source = cache|coalesced 的调用同样登记并授权到 scope_blobs(task)、结果是 Tx2 记录的
// blob 且没有 try；I15——恢复完成后不存在没有活跃 attempt 的进行中 resolving 调用）。
//
// 资源类检查独立扫描实际资源（provider.Scan）与 BlobStore 内容，不只从数据库推导。每条违反带类别：
// [A] 始终成立、[B] 期限内成立、[Q] 静止时成立；Q 类只在 quiescent 为真时检查。
package invariants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// Violation 是一条不变量违反。
type Violation struct {
	ID     string // 例如 "I4"
	Class  string // "A" | "B" | "Q"
	Detail string
}

// BlobRef 是应当存在于 BlobStore 的内容。
type BlobRef struct {
	SHA256 string
	Size   int64
	Origin string // 引用方，例如 "artifact t1/report@2"
}

// Store 是不变量检查需要的数据库读取（实现位于 internal/persistence/postgres）。
type Store interface {
	// DBViolations 返回只依赖数据库即可判定的违反：I2、I3、I4、I6（授权与指针）、I7、I8、I14（登记与授权，含缓存
	// 来源）、I15、I16。可以含 Q 类违反：Verify 只在 quiescent 为真时保留它们。
	DBViolations(ctx context.Context) ([]Violation, error)
	// CleanedEnvIDs 返回 cleanup_state = done 的环境（I1）。
	CleanedEnvIDs(ctx context.Context) ([]string, error)
	// ReferencedBlobs 返回已登记产物、已提交 checkpoint 引用与已完成调用结果的 blob（I5、I6、I14 的内容部分；
	// Origin 分别以 "artifact"、"checkpoint"、"call" 开头）。
	ReferencedBlobs(ctx context.Context) ([]BlobRef, error)
}

// Scanner 是 provider 的独立原始扫描。
type Scanner interface {
	Scan(ctx context.Context) (provider.ScanReport, error)
}

// Blobs 读取 BlobStore 的内容。
type Blobs interface {
	Open(sha256 string) (io.ReadCloser, error)
}

// Verify 运行全部检查。quiescent 为真时另检查 Q 类（实验结束、清理完成后）。
func Verify(ctx context.Context, s Store, scan Scanner, blobs Blobs, quiescent bool) ([]Violation, error) {
	db, err := s.DBViolations(ctx)
	if err != nil {
		return nil, fmt.Errorf("invariants: 数据库检查: %w", err)
	}
	var out []Violation
	for _, v := range db {
		if v.Class != "Q" || quiescent {
			out = append(out, v)
		}
	}
	refs, err := s.ReferencedBlobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("invariants: 读取 blob 引用: %w", err)
	}
	for _, r := range refs {
		if v := checkBlob(blobs, r); v != nil {
			out = append(out, *v)
		}
	}
	if quiescent {
		vs, err := cleanedEnvsHaveNoResources(ctx, s, scan)
		if err != nil {
			return nil, err
		}
		out = append(out, vs...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// checkBlob 复算 blob 内容的哈希与大小（I5：固定输出以正确哈希存在；I6：checkpoint 引用的内容完整；
// I14：已记录的调用结果不改变）。
func checkBlob(blobs Blobs, r BlobRef) *Violation {
	id := "I5"
	switch {
	case strings.HasPrefix(r.Origin, "checkpoint"):
		id = "I6"
	case strings.HasPrefix(r.Origin, "call"):
		id = "I14"
	}
	rc, err := blobs.Open(r.SHA256)
	if err != nil {
		return &Violation{ID: id, Class: "A", Detail: fmt.Sprintf("%s 引用的 blob %s 无法读取：%v", r.Origin, r.SHA256, err)}
	}
	defer func() { _ = rc.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, rc)
	switch {
	case err != nil:
		return &Violation{ID: id, Class: "A", Detail: fmt.Sprintf("%s 引用的 blob %s 读取失败：%v", r.Origin, r.SHA256, err)}
	case hex.EncodeToString(h.Sum(nil)) != r.SHA256:
		return &Violation{ID: id, Class: "A", Detail: fmt.Sprintf("%s 引用的 blob %s 内容哈希不符", r.Origin, r.SHA256)}
	case r.Size >= 0 && n != r.Size:
		return &Violation{ID: id, Class: "A", Detail: fmt.Sprintf("%s 引用的 blob %s 大小为 %d，登记为 %d", r.Origin, r.SHA256, n, r.Size)}
	}
	return nil
}

// cleanedEnvsHaveNoResources：I1——cleanup_state = done 的环境不存在任何实际挂载、cgroup、listener 或目录。
func cleanedEnvsHaveNoResources(ctx context.Context, s Store, scan Scanner) ([]Violation, error) {
	ids, err := s.CleanedEnvIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("invariants: 读取已清理环境: %w", err)
	}
	report, err := scan.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("invariants: 扫描: %w", err)
	}
	cleaned := make(map[string]bool, len(ids))
	for _, id := range ids {
		cleaned[id] = true
	}
	var out []Violation
	for _, it := range report.Items {
		if it.EnvID != "" && cleaned[it.EnvID] {
			out = append(out, Violation{ID: "I1", Class: "Q",
				Detail: fmt.Sprintf("环境 %s 已清理完成，但仍有 %s %s", it.EnvID, it.Layer, it.Path)})
		}
	}
	return out, nil
}
