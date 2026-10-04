// Package archtest 以测试固定包之间的依赖规则（代码组织设计 §3.2）。
//
// Plan 4 先固定持久化的依赖方向；其余规则随 Plan 6 加入同一文件。
package archtest

import (
	"os/exec"
	"strings"
	"testing"
)

const module = "github.com/wanghr0318-dotcom/go-agentbox"

// deps 返回包的全部传递依赖（含自身），以 GOOS=linux 计算。
func deps(t *testing.T, pkg string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", module+"/"+pkg)
	cmd.Env = append(cmd.Environ(), "GOOS=linux")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pkg, err)
	}
	return strings.Fields(string(out))
}

// TestConsumersDoNotDependOnPostgres：消费者包与引导决策不依赖 PostgreSQL 实现或数据库驱动；
// 实现（internal/persistence/postgres）反向依赖消费者的契约（设计 §1.1）。
func TestConsumersDoNotDependOnPostgres(t *testing.T) {
	forbidden := []string{module + "/internal/persistence/postgres", "github.com/jackc/pgx"}
	for _, pkg := range []string{
		"internal/api", "internal/task", "internal/runner", "internal/resource",
		"internal/ownership", "internal/persistence", "internal/datadir", "internal/blob",
	} {
		for _, d := range deps(t, pkg) {
			for _, f := range forbidden {
				if d == f || strings.HasPrefix(d, f+"/") {
					t.Errorf("%s 依赖了 %s", pkg, d)
				}
			}
		}
	}
}
