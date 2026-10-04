// Package faultinject 是系统级故障实验（规格 §16.4 E5）的命名钩子点：在 AGENTBOX_FAULT=<点>:<次数>
// 指定的点第 n 次到达时以 SIGKILL 杀死本进程，模拟 server 在该处崩溃。
//
// 本包编译进生产代码，但默认是空操作：只有同时满足"进程调用了 Enable"与"设置了环境变量"才生效。
// 生产入口 cmd/agentbox 从不调用 Enable；只有只供测试的 tests/e2e/agentbox-e2e 调用（archtest 检查）。
// 钩子点在各包中各占一行（Point(<常量>)），不改变所在代码的行为。
package faultinject

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Env 是故障声明的环境变量：<点>:<次数>，例如 "verdict.before:1"。
const Env = "AGENTBOX_FAULT"

// 钩子点（§16.4 E5）。"before"/"after" 指相应的持久化事务或物理操作之前、之后。
const (
	AttemptCreateBefore    = "attempt_create.before"    // task actor：CreateAttempt 事务之前
	AttemptCreateAfter     = "attempt_create.after"     // task actor：CreateAttempt 事务之后
	EnvCreateBefore        = "env_create.before"        // coordinator：provider.Create 之前（intent 已记录）
	EnvCreateAfter         = "env_create.after"         // coordinator：provider.Create 之后（intent 未结束）
	WorkerStarted          = "worker.started"           // runner：StartExec 成功之后（init 未发送）
	CheckpointCommitBefore = "checkpoint_commit.before" // runner：CommitCheckpoint 事务之前
	CheckpointCommitAfter  = "checkpoint_commit.after"  // runner：CommitCheckpoint 事务之后（回复未发送）
	EnvStopBefore          = "env_stop.before"          // coordinator：provider.Stop 之前
	EnvStopAfter           = "env_stop.after"           // coordinator：provider.Stop 确认之后（stopped_at 未记录）
	VerdictBefore          = "verdict.before"           // task actor：FinalizeAttempt 事务之前
	VerdictAfter           = "verdict.after"            // task actor：FinalizeAttempt 事务之后
	CleanupDestroyed       = "cleanup.destroyed"        // cleanup loop：Destroy 完成之后（cleanup_state 未记录）
)

// Points 是全部钩子点。
var Points = []string{
	AttemptCreateBefore, AttemptCreateAfter, EnvCreateBefore, EnvCreateAfter, WorkerStarted,
	CheckpointCommitBefore, CheckpointCommitAfter, EnvStopBefore, EnvStopAfter, VerdictBefore, VerdictAfter,
	CleanupDestroyed,
}

var (
	armed  atomic.Bool
	mu     sync.Mutex
	target string
	nth    int
	hits   int
)

// Enable 读取 AGENTBOX_FAULT 并在设置时武装钩子点；未设置时保持空操作。声明格式错误或点名未知时返回
// 错误（不武装）。只有只供测试的 main 调用它。
func Enable() error { return enable(os.Getenv(Env)) }

func enable(spec string) error {
	if spec == "" {
		return nil
	}
	point, n, err := parse(spec)
	if err != nil {
		return err
	}
	mu.Lock()
	target, nth, hits = point, n, 0
	mu.Unlock()
	armed.Store(true)
	return nil
}

func parse(spec string) (string, int, error) {
	point, count, ok := strings.Cut(spec, ":")
	if !ok {
		return "", 0, fmt.Errorf("faultinject: %s=%q 须为 <点>:<次数>", Env, spec)
	}
	n, err := strconv.Atoi(count)
	if err != nil || n < 1 {
		return "", 0, fmt.Errorf("faultinject: %s=%q 的次数须为正整数", Env, spec)
	}
	for _, p := range Points {
		if p == point {
			return point, n, nil
		}
	}
	return "", 0, fmt.Errorf("faultinject: 未知的钩子点 %q", point)
}

// Point 标记钩子点 name。武装且 name 是目标点时计数，第 n 次到达时先在 stderr 写一行记录，再以
// SIGKILL 杀死本进程（不返回）。未武装时是空操作。
func Point(name string) {
	if !armed.Load() {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if name != target {
		return
	}
	hits++
	if hits == nth {
		fmt.Fprintf(os.Stderr, "faultinject: SIGKILL at %s:%d\n", name, nth)
		kill()
	}
}
