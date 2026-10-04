package task

// 本文件声明 task actor 对 Gateway 入口的窄接口（规格 §9.1；代码组织 §6"撤销访问：决策者 task actor，
// 执行者 gateway/edge"），并给出 M1 的 fake 实现：M1 没有 Gateway，fake 不建立 listener，只记录调用。

import (
	"context"
	"sync"
)

// Access 是 attempt 的 Gateway 入口。Bind 在启动 Worker 之前为 attempt 建立入口并返回 Worker 可见的
// socket 路径；Revoke 在停止环境之前撤销该入口并断开已绑定的连接（幂等）。actor 只调用这两个方法，
// 不关闭 socket（代码组织 §5）。M2 由 gateway/edge 实现。
type Access interface {
	Bind(ctx context.Context, attemptID string) (socketPath string, err error)
	Revoke(ctx context.Context, attemptID string) error
}

// FakeGatewaySocket 是 FakeAccess.Bind 返回的路径（规格 §3.1 中环境内的 Gateway socket）。M1 不建立
// listener，Worker 不应连接它。
const FakeGatewaySocket = "/run/agentbox/gateway.sock"

// FakeAccess 是 M1 的 Access：不建立 listener，只按调用顺序记录绑定与撤销。并发安全。
type FakeAccess struct {
	mu      sync.Mutex
	bound   []string
	revoked []string
}

var _ Access = (*FakeAccess)(nil)

// Bind 记录绑定并返回 FakeGatewaySocket。
func (f *FakeAccess) Bind(_ context.Context, attemptID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bound = append(f.bound, attemptID)
	return FakeGatewaySocket, nil
}

// Revoke 记录撤销。
func (f *FakeAccess) Revoke(_ context.Context, attemptID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = append(f.revoked, attemptID)
	return nil
}

// Bound 返回已绑定的 attempt（按调用顺序）。
func (f *FakeAccess) Bound() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bound...)
}

// Revoked 返回已撤销的 attempt（按调用顺序，可能重复）。
func (f *FakeAccess) Revoked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.revoked...)
}
