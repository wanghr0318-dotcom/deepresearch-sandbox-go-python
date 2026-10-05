package task

// 本文件声明 task actor 对 Gateway 入口的窄接口（规格 §9.1；代码组织 §6"撤销访问：决策者 task actor，
// 执行者 gateway/edge"），并给出 M1 的 fake 实现：fake 不建立 listener，只记录调用。

import (
	"context"
	"sync"
)

// Access 是 attempt 的 Gateway 入口（M2 由 gateway/edge 实现，装配在 internal/app）。
//
//   - Bind 在创建环境之前为 attempt 建立入口，返回宿主侧 socket 路径（作为 Mounts.GatewaySocket 交给
//     provider，bind-mount 到环境内的 /run/agentbox/gateway.sock）；envID 是该 attempt 的环境，实现据此
//     核对 attempt 的归属。返回空路径表示没有入口（不挂载）。
//   - Revoke 关闭该 attempt 绑定的全部连接与 listener（幂等）。actor 只在 Store.RevokeAttemptAccess 已提交
//     （或被确定拒绝）之后调用它：持久化检查才是执行点，关闭连接只是清理（§9.1）。reason 是离开原因，
//     RevokeReasonCancel 时实现取消该 attempt 的在途上游 try，其余原因让 try 继续至期限并结算（§9.1 表）。
//
// actor 只调用这两个方法，不关闭 socket（代码组织 §5）。
type Access interface {
	Bind(ctx context.Context, attemptID, envID string) (socketPath string, err error)
	Revoke(ctx context.Context, attemptID, reason string) error
}

// RevokeReasonCancel 是"用户取消任务"的撤销原因（取消生效时的撤销）。取值与 gateway/call.ReasonCancel
// 相同（task 不导入 gateway，由装配测试核对二者一致）。
const RevokeReasonCancel = "cancel"

// FakeAccess 是 M1 的 Access：不建立 listener，只按调用顺序记录绑定与撤销。Bind 返回 Socket（零值为空，
// 即不挂载 Gateway socket）。并发安全。
type FakeAccess struct {
	Socket string

	mu      sync.Mutex
	bound   []string
	revoked []string
	reasons []string
}

var _ Access = (*FakeAccess)(nil)

// Bind 记录绑定并返回 Socket。
func (f *FakeAccess) Bind(_ context.Context, attemptID, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bound = append(f.bound, attemptID)
	return f.Socket, nil
}

// Revoke 记录撤销与原因。
func (f *FakeAccess) Revoke(_ context.Context, attemptID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = append(f.revoked, attemptID)
	f.reasons = append(f.reasons, reason)
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

// RevokeReasons 返回每次撤销的原因（与 Revoked 一一对应）。
func (f *FakeAccess) RevokeReasons() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.reasons...)
}
