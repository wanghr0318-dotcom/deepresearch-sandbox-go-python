// Package provider 定义环境生命周期的契约：类型、错误与完整操作集合（Provider 契约
// docs/design/2026-10-05-provider-contract.md）。本包只依赖标准库，不含实现与 I/O；
// 实现位于 internal/provider/local，消费者（resource、runner）各自声明所需的窄接口。
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"syscall"
	"time"
)

// EnvKind 是环境类型（规格 §2）。
type EnvKind string

const (
	KindTask    EnvKind = "task"
	KindSession EnvKind = "session"
	KindExec    EnvKind = "exec"
)

// EnvSpec 描述一个环境；所有字段由控制面在调用前确定并持久化（intent、UID 范围）。
type EnvSpec struct {
	EnvID     string
	InstallID string // 写入 owner.json，决定 cgroup 路径 agentbox-<install_id>/env-<env_id>
	Kind      EnvKind
	UIDBase   uint32 // UID 范围起点
	UIDSize   uint32 // 默认 4096
	Template  string // 只读 rootfs 模板标识
	Limits    Limits
	Mounts    Mounts
}

// Limits 是环境的资源限制（规格 §4.5）；memory.swap.max 固定为 0，cpu.max 的 period 固定为 100000。
type Limits struct {
	MemoryMax  int64
	PidsMax    int64
	CPUQuotaUs int64
	NoFile     uint64
	FSize      uint64 // 仅 exec 环境（RLIMIT_FSIZE）
	TmpBytes   int64  // /tmp、/run tmpfs 限额
}

// Mounts 按规格 §4.5：编排环境有 workspace 与 Gateway socket；exec 环境有 /in（只读）与 /out（tmpfs）。
type Mounts struct {
	Workspace     string // 宿主目录，挂到 /workspace；exec 为空
	GatewaySocket string // 宿主 socket 路径，挂到 /run/agentbox/gateway.sock；exec 为空
	In            string // 仅 exec：只读输入目录
	OutBytes      int64  // 仅 exec：/out tmpfs 大小
}

// ExecSpec 描述环境内一次执行。workload 身份固定为映射 uid/gid 1000，seccomp 配置由环境类型决定，
// 两者都不由调用方指定。
type ExecSpec struct {
	ExecID string // 调用方生成，用于控制通道 start/start_ack 关联与日志
	Argv   []string
	Env    []string
	Dir    string
}

// ExitStatus 是进程退出状态（规格 §4.1）。Signal 非 0 表示被该信号终止。
type ExitStatus struct {
	Code   int
	Signal syscall.Signal
}

// ResourceDiag 由 Runtime 读取环境 cgroup（规格 §4.1）。
type ResourceDiag struct {
	OOMKillDelta uint64
	OOMObserved  bool
	CPUUsageUsec uint64
}

// ExecHandle 是一次执行的句柄（规格 §4.1）。AttemptRunner 是 stdout/stderr 的唯一读取者。
type ExecHandle interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait() (ExitStatus, error) // 进程退出状态或控制连接错误
	Terminate(grace time.Duration) error
}

// EnvInfo 是 List 返回的一个环境（owner.json 属于本安装）。
type EnvInfo struct {
	EnvID    string
	Kind     EnvKind
	Complete bool // 各层齐全且 init 就绪；false 即 Create 会返回 ErrIncomplete 的残留
	Running  bool // 环境 cgroup 存在且 populated 为 1
}

// Owner 是扫描项的归属分类（规格 §14.1 扫描表）。
type Owner int

const (
	OwnedComplete Owner = iota + 1 // 属于本安装且完整
	OwnedPartial                   // 属于本安装但不完整（残留）
	Foreign                        // 属于其他安装
	Unknown                        // 无法判定归属（例如无 owner.json）
)

// ScanItem 是独立原始扫描中的一项。
type ScanItem struct {
	Layer string // "env_dir" | "mount" | "cgroup" | "listener" | "uid_files"
	Path  string
	EnvID string // 能识别时填写
	Owner Owner
}

// ScanReport 是独立原始扫描的结果（规格 §14.1 第 5 步）。
type ScanReport struct {
	Items []ScanItem
}

// Provider 是完整的环境操作集合（契约第 3 节）；消费者各自声明子集。
type Provider interface {
	Create(ctx context.Context, spec EnvSpec) (EnvInfo, error)
	StartExec(ctx context.Context, envID string, spec ExecSpec) (ExecHandle, error)
	Stop(ctx context.Context, envID string) error
	Destroy(ctx context.Context, envID string) error
	List(ctx context.Context) ([]EnvInfo, error)
	Scan(ctx context.Context) (ScanReport, error)
	ResourceDiag(ctx context.Context, envID string) (ResourceDiag, error)
}

// 错误（契约第 4 节）。ErrNotFound 只说明本次操作需要的那一层资源不存在，不说明其他层是否已清理。
var (
	ErrNotFound        = errors.New("provider: 不存在")
	ErrIncomplete      = errors.New("provider: 环境不完整")
	ErrConflict        = errors.New("provider: 同一 env_id 已存在且 spec 不同")
	ErrForeign         = errors.New("provider: 资源不属于本安装或归属无法判定")
	ErrStopping        = errors.New("provider: 环境已停止接受新的执行")
	ErrStartFailed     = errors.New("provider: 启动失败，workload 未运行")
	ErrControlLost     = errors.New("provider: 控制连接在确认前断开")
	ErrStopUnconfirmed = errors.New("provider: 期限内未确认执行树清空")
	ErrNotStopped      = errors.New("provider: 环境尚未确认停止")
)

// StartError 是 init 回复的 start_err：启动序列某步失败，workload 未运行。Reason 的取值由
// Plan 1B 的结论确定，本契约只规定"workload 未运行"。
type StartError struct{ Reason string }

func (e *StartError) Error() string        { return "provider: 启动失败: " + e.Reason }
func (e *StartError) Is(target error) bool { return target == ErrStartFailed }

// SpecHash 是 EnvSpec 的规范化哈希（JSON 编码后的 SHA-256，十六进制），写入 owner.json 并用于
// Create 的幂等比较。结构体字段顺序固定，因此编码稳定。
func SpecHash(s EnvSpec) string {
	b, err := json.Marshal(s)
	if err != nil { // EnvSpec 只含可编码的基本类型
		panic(fmt.Sprintf("provider: 编码 EnvSpec: %v", err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Validate 检查必填字段与按 Kind 的挂载组合（契约第 2 节、规格 §4.5）。
func (s EnvSpec) Validate() error {
	switch {
	case s.EnvID == "" || s.InstallID == "" || s.Template == "":
		return errors.New("provider: EnvSpec 缺少 env_id、install_id 或 template")
	case s.UIDSize == 0:
		return errors.New("provider: EnvSpec 的 UID 范围为空")
	}
	switch s.Kind {
	case KindTask, KindSession:
		if s.Mounts.In != "" || s.Mounts.OutBytes != 0 {
			return fmt.Errorf("provider: %s 环境不能有 /in 或 /out", s.Kind)
		}
	case KindExec:
		if s.Mounts.Workspace != "" || s.Mounts.GatewaySocket != "" {
			return errors.New("provider: exec 环境不能有 workspace 或 Gateway socket")
		}
	default:
		return fmt.Errorf("provider: 未知的环境类型 %q", s.Kind)
	}
	return nil
}
