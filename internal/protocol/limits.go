package protocol

// 协议版本（规格 §5.2、§5.3）。
const (
	// Version 是本包实现的协议主版本。
	Version = 1
	// BootstrapVersion 是 init 引导信封的版本，永不改变。
	BootstrapVersion = 1
)

// 大小上限（规格 §5.10，按 UTF-8 字节计）。
const (
	MaxEventBytes        = 1 << 20
	MaxInitBytes         = 1 << 20
	MaxControlBytes      = 16 << 10
	MaxInlineStateBytes  = 256 << 10
	MaxRefsPerCheckpoint = 1024
	MaxArtifactPathBytes = 4096
)

// maxLineBytes 是任何消息类型上限中的最大值；超过它的行不解析，直接判为过大。
const maxLineBytes = max(MaxEventBytes, MaxInitBytes)
