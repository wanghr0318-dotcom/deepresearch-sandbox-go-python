// Package ownership 实现安装身份引导与校验（规格 §7.4；设计 §3.3；修订提案
// docs/design/2026-10-05-installation-identity-amendment.md）。
//
// Decide 是纯函数，覆盖 §7.4 决策表的全部情况；Bootstrap 经三个窄接口编排引导步骤：
// InstallStore（数据库侧，由 internal/persistence/postgres 实现）、IDFile 与 TokenFile
// （数据目录侧，由 internal/datadir 实现）。本包不直接做 I/O。
package ownership

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
)

// TokenSize 是数据目录引导令牌的字节数；令牌哈希为其 SHA-256，同为 32 字节。
const TokenSize = 32

// DBState 是数据库侧与安装身份有关的事实。
type DBState struct {
	HasMigrations     bool          // 存在 schema_migrations 表
	HasAgentboxTables bool          // 存在任何 agentbox 业务表
	Installation      *Installation // installation 记录；nil 表示无记录
}

// Installation 是 installation 表中的单行。
type Installation struct {
	InstallID string
	Complete  bool
	TokenHash []byte // 发起引导的数据目录令牌的 SHA-256；修订前创建的旧记录为 nil
}

// FileState 是数据目录中身份文件与引导令牌的状态。
type FileState struct {
	Exists    bool   // install_id 存在
	InstallID string // install_id 内容
	TokenHash []byte // bootstrap_token 的 SHA-256；令牌不存在时为 nil
}

// Action 是引导决策的动作。
type Action int

const (
	// Initialize：全新安装。持久化令牌（已有则复用）→ 全部迁移与 installation(新 ID, pending,
	// 令牌哈希) 同一事务提交 → 写身份文件 → 置 complete。
	Initialize Action = iota + 1
	// WriteFileAndComplete：继续本数据目录发起、中断了的引导，写身份文件后置 complete。
	WriteFileAndComplete
	// Complete：身份文件已一致，只需置 complete。
	Complete
	// Proceed：已完成且一致，正常启动。
	Proceed
	// Refuse：拒绝启动，需要人工处理。
	Refuse
)

// Decision 是一次决策的结果；Refuse 时 Reason 说明原因。
type Decision struct {
	Action Action
	Reason string
}

// Decide 按规格 §7.4 的决策表（含引导令牌修订）判定下一步。"表不存在"本身不被当作全新安装：
// 只有整个库为空且数据目录无身份时才初始化。pending 记录只能由持有对应令牌的数据目录继续。
func Decide(db DBState, file FileState) Decision {
	empty := !db.HasMigrations && !db.HasAgentboxTables && db.Installation == nil
	inst := db.Installation
	switch {
	case empty && !file.Exists:
		return Decision{Action: Initialize}
	case empty:
		return refuse("数据库为空但数据目录已有 install_id：数据库丢失或被替换")
	case !db.HasMigrations:
		return refuse("存在 agentbox 表但没有 schema_migrations：未知 schema")
	case inst == nil:
		return refuse("有 schema_migrations 但无 installation 记录：初始迁移与记录原子提交，只能来自外部改动、损坏，或该库属于另一个使用 schema_migrations 的应用")
	case file.Exists && file.InstallID != inst.InstallID:
		return refuse(fmt.Sprintf("安装身份不一致：数据库 %q，数据目录 %q", inst.InstallID, file.InstallID))
	case !inst.Complete && file.Exists:
		return Decision{Action: Complete} // 身份文件只由发起者或通过令牌校验的继续路径写入
	case !inst.Complete && inst.TokenHash == nil:
		return refuse("pending 安装记录早于引导令牌，无法证明由本数据目录发起：确认没有其他数据目录使用该库后，需人工清空数据库并重新引导")
	case !inst.Complete && !bytes.Equal(file.TokenHash, inst.TokenHash):
		return refuse("pending 安装由另一个数据目录发起，或本数据目录的 bootstrap_token 已丢失：若确认原数据目录已永久丢失，需人工处理")
	case !inst.Complete:
		return Decision{Action: WriteFileAndComplete}
	case !file.Exists:
		return refuse("installation 已完成但数据目录没有 install_id：数据目录丢失或被替换")
	default:
		return Decision{Action: Proceed}
	}
}

func refuse(reason string) Decision { return Decision{Action: Refuse, Reason: reason} }

// InstallStore 是引导所需的数据库操作；调用方须已持有 advisory lock。
type InstallStore interface {
	InspectInstallation(ctx context.Context) (DBState, error)
	// InitializeInstallation 在同一事务中执行全部迁移并插入 installation(installID, pending,
	// tokenHash)。提交结果未知时返回错误，不在进程内重试。
	InitializeInstallation(ctx context.Context, installID string, tokenHash []byte) error
	// CompleteInstallation 把 installation 置为 complete。
	CompleteInstallation(ctx context.Context, installID string) error
}

// IDFile 是数据目录中的安装身份文件；Write 必须持久化（含父目录 fsync）。
type IDFile interface {
	Read() (id string, exists bool, err error)
	Write(id string) error
}

// TokenFile 是数据目录中的引导令牌。Read 区分不存在（exists 为 false）与读取失败或损坏
// （返回错误）；Write 必须持久化，且令牌已存在时失败而不覆盖。
type TokenFile interface {
	Read() (token []byte, exists bool, err error)
	Write(token []byte) error
}

// ErrRefused 表示引导决策为拒绝启动；具体错误为 *RefusedError。
var ErrRefused = errors.New("ownership: 拒绝启动")

// RefusedError 携带拒绝启动的原因。
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string        { return "ownership: 拒绝启动: " + e.Reason }
func (e *RefusedError) Is(target error) bool { return target == ErrRefused }

// Bootstrap 执行安装身份引导与校验，返回本安装的 install_id。任何一步中断后重新调用，
// 都会按当时的事实继续或明确拒绝。newID 与 newToken 只在全新安装且需要时调用。
//
// 前置条件（规格 §7.4 的顺序）：调用方已持有数据目录的 flock，然后已持有数据库 advisory lock；
// 两把锁共同保证同一时刻只有一个进程对同一数据目录与同一数据库执行引导。返回错误时 install_id
// 为空，调用方不得使用，并结束本次启动：初始化事务的提交结果未知时同样如此——第一次事务可能
// 尚未结束，同进程内查询为空不能证明未提交；下次启动重新取得所有权后按决策表恢复。拒绝原因
// 不含数据目录与数据库位置，由装配层包装后报告给运维。
func Bootstrap(ctx context.Context, store InstallStore, idFile IDFile, tokenFile TokenFile,
	newID func() string, newToken func() ([]byte, error)) (string, error) {
	id, err := bootstrap(ctx, store, idFile, tokenFile, newID, newToken)
	if err != nil {
		return "", err
	}
	return id, nil
}

func bootstrap(ctx context.Context, store InstallStore, idFile IDFile, tokenFile TokenFile,
	newID func() string, newToken func() ([]byte, error)) (string, error) {
	db, err := store.InspectInstallation(ctx)
	if err != nil {
		return "", fmt.Errorf("ownership: 读取数据库状态: %w", err)
	}
	if inst := db.Installation; inst != nil && inst.TokenHash != nil && len(inst.TokenHash) != sha256.Size {
		return "", fmt.Errorf("ownership: installation 的令牌哈希长度为 %d，不是 %d：记录损坏", len(inst.TokenHash), sha256.Size)
	}
	id, exists, err := idFile.Read()
	if err != nil {
		return "", fmt.Errorf("ownership: 读取 install_id: %w", err)
	}
	token, hasToken, err := tokenFile.Read() // 读取失败或损坏直接失败，不当作"不存在"
	if err != nil {
		return "", fmt.Errorf("ownership: 读取 bootstrap_token: %w", err)
	}
	file := FileState{Exists: exists, InstallID: id}
	if hasToken {
		file.TokenHash = tokenHash(token)
	}
	d := Decide(db, file)
	switch d.Action {
	case Initialize:
		hash, err := ensureToken(tokenFile, token, hasToken, newToken)
		if err != nil {
			return "", err
		}
		id = newID()
		if id == "" {
			return "", errors.New("ownership: 生成的 install_id 为空")
		}
		if err := store.InitializeInstallation(ctx, id, hash); err != nil {
			return "", fmt.Errorf("ownership: 初始化安装: %w", err)
		}
		return id, writeAndComplete(ctx, store, idFile, id)
	case WriteFileAndComplete:
		return db.Installation.InstallID, writeAndComplete(ctx, store, idFile, db.Installation.InstallID)
	case Complete:
		return id, complete(ctx, store, id)
	case Proceed:
		return id, nil
	default:
		return "", &RefusedError{Reason: d.Reason}
	}
}

// ensureToken 返回本数据目录令牌的哈希：已有有效令牌则复用，否则生成并在初始化事务之前持久化。
func ensureToken(tokenFile TokenFile, token []byte, hasToken bool, newToken func() ([]byte, error)) ([]byte, error) {
	if !hasToken {
		var err error
		if token, err = newToken(); err != nil {
			return nil, fmt.Errorf("ownership: 生成 bootstrap_token: %w", err)
		}
		if len(token) != TokenSize {
			return nil, fmt.Errorf("ownership: 生成的 bootstrap_token 为 %d 字节，不是 %d", len(token), TokenSize)
		}
		if err := tokenFile.Write(token); err != nil {
			return nil, fmt.Errorf("ownership: 写入 bootstrap_token: %w", err)
		}
	}
	return tokenHash(token), nil
}

func tokenHash(token []byte) []byte {
	h := sha256.Sum256(token)
	return h[:]
}

func writeAndComplete(ctx context.Context, store InstallStore, idFile IDFile, id string) error {
	if err := idFile.Write(id); err != nil {
		return fmt.Errorf("ownership: 写入 install_id: %w", err)
	}
	return complete(ctx, store, id)
}

func complete(ctx context.Context, store InstallStore, id string) error {
	if err := store.CompleteInstallation(ctx, id); err != nil {
		return fmt.Errorf("ownership: 置 complete: %w", err)
	}
	return nil
}
