// Package ownership 实现安装身份引导与校验（规格 §7.4；设计 §3.3）。
//
// Decide 是纯函数，覆盖 §7.4 决策表的全部情况；Bootstrap 经两个窄接口编排引导步骤：
// InstallStore（数据库侧，由 internal/persistence/postgres 实现）与 IDFile（数据目录侧，
// 由 internal/datadir 实现）。本包不直接做 I/O。
package ownership

import (
	"context"
	"errors"
	"fmt"
)

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
}

// FileState 是数据目录中 install_id 文件的状态。
type FileState struct {
	Exists    bool
	InstallID string
}

// Action 是引导决策的动作。
type Action int

const (
	// Initialize：全新安装。初始迁移与 installation(新 ID, pending) 同一事务提交 → 写身份文件 → 置 complete。
	Initialize Action = iota + 1
	// WriteFileAndComplete：继续中断的引导，写身份文件后置 complete。
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

// Decide 按规格 §7.4 的决策表判定下一步。"表不存在"本身不被当作全新安装：
// 只有整个库为空且数据目录无身份时才初始化。
func Decide(db DBState, file FileState) Decision {
	empty := !db.HasMigrations && !db.HasAgentboxTables && db.Installation == nil
	switch {
	case empty && !file.Exists:
		return Decision{Action: Initialize}
	case empty:
		return refuse("数据库为空但数据目录已有 install_id：数据库丢失或被替换")
	case !db.HasMigrations:
		return refuse("存在 agentbox 表但没有 schema_migrations：未知 schema")
	case db.Installation == nil:
		return refuse("有 schema_migrations 但无 installation 记录：初始迁移与记录原子提交，只能来自外部改动、损坏，或该库属于另一个使用 schema_migrations 的应用")
	case file.Exists && file.InstallID != db.Installation.InstallID:
		return refuse(fmt.Sprintf("安装身份不一致：数据库 %q，数据目录 %q", db.Installation.InstallID, file.InstallID))
	case !db.Installation.Complete && !file.Exists:
		return Decision{Action: WriteFileAndComplete}
	case !db.Installation.Complete:
		return Decision{Action: Complete}
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
	// InitializeInstallation 在同一事务中执行初始迁移并插入 installation(installID, pending)。
	InitializeInstallation(ctx context.Context, installID string) error
	// CompleteInstallation 把 installation 置为 complete。
	CompleteInstallation(ctx context.Context, installID string) error
}

// IDFile 是数据目录中的安装身份文件；Write 必须持久化（含父目录 fsync）。
type IDFile interface {
	Read() (id string, exists bool, err error)
	Write(id string) error
}

// ErrRefused 表示引导决策为拒绝启动；具体错误为 *RefusedError。
var ErrRefused = errors.New("ownership: 拒绝启动")

// RefusedError 携带拒绝启动的原因。
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string        { return "ownership: 拒绝启动: " + e.Reason }
func (e *RefusedError) Is(target error) bool { return target == ErrRefused }

// Bootstrap 执行安装身份引导与校验，返回本安装的 install_id。任何一步中断后重新调用，
// 都会按当时的事实继续或明确拒绝。newID 只在全新安装时调用。
//
// 前置条件（规格 §7.4 的顺序）：调用方已持有数据目录的 flock，然后已持有数据库 advisory lock；
// 两把锁共同保证同一时刻只有一个进程对同一数据目录与同一数据库执行引导。返回错误时 install_id
// 为空，调用方不得使用；拒绝原因不含数据目录与数据库位置，由装配层包装后报告给运维。
func Bootstrap(ctx context.Context, store InstallStore, file IDFile, newID func() string) (string, error) {
	id, err := bootstrap(ctx, store, file, newID)
	if err != nil {
		return "", err
	}
	return id, nil
}

func bootstrap(ctx context.Context, store InstallStore, file IDFile, newID func() string) (string, error) {
	db, err := store.InspectInstallation(ctx)
	if err != nil {
		return "", fmt.Errorf("ownership: 读取数据库状态: %w", err)
	}
	id, exists, err := file.Read()
	if err != nil {
		return "", fmt.Errorf("ownership: 读取 install_id: %w", err)
	}
	d := Decide(db, FileState{Exists: exists, InstallID: id})
	switch d.Action {
	case Initialize:
		id = newID()
		if id == "" {
			return "", errors.New("ownership: 生成的 install_id 为空")
		}
		if err := store.InitializeInstallation(ctx, id); err != nil {
			return "", fmt.Errorf("ownership: 初始化安装: %w", err)
		}
		return id, writeAndComplete(ctx, store, file, id)
	case WriteFileAndComplete:
		return db.Installation.InstallID, writeAndComplete(ctx, store, file, db.Installation.InstallID)
	case Complete:
		return id, complete(ctx, store, id)
	case Proceed:
		return id, nil
	default:
		return "", &RefusedError{Reason: d.Reason}
	}
}

func writeAndComplete(ctx context.Context, store InstallStore, file IDFile, id string) error {
	if err := file.Write(id); err != nil {
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
