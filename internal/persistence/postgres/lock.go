package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// advisoryKey 是本安装在数据库中的会话级 advisory lock 键（"agentbox" 的 ASCII）。
const advisoryKey int64 = 0x6167656e74626f78

// ErrAlreadyOwned 表示另一个实例正持有 advisory lock。
var ErrAlreadyOwned = errors.New("postgres: advisory lock 已被另一个实例持有")

// OwnershipOptions 配置失锁检测；零值字段取默认值（周期 1 s，单次检查超时 1 s）。
type OwnershipOptions struct {
	CheckInterval time.Duration
	CheckTimeout  time.Duration
}

// Ownership 是专用连接上的会话级 advisory lock（规格 §7.4；设计 §3.4）。
// 失去锁连接或无法确认锁仍在时进入不可逆的 ownership_lost：Lost() 关闭，Context() 取消。
type Ownership struct {
	conn      *pgx.Conn // 只由监视 goroutine 使用；Close 先停止监视再使用
	ctx       context.Context
	cancel    context.CancelFunc
	lost      chan struct{}
	lostOnce  sync.Once
	closeOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
	opt       OwnershipOptions
}

// AcquireOwnership 建立专用连接并取得 advisory lock；已被持有时返回 ErrAlreadyOwned。
func AcquireOwnership(ctx context.Context, dsn string, opt OwnershipOptions) (*Ownership, error) {
	if opt.CheckInterval == 0 {
		opt.CheckInterval = time.Second
	}
	if opt.CheckTimeout == 0 {
		opt.CheckTimeout = time.Second
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: 建立锁连接: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", advisoryKey).Scan(&acquired); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("postgres: 取得 advisory lock: %w", err)
	}
	if !acquired {
		_ = conn.Close(context.Background())
		return nil, ErrAlreadyOwned
	}
	o := &Ownership{conn: conn, lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}), opt: opt}
	o.ctx, o.cancel = context.WithCancel(context.Background())
	go o.monitor()
	return o, nil
}

// Lost 在失去所有权时关闭。
func (o *Ownership) Lost() <-chan struct{} { return o.lost }

// IsLost 报告是否已失去所有权。
func (o *Ownership) IsLost() bool {
	select {
	case <-o.lost:
		return true
	default:
		return false
	}
}

// Context 在失去所有权时取消；业务操作的 context 由它派生。
func (o *Ownership) Context() context.Context { return o.ctx }

// Close 停止监视并释放锁与连接。之后 IsLost 为 true。幂等：再次调用返回 nil。
func (o *Ownership) Close() error {
	var err error
	o.closeOnce.Do(func() {
		close(o.stop)
		<-o.done
		o.markLost()
		err = o.conn.Close(context.Background()) // 关闭会话即释放会话级 advisory lock
	})
	return err
}

func (o *Ownership) markLost() {
	o.lostOnce.Do(func() {
		close(o.lost)
		o.cancel()
	})
}

func (o *Ownership) monitor() {
	defer close(o.done)
	t := time.NewTicker(o.opt.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-t.C:
			if !o.stillHeld() {
				o.markLost()
				return
			}
		}
	}
}

// stillHeld 在锁连接上确认本会话仍持有 advisory lock。
func (o *Ownership) stillHeld() bool {
	ctx, cancel := context.WithTimeout(context.Background(), o.opt.CheckTimeout)
	defer cancel()
	var held bool
	err := o.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted)",
	).Scan(&held)
	return err == nil && held
}
