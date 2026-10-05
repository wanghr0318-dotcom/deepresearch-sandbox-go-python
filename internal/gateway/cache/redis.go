// Package cache 实现 Gateway 的搜索/抓取结果缓存（规格 §11）。
//
// Redis 只是可丢弃的加速层：它不保存任务状态、预算、恢复依据或锁，任何 Redis 错误都视为未命中（§11.1、§11.5）。
// 本文件是只用标准库实现的最小 RESP2 客户端（PING、GET、SET key val PX ms、DEL），带连接池，
// 每次操作有独立期限：读（GET、PING）默认 50 ms，写（SET、DEL）默认 100 ms，期限覆盖取连接、拨号、写请求与读回复。
// 本包不导入 persistence、gateway/call 与 net/http。
package cache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// KV 是缓存条目的存取接口，由 Redis 实现；测试可用内存实现。
// Get 未命中时返回 (nil, false, nil)。
type KV interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Del(ctx context.Context, key string) error
}

// 默认值（规格 §19）。
const (
	DefaultReadTimeout  = 50 * time.Millisecond
	DefaultWriteTimeout = 100 * time.Millisecond
	DefaultPoolSize     = 8
	// MaxValueSize 是缓存值的上限：Set 拒绝更大的值，Get 收到更大的回复时报错并丢弃该连接。
	MaxValueSize = 4 << 10
)

var (
	// ErrValueTooLarge：值超过 MaxValueSize。
	ErrValueTooLarge = errors.New("cache: value exceeds 4 KiB")
	// ErrProtocol：Redis 回复不符合 RESP2 或不是该命令预期的类型。
	ErrProtocol = errors.New("cache: RESP protocol error")
	// ErrClosed：客户端已关闭。
	ErrClosed = errors.New("cache: redis client closed")
)

// RedisError 是 Redis 的错误回复（"-ERR ..."）。收到它时连接仍然可用。
type RedisError struct{ Msg string }

func (e *RedisError) Error() string { return "cache: redis: " + e.Msg }

// RedisConfig 配置 Redis 客户端；零值字段取默认值。
type RedisConfig struct {
	Addr                      string
	ReadTimeout, WriteTimeout time.Duration // 50ms / 100ms
	PoolSize                  int           // 8：同时进行的操作（即打开的连接）上限
}

// Redis 是最小 RESP2 客户端，可并发使用。
type Redis struct {
	cfg    RedisConfig
	dialer net.Dialer
	slots  chan struct{} // 并发操作的许可，容量 PoolSize

	mu     sync.Mutex
	idle   []*redisConn
	closed bool
}

type redisConn struct {
	nc net.Conn
	br *bufio.Reader
}

// NewRedis 创建客户端；不拨号，连接在首次操作时建立。
func NewRedis(cfg RedisConfig) *Redis {
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = DefaultReadTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultWriteTimeout
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = DefaultPoolSize
	}
	return &Redis{cfg: cfg, slots: make(chan struct{}, cfg.PoolSize)}
}

// Ping 发送 PING，期望 PONG（读期限）。
func (r *Redis) Ping(ctx context.Context) error {
	rep, err := r.do(ctx, r.cfg.ReadTimeout, "PING")
	if err != nil {
		return err
	}
	if rep.kind != '+' || rep.str != "PONG" {
		return fmt.Errorf("%w: PING 回复不是 PONG", ErrProtocol)
	}
	return nil
}

// Get 读取 key（读期限）。不存在时返回 (nil, false, nil)；空值是命中且值非 nil。
func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	rep, err := r.do(ctx, r.cfg.ReadTimeout, "GET", key)
	if err != nil {
		return nil, false, err
	}
	if rep.kind != '$' {
		return nil, false, fmt.Errorf("%w: GET 回复类型 %q", ErrProtocol, rep.kind)
	}
	if rep.null {
		return nil, false, nil
	}
	return rep.bulk, true, nil
}

// Set 写入 key 并设置毫秒级有效期（写期限）。ttl 必须为正（不写永久条目），不足 1 ms 向上取整。
func (r *Redis) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if len(val) > MaxValueSize {
		return ErrValueTooLarge
	}
	if ttl <= 0 {
		return fmt.Errorf("cache: SET 的 ttl 必须为正，得到 %v", ttl)
	}
	ms := (ttl + time.Millisecond - 1) / time.Millisecond
	rep, err := r.do(ctx, r.cfg.WriteTimeout, "SET", key, val, "PX", strconv.FormatInt(int64(ms), 10))
	if err != nil {
		return err
	}
	if rep.kind != '+' || rep.str != "OK" {
		return fmt.Errorf("%w: SET 回复不是 OK", ErrProtocol)
	}
	return nil
}

// Del 删除 key（写期限）；key 不存在不是错误。
func (r *Redis) Del(ctx context.Context, key string) error {
	rep, err := r.do(ctx, r.cfg.WriteTimeout, "DEL", key)
	if err != nil {
		return err
	}
	if rep.kind != ':' {
		return fmt.Errorf("%w: DEL 回复类型 %q", ErrProtocol, rep.kind)
	}
	return nil
}

// Close 关闭空闲连接；之后的操作返回 ErrClosed，进行中的操作结束后关闭其连接。
func (r *Redis) Close() error {
	r.mu.Lock()
	idle := r.idle
	r.idle, r.closed = nil, true
	r.mu.Unlock()
	var errs []error
	for _, c := range idle {
		errs = append(errs, c.nc.Close())
	}
	return errors.Join(errs...)
}

// do 在一个独立期限内执行一条命令：期限取 now+timeout 与 ctx 期限中较早者，覆盖取许可、拨号、写与读。
// 只有 Redis 错误回复之后连接仍放回池中；I/O、协议与超长值错误都丢弃该连接（回复流位置不再可信）。
func (r *Redis) do(ctx context.Context, timeout time.Duration, args ...any) (reply, error) {
	if err := ctx.Err(); err != nil {
		return reply{}, err
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	cmd, _ := args[0].(string)

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return reply{}, ctx.Err()
	case <-timer.C:
		return reply{}, fmt.Errorf("cache: redis %s: 等待连接: %w", cmd, os.ErrDeadlineExceeded)
	}
	defer func() { <-r.slots }()

	c, err := r.conn(ctx, deadline)
	if err != nil {
		return reply{}, fmt.Errorf("cache: redis %s: %w", cmd, err)
	}
	if err := c.nc.SetDeadline(deadline); err != nil {
		_ = c.nc.Close() // 设置期限失败的连接不可用；关闭错误无可补救
		return reply{}, fmt.Errorf("cache: redis %s: %w", cmd, err)
	}
	// ctx 在操作中途被取消时立即让阻塞的读写返回。
	stop := context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(time.Unix(1, 0)) })

	rep, err := roundTrip(c, args)
	cancelled := !stop()
	var redisErr *RedisError
	reusable := (err == nil || errors.As(err, &redisErr)) && !cancelled
	if reusable {
		r.put(c)
	} else {
		_ = c.nc.Close() // 连接已作废；关闭错误无可补救
	}
	if cancelled && err != nil {
		return reply{}, fmt.Errorf("cache: redis %s: %w", cmd, ctx.Err())
	}
	if err != nil {
		if redisErr != nil {
			return reply{}, err
		}
		return reply{}, fmt.Errorf("cache: redis %s: %w", cmd, err)
	}
	return rep, nil
}

func roundTrip(c *redisConn, args []any) (reply, error) {
	if _, err := c.nc.Write(encodeCommand(args)); err != nil {
		return reply{}, err
	}
	return readReply(c.br)
}

func (r *Redis) conn(ctx context.Context, deadline time.Time) (*redisConn, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	if n := len(r.idle); n > 0 {
		c := r.idle[n-1]
		r.idle = r.idle[:n-1]
		r.mu.Unlock()
		return c, nil
	}
	r.mu.Unlock()
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	nc, err := r.dialer.DialContext(dctx, "tcp", r.cfg.Addr)
	if err != nil {
		return nil, err
	}
	return &redisConn{nc: nc, br: bufio.NewReader(nc)}, nil
}

func (r *Redis) put(c *redisConn) {
	r.mu.Lock()
	if r.closed || len(r.idle) >= r.cfg.PoolSize {
		r.mu.Unlock()
		_ = c.nc.Close() // 多余或已关闭客户端的连接；关闭错误无可补救
		return
	}
	r.idle = append(r.idle, c)
	r.mu.Unlock()
}

// encodeCommand 把参数编码为 RESP2 的批量字符串数组；参数为 string 或 []byte。
func encodeCommand(args []any) []byte {
	var b bytes.Buffer
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		var p []byte
		switch v := a.(type) {
		case string:
			p = []byte(v)
		case []byte:
			p = v
		default:
			panic(fmt.Sprintf("cache: 不支持的命令参数类型 %T", a))
		}
		b.WriteString("$" + strconv.Itoa(len(p)) + "\r\n")
		b.Write(p)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

// reply 是一个非错误的 RESP2 回复：'+' 简单字符串、':' 整数、'$' 批量字符串（null 表示 $-1）。
type reply struct {
	kind byte
	str  string
	n    int64
	bulk []byte
	null bool
}

// readReply 读取并解析一个回复。错误回复返回 *RedisError；数组与未知类型、缺少 CRLF、
// 非法长度均为 ErrProtocol；批量字符串超过 MaxValueSize 返回 ErrValueTooLarge（不读取其内容）。
func readReply(br *bufio.Reader) (reply, error) {
	line, err := readLine(br)
	if err != nil {
		return reply{}, err
	}
	if len(line) == 0 {
		return reply{}, fmt.Errorf("%w: 空行", ErrProtocol)
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return reply{kind: '+', str: string(body)}, nil
	case '-':
		return reply{}, &RedisError{Msg: string(body)}
	case ':':
		n, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return reply{}, fmt.Errorf("%w: 整数 %q", ErrProtocol, body)
		}
		return reply{kind: ':', n: n}, nil
	case '$':
		n, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil || n < -1 {
			return reply{}, fmt.Errorf("%w: 批量长度 %q", ErrProtocol, body)
		}
		if n == -1 {
			return reply{kind: '$', null: true}, nil
		}
		if n > MaxValueSize {
			return reply{}, fmt.Errorf("%w: 回复 %d 字节", ErrValueTooLarge, n)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return reply{}, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return reply{}, fmt.Errorf("%w: 批量字符串缺少 CRLF", ErrProtocol)
		}
		return reply{kind: '$', bulk: buf[:n:n]}, nil
	default:
		return reply{}, fmt.Errorf("%w: 不支持的回复类型 %q", ErrProtocol, line[0])
	}
}

// readLine 读取以 CRLF 结尾的一行（不含 CRLF）；行长受 bufio 缓冲区限制，超长为协议错误。
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, fmt.Errorf("%w: 行过长", ErrProtocol)
	}
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) > 0 {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: 行缺少 CR", ErrProtocol)
	}
	return line[:len(line)-2], nil
}
