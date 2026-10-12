package cache

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// pongServer answers every command with +PONG; restart closes every open connection (a Redis restart: the listener
// stays, the clients' pooled connections are dead).
type pongServer struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
	wg    sync.WaitGroup
}

func newPongServer(t *testing.T) *pongServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &pongServer{ln: ln}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				br := bufio.NewReader(c)
				for skipCommand(br) == nil {
					if _, err := io.WriteString(c, "+PONG\r\n"); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close() // 只为结束 Accept
		s.restart()
		s.wg.Wait()
	})
	return s
}

func (s *pongServer) restart() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close() // 模拟服务端重启；关闭错误无关紧要
	}
	s.conns = nil
}

// TestStalePoolDroppedAfterIOError：Redis 重启后池中的空闲连接全部失效。一次 I/O 失败之后客户端丢弃全部空闲连接，
// 下一次操作重新拨号成功——而不是每次操作取出一个失效连接再失败一次（限流器每 5 s 冷却才试一次 Redis 时，
// 那会使恢复推迟"空闲连接数 × 5 s"）。
func TestStalePoolDroppedAfterIOError(t *testing.T) {
	s := newPongServer(t)
	r := NewRedis(RedisConfig{Addr: s.ln.Addr().String(), ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 8})
	t.Cleanup(func() { _ = r.Close() })
	ctx := context.Background()

	// 8 个并发操作留下 8 个空闲连接。
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := r.Ping(ctx); err != nil {
				t.Errorf("Ping: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	r.mu.Lock()
	idle := len(r.idle)
	r.mu.Unlock()
	if idle < 2 {
		t.Fatalf("idle connections = %d, want at least 2 for the test to mean anything", idle)
	}

	s.restart()
	time.Sleep(50 * time.Millisecond) // 让关闭到达客户端一侧
	if err := r.Ping(ctx); err == nil {
		t.Fatal("the first Ping after the restart used a dead pooled connection and still succeeded")
	}
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("second Ping after the restart = %v; want a fresh connection (stale idle connections dropped)", err)
	}
}
