// Package cli 是 agentbox 的参考命令行客户端：通过 REST + SSE 访问 API（规格 §15）。
//
// 只用标准库。token 只从环境变量 AGENTBOX_TOKEN 或 <data-dir>/api.token 读取，
// 不接受命令行参数形式，也不出现在输出与错误里。
package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	tokenEnv       = "AGENTBOX_TOKEN"
	addrEnv        = "AGENTBOX_ADDR"
	defaultAddr    = "http://127.0.0.1:8080"
	maxAttempts    = 6  // 写请求的最大尝试次数（同一 request_id）
	maxIdleRetries = 10 // watch 连续无进展的最大重连次数
	maxBody        = 64 << 20
)

// Env 汇集可注入的外部依赖，测试用它替换睡眠、HTTP 客户端与 ID 生成。
type Env struct {
	HTTP   *http.Client
	Sleep  func(time.Duration)
	Getenv func(string) string
	NewID  func() string
}

func (e Env) withDefaults() Env {
	if e.HTTP == nil {
		e.HTTP = &http.Client{} // 不设整体超时：SSE 是长连接
	}
	if e.Sleep == nil {
		e.Sleep = time.Sleep
	}
	if e.Getenv == nil {
		e.Getenv = os.Getenv
	}
	if e.NewID == nil {
		e.NewID = newRequestID
	}
	return e
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("cli: 随机数不可用: " + err.Error())
	}
	return "cli-" + hex.EncodeToString(b[:])
}

// Run 以默认依赖执行命令，返回进程退出码。
func Run(args []string, stdout, stderr io.Writer) int {
	return RunEnv(Env{}, args, stdout, stderr)
}

type usageError struct{ msg string }

func (u usageError) Error() string { return u.msg }

// RunEnv 同 Run，但依赖可注入。
func RunEnv(env Env, args []string, stdout, stderr io.Writer) int {
	env = env.withDefaults()
	c := &cmd{env: env, out: stdout}
	err := c.dispatch(args)
	if err == nil {
		return 0
	}
	msg := c.redact(err.Error())
	var ue usageError
	if errors.As(err, &ue) {
		fmt.Fprintln(stderr, "错误: "+msg)
		fmt.Fprintln(stderr, "用法: agentbox task submit|watch|cancel|pause|resume|result|inspect ... | agentbox status")
		return 2
	}
	fmt.Fprintln(stderr, "错误: "+msg)
	return 1
}

type cmd struct {
	env   Env
	out   io.Writer
	addr  string
	token string
}

// redact 去掉文本中的 token，保证它不会进入输出。
func (c *cmd) redact(s string) string {
	if c.token != "" {
		s = strings.ReplaceAll(s, c.token, "[REDACTED]")
	}
	return s
}

func (c *cmd) dispatch(args []string) error {
	if len(args) == 0 {
		return usageError{"缺少命令"}
	}
	switch args[0] {
	case "status":
		fs, f := c.flags("status")
		if err := fs.Parse(args[1:]); err != nil {
			return usageError{err.Error()}
		}
		if err := c.setup(f); err != nil {
			return err
		}
		return c.getJSON("/status")
	case "task":
		if len(args) < 2 {
			return usageError{"task 缺少子命令"}
		}
		return c.task(args[1], args[2:])
	}
	return usageError{"未知命令 " + strconv.Quote(args[0])}
}

type common struct {
	addr, dataDir *string
}

func (c *cmd) flags(name string) (*flag.FlagSet, common) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs, common{
		addr:    fs.String("addr", "", "服务地址（默认 "+defaultAddr+"，或环境变量 "+addrEnv+"）"),
		dataDir: fs.String("data-dir", "", "数据目录，用于读取 api.token（token 也可由环境变量 "+tokenEnv+" 提供）"),
	}
}

func (c *cmd) setup(f common) error {
	addr := *f.addr
	if addr == "" {
		addr = c.env.Getenv(addrEnv)
	}
	if addr == "" {
		addr = defaultAddr
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	c.addr = strings.TrimRight(addr, "/")
	c.token = strings.TrimSpace(c.env.Getenv(tokenEnv))
	if c.token == "" && *f.dataDir != "" {
		b, err := os.ReadFile(filepath.Join(*f.dataDir, "api.token"))
		if err != nil {
			return fmt.Errorf("读取 api.token: %w", err)
		}
		c.token = strings.TrimSpace(string(b))
	}
	return nil
}

func (c *cmd) task(sub string, args []string) error {
	switch sub {
	case "submit":
		return c.submit(args)
	case "watch", "cancel", "pause", "resume", "result", "inspect":
	default:
		return usageError{"未知子命令 task " + sub}
	}
	fs, f := c.flags("task " + sub)
	var reason, reqID, outPath, artifact *string
	var version *int64
	switch sub {
	case "cancel", "pause", "resume":
		reason = fs.String("reason", "", "原因")
		reqID = fs.String("request-id", "", "request_id（缺省自动生成）")
	case "result":
		outPath = fs.String("out", "", "写入文件（缺省写标准输出）")
		artifact = fs.String("artifact", "", "下载该产物（缺省下载任务结果）")
		version = fs.Int64("version", 0, "与 --artifact 一起使用：产物版本（缺省最新版本）")
	}
	// 任务 ID 可在标志前或后。
	var id string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		id, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return usageError{err.Error()}
	}
	if id == "" && fs.NArg() == 1 {
		id = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return usageError{"多余的参数"}
	}
	if id == "" {
		return usageError{"缺少任务 ID"}
	}
	if sub == "result" && (*version < 0 || (*version != 0 && *artifact == "")) {
		return usageError{"--version 须为正整数且与 --artifact 一起使用"}
	}
	if err := c.setup(f); err != nil {
		return err
	}
	base := "/tasks/" + url.PathEscape(id)
	switch sub {
	case "watch":
		return c.watch(base + "/events")
	case "inspect":
		return c.getJSON(base + "/inspect")
	case "result":
		if *artifact == "" {
			return c.result(base+"/result", *outPath)
		}
		p := base + "/artifacts/" + url.PathEscape(*artifact)
		if *version > 0 {
			p += "?version=" + strconv.FormatInt(*version, 10)
		}
		return c.result(p, *outPath)
	}
	rid := *reqID
	if rid == "" {
		rid = c.env.NewID()
	}
	body := map[string]string{"request_id": rid}
	if *reason != "" {
		body["reason"] = *reason
	}
	return c.postJSON(base+"/"+sub, body)
}

func (c *cmd) submit(args []string) error {
	fs, f := c.flags("task submit")
	specArg := fs.String("spec", "", "任务 spec：JSON 对象，或 @文件")
	limitsArg := fs.String("limits", "", "资源限制：JSON 对象，或 @文件")
	reqID := fs.String("request-id", "", "request_id（缺省自动生成；重试沿用同一个）")
	if err := fs.Parse(args); err != nil {
		return usageError{err.Error()}
	}
	if fs.NArg() != 0 {
		return usageError{"多余的参数"}
	}
	if *specArg == "" {
		return usageError{"缺少 --spec"}
	}
	spec, err := jsonObjectArg(*specArg)
	if err != nil {
		return usageError{"--spec: " + err.Error()}
	}
	body := map[string]any{"spec": spec}
	if *limitsArg != "" {
		limits, err := jsonObjectArg(*limitsArg)
		if err != nil {
			return usageError{"--limits: " + err.Error()}
		}
		body["limits"] = limits
	}
	if err := c.setup(f); err != nil {
		return err
	}
	rid := *reqID
	if rid == "" {
		rid = c.env.NewID() // 只生成一次，重试复用
	}
	body["request_id"] = rid
	return c.postJSON("/tasks", body)
}

func jsonObjectArg(s string) (json.RawMessage, error) {
	if strings.HasPrefix(s, "@") {
		b, err := os.ReadFile(s[1:])
		if err != nil {
			return nil, err
		}
		s = string(b)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(s), &m); err != nil || m == nil {
		return nil, errors.New("须为 JSON 对象")
	}
	return json.RawMessage(s), nil
}

// ---- HTTP ----

// apiError 是服务端返回的错误。
type apiError struct {
	Status  int
	Code    string
	Message string
}

func (e *apiError) Error() string {
	if e.Status == http.StatusNotImplemented {
		return fmt.Sprintf("服务端尚未实现该端点（501 %s）: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// retryable 判断错误是否值得以同一请求重试：网络错误与 5xx（含 503 commit_unknown 等）。
// 501 是确定的"未实现"，不重试。
func retryable(err error) bool {
	var ae *apiError
	var fe fatal
	if errors.As(err, &fe) {
		return false
	}
	if errors.As(err, &ae) {
		return ae.Status >= 500 && ae.Status != http.StatusNotImplemented
	}
	return true
}

func (c *cmd) newRequest(method, path string, body []byte) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.addr+path, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

func readAPIError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	ae := &apiError{Status: resp.StatusCode}
	var e struct{ Code, Message string }
	if json.Unmarshal(b, &e) == nil && e.Code != "" {
		ae.Code, ae.Message = e.Code, e.Message
	} else {
		ae.Code, ae.Message = "unknown", strings.TrimSpace(string(b))
	}
	return ae
}

func backoff(n int) time.Duration {
	d := 100 * time.Millisecond << min(n, 6)
	return min(d, 5*time.Second)
}

// do 发送一次请求并返回 2xx 的响应体；非 2xx 返回 *apiError。
func (c *cmd) do(method, path string, body []byte) ([]byte, http.Header, error) {
	req, err := c.newRequest(method, path, body)
	if err != nil {
		return nil, nil, err
	}
	resp, err := c.env.HTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, nil, readAPIError(resp)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, nil, err
	}
	if len(b) > maxBody {
		return nil, nil, errors.New("响应过大")
	}
	return b, resp.Header, nil
}

// doRetry 对可重试错误带退避重试；写请求的 body 固定，因此 request_id 不变。
func (c *cmd) doRetry(method, path string, body []byte) ([]byte, http.Header, error) {
	var err error
	for n := 0; n < maxAttempts; n++ {
		var b []byte
		var h http.Header
		b, h, err = c.do(method, path, body)
		if err == nil || !retryable(err) {
			return b, h, err
		}
		if n < maxAttempts-1 {
			c.env.Sleep(backoff(n))
		}
	}
	return nil, nil, fmt.Errorf("重试 %d 次后仍失败: %w", maxAttempts, err)
}

func (c *cmd) getJSON(path string) error {
	b, _, err := c.doRetry(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	return c.printJSON(b)
}

func (c *cmd) postJSON(path string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	b, _, err := c.doRetry(http.MethodPost, path, raw)
	if err != nil {
		return err
	}
	return c.printJSON(b)
}

func (c *cmd) printJSON(b []byte) error {
	var buf bytes.Buffer
	if json.Indent(&buf, b, "", "  ") != nil {
		buf.Reset()
		buf.Write(b)
	}
	if buf.Len() == 0 || buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	_, err := c.out.Write(buf.Bytes())
	return err
}

// ---- result ----

// result 下载固定结果或产物版本并校验 sha256：与 ETag 声明的哈希比对，不符则报错且不输出内容。
// 服务端在发送时发现 blob 与登记不符会中止连接，此时以传输错误失败（重试后仍失败），同样不输出内容。
func (c *cmd) result(path, outPath string) error {
	body, hdr, err := c.doRetry(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	want := strings.ToLower(strings.Trim(strings.TrimSpace(hdr.Get("ETag")), `"`))
	if want == "" {
		return errors.New("响应缺少 ETag，无法校验 sha256")
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("sha256 不符: 期望 %s，实际 %s", want, got)
	}
	if outPath == "" {
		_, err = c.out.Write(body)
		return err
	}
	return os.WriteFile(outPath, body, 0o644)
}

// ---- watch ----

type sseEvent struct {
	id, typ string
	data    string
}

// readSSE 逐帧读取 SSE；注释行（心跳）与未知字段忽略。fn 返回 false 时停止。
func readSSE(r io.Reader, fn func(sseEvent) bool) error {
	br := bufio.NewReader(r)
	var ev sseEvent
	var data []string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err // 含 io.EOF：不完整的帧丢弃，由重连补齐
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if len(data) > 0 || ev.id != "" || ev.typ != "" {
				ev.data = strings.Join(data, "\n")
				if !fn(ev) {
					return nil
				}
			}
			ev, data = sseEvent{}, nil
		case strings.HasPrefix(line, ":"):
		default:
			k, v, _ := strings.Cut(line, ":")
			v = strings.TrimPrefix(v, " ")
			switch k {
			case "id":
				ev.id = v
			case "event":
				ev.typ = v
			case "data":
				data = append(data, v)
			}
		}
	}
}

// watch 跟随任务事件：断线后以 Last-Event-ID 带退避重连，按 task_seq 去重，
// 收到 task_terminal 即结束且不再发起任何请求（规格 §15.2、E16）。
func (c *cmd) watch(path string) error {
	var last int64
	idle := 0 // 连续无进展的失败次数
	for {
		terminal, progressed, err := c.watchOnce(path, &last)
		if terminal {
			return nil
		}
		if err != nil && !retryable(err) {
			return err
		}
		if progressed {
			idle = 0
		} else {
			idle++
		}
		if idle > maxIdleRetries {
			if err == nil {
				err = errors.New("流意外结束")
			}
			return fmt.Errorf("连续 %d 次重连无进展: %w", maxIdleRetries, err)
		}
		if idle > 0 {
			c.env.Sleep(backoff(idle - 1))
		}
	}
}

func (c *cmd) watchOnce(path string, last *int64) (terminal, progressed bool, err error) {
	req, err := c.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if *last > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(*last, 10))
	}
	resp, err := c.env.HTTP.Do(req)
	if err != nil {
		return false, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, false, readAPIError(resp)
	}
	var werr error
	rerr := readSSE(resp.Body, func(ev sseEvent) bool {
		seq, perr := strconv.ParseInt(ev.id, 10, 64)
		if perr != nil || seq <= 0 {
			werr = fmt.Errorf("事件缺少有效的 id: %q", ev.id)
			return false
		}
		if seq <= *last {
			return true // 重复
		}
		line := strings.NewReplacer("\r", "", "\n", "").Replace(ev.data)
		if _, werr = fmt.Fprintln(c.out, line); werr != nil {
			return false
		}
		*last = seq
		progressed = true
		if ev.typ == "task_terminal" {
			terminal = true
			return false
		}
		return true
	})
	if werr != nil {
		return false, progressed, fatal{werr}
	}
	return terminal, progressed, rerr
}

// fatal 包装不应重连的错误（如输出写失败或协议错误）。
type fatal struct{ error }

func (f fatal) Unwrap() error { return f.error }
