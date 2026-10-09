package call

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// exec 调度（规格 §10；Plan 15 D3、D6、D7、D8、D12、D16）。每次 POST /v1/exec 在全新的 exec 环境中执行一次：
//
//	解析（拒绝重复键与未知字段；校验 language/code/inputs/limits；limits 按上限截断）→ 指纹（§10.1）
//	→ Tx1 BeginCall（deadline = 排队上限 + 生效 wall + 30 s）→ 按已有记录分流（§9.4 表）
//	→ 输入授权（未授权 → failed{input_not_authorized}，403，不排队、不预留）
//	→ 等待 exec slot（≤ 排队上限与调用期限取早；超时 → failed{exec_queue_timeout}，504）
//	→ Tx2 ReserveExec（次数、CPU、wall 配额）→ Envs.Hold（到 try 结束）→ Envs.Create → /in 暂存（main.py 与输入）
//	→ 进程内取消检查（与 CancelAttempt 同一把锁）→ MarkExecStarting（I12 [A]）→ Envs.Start（stdin 立即关闭）
//	→ 并发读取 stdout/stderr（各保留 1 MiB）→ 等待退出、wall 到期或取消 → Envs.Stop（整个执行树）
//	→ Diag → OpenOutputs → 逐个 Blobs.Put（全部关闭）→ 结果 blob → Envs.Cleanup（尽力，D8）
//	→ SettleExec → stopped_at 已记录时归还 slot
//
// 与上游调用不同，exec 的取消不看离开原因（D7）：CancelAttempt 的任何原因与 CancelSubrun 都终止该 attempt
// （sub-run）的全部 exec，结局 cancelled（journal failed{exec_cancelled}，可重试类别，D6）。只有无法确认执行树
// 已停止或无法取得结果时为 unknown（不归还 slot、不收集，环境留给启动恢复与 cleanup loop）。
// exec 只经 ExecStore 预留与结算（money 路径的 ReserveTry/SettleTry 不区分 kind）。

// exec 的拒绝码与失败原因（其余见 execstore.go 的 CodeExec*）。
const (
	CodeInputNotAuthorized = "input_not_authorized" // 输入 blob 不在任务 scope 内（403）
	CodeInputsTooLarge     = "inputs_too_large"     // 输入累计超过 256 MiB（400；try 按 start_failed 结算）
	CodeExecQueueTimeout   = "exec_queue_timeout"   // 排队超过上限（504，可重试类别）
	CodeExecEnvUnavailable = "exec_env_unavailable" // 环境创建或暂存失败（503，可重试类别）
	CodeExecUnknown        = "exec_unknown"         // 无法确认执行树已停止或无法取得结果（502；journal unknown）
)

// exec 请求与结果的固定参数（§10.1、§19 补充）。
const (
	ExecAdapterVersion = "exec/1"
	ExecLanguage       = "python3"
	MaxExecCodeBytes   = 256 << 10 // code 的 UTF-8 字节上限
	MaxExecInputBytes  = 256 << 20 // 输入累计上限
	MaxExecStreamBytes = 1 << 20   // stdout、stderr 各自保留的上限（超额继续读取并丢弃）
	execMainRel        = ".agentbox/main.py"
	execDeadlineMargin = 30 * time.Second // 调用期限 = 排队上限 + 生效 wall + 30 s（§9.7）
)

// ExecArgv 返回固定的 argv（D3）：代码即入口，调用方不能指定 argv。
func ExecArgv() []string { return []string{"python3", "-I", "-B", "/in/" + execMainRel} }

// ExecEnviron 返回固定的环境变量（Global Constraints）：调用方不能指定。
func ExecEnviron() []string {
	return []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=/tmp", "LANG=C.UTF-8", "PYTHONDONTWRITEBYTECODE=1", "PYTHONUNBUFFERED=1"}
}

// ExecEnvs 是 exec 调度使用的环境操作（Task 10 以 resource.Coordinator + provider 实现）。
type ExecEnvs interface {
	// Create 经 resource coordinator 创建 exec 环境（intent、owner.json、UID 范围）；返回 /in 暂存目录。
	Create(ctx context.Context, r ExecEnvRequest) (inDir string, err error)
	Start(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error)
	// Stop 停止执行树并记录 stopped_at。Recorded 为真才可归还 exec slot；Blocked 表示期限内未确认停止。
	Stop(ctx context.Context, envID string) (ExecStop, error)
	Diag(ctx context.Context, envID string) (provider.ResourceDiag, error)
	OpenOutputs(ctx context.Context, envID string, max int) ([]provider.OutputFile, []provider.SkippedOutput, error)
	// Cleanup 尽力同步清理（Destroy、归还 UID 范围）；失败只记日志，由 cleanup loop 接手（D8）。
	Cleanup(ctx context.Context, envID string) error
	// Hold 声明 try 仍在使用该环境：持有期间 cleanup loop 不销毁它（exec 环境一记录 stopped_at 就是 cleanup
	// 候选，否则 loop 可能抢在诊断与收集 /out 之前销毁它）。runExecTry 在创建之前取得、结束时释放。
	Hold(envID string) (release func())
}

// ExecEnvRequest 是 Create 的输入。
type ExecEnvRequest struct {
	EnvID, AttemptID string
	MemoryBytes      int64 // 生效值
}

// ExecStop 是 Stop 的结果。
type ExecStop struct{ Stopped, Recorded, Blocked bool }

// ExecSlots 是 admission.ExecGate 的窄接口（全局与每任务 exec slot）。
type ExecSlots interface {
	Acquire(ctx context.Context, taskID string) (release func(), err error)
}

// ExecLimits 是一次 exec 的 wall 时间与内存。
type ExecLimits struct{ WallMs, MemoryBytes int64 }

// ExecConfig 装配 exec 调度；零值字段取 §19 补充的默认值。Store、Envs、Slots、ImageDigest 必填。
type ExecConfig struct {
	Store        ExecStore
	Envs         ExecEnvs
	Slots        ExecSlots
	Policy       ExecPolicy
	Default, Max ExecLimits
	QueueTimeout time.Duration // 默认 60 s
	CPURate      float64       // cpu.max 速率（核数），默认 1
	CPUMargin    float64       // 默认 0.1
	ImageDigest  string        // rootfs.TemplateDigest(ExecTemplate())
	StopTimeout  time.Duration // 停止执行树的期限，默认 10 s（§19 exit_grace）
}

func (x ExecConfig) withDefaults() (ExecConfig, error) {
	if x.Store == nil || x.Envs == nil || x.Slots == nil || x.ImageDigest == "" {
		return x, errors.New("call: ExecConfig 的 Store、Envs、Slots、ImageDigest 必填")
	}
	if x.Max.WallMs <= 0 {
		x.Max.WallMs = 300_000
	}
	if x.Max.MemoryBytes <= 0 {
		x.Max.MemoryBytes = 1 << 30
	}
	if x.Default.WallMs <= 0 {
		x.Default.WallMs = 60_000
	}
	if x.Default.MemoryBytes <= 0 {
		x.Default.MemoryBytes = 512 << 20
	}
	x.Default.WallMs = min(x.Default.WallMs, x.Max.WallMs)
	x.Default.MemoryBytes = min(x.Default.MemoryBytes, x.Max.MemoryBytes)
	if x.Policy.CountLimit <= 0 {
		x.Policy.CountLimit = 50
	}
	if x.Policy.CPULimitUsec <= 0 {
		x.Policy.CPULimitUsec = 600_000_000
	}
	if x.Policy.WallLimitMs <= 0 {
		x.Policy.WallLimitMs = 1_800_000
	}
	if x.QueueTimeout <= 0 {
		x.QueueTimeout = 60 * time.Second
	}
	if x.CPURate <= 0 {
		x.CPURate = 1
	}
	if x.CPUMargin <= 0 {
		x.CPUMargin = 0.1
	}
	if x.StopTimeout <= 0 {
		x.StopTimeout = 10 * time.Second
	}
	return x, nil
}

// cpuEstimate 是 CPU 预留（§10.3）：cpu.max 速率 × 生效 wall × (1 + 余量)，单位 usec，向上取整（减去浮点噪声）。
func (x *ExecConfig) cpuEstimate(wallMs int64) int64 {
	return int64(math.Ceil(x.CPURate*float64(wallMs)*1000*(1+x.CPUMargin) - 1e-6))
}

// ExecInvoke 是一次 POST /v1/exec。SubrunID 是 X-Agentbox-Subrun（D16：记入 calls.subrun_id；exec 配额是任务级）。
type ExecInvoke struct {
	TaskID, AttemptID, CallID, SubrunID string
	Body                                []byte
	Retry                               bool
	Supersedes, SupersedeReason         string
}

// execInput 是请求中的一个输入，也是指纹与暂存的单位。
type execInput struct {
	SHA256 string `json:"sha256"`
	Path   string `json:"path"`
}

// execParsed 是校验后的请求：inputs 按 path 排序，limits 为生效值（已截断）。
type execParsed struct {
	code   string
	inputs []execInput
	limits ExecLimits
}

// fields 把 JSON 对象解析为成员表，并拒绝 allowed 之外的成员（大小写敏感）。
func fields(raw json.RawMessage, allowed ...string) (map[string]json.RawMessage, string) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, upstream.CodeInvalidRequest
	}
	for k := range m {
		if !slices.Contains(allowed, k) {
			return nil, upstream.CodeUnsupportedField
		}
	}
	return m, ""
}

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// validInputPath：相对、规范（Clean 不变）、无 ".."、不以 "/" 开头、不在保留的 .agentbox/ 下（§10.1、D3）。
func validInputPath(p string) bool {
	if p == "" || p == "." || strings.HasPrefix(p, "/") || strings.ContainsRune(p, 0) || path.Clean(p) != p {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return false
		}
	}
	return p != ".agentbox" && !strings.HasPrefix(p, ".agentbox/")
}

// parseExec 解析并校验请求体；失败时返回错误码（400）。
func (c *Coordinator) parseExec(body []byte) (execParsed, string) {
	if _, err := jcs.Canonical(json.RawMessage(body)); err != nil {
		if errors.Is(err, jcs.ErrDuplicateKey) {
			return execParsed{}, CodeDuplicateJSONKey
		}
		return execParsed{}, upstream.CodeInvalidRequest
	}
	top, code := fields(body, "language", "code", "inputs", "limits")
	if code != "" {
		return execParsed{}, code
	}
	var lang, src *string
	if json.Unmarshal(top["language"], &lang) != nil || lang == nil || *lang != ExecLanguage ||
		json.Unmarshal(top["code"], &src) != nil || src == nil || len(*src) > MaxExecCodeBytes {
		return execParsed{}, upstream.CodeInvalidRequest
	}
	p := execParsed{code: *src, inputs: []execInput{}, limits: c.exec.Default}
	if raw, ok := top["inputs"]; ok {
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil {
			return execParsed{}, upstream.CodeInvalidRequest
		}
		seen := map[string]bool{}
		for _, it := range items {
			m, code := fields(it, "sha256", "path")
			if code != "" {
				return execParsed{}, code
			}
			var in execInput
			if json.Unmarshal(m["sha256"], &in.SHA256) != nil || json.Unmarshal(m["path"], &in.Path) != nil ||
				!isSHA256Hex(in.SHA256) || !validInputPath(in.Path) || seen[in.Path] {
				return execParsed{}, upstream.CodeInvalidRequest
			}
			seen[in.Path] = true
			p.inputs = append(p.inputs, in)
		}
		// 一个输入不能是另一个输入的上级目录（暂存时会冲突）。
		for _, in := range p.inputs {
			for d := path.Dir(in.Path); d != "."; d = path.Dir(d) {
				if seen[d] {
					return execParsed{}, upstream.CodeInvalidRequest
				}
			}
		}
		sort.Slice(p.inputs, func(a, b int) bool { return p.inputs[a].Path < p.inputs[b].Path })
	}
	if raw, ok := top["limits"]; ok {
		m, code := fields(raw, "wall_ms", "memory_bytes")
		if code != "" {
			return execParsed{}, code
		}
		for name, dst := range map[string]*int64{"wall_ms": &p.limits.WallMs, "memory_bytes": &p.limits.MemoryBytes} {
			v, ok := m[name]
			if !ok {
				continue
			}
			var n *int64
			if json.Unmarshal(v, &n) != nil || n == nil || *n <= 0 {
				return execParsed{}, upstream.CodeInvalidRequest
			}
			*dst = *n
		}
		p.limits.WallMs = min(p.limits.WallMs, c.exec.Max.WallMs)
		p.limits.MemoryBytes = min(p.limits.MemoryBytes, c.exec.Max.MemoryBytes)
	}
	return p, ""
}

// execFingerprint 是 §10.1 的指纹：sha256(JCS({endpoint, adapter_version, resolved: {language, code_sha256,
// inputs（按 path 排序）, image_digest, limits（生效值）}}))。
func execFingerprint(p execParsed, imageDigest string) (string, error) {
	type limits struct {
		WallMs      int64 `json:"wall_ms"`
		MemoryBytes int64 `json:"memory_bytes"`
	}
	type resolved struct {
		Language    string      `json:"language"`
		CodeSHA256  string      `json:"code_sha256"`
		Inputs      []execInput `json:"inputs"`
		ImageDigest string      `json:"image_digest"`
		Limits      limits      `json:"limits"`
	}
	sum := sha256.Sum256([]byte(p.code))
	canon, err := jcs.Canonical(struct {
		Endpoint       string   `json:"endpoint"`
		AdapterVersion string   `json:"adapter_version"`
		Resolved       resolved `json:"resolved"`
	}{ExecEndpoint, ExecAdapterVersion, resolved{ExecLanguage, hex.EncodeToString(sum[:]), p.inputs, imageDigest,
		limits{p.limits.WallMs, p.limits.MemoryBytes}}})
	if err != nil {
		return "", err
	}
	fp := sha256.Sum256(canon)
	return hex.EncodeToString(fp[:]), nil
}

// execJob 是一次 exec 调用在本进程中的执行（CancelAttempt 与 CancelSubrun 的对象）。
type execJob struct {
	in     ExecInvoke
	req    execParsed
	fp     string
	ctx    context.Context // Coordinator 自有；任何撤销原因（D7）、sub-run 取消与关闭都取消它
	cancel context.CancelCauseFunc
}

// errExecCancelled 是 CancelAttempt / CancelSubrun 终止 exec 的取消原因。
var errExecCancelled = errors.New("call: exec 所属的 attempt 或 sub-run 已撤销")

// ErrExecNotConfigured 表示 Coordinator 未配置 exec（Config.Exec 为 nil）；/v1/budget 据此省略 exec 配额。
var ErrExecNotConfigured = errors.New("call: 未配置 exec")

// ExecQuota 返回任务的 exec 配额，供 GET /v1/budget（Task 9）。尚无配额行（首次 exec 之前）时返回 server 策略的
// 上限、用量 0 与 false；有行时返回该行与 true（之后不随策略变化）。未配置 exec 为 ErrExecNotConfigured。
func (c *Coordinator) ExecQuota(ctx context.Context, taskID string) (ExecQuota, bool, error) {
	if c.exec == nil {
		return ExecQuota{}, false, ErrExecNotConfigured
	}
	q, err := c.exec.Store.LoadExecQuota(ctx, taskID)
	if errors.Is(err, persistence.ErrNotFound) {
		p := c.exec.Policy
		return ExecQuota{CountLimit: p.CountLimit, CPULimitUsec: p.CPULimitUsec, WallLimitMs: p.WallLimitMs}, false, nil
	}
	if err != nil {
		return ExecQuota{}, false, err
	}
	return q, true, nil
}

// Exec 处理 POST /v1/exec（§10.2）。Exec 为 nil 时返回 404 endpoint_not_configured。拒绝与失败以 Result 的
// Status/Code 返回、err 为 nil；err 非空表示存储等内部故障，或 ctx 结束（exec 在后台继续并结算，供同 ID 重放）。
func (c *Coordinator) Exec(ctx context.Context, in ExecInvoke) (res Result, err error) {
	ctx, span := obs.Start(ctx, "gateway.call", callAttrs(kindExec, in.TaskID, in.AttemptID, in.CallID, in.SubrunID)...)
	defer func() { endCall(span, kindExec, in.TaskID, res, err) }()
	return c.execCall(ctx, in)
}

// kindExec 是 exec 调用在可观测性中的类别（gateway.kind、指标 kind 标签）。
const kindExec = "exec"

func (c *Coordinator) execCall(ctx context.Context, in ExecInvoke) (Result, error) {
	if in.TaskID == "" || in.AttemptID == "" || in.CallID == "" {
		return Result{}, fmt.Errorf("%w: Exec 缺少 task_id、attempt_id 或 call_id", persistence.ErrInvalid)
	}
	if c.root.Err() != nil {
		return Result{}, ErrClosed
	}
	if c.exec == nil {
		return reject(CodeEndpointNotConfigured), nil
	}
	req, code := c.parseExec(in.Body)
	if code != "" {
		return reject(code), nil
	}
	fp, err := execFingerprint(req, c.exec.ImageDigest)
	if err != nil {
		return reject(upstream.CodeInvalidRequest), nil
	}
	key := callKey{in.TaskID, in.CallID}
	if !c.claim(key) {
		return c.execContended(ctx, in, fp)
	}
	j := &execJob{in: in, req: req, fp: fp}
	j.ctx, j.cancel = context.WithCancelCause(c.root)
	j.ctx = obs.Carry(j.ctx, ctx)
	if !c.registerExec(j) {
		j.cancel(nil)
		c.unclaim(key)
		return Result{}, ErrClosed
	}
	type outcome struct {
		res Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		defer c.wg.Done()
		r, err := c.runExec(j)
		j.cancel(nil)
		c.unregisterExec(j)
		c.unclaim(key)
		done <- outcome{r, err}
	}()
	select {
	case o := <-done:
		return o.res, o.err
	case <-ctx.Done():
		traceDetached(ctx, kindExec, func() (Result, error) { o := <-done; return o.res, o.err })
		return Result{}, ctx.Err()
	}
}

// registerExec 登记 exec 执行（CancelAttempt、CancelSubrun 与 Close）；已关闭时返回 false。
func (c *Coordinator) registerExec(j *execJob) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.root.Err() != nil {
		return false
	}
	c.wg.Add(1)
	m := c.execByAttempt[j.in.AttemptID]
	if m == nil {
		m = map[*execJob]bool{}
		c.execByAttempt[j.in.AttemptID] = m
	}
	m[j] = true
	if j.in.SubrunID != "" {
		sk := subrunKey{j.in.TaskID, j.in.AttemptID, j.in.SubrunID}
		sm := c.execBySubrun[sk]
		if sm == nil {
			sm = map[*execJob]bool{}
			c.execBySubrun[sk] = sm
		}
		sm[j] = true
	}
	return true
}

func (c *Coordinator) unregisterExec(j *execJob) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.execByAttempt[j.in.AttemptID]
	delete(m, j)
	if len(m) == 0 {
		delete(c.execByAttempt, j.in.AttemptID)
	}
	if j.in.SubrunID != "" {
		sk := subrunKey{j.in.TaskID, j.in.AttemptID, j.in.SubrunID}
		sm := c.execBySubrun[sk]
		delete(sm, j)
		if len(sm) == 0 {
			delete(c.execBySubrun, sk)
		}
	}
}

// execContended 处理进程内已有执行者的同 ID exec：仍按 §9.2 检查访问；指纹不同报告分歧，否则 call_in_progress。
func (c *Coordinator) execContended(ctx context.Context, in ExecInvoke, fp string) (Result, error) {
	if r, err := c.CheckAccess(ctx, in.TaskID, in.AttemptID, in.SubrunID); err != nil || r.Code != "" {
		return r, err
	}
	rec, _, err := c.store.LoadCall(ctx, in.TaskID, in.CallID)
	if err == nil && rec.Fingerprint != fp {
		return c.diverge(in.TaskID, in.AttemptID, in.CallID, ExecEndpoint, rec.Fingerprint, fp), nil
	}
	return reject(persistence.CodeCallInProgress), nil
}

// failExec 结束尚未预留（或预留被拒）的 exec 调用：调用置为 failed（原因即 code）；unknown 调用保持 unknown
// （此前 try 的 CPU 已转入 unknown，状态如实反映，之后仍可在上限内重跑）。
func (c *Coordinator) failExec(in ExecInvoke, state CallState, code string) (Result, error) {
	if state != StateUnknown {
		ctx, cancel := c.opCtx()
		err := c.store.FailCall(ctx, in.TaskID, in.CallID, code)
		cancel()
		if err != nil && !errors.Is(err, persistence.ErrRejected) && !errors.Is(err, persistence.ErrConflict) {
			return Result{}, err
		}
	}
	return reject(code), nil
}

// runExec 是一次 exec 调用的后台执行：Tx1 与按已有记录分流、输入授权、排队与 Tx2，然后运行这个 try。
func (c *Coordinator) runExec(j *execJob) (Result, error) {
	in, x := j.in, c.exec
	ctx, cancel := c.opCtx()
	res, err := c.store.BeginCall(ctx, BeginCallRequest{
		TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID, Fingerprint: j.fp, Endpoint: ExecEndpoint,
		Deadline:         x.QueueTimeout + time.Duration(j.req.limits.WallMs)*time.Millisecond + execDeadlineMargin,
		SupersedesCallID: in.Supersedes, SupersedeReason: in.SupersedeReason, SubrunID: in.SubrunID,
	})
	cancel()
	if err != nil {
		if r, ok := rejection(err); ok {
			return r, nil
		}
		return Result{}, err
	}
	rec := res.Record
	if res.Existing {
		if rec.Fingerprint != j.fp {
			return c.diverge(in.TaskID, in.AttemptID, in.CallID, ExecEndpoint, rec.Fingerprint, j.fp), nil
		}
		switch rec.State {
		case StateCompleted:
			return c.replay(rec)
		case StateResolving, StateInFlight:
			return reject(persistence.CodeCallInProgress), nil
		case StateUnknown:
			// 重跑：ReserveExec 确认此前 try 的环境均已 stopped_at（§10.1）。
		case StateFailed:
			if !in.Retry || !retryableReason(rec.FailReason) || rec.TriesUsed >= c.limits.MaxTries {
				return persistedFailure(rec), nil
			}
		default:
			return Result{}, fmt.Errorf("call: 调用 %s/%s 的状态 %q 未知", rec.TaskID, rec.CallID, rec.State)
		}
	}
	state := rec.State

	// 输入授权：每个 sha 须在 scope_blobs(task) 中；否则不排队、不预留、不建环境。
	for _, inp := range j.req.inputs {
		ctx, cancel := c.opCtx()
		ok, err := c.store.BlobAuthorized(ctx, in.TaskID, inp.SHA256)
		cancel()
		if err != nil {
			if _, ferr := c.failExec(in, state, CodeStoreUnavailable); ferr != nil {
				c.log.Warn("gateway: exec 输入授权检查失败后无法把调用置为 failed", "task_id", in.TaskID, "call_id", in.CallID, "err", ferr)
			}
			return Result{}, err
		}
		if !ok {
			return c.failExec(in, state, CodeInputNotAuthorized)
		}
	}

	// 等待 exec slot：受排队上限与调用期限（取早）约束，也随取消结束。
	qctx, qcancel := context.WithDeadline(j.ctx, earlier(rec.DeadlineAt, c.now().Add(x.QueueTimeout)))
	qstart := c.now()
	release, err := x.Slots.Acquire(qctx, in.TaskID)
	qerr := qctx.Err()
	qcancel()
	queueMs := max(c.now().Sub(qstart).Milliseconds(), 0)
	if err != nil {
		return c.failExec(in, state, c.execQueueCode(j, rec.DeadlineAt, qerr))
	}

	// Tx2：复查访问、期限、累计次数与 exec 配额后预留（提交之后才建环境）。
	rid, err := newExecReservationID()
	if err != nil {
		release()
		if _, ferr := c.failExec(in, state, CodeStoreUnavailable); ferr != nil {
			c.log.Warn("gateway: 无法把调用置为 failed", "task_id", in.TaskID, "call_id", in.CallID, "err", ferr)
		}
		return Result{}, err
	}
	sctx, scancel := c.opCtx()
	try, err := x.Store.ReserveExec(sctx, ReserveExecRequest{
		TaskID: in.TaskID, CallID: in.CallID, AttemptID: in.AttemptID, SubrunID: in.SubrunID, ReservationID: rid,
		CPUEstimateUsec: x.cpuEstimate(j.req.limits.WallMs), WallMs: j.req.limits.WallMs, QueueMs: queueMs,
		MaxTries: c.limits.MaxTries, Policy: x.Policy,
	})
	scancel()
	if err != nil {
		release()
		var rej *persistence.RejectedError
		if errors.As(err, &rej) {
			if rej.Code == persistence.CodeCallInProgress {
				return reject(rej.Code), nil
			}
			return c.failExec(in, state, rej.Code)
		}
		if _, ferr := c.failExec(in, state, CodeStoreUnavailable); ferr != nil {
			c.log.Warn("gateway: exec Tx2 失败后无法把调用置为 failed", "task_id", in.TaskID, "call_id", in.CallID, "err", ferr)
		}
		return Result{}, err
	}
	return c.runExecTry(&execRun{j: j, try: try, release: release, queueMs: queueMs})
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// execQueueCode 是排队失败的原因：关闭 → gateway_shutdown；取消（D7）→ exec_cancelled；调用期限已到 →
// call_deadline_exceeded；排队上限 → exec_queue_timeout；slot 拒绝（容量不可用）→ exec_env_unavailable。
func (c *Coordinator) execQueueCode(j *execJob, deadline time.Time, qerr error) string {
	switch {
	case c.root.Err() != nil:
		return CodeGatewayShutdown
	case j.ctx.Err() != nil:
		return CodeExecCancelled
	case !c.now().Before(deadline):
		return persistence.CodeCallDeadlineExceeded
	case qerr != nil:
		return CodeExecQueueTimeout
	}
	return CodeExecEnvUnavailable
}

// newExecReservationID 生成 Tx2 的操作身份（§7.3：事务前生成，ReserveExec 以它幂等）。
func newExecReservationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("call: 生成 reservation id: %w", err)
	}
	return "rsv-exec-" + hex.EncodeToString(b[:]), nil
}

// execRun 是一个已预留的 exec try 的执行状态。
type execRun struct {
	j       *execJob
	try     ExecTry
	release func()
	queueMs int64
	wallMs  int64
}

func (c *Coordinator) envCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.storeTimeout)
}

// runExecTry 建环境、暂存、启动并等待 exec，然后停止、收集与结算（§10.2）。
func (c *Coordinator) runExecTry(r *execRun) (Result, error) {
	x, j, envID := c.exec, r.j, r.try.EnvID
	// 停止（记录 stopped_at）、诊断、收集与同步清理都在持有期间：cleanup loop 不会在收集之前销毁环境。
	defer x.Envs.Hold(envID)()
	ctx, cancel := c.envCtx()
	inDir, err := x.Envs.Create(ctx, ExecEnvRequest{EnvID: envID, AttemptID: j.in.AttemptID, MemoryBytes: j.req.limits.MemoryBytes})
	cancel()
	if err != nil {
		c.log.Warn("gateway: 创建 exec 环境失败", "task_id", j.in.TaskID, "call_id", j.in.CallID, "env_id", envID, "err", err)
		return c.abortExec(r, ExecStartFailed, CodeExecEnvUnavailable, CodeExecEnvUnavailable+": "+err.Error(), false, false)
	}
	if code, err := c.stageExec(inDir, j.req); err != nil {
		c.log.Warn("gateway: exec /in 暂存失败", "task_id", j.in.TaskID, "call_id", j.in.CallID, "env_id", envID, "err", err)
		return c.abortExec(r, ExecStartFailed, code, code+": "+err.Error(), true, false)
	}
	// I12 [A]：进程内取消检查与 CancelAttempt 共用 c.mu——之前的取消在这里可见，之后的取消经 j.ctx 停止它。
	if c.execCancelled(j) {
		return c.abortExec(r, ExecCancelled, CodeExecCancelled, "cancelled before start", true, false)
	}
	ctx, cancel = c.opCtx()
	err = x.Store.MarkExecStarting(ctx, r.try)
	cancel()
	if err != nil { // 访问已失效（撤销先提交）或无法确认：不启动
		return c.abortExec(r, ExecCancelled, CodeExecCancelled, "mark_starting: "+err.Error(), true, false)
	}
	if c.execCancelled(j) {
		return c.abortExec(r, ExecCancelled, CodeExecCancelled, "cancelled before start", true, false)
	}
	ctx, cancel = c.envCtx()
	h, err := x.Envs.Start(ctx, envID, provider.ExecSpec{ExecID: envID, Argv: ExecArgv(), Env: ExecEnviron(), Dir: "/out"})
	cancel()
	if err != nil {
		if errors.Is(err, provider.ErrStartFailed) {
			return c.abortExec(r, ExecStartFailed, CodeExecStartFailed, err.Error(), true, false)
		}
		// 启动结果未知（ErrControlLost 等）：停止并确认后按 unknown 结算，保守计入 exec_count。
		return c.abortExec(r, ExecUnknown, CodeExecUnknown, "start: "+err.Error(), true, true)
	}
	return c.superviseExec(r, h)
}

func (c *Coordinator) execCancelled(j *execJob) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return j.ctx.Err() != nil
}

// stopExecEnv 停止执行树（整个 exec 环境）。Stopped 但未记录 stopped_at 时重试（slot 只在记录之后归还）。
func (c *Coordinator) stopExecEnv(envID string) ExecStop {
	var st ExecStop
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.exec.StopTimeout+c.storeTimeout)
		s, err := c.exec.Envs.Stop(ctx, envID)
		cancel()
		if err != nil {
			c.log.Warn("gateway: 停止 exec 环境失败", "env_id", envID, "err", err)
			continue
		}
		st = s
		if s.Blocked || (s.Stopped && s.Recorded) {
			return s
		}
	}
	return st
}

// abortExec 结束没有正常运行完的 try（创建、暂存、启动前取消、启动失败或启动结果未知）：停止并确认无残留 →
// 诊断（env 已创建时）→ 清理 → 结算 → 归还 slot。无法确认停止时按 unknown 结算、不清理、不归还。
func (c *Coordinator) abortExec(r *execRun, outcome ExecOutcome, code, detail string, created, started bool) (Result, error) {
	envID := r.try.EnvID
	stop := c.stopExecEnv(envID)
	if !stop.Stopped || stop.Blocked {
		return c.settleUnknown(r, started, "stop_unconfirmed; "+detail)
	}
	cpu, known := int64(0), true // 没有创建成功的环境没有用量
	if created {
		var d provider.ResourceDiag
		d, known = c.execDiag(envID)
		cpu = cpuUsec(d)
	}
	c.cleanupExec(envID)
	_, err := c.settleExec(ExecSettlement{Try: r.try, Outcome: outcome, Started: started, CPUUsec: cpu, CPUKnown: known,
		WallMs: r.wallMs, QueueMs: r.queueMs, Error: detail})
	if stop.Recorded {
		r.release()
	}
	if err != nil {
		return Result{}, err
	}
	return reject(code), nil
}

// settleUnknown 在无法确认执行树已停止时结算：CPU 全额转 unknown，不收集、不清理、不归还 slot（环境留给启动恢复
// 与 cleanup loop；Task 10 以 ExecGate.Occupy 占用）。
func (c *Coordinator) settleUnknown(r *execRun, started bool, detail string) (Result, error) {
	c.log.Error("gateway: 无法确认 exec 执行树已停止，exec slot 保持占用", "task_id", r.j.in.TaskID, "call_id", r.j.in.CallID,
		"env_id", r.try.EnvID)
	if _, err := c.settleExec(ExecSettlement{Try: r.try, Outcome: ExecUnknown, Started: started, WallMs: r.wallMs,
		QueueMs: r.queueMs, Error: detail}); err != nil {
		return Result{}, err
	}
	return reject(CodeExecUnknown), nil
}

// execDiag 读取停止后的 cpu.stat 与 memory.events；失败时 known 为假（CPU 按全额预留计入）。
func (c *Coordinator) execDiag(envID string) (provider.ResourceDiag, bool) {
	ctx, cancel := c.envCtx()
	defer cancel()
	d, err := c.exec.Envs.Diag(ctx, envID)
	if err != nil {
		c.log.Warn("gateway: 读取 exec 诊断失败，CPU 按全额预留计入", "env_id", envID, "err", err)
		return provider.ResourceDiag{}, false
	}
	return d, true
}

func cpuUsec(d provider.ResourceDiag) int64 { return int64(min(d.CPUUsageUsec, math.MaxInt64)) }

func (c *Coordinator) cleanupExec(envID string) {
	ctx, cancel := c.envCtx()
	defer cancel()
	if err := c.exec.Envs.Cleanup(ctx, envID); err != nil {
		c.log.Warn("gateway: exec 环境同步清理失败，交给 cleanup loop", "env_id", envID, "err", err)
	}
}

// settleExec 结算（存储故障时重试一次；以 reservation 状态幂等）。
func (c *Coordinator) settleExec(s ExecSettlement) (CallRecord, error) {
	var rec CallRecord
	var err error
	for i := 0; i < 2; i++ {
		ctx, cancel := c.opCtx()
		rec, err = c.exec.Store.SettleExec(ctx, s)
		cancel()
		if err == nil || errors.Is(err, persistence.ErrInvalid) || errors.Is(err, persistence.ErrConflict) ||
			errors.Is(err, persistence.ErrNotFound) {
			break
		}
	}
	if err != nil {
		c.log.Error("gateway: exec 结算失败", "task_id", s.Try.TaskID, "call_id", s.Try.CallID, "try_no", s.Try.TryNo, "err", err)
	}
	return rec, err
}

// capture 是 stdout 或 stderr 保留的部分。
type capture struct {
	data      []byte
	truncated bool
}

// readCapped 读取到 EOF：保留前 limit 字节，其余读取并丢弃（不让 workload 因管道写满而挂起，E36）。
func readCapped(rc io.ReadCloser, limit int64) capture {
	defer rc.Close()
	// 读取错误只会来自管道关闭（执行树已停止），按 EOF 处理。
	data, _ := io.ReadAll(io.LimitReader(rc, limit))
	n, _ := io.Copy(io.Discard, rc)
	return capture{data: data, truncated: n > 0}
}

// decodeStream 以 UTF-8 解码（D12）：截断处不完整的末尾字符去掉；非法字节替换为 U+FFFD 并报告。
func decodeStream(c capture) (string, bool) {
	b := c.data
	if c.truncated {
		for k := 1; k <= utf8.UTFMax-1 && k <= len(b); k++ {
			if utf8.RuneStart(b[len(b)-k]) {
				if !utf8.FullRune(b[len(b)-k:]) {
					b = b[:len(b)-k]
				}
				break
			}
		}
	}
	if utf8.Valid(b) {
		return string(b), false
	}
	return strings.ToValidUTF8(string(b), "�"), true
}

type waitResult struct {
	status provider.ExitStatus
	err    error
}

// superviseExec 运行已启动的 exec：并发读取输出，等待退出、wall 到期或取消，停止执行树，收集并结算。
func (c *Coordinator) superviseExec(r *execRun, h provider.ExecHandle) (Result, error) {
	x, j, envID := c.exec, r.j, r.try.EnvID
	if err := h.Stdin().Close(); err != nil {
		c.log.Warn("gateway: 关闭 exec stdin 失败", "env_id", envID, "err", err)
	}
	var readers sync.WaitGroup
	var stdout, stderr capture
	readers.Add(2)
	go func() { defer readers.Done(); stdout = readCapped(h.Stdout(), MaxExecStreamBytes) }()
	go func() { defer readers.Done(); stderr = readCapped(h.Stderr(), MaxExecStreamBytes) }()
	waitCh := make(chan waitResult, 1)
	go func() {
		st, err := h.Wait()
		waitCh <- waitResult{st, err}
	}()

	begin := c.now()
	timer := time.NewTimer(time.Duration(j.req.limits.WallMs) * time.Millisecond)
	var outcome ExecOutcome
	var wr *waitResult
	select {
	case w := <-waitCh:
		wr, outcome = &w, ExecCompleted
	case <-timer.C:
		outcome = ExecTimedOut
	case <-j.ctx.Done():
		outcome = ExecCancelled
	}
	timer.Stop()
	r.wallMs = max(c.now().Sub(begin).Milliseconds(), 0)

	// 停止整个执行树（populated 0）并记录 stopped_at；无法确认 → unknown，读取 goroutine 留给管道关闭时结束。
	stop := c.stopExecEnv(envID)
	if !stop.Stopped || stop.Blocked {
		return c.settleUnknown(r, true, "stop_unconfirmed")
	}
	// 执行树已停，管道必然 EOF；仍以期限防御异常的句柄。
	bound := time.NewTimer(x.StopTimeout)
	defer bound.Stop()
	drained := make(chan struct{})
	go func() { readers.Wait(); close(drained) }()
	ok := true
	select {
	case <-drained:
	case <-bound.C:
		ok = false
	}
	if ok && wr == nil {
		select {
		case w := <-waitCh:
			wr = &w
		case <-bound.C:
		}
	}
	detail := ""
	switch {
	case outcome == ExecCompleted && wr.err != nil:
		outcome, detail = ExecUnknown, "wait: "+wr.err.Error()
	case !ok && outcome != ExecCancelled:
		outcome, detail = ExecUnknown, "output_not_drained"
	}
	d, known := c.execDiag(envID)
	cpu := cpuUsec(d)

	var res Result
	settle := ExecSettlement{Try: r.try, Started: true, CPUUsec: cpu, CPUKnown: known, QueueMs: r.queueMs, WallMs: r.wallMs}
	if outcome == ExecCompleted || outcome == ExecTimedOut {
		var exit *provider.ExitStatus
		if wr != nil && wr.err == nil {
			exit = &wr.status
		}
		var diag *provider.ResourceDiag
		if known {
			diag = &d
		}
		body, outputs, err := c.collectExec(r, outcome, exit, diag, stdout, stderr)
		if err != nil {
			outcome, detail = ExecUnknown, "collect: "+err.Error()
		} else {
			ctx, cancel := c.opCtx()
			ref, err := c.blobs.Put(ctx, bytes.NewReader(body))
			cancel()
			if err != nil {
				outcome, detail = ExecUnknown, CodeBlobWriteFailed+": "+err.Error()
			} else {
				settle.ResultSHA256, settle.ResultSize, settle.Outputs = ref.SHA256, ref.Size, outputs
				res = Result{Body: body, BlobSHA256: ref.SHA256, Status: 200}
			}
		}
	}
	settle.Outcome, settle.Error = outcome, detail
	c.cleanupExec(envID)
	_, err := c.settleExec(settle)
	if stop.Recorded {
		r.release()
	}
	c.log.Info("gateway: exec", "task_id", j.in.TaskID, "attempt_id", j.in.AttemptID, "call_id", j.in.CallID,
		"try_no", r.try.TryNo, "env_id", envID, "outcome", string(outcome), "queue_ms", r.queueMs, "wall_ms", r.wallMs,
		"cpu_usec", cpu, "cpu_known", known)
	if err != nil {
		return Result{}, err
	}
	switch outcome {
	case ExecCompleted, ExecTimedOut:
		return res, nil
	case ExecCancelled:
		return reject(CodeExecCancelled), nil
	}
	return reject(CodeExecUnknown), nil
}

// execResult 是结果 blob 的内容，也是 HTTP 200 正文（Task 8 结果 JSON）。
type execResult struct {
	Status            string            `json:"status"`
	Exit              *execExit         `json:"exit"`
	Diag              *execDiagJSON     `json:"diag"`
	Stdout            string            `json:"stdout"`
	StdoutTruncated   bool              `json:"stdout_truncated"`
	StdoutInvalidUTF8 bool              `json:"stdout_invalid_utf8"`
	Stderr            string            `json:"stderr"`
	StderrTruncated   bool              `json:"stderr_truncated"`
	StderrInvalidUTF8 bool              `json:"stderr_invalid_utf8"`
	Outputs           []execOutputJSON  `json:"outputs"`
	SkippedOutputs    []execSkippedJSON `json:"skipped_outputs"`
	QueueMs           int64             `json:"queue_ms"`
	WallMs            int64             `json:"wall_ms"`
	Limits            execLimitsJSON    `json:"limits"`
	ImageDigest       string            `json:"image_digest"`
}

type execExit struct {
	Code   int `json:"code"`
	Signal int `json:"signal"`
}

type execDiagJSON struct {
	OOMKillDelta uint64 `json:"oom_kill_delta"`
	OOMObserved  bool   `json:"oom_observed"`
	CPUUsageUsec uint64 `json:"cpu_usage_usec"`
}

type execOutputJSON struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type execSkippedJSON struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type execLimitsJSON struct {
	WallMs      int64 `json:"wall_ms"`
	MemoryBytes int64 `json:"memory_bytes"`
}

// collectExec 打开 /out 的稳定文件并逐个保存为 blob（无论成败都在返回前全部关闭：Destroy 需要它们已关闭，
// 否则 EBUSY），然后组装结果 JSON。
func (c *Coordinator) collectExec(r *execRun, outcome ExecOutcome, exit *provider.ExitStatus, diag *provider.ResourceDiag,
	stdout, stderr capture) ([]byte, []ExecOutput, error) {
	ctx, cancel := c.envCtx()
	files, skipped, err := c.exec.Envs.OpenOutputs(ctx, r.try.EnvID, provider.MaxOutputFiles)
	cancel()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		for _, f := range files {
			if f.File != nil {
				if err := f.File.Close(); err != nil {
					c.log.Warn("gateway: 关闭 exec 输出文件失败", "env_id", r.try.EnvID, "path", f.Path, "err", err)
				}
			}
		}
	}()
	res := execResult{Status: string(outcome), Outputs: []execOutputJSON{}, SkippedOutputs: []execSkippedJSON{},
		QueueMs: r.queueMs, WallMs: r.wallMs, ImageDigest: c.exec.ImageDigest,
		Limits: execLimitsJSON{r.j.req.limits.WallMs, r.j.req.limits.MemoryBytes}}
	var outputs []ExecOutput
	for _, f := range files {
		if f.File == nil {
			return nil, nil, fmt.Errorf("输出 %s 没有打开的文件", f.Path)
		}
		ctx, cancel := c.opCtx()
		ref, err := c.blobs.Put(ctx, f.File)
		cancel()
		if err != nil {
			return nil, nil, fmt.Errorf("保存输出 %s: %w", f.Path, err)
		}
		outputs = append(outputs, ExecOutput{SHA256: ref.SHA256, Size: ref.Size})
		res.Outputs = append(res.Outputs, execOutputJSON{Path: f.Path, SHA256: ref.SHA256, Size: ref.Size})
	}
	for _, s := range skipped {
		res.SkippedOutputs = append(res.SkippedOutputs, execSkippedJSON{Path: s.Path, Reason: s.Reason})
	}
	if exit != nil {
		res.Exit = &execExit{Code: exit.Code, Signal: int(exit.Signal)}
	}
	if diag != nil {
		res.Diag = &execDiagJSON{OOMKillDelta: diag.OOMKillDelta, OOMObserved: diag.OOMObserved, CPUUsageUsec: diag.CPUUsageUsec}
	}
	res.StdoutTruncated, res.StderrTruncated = stdout.truncated, stderr.truncated
	res.Stdout, res.StdoutInvalidUTF8 = decodeStream(stdout)
	res.Stderr, res.StderrInvalidUTF8 = decodeStream(stderr)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(res); err != nil {
		return nil, nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), outputs, nil
}

// errInputsTooLarge 表示输入累计超过上限。
var errInputsTooLarge = errors.New("call: exec 输入累计超过上限")

// stageExec 把代码写入 /in/.agentbox/main.py，并按 inputs 从 BlobStore 复制（文件 0444、目录 0755，D2/D3）。
// 返回失败时的错误码：累计超过上限 → inputs_too_large；其余 → exec_env_unavailable。
func (c *Coordinator) stageExec(inDir string, req execParsed) (string, error) {
	if _, err := writeStaged(inDir, execMainRel, strings.NewReader(req.code), -1); err != nil {
		return CodeExecEnvUnavailable, err
	}
	var total int64
	for _, in := range req.inputs {
		rc, err := c.blobs.Open(in.SHA256)
		if err != nil {
			return CodeExecEnvUnavailable, fmt.Errorf("打开输入 %s: %w", in.SHA256, err)
		}
		n, err := writeStaged(inDir, in.Path, rc, c.execInputMax-total)
		if cerr := rc.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if errors.Is(err, errInputsTooLarge) {
			return CodeInputsTooLarge, err
		}
		if err != nil {
			return CodeExecEnvUnavailable, err
		}
		total += n
	}
	return "", nil
}

// writeStaged 在 root 下建立 rel（上级目录 0755）并写入 r；limit ≥ 0 时超过即 errInputsTooLarge。
func writeStaged(root, rel string, r io.Reader, limit int64) (int64, error) {
	cur := root
	if dir := path.Dir(rel); dir != "." {
		for _, seg := range strings.Split(dir, "/") {
			cur = filepath.Join(cur, seg)
			if err := os.Mkdir(cur, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
				return 0, err
			}
			if err := os.Chmod(cur, 0o755); err != nil {
				return 0, err
			}
		}
	}
	dst := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.Remove(dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return 0, err
	}
	src := r
	if limit >= 0 {
		src = io.LimitReader(r, limit+1)
	}
	n, err := io.Copy(f, src)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return n, err
	}
	if limit >= 0 && n > limit {
		return n, errInputsTooLarge
	}
	return n, os.Chmod(dst, 0o444)
}
