package protocol

// sub-run 扩展（规格 §5.4 sub-run 扩展、§13；M4 Plan 14）。经协商启用：宿主在 init 中携带
// extensions: ["subruns"]，Worker 的 ready 回 subruns: 1。subrun_started 的 status/code 是对规格
// §5.4 的补充（见 protocol/README.md「sub-run 扩展」）。
//
// 本包只依赖标准库，因此 ID 规则在此单独实现，与 internal/subrun.ValidID 一致。

// 消息类型。
const (
	TypeSubrunStart           = "subrun_start"            // Worker→宿主
	TypeSubrunStarted         = "subrun_started"          // 宿主→Worker
	TypeSubrunEnd             = "subrun_end"              // Worker→宿主（提议）
	TypeSubrunCancel          = "subrun_cancel"           // Worker→宿主（编排层放弃）
	TypeSubrunCancelRequested = "subrun_cancel_requested" // 宿主→Worker
)

// 枚举取值与上限。
const (
	// ExtensionSubruns 是 init.extensions 中请求 sub-run 扩展的名称。
	ExtensionSubruns = "subruns"
	// SubrunsExtVersion 是 ready.subruns 确认扩展时的取值（0 或缺省表示未确认）。
	SubrunsExtVersion = 1

	SubrunStatusStarted  = "started" // subrun_started.status
	SubrunStatusRejected = "rejected"

	SubrunEndSucceeded = "succeeded"
	SubrunEndFailed    = "failed"
	SubrunEndCancelled = "cancelled"

	SubrunCancelDeadline   = "deadline"
	SubrunCancelTaskCancel = "task_cancel"
	SubrunCancelPolicy     = "policy"

	// checkpoint subruns[].status 允许的取值
	CheckpointSubrunStarted   = "started"
	CheckpointSubrunCompleted = "completed"
	CheckpointSubrunFailed    = "failed"
	CheckpointSubrunCancelled = "cancelled"

	// SubrunTimedOut 只出现在宿主裁定的状态中（resume.subruns[]），以及 result.subruns[]。
	SubrunTimedOut = "timed_out"

	MaxSubrunIDBytes  = 32
	MaxSubrunDeadline = 3_600_000 // ms
	MaxSubrunSummary  = 4096      // subrun_end.summary 与 result.subruns[].summary，UTF-8 字节
	// MaxSubrunsPerTask 是每个 task 的逻辑 sub-run 上限，也是 subruns[] 数组的长度上限（规格 §13.1）。
	MaxSubrunsPerTask = 4
	// MaxParentStepIDBytes 是 subrun_start.parent_step_id 的上限（UTF-8 字节）。
	MaxParentStepIDBytes = 256
)

// subrun_started.code 的已知取值（rejected 时必填；宿主可扩展，Worker 按未知失败处理）。
const (
	SubrunRejectLimit          = "subrun_limit"
	SubrunRejectConflict       = "conflict"      // 同 ID 不同定义
	SubrunRejectClosed         = "subrun_closed" // 该 ID 已是终态
	SubrunRejectInvalidField   = "invalid_field"
	SubrunRejectRetryableError = "retryable_error" // Store 暂时故障，SDK 以同一定义重试
)

// 事件流错误码（sub-run 扩展）。
const (
	CodeExtensionMismatch      = "extension_mismatch"       // ready.subruns 与 init.extensions 的请求不一致
	CodeExtensionNotNegotiated = "extension_not_negotiated" // 未协商 subruns 却出现 sub-run 事件或字段
	CodeSubrunUnknown          = "subrun_unknown"           // 引用了本 attempt 中尚未 subrun_start 的 ID
)

// SubrunStart 请求启动一个 sub-run；BudgetCapMicro 缺省表示 sub-run 层只做归属、不设上限。
type SubrunStart struct {
	EventHeader
	SubrunID       string `json:"subrun_id"`
	ParentStepID   string `json:"parent_step_id"`
	BudgetCapMicro *int64 `json:"budget_cap_micro,omitempty"`
	DeadlineMS     int64  `json:"deadline_ms"`
}

// SubrunStarted 答复 subrun_start：started，或 rejected 并带 code。
type SubrunStarted struct {
	HostHeader
	SubrunID string `json:"subrun_id"`
	Status   string `json:"status"`
	Code     string `json:"code,omitempty"`
}

// SubrunEnd 提议结束一个 sub-run；succeeded 只有在已提交 checkpoint 列出 completed 后才成立（规格 §13.5）。
type SubrunEnd struct {
	EventHeader
	SubrunID string `json:"subrun_id"`
	Status   string `json:"status"`
	Summary  string `json:"summary"`
}

// SubrunCancel 表示编排层放弃一个 sub-run；reason 为自由文本。
type SubrunCancel struct {
	EventHeader
	SubrunID string `json:"subrun_id"`
	Reason   string `json:"reason"`
}

// SubrunCancelRequested 要求 Worker 取消一个 sub-run 并回复 subrun_end{cancelled}。
type SubrunCancelRequested struct {
	HostHeader
	SubrunID string `json:"subrun_id"`
	Reason   string `json:"reason"`
}

// CheckpointSubrun 是 checkpoint.subruns[] 的一项；ResultRef 只在 completed 时必填。
type CheckpointSubrun struct {
	SubrunID  string `json:"subrun_id"`
	Status    string `json:"status"`
	ResultRef string `json:"result_ref,omitempty"`
}

// ResumeSubrun 是 init.resume.subruns[] / task_start.resume.subruns[] 的一项（宿主裁定的状态）。
type ResumeSubrun struct {
	SubrunID  string `json:"subrun_id"`
	Status    string `json:"status"` // started | completed | cancelled | failed | timed_out
	ResultRef string `json:"result_ref,omitempty"`
}

// ResultSubrun 是 result.subruns[] 的一项。
type ResultSubrun struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Summary string `json:"summary"`
}

func (*SubrunStart) MessageType() string           { return TypeSubrunStart }
func (*SubrunStarted) MessageType() string         { return TypeSubrunStarted }
func (*SubrunEnd) MessageType() string             { return TypeSubrunEnd }
func (*SubrunCancel) MessageType() string          { return TypeSubrunCancel }
func (*SubrunCancelRequested) MessageType() string { return TypeSubrunCancelRequested }

// init 把 sub-run 消息注册到 task 与 session 两个 registry。sessionRegistry 是包级变量，
// 在 init 之前已由 registry 复制生成，因此两处都要登记。
func init() {
	for _, reg := range []messageRegistry{registry, sessionRegistry} {
		reg[HostToWorker][TypeSubrunStarted] = func() Message { return &SubrunStarted{} }
		reg[HostToWorker][TypeSubrunCancelRequested] = func() Message { return &SubrunCancelRequested{} }
		reg[WorkerToHost][TypeSubrunStart] = func() Message { return &SubrunStart{} }
		reg[WorkerToHost][TypeSubrunEnd] = func() Message { return &SubrunEnd{} }
		reg[WorkerToHost][TypeSubrunCancel] = func() Message { return &SubrunCancel{} }
	}
}

// ---- 校验 ----

// validSubrunID 判断 id 是否满足 ^[a-z0-9][a-z0-9_-]{0,31}$ 且不等于 root。
func validSubrunID(id string) bool {
	if id == "" || len(id) > MaxSubrunIDBytes || id == "root" {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '_' || c == '-') && i > 0:
		default:
			return false
		}
	}
	return true
}

// checkSubrunID：空为 missing_field，不合规则为 invalid_field。
func checkSubrunID(field, id string) error {
	if err := required(field, id); err != nil {
		return err
	}
	if !validSubrunID(id) {
		return newError(CodeInvalidField, "%s=%q 须匹配 ^[a-z0-9][a-z0-9_-]{0,31}$ 且不是 root", field, id)
	}
	return nil
}

func checkSummary(field, s string) error {
	if len(s) > MaxSubrunSummary {
		return newError(CodeInvalidField, "%s %d 字节，上限 %d", field, len(s), MaxSubrunSummary)
	}
	return nil
}

func (m *SubrunStart) validate() error {
	if err := firstErr(checkEvent(m.EventHeader), checkSubrunID("subrun_id", m.SubrunID), required("parent_step_id", m.ParentStepID)); err != nil {
		return err
	}
	if len(m.ParentStepID) > MaxParentStepIDBytes {
		return newError(CodeInvalidField, "parent_step_id %d 字节，上限 %d", len(m.ParentStepID), MaxParentStepIDBytes)
	}
	if m.DeadlineMS < 1 || m.DeadlineMS > MaxSubrunDeadline {
		return newError(CodeInvalidField, "deadline_ms=%d，须在 1–%d 之间", m.DeadlineMS, MaxSubrunDeadline)
	}
	if m.BudgetCapMicro != nil && *m.BudgetCapMicro < 0 {
		return newError(CodeInvalidField, "budget_cap_micro=%d，必须 ≥ 0", *m.BudgetCapMicro)
	}
	return nil
}

func (m *SubrunStarted) validate() error {
	if err := firstErr(checkVersion(m.V), checkSubrunID("subrun_id", m.SubrunID), oneOf("status", m.Status, SubrunStatusStarted, SubrunStatusRejected)); err != nil {
		return err
	}
	if m.Status == SubrunStatusRejected {
		if m.Code == "" {
			return newError(CodeInvalidField, "rejected 的 subrun_started 必须带 code")
		}
		return nil
	}
	if m.Code != "" {
		return newError(CodeInvalidField, "started 的 subrun_started 不得带 code")
	}
	return nil
}

func (m *SubrunEnd) validate() error {
	return firstErr(
		checkEvent(m.EventHeader),
		checkSubrunID("subrun_id", m.SubrunID),
		oneOf("status", m.Status, SubrunEndSucceeded, SubrunEndFailed, SubrunEndCancelled),
		checkSummary("summary", m.Summary),
	)
}

func (m *SubrunCancel) validate() error {
	return firstErr(checkEvent(m.EventHeader), checkSubrunID("subrun_id", m.SubrunID), required("reason", m.Reason))
}

func (m *SubrunCancelRequested) validate() error {
	return firstErr(
		checkVersion(m.V),
		checkSubrunID("subrun_id", m.SubrunID),
		oneOf("reason", m.Reason, SubrunCancelDeadline, SubrunCancelTaskCancel, SubrunCancelPolicy),
	)
}

// subrunEntry 是三种 subruns[] 数组项的公共部分。
type subrunEntry struct {
	id, status, resultRef string
}

// checkSubrunEntries 检查 subruns[]：至多 4 项、ID 合法且不重复、status 在允许范围内；
// withRef 为真时 completed 必须有合法 sha256 的 result_ref，其他状态不得有 result_ref。
func checkSubrunEntries(field string, entries []subrunEntry, withRef bool, statuses ...string) error {
	if len(entries) > MaxSubrunsPerTask {
		return newError(CodeInvalidField, "%s 有 %d 项，上限 %d", field, len(entries), MaxSubrunsPerTask)
	}
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		if err := firstErr(checkSubrunID(field+"[].id", e.id), oneOf(field+"[].status", e.status, statuses...)); err != nil {
			return err
		}
		if seen[e.id] {
			return newError(CodeInvalidField, "%s 中 ID %q 重复", field, e.id)
		}
		seen[e.id] = true
		if !withRef {
			continue
		}
		if e.status == CheckpointSubrunCompleted {
			if !validSHA256(e.resultRef) {
				return newError(CodeInvalidField, "%s 中 completed 的 %q 必须带合法 sha256 的 result_ref", field, e.id)
			}
		} else if e.resultRef != "" {
			return newError(CodeInvalidField, "%s 中 %s 的 %q 不得带 result_ref", field, e.status, e.id)
		}
	}
	return nil
}

// checkCheckpointSubruns 检查 checkpoint.subruns[]；result_ref 与 refs 合计受每 checkpoint 引用上限约束。
func checkCheckpointSubruns(subruns []CheckpointSubrun, refs int) error {
	entries := make([]subrunEntry, len(subruns))
	for i, s := range subruns {
		entries[i] = subrunEntry{s.SubrunID, s.Status, s.ResultRef}
	}
	if err := checkSubrunEntries("subruns", entries, true,
		CheckpointSubrunStarted, CheckpointSubrunCompleted, CheckpointSubrunFailed, CheckpointSubrunCancelled); err != nil {
		return err
	}
	for _, s := range subruns {
		if s.ResultRef != "" {
			refs++
		}
	}
	if refs > MaxRefsPerCheckpoint {
		return newError(CodeTooManyRefs, "refs 与 subruns[].result_ref 合计 %d 个，上限 %d", refs, MaxRefsPerCheckpoint)
	}
	return nil
}

func checkResumeSubruns(subruns []ResumeSubrun) error {
	entries := make([]subrunEntry, len(subruns))
	for i, s := range subruns {
		entries[i] = subrunEntry{s.SubrunID, s.Status, s.ResultRef}
	}
	return checkSubrunEntries("resume.subruns", entries, true,
		CheckpointSubrunStarted, CheckpointSubrunCompleted, CheckpointSubrunCancelled, CheckpointSubrunFailed, SubrunTimedOut)
}

func checkResultSubruns(subruns []ResultSubrun) error {
	entries := make([]subrunEntry, len(subruns))
	for i, s := range subruns {
		entries[i] = subrunEntry{id: s.ID, status: s.Status}
	}
	if err := checkSubrunEntries("result.subruns", entries, false,
		CheckpointSubrunStarted, CheckpointSubrunCompleted, CheckpointSubrunFailed, CheckpointSubrunCancelled, SubrunTimedOut); err != nil {
		return err
	}
	for _, s := range subruns {
		if err := checkSummary("result.subruns[].summary", s.Summary); err != nil {
			return err
		}
	}
	return nil
}

// checkExtensions 检查 init.extensions：每项非空（未知扩展名忽略，便于前向兼容）。
func checkExtensions(exts []string) error {
	for _, e := range exts {
		if e == "" {
			return newError(CodeInvalidField, "extensions 中不能有空串")
		}
	}
	return nil
}

func checkReadySubruns(v int64) error {
	if v != 0 && v != SubrunsExtVersion {
		return newError(CodeInvalidField, "subruns=%d，只能是 0 或 %d", v, SubrunsExtVersion)
	}
	return nil
}

func checkProgressSubrun(id string) error {
	if id != "" && !validSubrunID(id) {
		return newError(CodeInvalidField, "subrun_id=%q 不合法", id)
	}
	return nil
}

// ---- 事件流 ----

// subrunTracker 是事件流中 sub-run 扩展的状态：是否已协商，以及本 attempt 中已 subrun_start 的 ID。
// 恢复后的 attempt 须先以同一定义重发 subrun_start，才能再引用该 sub-run（checkpoint 的 subruns[]
// 除外：它可以列出恢复前已结束的 sub-run）。
type subrunTracker struct {
	negotiated bool
	known      map[string]bool
}

// requestsSubruns 判断 init.extensions 是否请求了 sub-run 扩展。
func requestsSubruns(exts []string) bool {
	for _, e := range exts {
		if e == ExtensionSubruns {
			return true
		}
	}
	return false
}

// checkReady：请求了扩展而 ready.subruns ≠ 1，或未请求却回 1 → extension_mismatch。
func (t *subrunTracker) checkReady(r *Ready) error {
	if (r.Subruns == SubrunsExtVersion) != t.negotiated {
		return newError(CodeExtensionMismatch, "ready.subruns=%d，宿主请求 subruns 扩展：%t", r.Subruns, t.negotiated)
	}
	return nil
}

// observe 检查 ready 之后、终态提议之前的一条 Worker 事件。
func (t *subrunTracker) observe(m Message) error {
	var id string
	start := false
	switch m := m.(type) {
	case *SubrunStart:
		id, start = m.SubrunID, true
	case *SubrunEnd:
		id = m.SubrunID
	case *SubrunCancel:
		id = m.SubrunID
	case *Progress:
		id = m.SubrunID
	case *Checkpoint:
		return t.requireNegotiated(len(m.Subruns) > 0, "checkpoint.subruns")
	case *Result:
		return t.requireNegotiated(len(m.Subruns) > 0, "result.subruns")
	}
	if id == "" {
		return nil
	}
	if err := t.requireNegotiated(true, m.MessageType()); err != nil {
		return err
	}
	if start {
		if t.known == nil {
			t.known = map[string]bool{}
		}
		t.known[id] = true
		return nil
	}
	return t.requireKnown(id, m.MessageType())
}

func (t *subrunTracker) requireNegotiated(uses bool, what string) error {
	if uses && !t.negotiated {
		return newError(CodeExtensionNotNegotiated, "未协商 subruns 扩展却出现 %s", what)
	}
	return nil
}

func (t *subrunTracker) requireKnown(id, what string) error {
	if !t.known[id] {
		return newError(CodeSubrunUnknown, "%s 引用了尚未 subrun_start 的 sub-run %q", what, id)
	}
	return nil
}

// hostSent 检查宿主发出的 sub-run 消息：只能针对本 attempt 中已 subrun_start 的 sub-run。
func (t *subrunTracker) hostSent(m Message) error {
	var id string
	switch m := m.(type) {
	case *SubrunStarted:
		id = m.SubrunID
	case *SubrunCancelRequested:
		id = m.SubrunID
	default:
		return nil
	}
	if err := t.requireNegotiated(true, m.MessageType()); err != nil {
		return err
	}
	return t.requireKnown(id, m.MessageType())
}

// NegotiateExtensions 记录宿主在 init 中请求的扩展（task 模式；session 模式由 SessionStream.HostSent
// 从 init 读取）。须在 ready 之前调用；未调用视为未请求任何扩展。
func (s *WorkerStream) NegotiateExtensions(requested []string) {
	s.subruns.negotiated = requestsSubruns(requested)
}
