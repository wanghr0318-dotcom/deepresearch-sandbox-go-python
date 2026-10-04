package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
)

func (m *Init) validate() error {
	if m.Bootstrap != BootstrapVersion {
		return newError(CodeInvalidField, "bootstrap=%d，期望 %d", m.Bootstrap, BootstrapVersion)
	}
	if len(m.ProtocolVersions) == 0 {
		return newError(CodeMissingField, "protocol_versions 不能为空")
	}
	if err := oneOf("mode", m.Mode, ModeTask, ModeSession); err != nil {
		return err
	}
	if m.Mode != ModeTask {
		return nil // session 扩展的字段由后续里程碑校验
	}
	if err := firstErr(required("task_id", m.TaskID), required("attempt_id", m.AttemptID), required("out_dir", m.OutDir)); err != nil {
		return err
	}
	if m.AttemptNo < 1 {
		return newError(CodeInvalidField, "attempt_no=%d，必须 ≥ 1", m.AttemptNo)
	}
	if err := checkRefs("input_refs", m.InputRefs, -1); err != nil {
		return err
	}
	if m.Resume == nil {
		return nil
	}
	return m.Resume.validate()
}

func (r *Resume) validate() error {
	if err := firstErr(required("resume.checkpoint_id", r.CheckpointID), required("resume.step_id", r.StepID)); err != nil {
		return err
	}
	if err := checkState(r.State, r.StateRef); err != nil {
		return err
	}
	return checkRefs("resume.refs", r.Refs, MaxRefsPerCheckpoint)
}

func (m *CheckpointResult) validate() error {
	return firstErr(
		checkVersion(m.V),
		required("checkpoint_id", m.CheckpointID),
		oneOf("scope", m.Scope, ScopeTask, ScopeSession),
		oneOf("status", m.Status, CheckpointCommitted, CheckpointConflict, CheckpointRejected, CheckpointRetryableError, CheckpointNotFound),
	)
}

func (m *ArtifactResult) validate() error {
	if err := firstErr(checkVersion(m.V), required("artifact_id", m.ArtifactID), oneOf("status", m.Status, ArtifactSaved, ArtifactRejected)); err != nil {
		return err
	}
	if m.Status == ArtifactRejected {
		return required("code", m.Code)
	}
	if m.Version < 1 {
		return newError(CodeInvalidField, "saved 的 version=%d，必须 ≥ 1", m.Version)
	}
	if !validSHA256(m.SHA256) {
		return newError(CodeInvalidField, "saved 的 sha256 不合法")
	}
	return nil
}

func (m *Cancel) validate() error { return validateStop(m.V, m.AttemptID, m.GraceMS) }
func (m *Pause) validate() error  { return validateStop(m.V, m.AttemptID, m.GraceMS) }

func validateStop(v int, attemptID string, graceMS int64) error {
	if err := firstErr(checkVersion(v), required("attempt_id", attemptID)); err != nil {
		return err
	}
	if graceMS < 0 {
		return newError(CodeInvalidField, "grace_ms=%d，必须 ≥ 0", graceMS)
	}
	return nil
}

func (m *Ready) validate() error {
	if err := checkEvent(m.EventHeader); err != nil {
		return err
	}
	if m.ProtocolVersion != Version {
		return newError(CodeVersionMismatch, "protocol_version=%d，期望 %d", m.ProtocolVersion, Version)
	}
	return firstErr(oneOf("mode", m.Mode, ModeTask, ModeSession), required("worker.name", m.Worker.Name))
}

func (m *Progress) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("kind", m.Kind))
}

func (m *Artifact) validate() error {
	if err := firstErr(checkEvent(m.EventHeader), required("artifact_id", m.ArtifactID)); err != nil {
		return err
	}
	if !validArtifactPath(m.Path) {
		return newError(CodePathInvalid, "产物路径 %q 必须是相对路径，且不含空、. 或 .. 分量", m.Path)
	}
	if !validSHA256(m.DeclaredSHA256) {
		return newError(CodeInvalidField, "declared_sha256 不合法")
	}
	if m.DeclaredSize < 0 {
		return newError(CodeInvalidField, "declared_size=%d，必须 ≥ 0", m.DeclaredSize)
	}
	return firstErr(required("media_type", m.MediaType), oneOf("visibility", m.Visibility, VisibilityOutput, VisibilityInternal))
}

func (m *Checkpoint) validate() error {
	if err := firstErr(
		checkEvent(m.EventHeader),
		required("checkpoint_id", m.CheckpointID),
		required("step_id", m.StepID),
		oneOf("scope", m.Scope, ScopeTask), // session scope 只能经 result 提议写入（规格 §5.5 第 7 条）
	); err != nil {
		return err
	}
	if err := checkState(m.State, m.StateRef); err != nil {
		return err
	}
	return checkRefs("refs", m.Refs, MaxRefsPerCheckpoint)
}

func (m *CheckpointQuery) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("checkpoint_id", m.CheckpointID), oneOf("scope", m.Scope, ScopeTask, ScopeSession))
}

func (m *Paused) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("checkpoint_id", m.CheckpointID))
}

func (m *Result) validate() error {
	if err := checkEvent(m.EventHeader); err != nil {
		return err
	}
	for _, o := range m.Outputs {
		if o == "" {
			return newError(CodeInvalidField, "outputs 中不能有空的 artifact_id")
		}
	}
	return nil
}

func (m *ErrorEvent) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("code", m.Code))
}

func (m *HandshakeError) validate() error {
	if m.Bootstrap != BootstrapVersion {
		return newError(CodeInvalidField, "bootstrap=%d，期望 %d", m.Bootstrap, BootstrapVersion)
	}
	return required("code", m.Code)
}

func checkVersion(v int) error {
	if v != Version {
		return newError(CodeVersionMismatch, "v=%d，期望 %d", v, Version)
	}
	return nil
}

func checkEvent(h EventHeader) error {
	if err := checkVersion(h.V); err != nil {
		return err
	}
	if h.Seq < 1 {
		return newError(CodeInvalidField, "seq=%d，必须 ≥ 1", h.Seq)
	}
	return nil
}

// checkState 检查 state 与 state_ref 必须且只能提供一个；inline state 按紧凑 JSON 计大小。
func checkState(state json.RawMessage, stateRef string) error {
	hasState, hasRef := hasJSONValue(state), stateRef != ""
	if hasState == hasRef {
		return newError(CodeInvalidField, "state 与 state_ref 必须且只能提供一个")
	}
	if hasRef {
		if !validSHA256(stateRef) {
			return newError(CodeInvalidField, "state_ref 不是合法的 sha256")
		}
		return nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, state); err != nil {
		return newError(CodeMalformedJSON, "state: %v", err)
	}
	if compact.Len() > MaxInlineStateBytes {
		return newError(CodeStateTooLarge, "inline state %d 字节，上限 %d", compact.Len(), MaxInlineStateBytes)
	}
	return nil
}

// checkRefs 检查 refs 中每个元素都是 sha256；limit < 0 表示不限个数。
func checkRefs(field string, refs []string, limit int) error {
	if limit >= 0 && len(refs) > limit {
		return newError(CodeTooManyRefs, "%s 有 %d 个，上限 %d", field, len(refs), limit)
	}
	for _, r := range refs {
		if !validSHA256(r) {
			return newError(CodeInvalidField, "%s 中有不合法的 sha256", field)
		}
	}
	return nil
}

func hasJSONValue(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) > 0 && !bytes.Equal(t, []byte("null"))
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// validArtifactPath 检查产物路径：相对、非空、无 NUL、不超过上限、不含空、. 或 .. 分量（规格 §5.6）。
func validArtifactPath(p string) bool {
	if p == "" || len(p) > MaxArtifactPathBytes || strings.ContainsRune(p, 0) || strings.HasPrefix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func required(field, value string) error {
	if value == "" {
		return newError(CodeMissingField, "%s 不能为空", field)
	}
	return nil
}

// oneOf 检查枚举字段：空值是 missing_field，不在允许范围内是 invalid_field。
func oneOf(field, value string, allowed ...string) error {
	if value == "" {
		return newError(CodeMissingField, "%s 不能为空", field)
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return newError(CodeInvalidField, "%s=%q 不在允许范围 %v 内", field, value, allowed)
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
