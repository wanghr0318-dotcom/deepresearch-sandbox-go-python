package api

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
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// 会话与 turn 端点（M4 Plan 12 Task 8；契约见 api/openapi.yaml 的 /sessions 与 /turns）。
//
// 授权：读操作对用户与运维开放，写操作只对用户开放（audienceUser：运维 403，运维不以用户身份对话）。用户只能访问
// 自己的会话：他人的、无主的、不存在的会话，以及（对用户）已 closed 的会话一律回答相同的 404 session_not_found；
// turn 同样回答 404 turn_not_found，⟨/⟩ 原文回答 404 not_found。归属在校验请求体之前判定，使 404 不随请求体变化。
// 日志不记录消息正文与 answers（访问日志只有方法、路径与状态）。

const (
	maxTextRunes    = 4000
	maxTitleRunes   = 80
	maxAnswers      = 3
	rawBlobMaxBytes = 32 << 20 // ⟨/⟩ 原文整体读入内存以脱敏；超过上限视为不可读
)

var (
	sessionWriteStatuses = []int{202, 400, 401, 403, 404, 409, 500, 503}
	sessionReadStatuses  = []int{200, 400, 401, 403, 404, 500, 503}
)

// sessionRoutes 只在启用账号时注册；Config.Sessions 为 nil 时一律 503 sessions_unavailable（sessionOp）。
var sessionRoutes = []route{
	{"POST", "/sessions", accessWrite, audienceUser, []int{201, 400, 401, 403, 409, 500, 503}, sessionOp((*Handler).createSession)},
	{"GET", "/sessions", accessRead, audienceSession, []int{200, 400, 401, 403, 500, 503}, sessionOp((*Handler).listSessions)},
	{"GET", "/sessions/{id}", accessRead, audienceSession, []int{200, 401, 403, 404, 500, 503}, sessionOp((*Handler).getSession)},
	{"PATCH", "/sessions/{id}", accessWrite, audienceUser, []int{200, 400, 401, 403, 404, 409, 500, 503}, sessionOp((*Handler).renameSession)},
	{"DELETE", "/sessions/{id}", accessWrite, audienceUser, []int{202, 401, 403, 404, 500, 503}, sessionOp((*Handler).deleteSession)},
	{"POST", "/sessions/{id}/wake", accessWrite, audienceUser, sessionWriteStatuses, sessionOp((*Handler).wakeSession)},
	{"POST", "/sessions/{id}/messages", accessWrite, audienceUser, sessionWriteStatuses, sessionOp((*Handler).sendMessage)},
	{"GET", "/sessions/{id}/turns", accessRead, audienceSession, sessionReadStatuses, sessionOp((*Handler).listTurns)},
	{"GET", "/sessions/{id}/events", accessRead, audienceSession, sessionReadStatuses, sessionOp((*Handler).streamSessionEvents)},
	{"POST", "/turns/{id}/stop", accessWrite, audienceUser, sessionWriteStatuses, sessionOp(turnControl("stop"))},
	{"POST", "/turns/{id}/continue", accessWrite, audienceUser, sessionWriteStatuses, sessionOp(turnControl("continue"))},
	{"POST", "/turns/{id}/finish", accessWrite, audienceUser, sessionWriteStatuses, sessionOp(turnControl("finish"))},
	{"POST", "/turns/{id}/answer", accessWrite, audienceUser, sessionWriteStatuses, sessionOp(turnControl("answer"))},
	{"POST", "/turns/{id}/restore", accessWrite, audienceUser, sessionWriteStatuses, sessionOp((*Handler).restoreTurn)},
	{"GET", "/turns/{id}/raw/{sha256}", accessRead, audienceSession, []int{200, 401, 403, 404, 500, 503}, sessionOp((*Handler).getTurnRaw)},
}

// sessionOp 在未配置会话（Config.Sessions 为 nil，即 server 没有会话 Worker）时回答 503 sessions_unavailable。
func sessionOp(fn func(h *Handler, w http.ResponseWriter, r *http.Request)) func(h *Handler, w http.ResponseWriter, r *http.Request) {
	return func(h *Handler, w http.ResponseWriter, r *http.Request) {
		if h.cfg.Sessions == nil {
			writeError(w, http.StatusServiceUnavailable, "sessions_unavailable", "服务端未启用会话")
			return
		}
		fn(h, w, r)
	}
}

func randomSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand 失败时无法安全生成 ID
	}
	return hex.EncodeToString(b[:])
}

// ---- 归属与错误 ----

const (
	msgSessionNotFound = "会话不存在"
	msgTurnNotFound    = "turn 不存在"
	msgRawNotFound     = "原文不存在"
)

// caller 返回调用者（会话端点总在启用账号时注册，authorize 已确定调用者）。
func caller(r *http.Request) principal {
	p, _ := principalFrom(r.Context())
	return p
}

// sessionFor 读取路径中的会话并对用户判定归属；不可见时写出 404 session_not_found 并返回 false。
func (h *Handler) sessionFor(w http.ResponseWriter, r *http.Request) (SessionView, bool) {
	v, err := h.cfg.Sessions.GetSession(r.Context(), r.PathValue("id"))
	if err != nil {
		writeSessionError(w, err, "session_not_found", msgSessionNotFound)
		return v, false
	}
	if p := caller(r); !p.admin && !userOwns(p, v) {
		writeError(w, http.StatusNotFound, "session_not_found", msgSessionNotFound)
		return v, false
	}
	return v, true
}

func userOwns(p principal, v SessionView) bool {
	return v.OwnerUserID != 0 && v.OwnerUserID == p.user.ID && v.State != "closed"
}

// turnFor 读取路径中 turn 所属的会话并对用户判定归属；不可见（含不是会话 turn 的任务）时写出 404 code 并返回 false。
func (h *Handler) turnFor(w http.ResponseWriter, r *http.Request, code, msg string) (sessionID string, ok bool) {
	ctx := r.Context()
	sessionID, owner, err := h.cfg.Sessions.TurnSession(ctx, r.PathValue("id"))
	if err != nil {
		writeSessionError(w, err, code, msg)
		return "", false
	}
	p := caller(r)
	if p.admin {
		return sessionID, true
	}
	if owner == 0 || owner != p.user.ID {
		writeError(w, http.StatusNotFound, code, msg)
		return "", false
	}
	v, err := h.cfg.Sessions.GetSession(ctx, sessionID)
	if err != nil {
		writeSessionError(w, err, code, msg)
		return "", false
	}
	if !userOwns(p, v) {
		writeError(w, http.StatusNotFound, code, msg)
		return "", false
	}
	return sessionID, true
}

// writeSessionError 把会话用例的错误映射为状态码与错误码；不存在回答 notFoundCode。
func writeSessionError(w http.ResponseWriter, err error, notFoundCode, notFoundMsg string) {
	var rej *persistence.RejectedError
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		writeError(w, http.StatusNotFound, notFoundCode, notFoundMsg)
	case errors.Is(err, ErrSessionClosed):
		writeError(w, http.StatusConflict, "session_closed", "会话正在删除")
	case errors.Is(err, ErrTurnInProgress):
		writeError(w, http.StatusConflict, "turn_in_progress", "当前研究仍在进行，请先停止")
	case errors.Is(err, ErrInvalidTurnState), errors.As(err, &rej):
		writeError(w, http.StatusConflict, "invalid_turn_state", "turn 的当前状态不允许该操作")
	case errors.Is(err, ErrNotRestorable):
		writeError(w, http.StatusConflict, "not_restorable", "该 turn 不可恢复（未取消或没有 checkpoint）")
	case errors.Is(err, ErrUserTaskRunning):
		writeError(w, http.StatusConflict, "user_task_running", "已有研究在进行中，请等待其结束")
	default:
		writeStoreError(w, err)
	}
}

// parseLimit 解析 limit 查询参数（默认 50，1–200）。
func parseLimit(w http.ResponseWriter, s string) (int, bool) {
	if s == "" {
		return defaultListLimit, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > maxListLimit {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit 须为 1 到 %d 的整数", maxListLimit))
		return 0, false
	}
	return n, true
}

// writeAccepted 写出无正文的 202。
func writeAccepted(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusAccepted)
}

// sessionJSON 是对外的会话视图：用户视图不含运维字段。
func sessionJSON(p principal, v SessionView) SessionView {
	if !p.admin {
		v.Owner, v.InternalState = "", ""
	}
	return v
}

// ---- /sessions ----

type createSessionBody struct {
	RequestID string `json:"request_id"`
	Title     string `json:"title,omitempty"`
}

// sessionHashInput 是会话写请求 body_hash 的输入：请求体加上用户与路径中的资源，使不同用户、不同会话或不同操作
// 的相同请求体得到不同的 body_hash（提交结果未知时按 request_id 核对，不会取到他人的结果）。
type sessionHashInput struct {
	Body      any    `json:"body"`
	UserID    int64  `json:"user_id"`
	Op        string `json:"op"`
	SessionID string `json:"session_id,omitempty"`
	TurnID    string `json:"turn_id,omitempty"`
}

func hashSessionRequest(w http.ResponseWriter, in sessionHashInput) ([]byte, bool) {
	canon, err := jcs.Canonical(in)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "请求体无法规范化")
		return nil, false
	}
	return bodyHash(canon), true
}

func (h *Handler) createSession(w http.ResponseWriter, r *http.Request) {
	p := caller(r)
	var body createSessionBody
	if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
		return
	}
	title := strings.TrimSpace(body.Title)
	if utf8.RuneCountInString(title) > maxTitleRunes {
		writeError(w, http.StatusBadRequest, "invalid_title", "标题不能超过 80 个字符")
		return
	}
	hash, ok := hashSessionRequest(w, sessionHashInput{Body: body, UserID: p.user.ID, Op: "create_session"})
	if !ok {
		return
	}
	ctx := r.Context()
	v, _, err := h.cfg.Sessions.CreateSession(ctx, CreateSessionRequest{RequestID: body.RequestID, BodyHash: hash,
		SessionID: h.cfg.NewSessionID(), OwnerUserID: p.user.ID, Title: title})
	if errors.Is(err, persistence.ErrCommitUnknown) {
		if id, ok := resolveUnknown(ctx, h.cfg.Store, body.RequestID, hash, decodeSessionID); ok {
			v, err = h.cfg.Sessions.GetSession(ctx, id)
		}
	}
	if err != nil {
		writeSessionError(w, err, "session_not_found", msgSessionNotFound)
		return
	}
	writeJSON(w, http.StatusCreated, sessionJSON(p, v)) // 重放时返回会话的当前视图，状态码相同
}

func decodeSessionID(b json.RawMessage) (string, bool) {
	var res struct {
		SessionID string `json:"session_id"`
	}
	return res.SessionID, json.Unmarshal(b, &res) == nil && res.SessionID != ""
}

type sessionListJSON struct {
	Sessions []SessionView `json:"sessions"`
	Next     string        `json:"next,omitempty"`
}

// validSessionCursor：after 是上一页最后一个 session_id。
var validSessionCursor = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func (h *Handler) listSessions(w http.ResponseWriter, r *http.Request) {
	p := caller(r)
	q := r.URL.Query()
	limit, ok := parseLimit(w, q.Get("limit"))
	if !ok {
		return
	}
	after := q.Get("after")
	if after != "" && !validSessionCursor.MatchString(after) {
		writeError(w, http.StatusBadRequest, "invalid_cursor", "after 不是合法的游标")
		return
	}
	owner := p.user.ID
	if p.admin {
		owner = 0
	}
	views, next, err := h.cfg.Sessions.ListSessions(r.Context(), owner, after, limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := sessionListJSON{Sessions: make([]SessionView, 0, len(views)), Next: next}
	for _, v := range views {
		out.Sessions = append(out.Sessions, sessionJSON(p, v))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) getSession(w http.ResponseWriter, r *http.Request) {
	v, ok := h.sessionFor(w, r)
	if !ok {
		return
	}
	p := caller(r)
	if p.admin {
		// GetSession 不含运维字段（owner、internal_state）；运维视图取自全部会话的列表。
		av, found, err := h.adminSessionView(r.Context(), v.SessionID)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		if found {
			v = av
		}
	}
	writeJSON(w, http.StatusOK, sessionJSON(p, v))
}

// adminSessionView 在运维视图的会话列表中查找一个会话（Sessions 没有按 ID 读取运维视图的用例）。
func (h *Handler) adminSessionView(ctx context.Context, sessionID string) (SessionView, bool, error) {
	after := ""
	for {
		views, next, err := h.cfg.Sessions.ListSessions(ctx, 0, after, maxListLimit)
		if err != nil {
			return SessionView{}, false, err
		}
		for _, v := range views {
			if v.SessionID == sessionID {
				return v, true, nil
			}
		}
		if next == "" || next == after {
			return SessionView{}, false, nil
		}
		after = next
	}
}

type renameBody struct {
	Title string `json:"title"`
}

func (h *Handler) renameSession(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.sessionFor(w, r); !ok {
		return
	}
	var body renameBody
	if !decodeBody(w, r, &body) {
		return
	}
	title := strings.TrimSpace(body.Title)
	if n := utf8.RuneCountInString(title); n < 1 || n > maxTitleRunes {
		writeError(w, http.StatusBadRequest, "invalid_title", "标题须为 1–80 个字符")
		return
	}
	v, err := h.cfg.Sessions.RenameSession(r.Context(), r.PathValue("id"), title)
	if err != nil {
		writeSessionError(w, err, "session_not_found", msgSessionNotFound)
		return
	}
	writeJSON(w, http.StatusOK, sessionJSON(caller(r), v))
}

func (h *Handler) deleteSession(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.sessionFor(w, r); !ok {
		return
	}
	v, err := h.cfg.Sessions.CloseSession(r.Context(), r.PathValue("id"))
	if err != nil {
		writeSessionError(w, err, "session_not_found", msgSessionNotFound)
		return
	}
	writeJSON(w, http.StatusAccepted, sessionJSON(caller(r), v))
}

type sessionWriteBody struct {
	RequestID string `json:"request_id"`
}

func (h *Handler) wakeSession(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.sessionFor(w, r); !ok {
		return
	}
	var body sessionWriteBody
	if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
		return
	}
	sessionID := r.PathValue("id")
	hash, ok := hashSessionRequest(w, sessionHashInput{Body: body, UserID: caller(r).user.ID, Op: "wake", SessionID: sessionID})
	if !ok {
		return
	}
	ctx := r.Context()
	err := h.cfg.Sessions.WakeSession(ctx, body.RequestID, hash, sessionID)
	if errors.Is(err, persistence.ErrCommitUnknown) {
		if id, ok := resolveUnknown(ctx, h.cfg.Store, body.RequestID, hash, decodeSessionID); ok && id == sessionID {
			err = nil
		}
	}
	if err != nil {
		writeSessionError(w, err, "session_not_found", msgSessionNotFound)
		return
	}
	writeAccepted(w)
}

type messageBody struct {
	RequestID    string `json:"request_id"`
	Text         string `json:"text"`
	DeepResearch *bool  `json:"deep_research"`
}

func (h *Handler) sendMessage(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.sessionFor(w, r); !ok {
		return
	}
	var body messageBody
	if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
		return
	}
	if body.DeepResearch == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "deep_research 必填")
		return
	}
	text := strings.TrimSpace(body.Text)
	if n := utf8.RuneCountInString(text); n < 1 || n > maxTextRunes {
		writeError(w, http.StatusBadRequest, "invalid_text", "消息须为 1–4000 个字符")
		return
	}
	sessionID := r.PathValue("id")
	hash, ok := hashSessionRequest(w, sessionHashInput{Body: body, UserID: caller(r).user.ID, Op: "message", SessionID: sessionID})
	if !ok {
		return
	}
	h.createTurn(w, r, CreateTurnRequest{RequestID: body.RequestID, SessionID: sessionID, BodyHash: hash,
		Text: text, DeepResearch: *body.DeepResearch}, "session_not_found", msgSessionNotFound)
}

// operatorTurnSpec 是运维在会话中创建 turn 时 spec 允许的字段（POST /tasks 带 session_id）。模型、额度与其余
// 配置仍由服务端生成（与用户消息相同）；research 的键覆盖服务端生成的 config.research（串并行对比用其设置
// scheduling 与 fixed_plan，由 Worker 校验）。
type operatorTurnSpec struct {
	Text         string          `json:"text"`
	DeepResearch *bool           `json:"deep_research"`
	Research     json.RawMessage `json:"research,omitempty"`
}

// createOperatorTurn 是 POST /tasks 带 session_id（规格 §15.1；Plan 14 的运维路径 C12-7）：在指定会话中创建
// 一个 turn，与用户发消息走同一事务（会话所有者为 turn 的所有者，不检查调用者是否为所有者）；成功回答 201
// CreateTaskResult（task_id 即 turn_id）。limits 由服务端决定，不接受请求中的 limits。
func (h *Handler) createOperatorTurn(w http.ResponseWriter, r *http.Request, body createTaskBody) {
	if h.cfg.Sessions == nil {
		writeError(w, http.StatusServiceUnavailable, "sessions_unavailable", "服务端未启用会话")
		return
	}
	if len(body.Limits) > 0 && string(bytes.TrimSpace(body.Limits)) != "null" {
		writeError(w, http.StatusBadRequest, "invalid_request", "带 session_id 时 limits 由服务端决定，不能指定")
		return
	}
	body.Limits = nil
	var in operatorTurnSpec
	dec := json.NewDecoder(bytes.NewReader(body.Spec))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "带 session_id 时 spec 只能含 text、deep_research、research")
		return
	}
	if in.DeepResearch == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "spec.deep_research 必填")
		return
	}
	text := strings.TrimSpace(in.Text)
	if n := utf8.RuneCountInString(text); n < 1 || n > maxTextRunes {
		writeError(w, http.StatusBadRequest, "invalid_text", "spec.text 须为 1–4000 个字符")
		return
	}
	if len(in.Research) > 0 && string(bytes.TrimSpace(in.Research)) != "null" && !isJSONObject(in.Research) {
		writeError(w, http.StatusBadRequest, "invalid_request", "spec.research 须为 JSON 对象")
		return
	}
	hash, ok := hashSessionRequest(w, sessionHashInput{Body: body, Op: "operator_turn", SessionID: body.SessionID})
	if !ok {
		return
	}
	h.createTurn(w, r, CreateTurnRequest{RequestID: body.RequestID, SessionID: body.SessionID, BodyHash: hash,
		Text: text, DeepResearch: *in.DeepResearch, Operator: true, Research: in.Research},
		"session_not_found", msgSessionNotFound)
}

// overlayResearch 把运维给出的 research 键覆盖到服务端生成的 spec.research 上（其余字段不变）。
func overlayResearch(spec, research json.RawMessage) (json.RawMessage, error) {
	if len(research) == 0 || string(bytes.TrimSpace(research)) == "null" {
		return spec, nil
	}
	var s map[string]json.RawMessage
	if err := json.Unmarshal(spec, &s); err != nil {
		return nil, err
	}
	base := map[string]json.RawMessage{}
	if raw, ok := s["research"]; ok && string(bytes.TrimSpace(raw)) != "null" {
		if err := json.Unmarshal(raw, &base); err != nil {
			return nil, err
		}
	}
	var over map[string]json.RawMessage
	if err := json.Unmarshal(research, &over); err != nil {
		return nil, err
	}
	for k, v := range over {
		base[k] = v
	}
	merged, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	s["research"] = merged
	return json.Marshal(s)
}

// createTurn 生成 spec 与 limits 并创建 turn（消息与恢复共用）；成功回答 202 MessageResult（运维路径 201
// CreateTaskResult）。
func (h *Handler) createTurn(w http.ResponseWriter, r *http.Request, req CreateTurnRequest, notFoundCode, notFoundMsg string) {
	spec, limits, err := h.cfg.TurnSpec(req.Text, req.DeepResearch)
	if err == nil && (!isJSONObject(spec) || (len(limits) > 0 && !isJSONObject(limits))) {
		err = errors.New("TurnSpec 返回的 spec 或 limits 不是 JSON 对象")
	}
	if err == nil && req.Research != nil {
		spec, err = overlayResearch(spec, req.Research)
	}
	if err != nil {
		h.logError("生成 turn spec", err)
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
		return
	}
	req.TaskID, req.OwnerUserID = h.cfg.NewTaskID(), caller(r).user.ID
	req.Spec, req.ConfigVersion, req.MaxFaultRetries = mustCanonical(spec), h.cfg.ConfigVersion, h.cfg.MaxFaultRetries
	if len(limits) > 0 {
		req.Limits = mustCanonical(limits)
	}
	ctx := r.Context()
	res, err := h.cfg.Sessions.CreateTurn(ctx, req)
	if errors.Is(err, persistence.ErrCommitUnknown) {
		if got, ok := resolveUnknown(ctx, h.cfg.Store, req.RequestID, req.BodyHash, func(b json.RawMessage) (CreateTurnResult, bool) {
			var c CreateTurnResult
			return c, json.Unmarshal(b, &c) == nil && c.TurnID != ""
		}); ok {
			res, err = got, nil
		}
	}
	if err != nil {
		writeSessionError(w, err, notFoundCode, notFoundMsg)
		return
	}
	if req.Operator {
		writeJSON(w, http.StatusCreated, CreateTaskResult{TaskID: res.TurnID})
		return
	}
	writeJSON(w, http.StatusAccepted, res) // 重放时返回首次结果，状态码相同
}

type turnListJSON struct {
	Turns []TurnView `json:"turns"`
}

func (h *Handler) listTurns(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.sessionFor(w, r); !ok {
		return
	}
	q := r.URL.Query()
	limit, ok := parseLimit(w, q.Get("limit"))
	if !ok {
		return
	}
	after := int64(-1) // turn_index 从 0 开始；省略即从头
	if s := q.Get("after_index"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid_request", "after_index 须为非负整数")
			return
		}
		after = n
	}
	turns, err := h.cfg.Sessions.ListTurns(r.Context(), r.PathValue("id"), after, limit)
	if err != nil {
		writeSessionError(w, err, "session_not_found", msgSessionNotFound)
		return
	}
	if turns == nil {
		turns = []TurnView{}
	}
	writeJSON(w, http.StatusOK, turnListJSON{Turns: turns})
}

// streamSessionEvents 以 SSE 发送整会话的事件：id = session_seq，Last-Event-ID 续传（任何已存在的 seq 都合法，
// 含用户不可见的记录），注释行心跳；用户只看到 ToSessionEvent 映射出的对外事件，不可见记录游标照常前进；
// session_state{closed} 之后关闭。turn 的 task_terminal 不关闭流（会话跨 turn 持续）。
func (h *Handler) streamSessionEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cursor, ok := parseCursor(r.Header.Get("Last-Event-ID"))
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_cursor", "Last-Event-ID 须为非负整数")
		return
	}
	if _, ok := h.sessionFor(w, r); !ok {
		return
	}
	sessionID := r.PathValue("id")
	if cursor > 0 { // session_seq 在存储中连续：seq > cursor-1 的记录存在即游标不超出最新
		recs, err := h.cfg.Sessions.ListSessionEvents(ctx, sessionID, cursor-1, 1)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		if len(recs) == 0 {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "Last-Event-ID 超出最新事件")
			return
		}
	}
	admin := caller(r).admin
	h.runSSE(w, r, cursor, func(ctx context.Context, cursor int64) ([]sseOut, int64, bool, bool, error) {
		recs, err := h.cfg.Sessions.ListSessionEvents(ctx, sessionID, cursor, eventPageSize)
		if err != nil {
			return nil, cursor, false, false, err
		}
		var out []sseOut
		for _, rec := range recs {
			if ev, keep := ToSessionEvent(rec, admin); keep {
				data, err := encodeJSON(ev)
				if err != nil {
					return out, cursor, false, false, err
				}
				out = append(out, sseOut{id: ev.Seq, event: ev.Type, data: data})
			}
			cursor = rec.SessionSeq
			if isSessionClosedRecord(rec) {
				return out, cursor, true, false, nil
			}
		}
		return out, cursor, false, len(recs) == eventPageSize, nil
	})
}

func isSessionClosedRecord(rec SessionEventRecord) bool {
	if rec.Source != "session" || rec.Type != SEvSessionState {
		return false
	}
	var p struct {
		State string `json:"state"`
	}
	return json.Unmarshal(rec.Payload, &p) == nil && p.State == "closed"
}

// ---- /turns ----

type answerJSON struct {
	QuestionID string  `json:"question_id"`
	Choice     *string `json:"choice,omitempty"`
	Other      *string `json:"other,omitempty"`
}

type answerBody struct {
	RequestID string       `json:"request_id"`
	Answers   []answerJSON `json:"answers"`
}

// validAnswers：1–3 项，question_id 非空，choice 与 other 恰有一个且非空。
func validAnswers(as []answerJSON) bool {
	if len(as) < 1 || len(as) > maxAnswers {
		return false
	}
	for _, a := range as {
		if strings.TrimSpace(a.QuestionID) == "" || (a.Choice == nil) == (a.Other == nil) {
			return false
		}
		v := a.Choice
		if v == nil {
			v = a.Other
		}
		if strings.TrimSpace(*v) == "" {
			return false
		}
	}
	return true
}

type turnControlJSON struct {
	TurnID         string `json:"turn_id"`
	ControlVersion int64  `json:"control_version"`
}

// turnControl 是 stop、continue、finish、answer：写入 turn 的控制意图，成功回答 202 TurnControlResult。
func turnControl(action string) func(h *Handler, w http.ResponseWriter, r *http.Request) {
	return func(h *Handler, w http.ResponseWriter, r *http.Request) {
		if _, ok := h.turnFor(w, r, "turn_not_found", msgTurnNotFound); !ok {
			return
		}
		turnID := r.PathValue("id")
		var reqBody any
		var requestID string
		var answers json.RawMessage
		if action == "answer" {
			var body answerBody
			if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
				return
			}
			if !validAnswers(body.Answers) {
				writeError(w, http.StatusBadRequest, "invalid_answers", "answers 须为 1–3 项，每项有 question_id 且 choice 与 other 恰有一个")
				return
			}
			b, err := jcs.Canonical(body.Answers)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid_request", "请求体无法规范化")
				return
			}
			reqBody, requestID, answers = body, body.RequestID, b
		} else {
			var body sessionWriteBody
			if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
				return
			}
			reqBody, requestID = body, body.RequestID
		}
		hash, ok := hashSessionRequest(w, sessionHashInput{Body: reqBody, UserID: caller(r).user.ID, Op: action, TurnID: turnID})
		if !ok {
			return
		}
		ctx := r.Context()
		req := TurnControlRequest{RequestID: requestID, BodyHash: hash, TaskID: turnID, Action: action, Answers: answers}
		res, err := h.cfg.Sessions.TurnControl(ctx, req)
		if errors.Is(err, persistence.ErrCommitUnknown) {
			if got, ok := resolveUnknown(ctx, h.cfg.Store, requestID, hash, func(b json.RawMessage) (ControlResult, bool) {
				var c ControlResult
				return c, json.Unmarshal(b, &c) == nil && c.TaskID == turnID
			}); ok {
				res, err = got, nil
			}
		}
		if err != nil {
			writeSessionError(w, err, "turn_not_found", msgTurnNotFound)
			return
		}
		writeJSON(w, http.StatusAccepted, turnControlJSON{TurnID: res.TaskID, ControlVersion: res.ControlVersion})
	}
}

// restoreTurn 在同一会话中开新 turn，从被取消 turn 的最后 checkpoint 继续（文本取源 turn，deep_research = true）。
func (h *Handler) restoreTurn(w http.ResponseWriter, r *http.Request) {
	sessionID, ok := h.turnFor(w, r, "turn_not_found", msgTurnNotFound)
	if !ok {
		return
	}
	var body sessionWriteBody
	if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
		return
	}
	turnID := r.PathValue("id")
	src, found, err := h.findTurn(r.Context(), sessionID, turnID)
	if err != nil {
		writeSessionError(w, err, "turn_not_found", msgTurnNotFound)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "turn_not_found", msgTurnNotFound)
		return
	}
	hash, ok := hashSessionRequest(w, sessionHashInput{Body: body, UserID: caller(r).user.ID, Op: "restore", TurnID: turnID})
	if !ok {
		return
	}
	h.createTurn(w, r, CreateTurnRequest{RequestID: body.RequestID, SessionID: sessionID, BodyHash: hash,
		Text: src.Text, DeepResearch: true, RestoredFromTaskID: turnID}, "turn_not_found", msgTurnNotFound)
}

// findTurn 在会话的 turn 列表中查找一个 turn（读取其文本）。
func (h *Handler) findTurn(ctx context.Context, sessionID, turnID string) (TurnView, bool, error) {
	after := int64(-1)
	for {
		turns, err := h.cfg.Sessions.ListTurns(ctx, sessionID, after, maxListLimit)
		if err != nil {
			return TurnView{}, false, err
		}
		for _, t := range turns {
			if t.TurnID == turnID {
				return t, true, nil
			}
		}
		if len(turns) < maxListLimit {
			return TurnView{}, false, nil
		}
		after = turns[len(turns)-1].TurnIndex
	}
}

// validSHA256 是 ⟨/⟩ 原文的 blob 引用：64 位小写十六进制。
var validSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// getTurnRaw 下载 turn 的原始模型或工具响应（⟨/⟩）：只提供授权到该 turn（task scope 或其会话 scope）的 blob；
// 用户得到 RedactRaw 脱敏后的内容，运维原样。非法 sha、未授权、他人的与不存在的 turn 一律 404 not_found。
// 内容整体读入并核对 sha256（脱敏需要完整内容），不符时 500 blob_unavailable，不发送任何内容。
func (h *Handler) getTurnRaw(w http.ResponseWriter, r *http.Request) {
	sha := r.PathValue("sha256")
	if !validSHA256.MatchString(sha) {
		writeError(w, http.StatusNotFound, "not_found", msgRawNotFound)
		return
	}
	if _, ok := h.turnFor(w, r, "not_found", msgRawNotFound); !ok {
		return
	}
	ctx := r.Context()
	authorized, err := h.cfg.Sessions.TurnBlobAuthorized(ctx, r.PathValue("id"), sha)
	if err != nil {
		writeSessionError(w, err, "not_found", msgRawNotFound)
		return
	}
	if !authorized {
		writeError(w, http.StatusNotFound, "not_found", msgRawNotFound)
		return
	}
	content, err := h.readBlob(sha)
	if err != nil {
		h.logError("读取原文 blob", err, "sha256", sha)
		writeError(w, http.StatusInternalServerError, "blob_unavailable", "原文内容不可读")
		return
	}
	if !caller(r).admin {
		content = RedactRaw(content)
	}
	hdr := w.Header()
	mediaType, name := "application/octet-stream", sha
	if json.Valid(content) {
		mediaType, name = "application/json", sha+".json"
	}
	hdr.Set("Content-Type", mediaType)
	hdr.Set("Content-Disposition", `attachment; filename="`+name+`"`)
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("Content-Length", strconv.Itoa(len(content)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(content); err != nil {
		h.logError("发送原文", err, "sha256", sha)
	}
}

// readBlob 读取整个 blob 并核对 sha256 与大小上限。
func (h *Handler) readBlob(sha string) ([]byte, error) {
	rc, err := h.cfg.Blobs.Open(sha)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }() // 只读，关闭错误不影响已读到的内容
	b, err := io.ReadAll(io.LimitReader(rc, rawBlobMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > rawBlobMaxBytes {
		return nil, fmt.Errorf("原文超过 %d 字节", rawBlobMaxBytes)
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != sha {
		return nil, errors.New("内容与 sha256 不符")
	}
	return b, nil
}
