package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/account"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/jcs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// SessionCookie 是用户会话 cookie 的名称（值为会话 ID 明文；库中只存其 sha256）。
const SessionCookie = "agentbox_session"

const (
	sessionTTL        = 7 * 24 * time.Hour // 不滑动续期
	registerPerMinute = 5                  // 每 IP
	loginPerMinute    = 10                 // 每 IP
	maxTopicRunes     = 500
)

// principal 是请求的调用者：运维（Bearer token）或用户（会话 cookie）。
type principal struct {
	admin bool
	user  User
}

type principalKey struct{}

func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// principalFrom 返回已认证的调用者；未启用账号或尚未认证时 ok 为 false。
func principalFrom(ctx context.Context) (principal, bool) {
	p, ok := ctx.Value(principalKey{}).(principal)
	return p, ok
}

// audience 表示启用账号（Config.Accounts 非 nil）时谁可以调用操作；未启用账号时不起作用。
type audience int

const (
	audienceAdmin  audience = iota // 只限运维；用户 403 forbidden
	audienceAnyone                 // 匿名可访问（不读会话 cookie）
	audienceTask                   // 运维，或任务的 owner；他人与无主任务 404 task_not_found
	audienceList                   // 运维看全部；用户只看自己的
	audienceUser                   // 只限用户会话；运维 403 forbidden
)

// accountRoutes 只在 Config.Accounts 非 nil 时注册。
var accountRoutes = []route{
	{"POST", "/auth/register", accessWrite, audienceAnyone, []int{201, 400, 401, 403, 409, 429, 500, 503}, (*Handler).register},
	{"POST", "/auth/login", accessWrite, audienceAnyone, []int{200, 400, 401, 403, 429, 500, 503}, (*Handler).login},
	{"POST", "/auth/logout", accessWrite, audienceUser, []int{204, 401, 403, 500, 503}, (*Handler).logout},
	{"GET", "/auth/me", accessRead, audienceUser, []int{200, 401, 403, 500, 503}, (*Handler).me},
	{"POST", "/research", accessWrite, audienceUser, []int{202, 400, 401, 403, 409, 500, 503}, (*Handler).createResearch},
}

// allRoutes 是全部可能注册的操作（openapi.yaml 描述的全集）。
func allRoutes() []route {
	return append(append([]route{}, routes...), accountRoutes...)
}

func (h *Handler) now() time.Time {
	if h.cfg.Now != nil {
		return h.cfg.Now()
	}
	return time.Now()
}

// clientIP 取 RemoteAddr 的主机部分（不信任 X-Forwarded-For 等请求头）。
func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// checkBearer 校验 Bearer token。open 为真时未配置 token 即通过（未启用账号时的 loopback 行为）；
// 启用账号时 open 为假：未配置 token 就没有任何 Bearer 能成为运维。
func (h *Handler) checkBearer(w http.ResponseWriter, r *http.Request, open bool) bool {
	if h.token == nil && open {
		return true
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if ok && h.token != nil && subtle.ConstantTimeCompare([]byte(got), h.token) == 1 {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="agentbox"`)
	writeError(w, http.StatusUnauthorized, "unauthorized", "缺少或无效的 Bearer token")
	return false
}

// authorize 在启用账号时确定调用者并按操作的 audience 放行。已由 serve 认证的 Bearer 调用者为运维；
// 否则读会话 cookie；两者都没有即为匿名（即便未配置 token 也不按运维处理），只能访问匿名操作。
// 返回附带调用者的请求；ok 为 false 时已写出响应。
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, aud audience) (*http.Request, bool) {
	if aud == audienceAnyone {
		return r, true
	}
	p, ok := principalFrom(r.Context())
	if !ok {
		if c, err := r.Cookie(SessionCookie); err == nil && c.Value != "" {
			u, err := h.cfg.Accounts.SessionUser(r.Context(), account.HashSessionID(c.Value))
			switch {
			case err == nil:
				p, ok = principal{user: u}, true
			case errors.Is(err, persistence.ErrNotFound):
			default:
				writeStoreError(w, err)
				return r, false
			}
		}
		if ok {
			r = r.WithContext(withPrincipal(r.Context(), p))
		}
	}
	switch {
	case !ok:
		writeError(w, http.StatusUnauthorized, "unauthorized", "需要登录或有效的 Bearer token")
		return r, false
	case aud == audienceAdmin && !p.admin:
		writeError(w, http.StatusForbidden, "forbidden", "该操作只对运维开放")
		return r, false
	case aud == audienceUser && p.admin:
		writeError(w, http.StatusForbidden, "forbidden", "该操作只对用户会话开放")
		return r, false
	}
	return r, true
}

// isUserCaller 报告调用者是否为用户会话（启用账号且不是运维）。
func isUserCaller(ctx context.Context) bool {
	p, ok := principalFrom(ctx)
	return ok && !p.admin
}

// userTaskJSON 是用户看到的任务视图：去掉内部的 current_attempt_id（运维视图不变）。
func userTaskJSON(ctx context.Context, v TaskView) taskJSON {
	j := toTaskJSON(v)
	if isUserCaller(ctx) {
		j.CurrentAttemptID = ""
	}
	return j
}

// userEventFields 是用户可见事件的允许列表：键为 "<source>/<type>"，值为 payload 中保留的字段，其余字段
// （费用、预算、模型、调用、try、用量、token、env、checkpoint 状态等）一律去掉。不在列表中的事件
// （Gateway 的 replay_divergence、worker 的 ready、checkpoint、checkpoint_query、artifact、paused、error 等）
// 对用户不出现；事件的 task_seq 不变，游标照常前进，Last-Event-ID 续传不受影响。
var userEventFields = map[string][]string{
	"host/task_created":         {},
	"host/attempt_created":      {"attempt_no"},
	"host/control_accepted":     {"desired", "control_version"},
	"host/control_applied":      {"status", "control_version"},
	"host/checkpoint_committed": {"checkpoint_id", "commit_seq", "step_id"},
	"host/artifact_saved":       {"artifact_id", "version"},
	"host/attempt_ended":        {"task_status", "status_reason"},
	"host/" + EventTaskTerminal: {"task_status", "status_reason"},
	"worker/progress":           {"step_id", "kind", "message"},
	"worker/result":             {"summary", "outputs"},
}

// userEvent 返回事件的用户视图；不在允许列表中的事件返回 false。worker_seq 是内部序号，也去掉。
func userEvent(ev Event) (Event, bool) {
	fields, ok := userEventFields[ev.Source+"/"+ev.Type]
	if !ok {
		return Event{}, false
	}
	var in map[string]json.RawMessage
	_ = json.Unmarshal(ev.Payload, &in) // payload 不是对象时按空对象处理：只保留允许的字段
	out := make(map[string]json.RawMessage, len(fields))
	for _, f := range fields {
		if v, ok := in[f]; ok {
			out[f] = v
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return Event{}, false
	}
	ev.Payload, ev.WorkerSeq = b, 0
	return ev, true
}

// ownsTask 对用户调用者检查路径中的任务属于自己；他人、无主与不存在的任务回答相同的 404。
func (h *Handler) ownsTask(w http.ResponseWriter, r *http.Request) bool {
	p, ok := principalFrom(r.Context())
	if !ok || p.admin {
		return true
	}
	owner, err := h.cfg.Accounts.TaskOwner(r.Context(), r.PathValue("id"))
	if err != nil && !errors.Is(err, persistence.ErrNotFound) {
		writeStoreError(w, err)
		return false
	}
	if err != nil || owner == 0 || owner != p.user.ID {
		writeError(w, http.StatusNotFound, "task_not_found", "任务不存在")
		return false
	}
	return true
}

// ---- /auth ----

type credentialsBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type userJSON struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}

// decodeQuiet 同 decodeBody，但错误消息不回显解码细节（请求体含密码）。
func decodeQuiet(w http.ResponseWriter, r *http.Request, v any) bool {
	if _, ok := readJSON(w, r, v); !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", "请求体须为一个合法的 JSON 对象")
		return false
	}
	return true
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	if !h.registerLimit.Allow(clientIP(r)) {
		writeError(w, http.StatusTooManyRequests, "rate_limited", "请求过于频繁，请稍后再试")
		return
	}
	var body credentialsBody
	if !decodeQuiet(w, r, &body) {
		return
	}
	display, key, err := account.NormalizeUsername(body.Username)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_username", "用户名须为 3–32 位字母、数字、下划线、点或连字符")
		return
	}
	if account.ValidatePassword(body.Password) != nil {
		writeError(w, http.StatusBadRequest, "invalid_password", "密码须为 8–128 个字符")
		return
	}
	hash, err := account.HashPassword(body.Password)
	if err != nil {
		h.logError("生成密码哈希", err)
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
		return
	}
	u, err := h.cfg.Accounts.CreateUser(r.Context(), display, key, hash)
	if errors.Is(err, persistence.ErrConflict) {
		writeError(w, http.StatusConflict, "username_taken", "用户名已被使用")
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if h.startSession(w, r, u) {
		writeJSON(w, http.StatusCreated, userJSON{Username: u.Username, Role: u.Role})
	}
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	if !h.loginLimit.Allow(clientIP(r)) {
		writeError(w, http.StatusTooManyRequests, "rate_limited", "请求过于频繁，请稍后再试")
		return
	}
	var body credentialsBody
	if !decodeQuiet(w, r, &body) {
		return
	}
	fail := func() { writeError(w, http.StatusUnauthorized, "invalid_credentials", "用户名或密码错误") }
	_, key, err := account.NormalizeUsername(body.Username)
	if err != nil {
		account.DummyVerify(body.Password) // 与真实校验同样的代价，不泄露用户名是否合法
		fail()
		return
	}
	u, hash, err := h.cfg.Accounts.UserForLogin(r.Context(), key)
	if errors.Is(err, persistence.ErrNotFound) {
		account.DummyVerify(body.Password) // 用户不存在时也付出一次哈希计算，消除时序差异
		fail()
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	// 先做真实校验再判停用，使停用用户与密码错误的耗时相同。
	if !account.VerifyPassword(hash, body.Password) || u.Disabled {
		fail()
		return
	}
	if h.startSession(w, r, u) {
		writeJSON(w, http.StatusOK, userJSON{Username: u.Username, Role: u.Role})
	}
}

// startSession 建立会话并下发 cookie；失败时已写出错误响应。
func (h *Handler) startSession(w http.ResponseWriter, r *http.Request, u User) bool {
	id, idHash, err := account.NewSessionID()
	if err != nil {
		h.logError("生成会话 ID", err)
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
		return false
	}
	if err := h.cfg.Accounts.CreateSession(r.Context(), idHash, u.ID, h.now().Add(sessionTTL)); err != nil {
		writeStoreError(w, err)
		return false
	}
	h.setSessionCookie(w, id, int(sessionTTL/time.Second))
	return true
}

// setSessionCookie 下发会话 cookie：HttpOnly、SameSite=Strict、Path=/；TLS 监听时带 Secure。
// maxAge 为负时删除（Max-Age=0）。
func (h *Handler) setSessionCookie(w http.ResponseWriter, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: value, Path: "/", MaxAge: maxAge,
		HttpOnly: true, Secure: h.cfg.SecureCookies, SameSite: http.SameSiteStrictMode,
	})
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(SessionCookie)
	if err != nil { // authorize 已确认会话有效，不会发生
		writeError(w, http.StatusUnauthorized, "unauthorized", "需要登录")
		return
	}
	if err := h.cfg.Accounts.DeleteSession(r.Context(), account.HashSessionID(c.Value)); err != nil {
		writeStoreError(w, err)
		return
	}
	h.setSessionCookie(w, "", -1)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context()) // audienceUser：authorize 已确认是用户
	writeJSON(w, http.StatusOK, userJSON{Username: p.user.Username, Role: p.user.Role})
}

// ---- /research ----

type researchBody struct {
	RequestID string `json:"request_id"`
	Topic     string `json:"topic"`
}

// researchHashInput 是 body_hash 的输入：请求体加上用户，使不同用户的相同请求体得到不同的 body_hash
// （提交结果未知时按 request_id 核对，不会取到他人的结果）。
type researchHashInput struct {
	researchBody
	UserID int64 `json:"user_id"`
}

// createResearch 以主题创建用户研究：spec 由 server 生成（固定模型与搜索配置），limits 用服务端默认值。
func (h *Handler) createResearch(w http.ResponseWriter, r *http.Request) {
	p, _ := principalFrom(r.Context()) // audienceUser：authorize 已确认是用户
	var body researchBody
	if !decodeBody(w, r, &body) || !validRequestID(w, body.RequestID) {
		return
	}
	topic := strings.TrimSpace(body.Topic)
	if n := utf8.RuneCountInString(topic); n < 1 || n > maxTopicRunes {
		writeError(w, http.StatusBadRequest, "invalid_topic", "主题须为 1–500 个字符")
		return
	}
	spec, err := h.cfg.ResearchSpec(topic)
	if err != nil || !isJSONObject(spec) {
		if err == nil {
			err = errors.New("ResearchSpec 返回的不是 JSON 对象")
		}
		h.logError("生成研究 spec", err)
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
		return
	}
	var limits json.RawMessage
	if h.cfg.EffectiveLimits != nil {
		if limits, err = h.cfg.EffectiveLimits(nil); err != nil || (len(limits) > 0 && !isJSONObject(limits)) {
			if err == nil {
				err = errors.New("EffectiveLimits 返回的不是 JSON 对象")
			}
			h.logError("研究的默认 limits", err)
			writeError(w, http.StatusInternalServerError, "internal", "内部错误")
			return
		}
	}
	canon, err := jcs.Canonical(researchHashInput{researchBody: body, UserID: p.user.ID})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "请求体无法规范化")
		return
	}
	req := CreateTaskRequest{
		RequestID: body.RequestID, BodyHash: bodyHash(canon), TaskID: h.cfg.NewTaskID(),
		Spec: mustCanonical(spec), ConfigVersion: h.cfg.ConfigVersion, MaxFaultRetries: h.cfg.MaxFaultRetries,
	}
	if len(limits) > 0 {
		req.Limits = mustCanonical(limits)
	}
	res, err := h.cfg.Accounts.CreateResearch(r.Context(), p.user.ID, req)
	if errors.Is(err, persistence.ErrCommitUnknown) {
		if got, ok := resolveUnknown(r.Context(), h.cfg.Store, req.RequestID, req.BodyHash, func(b json.RawMessage) (CreateTaskResult, bool) {
			var c CreateTaskResult
			return c, json.Unmarshal(b, &c) == nil && c.TaskID != ""
		}); ok {
			res, err = got, nil
		}
	}
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, res) // 重放时返回首次结果，状态码相同
	case errors.Is(err, ErrUserTaskRunning):
		writeError(w, http.StatusConflict, "user_task_running", "已有研究在进行中，请等待其结束")
	case errors.Is(err, persistence.ErrNotFound): // 用户在会话校验之后被停用或删除
		writeError(w, http.StatusUnauthorized, "unauthorized", "需要登录")
	default:
		writeStoreError(w, err)
	}
}
