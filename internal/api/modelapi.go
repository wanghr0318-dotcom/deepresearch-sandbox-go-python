package api

// 本文件是模型 API 的入口（模型网关设计 2026-10-11 §5、§7；G3）：POST /v1/chat/completions（OpenAI 格式）、
// POST /v1/messages（Anthropic 格式）与 GET /v1/models，只在配置了 Config.ModelAPI（--model-api）时注册。
// 认证与授权在这里完成，错误一律以调用方的格式返回（§7.5：OpenAI 的 error 对象或 Anthropic 的 error 对象），
// 而不是其余端点的 {code,message}：Authorization: Bearer abx_live_… 或 x-api-key: abx_live_… 的平台 key（需要 models
// scope），运维 Bearer token，或会话 cookie。认证通过后交给 Config.ModelAPI（internal/modelapi）处理。

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

var modelChatStatuses = []int{200, 400, 401, 403, 404, 429, 500, 502, 503}

// modelRoutes 只在配置了 ModelAPI 时注册；处理器在认证之后把请求交给 ModelAPI。
var modelRoutes = []route{
	{"POST", "/v1/chat/completions", accessWrite, audienceModels, modelChatStatuses, (*Handler).serveModelAPI},
	{"POST", "/v1/messages", accessWrite, audienceModels, modelChatStatuses, (*Handler).serveModelAPI},
	{"GET", "/v1/models", accessRead, audienceModels, []int{200, 401, 403, 503}, (*Handler).serveModelAPI},
}

// isModelPath 报告请求是否为已注册的模型 API 路径（serve 对它们跳过通用认证，由 authorizeModel 以调用方的格式认证）。
func (h *Handler) isModelPath(r *http.Request) bool {
	if h.cfg.ModelAPI == nil {
		return false
	}
	switch r.URL.Path {
	case "/v1/chat/completions", "/v1/messages", "/v1/models":
		return true
	}
	return false
}

// modelProtocol 是模型 API 请求的调用方格式：/v1/messages 与带 anthropic-version 的 /v1/models 为 Anthropic，其余为 OpenAI。
func modelProtocol(r *http.Request) llmproto.Protocol {
	if r.URL.Path == "/v1/messages" || (r.URL.Path == "/v1/models" && r.Header.Get("anthropic-version") != "") {
		return llmproto.ProtocolAnthropic
	}
	return llmproto.ProtocolOpenAI
}

// writeModelError 以调用方的格式写出错误。
func writeModelError(w http.ResponseWriter, p llmproto.Protocol, class llmproto.ErrorClass, msg string) {
	status, body := llmproto.EncodeError(p, llmproto.ErrorInfo{Class: class, Message: msg})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// authorizeModel 确定模型 API 的调用者：平台 key（Bearer 或 x-api-key）、运维 Bearer token 或会话 cookie；key 须有
// models scope。失败时以调用方的格式写出 401、403 或 503 并返回 false。
func (h *Handler) authorizeModel(w http.ResponseWriter, r *http.Request, p llmproto.Protocol) (*http.Request, bool) {
	fail := func(class llmproto.ErrorClass, msg string) (*http.Request, bool) {
		if class == llmproto.ClassInvalidAPIKey {
			w.Header().Set("WWW-Authenticate", `Bearer realm="agentbox"`)
		}
		reason := "auth"
		if class == llmproto.ClassUnavailable {
			reason = "unavailable"
		}
		obs.M().ModelRejected(string(p), reason)
		writeModelError(w, p, class, msg)
		return r, false
	}
	var pr principal
	if raw, isKey := h.keyCredential(r); isKey {
		id, err := h.cfg.APIKeys.Verify(r.Context(), raw)
		switch {
		case errors.Is(err, apikey.ErrInvalidKey):
			return fail(llmproto.ClassInvalidAPIKey, "invalid, revoked or expired API key")
		case err != nil:
			return fail(llmproto.ClassUnavailable, "credentials cannot be checked right now; try again later")
		}
		pr = keyPrincipal(id)
	} else if _, present := r.Header["Authorization"]; present {
		if !h.bearerIsOperator(r) {
			return fail(llmproto.ClassInvalidAPIKey, "invalid credentials: use an abx_live_ API key")
		}
		pr = principal{admin: true}
	} else {
		c, err := r.Cookie(SessionCookie)
		if err != nil || c.Value == "" {
			return fail(llmproto.ClassInvalidAPIKey, "missing credentials: send Authorization: Bearer <key> or x-api-key: <key>")
		}
		u, err := h.cfg.Accounts.SessionUser(r.Context(), account.HashSessionID(c.Value))
		switch {
		case errors.Is(err, persistence.ErrNotFound):
			return fail(llmproto.ClassInvalidAPIKey, "the session has expired; log in again")
		case err != nil:
			return fail(llmproto.ClassUnavailable, "credentials cannot be checked right now; try again later")
		}
		pr = principal{user: u}
	}
	if pr.key != "" && !pr.scopes.Has(tenant.Models) {
		return fail(llmproto.ClassPermissionDenied, "this API key lacks the models scope")
	}
	return r.WithContext(withPrincipal(r.Context(), pr)), true
}

// bearerIsOperator 报告 Authorization: Bearer 是否为运维 token（常数时间比较；未配置 token 时为假）。
func (h *Handler) bearerIsOperator(r *http.Request) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && h.token != nil && subtle.ConstantTimeCompare([]byte(got), h.token) == 1
}

// serveModelAPI 认证、检查运行模式，然后交给 ModelAPI。
func (h *Handler) serveModelAPI(w http.ResponseWriter, r *http.Request) {
	p := modelProtocol(r)
	r, ok := h.authorizeModel(w, r, p)
	if !ok {
		return
	}
	acc := accessWrite
	if r.Method == http.MethodGet {
		acc = accessRead
	}
	if _, msg, ok := h.modeAllows(acc); !ok {
		writeModelError(w, p, llmproto.ClassUnavailable, msg)
		return
	}
	h.cfg.ModelAPI.ServeHTTP(w, r)
}
