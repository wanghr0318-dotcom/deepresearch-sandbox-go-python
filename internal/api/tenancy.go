package api

// 本文件是多租户管理端点与平台 API key 认证（模型网关设计 2026-10-11 §5、§8；G1）：GET /v1/me，POST/GET/DELETE
// /v1/keys，POST/DELETE /v1/orgs/members，POST /v1/orgs（运维）。只在启用账号且配置了 Tenancy 时注册。
// API key 永远不能管理 key、成员与 org（audienceManage / audienceAdmin 对 key 一律 403 permission_denied），
// 泄露的 key 因而不能铸造新 key。

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

var tenantRoutes = []route{
	{"GET", "/v1/me", accessRead, audienceMe, []int{200, 401, 403, 500, 503}, (*Handler).getMe},
	{"POST", "/v1/keys", accessWrite, audienceManage, []int{201, 400, 401, 403, 404, 500, 503}, (*Handler).createKey},
	{"GET", "/v1/keys", accessRead, audienceManage, []int{200, 401, 403, 500, 503}, (*Handler).listKeys},
	{"DELETE", "/v1/keys/{id}", accessWrite, audienceManage, []int{200, 401, 403, 404, 500, 503}, (*Handler).revokeKey},
	{"POST", "/v1/orgs", accessWrite, audienceAdmin, []int{201, 400, 401, 403, 404, 409, 500, 503}, (*Handler).createOrg},
	{"POST", "/v1/orgs/members", accessWrite, audienceManage, []int{200, 400, 401, 403, 404, 409, 500, 503}, (*Handler).addMember},
	{"DELETE", "/v1/orgs/members/{user}", accessWrite, audienceManage, []int{204, 400, 401, 403, 404, 409, 500, 503}, (*Handler).removeMember},
}

// keyCredential 返回请求携带的平台 API key：Authorization: Bearer abx_…，或没有 Authorization 头时的 x-api-key
// （x-api-key 只用于 key：其他值同样按无效 key 处理）。未配置 Tenancy 时恒为 false（abx_ Bearer 按运维 token 判定）。
func (h *Handler) keyCredential(r *http.Request) (string, bool) {
	if h.cfg.Tenancy == nil {
		return "", false
	}
	if _, present := r.Header["Authorization"]; present {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		return got, ok && apikey.LooksLikeKey(got)
	}
	if _, present := r.Header["X-Api-Key"]; present {
		return r.Header.Get("X-Api-Key"), true
	}
	return "", false
}

// authenticateKey 校验 key 并把其用户（带 key 与 scope）放入请求；无效时 401 invalid_api_key。
func (h *Handler) authenticateKey(w http.ResponseWriter, r *http.Request, raw string) (*http.Request, bool) {
	id, err := h.cfg.APIKeys.Verify(r.Context(), raw)
	switch {
	case errors.Is(err, apikey.ErrInvalidKey):
		w.Header().Set("WWW-Authenticate", `Bearer realm="agentbox"`)
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "API key 无效、已吊销或已过期")
		return r, false
	case err != nil:
		writeStoreError(w, err)
		return r, false
	}
	tp := id.Principal
	u := User{ID: tp.UserID, Username: id.Username, Role: "user", OrgID: tp.OrgID, OrgRole: string(tp.Role)}
	return r.WithContext(withPrincipal(r.Context(), principal{user: u, key: tp.KeyID, scopes: tp.Scopes})), true
}

// writeTenancyError 映射多租户用例的错误；notFound 为 ErrNotFound 时的错误码与消息。
func writeTenancyError(w http.ResponseWriter, err error, notFoundCode, notFoundMsg string) {
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		writeError(w, http.StatusNotFound, notFoundCode, notFoundMsg)
	case errors.Is(err, tenant.ErrUserInOtherOrg):
		writeError(w, http.StatusConflict, "user_in_other_org", "该用户已属于另一个 org（每个用户只能属于一个 org）")
	case errors.Is(err, tenant.ErrLastOwner):
		writeError(w, http.StatusConflict, "last_owner", "org 须至少保留一个 owner")
	case errors.Is(err, tenant.ErrPersonalOrg):
		writeError(w, http.StatusConflict, "personal_org", "不能离开个人 org")
	default:
		writeStoreError(w, err)
	}
}

// usernameKey 规范化用户名；不合规的名字不可能存在，按 404 处理。
func usernameKey(w http.ResponseWriter, name string) (string, bool) {
	_, key, err := account.NormalizeUsername(name)
	if err != nil {
		writeError(w, http.StatusNotFound, "user_not_found", "用户不存在")
		return "", false
	}
	return key, true
}

// ---- /v1/me ----

type orgRefJSON struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type meJSON struct {
	Operator bool        `json:"operator"`
	Username string      `json:"username,omitempty"`
	Org      *orgRefJSON `json:"org,omitempty"`
	Role     string      `json:"role,omitempty"`
	Scopes   []string    `json:"scopes"`
	KeyID    string      `json:"key_id,omitempty"`
}

func (h *Handler) getMe(w http.ResponseWriter, r *http.Request) {
	p := caller(r)
	tp := p.tenant()
	out := meJSON{Operator: tp.Operator, Scopes: tp.Scopes.Names(), KeyID: tp.KeyID}
	if !p.admin {
		out.Username, out.Role = p.user.Username, string(tp.Role)
		m, err := h.memberOf(r, p.user.Username)
		switch {
		case err == nil:
			out.Org = &orgRefJSON{ID: m.OrgID, Name: m.OrgName}
			if !p.isKey() { // cookie：取当前角色；key 的角色来自校验时的记录
				out.Role = string(m.Role)
			}
		case !errors.Is(err, persistence.ErrNotFound): // 没有 org（不应发生）时省略 org
			writeStoreError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (p principal) isKey() bool { return p.key != "" }

// memberOf 读取用户（按展示用户名）的成员关系。
func (h *Handler) memberOf(r *http.Request, username string) (tenant.Membership, error) {
	_, key, err := account.NormalizeUsername(username)
	if err != nil {
		return tenant.Membership{}, persistence.ErrNotFound
	}
	return h.cfg.Tenancy.MemberByName(r.Context(), key)
}

// ---- /v1/keys ----

type keyJSON struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	OrgID      int64      `json:"org_id"`
	Username   string     `json:"username"`
	Scopes     []string   `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

func toKeyJSON(k tenant.Key) keyJSON {
	return keyJSON{ID: k.ID, Name: k.Name, OrgID: k.OrgID, Username: k.Username, Scopes: k.Scopes.Names(), CreatedAt: k.CreatedAt,
		ExpiresAt: k.ExpiresAt, RevokedAt: k.RevokedAt, LastUsedAt: k.LastUsedAt}
}

type createKeyBody struct {
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Username  string     `json:"username,omitempty"` // 只供运维：为该用户创建
}

type createdKeyJSON struct {
	Key string `json:"key"` // 完整 key，只在创建时返回这一次
	keyJSON
}

func (h *Handler) createKey(w http.ResponseWriter, r *http.Request) {
	p := caller(r)
	var body createKeyBody
	if !decodeBody(w, r, &body) {
		return
	}
	name := strings.TrimSpace(body.Name)
	if !apikey.ValidName(name) {
		writeError(w, http.StatusBadRequest, "invalid_request", "name 须为 1–64 个字符且不含控制字符")
		return
	}
	scopes, err := tenant.ParseScopes(body.Scopes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_scopes", "scopes 须为 runtime、models 中的一个或两个，且不重复")
		return
	}
	if body.ExpiresAt != nil && !body.ExpiresAt.After(h.now()) {
		writeError(w, http.StatusBadRequest, "invalid_request", "expires_at 须晚于当前时间")
		return
	}
	target := p.user.Username
	switch {
	case body.Username != "":
		target = body.Username
	case p.admin:
		writeError(w, http.StatusBadRequest, "invalid_request", "运维创建 key 须指定 username")
		return
	}
	key, ok := usernameKey(w, target)
	if !ok {
		return
	}
	m, err := h.cfg.Tenancy.MemberByName(r.Context(), key)
	if err != nil {
		writeTenancyError(w, err, "user_not_found", "用户不存在")
		return
	}
	if !tenant.CanCreateKeyFor(p.tenant(), m) {
		writeError(w, http.StatusForbidden, "forbidden", "只能为自己创建 key")
		return
	}
	g, err := h.cfg.APIKeys.Generate()
	if err != nil {
		h.logError("生成 API key", err)
		writeError(w, http.StatusInternalServerError, "internal", "内部错误")
		return
	}
	var exp *time.Time
	if body.ExpiresAt != nil {
		t := body.ExpiresAt.UTC()
		exp = &t
	}
	k, err := h.cfg.Tenancy.CreateKey(r.Context(), tenant.NewKey{ID: g.ID, UserID: m.UserID, Name: name, SecretHMAC: g.SecretHMAC,
		Scopes: scopes, ExpiresAt: exp})
	if err != nil {
		writeTenancyError(w, err, "user_not_found", "用户不存在")
		return
	}
	writeJSON(w, http.StatusCreated, createdKeyJSON{Key: g.Full, keyJSON: toKeyJSON(k)})
}

type keyListJSON struct {
	Keys []keyJSON `json:"keys"`
}

func (h *Handler) listKeys(w http.ResponseWriter, r *http.Request) {
	f, ok := tenant.KeyListFilter(caller(r).tenant())
	if !ok { // authorize 已拒绝 key 调用者，不会发生
		writeError(w, http.StatusForbidden, "permission_denied", "API key 不能管理 key")
		return
	}
	keys, err := h.cfg.Tenancy.ListKeys(r.Context(), f)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := keyListJSON{Keys: make([]keyJSON, 0, len(keys))}
	for _, k := range keys {
		out.Keys = append(out.Keys, toKeyJSON(k))
	}
	writeJSON(w, http.StatusOK, out)
}

// revokeKey 吊销 key（幂等）。调用者看不到的 key 与不存在的回答相同的 404。
func (h *Handler) revokeKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	notFound := func() { writeError(w, http.StatusNotFound, "key_not_found", "key 不存在") }
	if !apikey.ValidID(id) {
		notFound()
		return
	}
	k, err := h.cfg.Tenancy.GetKey(r.Context(), id)
	if err != nil {
		writeTenancyError(w, err, "key_not_found", "key 不存在")
		return
	}
	if !tenant.CanSeeKey(caller(r).tenant(), k) {
		notFound()
		return
	}
	if k, err = h.cfg.Tenancy.RevokeKey(r.Context(), id); err != nil {
		writeTenancyError(w, err, "key_not_found", "key 不存在")
		return
	}
	writeJSON(w, http.StatusOK, toKeyJSON(k))
}

// ---- /v1/orgs ----

type orgJSON struct {
	ID        int64              `json:"id"`
	Name      string             `json:"name"`
	Disabled  bool               `json:"disabled"`
	CreatedAt time.Time          `json:"created_at"`
	Members   int64              `json:"members"`
	Owner     *tenant.Membership `json:"owner,omitempty"`
}

type createOrgBody struct {
	Name  string `json:"name"`
	Owner string `json:"owner,omitempty"` // 可选：把该用户（须仍在个人 org 中）移入为 owner
}

func (h *Handler) createOrg(w http.ResponseWriter, r *http.Request) {
	var body createOrgBody
	if !decodeBody(w, r, &body) {
		return
	}
	if err := tenant.ValidateOrgName(body.Name); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_org_name", "org 名称须为 2–63 个 a-z、0-9、.、_、- 字符，以字母或数字开头，且不能是 operator 或 user-<数字>")
		return
	}
	ctx := r.Context()
	var ownerKey string
	if body.Owner != "" {
		var ok bool
		if ownerKey, ok = usernameKey(w, body.Owner); !ok {
			return
		}
		m, err := h.cfg.Tenancy.MemberByName(ctx, ownerKey) // 先确认能移入，避免建了 org 却加不了 owner
		if err != nil {
			writeTenancyError(w, err, "user_not_found", "用户不存在")
			return
		}
		if !tenant.IsPersonalOrg(m.OrgName, m.UserID) {
			writeTenancyError(w, tenant.ErrUserInOtherOrg, "", "")
			return
		}
	}
	o, err := h.cfg.Tenancy.CreateOrg(ctx, body.Name)
	if errors.Is(err, persistence.ErrConflict) {
		writeError(w, http.StatusConflict, "org_exists", "org 名称已被使用")
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := orgJSON{ID: o.ID, Name: o.Name, Disabled: o.Disabled, CreatedAt: o.CreatedAt, Members: o.Members}
	if ownerKey != "" {
		m, err := h.cfg.Tenancy.AddMember(ctx, o.ID, ownerKey, tenant.Owner)
		if err != nil {
			writeTenancyError(w, err, "user_not_found", "用户不存在")
			return
		}
		out.Owner, out.Members = &m, 1
	}
	writeJSON(w, http.StatusCreated, out)
}

// memberOrg 确定成员操作的目标 org：运维须给出 org 名称；owner 默认自己的 org（给出的名称须是自己的 org）。
func (h *Handler) memberOrg(w http.ResponseWriter, r *http.Request, orgName string) (int64, bool) {
	tp := caller(r).tenant()
	orgID := tp.OrgID
	switch {
	case orgName != "":
		o, err := h.cfg.Tenancy.OrgByName(r.Context(), orgName)
		if err != nil {
			writeTenancyError(w, err, "org_not_found", "org 不存在")
			return 0, false
		}
		orgID = o.ID
	case tp.Operator:
		writeError(w, http.StatusBadRequest, "invalid_request", "运维管理成员须指定 org")
		return 0, false
	}
	if !tenant.CanManageMembers(tp, orgID) {
		writeError(w, http.StatusForbidden, "forbidden", "只有 org 的 owner 或运维可以管理成员")
		return 0, false
	}
	return orgID, true
}

type addMemberBody struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	Org      string `json:"org,omitempty"`
}

func (h *Handler) addMember(w http.ResponseWriter, r *http.Request) {
	var body addMemberBody
	if !decodeBody(w, r, &body) {
		return
	}
	role, err := tenant.ParseRole(body.Role)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "role 须为 owner 或 member")
		return
	}
	orgID, ok := h.memberOrg(w, r, body.Org)
	if !ok {
		return
	}
	key, ok := usernameKey(w, body.Username)
	if !ok {
		return
	}
	m, err := h.cfg.Tenancy.AddMember(r.Context(), orgID, key, role)
	if errors.Is(err, persistence.ErrInvalid) {
		writeError(w, http.StatusBadRequest, "invalid_request", "保留 org 不接受成员")
		return
	}
	if err != nil {
		writeTenancyError(w, err, "user_not_found", "用户不存在")
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (h *Handler) removeMember(w http.ResponseWriter, r *http.Request) {
	orgID, ok := h.memberOrg(w, r, r.URL.Query().Get("org"))
	if !ok {
		return
	}
	key, ok := usernameKey(w, r.PathValue("user"))
	if !ok {
		return
	}
	if err := h.cfg.Tenancy.RemoveMember(r.Context(), orgID, key); err != nil {
		writeTenancyError(w, err, "member_not_found", "该用户不是 org 的成员")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}
