package api

// 本文件是限额与月度预算（模型网关设计 2026-10-11 §6、§8；G2）：GET/PUT /v1/limits/{kind}/{id} 与
// GET/PUT /v1/budgets/{kind}/{id}（kind 为 org、user、key；id 分别为 org 名称、用户名、key ID），以及运行时 API 创建
// 端点（POST /sessions、/research、/sessions/{id}/messages、/turns/{id}/restore，以及启动沙箱的唤醒与 continue、finish、answer；
// POST /tasks 只对运维开放，不限流）按 key、user、org 三级的每分钟请求数限流。
// 授权：owner 可读写本 org 的用户与 key、只读本 org；运维全部；成员与 API key 一律不可（key 为 403 permission_denied）。
// 用户与 key 层级的值不能超过其 org 自己的上限。调用者看不到的对象（其他 org）与不存在的回答完全相同的 404。

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

var quotaStatuses = []int{200, 400, 401, 403, 404, 500, 503}

// quotaRoutes 随多租户路由注册（tenantRoutes 含它们）；未配置 Quota 时这些端点为 503 quota_unavailable。
var quotaRoutes = []route{
	{"GET", "/v1/limits/{kind}/{id}", accessRead, audienceManage, quotaStatuses, (*Handler).getLimits},
	{"PUT", "/v1/limits/{kind}/{id}", accessWrite, audienceManage, quotaStatuses, (*Handler).putLimits},
	{"GET", "/v1/budgets/{kind}/{id}", accessRead, audienceManage, quotaStatuses, (*Handler).getBudget},
	{"PUT", "/v1/budgets/{kind}/{id}", accessWrite, audienceManage, []int{200, 400, 401, 403, 404, 409, 500, 503}, (*Handler).putBudget},
}

// rateLimitedRoutes 是按每分钟请求数限流的运行时创建端点（API key 与 cookie 调用者；运维不限）。
var rateLimitedRoutes = map[string]bool{
	"POST /sessions": true, "POST /research": true, "POST /sessions/{id}/messages": true,
	// 复审 F1：同样新建 turn（restore）或启动沙箱（唤醒会话；continue、finish、answer 以新的 attempt 恢复暂停的 turn）。
	"POST /turns/{id}/restore": true, "POST /sessions/{id}/wake": true,
	"POST /turns/{id}/continue": true, "POST /turns/{id}/finish": true, "POST /turns/{id}/answer": true,
}

// quotaTarget 是路径中的 {kind}/{id} 解析后的层级与其所属 org。
type quotaTarget struct {
	target quota.Target
	org    int64
	kind   string
	id     string
}

// quotaNotFoundMsg 是各层级唯一的 404 消息：不存在与其他 org 的对象回答完全相同的正文（复审 F4，不泄露存在性）。
var quotaNotFoundMsg = map[quota.Level]string{quota.LevelOrg: "org 不存在", quota.LevelUser: "用户不存在", quota.LevelKey: "key 不存在"}

// resolveQuotaTarget 解析 {kind}/{id} 并按读（write 为 false）或写授权；失败时已写出响应。
func (h *Handler) resolveQuotaTarget(w http.ResponseWriter, r *http.Request, write bool) (quotaTarget, bool) {
	if h.cfg.Quota == nil {
		writeError(w, http.StatusServiceUnavailable, "quota_unavailable", "服务端未启用限额与预算")
		return quotaTarget{}, false
	}
	qt := quotaTarget{kind: r.PathValue("kind"), id: r.PathValue("id")}
	level, err := quota.ParseLevel(qt.kind)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "kind 须为 org、user 或 key")
		return qt, false
	}
	ctx := r.Context()
	notFound := func() (quotaTarget, bool) {
		writeError(w, http.StatusNotFound, string(level)+"_not_found", quotaNotFoundMsg[level])
		return qt, false
	}
	// lookupFailed 把查找错误写出：不存在为同一个 404，其余为存储错误。
	lookupFailed := func(err error) (quotaTarget, bool) {
		if errors.Is(err, persistence.ErrNotFound) {
			return notFound()
		}
		writeStoreError(w, err)
		return qt, false
	}
	switch level {
	case quota.LevelOrg:
		o, err := h.cfg.Tenancy.OrgByName(ctx, qt.id)
		if err != nil {
			return lookupFailed(err)
		}
		qt.org, qt.target = o.ID, quota.Target{Level: level, ID: strconv.FormatInt(o.ID, 10)}
	case quota.LevelUser:
		_, key, err := account.NormalizeUsername(qt.id)
		if err != nil {
			return notFound()
		}
		m, err := h.cfg.Tenancy.MemberByName(ctx, key)
		if err != nil {
			return lookupFailed(err)
		}
		// 用户层级按 (org, user) 记（复审 F5）：指的是该用户在其当前 org 中的行。
		qt.org, qt.target = m.OrgID, quota.Target{Level: level, ID: quota.UserTargetID(m.OrgID, m.UserID)}
	case quota.LevelKey:
		if !apikey.ValidID(qt.id) {
			return notFound()
		}
		k, err := h.cfg.Tenancy.GetKey(ctx, qt.id)
		if err != nil {
			return lookupFailed(err)
		}
		qt.org, qt.target = k.OrgID, quota.Target{Level: level, ID: k.ID}
	}
	p := caller(r).tenant()
	if !p.Operator && p.OrgID != qt.org {
		return notFound()
	}
	if !tenant.CanReadLimits(p, qt.org) || (write && !tenant.CanWriteLimits(p, level == quota.LevelOrg, qt.org)) {
		writeError(w, http.StatusForbidden, "forbidden", "只有 org 的 owner（本 org 的用户与 key）或运维可以管理限额与预算")
		return qt, false
	}
	return qt, true
}

// ---- /v1/limits ----

type limitsJSON struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	quota.Limits
}

func (h *Handler) getLimits(w http.ResponseWriter, r *http.Request) {
	qt, ok := h.resolveQuotaTarget(w, r, false)
	if !ok {
		return
	}
	l, err := h.cfg.Quota.GetLimits(r.Context(), qt.target)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, limitsJSON{Kind: qt.kind, ID: qt.id, Limits: l})
}

func validLimit(v *int64) bool { return v == nil || (*v >= 0 && *v <= math.MaxInt32) }

func (h *Handler) putLimits(w http.ResponseWriter, r *http.Request) {
	var body quota.Limits
	if !decodeBody(w, r, &body) {
		return
	}
	for _, v := range []*int64{body.RPM, body.TPM, body.MaxSandboxes, body.MaxStreams} {
		if !validLimit(v) {
			writeError(w, http.StatusBadRequest, "invalid_request", "限额须为 null 或 0–2147483647 的整数")
			return
		}
	}
	qt, ok := h.resolveQuotaTarget(w, r, true)
	if !ok {
		return
	}
	ctx := r.Context()
	if qt.target.Level != quota.LevelOrg && body.MaxSandboxes != nil { // 复审 F12：并发沙箱只在 org 层级计数
		writeError(w, http.StatusBadRequest, "unsupported_limit", "max_sandboxes 只能在 org 层级设置")
		return
	}
	if qt.target.Level != quota.LevelOrg {
		orgLimits, err := h.cfg.Quota.GetLimits(ctx, quota.Target{Level: quota.LevelOrg, ID: strconv.FormatInt(qt.org, 10)})
		if err != nil {
			writeStoreError(w, err)
			return
		}
		if f := body.Exceeding(orgLimits); f != "" {
			writeError(w, http.StatusBadRequest, "exceeds_org_limit", f+" 不能超过所属 org 的上限")
			return
		}
	}
	if err := h.cfg.Quota.PutLimits(ctx, qt.target, body); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, limitsJSON{Kind: qt.kind, ID: qt.id, Limits: body})
}

// ---- /v1/budgets ----

type budgetJSON struct {
	Kind           string `json:"kind"`
	ID             string `json:"id"`
	Period         string `json:"period"` // YYYY-MM（UTC）
	LimitMicro     *int64 `json:"limit_micro"`
	ReservedMicro  int64  `json:"reserved_micro"`
	SpentMicro     int64  `json:"spent_micro"`
	UnknownMicro   int64  `json:"unknown_micro"`
	AvailableMicro *int64 `json:"available_micro"` // limit − spent − reserved − unknown；不限时为 null
}

func toBudgetJSON(qt quotaTarget, b quota.Budget) budgetJSON {
	out := budgetJSON{Kind: qt.kind, ID: qt.id, Period: b.Period.Format("2006-01"), LimitMicro: b.LimitMicro,
		ReservedMicro: b.ReservedMicro, SpentMicro: b.SpentMicro, UnknownMicro: b.UnknownMicro}
	if b.LimitMicro != nil {
		a := *b.LimitMicro - b.Used()
		out.AvailableMicro = &a
	}
	return out
}

func (h *Handler) getBudget(w http.ResponseWriter, r *http.Request) {
	var period time.Time // 零值：数据库时间所在的月份（与 hold 相同）
	if s := r.URL.Query().Get("period"); s != "" {
		p, err := time.Parse("2006-01", s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "period 须为 YYYY-MM")
			return
		}
		period = p
	}
	qt, ok := h.resolveQuotaTarget(w, r, false)
	if !ok {
		return
	}
	b, err := h.cfg.Quota.GetBudget(r.Context(), qt.target, period)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toBudgetJSON(qt, b))
}

type putBudgetBody struct {
	LimitMicro *int64 `json:"limit_micro"`
}

// putBudget 设置本月（UTC）的月度上限（null 为不限）；之后的月份沿用它。低于本月已用为 409 budget_below_usage。
func (h *Handler) putBudget(w http.ResponseWriter, r *http.Request) {
	var body putBudgetBody
	if !decodeBody(w, r, &body) {
		return
	}
	if body.LimitMicro != nil && *body.LimitMicro < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "limit_micro 须为 null 或非负整数（微美元）")
		return
	}
	qt, ok := h.resolveQuotaTarget(w, r, true)
	if !ok {
		return
	}
	ctx := r.Context()
	var period time.Time // 本月以数据库时间为准（与 hold 相同）
	if qt.target.Level != quota.LevelOrg && body.LimitMicro != nil {
		ob, err := h.cfg.Quota.GetBudget(ctx, quota.Target{Level: quota.LevelOrg, ID: strconv.FormatInt(qt.org, 10)}, period)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		if ob.LimitMicro != nil && *body.LimitMicro > *ob.LimitMicro {
			writeError(w, http.StatusBadRequest, "exceeds_org_limit", "月度预算不能超过所属 org 的上限")
			return
		}
	}
	b, err := h.cfg.Quota.PutBudget(ctx, qt.target, period, body.LimitMicro)
	var below *quota.BelowUsageError
	if errors.As(err, &below) {
		writeError(w, http.StatusConflict, "budget_below_usage", "上限低于本月已用（含预留）："+strconv.FormatInt(below.Used, 10)+" 微美元")
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toBudgetJSON(qt, b))
}

// ---- 运行时 API 限流 ----

// requestBuckets 是 p 各层级有 rpm 上限的请求桶（key → user → org）。
func requestBuckets(s quota.Scope, ls map[quota.Level]quota.Limits) []ratelimit.Bucket {
	var out []ratelimit.Bucket
	for _, t := range s.Targets() {
		if l, ok := ls[t.Level]; ok && l.RPM != nil {
			out = append(out, ratelimit.Bucket{Scope: string(t.Level) + ":" + t.ID, Kind: "rpm", Limit: *l.RPM, Window: time.Minute})
		}
	}
	return out
}

// rateLimit 对 API key 与 cookie 调用者按 key、user、org 三级的每分钟请求数限流（运维不限）。有任一级上限时写出
// x-ratelimit-limit-requests、x-ratelimit-remaining-requests、x-ratelimit-reset-requests；超限时 429
// rate_limit_exceeded 带 Retry-After（秒）。返回 false 时已写出响应。
func (h *Handler) rateLimit(w http.ResponseWriter, r *http.Request) bool {
	p, ok := principalFrom(r.Context())
	if !ok || p.admin {
		return true
	}
	s := quota.ScopeOf(p.tenant())
	if s.OrgID == 0 {
		return true // 没有 org 的用户（不应发生）：没有可用的限额
	}
	ls, err := h.cfg.Quota.LimitsFor(r.Context(), s)
	if err != nil {
		writeStoreError(w, err)
		return false
	}
	buckets := requestBuckets(s, ls)
	if len(buckets) == 0 {
		return true
	}
	d, err := h.cfg.RateLimiter.Take(r.Context(), buckets, 1)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeError(w, http.StatusServiceUnavailable, "store_unavailable", "请求已取消")
			return false
		}
		h.logError("限流", err)
		return true // 限流只保护容量（钱由预算保护）：限流器异常时放行
	}
	hd := w.Header()
	hd.Set("x-ratelimit-limit-requests", strconv.FormatInt(d.Limit, 10))
	hd.Set("x-ratelimit-remaining-requests", strconv.FormatInt(d.Remaining, 10))
	hd.Set("x-ratelimit-reset-requests", ceilSeconds(d.Reset).String())
	if !d.Allowed {
		hd.Set("Retry-After", strconv.FormatInt(int64(ceilSeconds(d.RetryAfter)/time.Second), 10))
		writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "请求过于频繁：超过每分钟请求数上限，请在 Retry-After 秒后重试")
		return false
	}
	return true
}

// ceilSeconds 把 d 向上取整到秒（至少 1 秒）。
func ceilSeconds(d time.Duration) time.Duration {
	s := (d + time.Second - 1) / time.Second
	if s < 1 {
		s = 1
	}
	return s * time.Second
}
