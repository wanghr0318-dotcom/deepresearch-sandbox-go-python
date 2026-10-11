package api

// 本文件是用量与账单（模型网关设计 2026-10-11 §8；G5）：GET /v1/usage?from&to&group_by&org&user&key 与
// GET /v1/bills/{yyyy-mm}，以及成员页使用的 GET /v1/orgs/members。数据来自周期汇总的 usage_daily（as_of 是上次汇总的时间）。
// 可见范围：成员只看自己（在其当前 org 中的用量），owner 看本 org，运维看全部（?org= 选一个 org；账单须指定 org）。
// API key 一律 403 permission_denied（只有 cookie 与运维）。调用者看不到的 org、用户、key 与不存在的回答相同的 404。

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

var usageStatuses = []int{200, 400, 401, 403, 404, 500, 503}

// usageRoutes 随多租户路由注册（tenantRoutes 含它们）；未配置 Usage 时用量与账单为 503 usage_unavailable。
var usageRoutes = []route{
	{"GET", "/v1/usage", accessRead, audienceManage, usageStatuses, (*Handler).getUsage},
	{"GET", "/v1/bills/{month}", accessRead, audienceManage, usageStatuses, (*Handler).getBill},
	{"GET", "/v1/orgs/members", accessRead, audienceManage, usageStatuses, (*Handler).listMembers},
}

// memberLister 是 tenant.Store 的可选能力（internal/persistence/postgres 实现）：org 的成员列表。
type memberLister interface {
	ListMembers(ctx context.Context, orgID int64) ([]tenant.Membership, error)
}

// usageScope 是一次用量查询可见的范围：org 0 为全部 org（只有运维），user 0、key "" 为不限。
type usageScope struct {
	org     int64
	orgName string
	user    int64
	key     string
	member  string // 成员（非 owner）查询时为其用户名：只看自己
}

// resolveUsageScope 按调用者与 ?org=、?user=、?key= 确定范围；失败时已写出响应。
func (h *Handler) resolveUsageScope(w http.ResponseWriter, r *http.Request) (usageScope, bool) {
	var sc usageScope
	if h.cfg.Usage == nil {
		writeError(w, http.StatusServiceUnavailable, "usage_unavailable", "服务端未启用用量统计")
		return sc, false
	}
	p := caller(r)
	tp := p.tenant()
	qv := r.URL.Query()
	ctx := r.Context()
	orgNotFound := func() (usageScope, bool) {
		writeError(w, http.StatusNotFound, "org_not_found", "org 不存在")
		return sc, false
	}
	if !tp.Operator {
		m, err := h.memberOf(r, p.user.Username)
		if err != nil {
			writeTenancyError(w, err, "org_not_found", "org 不存在")
			return sc, false
		}
		sc.org, sc.orgName = m.OrgID, m.OrgName
		if m.Role != tenant.Owner {
			sc.user, sc.member = m.UserID, m.Username
		}
	}
	if name := qv.Get("org"); name != "" {
		o, err := h.cfg.Tenancy.OrgByName(ctx, name)
		switch {
		case errors.Is(err, persistence.ErrNotFound):
			return orgNotFound()
		case err != nil:
			writeStoreError(w, err)
			return sc, false
		case !tp.Operator && o.ID != sc.org:
			return orgNotFound()
		}
		sc.org, sc.orgName = o.ID, o.Name
	}
	if name := qv.Get("user"); name != "" {
		m, err := h.memberOf(r, name)
		if err == nil && ((sc.org != 0 && m.OrgID != sc.org) || (sc.member != "" && m.UserID != sc.user)) {
			err = persistence.ErrNotFound
		}
		if err != nil {
			writeTenancyError(w, err, "user_not_found", "用户不存在")
			return sc, false
		}
		sc.user = m.UserID
	}
	if id := qv.Get("key"); id != "" {
		var k tenant.Key
		err := persistence.ErrNotFound
		if apikey.ValidID(id) {
			k, err = h.cfg.Tenancy.GetKey(ctx, id)
		}
		if err == nil && (!tenant.CanSeeKey(tp, k) || (sc.org != 0 && k.OrgID != sc.org)) {
			err = persistence.ErrNotFound
		}
		if err != nil {
			writeTenancyError(w, err, "key_not_found", "key 不存在")
			return sc, false
		}
		sc.key = k.ID
	}
	return sc, true
}

type usageJSON struct {
	From    string        `json:"from"`
	To      string        `json:"to"`
	GroupBy []usage.Dim   `json:"group_by"`
	Rows    []usage.Group `json:"rows"`
	Total   usage.Totals  `json:"total"`
	AsOf    *time.Time    `json:"as_of"` // 上次汇总的时间；从未汇总时为 null
}

// asOf 返回上次汇总的时间（从未汇总为 nil）。
func (h *Handler) asOf(r *http.Request) (*time.Time, error) {
	st, err := h.cfg.Usage.Status(r.Context())
	if err != nil || st.LastRunAt.IsZero() {
		return nil, err
	}
	t := st.LastRunAt.UTC()
	return &t, nil
}

func (h *Handler) getUsage(w http.ResponseWriter, r *http.Request) {
	qv := r.URL.Query()
	now := h.now().UTC()
	from, to := usage.Month(now), time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var err error
	if s := qv.Get("from"); s != "" {
		if from, err = usage.ParseDay(s); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "from 须为 YYYY-MM-DD")
			return
		}
	}
	if s := qv.Get("to"); s != "" {
		if to, err = usage.ParseDay(s); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "to 须为 YYYY-MM-DD")
			return
		}
	}
	dims, err := usage.ParseDims(qv.Get("group_by"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "group_by 须为 day、model、key、user、org、source 中不重复的若干个（逗号分隔）")
		return
	}
	q := usage.Query{From: from, To: to, GroupBy: dims}
	if q.Validate() != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "日期范围无效：from 不能晚于 to，且至多 366 天")
		return
	}
	sc, ok := h.resolveUsageScope(w, r)
	if !ok {
		return
	}
	q.OrgID, q.UserID, q.KeyID = sc.org, sc.user, sc.key
	rows, err := h.cfg.Usage.Usage(r.Context(), q)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if rows == nil {
		rows = []usage.Group{}
	}
	out := usageJSON{From: from.Format("2006-01-02"), To: to.Format("2006-01-02"), GroupBy: dims, Rows: rows}
	for _, g := range rows {
		out.Total.Add(g.Totals)
	}
	if out.AsOf, err = h.asOf(r); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

type billJSON struct {
	usage.Bill
	Org      orgRefJSON `json:"org"`
	Username string     `json:"username,omitempty"` // 成员的账单：只含本人的用量
	AsOf     *time.Time `json:"as_of"`
}

// getBill：一个月（UTC）的账单，按模型列出 token 与费用（已知与未知分开），另按来源汇总。
func (h *Handler) getBill(w http.ResponseWriter, r *http.Request) {
	month, err := usage.ParseMonth(r.PathValue("month"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "月份须为 YYYY-MM")
		return
	}
	sc, ok := h.resolveUsageScope(w, r)
	if !ok {
		return
	}
	if sc.org == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "运维查看账单须指定 org")
		return
	}
	from, to := usage.MonthRange(month)
	rows, err := h.cfg.Usage.Usage(r.Context(), usage.Query{From: from, To: to, OrgID: sc.org, UserID: sc.user, KeyID: sc.key,
		GroupBy: []usage.Dim{usage.DimModel, usage.DimSource}})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	out := billJSON{Bill: usage.NewBill(month, rows), Org: orgRefJSON{ID: sc.org, Name: sc.orgName}, Username: sc.member}
	if out.AsOf, err = h.asOf(r); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

type memberListJSON struct {
	Org     orgRefJSON          `json:"org"`
	Members []tenant.Membership `json:"members"`
}

// listMembers：org 的成员与角色（owner 看本 org；运维须给 ?org=）。成员 403。
func (h *Handler) listMembers(w http.ResponseWriter, r *http.Request) {
	ml, ok := h.cfg.Tenancy.(memberLister)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "members_unavailable", "服务端不支持列出成员")
		return
	}
	tp := caller(r).tenant()
	ctx := r.Context()
	org := orgRefJSON{ID: tp.OrgID}
	switch name := r.URL.Query().Get("org"); {
	case name != "":
		o, err := h.cfg.Tenancy.OrgByName(ctx, name)
		if err == nil && !tp.Operator && o.ID != tp.OrgID {
			err = persistence.ErrNotFound
		}
		if err != nil {
			writeTenancyError(w, err, "org_not_found", "org 不存在")
			return
		}
		org = orgRefJSON{ID: o.ID, Name: o.Name}
	case tp.Operator:
		writeError(w, http.StatusBadRequest, "invalid_request", "运维查看成员须指定 org")
		return
	}
	if !tenant.CanManageMembers(tp, org.ID) {
		writeError(w, http.StatusForbidden, "forbidden", "只有 org 的 owner 或运维可以查看成员")
		return
	}
	ms, err := ml.ListMembers(ctx, org.ID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if org.Name == "" && len(ms) > 0 {
		org.Name = ms[0].OrgName
	}
	writeJSON(w, http.StatusOK, memberListJSON{Org: org, Members: ms})
}
