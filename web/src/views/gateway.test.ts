import { mount } from "@vue/test-utils";
import { describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import type { ApiKey, Bill, Budget, GatewayApiLike, Invite, Me, Usage } from "../api/gateway";
import { flushAll } from "../components/testkit";
import { budgetBar, dailySeries, isPersonalOrg, remainingText, usd, usdExact } from "../lib/gateway";
import { gatewayServicesKey } from "../lib/gatewayServices";
import type { GatewayServices } from "../lib/gatewayServices";
import { parseHash } from "../lib/router";
import KeysView from "./KeysView.vue";
import OrgView from "./OrgView.vue";
import UsageView from "./UsageView.vue";

type Mocked<T> = { [K in keyof T]: T[K] & ReturnType<typeof vi.fn> };

const NOW = new Date("2026-10-12T08:00:00Z");
const SECRET = "abx_live_k_ABCDEFGHIJKLMNOP_SECRETSECRETSECRETSECRETSECRETSECRETSECRETS";

const OWNER: Me = { operator: false, username: "alice", org: { id: 7, name: "acme" }, role: "owner", scopes: ["runtime", "models"] };
const MEMBER: Me = { operator: false, username: "bob", org: { id: 7, name: "acme" }, role: "member", scopes: ["runtime", "models"] };

function key(over: Partial<ApiKey> = {}): ApiKey {
  return { id: "k_AAAAAAAAAAAAAAAA", name: "ci", org_id: 7, username: "alice", scopes: ["models"], created_at: "2026-10-01T00:00:00Z", ...over };
}

function invite(over: Partial<Invite> = {}): Invite {
  return {
    id: "inv1",
    org_id: 9,
    org: "beta",
    username: "alice",
    role: "member",
    invited_by: "carol",
    created_at: "2026-10-10T00:00:00Z",
    expires_at: "2026-10-17T00:00:00Z",
    ...over,
  };
}

const ZERO = { requests: 0, prompt_tokens: 0, completion_tokens: 0, cost_micro: 0, unknown_micro: 0 };

function emptyUsage(): Usage {
  return { from: "2026-10-01", to: "2026-10-12", group_by: ["day"], rows: [], total: { ...ZERO }, as_of: null, filter: { org: "acme" } };
}

function emptyBill(): Bill {
  return { month: "2026-10", org: { id: 7, name: "acme" }, models: [], sources: [], total: { ...ZERO, model: "", total_micro: 0 }, as_of: null, filter: { org: "acme" } };
}

function budget(over: Partial<Budget> = {}): Budget {
  return {
    kind: "org",
    id: "acme",
    period: "2026-10",
    limit_micro: 10_000_000,
    reserved_micro: 1_000_000,
    spent_micro: 4_000_000,
    unknown_micro: 500_000,
    available_micro: 4_500_000,
    ...over,
  };
}

function fakeApi(over: Partial<GatewayApiLike> = {}): Mocked<GatewayApiLike> {
  const base: GatewayApiLike = {
    me: vi.fn(async () => OWNER),
    listKeys: vi.fn(async () => [key()]),
    createKey: vi.fn(async (req) => ({ ...key({ id: "k_NEWNEWNEWNEWNEWN", name: req.name, scopes: req.scopes }), key: SECRET })),
    revokeKey: vi.fn(async (id: string) => key({ id, revoked_at: "2026-10-12T08:00:00Z" })),
    budget: vi.fn(async (kind, id) => budget({ kind, id })),
    usage: vi.fn(async () => emptyUsage()),
    bill: vi.fn(async () => emptyBill()),
    listMembers: vi.fn(async () => ({
      org: { id: 7, name: "acme" },
      members: [
        { org_id: 7, org: "acme", username: "alice", role: "owner" as const },
        { org_id: 7, org: "acme", username: "bob", role: "member" as const },
      ],
    })),
    removeMember: vi.fn(async () => {}),
    listInvites: vi.fn(async () => []),
    createInvite: vi.fn(async (username: string) => invite({ username, org_id: 7, org: "acme", invited_by: "alice" })),
    acceptInvite: vi.fn(async () => ({ org_id: 9, org: "beta", username: "alice", role: "member" as const })),
    deleteInvite: vi.fn(async () => {}),
  };
  for (const [k, fn] of Object.entries(over)) {
    (base as unknown as Record<string, unknown>)[k] = vi.isMockFunction(fn) ? fn : vi.fn(fn as (...a: unknown[]) => unknown);
  }
  return base as Mocked<GatewayApiLike>;
}

function kit(over: Partial<GatewayApiLike> = {}) {
  const api = fakeApi(over);
  const copy = vi.fn(async () => {});
  const services: GatewayServices = { api, copy, now: () => NOW };
  return { api, copy, global: { provide: { [gatewayServicesKey as symbol]: services } } };
}

describe("gateway routes and helpers", () => {
  it("parses the new pages and formats values", () => {
    expect(parseHash("#/keys")).toEqual({ name: "keys" });
    expect(parseHash("#/usage/")).toEqual({ name: "usage" });
    expect(parseHash("#/org")).toEqual({ name: "org" });
    expect(usd(1_234_567)).toBe("$1.23");
    expect(usd(1_234)).toBe("<$0.01");
    expect(usd(0)).toBe("$0.00");
    expect(usd(-500_000)).toBe("-$0.50");
    expect(usdExact(1_234)).toBe("$0.001234");
    expect(remainingText(-500_000)).toBe("超出 $0.50");
    expect(remainingText(4_500_000)).toBe("剩余 $4.50");
    expect(isPersonalOrg("user-12")).toBe(true);
    expect(isPersonalOrg("acme")).toBe(false);
  });

  it("fills missing days and computes budget segments", () => {
    const s = dailySeries([{ ...ZERO, day: "2026-10-02", cost_micro: 5, unknown_micro: 1, prompt_tokens: 3, completion_tokens: 2, requests: 1 }], "2026-10-01", "2026-10-03");
    expect(s.map((p) => [p.day, p.cost, p.tokens])).toEqual([
      ["2026-10-01", 0, 0],
      ["2026-10-02", 6, 5],
      ["2026-10-03", 0, 0],
    ]);
    const b = budgetBar(budget());
    expect([b.spentPct, b.unknownPct, b.reservedPct, b.remaining, b.over]).toEqual([40, 5, 10, 4_500_000, false]);
    expect(budgetBar(budget({ limit_micro: null, available_micro: null })).unlimited).toBe(true);
    const over = budgetBar(budget({ limit_micro: 100, spent_micro: 150, reserved_micro: 0, unknown_micro: 50, available_micro: -100 }));
    expect(over.over).toBe(true);
    expect([over.used, over.usedClamped]).toEqual([200, 100]);
    expect(over.spentPct + over.unknownPct + over.reservedPct).toBeCloseTo(100);
  });
});

describe("KeysView", () => {
  it("lists keys with their status and last use", async () => {
    const k = kit({
      listKeys: async () => [
        key({ last_used_at: "2026-10-11T00:00:00Z" }),
        key({ id: "k_BBBBBBBBBBBBBBBB", name: "old", revoked_at: "2026-10-05T00:00:00Z" }),
        key({ id: "k_CCCCCCCCCCCCCCCC", name: "short", expires_at: "2026-10-01T00:00:00Z" }),
      ],
    });
    const w = mount(KeysView, { global: k.global });
    await flushAll();
    const rows = w.findAll("tbody tr");
    expect(rows).toHaveLength(3);
    expect(rows[0]!.text()).toContain("有效");
    expect(rows[0]!.text()).not.toContain("从未使用");
    expect(rows[1]!.text()).toContain("已吊销");
    expect(rows[1]!.find('[data-action="revoke"]').exists()).toBe(false);
    expect(rows[2]!.text()).toContain("已过期");
    expect(rows[2]!.text()).toContain("从未使用");
  });

  it("creates a key and shows the full secret exactly once", async () => {
    const k = kit();
    const w = mount(KeysView, { global: k.global, attachTo: document.body });
    await flushAll();
    await w.get('[data-action="new-key"]').trigger("click");
    await w.get('[name="key-name"]').setValue("  deploy  ");
    await w.get('[name="scope-runtime"]').setValue(true);
    await w.get('[name="expiry"]').setValue("30");
    await w.get('[data-testid="key-form"]').trigger("submit");
    await flushAll();
    expect(k.api.createKey).toHaveBeenCalledTimes(1);
    const req = k.api.createKey.mock.calls[0]![0];
    expect(req.name).toBe("deploy");
    expect([...req.scopes].sort()).toEqual(["models", "runtime"]);
    expect(req.expires_at).toBe(new Date(NOW.getTime() + 30 * 86_400_000).toISOString());

    const dialog = w.get('[data-testid="secret-dialog"]');
    expect(dialog.get('[data-testid="secret"]').text()).toBe(SECRET);
    expect(dialog.text()).toContain("唯一一次显示完整密钥");
    await dialog.get('[data-action="copy"]').trigger("click");
    await flushAll();
    expect(k.copy).toHaveBeenCalledWith(SECRET);
    expect(dialog.get('[data-action="copy"]').text()).toBe("已复制");
    // The new key is in the list, without its secret.
    expect(w.find('[data-key="k_NEWNEWNEWNEWNEWN"]').exists()).toBe(true);

    await w.get('[data-action="close-secret"]').trigger("click");
    expect(w.find('[data-testid="secret-dialog"]').exists()).toBe(false);
    expect(w.html()).not.toContain(SECRET);
    await flushAll();
    expect(document.activeElement).toBe(w.get('[data-action="new-key"]').element); // focus returns
    w.unmount();
  });

  it("keeps focus inside the secret dialog and closes it with Esc from anywhere", async () => {
    const k = kit();
    const w = mount(KeysView, { global: k.global, attachTo: document.body });
    await flushAll();
    await w.get('[data-action="new-key"]').trigger("click");
    await w.get('[name="key-name"]').setValue("x");
    await w.get('[data-testid="key-form"]').trigger("submit");
    await flushAll();
    const close = w.get('[data-action="close-secret"]').element as HTMLElement;
    const copyBtn = w.get('[data-action="copy"]').element as HTMLElement;
    close.focus();
    await w.get('[data-testid="secret-dialog"]').trigger("keydown", { key: "Tab" });
    expect(document.activeElement).toBe(copyBtn); // wraps to the first control
    copyBtn.focus();
    await w.get('[data-testid="secret-dialog"]').trigger("keydown", { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(close);
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    await flushAll();
    expect(w.find('[data-testid="secret-dialog"]').exists()).toBe(false);
    expect(document.activeElement).toBe(w.get('[data-action="new-key"]').element);
    w.unmount();
  });

  it("validates the form and shows server errors", async () => {
    const k = kit({ createKey: vi.fn().mockRejectedValue(new ApiError(400, "invalid_scopes", "bad")) });
    const w = mount(KeysView, { global: k.global });
    await flushAll();
    await w.get('[data-action="new-key"]').trigger("click");
    await w.get('[data-testid="key-form"]').trigger("submit");
    expect(w.get('[data-testid="form-error"]').text()).toContain("名称");
    await w.get('[name="key-name"]').setValue("x");
    await w.get('[name="scope-models"]').setValue(false);
    await w.get('[data-testid="key-form"]').trigger("submit");
    expect(w.get('[data-testid="form-error"]').text()).toContain("权限范围");
    expect(k.api.createKey).not.toHaveBeenCalled();
    await w.get('[name="scope-models"]').setValue(true);
    await w.get('[data-testid="key-form"]').trigger("submit");
    await flushAll();
    expect(w.get('[data-testid="form-error"]').text()).toBe("请至少选择一个权限范围");
    expect(w.find('[data-testid="secret-dialog"]').exists()).toBe(false);
  });

  it("revokes only after confirmation", async () => {
    const k = kit();
    const w = mount(KeysView, { global: k.global });
    await flushAll();
    await w.get('[data-action="revoke"]').trigger("click");
    expect(k.api.revokeKey).not.toHaveBeenCalled();
    await w.get('[data-action="cancel-revoke"]').trigger("click");
    expect(w.find('[data-action="confirm-revoke"]').exists()).toBe(false);
    await w.get('[data-action="revoke"]').trigger("click");
    await w.get('[data-action="confirm-revoke"]').trigger("click");
    await flushAll();
    expect(k.api.revokeKey).toHaveBeenCalledWith("k_AAAAAAAAAAAAAAAA");
    expect(w.get("tbody tr").text()).toContain("已吊销");
  });

  it("shows the empty state, the invite banner, errors, and signs out on 401", async () => {
    let k = kit({ listKeys: async () => [], listInvites: async () => [invite(), invite({ id: "x", username: "bob" })] });
    let w = mount(KeysView, { global: k.global });
    await flushAll();
    expect(w.find('[data-testid="no-keys"]').exists()).toBe(true);
    expect(w.get('[data-testid="invite-banner"]').text()).toContain("1 个组织邀请");

    k = kit({ listKeys: vi.fn().mockRejectedValue(new ApiError(503, "store_unavailable", "x")) });
    w = mount(KeysView, { global: k.global });
    await flushAll();
    expect(w.get('[data-testid="list-error"]').text()).toContain("服务暂时不可用");

    k = kit({ me: vi.fn().mockRejectedValue(new ApiError(401, "unauthorized", "x")) });
    w = mount(KeysView, { global: k.global });
    await flushAll();
    expect(w.emitted("unauthorized")).toBeTruthy();
  });
});

describe("UsageView", () => {
  it("renders empty data without charts", async () => {
    const k = kit();
    const w = mount(UsageView, { global: k.global });
    await flushAll();
    expect(w.find('[data-testid="no-usage"]').exists()).toBe(true);
    expect(w.find('[data-testid="bar-chart"]').exists()).toBe(false);
    expect(w.get('[data-testid="total-cost"]').text()).toBe("$0.00");
    expect(w.get('[data-testid="bill"]').text()).toContain("本月还没有费用");
    expect(w.text()).toContain("尚未汇总");
    // Budgets: org, user and the caller's live key.
    expect(k.api.budget.mock.calls.map((c) => c.slice(0, 2))).toEqual([
      ["org", "acme"],
      ["user", "alice"],
      ["key", "k_AAAAAAAAAAAAAAAA"],
    ]);
    expect(w.findAll(".budget")).toHaveLength(3);
    expect(w.get('[data-budget="org:acme"]').text()).toContain("剩余 $4.50 / 上限 $10.00");
    expect(w.get('[data-budget="org:acme"] [role="meter"]').attributes("aria-valuenow")).toBe("5500000");
    expect(k.api.bill).toHaveBeenCalledWith("2026-10");
  });

  it("renders daily charts, breakdowns and the bill", async () => {
    const usage = vi.fn(async (p?: { group_by?: string }) => {
      const u = emptyUsage();
      u.total = { requests: 3, prompt_tokens: 100, completion_tokens: 50, cost_micro: 2_000_000, unknown_micro: 500_000 };
      u.as_of = "2026-10-12T07:59:00Z";
      if (p?.group_by === "day") u.rows = [{ ...u.total, day: "2026-10-05" }];
      if (p?.group_by === "model,source") {
        u.rows = [
          { ...ZERO, model: "smart", source: "model_api", requests: 2, cost_micro: 2_000_000, prompt_tokens: 100, completion_tokens: 50 },
          { ...ZERO, model: "search", source: "runtime", requests: 1, unknown_micro: 500_000 },
        ];
      }
      if (p?.group_by === "key") u.rows = [{ ...u.total, key_id: "k_AAAAAAAAAAAAAAAA", key_name: "ci" }];
      return u;
    });
    const bill = emptyBill();
    bill.models = [{ ...ZERO, model: "smart", requests: 2, cost_micro: 2_000_000, total_micro: 2_000_000 }];
    bill.total = { ...ZERO, model: "", requests: 2, cost_micro: 2_000_000, unknown_micro: 0, total_micro: 2_000_000 };
    const k = kit({ usage, bill: async () => bill });
    const w = mount(UsageView, { global: k.global });
    await flushAll();
    expect(w.findAll('[data-testid="bar-chart"]')).toHaveLength(2);
    expect(w.get('[data-testid="bar-chart"]').attributes("aria-label")).toContain("共 12 天");
    expect(w.get('[data-testid="total-cost"]').text()).toBe("$2.50");
    const models = w.findAll('[data-testid="by-model"] tbody tr');
    expect(models[0]!.text()).toContain("smart");
    expect(models[1]!.text()).toContain("运行时");
    expect(w.get('[data-testid="by-key"]').text()).toContain("ci");
    expect(w.get('[data-testid="bill-total"]').text()).toBe("$2.00");
    expect(w.find('[data-testid="as-of"]').exists()).toBe(true);
  });

  it("shows loading states, overshoot and the effective filter", async () => {
    let resolveBill: (b: Bill) => void = () => {};
    const k = kit({
      me: async () => MEMBER,
      bill: () => new Promise<Bill>((r) => (resolveBill = r)),
      budget: async (kind, id) => budget({ kind, id, limit_micro: 1_000_000, spent_micro: 1_200_000, unknown_micro: 0, reserved_micro: 300_000, available_micro: -500_000 }),
      usage: async () => ({ ...emptyUsage(), filter: { org: "acme", username: "bob" } }),
    });
    const w = mount(UsageView, { global: k.global });
    expect(w.find('[data-testid="bill-loading"]').exists()).toBe(true);
    expect(w.find('[data-testid="budgets-loading"]').exists()).toBe(true);
    await flushAll();
    expect(w.find('[data-testid="budgets-loading"]').exists()).toBe(false);
    expect(w.get('[data-testid="usage-filter"]').text()).toBe("范围：组织 acme · 用户 bob");
    const org = w.get('[data-budget="org:acme"]');
    expect(org.get('[data-testid="remaining"]').text()).toContain("超出 $0.50");
    const meter = org.get('[role="meter"]');
    expect(meter.attributes("aria-valuenow")).toBe("1000000");
    expect(meter.attributes("aria-valuemax")).toBe("1000000");
    expect(meter.attributes("aria-valuetext")).toContain("已超出上限");
    expect(org.text()).toContain("已超出上限");
    expect(org.get(".money").attributes("title")).toMatch(/^\$\d+\.\d{6}$/);
    resolveBill({ ...emptyBill(), username: "bob", filter: { org: "acme", username: "bob" } });
    await flushAll();
    expect(w.find('[data-testid="bill-loading"]').exists()).toBe(false);
    expect(w.get('[data-testid="bill-filter"]').text()).toContain("bob");
  });

  it("explains budgets to members and shows errors", async () => {
    let k = kit({ me: async () => MEMBER, budget: vi.fn().mockRejectedValue(new ApiError(403, "forbidden", "x")) });
    let w = mount(UsageView, { global: k.global });
    await flushAll();
    expect(w.get('[data-testid="budget-note"]').text()).toContain("只有组织的 owner");

    k = kit({ usage: vi.fn().mockRejectedValue(new ApiError(503, "usage_unavailable", "x")) });
    w = mount(UsageView, { global: k.global });
    await flushAll();
    expect(w.get('[data-testid="usage-error"]').text()).toBe("服务端未启用用量统计");
    expect(w.find('[data-testid="no-usage"]').exists()).toBe(false);
  });
});

describe("OrgView", () => {
  it("lets the invitee accept or decline", async () => {
    const k = kit({ me: async () => ({ ...OWNER, org: { id: 3, name: "user-3" } }), listInvites: async () => [invite(), invite({ id: "inv2", org: "gamma" })] });
    const w = mount(OrgView, { global: k.global });
    await flushAll();
    expect(w.findAll('[data-testid="received"] li')).toHaveLength(2);
    expect(w.find('[data-action="leave"]').exists()).toBe(false); // personal org
    expect(w.find('[data-testid="members"]').exists()).toBe(false);
    await w.get('[data-invite="inv2"] [data-action="decline"]').trigger("click");
    await flushAll();
    expect(k.api.deleteInvite).toHaveBeenCalledWith("inv2");
    await w.get('[data-invite="inv1"] [data-action="accept"]').trigger("click");
    await flushAll();
    expect(k.api.acceptInvite).toHaveBeenCalledWith("inv1");
    expect(w.get('[data-testid="org-message"]').text()).toContain("已加入新组织");
    expect(k.api.me.mock.calls.length).toBeGreaterThanOrEqual(3); // reloaded after each action
  });

  it("shows an accept error", async () => {
    const k = kit({ listInvites: async () => [invite()], acceptInvite: vi.fn().mockRejectedValue(new ApiError(404, "invite_not_found", "x")) });
    const w = mount(OrgView, { global: k.global });
    await flushAll();
    await w.get('[data-action="accept"]').trigger("click");
    await flushAll();
    expect(w.get('[data-testid="org-error"]').text()).toBe("邀请不存在或已过期");
  });

  it("lets owners list members, invite, revoke invites and remove members", async () => {
    const sent = invite({ id: "s1", username: "dave", org_id: 7, org: "acme", invited_by: "alice" });
    const k = kit({ listInvites: async () => [sent] });
    const w = mount(OrgView, { global: k.global });
    await flushAll();
    expect(w.findAll('[data-testid="members"] tbody tr')).toHaveLength(2);
    expect(w.get('[data-member="alice"]').find('[data-action="remove"]').exists()).toBe(false);
    expect(w.get('[data-testid="sent"]').text()).toContain("dave");

    await w.get('[name="invite-username"]').setValue(" erin ");
    await w.get('[name="invite-role"]').setValue("owner");
    await w.get('[data-testid="invite-form"]').trigger("submit");
    await flushAll();
    expect(k.api.createInvite).toHaveBeenCalledWith("erin", "owner");

    k.api.createInvite.mockRejectedValueOnce(new ApiError(409, "already_member", "x"));
    await w.get('[name="invite-username"]').setValue("bob");
    await w.get('[data-testid="invite-form"]').trigger("submit");
    await flushAll();
    expect(w.get('[data-testid="invite-error"]').text()).toBe("该用户已经是组织成员");

    await w.get('[data-action="revoke-invite"]').trigger("click");
    await flushAll();
    expect(k.api.deleteInvite).toHaveBeenCalledWith("s1");

    await w.get('[data-member="bob"] [data-action="remove"]').trigger("click");
    expect(k.api.removeMember).not.toHaveBeenCalled();
    await w.get('[data-member="bob"] [data-action="confirm-remove"]').trigger("click");
    await flushAll();
    expect(k.api.removeMember).toHaveBeenCalledWith("bob");
  });

  it("lets members leave after confirmation and hides management", async () => {
    const k = kit({ me: async () => MEMBER });
    const w = mount(OrgView, { global: k.global });
    await flushAll();
    expect(k.api.listMembers).not.toHaveBeenCalled();
    expect(w.find('[data-testid="not-owner"]').exists()).toBe(true);
    expect(w.find('[data-testid="invite-form"]').exists()).toBe(false);
    await w.get('[data-action="leave"]').trigger("click");
    expect(k.api.removeMember).not.toHaveBeenCalled();
    await w.get('[data-action="confirm-leave"]').trigger("click");
    await flushAll();
    expect(k.api.removeMember).toHaveBeenCalledWith("bob");
    expect(w.get('[data-testid="org-message"]').text()).toContain("回到个人组织");
  });

  it("shows a load error", async () => {
    const k = kit({ me: vi.fn().mockRejectedValue(new ApiError(500, "internal", "x")) });
    const w = mount(OrgView, { global: k.global });
    await flushAll();
    expect(w.get('[data-testid="org-error"]').text()).toContain("服务暂时不可用");
  });
});
