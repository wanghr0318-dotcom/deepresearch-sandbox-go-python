// 模型网关页面的纯函数：错误文案、金额与日期格式、预算进度、按日序列。
import { ApiError } from "../api/client";
import type { Budget, UsageRow } from "../api/gateway";
import { userErrorMessage } from "./userErrors";

export const GATEWAY_ERRORS: Record<string, string> = {
  forbidden: "你没有权限执行这个操作",
  permission_denied: "API 密钥不能管理账号，请用登录会话操作",
  key_not_found: "找不到这个密钥",
  user_not_found: "找不到这个用户",
  org_not_found: "找不到这个组织",
  invite_not_found: "邀请不存在或已过期",
  member_not_found: "该用户不是组织成员",
  already_member: "该用户已经是组织成员",
  user_in_other_org: "该用户已属于另一个组织",
  personal_org_closed: "个人组织不能邀请其他成员，请先让运维创建团队组织",
  last_owner: "组织至少要保留一个 owner",
  personal_org: "不能离开个人组织",
  invalid_scopes: "请至少选择一个权限范围",
  usage_unavailable: "服务端未启用用量统计",
  quota_unavailable: "服务端未启用预算",
  invalid_request: "请求无效，请检查输入",
};

/** 任意错误 → 网关页面的中文提示。 */
export function gatewayErrorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    const msg = GATEWAY_ERRORS[e.code];
    if (msg) return msg;
  }
  return userErrorMessage(e);
}

export function isUnauthorized(e: unknown): boolean {
  return e instanceof ApiError && e.status === 401;
}

/** 微美元 → 显示用的 "$1.23"：一律两位小数（复审 N3），不足一美分的非零金额为 "<$0.01"，负数为 "-$0.50"。 */
export function usd(micro: number | null | undefined): string {
  const m = Number(micro ?? 0);
  if (!Number.isFinite(m)) return "—";
  const sign = m < 0 ? "-" : "";
  const v = Math.abs(m) / 1_000_000;
  if (v > 0 && v < 0.005) return `${sign}<$0.01`;
  return `${sign}$${v.toFixed(2)}`;
}

/** 微美元 → 精确值 "$0.001234"（六位小数，用于 title 提示）。 */
export function usdExact(micro: number | null | undefined): string {
  const m = Number(micro ?? 0);
  if (!Number.isFinite(m)) return "—";
  const sign = m < 0 ? "-" : "";
  return `${sign}$${(Math.abs(m) / 1_000_000).toFixed(6)}`;
}

/** 预算剩余：负数（超出上限）显示为 "超出 $0.50"。 */
export function remainingText(micro: number | null | undefined): string {
  const m = Number(micro ?? 0);
  return m < 0 ? `超出 ${usd(-m)}` : `剩余 ${usd(m)}`;
}

export function formatNumber(n: number | null | undefined): string {
  return Number(n ?? 0).toLocaleString("zh-CN");
}

const pad = (n: number) => String(n).padStart(2, "0");

/** UTC 日期 YYYY-MM-DD。 */
export function utcDay(d: Date): string {
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

/** UTC 月份 YYYY-MM。 */
export function utcMonth(d: Date): string {
  return utcDay(d).slice(0, 7);
}

export function addDays(day: string, n: number): string {
  const d = new Date(`${day}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return utcDay(d);
}

/** from..to（含）之间的每一天。 */
export function daysBetween(from: string, to: string): string[] {
  const out: string[] = [];
  for (let d = from; d <= to && out.length < 400; d = addDays(d, 1)) out.push(d);
  return out;
}

/** 本地时间 YYYY-MM-DD HH:MM；空值为 "—"。 */
export function formatDateTime(ts: string | null | undefined): string {
  if (!ts) return "—";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

export interface DayPoint {
  day: string;
  cost: number; // 已知 + 未知（微美元）
  unknown: number;
  tokens: number;
  requests: number;
}

/** 按日分组的行 → 区间内每天一个点（没有用量的天为 0）。 */
export function dailySeries(rows: UsageRow[], from: string, to: string): DayPoint[] {
  const by = new Map<string, DayPoint>();
  for (const day of daysBetween(from, to)) by.set(day, { day, cost: 0, unknown: 0, tokens: 0, requests: 0 });
  for (const r of rows) {
    const p = r.day ? by.get(r.day) : undefined;
    if (!p) continue;
    p.cost += r.cost_micro + r.unknown_micro;
    p.unknown += r.unknown_micro;
    p.tokens += r.prompt_tokens + r.completion_tokens;
    p.requests += r.requests;
  }
  return [...by.values()];
}

export interface BudgetBar {
  unlimited: boolean;
  /** 已用（花费 + 未知 + 预留），截断到上限，供 aria-valuenow。 */
  used: number;
  usedClamped: number;
  spentPct: number;
  unknownPct: number;
  reservedPct: number;
  remaining: number | null;
  over: boolean;
}

/** 预算 → 进度条的三段（已花费、未知、预留，占上限的百分比，合计不超过 100）。 */
export function budgetBar(b: Budget): BudgetBar {
  const limit = b.limit_micro;
  if (limit === null || limit === undefined) {
    const used = b.spent_micro + b.unknown_micro + b.reserved_micro;
    return { unlimited: true, used, usedClamped: used, spentPct: 0, unknownPct: 0, reservedPct: 0, remaining: null, over: false };
  }
  const used = b.spent_micro + b.unknown_micro + b.reserved_micro;
  const pct = (v: number) => (limit > 0 ? (v / limit) * 100 : v > 0 ? 100 : 0);
  let spentPct = pct(b.spent_micro);
  let unknownPct = pct(b.unknown_micro);
  let reservedPct = pct(b.reserved_micro);
  const sum = spentPct + unknownPct + reservedPct;
  if (sum > 100) {
    spentPct = (spentPct / sum) * 100;
    unknownPct = (unknownPct / sum) * 100;
    reservedPct = (reservedPct / sum) * 100;
  }
  return {
    unlimited: false,
    used,
    usedClamped: Math.min(used, limit),
    spentPct,
    unknownPct,
    reservedPct,
    remaining: b.available_micro ?? limit - used,
    over: used > limit,
  };
}

/** 个人组织（user-<id>）不能邀请成员，也不能离开。 */
export function isPersonalOrg(name: string | undefined): boolean {
  return !!name && /^user-\d+$/.test(name);
}

export const SCOPE_LABELS: Record<string, string> = { runtime: "运行时（会话、研究）", models: "模型 API" };
export const ROLE_LABELS: Record<string, string> = { owner: "owner", member: "成员" };
export const SOURCE_LABELS: Record<string, string> = { model_api: "模型 API", runtime: "运行时" };

export function modelLabel(m: string | undefined): string {
  return m ? m : "（未记录）";
}
