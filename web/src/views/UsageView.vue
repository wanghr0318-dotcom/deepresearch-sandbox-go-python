<template>
  <div class="gw-page">
    <header class="gw-head">
      <div>
        <h1 class="gw-title">用量</h1>
        <p class="gw-sub">
          {{ scopeText }}的模型 API 与运行时（会话、研究）调用的请求数、token 与费用（美元，UTC 日期）。
          <span v-if="asOf" data-testid="as-of">截至 {{ formatDateTime(asOf) }} 的汇总。</span>
          <span v-else-if="loaded">尚未汇总（服务端每分钟汇总一次）。</span>
        </p>
      </div>
    </header>

    <form class="row range" @submit.prevent="load">
      <label class="field inline">从 <input v-model="from" type="date" name="from" /></label>
      <label class="field inline">到 <input v-model="to" type="date" name="to" /></label>
      <button class="btn" type="submit" :disabled="loading">{{ loading ? "加载中…" : "查询" }}</button>
    </form>

    <div v-if="error" class="user-error" role="alert" data-testid="usage-error">{{ error }}</div>

    <template v-if="loaded && !error">
      <section class="tiles" aria-label="区间合计">
        <div class="tile">
          <span class="t-label">费用</span><span class="t-value" data-testid="total-cost">{{ usd(totalCost) }}</span>
        </div>
        <div class="tile">
          <span class="t-label">其中未知</span><span class="t-value">{{ usd(total.unknown_micro) }}</span>
          <span class="t-hint">结果未知时按预估全额计入</span>
        </div>
        <div class="tile"><span class="t-label">请求</span><span class="t-value">{{ formatNumber(total.requests) }}</span></div>
        <div class="tile">
          <span class="t-label">token</span>
          <span class="t-value">{{ formatNumber(total.prompt_tokens + total.completion_tokens) }}</span>
          <span class="t-hint">输入 {{ formatNumber(total.prompt_tokens) }} · 输出 {{ formatNumber(total.completion_tokens) }}</span>
        </div>
      </section>

      <section v-if="total.requests === 0" class="panel empty" data-testid="no-usage">这段时间没有用量。</section>
      <section v-else class="gw-grid">
        <div class="panel">
          <BarChart title="每日费用" :points="costPoints" :format="usd" />
        </div>
        <div class="panel">
          <BarChart title="每日 token" :points="tokenPoints" :format="formatNumber" empty-text="这段时间没有 token 记录（运行时调用不记录 token）" />
        </div>
      </section>

      <section v-if="total.requests > 0" class="gw-grid">
        <div class="panel">
          <h2 class="panel-title">按模型</h2>
          <table class="data-table" data-testid="by-model">
            <thead><tr><th>模型</th><th>来源</th><th class="num">请求</th><th class="num">token</th><th class="num">费用</th></tr></thead>
            <tbody>
              <tr v-for="r in byModel" :key="`${r.model}|${r.source}`">
                <td>{{ modelLabel(r.model) }}</td>
                <td>{{ SOURCE_LABELS[r.source ?? ""] ?? r.source }}</td>
                <td class="num">{{ formatNumber(r.requests) }}</td>
                <td class="num">{{ formatNumber(r.prompt_tokens + r.completion_tokens) }}</td>
                <td class="num">{{ usd(r.cost_micro + r.unknown_micro) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="panel">
          <h2 class="panel-title">按密钥</h2>
          <table class="data-table" data-testid="by-key">
            <thead><tr><th>密钥</th><th class="num">请求</th><th class="num">token</th><th class="num">费用</th></tr></thead>
            <tbody>
              <tr v-for="r in byKey" :key="r.key_id ?? ''">
                <td>
                  <template v-if="r.key_id">{{ r.key_name }} <span class="mono muted">{{ r.key_id }}</span></template>
                  <span v-else class="muted">登录会话（无密钥）</span>
                </td>
                <td class="num">{{ formatNumber(r.requests) }}</td>
                <td class="num">{{ formatNumber(r.prompt_tokens + r.completion_tokens) }}</td>
                <td class="num">{{ usd(r.cost_micro + r.unknown_micro) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </template>

    <section class="panel">
      <h2 class="panel-title">本月预算</h2>
      <div v-if="budgetNote" class="muted" data-testid="budget-note">{{ budgetNote }}</div>
      <ul v-else class="budgets">
        <li v-for="b in budgets" :key="`${b.kind}:${b.id}`" class="budget" :data-budget="`${b.kind}:${b.id}`">
          <div class="b-head">
            <span class="b-name">{{ KIND_LABELS[b.kind] }} {{ b.label }}</span>
            <span class="spacer"></span>
            <span v-if="bar(b).unlimited" class="muted">不限额 · 已用 {{ usd(b.spent_micro + b.unknown_micro) }}</span>
            <span v-else :class="{ over: bar(b).over }">剩余 {{ usd(bar(b).remaining) }} / 上限 {{ usd(b.limit_micro) }}</span>
          </div>
          <div
            v-if="!bar(b).unlimited"
            class="b-track"
            role="meter"
            :aria-label="`${KIND_LABELS[b.kind]} ${b.label} 的预算用量`"
            aria-valuemin="0"
            :aria-valuemax="b.limit_micro ?? 0"
            :aria-valuenow="b.spent_micro + b.unknown_micro + b.reserved_micro"
          >
            <span class="seg spent" :style="{ width: `${bar(b).spentPct}%` }"></span>
            <span class="seg unknown" :style="{ width: `${bar(b).unknownPct}%` }"></span>
            <span class="seg reserved" :style="{ width: `${bar(b).reservedPct}%` }"></span>
          </div>
          <div class="b-legend muted">
            已花费 {{ usd(b.spent_micro) }} · 未知 {{ usd(b.unknown_micro) }} · 预留中 {{ usd(b.reserved_micro) }}
          </div>
        </li>
      </ul>
    </section>

    <section class="panel" data-testid="bill">
      <h2 class="panel-title">本月账单 {{ bill?.month ?? "" }}<span v-if="bill?.username" class="muted">（你在组织中的部分）</span></h2>
      <div v-if="billError" class="user-error" role="alert">{{ billError }}</div>
      <div v-else-if="bill && bill.total.requests === 0" class="muted">本月还没有费用。</div>
      <template v-else-if="bill">
        <p class="bill-total">
          合计 <strong data-testid="bill-total">{{ usd(bill.total.total_micro) }}</strong>
          <span class="muted">（已知 {{ usd(bill.total.cost_micro) }}，未知 {{ usd(bill.total.unknown_micro) }}，{{ formatNumber(bill.total.requests) }} 次请求）</span>
        </p>
        <table class="data-table">
          <thead><tr><th>模型</th><th class="num">输入 token</th><th class="num">输出 token</th><th class="num">已知</th><th class="num">未知</th><th class="num">合计</th></tr></thead>
          <tbody>
            <tr v-for="l in bill.models" :key="l.model">
              <td>{{ modelLabel(l.model) }}</td>
              <td class="num">{{ formatNumber(l.prompt_tokens) }}</td>
              <td class="num">{{ formatNumber(l.completion_tokens) }}</td>
              <td class="num">{{ usd(l.cost_micro) }}</td>
              <td class="num">{{ usd(l.unknown_micro) }}</td>
              <td class="num">{{ usd(l.total_micro) }}</td>
            </tr>
          </tbody>
        </table>
      </template>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ApiError } from "../api/client";
import type { Bill, Budget, BudgetKind, Me, UsageRow, UsageTotals } from "../api/gateway";
import BarChart from "../components/BarChart.vue";
import {
  SOURCE_LABELS,
  budgetBar,
  dailySeries,
  formatDateTime,
  formatNumber,
  gatewayErrorMessage,
  isUnauthorized,
  modelLabel,
  usd,
  utcDay,
  utcMonth,
} from "../lib/gateway";
import { useGatewayServices } from "../lib/gatewayServices";

const emit = defineEmits<{ unauthorized: [] }>();
const { api, now } = useGatewayServices();

const KIND_LABELS: Record<BudgetKind, string> = { org: "组织", user: "用户", key: "密钥" };
type LabeledBudget = Budget & { label: string };

const today = utcDay(now());
const from = ref(`${today.slice(0, 7)}-01`);
const to = ref(today);

const me = ref<Me | null>(null);
const loading = ref(false);
const loaded = ref(false);
const error = ref("");
const asOf = ref<string | null>(null);
const daily = ref<UsageRow[]>([]);
const byModel = ref<UsageRow[]>([]);
const byKey = ref<UsageRow[]>([]);
const total = ref<UsageTotals>({ requests: 0, prompt_tokens: 0, completion_tokens: 0, cost_micro: 0, unknown_micro: 0 });
const budgets = ref<LabeledBudget[]>([]);
const budgetNote = ref("");
const bill = ref<Bill | null>(null);
const billError = ref("");

const totalCost = computed(() => total.value.cost_micro + total.value.unknown_micro);
const series = computed(() => dailySeries(daily.value, from.value, to.value));
const costPoints = computed(() => series.value.map((p) => ({ label: p.day, value: p.cost, accent: p.unknown > 0 && p.unknown === p.cost })));
const tokenPoints = computed(() => series.value.map((p) => ({ label: p.day, value: p.tokens })));
const scopeText = computed(() => {
  if (!me.value) return "";
  return me.value.role === "owner" ? `组织 ${me.value.org?.name ?? ""} ` : "你";
});

function bar(b: Budget) {
  return budgetBar(b);
}

function fail(e: unknown): string {
  if (isUnauthorized(e)) emit("unauthorized");
  return gatewayErrorMessage(e);
}

async function loadUsage(): Promise<void> {
  if (!from.value || !to.value || from.value > to.value) {
    error.value = "请选择有效的日期范围（开始不晚于结束）";
    return;
  }
  loading.value = true;
  try {
    const range = { from: from.value, to: to.value };
    const [d, m, k] = await Promise.all([
      api.usage({ ...range, group_by: "day" }),
      api.usage({ ...range, group_by: "model,source" }),
      api.usage({ ...range, group_by: "key" }),
    ]);
    daily.value = d.rows;
    byModel.value = [...m.rows].sort((a, b) => b.cost_micro + b.unknown_micro - (a.cost_micro + a.unknown_micro));
    byKey.value = [...k.rows].sort((a, b) => b.cost_micro + b.unknown_micro - (a.cost_micro + a.unknown_micro));
    total.value = d.total;
    asOf.value = d.as_of;
    error.value = "";
    loaded.value = true;
  } catch (e) {
    error.value = fail(e);
  } finally {
    loading.value = false;
  }
}

async function loadBudgets(): Promise<void> {
  const m = me.value;
  if (!m?.username) return;
  const targets: { kind: BudgetKind; id: string; label: string }[] = [];
  if (m.org?.name) targets.push({ kind: "org", id: m.org.name, label: m.org.name });
  targets.push({ kind: "user", id: m.username, label: m.username });
  try {
    const keys = await api.listKeys();
    for (const k of keys) if (!k.revoked_at && k.username === m.username) targets.push({ kind: "key", id: k.id, label: k.name });
  } catch {
    // 只影响密钥层级的进度条
  }
  try {
    budgets.value = await Promise.all(targets.map(async (t) => ({ ...(await api.budget(t.kind, t.id)), label: t.label })));
    budgetNote.value = budgets.value.length === 0 ? "没有预算。" : "";
  } catch (e) {
    if (e instanceof ApiError && e.status === 403) budgetNote.value = "只有组织的 owner 可以查看预算；你的用量见上方与本月账单。";
    else budgetNote.value = fail(e);
  }
}

async function loadBill(): Promise<void> {
  try {
    bill.value = await api.bill(utcMonth(now()));
    billError.value = "";
  } catch (e) {
    billError.value = fail(e);
  }
}

async function load(): Promise<void> {
  await loadUsage();
}

onMounted(async () => {
  try {
    me.value = await api.me();
  } catch (e) {
    error.value = fail(e);
    return;
  }
  await Promise.all([loadUsage(), loadBudgets(), loadBill()]);
});
</script>

<style scoped>
.range {
  gap: 12px;
}
.field.inline {
  flex-direction: row;
  align-items: center;
  gap: 6px;
}
.tiles {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 12px;
}
.tile {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.t-label {
  font-size: 0.75rem;
  color: #6b7280;
  font-weight: 600;
}
.t-value {
  font-size: 1.35rem;
  font-weight: 700;
  color: #111827;
  font-variant-numeric: tabular-nums;
}
.t-hint {
  font-size: 0.72rem;
  color: #9ca3af;
}
.budgets {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.budget {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 0.85rem;
}
.b-head {
  display: flex;
  align-items: center;
  gap: 8px;
}
.b-name {
  font-weight: 600;
}
.b-track {
  display: flex;
  height: 10px;
  border-radius: 999px;
  background: #f3f4f6;
  overflow: hidden;
}
.seg.spent {
  background: #7c3aed;
}
.seg.unknown {
  background: #f59e0b;
}
.seg.reserved {
  background: #c4b5fd;
}
.b-legend {
  font-size: 0.75rem;
}
.over {
  color: #b91c1c;
  font-weight: 700;
}
.bill-total {
  margin: 0 0 10px;
  font-size: 0.95rem;
}
</style>
