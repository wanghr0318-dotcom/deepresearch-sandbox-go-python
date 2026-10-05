<template>
  <div class="calls">
    <div class="stats">
      <div class="stat">
        <div class="stat-label">已结算费用</div>
        <div class="stat-value" data-testid="total-cost">{{ formatMicroUSD(summary.cost) }}</div>
      </div>
      <div class="stat">
        <div class="stat-label">模型 / 工具调用</div>
        <div class="stat-value">{{ summary.calls }}</div>
      </div>
      <div class="stat">
        <div class="stat-label">上游尝试</div>
        <div class="stat-value">{{ summary.tries }}</div>
      </div>
      <div class="stat">
        <div class="stat-label">失败调用</div>
        <div class="stat-value" :class="{ bad: summary.failed > 0 }">{{ summary.failed }}</div>
      </div>
      <div class="stat">
        <div class="stat-label">累计上游延迟</div>
        <div class="stat-value">{{ formatMs(summary.latency) }}</div>
      </div>
    </div>
    <div v-if="budgetFailures.length" class="error-banner" data-testid="budget-failure">
      <span class="error-title">预算限制</span>：{{ budgetFailures.length }} 个调用因预算被拒（{{ budgetFailures.join("、") }}）。
    </div>

    <div v-if="calls.length === 0" class="empty">没有 Gateway 调用记录</div>
    <table v-else class="data-table">
      <thead>
        <tr>
          <th>call</th>
          <th>端点</th>
          <th>模型</th>
          <th>状态</th>
          <th class="num">尝试</th>
          <th class="num">费用</th>
          <th class="num">延迟</th>
          <th>说明</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="c in calls" :key="c.call_id">
          <tr :data-call="c.call_id">
            <td class="mono" :title="c.call_id">
              <button class="expander" type="button" :aria-expanded="open.has(c.call_id)" @click="toggle(c.call_id)">
                {{ open.has(c.call_id) ? "▾" : "▸" }}
              </button>
              {{ shortId(c.call_id) }}
            </td>
            <td class="mono">{{ c.endpoint }}</td>
            <td class="mono">{{ modelOf(c) }}</td>
            <td><span class="badge" :class="stateClass(c.state)">{{ c.state }}</span></td>
            <td class="num">{{ c.tries_used }}</td>
            <td class="num mono">{{ formatMicroUSD(c.cost_charged_micro) }}</td>
            <td class="num mono">{{ formatMs(latencyOf(c)) }}</td>
            <td class="small">
              <span v-if="c.fail_reason" class="bad">{{ c.fail_reason }}</span>
              <span v-if="c.possible_external_duplicate" class="badge paused" title="上游可能已执行过同一请求">可能重复</span>
              <span v-if="c.supersedes_call_id" class="muted">取代 {{ shortId(c.supersedes_call_id) }}</span>
            </td>
          </tr>
          <tr v-if="open.has(c.call_id)" class="tries-row">
            <td colspan="8">
              <table class="data-table tries">
                <thead>
                  <tr>
                    <th>try</th>
                    <th>attempt</th>
                    <th>状态</th>
                    <th>结果</th>
                    <th class="num">费用</th>
                    <th class="num">延迟</th>
                    <th>错误</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="t in c.tries" :key="t.try_no">
                    <td class="mono">{{ t.try_no }}</td>
                    <td class="mono" :title="t.attempt_id">{{ shortId(t.attempt_id) }}</td>
                    <td>{{ t.state }}</td>
                    <td>{{ t.outcome ?? "—" }}</td>
                    <td class="num mono">{{ formatMicroUSD(t.cost_micro) }}</td>
                    <td class="num mono">{{ formatMs(t.latency_ms) }}</td>
                    <td class="small bad">{{ t.error ?? "" }}</td>
                  </tr>
                </tbody>
              </table>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive } from "vue";
import type { Inspection, Schemas } from "../api/client";
import { formatMicroUSD, formatMs, shortId } from "../lib/format";

type Call = Schemas["Call"];

const props = defineProps<{ inspection: Inspection | null }>();

const calls = computed<Call[]>(() => props.inspection?.calls ?? []);
const open = reactive(new Set<string>());

function toggle(id: string): void {
  if (open.has(id)) open.delete(id);
  else open.add(id);
}

function latencyOf(c: Call): number {
  return (c.tries ?? []).reduce((s, t) => s + (t.latency_ms || 0), 0);
}

/** 调用使用的模型（journal 记录的解析后模型）；非 chat 调用没有该字段，显示 "—"。 */
function modelOf(c: Call): string {
  return c.model || "—";
}

function stateClass(state: string): string {
  switch (state) {
    case "completed":
      return "succeeded";
    case "failed":
      return "failed";
    case "in_flight":
    case "resolving":
      return "running";
    default:
      return "paused";
  }
}

const summary = computed(() => {
  let cost = 0;
  let tries = 0;
  let failed = 0;
  let latency = 0;
  for (const c of calls.value) {
    cost += c.cost_charged_micro || 0;
    tries += c.tries?.length ?? 0;
    if (c.state === "failed") failed++;
    latency += latencyOf(c);
  }
  return { cost, tries, failed, latency, calls: calls.value.length };
});

const budgetFailures = computed(() =>
  calls.value.filter((c) => (c.fail_reason ?? "").startsWith("budget")).map((c) => `${shortId(c.call_id)}: ${c.fail_reason}`),
);
</script>

<style scoped>
.calls {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 10px;
}
.stat {
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 10px 14px;
}
.stat-label {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: #6b7280;
}
.stat-value {
  font-size: 1.25rem;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: #111827;
}
.bad {
  color: #b91c1c;
}
.small {
  font-size: 0.75rem;
}
.expander {
  border: none;
  background: transparent;
  cursor: pointer;
  color: #6b7280;
  padding: 0 4px 0 0;
}
.tries-row > td {
  background: #fafafa;
  padding: 4px 10px 10px 28px;
}
.tries {
  font-size: 0.78rem;
}
</style>
