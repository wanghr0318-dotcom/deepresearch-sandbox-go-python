<template>
  <div class="subruns">
    <div class="stats">
      <div class="stat" data-testid="task-total">
        <div class="stat-label">任务总额（task 层）</div>
        <div class="stat-value">{{ budget ? formatMicroUSD(budget.spent_micro) : "—" }}</div>
        <div v-if="budget" class="stat-note muted">
          上限 {{ formatMicroUSD(budget.limit_micro) }} · 预留 {{ formatMicroUSD(budget.reserved_micro) }} · 未知
          {{ formatMicroUSD(budget.unknown_micro) }}
        </div>
      </div>
      <div class="stat" data-testid="subrun-total">
        <div class="stat-label">sub-run 合计</div>
        <div class="stat-value">{{ formatMicroUSD(subrunSpent) }}</div>
        <div class="stat-note muted">{{ subruns.length }} 个 sub-run</div>
      </div>
      <div class="stat" data-testid="root-spent">
        <div class="stat-label">root（主 Agent）</div>
        <div class="stat-value">{{ budget ? formatMicroUSD(rootSpent) : "—" }}</div>
        <div class="stat-note muted">任务总额 − sub-run 合计</div>
      </div>
      <div v-if="budget" class="stat" data-testid="tool-calls">
        <div class="stat-label">工具调用</div>
        <div class="stat-value">
          {{ budget.tool_calls_used }}<template v-if="budget.tool_call_limit != null"> / {{ budget.tool_call_limit }}</template>
        </div>
      </div>
    </div>

    <div v-if="subruns.length === 0" class="empty">没有 sub-run</div>
    <table v-else class="data-table">
      <thead>
        <tr>
          <th>sub-run</th>
          <th>状态</th>
          <th class="timeline-col">时间线</th>
          <th class="num">时长</th>
          <th class="num">已花费 / 上限</th>
          <th class="num">预留</th>
          <th class="num">未知</th>
          <th class="num">调用</th>
          <th>原因</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="s in subruns" :key="s.subrun_id" :data-subrun="s.subrun_id">
          <td class="mono" :title="`父步骤 ${s.parent_step_id}`">{{ s.subrun_id }}</td>
          <td><span class="badge" :class="statusClass(s.status)">{{ s.status }}</span></td>
          <td class="timeline-col">
            <div class="track" :title="trackTitle(s)">
              <div class="bar" :class="statusClass(s.status)" data-testid="subrun-bar" :style="barStyle(s)"></div>
            </div>
          </td>
          <td class="num mono">{{ durationOf(s) }}</td>
          <td class="num mono">
            {{ formatMicroUSD(s.spent_micro) }} /
            <span v-if="s.cap_micro != null">{{ formatMicroUSD(s.cap_micro) }}</span>
            <span v-else class="muted">不设上限</span>
          </td>
          <td class="num mono">{{ formatMicroUSD(s.reserved_micro) }}</td>
          <td class="num mono" :class="{ bad: s.unknown_micro > 0 }">{{ formatMicroUSD(s.unknown_micro) }}</td>
          <td class="num">{{ s.calls }}</td>
          <td class="small">
            <span v-if="s.failure_reason" class="bad">{{ s.failure_reason }}</span>
            <span v-if="s.cancel_reason && s.cancel_reason !== s.failure_reason" class="muted">取消：{{ s.cancel_reason }}</span>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup lang="ts">
// 运维工作台的 sub-run 表（规格 §15.4）：每个 sub-run 的状态、时间线条（同一时间窗内对齐）、sub-run 层费用与
// 失败/取消原因；上方是两层费用：task 层总额、sub-run 合计与 root 部分。数据来自 GET /tasks/{id}/inspect。
import { computed } from "vue";
import type { Inspection, Subrun } from "../api/client";
import { formatMicroUSD } from "../lib/format";

const props = defineProps<{ inspection: Inspection | null; now?: number }>();

const subruns = computed<Subrun[]>(() => props.inspection?.subruns ?? []);
const budget = computed(() => props.inspection?.budget);
const subrunSpent = computed(() => subruns.value.reduce((s, x) => s + (x.spent_micro || 0), 0));
const rootSpent = computed(() => Math.max(0, (budget.value?.spent_micro ?? 0) - subrunSpent.value));

const TERMINAL = new Set(["completed", "cancelled", "failed", "timed_out"]);

function ms(ts: string | undefined): number {
  const v = ts ? Date.parse(ts) : NaN;
  return Number.isNaN(v) ? NaN : v;
}

/** 进行中的 sub-run 延伸到 now。 */
function endOf(s: Subrun, now: number): number {
  const e = ms(s.ended_at);
  if (!Number.isNaN(e)) return e;
  return TERMINAL.has(s.status) ? ms(s.started_at) : now;
}

const timeWindow = computed(() => {
  const now = props.now ?? Date.now();
  let start = Infinity;
  let end = -Infinity;
  for (const s of subruns.value) {
    const a = ms(s.started_at);
    if (Number.isNaN(a)) continue;
    start = Math.min(start, a);
    end = Math.max(end, endOf(s, now), a);
  }
  return { start, end, now };
});

function pct(x: number): string {
  return `${+Math.min(100, Math.max(0, x)).toFixed(2)}%`;
}

function barStyle(s: Subrun): Record<string, string> {
  const { start, end, now } = timeWindow.value;
  const a = ms(s.started_at);
  const span = end - start;
  if (Number.isNaN(a) || !(span > 0)) return { left: "0%", width: "100%" };
  const b = endOf(s, now);
  return { left: pct(((a - start) / span) * 100), width: pct(((Math.max(b, a) - a) / span) * 100) };
}

function durationOf(s: Subrun): string {
  const a = ms(s.started_at);
  const b = ms(s.ended_at);
  if (Number.isNaN(a) || Number.isNaN(b)) return TERMINAL.has(s.status) ? "—" : "进行中";
  const sec = Math.round((b - a) / 1000);
  return sec >= 60 ? `${Math.floor(sec / 60)}m${sec % 60}s` : `${sec}s`;
}

function trackTitle(s: Subrun): string {
  return `开始 ${s.started_at}${s.ended_at ? ` · 结束 ${s.ended_at}` : ""} · 截止 ${s.deadline_at}`;
}

function statusClass(status: string): string {
  switch (status) {
    case "started":
    case "end_proposed":
      return "running";
    case "cancel_requested":
      return "cancelling";
    case "completed":
      return "succeeded";
    case "cancelled":
      return "cancelled";
    case "failed":
    case "timed_out":
      return "failed";
    default:
      return "";
  }
}
</script>

<style scoped>
.subruns {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
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
.stat-note {
  font-size: 0.72rem;
  margin-top: 2px;
}
.timeline-col {
  width: 28%;
  min-width: 140px;
}
.track {
  position: relative;
  height: 10px;
  margin-top: 4px;
  border-radius: 5px;
  background: #f3f4f6;
}
.bar {
  position: absolute;
  top: 0;
  bottom: 0;
  min-width: 2px;
  border-radius: 5px;
  background: #9ca3af;
}
.bar.running {
  background: #60a5fa;
}
.bar.cancelling {
  background: #fb923c;
}
.bar.succeeded {
  background: #34d399;
}
.bar.cancelled {
  background: #9ca3af;
}
.bar.failed {
  background: #f87171;
}
.bad {
  color: #b91c1c;
}
.small {
  font-size: 0.75rem;
}
.small span + span {
  margin-left: 6px;
}
@media (max-width: 720px) {
  .data-table {
    display: block;
    overflow-x: auto;
  }
}
</style>
