<template>
  <figure class="bar-chart">
    <figcaption class="bc-title">{{ title }}</figcaption>
    <div v-if="empty" class="bc-empty">{{ emptyText }}</div>
    <svg
      v-else
      :viewBox="`0 0 ${width} ${height}`"
      preserveAspectRatio="none"
      role="img"
      :aria-label="summary"
      class="bc-svg"
      data-testid="bar-chart"
    >
      <line :x1="0" :x2="width" :y1="plotH" :y2="plotH" class="bc-axis" />
      <g v-for="(b, i) in bars" :key="b.label">
        <rect
          :x="i * slot + gap / 2"
          :y="plotH - b.h"
          :width="Math.max(slot - gap, 1)"
          :height="b.h"
          rx="2"
          class="bc-bar"
          :class="{ accent: b.accent }"
        >
          <title>{{ b.label }}：{{ format(b.value) }}</title>
        </rect>
      </g>
    </svg>
    <div v-if="!empty" class="bc-scale muted">
      <span>{{ points[0]?.label }}</span>
      <span class="bc-max">最高 {{ format(max) }}</span>
      <span>{{ points[points.length - 1]?.label }}</span>
    </div>
  </figure>
</template>

<script setup lang="ts">
// 极简的无依赖柱状图（SVG）：每根柱子带 <title> 提示，整图有 aria-label 摘要；数据明细由页面以表格另行给出。
import { computed } from "vue";

export interface BarPoint {
  label: string;
  value: number;
  accent?: boolean;
}

const props = withDefaults(
  defineProps<{ title: string; points: BarPoint[]; format?: (v: number) => string; emptyText?: string }>(),
  { format: (v: number) => String(v), emptyText: "这段时间没有数据" },
);

const width = 600;
const height = 140;
const plotH = 132;
const gap = 3;

const max = computed(() => Math.max(0, ...props.points.map((p) => p.value)));
const empty = computed(() => props.points.length === 0 || max.value === 0);
const slot = computed(() => width / Math.max(props.points.length, 1));
const bars = computed(() =>
  props.points.map((p) => ({ ...p, h: max.value > 0 ? Math.max((p.value / max.value) * (plotH - 4), p.value > 0 ? 2 : 0) : 0 })),
);
const summary = computed(() => {
  const total = props.points.reduce((s, p) => s + p.value, 0);
  return `${props.title}：共 ${props.points.length} 天，合计 ${props.format(total)}，单日最高 ${props.format(max.value)}`;
});
</script>

<style scoped>
.bar-chart {
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.bc-title {
  font-size: 0.82rem;
  font-weight: 700;
  color: #4b5563;
}
.bc-svg {
  width: 100%;
  height: 140px;
  display: block;
}
.bc-axis {
  stroke: #e5e7eb;
  stroke-width: 1;
}
.bc-bar {
  fill: #8b5cf6;
}
.bc-bar.accent {
  fill: #f59e0b;
}
.bc-bar:hover {
  fill: #6d28d9;
}
.bc-scale {
  display: flex;
  justify-content: space-between;
  font-size: 0.72rem;
}
.bc-empty {
  padding: 28px;
  text-align: center;
  color: #9ca3af;
  font-size: 0.85rem;
  border: 1px dashed #e5e7eb;
  border-radius: 10px;
}
</style>
