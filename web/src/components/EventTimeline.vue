<template>
  <div class="timeline">
    <div class="legend muted">
      <span class="badge host">host</span> 宿主事件
      <span class="badge worker">worker</span> Worker 事件
      <span class="count">共 {{ events.length }} 条，按 task_seq 排序</span>
    </div>
    <section v-for="lane in laneList" :key="lane.id" class="lane" :data-lane="lane.id">
      <header class="lane-header">
        <span class="lane-name mono">{{ lane.id === ROOT_LANE ? "root" : `sub-run ${lane.id}` }}</span>
        <span class="muted">{{ lane.events.length }} 条</span>
      </header>
      <div v-if="lane.events.length === 0" class="empty">{{ emptyText }}</div>
      <ol v-else class="events">
        <li
          v-for="ev in lane.events"
          :key="ev.task_seq"
          class="event"
          :class="[sourceKind(ev), { terminal: ev.type === 'task_terminal' }]"
          :data-seq="ev.task_seq"
        >
          <span class="seq mono">#{{ ev.task_seq }}</span>
          <span class="time mono muted">{{ formatTime(ev.ts) }}</span>
          <span class="badge" :class="sourceKind(ev)">{{ ev.source }}</span>
          <div class="body">
            <div class="head">
              <span class="type">{{ ev.type }}</span>
              <span v-if="ev.worker_seq" class="muted mono small">worker_seq {{ ev.worker_seq }}</span>
              <span v-if="ev.attempt_id" class="muted mono small" :title="ev.attempt_id">attempt {{ shortId(ev.attempt_id) }}</span>
            </div>
            <div v-if="eventSummary(ev)" class="summary mono">{{ eventSummary(ev) }}</div>
            <details v-if="ev.payload && Object.keys(ev.payload).length" class="payload">
              <summary>payload</summary>
              <pre class="raw-json">{{ JSON.stringify(ev.payload, null, 2) }}</pre>
            </details>
          </div>
        </li>
      </ol>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { TaskEvent } from "../api/client";
import { formatTime, shortId } from "../lib/format";
import { ROOT_LANE, eventSummary, lanes, sourceKind } from "../lib/timeline";

const props = withDefaults(defineProps<{ events: TaskEvent[]; emptyText?: string }>(), {
  emptyText: "暂无事件",
});

const laneList = computed(() => lanes(props.events));
</script>

<style scoped>
.timeline {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.legend {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.78rem;
}
.legend .count {
  margin-left: auto;
}
.lane {
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  overflow: hidden;
}
.lane-header {
  display: flex;
  justify-content: space-between;
  padding: 8px 14px;
  background: #f9fafb;
  border-bottom: 1px solid #e5e7eb;
  font-size: 0.8rem;
}
.lane-name {
  font-weight: 700;
  color: #4b5563;
}
.events {
  list-style: none;
  margin: 0;
  padding: 0;
}
.event {
  display: grid;
  grid-template-columns: 56px 104px 64px minmax(0, 1fr);
  gap: 10px;
  align-items: start;
  padding: 8px 14px;
  border-bottom: 1px solid #f3f4f6;
  border-left: 3px solid transparent;
}
.event:last-child {
  border-bottom: none;
}
.event.host {
  border-left-color: #a78bfa;
}
.event.worker {
  border-left-color: #38bdf8;
  background: #fbfdff;
}
.event.terminal {
  background: #f5f3ff;
}
.seq {
  color: #9ca3af;
  text-align: right;
}
.badge {
  justify-self: start;
}
.head {
  display: flex;
  gap: 10px;
  align-items: baseline;
  flex-wrap: wrap;
}
.type {
  font-weight: 700;
  font-size: 0.86rem;
  color: #111827;
}
.small {
  font-size: 0.72rem;
}
.summary {
  color: #4b5563;
  margin-top: 2px;
  word-break: break-all;
}
.payload summary {
  cursor: pointer;
  font-size: 0.75rem;
  color: #6b7280;
  margin-top: 4px;
}
.payload .raw-json {
  margin-top: 6px;
  max-height: 320px;
}
</style>
