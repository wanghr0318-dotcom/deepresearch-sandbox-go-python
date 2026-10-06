<template>
  <section v-if="status === 'paused' && stop" class="stop-card" aria-label="研究已暂停">
    <div class="s-head">
      <span class="s-badge paused">研究已暂停</span>
      <span class="s-sub">可以继续研究，或用目前的资料立即写报告</span>
    </div>

    <ul v-if="stop.todo.length" class="todo">
      <li v-for="(t, i) in stop.todo" :key="i" :class="t.status">
        <span class="mark" aria-hidden="true">{{ TODO_MARK[t.status] }}</span>
        <span class="t-title">{{ t.title }}</span>
        <span class="sr-only">（{{ TODO_LABEL[t.status] }}）</span>
      </li>
    </ul>

    <dl class="stats">
      <div>
        <dt>子主题</dt>
        <dd>{{ stop.subtopicsDone }} / {{ stop.subtopicsTotal }}</dd>
      </div>
      <div>
        <dt>已读来源</dt>
        <dd>{{ stop.sources }}</dd>
      </div>
      <div>
        <dt>工具调用</dt>
        <dd>{{ stop.budget.used }} / {{ stop.budget.limit }}</dd>
      </div>
    </dl>

    <div v-if="stop.findings" class="findings">
      <h4 class="f-title">目前发现</h4>
      <p class="f-text">{{ stop.findings }}</p>
    </div>

    <div class="actions">
      <button class="btn primary small" type="button" data-action="continue" :disabled="busy" @click="emit('continue')">继续</button>
      <button
        class="btn small"
        type="button"
        data-action="finish"
        :disabled="busy || !stop.canFinish"
        :aria-describedby="stop.canFinish ? undefined : 'finish-hint'"
        @click="emit('finish')"
      >
        立即写报告
      </button>
      <span v-if="!stop.canFinish" id="finish-hint" class="hint">至少完成一个子主题后可用</span>
    </div>
  </section>

  <section v-else-if="status === 'cancelled' && canRestore" class="stop-card cancelled" aria-label="研究已停止">
    <div class="s-head">
      <span class="s-badge">已停止</span>
      <span class="s-sub">
        <template v-if="stop">已完成 {{ stop.subtopicsDone }} / {{ stop.subtopicsTotal }} 个子主题，读过 {{ stop.sources }} 个来源。</template>
        可以从停下的地方恢复研究。
      </span>
    </div>
    <div class="actions">
      <button class="btn primary small" type="button" data-action="restore" :disabled="busy" @click="emit('restore')">恢复</button>
    </div>
  </section>
</template>

<script setup lang="ts">
// 停止卡：暂停的研究显示进度卡、"目前发现"与"继续 / 立即写报告"；被取消且可恢复的研究显示"已停止 + 恢复"。
import type { StopInfo, TodoItem } from "../../lib/chat";

defineProps<{ stop?: StopInfo; status: string; canRestore: boolean; busy: boolean }>();
const emit = defineEmits<{ continue: []; finish: []; restore: [] }>();

const TODO_MARK: Record<TodoItem["status"], string> = { done: "✓", in_progress: "●", pending: "○", skipped: "–" };
const TODO_LABEL: Record<TodoItem["status"], string> = { done: "已完成", in_progress: "进行中", pending: "未开始", skipped: "已跳过" };
</script>

<style scoped>
.stop-card {
  margin-left: 24px;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #ffffff;
  padding: 12px 14px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  font-size: 0.86rem;
}
.s-head {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}
.s-badge {
  font-weight: 700;
  font-size: 0.78rem;
  padding: 2px 9px;
  border-radius: 10px;
  background: #e5e7eb;
  color: #374151;
}
.s-badge.paused {
  background: #fef3c7;
  color: #92400e;
}
.s-sub {
  color: #6b7280;
  font-size: 0.8rem;
}
.todo {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.todo li {
  display: flex;
  gap: 6px;
  color: #8c959f;
}
.todo li.done {
  color: #1a7f37;
}
.todo li.in_progress {
  color: #9a6700;
}
.todo li.skipped .t-title {
  text-decoration: line-through;
}
.mark {
  width: 12px;
  flex-shrink: 0;
  text-align: center;
}
.stats {
  margin: 0;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.stats > div {
  flex: 1;
  min-width: 90px;
  padding: 6px 10px;
  border-radius: 8px;
  background: #f9fafb;
  border: 1px solid #f3f4f6;
}
.stats dt {
  font-size: 0.72rem;
  color: #6b7280;
}
.stats dd {
  margin: 0;
  font-weight: 700;
  color: #6d28d9;
  font-variant-numeric: tabular-nums;
}
.findings {
  border-left: 3px solid #ddd6fe;
  padding-left: 10px;
}
.f-title {
  margin: 0 0 2px;
  font-size: 0.8rem;
  color: #4b5563;
}
.f-text {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
  color: #1f2937;
}
.actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}
.hint {
  font-size: 0.76rem;
  color: #8c959f;
}
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
}
</style>
